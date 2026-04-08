#!/usr/bin/env python3
"""
Warehance → Google Sheets Multi-Client Sync Agent

Features:
  - Auto-discovers new clients in Warehance and provisions Google Sheets
  - Fetches bill-details from Warehance API (or CSV for testing)
  - Transforms into per-order summary with Packaging Type column
  - Writes to client's Google Sheet (AllReports + Payments tabs)
  - Detects anomalies (Package=0, Pick&Pack=0) → Telegram alerts
  - Writes P&L data with packaging/shipping profit breakdown
  - Backs up raw data as CSV to Google Drive (optional)

Usage:
    python agent.py                                # All clients, last 1 day
    python agent.py --days 7                       # All clients, last 7 days
    python agent.py --client 001                   # Only client 001
    python agent.py --csv bill.csv --client 001    # Test with CSV
    python agent.py --schedule                     # Daily daemon at 06:00 UTC
    python agent.py --discover                     # Only run client discovery
    python agent.py --setup-business-pnl           # Create Business P&L tab
"""

import argparse
import json
import logging
import csv
import io
import os
import sys
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from warehance_client import WarehanceClient
from sheets_writer import GoogleSheetsWriter
from transformer import transform_bill_details, parse_csv_file, PICKING_KEYWORDS, _matches_any, _safe_float, _get_category, _get_order_number
from write_pnl import write_pnl_row, format_pnl_tab
from telegram_notifier import TelegramNotifier
from gdrive_backup import GDriveBackup
from client_discovery import discover_and_provision
from business_pnl import setup_business_pnl


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    return {
        "warehance_api_key": os.getenv("WAREHANCE_API_KEY"),
        "warehance_base_url": os.getenv("WAREHANCE_BASE_URL", "https://api.warehance.com/v1"),
        "google_sa_file": os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json"),
        "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
        "gdrive_backup_folder": os.getenv("GDRIVE_BACKUP_FOLDER", "Warehance Backups"),
        "days_back": int(os.getenv("SYNC_DAYS_BACK", "1")),
        "sync_mode": os.getenv("SYNC_MODE", "append"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "log_file": os.getenv("LOG_FILE", "logs/sync.log"),
        "enable_discovery": os.getenv("ENABLE_CLIENT_DISCOVERY", "true").lower() == "true",
        "enable_backup": os.getenv("ENABLE_GDRIVE_BACKUP", "false").lower() == "true",
    }


def load_clients(filepath: str = "clients.json") -> tuple[list[dict], str, str]:
    path = Path(__file__).parent / filepath
    if not path.exists():
        raise FileNotFoundError(f"clients.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    clients = data.get("clients", [])
    dashboard_id = data.get("dashboard_spreadsheet_id", "")
    pnl_id = data.get("pnl_spreadsheet_id", "")
    logging.getLogger("config").info(f"Loaded {len(clients)} clients")
    return clients, dashboard_id, pnl_id


def validate(config: dict, clients: list[dict], use_csv: bool = False) -> list[str]:
    errors = []
    if not use_csv and not config["warehance_api_key"]:
        errors.append("WAREHANCE_API_KEY is required (or use --csv)")
    sa = Path(config["google_sa_file"])
    if not sa.exists():
        errors.append(f"Service account not found: {sa.absolute()}")
    for c in clients:
        sid = c.get("spreadsheet_id", "")
        if not sid or sid.startswith("ВСТАВЬТЕ"):
            errors.append(f"Client '{c.get('name','?')}': spreadsheet_id not set")
        # Skip warehance_id / billing_profile_id check for manual-only clients (id=0)
        if c.get("warehance_id", 0) == 0:
            continue
        if not c.get("billing_profile_id"):
            errors.append(f"Client '{c.get('name','?')}': billing_profile_id not set")
    return errors


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: str, log_file: str):
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


# ---------------------------------------------------------------------------
# Split-day pick fee helpers
# ---------------------------------------------------------------------------

def _fetch_prev_day_pick_fees(
    client: dict,
    config: dict,
    target_orders: set[str],
) -> dict[str, float]:
    """
    Fetch the previous day's bill for the same client to extract pick fees
    for orders that were picked one day and shipped the next.

    Returns dict mapping order_number -> pick_fee from previous day.
    """
    logger = logging.getLogger("split_day")
    pacific = ZoneInfo("America/Los_Angeles")
    now_pacific = datetime.now(pacific)
    # Previous day = target_day - 1 (so 2 days back from now if current sync is 1 day back)
    prev_day = (now_pacific - timedelta(days=config["days_back"] + 1)).date()

    day_start_pt = datetime(prev_day.year, prev_day.month, prev_day.day, 0, 0, 0, tzinfo=pacific)
    day_end_pt = datetime(prev_day.year, prev_day.month, prev_day.day, 23, 59, 59, tzinfo=pacific)
    _fmt_tz = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S") + dt.strftime("%z")[:3] + ":" + dt.strftime("%z")[3:]
    bill_start = _fmt_tz(day_start_pt)
    bill_end = _fmt_tz(day_end_pt)

    logger.info(f"Fetching prev-day bill for {client['name']}: {prev_day} ({bill_start} — {bill_end})")

    api_key = os.getenv("WAREHANCE_API_KEY")
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    # Create bill for previous day
    r = requests.post("https://api.warehance.com/v1/bills", headers=headers, json={
        "client_id": client["warehance_id"],
        "billing_profile_id": client.get("billing_profile_id"),
        "start_date": bill_start,
        "end_date": bill_end,
    }, timeout=30)
    r.raise_for_status()
    bill_id = r.json()["data"]["id"]
    logger.info(f"Previous-day bill {bill_id} created")

    # Wait for generation
    h = {"X-API-Key": api_key}
    prev_rows = []
    for attempt in range(15):
        try:
            r2 = requests.get(f"https://api.warehance.com/v1/bills/{bill_id}", headers=h, timeout=30)
            bill_data = r2.json()["data"]
            csv_url = bill_data.get("line_item_details_csv_url", "")
            if csv_url and bill_data.get("generation_status") == "Completed":
                cr = requests.get(csv_url, timeout=60)
                cr.raise_for_status()
                reader = csv.DictReader(io.StringIO(cr.text))
                prev_rows = list(reader)
                break
        except Exception:
            pass
        time.sleep(2)

    if not prev_rows:
        logger.info("No rows in previous day's bill")
        return {}

    # Extract pick fees ONLY for the target orders
    pick_fees: dict[str, float] = {}
    for row in prev_rows:
        order_num = _get_order_number(row)
        if order_num not in target_orders:
            continue
        category = _get_category(row)
        if _matches_any(category, PICKING_KEYWORDS):
            amount = _safe_float(row.get("Amount", "0"))
            pick_fees[order_num] = pick_fees.get(order_num, 0) + amount

    logger.info(
        f"Previous day: {len(prev_rows)} rows total, "
        f"found pick fees for {len(pick_fees)}/{len(target_orders)} target orders"
    )
    return pick_fees


def _merge_pick_fees(result: dict, prev_pick: dict[str, float]):
    """
    Merge pick fees from the previous day into the current result.
    Updates report_rows in-place and removes resolved orders from anomalies.
    """
    # Update report_rows
    for row in result["report_rows"]:
        onum = row.get("Order Number", "")
        if onum in prev_pick:
            from transformer import _round2
            fee = _round2(prev_pick[onum])
            row["Pick&Pack fee"] = fee
            # Recalculate total for this order row
            pick = fee
            pkg = row["Package cost"] if isinstance(row["Package cost"], (int, float)) else 0
            ship = row["Shipping cost"] if isinstance(row["Shipping cost"], (int, float)) else 0
            row["Total"] = _round2(pick + pkg + ship)

    # Recalculate grand total
    from transformer import _round2
    grand_total = sum(
        r["Total"] for r in result["report_rows"]
        if isinstance(r["Total"], (int, float)) and r.get("Order Number") != "Total"
    )
    result["grand_total"] = _round2(grand_total)

    # Update Total row
    for row in result["report_rows"]:
        if row.get("Order Number") == "Total":
            row["Total"] = _round2(grand_total)
            break

    # Update payments
    result["payments_row"]["paid"] = _round2(grand_total)

    # Remove resolved anomalies
    resolved = set(prev_pick.keys())
    result["anomalies"] = [
        a for a in result["anomalies"]
        if not (a.get("type") == "missing_pick_fee" and a.get("order_number") in resolved)
    ]
    result["missing_pick_orders"] = [
        o for o in result.get("missing_pick_orders", []) if o not in resolved
    ]


# ---------------------------------------------------------------------------
# Sync one client
# ---------------------------------------------------------------------------

def sync_client(
    client: dict,
    config: dict,
    gs: GoogleSheetsWriter,
    tg: TelegramNotifier,
    backup: GDriveBackup | None = None,
    wh: WarehanceClient | None = None,
    csv_path: str | None = None,
) -> dict:
    logger = logging.getLogger("sync")
    client_name = client.get("name", "Unknown")
    client_number = client.get("number", "???")
    spreadsheet_id = client["spreadsheet_id"]

    logger.info(f"--- {client_number}.{client_name} ---")

    # 1. Fetch raw data
    if csv_path:
        raw_rows = parse_csv_file(csv_path)
    else:
        # Calculate billing day in Pacific Time (Warehance server timezone)
        pacific = ZoneInfo("America/Los_Angeles")
        now_pacific = datetime.now(pacific)
        target_day = (now_pacific - timedelta(days=config["days_back"])).date()

        # Billing range: target_day 00:00:00–23:59:59 Pacific Time with TZ offset
        day_start_pt = datetime(target_day.year, target_day.month, target_day.day, 0, 0, 0, tzinfo=pacific)
        day_end_pt = datetime(target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=pacific)
        _fmt_tz = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S") + dt.strftime("%z")[:3] + ":" + dt.strftime("%z")[3:]
        bill_start = _fmt_tz(day_start_pt)
        bill_end = _fmt_tz(day_end_pt)

        logger.info(f"Billing range: {target_day} → {bill_start} to {bill_end}")

        billing_profile_id = client.get("billing_profile_id")
        if not billing_profile_id:
            logger.warning(f"No billing_profile_id for {client_name}")
            return {"client": client_name, "error": "no billing_profile_id"}

        api_key = os.getenv("WAREHANCE_API_KEY")
        headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

        try:
            r = requests.post("https://api.warehance.com/v1/bills", headers=headers, json={
                "client_id": client["warehance_id"],
                "billing_profile_id": billing_profile_id,
                "start_date": bill_start,
                "end_date": bill_end,
            }, timeout=30)
            r.raise_for_status()
            bill_id = r.json()["data"]["id"]
            logger.info(f"Created bill {bill_id} for {client_name} ({target_day})")
        except Exception as e:
            logger.error(f"Bill creation failed for {client_name}: {e}")
            return {"client": client_name, "error": str(e)}

        # Wait for bill generation
        raw_rows = []
        h = {"X-API-Key": api_key}
        for attempt in range(15):
            try:
                r2 = requests.get(f"https://api.warehance.com/v1/bills/{bill_id}", headers=h, timeout=30)
                bill_data = r2.json()["data"]
                csv_url = bill_data.get("line_item_details_csv_url", "")
                if csv_url and bill_data.get("generation_status") == "Completed":
                    cr = requests.get(csv_url, timeout=60)
                    cr.raise_for_status()
                    reader = csv.DictReader(io.StringIO(cr.text))
                    raw_rows = list(reader)
                    logger.info(f"Downloaded {len(raw_rows)} rows for {client_name}")
                    break
            except Exception:
                pass
            time.sleep(2)

    if not raw_rows:
        logger.info(f"No bill data for {client_name} — writing zero-activity report")
        # Build a minimal zero-activity result so AllReports/Payments/P&L still get a row
        pacific = ZoneInfo("America/Los_Angeles")
        now_pacific = datetime.now(pacific)
        target_day = (now_pacific - timedelta(days=config["days_back"])).date()
        zero_date_full = target_day.strftime("%m.%d.%Y")   # AllReports format
        zero_date_short = target_day.strftime("%-m.%d")      # Date column short
        zero_date_pay = target_day.strftime("%m/%d/%y")      # Payments format

        report_rows = [
            {"Date": "", "Order Number": "Storage", "_spacer": "",
             "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "",
             "Pick&Pack fee": "", "Package cost": "", "Total": 0},
            {"Date": "", "Order Number": "Return Processing Charges", "_spacer": "",
             "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "",
             "Pick&Pack fee": "", "Package cost": "", "Total": 0},
            {"Date": "", "Order Number": "Return Labels Charges", "_spacer": "",
             "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "",
             "Pick&Pack fee": "", "Package cost": "", "Total": 0},
            {"Date": zero_date_full, "Order Number": "Total", "_spacer": "",
             "Tracking number": "", "Storage/Returns": "", "Shipping cost": "",
             "Pick&Pack fee": "", "Package cost": "", "Total": 0},
        ]
        from transformer import ALLREPORTS_HEADERS
        result = {
            "report_rows": report_rows,
            "headers": ALLREPORTS_HEADERS,
            "payments_row": {"date": zero_date_pay, "paid": 0},
            "payments_rows": [],  # SOLMAR 257 breakdown handled below
            "grand_total": 0,
            "anomalies": [],
            "report_date": zero_date_full,
            "missing_pick_orders": [],
            "category_totals": {"storage": 0, "return_processing": 0, "return_labels": 0, "orders_total": 0},
        }
        # For SOLMAR 257: write a zero Shopify line
        if client_number == "257":
            result["payments_rows"] = [{"date": zero_date_pay, "paid": 0, "comment": "Shopify"}]

        raw_rows = []  # keep it empty for the backup/transform skip below
        order_count = 0
        anomalies = []
        payments = result["payments_row"]
        report_label = f"Report — {client_number} {client_name}"

        # Skip directly to writing (no backup, no transform, no split-day)
        # --- Write AllReports ---
        allreports_tab = client.get("allreports_tab", "AllReports")
        report_date = result["report_date"]
        try:
            client_ss = gs.client.open_by_key(spreadsheet_id)
            ar_ws = client_ss.worksheet(allreports_tab)
            existing_vals = ar_ws.get_all_values()
            date_exists = any(
                row[0] == report_date and len(row) > 1 and row[1] == "Total"
                for row in existing_vals
            )
            if date_exists:
                logger.info(f"AllReports already has zero-activity data for {report_date}, skipping")
            else:
                gs.write_allreports(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=allreports_tab,
                    records=report_rows,
                    headers=result["headers"],
                    report_date=report_date,
                    report_label=report_label,
                )
        except Exception as e:
            logger.error(f"Zero-activity AllReports write failed for {client_name}: {e}")

        # --- Write Payments ---
        payments_tab = client.get("payments_tab", "Payments")
        payments_rows_list = result.get("payments_rows", [])
        if payments_rows_list and client_number == "257":
            for pr in payments_rows_list:
                gs.write_payment(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=payments_tab,
                    date=pr["date"],
                    paid_amount=pr["paid"],
                    comment=pr.get("comment", ""),
                )
        else:
            gs.write_payment(
                spreadsheet_id=spreadsheet_id,
                tab_name=payments_tab,
                date=payments["date"],
                paid_amount=payments["paid"],
            )

        # --- Write P&L ---
        try:
            pnl_date = target_day.strftime("%m/%d/%Y")
            write_pnl_row(
                service_account_file=config["google_sa_file"],
                client_number=client_number,
                client_name=client_name,
                date_str=pnl_date,
                transform_result=result,
                shipments=[],
                pnl_spreadsheet_id=config.get("pnl_spreadsheet_id", ""),
            )
            logger.info(f"P&L zero-activity row written for {client_name}")
        except Exception as e:
            logger.warning(f"P&L write failed for zero-activity {client_name}: {e}")

        return {"client": client_name, "raw_rows": 0, "orders": 0, "total": 0}

    # 2. Backup raw data to Google Drive
    if config.get("enable_backup") and backup:
        try:
            backup.backup_rows(
                client_number=client_number,
                client_name=client_name,
                rows=raw_rows,
                root_folder_name=config["gdrive_backup_folder"],
            )
        except Exception as e:
            logger.error(f"Backup failed for {client_name}: {e}")

    # 3. Transform with client-level alert settings
    alert_settings = {
        "check_package_cost": client.get("check_package_cost", True),
        "check_pick_fee": client.get("check_pick_fee", True),
    }
    result = transform_bill_details(
        raw_rows, client_name=client_name,
        client_number=client_number, alert_settings=alert_settings,
    )

    # 3a. Split-day pick fee resolution: fetch previous day's bill
    missing_pick = result.get("missing_pick_orders", [])
    if missing_pick and not csv_path:
        logger.info(
            f"{client_name}: {len(missing_pick)} orders missing pick fee — "
            f"fetching previous day's bill for resolution"
        )
        try:
            prev_pick = _fetch_prev_day_pick_fees(
                client=client, config=config, target_orders=set(missing_pick),
            )
            if prev_pick:
                logger.info(f"Resolved {len(prev_pick)} pick fees from previous day")
                _merge_pick_fees(result, prev_pick)
            else:
                logger.info("No matching pick fees found in previous day's bill")
        except Exception as e:
            logger.warning(f"Previous-day pick fee fetch failed for {client_name}: {e}")

    report_rows = result["report_rows"]
    payments = result["payments_row"]
    anomalies = result["anomalies"]

    order_count = sum(
        1 for r in report_rows
        if r["Order Number"] not in {
            "Storage", "Return Processing Charges",
            "Return Labels Charges", "Total"
        }
    )

    # 3b. Build per-service payments breakdown for client 257 (SOLMAR)
    if client_number == "257":
        cat_totals = result.get("category_totals", {})
        pay_date = payments["date"]
        prows = []
        storage = cat_totals.get("storage", 0)
        orders_total = cat_totals.get("orders_total", 0)
        returns = cat_totals.get("return_processing", 0)
        ret_labels = cat_totals.get("return_labels", 0)
        if storage > 0:
            prows.append({"date": pay_date, "paid": round(storage, 2), "comment": "Storage"})
        if orders_total > 0:
            prows.append({"date": "" if prows else pay_date, "paid": round(orders_total, 2), "comment": "Shopify"})
        if returns > 0:
            prows.append({"date": "" if prows else pay_date, "paid": round(returns, 2), "comment": "Returns"})
        if ret_labels > 0:
            prows.append({"date": "" if prows else pay_date, "paid": round(ret_labels, 2), "comment": "Return Labels Charges"})
        if not prows:
            prows.append({"date": pay_date, "paid": 0, "comment": "Storage"})
        result["payments_rows"] = prows

    # 4. Send anomaly alerts to Telegram
    if anomalies:
        tg.notify_anomalies(
            client_name=client_name,
            client_number=client_number,
            anomalies=anomalies,
            spreadsheet_id=spreadsheet_id,
        )

    # 5. Write AllReports tab (with duplicate check)
    allreports_tab = client.get("allreports_tab", "AllReports")
    report_label = f"Report — {client_number} {client_name}"

    # Dedup check: see if this date already exists in AllReports
    report_date = result["report_date"]
    try:
        client_ss = gs.client.open_by_key(spreadsheet_id)
        ar_ws = client_ss.worksheet(allreports_tab)
        existing_vals = ar_ws.get_all_values()
        # Check if report_date already appears in the Total rows
        date_exists = any(
            row[0] == report_date and len(row) > 1 and row[1] == "Total"
            for row in existing_vals
        )
        if date_exists:
            logger.info(f"AllReports already has data for {report_date}, skipping write for {client_name}")
        else:
            gs.write_allreports(
                spreadsheet_id=spreadsheet_id,
                tab_name=allreports_tab,
                records=report_rows,
                headers=result["headers"],
                report_date=report_date,
                report_label=report_label,
            )
    except Exception as e:
        logger.error(f"AllReports dedup check failed for {client_name}, writing anyway: {e}")
        gs.write_allreports(
            spreadsheet_id=spreadsheet_id,
            tab_name=allreports_tab,
            records=report_rows,
            headers=result["headers"],
            report_date=report_date,
            report_label=report_label,
        )

    # 6. Write Payments tab
    payments_tab = client.get("payments_tab", "Payments")
    payments_rows_list = result.get("payments_rows", [])

    if payments_rows_list and client_number == "257":
        # Client 257 (SOLMAR): write multiple payment rows with comments
        for pr in payments_rows_list:
            gs.write_payment(
                spreadsheet_id=spreadsheet_id,
                tab_name=payments_tab,
                date=pr["date"],
                paid_amount=pr["paid"],
                comment=pr.get("comment", ""),
            )
        logger.info(
            f"Payments: wrote {len(payments_rows_list)} lines for {client_name} "
            f"(breakdown: {', '.join(p.get('comment','') for p in payments_rows_list)})"
        )
    else:
        # Standard: single payment line
        gs.write_payment(
            spreadsheet_id=spreadsheet_id,
            tab_name=payments_tab,
            date=payments["date"],
            paid_amount=payments["paid"],
        )

    # 7. Write P&L Data (with retry for robustness)
    pnl_attempts = 2
    for pnl_try in range(pnl_attempts):
        try:
            pacific = ZoneInfo("America/Los_Angeles")
            now_pacific = datetime.now(pacific)
            target_day = (now_pacific - timedelta(days=config["days_back"])).date()
            pnl_date = target_day.strftime("%m/%d/%Y")

            # Fetch shipments for real cost data (same Pacific Time range)
            shipments = []
            if not csv_path:
                try:
                    day_start_pt = datetime(target_day.year, target_day.month, target_day.day, 0, 0, 0, tzinfo=pacific)
                    day_end_pt = datetime(target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=pacific)
                    ship_start = day_start_pt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    ship_end = day_end_pt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    api_key = os.getenv("WAREHANCE_API_KEY")
                    from warehance_client import WarehanceClient as WC
                    wh_temp = WC(api_key=api_key)
                    shipments = wh_temp.get_shipments(
                        client_id=client["warehance_id"],
                        date_from=ship_start,
                        date_to=ship_end,
                    )
                except Exception as e:
                    logger.warning(f"Shipments fetch for P&L failed for {client_name}: {e}")

            write_pnl_row(
                service_account_file=config["google_sa_file"],
                client_number=client_number,
                client_name=client_name,
                date_str=pnl_date,
                transform_result=result,
                shipments=shipments,
                pnl_spreadsheet_id=config.get("pnl_spreadsheet_id", ""),
            )
            logger.info(f"P&L row written for {client_name}")
            break
        except Exception as e:
            if pnl_try < pnl_attempts - 1:
                logger.warning(f"P&L write attempt {pnl_try + 1} failed for {client_name}: {e}, retrying...")
                time.sleep(3)
            else:
                logger.warning(f"P&L write failed for {client_name} after {pnl_attempts} attempts: {e}")

    return {
        "client": client_name,
        "raw_rows": len(raw_rows),
        "orders": order_count,
        "total": result["grand_total"],
        "anomalies": len(anomalies),
    }


# ---------------------------------------------------------------------------
# Sync all
# ---------------------------------------------------------------------------

def sync_all(
    config: dict,
    clients: list[dict],
    csv_path: str | None = None,
) -> list[dict]:
    logger = logging.getLogger("sync")
    start = time.time()
    logger.info("=" * 60)
    logger.info(f"Sync started | clients={len(clients)} | days_back={config['days_back']}")

    gs = GoogleSheetsWriter(service_account_file=config["google_sa_file"])
    tg = TelegramNotifier(
        bot_token=config["telegram_bot_token"],
        chat_id=config["telegram_chat_id"],
    )

    # Auto-discover new clients before syncing
    if config.get("enable_discovery") and not csv_path:
        try:
            wh = WarehanceClient(
                api_key=config["warehance_api_key"],
                base_url=config["warehance_base_url"],
            )
            new_clients = discover_and_provision(
                wh=wh,
                service_account_file=config["google_sa_file"],
                dashboard_id=config.get("dashboard_spreadsheet_id", ""),
                tg=tg,
            )
            if new_clients:
                logger.info(f"Discovered {len(new_clients)} new client(s), reloading clients.json")
                clients, _, _ = load_clients()
        except Exception as e:
            logger.warning(f"Client discovery failed (continuing with existing clients): {e}")

    backup = None
    if config.get("enable_backup"):
        try:
            backup = GDriveBackup(service_account_file=config["google_sa_file"])
        except Exception as e:
            logger.warning(f"Drive backup init failed: {e}")

    wh = None
    if not csv_path:
        wh = WarehanceClient(
            api_key=config["warehance_api_key"],
            base_url=config["warehance_base_url"],
        )
        if not wh.check_auth():
            tg.notify_error("Warehance auth failed. Проверьте API key.")
            raise RuntimeError("Warehance auth failed.")

    results = []
    for client in clients:
        # Skip manual-only clients (warehance_id=0) — they have no API data
        if not csv_path and client.get("warehance_id", 0) == 0:
            logger.info(f"Skipping {client.get('number','?')}.{client.get('name','?')} — manual-only client (warehance_id=0)")
            continue
        try:
            r = sync_client(client, config, gs, tg, backup=backup, wh=wh, csv_path=csv_path)
            results.append(r)
        except Exception as e:
            logger.error(f"Failed: {client.get('name','?')}: {e}", exc_info=True)
            results.append({"client": client.get("name", "?"), "error": str(e)})

    elapsed = round(time.time() - start, 2)

    # Summary log
    logger.info(f"Sync completed in {elapsed}s")
    for r in results:
        if "error" in r:
            logger.warning(f"  ❌ {r['client']}: {r['error']}")
        else:
            anom = f" | ⚠️{r.get('anomalies',0)} anomalies" if r.get("anomalies") else ""
            logger.info(f"  ✅ {r['client']}: {r.get('orders',0)} orders, ${r.get('total',0):.2f}{anom}")

    # Update Dashboard balances
    try:
        dash_id = config.get("dashboard_spreadsheet_id", "")
        if dash_id:
            dash_ss = gs.client.open_by_key(dash_id)
            dash_ws = dash_ss.worksheet("Clients")
            dash_vals = dash_ws.get_all_values()

            def _norm_num(s):
                """Normalize client number: strip whitespace & leading zeros."""
                return s.strip().lstrip("0") or "0"

            # Build set of normalized client numbers already in Dashboard
            dash_client_nums = set()
            for i, row in enumerate(dash_vals):
                if i == 0:
                    continue
                client_num = row[1].strip() if len(row) > 1 else ""
                if client_num:
                    dash_client_nums.add(_norm_num(client_num))
                for client in clients:
                    if _norm_num(client["number"]) == _norm_num(client_num):
                        try:
                            client_ss = gs.client.open_by_key(client["spreadsheet_id"])
                            pay_ws = client_ss.worksheet("Payments")
                            pay_vals = pay_ws.get_all_values()
                            balance = ""
                            for pr in pay_vals:
                                if len(pr) > 3 and pr[0].strip().lower() == "total":
                                    balance = pr[3]
                            dash_ws.update_cell(i + 1, 5, balance)
                        except Exception as e:
                            logger.warning(f"Dashboard balance error for {client_num}: {e}")
                        break

            # Add missing clients to Dashboard
            next_row = len(dash_vals) + 1
            for client in clients:
                if _norm_num(client["number"]) not in dash_client_nums:
                    try:
                        sid = client["spreadsheet_id"]
                        url = f"https://docs.google.com/spreadsheets/d/{sid}/edit"
                        # Read balance from the client's Payments tab
                        client_ss = gs.client.open_by_key(sid)
                        pay_ws = client_ss.worksheet("Payments")
                        pay_vals = pay_ws.get_all_values()
                        balance = ""
                        for pr in pay_vals:
                            if len(pr) > 3 and pr[0].strip().lower() == "total":
                                balance = pr[3]
                        row_data = [next_row - 1, client["number"], client["name"], url, balance]
                        dash_ws.update(f"A{next_row}:E{next_row}", [row_data], value_input_option="USER_ENTERED")
                        logger.info(f"Dashboard: added missing client {client['number']}.{client['name']}")
                        next_row += 1
                    except Exception as e:
                        logger.warning(f"Dashboard add client {client['number']} failed: {e}")

            logger.info("Dashboard balances updated")

            # Send balances list to Telegram
            try:
                dash_vals_updated = dash_ws.get_all_values()
                balance_lines = ["<b>💰 Балансы клиентов</b>", ""]
                total_balance = 0.0
                for row in dash_vals_updated[1:]:  # skip header
                    if len(row) >= 5 and row[1].strip():
                        client_num = row[1].strip()
                        client_name = row[2].strip() if len(row) > 2 else ""
                        bal_str = row[4].strip() if len(row) > 4 else ""
                        if bal_str:
                            try:
                                bal_val = float(bal_str.replace(",", "").replace("$", "").replace(" ", ""))
                                total_balance += bal_val
                                sign = "🟢" if bal_val >= 0 else "🔴"
                                balance_lines.append(f"{sign} <b>{client_num}</b> {client_name}: <code>${bal_val:,.2f}</code>")
                            except ValueError:
                                balance_lines.append(f"⚪ <b>{client_num}</b> {client_name}: {bal_str}")
                        else:
                            balance_lines.append(f"⚪ <b>{client_num}</b> {client_name}: —")
                balance_lines.append("")
                balance_lines.append(f"<b>Итого: <code>${total_balance:,.2f}</code></b>")
                tg.send("\n".join(balance_lines))
            except Exception as e:
                logger.warning(f"Telegram balances message failed: {e}")

    except Exception as e:
        logger.warning(f"Dashboard update failed: {e}")

    tg.notify_sync_summary(results, elapsed)

    logger.info("=" * 60)
    return results


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def run_scheduled(config: dict, clients: list[dict], run_time: str = "06:00"):
    import schedule
    logger = logging.getLogger("scheduler")
    logger.info(f"Daily sync scheduled at {run_time} UTC")

    def job():
        try:
            # Reload clients each run (may have been updated by discovery)
            fresh_clients, dash_id, pnl_id = load_clients()
            config["dashboard_spreadsheet_id"] = dash_id
            config["pnl_spreadsheet_id"] = pnl_id
            sync_all(config, fresh_clients)
        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)

    schedule.every().day.at(run_time).do(job)
    logger.info("Running initial sync...")
    job()

    while True:
        schedule.run_pending()
        time.sleep(60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Warehance → Google Sheets sync")
    parser.add_argument("--csv", metavar="FILE", help="Use local CSV instead of API")
    parser.add_argument("--client", metavar="NUM", help="Sync only this client number (e.g. 001)")
    parser.add_argument("--schedule", action="store_true", help="Run as daily daemon")
    parser.add_argument("--time", default="06:00", help="HH:MM UTC (default: 06:00)")
    parser.add_argument("--days", type=int, help="Override SYNC_DAYS_BACK")
    parser.add_argument("--discover", action="store_true", help="Only run client discovery, no sync")
    parser.add_argument("--setup-business-pnl", action="store_true", help="Create/update Business P&L tab")
    parser.add_argument("--no-discovery", action="store_true", help="Skip auto-discovery this run")
    args = parser.parse_args()

    config = load_config()
    if args.days:
        config["days_back"] = args.days
    if args.no_discovery:
        config["enable_discovery"] = False

    setup_logging(config["log_level"], config["log_file"])
    logger = logging.getLogger("main")

    try:
        clients, dashboard_id, pnl_id = load_clients()
        config["dashboard_spreadsheet_id"] = dashboard_id
        config["pnl_spreadsheet_id"] = pnl_id
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # --- Discovery-only mode ---
    if args.discover:
        wh = WarehanceClient(
            api_key=config["warehance_api_key"],
            base_url=config["warehance_base_url"],
        )
        tg = TelegramNotifier(
            bot_token=config["telegram_bot_token"],
            chat_id=config["telegram_chat_id"],
        )
        result = discover_and_provision(
            wh=wh,
            service_account_file=config["google_sa_file"],
            dashboard_id=dashboard_id,
            tg=tg,
        )
        print(f"\nDiscovered {len(result)} new client(s)")
        for c in result:
            print(f"  {c['number']}.{c['name']} → {c['spreadsheet_id']}")
        return

    # --- Business P&L setup mode ---
    if args.setup_business_pnl:
        setup_business_pnl(
            service_account_file=config["google_sa_file"],
            pnl_spreadsheet_id=pnl_id,
        )
        print("Business P&L tab created/updated!")
        return

    # Filter by client number
    if args.client:
        clients = [c for c in clients if c.get("number") == args.client]
        if not clients:
            logger.error(f"Client '{args.client}' not found in clients.json")
            sys.exit(1)

    errors = validate(config, clients, use_csv=bool(args.csv))
    if errors:
        for e in errors:
            logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.schedule:
        run_scheduled(config, clients, run_time=args.time)
    else:
        try:
            results = sync_all(config, clients, csv_path=args.csv)
            print()
            for r in results:
                if "error" in r:
                    print(f"❌ {r['client']}: {r['error']}")
                else:
                    anom = f" | ⚠️{r.get('anomalies',0)}" if r.get("anomalies") else ""
                    print(f"✅ {r['client']}: {r.get('orders',0)} orders, ${r.get('total',0):.2f}{anom}")
        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
