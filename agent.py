import requests
#!/usr/bin/env python3
"""
Warehance → Google Sheets Multi-Client Sync Agent

Features:
  - Fetches bill-details from Warehance API (or CSV for testing)
  - Transforms into per-order summary (multiple parcels summed)
  - Writes to client's Google Sheet (AllReports + Payments tabs)
  - Detects anomalies (Package=0, Pick&Pack=0) → Telegram alerts
  - Backs up raw data as CSV to Google Drive

Usage:
    python agent.py                                # All clients, last 1 day
    python agent.py --days 7                       # All clients, last 7 days
    python agent.py --client 001                   # Only client 001
    python agent.py --csv bill.csv --client 001    # Test with CSV
    python agent.py --schedule                     # Daily daemon at 06:00 UTC
"""

import argparse
import json
import logging
import csv
import io
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from warehance_client import WarehanceClient
from sheets_writer import GoogleSheetsWriter
from transformer import transform_bill_details, parse_csv_file
from telegram_notifier import TelegramNotifier
from gdrive_backup import GDriveBackup


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
    }


def load_clients(filepath: str = "clients.json") -> list[dict]:
    path = Path(__file__).parent / filepath
    if not path.exists():
        raise FileNotFoundError(f"clients.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    clients = data.get("clients", [])
    logging.getLogger("config").info(f"Loaded {len(clients)} clients")
    return clients


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
        # Create bill via API for yesterday
        yesterday = datetime.now() - timedelta(days=config["days_back"])
        day_start = yesterday.strftime("%Y-%m-%d")
        day_end = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")
        
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
                "start_date": f"{day_start}T00:00:00Z",
                "end_date": f"{day_end}T00:00:00Z",
            }, timeout=30)
            r.raise_for_status()
            bill_id = r.json()["data"]["id"]
            logger.info(f"Created bill {bill_id} for {client_name} ({day_start})")
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
        logger.info(f"No data for {client_name}")
        return {"client": client_name, "raw_rows": 0, "orders": 0, "total": 0}

    # 2. Backup raw data to Google Drive
    if False and backup:
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
    result = transform_bill_details(raw_rows, client_name=client_name, alert_settings=alert_settings)
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

    # 4. Send anomaly alerts to Telegram
    if anomalies:
        tg.notify_anomalies(
            client_name=client_name,
            client_number=client_number,
            anomalies=anomalies,
            spreadsheet_id=spreadsheet_id,
        )

    # 5. Write AllReports tab
    allreports_tab = client.get("allreports_tab", "AllReports")
    report_label = f"Report — {client_number} {client_name}"
    gs.write_allreports(
        spreadsheet_id=spreadsheet_id,
        tab_name=allreports_tab,
        records=report_rows,
        headers=result["headers"],
        report_date=result["report_date"],
        report_label=report_label,
    )

    # 6. Write Payments tab
    payments_tab = client.get("payments_tab", "Payments")
    gs.write_payment(
        spreadsheet_id=spreadsheet_id,
        tab_name=payments_tab,
        date=payments["date"],
        paid_amount=payments["paid"],
    )

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

    backup = None
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

    # Telegram summary
    # Update Dashboard balances
    try:
        dash_id = config.get("dashboard_spreadsheet_id", "")
        if dash_id:
            dash_ss = gs.client.open_by_key(dash_id)
            dash_ws = dash_ss.worksheet("Clients")
            dash_vals = dash_ws.get_all_values()
            for i, row in enumerate(dash_vals):
                if i == 0:
                    continue
                client_num = row[1] if len(row) > 1 else ""
                # Find matching client and get their Payments total
                for client in clients:
                    if client["number"] == client_num:
                        try:
                            client_ss = gs.client.open_by_key(client["spreadsheet_id"])
                            pay_ws = client_ss.worksheet("Payments")
                            pay_vals = pay_ws.get_all_values()
                            # Find Total row — last row with "Total" in column B
                            balance = ""
                            for pr in pay_vals:
                                if len(pr) > 3 and pr[1].strip().lower() == "total":
                                    balance = pr[3]
                            dash_ws.update_cell(i + 1, 5, balance)
                        except Exception as e:
                            logger.warning(f"Dashboard balance error for {client_num}: {e}")
                        break
            logger.info("Dashboard balances updated")
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
            sync_all(config, clients)
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
    args = parser.parse_args()

    config = load_config()
    if args.days:
        config["days_back"] = args.days

    setup_logging(config["log_level"], config["log_file"])
    logger = logging.getLogger("main")

    try:
        clients = load_clients()
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Filter by client number (e.g. --client 001)
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
