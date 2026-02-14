#!/usr/bin/env python3
"""
Warehance → Google Sheets Multi-Client Sync Agent

For each client in clients.json:
  1. Fetches bill-details from Warehance API (or CSV for testing)
  2. Transforms into per-order summary
  3. Writes to client's Google Sheet (FBM tab + Payments tab)

Usage:
    python agent.py                                # All clients, last 1 day
    python agent.py --days 7                       # All clients, last 7 days
    python agent.py --client 279                   # Only client 279
    python agent.py --csv bill.csv --client 001    # Test with CSV for one client
    python agent.py --schedule                     # Daily daemon at 06:00 UTC
    python agent.py --schedule --time 09:00        # Custom time
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from warehance_client import WarehanceClient
from sheets_writer import GoogleSheetsWriter
from transformer import transform_bill_details, parse_csv_file


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
        "days_back": int(os.getenv("SYNC_DAYS_BACK", "1")),
        "sync_mode": os.getenv("SYNC_MODE", "append"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "log_file": os.getenv("LOG_FILE", "logs/sync.log"),
    }


def load_clients(filepath: str = "clients.json") -> list[dict]:
    path = Path(__file__).parent / filepath
    if not path.exists():
        raise FileNotFoundError(
            f"clients.json not found at {path}\n"
            f"Create it from the template — see SETUP_GUIDE.md step 3.4"
        )
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    clients = data.get("clients", [])
    logger = logging.getLogger("config")
    logger.info(f"Loaded {len(clients)} clients from {filepath}")
    return clients


def validate(config: dict, clients: list[dict], use_csv: bool = False) -> list[str]:
    errors = []
    if not use_csv and not config["warehance_api_key"]:
        errors.append("WAREHANCE_API_KEY is required (or use --csv)")
    sa = Path(config["google_sa_file"])
    if not sa.exists():
        errors.append(f"Service account file not found: {sa.absolute()}")
    for c in clients:
        if not c.get("spreadsheet_id") or c["spreadsheet_id"].startswith("ВСТАВЬТЕ"):
            errors.append(f"Client '{c.get('name', '?')}': spreadsheet_id not set")
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
    wh: WarehanceClient | None = None,
    csv_path: str | None = None,
) -> dict:
    """Sync one client: Warehance → FBM tab + Payments tab."""
    logger = logging.getLogger("sync")
    client_name = client.get("name", client["warehance_client_id"])
    spreadsheet_id = client["spreadsheet_id"]

    logger.info(f"--- Client: {client_name} (ID: {client['warehance_client_id']}) ---")

    # 1. Fetch raw data
    if csv_path:
        raw_rows = parse_csv_file(csv_path)
    elif wh:
        raw_rows = wh.get_bill_details_for_client(
            client_id=client["warehance_client_id"],
            days_back=config["days_back"],
        )
    else:
        logger.error("No data source (API or CSV)")
        return {"client": client_name, "error": "no data source"}

    if not raw_rows:
        logger.info(f"No data for {client_name}")
        return {"client": client_name, "raw_rows": 0, "orders": 0}

    # 2. Transform
    result = transform_bill_details(raw_rows)
    fbm_rows = result["fbm_rows"]
    payments = result["payments_row"]

    order_count = sum(
        1 for r in fbm_rows
        if r["Order Number"] not in {
            "Storage", "Return Processing Charges",
            "Return Labels Charges", "Total"
        }
    )

    # 3. Write FBM tab
    fbm_tab = client.get("fbm_tab", "FBM")
    rows_written = gs.write_fbm(
        spreadsheet_id=spreadsheet_id,
        tab_name=fbm_tab,
        records=fbm_rows,
        headers=result["fbm_headers"],
        mode=config["sync_mode"],
    )

    # 4. Write Payments tab
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
        "rows_written": rows_written,
        "total": result["grand_total"],
    }


# ---------------------------------------------------------------------------
# Sync all clients
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

    wh = None
    if not csv_path:
        wh = WarehanceClient(
            api_key=config["warehance_api_key"],
            base_url=config["warehance_base_url"],
        )
        if not wh.check_auth():
            raise RuntimeError("Warehance auth failed. Check API key.")

    results = []
    for client in clients:
        try:
            r = sync_client(client, config, gs, wh=wh, csv_path=csv_path)
            results.append(r)
        except Exception as e:
            logger.error(f"Failed for {client.get('name', '?')}: {e}", exc_info=True)
            results.append({"client": client.get("name", "?"), "error": str(e)})

    elapsed = round(time.time() - start, 2)
    logger.info(f"Sync completed in {elapsed}s")
    for r in results:
        if "error" in r:
            logger.warning(f"  ❌ {r['client']}: {r['error']}")
        else:
            logger.info(f"  ✅ {r['client']}: {r.get('orders', 0)} orders, ${r.get('total', 0):.2f}")
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
    parser = argparse.ArgumentParser(
        description="Warehance → Google Sheets multi-client sync"
    )
    parser.add_argument("--csv", metavar="FILE", help="Use local CSV instead of API")
    parser.add_argument("--client", metavar="ID", help="Sync only this client ID")
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

    # Filter to one client if specified
    if args.client:
        clients = [c for c in clients if c["warehance_client_id"] == args.client]
        if not clients:
            logger.error(f"Client ID '{args.client}' not found in clients.json")
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
                    print(f"✅ {r['client']}: {r.get('orders', 0)} orders, "
                          f"${r.get('total', 0):.2f} → sheet updated")
        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
