#!/usr/bin/env python3
"""
Backfill: creates bills day-by-day via API, downloads CSV, writes to Sheets.
Usage: python3 backfill.py 001 2026-01-01 2026-02-11
       python3 backfill.py 001 2026-01-01 2026-03-02 --clear
"""

import json
import logging
import sys
import time
import requests
import io
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")

from sheets_writer import GoogleSheetsWriter
from transformer import transform_bill_details

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("backfill")

API_KEY = os.getenv("WAREHANCE_API_KEY")
SA_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")
BASE = "https://api.warehance.com/v1"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

PACIFIC = ZoneInfo("America/Los_Angeles")


def create_bill(client_id: int, profile_id: int, start_utc: str, end_utc: str) -> int:
    """POST /bills → returns bill ID."""
    r = requests.post(f"{BASE}/bills", headers=HEADERS, json={
        "client_id": client_id,
        "billing_profile_id": profile_id,
        "start_date": start_utc,
        "end_date": end_utc,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["id"]


def wait_for_bill(bill_id: int, max_wait: int = 30) -> dict:
    """Poll until bill is ready, return bill data with CSV URL."""
    h = {"X-API-Key": API_KEY}
    for i in range(max_wait):
        r = requests.get(f"{BASE}/bills/{bill_id}", headers=h, timeout=30)
        data = r.json()["data"]
        status = data.get("generation_status", "")
        if status == "Completed" and data.get("line_item_details_csv_url"):
            return data
        time.sleep(1)
    return data


def download_csv(csv_url: str) -> list[dict]:
    r = requests.get(csv_url, timeout=60)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    return list(reader)


def day_to_utc_range(day_date) -> tuple[str, str]:
    """Convert a date to Pacific Time 00:00:00–23:59:59, returned as UTC strings."""
    day_start_pt = datetime(day_date.year, day_date.month, day_date.day, 0, 0, 0, tzinfo=PACIFIC)
    day_end_pt = datetime(day_date.year, day_date.month, day_date.day, 23, 59, 59, tzinfo=PACIFIC)
    start_utc = day_start_pt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = day_end_pt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return start_utc, end_utc


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 backfill.py CLIENT_NUM [START_DATE] [END_DATE] [--clear]")
        print("Example: python3 backfill.py 001 2026-01-01 2026-02-11")
        print("         python3 backfill.py 001 2026-01-01 2026-03-02 --clear")
        sys.exit(1)

    # Parse args (positional + --clear flag)
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    clear_first = "--clear" in sys.argv

    client_num = args[0]
    start_date = args[1] if len(args) > 1 else "2026-01-01"
    end_date = args[2] if len(args) > 2 else "2026-02-11"

    # Load client
    with open("clients.json") as f:
        data = json.load(f)
    client = None
    for c in data["clients"]:
        if c["number"] == client_num:
            client = c
            break
    if not client:
        print(f"Client {client_num} not found!")
        sys.exit(1)

    client_id = client["warehance_id"]
    profile_id = client["billing_profile_id"]
    spreadsheet_id = client["spreadsheet_id"]
    client_name = client["name"]
    allreports_tab = client.get("allreports_tab", "AllReports")
    payments_tab = client.get("payments_tab", "Payments")
    alert_settings = {
        "check_package_cost": client.get("check_package_cost", True),
        "check_pick_fee": client.get("check_pick_fee", True),
    }

    # Generate date list
    dt_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    dt_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    days = []
    current = dt_start
    while current <= dt_end:
        days.append(current)
        current += timedelta(days=1)

    print(f"\n{'='*60}")
    print(f"BACKFILL: {client_num}.{client_name}")
    print(f"Profile: {profile_id}")
    print(f"Period: {start_date} → {end_date} ({len(days)} days)")
    print(f"Sheet: {spreadsheet_id}")
    print(f"Timezone: Pacific Time (America/Los_Angeles)")
    if clear_first:
        print(f"MODE: CLEAR + REWRITE (AllReports & Payments will be wiped first)")
    print(f"{'='*60}")

    confirm = input("\nStart backfill? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    gs = GoogleSheetsWriter(service_account_file=SA_FILE)

    # Clear tabs if requested
    if clear_first:
        print("\nClearing and re-initializing tabs...")
        gs.clear_and_init_allreports(spreadsheet_id, allreports_tab, client_num, client_name)
        gs.clear_and_init_payments(spreadsheet_id, payments_tab)
        print("Tabs cleared and re-initialized with branded headers.")

    total_orders = 0
    total_amount = 0.0
    skipped = 0

    for i, day_date in enumerate(days):
        print(f"\n[{i+1}/{len(days)}] {day_date}", end=" ")

        start_utc, end_utc = day_to_utc_range(day_date)

        # 1. Create bill
        try:
            bill_id = create_bill(client_id, profile_id, start_utc, end_utc)
        except Exception as e:
            print(f"❌ Create failed: {e}")
            continue

        # 2. Wait for generation
        bill_data = wait_for_bill(bill_id)
        csv_url = bill_data.get("line_item_details_csv_url", "")
        charges = bill_data.get("total_amount", 0)

        # Format payment date
        pay_date = day_date.strftime("%m/%d/%y")

        if not csv_url or charges == 0:
            # Write $0 day to AllReports and Payments
            from transformer import ALLREPORTS_HEADERS
            zero_date = day_date.strftime("%m.%d.%Y")
            zero_rows = [
                {"Date": "", "Order Number": "Storage", "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "", "Total": 0},
                {"Date": "", "Order Number": "Return Processing Charges", "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "", "Total": 0},
                {"Date": "", "Order Number": "Return Labels Charges", "Tracking number": "", "Storage/Returns": 0, "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "", "Total": 0},
                {"Date": zero_date, "Order Number": "Total", "Tracking number": "", "Storage/Returns": "", "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "", "Total": 0},
            ]
            try:
                report_label = f"Report — {client_num} {client_name}"
                gs.write_allreports(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=allreports_tab,
                    records=zero_rows,
                    headers=ALLREPORTS_HEADERS,
                    report_date=zero_date,
                    report_label=report_label,
                )
                gs.write_payment(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=payments_tab,
                    date=pay_date,
                    paid_amount=0,
                )
                print(f"⏭ $0 — written to both tabs")
            except Exception as e:
                print(f"❌ Write failed: {e}")
            skipped += 1
            continue

        # 3. Download CSV
        try:
            rows = download_csv(csv_url)
        except Exception as e:
            print(f"❌ CSV failed: {e}")
            continue

        if not rows:
            skipped += 1
            continue

        # 4. Transform
        result = transform_bill_details(rows, client_name=client_name, alert_settings=alert_settings)
        report_rows = result["report_rows"]
        order_count = sum(
            1 for r in report_rows
            if r["Order Number"] not in {"Storage", "Return Processing Charges", "Return Labels Charges", "Total"}
        )

        # 5. Write to Sheets
        try:
            report_label = f"Report — {client_num} {client_name}"
            gs.write_allreports(
                spreadsheet_id=spreadsheet_id,
                tab_name=allreports_tab,
                records=report_rows,
                headers=result["headers"],
                report_date=result["report_date"],
                report_label=report_label,
            )
            gs.write_payment(
                spreadsheet_id=spreadsheet_id,
                tab_name=payments_tab,
                date=result["payments_row"]["date"],
                paid_amount=result["payments_row"]["paid"],
            )
        except Exception as e:
            print(f"❌ Sheets write failed: {e}")
            continue

        total_orders += order_count
        total_amount += result["grand_total"]
        print(f"✅ {order_count} orders, ${result['grand_total']:.2f}")

        # Rate limit
        time.sleep(2)

    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE")
    print(f"Days processed: {len(days) - skipped}/{len(days)}")
    print(f"Total orders: {total_orders}")
    print(f"Total amount: ${total_amount:.2f}")
    print(f"Skipped (no data): {skipped}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
