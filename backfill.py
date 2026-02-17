#!/usr/bin/env python3
"""
Backfill: creates bills day-by-day via API, downloads CSV, writes to Sheets + P&L.
Usage: python3 backfill.py 001 2026-01-01 2026-02-11
       python3 backfill.py 001 2026-02-14 2026-02-17 --yes  (skip confirmation)
"""

import json
import logging
import sys
import time
import requests
import io
import csv
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")

from sheets_writer import GoogleSheetsWriter
from transformer import transform_bill_details
from warehance_client import WarehanceClient
from write_pnl import write_pnl_row

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("backfill")

API_KEY = os.getenv("WAREHANCE_API_KEY")
SA_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")
BASE = "https://api.warehance.com/v1"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}


def create_bill(client_id: int, profile_id: int, start: str, end: str) -> int:
    """POST /bills → returns bill ID."""
    r = requests.post(f"{BASE}/bills", headers=HEADERS, json={
        "client_id": client_id,
        "billing_profile_id": profile_id,
        "start_date": f"{start}T00:00:00Z",
        "end_date": f"{end}T00:00:00Z",
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


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 backfill.py CLIENT_NUM [START_DATE] [END_DATE] [--yes]")
        print("Example: python3 backfill.py 001 2026-01-01 2026-02-11")
        sys.exit(1)

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    auto_yes = "--yes" in sys.argv

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
    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    days = []
    current = dt_start
    while current < dt_end:
        next_day = current + timedelta(days=1)
        days.append((current.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d")))
        current = next_day

    print(f"\n{'='*60}")
    print(f"BACKFILL: {client_num}.{client_name}")
    print(f"Profile: {profile_id}")
    print(f"Period: {start_date} → {end_date} ({len(days)} days)")
    print(f"Sheet: {spreadsheet_id}")
    print(f"{'='*60}")

    if not auto_yes:
        confirm = input("\nStart backfill? (y/n): ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    gs = GoogleSheetsWriter(service_account_file=SA_FILE)

    # Fetch shipments map once for this client
    wh = WarehanceClient(api_key=API_KEY)
    print("Fetching shipments data...")
    shipments_map = wh.get_shipments_map(client_id)
    print(f"Loaded {len(shipments_map)} order shipment records")

    total_orders = 0
    total_amount = 0.0
    skipped = 0

    for i, (day_start, day_end) in enumerate(days):
        print(f"\n[{i+1}/{len(days)}] {day_start}", end=" ")

        # 1. Create bill
        try:
            bill_id = create_bill(client_id, profile_id, day_start, day_end)
        except Exception as e:
            print(f"❌ Create failed: {e}")
            continue

        # 2. Wait for generation
        bill_data = wait_for_bill(bill_id)
        csv_url = bill_data.get("line_item_details_csv_url", "")
        charges = bill_data.get("total_amount", 0)

        pay_date = datetime.strptime(day_start, "%Y-%m-%d").strftime("%m/%d/%y")
        pnl_date = datetime.strptime(day_start, "%Y-%m-%d").strftime("%m/%d/%Y")

        if not csv_url or charges == 0:
            # Write $0 day to AllReports and Payments
            from transformer import ALLREPORTS_HEADERS
            zero_date = datetime.strptime(day_start, "%Y-%m-%d").strftime("%m.%d.%Y")
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
                # Write $0 P&L row
                zero_result = {"report_rows": zero_rows, "grand_total": 0}
                write_pnl_row(SA_FILE, client_num, client_name, pnl_date,
                              zero_result, shipping_cost=0, packaging_cost=0)
                print(f"⏭ $0 — written to all tabs")
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

        # 5. Calculate shipment costs
        day_shipping_cost = 0.0
        day_packaging_cost = 0.0
        for r in report_rows:
            onum = r.get("Order Number", "")
            if onum in {"Storage", "Return Processing Charges", "Return Labels Charges", "Total"}:
                continue
            sdata = shipments_map.get(onum)
            if sdata:
                day_shipping_cost += sdata["shipment_cost"]
                day_packaging_cost += WarehanceClient.calc_packaging_cost(sdata["boxes"])

        # 6. Write to Sheets
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

        # 7. Write P&L row
        try:
            write_pnl_row(SA_FILE, client_num, client_name, pnl_date,
                          result,
                          shipping_cost=round(day_shipping_cost, 2),
                          packaging_cost=round(day_packaging_cost, 4))
        except Exception as e:
            print(f"⚠️ P&L write failed: {e}")

        total_orders += order_count
        total_amount += result["grand_total"]
        print(f"✅ {order_count} orders, ${result['grand_total']:.2f} | ship=${day_shipping_cost:.2f} pkg=${day_packaging_cost:.4f}")

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
