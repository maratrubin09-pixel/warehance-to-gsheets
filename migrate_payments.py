#!/usr/bin/env python3
"""
Migration script: adds Total row (row 2) to existing Payments tabs.

Existing layout:
  Row 1: Headers (Date | Deposit | Paid | Balance | Comments | Customer info)
  Row 2+: Data rows

New layout:
  Row 1: Headers
  Row 2: Total row (Total | =SUM(B3:B) | =SUM(C3:C) | =B2-C2)
  Row 3+: Data rows

Also removes per-row Balance values (column D) since Balance is now
calculated only in the Total row.

Usage:
    python migrate_payments.py              # All clients
    python migrate_payments.py --client 257 # Single client
    python migrate_payments.py --dry-run    # Preview without changes
"""

import json
import sys
import argparse
import logging
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SA_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("migrate")


def migrate_client(gc, client: dict, dry_run: bool = False):
    """Add Total row to Payments tab for one client."""
    client_name = f"{client['number']}.{client['name']}"
    spreadsheet_id = client["spreadsheet_id"]
    payments_tab = client.get("payments_tab", "Payments")

    try:
        ss = gc.open_by_key(spreadsheet_id)
        ws = ss.worksheet(payments_tab)
    except Exception as e:
        logger.error(f"{client_name}: Cannot open sheet — {e}")
        return False

    # Read current data
    all_vals = ws.get_all_values()
    if not all_vals:
        logger.warning(f"{client_name}: Empty Payments tab")
        return False

    headers = all_vals[0]
    logger.info(f"{client_name}: {len(all_vals) - 1} data rows, headers: {headers}")

    # Check if Total row already exists (row 2)
    if len(all_vals) > 1 and all_vals[1][0] == "Total":
        logger.info(f"{client_name}: Total row already exists, skipping")
        return True

    if dry_run:
        logger.info(f"{client_name}: [DRY RUN] Would insert Total row at row 2")
        return True

    # Insert empty row at position 2 (shifts existing data down)
    ws.insert_row([], index=2)

    # Write Total row with formulas
    ws.update("A2:D2", [["Total", '=SUM(B3:B)', '=SUM(C3:C)', '=B2-C2']],
              value_input_option="USER_ENTERED")

    # Format Total row (pink bg, white bold text)
    sheet_id = ws._properties["sheetId"]
    from brand_config import PINK, WHITE, DEEP_PURPLE

    reqs = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1, "endRowIndex": 2,
                    "startColumnIndex": 0, "endColumnIndex": 6,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": PINK["red"], "green": PINK["green"], "blue": PINK["blue"]},
                        "textFormat": {
                            "bold": True,
                            "fontSize": 15,
                            "foregroundColor": {"red": WHITE["red"], "green": WHITE["green"], "blue": WHITE["blue"]},
                        },
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.horizontalAlignment",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": 1, "endIndex": 2,
                },
                "properties": {"pixelSize": 42},
                "fields": "pixelSize",
            }
        },
        {
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1, "endRowIndex": 2,
                    "startColumnIndex": 0, "endColumnIndex": 6,
                },
                "bottom": {
                    "style": "SOLID_MEDIUM",
                    "color": {"red": DEEP_PURPLE["red"], "green": DEEP_PURPLE["green"], "blue": DEEP_PURPLE["blue"]},
                },
            }
        },
    ]
    ss.batch_update({"requests": reqs})

    # Clear per-row Balance values (column D, rows 3+) — they're stale hardcoded values
    # Leave them for now as historical reference; Total row shows correct balance
    logger.info(f"{client_name}: ✅ Total row added successfully")
    return True


def main():
    parser = argparse.ArgumentParser(description="Migrate Payments tabs to add Total row")
    parser.add_argument("--client", metavar="NUM", help="Migrate only this client number")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    creds = Credentials.from_service_account_file(SA_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    with open("clients.json") as f:
        data = json.load(f)

    clients = data["clients"]
    if args.client:
        clients = [c for c in clients if c["number"] == args.client]
        if not clients:
            print(f"Client {args.client} not found")
            sys.exit(1)

    print(f"{'=' * 60}")
    print(f"MIGRATE PAYMENTS — {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Clients: {len(clients)}")
    print(f"{'=' * 60}")

    success = 0
    for client in clients:
        try:
            if migrate_client(gc, client, dry_run=args.dry_run):
                success += 1
        except Exception as e:
            logger.error(f"{client['number']}.{client['name']}: {e}")
        import time
        time.sleep(1)  # Rate limit

    print(f"\n{'=' * 60}")
    print(f"Done: {success}/{len(clients)} migrated")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
