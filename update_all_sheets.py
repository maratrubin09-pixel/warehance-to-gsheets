#!/usr/bin/env python3
"""
Update all client sheets:
1. Rename "FBM Fee" → "Pick&Pack Fee" in header row
2. Replace hardcoded Total values with formulas in AllReports
3. Replace hardcoded Charges/Balance with formulas in Payments
"""

import json
import logging
import sys
import time

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("update_sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def process_allreports(ss, ws):
    """Rename header + replace hardcoded values with formulas in AllReports."""
    vals = ws.get_all_values()
    if len(vals) < 5:
        return 0, "empty"

    # --- Step 1: Rename FBM Fee in header (row 4) ---
    header_row = vals[3]  # 0-indexed
    renamed = False
    for col_idx, cell in enumerate(header_row):
        if cell.strip().lower() in ("fbm fee", "fbm fee"):
            col_letter = chr(65 + col_idx)  # A=65
            ws.update(f"{col_letter}4", [["Pick&Pack Fee"]], value_input_option="RAW")
            renamed = True
            break

    # --- Step 2: Replace Total column (I) values with formulas ---
    # Column I = index 8 (9th column, 0-indexed)
    # Find the Total column index dynamically
    total_col_idx = None
    for ci, h in enumerate(header_row):
        if h.strip().lower() == "total":
            total_col_idx = ci
            break

    if total_col_idx is None:
        return 0, "no Total column"

    total_col_letter = chr(65 + total_col_idx)  # Usually 'I'

    # Find Storage/Returns column (usually E, index 4)
    storage_col_idx = None
    for ci, h in enumerate(header_row):
        if "storage" in h.strip().lower():
            storage_col_idx = ci
            break

    # Column letters for SUM formula (E, F, G, H for 9-col layout)
    # These are the value columns between Storage and Total
    if storage_col_idx is not None and total_col_idx is not None:
        # Shipping = storage_col_idx + 1, Pick&Pack = +2, PkgCost = +3
        ship_col = chr(65 + storage_col_idx + 1)
        pick_col = chr(65 + storage_col_idx + 2)
        pkg_col = chr(65 + storage_col_idx + 3)
        stor_col = chr(65 + storage_col_idx)
    else:
        return 0, "cannot determine columns"

    formula_updates = []
    formulas_count = 0

    # Track day blocks
    first_data_row = None  # 1-indexed

    for i in range(4, len(vals)):  # Start from row 5 (index 4)
        row = vals[i]
        row_1idx = i + 1  # 1-indexed

        # Detect row type
        order_num = row[1].strip() if len(row) > 1 else ""

        if "Report —" in order_num or "Report —" in order_num:
            # Report header — next row starts a new block
            first_data_row = row_1idx + 1
            continue

        if order_num == "Total":
            # Day total — SUM of all rows in this block
            if first_data_row is not None:
                last_data_row = row_1idx - 1
                if last_data_row >= first_data_row:
                    formula = f"=SUM({total_col_letter}{first_data_row}:{total_col_letter}{last_data_row})"
                    formula_updates.append({
                        "range": f"{total_col_letter}{row_1idx}",
                        "values": [[formula]]
                    })
                    formulas_count += 1
            first_data_row = None
            continue

        if order_num in ("Storage", "Return Processing Charges", "Return Labels Charges"):
            # Summary row — Total = Storage/Returns value
            formula = f"={stor_col}{row_1idx}"
            formula_updates.append({
                "range": f"{total_col_letter}{row_1idx}",
                "values": [[formula]]
            })
            formulas_count += 1
            if first_data_row is None:
                first_data_row = row_1idx
            continue

        # Regular order row — Total = SUM(E,F,G,H)
        if order_num and order_num.startswith("#"):
            formula = f"=SUM({stor_col}{row_1idx},{ship_col}{row_1idx},{pick_col}{row_1idx},{pkg_col}{row_1idx})"
            formula_updates.append({
                "range": f"{total_col_letter}{row_1idx}",
                "values": [[formula]]
            })
            formulas_count += 1
            if first_data_row is None:
                first_data_row = row_1idx

    # Batch update formulas (max 60000 cells per batch)
    if formula_updates:
        # Split into chunks of 500 to avoid API limits
        for chunk_start in range(0, len(formula_updates), 500):
            chunk = formula_updates[chunk_start:chunk_start + 500]
            ws.batch_update(chunk, value_input_option="USER_ENTERED")

    return formulas_count, "ok"


def process_payments(ss, ws):
    """Replace hardcoded Charges/Balance with formulas in Payments."""
    vals = ws.get_all_values()
    if len(vals) < 3:
        return 0, "empty"

    # Find Total row
    total_row_idx = None
    data_rows = []  # list of 1-indexed row numbers

    for i in range(2, len(vals)):  # Start from row 3 (index 2)
        row = vals[i]
        row_1idx = i + 1
        cell_a = row[0].strip() if row else ""

        if cell_a.lower() == "total":
            total_row_idx = row_1idx
            continue

        # Data row — has a date
        if cell_a and cell_a.lower() != "total":
            data_rows.append(row_1idx)

    formula_updates = []
    formulas_count = 0

    for r in data_rows:
        # Column C (Charges) — SUMIFS formula
        charges = f'=SUMIFS(AllReports!I$5:I$50000,AllReports!A$5:A$50000,A{r},AllReports!B$5:B$50000,"Total")'
        # Column D (Balance) — Deposit minus Charges
        balance = f"=B{r}-C{r}"
        formula_updates.append({
            "range": f"C{r}:D{r}",
            "values": [[charges, balance]]
        })
        formulas_count += 2

    # Total row — column D with wide range
    if total_row_idx:
        formula_updates.append({
            "range": f"D{total_row_idx}",
            "values": [["=SUM(D3:D999)"]]
        })
        formulas_count += 1

    if formula_updates:
        for chunk_start in range(0, len(formula_updates), 500):
            chunk = formula_updates[chunk_start:chunk_start + 500]
            ws.batch_update(chunk, value_input_option="USER_ENTERED")

    return formulas_count, "ok"


def restructure_payments(ss, ws):
    """Add 3 blank rows between last data and Total, set wide-range Total formula."""
    vals = ws.get_all_values()
    if len(vals) < 3:
        return "empty"

    sheet_id = ws._properties["sheetId"]

    # Find Total row and last data row
    total_row_idx = None  # 1-indexed
    last_data_idx = None  # 1-indexed

    for i in range(2, len(vals)):
        row = vals[i]
        cell_a = row[0].strip() if row else ""
        row_1idx = i + 1

        if cell_a.lower() == "total":
            total_row_idx = row_1idx
        elif cell_a:
            last_data_idx = row_1idx

    if not total_row_idx:
        return "no Total row"
    if not last_data_idx:
        return "no data rows"

    # Count blank rows between last data and Total
    blank_count = total_row_idx - last_data_idx - 1
    needed = 3 - blank_count

    if needed > 0:
        # Insert blank rows after last data row
        insert_at = last_data_idx  # 0-indexed = last_data_idx (insert AFTER it)
        ss.batch_update({"requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": insert_at,
                    "endIndex": insert_at + needed,
                },
                "inheritFromBefore": True,
            }
        }]})
        total_row_idx += needed  # Total shifted down
    elif needed < 0:
        # Too many blank rows — delete excess
        delete_count = -needed
        delete_start = last_data_idx  # 0-indexed (row after last data)
        ss.batch_update({"requests": [{
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": delete_start,
                    "endIndex": delete_start + delete_count,
                }
            }
        }]})
        total_row_idx -= delete_count

    # Update Total formula to wide range
    ws.update(f"D{total_row_idx}", [["=SUM(D3:D999)"]], value_input_option="USER_ENTERED")

    return f"ok (gap={blank_count}→3, Total@row{total_row_idx})"


def main():
    creds = Credentials.from_service_account_file("config/service_account.json", scopes=SCOPES)
    gc = gspread.authorize(creds)

    with open("clients.json") as f:
        clients = json.load(f)["clients"]

    results = []

    for client in clients:
        num = client["number"]
        name = client["name"]
        sid = client["spreadsheet_id"]

        try:
            ss = gc.open_by_key(sid)
            logger.info(f"--- {num} {name} ---")

            # AllReports
            try:
                ws_ar = ss.worksheet("AllReports")
                ar_vals = ws_ar.get_all_values()
                ar_rows = len(ar_vals)
                ar_formulas, ar_status = process_allreports(ss, ws_ar)
            except gspread.WorksheetNotFound:
                ar_rows = 0
                ar_formulas = 0
                ar_status = "no tab"

            # Payments
            try:
                ws_pay = ss.worksheet("Payments")
                pay_vals = ws_pay.get_all_values()
                pay_rows = len(pay_vals)
                pay_formulas, pay_status = process_payments(ss, ws_pay)
            except gspread.WorksheetNotFound:
                pay_rows = 0
                pay_formulas = 0
                pay_status = "no tab"

            total_formulas = ar_formulas + pay_formulas
            status = f"AR:{ar_status} PAY:{pay_status}"
            results.append((num, name, ar_rows, total_formulas, pay_rows, status))
            logger.info(f"  AllReports: {ar_rows} rows, {ar_formulas} formulas | Payments: {pay_rows} rows, {pay_formulas} formulas")

        except Exception as e:
            results.append((num, name, 0, 0, 0, f"ERROR: {e}"))
            logger.error(f"  ERROR: {e}")

        time.sleep(2)

    # Print summary table
    print("\n" + "=" * 90)
    print(f"{'Client':<8} {'Name':<30} {'AR Rows':<10} {'Formulas':<12} {'Pay Rows':<10} {'Status'}")
    print("-" * 90)
    for num, name, ar, formulas, pay, status in results:
        print(f"{num:<8} {name:<30} {ar:<10} {formulas:<12} {pay:<10} {status}")
    print("=" * 90)


if __name__ == "__main__":
    main()
