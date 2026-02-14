#!/usr/bin/env python3
"""
Creates all client spreadsheets + master dashboard.
Each spreadsheet gets:
  - AllReports tab with branded header
  - Payments tab with headers + Total row
Shares with office@fastprepusa.com (owner) and bwmodnick@gmail.com (editor)
"""

import json
import time
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SA_FILE = "config/service_account.json"
OWNER_EMAIL = "office@fastprepusa.com"
EDITOR_EMAIL = "bwmodnick@gmail.com"

# Brand colors (RGB 0-1)
DEEP_PURPLE = {"red": 0.176, "green": 0.106, "blue": 0.412}
PURPLE = {"red": 0.424, "green": 0.247, "blue": 0.710}
PINK = {"red": 0.914, "green": 0.118, "blue": 0.549}
GREEN = {"red": 0.0, "green": 0.769, "blue": 0.549}
LIGHT_PURPLE = {"red": 0.941, "green": 0.902, "blue": 1.0}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

# Column widths for AllReports
COL_WIDTHS_AR = [80, 160, 240, 110, 110, 110, 110, 110]
# Column widths for Payments
COL_WIDTHS_PAY = [120, 120, 120, 140, 180, 180]


def fmt_req(sheet_id, row, col_start, col_end, bg=None, fg=None, bold=False, size=None, halign=None):
    """Build a repeatCell formatting request."""
    fmt = {}
    fields = []
    if bg:
        fmt["backgroundColor"] = bg
        fields.append("userEnteredFormat.backgroundColor")
    tf = {}
    if bold: tf["bold"] = True
    if size: tf["fontSize"] = size
    if fg: tf["foregroundColor"] = fg
    if tf:
        fmt["textFormat"] = tf
        fields.append("userEnteredFormat.textFormat")
    if halign:
        fmt["horizontalAlignment"] = halign
        fields.append("userEnteredFormat.horizontalAlignment")
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": row, "endRowIndex": row+1,
                       "startColumnIndex": col_start, "endColumnIndex": col_end},
            "cell": {"userEnteredFormat": fmt},
            "fields": ",".join(fields),
        }
    }


def col_width_req(sheet_id, col, width):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                       "startIndex": col, "endIndex": col+1},
            "properties": {"pixelSize": width}, "fields": "pixelSize",
        }
    }


def row_height_req(sheet_id, row, height):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                       "startIndex": row, "endIndex": row+1},
            "properties": {"pixelSize": height}, "fields": "pixelSize",
        }
    }


def merge_req(sheet_id, r1, r2, c1, c2):
    return {
        "mergeCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": r1, "endRowIndex": r2,
                       "startColumnIndex": c1, "endColumnIndex": c2},
            "mergeType": "MERGE_ALL",
        }
    }


def setup_allreports(ss, sheet_id):
    """Set up AllReports tab with branded header."""
    reqs = []

    # Column widths
    for i, w in enumerate(COL_WIDTHS_AR):
        reqs.append(col_width_req(sheet_id, i, w))

    # Row 1-2: Header banner (deep purple bg)
    reqs.append(row_height_req(sheet_id, 0, 30))
    reqs.append(row_height_req(sheet_id, 1, 30))
    reqs.append(fmt_req(sheet_id, 0, 0, 8, bg=DEEP_PURPLE, fg=WHITE, bold=True, size=14))
    reqs.append(fmt_req(sheet_id, 1, 0, 8, bg=DEEP_PURPLE, fg=WHITE, bold=True, size=14))

    # Merge A1:B2 for FAST PREP USA
    reqs.append(merge_req(sheet_id, 0, 2, 0, 2))

    # Row 3: spacer
    reqs.append(row_height_req(sheet_id, 2, 8))

    # Row 4: column headers (purple bg)
    reqs.append(row_height_req(sheet_id, 3, 32))
    reqs.append(fmt_req(sheet_id, 3, 0, 8, bg=PURPLE, fg=WHITE, bold=True, size=11, halign="CENTER"))

    ss.batch_update({"requests": reqs})

    ws = ss.sheet1
    # Header content
    ws.update("A1", "FAST PREP USA", value_input_option="RAW")
    ws.update("C1", "Shipping Status", value_input_option="RAW")
    ws.update("D1", "Balance", value_input_option="RAW")
    ws.update("E1", "Payments:  Zelle  ·  Wise  ·  Payoneer  ·  PayPal", value_input_option="RAW")
    ws.update("C2", "ON", value_input_option="RAW")
    ws.update("E2", "payments@fastprepusa.com", value_input_option="RAW")

    # Shipping Status green
    reqs2 = [fmt_req(sheet_id, 1, 2, 3, bg=GREEN, fg=WHITE, bold=True, size=14, halign="CENTER")]
    # Balance formula will be set later by sync
    reqs2.append(fmt_req(sheet_id, 1, 3, 4, bg=DEEP_PURPLE, fg=PINK, bold=True, size=14, halign="CENTER"))
    # Payment methods
    reqs2.append(fmt_req(sheet_id, 0, 4, 8, bg=DEEP_PURPLE, fg=WHITE, bold=False, size=10, halign="CENTER"))
    reqs2.append(merge_req(sheet_id, 0, 1, 4, 8))
    reqs2.append(fmt_req(sheet_id, 1, 4, 8, bg=DEEP_PURPLE, fg=PINK, bold=False, size=11, halign="CENTER"))
    ss.batch_update({"requests": reqs2})

    # Column headers row 4
    headers = ["Date", "Order Number", "Tracking number", "Storage/Returns",
               "Shipping cost", "Pick&Pack fee", "Package cost", "Total"]
    ws.update("A4:H4", [headers], value_input_option="RAW")


def setup_payments(ss):
    """Set up Payments tab with headers + Total row."""
    ws = ss.add_worksheet(title="Payments", rows=1000, cols=6)
    pay_sheet_id = ws._properties["sheetId"]

    reqs = []
    for i, w in enumerate(COL_WIDTHS_PAY):
        reqs.append(col_width_req(pay_sheet_id, i, w))

    # Row 1: headers (purple bg)
    reqs.append(row_height_req(pay_sheet_id, 0, 36))
    reqs.append(fmt_req(pay_sheet_id, 0, 0, 6, bg=PURPLE, fg=WHITE, bold=True, size=13, halign="CENTER"))

    # Row 2: Total (will be pushed down by data)
    reqs.append(row_height_req(pay_sheet_id, 1, 42))
    reqs.append(fmt_req(pay_sheet_id, 1, 0, 6, bg={"red": 0.914, "green": 0.118, "blue": 0.549},
                         fg=WHITE, bold=True, size=15, halign="CENTER"))

    # Border under Total
    reqs.append({
        "updateBorders": {
            "range": {"sheetId": pay_sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                       "startColumnIndex": 0, "endColumnIndex": 6},
            "bottom": {"style": "SOLID_MEDIUM", "color": DEEP_PURPLE},
        }
    })
    ss.batch_update({"requests": reqs})

    headers = ["Date", "Deposit", "Paid", "Balance", "Comments", "Customer info"]
    ws.update("A1:F1", [headers], value_input_option="RAW")
    ws.update("B2", "Total", value_input_option="RAW")
    ws.update("D2", "=SUM(D2:D2)", value_input_option="USER_ENTERED")

    return ws


def create_client_sheet(gc, number, name):
    """Create a branded spreadsheet for one client."""
    title = f"{number}_{name.replace(' ', '_')}"
    ss = gc.create(title)
    print(f"  Created: {title}")

    # Rename Sheet1 to AllReports
    ws = ss.sheet1
    ws.update_title("AllReports")
    sheet_id = ws._properties["sheetId"]

    # Set up AllReports
    setup_allreports(ss, sheet_id)

    # Set up Payments
    setup_payments(ss)

    # Share
    ss.share(OWNER_EMAIL, perm_type="user", role="owner", notify=False)
    ss.share(EDITOR_EMAIL, perm_type="user", role="writer", notify=False)
    print(f"  Shared with {OWNER_EMAIL} + {EDITOR_EMAIL}")

    return ss.id


def create_dashboard(gc, clients):
    """Create master dashboard spreadsheet."""
    ss = gc.create("Fast Prep USA — Dashboard")
    ws = ss.sheet1
    ws.update_title("Clients")
    sheet_id = ws._properties["sheetId"]

    reqs = []
    # Column widths
    widths = [50, 60, 200, 400, 140]  # #, №, Client, Link, Balance
    for i, w in enumerate(widths):
        reqs.append(col_width_req(sheet_id, i, w))

    # Header row
    reqs.append(row_height_req(sheet_id, 0, 40))
    reqs.append(fmt_req(sheet_id, 0, 0, 5, bg=DEEP_PURPLE, fg=WHITE, bold=True, size=13, halign="CENTER"))

    ss.batch_update({"requests": reqs})

    # Headers
    ws.update("A1:E1", [["#", "№", "Client", "Spreadsheet", "Balance"]], value_input_option="RAW")

    # Client rows
    rows = []
    for i, c in enumerate(clients):
        sid = c.get("spreadsheet_id", "")
        url = f"https://docs.google.com/spreadsheets/d/{sid}/edit" if sid and "ВСТАВЬТЕ" not in sid else ""
        # IMPORTRANGE to get Total balance from Payments
        if url:
            # Get last cell in Balance column of Payments
            balance_formula = f'=IFERROR(IMPORTRANGE("{sid}","Payments!D1000"),"—")'
        else:
            balance_formula = "—"
        rows.append([i+1, c["number"], c["name"], url, balance_formula])

    ws.update(f"A2:E{1+len(rows)}", rows, value_input_option="USER_ENTERED")

    # Format data rows
    reqs2 = []
    for i in range(len(rows)):
        reqs2.append(fmt_req(sheet_id, i+1, 0, 5, size=11, halign="CENTER"))
        reqs2.append(fmt_req(sheet_id, i+1, 2, 3, halign="LEFT"))
        reqs2.append(fmt_req(sheet_id, i+1, 3, 4, halign="LEFT"))
        reqs2.append(row_height_req(sheet_id, i+1, 30))
    # Balance column — pink bold
    for i in range(len(rows)):
        reqs2.append(fmt_req(sheet_id, i+1, 4, 5, fg=PINK, bold=True, size=12, halign="CENTER"))
    ss.batch_update({"requests": reqs2})

    # Share
    ss.share(OWNER_EMAIL, perm_type="user", role="owner", notify=False)
    ss.share(EDITOR_EMAIL, perm_type="user", role="writer", notify=False)

    print(f"\n✅ Dashboard created: {ss.url}")
    return ss.id


def main():
    creds = Credentials.from_service_account_file(SA_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    with open("clients.json") as f:
        data = json.load(f)

    clients = data["clients"]
    created = 0

    print("=" * 60)
    print("CREATING CLIENT SPREADSHEETS")
    print("=" * 60)

    for c in clients:
        sid = c.get("spreadsheet_id", "")
        if sid and "ВСТАВЬТЕ" not in sid:
            print(f"⏭ {c['number']} {c['name']} — already has sheet")
            continue

        print(f"\n📄 {c['number']} {c['name']}")
        try:
            new_id = create_client_sheet(gc, c["number"], c["name"])
            c["spreadsheet_id"] = new_id
            created += 1
            print(f"  ✅ ID: {new_id}")
        except Exception as e:
            print(f"  ❌ Error: {e}")

        time.sleep(2)  # Rate limit

    # Save updated clients.json
    with open("clients.json", "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n✅ clients.json updated with {created} new spreadsheet IDs")

    # Create dashboard
    print("\n" + "=" * 60)
    print("CREATING MASTER DASHBOARD")
    print("=" * 60)

    dashboard_id = create_dashboard(gc, clients)
    data["dashboard_spreadsheet_id"] = dashboard_id
    with open("clients.json", "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"DONE! Created {created} sheets + 1 dashboard")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
