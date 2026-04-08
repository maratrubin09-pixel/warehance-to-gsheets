#!/usr/bin/env python3
"""
Client Discovery — detects new clients in Warehance and auto-provisions Google Sheets.

Flow:
  1. GET /v1/clients — fetch all clients from Warehance API
  2. Compare with clients.json — find new ones
  3. For each new client:
     a. Assign next available client number
     b. GET /v1/billing-profiles — find or assign billing profile
     c. Create Google Sheet (AllReports + Payments) with branding
     d. Share with office@ and editor
     e. Add to clients.json
     f. Update Dashboard
     g. Send Telegram notification
"""

import json
import logging
import time
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from warehance_client import WarehanceClient
from brand_config import (
    DEEP_PURPLE, PURPLE, PINK, GREEN, WHITE,
    COL_WIDTHS_ALLREPORTS, COL_WIDTHS_PAYMENTS,
    OWNER_EMAIL, EDITOR_EMAIL,
    DEFAULT_BILLING_PROFILE_ID, SPECIAL_BILLING_PROFILES,
)
from telegram_notifier import TelegramNotifier

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# AllReports headers (9-col Sarmali format)
ALLREPORTS_HEADERS = [
    "Date", "Order Number", "", "Tracking number",
    "Storage/Returns", "Shipping cost", "Pick&Pack fee",
    "Package cost", "Total",
]

PAYMENTS_HEADERS = ["Date", "Deposit", "Paid", "Balance", "Comments", "Customer info"]

# Warehance IDs of deleted/inactive clients — skip during discovery
IGNORED_WAREHANCE_IDS = {
    "231185181607",  # 150 (deleted)
    "231185181613",  # 224 (deleted)
    "231185181629",  # 105 MiniBoso (deleted)
    "231185181636",  # 280 Anatolii Ufimtsev (deleted)
    "231185181700",  # 166 U-TECH LLC (deleted)
    "231185181785",  # 259 AMTM COSMETICS LLC (deleted)
}


def fetch_warehance_clients(wh: WarehanceClient) -> list[dict]:
    """Fetch all clients from Warehance API with pagination."""
    all_clients = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = wh._get("/clients", params={"limit": limit, "offset": offset})
            data = resp.get("data", {})
            clients = data.get("clients", [])
            if not clients:
                break
            all_clients.extend(clients)
            total = data.get("total_count", 0)
            if offset + limit >= total:
                break
            offset += limit
        except Exception as e:
            logger.error(f"Failed to fetch clients from Warehance: {e}")
            break

    logger.info(f"Fetched {len(all_clients)} clients from Warehance API")
    return all_clients


def fetch_billing_profiles(wh: WarehanceClient) -> list[dict]:
    """Fetch all billing profiles from Warehance API."""
    all_profiles = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = wh._get("/billing-profiles", params={"limit": limit, "offset": offset})
            data = resp.get("data", {})
            profiles = data.get("billing_profiles", [])
            if not profiles:
                break
            all_profiles.extend(profiles)
            total = data.get("total_count", 0)
            if offset + limit >= total:
                break
            offset += limit
        except Exception as e:
            logger.error(f"Failed to fetch billing profiles: {e}")
            break

    logger.info(f"Fetched {len(all_profiles)} billing profiles from Warehance API")
    return all_profiles


def _next_client_number(existing_clients: list[dict]) -> str:
    """Find the next available 3-digit client number."""
    existing_nums = set()
    for c in existing_clients:
        try:
            existing_nums.add(int(c["number"]))
        except (ValueError, KeyError):
            pass
    # Start from max+1
    next_num = max(existing_nums) + 1 if existing_nums else 1
    return str(next_num).zfill(3)


def _fmt_req(sheet_id, row, col_start, col_end, bg=None, fg=None, bold=False, size=None, halign=None):
    """Build a repeatCell formatting request."""
    fmt = {}
    fields = []
    if bg:
        fmt["backgroundColor"] = bg
        fields.append("userEnteredFormat.backgroundColor")
    tf = {}
    if bold:
        tf["bold"] = True
    if size:
        tf["fontSize"] = size
    if fg:
        tf["foregroundColor"] = fg
    if tf:
        fmt["textFormat"] = tf
        fields.append("userEnteredFormat.textFormat")
    if halign:
        fmt["horizontalAlignment"] = halign
        fields.append("userEnteredFormat.horizontalAlignment")
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row, "endRowIndex": row + 1,
                "startColumnIndex": col_start, "endColumnIndex": col_end,
            },
            "cell": {"userEnteredFormat": fmt},
            "fields": ",".join(fields),
        }
    }


def _col_width_req(sheet_id, col, width):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": col, "endIndex": col + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize",
        }
    }


def _row_height_req(sheet_id, row, height):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": row, "endIndex": row + 1},
            "properties": {"pixelSize": height}, "fields": "pixelSize",
        }
    }


def _merge_req(sheet_id, r1, r2, c1, c2):
    return {
        "mergeCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": r1, "endRowIndex": r2,
                      "startColumnIndex": c1, "endColumnIndex": c2},
            "mergeType": "MERGE_ALL",
        }
    }


def create_client_sheet(gc, number: str, name: str) -> str:
    """Create a branded Google Sheet for a new client. Returns spreadsheet_id."""
    title = f"{number}_{name.replace(' ', '_')}"
    ss = gc.create(title)
    logger.info(f"Created spreadsheet: {title}")

    # --- AllReports tab ---
    ws = ss.sheet1
    ws.update_title("AllReports")
    sheet_id = ws._properties["sheetId"]
    num_cols = len(ALLREPORTS_HEADERS)

    reqs = []
    for i, w in enumerate(COL_WIDTHS_ALLREPORTS):
        reqs.append(_col_width_req(sheet_id, i, w))

    # Row 1-2: Header banner
    reqs.append(_row_height_req(sheet_id, 0, 30))
    reqs.append(_row_height_req(sheet_id, 1, 30))
    reqs.append(_fmt_req(sheet_id, 0, 0, num_cols, bg=DEEP_PURPLE, fg=WHITE, bold=True, size=14))
    reqs.append(_fmt_req(sheet_id, 1, 0, num_cols, bg=DEEP_PURPLE, fg=WHITE, bold=True, size=14))
    reqs.append(_merge_req(sheet_id, 0, 2, 0, 2))  # A1:B2 FAST PREP USA

    # Row 3: spacer
    reqs.append(_row_height_req(sheet_id, 2, 8))

    # Row 4: column headers
    reqs.append(_row_height_req(sheet_id, 3, 32))
    reqs.append(_fmt_req(sheet_id, 3, 0, num_cols, bg=PURPLE, fg=WHITE, bold=True, size=11, halign="CENTER"))

    ss.batch_update({"requests": reqs})

    # Header content
    ws.update("A1", "FAST PREP USA", value_input_option="RAW")
    ws.update("C1", "Shipping Status", value_input_option="RAW")
    ws.update("D1", "Balance", value_input_option="RAW")
    ws.update("E1", "Payments:  Zelle  ·  Wise  ·  Payoneer  ·  PayPal", value_input_option="RAW")
    ws.update("C2", "ON", value_input_option="RAW")
    ws.update("E2", "payments@fastprepusa.com", value_input_option="RAW")

    # Extra formatting
    reqs2 = [
        _fmt_req(sheet_id, 1, 2, 3, bg=GREEN, fg=WHITE, bold=True, size=14, halign="CENTER"),
        _fmt_req(sheet_id, 1, 3, 4, bg=DEEP_PURPLE, fg=PINK, bold=True, size=14, halign="CENTER"),
        _fmt_req(sheet_id, 0, 4, num_cols, bg=DEEP_PURPLE, fg=WHITE, bold=False, size=10, halign="CENTER"),
        _merge_req(sheet_id, 0, 1, 4, num_cols),
        _fmt_req(sheet_id, 1, 4, num_cols, bg=DEEP_PURPLE, fg=PINK, bold=False, size=11, halign="CENTER"),
    ]
    ss.batch_update({"requests": reqs2})

    # Column headers row 4
    header_range = f"A4:{chr(64 + num_cols)}4"
    ws.update(header_range, [ALLREPORTS_HEADERS], value_input_option="RAW")

    # --- Payments tab ---
    pay_ws = ss.add_worksheet(title="Payments", rows=1000, cols=6)
    pay_sheet_id = pay_ws._properties["sheetId"]

    pay_reqs = []
    for i, w in enumerate(COL_WIDTHS_PAYMENTS):
        pay_reqs.append(_col_width_req(pay_sheet_id, i, w))

    pay_reqs.append(_row_height_req(pay_sheet_id, 0, 36))
    pay_reqs.append(_fmt_req(pay_sheet_id, 0, 0, 6, bg=PURPLE, fg=WHITE, bold=True, size=13, halign="CENTER"))
    pay_reqs.append(_row_height_req(pay_sheet_id, 1, 42))
    pay_reqs.append(_fmt_req(pay_sheet_id, 1, 0, 6, bg=PINK, fg=WHITE, bold=True, size=15, halign="CENTER"))
    pay_reqs.append({
        "updateBorders": {
            "range": {"sheetId": pay_sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": 6},
            "bottom": {"style": "SOLID_MEDIUM", "color": DEEP_PURPLE},
        }
    })
    ss.batch_update({"requests": pay_reqs})

    pay_ws.update("A1:F1", [PAYMENTS_HEADERS], value_input_option="RAW")
    # Total row: sums all deposits and charges, Balance = Deposits - Charges
    pay_ws.update("A2:D2", [["Total", '=SUM(B3:B)', '=SUM(C3:C)', '=B2-C2']], value_input_option="USER_ENTERED")

    # Share
    ss.share(OWNER_EMAIL, perm_type="user", role="owner", notify=False)
    ss.share(EDITOR_EMAIL, perm_type="user", role="writer", notify=False)
    logger.info(f"Shared {title} with {OWNER_EMAIL} + {EDITOR_EMAIL}")

    return ss.id


def update_dashboard(gc, dashboard_id: str, new_client: dict):
    """Add new client row to the Dashboard."""
    if not dashboard_id:
        logger.warning("No dashboard_spreadsheet_id, skipping dashboard update")
        return

    try:
        ss = gc.open_by_key(dashboard_id)
        ws = ss.worksheet("Clients")
        existing = ws.get_all_values()

        # Next row number
        next_row = len(existing) + 1
        row_num = next_row - 1  # sequence number

        sid = new_client["spreadsheet_id"]
        url = f"https://docs.google.com/spreadsheets/d/{sid}/edit"
        # Use INDIRECT to get last non-empty balance instead of hardcoded row
        balance_formula = f'=IFERROR(IMPORTRANGE("{sid}","Payments!D2"),"—")'

        row = [row_num, new_client["number"], new_client["name"], url, balance_formula]
        ws.update(f"A{next_row}:E{next_row}", [row], value_input_option="USER_ENTERED")

        # Format the new row
        sheet_id = ws._properties["sheetId"]
        reqs = [
            _fmt_req(sheet_id, next_row - 1, 0, 5, size=11, halign="CENTER"),
            _fmt_req(sheet_id, next_row - 1, 2, 3, halign="LEFT"),
            _fmt_req(sheet_id, next_row - 1, 3, 4, halign="LEFT"),
            _row_height_req(sheet_id, next_row - 1, 30),
            _fmt_req(sheet_id, next_row - 1, 4, 5, fg=PINK, bold=True, size=12, halign="CENTER"),
        ]
        ss.batch_update({"requests": reqs})
        logger.info(f"Dashboard updated with new client: {new_client['number']}.{new_client['name']}")
    except Exception as e:
        logger.error(f"Dashboard update failed: {e}")


def discover_and_provision(
    wh: WarehanceClient,
    service_account_file: str,
    clients_json_path: str = "clients.json",
    dashboard_id: str = "",
    tg: TelegramNotifier = None,
) -> list[dict]:
    """
    Main discovery function. Returns list of newly provisioned clients.

    Steps:
      1. Fetch clients from Warehance API
      2. Compare with clients.json
      3. Create sheets for new clients
      4. Update clients.json and Dashboard
    """
    # Load existing clients
    path = Path(clients_json_path)
    if not path.exists():
        logger.error(f"clients.json not found at {path}")
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    existing_clients = data.get("clients", [])
    existing_wh_ids = {c["warehance_id"] for c in existing_clients}

    # Fetch from Warehance
    wh_clients = fetch_warehance_clients(wh)
    if not wh_clients:
        logger.info("No clients found in Warehance or API error")
        return []

    # Find new clients (excluding ignored/deleted ones)
    new_wh_clients = [
        c for c in wh_clients
        if c["id"] not in existing_wh_ids
        and str(c["id"]) not in IGNORED_WAREHANCE_IDS
    ]
    if not new_wh_clients:
        logger.info("No new clients found in Warehance")
        return []

    logger.info(f"Found {len(new_wh_clients)} new client(s) in Warehance")

    # Auth for Google Sheets
    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    gc = gspread.authorize(creds)

    provisioned = []

    for wh_client in new_wh_clients:
        wh_id = wh_client["id"]
        wh_name = wh_client["name"]
        client_number = _next_client_number(existing_clients + provisioned)

        logger.info(f"Provisioning new client: {client_number}.{wh_name} (Warehance ID: {wh_id})")

        try:
            # Create Google Sheet
            spreadsheet_id = create_client_sheet(gc, client_number, wh_name)

            # Determine billing profile
            billing_profile_id = SPECIAL_BILLING_PROFILES.get(
                client_number, DEFAULT_BILLING_PROFILE_ID
            )

            # Build client config
            new_client = {
                "warehance_id": wh_id,
                "number": client_number,
                "name": wh_name,
                "spreadsheet_id": spreadsheet_id,
                "allreports_tab": "AllReports",
                "payments_tab": "Payments",
                "check_package_cost": True,
                "check_pick_fee": True,
                "billing_profile_id": billing_profile_id,
            }

            # Add to data
            data["clients"].append(new_client)
            existing_clients.append(new_client)
            provisioned.append(new_client)

            # Update Dashboard
            update_dashboard(gc, dashboard_id, new_client)

            # Telegram notification
            if tg:
                sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                tg.send(
                    f"<b>🆕 Новый клиент обнаружен</b>\n\n"
                    f"<b>{client_number}.{wh_name}</b>\n"
                    f"Warehance ID: <code>{wh_id}</code>\n"
                    f"Billing profile: <code>{billing_profile_id}</code>\n"
                    f"Таблица создана и настроена.\n\n"
                    f'🔗 <a href="{sheet_url}">Открыть таблицу</a>'
                )

            logger.info(f"Provisioned: {client_number}.{wh_name} → {spreadsheet_id}")
            time.sleep(2)  # Rate limit

        except Exception as e:
            logger.error(f"Failed to provision {wh_name}: {e}", exc_info=True)
            if tg:
                tg.send(f"<b>❌ Ошибка создания таблицы</b>\n\n{wh_name}: {e}")

    # Save updated clients.json
    if provisioned:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"clients.json updated with {len(provisioned)} new client(s)")

    return provisioned


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

    api_key = os.getenv("WAREHANCE_API_KEY")
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")

    wh = WarehanceClient(api_key=api_key)

    with open("clients.json") as f:
        cfg = json.load(f)

    tg = TelegramNotifier()
    result = discover_and_provision(
        wh=wh,
        service_account_file=sa_file,
        dashboard_id=cfg.get("dashboard_spreadsheet_id", ""),
        tg=tg,
    )
    print(f"\nProvisioned {len(result)} new client(s)")
    for c in result:
        print(f"  {c['number']}.{c['name']} → {c['spreadsheet_id']}")
