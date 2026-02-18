"""Write daily P&L data to the P&L Dashboard spreadsheet."""
import json
import logging
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# Column headers for the Data tab
PNL_HEADERS = [
    "Date", "Client #", "Client Name", "Orders",
    "Storage", "Return Processing", "Pick & Pack", "Packaging Revenue",
    "Shipping Revenue", "Return Labels",
    "Shipping Cost", "Packaging Cost",
    "Total Revenue", "Total Cost", "Gross Profit", "Margin %",
]

# Load packaging costs mapping (box name -> cost)
_pkg_costs_path = Path(__file__).parent / "packaging_costs.json"
_PACKAGING_COSTS: dict[str, float] = {}
if _pkg_costs_path.exists():
    with open(_pkg_costs_path, "r", encoding="utf-8") as f:
        _PACKAGING_COSTS = json.load(f)
    logger.debug(f"Loaded {len(_PACKAGING_COSTS)} packaging cost entries")


def _calc_costs_from_shipments(shipments: list[dict]) -> dict:
    """
    Calculate real shipping cost and packaging cost from Shipments API data.

    shipments: list of dicts with keys:
      - order_number: str
      - shipment_cost: float  (carrier cost)
      - parcels: list of {box: str, tracking_number: str}

    Returns dict with total_shipping_cost and total_packaging_cost.
    """
    total_shipping_cost = 0.0
    total_packaging_cost = 0.0
    unknown_boxes = set()

    for s in shipments:
        total_shipping_cost += s.get("shipment_cost", 0)
        for parcel in s.get("parcels", []):
            box_name = parcel.get("box", "")
            if box_name in _PACKAGING_COSTS:
                total_packaging_cost += _PACKAGING_COSTS[box_name]
            elif box_name:
                unknown_boxes.add(box_name)

    if unknown_boxes:
        logger.warning(f"Unknown box types (no cost mapping): {unknown_boxes}")

    return {
        "total_shipping_cost": round(total_shipping_cost, 2),
        "total_packaging_cost": round(total_packaging_cost, 2),
    }


def write_pnl_row(
    service_account_file: str,
    client_number: str,
    client_name: str,
    date_str: str,
    transform_result: dict,
    shipments: list[dict] | None = None,
    pnl_spreadsheet_id: str = "",
):
    """
    Write one row per client per day to P&L Data tab.

    transform_result: dict from transform_bill_details()
    shipments: list from WarehanceClient.get_shipments()
    pnl_spreadsheet_id: ID of the P&L Dashboard spreadsheet
    """
    if not pnl_spreadsheet_id:
        logger.warning("No P&L spreadsheet ID configured, skipping P&L write")
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    gc = gspread.authorize(creds)

    ss = gc.open_by_key(pnl_spreadsheet_id)

    # Get or create Data tab
    try:
        ws = ss.worksheet("Data")
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title="Data", rows=1000, cols=len(PNL_HEADERS))
        ws.append_row(PNL_HEADERS, value_input_option="USER_ENTERED")
        _format_headers(ss, ws)

    # Aggregate revenue from report_rows
    report_rows = transform_result["report_rows"]

    storage = 0.0
    return_processing = 0.0
    return_labels = 0.0
    pick_pack = 0.0
    packaging_rev = 0.0
    shipping_rev = 0.0
    orders = 0

    for r in report_rows:
        onum = r.get("Order Number", "")
        if onum == "Storage":
            storage = float(r["Total"])
        elif onum == "Return Processing Charges":
            return_processing = float(r["Total"])
        elif onum == "Return Labels Charges":
            return_labels = float(r["Total"])
        elif onum == "Total":
            continue
        else:
            orders += 1
            s = r.get("Shipping cost", 0) or 0
            p = r.get("Pick&Pack fee", 0) or 0
            pkg = r.get("Package cost", 0) or 0
            shipping_rev += float(s)
            pick_pack += float(p)
            packaging_rev += float(pkg)

    # Calculate costs from shipments data
    if shipments:
        costs = _calc_costs_from_shipments(shipments)
        shipping_cost = costs["total_shipping_cost"]
        packaging_cost = costs["total_packaging_cost"]
    else:
        shipping_cost = 0.0
        packaging_cost = 0.0
        logger.info("No shipments data provided, costs set to $0")

    total_revenue = storage + return_processing + pick_pack + packaging_rev + shipping_rev + return_labels
    total_cost = shipping_cost + packaging_cost
    gross_profit = total_revenue - total_cost
    margin = round(gross_profit / total_revenue * 100, 1) if total_revenue > 0 else 0

    row = [
        date_str,
        "'" + str(client_number),
        client_name,
        orders,
        round(storage, 2),
        round(return_processing, 2),
        round(pick_pack, 2),
        round(packaging_rev, 2),
        round(shipping_rev, 2),
        round(return_labels, 2),
        round(shipping_cost, 2),
        round(packaging_cost, 2),
        round(total_revenue, 2),
        round(total_cost, 2),
        round(gross_profit, 2),
        margin,
    ]

    # Duplicate protection: check if row for this date+client already exists
    existing = ws.get_all_values()
    for i, existing_row in enumerate(existing):
        if i == 0:
            continue  # skip headers
        if len(existing_row) >= 3 and existing_row[0] == date_str and existing_row[1] == str(client_number):
            # Update existing row instead of appending
            cell_range = f"A{i + 1}:P{i + 1}"
            ws.update(cell_range, [row], value_input_option="USER_ENTERED")
            logger.info(f"P&L row updated (dedup) for {client_name} on {date_str}")
            return row

    # Ensure headers exist if tab was empty
    if not existing or existing[0] != PNL_HEADERS:
        if not existing:
            ws.append_row(PNL_HEADERS, value_input_option="USER_ENTERED")
            _format_headers(ss, ws)

    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info(f"P&L row appended for {client_name} on {date_str}")
    return row


def _format_headers(ss, ws):
    """Apply formatting to the header row of the Data tab."""
    sheet_id = ws.id
    requests_list = [
        # Freeze header row
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        # Header row formatting: dark purple bg, white text, bold, centered
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.29, "green": 0.0, "blue": 0.51},
                        "textFormat": {
                            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                            "bold": True,
                            "fontSize": 10,
                        },
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)",
            }
        },
        # Header row height
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        },
        # Column widths
        *_col_width_requests(sheet_id),
    ]
    ss.batch_update({"requests": requests_list})


def _col_width_requests(sheet_id: int) -> list[dict]:
    """Generate column width update requests."""
    widths = [
        100,  # A: Date
        70,   # B: Client #
        160,  # C: Client Name
        60,   # D: Orders
        90,   # E: Storage
        120,  # F: Return Processing
        90,   # G: Pick & Pack
        120,  # H: Packaging Revenue
        120,  # I: Shipping Revenue
        110,  # J: Return Labels
        100,  # K: Shipping Cost
        110,  # L: Packaging Cost
        110,  # M: Total Revenue
        90,   # N: Total Cost
        100,  # O: Gross Profit
        80,   # P: Margin %
    ]
    reqs = []
    for i, w in enumerate(widths):
        reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": w},
                "fields": "pixelSize",
            }
        })
    return reqs


def format_pnl_tab(service_account_file: str, pnl_spreadsheet_id: str):
    """One-time formatting setup for the P&L Data tab."""
    if not pnl_spreadsheet_id:
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(pnl_spreadsheet_id)

    try:
        ws = ss.worksheet("Data")
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title="Data", rows=1000, cols=len(PNL_HEADERS))
        ws.append_row(PNL_HEADERS, value_input_option="USER_ENTERED")

    _format_headers(ss, ws)
    logger.info("P&L Data tab formatted")


if __name__ == "__main__":
    print("Use write_pnl_row() from agent.py, or format_pnl_tab() for one-time setup")
