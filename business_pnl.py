"""
Business P&L — Monthly profit & loss statement for the entire business.

Creates/updates a "Business P&L" worksheet in the P&L spreadsheet with:
  - Top section: Revenue & COGS (auto-populated from Client P&L Data tab)
  - Bottom section: Operating Expenses (manual input monthly)
  - Net Profit calculation

Columns: Row labels | Jan | Feb | Mar | ... | Dec | YTD
"""

import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from brand_config import DEEP_PURPLE, PURPLE, PINK, WHITE

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Month columns: B=Jan, C=Feb, ..., M=Dec, N=YTD
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUMBERS = {m: i + 1 for i, m in enumerate(MONTHS)}

TAB_NAME = "Business P&L"
DATA_TAB = "Data"

# Row structure (1-indexed from content start at row 3)
ROWS = [
    # --- REVENUE ---
    {"label": "REVENUE", "type": "section_header"},
    {"label": "Pick & Pack", "type": "auto", "data_col_idx": 4},   # E in Data tab (Pick & Pack Revenue)
    {"label": "Storage", "type": "auto", "data_col_idx": 5},       # F (Storage Revenue)
    {"label": "Return Processing", "type": "auto", "data_col_idx": 6},  # G
    {"label": "Return Labels", "type": "auto", "data_col_idx": 7},      # H
    {"label": "Packaging Revenue", "type": "auto", "data_col_idx": 8},  # I
    {"label": "Shipping Revenue", "type": "auto", "data_col_idx": 11},  # L
    {"label": "Total Revenue", "type": "sum_section", "sum_rows": [1, 2, 3, 4, 5, 6]},
    {"label": "", "type": "spacer"},
    # --- COST OF GOODS ---
    {"label": "COST OF GOODS SOLD", "type": "section_header"},
    {"label": "Shipping Cost (Carriers)", "type": "auto", "data_col_idx": 12},  # M
    {"label": "Packaging Cost (Materials)", "type": "auto", "data_col_idx": 9},  # J
    {"label": "Total COGS", "type": "sum_section", "sum_rows": [10, 11]},
    {"label": "", "type": "spacer"},
    # --- GROSS PROFIT ---
    {"label": "GROSS PROFIT", "type": "diff", "plus_row": 7, "minus_row": 12},
    {"label": "Gross Margin %", "type": "pct", "numerator_row": 14, "denominator_row": 7},
    {"label": "", "type": "spacer"},
    # --- OPERATING EXPENSES ---
    {"label": "OPERATING EXPENSES", "type": "section_header"},
    {"label": "Rent", "type": "manual"},
    {"label": "Utilities (Electric/Water)", "type": "manual"},
    {"label": "Internet & Phone", "type": "manual"},
    {"label": "Salaries & Wages", "type": "manual"},
    {"label": "Advertising & Marketing", "type": "manual"},
    {"label": "Software & Tools", "type": "manual"},
    {"label": "Insurance", "type": "manual"},
    {"label": "Other Expenses", "type": "manual"},
    {"label": "Total Operating Expenses", "type": "sum_section", "sum_rows": [18, 19, 20, 21, 22, 23, 24, 25]},
    {"label": "", "type": "spacer"},
    # --- NET PROFIT ---
    {"label": "NET PROFIT", "type": "diff", "plus_row": 14, "minus_row": 26},
    {"label": "Net Margin %", "type": "pct", "numerator_row": 28, "denominator_row": 7},
]

# Offset: row 1 = title, row 2 = month headers, data starts at row 3
HEADER_OFFSET = 2


def _col_letter(col_num: int) -> str:
    """Convert 1-based column number to letter (1=A, 2=B, ..., 27=AA)."""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _data_row(row_idx: int) -> int:
    """Convert 0-based ROWS index to actual sheet row number."""
    return HEADER_OFFSET + row_idx + 1  # 1-indexed


def setup_business_pnl(service_account_file: str, pnl_spreadsheet_id: str, year: int = None):
    """
    Create or recreate the Business P&L tab.
    Populates formulas for auto rows, leaves manual rows empty.
    """
    if not pnl_spreadsheet_id:
        logger.warning("No P&L spreadsheet ID, skipping Business P&L setup")
        return

    if year is None:
        year = datetime.now().year

    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(pnl_spreadsheet_id)

    # Get or create tab
    try:
        ws = ss.worksheet(TAB_NAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=TAB_NAME, rows=50, cols=15)

    sheet_id = ws._properties["sheetId"]

    # --- Write headers ---
    # Row 1: Title
    title = f"Fast Prep USA — P&L {year}"
    ws.update("A1", title, value_input_option="RAW")

    # Row 2: Column headers
    header_row = [""] + MONTHS + ["YTD"]
    ws.update("A2:O2", [header_row], value_input_option="RAW")

    # --- Write row labels and formulas ---
    all_values = []
    for i, row_def in enumerate(ROWS):
        sheet_row = _data_row(i)
        label = row_def["label"]
        row_type = row_def["type"]
        row_values = [label]

        for m_idx in range(12):  # Jan=0 ... Dec=11
            month_num = m_idx + 1
            col_letter = _col_letter(m_idx + 2)  # B=Jan, C=Feb, ...

            if row_type == "section_header" or row_type == "spacer":
                row_values.append("")

            elif row_type == "auto":
                # SUMPRODUCT from Data tab: sum column where month matches
                data_col = _col_letter(row_def["data_col_idx"] + 1)
                # Formula: sum values from Data tab where MONTH(date) = month_num and YEAR(date) = year
                formula = (
                    f'=SUMPRODUCT((MONTH({DATA_TAB}!$A$2:$A$9999)={month_num})'
                    f'*(YEAR({DATA_TAB}!$A$2:$A$9999)={year})'
                    f'*{DATA_TAB}!${data_col}$2:${data_col}$9999)'
                )
                row_values.append(formula)

            elif row_type == "sum_section":
                # Sum of specific rows in this column
                refs = [f"{col_letter}{_data_row(r)}" for r in row_def["sum_rows"]]
                formula = "=" + "+".join(refs)
                row_values.append(formula)

            elif row_type == "diff":
                plus = f"{col_letter}{_data_row(row_def['plus_row'])}"
                minus = f"{col_letter}{_data_row(row_def['minus_row'])}"
                formula = f"={plus}-{minus}"
                row_values.append(formula)

            elif row_type == "pct":
                num = f"{col_letter}{_data_row(row_def['numerator_row'])}"
                den = f"{col_letter}{_data_row(row_def['denominator_row'])}"
                formula = f'=IFERROR({num}/{den},0)'
                row_values.append(formula)

            elif row_type == "manual":
                row_values.append("")  # User fills in manually

        # YTD column (N) = SUM(B:M)
        ytd_col = _col_letter(14)  # column N
        if row_type in ("auto", "sum_section", "diff", "manual"):
            b_ref = f"B{sheet_row}"
            m_ref = f"M{sheet_row}"
            row_values.append(f"=SUM({b_ref}:{m_ref})")
        elif row_type == "pct":
            num = f"N{_data_row(row_def['numerator_row'])}"
            den = f"N{_data_row(row_def['denominator_row'])}"
            row_values.append(f'=IFERROR({num}/{den},0)')
        else:
            row_values.append("")

        all_values.append(row_values)

    # Write all data at once
    last_row = _data_row(len(ROWS) - 1)
    ws.update(f"A3:O{last_row}", all_values, value_input_option="USER_ENTERED")

    # --- Formatting ---
    reqs = []

    # Title row
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": 15},
            "cell": {"userEnteredFormat": {
                "backgroundColor": DEEP_PURPLE,
                "textFormat": {"foregroundColor": WHITE, "bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })
    reqs.append({
        "mergeCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": 15},
            "mergeType": "MERGE_ALL",
        }
    })

    # Month headers row
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": 15},
            "cell": {"userEnteredFormat": {
                "backgroundColor": PURPLE,
                "textFormat": {"foregroundColor": WHITE, "bold": True, "fontSize": 11},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })

    # Section headers (bold, purple text, light bg)
    for i, row_def in enumerate(ROWS):
        if row_def["type"] == "section_header":
            r = _data_row(i) - 1  # 0-indexed
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r + 1,
                              "startColumnIndex": 0, "endColumnIndex": 15},
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": {"red": 0.941, "green": 0.902, "blue": 1.0},
                        "textFormat": {"foregroundColor": DEEP_PURPLE, "bold": True, "fontSize": 11},
                    }},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            })

    # Total/profit rows (bold, pink highlight)
    highlight_labels = {"Total Revenue", "Total COGS", "GROSS PROFIT", "Total Operating Expenses", "NET PROFIT"}
    for i, row_def in enumerate(ROWS):
        if row_def["label"] in highlight_labels:
            r = _data_row(i) - 1
            bg = PINK if row_def["label"] in ("GROSS PROFIT", "NET PROFIT") else PURPLE
            fg = WHITE
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r + 1,
                              "startColumnIndex": 0, "endColumnIndex": 15},
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": bg,
                        "textFormat": {"foregroundColor": fg, "bold": True, "fontSize": 11},
                        "horizontalAlignment": "CENTER",
                    }},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                }
            })
            # Keep label left-aligned
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r + 1,
                              "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            })

    # Margin % rows — format as percentage
    for i, row_def in enumerate(ROWS):
        if row_def["type"] == "pct":
            r = _data_row(i) - 1
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r + 1,
                              "startColumnIndex": 1, "endColumnIndex": 15},
                    "cell": {"userEnteredFormat": {
                        "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
                    }},
                    "fields": "userEnteredFormat.numberFormat",
                }
            })

    # Currency format for all number cells
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": last_row,
                      "startColumnIndex": 1, "endColumnIndex": 15},
            "cell": {"userEnteredFormat": {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"},
            }},
            "fields": "userEnteredFormat.numberFormat",
        }
    })

    # Column widths
    col_widths = [220] + [100] * 12 + [120]  # A=labels, B-M=months, N=YTD
    for i, w in enumerate(col_widths):
        reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": w}, "fields": "pixelSize",
            }
        })

    # Row heights
    reqs.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 40}, "fields": "pixelSize",
        }
    })
    reqs.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 32}, "fields": "pixelSize",
        }
    })

    # Freeze first 2 rows + column A
    reqs.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": 2, "frozenColumnCount": 1},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    })

    ss.batch_update({"requests": reqs})
    logger.info(f"Business P&L tab created/updated for {year}")


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")
    pnl_id = "1lQz6_Vx4rx0j1WwBhgJRVF25IC6DT3AqCGBBxLi6C1Y"

    setup_business_pnl(sa_file, pnl_id, year=2026)
    print("Business P&L tab created!")
