"""
Google Sheets Writer — AllReports + Payments with brand formatting.
"""

import logging
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

LIGHT_PURPLE = {"red": 0.941, "green": 0.902, "blue": 1.0}
PINK_LIGHT   = {"red": 0.988, "green": 0.894, "blue": 0.949}
WHITE        = {"red": 1.0, "green": 1.0, "blue": 1.0}
PURPLE       = {"red": 0.424, "green": 0.247, "blue": 0.710}
PURPLE_TEXT  = {"red": 0.176, "green": 0.106, "blue": 0.412}
PINK         = {"red": 0.914, "green": 0.118, "blue": 0.549}
PINK_TEXT    = {"red": 0.914, "green": 0.118, "blue": 0.549}
BLACK        = {"red": 0.0, "green": 0.0, "blue": 0.0}


class GoogleSheetsWriter:
    def __init__(self, service_account_file: str):
        self.credentials = Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES
        )
        self.client = gspread.authorize(self.credentials)

    def _open(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        ss = self.client.open_by_key(spreadsheet_id)
        logger.info(f"Opened: {ss.title}")
        return ss

    def _get_ws(self, ss: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
        try:
            return ss.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title=name, rows=1000, cols=10)
            logger.info(f"Created worksheet: {name}")
            return ws

    def _get_sheet_id(self, ws: gspread.Worksheet) -> int:
        return ws._properties["sheetId"]

    def _make_format_request(self, sheet_id, row_idx, col_start, col_end,
                              bg_color=None, bold=False, font_size=None,
                              fg_color=None, h_align=None):
        fmt = {}
        fields = []
        if bg_color:
            fmt["backgroundColor"] = bg_color
            fields.append("userEnteredFormat.backgroundColor")
        if bold or font_size or fg_color:
            text_fmt = {}
            if bold:
                text_fmt["bold"] = True
            if font_size:
                text_fmt["fontSize"] = font_size
            if fg_color:
                text_fmt["foregroundColor"] = fg_color
            fmt["textFormat"] = text_fmt
            fields.append("userEnteredFormat.textFormat")
        if h_align:
            fmt["horizontalAlignment"] = h_align
            fields.append("userEnteredFormat.horizontalAlignment")
        return {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_idx,
                    "endRowIndex": row_idx + 1,
                    "startColumnIndex": col_start,
                    "endColumnIndex": col_end,
                },
                "cell": {"userEnteredFormat": fmt},
                "fields": ",".join(fields),
            }
        }

    def _make_row_height_request(self, sheet_id, row_idx, height):
        return {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_idx,
                    "endIndex": row_idx + 1,
                },
                "properties": {"pixelSize": height},
                "fields": "pixelSize",
            }
        }

    def clear_tab(self, spreadsheet_id: str, tab_name: str):
        """Clear all data from a tab (keeps the tab itself)."""
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        ws.clear()
        logger.info(f"Cleared tab '{tab_name}'")

    def init_payments_tab(self, spreadsheet_id: str, tab_name: str):
        """Re-create Payments tab structure after clearing."""
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)

        # Headers + Total row
        ws.append_row(["Date", "Deposit", "Paid", "Balance", "Comments"],
                      value_input_option="USER_ENTERED")
        ws.append_row(["", "Total", "", "=SUM(D2:D2)", ""],
                      value_input_option="USER_ENTERED")

        # Format header row
        fmt_requests = [
            self._make_format_request(
                sheet_id, 0, 0, 5,
                bg_color=PURPLE, bold=True, font_size=12,
                fg_color=WHITE, h_align="CENTER"),
            self._make_row_height_request(sheet_id, 0, 36),
            # Format Total row
            self._make_format_request(
                sheet_id, 1, 0, 5,
                bg_color=PINK, bold=True, font_size=15,
                fg_color=WHITE, h_align="CENTER"),
            self._make_row_height_request(sheet_id, 1, 42),
        ]
        ss.batch_update({"requests": fmt_requests})
        logger.info(f"Initialized Payments tab structure in '{tab_name}'")

    # ------------------------------------------------------------------
    # AllReports tab
    # ------------------------------------------------------------------

    def write_allreports(self, spreadsheet_id, tab_name, records, headers,
                          report_date="", report_label=""):
        if not records:
            return 0
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)
        all_vals = ws.get_all_values()
        start_row = len(all_vals)
        rows = []
        row_types = []
        if report_label:
            rh = ["" for _ in headers]
            rh[0] = report_date
            rh[1] = report_label
            rows.append(rh)
            row_types.append("report_header")
        for rec in records:
            row = [rec.get(h, "") for h in headers]
            order = rec.get("Order Number", "")
            if order == "Total":
                row_types.append("total")
            elif order in ("Storage", "Return Processing Charges", "Return Labels Charges"):
                row_types.append("summary")
            else:
                row_types.append("data")
            rows.append(row)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        num_cols = len(headers)
        fmt_requests = []
        for i, row_type in enumerate(row_types):
            abs_row = start_row + i
            if row_type == "report_header":
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 0, num_cols,
                    bg_color=LIGHT_PURPLE, bold=True, font_size=11,
                    fg_color=PURPLE_TEXT, h_align="LEFT"))
                fmt_requests.append(self._make_row_height_request(sheet_id, abs_row, 32))
            elif row_type == "total":
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 0, num_cols,
                    bg_color=PINK_LIGHT, bold=True, font_size=14,
                    fg_color=PINK_TEXT, h_align="CENTER"))
                fmt_requests.append(self._make_row_height_request(sheet_id, abs_row, 36))
            elif row_type == "summary":
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 0, num_cols,
                    bg_color=WHITE, font_size=11, fg_color=BLACK, h_align="CENTER"))
            elif row_type == "data":
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 0, num_cols,
                    bg_color=WHITE, font_size=11, fg_color=BLACK, h_align="CENTER"))
                fmt_requests.append(self._make_row_height_request(sheet_id, abs_row, 28))
        last_row = start_row + len(rows) - 1
        fmt_requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": last_row,
                    "endRowIndex": last_row + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols,
                },
                "bottom": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT},
            }
        })
        if fmt_requests:
            ss.batch_update({"requests": fmt_requests})
        logger.info(f"Wrote {len(rows)} formatted rows to '{tab_name}'")
        return len(rows)

    # ------------------------------------------------------------------
    # Payments tab
    # ------------------------------------------------------------------

    def write_payment(self, spreadsheet_id, tab_name, date, paid_amount):
        """Insert daily row into Payments. Always writes, even if $0."""
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)
        all_values = ws.get_all_values()
        num_cols = max(len(r) for r in all_values) if all_values else 6

        total_row_idx = None
        for i, row in enumerate(all_values):
            if len(row) > 1 and row[1].strip().lower() == "total":
                total_row_idx = i + 1
            if len(row) > 0 and row[0].strip() == date:
                # Date exists — update Paid in column C
                ws.update_cell(i + 1, 3, paid_amount)
                # Fix balance formula for this row (daily: deposit - paid)
                ws.update_cell(i + 1, 4, f"=B{i+1}-C{i+1}")
                logger.info(f"Updated Payments {date}: ${paid_amount}")
                return True

        if total_row_idx:
            new_row_idx = total_row_idx  # insert before Total
            # Balance formula: deposit - paid (daily, not running)
            balance_formula = f"=B{new_row_idx}-C{new_row_idx}"

            ws.insert_row([date, "", paid_amount, balance_formula],
                          index=new_row_idx, value_input_option="USER_ENTERED")
            new_total_idx = total_row_idx + 1

            # Fix formatting
            fmt_requests = []
            fmt_requests.append(self._make_format_request(
                sheet_id, new_row_idx - 1, 0, num_cols,
                bg_color=WHITE, bold=False, font_size=12,
                fg_color=BLACK, h_align="CENTER"))
            fmt_requests.append(self._make_row_height_request(
                sheet_id, new_row_idx - 1, 32))
            fmt_requests.append(self._make_format_request(
                sheet_id, new_total_idx - 1, 0, num_cols,
                bg_color=PINK, bold=True, font_size=15,
                fg_color=WHITE, h_align="CENTER"))
            fmt_requests.append(self._make_row_height_request(
                sheet_id, new_total_idx - 1, 42))
            fmt_requests.append({
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": new_total_idx - 1,
                        "endRowIndex": new_total_idx,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "bottom": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT},
                }
            })
            ss.batch_update({"requests": fmt_requests})

            # Update Total formula
            self._set_total_formula(ws, new_total_idx)
            logger.info(f"Inserted Payments {date}: ${paid_amount}")
        else:
            ws.append_row([date, "", paid_amount, ""], value_input_option="USER_ENTERED")
            logger.info(f"Appended Payments {date}: ${paid_amount}")
        return True

    def _set_total_formula(self, ws, total_row):
        data_end = total_row - 1
        ws.update_cell(total_row, 4, f"=SUM(D2:D{data_end})")
