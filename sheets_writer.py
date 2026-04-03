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

    def clear_and_init_allreports(self, spreadsheet_id: str, tab_name: str,
                                    client_number: str = "", client_name: str = ""):
        """Clear AllReports tab and re-create the branded header structure."""
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)

        # Unmerge all cells first, then clear
        ss.batch_update({"requests": [{
            "unmergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1000,
                          "startColumnIndex": 0, "endColumnIndex": 20}
            }
        }]})
        ws.clear()
        ss.batch_update({"requests": [{
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1000,
                          "startColumnIndex": 0, "endColumnIndex": 20},
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        }]})

        DEEP_PURPLE = {"red": 0.176, "green": 0.106, "blue": 0.412}
        GREEN = {"red": 0.0, "green": 0.769, "blue": 0.549}
        # v2.1: 8 columns — Date | Order Number | Tracking | Pick&Pack fee | Packaging Type | Packaging Cost | Shipping cost | Total
        COL_WIDTHS = [80, 160, 240, 110, 160, 110, 110, 110]

        reqs = []
        for i, w in enumerate(COL_WIDTHS):
            reqs.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w}, "fields": "pixelSize",
                }
            })

        reqs.append(self._make_row_height_request(sheet_id, 0, 30))
        reqs.append(self._make_row_height_request(sheet_id, 1, 30))
        reqs.append(self._make_format_request(
            sheet_id, 0, 0, 8, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=WHITE))
        reqs.append(self._make_format_request(
            sheet_id, 1, 0, 8, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=WHITE))
        reqs.append({
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 2,
                          "startColumnIndex": 0, "endColumnIndex": 2},
                "mergeType": "MERGE_ALL",
            }
        })
        reqs.append(self._make_row_height_request(sheet_id, 2, 8))
        reqs.append(self._make_row_height_request(sheet_id, 3, 32))
        reqs.append(self._make_format_request(
            sheet_id, 3, 0, 8, bg_color=PURPLE, bold=True, font_size=11, fg_color=WHITE, h_align="CENTER"))
        reqs.append(self._make_format_request(
            sheet_id, 1, 2, 3, bg_color=GREEN, bold=True, font_size=14, fg_color=WHITE, h_align="CENTER"))
        reqs.append(self._make_format_request(
            sheet_id, 1, 3, 4, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=PINK, h_align="CENTER"))
        reqs.append(self._make_format_request(
            sheet_id, 0, 4, 8, bg_color=DEEP_PURPLE, font_size=10, fg_color=WHITE, h_align="CENTER"))
        reqs.append({
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 4, "endColumnIndex": 8},
                "mergeType": "MERGE_ALL",
            }
        })
        reqs.append(self._make_format_request(
            sheet_id, 1, 4, 8, bg_color=DEEP_PURPLE, font_size=11, fg_color=PINK, h_align="CENTER"))

        ss.batch_update({"requests": reqs})

        ws.update("A1", [["FAST PREP USA"]], value_input_option="RAW")
        ws.update("C1", [["Shipping Status"]], value_input_option="RAW")
        ws.update("D1", [["Balance"]], value_input_option="RAW")
        ws.update("E1", [["Payments:  Zelle  ·  Wise  ·  Payoneer  ·  PayPal"]], value_input_option="RAW")
        ws.update("C2", [["ON"]], value_input_option="RAW")
        ws.update("D2", [["=INDEX(Payments!D:D,MATCH(\"Total\",Payments!A:A,0))"]], value_input_option="USER_ENTERED")
        ws.update("E2", [["payments@fastprepusa.com"]], value_input_option="RAW")

        # v2.1 column headers
        headers = ["Date", "Order Number", "Tracking number",
                   "Pick&Pack fee", "Packaging Type", "Packaging Cost",
                   "Shipping cost", "Total"]
        ws.update("A4:H4", [headers], value_input_option="RAW")

        logger.info(f"AllReports tab cleared and re-initialized for {client_number} {client_name}")

    def clear_and_init_payments(self, spreadsheet_id: str, tab_name: str):
        """Clear Payments tab and re-create the branded structure.

        Layout:
          Row 1: Headers
          Row 2: Total row (moves down as data is inserted before it)
        """
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)

        # Unmerge + clear + clear formatting
        try:
            ss.batch_update({"requests": [{
                "unmergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1000,
                              "startColumnIndex": 0, "endColumnIndex": 20}
                }
            }]})
        except Exception:
            pass
        ws.clear()
        ss.batch_update({"requests": [{
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1000,
                          "startColumnIndex": 0, "endColumnIndex": 20},
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        }]})

        COL_WIDTHS_PAY = [120, 120, 120, 140, 180, 180]

        reqs = []
        for i, w in enumerate(COL_WIDTHS_PAY):
            reqs.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w}, "fields": "pixelSize",
                }
            })

        # Row 1: headers
        reqs.append(self._make_row_height_request(sheet_id, 0, 36))
        reqs.append(self._make_format_request(
            sheet_id, 0, 0, 6, bg_color=PURPLE, bold=True, font_size=13, fg_color=WHITE, h_align="CENTER"))
        # Row 2: Total (will move down as data is inserted before it)
        reqs.append(self._make_row_height_request(sheet_id, 1, 42))
        reqs.append(self._make_format_request(
            sheet_id, 1, 0, 6, bg_color=PINK, bold=True, font_size=13, fg_color=WHITE, h_align="CENTER"))
        # Border above Total
        reqs.append({
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": 0, "endColumnIndex": 6},
                "top": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT},
            }
        })

        ss.batch_update({"requests": reqs})

        # Write content
        headers = ["Date", "Deposit", "Paid", "Balance", "Comments", "Customer info"]
        ws.update("A1:F1", [headers], value_input_option="RAW")
        # Total row at row 2 (will be pushed down as write_payment inserts before it)
        # Only Balance column gets a formula — Deposit and Paid stay empty
        ws.update("A2:F2", [["Total", "", "", 0, "", ""]],
                  value_input_option="USER_ENTERED")

        logger.info(f"Payments tab cleared and re-initialized")

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

        # --- Replace hardcoded totals with formulas ---
        # start_row is 0-based index of the first new row in the sheet.
        # Excel/Sheets row numbers are 1-based, so first new row = start_row + 1.
        first_data_row = None   # 1-based row of first order/summary line (after report_header)
        last_data_row = None    # 1-based row of last order/summary line (before Total)

        for i, row_type in enumerate(row_types):
            abs_row_1based = start_row + i + 1  # 1-based sheet row number
            if row_type == "data":
                # Order row: Total = Pick&Pack fee + Packaging Cost + Shipping cost
                # Columns: D=Pick&Pack, F=Packaging Cost, G=Shipping cost, H=Total
                rows[i][7] = f"=D{abs_row_1based}+F{abs_row_1based}+G{abs_row_1based}"
                if first_data_row is None:
                    first_data_row = abs_row_1based
                last_data_row = abs_row_1based
            elif row_type == "summary":
                # Storage / Return rows: Total = Shipping cost column
                rows[i][7] = f"=G{abs_row_1based}"
                if first_data_row is None:
                    first_data_row = abs_row_1based
                last_data_row = abs_row_1based
            elif row_type == "total":
                # Daily Total row: sum of all H cells for this day's data
                if first_data_row and last_data_row:
                    rows[i][7] = f"=SUM(H{first_data_row}:H{last_data_row})"
                # else: keep the numeric value as fallback

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

    def _find_total_row(self, ws, ss=None) -> int:
        """Find the row number (1-based) of the Total row in Payments tab.

        Searches columns A and B for 'Total'. If multiple Total rows exist,
        deletes the extras (keeps only the last one) and returns its position.
        Returns 0 if not found.
        """
        all_vals = ws.get_all_values()
        total_rows = []  # list of 1-based row numbers
        for i, row in enumerate(all_vals):
            cell_a = str(row[0]).strip().lower() if len(row) > 0 else ""
            cell_b = str(row[1]).strip().lower() if len(row) > 1 else ""
            if cell_a == "total" or cell_b == "total":
                total_rows.append(i + 1)  # 1-based

        if not total_rows:
            return 0

        if len(total_rows) > 1 and ss is not None:
            # Multiple Total rows found — delete all except the last one
            sheet_id = self._get_sheet_id(ws)
            # Delete from bottom to top to preserve row indices
            for tr in reversed(total_rows[:-1]):
                logger.warning(f"Payments: deleting extra Total row at {tr}")
                ss.batch_update({"requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": tr - 1,  # 0-based
                            "endIndex": tr,
                        }
                    }
                }]})
            # Recalculate last Total position after deletions
            deleted_before_last = sum(1 for tr in total_rows[:-1] if tr < total_rows[-1])
            return total_rows[-1] - deleted_before_last

        return total_rows[-1]

    def write_payment(self, spreadsheet_id, tab_name, date, paid_amount, comment=""):
        """
        Insert a payment row into the Payments tab, before the Total row.

        Layout:
          Row 1: Headers
          Row 2..N-1: Data (auto + manual)
          Row N: Total row (formulas, pink background)

        New rows are inserted just before Total. Total row formulas are
        updated to cover the full data range after each insert.
        Duplicate check: skips if date+comment already exists.
        """
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)

        # --- Dedup check: skip if date+comment already present ---
        all_vals = ws.get_all_values()
        for row in all_vals:
            row_date = str(row[0]).strip() if len(row) > 0 else ""
            row_comment = str(row[4]).strip() if len(row) > 4 else ""
            if row_date == str(date).strip() and row_comment == str(comment).strip():
                # Skip if same date and same comment (handles SOLMAR multi-line)
                if row_date.lower() != "total" and row_date.lower() != "date":
                    logger.info(f"Payments dedup: {date} ({comment}) already exists, skipping")
                    return True

        total_row = self._find_total_row(ws, ss=ss)
        if total_row == 0:
            # No Total row found — append at end and create Total after it
            all_values = ws.get_all_values()
            insert_row = len(all_values) + 1
        else:
            # Insert a blank row right before Total (shifts Total down by 1)
            ss.batch_update({"requests": [{
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": total_row - 1,  # 0-based
                        "endIndex": total_row,
                    },
                    "inheritFromBefore": False,
                }
            }]})
            insert_row = total_row  # data goes where Total was; Total is now +1

        # Build row: Date | Deposit (empty) | Paid | Balance (formula) | Comments | Customer info (empty)
        balance_formula = f"=B{insert_row}-C{insert_row}"
        row = [date, "", paid_amount, balance_formula, comment, ""]
        ws.update(f"A{insert_row}:F{insert_row}", [row], value_input_option="USER_ENTERED")

        # Format the new data row
        fmt_requests = [
            self._make_format_request(
                sheet_id, insert_row - 1, 0, 6,
                bg_color=WHITE, bold=False, font_size=12,
                fg_color=BLACK, h_align="CENTER"),
            self._make_row_height_request(sheet_id, insert_row - 1, 32),
        ]
        ss.batch_update({"requests": fmt_requests})

        # Update Total row formulas to cover all data rows (2 to new_total_row-1)
        new_total_row = insert_row + 1 if total_row else insert_row + 1
        if total_row == 0:
            # Create Total row — only Balance column gets a formula
            last_data_row = insert_row
            total_data = ["Total", "", "",
                          f"=SUM(D2:D{last_data_row})", "", ""]
            ws.update(f"A{new_total_row}:F{new_total_row}", [total_data],
                      value_input_option="USER_ENTERED")
            # Format Total row
            total_fmt = [
                self._make_format_request(
                    sheet_id, new_total_row - 1, 0, 6,
                    bg_color=PINK, bold=True, font_size=13, fg_color=WHITE, h_align="CENTER"),
                self._make_row_height_request(sheet_id, new_total_row - 1, 42),
                {"updateBorders": {
                    "range": {"sheetId": sheet_id,
                              "startRowIndex": new_total_row - 1, "endRowIndex": new_total_row,
                              "startColumnIndex": 0, "endColumnIndex": 6},
                    "top": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT},
                }},
            ]
            ss.batch_update({"requests": total_fmt})
        else:
            # Total row shifted to new_total_row — update Balance formula only
            last_data_row = new_total_row - 1
            total_formulas = ["Total", "", "",
                              f"=SUM(D2:D{last_data_row})", "", ""]
            ws.update(f"A{new_total_row}:F{new_total_row}", [total_formulas],
                      value_input_option="USER_ENTERED")

        comment_log = f" ({comment})" if comment else ""
        logger.info(f"Inserted Payments {date}: ${paid_amount}{comment_log} at row {insert_row}")
        return True
