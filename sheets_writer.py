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
        # Also clear formatting
        ss.batch_update({"requests": [{
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1000,
                          "startColumnIndex": 0, "endColumnIndex": 20},
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        }]})

        # Brand colors
        DEEP_PURPLE = {"red": 0.176, "green": 0.106, "blue": 0.412}
        GREEN = {"red": 0.0, "green": 0.769, "blue": 0.549}
        COL_WIDTHS = [80, 160, 200, 40, 110, 110, 110, 110, 110]

        reqs = []
        # Column widths
        for i, w in enumerate(COL_WIDTHS):
            reqs.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w}, "fields": "pixelSize",
                }
            })

        # Row 1-2: Header banner (deep purple)
        reqs.append(self._make_row_height_request(sheet_id, 0, 30))
        reqs.append(self._make_row_height_request(sheet_id, 1, 30))
        reqs.append(self._make_format_request(
            sheet_id, 0, 0, 9, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=WHITE))
        reqs.append(self._make_format_request(
            sheet_id, 1, 0, 9, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=WHITE))
        # Merge A1:B2 for FAST PREP USA
        reqs.append({
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 2,
                          "startColumnIndex": 0, "endColumnIndex": 2},
                "mergeType": "MERGE_ALL",
            }
        })
        # Row 3: spacer
        reqs.append(self._make_row_height_request(sheet_id, 2, 8))
        # Row 4: column headers (purple bg)
        reqs.append(self._make_row_height_request(sheet_id, 3, 32))
        reqs.append(self._make_format_request(
            sheet_id, 3, 0, 9, bg_color=PURPLE, bold=True, font_size=11, fg_color=WHITE, h_align="CENTER"))

        # Shipping Status green cell
        reqs.append(self._make_format_request(
            sheet_id, 1, 2, 3, bg_color=GREEN, bold=True, font_size=14, fg_color=WHITE, h_align="CENTER"))
        # Balance cell (columns D-E merged)
        reqs.append(self._make_format_request(
            sheet_id, 1, 3, 5, bg_color=DEEP_PURPLE, bold=True, font_size=14, fg_color=PINK, h_align="CENTER"))
        # Payment methods row 1 F-I
        reqs.append(self._make_format_request(
            sheet_id, 0, 5, 9, bg_color=DEEP_PURPLE, font_size=10, fg_color=WHITE, h_align="CENTER"))
        reqs.append({
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 5, "endColumnIndex": 9},
                "mergeType": "MERGE_ALL",
            }
        })
        # Payment email row 2 F-I
        reqs.append(self._make_format_request(
            sheet_id, 1, 5, 9, bg_color=DEEP_PURPLE, font_size=11, fg_color=PINK, h_align="CENTER"))

        ss.batch_update({"requests": reqs})

        # Write header content
        ws.update("A1", [["FAST PREP USA"]], value_input_option="RAW")
        ws.update("C1", [["Shipping Status"]], value_input_option="RAW")
        ws.update("D1", [["Balance"]], value_input_option="RAW")
        ws.update("F1", [["Payments:  Zelle  ·  Wise  ·  Payoneer  ·  PayPal"]], value_input_option="RAW")
        ws.update("C2", [["ON"]], value_input_option="RAW")
        # Balance formula: total deposits minus total paid from Payments tab
        ws.update("D2", [["=SUM(Payments!B:B)-SUM(Payments!C:C)"]], value_input_option="USER_ENTERED")
        ws.update("F2", [["payments@fastprepusa.com"]], value_input_option="RAW")

        # Column headers row 4
        headers = ["Date", "Order #", "Tracking", "",
                   "Storage/Ret.", "Shipping", "Pick&Pack Fee", "Pkg Cost", "Total"]
        ws.update("A4:I4", [headers], value_input_option="RAW")

        logger.info(f"AllReports tab cleared and re-initialized for {client_number} {client_name}")

    def clear_and_init_payments(self, spreadsheet_id: str, tab_name: str):
        """Clear Payments tab and re-create the branded structure."""
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
        # Row 2: Total
        reqs.append(self._make_row_height_request(sheet_id, 1, 42))
        reqs.append(self._make_format_request(
            sheet_id, 1, 0, 6, bg_color=PINK, bold=True, font_size=15, fg_color=WHITE, h_align="CENTER"))
        # Border under Total
        reqs.append({
            "updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                          "startColumnIndex": 0, "endColumnIndex": 6},
                "bottom": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT},
            }
        })

        ss.batch_update({"requests": reqs})

        # Write content
        headers = ["Date", "Deposit", "Paid", "Balance", "Comments", "Customer info"]
        ws.update("A1:F1", [headers], value_input_option="RAW")
        ws.update("B2", [["Total"]], value_input_option="RAW")
        ws.update("D2", [["=SUM(D2:D2)"]], value_input_option="USER_ENTERED")

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
        # Track first data row of this block for Total formula
        first_data_row_1idx = None  # 1-indexed row number

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

        # --- Replace hardcoded values with formulas ---
        # Column I (9) = Total, E=Storage, F=Shipping, G=Pick&Pack, H=PkgCost
        formula_updates = []
        for i, row_type in enumerate(row_types):
            sheet_row = start_row + i + 1  # 1-indexed

            if row_type == "report_header":
                first_data_row_1idx = sheet_row + 1  # next row is first data
            elif row_type == "data":
                if first_data_row_1idx is None:
                    first_data_row_1idx = sheet_row
                # Total = SUM(E,F,G,H) for this row
                formula_updates.append({
                    "range": f"I{sheet_row}",
                    "values": [[f"=SUM(E{sheet_row},F{sheet_row},G{sheet_row},H{sheet_row})"]]
                })
            elif row_type == "summary":
                if first_data_row_1idx is None:
                    first_data_row_1idx = sheet_row
                # Total = E (Storage/Returns value)
                formula_updates.append({
                    "range": f"I{sheet_row}",
                    "values": [[f"=E{sheet_row}"]]
                })
            elif row_type == "total":
                # Total = SUM of all rows in this day's block
                last_data_row = sheet_row - 1
                if first_data_row_1idx and last_data_row >= first_data_row_1idx:
                    formula_updates.append({
                        "range": f"I{sheet_row}",
                        "values": [[f"=SUM(I{first_data_row_1idx}:I{last_data_row})"]]
                    })

        if formula_updates:
            ws.batch_update(formula_updates, value_input_option="USER_ENTERED")
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

    def write_payment(self, spreadsheet_id, tab_name, date, paid_amount, comment=""):
        """Insert daily row into Payments. Always writes, even if $0."""
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)
        all_values = ws.get_all_values()
        num_cols = max(len(r) for r in all_values) if all_values else 6

        total_row_idx = None
        for i, row in enumerate(all_values):
            if len(row) > 0 and row[0].strip().lower() == "total":
                total_row_idx = i + 1
            if len(row) > 0 and row[0].strip() == date:
                # Date exists — update formulas for this row
                r = i + 1
                charges_formula = f'=SUMIFS(AllReports!I$5:I$50000,AllReports!A$5:A$50000,A{r},AllReports!B$5:B$50000,"Total")'
                balance_formula = f"=B{r}-C{r}"
                ws.update(f"C{r}:D{r}", [[charges_formula, balance_formula]],
                          value_input_option="USER_ENTERED")
                logger.info(f"Updated Payments {date}: formulas at row {r}")
                return True

        if total_row_idx:
            # Insert before the blank gap (not right before Total)
            # Find last data row (non-empty, non-Total) before Total
            insert_at = total_row_idx  # fallback: before Total
            for i in range(total_row_idx - 2, 1, -1):  # scan backwards (0-indexed)
                row_val = all_values[i] if i < len(all_values) else []
                cell_a = row_val[0].strip() if row_val else ""
                if cell_a and cell_a.lower() != "total":
                    insert_at = i + 2  # insert right after last data row (1-indexed)
                    break
            new_row_idx = insert_at
            charges_formula = f'=SUMIFS(AllReports!I$5:I$50000,AllReports!A$5:A$50000,A{new_row_idx},AllReports!B$5:B$50000,"Total")'
            balance_formula = f"=B{new_row_idx}-C{new_row_idx}"

            ws.insert_row([date, "", charges_formula, balance_formula, comment],
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
            ws.append_row([date, "", paid_amount, "", comment], value_input_option="USER_ENTERED")
            logger.info(f"Appended Payments {date}: ${paid_amount}")
        return True

    def write_payment_multi(self, spreadsheet_id, tab_name, rows_data):
        """Insert multiple payment rows for one day (used by 257 SOLMAR).

        rows_data: list of dicts with keys: date, paid, comment
        First row has the date, subsequent rows have empty date.
        All rows for the same day are inserted together.
        """
        if not rows_data:
            return
        ss = self._open(spreadsheet_id)
        ws = self._get_ws(ss, tab_name)
        sheet_id = self._get_sheet_id(ws)
        all_values = ws.get_all_values()
        num_cols = max(len(r) for r in all_values) if all_values else 6

        pay_date = rows_data[0]["date"]

        # Check if date already exists
        for i, row in enumerate(all_values):
            if len(row) > 0 and row[0].strip() == pay_date:
                logger.info(f"Payments {pay_date} already exists for SOLMAR, skipping")
                return

        # Find insert position (before blank gap / Total)
        total_row_idx = None
        for i, row in enumerate(all_values):
            if len(row) > 0 and row[0].strip().lower() == "total":
                total_row_idx = i + 1
                break

        if not total_row_idx:
            logger.warning("No Total row found in Payments")
            return

        # Find last data row before Total
        insert_at = total_row_idx
        for i in range(total_row_idx - 2, 1, -1):
            row_val = all_values[i] if i < len(all_values) else []
            cell_a = row_val[0].strip() if row_val else ""
            if cell_a and cell_a.lower() != "total":
                insert_at = i + 2
                break

        # Build rows to insert (hardcoded values, not SUMIFS — avoids date format mismatch)
        # Use placeholder for Balance — will fix formulas after all rows inserted
        new_rows = []
        for idx, rd in enumerate(rows_data):
            date_cell = pay_date if idx == 0 else ""
            charges = rd["paid"]
            comment = rd.get("comment", "")
            new_rows.append([date_cell, "", str(charges) if charges else "0", "", comment])

        # Insert rows (from bottom up to preserve indices)
        for row_data in reversed(new_rows):
            ws.insert_row(row_data, index=insert_at, value_input_option="USER_ENTERED")

        # Now fix Balance formulas — rows are at insert_at to insert_at+len-1
        balance_updates = []
        for idx in range(len(new_rows)):
            actual_row = insert_at + idx  # 1-indexed
            balance_updates.append({
                "range": f"D{actual_row}",
                "values": [[f"=B{actual_row}-C{actual_row}"]]
            })
        if balance_updates:
            ws.batch_update(balance_updates, value_input_option="USER_ENTERED")

        new_total_idx = total_row_idx + len(new_rows)

        # Format new rows
        YELLOW = {"red": 1.0, "green": 1.0, "blue": 0.0}
        GREEN = {"red": 0.0, "green": 1.0, "blue": 0.0}
        fmt_requests = []
        for idx in range(len(new_rows)):
            abs_row = insert_at - 1 + idx
            fmt_requests.append(self._make_format_request(
                sheet_id, abs_row, 0, num_cols,
                bg_color=WHITE, bold=False, font_size=10,
                fg_color=BLACK, h_align="LEFT"))
            fmt_requests.append(self._make_row_height_request(sheet_id, abs_row, 32))
            # Color comment cell
            comment = new_rows[idx][4]
            if comment == "Storage":
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 4, 5, bg_color=YELLOW))
            elif "Return" in comment:
                fmt_requests.append(self._make_format_request(
                    sheet_id, abs_row, 4, 5, bg_color=GREEN))

        # Re-format Total row
        fmt_requests.append(self._make_format_request(
            sheet_id, new_total_idx - 1, 0, num_cols,
            bg_color=PINK, bold=True, font_size=15,
            fg_color=WHITE, h_align="CENTER"))
        fmt_requests.append(self._make_row_height_request(sheet_id, new_total_idx - 1, 42))

        if fmt_requests:
            ss.batch_update({"requests": fmt_requests})

        self._set_total_formula(ws, new_total_idx)
        logger.info(f"Inserted {len(new_rows)} Payments rows for {pay_date} (SOLMAR)")

    def _set_total_formula(self, ws, total_row):
        # INDIRECT keeps "D3" fixed when rows are inserted above.
        # ROW()-1 dynamically finds the row before Total.
        ws.update_cell(total_row, 4, '=SUM(INDIRECT("D3:D"&ROW()-1))')
