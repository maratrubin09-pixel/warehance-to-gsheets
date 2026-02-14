"""
Google Sheets Writer

Writes to two tabs per client:
  - FBM tab: per-order breakdown + summary rows
  - Payments tab: daily total in column C (Paid)
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


class GoogleSheetsWriter:
    def __init__(self, service_account_file: str):
        self.credentials = Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES
        )
        self.client = gspread.authorize(self.credentials)

    def _open_spreadsheet(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        ss = self.client.open_by_key(spreadsheet_id)
        logger.info(f"Opened spreadsheet: {ss.title}")
        return ss

    def _get_or_create_worksheet(
        self, spreadsheet: gspread.Spreadsheet, name: str
    ) -> gspread.Worksheet:
        try:
            return spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=name, rows=1000, cols=26)
            logger.info(f"Created new worksheet: {name}")
            return ws

    # ------------------------------------------------------------------
    # FBM tab
    # ------------------------------------------------------------------

    def write_fbm(
        self,
        spreadsheet_id: str,
        tab_name: str,
        records: list[dict],
        headers: list[str],
        mode: str = "append",
    ) -> int:
        """Write order rows + summary to FBM tab."""
        if not records:
            logger.info("No FBM records to write")
            return 0

        ss = self._open_spreadsheet(spreadsheet_id)
        ws = self._get_or_create_worksheet(ss, tab_name)

        rows = []
        for rec in records:
            rows.append([rec.get(h, "") for h in headers])

        if mode == "replace":
            ws.clear()
            ws.append_row(headers, value_input_option="RAW")
        elif mode == "append":
            existing = ws.get_all_values()
            if not existing:
                ws.append_row(headers, value_input_option="RAW")

        ws.append_rows(rows, value_input_option="USER_ENTERED")
        logger.info(f"Wrote {len(rows)} rows to '{tab_name}'")
        return len(rows)

    # ------------------------------------------------------------------
    # Payments tab
    # ------------------------------------------------------------------

    def write_payment(
        self,
        spreadsheet_id: str,
        tab_name: str,
        date: str,
        paid_amount: float,
    ) -> bool:
        """
        Write a daily total to the Payments tab.

        Layout from screenshot:
          A = Date
          B = Deposit
          C = Paid
          D = Balance (formula, don't touch)

        Appends a new row with date in A and paid amount in C.
        """
        if paid_amount == 0:
            logger.info("Paid amount is 0, skipping Payments write")
            return False

        ss = self._open_spreadsheet(spreadsheet_id)
        ws = self._get_or_create_worksheet(ss, tab_name)

        # Find the right row: look for "Total" row or last non-empty row
        all_values = ws.get_all_values()

        # Find the row with "Total" in column B (index 1)
        total_row_idx = None
        last_data_row_idx = 0

        for i, row in enumerate(all_values):
            # Column B = index 1
            if len(row) > 1 and row[1].strip().lower() == "total":
                total_row_idx = i + 1  # gspread is 1-indexed
            if any(cell.strip() for cell in row):
                last_data_row_idx = i + 1

        if total_row_idx:
            # Insert BEFORE the Total row
            # First, check if this date already exists
            for i, row in enumerate(all_values):
                if len(row) > 0 and row[0].strip() == date:
                    # Update existing row — column C (index 3 in 1-indexed)
                    ws.update_cell(i + 1, 3, paid_amount)
                    logger.info(f"Updated Payments row for {date}: {paid_amount}")
                    return True

            # Insert new row before Total
            ws.insert_row(
                [date, "", paid_amount, ""],
                index=total_row_idx,
                value_input_option="USER_ENTERED",
            )
            logger.info(f"Inserted Payments row before Total for {date}: {paid_amount}")
        else:
            # No Total row found — just append
            # Check for duplicate date first
            for i, row in enumerate(all_values):
                if len(row) > 0 and row[0].strip() == date:
                    ws.update_cell(i + 1, 3, paid_amount)
                    logger.info(f"Updated Payments row for {date}: {paid_amount}")
                    return True

            ws.append_row(
                [date, "", paid_amount, ""],
                value_input_option="USER_ENTERED",
            )
            logger.info(f"Appended Payments row for {date}: {paid_amount}")

        return True
