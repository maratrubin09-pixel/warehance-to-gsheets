#!/usr/bin/env python3
"""
Full backfill ALL clients from first active day to 04/06/2026.
- Clears AllReports + Payments
- Rebuilds day-by-day from Warehance API
- Writes formulas (SUM for order/day totals, SUMIFS for Payments)
- Preserves existing deposits in Payments
- Adds 3 blank rows before Total in Payments
"""

import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, date
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

# ── Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backfill_all")

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

API_KEY = os.getenv("WAREHANCE_API_KEY", "")
PACIFIC = ZoneInfo("America/Los_Angeles")
END_DATE = date(2026, 4, 6)
HEADERS_API = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Formatting colors
LIGHT_PURPLE = {"red": 0.941, "green": 0.902, "blue": 1.0}
PINK_LIGHT = {"red": 0.988, "green": 0.894, "blue": 0.949}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
PURPLE_TEXT = {"red": 0.176, "green": 0.106, "blue": 0.412}
PINK_TEXT = {"red": 0.914, "green": 0.118, "blue": 0.549}
PINK = {"red": 0.914, "green": 0.118, "blue": 0.549}
BLACK = {"red": 0.0, "green": 0.0, "blue": 0.0}

sys.path.insert(0, os.path.dirname(__file__))
from transformer import transform_bill_details, ALLREPORTS_HEADERS


# ── Helpers ──────────────────────────────────────────────────────────

def fmt_tz(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + dt.strftime("%z")[:3] + ":" + dt.strftime("%z")[3:]


def create_bill_for_day(client_id, billing_profile_id, target_day):
    day_start = datetime(target_day.year, target_day.month, target_day.day, 0, 0, 0, tzinfo=PACIFIC)
    day_end = datetime(target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=PACIFIC)
    try:
        r = requests.post("https://api.warehance.com/v1/bills", headers=HEADERS_API, json={
            "client_id": client_id, "billing_profile_id": billing_profile_id,
            "start_date": fmt_tz(day_start), "end_date": fmt_tz(day_end),
        }, timeout=30)
        r.raise_for_status()
        bill_id = r.json()["data"]["id"]
    except Exception as e:
        logger.error(f"  Bill creation failed {target_day}: {e}")
        return []
    for _ in range(20):
        try:
            r2 = requests.get(f"https://api.warehance.com/v1/bills/{bill_id}",
                              headers={"X-API-Key": API_KEY}, timeout=30)
            d = r2.json()["data"]
            url = d.get("line_item_details_csv_url", "")
            if url and d.get("generation_status") == "Completed":
                cr = requests.get(url, timeout=60)
                cr.raise_for_status()
                return list(csv.DictReader(io.StringIO(cr.text)))
        except Exception:
            pass
        time.sleep(2)
    return []


def find_first_active_day(client_id, billing_profile_id):
    for month_start in [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1), date(2026, 4, 1)]:
        month_end = date(2026, 4, 6) if month_start.month == 4 else \
            date(2026, month_start.month + 1, 1) - timedelta(days=1)
        ds = datetime(month_start.year, month_start.month, month_start.day, 0, 0, 0, tzinfo=PACIFIC)
        de = datetime(month_end.year, month_end.month, month_end.day, 23, 59, 59, tzinfo=PACIFIC)
        try:
            r = requests.post("https://api.warehance.com/v1/bills", headers=HEADERS_API, json={
                "client_id": client_id, "billing_profile_id": billing_profile_id,
                "start_date": fmt_tz(ds), "end_date": fmt_tz(de),
            }, timeout=30)
            r.raise_for_status()
            bill_id = r.json()["data"]["id"]
        except Exception:
            continue
        for _ in range(20):
            try:
                r2 = requests.get(f"https://api.warehance.com/v1/bills/{bill_id}",
                                  headers={"X-API-Key": API_KEY}, timeout=30)
                b = r2.json()["data"]
                url = b.get("line_item_details_csv_url", "")
                if url and b.get("generation_status") == "Completed":
                    cr = requests.get(url, timeout=60)
                    rows = list(csv.DictReader(io.StringIO(cr.text)))
                    if rows:
                        # Start 1 day before month to catch timezone edge
                        first = month_start - timedelta(days=1)
                        return max(first, date(2026, 1, 1))
                    break
            except Exception:
                pass
            time.sleep(2)
        time.sleep(1)
    return None


def mk_fmt(sheet_id, row, c0, c1, bg=None, bold=False, sz=None, fg=None, ha=None):
    fmt, fields = {}, []
    if bg:
        fmt["backgroundColor"] = bg; fields.append("userEnteredFormat.backgroundColor")
    if bold or sz or fg:
        tf = {}
        if bold: tf["bold"] = True
        if sz: tf["fontSize"] = sz
        if fg: tf["foregroundColor"] = fg
        fmt["textFormat"] = tf; fields.append("userEnteredFormat.textFormat")
    if ha:
        fmt["horizontalAlignment"] = ha; fields.append("userEnteredFormat.horizontalAlignment")
    return {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": row,
            "endRowIndex": row+1, "startColumnIndex": c0, "endColumnIndex": c1},
            "cell": {"userEnteredFormat": fmt}, "fields": ",".join(fields)}}


def mk_height(sheet_id, row, h):
    return {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS",
            "startIndex": row, "endIndex": row+1}, "properties": {"pixelSize": h}, "fields": "pixelSize"}}


def retry_api(func, retries=3):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            if "429" in str(e) and i < retries - 1:
                logger.warning(f"  Rate limit, waiting {60*(i+1)}s...")
                time.sleep(60 * (i + 1))
            else:
                raise


def parse_deposit_date(date_str):
    """Parse deposit date in various formats to date object."""
    for fmt in ["%m.%d.%Y", "%m/%d/%y", "%m/%d/%Y", "%m.%d.%y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


# ── Main backfill ────────────────────────────────────────────────────

def backfill_client(client, gc):
    num = client["number"]
    name = client["name"]
    sid = client["spreadsheet_id"]
    wh_id = client["warehance_id"]
    bp_id = client["billing_profile_id"]

    logger.info(f"{'='*60}")
    logger.info(f"CLIENT {num} {name}")
    logger.info(f"{'='*60}")

    # 1. First active day
    logger.info("  Finding first active day...")
    first_day = find_first_active_day(wh_id, bp_id)
    if not first_day:
        logger.warning(f"  No data found")
        return {"client": f"{num} {name}", "status": "no data", "days_data": 0,
                "orders": 0, "ar_rows": 0, "first_day": "N/A", "deposits": 0}
    logger.info(f"  First: {first_day}")

    ss = retry_api(lambda: gc.open_by_key(sid))
    time.sleep(1)

    # 2. Save deposits
    ws_pay = retry_api(lambda: ss.worksheet("Payments"))
    pay_vals = ws_pay.get_all_values()
    deposits = {}
    for row in pay_vals[2:]:
        if len(row) > 1 and row[1].strip() and row[0].strip().lower() != "total":
            pd = parse_deposit_date(row[0])
            if pd:
                deposits[pd] = row[1].strip()
    if deposits:
        logger.info(f"  Saved {len(deposits)} deposits")

    # 3. Clear AllReports
    ws_ar = retry_api(lambda: ss.worksheet("AllReports"))
    ar_sid = ws_ar._properties["sheetId"]
    ar_vals = ws_ar.get_all_values()
    if len(ar_vals) > 5:
        retry_api(lambda: ss.batch_update({"requests": [{"deleteDimension": {"range": {
            "sheetId": ar_sid, "dimension": "ROWS", "startIndex": 5, "endIndex": len(ar_vals)}}}]}))
    ws_ar.batch_clear(["A5:Z5"])
    logger.info(f"  Cleared AllReports ({len(ar_vals)} rows)")
    time.sleep(1)

    # 4. Clear Payments
    pay_sid = ws_pay._properties["sheetId"]
    if len(pay_vals) > 3:
        retry_api(lambda: ss.batch_update({"requests": [{"deleteDimension": {"range": {
            "sheetId": pay_sid, "dimension": "ROWS", "startIndex": 3, "endIndex": len(pay_vals)}}}]}))
    ws_pay.batch_clear(["A3:Z3"])
    logger.info(f"  Cleared Payments ({len(pay_vals)} rows)")
    time.sleep(1)

    # 5. Backfill AllReports day by day
    ncols = len(ALLREPORTS_HEADERS)
    days_data = 0
    total_orders = 0
    dates_with_data = []

    current = first_day
    while current <= END_DATE:
        raw = create_bill_for_day(wh_id, bp_id, current)
        if not raw:
            current += timedelta(days=1)
            continue

        result = transform_bill_details(raw, client_name=name, client_number=num,
            alert_settings={"check_package_cost": client.get("check_package_cost", True),
                            "check_pick_fee": client.get("check_pick_fee", True)})

        date_us = current.strftime("%m/%d/%Y")
        for rec in result["report_rows"]:
            if rec.get("Order Number") == "Total":
                rec["Date"] = date_us

        oc = sum(1 for r in result["report_rows"]
                 if r["Order Number"] not in {"Storage", "Return Processing Charges",
                                               "Return Labels Charges", "Total"})

        rows_w = []
        rtypes = []
        rh = [""] * ncols
        rh[0] = date_us
        rh[1] = f"Report — {num} {name}"
        rows_w.append(rh)
        rtypes.append("rh")

        for rec in result["report_rows"]:
            rows_w.append([rec.get(h, "") for h in ALLREPORTS_HEADERS])
            o = rec.get("Order Number", "")
            rtypes.append("t" if o == "Total" else
                         "s" if o in ("Storage", "Return Processing Charges", "Return Labels Charges") else "d")

        ws_ar = retry_api(lambda: ss.worksheet("AllReports"))
        start = len(ws_ar.get_all_values())
        retry_api(lambda: ws_ar.append_rows(rows_w, value_input_option="USER_ENTERED"))

        # Formatting
        fmts = []
        for i, rt in enumerate(rtypes):
            ar = start + i
            if rt == "rh":
                fmts.append(mk_fmt(ar_sid, ar, 0, ncols, bg=LIGHT_PURPLE, bold=True, sz=11, fg=PURPLE_TEXT, ha="LEFT"))
                fmts.append(mk_height(ar_sid, ar, 32))
            elif rt == "t":
                fmts.append(mk_fmt(ar_sid, ar, 0, ncols, bg=PINK_LIGHT, bold=True, sz=14, fg=PINK_TEXT, ha="CENTER"))
                fmts.append(mk_height(ar_sid, ar, 36))
            else:
                fmts.append(mk_fmt(ar_sid, ar, 0, ncols, bg=WHITE, sz=11, fg=BLACK, ha="CENTER"))
                if rt == "d":
                    fmts.append(mk_height(ar_sid, ar, 28))
        lr = start + len(rows_w) - 1
        fmts.append({"updateBorders": {"range": {"sheetId": ar_sid, "startRowIndex": lr,
            "endRowIndex": lr+1, "startColumnIndex": 0, "endColumnIndex": ncols},
            "bottom": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT}}})
        if fmts:
            retry_api(lambda: ss.batch_update({"requests": fmts}))

        # Formulas
        fu = []
        fdr = None
        for i, rt in enumerate(rtypes):
            sr = start + i + 1
            if rt == "rh":
                fdr = sr + 1
            elif rt == "d":
                if fdr is None: fdr = sr
                fu.append({"range": f"I{sr}", "values": [[f"=SUM(E{sr},F{sr},G{sr},H{sr})"]]})
            elif rt == "s":
                if fdr is None: fdr = sr
                fu.append({"range": f"I{sr}", "values": [[f"=E{sr}"]]})
            elif rt == "t":
                ldr = sr - 1
                if fdr and ldr >= fdr:
                    fu.append({"range": f"I{sr}", "values": [[f"=SUM(I{fdr}:I{ldr})"]]})
        if fu:
            retry_api(lambda: ws_ar.batch_update(fu, value_input_option="USER_ENTERED"))

        dates_with_data.append(current)
        days_data += 1
        total_orders += oc
        if oc > 0:
            logger.info(f"  {current}: {oc} orders, ${result['grand_total']:.2f}")
        current += timedelta(days=1)
        time.sleep(1)

    # 6. Build Payments
    logger.info(f"  Building Payments ({len(dates_with_data)} days, {len(deposits)} deposits)...")
    ws_pay = retry_api(lambda: ss.worksheet("Payments"))

    pay_rows = []
    covered = set()
    for d in dates_with_data:
        rn = 3 + len(pay_rows)
        dep = deposits.get(d, "")
        covered.add(d)
        pay_rows.append([
            d.strftime("%m/%d/%Y"), dep,
            f'=SUMIFS(AllReports!I$5:I$50000,AllReports!A$5:A$50000,A{rn},AllReports!B$5:B$50000,"Total")',
            f"=B{rn}-C{rn}", "",
        ])

    # Deposits on dates without charges
    for dep_date, dep_val in sorted(deposits.items()):
        if dep_date not in covered and dep_val:
            rn = 3 + len(pay_rows)
            pay_rows.append([
                dep_date.strftime("%m/%d/%Y"), dep_val,
                f'=SUMIFS(AllReports!I$5:I$50000,AllReports!A$5:A$50000,A{rn},AllReports!B$5:B$50000,"Total")',
                f"=B{rn}-C{rn}", "",
            ])

    # 3 blank rows + Total
    for _ in range(3):
        pay_rows.append(["", "", "", "", ""])
    pay_rows.append(["Total", "", "", "=SUM(D3:D999)", ""])

    retry_api(lambda: ws_pay.append_rows(pay_rows, value_input_option="USER_ENTERED"))

    # Format
    fmts = []
    for i in range(len(pay_rows) - 4):
        fmts.append(mk_fmt(pay_sid, 2+i, 0, 5, bg=WHITE, sz=12, fg=BLACK, ha="CENTER"))
        fmts.append(mk_height(pay_sid, 2+i, 32))
    tr = 2 + len(pay_rows) - 1
    fmts.append(mk_fmt(pay_sid, tr, 0, 5, bg=PINK, bold=True, sz=15, fg=WHITE, ha="CENTER"))
    fmts.append(mk_height(pay_sid, tr, 42))
    fmts.append({"updateBorders": {"range": {"sheetId": pay_sid, "startRowIndex": tr,
        "endRowIndex": tr+1, "startColumnIndex": 0, "endColumnIndex": 5},
        "bottom": {"style": "SOLID_MEDIUM", "color": PURPLE_TEXT}}})
    if fmts:
        retry_api(lambda: ss.batch_update({"requests": fmts}))

    final_rows = len(retry_api(lambda: ss.worksheet("AllReports")).get_all_values())
    logger.info(f"  ✅ {days_data} days, {total_orders} orders, {final_rows} rows, {len(deposits)} deposits")

    return {"client": f"{num} {name}", "first_day": str(first_day), "days_data": days_data,
            "orders": total_orders, "ar_rows": final_rows, "deposits": len(deposits), "status": "ok"}


def main():
    creds = Credentials.from_service_account_file("config/service_account.json", scopes=SCOPES)
    gc = gspread.authorize(creds)

    with open("clients.json") as f:
        clients = json.load(f)["clients"]

    results = []
    for client in clients:
        try:
            r = backfill_client(client, gc)
            results.append(r)
        except Exception as e:
            logger.error(f"FAILED {client['number']} {client['name']}: {e}", exc_info=True)
            results.append({"client": f"{client['number']} {client['name']}", "status": f"ERROR: {e}",
                            "first_day": "?", "days_data": 0, "orders": 0, "ar_rows": 0, "deposits": 0})
        time.sleep(10)

    print("\n" + "=" * 100)
    print("BACKFILL COMPLETE")
    print("=" * 100)
    print(f"{'Client':<30} {'First':<12} {'Days':<6} {'Orders':<8} {'Rows':<8} {'Dep':<5} {'Status'}")
    print("-" * 100)
    for r in results:
        print(f"{r.get('client','?'):<30} {r.get('first_day','?'):<12} "
              f"{r.get('days_data','?'):<6} {r.get('orders','?'):<8} "
              f"{r.get('ar_rows','?'):<8} {r.get('deposits','?'):<5} {r.get('status','?')}")
    print("=" * 100)


if __name__ == "__main__":
    main()
