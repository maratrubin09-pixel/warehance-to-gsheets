#!/usr/bin/env python3
"""
Warehance Bill Rounding Analysis
Fetches bills for Jan 15 and Jan 16, 2026 and analyzes rounding behavior.
"""
import requests
import csv
import json
import io
import time
import os
from decimal import Decimal, ROUND_HALF_UP

API_BASE = "https://api.warehance.com/v1"
API_KEY = os.environ["WAREHANCE_API_KEY"]

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

CLIENT_ID = 154
BILLING_PROFILE_ID = 231185181547

DAYS = [
    {
        "label": "Jan 15, 2026",
        "start": "2026-01-15T00:00:00-07:00",
        "end":   "2026-01-15T23:59:59-07:00",
    },
    {
        "label": "Jan 16, 2026",
        "start": "2026-01-16T00:00:00-07:00",
        "end":   "2026-01-16T23:59:59-07:00",
    },
]


def create_bill(start_date, end_date):
    payload = {
        "client_id": CLIENT_ID,
        "billing_profile_id": BILLING_PROFILE_ID,
        "start_date": start_date,
        "end_date": end_date,
    }
    resp = requests.post(f"{API_BASE}/bills", headers=HEADERS, json=payload)
    print(f"  CREATE status: {resp.status_code}")
    print(f"  CREATE body: {resp.text[:500]}")
    resp.raise_for_status()
    return resp.json()


def wait_for_bill(bill_id, max_wait=120):
    for attempt in range(max_wait // 5):
        resp = requests.get(f"{API_BASE}/bills/{bill_id}", headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("generation_status") or data.get("status", "")
        print(f"  Poll {attempt+1}: status={status}")
        if status in ("completed", "failed", "generated", "ready", "done"):
            return data
        if data.get("line_item_details_csv_url") or data.get("line_items"):
            return data
        time.sleep(5)
    resp = requests.get(f"{API_BASE}/bills/{bill_id}", headers=HEADERS)
    return resp.json()


def find_col(row, candidates):
    for c in candidates:
        if c in row:
            return c
    return None


def analyze_day(day_info):
    label = day_info["label"]
    print(f"\n{'='*60}")
    print(f"ANALYZING: {label}")
    print(f"{'='*60}")

    bill_create = create_bill(day_info["start"], day_info["end"])
    bill_id = (
        bill_create.get("id")
        or bill_create.get("bill_id")
        or (bill_create.get("bill") or {}).get("id")
    )
    if not bill_id:
        print(f"ERROR: No bill ID in: {json.dumps(bill_create, indent=2)}")
        return None
    print(f"Bill ID: {bill_id}")

    print(f"\n--- Waiting for generation ---")
    bill = wait_for_bill(bill_id)
    print(f"\nBill response (first 3000 chars):")
    bill_str = json.dumps(bill, indent=2)
    print(bill_str[:3000])
    if len(bill_str) > 3000:
        print(f"... [truncated, total {len(bill_str)} chars]")

    total_amount = bill.get("total_amount")
    csv_url = bill.get("line_item_details_csv_url")
    line_items = bill.get("line_items", [])

    print(f"\n--- BILL SUMMARY ---")
    print(f"total_amount: {total_amount}")
    print(f"CSV URL: {csv_url}")
    print(f"line_items count: {len(line_items)}")
    print(f"\n--- LINE ITEMS (category summaries) ---")
    for li in line_items:
        cat = li.get("category") or li.get("name") or li.get("charge_rule_name", "N/A")
        amt = li.get("total_amount") or li.get("amount")
        qty = li.get("quantity")
        print(f"  {cat}: total_amount={amt}, quantity={qty}, keys={list(li.keys())}")

    csv_rows = []
    raw_amounts = []
    amount_col = None
    order_col = None

    if csv_url:
        print(f"\n--- DOWNLOADING CSV ---")
        csv_resp = requests.get(csv_url)
        csv_resp.raise_for_status()
        csv_content = csv_resp.text
        print(f"CSV size: {len(csv_content)} chars")
        reader = csv.DictReader(io.StringIO(csv_content))
        for row in reader:
            csv_rows.append(row)
        print(f"CSV rows: {len(csv_rows)}")
        if csv_rows:
            print(f"CSV columns: {list(csv_rows[0].keys())}")
            amount_col = find_col(csv_rows[0], ["Amount", "amount", "Total", "total"])
            order_col = find_col(csv_rows[0], ["Order Number", "order_number", "OrderNumber", "Order", "order"])
            rule_col = find_col(csv_rows[0], ["Charge Rule Name", "charge_rule_name", "ChargeRuleName", "Rule", "rule"])

            print(f"\n--- CSV ROWS: Amount | Rule | Order ---")
            for i, row in enumerate(csv_rows):
                a = row.get(amount_col, "N/A") if amount_col else "N/A"
                r = row.get(rule_col, "N/A")[:40] if rule_col else "N/A"
                o = row.get(order_col, "N/A") if order_col else "N/A"
                print(f"  [{i+1:3d}] Amount={a:>14} | Rule={r:40s} | Order={o}")
                if amount_col:
                    try:
                        raw_amounts.append(float(str(a).replace(",", "").strip()))
                    except ValueError:
                        raw_amounts.append(0.0)
    else:
        print("No CSV URL available.")

    # --- ROUNDING ANALYSIS ---
    print(f"\n--- ROUNDING ANALYSIS ---")
    analysis = {}
    if raw_amounts:
        float_sum = sum(raw_amounts)
        rounded_each = [round(a, 2) for a in raw_amounts]
        sum_rounded_each = sum(rounded_each)
        decimal_amounts = [Decimal(str(a)) for a in raw_amounts]
        decimal_sum = sum(decimal_amounts)
        decimal_rounded = float(decimal_sum.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        bill_total = float(total_amount) if total_amount is not None else None

        print(f"  Count of CSV lines:              {len(raw_amounts)}")
        print(f"  Sum of raw floats:               {float_sum:.8f}")
        print(f"  round(sum_of_floats, 2):         {round(float_sum, 2):.2f}")
        print(f"  sum(round(each_line, 2)):         {sum_rounded_each:.2f}")
        print(f"  Decimal sum:                     {float(decimal_sum):.8f}")
        print(f"  Decimal sum rounded to 2dp:      {decimal_rounded:.2f}")
        print(f"  Bill total_amount:               {total_amount}")

        analysis = {
            "line_count": len(raw_amounts),
            "float_sum": float_sum,
            "round_float_sum": round(float_sum, 2),
            "sum_rounded_each_line": sum_rounded_each,
            "decimal_sum": float(decimal_sum),
            "decimal_rounded": decimal_rounded,
            "bill_total": bill_total,
        }

        if bill_total is not None:
            m1 = abs(round(float_sum, 2) - bill_total) < 0.005
            m2 = abs(sum_rounded_each - bill_total) < 0.005
            m3 = abs(decimal_rounded - bill_total) < 0.005
            print(f"\n  MATCH: 'round whole sum at end'     => {m1}")
            print(f"  MATCH: 'sum of individually rounded' => {m2}")
            print(f"  MATCH: 'decimal sum rounded'          => {m3}")
            analysis.update({"match_round_at_end": m1, "match_sum_rounded_lines": m2, "match_decimal": m3})

        # Per-order analysis
        if order_col and csv_rows:
            order_groups = {}
            for row, amt in zip(csv_rows, raw_amounts):
                order = row.get(order_col, "UNKNOWN")
                order_groups.setdefault(order, []).append(amt)

            print(f"\n  Per-Order Analysis ({len(order_groups)} orders):")
            per_order_rounded_sum = 0.0
            order_details = []
            for order, amts in sorted(order_groups.items()):
                order_raw = sum(amts)
                order_r = round(order_raw, 2)
                lines_r = sum(round(a, 2) for a in amts)
                per_order_rounded_sum += order_r
                diff = order_r - lines_r
                order_details.append({
                    "order": order,
                    "raw_sum": order_raw,
                    "round_raw_sum": order_r,
                    "sum_rounded_lines": lines_r,
                    "diff": diff,
                })
                if len(order_details) <= 30:
                    print(f"    Order {order}: raw={order_raw:.6f} | round(raw)={order_r:.2f} | sum(round_lines)={lines_r:.2f} | diff={diff:.4f}")
            if len(order_details) > 30:
                print(f"    ... [{len(order_details) - 30} more orders not shown]")

            print(f"\n  Sum of per-order-rounded:        {per_order_rounded_sum:.2f}")
            print(f"  Bill total_amount:               {total_amount}")
            if bill_total is not None:
                m4 = abs(per_order_rounded_sum - bill_total) < 0.005
                print(f"  MATCH: 'round per-order then sum' => {m4}")
                analysis["match_per_order_rounded"] = m4
                analysis["per_order_rounded_sum"] = per_order_rounded_sum
            analysis["order_details"] = order_details
    else:
        print("  No amount data to analyze.")

    return {
        "label": label,
        "bill_id": bill_id,
        "total_amount": total_amount,
        "line_items": line_items,
        "csv_row_count": len(csv_rows),
        "csv_rows_full": csv_rows,
        "analysis": analysis,
    }


all_results = {}
for day in DAYS:
    try:
        result = analyze_day(day)
        if result:
            all_results[day["label"]] = result
    except Exception as e:
        print(f"\nERROR on {day['label']}: {e}")
        import traceback
        traceback.print_exc()

print(f"\n\n{'='*60}")
print("FINAL SUMMARY")
print(f"{'='*60}")
for label, r in all_results.items():
    print(f"\n{label}:")
    print(f"  Bill ID:       {r.get('bill_id')}")
    print(f"  Total amount:  {r.get('total_amount')}")
    print(f"  CSV rows:      {r.get('csv_row_count')}")
    a = r.get("analysis", {})
    if a:
        print(f"  Float sum:     {a.get('float_sum', 'N/A'):.6f}" if isinstance(a.get('float_sum'), float) else f"  Float sum:     N/A")
        print(f"  Round(sum):    {a.get('round_float_sum', 'N/A')}")
        print(f"  Sum(rounds):   {a.get('sum_rounded_each_line', 'N/A')}")
        print(f"  Match round-at-end:    {a.get('match_round_at_end', 'N/A')}")
        print(f"  Match sum-rounds:      {a.get('match_sum_rounded_lines', 'N/A')}")
        print(f"  Match per-order:       {a.get('match_per_order_rounded', 'N/A')}")

with open("rounding_results.json", "w") as f:
    json.dump(all_results, f, indent=2, default=str)
print("\nSaved rounding_results.json")
