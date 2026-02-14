"""
Data Transformer for Warehance Bill Details → Google Sheets

Transforms raw charge rows into:
1. FBM tab: per-order rows + summary (Storage, Returns, Total)
2. Payments tab: single row with date + daily total
"""

import csv
import logging
from datetime import datetime
from io import StringIO
from typing import Any

logger = logging.getLogger(__name__)

# FBM tab column headers (matching the client's Google Sheet)
FBM_HEADERS = [
    "Date",
    "Order Number",
    "Tracking number",
    "Storage/Returns",
    "Shipping cost",
    "FBM fee",
    "Package cost",
    "Total",
]

# Charge category mapping
SHIPPING_CATEGORIES = {"shipments"}
PICKING_CATEGORIES = {"picking"}
PACKAGE_CATEGORIES = {"shipment_parcels"}
STORAGE_CATEGORIES = {"storage"}
RETURN_PROCESSING_CATEGORIES = {"returns", "return_processing"}
RETURN_LABEL_CATEGORIES = {"return_labels", "return_shipments"}


def parse_csv_file(filepath: str) -> list[dict]:
    """Parse CSV file into list of dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _safe_float(val: str) -> float:
    if not val or not val.strip():
        return 0.0
    return float(val.replace(",", "."))


def _format_date_short(iso_date: str) -> str:
    """'2026-02-03T18:14:11Z' → '02.03'"""
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.strftime("%m.%d")
    except (ValueError, TypeError):
        return iso_date[:10]


def _format_date_payments(iso_date: str) -> str:
    """'2026-02-03T18:14:11Z' → '02/03/26'"""
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except (ValueError, TypeError):
        return iso_date[:10]


def transform_bill_details(rows: list[dict]) -> dict:
    """
    Transform raw Warehance bill-detail rows.

    Returns dict with:
        "fbm_rows": list of dicts for FBM tab
        "fbm_headers": list of column names
        "payments_row": {"date": "02/03/26", "paid": 24.92}
        "grand_total": float
    """
    orders: dict[str, dict[str, Any]] = {}
    storage_total = 0.0
    return_processing_total = 0.0
    return_labels_total = 0.0
    first_date = ""

    for row in rows:
        amount = _safe_float(row.get("Amount", "0"))
        category = row.get("Charge Category", "").strip().lower()
        order_num = row.get("Order Number", "").strip()

        # Track the earliest date for Payments tab
        row_date = row.get("Date", "")
        if row_date and (not first_date or row_date < first_date):
            first_date = row_date

        if category in STORAGE_CATEGORIES:
            storage_total += amount
            continue
        if category in RETURN_PROCESSING_CATEGORIES:
            return_processing_total += amount
            continue
        if category in RETURN_LABEL_CATEGORIES:
            return_labels_total += amount
            continue
        if not order_num:
            continue

        if order_num not in orders:
            shipment_date = row.get("Shipment Date", "") or row.get("Date", "")
            orders[order_num] = {
                "date": shipment_date,
                "order_number": order_num,
                "tracking": row.get("Tracking Number", ""),
                "shipping_cost": 0.0,
                "fbm_fee": 0.0,
                "package_cost": 0.0,
            }

        entry = orders[order_num]
        if not entry["tracking"] and row.get("Tracking Number"):
            entry["tracking"] = row["Tracking Number"]
        if not entry["date"]:
            entry["date"] = row.get("Shipment Date", "") or row.get("Date", "")

        if category in SHIPPING_CATEGORIES:
            entry["shipping_cost"] += amount
        elif category in PICKING_CATEGORIES:
            entry["fbm_fee"] += amount
        elif category in PACKAGE_CATEGORIES:
            entry["package_cost"] += amount

    # --- Build FBM rows ---
    fbm_rows = []
    sorted_orders = sorted(orders.values(), key=lambda o: (o["date"], o["order_number"]))

    for entry in sorted_orders:
        total = entry["shipping_cost"] + entry["fbm_fee"] + entry["package_cost"]
        fbm_rows.append({
            "Date": _format_date_short(entry["date"]),
            "Order Number": entry["order_number"],
            "Tracking number": entry["tracking"],
            "Storage/Returns": "",
            "Shipping cost": round(entry["shipping_cost"], 2) if entry["shipping_cost"] else "",
            "FBM fee": round(entry["fbm_fee"], 2) if entry["fbm_fee"] else "",
            "Package cost": round(entry["package_cost"], 2) if entry["package_cost"] else "",
            "Total": round(total, 2),
        })

    # Summary rows
    fbm_rows.append({
        "Date": "", "Order Number": "Storage", "Tracking number": "",
        "Storage/Returns": round(storage_total, 2),
        "Shipping cost": "", "FBM fee": "", "Package cost": "",
        "Total": round(storage_total, 2),
    })
    fbm_rows.append({
        "Date": "", "Order Number": "Return Processing Charges", "Tracking number": "",
        "Storage/Returns": round(return_processing_total, 2),
        "Shipping cost": "", "FBM fee": "", "Package cost": "",
        "Total": round(return_processing_total, 2),
    })
    fbm_rows.append({
        "Date": "", "Order Number": "Return Labels Charges", "Tracking number": "",
        "Storage/Returns": round(return_labels_total, 2),
        "Shipping cost": "", "FBM fee": "", "Package cost": "",
        "Total": round(return_labels_total, 2),
    })

    grand_total = sum(
        r["Total"] for r in fbm_rows if isinstance(r["Total"], (int, float))
    )
    fbm_rows.append({
        "Date": "", "Order Number": "Total", "Tracking number": "",
        "Storage/Returns": "", "Shipping cost": "", "FBM fee": "", "Package cost": "",
        "Total": round(grand_total, 2),
    })

    logger.info(
        f"Transformed {len(rows)} raw rows → {len(sorted_orders)} orders | "
        f"Grand total: {grand_total:.2f}"
    )

    return {
        "fbm_rows": fbm_rows,
        "fbm_headers": FBM_HEADERS,
        "payments_row": {
            "date": _format_date_payments(first_date),
            "paid": round(grand_total, 2),
        },
        "grand_total": grand_total,
    }
