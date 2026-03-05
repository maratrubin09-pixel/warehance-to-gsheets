"""
Data Transformer — Warehance bill-details -> Google Sheets format.

Handles TWO CSV formats:
  Format A (old): Charge Category, Amount, Date, Order Number, Tracking Number, Shipment Date
  Format B (API): Charge Type Category, Charge Rule Name, Amount, Date, Description, Order Number
"""

import csv
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

logger = logging.getLogger(__name__)

ALLREPORTS_HEADERS = [
    "Date", "Order Number", "Tracking number", "Storage/Returns",
    "Shipping cost", "Pick&Pack fee", "Package cost", "Total",
]

# Category mapping — handles both formats
SHIPPING_KEYWORDS = {"shipments", "shipping", "ship"}
PICKING_KEYWORDS = {"picking", "pick", "pick_and_pack"}
PACKAGE_KEYWORDS = {"shipment_parcels", "parcels", "parcel", "packaging", "package"}
STORAGE_KEYWORDS = {"storage"}
RETURN_PROCESSING_KEYWORDS = {"returns", "return_processing", "return processing"}
RETURN_LABEL_KEYWORDS = {"return_labels", "return_shipments", "return labels", "return shipments"}


def parse_csv_file(filepath: str) -> list[dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    val = str(val).strip()
    if not val:
        return 0.0
    return float(val.replace(",", "."))


def _get_category(row: dict) -> str:
    """Extract charge category from either CSV format."""
    cat = row.get("Charge Category", "") or row.get("Charge Type Category", "")
    return cat.strip().lower()


def _get_order_number(row: dict) -> str:
    return (row.get("Order Number", "") or "").strip()


def _get_tracking(row: dict) -> str:
    return (row.get("Tracking Number", "") or row.get("Tracking number", "") or "").strip()


def _get_date(row: dict) -> str:
    return row.get("Date", "") or row.get("Shipment Date", "") or ""


def _matches_any(category: str, keywords: set) -> bool:
    if category in keywords:
        return True
    for kw in keywords:
        if kw in category:
            return True
    return False


def _format_date_short(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        return dt.strftime("%m.%d")
    except (ValueError, TypeError):
        return iso_date[:10]


_PACIFIC = ZoneInfo("America/Los_Angeles")


def _format_date_full(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        dt = dt.astimezone(_PACIFIC)
        return dt.strftime("%m.%d.%Y")
    except (ValueError, TypeError):
        return iso_date[:10]


def _format_date_payments(iso_date: str) -> str:
    if not iso_date:
        return ""
    try:
        dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        dt = dt.astimezone(_PACIFIC)
        return dt.strftime("%m/%d/%y")
    except (ValueError, TypeError):
        return iso_date[:10]


def transform_bill_details(rows: list[dict], client_name: str = "", alert_settings: dict = None) -> dict:
    orders: dict[str, dict[str, Any]] = {}
    storage_total = 0.0
    return_processing_total = 0.0
    return_labels_total = 0.0
    first_date = ""

    for row in rows:
        amount = _safe_float(row.get("Amount", "0"))
        category = _get_category(row)
        order_num = _get_order_number(row)
        row_date = _get_date(row)

        if row_date and (not first_date or row_date < first_date):
            first_date = row_date

        if _matches_any(category, STORAGE_KEYWORDS):
            storage_total += amount
            continue
        if _matches_any(category, RETURN_PROCESSING_KEYWORDS):
            return_processing_total += amount
            continue
        if _matches_any(category, RETURN_LABEL_KEYWORDS):
            return_labels_total += amount
            continue
        if not order_num:
            continue

        if order_num not in orders:
            orders[order_num] = {
                "date": row_date,
                "order_number": order_num,
                "tracking": _get_tracking(row),
                "shipping_cost": 0.0,
                "pick_fee": 0.0,
                "package_cost": 0.0,
            }

        entry = orders[order_num]
        if not entry["tracking"]:
            t = _get_tracking(row)
            if t:
                entry["tracking"] = t
        if not entry["date"] and row_date:
            entry["date"] = row_date

        # IMPORTANT: check shipment_parcels BEFORE shipments
        # because "shipments" is a substring of "shipment_parcels"
        if _matches_any(category, PACKAGE_KEYWORDS):
            entry["package_cost"] += amount
        elif _matches_any(category, PICKING_KEYWORDS):
            entry["pick_fee"] += amount
        elif _matches_any(category, SHIPPING_KEYWORDS):
            entry["shipping_cost"] += amount

    # Anomaly detection (respects per-client settings)
    if alert_settings is None:
        alert_settings = {"check_package_cost": True, "check_pick_fee": True}
    anomalies = []
    for entry in orders.values():
        onum = entry["order_number"]
        has_shipping = entry["shipping_cost"] > 0
        if alert_settings.get("check_package_cost", True) and has_shipping and entry["package_cost"] == 0:
            anomalies.append({
                "order_number": onum,
                "issue": "📦 Package cost = $0.00 — не проставлена стоимость упаковки",
            })
        if alert_settings.get("check_pick_fee", True) and has_shipping and entry["pick_fee"] == 0:
            anomalies.append({
                "order_number": onum,
                "issue": "🔧 Pick&Pack fee = $0.00 — возможно пикали вчера, отправили сегодня",
            })

    # Build report rows
    report_rows = []
    sorted_orders = sorted(orders.values(), key=lambda o: (o["date"], o["order_number"]))

    for entry in sorted_orders:
        total = entry["shipping_cost"] + entry["pick_fee"] + entry["package_cost"]
        report_rows.append({
            "Date": _format_date_short(entry["date"]),
            "Order Number": entry["order_number"],
            "Tracking number": entry["tracking"],
            "Storage/Returns": "",
            "Shipping cost": round(entry["shipping_cost"], 2) if entry["shipping_cost"] else "",
            "Pick&Pack fee": round(entry["pick_fee"], 2) if entry["pick_fee"] else "",
            "Package cost": round(entry["package_cost"], 2) if entry["package_cost"] else "",
            "Total": round(total, 2),
        })

    report_rows.append({
        "Date": "", "Order Number": "Storage", "Tracking number": "",
        "Storage/Returns": round(storage_total, 2),
        "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "",
        "Total": round(storage_total, 2),
    })
    report_rows.append({
        "Date": "", "Order Number": "Return Processing Charges", "Tracking number": "",
        "Storage/Returns": round(return_processing_total, 2),
        "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "",
        "Total": round(return_processing_total, 2),
    })
    report_rows.append({
        "Date": "", "Order Number": "Return Labels Charges", "Tracking number": "",
        "Storage/Returns": round(return_labels_total, 2),
        "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "",
        "Total": round(return_labels_total, 2),
    })

    grand_total = sum(
        r["Total"] for r in report_rows if isinstance(r["Total"], (int, float))
    )
    report_rows.append({
        "Date": _format_date_full(first_date), "Order Number": "Total",
        "Tracking number": "", "Storage/Returns": "",
        "Shipping cost": "", "Pick&Pack fee": "", "Package cost": "",
        "Total": round(grand_total, 2),
    })

    logger.info(
        f"Transformed {len(rows)} raw -> {len(sorted_orders)} orders | "
        f"Total: ${grand_total:.2f} | Anomalies: {len(anomalies)}"
    )

    return {
        "report_rows": report_rows,
        "headers": ALLREPORTS_HEADERS,
        "payments_row": {
            "date": _format_date_payments(first_date),
            "paid": round(grand_total, 2),
        },
        "grand_total": grand_total,
        "anomalies": anomalies,
        "report_date": _format_date_full(first_date),
    }
