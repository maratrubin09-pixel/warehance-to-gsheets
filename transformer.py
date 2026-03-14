"""
Data Transformer — Warehance bill-details -> Google Sheets format.

Handles TWO CSV formats:
  Format A (old): Charge Category, Amount, Date, Order Number, Tracking Number, Shipment Date
  Format B (API): Charge Type Category, Charge Rule Name, Amount, Date, Description, Order Number

v2.1:
  - Packaging Type column
  - Unknown category logging
  - Split-day pick fee detection (returns list for agent to resolve)
  - Improved anomaly logic for package cost (custom package, client 154)
  - Payments breakdown for client 257 (Storage, Returns, Orders, Return Labels)
"""

import csv
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

logger = logging.getLogger(__name__)

# Column order for AllReports
ALLREPORTS_HEADERS = [
    "Date", "Order Number", "Tracking number",
    "Pick&Pack fee", "Packaging Type", "Packaging Cost",
    "Shipping cost", "Total",
]

# Category mapping — handles both formats
SHIPPING_KEYWORDS = {"shipments", "shipping", "ship"}
PICKING_KEYWORDS = {"picking", "pick", "pick_and_pack"}
PACKAGE_KEYWORDS = {"shipment_parcels", "parcels", "parcel", "packaging", "package"}
STORAGE_KEYWORDS = {"storage"}
RETURN_PROCESSING_KEYWORDS = {"returns", "return_processing", "return processing"}
RETURN_LABEL_KEYWORDS = {"return_labels", "return_shipments", "return labels", "return shipments"}

_ALL_KNOWN_KEYWORDS = (
    SHIPPING_KEYWORDS | PICKING_KEYWORDS | PACKAGE_KEYWORDS |
    STORAGE_KEYWORDS | RETURN_PROCESSING_KEYWORDS | RETURN_LABEL_KEYWORDS
)


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
    cat = row.get("Charge Category", "") or row.get("Charge Type Category", "")
    return cat.strip().lower()


def _get_order_number(row: dict) -> str:
    return (row.get("Order Number", "") or "").strip()


def _get_tracking(row: dict) -> str:
    return (row.get("Tracking Number", "") or row.get("Tracking number", "") or "").strip()


def _get_date(row: dict) -> str:
    return row.get("Date", "") or row.get("Shipment Date", "") or ""


def _get_description(row: dict) -> str:
    return (
        row.get("Charge Rule Name", "") or
        row.get("Description", "") or
        row.get("description", "") or
        ""
    ).strip()


def _matches_any(category: str, keywords: set) -> bool:
    if category in keywords:
        return True
    for kw in keywords:
        if kw in category:
            return True
    return False


def _is_known_category(category: str) -> bool:
    return _matches_any(category, _ALL_KNOWN_KEYWORDS)


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


def _extract_packaging_type(row: dict) -> str:
    desc = _get_description(row)
    if not desc:
        return ""
    desc_lower = desc.lower()
    for prefix in ("box ", "poly mailer ", "bubble mailer "):
        if prefix in desc_lower:
            idx = desc_lower.index(prefix)
            rest = desc[idx:]
            for sep in (",", ";", "|", " - "):
                if sep in rest:
                    rest = rest[:rest.index(sep)]
            return rest.strip()
    return desc


def _is_custom_package(packaging_type: str) -> bool:
    """Check if the packaging type is 'custom' (client's own packaging)."""
    if not packaging_type:
        return False
    pt = packaging_type.lower().strip()
    return "custom" in pt or "own" in pt or "client" in pt


def transform_bill_details(
    rows: list[dict],
    client_name: str = "",
    client_number: str = "",
    alert_settings: dict = None,
) -> dict:
    """
    Transform raw bill CSV rows into report format.

    Returns dict with:
      - report_rows: list of dicts for AllReports
      - headers: column headers
      - payments_row: single payment line (for most clients)
      - payments_rows: multiple payment lines (for client 257 breakdown)
      - grand_total: float
      - anomalies: list of anomaly dicts
      - report_date: formatted date string
      - missing_pick_orders: list of order numbers with no pick fee (for split-day resolution)
      - category_totals: dict with storage, return_processing, return_labels, orders_total
    """
    orders: dict[str, dict[str, Any]] = {}
    storage_total = 0.0
    return_processing_total = 0.0
    return_labels_total = 0.0
    first_date = ""
    unknown_categories = set()

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

        if category and not _is_known_category(category):
            unknown_categories.add(category)

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
                "packaging_type": "",
            }

        entry = orders[order_num]
        if not entry["tracking"]:
            t = _get_tracking(row)
            if t:
                entry["tracking"] = t
        if not entry["date"] and row_date:
            entry["date"] = row_date

        if _matches_any(category, PACKAGE_KEYWORDS):
            entry["package_cost"] += amount
            pkg_type = _extract_packaging_type(row)
            if pkg_type and not entry["packaging_type"]:
                entry["packaging_type"] = pkg_type
        elif _matches_any(category, PICKING_KEYWORDS):
            entry["pick_fee"] += amount
        elif _matches_any(category, SHIPPING_KEYWORDS):
            entry["shipping_cost"] += amount

    if unknown_categories:
        logger.warning(
            f"[{client_name}] Unknown charge categories (not mapped): {unknown_categories}. "
            f"These charges were SKIPPED. Consider adding them to transformer.py keywords."
        )

    # --- Anomaly detection (improved) ---
    if alert_settings is None:
        alert_settings = {"check_package_cost": True, "check_pick_fee": True}

    anomalies = []
    missing_pick_orders = []  # Orders that need pick fee from previous day

    for entry in orders.values():
        onum = entry["order_number"]
        has_shipping = entry["shipping_cost"] > 0

        # Pick&Pack fee check: if order has shipping but pick_fee=0, it's a split-day order
        if alert_settings.get("check_pick_fee", True) and has_shipping and entry["pick_fee"] == 0:
            missing_pick_orders.append(onum)
            anomalies.append({
                "order_number": onum,
                "issue": "🔧 Pick&Pack fee = $0.00 — пикали в предыдущий день, нужно подтянуть",
                "type": "missing_pick_fee",
            })

        # Package cost check (improved logic)
        if alert_settings.get("check_package_cost", True) and has_shipping and entry["package_cost"] == 0:
            # Custom package → OK, not an anomaly
            if _is_custom_package(entry["packaging_type"]):
                continue

            # Client 154 special rule: pick_fee = 1.50 (single item) and package=0 → OK
            if client_number == "154" and entry["pick_fee"] == 1.50:
                continue

            anomalies.append({
                "order_number": onum,
                "issue": "📦 Package cost = $0.00 — не проставлена стоимость упаковки",
                "type": "missing_package_cost",
            })

    # --- Build report rows ---
    report_rows = []
    sorted_orders = sorted(orders.values(), key=lambda o: (o["date"], o["order_number"]))

    orders_total = 0.0  # Sum of all order totals (for 257 breakdown)
    for entry in sorted_orders:
        total = entry["shipping_cost"] + entry["pick_fee"] + entry["package_cost"]
        orders_total += total
        report_rows.append({
            "Date": _format_date_short(entry["date"]),
            "Order Number": entry["order_number"],
            "Tracking number": entry["tracking"],
            "Pick&Pack fee": round(entry["pick_fee"], 2) if entry["pick_fee"] else "",
            "Packaging Type": entry["packaging_type"],
            "Packaging Cost": round(entry["package_cost"], 2) if entry["package_cost"] else "",
            "Shipping cost": round(entry["shipping_cost"], 2) if entry["shipping_cost"] else "",
            "Total": round(total, 2),
        })

    # Summary rows (daily charges)
    report_rows.append({
        "Date": "", "Order Number": "Storage", "Tracking number": "",
        "Pick&Pack fee": "", "Packaging Type": "", "Packaging Cost": "",
        "Shipping cost": round(storage_total, 2) if storage_total else "",
        "Total": round(storage_total, 2),
    })
    report_rows.append({
        "Date": "", "Order Number": "Return Processing Charges", "Tracking number": "",
        "Pick&Pack fee": "", "Packaging Type": "", "Packaging Cost": "",
        "Shipping cost": round(return_processing_total, 2) if return_processing_total else "",
        "Total": round(return_processing_total, 2),
    })
    report_rows.append({
        "Date": "", "Order Number": "Return Labels Charges", "Tracking number": "",
        "Pick&Pack fee": "", "Packaging Type": "", "Packaging Cost": "",
        "Shipping cost": round(return_labels_total, 2) if return_labels_total else "",
        "Total": round(return_labels_total, 2),
    })

    grand_total = sum(
        r["Total"] for r in report_rows if isinstance(r["Total"], (int, float))
    )
    report_rows.append({
        "Date": _format_date_full(first_date), "Order Number": "Total",
        "Tracking number": "", "Pick&Pack fee": "", "Packaging Type": "",
        "Packaging Cost": "", "Shipping cost": "",
        "Total": round(grand_total, 2),
    })

    logger.info(
        f"Transformed {len(rows)} raw -> {len(sorted_orders)} orders | "
        f"Total: ${grand_total:.2f} | Anomalies: {len(anomalies)} | "
        f"Missing pick fee: {len(missing_pick_orders)}"
    )

    # --- Payments data ---
    pay_date = _format_date_payments(first_date)

    # Standard single-line payment (used for all clients except 257)
    payments_row = {
        "date": pay_date,
        "paid": round(grand_total, 2),
    }

    # Breakdown for client 257 (SOLMAR): multiple lines with comments
    payments_rows = []
    if client_number == "257":
        # Orders (Shopify)
        if orders_total > 0:
            payments_rows.append({
                "date": pay_date,
                "paid": round(orders_total, 2),
                "comment": "Shopify",
            })
        # Storage
        if storage_total > 0:
            payments_rows.append({
                "date": pay_date,
                "paid": round(storage_total, 2),
                "comment": "Storage",
            })
        # Return Labels
        if return_labels_total > 0:
            payments_rows.append({
                "date": pay_date,
                "paid": round(return_labels_total, 2),
                "comment": "Return Labels",
            })
        # Returns (Return Processing)
        if return_processing_total > 0:
            payments_rows.append({
                "date": pay_date,
                "paid": round(return_processing_total, 2),
                "comment": "Returns",
            })
        # If nothing at all, still write a zero line
        if not payments_rows:
            payments_rows.append({
                "date": pay_date,
                "paid": 0,
                "comment": "",
            })

    # Category totals (for external use)
    category_totals = {
        "storage": round(storage_total, 2),
        "return_processing": round(return_processing_total, 2),
        "return_labels": round(return_labels_total, 2),
        "orders_total": round(orders_total, 2),
    }

    return {
        "report_rows": report_rows,
        "headers": ALLREPORTS_HEADERS,
        "payments_row": payments_row,
        "payments_rows": payments_rows,  # For client 257 only
        "grand_total": grand_total,
        "anomalies": anomalies,
        "report_date": _format_date_full(first_date),
        "missing_pick_orders": missing_pick_orders,
        "category_totals": category_totals,
    }
