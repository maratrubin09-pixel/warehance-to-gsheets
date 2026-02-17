"""
Warehance API Client — fetches bills, downloads CSV details, and shipments.
"""

import csv
import io
import json
import logging
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://api.warehance.com/v1"

# Load packaging costs lookup
_pkg_costs_path = Path(__file__).parent / "packaging_costs.json"
PACKAGING_COSTS = {}
if _pkg_costs_path.exists():
    with open(_pkg_costs_path) as f:
        PACKAGING_COSTS = json.load(f)


class WarehanceClient:
    def __init__(self, api_key: str, base_url: str = BASE_URL):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        })
        self._shipments_cache: dict[int, dict] = {}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def check_auth(self) -> bool:
        try:
            r = self._get("/bills", params={"limit": 1})
            ok = r.get("status") == "success"
            logger.info(f"Warehance auth {'OK' if ok else 'FAILED'}")
            return ok
        except Exception as e:
            logger.error(f"Auth check failed: {e}")
            return False

    def get_bills_for_client(self, client_id: int, days_back: int = 1) -> list[dict]:
        """Get all bills for a client."""
        params = {"client_id": client_id, "limit": 50}
        resp = self._get("/bills", params=params)
        bills = resp.get("data", {}).get("bills", [])
        logger.info(f"Found {len(bills)} bills for client {client_id}")
        return bills

    def download_bill_csv(self, csv_url: str) -> list[dict]:
        """Download CSV from S3 presigned URL and parse into rows."""
        try:
            resp = requests.get(csv_url, timeout=60)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            rows = list(reader)
            logger.info(f"Downloaded CSV: {len(rows)} rows, columns: {list(rows[0].keys()) if rows else 'empty'}")
            return rows
        except Exception as e:
            logger.error(f"CSV download failed: {e}")
            return []

    def get_bill_details_for_client(self, client_id: int, days_back: int = 1) -> list[dict]:
        """Get bills -> download most recent CSV -> return parsed rows."""
        bills = self.get_bills_for_client(client_id, days_back=days_back)
        if not bills:
            return []

        # Sort by created_at descending, take the most recent
        bills.sort(key=lambda b: b.get("created_at", ""), reverse=True)

        for bill in bills:
            csv_url = bill.get("line_item_details_csv_url", "")
            if csv_url:
                bill_name = bill.get("bill_name", "?")
                logger.info(f"Downloading: {bill_name}")
                rows = self.download_bill_csv(csv_url)
                if rows:
                    return rows

        logger.warning(f"No CSV in {len(bills)} bills for client {client_id}")
        return []

    def get_shipments_map(self, client_id: int) -> dict:
        """
        Fetch all shipments for a client and return a lookup map:
        { order_number: { "shipment_cost": float, "boxes": [str, ...] } }

        Uses cursor-based pagination. Results are cached per client_id.
        """
        if client_id in self._shipments_cache:
            return self._shipments_cache[client_id]

        shipments_map: dict[str, dict] = {}
        cursor = None
        page = 0

        while True:
            params = {"limit": 100, "client_id": client_id}
            if cursor:
                params["cursor"] = cursor

            try:
                resp = self._get("/shipments", params=params)
            except Exception as e:
                logger.error(f"Shipments fetch failed (page {page}): {e}")
                break

            data = resp.get("data", {})
            shipments = data.get("shipments") or []

            for s in shipments:
                if s.get("voided"):
                    continue
                order_num = s.get("order", {}).get("order_number", "")
                if not order_num:
                    continue

                cost = float(s.get("shipment_cost", 0) or 0)
                boxes = []
                for parcel in s.get("shipment_parcels", []):
                    box = parcel.get("box", "")
                    if box:
                        boxes.append(box)

                if order_num in shipments_map:
                    shipments_map[order_num]["shipment_cost"] += cost
                    shipments_map[order_num]["boxes"].extend(boxes)
                else:
                    shipments_map[order_num] = {
                        "shipment_cost": cost,
                        "boxes": boxes,
                    }

            page += 1

            if not data.get("has_next_page"):
                break
            cursor = data.get("next_cursor", "")
            if not cursor:
                break

        logger.info(f"Shipments map for client {client_id}: {len(shipments_map)} orders across {page} pages")
        self._shipments_cache[client_id] = shipments_map
        return shipments_map

    @staticmethod
    def calc_packaging_cost(boxes: list[str]) -> float:
        """Calculate total packaging material cost from box types."""
        total = 0.0
        for box in boxes:
            cost = PACKAGING_COSTS.get(box, 0)
            if cost == 0 and box:
                # Try fuzzy match: strip extra spaces, normalize
                normalized = box.strip()
                for key, val in PACKAGING_COSTS.items():
                    if key.lower() == normalized.lower():
                        cost = val
                        break
            total += cost
        return round(total, 4)
