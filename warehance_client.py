"""
Warehance API Client — fetches bills and downloads CSV details.
"""

import csv
import io
import logging

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://api.warehance.com/v1"


class WarehanceClient:
    def __init__(self, api_key: str, base_url: str = BASE_URL):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        })

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

    def get_shipments(self, client_id: int, date_from: str, date_to: str) -> list[dict]:
        """
        Fetch shipments for a client in a date range.

        Returns list of dicts with keys:
          - order_number: str
          - shipment_cost: float  (carrier cost)
          - parcels: list of {box: str, tracking_number: str}
        """
        all_shipments = []
        page = 1
        while True:
            try:
                resp = self._get("/shipments", params={
                    "client_id": client_id,
                    "date_from": date_from,
                    "date_to": date_to,
                    "limit": 100,
                    "page": page,
                })
            except Exception as e:
                logger.error(f"Shipments fetch failed (page {page}): {e}")
                break

            data = resp.get("data", {})
            shipments = data.get("shipments", [])
            if not shipments:
                break

            for s in shipments:
                order = s.get("order", {}) or {}
                order_number = order.get("order_number", "")
                shipment_cost = float(s.get("shipment_cost", 0) or 0)
                parcels = []
                for p in s.get("shipment_parcels", []):
                    parcels.append({
                        "box": p.get("box", ""),
                        "tracking_number": p.get("tracking_number", ""),
                    })
                all_shipments.append({
                    "order_number": order_number,
                    "shipment_cost": shipment_cost,
                    "parcels": parcels,
                })

            # Check for next page
            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

        logger.info(f"Fetched {len(all_shipments)} shipments for client {client_id}")
        return all_shipments
