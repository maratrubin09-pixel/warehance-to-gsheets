"""
Warehance WMS API Client

API Reference: https://developer.warehance.com
Endpoints used:
  GET /auth-check
  GET /bills               — list bills (filterable by client)
  GET /bills/{id}/line-items — bill charge details (equivalent to CSV export)
"""

import csv
import logging
from datetime import datetime, timedelta
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class WarehanceClient:

    def __init__(self, api_key: str, base_url: str = "https://api.warehance.com/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def check_auth(self) -> bool:
        try:
            self._get("/auth-check")
            logger.info("Warehance auth OK")
            return True
        except Exception as e:
            logger.error(f"Auth failed: {e}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
    def _get(self, endpoint: str, params: dict | None = None) -> Any:
        url = f"{self.base_url}{endpoint}"
        logger.debug(f"GET {url} params={params}")
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _get_paginated(self, endpoint: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        params.setdefault("page", 1)
        params.setdefault("per_page", 100)
        all_items = []

        while True:
            data = self._get(endpoint, params)
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            all_items.extend(items)

            if isinstance(data, dict):
                total_pages = data.get("last_page") or data.get("total_pages", 1)
                if params["page"] >= total_pages:
                    break
            if len(items) < params["per_page"]:
                break
            params["page"] += 1

        return all_items

    # ------------------------------------------------------------------
    # Bills
    # ------------------------------------------------------------------

    def get_bills(
        self,
        client_id: str | None = None,
        since: datetime | None = None,
    ) -> list[dict]:
        """Fetch bills, optionally filtered by client and date."""
        params = {}
        if client_id:
            params["client_id"] = client_id
        if since:
            params["created_at_min"] = since.strftime("%Y-%m-%dT%H:%M:%S")
        bills = self._get_paginated("/bills", params)
        logger.info(f"Fetched {len(bills)} bills" +
                     (f" for client {client_id}" if client_id else ""))
        return bills

    def get_bill_line_items(self, bill_id: str | int) -> list[dict]:
        """Fetch line items (charges) for a specific bill."""
        items = self._get_paginated(f"/bills/{bill_id}/line-items")
        logger.info(f"Fetched {len(items)} line items for bill {bill_id}")
        return items

    def get_bill_details_for_client(
        self,
        client_id: str,
        days_back: int = 1,
    ) -> list[dict]:
        """
        Fetch all bill-detail rows for a client over the given period.
        This replicates the manual CSV export from Warehance.
        """
        since = datetime.utcnow() - timedelta(days=days_back)
        bills = self.get_bills(client_id=client_id, since=since)

        all_items = []
        for bill in bills:
            bid = bill.get("id")
            if not bid:
                continue
            try:
                items = self.get_bill_line_items(bid)
                all_items.extend(items)
            except Exception as e:
                logger.error(f"Failed to get line items for bill {bid}: {e}")

        logger.info(
            f"Client {client_id}: {len(all_items)} bill-detail rows "
            f"from {len(bills)} bills (last {days_back} day(s))"
        )
        return all_items

    # ------------------------------------------------------------------
    # CSV fallback
    # ------------------------------------------------------------------

    @staticmethod
    def load_from_csv(filepath: str) -> list[dict]:
        with open(filepath, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        logger.info(f"Loaded {len(rows)} rows from CSV: {filepath}")
        return rows
