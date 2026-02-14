"""
Google Drive Backup — saves raw bill data as CSV to Drive.

Structure:
  Warehance Backups/
    └── 2026-02-03/
          ├── 001_Sarmali_Inc.csv
          └── 279_Amzammia.csv
"""

import csv
import io
import logging
import os
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
]


class GDriveBackup:
    def __init__(self, service_account_file: str):
        creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
        self.service = build("drive", "v3", credentials=creds)
        self._folder_cache = {}

    def _find_or_create_folder(self, name: str, parent_id: str = None) -> str:
        cache_key = f"{parent_id}:{name}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            q += f" and '{parent_id}' in parents"

        results = self.service.files().list(q=q, spaces="drive", fields="files(id, name)").execute()
        files = results.get("files", [])
        if files:
            fid = files[0]["id"]
        else:
            meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
            if parent_id:
                meta["parents"] = [parent_id]
            f = self.service.files().create(body=meta, fields="id").execute()
            fid = f["id"]
            logger.info(f"Created Drive folder: {name}")

        self._folder_cache[cache_key] = fid
        return fid

    def backup_rows(self, client_number: str, client_name: str,
                    rows: list[dict], date_str: str = None,
                    root_folder_name: str = "Warehance Backups"):
        if not rows:
            return

        date_str = date_str or datetime.utcnow().strftime("%Y-%m-%d")
        safe_name = client_name.replace(" ", "_").replace("/", "_")
        filename = f"{client_number}_{safe_name}.csv"

        root_id = self._find_or_create_folder(root_folder_name)
        date_id = self._find_or_create_folder(date_str, parent_id=root_id)

        buf = io.StringIO()
        if rows:
            writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        media = MediaInMemoryUpload(buf.getvalue().encode("utf-8"), mimetype="text/csv")
        meta = {"name": filename, "parents": [date_id]}
        self.service.files().create(body=meta, media_body=media, fields="id").execute()
        logger.info(f"Backed up {filename} to Drive/{root_folder_name}/{date_str}/")
