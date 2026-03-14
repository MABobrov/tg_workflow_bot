#!/usr/bin/env python3
"""Discover all sheets and their headers in both spreadsheets."""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SA_FILE_PATHS = [
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
    "./secrets/google-sa.json",
    str(Path.home() / "Desktop" / "Меню бота" / "секреты" / "secrets" / "google-sa.json"),
]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

SPREADSHEETS = {
    "Отдел продаж (source)": "1i6fZi8TLC8ghtuRLZYkHt-3UsfoJ50Ng4EJuMMQXjN4",
    "Bot Organizer": "14hrBVQSrme8t-b01nOoomrh43n0AsB1xhFeSLy6VNaU",
}

def get_credentials():
    for path in SA_FILE_PATHS:
        if path and os.path.isfile(path):
            return Credentials.from_service_account_file(path, scopes=SCOPES)
    raise FileNotFoundError("SA JSON not found")

def col_letter(i):
    result = ""
    while True:
        result = chr(ord("A") + i % 26) + result
        i = i // 26 - 1
        if i < 0:
            break
    return result

def main():
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheets_api = service.spreadsheets()

    for name, sid in SPREADSHEETS.items():
        print(f"\n{'='*60}")
        print(f"SPREADSHEET: {name}")
        print(f"ID: {sid}")
        print(f"{'='*60}")

        try:
            sp_info = sheets_api.get(spreadsheetId=sid).execute()
            print(f"Title: {sp_info.get('properties', {}).get('title', '?')}")
        except Exception as e:
            print(f"ERROR accessing: {e}")
            continue

        for s in sp_info.get("sheets", []):
            props = s["properties"]
            title = props["title"]
            sheet_id = props["sheetId"]
            row_count = props.get("gridProperties", {}).get("rowCount", "?")
            col_count = props.get("gridProperties", {}).get("columnCount", "?")
            print(f"\n  --- Sheet: '{title}' (id={sheet_id}, rows={row_count}, cols={col_count}) ---")

            # Read first 8 rows
            try:
                rng = f"'{title}'!A1:BZ8"
                result = sheets_api.values().get(
                    spreadsheetId=sid, range=rng,
                ).execute()
                rows = result.get("values", [])
                for r_idx, row in enumerate(rows):
                    safe_vals = []
                    for c_idx, v in enumerate(row):
                        sv = str(v).encode("ascii", "replace").decode("ascii")[:40]
                        safe_vals.append(f"{col_letter(c_idx)}:{sv}")
                    print(f"    Row {r_idx+1}: {' | '.join(safe_vals[:20])}")
                    if len(row) > 20:
                        print(f"           ... +{len(row)-20} more cols")
            except Exception as e:
                print(f"    ERROR reading: {e}")

if __name__ == "__main__":
    main()
