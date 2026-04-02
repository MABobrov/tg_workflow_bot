#!/usr/bin/env python3
"""One-time import of historical amoCRM leads into the bot's leads table.

Usage (inside Docker container):
    python3 scripts/import_amo_leads.py [path_to_json]

Default JSON path: /tmp/amo_leads_2026.json
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("data/bot.sqlite3")
DEFAULT_JSON = Path("/tmp/amo_leads_2026.json")


def ts_to_iso(unix_ts: int) -> str:
    """Convert unix timestamp to ISO 8601 UTC string."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def main() -> None:
    json_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_JSON

    if not json_path.exists():
        print(f"ERROR: JSON file not found: {json_path}")
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"ERROR: Database not found: {DB_PATH}")
        sys.exit(1)

    with open(json_path, encoding="utf-8") as f:
        leads = json.load(f)

    print(f"Loaded {len(leads)} leads from {json_path}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    now_iso = datetime.now(timezone.utc).isoformat()

    inserted = 0
    skipped = 0
    errors = 0
    max_ts = 0

    for lead in leads:
        amo_id = int(lead["id"])
        created_ts = int(lead.get("created_at", 0))

        if created_ts > max_ts:
            max_ts = created_ts

        # Check if already exists
        existing = conn.execute(
            "SELECT 1 FROM leads WHERE amo_lead_id = ?", (amo_id,)
        ).fetchone()
        if existing:
            skipped += 1
            continue

        try:
            conn.execute(
                """
                INSERT INTO leads(
                    amo_lead_id, name, price, pipeline_id, status_id,
                    responsible_user_id, claimed_by, claimed_at,
                    escalated, workchat_message_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 1, NULL, ?, ?)
                """,
                (
                    amo_id,
                    lead.get("name"),
                    lead.get("price"),
                    lead.get("pipeline_id"),
                    lead.get("status_id"),
                    lead.get("responsible_user_id"),
                    ts_to_iso(created_ts) if created_ts else now_iso,
                    now_iso,
                ),
            )
            inserted += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR inserting amo_lead_id={amo_id}: {e}")

    conn.commit()

    # Update lead_poller watermark so poller doesn't re-fetch these
    if max_ts:
        conn.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)",
            ("lead_poller_last_ts", str(max_ts)),
        )
        conn.commit()
        print(f"Updated lead_poller_last_ts = {max_ts} ({ts_to_iso(max_ts)})")

    # Final count
    total = conn.execute("SELECT COUNT(*) as c FROM leads").fetchone()["c"]
    conn.close()

    print(f"\nResults: inserted={inserted}, skipped={skipped}, errors={errors}")
    print(f"Total leads in DB: {total}")


if __name__ == "__main__":
    main()
