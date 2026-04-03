"""One-time script: backfill contact_name, phone, tags for existing leads from amoCRM API."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "/root/tg_workflow_bot/data/bot.sqlite3")
AMO_BASE = os.getenv("AMOCRM_BASE_URL", "").rstrip("/")
AMO_TOKEN = os.getenv("AMOCRM_ACCESS_TOKEN", "")


async def amo_get(session: aiohttp.ClientSession, path: str, params=None):
    headers = {"Authorization": f"Bearer {AMO_TOKEN}"}
    async with session.get(f"{AMO_BASE}{path}", headers=headers, params=params) as resp:
        if resp.status == 204:
            return None
        text = await resp.text()
        if resp.status >= 400:
            log.error("API %s -> %s: %s", path, resp.status, text[:200])
            return None
        return json.loads(text)


async def get_contact_phone_name(session: aiohttp.ClientSession, contact_id: int):
    data = await amo_get(session, f"/api/v4/contacts/{contact_id}")
    if not data:
        return None, None
    name = data.get("name")
    phone = None
    for field in data.get("custom_fields_values") or []:
        if field.get("field_code") == "PHONE":
            vals = field.get("values") or []
            if vals:
                phone = str(vals[0].get("value", ""))
                break
    return phone, name


async def main():
    if not AMO_BASE or not AMO_TOKEN:
        log.error("Set AMOCRM_BASE_URL and AMOCRM_ACCESS_TOKEN env vars")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    rows = conn.execute(
        "SELECT id, amo_lead_id FROM leads "
        "WHERE (contact_name IS NULL OR contact_name = '') "
        "ORDER BY id"
    ).fetchall()
    log.info("Found %d leads to backfill", len(rows))

    updated = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for i, row in enumerate(rows):
            amo_id = row["amo_lead_id"]
            db_id = row["id"]

            # Fetch lead with contacts and tags
            data = await amo_get(
                session, f"/api/v4/leads/{amo_id}",
                params=[("with", "contacts")],
            )
            if not data:
                log.warning("[%d/%d] amo_lead_id=%s — not found", i + 1, len(rows), amo_id)
                await asyncio.sleep(0.3)
                continue

            # Tags
            tags = data.get("_embedded", {}).get("tags") or []
            tag_names = [t.get("name") for t in tags if t.get("name")]
            tags_json = json.dumps(tag_names, ensure_ascii=False) if tag_names else None

            # Contact
            phone, contact_name = None, None
            contacts = data.get("_embedded", {}).get("contacts") or []
            if contacts:
                c = next((x for x in contacts if x.get("is_main")), contacts[0])
                cid = c.get("id")
                if cid:
                    phone, contact_name = await get_contact_phone_name(session, int(cid))
                    await asyncio.sleep(0.25)

            # Update DB (sync sqlite3 — WAL mode allows concurrent reads)
            for attempt in range(3):
                try:
                    conn.execute(
                        "UPDATE leads SET contact_name=?, phone=?, tags_json=? WHERE id=?",
                        (contact_name, phone, tags_json, db_id),
                    )
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < 2:
                        log.warning("DB locked, retry %d...", attempt + 1)
                        await asyncio.sleep(2)
                    else:
                        raise

            updated += 1
            if updated % 20 == 0:
                log.info("[%d/%d] %d updated...", i + 1, len(rows), updated)

            await asyncio.sleep(0.25)

    conn.close()
    log.info("Done. Updated %d / %d leads", updated, len(rows))


if __name__ == "__main__":
    asyncio.run(main())
