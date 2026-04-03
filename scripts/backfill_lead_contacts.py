"""One-time script: backfill contact_name, phone, tags for existing leads from amoCRM API."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- config from env ---
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
            log.error("API %s → %s: %s", path, resp.status, text[:200])
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

    import aiosqlite
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=30000")

    cur = await db.execute(
        "SELECT id, amo_lead_id FROM leads "
        "WHERE (contact_name IS NULL OR contact_name = '') "
        "ORDER BY id"
    )
    rows = await cur.fetchall()
    log.info("Found %d leads to backfill", len(rows))

    updated = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for i, row in enumerate(rows):
            amo_id = row["amo_lead_id"]
            db_id = row["id"]

            # Fetch lead with contacts and tags
            data = await amo_get(
                session, "/api/v4/leads/" + str(amo_id),
                params=[("with", "contacts")],
            )
            if not data:
                log.warning("[%d/%d] amo_lead_id=%s — API returned nothing", i+1, len(rows), amo_id)
                await asyncio.sleep(0.3)
                continue

            # Tags
            tags = data.get("_embedded", {}).get("tags") or []
            tag_names = [t.get("name") for t in tags if t.get("name")]
            tags_json = json.dumps(tag_names, ensure_ascii=False) if tag_names else None

            # Contact: phone + name
            phone, contact_name = None, None
            contacts = data.get("_embedded", {}).get("contacts") or []
            if contacts:
                c = next((x for x in contacts if x.get("is_main")), contacts[0])
                cid = c.get("id")
                if cid:
                    phone, contact_name = await get_contact_phone_name(session, int(cid))
                    await asyncio.sleep(0.25)

            # Update DB
            await db.execute(
                "UPDATE leads SET contact_name=?, phone=?, tags_json=? WHERE id=?",
                (contact_name, phone, tags_json, db_id),
            )
            updated += 1

            if updated % 20 == 0:
                await db.commit()
                log.info("[%d/%d] %d updated so far...", i+1, len(rows), updated)

            await asyncio.sleep(0.25)  # rate limit

    await db.commit()
    await db.close()
    log.info("Done. Updated %d / %d leads", updated, len(rows))


if __name__ == "__main__":
    asyncio.run(main())
