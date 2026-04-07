# -*- coding: utf-8 -*-
"""Import ALL leads from amoCRM created since 30.01.2025, including closed ones."""
import asyncio
import sys
import os
import json
sys.path.insert(0, "/app")

from app.config import load_config
from app.db import Database
from app.integrations.amocrm import AmoCRMService, AmoConfig

SOURCE_FIELD_ID = 1063391
# 30.01.2025 00:00:00 UTC
SINCE_TS = 1738195200


def _extract_tags(lead):
    tags = lead.get("_embedded", {}).get("tags") or []
    if not tags:
        return None
    names = [t.get("name") for t in tags if t.get("name")]
    return json.dumps(names, ensure_ascii=False) if names else None


def _extract_source(lead):
    for cf in lead.get("custom_fields_values") or []:
        if cf.get("field_id") == SOURCE_FIELD_ID:
            values = cf.get("values") or []
            if values:
                return str(values[0].get("value", ""))
    return None


async def _extract_contact_info(amocrm, lead):
    contacts = lead.get("_embedded", {}).get("contacts") or []
    if not contacts:
        return None, None
    contact_entry = next((c for c in contacts if c.get("is_main")), contacts[0])
    contact_id = contact_entry.get("id")
    if not contact_id:
        return None, None
    try:
        contact = await amocrm.get_contact(int(contact_id))
        phone = AmoCRMService.extract_phone(contact)
        name = AmoCRMService.extract_contact_name(contact)
        return phone, name
    except Exception:
        return None, None


async def main():
    cfg = load_config()
    db = Database(cfg.db_path)
    await db.connect()

    amo_cfg = AmoConfig(
        enabled=cfg.amocrm_enabled, base_url=cfg.amocrm_base_url,
        client_id=cfg.amocrm_client_id, client_secret=cfg.amocrm_client_secret,
        redirect_uri=cfg.amocrm_redirect_uri, access_token=cfg.amocrm_access_token,
        refresh_token=cfg.amocrm_refresh_token,
    )
    amocrm = AmoCRMService(amo_cfg, db)
    await amocrm.start()

    cur = await db.conn.execute("SELECT amo_lead_id FROM leads")
    rows = await cur.fetchall()
    existing_ids = {int(r["amo_lead_id"]) for r in rows}
    print("Existing in DB: %d" % len(existing_ids))

    page = 1
    total_fetched = 0
    imported = 0

    while True:
        try:
            leads = await amocrm.list_leads(
                page=page, limit=50,
                filter_={"created_at": {"from": SINCE_TS}},
                order={"created_at": "asc"},
                with_=["contacts"],
            )
        except Exception as e:
            print("Error page %d: %s" % (page, e))
            break
        if not leads:
            break
        total_fetched += len(leads)

        for lead in leads:
            amo_id = int(lead["id"])
            if amo_id in existing_ids:
                continue

            # NO status filter — import ALL leads including closed
            status_id = lead.get("status_id")
            phone, contact_name = await _extract_contact_info(amocrm, lead)
            tags_json = _extract_tags(lead)
            source = _extract_source(lead)

            try:
                await db.create_lead(
                    amo_lead_id=amo_id,
                    name=lead.get("name"),
                    price=lead.get("price"),
                    pipeline_id=lead.get("pipeline_id"),
                    status_id=status_id,
                    responsible_user_id=lead.get("responsible_user_id"),
                    phone=phone, contact_name=contact_name,
                    tags_json=tags_json, source=source,
                )
                imported += 1
            except Exception as e:
                print("Failed amo_id=%s: %s" % (amo_id, e))
            await asyncio.sleep(0.25)

        print("Page %d: %d leads (imported so far: %d)" % (page, len(leads), imported))
        if len(leads) < 50:
            break
        page += 1
        await asyncio.sleep(0.5)

    print("\nTotal fetched: %d, newly imported: %d" % (total_fetched, imported))
    print("New DB total: %d" % (len(existing_ids) + imported))
    await amocrm.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
