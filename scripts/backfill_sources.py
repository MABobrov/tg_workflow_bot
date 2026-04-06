# -*- coding: utf-8 -*-
"""Backfill source (Источник) for existing leads.

Matches leads by normalized phone number, writes source to both:
1. Local DB (leads.source column)
2. amoCRM custom field Источник (field_id=1063391)
"""
import asyncio
import sys
import os
import re
sys.path.insert(0, "/app")

from app.config import load_config
from app.db import Database
from app.integrations.amocrm import AmoCRMService, AmoConfig

SOURCE_FIELD_ID = 1063391

# Data from user's list: (phone_last10, source)
# Format: phone -> source
LEAD_SOURCES = [
    ("9623614662", "Авито"),
    ("9647801119", "Авито"),
    ("9030037642", "Авито 2"),
    ("9150100800", "Авито"),
    ("9267088872", "Авито"),
    ("9991778621", "Авито"),
    ("9175114010", "сосед Паша"),
    ("9851505888", "Авито зв"),
    ("9172254555", "Сайт"),
    ("9201852065", "Авито зв"),
    ("9167123085", "от АП"),
    ("9671300505", "Авито 2 зв"),
    ("9661996869", "Авито зв"),
    ("9261414202", "Сайт"),
    ("9688825825", "Сайт"),
    ("9611874668", "Авито 2 зв"),
    ("9253017022", "Сайт"),
    ("9254275554", "Авито 2"),
    ("9636034984", "Авито"),
    ("9128050338", "Авито"),
    ("9017344104", "Авито"),
    ("9265881984", "Сайт"),
    # 66969619413 - international, skip or handle separately
    ("9035838313", "Авито"),
    ("9912111146", "Сайт"),
    ("9054252612", "Авито"),
    ("9049612751", "Сайт"),
    ("9162281519", "Сайт"),
    ("9104064825", "от АП"),
    ("9818491080", "Авито"),
    ("9672499369", "Сайт"),
    ("9161867216", "Авито зв"),
    ("9256402544", "Авито"),
    ("9263843654", "Авито зв"),
    ("9296057473", "от АП"),
    ("9184442311", "Авито"),
    ("9031004937", "тон"),
    ("9672647791", "тон"),
    ("9852009983", "тон"),
    ("9898100310", "от САБ"),
    ("9680948339", "Сайт"),
    ("9060712706", "Авито"),
    ("9269395495", "Сайт"),
    ("9965599803", "Сайт"),
    ("9169269154", "Авито зв"),
    ("9132159000", "от КВ"),
    ("9825772389", "Комус"),
    ("9163509526", "от САБ"),
    ("9258674585", "от КВ"),
    ("9162293139", "Авито зв"),
    ("9161611744", "тон"),
    ("9625497749", "тон"),
    ("9992194569", "тон"),
    ("9099178546", "тон"),
    ("9102413111", "тон"),
    ("9032670655", "тон"),
    ("9033244287", "тон"),
    ("9250116802", "Сайт"),
    ("9652856705", "Авито зв"),
    ("9162228888", "от КВ"),
    ("9106109659", "тон"),
    ("9011838352", "тон"),
    ("9629500550", "тон"),
    ("9162845213", "тон"),
    ("9850530808", "тон"),
    ("9254776843", "Авито"),
    ("9857611372", "Сайт"),
    ("9689490104", "Сайт"),
    ("9162102889", "тон"),
    ("9588102972", "тон"),
    ("9268925482", "Авито"),
    ("9167413554", "от Петралюма"),
    ("9162855293", "тон"),
    ("9661554995", "тон"),
    ("9255702601", "от КИА"),
    ("9851943751", "тон"),
    ("9917715083", "Авито"),
    ("9037654020", "тон"),
    ("9611111390", "тон"),
    ("9039609760", "Авито зв"),
    ("9857664253", "от САБ"),
    ("9060404077", "Авито зв"),
    ("9997678787", "от Ромы"),
]


def _normalize_phone(phone: str | None) -> str:
    """Normalize phone to last 10 digits."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    return digits[-10:] if len(digits) >= 10 else digits


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

    # Build phone -> source mapping
    source_map = {}
    for phone_suffix, source in LEAD_SOURCES:
        source_map[phone_suffix] = source

    # Get all leads from DB
    all_leads = await db.list_all_amo_leads(limit=10000)
    updated = 0
    amo_updated = 0

    for lead in all_leads:
        phone_norm = _normalize_phone(lead.get("phone"))
        if not phone_norm or phone_norm not in source_map:
            continue

        source = source_map[phone_norm]
        amo_id = lead["amo_lead_id"]

        # Update local DB
        if lead.get("source") != source:
            await db.update_lead_source(amo_id, source)
            updated += 1
            print("DB updated: amo_id=%s phone=%s source=%s" % (amo_id, phone_norm, source))

        # Update amoCRM custom field
        try:
            await amocrm.update_lead(amo_id, {
                "custom_fields_values": [
                    {
                        "field_id": SOURCE_FIELD_ID,
                        "values": [{"value": source}],
                    }
                ]
            })
            amo_updated += 1
            print("  amoCRM updated: amo_id=%s" % amo_id)
        except Exception as e:
            print("  amoCRM FAILED for amo_id=%s: %s" % (amo_id, e))

        # Rate limit
        await asyncio.sleep(0.3)

    print("\nDone! DB updated: %d, amoCRM updated: %d" % (updated, amo_updated))

    await amocrm.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
