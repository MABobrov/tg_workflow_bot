from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import timedelta
from typing import Any

from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import LeadAssignCb, LeadCb
from ..db import Database
from ..enums import Role
from ..integrations.amocrm import AmoCRMService
from ..services.notifier import Notifier
from ..utils import to_iso, utcnow

log = logging.getLogger(__name__)

ESCALATION_MINUTES = 15
EXCLUDED_STATUS_IDS = {143}  # закрыт не реализован — не импортируем
TERMINAL_STATUS_IDS = {142, 143}  # успешно / закрыт — статус не обновляем
STATUS_SYNC_INTERVAL = 1800  # 30 minutes


def _fmt_lead_card(lead_data: dict[str, Any]) -> str:
    """Format a human-readable lead card for Telegram."""
    name = lead_data.get("name") or "—"
    price = lead_data.get("price")
    price_str = f"{int(price):,} ₽".replace(",", " ") if price else "—"
    amo_id = lead_data.get("amo_lead_id") or lead_data.get("id", "?")
    return (
        f"🔔 <b>Новый лид из amoCRM</b>\n\n"
        f"📝 Название: {name}\n"
        f"💰 Бюджет: {price_str}\n"
        f"🆔 amoCRM ID: <code>{amo_id}</code>\n"
    )


def _claim_kb(lead_id: int) -> Any:
    """Inline keyboard with a single 'Claim' button for managers."""
    b = InlineKeyboardBuilder()
    b.button(
        text="🙋 Взять лид в работу",
        callback_data=LeadCb(lead_id=lead_id, action="claim").pack(),
    )
    b.adjust(1)
    return b.as_markup()


async def _build_assign_kb(db: Database, lead_id: int) -> Any:
    """Inline keyboard for RP/GD to assign lead to a specific manager."""
    manager_roles = [Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN]
    seen_ids: set[int] = set()
    managers = []
    for role in manager_roles:
        for user in await db.find_users_by_role(role, limit=20):
            if user.telegram_id in seen_ids:
                continue
            seen_ids.add(user.telegram_id)
            managers.append(user)
    b = InlineKeyboardBuilder()
    for m in managers:
        label = f"@{m.username}" if m.username else (m.full_name or str(m.telegram_id))
        b.button(
            text=f"👤 {label}",
            callback_data=LeadAssignCb(lead_id=lead_id, manager_id=m.telegram_id).pack(),
        )
    b.adjust(1)
    return b.as_markup()


async def _publish_lead(
    db: Database,
    notifier: Notifier,
    lead_row: dict[str, Any],
) -> None:
    """Send lead card to work chat with claim button."""
    if not notifier.workchat_events_enabled:
        return

    text = _fmt_lead_card(lead_row)
    kb = _claim_kb(int(lead_row["id"]))

    if not notifier.work_chat_id:
        log.warning("Cannot publish lead %s: no work_chat_id", lead_row["id"])
        return

    try:
        msg = await notifier.bot.send_message(
            chat_id=int(notifier.work_chat_id),
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        await db.set_lead_workchat_msg(int(lead_row["id"]), msg.message_id)
        log.info("Published lead %s (amo=%s) to work chat, msg_id=%s",
                 lead_row["id"], lead_row["amo_lead_id"], msg.message_id)
    except Exception:
        log.exception("Failed to publish lead %s to work chat", lead_row["id"])


async def _escalate_lead(
    db: Database,
    notifier: Notifier,
    lead_row: dict[str, Any],
) -> None:
    """Notify RP and GD about unclaimed lead after 15 minutes."""
    text = (
        f"⚠️ <b>Лид не взят более {ESCALATION_MINUTES} мин!</b>\n\n"
        f"{_fmt_lead_card(lead_row)}\n"
        f"Назначьте менеджера вручную:"
    )
    kb = await _build_assign_kb(db, int(lead_row["id"]))

    # Notify RP users
    rp_users = await db.find_users_by_role(Role.RP, limit=10)
    for u in rp_users:
        await notifier.safe_send(u.telegram_id, text, reply_markup=kb)

    # Notify GD users
    gd_users = await db.find_users_by_role(Role.GD, limit=10)
    for u in gd_users:
        await notifier.safe_send(u.telegram_id, text, reply_markup=kb)

    await db.set_lead_escalated(int(lead_row["id"]))
    log.info("Escalated unclaimed lead %s (amo=%s)", lead_row["id"], lead_row["amo_lead_id"])


async def _extract_contact_info(
    amocrm: AmoCRMService,
    lead: dict[str, Any],
) -> tuple[str | None, str | None]:
    """Fetch phone and contact name from amoCRM lead's embedded contacts."""
    contacts = lead.get("_embedded", {}).get("contacts") or []
    if not contacts:
        return None, None
    # prefer main contact
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
        log.exception("Failed to fetch contact %s", contact_id)
        return None, None


def _extract_tags(lead: dict[str, Any]) -> str | None:
    """Extract tags from lead as JSON array of names."""
    tags = lead.get("_embedded", {}).get("tags") or []
    if not tags:
        return None
    names = [t.get("name") for t in tags if t.get("name")]
    return json.dumps(names, ensure_ascii=False) if names else None


AMOCRM_SOURCE_FIELD_ID = 1063391  # custom field "Источник"


def _extract_source(lead: dict[str, Any]) -> str | None:
    """Extract source (Источник) from amoCRM custom field."""
    for cf in lead.get("custom_fields_values") or []:
        if cf.get("field_id") == AMOCRM_SOURCE_FIELD_ID:
            values = cf.get("values") or []
            if values:
                return str(values[0].get("value", ""))
    return None


async def _poll_new_leads(
    db: Database,
    amocrm: AmoCRMService,
    notifier: Notifier,
    last_ts: int,
) -> int:
    """Fetch new leads from amoCRM created after last_ts (unix timestamp).
    Returns the new high-watermark timestamp.
    """
    new_ts = last_ts
    try:
        leads = await amocrm.list_leads(
            limit=50,
            filter_={"created_at": {"from": last_ts}},
            order={"created_at": "asc"},
            with_=["contacts"],
        )
    except Exception:
        log.exception("Failed to poll amoCRM leads")
        return new_ts

    for lead in leads:
        amo_id = int(lead["id"])
        created_at = int(lead.get("created_at", 0))
        if created_at > new_ts:
            new_ts = created_at

        # filter: skip excluded statuses
        status_id = lead.get("status_id")
        if status_id and int(status_id) in EXCLUDED_STATUS_IDS:
            continue

        # skip if already known
        if await db.lead_exists(amo_id):
            continue

        # extract contact info (phone, name) from amoCRM
        phone, contact_name = await _extract_contact_info(amocrm, lead)

        # extract tags and source
        tags_json = _extract_tags(lead)
        source = _extract_source(lead)

        # store in DB
        lead_row = await db.create_lead(
            amo_lead_id=amo_id,
            name=lead.get("name"),
            price=lead.get("price"),
            pipeline_id=lead.get("pipeline_id"),
            status_id=status_id,
            responsible_user_id=lead.get("responsible_user_id"),
            phone=phone,
            contact_name=contact_name,
            tags_json=tags_json,
            source=source,
        )

        # publish to work chat
        await _publish_lead(db, notifier, lead_row)

        # rate-limit: avoid hitting amoCRM API limits
        await asyncio.sleep(0.25)

    return new_ts


async def _check_escalations(db: Database, notifier: Notifier) -> None:
    """Check for unclaimed leads older than ESCALATION_MINUTES and escalate."""
    cutoff = utcnow() - timedelta(minutes=ESCALATION_MINUTES)
    cutoff_iso = to_iso(cutoff)
    leads = await db.list_unescalated_leads(cutoff_iso)
    for lead_row in leads:
        await _escalate_lead(db, notifier, lead_row)


async def _sync_lead_statuses(
    db: Database,
    amocrm: AmoCRMService,
    last_status_sync_ts: int,
) -> int:
    """Fetch leads updated since last_status_sync_ts, update status_id in DB.

    Returns the new watermark timestamp.
    """
    new_ts = last_status_sync_ts
    page = 1

    while True:
        try:
            leads = await amocrm.list_leads(
                page=page,
                limit=50,
                filter_={"updated_at": {"from": last_status_sync_ts}},
                order={"updated_at": "asc"},
            )
        except Exception:
            log.exception("Failed to fetch leads for status sync (page %d)", page)
            break

        if not leads:
            break

        for lead in leads:
            amo_id = int(lead["id"])
            updated_at = int(lead.get("updated_at", 0))
            if updated_at > new_ts:
                new_ts = updated_at

            remote_status = lead.get("status_id")
            if remote_status is None:
                continue

            local_lead = await db.get_lead_by_amo_id(amo_id)
            if local_lead is None:
                continue

            # Update status if changed
            local_status = local_lead.get("status_id")
            if local_status != remote_status:
                if not (local_status and int(local_status) in TERMINAL_STATUS_IDS):
                    await db.update_lead_status(amo_id, int(remote_status))
                    log.info("Lead %s status updated: %s -> %s", amo_id, local_status, remote_status)

            # Update source if changed
            remote_source = _extract_source(lead)
            local_source = local_lead.get("source")
            if remote_source and remote_source != local_source:
                await db.update_lead_source(amo_id, remote_source)
                log.info("Lead %s source updated: %s -> %s", amo_id, local_source, remote_source)

        if len(leads) < 50:
            break
        page += 1
        await asyncio.sleep(0.5)

    return new_ts


async def lead_poller_loop(
    db: Database,
    amocrm: AmoCRMService,
    notifier: Notifier,
    interval_seconds: int = 30,
) -> None:
    """Background loop: polls amoCRM for new leads and handles escalations.

    Runs every `interval_seconds` seconds.
    Status sync runs every STATUS_SYNC_INTERVAL seconds (~30 min).
    """
    log.info("Lead poller started (interval=%ss, escalation=%smin, status_sync=%ss)",
             interval_seconds, ESCALATION_MINUTES, STATUS_SYNC_INTERVAL)

    # Initialize watermark: start from ~1 hour ago to avoid missing recent leads
    last_ts = int(time.time()) - 3600

    # Try to restore watermark from DB
    saved = await db.get_setting("lead_poller_last_ts")
    if saved:
        try:
            last_ts = int(saved)
        except ValueError:
            pass

    # Initialize status sync watermark
    last_status_ts = int(time.time()) - STATUS_SYNC_INTERVAL
    saved_status = await db.get_setting("lead_status_sync_last_ts")
    if saved_status:
        try:
            last_status_ts = int(saved_status)
        except ValueError:
            pass

    last_status_check = 0.0  # monotonic time of last status sync

    while True:
        try:
            new_ts = await _poll_new_leads(db, amocrm, notifier, last_ts)
            if new_ts > last_ts:
                last_ts = new_ts
                await db.set_setting("lead_poller_last_ts", str(last_ts))

            await _check_escalations(db, notifier)

            # --- Sync lead statuses every STATUS_SYNC_INTERVAL ---
            now_mono = time.monotonic()
            if now_mono - last_status_check >= STATUS_SYNC_INTERVAL:
                new_status_ts = await _sync_lead_statuses(db, amocrm, last_status_ts)
                if new_status_ts > last_status_ts:
                    last_status_ts = new_status_ts
                    await db.set_setting("lead_status_sync_last_ts", str(last_status_ts))
                last_status_check = now_mono
                log.info("Lead status sync complete, watermark=%s", last_status_ts)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Lead poller iteration error")

        await asyncio.sleep(interval_seconds)
