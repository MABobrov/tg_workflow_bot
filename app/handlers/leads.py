"""Handlers for lead claiming (managers) and lead assignment (RP/GD).

Inline buttons are attached to messages published by the lead_poller service.
"""

from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery

from ..callbacks import LeadAssignCb, LeadCb
from ..config import Config
from ..db import Database
from ..enums import MANAGER_ROLES, Role
from ..integrations.amocrm import AmoCRMService
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..utils import parse_roles

log = logging.getLogger(__name__)
router = Router()

LEAD_MANAGER_ROLES = {Role.MANAGER, *MANAGER_ROLES}


# ==================== MANAGER: CLAIM LEAD ====================

@router.callback_query(LeadCb.filter(F.action == "claim"))
async def lead_claim(
    cb: CallbackQuery,
    callback_data: LeadCb,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """A manager clicks '🙋 Взять лид в работу' in work chat."""
    u = cb.from_user
    if not u:
        return

    # Check that user is a manager
    user_row = await db.get_user_optional(u.id)
    if not user_row:
        await cb.answer("Вы не зарегистрированы в боте.", show_alert=True)
        return
    if not user_row.is_active:
        await cb.answer("Ваш доступ к боту заблокирован.", show_alert=True)
        return
    user_roles = parse_roles(user_row.role)
    if not any(role in LEAD_MANAGER_ROLES for role in user_roles):
        await cb.answer("Только менеджер может взять лид.", show_alert=True)
        return

    lead_id = callback_data.lead_id

    # Atomic claim
    success = await db.claim_lead(lead_id, u.id)
    if not success:
        # Already claimed — find out by whom
        try:
            lead_row = await db.get_lead(lead_id)
        except KeyError:
            await cb.answer("Лид не найден.", show_alert=True)
            return
        claimer_id = lead_row.get("claimed_by")
        if claimer_id:
            claimer = await db.get_user_optional(int(claimer_id))
            name = f"@{claimer.username}" if claimer and claimer.username else str(claimer_id)
            await cb.answer(f"Лид уже взят: {name}", show_alert=True)
        else:
            await cb.answer("Не удалось взять лид. Попробуйте ещё раз.", show_alert=True)
        return

    await cb.answer("✅ Лид взят в работу!")

    # Update message in work chat — remove claim button, show who claimed
    lead_row = await db.get_lead(lead_id)
    manager_label = f"@{u.username}" if u.username else (u.full_name or str(u.id))
    new_text = (
        f"✅ <b>Лид взят в работу</b>\n\n"
        f"📝 Название: {lead_row.get('name') or '—'}\n"
        f"🆔 amoCRM ID: <code>{lead_row.get('amo_lead_id')}</code>\n"
        f"👤 Менеджер: {manager_label}\n"
    )
    try:
        if cb.message:
            await cb.message.edit_text(new_text, reply_markup=None)
    except Exception:
        log.warning("Failed to edit lead claim message in work chat")

    # Assign in amoCRM
    if integrations.amocrm:
        amo_lead_id = lead_row.get("amo_lead_id")
        if amo_lead_id:
            # Look up the amoCRM user_id for this manager
            amo_user_id = await _resolve_amo_user_id(db, integrations.amocrm, u.id)
            if amo_user_id:
                try:
                    await integrations.amocrm.update_lead(
                        int(amo_lead_id),
                        {"responsible_user_id": amo_user_id},
                    )
                    log.info("Assigned amoCRM lead %s to amo_user %s", amo_lead_id, amo_user_id)
                except Exception:
                    log.exception("Failed to assign amoCRM lead %s", amo_lead_id)
            else:
                log.warning("No amoCRM user mapping for telegram_id=%s", u.id)

    # Notify the manager in private chat
    await notifier.safe_send(
        u.id,
        f"✅ Вы взяли лид <b>{lead_row.get('name') or '—'}</b> "
        f"(amoCRM #{lead_row.get('amo_lead_id')}).\n"
        f"Для создания проекта используйте «Проверить КП / Запросить документы».",
    )

    log.info("Lead %s claimed by manager %s (@%s)", lead_id, u.id, u.username)


# ==================== RP/GD: ASSIGN LEAD TO MANAGER ====================

@router.callback_query(LeadAssignCb.filter())
async def lead_assign(
    cb: CallbackQuery,
    callback_data: LeadAssignCb,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """RP or GD assigns an unclaimed lead to a specific manager."""
    u = cb.from_user
    if not u:
        return

    # Check that user is RP or GD or admin
    user_row = await db.get_user_optional(u.id)
    if not user_row:
        await cb.answer("Вы не зарегистрированы.", show_alert=True)
        return
    if not user_row.is_active:
        await cb.answer("Ваш доступ к боту заблокирован.", show_alert=True)
        return
    user_roles = parse_roles(user_row.role)
    is_authorized = (
        Role.RP in user_roles or
        Role.GD in user_roles or
        u.id in (config.admin_ids or set())
    )
    if not is_authorized:
        await cb.answer("Только РП или ГД могут назначать лиды.", show_alert=True)
        return

    lead_id = callback_data.lead_id
    manager_id = callback_data.manager_id

    # Force assign
    await db.assign_lead(lead_id, manager_id)
    await cb.answer("✅ Лид назначен!")

    lead_row = await db.get_lead(lead_id)
    manager = await db.get_user_optional(manager_id)
    manager_label = f"@{manager.username}" if manager and manager.username else str(manager_id)
    assigner_label = f"@{u.username}" if u.username else str(u.id)

    # Edit the escalation message to show assignment
    try:
        if cb.message:
            await cb.message.edit_text(
                f"✅ <b>Лид назначен</b>\n\n"
                f"📝 Название: {lead_row.get('name') or '—'}\n"
                f"🆔 amoCRM ID: <code>{lead_row.get('amo_lead_id')}</code>\n"
                f"👤 Менеджер: {manager_label}\n"
                f"🔧 Назначил: {assigner_label}",
                reply_markup=None,
            )
    except Exception:
        log.warning("Failed to edit lead assign message")

    # Also update the original work-chat message if it exists
    workchat_msg_id = lead_row.get("workchat_message_id")
    if workchat_msg_id and notifier.work_chat_id and notifier.workchat_events_enabled:
        try:
            await notifier.bot.edit_message_text(
                chat_id=int(notifier.work_chat_id),
                message_id=int(workchat_msg_id),
                text=(
                    f"✅ <b>Лид назначен</b>\n\n"
                    f"📝 Название: {lead_row.get('name') or '—'}\n"
                    f"🆔 amoCRM ID: <code>{lead_row.get('amo_lead_id')}</code>\n"
                    f"👤 Менеджер: {manager_label}\n"
                    f"🔧 Назначил: {assigner_label}"
                ),
                reply_markup=None,
            )
        except Exception:
            log.warning("Failed to edit original workchat lead message")

    # Assign in amoCRM
    if integrations.amocrm:
        amo_lead_id = lead_row.get("amo_lead_id")
        if amo_lead_id:
            amo_user_id = await _resolve_amo_user_id(db, integrations.amocrm, manager_id)
            if amo_user_id:
                try:
                    await integrations.amocrm.update_lead(
                        int(amo_lead_id),
                        {"responsible_user_id": amo_user_id},
                    )
                except Exception:
                    log.exception("Failed to assign amoCRM lead %s", amo_lead_id)

    # Notify the assigned manager
    await notifier.safe_send(
        manager_id,
        f"📋 Вам назначен лид: <b>{lead_row.get('name') or '—'}</b> "
        f"(amoCRM #{lead_row.get('amo_lead_id')}).\n"
        f"Назначил: {assigner_label}\n"
        f"Для создания проекта используйте «Проверить КП / Запросить документы».",
    )

    log.info(
        "Lead %s assigned to manager %s by %s (@%s)",
        lead_id, manager_id, u.id, u.username,
    )


# ==================== HELPER: resolve amoCRM user ID ====================

async def _resolve_amo_user_id(
    db: Database,
    amocrm: AmoCRMService,
    telegram_id: int,
) -> int | None:
    """Try to find the amoCRM user_id for a given Telegram user.

    Strategy:
    1. Check DB setting `amo_user_map:{telegram_id}`
    2. Try to match by email/name from amoCRM users list (cache in DB)
    """
    # 1. Direct mapping from settings
    key = f"amo_user_map:{telegram_id}"
    val = await db.get_setting(key)
    if val:
        try:
            return int(val)
        except ValueError:
            pass

    # 2. If no mapping exists, we can't auto-resolve without additional data.
    #    Admin can set it via: /setsetting amo_user_map:<tg_id> <amo_user_id>
    #    For now, return None.
    return None
