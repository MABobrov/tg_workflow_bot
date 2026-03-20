"""
New handlers for RP (Руководитель проектов) role.

Main menu (March 2026 layout):
- Проверка КП / Выставление Счета   (placeholder)
- Чат с ГД                          (placeholder)
- Счета в Работе                    (placeholder)
- Менеджер 1 (КВ) — chat-proxy
- Счета на оплату — мониторинг
- Менеджер 2 (КИА) — chat-proxy
- Бухгалтерия (УПД) — ЭДО запрос
- Монтажная гр. — submenu (Чат / В работу)
- Счет закрыт                       (placeholder)
- Лид на расчет (LeadToProjectSG)

Legacy (still handled for backward compat):
- Входящие Отд.Продаж
- Счета в Работу (мониторинг, legacy)
- Счет End (входящие условия, legacy)
- Проблема / Вопрос (legacy)

Other:
- Смена роли РП ↔ НПН
- Поиск Счета (в manager_new.py)
- Ответ на КП от менеджера (KpReviewResponseSG)
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, Role, TaskStatus, TaskType
from ..keyboards import (
    RP_BTN_CHECK_KP,
    RP_BTN_CHAT_GD,
    RP_BTN_EDO,
    RP_BTN_INVOICE_CLOSED,
    RP_BTN_INVOICE_END,
    RP_BTN_INVOICE_START,
    RP_BTN_INVOICES_PAY,
    RP_BTN_INVOICES_WORK,
    RP_BTN_ISSUE,
    RP_BTN_LEAD,
    RP_BTN_MGR_KIA,
    RP_BTN_MGR_KV,
    RP_BTN_MONTAZH,
    RP_BTN_ROLE_RP,
    RP_BTN_ROLE_RP_INACTIVE,
    RP_BTN_ROLE_NPN,
    RP_BTN_ROLE_NPN_ACTIVE,
    RP_MONTAZH_BTN_RAZMERY,
    RP_SUBBTN_MGR_KIA,
    RP_SUBBTN_MGR_KV,
    RP_SUBBTN_MONTAZH,
    edo_type_kb,
    invoice_list_kb,
    invoices_work_list_kb,
    kp_issued_list_kb,
    kp_payment_type_kb,
    kp_response_kb,
    kp_task_list_kb,
    lead_pick_manager_kb,
    main_menu,
    rp_chat_gd_submenu,
    rp_chat_submenu,
    rp_montazh_submenu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    EdoRequestSG,
    KpReviewSG,
    LeadToProjectSG,
    ManagerChatProxySG,
    RpRazmerySG,
    RpSupplierInvoiceSG,
)
from ..utils import answer_service, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


@router.message.outer_middleware()
async def _rp_auto_refresh(handler, event: Message, data: dict):  # type: ignore[type-arg]
    """При каждом сообщении от РП — обновляем reply-клавиатуру с бейджами."""
    result = await handler(event, data)
    u = event.from_user
    if not u:
        return result
    db_rp: Database | None = data.get("db")
    cfg = data.get("config")
    if not db_rp or not cfg:
        return result
    try:
        user = await db_rp.get_user_optional(u.id)
        if not user or not user.role:
            return result
        menu_role = resolve_active_menu_role(u.id, user.role)
        if menu_role != Role.RP:
            return result
        unread = await db_rp.count_unread_tasks(u.id)
        uc = await db_rp.count_unread_by_channel(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        # RP-specific badge counts
        rp_t = await db_rp.count_rp_role_tasks(u.id)
        rp_m = await db_rp.count_rp_role_messages(u.id)
        rp_ckp = await db_rp.count_rp_check_kp_tasks(u.id)
        rp_ipay = await db_rp.count_rp_invoice_pay_tasks(u.id)
        rp_ch_kv = await db_rp.count_rp_channel_unread(u.id, "rp_to_manager_kv")
        rp_ch_kia = await db_rp.count_rp_channel_unread(u.id, "rp_to_manager_kia")
        rp_ch_mont = await db_rp.count_rp_channel_unread(u.id, "montazh")
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
            unread_channels=uc,
            rp_tasks=rp_t, rp_messages=rp_m,
            rp_check_kp=rp_ckp, rp_invoices_pay=rp_ipay,
            rp_ch_mgr_kv=rp_ch_kv, rp_ch_mgr_kia=rp_ch_kia,
            rp_ch_montazh=rp_ch_mont,
        )
        await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
    except Exception:
        log.debug("rp auto-refresh failed", exc_info=True)
    return result


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


def _invoice_status_label(status: str | None) -> str:
    return {
        "new": "🆕 Новый",
        "pending": "⏳ Ждёт подтверждения ГД",
        "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен",
        "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
        "credit": "🏦 Кредит",
    }.get(status or "", status or "—")


def _invoice_status_emoji(status: str | None) -> str:
    return {
        "new": "🆕",
        "pending": "⏳",
        "in_progress": "🔄",
        "paid": "✅",
        "on_hold": "⏸",
        "rejected": "❌",
        "closing": "📌",
        "ended": "🏁",
        "credit": "🏦",
    }.get(status or "", "❓")


async def _answer_or_edit(
    target: Message | CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(  # type: ignore[union-attr]
                text,
                reply_markup=reply_markup,
            )
            return
        except Exception:
            await target.message.answer(  # type: ignore[union-attr]
                text,
                reply_markup=reply_markup,
            )
            return

    await target.answer(text, reply_markup=reply_markup)


# =====================================================================
# ВХОДЯЩИЕ ОТД.ПРОДАЖ
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие Отд.Продаж"))
async def rp_inbox_sales(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    if not tasks:
        await message.answer("📥 Входящих задач нет ✅")
        return
    await message.answer(
        f"📥 <b>Входящие Отд.Продаж</b> ({len(tasks)}):\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


# =====================================================================
# СЧЕТ В РАБОТУ (мониторинг для РП)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_START))
async def rp_invoice_start_monitor(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await _show_invoices_work_dashboard(message, db)


@router.callback_query(F.data.startswith("rpinv:view:"))
async def rp_invoice_view(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = _invoice_status_label(inv.get("status"))

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )
    conditions = await db.check_close_conditions(invoice_id)
    c1 = "✅" if conditions["installer_ok"] else "⏳"
    c2 = "✅" if conditions["edo_signed"] else "⏳"
    c3 = "✅" if conditions["no_debts"] else "⏳"
    text += (
        f"\n<b>Условия:</b>\n"
        f"{c1} 1. Монтажник — Счет ОК\n"
        f"{c2} 2. ЭДО — подписано\n"
        f"{c3} 3. Долгов нет\n"
    )
    await cb.message.answer(text)  # type: ignore[union-attr]


# =====================================================================
# СЧЕТА НА ОПЛАТУ (💳 — мониторинг + создание, Этап 7)
#
# Мониторинг: PENDING_PAYMENT + IN_PROGRESS
# Создание: InvoiceCreateSG flow (handlers in legacy rp.py)
#
# Callbacks:
#   rp_inv_pay:create — начать создание счёта на оплату (→ InvoiceCreateSG)
#   rp_inv_pay:refresh — обновить список
# =====================================================================


def _invoices_pay_kb(
    invoices: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки для «Счета на оплату»: список + кнопка создания."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        status_emoji = _invoice_status_emoji(inv.get("status"))
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        text = f"{status_emoji} №{inv.get('invoice_number', '?')} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_work:view:{inv['id']}")
    b.button(text="➕ Создать счёт на оплату", callback_data="rp_inv_pay:create")
    b.button(text="🔄 Обновить", callback_data="rp_inv_pay:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


async def _show_invoices_pay_dashboard(
    target: Message | CallbackQuery,
    db: Database,
) -> None:
    pending = await db.list_invoices(status=InvoiceStatus.PENDING_PAYMENT, limit=30)
    in_progress = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS, limit=30)
    all_inv = list(pending) + list(in_progress)

    if not all_inv:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Создать счёт на оплату", callback_data="rp_inv_pay:create")
        b.button(text="🔄 Обновить", callback_data="rp_inv_pay:refresh")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await _answer_or_edit(
            target,
            "💳 <b>Счета на оплату</b>\n\n"
            "Нет счетов, ожидающих оплаты ✅\n\n"
            "Можно создать новый счёт или обновить список.",
            reply_markup=b.as_markup(),
        )
        return

    header_parts: list[str] = []
    if pending:
        header_parts.append(f"⏳ Ожидают: {len(pending)}")
    if in_progress:
        header_parts.append(f"🔄 В работе: {len(in_progress)}")

    await _answer_or_edit(
        target,
        f"💳 <b>Счета на оплату</b> ({len(all_inv)})\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите для просмотра или создайте новый:",
        reply_markup=_invoices_pay_kb(all_inv),
    )


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICES_PAY))
async def rp_invoices_pay(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: Счета на оплату (мониторинг + создание)."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await _show_invoices_pay_dashboard(message, db)


@router.callback_query(F.data == "rp_inv_pay:refresh")
async def rp_invoices_pay_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить список «Счета на оплату»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await state.clear()
    await _show_invoices_pay_dashboard(cb, db)


@router.callback_query(F.data == "rp_inv_pay:create")
async def rp_invoices_pay_create(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание счёта на оплату ГД (→ InvoiceCreateSG).

    Simplified: skip project selection, go directly to invoice picker.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    from ..states import InvoiceCreateSG
    from ..keyboards import invoice_select_kb

    invoices = await db.list_invoices_in_work(limit=20, only_regular=True)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Нет счетов в работе."
        )
        return

    await state.clear()
    await state.set_state(InvoiceCreateSG.parent_invoice)
    await cb.message.answer(  # type: ignore[union-attr]
        "💳 <b>Счёт на оплату ГД</b>\n"
        "Шаг 1: выберите счёт объекта (№, адрес):",
        reply_markup=invoice_select_kb(invoices, prefix="inv_create_parent", allow_skip=True, back_callback="nav:home"),
    )


# =====================================================================
# СЧЕТ END (входящие для РП)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_END))
async def rp_invoice_end(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.CLOSING)
    ended = await db.list_invoices(status=InvoiceStatus.ENDED, limit=10)
    all_inv = list(invoices) + list(ended)

    if not all_inv:
        await answer_service(message, "🏁 Нет счетов в процессе закрытия / закрытых.", delay_seconds=60)
        return
    await message.answer(
        f"🏁 <b>Счет End</b> ({len(all_inv)}):\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv", back_callback="nav:home"),
    )


# =====================================================================
# ПРОБЛЕМА / ВОПРОС
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_ISSUE))
async def rp_issue(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    issues = [t for t in tasks if t.get("type") == TaskType.ISSUE]
    if not issues:
        await answer_service(message, "🆘 Нет входящих проблем/вопросов.", delay_seconds=60)
        return
    await message.answer(
        f"🆘 <b>Проблема / Вопрос</b> ({len(issues)}):",
        reply_markup=tasks_kb(issues, back_callback="nav:home"),
    )


# =====================================================================
# МЕНЕДЖЕР 1 (КВ) — chat-proxy
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_MGR_KV) or (m.text or "").strip().startswith(RP_SUBBTN_MGR_KV))
async def rp_chat_mgr_kv(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kv")
    # #38: Invoice picker перед чатом
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True)
    kv_invoices = [i for i in invoices if i.get("creator_role") == "manager_kv"]
    if kv_invoices:
        b = InlineKeyboardBuilder()
        for inv in kv_invoices[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"rp_chat_inv:kv:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="rp_chat_inv:kv:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "👤 <b>Менеджер 1 (КВ)</b>\n\n"
            "Выберите счёт для привязки к переписке:",
            reply_markup=b.as_markup(),
        )
    else:
        await message.answer(
            "👤 <b>Менеджер 1 (КВ)</b>\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_MGR_KIA) or (m.text or "").strip().startswith(RP_SUBBTN_MGR_KIA))
async def rp_chat_mgr_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kia")
    # #38: Invoice picker перед чатом
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True)
    kia_invoices = [i for i in invoices if i.get("creator_role") == "manager_kia"]
    if kia_invoices:
        b = InlineKeyboardBuilder()
        for inv in kia_invoices[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"rp_chat_inv:kia:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="rp_chat_inv:kia:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "👤 <b>Менеджер 2 (КИА)</b>\n\n"
            "Выберите счёт для привязки к переписке:",
            reply_markup=b.as_markup(),
        )
    else:
        await message.answer(
            "👤 <b>Менеджер 2 (КИА)</b>\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


# =====================================================================
# INVOICE PICKER FOR CHAT (#38/#39)
# =====================================================================

@router.callback_query(F.data.startswith("rp_chat_inv:"))
async def rp_chat_invoice_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП выбрал счёт для привязки к чату с менеджером (#38)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    mgr_key = parts[1]  # kv, kia, montazh
    inv_id = int(parts[2])

    channel_map = {"kv": "rp_to_manager_kv", "kia": "rp_to_manager_kia", "montazh": "montazh"}
    channel = channel_map.get(mgr_key, f"rp_to_manager_{mgr_key}")

    await state.update_data(channel=channel, linked_invoice_id=inv_id if inv_id else None)

    label_map = {"kv": "Менеджер 1 (КВ)", "kia": "Менеджер 2 (КИА)", "montazh": "Монтажная гр."}
    label = label_map.get(mgr_key, mgr_key)

    inv_text = ""
    if inv_id:
        inv = await db.get_invoice(inv_id)
        if inv:
            inv_text = f"\n📄 Привязан счёт: №{inv.get('invoice_number', '?')}"

    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"👤 <b>{label}</b>{inv_text}\n\nВыберите действие:",
        )
    except Exception:
        pass

    if mgr_key == "montazh":
        await cb.message.answer(  # type: ignore[union-attr]
            f"🔧 <b>Монтажная гр.</b>{inv_text}\n\nВыберите действие:",
            reply_markup=rp_montazh_submenu("⬅️ Назад"),
        )
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            f"👤 <b>{label}</b>{inv_text}\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


# =====================================================================
# МОНТАЖНАЯ ГР. — chat-proxy + В работу (Этап 9)
#
# Submenu: 💬 Чат / 🔧 В работу
# - Чат → ManagerChatProxySG (standard chat-proxy with montazh channel)
# - В работу → список активных счетов с монтажниками
#
# Callbacks:
#   rp_montazh:work_view:\d+  — карточка счёта «В работу»
#   rp_montazh:work_refresh   — обновить список «В работу»
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_MONTAZH) or (m.text or "").strip().startswith(RP_SUBBTN_MONTAZH))
async def rp_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    # #39: Invoice picker перед чатом с монтажником
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True)
    montazh_inv = [i for i in invoices if i.get("montazh_stage") and i["montazh_stage"] != "none"]
    if montazh_inv:
        b = InlineKeyboardBuilder()
        for inv in montazh_inv[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"rp_chat_inv:montazh:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="rp_chat_inv:montazh:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "🔧 <b>Монтажная гр.</b>\n\n"
            "Выберите счёт для привязки к переписке:",
            reply_markup=b.as_markup(),
        )
        return
    await message.answer(
        "🔧 <b>Монтажная гр.</b>\n\nВыберите действие:",
        reply_markup=rp_montazh_submenu("⬅️ Назад"),
    )


@router.message(ManagerChatProxySG.menu, F.text == "💬 Чат")
async def rp_montazh_chat(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Монтажная гр. → Чат: переписка с монтажниками."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return  # Only handle montazh context
    limit = getattr(config, "chat_history_limit", 20)
    messages_list = await db.list_chat_messages(channel, limit=limit)
    if not messages_list:
        await message.answer("💬 Пока нет сообщений в чате с монтажной группой.")
        return
    lines: list[str] = [f"💬 <b>Чат — Монтажная гр.</b> (последние {len(messages_list)}):\n"]
    for m in messages_list:
        sender_id = m.get("sender_id", 0)
        sender_label = await get_initiator_label(db, int(sender_id)) if sender_id else "?"
        text_msg = m.get("text", "")
        ts = m.get("created_at", "")[:16]
        direction = m.get("direction", "")
        arrow = "→" if direction == "outgoing" else "←"
        lines.append(f"<b>{sender_label}</b> {arrow} ({ts}):\n{text_msg}")
    await message.answer("\n\n".join(lines[-12:]))


@router.message(ManagerChatProxySG.menu, F.text == "🔧 В работу")
async def rp_montazh_in_work(message: Message, state: FSMContext, db: Database) -> None:
    """Монтажная гр. → В работу: список активных счетов монтажной группы."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return  # Only handle montazh context

    # Get invoices in active statuses (not ended/credit)
    invoices = await db.list_invoices_in_work(limit=50)
    if not invoices:
        await message.answer(
            "🔧 <b>Монтажная гр. — В работу</b>\n\n"
            "Нет активных счетов ✅"
        )
        return

    b = InlineKeyboardBuilder()
    for inv in invoices:
        ok_emoji = "✅" if inv.get("installer_ok") else "⏳"
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        text = f"{ok_emoji} №{inv.get('invoice_number', '?')} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_montazh:work_view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="rp_montazh:work_refresh")
    b.adjust(1)

    n_ok = sum(1 for inv in invoices if inv.get("installer_ok"))
    n_pending = len(invoices) - n_ok
    stats = []
    if n_ok:
        stats.append(f"✅ Счет ОК: {n_ok}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await message.answer(
        f"🔧 <b>Монтажная гр. — В работу</b> ({len(invoices)})\n"
        f"{' | '.join(stats)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:work_refresh")
async def rp_montazh_work_refresh(cb: CallbackQuery, db: Database) -> None:
    """Обновить список «В работу» монтажной группы."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")

    invoices = await db.list_invoices_in_work(limit=50)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "🔧 Нет активных счетов ✅"
        )
        return

    b = InlineKeyboardBuilder()
    for inv in invoices:
        ok_emoji = "✅" if inv.get("installer_ok") else "⏳"
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        text = f"{ok_emoji} №{inv.get('invoice_number', '?')} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_montazh:work_view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="rp_montazh:work_refresh")
    b.adjust(1)

    n_ok = sum(1 for inv in invoices if inv.get("installer_ok"))
    n_pending = len(invoices) - n_ok
    stats = []
    if n_ok:
        stats.append(f"✅ Счет ОК: {n_ok}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await cb.message.answer(  # type: ignore[union-attr]
        f"🔧 <b>В работу</b> ({len(invoices)})\n"
        f"{' | '.join(stats)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^rp_montazh:work_view:\d+$"))
async def rp_montazh_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта «В работу» монтажной группы."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = _invoice_status_label(inv.get("status"))

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    text = (
        f"🔧 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    # Installer OK status
    if inv.get("installer_ok"):
        ok_by = ""
        if inv.get("installer_ok_by"):
            ok_by = await get_initiator_label(db, int(inv["installer_ok_by"]))
            ok_by = f" ({ok_by})"
        ok_at = inv.get("installer_ok_at", "")[:10] if inv.get("installer_ok_at") else ""
        text += f"\n✅ <b>Монтажник — Счет ОК</b>{ok_by} {ok_at}\n"
    else:
        text += "\n⏳ Монтажник — ожидание «Счет ОК»\n"

    # ZP status
    zp_label = {
        "not_requested": "⏳ Не запрошен",
        "requested": "📤 Отправлен ГД",
        "approved": "✅ ЗП ОК",
    }.get(inv.get("zp_status", "not_requested"), inv.get("zp_status", ""))
    text += f"💵 Расчёт ЗП: {zp_label}\n"

    # EDO status
    if inv.get("edo_signed"):
        text += "📄 ЭДО: ✅ Подписано\n"
    else:
        text += "📄 ЭДО: ⏳ Не подписано\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_montazh:work_refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# РАЗМЕРЫ — workflow проверки размеров стекла (РП-сторона)
# =====================================================================

@router.message(ManagerChatProxySG.menu, F.text == RP_MONTAZH_BTN_RAZMERY)
async def rp_razmery_inbox(message: Message, state: FSMContext, db: Database) -> None:
    """Монтажная гр. → Размеры: inbox заявок на размеры."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return

    reqs = await db.list_razmery_requests_for_rp()
    if not reqs:
        await message.answer(
            "📐 <b>Размеры</b>\n\nНет активных заявок ✅"
        )
        return

    _STATUS_LABEL = {
        "pending": "🆕 Новый",
        "rp_received": "📝 Ожидает формы",
        "error": "❌ Ошибка → исправить",
        "verification_sent": "📤 Отправлено",
    }

    b = InlineKeyboardBuilder()
    for req in reqs:
        inv_num = req.get("invoice_number") or f"#{req['invoice_id']}"
        sl = _STATUS_LABEL.get(req["status"], req["status"])
        b.button(
            text=f"{sl}: №{inv_num}"[:55],
            callback_data=f"razmok_rp:view:{req['id']}",
        )
    b.adjust(1)

    stats = {}
    for req in reqs:
        s = req["status"]
        stats[s] = stats.get(s, 0) + 1
    stats_line = " | ".join(f"{_STATUS_LABEL.get(k, k)}: {v}" for k, v in stats.items())

    await message.answer(
        f"📐 <b>Размеры</b> ({len(reqs)})\n"
        f"{stats_line}\n\n"
        "Нажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("razmok_rp:view:"))
async def rp_razmery_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка заявки на размеры."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    addr = inv.get("object_address", "—") if inv else "—"
    inst_label = await get_initiator_label(db, req["installer_id"])

    _STATUS_LABEL = {
        "pending": "🆕 Ожидает подтверждения",
        "rp_received": "📝 Ожидает формы поставщика",
        "error": "❌ Ошибка — нужно исправить",
        "verification_sent": "📤 Отправлено монтажнику",
    }

    text = (
        f"📐 <b>Заявка на размеры #{req['id']}</b>\n\n"
        f"🧾 Счёт: №{inv_num}\n"
        f"📍 Адрес: {addr}\n"
        f"👷 Монтажник: {inst_label}\n"
        f"📊 Статус: {_STATUS_LABEL.get(req['status'], req['status'])}\n"
    )
    if req.get("installer_comment"):
        text += f"💬 Комментарий: {req['installer_comment']}\n"
    if req.get("result") == "error" and req.get("result_comment"):
        text += f"\n❌ <b>Ошибка от монтажника:</b>\n{req['result_comment']}\n"

    b = InlineKeyboardBuilder()
    if req["status"] == "pending":
        b.button(text="✅ ОК (принял)", callback_data=f"razmok_rp:received:{req_id}")
    elif req["status"] in ("rp_received", "error"):
        b.button(text="📐 Отправить форму", callback_data=f"razmok_rp:send_form:{req_id}")
    elif req["status"] == "verification_sent":
        b.button(text="⏳ Ожидаем ответ", callback_data=f"razmok_rp:noop:{req_id}")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("razmok_rp:noop:"))
async def rp_razmery_noop(cb: CallbackQuery) -> None:
    await cb.answer("Ожидаем ответ монтажника")


@router.callback_query(F.data.startswith("razmok_rp:received:"))
async def rp_razmery_confirm_receipt(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """РП подтверждает получение бланка."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("✅ Принял")
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        return

    await db.update_razmery_request(req_id, status="rp_received", rp_id=cb.from_user.id)

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"

    # Уведомить монтажника
    await notifier.safe_send(
        req["installer_id"],
        f"✅ РП принял бланк размеров по счёту №{inv_num}.\n"
        "Ожидайте форму поставщика для проверки.",
    )
    await refresh_recipient_keyboard(notifier, db, config, req["installer_id"])

    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Получение подтверждено. Теперь заполните форму поставщика и отправьте монтажнику.\n"
        "Используйте кнопку «📐 Размеры» → выберите заявку → «Отправить форму».",
    )


# --- Шаг 2: РП отправляет форму поставщика ---

@router.callback_query(F.data.startswith("razmok_rp:send_form:"))
async def rp_razmery_start_form(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП начинает отправку формы поставщика."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    await state.update_data(rp_razmery_req_id=req_id, rp_razmery_attachments=[])
    await state.set_state(RpRazmerySG.comment)
    await cb.message.answer(  # type: ignore[union-attr]
        "📐 <b>Форма поставщика</b>\n\n"
        "Добавьте комментарий к форме\n"
        "(или «-» для пропуска, «❌ Отмена» для отмены):",
    )


@router.message(RpRazmerySG.comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(RpRazmerySG.attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
async def rp_razmery_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await message.answer(
        "❌ Отменено.",
        reply_markup=rp_montazh_submenu("⬅️ Назад"),
    )


@router.message(RpRazmerySG.comment)
async def rp_razmery_form_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = None if text == "-" else text
    await state.update_data(rp_razmery_comment=comment)
    await state.set_state(RpRazmerySG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить монтажнику", callback_data="razmok_rp:form_create")
    b.button(text="⏭ Без вложений", callback_data="razmok_rp:form_create")
    b.adjust(1)
    await message.answer(
        "Прикрепите форму поставщика (фото/документ).\n"
        "Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(RpRazmerySG.attachments)
async def rp_razmery_form_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("rp_razmery_attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id})
    elif message.photo:
        attachments.append({"file_type": "photo", "file_id": message.photo[-1].file_id})
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(rp_razmery_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "razmok_rp:form_create")
async def rp_razmery_form_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Финализация: отправить форму поставщика монтажнику."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    req_id = data.get("rp_razmery_req_id")
    comment = data.get("rp_razmery_comment")
    attachments = data.get("rp_razmery_attachments", [])

    from ..utils import to_iso, utcnow
    now = to_iso(utcnow())

    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    await db.update_razmery_request(
        req_id,
        status="verification_sent",
        rp_id=u.id,
        rp_comment=comment,
        rp_sent_at=now,
        result=None,
        result_comment=None,
    )

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    rp_label = await get_initiator_label(db, u.id)

    # Уведомить монтажника
    inst_b = InlineKeyboardBuilder()
    inst_b.button(text="✅ Размеры ОК", callback_data=f"razmok_inst:ok:{req_id}")
    inst_b.button(text="❌ Ошибка", callback_data=f"razmok_inst:error:{req_id}")
    inst_b.adjust(2)

    msg = (
        f"📐 <b>Проверка размеров</b>\n"
        f"👤 От: {rp_label}\n"
        f"🧾 Счёт: №{inv_num}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"
    msg += "\nПроверьте форму и подтвердите:"

    await notifier.safe_send(
        req["installer_id"], msg, reply_markup=inst_b.as_markup(),
    )
    for a in attachments:
        await notifier.safe_send_media(req["installer_id"], a["file_type"], a["file_id"])
    await refresh_recipient_keyboard(notifier, db, config, req["installer_id"])

    # Вернуть РП в подменю montazh
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Форма поставщика отправлена монтажнику по счёту №{inv_num}.",
        reply_markup=rp_montazh_submenu("⬅️ Назад"),
    )


# =====================================================================
# ПРОВЕРКА КП / ВЫСТАВЛЕНИЕ СЧЕТА — полный flow (Этап 5)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_CHECK_KP))
async def rp_check_kp(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: показать входящие CHECK_KP задачи."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    u = message.from_user
    if not u:
        return

    tasks = await db.list_check_kp_tasks(u.id)

    if not tasks:
        await answer_service(
            message,
            "📋 <b>Проверка КП / Выставление Счета</b>\n\n"
            "Входящих запросов на проверку КП нет ✅\n\n"
            "Используйте «📑 Выставленные счета» для просмотра обработанных.",
            delay_seconds=60,
        )
        # Всё равно показываем кнопку «Выставленные счета»
        b = InlineKeyboardBuilder()
        b.button(text="📑 Выставленные счета", callback_data="kp_resp:issued")
        b.adjust(1)
        await message.answer("—", reply_markup=b.as_markup())
        return

    # Подсчёт по менеджерам
    mgr_counts: dict[str, int] = {}
    for t in tasks:
        payload = json.loads(t.get("payload_json") or "{}")
        mrole = payload.get("manager_role", "manager")
        lbl = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}.get(mrole, "Менеджер")
        mgr_counts[lbl] = mgr_counts.get(lbl, 0) + 1

    summary_parts = [f"{lbl}: {cnt}" for lbl, cnt in mgr_counts.items()]

    await message.answer(
        f"📋 <b>Проверка КП / Выставление Счета</b>\n\n"
        f"Входящих запросов: <b>{len(tasks)}</b>\n"
        f"По менеджерам: {', '.join(summary_parts)}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=kp_task_list_kb(tasks, show_issued=True),
    )


# =====================================================================
# ЧАТ С ГД — chat-proxy (RP ↔ GD)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_CHAT_GD))
async def rp_chat_gd(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_gd")
    await message.answer(
        "👤 <b>Чат с ГД</b>\n\nВыберите действие:",
        reply_markup=rp_chat_gd_submenu("⬅️ Назад"),
    )


# =====================================================================
# СЧЕТА В РАБОТЕ — дашборд (Этап 6)
#
# Показывает счета со статусами PENDING/IN_PROGRESS/PAID (исключая Кред)
# с двойными индикаторами:
#   💰 = оплата (⏳ ожидает / 🔄 в работе / ✅ оплачен)
#   📄 = документы ЭДО (⏳ не подписано / ✅ подписано)
#
# Callbacks:
#   rp_work:view:\d+  — карточка счёта
#   rp_work:refresh   — обновить список
# =====================================================================


async def _show_invoices_work_dashboard(
    target: Message | CallbackQuery,
    db: Database,
) -> None:
    """Общий хелпер: показать дашборд «Счета в Работе»."""
    invoices = await db.list_invoices_in_work(limit=50)

    if not invoices:
        b = InlineKeyboardBuilder()
        b.button(text="🔄 Обновить", callback_data="rp_work:refresh")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        text = (
            "💼 <b>Счета в Работе</b>\n\n"
            "Нет активных счетов ✅"
        )
        await _answer_or_edit(target, text, reply_markup=b.as_markup())
        return

    # Statistics by status
    n_pending = sum(1 for inv in invoices if inv.get("status") == "pending")
    n_progress = sum(1 for inv in invoices if inv.get("status") == "in_progress")
    n_paid = sum(1 for inv in invoices if inv.get("status") == "paid")

    # EDO signing stats
    n_edo_signed = sum(1 for inv in invoices if inv.get("edo_signed"))
    n_edo_pending = len(invoices) - n_edo_signed

    header_parts: list[str] = []
    if n_pending:
        header_parts.append(f"⏳ Ждёт подтверждения: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")
    if n_paid:
        header_parts.append(f"✅ Оплачены: {n_paid}")

    edo_parts: list[str] = []
    if n_edo_signed:
        edo_parts.append(f"✅ Подписано: {n_edo_signed}")
    if n_edo_pending:
        edo_parts.append(f"⏳ Не подписано: {n_edo_pending}")

    text = (
        f"💼 <b>Счета в Работе</b> ({len(invoices)})\n\n"
        f"<b>💰 Оплата:</b> {' | '.join(header_parts)}\n"
        f"<b>📄 ЭДО:</b> {' | '.join(edo_parts)}\n\n"
        "Нажмите на счёт для просмотра:"
    )

    await _answer_or_edit(
        target,
        text,
        reply_markup=invoices_work_list_kb(invoices),
    )


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICES_WORK))
async def rp_invoices_work(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Счета в Работе»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await _show_invoices_work_dashboard(message, db)


@router.callback_query(F.data == "rp_work:refresh")
async def rp_invoices_work_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд «Счета в Работе»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await _show_invoices_work_dashboard(cb, db)


@router.callback_query(F.data.regexp(r"^rp_work:view:\d+$"))
async def rp_invoices_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта из дашборда «Счета в Работе»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = _invoice_status_label(inv.get("status"))

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    # Creator info
    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))
    creator_role_label = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(inv.get("creator_role", ""), inv.get("creator_role", ""))

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"📊 Статус: {status_label}\n"
        f"👤 Создал: {creator_label} ({creator_role_label})\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    # Close conditions
    conditions = await db.check_close_conditions(invoice_id)
    c1 = "✅" if conditions["installer_ok"] else "⏳"
    c2 = "✅" if conditions["edo_signed"] else "⏳"
    c3 = "✅" if conditions["no_debts"] else "⏳"
    c4 = "✅" if conditions["zp_approved"] else "⏳"
    text += (
        f"\n<b>Условия закрытия:</b>\n"
        f"{c1} 1. Монтажник — Счет ОК\n"
        f"{c2} 2. ЭДО — подписано\n"
        f"{c3} 3. Долгов нет\n"
        f"{c4} 4. ЗП — расчёт подтверждён\n"
    )

    # ── Supplier payments grouped by material category ──
    grouped = await db.list_supplier_payments_grouped(invoice_id)
    _CAT_LABELS = {
        "metal": ("🔩", "Металл"),
        "glass": ("🪟", "Стекло"),
        "additional": ("📦", "Доп. Материалы"),
        "services": ("🚚", "Оплата услуг"),
    }
    has_any_material = any(grouped[cat] for cat in grouped)
    text += "\n<b>📦 Заказанные материалы:</b>\n"
    if not has_any_material:
        text += "  Нет записей\n"
    else:
        for cat_key, (icon, label) in _CAT_LABELS.items():
            items = grouped.get(cat_key, [])
            if items:
                total = sum(p["amount"] for p in items)
                total_s = f"{total:,.0f}".replace(",", " ")
                details = ", ".join(
                    p.get("supplier") or "—" for p in items
                )
                text += f"✅ {icon} {label} — {total_s}₽ ({details})\n"
            else:
                text += f"⏳ {icon} {label}\n"

    # ── UPD supplier signing via EDO ──
    upd_signed = await db.get_edo_upd_status_for_invoice(invoice_id)
    upd_icon = "✅" if upd_signed else "⏳"
    text += f"\n<b>📄 УПД поставщика:</b>\n{upd_icon} Подписание по ЭДО\n"

    # Payment file info
    if inv.get("payment_file_id"):
        text += "\n💸 Платёжка: прикреплена ✅\n"
    if inv.get("payment_comment"):
        text += f"💬 Коммент. к оплате: {inv['payment_comment']}\n"

    b = InlineKeyboardBuilder()
    b.button(text="💬 Переписка", callback_data=f"rp_work:msgs:{invoice_id}")
    b.button(text="📋 Задачи", callback_data=f"rp_work:tasks:{invoice_id}")
    b.button(text="📦 Расходы", callback_data=f"rp_work:expenses:{invoice_id}")
    b.button(text="📎 Счёт ГД", callback_data=f"rp_work:send_inv_gd:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_work:refresh")
    b.adjust(2, 2, 1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# --- Вложенные страницы карточки «Счета в Работе» ---


@router.callback_query(F.data.regexp(r"^rp_work:msgs:\d+$"))
async def rp_work_messages(cb: CallbackQuery, db: Database) -> None:
    """Переписка, привязанная к счёту."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    messages = await db.list_chat_messages_by_invoice(invoice_id, limit=30)

    num = inv.get("invoice_number") or f"#{inv.get('id')}"
    lines: list[str] = [f"💬 <b>Переписка — Счёт №{num}</b>\n"]

    if not messages:
        lines.append("Нет привязанных сообщений.")
    else:
        for msg in reversed(messages):  # chronological order
            channel = msg.get("channel", "—")
            text_content = (msg.get("text") or "")[:120]
            dt = (msg.get("created_at") or "")[:16]
            direction = "→" if msg.get("direction") == "outgoing" else "←"
            has_file = " 📎" if msg.get("has_attachment") else ""
            lines.append(f"{dt} [{channel}] {direction} {text_content}{has_file}")

    text_out = "\n".join(lines)
    if len(text_out) > 3800:
        text_out = text_out[:3800] + "\n\n... (обрезано)"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_work:tasks:\d+$"))
async def rp_work_tasks(cb: CallbackQuery, db: Database) -> None:
    """Задачи, привязанные к счёту."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    tasks = await db.list_tasks_by_invoice(invoice_id, limit=30)

    num = inv.get("invoice_number") or f"#{inv.get('id')}"
    lines: list[str] = [f"📋 <b>Задачи — Счёт №{num}</b>\n"]

    if not tasks:
        lines.append("Нет привязанных задач.")
    else:
        status_emoji = {
            "open": "🟡", "in_progress": "🔵", "done": "✅", "rejected": "❌",
        }
        for t in tasks:
            s_emoji = status_emoji.get(t.get("status", ""), "❓")
            t_type = (t.get("task_type") or "—").replace("_", " ").title()
            assignee_id = t.get("assignee_id")
            assignee_label = ""
            if assignee_id:
                u = await db.get_user_optional(int(assignee_id))
                if u:
                    assignee_label = f" → @{u.username}" if u.username else f" → {u.full_name or assignee_id}"
            dt = (t.get("created_at") or "")[:10]
            lines.append(f"{s_emoji} {t_type}{assignee_label} ({dt})")

    text_out = "\n".join(lines)
    if len(text_out) > 3800:
        text_out = text_out[:3800] + "\n\n... (обрезано)"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_work:expenses:\d+$"))
async def rp_work_expenses(cb: CallbackQuery, db: Database) -> None:
    """Расходы по счёту (расширенный доступ РП — с суммами, без маржи)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    from ..utils import format_rp_expenses

    children = await db.list_child_invoices(invoice_id)
    supplier_payments = await db.list_supplier_payments_for_invoice(invoice_id)

    text_out = format_rp_expenses(inv, children, supplier_payments)

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


# --- Добавление закрытых счетов обратно в работу ---


@router.callback_query(F.data == "rp_work:add_ended")
async def rp_work_add_ended(cb: CallbackQuery, db: Database) -> None:
    """Показать список ended-счетов для возврата в работу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    ended = await db.list_ended_invoices(limit=20)
    if not ended:
        await cb.message.answer(  # type: ignore[union-attr]
            "✅ Нет закрытых счетов для возврата."
        )
        return

    b = InlineKeyboardBuilder()
    for inv in ended:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:28]
        label = f"🏁 №{num}"
        if addr:
            label += f" — {addr}"
        b.button(text=label[:60], callback_data=f"rp_work:return:{inv['id']}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_work:refresh")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Закрытые счета</b> ({len(ended)})\n\n"
        "Выберите счёт для возврата в работу:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^rp_work:return:\d+$"))
async def rp_work_return_confirm(cb: CallbackQuery, db: Database) -> None:
    """Подтверждение возврата ended-счёта в работу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Вернуть в работу", callback_data=f"rp_work:return_ok:{invoice_id}")
    b.button(text="❌ Отмена", callback_data="rp_work:add_ended")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"🔄 <b>Вернуть счёт в работу?</b>\n\n"
        f"📄 №{inv.get('invoice_number', '?')}\n"
        f"📍 {inv.get('object_address', '-')}\n"
        f"💰 {amount_str}\n"
        f"📊 Текущий статус: 🏁 Закрыт\n\n"
        "Статус изменится на «В работе».",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^rp_work:return_ok:\d+$"))
async def rp_work_return_ok(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """Вернуть ended-счёт в работу (status → in_progress)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("✅ Возвращён в работу")

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_invoice_status(invoice_id, InvoiceStatus.IN_PROGRESS)
    await integrations.sync_invoice_status(inv["invoice_number"], InvoiceStatus.IN_PROGRESS)

    # Refresh the dashboard
    await _show_invoices_work_dashboard(cb, db)


# =====================================================================
# ОТПРАВКА СЧЁТА ОТ ПОСТАВЩИКА → ГД (из карточки «Счета в работе»)
# =====================================================================


@router.callback_query(F.data.regexp(r"^rp_work:send_inv_gd:\d+$"))
async def rp_work_send_inv_gd_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Начать отправку счёта от поставщика для ГД."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(RpSupplierInvoiceSG.attachments)
    await state.update_data(invoice_id=invoice_id, attachments=[])

    num = inv.get("invoice_number") or f"#{invoice_id}"
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без файлов → Комментарий", callback_data="rp_sinv:skip_attach")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"📎 <b>Счёт от поставщика → ГД</b>\n"
        f"Счёт: №{num}\n\n"
        "Прикрепите файл(ы) счёта от поставщика (документ или фото).\n"
        "Когда все файлы прикреплены — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(RpSupplierInvoiceSG.attachments)
async def rp_sinv_attach(message: Message, state: FSMContext) -> None:
    """Получить файл(ы) от РП для отправки ГД."""
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])

    if message.document:
        attachments.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("📎 Прикрепите файл или фото. Для продолжения нажмите кнопку.")
        return

    await state.update_data(attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Далее ({len(attachments)} файл.)", callback_data="rp_sinv:skip_attach")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)
    await message.answer(
        f"📎 Принял. Файлов: <b>{len(attachments)}</b>.\n"
        "Прикрепите ещё или нажмите «Далее»:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_sinv:skip_attach")
async def rp_sinv_to_comment(cb: CallbackQuery, state: FSMContext) -> None:
    """Перейти к вводу комментария."""
    await cb.answer()

    await state.set_state(RpSupplierInvoiceSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без комментария", callback_data="rp_sinv:send")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "💬 Введите комментарий к счёту (или нажмите «Без комментария»):",
        reply_markup=b.as_markup(),
    )


@router.message(RpSupplierInvoiceSG.comment)
async def rp_sinv_comment_text(
    message: Message, state: FSMContext, db: Database,
    config: "Config", notifier: "Notifier",
) -> None:
    """Получить комментарий и отправить ГД."""
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment)
    await _rp_sinv_finalize(message, state, db, config, notifier, message.from_user)


@router.callback_query(F.data == "rp_sinv:send")
async def rp_sinv_send_no_comment(
    cb: CallbackQuery, state: FSMContext, db: Database,
    config: "Config", notifier: "Notifier",
) -> None:
    """Отправить без комментария."""
    await cb.answer()
    await state.update_data(comment="")
    await _rp_sinv_finalize(cb.message, state, db, config, notifier, cb.from_user)  # type: ignore[arg-type]


@router.callback_query(F.data == "rp_sinv:cancel")
async def rp_sinv_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Отменить отправку счёта."""
    await cb.answer("❌ Отменено")
    await state.clear()
    u = cb.from_user
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Отправка счёта отменена.", reply_markup=kb)  # type: ignore[union-attr]


async def _rp_sinv_finalize(
    event_msg: Any,
    state: FSMContext,
    db: Database,
    config: Any,
    notifier: Any,
    from_user: Any,
) -> None:
    """Создать задачу SUPPLIER_INVOICE и отправить ГД."""
    data = await state.get_data()
    await state.clear()

    invoice_id = data.get("invoice_id")
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    comment: str = data.get("comment", "")

    inv = await db.get_invoice(invoice_id) if invoice_id else None
    num = (inv.get("invoice_number") if inv else None) or f"#{invoice_id}"

    from ..services.assignment import resolve_default_assignee
    from ..enums import TaskType, TaskStatus
    from ..utils import utcnow, to_iso
    from datetime import timedelta

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await event_msg.answer("⚠️ ГД не найден. Настройте роль GD.")
        return

    due = utcnow() + timedelta(hours=7)
    task = await db.create_task(
        project_id=inv.get("project_id") if inv else None,
        type_=TaskType.SUPPLIER_INVOICE,
        status=TaskStatus.OPEN,
        created_by=from_user.id if from_user else 0,
        assigned_to=int(gd_id),
        due_at_iso=to_iso(due),
        payload={
            "invoice_id": invoice_id,
            "invoice_number": num,
            "comment": comment,
            "sender_id": from_user.id if from_user else 0,
            "sender_username": (from_user.username if from_user else ""),
        },
    )

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    initiator = await get_initiator_label(db, from_user.id) if from_user else "?"
    gd_text = (
        f"📎 <b>Счёт от поставщика</b>\n"
        f"👤 От: {initiator}\n"
        f"📄 Счёт: №{num}\n"
    )
    if inv and inv.get("object_address"):
        gd_text += f"📍 Объект: {inv['object_address'][:50]}\n"
    if comment:
        gd_text += f"💬 {comment}\n"
    gd_text += f"\n📎 Вложений: {len(attachments)}"

    from ..keyboards import task_actions_kb
    await notifier.safe_send(
        int(gd_id), gd_text,
        reply_markup=task_actions_kb(task),
    )

    for a in attachments:
        try:
            if a.get("file_type") == "document":
                await notifier.bot.send_document(int(gd_id), a["file_id"])
            elif a.get("file_type") == "photo":
                await notifier.bot.send_photo(int(gd_id), a["file_id"])
        except Exception:
            log.warning("Failed to send attachment to GD %s", gd_id, exc_info=True)

    await event_msg.answer(
        f"✅ Счёт от поставщика отправлен ГД (счёт №{num}).\n"
        f"📎 Файлов: {len(attachments)}"
    )


# =====================================================================
# БУХГАЛТЕРИЯ (УПД) — ЭДО-запрос от РП (Этап 8)
#
# Дашборд: список исходящих ЭДО-запросов + кнопка «Создать»
# Создание запроса → EdoRequestSG flow (handlers in manager_new.py)
# Просмотр карточки запроса с ответом бухгалтерии
#
# Callbacks:
#   rp_edo:create — начать создание нового запроса
#   rp_edo:view:\d+ — просмотр карточки запроса
#   rp_edo:refresh — обновить дашборд
# =====================================================================


def _edo_requests_list_kb(
    requests: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком ЭДО-запросов РП."""
    b = InlineKeyboardBuilder()
    for r in requests:
        status_emoji = {"open": "⏳", "done": "✅"}.get(r.get("status", ""), "❓")
        req_type_label = {
            "sign_invoice": "Подпись счёт",
            "sign_closing": "Закрывающие",
            "sign_upd": "УПД поставщика",
            "other": "Другое",
        }.get(r.get("request_type", ""), r.get("request_type", ""))
        inv_num = r.get("invoice_number") or ""
        text = f"{status_emoji} {req_type_label}"
        if inv_num:
            text += f" №{inv_num}"
        b.button(text=text[:60], callback_data=f"rp_edo:view:{r['id']}")
    b.button(text="➕ Новый запрос ЭДО", callback_data="rp_edo:create")
    b.button(text="🔄 Обновить", callback_data="rp_edo:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


async def _show_edo_dashboard(
    target: Message | CallbackQuery,
    db: Database,
    user_id: int,
) -> None:
    """Показать дашборд «Бухгалтерия (УПД)» для РП."""
    requests = await db.list_edo_requests(requested_by=user_id, limit=30)
    counts = await db.count_edo_requests_by_user(user_id)

    if not requests:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Новый запрос ЭДО", callback_data="rp_edo:create")
        b.adjust(1)
        text = (
            "📄 <b>Бухгалтерия (УПД)</b>\n\n"
            "Нет ЭДО-запросов.\n\n"
            "Нажмите для создания нового:"
        )
        if isinstance(target, CallbackQuery):
            await target.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
        else:
            await target.answer(text, reply_markup=b.as_markup())
        return

    stats_parts: list[str] = []
    if counts.get("open", 0):
        stats_parts.append(f"⏳ Ожидают: {counts['open']}")
    if counts.get("done", 0):
        stats_parts.append(f"✅ Выполнено: {counts['done']}")

    text = (
        f"📄 <b>Бухгалтерия (УПД)</b> ({len(requests)})\n"
        f"{' | '.join(stats_parts)}\n\n"
        "Ваши ЭДО-запросы:"
    )

    if isinstance(target, CallbackQuery):
        await target.message.answer(text, reply_markup=_edo_requests_list_kb(requests))  # type: ignore[union-attr]
    else:
        await target.answer(text, reply_markup=_edo_requests_list_kb(requests))


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_EDO))
async def rp_edo_request(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Бухгалтерия (УПД)»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    u = message.from_user
    if not u:
        return
    await _show_edo_dashboard(message, db, u.id)


@router.callback_query(F.data == "rp_edo:create")
async def rp_edo_create(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание нового ЭДО-запроса."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(EdoRequestSG.request_type)
    await cb.message.answer(  # type: ignore[union-attr]
        "📄 <b>Новый запрос ЭДО</b>\n\n"
        "Выберите тип запроса:",
        reply_markup=edo_type_kb(),
    )


@router.callback_query(F.data == "rp_edo:refresh")
async def rp_edo_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд ЭДО-запросов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    u = cb.from_user
    if not u:
        return
    await _show_edo_dashboard(cb, db, u.id)


@router.callback_query(F.data.regexp(r"^rp_edo:view:\d+$"))
async def rp_edo_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка ЭДО-запроса с ответом бухгалтерии."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    edo_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_edo_request(edo_id)
    if not req:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return

    req_type_label = {
        "sign_invoice": "Подписать по ЭДО (счет)",
        "sign_closing": "Закрывающие по ЭДО",
        "sign_upd": "Подписать по ЭДО УПД поставщика",
        "other": "Другое",
    }.get(req.get("request_type", ""), req.get("request_type", ""))

    status_label = {
        "open": "⏳ Ожидает",
        "done": "✅ Выполнено",
    }.get(req.get("status", ""), req.get("status", ""))

    text = (
        f"📄 <b>ЭДО-запрос #{req['id']}</b>\n\n"
        f"📋 Тип: {req_type_label}\n"
    )
    if req.get("invoice_number"):
        text += f"🔢 Счёт №: {req['invoice_number']}\n"
    if req.get("description"):
        text += f"📝 Описание: {req['description']}\n"
    if req.get("comment"):
        text += f"💬 Комментарий: {req['comment']}\n"
    text += (
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {req.get('created_at', '-')[:10]}\n"
    )

    # Response from accounting
    if req.get("status") == "done":
        resp_type = {
            "signed": "✅ Подписано",
            "ok": "✅ ОК",
            "waiting": "⏳ Ожидание",
            "need_docs": "📄 Запрос документов",
        }.get(req.get("response_type", ""), req.get("response_type", ""))
        text += (
            f"\n<b>Ответ бухгалтерии:</b>\n"
            f"📋 Результат: {resp_type}\n"
        )
        if req.get("response_comment"):
            text += f"💬 Комментарий: {req['response_comment']}\n"
        if req.get("completed_at"):
            text += f"📅 Выполнено: {req['completed_at'][:10]}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_edo:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# СЧЕТ ЗАКРЫТ — дашборд (Этап 10)
#
# Показывает ENDED счета, сгруппированные по месяцам.
# Счётчик на кнопке: кол-во закрытых за текущий месяц.
# Поиск по номеру счёта / адресу.
#
# Callbacks:
#   rp_closed:view:\d+   — карточка закрытого счёта
#   rp_closed:refresh    — обновить список
#   rp_closed:all        — показать все (не только текущий месяц)
#   rp_closed:search     — поиск по номеру/адресу (inline → FSM)
# =====================================================================


def _ended_invoices_kb(
    invoices: list[dict[str, Any]],
    show_all: bool = False,
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком закрытых счетов."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        closed_date = (inv.get("updated_at") or inv.get("created_at", ""))[:10]
        text = f"🏁 №{inv.get('invoice_number', '?')} — {amount_str} ({closed_date})"
        b.button(text=text[:60], callback_data=f"rp_closed:view:{inv['id']}")
    if not show_all:
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
    b.button(text="🔍 Поиск", callback_data="rp_closed:search")
    b.button(text="🔄 Обновить", callback_data="rp_closed:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


def _current_month_start() -> str:
    """Return ISO date string for the 1st day of the current month."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_CLOSED))
async def rp_invoice_closed(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Счет закрыт»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    month_start = _current_month_start()
    invoices = await db.list_ended_invoices(month_start=month_start, limit=30)
    total_this_month = await db.count_ended_invoices(month_start=month_start)
    total_all = await db.count_ended_invoices()

    if not invoices and total_all == 0:
        b = InlineKeyboardBuilder()
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await message.answer(
            "🏁 <b>Счет закрыт</b>\n\n"
            "Нет закрытых счетов.",
            reply_markup=b.as_markup(),
        )
        return

    if not invoices:
        # No invoices this month but there are older ones
        b = InlineKeyboardBuilder()
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await message.answer(
            f"🏁 <b>Счет закрыт</b>\n\n"
            f"За текущий месяц: <b>0</b>\n"
            f"Всего: <b>{total_all}</b>\n\n"
            "Нажмите «Все» для просмотра:",
            reply_markup=b.as_markup(),
        )
        return

    await message.answer(
        f"🏁 <b>Счет закрыт</b>\n\n"
        f"За текущий месяц: <b>{total_this_month}</b>\n"
        f"Всего: <b>{total_all}</b>\n\n"
        "Закрытые счета (текущий месяц):",
        reply_markup=_ended_invoices_kb(invoices),
    )


@router.callback_query(F.data == "rp_closed:refresh")
async def rp_invoice_closed_refresh(cb: CallbackQuery, db: Database) -> None:
    """Обновить список «Счет закрыт»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")

    month_start = _current_month_start()
    invoices = await db.list_ended_invoices(month_start=month_start, limit=30)
    total_this_month = await db.count_ended_invoices(month_start=month_start)
    total_all = await db.count_ended_invoices()

    if not invoices:
        b = InlineKeyboardBuilder()
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"🏁 За текущий месяц: <b>0</b> | Всего: <b>{total_all}</b>",
            reply_markup=b.as_markup(),
        )
        return

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Счет закрыт</b>\n\n"
        f"За месяц: <b>{total_this_month}</b> | Всего: <b>{total_all}</b>\n\n"
        "Закрытые счета (текущий месяц):",
        reply_markup=_ended_invoices_kb(invoices),
    )


@router.callback_query(F.data == "rp_closed:all")
async def rp_invoice_closed_all(cb: CallbackQuery, db: Database) -> None:
    """Показать все закрытые счета (не только текущий месяц)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_ended_invoices(limit=50)
    if not invoices:
        await cb.message.answer("🏁 Нет закрытых счетов.")  # type: ignore[union-attr]
        return

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Все закрытые счета</b> ({len(invoices)})\n\n"
        "Нажмите для просмотра:",
        reply_markup=_ended_invoices_kb(invoices, show_all=True),
    )


@router.callback_query(F.data.regexp(r"^rp_closed:view:\d+$"))
async def rp_invoice_closed_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка закрытого счёта."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    is_credit = inv.get("is_credit") or inv.get("status") == "credit"
    payment_label = "🏦 Кред (кредит)" if is_credit else "💳 б/н (безналичный)"

    # Creator info
    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    text = (
        f"🏁 <b>Счёт №{inv['invoice_number']} — ЗАКРЫТ</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"💳 Оплата: {payment_label}\n"
        f"👤 Создал: {creator_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
        f"📅 Закрыт: {(inv.get('updated_at') or '-')[:10]}\n"
    )

    # Close conditions summary
    conditions = await db.check_close_conditions(invoice_id)
    c1 = "✅" if conditions["installer_ok"] else "⏳"
    c2 = "✅" if conditions["edo_signed"] else "⏳"
    c3 = "✅" if conditions["no_debts"] else "⏳"
    c4 = "✅" if conditions["zp_approved"] else "⏳"
    text += (
        f"\n<b>Условия:</b>\n"
        f"{c1} 1. Монтажник — Счет ОК\n"
        f"{c2} 2. ЭДО — подписано\n"
        f"{c3} 3. Долгов нет\n"
        f"{c4} 4. ЗП — расчёт ОК\n"
    )

    if inv.get("close_comment"):
        text += f"\n💬 Комментарий: {inv['close_comment']}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_closed:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "rp_closed:search")
async def rp_invoice_closed_search(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Поиск закрытого счёта → переход в FSM поиска."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    from ..states import InvoiceSearchSG
    await state.clear()
    await state.set_state(InvoiceSearchSG.value)
    await state.update_data(search_context="closed")
    await cb.message.answer(  # type: ignore[union-attr]
        "🔍 <b>Поиск счёта</b>\n\n"
        "Введите номер счёта или адрес для поиска:",
    )


# =====================================================================
# ЛИД НА РАСЧЕТ (Этап 11)
#
# Дашборд: список лидов + статистика + создание
# Создание: менеджер → описание → источник (inline) → вложения
# Источники: Свой клиент, Повторное обращение, Парсеры лидов, Другое
#
# Callbacks:
#   rp_lead:create    — начать создание нового лида
#   rp_lead:stats     — статистика конверсии
#   rp_lead:refresh   — обновить дашборд
#   rp_lead:view:\d+  — карточка лида
#   lead_src:*        — выбор источника лида (inline)
# =====================================================================

_LEAD_SOURCES = [
    ("Свой клиент", "own"),
    ("Повторное обращение", "repeat"),
    ("Парсеры лидов", "parsers"),
    ("Другое", "other"),
]


def _lead_source_kb() -> InlineKeyboardMarkup:
    """Inline-кнопки выбора источника лида."""
    b = InlineKeyboardBuilder()
    for label, key in _LEAD_SOURCES:
        b.button(text=label, callback_data=f"lead_src:{key}")
    b.button(text="❌ Отмена", callback_data="lead:cancel")
    b.adjust(2, 2, 1)
    return b.as_markup()


def _leads_list_kb(
    leads: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком лидов."""
    b = InlineKeyboardBuilder()
    for lead in leads:
        mgr_role = lead.get("assigned_manager_role", "")
        mgr_label = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }.get(mgr_role, "Менеджер")
        source = lead.get("lead_source", "—") or "—"
        responded = "✅" if lead.get("response_at") else "⏳"
        date_str = (lead.get("assigned_at") or lead.get("created_at", ""))[:10]
        text = f"{responded} {mgr_label} | {source[:15]} ({date_str})"
        b.button(text=text[:60], callback_data=f"rp_lead:view:{lead['id']}")
    b.button(text="➕ Новый лид", callback_data="rp_lead:create")
    b.button(text="📊 Статистика", callback_data="rp_lead:stats")
    b.button(text="🔄 Обновить", callback_data="rp_lead:refresh")
    b.adjust(1)
    return b.as_markup()


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_LEAD))
async def start_lead_to_project(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Лид на расчет»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    leads = await db.list_leads(limit=20)
    total = await db.count_leads_total()

    if not leads:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Новый лид", callback_data="rp_lead:create")
        b.adjust(1)
        await message.answer(
            "🎯 <b>Лид на расчет</b>\n\n"
            "Нет лидов.\n\n"
            "Нажмите для создания нового:",
            reply_markup=b.as_markup(),
        )
        return

    # Count responded
    n_responded = sum(1 for lead in leads if lead.get("response_at"))
    n_pending = len(leads) - n_responded

    stats = []
    if n_responded:
        stats.append(f"✅ Обработано: {n_responded}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await message.answer(
        f"🎯 <b>Лид на расчет</b> (всего: {total})\n"
        f"{' | '.join(stats)}\n\n"
        "Последние лиды:",
        reply_markup=_leads_list_kb(leads),
    )


@router.callback_query(F.data == "rp_lead:refresh")
async def lead_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд лидов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await state.clear()

    leads = await db.list_leads(limit=20)
    total = await db.count_leads_total()

    if not leads:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Новый лид", callback_data="rp_lead:create")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            "🎯 Нет лидов.",
            reply_markup=b.as_markup(),
        )
        return

    n_responded = sum(1 for lead in leads if lead.get("response_at"))
    n_pending = len(leads) - n_responded
    stats = []
    if n_responded:
        stats.append(f"✅ Обработано: {n_responded}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await cb.message.answer(  # type: ignore[union-attr]
        f"🎯 <b>Лид на расчет</b> (всего: {total})\n"
        f"{' | '.join(stats)}\n\n"
        "Последние лиды:",
        reply_markup=_leads_list_kb(leads),
    )


@router.callback_query(F.data.regexp(r"^rp_lead:view:\d+$"))
async def lead_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка лида."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    lead_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    # Get lead from lead_tracking table
    lead = await db.get_lead(lead_id)
    if not lead:
        await cb.message.answer("❌ Лид не найден.")  # type: ignore[union-attr]
        return

    mgr_role = lead.get("assigned_manager_role", "")
    mgr_label = {
        "manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(mgr_role, "Менеджер")

    # Get manager name
    mgr_name = mgr_label
    if lead.get("assigned_manager_id"):
        mgr_name = await get_initiator_label(db, int(lead["assigned_manager_id"]))
        mgr_name = f"{mgr_name} ({mgr_label})"

    responded_label = "✅ Обработан" if lead.get("response_at") else "⏳ Ожидает ответа"
    proc_time = ""
    if lead.get("processing_time_minutes"):
        minutes = lead["processing_time_minutes"]
        if minutes < 60:
            proc_time = f"\n⏱ Время отклика: {minutes} мин"
        else:
            hours = minutes // 60
            proc_time = f"\n⏱ Время отклика: {hours}ч {minutes % 60}мин"

    text = (
        f"🎯 <b>Лид #{lead['id']}</b>\n\n"
        f"👤 Менеджер: {mgr_name}\n"
        f"📌 Источник: {lead.get('lead_source', '—')}\n"
        f"📊 Статус: {responded_label}\n"
        f"📅 Назначен: {(lead.get('assigned_at') or '-')[:10]}\n"
    )
    if lead.get("response_at"):
        text += f"📅 Ответ: {lead['response_at'][:10]}\n"
    text += proc_time

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_lead:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "rp_lead:stats")
async def lead_stats(cb: CallbackQuery, db: Database) -> None:
    """Статистика конверсии лидов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    stats = await db.get_lead_stats()
    total = stats["total"]
    responded = stats["responded"]

    text = (
        f"📊 <b>Статистика лидов</b>\n\n"
        f"📋 Всего: <b>{total}</b>\n"
        f"✅ Обработано: <b>{responded}</b>\n"
        f"⏳ Ожидают: <b>{total - responded}</b>\n"
    )

    if stats["by_manager"]:
        text += "\n<b>По менеджерам:</b>\n"
        for entry in stats["by_manager"]:
            mgr_label = {
                "manager_kv": "КВ", "manager_kia": "КИА",
                "manager_npn": "НПН",
            }.get(entry.get("assigned_manager_role", ""), entry.get("assigned_manager_role", ""))
            avg_time = entry.get("avg_time")
            avg_str = ""
            if avg_time and avg_time > 0:
                if avg_time < 60:
                    avg_str = f" (ср. отклик: {int(avg_time)}мин)"
                else:
                    avg_str = f" (ср. отклик: {int(avg_time // 60)}ч)"
            text += f"  {mgr_label}: {entry['total']} лидов{avg_str}\n"

    if stats["by_source"]:
        text += "\n<b>По источникам:</b>\n"
        for entry in stats["by_source"]:
            source = entry.get("lead_source") or "—"
            text += f"  {source}: {entry['total']}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к лидам", callback_data="rp_lead:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# ---------- Создание нового лида ----------

@router.callback_query(F.data == "rp_lead:create")
async def lead_create_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание нового лида (Шаг 1: менеджер)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(LeadToProjectSG.pick_manager)
    await cb.message.answer(  # type: ignore[union-attr]
        "🎯 <b>Новый лид</b>\n\n"
        "Шаг 1/4: Выберите менеджера-получателя:",
        reply_markup=lead_pick_manager_kb(),
    )


@router.callback_query(F.data.startswith("lead_mgr:"))
async def lead_pick_manager(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    manager_role = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(manager_role=manager_role)
    await state.set_state(LeadToProjectSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 2/4: Опишите лид (описание + контактная информация):"
    )


@router.message(LeadToProjectSG.description)
async def lead_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(LeadToProjectSG.source)
    await message.answer(
        "Шаг 3/4: Выберите <b>источник лида</b>:",
        reply_markup=_lead_source_kb(),
    )


@router.callback_query(F.data == "lead:cancel")
async def lead_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Отмена создания лида."""
    await cb.answer("❌ Отменено")
    await state.clear()
    u = cb.from_user
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Создание лида отменено.", reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("lead_src:"))
async def lead_source_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбор источника лида из предустановленных."""
    await cb.answer()
    source_key = cb.data.split(":")[-1]  # type: ignore[union-attr]

    source_labels = {key: label for label, key in _LEAD_SOURCES}
    source_label = source_labels.get(source_key, source_key)

    if source_key == "other":
        await state.update_data(source_type="other")
        await cb.message.answer(  # type: ignore[union-attr]
            "Укажите <b>источник лида</b> вручную:"
        )
        return  # Stays in LeadToProjectSG.source, next text message will be handled

    await state.update_data(source=source_label, attachments=[])
    await state.set_state(LeadToProjectSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="lead:create")
    b.button(text="⏭ Без вложений", callback_data="lead:create")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📌 Источник: <b>{source_label}</b>\n\n"
        "Шаг 4/4: Прикрепите файлы/фото или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(LeadToProjectSG.source)
async def lead_source_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод источника лида (Другое)."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Укажите источник:")
        return
    await state.update_data(source=text, attachments=[])
    await state.set_state(LeadToProjectSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="lead:create")
    b.button(text="⏭ Без вложений", callback_data="lead:create")
    b.adjust(1)
    await message.answer(
        "Шаг 4/4: Прикрепите файлы/фото или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(LeadToProjectSG.attachments)
async def lead_attachments(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "lead:create")
async def lead_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    manager_role = data["manager_role"]
    description = data["description"]
    source = data.get("source", "")
    attachments = data.get("attachments", [])

    # Find manager ID
    manager_id = config.get_role_id(manager_role)
    if not manager_id:
        # Try to find by role in DB
        manager_id_resolved = await resolve_default_assignee(db, config, manager_role)
        if manager_id_resolved:
            manager_id = int(manager_id_resolved)

    if not manager_id:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Менеджер {manager_role} не найден."
        )
        await state.clear()
        return

    # Create project to link lead → invoice chain
    project = await db.create_project(
        title=f"Лид: {source[:50] if source else 'б/и'}",
        address=None,
        client=None,
        amount=None,
        deadline_iso=None,
        status="lead",
        manager_id=manager_id,
        rp_id=u.id,
    )
    project_id = int(project["id"])

    lead_id = await db.create_lead_tracking(
        assigned_by=u.id,
        assigned_manager_id=manager_id,
        assigned_manager_role=manager_role,
        lead_source=source,
        project_id=project_id,
    )

    task = await db.create_task(
        project_id=project_id,
        type_=TaskType.LEAD_TO_PROJECT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=manager_id,
        due_at_iso=None,
        payload={
            "lead_id": lead_id,
            "project_id": project_id,
            "description": description,
            "source": source,
            "manager_role": manager_role,
        },
    )
    await db.link_lead_tracking(lead_id, task_id=int(task["id"]))

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    role_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(manager_role, manager_role)

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"🎯 <b>Новый лид от РП</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}\n"
        f"📌 Источник: {source}\n"
    )

    from ..keyboards import task_actions_kb
    await notifier.safe_send(manager_id, msg, reply_markup=task_actions_kb(task))
    for a in attachments:
        await notifier.safe_send_media(manager_id, a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, manager_id)

    menu_role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Лид отправлен {role_label}.\n"
        f"📌 Источник: {source}",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(u.id),
                rp_messages=await db.count_rp_role_messages(u.id),
            ),
        ),
    )


# =====================================================================
# СМЕНА РОЛИ РП ↔ НПН (кнопки в первой строке меню)
# =====================================================================

@router.message(F.text.in_({RP_BTN_ROLE_NPN, RP_BTN_ROLE_RP_INACTIVE}))
async def role_switch_to_other(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Switch to the other role (RP->NPN or NPN->RP) when clicking the inactive role button."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return
    await state.clear()

    u = message.from_user
    if not u:
        return

    role = await _current_role(db, u.id)

    # Determine target role
    if role == Role.RP:
        target_role = Role.MANAGER_NPN
        role_label_str = "Менеджер НПН"
    else:
        target_role = Role.RP
        role_label_str = "РП"

    # Switch role in DB
    await db.switch_user_role(u.id, target_role)

    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        f"✅ Роль изменена на: <b>{role_label_str}</b>",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                target_role, is_admin=is_admin,
                unread=await db.count_unread_tasks(u.id),
                rp_tasks=await db.count_rp_role_tasks(u.id),
                rp_messages=await db.count_rp_role_messages(u.id),
            ),
        ),
    )


@router.message(F.text.in_({RP_BTN_ROLE_RP, RP_BTN_ROLE_NPN_ACTIVE}))
async def role_switch_already_active(message: Message, db: Database, config: Config) -> None:
    """User clicked the already-active role button — just refresh the menu."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return

    u = message.from_user
    if not u:
        return

    role = await _current_role(db, u.id)
    menu_role, isolated_role = await _current_menu(db, u.id)
    is_admin = u.id in (config.admin_ids or set())
    role_label_str = "РП" if role == Role.RP else "Менеджер НПН"

    await message.answer(
        f"Вы уже в роли <b>{role_label_str}</b>.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=is_admin,
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(u.id),
                rp_messages=await db.count_rp_role_messages(u.id),
            ),
        ),
    )


# =====================================================================
# ПОИСК СЧЕТА — обрабатывается в manager_new.py (принимает Role.RP)
# =====================================================================


# =====================================================================
# ОТВЕТ НА КП ОТ МЕНЕДЖЕРА — полный flow (Этап 5)
#
# Callback prefixes:
#   kp_review:\d+     — inline-кнопка из уведомления менеджера (открывает карточку)
#   kp_resp:view:\d+  — просмотр карточки задачи
#   kp_resp:yes:\d+   — Да → выбор типа оплаты
#   kp_resp:no:\d+    — Нет → FSM reject_comment
#   kp_resp:bn:\d+    — б/н → FSM documents
#   kp_resp:cred:\d+  — Кред → FSM comment (без документов)
#   kp_resp:back      — назад к списку CHECK_KP задач
#   kp_resp:issued    — Выставленные счета
#   kp_issued:view:\d+ — просмотр выставленного счёта
# =====================================================================


async def _show_kp_task_card(
    target: CallbackQuery,
    db: Database,
    task_id: int,
) -> None:
    """Показать карточку CHECK_KP задачи с кнопками Да/Нет."""
    task = await db.get_task(task_id)
    if not task:
        await target.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    payload = json.loads(task.get("payload_json") or "{}")
    invoice_number = payload.get("invoice_number", "?")
    address = payload.get("address", "—")
    amount = payload.get("amount", 0)
    comment = payload.get("comment", "")
    manager_role = payload.get("manager_role", "manager")
    manager_id = payload.get("manager_id")

    mgr_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(manager_role, "Менеджер")

    # Get manager name
    mgr_name = mgr_label
    if manager_id:
        mgr_name = await get_initiator_label(db, int(manager_id))
        mgr_name = f"{mgr_name} ({mgr_label})"

    try:
        amount_str = f"{float(amount):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{amount}₽"

    text = (
        f"📋 <b>Проверка КП — карточка</b>\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {address}\n"
        f"💰 Сумма: {amount_str}\n"
        f"👤 От: {mgr_name}\n"
    )
    if comment:
        text += f"💬 Комментарий: {comment}\n"

    text += (
        f"\n📅 Создан: {task.get('created_at', '-')[:10]}\n"
        f"\n<b>Ваше решение:</b>"
    )

    await target.message.answer(  # type: ignore[union-attr]
        text,
        reply_markup=kp_response_kb(task_id),
    )

    # Show attached КП documents
    attachments = await db.list_attachments(int(task["id"]))
    if attachments:
        # Show docs inline (just list them, user sees them in notification)
        att_text = f"📎 Вложения КП ({len(attachments)} файл(ов))"
        await target.message.answer(att_text)  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^kp_review:\d+$"))
async def kp_review_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Inline-кнопка из уведомления менеджера → показать карточку задачи."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _show_kp_task_card(cb, db, task_id)


@router.callback_query(F.data.regexp(r"^kp_resp:view:\d+$"))
async def kp_view_task(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Просмотр карточки CHECK_KP задачи из списка."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _show_kp_task_card(cb, db, task_id)


@router.callback_query(F.data == "kp_resp:back")
async def kp_back_to_list(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Назад к списку входящих CHECK_KP задач."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    u = cb.from_user
    if not u:
        return

    tasks = await db.list_check_kp_tasks(u.id)
    if not tasks:
        await cb.message.answer(  # type: ignore[union-attr]
            "📋 Входящих запросов на проверку КП нет ✅",
        )
        return

    mgr_counts: dict[str, int] = {}
    for t in tasks:
        payload = json.loads(t.get("payload_json") or "{}")
        mrole = payload.get("manager_role", "manager")
        lbl = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}.get(mrole, "Менеджер")
        mgr_counts[lbl] = mgr_counts.get(lbl, 0) + 1
    summary_parts = [f"{lbl}: {cnt}" for lbl, cnt in mgr_counts.items()]

    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 <b>Проверка КП / Выставление Счета</b>\n\n"
        f"Входящих запросов: <b>{len(tasks)}</b>\n"
        f"По менеджерам: {', '.join(summary_parts)}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=kp_task_list_kb(tasks, show_issued=True),
    )


# ---------- ДА → Выбор типа оплаты ----------

@router.callback_query(F.data.regexp(r"^kp_resp:yes:\d+$"))
async def kp_resp_yes(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП нажал Да → сбор документов (Счёт, Договор, Приложение)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(KpReviewSG.documents)
    await state.update_data(task_id=task_id, documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "📋 <b>Одобрение КП</b>\n\n"
        "Прикрепите готовые документы:\n"
        "• Счёт\n• Договор\n• Приложение к договору\n\n"
        "Отправляйте файлы по одному.",
    )


# ---------- б/н (безналичный) → Документы → Комментарий ----------

@router.callback_query(F.data.regexp(r"^kp_resp:bn:\d+$"))
async def kp_resp_bn(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """б/н выбран → FSM: сбор документов (Счёт, Договор, Приложение)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(KpReviewSG.documents)
    await state.update_data(task_id=task_id, payment_type="bn", documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "📋 <b>Ответ на КП (б/н)</b>\n\n"
        "Прикрепите готовые документы:\n"
        "• Счёт\n"
        "• Договор\n"
        "• Приложение к договору\n\n"
        "Отправляйте файлы по одному.",
    )


@router.message(KpReviewSG.documents)
async def kp_review_documents(message: Message, state: FSMContext) -> None:
    """Сбор документов для б/н ответа на КП."""
    data = await state.get_data()
    documents: list[dict[str, Any]] = data.get("documents", [])

    if message.document:
        documents.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        documents.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    else:
        if documents:
            # Текстовое сообщение = переход к комментарию
            await state.update_data(documents=documents)
            await state.set_state(KpReviewSG.comment)
            await message.answer("Добавьте <b>комментарий</b> (или «—» для пропуска):")
            return
        await message.answer("Пришлите файл или фото:")
        return

    await state.update_data(documents=documents)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Далее (комментарий)", callback_data="kp_review:next")
    b.adjust(1)
    await message.answer(
        f"📎 Принял. Документов: <b>{len(documents)}</b>.\n"
        "Ещё файлы или нажмите «Далее».",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "kp_review:next")
async def kp_review_next(cb: CallbackQuery, state: FSMContext) -> None:
    """Кнопка 'Далее' → переход к комментарию."""
    await cb.answer()
    await state.set_state(KpReviewSG.comment)
    await cb.message.answer(  # type: ignore[union-attr]
        "Добавьте <b>комментарий</b> (или «—» для пропуска):"
    )


# ---------- Кред (кредит) → Комментарий (без документов) ----------

@router.callback_query(F.data.regexp(r"^kp_resp:cred:\d+$"))
async def kp_resp_cred(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Кред выбран → FSM: комментарий (документы не требуются)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(KpReviewSG.comment)
    await state.update_data(task_id=task_id, payment_type="cred", documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "🏦 <b>Ответ на КП (Кред)</b>\n\n"
        "Документы не требуются (банк оформляет самостоятельно).\n\n"
        "Добавьте <b>комментарий</b> (или «—» для пропуска):",
    )


# ---------- Комментарий (Да — б/н или Кред) → Финализация ----------

@router.message(KpReviewSG.comment)
async def kp_review_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Финализация ответа «Да» → РП выставляет счёт."""
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    task_id = data["task_id"]
    documents = data.get("documents", [])

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json") or "{}")
    manager_id = payload.get("manager_id")
    invoice_number = payload.get("invoice_number", "?")
    invoice_id = payload.get("invoice_id")  # set only for existing invoices
    manager_role = payload.get("manager_role", "manager")

    # Mark task as done
    await db.update_task_status(task_id, TaskStatus.DONE)

    # РП выставляет счёт: создаёт invoice (или обновляет существующий)
    # Статус б/н или кредит определяется из Импорт ОП, не здесь
    is_new = payload.get("is_new_invoice", True)

    if is_new and not invoice_id:
        # Создаём project + invoice (РП выставляет счёт)
        project = await db.create_project(
            title=f"Счёт: {invoice_number}",
            address=payload.get("address") or None,
            client=payload.get("client_name") or None,
            amount=float(payload.get("amount") or 0) or None,
            deadline_iso=None,
            status="active",
            manager_id=int(manager_id) if manager_id else None,
            rp_id=message.from_user.id,
        )
        project_id = int(project["id"])

        try:
            invoice_id = await db.create_invoice(
                invoice_number=invoice_number,
                project_id=project_id,
                created_by=int(manager_id) if manager_id else message.from_user.id,
                creator_role=manager_role,
                client_name=payload.get("client_name", ""),
                object_address=payload.get("address", ""),
                amount=payload.get("amount", 0),
                description=payload.get("comment", ""),
                payment_terms=payload.get("payment_type", ""),
                deadline_days=payload.get("deadline_days"),
            )
        except ValueError:
            await message.answer(
                f"⚠️ Счёт №{invoice_number} уже существует в базе."
            )
            await state.clear()
            return

        # Статус pending_payment, документы
        upd: dict[str, Any] = {"status": InvoiceStatus.PENDING_PAYMENT}
        if documents:
            upd["documents_json"] = json.dumps(documents, ensure_ascii=False)
        await db.update_invoice(invoice_id, **upd)

        # Лид → "счет выставлен" (фиксация менеджера + даты)
        # Если lead_tracking записи нет — создаёт привязку менеджера к счёту
        try:
            await db.update_lead_to_invoice_issued(
                project_id, invoice_id,
                manager_id=int(manager_id) if manager_id else None,
                manager_role=manager_role,
            )
        except Exception:
            log.warning("Failed to update lead status for project_id=%s", project_id)

    elif invoice_id:
        # Существующий invoice — обновляем статус + документы
        upd2: dict[str, Any] = {"status": InvoiceStatus.PENDING_PAYMENT}
        if documents:
            upd2["documents_json"] = json.dumps(documents, ensure_ascii=False)
        await db.update_invoice(invoice_id, **upd2)

        # Привязка менеджера к счёту (если нет lead_tracking)
        inv = await db.get_invoice(invoice_id)
        if inv and inv.get("project_id"):
            try:
                await db.update_lead_to_invoice_issued(
                    int(inv["project_id"]), invoice_id,
                    manager_id=int(manager_id) if manager_id else None,
                    manager_role=manager_role or inv.get("creator_role"),
                )
            except Exception:
                log.warning("Failed to update lead status for invoice_id=%s", invoice_id)

    # Notify manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)

        msg = (
            f"📋 <b>Счёт №{invoice_number} выставлен</b>\n"
            f"👤 От: {initiator}\n\n"
            f"РП проверил КП и подготовил документы.\n"
        )

        if comment:
            msg += f"\n💬 Комментарий РП: {comment}"

        # Кнопка "Задача ок" для менеджера
        confirm_kb = InlineKeyboardBuilder()
        confirm_kb.button(
            text="✅ Задача ок",
            callback_data=f"mgr_kp_ok:{task_id}",
        )
        await notifier.safe_send(
            int(manager_id), msg, reply_markup=confirm_kb.as_markup(),
        )

        # Send attached documents
        for doc in documents:
            await notifier.safe_send_media(
                int(manager_id), doc["file_type"], doc["file_id"],
                caption=doc.get("caption"),
            )

        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Счёт №{invoice_number} выставлен. Менеджер уведомлён.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(message.from_user.id),
                rp_messages=await db.count_rp_role_messages(message.from_user.id),
            ),
        ),
    )


# ---------- НЕТ → Комментарий → Отклонение ----------

@router.callback_query(F.data.regexp(r"^kp_resp:no:\d+$"))
async def kp_resp_no(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП нажал Нет → FSM: ввод комментария к отклонению."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(KpReviewSG.reject_comment)
    await state.update_data(task_id=task_id)

    await cb.message.answer(  # type: ignore[union-attr]
        "❌ <b>Отклонение КП</b>\n\n"
        "Укажите <b>причину отклонения</b> (комментарий):",
    )


@router.message(KpReviewSG.reject_comment)
async def kp_reject_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Финализация отклонения (Нет)."""
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if not comment:
        await message.answer("Напишите причину отклонения:")
        return

    data = await state.get_data()
    task_id = data["task_id"]

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json") or "{}")
    manager_id = payload.get("manager_id")
    invoice_number = payload.get("invoice_number", "?")
    invoice_id = payload.get("invoice_id")

    # Mark task as rejected
    await db.update_task_status(task_id, TaskStatus.REJECTED)

    # Update invoice status
    if invoice_id:
        await db.update_invoice(invoice_id, status=InvoiceStatus.REJECTED)

    # Notify manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)
        msg = (
            f"❌ <b>КП по счёту №{invoice_number} отклонён</b>\n"
            f"👤 От: {initiator}\n\n"
            f"💬 Причина: {comment}\n"
        )
        await notifier.safe_send(int(manager_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"❌ КП по счёту №{invoice_number} отклонён. Менеджер уведомлён.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(message.from_user.id),
                rp_messages=await db.count_rp_role_messages(message.from_user.id),
            ),
        ),
    )


# ---------- ВЫСТАВЛЕННЫЕ СЧЕТА ----------

@router.callback_query(F.data == "kp_resp:issued")
async def kp_issued_list(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Показать «Выставленные счета» — обработанные РП."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_rp_issued_invoices(limit=30)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "📑 <b>Выставленные счета</b>\n\nСписок пуст.",
        )
        return

    # Count by type
    n_bn = sum(1 for inv in invoices if not inv.get("is_credit"))
    n_cred = sum(1 for inv in invoices if inv.get("is_credit") or inv.get("status") == "credit")

    header = f"📑 <b>Выставленные счета</b> ({len(invoices)})\n"
    if n_bn > 0:
        header += f"💳 б/н: {n_bn}"
    if n_cred > 0:
        header += f"  🏦 Кред: {n_cred}"
    header += "\n\nНажмите для просмотра:"

    await cb.message.answer(  # type: ignore[union-attr]
        header,
        reply_markup=kp_issued_list_kb(invoices),
    )


@router.callback_query(F.data.regexp(r"^kp_issued:view:\d+$"))
async def kp_issued_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр карточки выставленного счёта."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    is_credit = inv.get("is_credit") or inv.get("status") == "credit"
    payment_label = "🏦 Кред (кредит)" if is_credit else "💳 б/н (безналичный)"

    status_label = _invoice_status_label(inv.get("status"))

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"💳 Оплата: {payment_label}\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    if not is_credit:
        conditions = await db.check_close_conditions(invoice_id)
        c1 = "✅" if conditions["installer_ok"] else "⏳"
        c2 = "✅" if conditions["edo_signed"] else "⏳"
        c3 = "✅" if conditions["no_debts"] else "⏳"
        text += (
            f"\n<b>Условия закрытия:</b>\n"
            f"{c1} 1. Монтажник — Счет ОК\n"
            f"{c2} 2. ЭДО — подписано\n"
            f"{c3} 3. Долгов нет\n"
        )

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="kp_resp:issued")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
