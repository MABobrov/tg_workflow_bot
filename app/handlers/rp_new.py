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
from aiogram.types import CallbackQuery, Message
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
from ..services.notifier import Notifier
from ..states import (
    EdoRequestSG,
    KpReviewResponseSG,
    KpReviewSG,
    LeadToProjectSG,
    ManagerChatProxySG,
)
from ..utils import answer_service, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


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
        reply_markup=tasks_kb(tasks),
    )


# =====================================================================
# СЧЕТ В РАБОТУ (мониторинг для РП)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_START))
async def rp_invoice_start_monitor(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    in_progress = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS, limit=50)
    paid = await db.list_invoices(status=InvoiceStatus.PAID, limit=50)
    invoices = list(in_progress) + list(paid)
    if not invoices:
        await answer_service(message, "💼 Нет счетов «В работе».", delay_seconds=60)
        return
    header_parts: list[str] = []
    if in_progress:
        header_parts.append(f"🔄 В работе: {len(in_progress)}")
    if paid:
        header_parts.append(f"✅ Оплачены: {len(paid)}")
    await message.answer(
        f"💼 <b>Счета В Работе</b> ({len(invoices)}):\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(invoices, action_prefix="rpinv"),
    )


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

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ожидает", "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен", "on_hold": "⏸ Отложен", "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

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
# СЧЕТА НА ОПЛАТУ (💳 — мониторинг PENDING_PAYMENT + IN_PROGRESS)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICES_PAY))
async def rp_invoices_pay(message: Message, db: Database) -> None:
    """Show invoices pending payment and in-progress for RP monitoring."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return

    pending = await db.list_invoices(status=InvoiceStatus.PENDING_PAYMENT, limit=30)
    in_progress = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS, limit=30)
    all_inv = list(pending) + list(in_progress)

    if not all_inv:
        await answer_service(message, "💳 Нет счетов, ожидающих оплаты.", delay_seconds=60)
        return

    n_pending = len(pending)
    n_progress = len(in_progress)

    header_parts = []
    if n_pending:
        header_parts.append(f"⏳ Ожидают: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")

    await message.answer(
        f"💳 <b>Счета на оплату</b>\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv"),
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
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv"),
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
        reply_markup=tasks_kb(issues),
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
    await message.answer(
        "👤 <b>Менеджер 2 (КИА)</b>\n\nВыберите действие:",
        reply_markup=rp_chat_submenu("⬅️ Назад"),
    )


# =====================================================================
# МОНТАЖНАЯ ГР. — chat-proxy
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_MONTAZH) or (m.text or "").strip().startswith(RP_SUBBTN_MONTAZH))
async def rp_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await message.answer(
        "🔧 <b>Монтажная гр.</b>\n\nВыберите действие:",
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
        text = (
            "💼 <b>Счета в Работе</b>\n\n"
            "Нет активных счетов ✅"
        )
        if isinstance(target, CallbackQuery):
            await target.message.answer(text)  # type: ignore[union-attr]
        else:
            await target.answer(text)
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
        header_parts.append(f"⏳ Ожидают оплаты: {n_pending}")
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

    if isinstance(target, CallbackQuery):
        await target.message.answer(  # type: ignore[union-attr]
            text,
            reply_markup=invoices_work_list_kb(invoices),
        )
    else:
        await target.answer(
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

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ожидает оплаты",
        "in_progress": "🔄 В работе", "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен", "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие", "ended": "🏁 Счет End",
        "credit": "🏦 Кредит",
    }.get(inv["status"], inv["status"])

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

    # Payment file info
    if inv.get("payment_file_id"):
        text += "\n💸 Платёжка: прикреплена ✅\n"
    if inv.get("payment_comment"):
        text += f"💬 Коммент. к оплате: {inv['payment_comment']}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_work:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# БУХГАЛТЕРИЯ (УПД) — ЭДО-запрос от РП
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_EDO))
async def rp_edo_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(EdoRequestSG.request_type)
    await message.answer(
        "📄 <b>Бухгалтерия (УПД)</b>\n\n"
        "Выберите тип запроса:",
        reply_markup=edo_type_kb(),
    )


# =====================================================================
# СЧЕТ ЗАКРЫТ (placeholder)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_CLOSED))
async def rp_invoice_closed(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    u = message.from_user
    if not u:
        return
    menu_role, isolated_role = await _current_menu(db, u.id)
    await message.answer(
        "🚧 <b>В разработке</b>\n\n"
        "Раздел «Счет закрыт» будет доступен в ближайшем обновлении.",
        reply_markup=private_only_reply_markup(
            message,
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
# ЛИД НА РАСЧЕТ (LeadToProjectSG)  — ранее «Лид в проект»
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_LEAD))
async def start_lead_to_project(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(LeadToProjectSG.pick_manager)
    await message.answer(
        "🎯 <b>Лид в проект</b>\n\n"
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
    await message.answer("Шаг 3/4: Укажите <b>источник лида</b> (сайт / звонок / рекомендация / ...):")


@router.message(LeadToProjectSG.source)
async def lead_source(message: Message, state: FSMContext) -> None:
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

    lead_id = await db.create_lead_tracking(
        assigned_by=u.id,
        assigned_manager_id=manager_id,
        assigned_manager_role=manager_role,
        lead_source=source,
    )

    task = await db.create_task(
        project_id=None,
        type_=TaskType.LEAD_TO_PROJECT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=manager_id,
        due_at_iso=None,
        payload={
            "lead_id": lead_id,
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
        f"✅ Лид отправлен {role_label}.",
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
        from ..services.notifier import Notifier
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
    """РП нажал Да → выбор системы оплаты (б/н или Кред)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    payload = json.loads(task.get("payload_json") or "{}")
    invoice_number = payload.get("invoice_number", "?")
    try:
        amount_str = f"{float(payload.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{payload.get('amount', 0)}₽"

    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 <b>Счёт №{invoice_number}</b> — {amount_str}\n\n"
        "Выберите <b>систему оплаты</b>:",
        reply_markup=kp_payment_type_kb(task_id),
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
    """Финализация ответа «Да» (б/н или Кред)."""
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    task_id = data["task_id"]
    payment_type = data.get("payment_type", "bn")  # "bn" or "cred"
    documents = data.get("documents", [])

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json") or "{}")
    manager_id = payload.get("manager_id")
    invoice_number = payload.get("invoice_number", "?")
    invoice_id = payload.get("invoice_id")

    # Mark task as done
    await db.update_task_status(task_id, TaskStatus.DONE)

    # Update invoice based on payment type
    is_credit = payment_type == "cred"

    if invoice_id:
        if is_credit:
            # Кред: is_credit=1, status=credit, документы не нужны
            await db.update_invoice(
                invoice_id,
                is_credit=1,
                status=InvoiceStatus.CREDIT,
            )
        else:
            # б/н: обычный flow, документы прикреплены, status=pending_payment
            await db.update_invoice(
                invoice_id,
                is_credit=0,
                status=InvoiceStatus.PENDING_PAYMENT,
                documents_json=json.dumps(documents, ensure_ascii=False) if documents else None,
            )

    # Notify manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)

        if is_credit:
            msg = (
                f"🏦 <b>Счёт №{invoice_number} — Кред</b>\n"
                f"👤 От: {initiator}\n\n"
                f"РП одобрил КП.\n"
                f"Система оплаты: <b>Кредит</b>\n"
                f"Документы оформляет банк.\n"
            )
        else:
            msg = (
                f"📋 <b>Документы по счёту №{invoice_number}</b>\n"
                f"👤 От: {initiator}\n\n"
                f"РП проверил КП и подготовил:\n"
                f"• Счёт, Договор, Приложение\n"
                f"Система оплаты: <b>б/н</b>\n"
            )

        if comment:
            msg += f"\n💬 Комментарий РП: {comment}"

        await notifier.safe_send(int(manager_id), msg)

        # Send attached documents (only for б/н)
        if not is_credit:
            for doc in documents:
                await notifier.safe_send_media(
                    int(manager_id), doc["file_type"], doc["file_id"],
                    caption=doc.get("caption"),
                )

        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    credit_label = " (Кред)" if is_credit else ""
    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Ответ отправлен менеджеру по счёту №{invoice_number}{credit_label}.",
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

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ожидает оплаты",
        "in_progress": "🔄 В работе", "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен", "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие", "ended": "🏁 Счет End",
        "credit": "🏦 Кредит",
    }.get(inv["status"], inv["status"])

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
