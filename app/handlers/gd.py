"""Handlers specific to the GD (Генеральный директор) role.

Phase 1:
- "Срочно для ГД" — shows list of open URGENT_GD + PAYMENT_CONFIRM tasks
- "Синхронизация данных" — triggers Google Sheets resync from GD main menu

Phase 2:
- Chat-proxy buttons: Чат с РП, Замеры, Бухгалтерия, Монтажная гр., Отд.Продаж,
  КВ Кред, КИА Кред, НПН Кред
"""

from __future__ import annotations

import html
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import SummaryCb, TaskCb
from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import (
    gd_sales_submenu,
    gd_chat_write_to_kb_universal,
    invoice_select_kb,
    GD_BTN_ACCOUNTING,
    GD_BTN_CHAT_RP,
    GD_BTN_INVOICES,
    GD_BTN_INVOICES_WORK,
    GD_BTN_KIA_CRED,
    GD_BTN_NPN_CRED,
    GD_BTN_KV_CRED,
    GD_SUBBTN_KIA_CRED,
    GD_SUBBTN_NPN_CRED,
    GD_SUBBTN_KV_CRED,
    GD_BTN_MONTAZH,
    GD_BTN_SALES,
    GD_BTN_SEARCH_INVOICE,
    GD_BTN_DAILY_SUMMARY,
    GD_BTN_SYNC,
    GD_BTN_ZAMERY,
    main_menu,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..services.sheets_sync import export_to_sheets, import_from_source_sheet
from ..states import ChatProxySG, InvoiceSearchSG, SalesWriteSG
from .chat_proxy import channel_label, enter_chat_menu, gd_channel_menu
from ..utils import (
    answer_service,
    format_dt_iso,
    get_initiator_label,
    parse_roles,
    private_only_reply_markup,
    project_status_label,
    refresh_recipient_keyboard,
    task_status_label,
    task_type_label,
    try_json_loads,
)
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

GD_ACCESS_ROLES = [Role.GD, Role.TD]
SALES_SOURCE_ROLES = {Role.RP, Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN}


async def _search_invoice_tasks_by_criteria(
    db: Database,
    criteria: str,
    value: str,
    *,
    limit: int = 20,
) -> list[dict[str, object]]:
    fields = [criteria]
    if criteria == "project":
        fields = ["address", "object_address"]

    found_by_id: dict[int, dict[str, object]] = {}
    for field in fields:
        rows = await db.search_tasks_by_payload(
            field=field,
            value=value,
            type_filter=[TaskType.INVOICE_PAYMENT, TaskType.SUPPLIER_PAYMENT],
            limit=limit,
        )
        for row in rows:
            found_by_id[int(row["id"])] = row
            if len(found_by_id) >= limit:
                break
        if len(found_by_id) >= limit:
            break
    return list(found_by_id.values())[:limit]


async def _is_sales_not_urgent_task(db: Database, task: dict[str, object]) -> bool:
    payload = try_json_loads(task.get("payload_json"))
    sender_roles = set(parse_roles(str(payload.get("sender_role") or "")))
    if sender_roles & SALES_SOURCE_ROLES:
        return True

    sender_id = payload.get("sender_id") or task.get("created_by")
    try:
        sender_id_int = int(sender_id) if sender_id is not None else None
    except (TypeError, ValueError):
        sender_id_int = None
    if sender_id_int is None:
        return False

    sender = await db.get_user_optional(sender_id_int)
    if not sender:
        return False
    return bool(set(parse_roles(sender.role)) & SALES_SOURCE_ROLES)


# ---------------------------------------------------------------------------
# "📥 Входящие для ГД" — all incoming tasks for GD
# ---------------------------------------------------------------------------

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие для ГД"))
async def gd_inbox_all(message: Message, db: Database, config: Config) -> None:
    """Show GD all open tasks (urgent, payment confirm, GD_TASK, etc.)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    all_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        limit=50,
        exclude_created_by=user_id,
    )

    is_admin = user_id in (config.admin_ids or set())

    if not all_tasks:
        await answer_service(
            message,
            "✅ Нет входящих задач.",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id))),
        )
        return

    # Count by type for summary
    n_urgent = sum(1 for t in all_tasks if t.get("type") == TaskType.URGENT_GD)
    n_payment = sum(1 for t in all_tasks if t.get("type") == TaskType.PAYMENT_CONFIRM)
    n_invoice = sum(1 for t in all_tasks if t.get("type") == TaskType.INVOICE_PAYMENT)
    n_other = len(all_tasks) - n_urgent - n_payment - n_invoice

    parts = []
    if n_urgent:
        parts.append(f"🚨 Срочных: {n_urgent}")
    if n_payment:
        parts.append(f"💰 Подтв.оплат: {n_payment}")
    if n_invoice:
        parts.append(f"📄 Счетов: {n_invoice}")
    if n_other:
        parts.append(f"📋 Прочих: {n_other}")

    text = (
        f"<b>📥 Входящие для ГД</b> ({len(all_tasks)})\n"
        f"{' | '.join(parts)}\n\n"
        "Выберите задачу:"
    )

    await message.answer(text, reply_markup=tasks_kb(all_tasks, back_callback="nav:home"))




# ---------------------------------------------------------------------------
# "Счета на Оплату" — show INVOICE_PAYMENT tasks for GD
# ---------------------------------------------------------------------------

@router.message(F.text.startswith(GD_BTN_INVOICES))
async def gd_invoices(message: Message, db: Database, config: Config) -> None:
    """Show only invoice_payment tasks (requests from RP/Manager)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    # Only show invoice_payment tasks — actual payment requests
    invoice_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.INVOICE_PAYMENT,
        limit=100,
    )

    is_admin = user_id in (config.admin_ids or set())

    if not invoice_tasks:
        await answer_service(
            message,
            "✅ Нет счетов на оплату.",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id))),
        )
        return

    b = InlineKeyboardBuilder()
    for t in invoice_tasks:
        payload = try_json_loads(t.get("payload_json") or "{}")
        inv_num = payload.get("invoice_number") or f"#{t['id']}"
        label = f"💰 №{inv_num}"
        b.button(text=label[:60], callback_data=TaskCb(task_id=int(t["id"]), action="open").pack())
    b.button(text="🔄 Обновить", callback_data="gd_inv:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    header = f"<b>Счета на Оплату</b> ({len(invoice_tasks)})"
    header += "\n\nНажмите на счёт для просмотра:"

    await message.answer(header, reply_markup=b.as_markup())


@router.callback_query(F.data == "gd_inv:refresh")
async def gd_invoices_refresh(cb: CallbackQuery, db: Database) -> None:
    """Refresh the 'Счета на Оплату' dashboard (inline)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer("🔄 Обновлено")

    user_id = cb.from_user.id  # type: ignore[union-attr]

    invoice_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.INVOICE_PAYMENT,
        limit=100,
    )

    b = InlineKeyboardBuilder()
    for t in invoice_tasks:
        payload = try_json_loads(t.get("payload_json") or "{}")
        inv_num = payload.get("invoice_number") or f"#{t['id']}"
        label = f"💰 №{inv_num}"
        b.button(text=label[:60], callback_data=TaskCb(task_id=int(t["id"]), action="open").pack())
    b.button(text="🔄 Обновить", callback_data="gd_inv:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    if not invoice_tasks:
        header = "✅ Нет счетов на оплату."
    else:
        header = f"<b>Счета на Оплату</b> ({len(invoice_tasks)})"
        header += "\n\nНажмите на счёт для просмотра:"

    await cb.message.answer(header, reply_markup=b.as_markup())  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# "📊 Счета в работе" — full invoice list for GD (same as RP dashboard)
# ---------------------------------------------------------------------------

@router.message(F.text.startswith(GD_BTN_INVOICES_WORK))
async def gd_invoices_work(message: Message, db: Database) -> None:
    """Show full list of invoices in work (same view as RP)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    invoices = await db.list_invoices_in_work(limit=50, only_regular=True)

    if not invoices:
        await answer_service(message, "✅ Нет счетов в работе.", delay_seconds=60)
        return

    n_pending = sum(1 for inv in invoices if inv.get("status") == "pending")
    n_progress = sum(1 for inv in invoices if inv.get("status") == "in_progress")
    n_paid = sum(1 for inv in invoices if inv.get("status") == "paid")

    header_parts: list[str] = []
    if n_pending:
        header_parts.append(f"⏳ Ждёт: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")
    if n_paid:
        header_parts.append(f"✅ Оплачены: {n_paid}")

    b = InlineKeyboardBuilder()
    for inv in invoices[:20]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        status_icon = {"pending": "⏳", "in_progress": "🔄", "paid": "✅"}.get(inv["status"], "")
        try:
            amt = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amt = ""
        label = f"{status_icon} №{num}"
        if addr:
            label += f" — {addr}"
        if amt:
            label += f" ({amt})"
        b.button(text=label[:60], callback_data=f"gd_work:view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="gd_work:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    text = (
        f"📊 <b>Счета в работе</b> ({len(invoices)})\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите на счёт для просмотра:"
    )
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "gd_work:refresh")
async def gd_invoices_work_refresh(cb: CallbackQuery, db: Database) -> None:
    """Refresh the invoices-in-work dashboard for GD."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer("🔄 Обновлено")

    invoices = await db.list_invoices_in_work(limit=50, only_regular=True)
    if not invoices:
        await cb.message.answer("✅ Нет счетов в работе.")  # type: ignore[union-attr]
        return

    n_pending = sum(1 for inv in invoices if inv.get("status") == "pending")
    n_progress = sum(1 for inv in invoices if inv.get("status") == "in_progress")
    n_paid = sum(1 for inv in invoices if inv.get("status") == "paid")

    header_parts: list[str] = []
    if n_pending:
        header_parts.append(f"⏳ Ждёт: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")
    if n_paid:
        header_parts.append(f"✅ Оплачены: {n_paid}")

    b = InlineKeyboardBuilder()
    for inv in invoices[:20]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        status_icon = {"pending": "⏳", "in_progress": "🔄", "paid": "✅"}.get(inv["status"], "")
        try:
            amt = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amt = ""
        label = f"{status_icon} №{num}"
        if addr:
            label += f" — {addr}"
        if amt:
            label += f" ({amt})"
        b.button(text=label[:60], callback_data=f"gd_work:view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="gd_work:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    text = (
        f"📊 <b>Счета в работе</b> ({len(invoices)})\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите на счёт для просмотра:"
    )
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^gd_work:view:\d+$"))
async def gd_invoices_work_view(cb: CallbackQuery, db: Database) -> None:
    """Invoice card from GD work dashboard — full cost card."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ждёт подтверждения",
        "in_progress": "🔄 В работе", "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен", "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие", "ended": "🏁 Счет End",
        "credit": "🏦 Кредит",
    }.get(inv["status"], inv["status"])

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"📊 Статус: {status_label}\n"
        f"👤 Создал: {creator_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    # Full cost card
    from ..utils import format_cost_card
    cost = await db.get_full_invoice_cost_card(invoice_id)
    if cost and cost.get("total_cost", 0) > 0:
        text += format_cost_card(inv, cost)

    b = InlineKeyboardBuilder()
    b.button(text="📊 Себестоимость", callback_data=f"inv_stats:{invoice_id}")
    b.button(text="💬 Сообщения", callback_data=f"inv_msgs:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="gd_work:refresh")
    b.adjust(2, 1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# "Поиск счёта" — search invoices by criteria
# ---------------------------------------------------------------------------

@router.message(
    lambda m: (m.text or "").strip() in {GD_BTN_SEARCH_INVOICE, "Поиск Счета"}
)
async def gd_search_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    """Start invoice search flow."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    await state.clear()
    await state.set_state(InvoiceSearchSG.criteria)

    b = InlineKeyboardBuilder()
    b.button(text="По № счёта", callback_data="inv_search:invoice_number")
    b.button(text="По поставщику", callback_data="inv_search:supplier")
    b.button(text="По проекту", callback_data="inv_search:project")
    b.button(text="По сумме", callback_data="inv_search:amount")
    b.adjust(2)
    await message.answer(
        "<b>Поиск счёта</b>\n\nВыберите критерий поиска:",
        reply_markup=b.as_markup(),
    )


SEARCH_CRITERIA_LABELS = {
    "invoice_number": "№ счёта",
    "supplier": "поставщик",
    "project": "проект",
    "amount": "сумма",
}


@router.callback_query(F.data.startswith("inv_search:"))
async def gd_search_pick_criteria(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """User picked a search criterion."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    criteria = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.update_data(search_criteria=criteria)
    await state.set_state(InvoiceSearchSG.value)

    label = SEARCH_CRITERIA_LABELS.get(criteria, criteria)
    await cb.message.answer(  # type: ignore[union-attr]
        f"Введите значение для поиска по <b>{label}</b>:",
    )


@router.message(InvoiceSearchSG.value)
async def gd_search_execute(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Execute invoice search."""
    data = await state.get_data()
    criteria = data.get("search_criteria", "")
    value = (message.text or "").strip()

    if not value:
        await message.answer("Введите значение для поиска:")
        return

    results = await _search_invoice_tasks_by_criteria(db, criteria, value, limit=20)

    await state.clear()

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    if not results:
        await answer_service(
            message,
            "Ничего не найдено.",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id))),
        )
        return

    await message.answer(
        f"<b>Результаты поиска</b> ({len(results)}):",
        reply_markup=tasks_kb(results, back_callback="nav:home"),
    )

# ---------------------------------------------------------------------------
# Chat-proxy buttons — each opens chat submenu with its channel
# ---------------------------------------------------------------------------

@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_CHAT_RP))
async def gd_chat_rp(message: Message, state: FSMContext, db: Database) -> None:
    """#51: Чат с РП — с привязкой к счёту."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    # Invoice picker перед чатом
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True)
    if invoices:
        b = InlineKeyboardBuilder()
        for inv in invoices[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"gd_chat_inv:rp:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="gd_chat_inv:rp:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "💬 <b>Чат с РП</b>\n\nВыберите счёт для привязки:",
            reply_markup=b.as_markup(),
        )
        return
    await enter_chat_menu(message, state, channel="rp")


@router.callback_query(F.data.startswith("gd_chat_inv:"))
async def gd_chat_invoice_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """ГД выбрал счёт для привязки к чату (#51)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    channel = parts[1]  # rp, montazh, etc.
    inv_id = int(parts[2])

    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel, linked_invoice_id=inv_id if inv_id else None)

    inv_text = ""
    if inv_id:
        inv = await db.get_invoice(inv_id)
        if inv:
            inv_text = f"\n📄 Привязан счёт: №{inv.get('invoice_number', '?')}"

    label = channel_label(channel)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"💬 <b>{label}</b>{inv_text}\n\nВыберите действие:",
        )
    except Exception:
        pass

    await cb.message.answer(  # type: ignore[union-attr]
        f"💬 <b>{label}</b>{inv_text}\n\nВыберите действие:",
        reply_markup=gd_channel_menu(channel),
    )


@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_ZAMERY))
async def gd_chat_zamery(message: Message, state: FSMContext, db: Database) -> None:
    """#59: ГД Замеры — подменю: чат + создание задачи."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    b = InlineKeyboardBuilder()
    b.button(text="💬 Чат с замерщиками", callback_data="gd_zamery:chat")
    b.button(text="📋 Создать задачу на замер", callback_data="gd_zamery:create_task")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    await message.answer(
        "📐 <b>Замеры</b>\n\nВыберите действие:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "gd_zamery:chat")
async def gd_zamery_chat(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    await enter_chat_menu(cb.message, state, channel="zamery")  # type: ignore[arg-type]


@router.callback_query(F.data == "gd_zamery:create_task")
async def gd_zamery_create_task(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """#59: Начать создание задачи на замер от ГД."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    from ..states import GdTaskCreateSG
    await state.clear()
    await state.set_state(GdTaskCreateSG.description)
    await state.update_data(task_channel="zamery", task_type="zamery_request")
    await cb.message.answer(  # type: ignore[union-attr]
        "📋 <b>Задача на замер</b>\n\n"
        "Опишите задачу (адрес, дата/время, контакт клиента):",
    )


@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_ACCOUNTING))
async def gd_chat_accounting(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="accounting")


@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_MONTAZH))
async def gd_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    # --- Montazh statistics dashboard ---
    confirmed = await db.list_installer_confirmed_invoices()
    unconfirmed = await db.list_installer_unconfirmed_invoices()

    stage_counts: dict[str, int] = {}
    for inv in confirmed:
        stage = inv.get("montazh_stage") or "in_work"
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

    stage_labels = {
        "in_work": "🔨 В работе",
        "razmery_ok": "📐 Размеры ОК",
        "invoice_ok": "✅ Счёт ОК",
    }
    lines = ["📊 <b>Монтажная — статистика</b>\n"]
    if unconfirmed:
        lines.append(f"⏳ Ожидают принятия: {len(unconfirmed)}")
    for stage, label in stage_labels.items():
        cnt = stage_counts.get(stage, 0)
        if cnt:
            lines.append(f"{label}: {cnt}")
    total = len(confirmed) + len(unconfirmed)
    lines.append(f"\n📋 Всего счетов: {total}")

    # Show first 10 invoices in work
    if confirmed[:10]:
        lines.append("")
        for inv in confirmed[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "")[:25]
            stage = inv.get("montazh_stage") or "in_work"
            icon = {"in_work": "🔨", "razmery_ok": "📐", "invoice_ok": "✅"}.get(stage, "")
            lines.append(f"{icon} №{num} — {addr}")

    await message.answer("\n".join(lines))
    await enter_chat_menu(message, state, channel="montazh")


@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_SALES))
async def gd_chat_sales(message: Message, state: FSMContext, db: Database) -> None:
    """Отд.Продаж — составной канал."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel="otd_prodazh")
    await message.answer(
        "💬 <b>Отд.Продаж</b>\n\nВыберите действие:",
        reply_markup=gd_sales_submenu(back_label="⬅️ Назад"),
    )


@router.message(lambda m: any((m.text or "").strip().startswith(b) for b in (GD_BTN_KV_CRED, GD_SUBBTN_KV_CRED)))
async def gd_chat_kv(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="manager_kv")


@router.message(lambda m: any((m.text or "").strip().startswith(b) for b in (GD_BTN_KIA_CRED, GD_SUBBTN_KIA_CRED)))
async def gd_chat_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="manager_kia")


@router.message(lambda m: any((m.text or "").strip().startswith(b) for b in (GD_BTN_NPN_CRED, GD_SUBBTN_NPN_CRED)))
async def gd_chat_npn(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="manager_npn")


# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Отд.Продаж — composite handlers
# ---------------------------------------------------------------------------

@router.message(ChatProxySG.menu, F.text == "📨 Входящие")
async def sales_incoming(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Show NOT_URGENT_GD tasks from RP/managers."""
    data = await state.get_data()
    channel = data.get("channel", "")
    if channel != "otd_prodazh":
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.NOT_URGENT_GD,
        limit=50,
    )
    tasks = [task for task in tasks if await _is_sales_not_urgent_task(db, task)]

    if not tasks:
        await answer_service(
            message,
            "✅ Нет входящих «Не срочно ГД».",
            delay_seconds=60,
            reply_markup=gd_sales_submenu(),
        )
        return

    await message.answer(
        f"<b>Входящие «Не срочно»</b> ({len(tasks)}):",
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


_SALES_INV_PREFIX = "saleswrite_inv"


async def _show_sales_invoice_picker_or_write(
    message: Message,
    state: FSMContext,
    db: Database,
    *,
    label: str,
) -> None:
    """Показать invoice picker перед вводом сообщения, или сразу перейти к writing."""
    invoices = await db.list_invoices_for_selection(limit=15, only_regular=True)
    if invoices:
        await state.set_state(SalesWriteSG.invoice_pick)
        await message.answer(
            f"✏️ <b>Написать → {label}</b>\n"
            "По какому счёту вопрос?\n"
            "Для отмены: <code>/cancel</code>.",
            reply_markup=invoice_select_kb(invoices, prefix=_SALES_INV_PREFIX, back_callback="nav:home"),
        )
    else:
        await state.update_data(linked_invoice_id=None)
        await state.set_state(SalesWriteSG.writing)
        await message.answer(
            f"✏️ <b>Написать → {label}</b>\n\n"
            "Введите текст сообщения.\n"
            "Можно прикрепить файлы/фото.\n"
            "Для отмены: /cancel",
        )


@router.callback_query(F.data.startswith(f"{_SALES_INV_PREFIX}:"))
async def gd_write_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """GD выбрал счёт (или 'Без привязки') перед написанием сообщения."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    linked = None if val == "skip" else int(val)
    await state.update_data(linked_invoice_id=linked)
    await state.set_state(SalesWriteSG.writing)

    data = await state.get_data()
    targets = data.get("sales_targets", [])
    channel = data.get("write_channel", "")

    from .chat_proxy import channel_label as _ch_label
    if len(targets) > 1:
        label = f"Всем в {_ch_label(channel)}"
    else:
        label = _ch_label(targets[0]) if targets else _ch_label(channel)

    inv_label = ""
    if linked:
        inv = await db.get_invoice(linked)
        if inv:
            inv_label = f"\n📋 Счёт: №{inv.get('invoice_number', '?')}"

    await cb.message.answer(  # type: ignore[union-attr]
        f"✏️ <b>Написать → {label}</b>{inv_label}\n\n"
        "Введите текст сообщения.\n"
        "Можно прикрепить файлы/фото.\n"
        "Для отмены: /cancel",
    )


@router.message(ChatProxySG.menu, F.text == "✏️ Написать")
async def gd_write_pick_target(message: Message, state: FSMContext) -> None:
    """Show 'Кому?' target picker for ALL GD channels."""
    data = await state.get_data()
    channel = data.get("channel", "")

    from .chat_proxy import CHANNEL_WRITE_TARGETS, channel_label as _ch_label

    targets = CHANNEL_WRITE_TARGETS.get(channel, [])
    if targets:
        await state.set_state(SalesWriteSG.pick_target)
        await state.update_data(write_channel=channel)
        label = _ch_label(channel)
        await message.answer(
            f"✏️ <b>Написать → {label}</b>\n\nВыберите адресата:",
            reply_markup=gd_chat_write_to_kb_universal(targets),
        )
    else:
        # Fallback — direct writing (no known targets)
        from .chat_proxy import enter_writing
        await enter_writing(message, state, channel)


@router.message(SalesWriteSG.pick_target)
async def gd_write_target_picked(message: Message, state: FSMContext, db: Database) -> None:
    """User picked a target from the universal write submenu."""
    text = (message.text or "").strip()
    data = await state.get_data()
    channel = data.get("write_channel", data.get("channel", ""))

    from .chat_proxy import CHANNEL_WRITE_TARGETS, channel_label as _ch_label, gd_channel_menu

    targets = CHANNEL_WRITE_TARGETS.get(channel, [])

    # --- Назад ---
    if text == "⬅️ Назад":
        await state.set_state(ChatProxySG.menu)
        await state.update_data(channel=channel)
        label = _ch_label(channel)
        await message.answer(
            f"💬 <b>{label}</b>\n\nВыберите действие:",
            reply_markup=gd_channel_menu(channel),
        )
        return

    # --- Написать всем ---
    if text == "➡️ Написать всем":
        all_channels = [t[0] for t in targets]
        await state.update_data(sales_targets=all_channels, write_channel=channel)
        label = f"Всем в {_ch_label(channel)}"
        await _show_sales_invoice_picker_or_write(message, state, db, label=label)
        return

    # --- Конкретный адресат ---
    target_channel = None
    for ch, btn_label in targets:
        if btn_label == text:
            target_channel = ch
            break

    if not target_channel:
        await message.answer("Выберите адресата из кнопок.")
        return

    await state.update_data(sales_targets=[target_channel], write_channel=channel)
    label = _ch_label(target_channel)
    await _show_sales_invoice_picker_or_write(message, state, db, label=label)


@router.message(SalesWriteSG.writing)
async def gd_write_send_message(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Send message to selected targets (universal for all GD channels)."""
    data = await state.get_data()
    targets = data.get("sales_targets", [])
    channel = data.get("write_channel", "otd_prodazh")
    linked_invoice_id = data.get("linked_invoice_id")
    u = message.from_user
    if not u:
        return

    text = (message.text or message.caption or "").strip()

    file_info = None
    if message.document:
        file_info = {"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id}
    elif message.photo:
        ph = message.photo[-1]
        file_info = {"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id}
    elif message.video:
        file_info = {"file_type": "video", "file_id": message.video.file_id, "file_unique_id": message.video.file_unique_id}

    if not text and not file_info:
        await message.answer("Введите текст или прикрепите файл.")
        return

    from .chat_proxy import resolve_channel_target, channel_label as _ch_label, is_group_channel, gd_channel_menu

    sent_count = 0
    for ch in targets:
        target_id = await resolve_channel_target(ch, db, config)
        if not target_id:
            continue

        # Save to DB
        await db.save_chat_message(
            channel=ch,
            sender_id=u.id,
            direction="outgoing",
            text=text or None,
            receiver_id=target_id if not is_group_channel(ch) else None,
            receiver_chat_id=target_id if is_group_channel(ch) else None,
            tg_message_id=message.message_id,
            has_attachment=bool(file_info),
            invoice_id=linked_invoice_id,
        )

        label = _ch_label(ch)
        header = f"📩 <b>От ГД</b> ({label}):\n\n"
        if text:
            await notifier.safe_send(target_id, header + text)
        if file_info:
            await notifier.safe_send_media(target_id, file_info["file_type"], file_info["file_id"], caption=message.caption)
        if not is_group_channel(ch):
            await refresh_recipient_keyboard(notifier, db, config, int(target_id))
        sent_count += 1

    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)
    await message.answer(
        f"✅ Отправлено {sent_count} адресатам.",
        reply_markup=gd_channel_menu(channel),
    )


# "Сообщение Всем" — broadcast to all channels
# ---------------------------------------------------------------------------
# "📊 Сводка дня" — daily dashboard for GD
# ---------------------------------------------------------------------------

@router.message(
    lambda m: (m.text or "").strip() == GD_BTN_DAILY_SUMMARY
)
async def gd_daily_summary(message: Message, db: Database, config: Config) -> None:
    """Агрегированная сводка дня для ГД."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    text, markup = await _build_summary(db)
    await message.answer(text, reply_markup=markup)


async def _build_summary(db: Database) -> tuple[str, "InlineKeyboardBuilder"]:
    """Build summary text + inline keyboard with drill-down buttons."""
    from datetime import date as _date

    s = await db.get_daily_summary()

    inv = s["invoices_by_status"]
    pending = inv.get("pending", 0)
    in_progress = inv.get("in_progress", 0)
    paid = inv.get("paid", 0)
    closing = inv.get("closing", 0)

    total_amt = s["total_amount"] or 0
    total_debt = s["total_debt"] or 0

    tasks_open = s["tasks_open"]
    urgent = tasks_open.get("urgent_gd", 0) + tasks_open.get("not_urgent_gd", 0)
    inv_pay = tasks_open.get("invoice_payment", 0)
    suppl_pay = tasks_open.get("supplier_payment", 0)

    overdue = s["overdue"]
    today_dl = s["today_deadline"]
    soon_dl = s["soon_deadline"]

    lines = [
        "<b>📊 Сводка дня</b>",
        "",
        "<b>📄 Счета в работе:</b> " + str(s["in_work"]),
        f"  ⏳ Ожидают оплаты: {pending}",
        f"  🔧 В работе: {in_progress}",
        f"  💰 Оплачены: {paid}",
        f"  🏁 На закрытии: {closing}",
        f"  ✅ Закрыто за месяц: {s['ended_month']}",
        "",
        "<b>💵 Финансы (активные счета):</b>",
        f"  Сумма: <b>{total_amt:,.0f}₽</b>".replace(",", " "),
        f"  Долг: <b>{total_debt:,.0f}₽</b>".replace(",", " "),
    ]

    # Секция «Открытые задачи» — только если есть хоть одна
    total_tasks = urgent + inv_pay + suppl_pay + s["zp_pending"]
    if total_tasks:
        lines.append("")
        lines.append("<b>📋 Открытые задачи:</b>")
        if urgent:
            lines.append(f"  🚨 Срочные/Не срочные ГД: {urgent}")
        if inv_pay:
            lines.append(f"  💳 Счета на оплату: {inv_pay}")
        if suppl_pay:
            lines.append(f"  💸 Оплата поставщику: {suppl_pay}")
        if s["zp_pending"]:
            lines.append(f"  💰 ЗП-запросы: {s['zp_pending']}")

    if overdue or today_dl or soon_dl:
        lines.append("")
        lines.append("<b>⏰ Дедлайны:</b>")
        if overdue:
            lines.append(f"  🔴 Просрочено: {overdue}")
        if today_dl:
            lines.append(f"  🔴 Срок сегодня: {today_dl}")
        if soon_dl:
            lines.append(f"  ⚠️ До 3 дней: {soon_dl}")

    lines.append("")
    lines.append("<i>Нажмите на строку для просмотра деталей:</i>")

    # Build inline keyboard with drill-down buttons for non-zero counts
    b = InlineKeyboardBuilder()
    _summary_btn = [
        ("⏳ Ожидают оплаты", "inv_pending", pending),
        ("🔧 В работе", "inv_inprog", in_progress),
        ("💰 Оплачены", "inv_paid", paid),
        ("🏁 На закрытии", "inv_closing", closing),
        ("🚨 Срочные ГД", "task_urgent", urgent),
        ("💳 Счета на оплату", "task_invpay", inv_pay),
        ("💸 Оплата поставщику", "task_supplpay", suppl_pay),
        ("💰 ЗП-запросы", "zp_pending", s["zp_pending"]),
        ("🔴 Просрочено", "dl_overdue", overdue),
        ("🔴 Срок сегодня", "dl_today", today_dl),
        ("⚠️ До 3 дней", "dl_soon", soon_dl),
    ]
    for label, section, count in _summary_btn:
        if count:
            b.button(
                text=f"{label}: {count}",
                callback_data=SummaryCb(section=section, action="list").pack(),
            )
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


# ---------------------------------------------------------------------------
# Сводка дня — drill-down по секциям
# ---------------------------------------------------------------------------

@router.callback_query(SummaryCb.filter(F.action == "list"))
async def gd_summary_drilldown(
    cb: CallbackQuery, callback_data: SummaryCb, db: Database,
) -> None:
    """Show individual items for a summary section."""
    from datetime import date as _date, datetime as _dt

    section = callback_data.section
    b = InlineKeyboardBuilder()

    # ---- Invoice sections ----
    if section.startswith("inv_"):
        status_map = {
            "inv_pending": ("pending", "⏳ Ожидают оплаты"),
            "inv_inprog": ("in_progress", "🔧 В работе"),
            "inv_paid": ("paid", "💰 Оплачены"),
            "inv_closing": ("closing", "🏁 На закрытии"),
        }
        status, title = status_map.get(section, ("pending", section))
        invoices = await db.list_invoices(status=status, limit=50)
        if not invoices:
            await cb.answer("Список пуст", show_alert=True)
            return
        text = f"<b>📊 {title}</b> ({len(invoices)})\n\nВыберите счёт:"
        for inv in invoices:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = inv.get("address") or ""
            label = f"{num} — {addr}"[:60]
            b.button(text=label, callback_data=f"gd_work:view:{inv['id']}")
        b.adjust(1)

    # ---- Task sections ----
    elif section.startswith("task_"):
        type_map = {
            "task_urgent": (["urgent_gd", "not_urgent_gd"], "🚨 Срочные ГД"),
            "task_invpay": (["invoice_payment"], "💳 Счета на оплату"),
            "task_supplpay": (["supplier_payment"], "💸 Оплата поставщику"),
        }
        task_types, title = type_map.get(section, ([], section))
        task_list = await db.list_tasks_open_by_types(task_types)
        if not task_list:
            await cb.answer("Задач нет", show_alert=True)
            return
        # Use existing tasks_kb with delete buttons
        b.button(text="⬅️ Назад к сводке", callback_data=SummaryCb(section="", action="back").pack())
        kb = tasks_kb(task_list, show_delete=True, back_callback=SummaryCb(section="", action="back").pack())
        text = f"<b>📊 {title}</b> ({len(task_list)})\n\nНажмите на задачу для действий:"
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
        except Exception:
            pass
        await cb.answer()
        return

    # ---- ZP pending ----
    elif section == "zp_pending":
        invoices = await db.list_zp_pending_invoices()
        if not invoices:
            await cb.answer("ЗП-запросов нет", show_alert=True)
            return
        text = f"<b>📊 💰 ЗП-запросы</b> ({len(invoices)})\n\nВыберите счёт:"
        for inv in invoices:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = inv.get("address") or ""
            label = f"{num} — {addr}"[:60]
            b.button(text=label, callback_data=f"gd_work:view:{inv['id']}")
        b.adjust(1)

    # ---- Deadline sections ----
    elif section.startswith("dl_"):
        deadlines = await db.list_invoices_approaching_deadline()
        dl_map = {
            "dl_overdue": ("🔴 Просрочено", lambda d: d < 0),
            "dl_today": ("🔴 Срок сегодня", lambda d: d == 0),
            "dl_soon": ("⚠️ До 3 дней", lambda d: 0 < d <= 3),
        }
        title, pred = dl_map.get(section, ("Дедлайны", lambda d: True))
        filtered: list[dict] = []
        for inv in deadlines:
            raw = inv.get("deadline_end_date")
            if not raw:
                continue
            try:
                end = _dt.fromisoformat(str(raw)).date()
            except (ValueError, TypeError):
                continue
            delta = (end - _date.today()).days
            if pred(delta):
                filtered.append(inv)
        if not filtered:
            await cb.answer("Список пуст", show_alert=True)
            return
        text = f"<b>📊 {title}</b> ({len(filtered)})\n\nВыберите счёт:"
        for inv in filtered:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = inv.get("address") or ""
            label = f"{num} — {addr}"[:60]
            b.button(text=label, callback_data=f"gd_work:view:{inv['id']}")
        b.adjust(1)
    else:
        await cb.answer("Неизвестная секция", show_alert=True)
        return

    b.button(text="⬅️ Назад к сводке", callback_data=SummaryCb(section="", action="back").pack())
    b.adjust(1)
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()


@router.callback_query(SummaryCb.filter(F.action == "back"))
async def gd_summary_back(cb: CallbackQuery, db: Database) -> None:
    """Return to the daily summary view."""
    text, markup = await _build_summary(db)
    try:
        await cb.message.edit_text(text, reply_markup=markup)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()


# ---------------------------------------------------------------------------
# "Синхронизация данных" — Google Sheets resync from GD main menu
# ---------------------------------------------------------------------------

@router.message(
    lambda m: (m.text or "").strip() == GD_BTN_SYNC
)
async def gd_sync_data(message: Message, db: Database, config: Config, integrations: IntegrationHub) -> None:
    """Trigger Google Sheets resync + show detailed task/project summary."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())
    tz = config.timezone

    # --- 1. Google Sheets sync (if enabled) ---
    if integrations.sheets:
        await message.answer("⏳ Запускаю синхронизацию данных с Google Sheets...")

        # Импорт из ОП (Отдел продаж) → БД бота
        imported = await import_from_source_sheet(
            db, integrations.sheets, log_prefix="gd_sync",
        )

        # Экспорт БД → invoice-лист
        stats = await export_to_sheets(
            db,
            integrations.sheets,
            include_invoice_cost=True,
            sync_invoices=True,
        )

        await message.answer(
            "✅ Синхронизация Google Sheets завершена.\n"
            f"Импорт ОП: <b>{imported}</b> | "
            f"Проектов: <b>{stats['projects']}</b> | "
            f"Задач: <b>{stats['tasks']}</b> | "
            f"Счетов: <b>{stats['invoices']}</b>",
        )

    # --- 2. Leads summary (one block) ---
    all_leads = await db.list_leads(limit=100)
    active_leads = [l for l in all_leads if l.get("status") == "lead"]
    if active_leads:
        lines_l = [f"📨 <b>Лиды ({len(active_leads)})</b>\n"]
        for l in active_leads:
            name = html.escape(l.get("client_name") or "—")
            phone = html.escape(l.get("phone") or "—")
            role = (l.get("assigned_manager_role") or "").upper()
            lines_l.append(f"  • {name} | {phone} | {role}")
        await message.answer("\n".join(lines_l))
    else:
        await message.answer("📨 Активных лидов нет.")

    # --- 3. Projects summary (one block) ---
    all_projects_list = await db.list_recent_projects(limit=500)
    active_projects = [
        p for p in all_projects_list
        if p.get("status") and p.get("status") != "archive"
    ]
    active_projects.sort(key=lambda p: p.get("updated_at") or "", reverse=True)

    if active_projects:
        lines_p = [f"🏗 <b>Проекты ({len(active_projects)})</b>\n"]
        for p in active_projects[:30]:
            code = html.escape(p.get("code") or f"#{p['id']}")
            client = html.escape(p.get("client") or "—")
            pstatus = project_status_label(str(p.get("status") or ""))
            amount = p.get("amount")
            amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
            lines_p.append(f"  • <b>{code}</b> {client} | {amount_s} | {pstatus}")
        if len(active_projects) > 30:
            lines_p.append(f"\n  ... и ещё {len(active_projects) - 30}")
        text_p = "\n".join(lines_p)
        # Split if too long
        if len(text_p) > 4000:
            await message.answer(text_p[:4000])
        else:
            await message.answer(text_p)
    else:
        await message.answer("🏗 Активных проектов нет.")

    await answer_service(
        message,
        "✅ Синхронизация данных завершена.",
        delay_seconds=300,
        reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id))),
    )


# ---------------------------------------------------------------------------
# Invoice cost statistics + all messages per invoice
# ---------------------------------------------------------------------------

@router.callback_query(F.data.regexp(r"^inv_stats:\d+$"))
async def gd_invoice_stats(cb: CallbackQuery, db: Database) -> None:
    """Полная карточка себестоимости по родительскому счёту."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    parent_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(parent_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return

    cost = await db.get_full_invoice_cost_card(parent_id)
    from ..utils import format_cost_card
    # Add Plan/Fact button if estimated data exists
    pf = await db.get_plan_fact_card(parent_id)
    b = InlineKeyboardBuilder()
    if pf.get("has_estimated"):
        b.button(text="📊 План/Факт", callback_data=f"inv_planfact:{parent_id}")
        b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        format_cost_card(inv, cost),
        reply_markup=b.as_markup() if pf.get("has_estimated") else None,
    )


@router.callback_query(F.data.regexp(r"^inv_planfact:\d+$"))
async def gd_invoice_plan_fact(cb: CallbackQuery, db: Database) -> None:
    """Карточка План/Факт для ГД."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return

    pf = await db.get_plan_fact_card(invoice_id)
    if not pf.get("has_estimated"):
        await cb.message.answer("⚠️ Расчётные данные не заполнены для этого счёта.")  # type: ignore[union-attr]
        return

    from ..utils import format_plan_fact_card
    await cb.message.answer(format_plan_fact_card(inv, pf))  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^inv_msgs:\d+$"))
async def gd_invoice_messages(cb: CallbackQuery, db: Database) -> None:
    """Все сообщения из всех каналов, привязанные к конкретному счёту."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return

    messages = await db.list_chat_messages_by_invoice(invoice_id, limit=30)
    num = inv.get("invoice_number") or f"#{invoice_id}"

    if not messages:
        await cb.message.answer(  # type: ignore[union-attr]
            f"💬 <b>Переписка — Счёт №{html.escape(str(num))}</b>\n\n"
            "Нет привязанных сообщений."
        )
        return

    lines = [f"💬 <b>Переписка — Счёт №{html.escape(str(num))}</b>\n"]
    for m in reversed(messages):
        direction = "➡️" if m.get("direction") == "outgoing" else "⬅️"
        ts = (m.get("created_at") or "")[:16].replace("T", " ")
        channel = m.get("channel") or "?"
        text_preview = (m.get("text") or "📎 [вложение]")[:60]
        lines.append(f"{direction} {ts} [{channel}] {html.escape(text_preview)}")

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:4000] + "\n..."
    await cb.message.answer(result)  # type: ignore[union-attr]
