"""Handlers specific to the GD (Генеральный директор) role.

Phase 1:
- "Срочно для ГД" — shows list of open URGENT_GD + PAYMENT_CONFIRM tasks
- "Синхронизация данных" — triggers Google Sheets resync from GD main menu

Phase 2:
- Chat-proxy buttons: Чат с РП, Замеры, Бухгалтерия, Монтажная гр., Отд.Продаж,
  КВ Кред, КИА Кред, НПН Кред
"""

from __future__ import annotations

import asyncio
import html
import logging
from datetime import datetime

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import (
    RpZpPayActCb, RpZpPayCb, RpZpPaySelCb, RpZpRejectCb,
    SummaryCb, TaskCb,
    ZamZpPayActCb, ZamZpPayCb, ZamZpPaySelCb, ZamZpRejectCb,
)
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
    GD_BTN_RECALC,
    GD_BTN_SYNC,
    GD_BTN_ZAMERY,
    main_menu,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..services.sheets_sync import export_to_sheets, import_from_source_sheet
from ..states import (
    ChatProxySG,
    GdAdvancePaySG,
    GdAdvanceRejectSG,
    GdDepositSG,
    InvoiceSearchSG,
    OpAddSG,
    RpZpPaySG,
    SalesWriteSG,
    ZamerySettlementPaySG,
    ZamZpPaySG,
)
from .chat_proxy import channel_label, enter_chat_menu, gd_channel_menu
from .common import _show_main_menu
from ..utils import (
    answer_service,
    build_deposit_history_card,
    build_gd_sync_card_text,
    build_invoice_section,
    format_card_section,
    format_dt_iso,
    format_invoice_card_standard,
    format_manager_recalc_card,
    format_zamery_settlement_card,
    format_zamery_settlement_detail_cards,
    get_initiator_label,
    parse_roles,
    private_only_reply_markup,
    refresh_recipient_keyboard,
    task_status_label,
    task_type_label,
    try_json_loads,
)
from .auth import require_role_callback, require_role_message
from .money_guard import money_confirm_guard

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

GD_ACCESS_ROLES = [Role.GD, Role.TD]
SALES_SOURCE_ROLES = {Role.RP, Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN}


def _depo_sum(n: float) -> str:
    """Сумма для <pre>-ячейки эталона: пробел-разделитель разрядов, без ₽
    (feedback_card_telegram_pre_alignment — ₽ из ячеек убрать)."""
    return f"{float(n or 0):,.0f}".replace(",", " ")


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
async def gd_inbox_all(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    """Show GD all open tasks (urgent, payment confirm, GD_TASK, etc.)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    all_tasks_raw = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        limit=50,
        exclude_created_by=user_id,
    )

    # Исключаем задачи, которые показываются в отдельных кнопках
    _INBOX_EXCLUDE = {TaskType.INVOICE_PAYMENT, TaskType.ZP_INSTALLER}
    all_tasks = [
        t for t in all_tasks_raw
        if t.get("type") not in _INBOX_EXCLUDE
    ]

    is_admin = user_id in (config.admin_ids or set())

    if not all_tasks:
        await answer_service(
            message,
            "✅ Нет входящих задач.",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id), gd_total_open_tasks=await db.count_gd_more_total_open_tasks(user_id))),
        )
        return

    # user 04.07: одна задача → сразу карточка установленного дизайна (с сутью
    # задачи), без промежуточного пикера «Выберите задачу». Несколько — список.
    # Рендер общий с кликом по задаче (tasks.send_task_open_card).
    if len(all_tasks) == 1:
        from .tasks import send_task_open_card
        from ..services.menu_scope import resolve_active_menu_role
        task = all_tasks[0]
        _u = await db.get_user_optional(user_id)
        viewer_role = resolve_active_menu_role(user_id, _u.role if _u else None)
        await send_task_open_card(message, db, config, task, viewer_role)
        attaches = await db.list_attachments(int(task["id"]))
        if attaches:
            await message.answer(f"📎 Вложения: {len(attaches)}")
            for a in attaches[:10]:
                await notifier.safe_send_media(
                    user_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"),
                )
        return

    # Count by type for summary
    n_urgent = sum(1 for t in all_tasks if t.get("type") == TaskType.URGENT_GD)
    n_payment = sum(1 for t in all_tasks if t.get("type") == TaskType.PAYMENT_CONFIRM)
    n_other = len(all_tasks) - n_urgent - n_payment

    parts = []
    if n_urgent:
        parts.append(f"🚨 Срочных: {n_urgent}")
    if n_payment:
        parts.append(f"💰 Подтв.оплат: {n_payment}")
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

async def _build_gd_invoices_view(db: Database, user_id: int):
    """Сводная карточка + клавиатура для ГД-списка «Счета на оплату».

    Карточка в принятом дизайне (моноширинный <pre>-блок). Каждая задача
    invoice_payment = блок из 3 строк (user 26.06 — было одной строкой
    «иконка · улица · менеджер», без «за что»/«от кого»/номера счёта):
        {иконка} {Категория}                {сумма}
        №{номер счёта} · {улица}
        от: {инициатор} ({роль}) · мен. {КВ/КИА/НПН}
    Внизу — «Итого» (сумма). Кнопки — те же
    задачи INVOICE_PAYMENT, в подписи иконка/улица/сумма. Кнопка «Обновить»
    убрана (user 26.06 — плодила копии; список и так пересобирает reply-кнопка
    «Счета на Оплату»). Возвращает (card_text | None, markup | None).

    Только отображение. Источники: payload задачи (amount, material_type,
    invoice_number) + object_address счёта по invoice_id + инициатор
    (created_by/creator_role). Категорию/иконку даёт _mt_to_cat/CATS
    (🔩 металл/🔷 стекло/💪 грузчики/🚚 логистика/🧱 доп.мат/🧾 доп.усл),
    адрес — _addr_cell (единое правило карточек ГД, owner 30.07: Москва →
    улица, НЕ Москва → город; был _street, город терялся),
    выравнивание — vw (эмодзи ≈ 2 колонки).
    """
    from ..rp_start_card import _addr_cell, _mt_to_cat, CATS, vw

    tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.INVOICE_PAYMENT,
        limit=100,
    )
    if not tasks:
        return None, None

    icon_by_cat = {k: ic for (k, ic, *_rest) in CATS}
    title_by_cat = {k: ttl for (k, _ic, _fld, ttl) in CATS}
    role_lbl = {"rp": "РП", "manager": "менеджер", "gd": "ГД", "td": "ТД"}

    def _money(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    async def _creator(t: dict) -> str:
        cid = t.get("created_by")
        if not cid:
            return "—"
        lbl = role_lbl.get((t.get("creator_role") or "").lower(), "")
        u = await db.get_user_optional(int(cid))
        name = (u.full_name.split()[0] if (u and u.full_name) else "") or f"#{cid}"
        name = html.escape(name)
        return f"{name} ({lbl})" if lbl else name

    INDENT = "   "
    blocks: list[list[tuple[str, str]]] = []  # блок = [(label, value)]; value="" → строка без right-align
    total = 0.0
    b = InlineKeyboardBuilder()
    for t in tasks:
        payload = try_json_loads(t.get("payload_json") or "{}")
        inv_num = payload.get("invoice_number") or f"#{t['id']}"
        amount = float(payload.get("amount") or 0)
        total += amount
        cat = _mt_to_cat(payload.get("material_type") or "")
        icon = icon_by_cat.get(cat, "🧱")
        cat_title = title_by_cat.get(cat, "Прочее")
        inv_id = payload.get("invoice_id") or payload.get("parent_invoice_id")
        addr = ""
        if inv_id:
            inv = await db.get_invoice(int(inv_id))
            addr = (inv or {}).get("object_address") or ""
        street = _addr_cell(addr, 14) if addr else "—"
        mgr = "КИА" if "КИА" in inv_num else ("НПН" if "НПН" in inv_num else "КВ")
        who = await _creator(t)
        amt_s = _money(amount)
        blocks.append([
            (f"{icon} {cat_title}", amt_s),
            (f"№{html.escape(str(inv_num))} · {html.escape(street)}", ""),
            (f"от: {who} · мен. {mgr}", ""),
        ])
        b.button(
            text=f"{icon} {street if addr else inv_num} · {amt_s}"[:60],
            callback_data=TaskCb(task_id=int(t["id"]), action="open").pack(),
        )
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    foot = [("Итого", _money(total))]

    # Динамическая ширина: max визуальная по всем right-align (label+value, +1 зазор)
    # и левым (value="") строкам — чтобы суммы и footer сходились в один столбец.
    raw: list[int] = []
    for blk in blocks:
        for lbl, val in blk:
            raw.append(vw(INDENT) + vw(lbl) + (1 + vw(val) if val else 0))
    for lbl, val in foot:
        raw.append(vw(INDENT) + vw(lbl) + 1 + vw(val))
    width = max(raw) if raw else 30

    def _rline(lbl: str, val: str) -> str:
        pad = max(1, width - vw(INDENT) - vw(lbl) - vw(val))
        return f"{INDENT}{lbl}{' ' * pad}{val}"

    body_lines: list[str] = []
    for i, blk in enumerate(blocks):
        if i:
            body_lines.append("")  # пустая строка-разделитель между блоками
        for lbl, val in blk:
            body_lines.append(_rline(lbl, val) if val else f"{INDENT}{lbl}")
    body_lines.append(INDENT + "━" * max(3, width - vw(INDENT)))
    for lbl, val in foot:
        body_lines.append(_rline(lbl, val))

    body = "\n".join(body_lines)
    card = f"<b>💰  Счета на оплату</b>\n<pre>{body}</pre>"
    return card, b


@router.message(F.text.startswith(GD_BTN_INVOICES))
async def gd_invoices(message: Message, db: Database, config: Config) -> None:
    """Show only invoice_payment tasks (requests from RP/Manager)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    card, markup = await _build_gd_invoices_view(db, user_id)

    if card is None:
        await answer_service(
            message,
            "✅ Нет счетов на оплату.",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id), gd_total_open_tasks=await db.count_gd_more_total_open_tasks(user_id))),
        )
        return

    await message.answer(card, reply_markup=markup.as_markup())


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

    # Карточка «Осталось закупить» — план − факт по 4 категориям.
    from ..utils import format_inwork_remaining
    await message.answer(format_inwork_remaining(invoices))

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

    # Карточка «Осталось закупить» — план − факт по 4 категориям.
    from ..utils import format_inwork_remaining
    await cb.message.answer(format_inwork_remaining(invoices))  # type: ignore[union-attr]

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
    """Invoice card from GD work dashboard — Plan/Fact card (shortcut)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    b = InlineKeyboardBuilder()
    b.button(text="💬 Сообщения", callback_data=f"inv_msgs:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="gd_work:refresh")
    b.adjust(1)

    # Try Plan/Fact card first (most informative)
    pf = await db.get_plan_fact_card(invoice_id)
    if pf.get("has_estimated"):
        from ..utils import format_plan_fact_card
        text = format_plan_fact_card(inv, pf)
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
        return

    # Fallback: карточка по эталону для счётов без estimated.
    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    section = await build_invoice_section(db, inv, invoice_id)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
    )

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
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id), unread_channels=await db.count_unread_by_channel(user_id), gd_inbox_unread=await db.count_gd_inbox_tasks(user_id), gd_invoice_unread=await db.count_gd_invoice_tasks(user_id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id), gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id), gd_total_open_tasks=await db.count_gd_more_total_open_tasks(user_id))),
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
    # invoice_ctx_set — «вопрос про счёт ГД уже задан и отвечен» (в т.ч. «Без
    # привязки»). Флаг читает chat_proxy.gd_task_create_start и НЕ спрашивает тот
    # же список счетов второй раз при создании задачи (owner 30.07).
    await state.update_data(
        channel=channel,
        linked_invoice_id=inv_id if inv_id else None,
        invoice_ctx_set=True,
    )

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
    b.button(text="💰 Взаиморасчёты с замерщиком", callback_data="gd_zamery:settle")
    b.button(text="❓ Атрибуция замеров", callback_data="gd_zamery:attr_map")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    # График замеров (календарь-окно) — сразу карточкой, ниже пункты меню (owner 14.07;
    # переехал сюда из стартовой карты ГД). None если замерщиков нет → фолбэк-заголовок.
    _cal = None
    try:
        from ..zamery_start_card import build_zamery_calendar_section
        _cal = await build_zamery_calendar_section(db)
    except Exception:
        log.exception("gd_zamery: calendar section failed")
    text = (
        f"{_cal}\n\n<b>Замеры</b> — выберите действие:"
        if _cal else "📐 <b>Замеры</b>\n\nВыберите действие:"
    )
    await message.answer(text, reply_markup=b.as_markup())


async def _render_gd_settlement(
    target: Message, db: Database, config: Config,
) -> None:
    """Карточка взаиморасчётов с замерщиком для ГД + кнопка «Добавить платёж»."""
    from ..services.assignment import resolve_default_assignee
    surveyor_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not surveyor_id:
        await target.answer("⚠️ Замерщик не назначен (нет пользователя с ролью «Замерщик»).")
        return
    summary = await db.get_zamery_settlement_summary(int(surveyor_id))
    su = await db.get_user_optional(int(surveyor_id))
    name = (su.full_name if su else None) or "Замерщик"
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить платёж", callback_data="gd_zamery:settle_pay")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    # Помесячная детализация по замерам (Вариант A, ТЗ 14.07) — вторым блоком под
    # сводкой; кнопки уводим на детализацию (снизу). Дата оплаты фиксируется кнопкой
    # «➕ Добавить платёж» (mark_zamery_paid), эта карточка её показывает.
    detail = ""
    try:
        detail = format_zamery_settlement_detail_cards(
            await db.list_zamery_settlement_detail(int(surveyor_id))
        )
    except Exception:
        log.exception("gd_settlement: detail card failed")
    if detail:
        await target.answer(format_zamery_settlement_card(summary, name))
        await target.answer(detail, reply_markup=b.as_markup())
    else:
        await target.answer(
            format_zamery_settlement_card(summary, name), reply_markup=b.as_markup(),
        )


@router.callback_query(F.data == "gd_zamery:settle")
async def gd_zamery_settle(cb: CallbackQuery, db: Database, config: Config) -> None:
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    await _render_gd_settlement(cb.message, db, config)  # type: ignore[arg-type]


async def _render_zam_attr_map(
    target: Message, db: Database, config: Config,
) -> None:
    """Карта атрибуции замеров: сколько распределено по КВ/НПН/КИА + список UNK.

    Кнопки: «Спросить замерщика» (шлёт вопросы по UNK), «Записать в журнал».
    """
    from ..services.assignment import resolve_default_assignee
    from .zamery import _attr_fmt_ddmm
    surveyor_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not surveyor_id:
        await target.answer("⚠️ Замерщик не назначен (нет пользователя с ролью «Замерщик»).")
        return
    rows = await db.list_zamery_attribution(int(surveyor_id))
    total = len(rows)
    by_role = {"manager_kv": 0, "manager_npn": 0, "manager_kia": 0}
    unk: list[dict] = []
    for r in rows:
        role = r.get("requester_role") or ""
        if role in by_role:
            by_role[role] += 1
        else:
            unk.append(r)
    # Эталон <pre>-дизайн (owner 14.07; прежде — плоские буллеты «• КВ: N» = anti-pattern B).
    _dist = total - len(unk)
    _attr_card = format_card_section(
        "📊", "Атрибуция замеров",
        items=[
            ("Распределено", f"{_dist} из {total}"),
            ("КВ (Кирилл)", str(by_role["manager_kv"])),
            ("НПН (Паша)", str(by_role["manager_npn"])),
            ("КИА (Илья)", str(by_role["manager_kia"])),
        ],
    )
    parts = [_attr_card]
    if unk:
        _unk_items = [
            (
                f"{_attr_fmt_ddmm(r.get('scheduled_date'))}  "
                f"{html.escape(r.get('address') or '—')}",
                "",
            )
            for r in unk
        ]
        parts.append(
            format_card_section(
                "❓", f"Без менеджера — {len(unk)}",
                items=_unk_items, compact=True,
            )
        )
    else:
        parts.append("<i>🎉 Все замеры распределены.</i>")
    b = InlineKeyboardBuilder()
    if unk:
        b.button(
            text=f"📨 Спросить замерщика ({len(unk)})",
            callback_data="gd_zamery:ask_mgr",
        )
    b.button(text="🔄 Записать в журнал", callback_data="gd_zamery:attr_journal")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    await target.answer("\n\n".join(parts), reply_markup=b.as_markup())


@router.callback_query(F.data == "gd_zamery:attr_map")
async def gd_zamery_attr_map(cb: CallbackQuery, db: Database, config: Config) -> None:
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    await _render_zam_attr_map(cb.message, db, config)  # type: ignore[arg-type]


@router.callback_query(F.data == "gd_zamery:ask_mgr")
async def gd_zamery_ask_mgr(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """ГД: разослать замерщику вопросы атрибуции по всем UNK-замерам."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    from ..services.assignment import resolve_default_assignee
    from .zamery import build_attr_question
    surveyor_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not surveyor_id:
        await cb.answer("⚠️ Замерщик не назначен.", show_alert=True)
        return
    rows = await db.list_zamery_attribution(int(surveyor_id))
    unk = [r for r in rows if not (r.get("requester_role") or "")]
    if not unk:
        await cb.answer("Все замеры уже распределены ✅", show_alert=True)
        return
    await cb.answer(f"Отправляю замерщику {len(unk)} вопрос(ов)…")
    intro_ok = await notifier.safe_send(
        int(surveyor_id),
        "❓ <b>Помоги распределить замеры</b>\n\n"
        "По каждому адресу ниже нажми, кто из менеджеров тебя отправлял "
        "(Кирилл / Паша / Илья), или «🤷 Не помню».",
    )
    sent = 0
    for r in unk:
        text, kb = build_attr_question(r)
        if await notifier.safe_send(int(surveyor_id), text, reply_markup=kb):
            sent += 1
    note = (
        f"📨 Отправил замерщику {sent} из {len(unk)} вопрос(ов). "
        "Ответы буду записывать и присылать сюда."
    )
    if not intro_ok:
        note += "\n⚠️ Похоже, замерщик не открывал чат с ботом — сообщения могли не дойти."
    await cb.message.answer(note)  # type: ignore[union-attr]


@router.callback_query(F.data == "gd_zamery:attr_journal")
async def gd_zamery_attr_journal(
    cb: CallbackQuery, db: Database, config: Config, integrations: IntegrationHub,
) -> None:
    """ГД: перезаписать журнал замеров на лист Leads (колонка «Менеджер»)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    from ..services.assignment import resolve_default_assignee
    surveyor_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not surveyor_id:
        await cb.answer("⚠️ Замерщик не назначен.", show_alert=True)
        return
    if not integrations.sheets:
        await cb.answer("⚠️ Google Sheets не подключён.", show_alert=True)
        return
    await cb.answer("Записываю журнал…")
    try:
        journal = await db.list_zamery_journal(
            int(surveyor_id), "2000-01-01", "2100-12-31",
        )
        await asyncio.to_thread(integrations.sheets.upsert_zamery_journal_sync, journal)
        await cb.message.answer(  # type: ignore[union-attr]
            "✅ Журнал замеров обновлён на листе Leads (колонка «Менеджер»)."
        )
    except Exception:
        log.exception("gd_zamery_attr_journal: journal sync failed")
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Не удалось обновить журнал (см. логи)."
        )


@router.callback_query(F.data == "gd_zamery:settle_pay")
async def gd_zamery_settle_pay_start(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """ГД: начать добавление платежа замерщику."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    from ..services.assignment import resolve_default_assignee
    surveyor_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not surveyor_id:
        await cb.message.answer("⚠️ Замерщик не назначен.")  # type: ignore[union-attr]
        return
    await state.clear()
    await state.update_data(settle_surveyor_id=int(surveyor_id))
    await state.set_state(ZamerySettlementPaySG.enter_amount)
    await cb.message.answer(  # type: ignore[union-attr]
        "💰 <b>Платёж замерщику</b>\n\nВведите сумму платежа (число, ₽):",
    )


@router.message(ZamerySettlementPaySG.enter_amount)
async def gd_zamery_settle_pay_amount(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(raw)
    except ValueError:
        amount = 0.0
    if amount <= 0:
        await message.answer("❌ Введите положительное число. Сумма платежа:")
        return
    await state.update_data(settle_amount=amount)
    await state.set_state(ZamerySettlementPaySG.enter_date)
    await message.answer(
        "📅 Дата платежа в формате ДД.ММ.ГГГГ (или напишите «сегодня»):",
    )


@router.message(ZamerySettlementPaySG.enter_date)
async def gd_zamery_settle_pay_date(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip().lower()
    if raw in ("сегодня", "today", ""):
        entry_date = datetime.now().strftime("%Y-%m-%d")
    else:
        try:
            entry_date = datetime.strptime(raw, "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            await message.answer("❌ Неверный формат. Введите дату ДД.ММ.ГГГГ или «сегодня»:")
            return
    await state.update_data(settle_date=entry_date)
    await state.set_state(ZamerySettlementPaySG.enter_comment)
    await message.answer("💬 Комментарий к платежу (или «-» чтобы пропустить):")


@router.message(ZamerySettlementPaySG.enter_comment)
async def gd_zamery_settle_pay_comment(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    comment = (message.text or "").strip()
    if comment in ("-", "—", "нет", ""):
        comment = None
    data = await state.get_data()
    surveyor_id = int(data.get("settle_surveyor_id") or 0)
    amount = float(data.get("settle_amount") or 0)
    entry_date = str(data.get("settle_date") or "")
    await state.clear()
    if not surveyor_id or amount <= 0 or not entry_date:
        await message.answer("❌ Данные платежа потеряны, начните заново.")
        return
    await db.add_zamery_settlement_entry(
        surveyor_id=surveyor_id, entry_date=entry_date, kind="payment",
        amount=amount, comment=comment, created_by=message.from_user.id,  # type: ignore[union-attr]
    )
    await message.answer("✅ Платёж добавлен.")
    await _render_gd_settlement(message, db, config)


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
    # Карточка «Документооборот» (read-only витрина, та же, что у бухгалтера —
    # без кредита/себестоимости) + ниже штатное меню чата бухгалтерии.
    from .accounting_new import build_acc_start_card_text
    try:
        card = await build_acc_start_card_text(db, message.from_user.id)  # type: ignore[union-attr]
        await message.answer(card)
    except Exception:
        log.warning("gd_chat_accounting: acc card render failed", exc_info=True)
    await enter_chat_menu(message, state, channel="accounting")


@router.message(lambda m: (m.text or "").strip().startswith(GD_BTN_MONTAZH))
async def gd_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    # --- Монтажная статистика (эталон-v2 карточка, read-only витрина) ---
    from ..utils import format_card_section
    confirmed = await db.list_installer_confirmed_invoices()
    unconfirmed = await db.list_installer_unconfirmed_invoices()

    stage_counts: dict[str, int] = {}
    for inv in confirmed:
        stage = inv.get("montazh_stage") or "in_work"
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    total = len(confirmed) + len(unconfirmed)

    # Блок 1 — счётчики этапов (центральный эталон-рендерер, числа справа,
    # итог под линией ━ в теле).
    card = format_card_section(
        "🔨", "Монтажная",
        items=[
            ("Ожидают принятия", str(len(unconfirmed))),
            ("В работе", str(stage_counts.get("in_work", 0))),
            ("Размеры ОК", str(stage_counts.get("razmery_ok", 0))),
            ("Счёт ОК", str(stage_counts.get("invoice_ok", 0))),
        ],
        total=str(total),
    )

    # Блок 2 — список ВСЕХ счетов в монтаже левым блоком: кириллица в адресах/
    # номерах не выравнивается колонками ([[feedback_card_telegram_pre_alignment]]),
    # поэтому без колонок — иконка этапа + № + адрес свободным текстом.
    if confirmed:
        # Адрес — единое правило карточек ГД (owner 30.07): Москва → сокр. улица,
        # НЕ Москва → город. Был сырой срез [:20] («г. Москва, Долгору») — из всех
        # карточек ГД только здесь адрес резался посимвольно.
        from ..rp_start_card import _addr_cell
        icons = {"in_work": "🔨", "razmery_ok": "📐", "invoice_ok": "✅"}
        rows = []
        for inv in confirmed:
            num = str(inv.get("invoice_number") or f"#{inv['id']}")
            addr = _addr_cell(inv.get("object_address"), 20) if inv.get("object_address") else "—"
            icon = icons.get(inv.get("montazh_stage") or "in_work", "")
            rows.append(f"  {icon} {num} — {addr}")
        card += "\n<b>🧱  Счета в монтаже</b>\n<pre>" + "\n".join(rows) + "</pre>"

    await message.answer(card)
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
    await enter_chat_menu(message, state, channel="manager_kv", db=db)


@router.message(lambda m: any((m.text or "").strip().startswith(b) for b in (GD_BTN_KIA_CRED, GD_SUBBTN_KIA_CRED)))
async def gd_chat_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="manager_kia", db=db)


@router.message(lambda m: any((m.text or "").strip().startswith(b) for b in (GD_BTN_NPN_CRED, GD_SUBBTN_NPN_CRED)))
async def gd_chat_npn(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await enter_chat_menu(message, state, channel="manager_npn", db=db)


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
    """Показать invoice picker перед вводом сообщения, или сразу перейти к writing.

    Счёт, выбранный при ВХОДЕ в чат («Чат с РП» → gd_chat_invoice_picked, флаг
    invoice_ctx_set), НАСЛЕДУЕТСЯ — тот же вопрос второй раз не задаём (owner
    31.07). Правка 30.07 сняла повтор только на пути «📋 Задачи → ➕ Создать
    задачу» (chat_proxy._can_skip_invoice_picker), а «✏️ Написать» продолжал
    спрашивать: ГД видел вопрос про привязку ДВАЖДЫ и лишь после второго попадал
    на ввод текста. Флаг ставит только rp-вход, прочие каналы не затронуты.
    """
    data = await state.get_data()
    if data.get("invoice_ctx_set"):
        linked = data.get("linked_invoice_id")
        inv_label = ""
        if linked:
            inv = await db.get_invoice(int(linked))
            if inv:
                inv_label = f"\n📋 Счёт: №{inv.get('invoice_number', '?')}"
        await state.set_state(SalesWriteSG.writing)
        await message.answer(
            f"✏️ <b>Написать → {label}</b>{inv_label}\n\n"
            "Введите текст сообщения.\n"
            "Можно прикрепить файлы/фото.\n"
            "Для отмены: /cancel",
        )
        return

    # ГД видит кредит ([[feedback_credit_filter_accounting_only]]): обычные + кредитные.
    # limit=30 + свежие первыми (db.py) — чтобы активный кредит влезал в выборку.
    invoices = await db.list_invoices_for_selection(limit=30, only_regular=True, include_credit=True)
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


@router.message(SalesWriteSG.invoice_pick, F.text == "⬅️ Назад")
@router.message(SalesWriteSG.writing, F.text == "⬅️ Назад")
async def gd_sales_back_to_home(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """«⬅️ Назад» из выбора счёта / ввода сообщения → главное меню ГД."""
    await state.clear()
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    await _show_main_menu(message, db, config, role=role, silent=True)


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

    def _f(n: float) -> str:
        """Format number with spaces as thousands separator."""
        return f"{n:,.0f}".replace(",", " ")

    # Pre-compute open credit invoices (КВ/КИА/НПН).
    # DA остаётся только у последнего open per channel (running carry forward);
    # предыдущие open показываются с DA=0 — их остаток уже передан следующему.
    _credit_open: list[dict] = []
    _credit_total_da = 0.0
    for _crole in ("manager_kv", "manager_kia", "manager_npn"):
        try:
            _cs = await db.get_credit_balance_summary(_crole)
        except Exception:
            log.debug("daily_summary: credit block — %s load failed", _crole, exc_info=True)
            continue
        _open = [r for r in (_cs.get("invoices") or []) if not r.get("is_closed")]
        if not _open:
            continue
        for _prev in _open[:-1]:
            _copy = dict(_prev)
            _copy["da"] = 0.0
            _credit_open.append(_copy)
        _credit_open.append(_open[-1])
        _credit_total_da += float(_cs.get("total_da") or 0)
    _credit_count = len(_credit_open)

    _in_work_total = s['in_work'] + _credit_count
    lines = [
        "<b>📊 Сводка дня</b>\n",
        "<pre>",
        f"{'📄 Счета в работе':─<28s} {_in_work_total:>5}",
        f"{'  Ожидают оплаты':28s} {pending:>5}",
        f"{'  В работе':28s} {in_progress + _credit_count:>5}",
        f"{'  Оплачены':28s} {paid:>5}",
    ]

    # В блок «🏦 Кредит» попадают только счета с положительным остатком:
    # carry-in переданные следующему счёту (da=0) скрываем.
    _credit_visible = [r for r in _credit_open if float(r.get("da") or 0) > 0]
    if _credit_visible:
        lines.append(f"{'─' * 34}")
        lines.append(f"{'🏦 Кредит':28s} {len(_credit_visible):>5}")
        for _r in _credit_visible:
            _num = _r.get("invoice_number") or f"#{_r['id']}"
            _da = float(_r.get("da") or 0)
            lines.append(f"{'  ' + str(_num):28s} {_f(_da):>10}₽")
        lines.append(f"{'  ИТОГО':28s} {_f(_credit_total_da):>10}₽")

    lines.extend([
        f"{'─' * 34}",
        f"{'💵 Финансы':28s}",
        f"{'  Сумма':28s} {_f(total_amt):>10}₽",
        f"{'  Долг':28s} {_f(total_debt):>10}₽",
    ])

    total_tasks = urgent + inv_pay + suppl_pay + s["zp_pending"]
    if total_tasks:
        lines.append(f"{'─' * 34}")
        lines.append(f"{'📋 Открытые задачи':28s} {total_tasks:>5}")
        if urgent:
            lines.append(f"{'  Срочные ГД':28s} {urgent:>5}")
        if inv_pay:
            lines.append(f"{'  Счета на оплату':28s} {inv_pay:>5}")
        if suppl_pay:
            lines.append(f"{'  Оплата поставщику':28s} {suppl_pay:>5}")
        if s["zp_pending"]:
            lines.append(f"{'  ЗП-запросы':28s} {s['zp_pending']:>5}")

    if overdue or today_dl or soon_dl:
        lines.append(f"{'─' * 34}")
        lines.append(f"{'⏰ Дедлайны':28s}")
        if overdue:
            lines.append(f"{'  🔴 Просрочено':28s} {overdue:>5}")
        if today_dl:
            lines.append(f"{'  🔴 Срок сегодня':28s} {today_dl:>5}")
        if soon_dl:
            lines.append(f"{'  ⚠️ До 3 дней':28s} {soon_dl:>5}")

    # --- 📈 Баланс компании (помесячно за текущий год + YTD) ---
    from datetime import datetime as _datetime
    from zoneinfo import ZoneInfo as _ZoneInfo
    _balance_year = _datetime.now(_ZoneInfo("Europe/Moscow")).year
    try:
        _monthly_rows = await db.list_monthly_op_company(year=_balance_year)
    except Exception:
        log.debug("daily_summary: balance_company block — load failed", exc_info=True)
        _monthly_rows = []

    if _monthly_rows:
        _months_nom = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }

        def _signed(n: float) -> str:
            return ("+" + _f(n)) if n >= 0 else _f(n)

        lines.append(f"{'─' * 34}")
        lines.append(f"📈 Баланс компании {_balance_year}")
        for _r in _monthly_rows:
            _m = int(_r.get("month") or 0)
            _label = _months_nom.get(_m, f"M{_m}")
            _bal = float(_r.get("balance_month") or 0)
            lines.append(f"{'  ' + _label:28s} {_signed(_bal):>10s}₽")
        _ytd = float(_monthly_rows[-1].get("balance_running_ytd") or 0)
        lines.append(f"{'─' * 34}")
        lines.append(f"{'  YTD':28s} {_signed(_ytd):>10s}₽")

    lines.append("</pre>")

    # --- 🛒 Закупки (короткая сводка; подробности — по кнопке) ---
    _has_purchases = False
    try:
        from ..utils import build_purchases_summary_line
        _in_work = await db.list_invoices(statuses=["in_progress", "credit"], limit=100)
        _purchases_lines = build_purchases_summary_line(_in_work)
        if _purchases_lines:
            lines.extend(_purchases_lines)
            _has_purchases = True
    except Exception:
        log.debug("daily_summary: purchases summary — render failed", exc_info=True)

    # Build inline keyboard with drill-down buttons for non-zero counts
    b = InlineKeyboardBuilder()
    _summary_btn = [
        ("🔧 В работе", "inv_inprog", in_progress + _credit_count),
    ]
    for label, section, count in _summary_btn:
        if count:
            b.button(
                text=f"{label}: {count}",
                callback_data=SummaryCb(section=section, action="list").pack(),
            )
    # Подробности по закупкам — отдельным сообщением, чтобы не раздувать Сводку.
    if _has_purchases:
        b.button(text="🛒 Подробнее по закупкам", callback_data="purchases:detail")
    # Управление «Баланс компании» — ручной ввод/удаление op_company_entries.
    b.button(text="➕ Добавить расход", callback_data="op_add:start")
    b.button(text="➖ Удалить расход", callback_data="op_del:start")
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

    # ---- In-work invoices (включая кредитные): list only ----
    # Агрегатная сводка убрана (user 2026-05-18) — карточка plan/fact по конкретному
    # счёту (gd_work:view) уже даёт «принятый образец», агрегат дублировал её визуально.
    if section == "inv_inprog":
        invoices = await db.list_invoices(statuses=["in_progress", "credit"], limit=100)
        if not invoices:
            await cb.answer("Список пуст", show_alert=True)
            return
        await cb.answer()

        # Кружок прогресса закупок:
        #   🟢 — закуплено стекло или доп.материалы (продвинутый этап)
        #   🟡 — закуплен только металл
        #   🔴 — закупок нет
        def _purchase_dot(inv: dict) -> str:
            glass = float(inv.get("cost_glass") or 0)
            extra = float(inv.get("cost_extra_mat") or 0)
            metal = float(inv.get("cost_metal") or 0)
            if glass > 0 or extra > 0:
                return "🟢"
            if metal > 0:
                return "🟡"
            return "🔴"

        for inv in invoices:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = inv.get("object_address") or ""
            dot = _purchase_dot(inv)
            label = f"{dot} {num} — {addr}"[:62]
            b.button(text=label, callback_data=f"gd_work:view:{inv['id']}")
        b.button(text="⬅️ Назад к сводке", callback_data=SummaryCb(section="", action="back").pack())
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"<b>🔧 В работе</b> ({len(invoices)})\n\nВыберите счёт:",
            reply_markup=b.as_markup(),
        )
        return

    # ---- Invoice sections ----
    if section.startswith("inv_"):
        status_map = {
            "inv_pending": ("pending", "⏳ Ожидают оплаты"),
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
            addr = inv.get("object_address") or ""
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
            addr = inv.get("object_address") or ""
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
            addr = inv.get("object_address") or ""
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


@router.callback_query(F.data == "purchases:detail")
async def gd_purchases_detail(cb: CallbackQuery, db: Database) -> None:
    """Подробная таблица закупок по каждому счёту in_progress+credit — отдельным сообщением."""
    from ..utils import build_purchases_in_work_block

    invoices = await db.list_invoices(statuses=["in_progress", "credit"], limit=100)
    lines = build_purchases_in_work_block(invoices)
    if not lines:
        await cb.answer("Нет счетов в работе", show_alert=True)
        return

    text = "\n".join(lines)
    TG_LIMIT = 4000  # запас 96 байт под HTML-теги

    if len(text) <= TG_LIMIT:
        await cb.message.answer(text)  # type: ignore[union-attr]
        await cb.answer()
        return

    # Chunking — режем по границам строк, не разрывая <pre>…</pre>.
    chunks: list[list[str]] = []
    cur: list[str] = []
    cur_len = 0
    in_pre = False
    for ln in lines:
        # +1 на разделитель \n при join
        if cur_len + len(ln) + 1 > TG_LIMIT and not in_pre and cur:
            chunks.append(cur)
            cur = []
            cur_len = 0
        cur.append(ln)
        cur_len += len(ln) + 1
        if "<pre>" in ln:
            in_pre = True
        if "</pre>" in ln:
            in_pre = False
    if cur:
        chunks.append(cur)

    for chunk in chunks:
        await cb.message.answer("\n".join(chunk))  # type: ignore[union-attr]
    await cb.answer()


# ---------------------------------------------------------------------------
# "Синхронизация данных" — Google Sheets resync from GD main menu
# ---------------------------------------------------------------------------

@router.message(
    lambda m: (m.text or "").strip() == GD_BTN_SYNC
)
async def gd_sync_data(
    message: Message,
    db: Database,
    config: Config,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Trigger Google Sheets resync + show ONE consolidated card."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    # Typing-индикатор пока бот тянет ОП-лист и экспортирует.
    try:
        await message.bot.send_chat_action(message.chat.id, "typing")  # type: ignore[union-attr]
    except Exception:
        pass

    # --- 1. Выполнить sync с Google Sheets (stats не показываем в карточке) ---
    if integrations.sheets:
        try:
            await import_from_source_sheet(
                db, integrations.sheets,
                log_prefix="gd_sync",
                integrations=integrations,
                notifier=notifier,
                config=config,
            )
            await export_to_sheets(
                db, integrations.sheets,
                include_invoice_cost=True,
                sync_invoices=True,
                amocrm_user_map=getattr(config, "amocrm_user_map", None),
                amocrm=integrations.amocrm,
            )
        except Exception:
            log.exception("gd_sync: Google Sheets sync failed")

    # --- 2-3. Карточка sync_data_v2 ---
    # Логика вынесена в utils.build_gd_sync_card_text, чтобы daily_sync.py
    # мог отправлять идентичную карточку всем admin_ids после 09:00 МСК cron.
    text = await build_gd_sync_card_text(db, config, user_id)

    markup = private_only_reply_markup(
        message,
        main_menu(
            Role.GD,
            is_admin=is_admin,
            unread=await db.count_unread_tasks(user_id),
            unread_channels=await db.count_unread_by_channel(user_id),
            gd_inbox_unread=await db.count_gd_inbox_tasks(user_id),
            gd_invoice_unread=await db.count_gd_invoice_tasks(user_id),
            gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(user_id),
            gd_supplier_pay_unread=await db.count_gd_supplier_pay_tasks(user_id),
            gd_total_open_tasks=await db.count_gd_more_total_open_tasks(user_id),
        ),
    )

    # Карточка остаётся в чате permanent (без auto-delete) — Сергей жмёт sync
    # редко и должен видеть последнюю сводку при следующем заходе.
    await message.answer(text, reply_markup=markup)

    # Статистика по менеджерам теперь ВНУТРИ стартовой карты (секцией между «Лиды» и
    # «Кредитный баланс», см. utils.build_gd_sync_card_text) — отдельное сообщение
    # убрано (owner 14.07). График замеров (календарь) переехал на кнопку «Замеры».

    # Перенос ПЕРЕПЛАТЫ ЗП менеджера на баланс аванса (owner 23.06): после синка
    # ОП общая сумма удержаний (CN) каждого менеджера по счетам с погашенным долгом
    # переносится ОДНОЙ строкой на его баланс аванса (идемпотентно — только дельта).
    # Менеджер далее сам распределяет аванс по объектам, гасится из ЗП по правилам
    # авансирования. Бланк ЗП при этом платится полностью (без двойного счёта).
    try:
        swept = await db.sweep_manager_overpay_to_advance()
        if swept and integrations.sheets:
            try:
                await integrations.sync_advances_journal()
            except Exception:
                log.warning("gd_sync: sync_advances_journal after overpay sweep failed")
    except Exception:
        log.exception("gd_sync: manager overpay sweep failed")

    # Карточки «Перерасчёт прибыли» БОЛЬШЕ НЕ присылаются автоматически после синка
    # (owner 27.06: убрать из стартовых карточек ГД). Доступны по требованию через
    # кнопку «📊 Перерасчёт прибыли» (GD_BTN_RECALC → gd_manager_recalc). Функция
    # _send_manager_recalc_cards и кнопка сохранены.


# ---------------------------------------------------------------------------
# «📊 Перерасчёт прибыли» — карточки по счетам с переплатой ЗП менеджера (CN≠0,
# долг погашен). Кнопка в подменю ГД «Ещё» + авто-присылка после синка. owner 23.06.
# ---------------------------------------------------------------------------

async def _send_manager_recalc_cards(message: Message, db: Database) -> int:
    """Отправить ГД карточки «Перерасчёт прибыли» по всем счетам под механизмом
    (CN≠0 И долг=0). Возвращает число отправленных. Display-only, деньги не трогает.

    ТЗ 02.07: на каждой карточке — inline «📨 Отправить менеджеру» (→ recalc_send).
    Если задача по счёту уже открыта — кнопка не показывается (ждём согласия)."""
    ids = await db.list_invoices_under_recalc()
    for inv_id in ids:
        inv = await db.get_invoice(inv_id)
        if not inv:
            continue
        card = format_manager_recalc_card(inv)
        markup = None
        if await db.invoice_recalc_already_sent(inv_id):
            card += "\n<i>📨 Уже отправлено менеджеру.</i>"
        else:
            b = InlineKeyboardBuilder()
            b.button(text="📨 Отправить менеджеру", callback_data=f"recalc_send:{inv_id}")
            b.adjust(1)
            markup = b.as_markup()
        await message.answer(card, reply_markup=markup)
    return len(ids)


@router.message(
    lambda m: (m.text or "").strip() == GD_BTN_RECALC
)
async def gd_manager_recalc(message: Message, db: Database) -> None:
    """ГД жмёт «📊 Перерасчёт прибыли» — карточки по счетам с переплатой ЗП мен."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    n = await _send_manager_recalc_cards(message, db)
    if n == 0:
        await message.answer(
            "✅ Нет счетов под перерасчётом ЗП менеджера.\n"
            "<i>Условие: заполнена переплата (CN) и долг по счёту погашен.</i>"
        )


@router.callback_query(F.data.regexp(r"^recalc_send:\d+$"))
async def gd_recalc_send(
    cb: CallbackQuery, db: Database, notifier: Notifier, config: Config,
) -> None:
    """ТЗ 02.07: ГД жмёт «📨 Отправить менеджеру» на карточке перерасчёта.

    Создаёт задачу RECALC_CONFIRM менеджеру-автору счёта (created_by) и шлёт ему
    карточку с кнопкой «✅ С перерасчётом согласен». Деньги НЕ трогает — аванс
    зачисляется только после согласия менеджера (recalc_agree). Дедуп по счёту."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Отправляем ОСТАТОК, а не полный |CN| (фикс 30.07): часть переплаты мог уже
    # перенести авто-свип на синке ГД, и тогда согласие менеджера начислило бы
    # аванс второй раз. zp_hold_advanced — общий трекер обоих каналов.
    cn_abs = abs(float(inv.get("zp_manager_hold") or 0))  # |CN|
    advanced = float(inv.get("zp_hold_advanced") or 0)
    amount = round(cn_abs - advanced, 2)
    if cn_abs <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ По счёту нет переплаты ЗП (CN=0) — отправлять нечего.")
        return
    if amount <= 0:
        adv_s = f"{advanced:,.0f}".replace(",", " ")
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Переплата по счёту уже перенесена в аванс менеджера "
            f"({adv_s} ₽) — отправлять нечего.")
        return
    manager_id = inv.get("created_by")
    if not manager_id:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ У счёта не указан менеджер-автор (created_by) — некому отправить.")
        return
    if await db.invoice_recalc_already_sent(inv_id):
        await cb.message.answer(  # type: ignore[union-attr]
            "⏳ По этому счёту перерасчёт уже отправлен менеджеру (ждём согласия "
            "или аванс уже зачислен). Повторно не отправляем.")
        return
    num = inv.get("invoice_number") or "—"
    gd_id = cb.from_user.id  # type: ignore[union-attr]
    try:
        await db.create_task(
            project_id=None,
            type_=TaskType.RECALC_CONFIRM,
            status=TaskStatus.OPEN,
            created_by=gd_id,
            assigned_to=int(manager_id),
            due_at_iso=None,
            payload={
                "invoice_id": inv_id,
                "invoice_number": num,
                "amount": amount,
                "gd_id": gd_id,
            },
        )
    except ValueError as e:
        await cb.message.answer(f"⚠️ Не удалось создать задачу: {e}")  # type: ignore[union-attr]
        return
    # Пуш карточки менеджеру + кнопка согласия.
    b = InlineKeyboardBuilder()
    b.button(text="✅ С перерасчётом согласен", callback_data=f"recalc_agree:{inv_id}")
    b.adjust(1)
    await notifier.safe_send(
        int(manager_id),
        format_manager_recalc_card(inv),
        reply_markup=b.as_markup(),
    )
    try:
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    except Exception:
        log.warning("recalc_send: refresh keyboard failed for %s", manager_id)
    # Снять кнопку у ГД (защита от повторной отправки) + подтвердить.
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    amt_s = f"{amount:,.0f}".replace(",", " ")
    await cb.message.answer(  # type: ignore[union-attr]
        f"📨 Отправлено менеджеру по счёту №{num}.\n"
        f"После нажатия «✅ Согласен» сумма перерасчёта <b>{amt_s} ₽</b> уйдёт в его "
        f"авансовый кошелёк как выданный аванс.\n"
        f"<i>После зачисления счёт сам уйдёт из списка перерасчёта — повторно "
        f"начислено не будет. Обнулить CN в «Импорт ОП» (CF) можно для чистоты "
        f"листа: столбец BZ до этого показывает «Перерасчет прибыли».</i>"
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


# ============================================================================
# Op Company entries — UI для ручного ввода операционных расходов компании.
# Пишет в БД op_company_entries → бот мержит с «Импорт ОП» и зеркалит
# в лист «Баланс компании». В «Импорт ОП» запись запрещена.
# ============================================================================

_OP_TYPE_LABELS = {
    "cashless": "💰 Б/н расход",
    "taxes": "🏦 Налоги",
    "loan": "💸 Займ",
    "cash": "💵 Нал/прочее",
}

# Пресет «ЗП Директор»: фикс-описание + сумма по умолчанию (можно менять).
# Пишется как cashless (б/н) → столбец C листа «Баланс компании». НДС нет (ЗП).
DIRECTOR_ZP_DESC = "ЗП Директор"
DIRECTOR_ZP_DEFAULT = 12362.0


def _fmt_rub(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ")


async def _op_render_confirm(target: Message, state: FSMContext) -> None:
    """Отрисовать карточку подтверждения расхода по данным FSM (переиспользуется
    generic-флоу через описание и пресет-флоу «ЗП Директор»)."""
    data = await state.get_data()
    typ = data["type"]
    amount = float(data["amount"])
    desc = data.get("description") or ""

    extra_lines: list[str] = []
    bind_num = data.get("bind_invoice_number")
    if bind_num:
        # Bound-путь: показать привязку; НДС-строк нет (не задаётся).
        _choices, _labels = _bn_bind_categories()
        cat = data.get("bind_category") or ""
        extra_lines.append(f"  Счёт: №{html.escape(str(bind_num))}")
        extra_lines.append(f"  Категория: {_labels.get(cat, cat)}")
        extra_lines.append("  → в расходы счёта (DP–DV)")
    elif typ == "cashless":
        nds_val = float(data.get("nds") or 0)
        if nds_val > 0:
            extra_lines.append(f"  НДС (22%): {_fmt_rub(nds_val)} ₽")
            extra_lines.append(f"  Чистая: {_fmt_rub(amount - nds_val)} ₽")
        else:
            extra_lines.append("  Без НДС")
    if typ == "loan":
        ld = data.get("loan_direction")
        extra_lines.append(f"  Направление: {'📥 Входящий' if ld == 'in' else '📤 Возврат'}")

    text = (
        "<b>Подтвердите расход:</b>\n\n"
        f"  Тип: {_OP_TYPE_LABELS.get(typ, typ)}\n"
        f"  Сумма: <b>{_fmt_rub(amount)}</b> ₽\n"
        + ("\n".join(extra_lines) + "\n" if extra_lines else "")
        + f"  Описание: {html.escape(desc)}"
    )

    b = InlineKeyboardBuilder()
    b.button(text="✅ Записать", callback_data="op_add:confirm")
    b.button(text="❌ Отмена", callback_data="op_add:cancel")
    b.adjust(1)
    await target.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "op_add:start")
async def op_add_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 1: выбор типа расхода."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await state.clear()
    await state.set_state(OpAddSG.type)

    b = InlineKeyboardBuilder()
    for key, label in _OP_TYPE_LABELS.items():
        b.button(text=label, callback_data=f"op_add:type:{key}")
    b.button(text="🧑‍💼 ЗП Директор", callback_data="op_add:preset:director")
    b.button(text="❌ Отмена", callback_data="op_add:cancel")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "<b>➕ Добавить расход</b>\n\nВыберите тип расхода:",
        reply_markup=b.as_markup(),
    )
    await cb.answer()


@router.callback_query(F.data == "op_add:preset:director")
async def op_add_preset_director(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Пресет «ЗП Директор»: cashless + фикс-описание, сумма по умолчанию 12362
    (можно ввести свою). Пропускает шаги НДС и описания."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await state.clear()
    await state.update_data(
        type="cashless", description=DIRECTOR_ZP_DESC, nds=None, preset="director",
    )
    await state.set_state(OpAddSG.amount)
    b = InlineKeyboardBuilder()
    b.button(text=f"✅ {_fmt_rub(DIRECTOR_ZP_DEFAULT)} ₽", callback_data="op_add:preset_amt")
    b.button(text="❌ Отмена", callback_data="op_add:cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"<b>🧑‍💼 ЗП Директор</b>\n\n"
        f"Сумма по умолчанию <b>{_fmt_rub(DIRECTOR_ZP_DEFAULT)}</b> ₽.\n"
        f"Нажмите кнопку ниже или введите другую сумму ₽:",
        reply_markup=b.as_markup(),
    )
    await cb.answer()


@router.callback_query(F.data == "op_add:preset_amt", StateFilter(OpAddSG.amount))
async def op_add_preset_amt(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Пресет: подтвердить сумму по умолчанию → сразу карточка подтверждения."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    if (await state.get_data()).get("preset") != "director":
        await cb.answer("Сессия истекла, начните заново", show_alert=True)
        return
    await state.update_data(amount=DIRECTOR_ZP_DEFAULT)
    await state.set_state(OpAddSG.confirm)
    await _op_render_confirm(cb.message, state)  # type: ignore[arg-type]
    await cb.answer()


def _bn_bind_categories() -> tuple[list[tuple[str, str]], dict[str, str]]:
    """Категории затрат bound-расхода — те же 7, что в кредит-кошельке.

    Lazy-import из manager_new (по образцу rp_new ↔ installer_new): единый
    источник ключей cost_type → лейблов, без риска circular import.
    """
    from .manager_new import _CREDIT_COST_CHOICES, _CREDIT_COST_LABELS
    return _CREDIT_COST_CHOICES, _CREDIT_COST_LABELS


@router.callback_query(F.data.startswith("op_add:type:"))
async def op_add_type(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 2: cashless → развилка «привязка/без» (ТЗ 16.07); прочие типы → сумма."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    typ = cb.data.split(":")[-1]
    if typ not in _OP_TYPE_LABELS:
        await cb.answer("Неизвестный тип", show_alert=True)
        return
    await state.update_data(type=typ)

    if typ == "cashless":
        # ТЗ owner 16.07: «Б/н расход» — спросить про привязку к счёту.
        # Канон развилки — CreditWalletSpendSG (manager_new): привязка →
        # расходы счёта DP–DV, без привязки → «Баланс компании» (как раньше).
        await state.set_state(OpAddSG.bind_mode)
        b = InlineKeyboardBuilder()
        b.button(text="🔗 С привязкой к счёту", callback_data="op_add:bind:bound")
        b.button(text="📄 Без привязки", callback_data="op_add:bind:free")
        b.button(text="❌ Отмена", callback_data="op_add:cancel")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"<b>{_OP_TYPE_LABELS[typ]}</b>\n\n"
            "Расход с привязкой к счёту?\n"
            "🔗 <b>С привязкой</b> — ляжет в расходы счёта (DP–DV)\n"
            "📄 <b>Без привязки</b> — в «Баланс компании» (как раньше)",
            reply_markup=b.as_markup(),
        )
        await cb.answer()
        return

    await state.set_state(OpAddSG.amount)
    await cb.message.answer(  # type: ignore[union-attr]
        f"<b>{_OP_TYPE_LABELS[typ]}</b>\n\nВведите сумму ₽ (например: <code>15000</code>):"
    )
    await cb.answer()


@router.callback_query(F.data.startswith("op_add:bind:"), StateFilter(OpAddSG.bind_mode))
async def op_add_bind_choice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 2а (cashless): «с привязкой» → пикер счёта; «без» → сумма (старый путь)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    choice = cb.data.split(":")[-1]
    if choice == "free":
        await state.set_state(OpAddSG.amount)
        await cb.message.answer(  # type: ignore[union-attr]
            f"<b>{_OP_TYPE_LABELS['cashless']}</b>\n\nВведите сумму ₽ (например: <code>15000</code>):"
        )
        await cb.answer()
        return
    if choice != "bound":
        await cb.answer("Неверный выбор", show_alert=True)
        return

    # Материнские счета: «в работе» (cap 30) + последние закрытые 🏁 (cap 10) —
    # scope как у ГД в кредит-кошельке (manager_new.py cwspend).
    invoices = await db.list_invoices_in_work(limit=60, include_credit=True)
    invoices = [i for i in invoices if not i.get("parent_invoice_id")]
    ended = await db.list_ended_invoices(limit=25, include_credit=True)
    inwork_ids = {i["id"] for i in invoices}
    ended = [i for i in ended if not i.get("parent_invoice_id") and i["id"] not in inwork_ids][:10]

    if not invoices and not ended:
        b = InlineKeyboardBuilder()
        b.button(text="📄 Без привязки", callback_data="op_add:bind:free")
        b.button(text="❌ Отмена", callback_data="op_add:cancel")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            "Нет счетов для привязки — можно записать без привязки:",
            reply_markup=b.as_markup(),
        )
        await cb.answer()
        return

    await state.set_state(OpAddSG.bind_invoice)
    b = InlineKeyboardBuilder()
    for inv in invoices[:30]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        lbl = f"📄 {num}" + (f" · {addr}" if addr else "")
        b.button(text=lbl, callback_data=f"op_add:inv:{inv['id']}")
    for inv in ended:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        lbl = f"🏁 {num}" + (f" · {addr}" if addr else "")
        b.button(text=lbl, callback_data=f"op_add:inv:{inv['id']}")
    b.button(text="❌ Отмена", callback_data="op_add:cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "Выберите счёт для привязки расхода:", reply_markup=b.as_markup(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("op_add:inv:"), StateFilter(OpAddSG.bind_invoice))
async def op_add_bind_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 2б (bound): счёт выбран → категория затрат (7 кнопок, как cwspend)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.answer("Счёт не найден", show_alert=True)
        return
    if inv.get("parent_invoice_id"):
        # cost_card на лист считается только для материнских счетов.
        await cb.answer("Это дочерний счёт — выберите материнский", show_alert=True)
        return
    await state.update_data(
        bind_invoice_id=invoice_id,
        bind_invoice_number=inv.get("invoice_number") or f"#{invoice_id}",
    )
    await state.set_state(OpAddSG.bind_category)
    choices, _labels = _bn_bind_categories()
    b = InlineKeyboardBuilder()
    for ct, lbl in choices:
        b.button(text=lbl, callback_data=f"op_add:cat:{ct}")
    b.button(text="❌ Отмена", callback_data="op_add:cancel")
    b.adjust(2)
    await cb.message.answer(  # type: ignore[union-attr]
        f"Счёт №{html.escape(str(inv.get('invoice_number') or invoice_id))}.\n\nКатегория затрат:",
        reply_markup=b.as_markup(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("op_add:cat:"), StateFilter(OpAddSG.bind_category))
async def op_add_bind_category(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 2в (bound): категория выбрана → сумма."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    cost_type = cb.data.split(":")[-1]
    choices, labels = _bn_bind_categories()
    if cost_type not in labels:
        await cb.answer("Неизвестная категория", show_alert=True)
        return
    await state.update_data(bind_category=cost_type)
    await state.set_state(OpAddSG.amount)
    await cb.message.answer(  # type: ignore[union-attr]
        f"Категория: <b>{labels[cost_type]}</b>.\n\nВведите сумму ₽ (например: <code>15000</code>):"
    )
    await cb.answer()


@router.message(StateFilter(OpAddSG.amount))
async def op_add_amount(message: Message, state: FSMContext, db: Database) -> None:
    """Шаг 2.5: парс суммы → следующий шаг (НДС / направление займа / описание)."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    txt = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(txt)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Сумма должна быть положительным числом. Попробуйте ещё раз:")
        return
    await state.update_data(amount=amount)
    data = await state.get_data()
    typ = data["type"]

    if data.get("preset") == "director":
        # Пресет «ЗП Директор»: описание и НДС уже заданы → сразу подтверждение.
        await state.set_state(OpAddSG.confirm)
        await _op_render_confirm(message, state)
        return

    if data.get("bind_invoice_id"):
        # Bound-путь (привязка к счёту): НДС не задаём — у supplier_payments /
        # invoices.cost_* поля НДС нет (bound-канон cwspend его тоже не задаёт).
        await state.set_state(OpAddSG.description)
        await message.answer(
            "Введите назначение расхода (например: <code>Металл Оптима-профиль</code>):"
        )
        return

    if typ == "cashless":
        nds_auto = round(amount * 22.0 / 122.0, 2)
        net_auto = round(amount - nds_auto, 2)
        b = InlineKeyboardBuilder()
        b.button(text=f"✅ С НДС 22% (НДС = {_fmt_rub(nds_auto)} ₽)", callback_data="op_add:nds:auto")
        b.button(text="➖ Без НДС", callback_data="op_add:nds:none")
        b.button(text="❌ Отмена", callback_data="op_add:cancel")
        b.adjust(1)
        await state.set_state(OpAddSG.nds)
        await message.answer(
            f"<b>Сумма: {_fmt_rub(amount)} ₽</b>\n\n"
            f"Расчётная ставка НДС 22%:\n"
            f"  • С НДС: {_fmt_rub(amount)} ₽\n"
            f"  • НДС: {_fmt_rub(nds_auto)} ₽\n"
            f"  • Чистая (без НДС): {_fmt_rub(net_auto)} ₽\n\n"
            f"Выберите вариант:",
            reply_markup=b.as_markup(),
        )
    elif typ == "loan":
        b = InlineKeyboardBuilder()
        b.button(text="📥 Входящий (+)", callback_data="op_add:loan_dir:in")
        b.button(text="📤 Возврат (−)", callback_data="op_add:loan_dir:refund")
        b.adjust(1)
        await state.set_state(OpAddSG.loan_direction)
        await message.answer("Это входящий займ или возврат?", reply_markup=b.as_markup())
    else:
        await state.set_state(OpAddSG.description)
        await message.answer("Введите описание (например: <code>Реклама Яндекс.Директ</code>):")


@router.callback_query(F.data.startswith("op_add:nds:"), StateFilter(OpAddSG.nds))
async def op_add_nds_choice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 3 (cashless): выбор НДС (22% авто / без НДС) → описание."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    choice = cb.data.split(":")[-1]
    if choice not in ("auto", "none"):
        await cb.answer("Неверный выбор", show_alert=True)
        return
    data = await state.get_data()
    amount = float(data["amount"])
    if choice == "auto":
        nds = round(amount * 22.0 / 122.0, 2)
        await state.update_data(nds=nds)
        nds_label = f"НДС 22% = {_fmt_rub(nds)} ₽"
    else:
        await state.update_data(nds=None)
        nds_label = "Без НДС"
    await state.set_state(OpAddSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        f"{nds_label}.\n\nВведите описание (например: <code>Реклама Авито</code>):"
    )
    await cb.answer()


@router.callback_query(F.data.startswith("op_add:loan_dir:"), StateFilter(OpAddSG.loan_direction))
async def op_add_loan_dir(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 3' (loan): сохранить направление → описание."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    direction = cb.data.split(":")[-1]  # "in" | "refund"
    if direction not in ("in", "refund"):
        await cb.answer("Неверное направление", show_alert=True)
        return
    await state.update_data(loan_direction=direction)
    await state.set_state(OpAddSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        "Введите описание (например: <code>Возврат займа Бобров</code>):"
    )
    await cb.answer()


@router.message(StateFilter(OpAddSG.description))
async def op_add_description(message: Message, state: FSMContext, db: Database) -> None:
    """Шаг 4: парс описания → карточка подтверждения."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    desc = (message.text or "").strip()
    if not (3 <= len(desc) <= 200):
        await message.answer("⚠️ Описание должно быть 3–200 символов. Попробуйте ещё раз:")
        return
    await state.update_data(description=desc)
    await state.set_state(OpAddSG.confirm)
    await _op_render_confirm(message, state)


@router.callback_query(F.data == "op_add:confirm", StateFilter(OpAddSG.confirm))
@money_confirm_guard
async def op_add_confirm(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    config: Config,
    notifier: Notifier,
) -> None:
    """Шаг 5: запись расхода.

    Free-путь: INSERT в op_company_entries + sync «Баланс компании» + audit.
    Bound-путь (ТЗ 16.07): create_supplier_payment → invoices.cost_*/DP–DV
    материнского счёта + sync_invoice_row + audit; в op_company_entries НЕ
    пишем (задвоение в «Балансе компании»), кредит-кошелёк НЕ трогаем.
    @money_confirm_guard — анти-двойной-клик (прецедент 05.06: тройная запись).
    """
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    data = await state.get_data()
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _zi
    now = _dt.now(_zi("Europe/Moscow"))
    typ = data["type"]
    amount = float(data["amount"])
    description = data["description"]

    bind_invoice_id = data.get("bind_invoice_id")
    if typ == "cashless" and bind_invoice_id:
        _choices, _labels = _bn_bind_categories()
        cost_type = data.get("bind_category") or "extra_mat"
        bind_num = str(data.get("bind_invoice_number") or "")
        # Назначение кладём в поле supplier (свободный текст) — своей колонки
        # у supplier_payments нет; полный след — в audit_log.
        sp_id = await db.create_supplier_payment(
            parent_invoice_id=int(bind_invoice_id),
            amount=amount,
            material_type=cost_type,
            invoice_number=bind_num,
            supplier=description[:100],
            task_id=None,
            created_by=cb.from_user.id,
        )
        sync_note = ""
        try:
            await integrations.sync_invoice_row(int(bind_invoice_id))
        except Exception as ex:
            log.warning("op_add bound: sync_invoice_row failed: %s", ex)
            sync_note = "\n\n⚠️ Строка Invoices не пересинхронизирована (ошибка)."

        await db.audit(
            actor_id=cb.from_user.id,
            action="op_bn_expense_bound_added",
            entity="supplier_payments",
            entity_id=str(sp_id),
            payload={
                "invoice_id": int(bind_invoice_id),
                "invoice_number": bind_num,
                "amount": amount,
                "cost_type": cost_type,
                "description": description,
                "source": "gd_op_add_bound",
            },
        )

        # Стекло/доп.материалы по НАЁМНОМУ счёту → задача ГД на ЗП монтаж
        # (owner 06.08). Отдельный try: расход уже записан, и сбой этой ветки
        # не должен превращаться в ошибку записи расхода.
        try:
            from .installer_new import on_invoice_cost_recorded
            _naem = await on_invoice_cost_recorded(
                db, config, notifier, integrations,
                invoice_id=int(bind_invoice_id),
                material_type=cost_type,
                amount=amount,
                actor_id=cb.from_user.id,
            )
            if _naem.get("created"):
                log.info(
                    "naem_zp: задача ГД открыта по б/н расходу %s, счёт=%s сумма=%s",
                    cost_type, bind_num, _naem.get("amount"),
                )
        except Exception:
            log.warning(
                "naem_zp: авто-задача ЗП не создана (gd bound inv=%s)",
                bind_invoice_id, exc_info=True,
            )

        await state.clear()
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"✅ <b>Б/н расход записан на счёт</b> (#{sp_id})\n\n"
                f"  Счёт: №{html.escape(bind_num)}\n"
                f"  Категория: {_labels.get(cost_type, cost_type)}\n"
                f"  Сумма: <b>{_fmt_rub(amount)}</b> ₽\n"
                f"  Назначение: {html.escape(description)}"
                + sync_note
            )
        except Exception:
            pass
        await cb.answer("Сохранено")
        return

    kw: dict[str, object] = {
        "year": now.year,
        "month": now.month,
        "source": "manual_bot_entry",
        "date_iso": now.strftime("%Y-%m-%d"),
    }
    date_dmy = now.strftime("%d.%m.%Y")

    if typ == "cashless":
        kw["cashless_amount"] = amount
        if data.get("nds"):
            kw["nds"] = float(data["nds"])
        kw["description"] = description
        kw["date_display"] = date_dmy
    elif typ == "taxes":
        kw["taxes"] = amount
        kw["description"] = description
        kw["date_display"] = date_dmy
    elif typ == "loan":
        signed = amount if data.get("loan_direction") == "in" else -amount
        kw["loan"] = signed
        kw["description"] = description
        kw["date_display"] = date_dmy
    elif typ == "cash":
        kw["other_amount"] = amount
        kw["description_credit"] = description
        kw["date_other_display"] = date_dmy

    entry_id = await db.add_op_company_entry(**kw)

    sync_note = ""
    if integrations.sheets:
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception as ex:
            log.warning("op_add: sync_balance_company_sheet failed: %s", ex)
            sync_note = "\n\n⚠️ Лист «Баланс компании» не пересинхронизирован (ошибка)."
    else:
        sync_note = "\n\n⚠️ Sheets disabled — лист не обновлён."

    await db.audit(
        actor_id=cb.from_user.id,
        action="op_company_expense_added",
        entity="op_company_entries",
        entity_id=str(entry_id),
        payload={"type": typ, **{k: v for k, v in kw.items() if k != "source"}},
    )

    await state.clear()
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ <b>Расход записан</b> (#{entry_id})\n\n"
            f"  Тип: {_OP_TYPE_LABELS.get(typ, typ)}\n"
            f"  Сумма: <b>{_fmt_rub(amount)}</b> ₽\n"
            f"  Описание: {html.escape(description)}"
            + sync_note
        )
    except Exception:
        pass
    await cb.answer("Сохранено")


@router.callback_query(F.data == "op_add:cancel")
async def op_add_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена FSM (на любом шаге)."""
    await state.clear()
    try:
        await cb.message.edit_text("❌ Отменено.")  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()


# ----- Удаление op_company_entries -----

@router.callback_query(F.data == "op_del:start")
async def op_del_start(cb: CallbackQuery, db: Database) -> None:
    """Показать последние 10 записей текущего месяца → кнопки удаления."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _zi
    now = _dt.now(_zi("Europe/Moscow"))
    entries = await db.list_op_company_entries(year=now.year, month=now.month)
    if not entries:
        await cb.answer(
            f"Нет записей за {now.month:02d}.{now.year}. "
            f"Доступны только записи, добавленные через бот.",
            show_alert=True,
        )
        return

    entries = entries[-10:]
    b = InlineKeyboardBuilder()
    lines = [
        f"<b>➖ Удалить расход</b>",
        f"Записи за {now.month:02d}.{now.year} (последние {len(entries)}):\n",
    ]
    for e in entries:
        eid = e["id"]
        desc = (e.get("description") or e.get("description_credit") or "")[:40]
        amt = (
            e.get("cashless_amount")
            or e.get("taxes")
            or e.get("loan")
            or e.get("other_amount")
            or 0
        )
        label = f"#{eid} · {_fmt_rub(abs(float(amt)))}₽ · {desc}"[:60]
        b.button(text=label, callback_data=f"op_del:pick:{eid}")
        lines.append(f"  #{eid}: {html.escape(desc)} — {_fmt_rub(abs(float(amt)))} ₽")
    b.button(text="⬅️ Отмена", callback_data="op_add:cancel")
    b.adjust(1)

    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]
    await cb.answer()


@router.callback_query(F.data.startswith("op_del:pick:"))
async def op_del_pick(
    cb: CallbackQuery,
    db: Database,
    integrations: IntegrationHub,
) -> None:
    """Удалить выбранную запись + sync + audit."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    try:
        entry_id = int(cb.data.split(":")[-1])
    except ValueError:
        await cb.answer("Неверный ID", show_alert=True)
        return

    entries = await db.list_op_company_entries()
    entry = next((e for e in entries if int(e["id"]) == entry_id), None)
    if not entry:
        await cb.answer("Запись не найдена", show_alert=True)
        return

    await db.delete_op_company_entry(entry_id)

    sync_note = ""
    if integrations.sheets:
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception as ex:
            log.warning("op_del: sync_balance_company_sheet failed: %s", ex)
            sync_note = "\n⚠️ Лист не пересинхронизирован."

    await db.audit(
        actor_id=cb.from_user.id,
        action="op_company_expense_deleted",
        entity="op_company_entries",
        entity_id=str(entry_id),
        payload=dict(entry),
    )

    desc = (entry.get("description") or entry.get("description_credit") or "")[:50]
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ Удалена запись #{entry_id}: {html.escape(desc)}" + sync_note
        )
    except Exception:
        pass
    await cb.answer("Удалено")


# =====================================================================
# ТЗ 2026-05-19 блок C: Авансы монтажника — ГД approve/reject/pay.
# =====================================================================


@router.callback_query(F.data.startswith("gd_adv_appr:"))
async def gd_advance_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """ГД одобряет запрос аванса."""
    if not cb.from_user:
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_advance_request(req_id)
    if not req:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return
    if req["status"] != "requested":
        await cb.message.answer(f"⚠️ Запрос уже {req['status']}.")  # type: ignore[union-attr]
        return
    await db.approve_advance_request(req_id, approved_by=cb.from_user.id)
    items = await db.get_advance_request_items(req_id)
    total = sum(float(i["amount"]) for i in items)
    await integrations.sync_advances_journal()
    b = InlineKeyboardBuilder()
    b.button(text="💸 Оплатить", callback_data=f"gd_adv_pay:{req_id}")
    b.adjust(1)
    try:
        await cb.message.edit_reply_markup(reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос аванса #{req_id} одобрен — {total:,.0f} ₽.\n"
        "Нажмите «💸 Оплатить» когда сделаете платёж.",
    )
    # Notify монтажника
    await notifier.safe_send(
        int(req["installer_id"]),
        f"✅ <b>Аванс одобрен</b>\n"
        f"Запрос #{req_id} — {total:,.0f} ₽.\n"
        "Ожидайте оплату.",
    )


@router.callback_query(F.data.startswith("gd_adv_rej:"))
async def gd_advance_reject_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД нажимает «Отклонить» — FSM ввода причины."""
    if not cb.from_user:
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_advance_request(req_id)
    if not req:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return
    if req["status"] != "requested":
        await cb.message.answer(f"⚠️ Запрос уже {req['status']}.")  # type: ignore[union-attr]
        return
    await state.set_state(GdAdvanceRejectSG.reason)
    await state.update_data(advance_req_id=req_id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"❌ Отклонение запроса #{req_id}.\n"
        "Введите причину (текст ≥ 3 символов):",
    )


@router.message(GdAdvanceRejectSG.reason, F.text)
async def gd_advance_reject_reason_input(
    message: Message, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not message.from_user:
        return
    reason = (message.text or "").strip()
    if len(reason) < 3:
        await message.answer("Причина слишком короткая. Введите ≥ 3 символов.")
        return
    data = await state.get_data()
    req_id = int(data.get("advance_req_id") or 0)
    if not req_id:
        await message.answer("❌ Сессия потеряна.")
        await state.clear()
        return
    req = await db.get_advance_request(req_id)
    if not req:
        await message.answer("❌ Запрос не найден.")
        await state.clear()
        return
    await db.reject_advance_request(req_id, rejected_by=message.from_user.id, reason=reason)
    await integrations.sync_advances_journal()
    await state.clear()
    await message.answer(f"❌ Запрос аванса #{req_id} отклонён.\nПричина: {reason}")
    await notifier.safe_send(
        int(req["installer_id"]),
        f"❌ <b>Аванс отклонён</b>\n"
        f"Запрос #{req_id}.\n"
        f"Причина: {reason}",
    )


@router.callback_query(F.data.startswith("gd_adv_pay:"))
async def gd_advance_pay_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД нажимает «Оплатить» — FSM загрузки чека/п/п."""
    if not cb.from_user:
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_advance_request(req_id)
    if not req:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return
    if req["status"] != "approved":
        await cb.message.answer(f"⚠️ Запрос в статусе {req['status']} — оплата невозможна.")  # type: ignore[union-attr]
        return
    await state.set_state(GdAdvancePaySG.receipt)
    await state.update_data(advance_req_id=req_id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 Оплата запроса #{req_id}.\n"
        "Пришлите чек/п/п (фото или документ), либо отправьте «—» чтобы оплатить без файла.",
    )


@router.message(GdAdvancePaySG.receipt)
async def gd_advance_pay_receipt_input(
    message: Message, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    req_id = int(data.get("advance_req_id") or 0)
    if not req_id:
        await message.answer("❌ Сессия потеряна.")
        await state.clear()
        return
    req = await db.get_advance_request(req_id)
    if not req:
        await message.answer("❌ Запрос не найден.")
        await state.clear()
        return
    file_id: str | None = None
    if (message.text or "").strip() == "—":
        file_id = None
    elif message.document:
        file_id = message.document.file_id
    elif message.photo:
        file_id = message.photo[-1].file_id
    else:
        await message.answer("Пришлите фото/документ или «—» для оплаты без файла.")
        return
    await db.pay_advance_request(
        req_id, paid_by=message.from_user.id, payment_file_id=file_id,
    )
    await state.clear()
    items = await db.get_advance_request_items(req_id)
    total = sum(float(i["amount"]) for i in items)
    # Sync invoices with new advance_paid columns + общий лист
    for it in items:
        try:
            await integrations.sync_invoice_row(int(it["invoice_id"]))
        except Exception as e:
            log.warning("sync_invoice_row failed after advance pay: %s", e)
    await integrations.sync_advances_journal()
    await message.answer(
        f"✅ Запрос аванса #{req_id} оплачен ({total:,.0f} ₽)."
    )
    # Notify монтажника
    msg = (
        f"💸 <b>Аванс выплачен</b>\n"
        f"Запрос #{req_id} — {total:,.0f} ₽.\n"
    )
    if file_id:
        msg += "📎 Чек прикреплён к этому сообщению."
    await notifier.safe_send(int(req["installer_id"]), msg)
    if file_id:
        try:
            if message.document:
                await notifier.safe_send_document(
                    int(req["installer_id"]), file_id,
                )
            else:
                await notifier.safe_send_photo(
                    int(req["installer_id"]), file_id,
                )
        except Exception as e:
            log.warning("Failed to forward advance receipt to installer: %s", e)


# =====================================================================
# TZ tingly-twirling-whistle 2026-05-25: ГД-инициированный депозит сотруднику.
# =====================================================================

DEPOSIT_MAX_AMOUNT = 500_000.0  # cap на одну транзакцию депозита, ₽

# Whitelist сотрудников для депозитов (имя → telegram_id).
# None = сотрудник в списке, но tg_id не настроен (кнопка disabled).
EMPLOYEE_DEPOSIT_WHITELIST: dict[str, int | None] = {
    "Павел": 6546325840,
    "Игорь": 1072734744,
    "Кирилл": 5641023011,
    "Илья": 495451226,
}


async def _gd_funds_type_screen(
    db: Database, installer_id: int, name: str, emp_role: str, is_dual: bool,
) -> tuple[str, Any]:
    """Экран «выбор типа пополнения» (Аванс/Депозит).

    Для двуролевого РП+Менеджер (Павел, is_dual) добавляет выбор кошелька
    (РП / Менеджер NPN) — кошелёк кодируется суффиксом callback_data
    'gd_funds:type:{advance|deposit}:{rp|manager_npn}'. Используется и в
    pick_installer, и в back-навигации — чтобы экраны не разъезжались.
    """
    b = InlineKeyboardBuilder()
    if is_dual:
        # Двухуровневое меню (user 02.06): уровень 1 — операция; кошелёк (РП /
        # Менеджер NPN) выбирается на уровне 2 в _gd_funds_wallet_screen.
        b.button(text="💰 Аванс", callback_data="gd_funds:op:advance")
        b.button(text="💸 Депозит", callback_data="gd_funds:op:deposit")
        b.button(text="📤 Запрос из депозита", callback_data="gd_funds:op:request")
    else:
        if emp_role in ("installer", "manager"):
            b.button(text="💰 Пополнить баланс аванса", callback_data="gd_funds:type:advance")
        b.button(text="💸 Пополнить баланс депозита", callback_data="gd_funds:type:deposit")
        b.button(text="📤 Запросить из депозита", callback_data="gd_funds:type:request")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_installer")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    if is_dual:
        rp_adv = await db.get_advance_balance(installer_id, "rp")
        rp_depo = await db.get_deposit_balance(installer_id, "rp")
        mgr_adv = await db.get_advance_balance(installer_id, "manager_npn")
        mgr_depo = await db.get_deposit_balance(installer_id, "manager_npn")
        def _f(v: float) -> str:
            return f"{v:,.0f}".replace(",", " ")
        text = (
            f"<pre>👤 <b>{html.escape(str(name))}</b> ━━━━━━ <b>РП + менеджер</b>\n"
            f"🟦 РП\n"
            f"   {'Аванс':<9}{_f(rp_adv):>9} ₽\n"
            f"   {'Депозит':<9}{_f(rp_depo):>9} ₽\n"
            f"🟨 Менеджер\n"
            f"   {'Аванс':<9}{_f(mgr_adv):>9} ₽\n"
            f"   {'Депозит':<9}{_f(mgr_depo):>9} ₽</pre>\n\n"
            f"Выберите операцию:"
        )
    else:
        deposit_bal = await db.get_deposit_balance(installer_id)
        advance_bal = await db.get_advance_balance(installer_id)
        role_hint = {"installer": "монтажник", "manager": "менеджер"}.get(emp_role, "сотрудник")
        def _f(v: float) -> str:
            return f"{v:,.0f}".replace(",", " ")
        type_hint = (
            "• Аванс — общий пул, сотрудник сам распределит по счетам.\n"
            "• Депозит — кошелёк, расход через withdraw.\n\n"
            if emp_role in ("installer", "manager")
            else "Для этой роли доступен только депозит (кошелёк).\n\n"
        )
        text = (
            f"<pre>👤 <b>{html.escape(str(name))}</b> ━━━━━━ <b>{role_hint}</b>\n"
            f"   {'Аванс':<9}{_f(advance_bal):>9} ₽\n"
            f"   {'Депозит':<9}{_f(deposit_bal):>9} ₽</pre>\n\n"
            f"{type_hint}"
            f"Выберите тип операции:"
        )
    return text, b.as_markup()


def _gd_funds_wallet_screen(op: str, name: str) -> tuple[str, Any]:
    """Уровень 2 dual-меню (user 02.06): после выбора операции — выбор кошелька.

    Кошелёк кодируется суффиксом существующего callback
    'gd_funds:type:{advance|deposit|request}:{rp|manager_npn}' — обработчики
    gd_funds_pick_{advance,deposit,request} ловят его без изменений.
    """
    op_label = {
        "advance": "💰 Аванс",
        "deposit": "💸 Депозит",
        "request": "📤 Запрос из депозита",
    }.get(op, op)
    b = InlineKeyboardBuilder()
    b.button(text="🟦 Кошелёк РП", callback_data=f"gd_funds:type:{op}:rp")
    b.button(text="🟨 Менеджер NPN", callback_data=f"gd_funds:type:{op}:manager_npn")
    b.button(text="⬅️ Назад", callback_data="gd_funds:op:back")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    text = (
        f"{op_label} — <b>{html.escape(str(name))}</b>\n\n"
        f"Выберите кошелёк:"
    )
    return text, b.as_markup()


@router.callback_query(F.data == "gd_deposit:start")
async def gd_deposit_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД нажимает «💸 Финансы» — старт FSM выбора сотрудника, потом ветвление Аванс/Депозит.

    Whitelist = EMPLOYEE_DEPOSIT_WHITELIST (Павел/Игорь/Кирилл/Илья).
    Кнопки сотрудников с None tg_id показаны как «🚧 не настроен» (disabled).
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    await cb.answer()
    b = InlineKeyboardBuilder()
    # Сводная эталонная карточка по всем сотрудникам (общий обзор балансов) — user 02.06.
    # Балансы тянем без wallet_role → итог по всем кошелькам (Павел-dual = оба).
    def _m(v: float) -> str:
        return f"{v:,.0f} ₽".replace(",", " ")
    card_rows: list[str] = [f"{'':<8}{'Аванс':>9}{'Депозит':>10}"]
    for name, tid in EMPLOYEE_DEPOSIT_WHITELIST.items():
        if tid is None:
            # Заглушка — кнопка не активна, callback показывает alert.
            card_rows.append(f"{name:<8}{'—':>9}{'—':>10}")
            b.button(text=f"🚧 {name} (не настроен)", callback_data=f"gd_deposit:nottid:{name}")
            continue
        adv = await db.get_advance_balance(tid)
        depo = await db.get_deposit_balance(tid)
        card_rows.append(f"{name:<8}{_m(adv):>9}{_m(depo):>10}")
        label = f"👤 {name}"
        if depo > 0:
            label += f" · {depo:,.0f} ₽".replace(",", " ")
        b.button(text=label, callback_data=f"gd_deposit:pick:{tid}")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    summary_card = (
        "<pre>👥 <b>Сотрудники</b> ━━━━━━ <b>Финансы</b>\n"
        + "\n".join(card_rows)
        + "</pre>\n\n"
    )
    await state.set_state(GdDepositSG.select_installer)
    await cb.message.answer(  # type: ignore[union-attr]
        summary_card + "💸 <b>Депозит сотруднику</b>\n\nВыберите получателя:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("gd_deposit:nottid:"))
async def gd_deposit_nottid(cb: CallbackQuery) -> None:
    """Click on disabled employee — show alert."""
    name = cb.data.split(":")[-1] if cb.data else "?"  # type: ignore[union-attr]
    await cb.answer(
        f"⚠️ Telegram-ID сотрудника «{name}» не настроен. "
        "Запросите ID у Сергея и добавьте в EMPLOYEE_DEPOSIT_WHITELIST.",
        show_alert=True,
    )


@router.callback_query(GdDepositSG.select_installer, F.data.startswith("gd_deposit:pick:"))
async def gd_deposit_pick_installer(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """После выбора сотрудника — развилка Аванс/Депозит (TZ synthetic-hopping-ocean)."""
    if not cb.from_user:
        return
    await cb.answer()
    installer_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    valid_ids = {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid is not None}
    if installer_id not in valid_ids:
        await cb.message.answer("❌ Сотрудник вне whitelist.")  # type: ignore[union-attr]
        await state.clear()
        return
    name = next(
        (n for n, t in EMPLOYEE_DEPOSIT_WHITELIST.items() if t == installer_id),
        str(installer_id),
    )
    # Определяем роль сотрудника для branch advance.
    user = await db.get_user_optional(installer_id)
    role_raw = (user.role if user else "") or ""
    if "installer" in role_raw:
        emp_role = "installer"
    elif "manager" in role_raw:
        emp_role = "manager"
    else:
        emp_role = "other"
    roles_list = [r.strip().lower() for r in role_raw.split(",")]
    is_dual = "rp" in roles_list  # двуролевой РП+Менеджер (Павел): выбор кошелька
    await state.update_data(
        deposit_installer_id=installer_id,
        deposit_employee_name=name,
        deposit_employee_role=emp_role,
        deposit_employee_is_dual=is_dual,
    )
    await state.set_state(GdDepositSG.select_type)
    text, kb = await _gd_funds_type_screen(db, installer_id, name, emp_role, is_dual)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(GdDepositSG.select_type, F.data.startswith("gd_funds:op:"))
async def gd_funds_pick_op(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Уровень 1 dual-меню (user 02.06): выбор операции → экран выбора кошелька.

    op == 'back' возвращает на уровень 1 (экран операций). Остаёмся в состоянии
    select_type — обработчики gd_funds:type:* ловят выбор кошелька (уровень 2).
    """
    await cb.answer()
    op = (cb.data or "").split(":")[-1]
    data = await state.get_data()
    installer_id = int(data.get("deposit_installer_id") or 0)
    name = data.get("deposit_employee_name") or "сотрудник"
    emp_role = data.get("deposit_employee_role") or "other"
    if op == "back":
        text, kb = await _gd_funds_type_screen(db, installer_id, name, emp_role, True)
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
        return
    if op not in ("advance", "deposit", "request"):
        return
    text, kb = _gd_funds_wallet_screen(op, name)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(GdDepositSG.select_type, F.data.startswith("gd_funds:type:deposit"))
async def gd_funds_pick_deposit(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    """Развилка → Депозит. Переход к вводу суммы депозита."""
    await cb.answer()
    parts = (cb.data or "").split(":")  # gd_funds:type:deposit[:wallet]
    wallet_role = parts[3] if len(parts) > 3 else None
    data = await state.get_data()
    name = data.get("deposit_employee_name") or "сотрудник"
    await state.update_data(deposit_wallet_role=wallet_role)
    await state.set_state(GdDepositSG.enter_amount)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 <b>Депозит — {html.escape(str(name))}</b>\n\n"
        f"Введите сумму депозита (₽, целое число, ≤ {DEPOSIT_MAX_AMOUNT:,.0f}):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.select_type, F.data.startswith("gd_funds:type:advance"))
async def gd_funds_pick_advance(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """funds-2balances 25.05: ГД пополняет advance-баланс сотрудника. БЕЗ выбора счёта.

    Сотрудник сам потом распределяет аванс под конкретный счёт через
    «💰 Распределить аванс» в своём меню.
    """
    await cb.answer()
    parts = (cb.data or "").split(":")  # gd_funds:type:advance[:wallet]
    wallet_role = parts[3] if len(parts) > 3 else None
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    name = data.get("deposit_employee_name") or str(employee_id)
    emp_role = data.get("deposit_employee_role") or "other"
    await state.update_data(deposit_wallet_role=wallet_role)
    if emp_role not in ("installer", "manager"):
        await cb.message.answer(  # type: ignore[union-attr]
            "❌ Пополнение аванса доступно только для монтажника или менеджера.",
        )
        await state.clear()
        return
    await state.set_state(GdDepositSG.adv_enter_amount)
    role_hint = "монтаж" if emp_role == "installer" else "продажа"
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Пополнить аванс ({role_hint}) — {html.escape(str(name))}</b>\n\n"
        f"Введите сумму аванса (₽, целое, ≤ {DEPOSIT_MAX_AMOUNT:,.0f}):\n"
        f"<i>Сотрудник сам распределит аванс по своим счетам.</i>",
        reply_markup=b.as_markup(),
    )


# ============================================================================
# ТЗ C (30.05): ГД запрашивает сумму ИЗ депозита сотрудника → входящая задача.
# Депозит уменьшается ТОЛЬКО после подтверждения сотрудником (вариант B).
# ============================================================================


@router.callback_query(GdDepositSG.select_type, F.data.startswith("gd_funds:type:request"))
async def gd_funds_pick_request(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Развилка → Запрос из депозита. Проверяем остаток, переходим к вводу суммы."""
    await cb.answer()
    parts = (cb.data or "").split(":")  # gd_funds:type:request[:wallet]
    wallet_role = parts[3] if len(parts) > 3 else None
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    name = data.get("deposit_employee_name") or str(employee_id)
    if not employee_id:
        await cb.message.answer("❌ Сессия потеряна. Начните заново.")  # type: ignore[union-attr]
        await state.clear()
        return
    depo = await db.get_deposit_balance(employee_id, wallet_role)
    if depo <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ У сотрудника «{name}» нет средств на депозите. "
            "Сначала пополните депозит.",
        )
        return
    await state.update_data(deposit_wallet_role=wallet_role, req_depo_balance=depo)
    await state.set_state(GdDepositSG.req_enter_amount)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📤 <b>Запрос из депозита — {html.escape(str(name))}</b>\n\n"
        f"Доступно на депозите: <b>{depo:,.0f} ₽</b>\n\n"
        f"Введите сумму запроса (₽, целое, ≤ {depo:,.0f}):",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.req_enter_amount, F.text)
async def gd_request_amount_input(
    message: Message, state: FSMContext,
) -> None:
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 5000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    data = await state.get_data()
    depo = float(data.get("req_depo_balance") or 0)
    if amount > depo + 0.001:
        await message.answer(
            f"❌ Сумма больше остатка депозита ({depo:,.0f} ₽). Введите меньше.",
        )
        return
    await state.update_data(req_amount=amount)
    await state.set_state(GdDepositSG.req_enter_purpose)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:req_enter_amount")
    b.adjust(1)
    await message.answer(
        f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
        "Укажите <b>назначение</b> (на что запрашивается; обязательно):",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.req_enter_purpose, F.text)
async def gd_request_purpose_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    purpose = (message.text or "").strip()
    if not purpose or purpose == "—":
        await message.answer("❌ Назначение обязательно. Напишите, на что запрашивается сумма.")
        return
    purpose = purpose[:500]
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("req_amount") or 0)
    if not employee_id or amount <= 0:
        await message.answer("❌ Сессия потеряна. Начните заново.")
        await state.clear()
        return
    await state.update_data(req_purpose=purpose)
    await state.set_state(GdDepositSG.req_attach)
    b = InlineKeyboardBuilder()
    b.button(text="⏭️ Пропустить (без файла)", callback_data="gd_depo_req:skip_file")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:req_enter_purpose")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    await message.answer(
        f"Назначение: {html.escape(purpose)}\n\n"
        "📎 По желанию приложите файл для сотрудника (фото/документ) — "
        "например, реквизиты или счёт. Или нажмите «Пропустить»:",
        reply_markup=b.as_markup(),
    )


async def _gd_req_show_confirm(target_msg: Message, state: FSMContext, db: Database) -> None:
    """Экран подтверждения запроса из депозита (GdDepositSG.req_confirm).

    Вызывается из шага req_attach (после файла или «Пропустить»). Показывает
    сумму/назначение + пометку, приложен ли файл от ГД.
    """
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("req_amount") or 0)
    purpose = str(data.get("req_purpose") or "").strip()
    name = data.get("deposit_employee_name") or str(employee_id)
    has_file = bool(data.get("req_gd_file_id"))
    depo = await db.get_deposit_balance(employee_id, data.get("deposit_wallet_role"))
    await state.set_state(GdDepositSG.req_confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить запрос", callback_data="gd_depo_req:confirm")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:req_enter_purpose")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    card = format_card_section(
        "📤", "Подтвердите запрос из депозита",
        items=[
            ("Сотрудник", html.escape(str(name))),
            ("Сумма", _depo_sum(amount)),
            ("Назначение", html.escape(purpose)),
            ("Депозит сейчас", _depo_sum(depo)),
        ],
    )
    file_line = "\n📎 <i>Файл приложен</i>" if has_file else ""
    await target_msg.answer(
        f"{card}{file_line}\n"
        f"<i>Сотруднику придёт задача на подтверждение. "
        f"Депозит уменьшится только после его согласия.</i>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.req_attach, F.data == "gd_depo_req:skip_file")
async def gd_request_skip_file(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """req_attach: «Пропустить» — без файла → экран подтверждения."""
    await cb.answer()
    await state.update_data(req_gd_file_id=None, req_gd_file_type=None)
    await _gd_req_show_confirm(cb.message, state, db)  # type: ignore[arg-type]


@router.message(GdDepositSG.req_attach)
async def gd_request_attach_file(
    message: Message, state: FSMContext, db: Database,
) -> None:
    """req_attach: ГД прислал фото/документ → сохранить в state → подтверждение."""
    file_id: str | None = None
    file_type: str | None = None
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    else:
        await message.answer(
            "Пришлите фото/документ, либо нажмите «Пропустить» под предыдущим сообщением.",
        )
        return
    await state.update_data(req_gd_file_id=file_id, req_gd_file_type=file_type)
    await _gd_req_show_confirm(message, state, db)


@router.callback_query(GdDepositSG.req_confirm, F.data == "gd_depo_req:confirm")
@money_confirm_guard
async def gd_request_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
) -> None:
    """ГД отправляет запрос → создаётся задача GD_DEPOSIT_REQUEST сотруднику.

    Депозит НЕ трогается здесь — только после подтверждения сотрудником.
    """
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("req_amount") or 0)
    purpose = str(data.get("req_purpose") or "").strip()
    wallet_role = data.get("deposit_wallet_role")
    name = data.get("deposit_employee_name") or str(employee_id)
    gd_file_id = data.get("req_gd_file_id")
    gd_file_type = data.get("req_gd_file_type")
    if not employee_id or amount <= 0 or not purpose:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    # UX-гард: финальная проверка остатка (реальное списание — при подтверждении сотрудником).
    depo = await db.get_deposit_balance(employee_id, wallet_role)
    if amount > depo + 0.001:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Сумма {amount:,.0f} ₽ больше остатка депозита ({depo:,.0f} ₽).",
        )
        await state.clear()
        return
    try:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.GD_DEPOSIT_REQUEST,
            status=TaskStatus.OPEN,
            created_by=cb.from_user.id,
            assigned_to=employee_id,
            due_at_iso=None,
            payload={
                "kind": "gd_deposit_request",
                "amount": amount,
                "purpose": purpose,
                "wallet_role": wallet_role,
                "employee_id": employee_id,
                "employee_name": name,
                "gd_id": cb.from_user.id,
                "gd_file_id": gd_file_id,
                "gd_file_type": gd_file_type,
            },
        )
    except ValueError as e:
        await cb.message.answer(f"❌ Не удалось создать запрос: {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    task_id = int(task["id"])
    await state.clear()
    _sent_card = format_card_section(
        "📤", f"Запрос #{task_id} отправлен",
        items=[
            ("Сотрудник", html.escape(str(name))),
            ("Сумма", _depo_sum(amount)),
            ("Назначение", html.escape(purpose)),
        ],
    )
    await cb.message.answer(  # type: ignore[union-attr]
        f"{_sent_card}\n"
        f"<i>Депозит уменьшится после подтверждения сотрудником.</i>",
    )
    # Уведомление сотруднику: ШАГ 1 — подтвердить прочтение (двухшаговый flow 04.06).
    # Списание произойдёт только на шаге 2 (подтверждение исполнения + опц. вложение).
    bk = InlineKeyboardBuilder()
    bk.button(text="✅ Подтвердить прочтение", callback_data=f"inst_depo_req:read:{task_id}")
    bk.button(text="❌ Отклонить", callback_data=f"inst_depo_req:reject:{task_id}")
    bk.adjust(1)
    _emp_card = format_card_section(
        "📥", "Запрос ГД из вашего депозита",
        items=[
            ("Сумма", _depo_sum(amount)),
            ("Назначение", html.escape(purpose)),
        ],
    )
    await notifier.safe_send(
        employee_id,
        f"{_emp_card}\n"
        f"<i>Шаг 1/2 — подтвердите прочтение. Списание произойдёт только после "
        f"подтверждения исполнения.</i>\n"
        f"<i>Также доступно: «💰 Запрос ЗП» → «💳 Депозит» 🔴</i>",
        reply_markup=bk.as_markup(),
    )
    # Опц. файл от ГД (если приложен при создании запроса) — пересылаем сотруднику.
    if gd_file_id and gd_file_type:
        try:
            await notifier.safe_send_media(
                employee_id, gd_file_type, gd_file_id,
                caption=f"📎 Файл от ГД к запросу #{task_id}",
            )
        except Exception as e:
            log.warning("forward GD depo-request file to employee failed: %s", e)


@router.callback_query(F.data.startswith("gd_depo_req_emp:confirm:"))
async def gd_depo_req_emp_confirm(
    cb: CallbackQuery, db: Database, notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Сотрудник подтверждает запрос ГД → списание с депозита + done + notify ГД.

    Анти-двойное-списание: сначала атомарно «забираем» задачу
    (update_task_status DONE, expected OPEN); только победивший клик создаёт withdraw.
    При нехватке средств — откат задачи в OPEN.
    """
    if not cb.from_user or not cb.data:
        return
    try:
        task_id = int(cb.data.split(":")[-1])
    except ValueError:
        await cb.answer("Некорректный запрос.", show_alert=True)
        return
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена.", show_alert=True)
        return
    if int(task.get("assigned_to") or 0) != cb.from_user.id:
        await cb.answer("Эта задача не для вас.", show_alert=True)
        return
    if str(task.get("status")) != TaskStatus.OPEN:
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    payload = try_json_loads(task.get("payload_json"))
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    wallet_role = payload.get("wallet_role")
    gd_id = int(payload.get("gd_id") or task.get("created_by") or 0)
    name = payload.get("employee_name") or str(cb.from_user.id)
    if amount <= 0:
        await cb.answer("Некорректная сумма запроса.", show_alert=True)
        return
    # Атомарно забираем задачу — единственный победитель пойдёт списывать.
    claimed = await db.update_task_status(
        task_id, TaskStatus.DONE, expected_statuses=(TaskStatus.OPEN,),
    )
    if claimed is None:
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    try:
        await db.create_gd_deposit_withdrawal(
            employee_id=cb.from_user.id,
            amount=amount,
            comment=f"Запрос ГД: {purpose}",
            gd_id=gd_id,
            wallet_role=wallet_role,
        )
    except ValueError as e:
        # Откат: возвращаем задачу в OPEN, депозит не тронут.
        await db.update_task_status(
            task_id, TaskStatus.OPEN, expected_statuses=(TaskStatus.DONE,),
        )
        await cb.answer(f"⚠️ {e}", show_alert=True)
        if gd_id:
            await notifier.safe_send(
                gd_id,
                f"⚠️ <b>Запрос #{task_id} не выполнен</b>\n"
                f"{html.escape(str(name))} подтвердил, но списание не прошло: {html.escape(str(e))}",
            )
        return
    new_depo = await db.get_deposit_balance(cb.from_user.id, wallet_role)
    await cb.answer("✅ Подтверждено.")
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ <b>Подтверждено</b>\n\n"
            f"Списано с депозита: <b>{amount:,.0f} ₽</b>\n"
            f"Назначение: {html.escape(purpose)}\n"
            f"Остаток депозита: <b>{new_depo:,.0f} ₽</b>",
        )
    except Exception:
        pass
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after gd_depo_req confirm: %s", e)
    if gd_id:
        await notifier.safe_send(
            gd_id,
            f"✅ <b>Запрос #{task_id} подтверждён</b>\n"
            f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
            f"Списано с депозита: <b>{amount:,.0f} ₽</b>\n"
            f"Назначение: {html.escape(purpose)}\n"
            f"Остаток депозита: <b>{new_depo:,.0f} ₽</b>",
        )


@router.callback_query(F.data.startswith("gd_depo_req_emp:reject:"))
async def gd_depo_req_emp_reject(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """Сотрудник отклоняет запрос ГД → задача rejected, депозит не тронут, notify ГД."""
    if not cb.from_user or not cb.data:
        return
    try:
        task_id = int(cb.data.split(":")[-1])
    except ValueError:
        await cb.answer("Некорректный запрос.", show_alert=True)
        return
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена.", show_alert=True)
        return
    if int(task.get("assigned_to") or 0) != cb.from_user.id:
        await cb.answer("Эта задача не для вас.", show_alert=True)
        return
    if str(task.get("status")) != TaskStatus.OPEN:
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    claimed = await db.update_task_status(
        task_id, TaskStatus.REJECTED, expected_statuses=(TaskStatus.OPEN,),
    )
    if claimed is None:
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    payload = try_json_loads(task.get("payload_json"))
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    gd_id = int(payload.get("gd_id") or task.get("created_by") or 0)
    name = payload.get("employee_name") or str(cb.from_user.id)
    await cb.answer("Отклонено.")
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"❌ <b>Запрос отклонён</b>\n\n"
            f"Сумма: {amount:,.0f} ₽\n"
            f"Назначение: {html.escape(purpose)}\n"
            f"<i>Депозит не тронут.</i>",
        )
    except Exception:
        pass
    if gd_id:
        await notifier.safe_send(
            gd_id,
            f"❌ <b>Запрос #{task_id} отклонён сотрудником</b>\n"
            f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
            f"Сумма: <b>{amount:,.0f} ₽</b> · {html.escape(purpose)}\n"
            f"<i>Депозит не тронут.</i>",
        )


@router.callback_query(GdDepositSG.adv_select_invoice, F.data.startswith("gd_adv:inv:"))
async def gd_advance_pick_invoice(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    data = await state.get_data()
    installer_id = int(data.get("deposit_installer_id") or 0)
    emp_role = data.get("deposit_employee_role") or "installer"
    ownership_field = "created_by" if emp_role == "manager" else "assigned_to"
    if inv.get(ownership_field) != installer_id:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Счёт не принадлежит выбранному сотруднику ({ownership_field}).",
        )
        await state.clear()
        return
    num = inv.get("invoice_number") or f"id={invoice_id}"
    addr = inv.get("object_address") or "—"
    await state.update_data(
        adv_invoice_id=invoice_id,
        adv_invoice_label=f"№{num} {addr}",
    )
    await state.set_state(GdDepositSG.adv_enter_amount)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 Счёт: <b>№{num}</b>\n"
        f"📍 {html.escape(str(addr))}\n\n"
        f"Введите сумму аванса (₽, целое, ≤ {DEPOSIT_MAX_AMOUNT:,.0f}):",
    )


@router.message(GdDepositSG.adv_enter_amount, F.text)
async def gd_advance_amount_input(
    message: Message, state: FSMContext,
) -> None:
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 50000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    if amount > DEPOSIT_MAX_AMOUNT:
        await message.answer(
            f"❌ Сумма превышает лимит {DEPOSIT_MAX_AMOUNT:,.0f} ₽. Введите меньше.",
        )
        return
    await state.update_data(adv_amount=amount)
    await state.set_state(GdDepositSG.adv_attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без чека", callback_data="gd_adv:skip_receipt")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_enter_amount")
    b.adjust(1)
    await message.answer(
        f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
        "📎 Пришлите чек/п/п <b>(фото или документ)</b> — или «⏭ Без чека».\n"
        "<i>Для ГД чек не обязателен.</i>",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.adv_attach_receipt)
async def gd_advance_receipt_input(
    message: Message, state: FSMContext,
) -> None:
    file_id: str | None = None
    if message.document:
        file_id = message.document.file_id
    elif message.photo:
        file_id = message.photo[-1].file_id
    else:
        await message.answer("❌ Пришлите фото или документ (PDF/JPG), либо нажмите «⏭ Без чека».")
        return
    await state.update_data(adv_receipt_file_id=file_id)
    await state.set_state(GdDepositSG.adv_enter_comment)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_attach_receipt")
    b.adjust(1)
    await message.answer(
        "📎 Чек принят.\n\n"
        "Введите комментарий (например: «аванс на материалы») или «—» если без комментария:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.adv_attach_receipt, F.data == "gd_adv:skip_receipt")
async def gd_advance_skip_receipt(cb: CallbackQuery, state: FSMContext) -> None:
    """ГД пропускает чек при пополнении аванса (чек опционален для ГД, ТЗ 30.05)."""
    await cb.answer()
    await state.update_data(adv_receipt_file_id="")
    await state.set_state(GdDepositSG.adv_enter_comment)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_attach_receipt")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "Без чека.\n\n"
        "Введите комментарий (например: «аванс на материалы») или «—» если без комментария:",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.adv_enter_comment, F.text)
async def gd_advance_comment_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    comment_raw = (message.text or "").strip()
    comment: str | None = None if comment_raw == "—" else comment_raw[:500]
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("adv_amount") or 0)
    file_id = data.get("adv_receipt_file_id") or ""
    if not employee_id or amount <= 0:
        await message.answer("❌ Сессия потеряна. Начните заново.")
        await state.clear()
        return
    await state.update_data(adv_comment=comment)
    await state.set_state(GdDepositSG.adv_confirm)
    name = data.get("deposit_employee_name") or str(employee_id)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="gd_adv:confirm")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_enter_comment")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    await message.answer(
        f"💰 <b>Подтвердите пополнение аванса</b>\n\n"
        f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
        f"Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"Комментарий: {html.escape(comment or '—')}\n"
        f"Чек: {'📎 прикреплён' if file_id else '— (без чека)'}\n\n"
        f"<i>Сотрудник сам распределит аванс по счетам через своё меню.</i>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.adv_confirm, F.data == "gd_adv:confirm")
@money_confirm_guard
async def gd_advance_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """funds-2balances 25.05: ГД пополняет advance-баланс. Без invoice_id (сотрудник сам распределит)."""
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    employee_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("adv_amount") or 0)
    file_id = str(data.get("adv_receipt_file_id") or "")
    comment = data.get("adv_comment")
    if not employee_id or amount <= 0:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id = await db.create_gd_advance_topup(
            employee_id=employee_id,
            amount=amount,
            gd_id=cb.from_user.id,
            payment_file_id=file_id,
            comment=comment,
            wallet_role=data.get("deposit_wallet_role"),
        )
    except ValueError as e:
        await cb.message.answer(f"❌ Ошибка: {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    name = data.get("deposit_employee_name") or str(employee_id)
    await state.clear()
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after gd_advance_topup: %s", e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Аванс #{req_id} пополнен.\n"
        f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
        f"Сумма: <b>{amount:,.0f} ₽</b>",
    )
    # Notify сотрудника: текст + пересылка чека.
    notify_text = (
        f"💰 <b>ГД пополнил ваш advance-баланс</b>\n"
        f"Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"Комментарий: {html.escape(comment or '—')}\n\n"
        f"<i>Распределите аванс по счетам через меню «💰 Распределить аванс».</i>"
    )
    await notifier.safe_send(employee_id, notify_text)
    if file_id:
        try:
            await notifier.safe_send_photo(employee_id, file_id)
        except Exception:
            try:
                await notifier.safe_send_document(employee_id, file_id)
            except Exception as e:
                log.warning("Failed to forward advance receipt to employee: %s", e)


@router.message(GdDepositSG.enter_amount, F.text)
async def gd_deposit_amount_input(
    message: Message, state: FSMContext,
) -> None:
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 50000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    if amount > DEPOSIT_MAX_AMOUNT:
        await message.answer(
            f"❌ Сумма превышает лимит {DEPOSIT_MAX_AMOUNT:,.0f} ₽. Введите меньше.",
        )
        return
    await state.update_data(deposit_amount=amount)
    await state.set_state(GdDepositSG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без чека", callback_data="gd_deposit:skip_receipt")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:enter_amount")
    b.adjust(1)
    await message.answer(
        f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
        "📎 Пришлите чек/п/п <b>(фото или документ)</b> — или «⏭ Без чека».\n"
        "<i>Для ГД чек не обязателен.</i>",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.attach_receipt)
async def gd_deposit_receipt_input(
    message: Message, state: FSMContext,
) -> None:
    file_id: str | None = None
    if message.document:
        file_id = message.document.file_id
    elif message.photo:
        file_id = message.photo[-1].file_id
    else:
        await message.answer(
            "❌ Пришлите фото или документ (PDF/JPG), либо нажмите «⏭ Без чека».",
        )
        return
    await state.update_data(deposit_receipt_file_id=file_id)
    await state.set_state(GdDepositSG.enter_comment)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:attach_receipt")
    b.adjust(1)
    await message.answer(
        "📎 Чек принят.\n\n"
        "Введите комментарий (например: «премия за КВ 4», «аванс на лето») или «—» если без комментария:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.attach_receipt, F.data == "gd_deposit:skip_receipt")
async def gd_deposit_skip_receipt(cb: CallbackQuery, state: FSMContext) -> None:
    """ГД пропускает чек при внесении депозита (чек опционален для ГД, ТЗ 30.05)."""
    await cb.answer()
    await state.update_data(deposit_receipt_file_id="")
    await state.set_state(GdDepositSG.enter_comment)
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:attach_receipt")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "Без чека.\n\n"
        "Введите комментарий (например: «премия за КВ 4», «аванс на лето») или «—» если без комментария:",
        reply_markup=b.as_markup(),
    )


@router.message(GdDepositSG.enter_comment, F.text)
async def gd_deposit_comment_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    comment_raw = (message.text or "").strip()
    comment: str | None = None if comment_raw == "—" else comment_raw[:500]
    data = await state.get_data()
    installer_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("deposit_amount") or 0)
    file_id = data.get("deposit_receipt_file_id") or ""
    if not installer_id or amount <= 0:
        await message.answer("❌ Сессия потеряна. Начните заново.")
        await state.clear()
        return
    await state.update_data(deposit_comment=comment)
    await state.set_state(GdDepositSG.confirm)
    balance_before = await db.get_deposit_balance(installer_id, data.get("deposit_wallet_role"))
    balance_after = balance_before + amount
    name = data.get("deposit_employee_name") or str(installer_id)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="gd_deposit:confirm")
    b.button(text="⬅️ Назад", callback_data="gd_deposit:back:enter_comment")
    b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
    b.adjust(1)
    await message.answer(
        f"💸 <b>Подтвердите депозит</b>\n\n"
        f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
        f"Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"Комментарий: {html.escape(comment or '—')}\n"
        f"Чек: {'📎 прикреплён' if file_id else '— (без чека)'}\n\n"
        f"Баланс депозита: {balance_before:,.0f} ₽ → <b>{balance_after:,.0f} ₽</b>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(GdDepositSG.confirm, F.data == "gd_deposit:confirm")
@money_confirm_guard
async def gd_deposit_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    installer_id = int(data.get("deposit_installer_id") or 0)
    amount = float(data.get("deposit_amount") or 0)
    file_id = str(data.get("deposit_receipt_file_id") or "")
    comment = data.get("deposit_comment")
    if not installer_id or amount <= 0:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id, offset_invoice_ids = await db.create_gd_deposit(
            installer_id=installer_id,
            amount=amount,
            gd_id=cb.from_user.id,
            payment_file_id=file_id,
            comment=comment,
            wallet_role=data.get("deposit_wallet_role"),
        )
    except ValueError as e:
        await cb.message.answer(f"❌ Ошибка: {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    name = data.get("deposit_employee_name") or str(installer_id)
    new_balance = await db.get_deposit_balance(installer_id, data.get("deposit_wallet_role"))
    await state.clear()
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after gd_deposit: %s", e)
    # Пересобрать строки счетов, задетых авто-зачётом депозита (CH/метрики/статус ЗП);
    # без этого б/н счета отстают в листе (кредитные подтягивают кредит-хуки).
    for _inv_id in offset_invoice_ids:
        try:
            await integrations.sync_invoice_row(int(_inv_id))
        except Exception as e:
            log.warning("sync_invoice_row after gd_deposit auto-offset failed inv=%s: %s", _inv_id, e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Депозит #{req_id} зачислен.\n"
        f"Сотрудник: <b>{html.escape(str(name))}</b>\n"
        f"Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"Баланс: <b>{new_balance:,.0f} ₽</b>",
    )
    # Notify получателю: header + эталон-карточка истории депозита (спек 04.06) + чек.
    header = (
        f"💳 <b>ГД пополнил ваш депозит на {amount:,.0f} ₽</b>"
        + (f"\nКомментарий: {html.escape(comment)}" if comment else "")
    )
    await notifier.safe_send(installer_id, header)
    try:
        card = await build_deposit_history_card(
            db, installer_id, data.get("deposit_wallet_role"),
        )
        await notifier.safe_send(installer_id, card)
    except Exception as e:
        log.warning("deposit topup card render failed: %s", e)
    if file_id:
        try:
            # Не знаем точно doc/photo — попробуем оба, любая ошибка просто залогируется.
            await notifier.safe_send_photo(installer_id, file_id)
        except Exception:
            try:
                await notifier.safe_send_document(installer_id, file_id)
            except Exception as e:
                log.warning("Failed to forward deposit receipt to installer: %s", e)


@router.callback_query(F.data.startswith("gd_deposit:back:"))
async def gd_deposit_back(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Универсальный «⬅️ Назад» по FSM GdDepositSG. Target в callback_data."""
    if not cb.data:
        return
    await cb.answer()
    target = cb.data.split(":")[-1]
    data = await state.get_data()
    installer_id = int(data.get("deposit_installer_id") or 0)
    name = data.get("deposit_employee_name") or (str(installer_id) if installer_id else "сотрудник")
    emp_role = data.get("deposit_employee_role") or "other"

    if target == "select_installer":
        b = InlineKeyboardBuilder()
        for nm, tid in EMPLOYEE_DEPOSIT_WHITELIST.items():
            if tid is None:
                b.button(text=f"🚧 {nm} (не настроен)", callback_data=f"gd_deposit:nottid:{nm}")
            else:
                bal = await db.get_deposit_balance(tid)
                lbl = f"👤 {nm}"
                if bal > 0:
                    lbl += f" · {bal:,.0f} ₽".replace(",", " ")
                b.button(text=lbl, callback_data=f"gd_deposit:pick:{tid}")
        b.button(text="❌ Отмена", callback_data="gd_deposit:cancel")
        b.adjust(1)
        await state.set_state(GdDepositSG.select_installer)
        await cb.message.answer(  # type: ignore[union-attr]
            "💸 <b>Депозит сотруднику</b>\n\nВыберите получателя:",
            reply_markup=b.as_markup(),
        )
        return

    if target == "select_type":
        is_dual = bool(data.get("deposit_employee_is_dual"))
        await state.set_state(GdDepositSG.select_type)
        text, kb = await _gd_funds_type_screen(db, installer_id, name, emp_role, is_dual)
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
        return

    if target == "enter_amount":
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
        b.adjust(1)
        await state.set_state(GdDepositSG.enter_amount)
        await cb.message.answer(  # type: ignore[union-attr]
            f"💸 <b>Депозит — {html.escape(str(name))}</b>\n\n"
            f"Введите сумму депозита (₽, целое число, ≤ {DEPOSIT_MAX_AMOUNT:,.0f}):",
            reply_markup=b.as_markup(),
        )
        return

    if target == "adv_enter_amount":
        role_hint = "монтаж" if emp_role == "installer" else "продажа"
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
        b.adjust(1)
        await state.set_state(GdDepositSG.adv_enter_amount)
        await cb.message.answer(  # type: ignore[union-attr]
            f"💰 <b>Пополнить аванс ({role_hint}) — {html.escape(str(name))}</b>\n\n"
            f"Введите сумму аванса (₽, целое, ≤ {DEPOSIT_MAX_AMOUNT:,.0f}):\n"
            f"<i>Сотрудник сам распределит аванс по своим счетам.</i>",
            reply_markup=b.as_markup(),
        )
        return

    if target == "attach_receipt":
        amount = float(data.get("deposit_amount") or 0)
        b = InlineKeyboardBuilder()
        b.button(text="⏭ Без чека", callback_data="gd_deposit:skip_receipt")
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:enter_amount")
        b.adjust(1)
        await state.set_state(GdDepositSG.attach_receipt)
        await cb.message.answer(  # type: ignore[union-attr]
            f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
            "📎 Пришлите чек/п/п <b>(фото или документ)</b> — или «⏭ Без чека».\n"
            "<i>Для ГД чек не обязателен.</i>",
            reply_markup=b.as_markup(),
        )
        return

    if target == "adv_attach_receipt":
        amount = float(data.get("adv_amount") or 0)
        b = InlineKeyboardBuilder()
        b.button(text="⏭ Без чека", callback_data="gd_adv:skip_receipt")
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_enter_amount")
        b.adjust(1)
        await state.set_state(GdDepositSG.adv_attach_receipt)
        await cb.message.answer(  # type: ignore[union-attr]
            f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
            "📎 Пришлите чек/п/п <b>(фото или документ)</b> — или «⏭ Без чека».\n"
            "<i>Для ГД чек не обязателен.</i>",
            reply_markup=b.as_markup(),
        )
        return

    if target == "enter_comment":
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:attach_receipt")
        b.adjust(1)
        await state.set_state(GdDepositSG.enter_comment)
        await cb.message.answer(  # type: ignore[union-attr]
            "📎 Чек принят.\n\n"
            "Введите комментарий (например: «премия за КВ 4», «аванс на лето») или «—» если без комментария:",
            reply_markup=b.as_markup(),
        )
        return

    if target == "adv_enter_comment":
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:adv_attach_receipt")
        b.adjust(1)
        await state.set_state(GdDepositSG.adv_enter_comment)
        await cb.message.answer(  # type: ignore[union-attr]
            "📎 Чек принят.\n\n"
            "Введите комментарий (например: «аванс на материалы») или «—» если без комментария:",
            reply_markup=b.as_markup(),
        )
        return

    if target == "req_enter_amount":
        depo = float(data.get("req_depo_balance") or 0)
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:select_type")
        b.adjust(1)
        await state.set_state(GdDepositSG.req_enter_amount)
        await cb.message.answer(  # type: ignore[union-attr]
            f"📤 <b>Запрос из депозита — {html.escape(str(name))}</b>\n\n"
            f"Доступно на депозите: <b>{depo:,.0f} ₽</b>\n\n"
            f"Введите сумму запроса (₽, целое, ≤ {depo:,.0f}):",
            reply_markup=b.as_markup(),
        )
        return

    if target == "req_enter_purpose":
        amount = float(data.get("req_amount") or 0)
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="gd_deposit:back:req_enter_amount")
        b.adjust(1)
        await state.set_state(GdDepositSG.req_enter_purpose)
        await cb.message.answer(  # type: ignore[union-attr]
            f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
            "Укажите <b>назначение</b> (на что запрашивается; обязательно):",
            reply_markup=b.as_markup(),
        )
        return


@router.callback_query(F.data == "gd_deposit:cancel")
async def gd_deposit_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Внесение депозита отменено.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]


# ==================== B2 TZ v8 (cart-rework): ГД-реакция на групповой запрос ЗП РП ====================

def _fmt_rub(value: float | int | None) -> str:
    """5500 → '5 500'. Без знака валюты (его подставляем рядом)."""
    return f"{float(value or 0):,.0f}".replace(",", " ")


def _parse_task_payload(task: dict) -> dict:
    """Достать payload_json из task → dict (или пустой dict при ошибке)."""
    raw = task.get("payload_json") or task.get("payload")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            import json as _json
            return _json.loads(raw) or {}
        except Exception:
            return {}
    return {}


async def _find_group_tasks(db: Database, group_id: str) -> list[dict]:
    """Все ZP_RP-задачи одной группы (по group_id в payload_json)."""
    cur = await db.conn.execute(
        "SELECT * FROM tasks "
        "WHERE type = ? AND json_extract(payload_json, '$.group_id') = ?",
        (TaskType.ZP_RP.value, group_id),
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def _claim_rp_zp_group(
    db: Database, group_id: str, task_id: int, new_status: str
) -> bool:
    """Атомарно «застолбить» группу ЗП РП перед side-effects (CAS между ГД).

    Группа ЗП РП = ONE task per ГД (общий group_id). ``money_confirm_guard``
    сериализует лишь двойной клик ОДНОГО ГД, но не двух разных. Этот claim
    переводит ВСЕ open/in_progress задачи группы в *new_status* одним UPDATE:
    победитель получает True и делает запись AR/AS + notify + audit; проигравший —
    False и выходит без дублей уведомления/синка/аудита. При пустом group_id —
    fallback на одиночный task (CAS через update_task_status).
    """
    expected = (TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value)
    if group_id:
        claimed = await db.claim_group_tasks(
            group_id, TaskType.ZP_RP.value, new_status, expected_statuses=expected,
        )
        return claimed > 0
    updated = await db.update_task_status(task_id, new_status, expected_statuses=expected)
    return updated is not None


def _render_rp_zp_select(
    task_id: int,
    payment_type: str | None,
    unpaid: list[dict],
    selected: set[int],
) -> tuple[str, Any]:
    """Экран тумблер-выбора счетов к выплате (частичная выплата ЗП РП 10%).

    unpaid — [{invoice_id, invoice_number, amount}] неоплаченных счетов задачи;
    selected — множество выбранных invoice_id. По умолчанию выбраны все. Снятые
    счета останутся открытым запросом (задача закроется лишь когда всё выплачено).
    """
    pt_suffix = (" · 🏦 Кредитные" if payment_type == "credit" else " · 💳 Б/н") if payment_type else ""
    total = sum(float(it.get("amount") or 0) for it in unpaid if int(it.get("invoice_id") or 0) in selected)
    n_sel = sum(1 for it in unpaid if int(it.get("invoice_id") or 0) in selected)
    lines = [f"<pre>💰 <b>Выплата ЗП РП 10%</b>{pt_suffix}"]
    for it in unpaid:
        iid = int(it.get("invoice_id") or 0)
        num = str(it.get("invoice_number") or "?")
        amt = float(it.get("amount") or 0)
        mark = "✓" if iid in selected else " "
        lines.append(f" {mark} №{num:<18s} {_fmt_rub(amt):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Выбрано  {n_sel}/{len(unpaid)}")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append("\nОтметьте счета к выплате (тап — вкл/выкл). Снятые останутся открытым запросом.")
    b = InlineKeyboardBuilder()
    for it in unpaid:
        iid = int(it.get("invoice_id") or 0)
        num = str(it.get("invoice_number") or "?")
        amt = float(it.get("amount") or 0)
        mark = "✓ " if iid in selected else "▫️ "
        b.button(
            text=f"{mark}№{num} — {_fmt_rub(amt)}₽",
            callback_data=RpZpPaySelCb(task_id=task_id, inv_id=iid, action="toggle").pack(),
        )
    b.button(text="✅ Все", callback_data=RpZpPaySelCb(task_id=task_id, inv_id=0, action="all").pack())
    b.button(text="▫️ Снять все", callback_data=RpZpPaySelCb(task_id=task_id, inv_id=0, action="none").pack())
    if selected:
        b.button(
            text=f"💳 Выплатить выбранные ({_fmt_rub(total)} ₽)",
            callback_data=RpZpPaySelCb(task_id=task_id, inv_id=0, action="go").pack(),
        )
    b.button(text="❌ Отмена", callback_data=RpZpPayActCb(task_id=task_id, action="cancel").pack())
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


def _render_rp_zp_pay_confirm(
    task_id: int,
    payment_type: str | None,
    total: float,
    n_inv: int,
    has_receipt: bool,
) -> tuple[str, Any]:
    """Экран подтверждения выплаты ЗП РП 10% (платёжка опциональна)."""
    pt_suffix = (" · 🏦 Кредитные" if payment_type == "credit" else " · 💳 Б/н") if payment_type else ""
    receipt_line = "  Платёжка: <b>прикреплена</b>\n" if has_receipt else ""
    tail = (
        "и РП получит уведомление + платёжку."
        if has_receipt
        else "и РП получит уведомление о выплате."
    )
    text = (
        f"💰 <b>Выплата ЗП РП 10%</b>{pt_suffix}\n\n"
        f"  Счетов: <b>{n_inv}</b>\n"
        f"  Сумма: <b>{_fmt_rub(total)} ₽</b>\n"
        f"{receipt_line}\n"
        f"Платёжка опциональна. После подтверждения счета отмечаются выплаченными (AR/AS), "
        f"{tail}"
    )
    b = InlineKeyboardBuilder()
    if has_receipt:
        b.button(
            text="✅ Выплатить и отправить РП",
            callback_data=RpZpPayActCb(task_id=task_id, action="submit").pack(),
        )
    else:
        b.button(
            text="✅ Выплатить без платёжки",
            callback_data=RpZpPayActCb(task_id=task_id, action="submit").pack(),
        )
        b.button(
            text="📎 С платёжкой",
            callback_data=RpZpPayActCb(task_id=task_id, action="attach").pack(),
        )
    b.button(
        text="❌ Отмена",
        callback_data=RpZpPayActCb(task_id=task_id, action="cancel").pack(),
    )
    b.adjust(1)
    return text, b.as_markup()


@router.callback_query(RpZpPayCb.filter())
async def gd_rp_zp_pay(
    cb: CallbackQuery,
    callback_data: RpZpPayCb,
    db: Database,
    state: FSMContext,
) -> None:
    """B2 ГД: «✅ Выплатить» → экран тумблер-выбора счетов к выплате (FSM RpZpPaySG.select).

    ГД отмечает, какие счета оплатить сейчас (по умолчанию все). Далее — экран
    подтверждения с ОПЦИОНАЛЬНОЙ платёжкой. Сама выплата (UPDATE AR/AS + sync +
    close/reopen group tasks + notify) — в rp_zp_pay_submit. Уже выплаченные счета
    (частичная выплата ранее) в список не попадают.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    task_id = callback_data.task_id
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    payload = _parse_task_payload(task)
    invoices_info = payload.get("invoices") or []
    payment_type = payload.get("payment_type") or ""
    if not invoices_info:
        await cb.answer("Список счетов пуст", show_alert=True)
        return
    # Оставляем только НЕоплаченные счета (частичная выплата: rp_payout_op уже стоит).
    unpaid: list[dict] = []
    for it in invoices_info:
        iid = int(it.get("invoice_id") or 0)
        if not iid:
            continue
        inv = await db.get_invoice(iid)
        if inv and float(inv.get("rp_payout_op") or 0) > 0:
            continue  # уже выплачен ранее
        unpaid.append({
            "invoice_id": iid,
            "invoice_number": it.get("invoice_number"),
            "amount": float(it.get("amount") or 0),
        })
    if not unpaid:
        await cb.answer("Все счета уже выплачены", show_alert=True)
        return
    selected = {int(it["invoice_id"]) for it in unpaid}  # по умолчанию выбраны все
    await state.clear()
    await state.set_state(RpZpPaySG.select)
    await state.update_data(
        task_id=task_id,
        payment_type=payment_type,
        group_id=payload.get("group_id") or "",
        unpaid=unpaid,
        selected=list(selected),
        receipt_file_id=None,
        receipt_file_type=None,
    )
    text, kb = _render_rp_zp_select(task_id, payment_type, unpaid, selected)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
    await cb.answer()


@router.callback_query(RpZpPaySelCb.filter(), RpZpPaySG.select)
async def rp_zp_pay_select(
    cb: CallbackQuery,
    callback_data: RpZpPaySelCb,
    state: FSMContext,
) -> None:
    """Тумблер-выбор счетов к выплате: toggle/all/none — перерисовка; go — к confirm."""
    data = await state.get_data()
    unpaid = data.get("unpaid") or []
    selected = {int(x) for x in (data.get("selected") or [])}
    task_id = int(data.get("task_id") or callback_data.task_id or 0)
    pt = data.get("payment_type") or ""
    all_ids = {int(it.get("invoice_id") or 0) for it in unpaid}
    act = callback_data.action
    if act == "toggle":
        iid = int(callback_data.inv_id)
        selected.discard(iid) if iid in selected else selected.add(iid)
    elif act == "all":
        selected = set(all_ids)
    elif act == "none":
        selected = set()
    elif act == "go":
        if not selected:
            await cb.answer("Выберите хотя бы один счёт", show_alert=True)
            return
        sel_inv = [it for it in unpaid if int(it.get("invoice_id") or 0) in selected]
        total = sum(float(it.get("amount") or 0) for it in sel_inv)
        await state.update_data(selected=list(selected), total=total, n_inv=len(sel_inv))
        await state.set_state(RpZpPaySG.confirm)
        text, kb = _render_rp_zp_pay_confirm(task_id, pt, total, len(sel_inv), has_receipt=False)
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
        await cb.answer()
        return
    await state.update_data(selected=list(selected))
    text, kb = _render_rp_zp_select(task_id, pt, unpaid, selected)
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()


@router.callback_query(RpZpPayActCb.filter(F.action == "attach"), RpZpPaySG.confirm)
async def rp_zp_pay_attach(cb: CallbackQuery, state: FSMContext) -> None:
    """ГД выбрал приложить платёжку → ждём фото/документ."""
    await cb.answer()
    data = await state.get_data()
    tid = int(data.get("task_id") or 0)
    await state.set_state(RpZpPaySG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=RpZpPayActCb(task_id=tid, action="cancel").pack())
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📎 Пришлите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )
    except Exception:
        await cb.message.answer(  # type: ignore[union-attr]
            "📎 Пришлите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )


@router.message(RpZpPaySG.attach_receipt, F.content_type.in_({"document", "photo", "video"}))
async def rp_zp_pay_receipt(message: Message, state: FSMContext) -> None:
    """Получена платёжка → сохранить + вернуться к подтверждению (теперь с платёжкой)."""
    file_id: str | None = None
    file_type: str = ""
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
    if not file_id:
        await message.answer("❌ Не удалось получить файл. Прикрепите PDF, фото или документ.")
        return
    data = await state.get_data()
    await state.update_data(receipt_file_id=file_id, receipt_file_type=file_type)
    await state.set_state(RpZpPaySG.confirm)
    tid = int(data.get("task_id") or 0)
    pt = data.get("payment_type") or ""
    total = float(data.get("total") or 0)
    n_inv = int(data.get("n_inv") or 0)
    text, kb = _render_rp_zp_pay_confirm(tid, pt, total, n_inv, has_receipt=True)
    await message.answer(text, reply_markup=kb)


@router.message(RpZpPaySG.attach_receipt)
async def rp_zp_pay_receipt_invalid(message: Message) -> None:
    """Некорректный ввод вместо платёжки."""
    await message.answer("📎 Прикрепите платёжку (PDF, фото или документ), либо нажмите «❌ Отмена».")


@router.callback_query(RpZpPayActCb.filter(F.action == "cancel"))
async def rp_zp_pay_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена выплаты ЗП РП на любом шаге (платёжка ещё не проведена)."""
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Выплата ЗП РП отменена.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Выплата ЗП РП отменена.")  # type: ignore[union-attr]


@router.callback_query(RpZpPayActCb.filter(F.action == "submit"), RpZpPaySG.confirm)
@money_confirm_guard
async def rp_zp_pay_submit(
    cb: CallbackQuery,
    callback_data: RpZpPayActCb,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Финал: UPDATE rp_payout_op/AR по ВЫБРАННЫМ счетам + close/reopen group + notify РП.

    Частичная выплата: платятся только счета из state['selected']. Снятые (не
    выплаченные) счета остаются открытым запросом — группа переоткрывается с
    урезанным payload; закрывается в DONE лишь когда выплачено всё.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    data = await state.get_data()
    task_id = int(data.get("task_id") or callback_data.task_id or 0)
    selected_ids = {int(x) for x in (data.get("selected") or [])}
    receipt_file_id = data.get("receipt_file_id")
    receipt_file_type = data.get("receipt_file_type")
    await state.clear()  # anti-replay (+ @money_confirm_guard от конкурентного двойного клика)
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    # race-guard: другой ГД уже мог выплатить/отклонить группу
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    if not selected_ids:
        await cb.answer("Сессия потеряна — откройте задачу заново", show_alert=True)
        return
    payload = _parse_task_payload(task)
    invoices_info = payload.get("invoices") or []
    rp_id = payload.get("rp_id")
    group_id = payload.get("group_id") or ""
    _pt = payload.get("payment_type")
    pt_suffix = (" · 🏦 Кредитные" if _pt == "credit" else " · 💳 Б/н") if _pt else ""
    # CAS-claim группы ДО любых side-effects: при гонке двух ГД победит ровно один,
    # второй выйдет здесь — без дубля уведомления РП / синка / аудита. Закрывает ВСЕ
    # задачи группы → done; ниже (при частичной выплате) переоткроем с остатком.
    if not await _claim_rp_zp_group(db, group_id, task_id, TaskStatus.DONE.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    today_str = datetime.now().strftime("%d.%m.%Y")
    # 1. Per-invoice UPDATE rp_payout_op + rp_payout_date_op + sync — ТОЛЬКО выбранные.
    paid_lines: list[str] = []
    paid_ids: set[int] = set()
    total_paid = 0.0
    for it in invoices_info:
        inv_id = int(it.get("invoice_id") or 0)
        if not inv_id or inv_id not in selected_ids:
            continue  # снятые счета не платим (останутся открытым запросом)
        inv = await db.get_invoice(inv_id)
        if not inv:
            continue
        if float(inv.get("rp_payout_op") or 0) > 0:
            continue  # идемпотентность: уже выплачен ранее
        amount = float(inv.get("rp_request_op") or it.get("amount") or 0)
        if amount <= 0:
            continue
        try:
            await db.update_invoice(
                inv_id,
                rp_payout_op=amount,
                rp_payout_date_op=today_str,
            )
            await integrations.sync_invoice_row(inv_id)
        except Exception:
            log.exception("rp_zp_pay_submit: update/sync failed inv_id=%s", inv_id)
            continue
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="rp_zp_payout",
                entity="invoice",
                entity_id=str(inv_id),
                payload={"amount": amount, "date": today_str, "group_id": group_id},
            )
        except Exception:
            log.exception("rp_zp_pay_submit: audit failed inv_id=%s", inv_id)
        paid_lines.append(f"   №{(it.get('invoice_number') or '?'):<18s} {_fmt_rub(amount):>10s} ₽")
        paid_ids.add(inv_id)
        total_paid += amount
    # 2. Остаток = счета, которые фактически НЕ выплачены (снятые + не удалось).
    #    Есть остаток → переоткрываем группу с урезанным payload (частичная выплата).
    remaining = [it for it in invoices_info if int(it.get("invoice_id") or 0) not in paid_ids]
    remaining_note = ""
    if remaining:
        rem_total = sum(float(it.get("amount") or 0) for it in remaining)
        patch = {
            "invoices": remaining,
            "invoice_ids": [int(it.get("invoice_id") or 0) for it in remaining],
            "total": rem_total,
        }
        try:
            await db.reopen_group_tasks(
                group_id, TaskType.ZP_RP.value, patch, fallback_task_id=task_id,
            )
        except Exception:
            log.exception("rp_zp_pay_submit: reopen_group_tasks failed group=%s", group_id)
        rem_lines = "\n".join(f"   №{(it.get('invoice_number') or '?')}" for it in remaining)
        remaining_note = (
            "\n   ┈┈┈ осталось к выплате ┈┈┈\n"
            + rem_lines
            + f"\n   Осталось  {_fmt_rub(rem_total)} ₽"
        )
    # 3. Notify РП — список выплаченных (+ остаток, если частично; + опц. платёжка).
    receipt_note = "\n   Платёжка              приложена" if (receipt_file_id and paid_lines) else ""
    if rp_id and paid_lines:
        notify = (
            f"<pre>✅ <b>Выплачено ЗП РП</b>{pt_suffix}\n"
            + "\n".join(paid_lines)
            + f"\n   Дата                  {today_str}"
            + receipt_note
            + "\n   ━━━━━━━━━━━━━━━━"
            + f"\n   Итого  {_fmt_rub(total_paid)} ₽"
            + remaining_note
            + "</pre>"
        )
        try:
            await notifier.safe_send(int(rp_id), notify)
        except Exception:
            log.exception("rp_zp_pay_submit: notify РП %s failed", rp_id)
        if receipt_file_id:
            try:
                await notifier.safe_send_media(
                    int(rp_id),
                    str(receipt_file_type or "document"),
                    str(receipt_file_id),
                    caption="💳 Платёжка по выплате ЗП РП",
                )
            except Exception:
                log.exception("rp_zp_pay_submit: receipt to РП %s failed", rp_id)
    # 4. Update GD's own card
    if paid_lines:
        gd_text = (
            f"<pre>✅ <b>Выплачено</b>{pt_suffix}\n"
            + "\n".join(paid_lines)
            + f"\n   Дата                  {today_str}"
            + receipt_note
            + "\n   ━━━━━━━━━━━━━━━━"
            + f"\n   Итого  {_fmt_rub(total_paid)} ₽"
            + remaining_note
            + "</pre>"
        )
        answer_txt = "Выплачено частично" if remaining else "Выплачено"
    else:
        gd_text = "ℹ️ Нет счетов к выплате (возможно, уже оплачены)."
        answer_txt = "Нет счетов к выплате"
    try:
        await cb.message.edit_text(gd_text)  # type: ignore[union-attr]
    except Exception:
        log.exception("rp_zp_pay_submit: edit_text failed")
    await cb.answer(answer_txt)


@router.callback_query(RpZpRejectCb.filter())
async def gd_rp_zp_reject(
    cb: CallbackQuery,
    callback_data: RpZpRejectCb,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """B2 ГД: «❌ Отклонить» → reset rp_request_op для всех + mark group tasks REJECTED + notify РП."""
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    task_id = callback_data.task_id
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    payload = _parse_task_payload(task)
    invoices_info = payload.get("invoices") or []
    rp_id = payload.get("rp_id")
    group_id = payload.get("group_id") or ""
    _pt = payload.get("payment_type")
    pt_suffix = (" · 🏦 Кредитные" if _pt == "credit" else " · 💳 Б/н") if _pt else ""
    # CAS-claim группы ДО side-effects: при гонке (2 ГД отклоняют, либо отклонение
    # против выплаты) победит ровно один — второй выйдет без дубля уведомления/синка.
    # Группа атомарно переводится в rejected (заменяет прежний пост-фактум цикл).
    if not await _claim_rp_zp_group(db, group_id, task_id, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    rejected_lines: list[str] = []
    for it in invoices_info:
        inv_id = int(it.get("invoice_id") or 0)
        if not inv_id:
            continue
        try:
            await db.update_invoice(inv_id, rp_request_op=None)
            await integrations.sync_invoice_row(inv_id)
        except Exception:
            log.exception("gd_rp_zp_reject: update/sync failed inv_id=%s", inv_id)
            continue
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="rp_zp_reject",
                entity="invoice",
                entity_id=str(inv_id),
                payload={"group_id": group_id},
            )
        except Exception:
            log.exception("gd_rp_zp_reject: audit failed inv_id=%s", inv_id)
        rejected_lines.append(f"   №{(it.get('invoice_number') or '?')}")
    # (Задачи группы уже переведены в rejected CAS-claim'ом выше.)
    if rp_id:
        body = "\n".join(rejected_lines) if rejected_lines else "   (счета не указаны)"
        try:
            await notifier.safe_send(
                int(rp_id),
                f"<pre>❌ <b>Запрос ЗП РП отклонён</b>{pt_suffix}\n{body}</pre>",
            )
        except Exception:
            log.exception("gd_rp_zp_reject: notify РП %s failed", rp_id)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"<pre>❌ <b>Отклонено</b>{pt_suffix}\n" + "\n".join(rejected_lines) + "</pre>",
        )
    except Exception:
        log.exception("gd_rp_zp_reject: edit_text failed")
    await cb.answer("Отклонено")


# ============================================================================
# ЗП ЗАМЕРЩИКА (объединение «Оплата замеров» + леджер, ТЗ 06.07) — GD-сторона.
# Копия потока ЗП РП, но: единица = замер (zamery_requests), а не счёт; платёж
# заносится во взаиморасчёты (add_zamery_settlement_entry, kind='payment') → долг
# падает ровно на Σ выбранных; на выбранных ставится paid_amount=total_cost. Задача
# ZP_ZAMERY_BATCH одиночная (не группа), поэтому CAS-claim по task-статусу, а
# идемпотентность платежа обеспечивает mark_zamery_paid (paid_amount IS NULL).
# ============================================================================

def _render_zam_zp_select(
    task_id: int,
    zamery: list[dict],
    selected: set[int],
) -> tuple[str, Any]:
    """Экран тумблер-выбора замеров к оплате (частичная выплата ЗП замерщика).

    zamery — [{id, address, total_cost}] неоплаченных замеров задачи; selected —
    множество выбранных id (по умолчанию все). Снятые останутся открытым запросом.
    """
    total = sum(float(z.get("total_cost") or 0) for z in zamery if int(z.get("id") or 0) in selected)
    n_sel = sum(1 for z in zamery if int(z.get("id") or 0) in selected)
    lines = ["<pre>💰 <b>Выплата ЗП замерщика</b>"]
    for z in zamery:
        zid = int(z.get("id") or 0)
        addr = html.escape(str(z.get("address") or f"#{zid}"))
        amt = float(z.get("total_cost") or 0)
        mark = "✓" if zid in selected else " "
        # сумма слева (моноширинно), адрес в конце — колонка сумм не съезжает на кириллице
        lines.append(f" {mark} {_fmt_rub(amt):>7} ₽  {addr}")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Выбрано  {n_sel}/{len(zamery)}")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append("\nОтметьте замеры к оплате (тап — вкл/выкл). Снятые останутся открытым запросом.")
    b = InlineKeyboardBuilder()
    for z in zamery:
        zid = int(z.get("id") or 0)
        addr = str(z.get("address") or f"#{zid}")
        amt = float(z.get("total_cost") or 0)
        mark = "✓ " if zid in selected else "▫️ "
        b.button(
            text=f"{mark}{addr} — {_fmt_rub(amt)}₽",
            callback_data=ZamZpPaySelCb(task_id=task_id, zam_id=zid, action="toggle").pack(),
        )
    b.button(text="✅ Все", callback_data=ZamZpPaySelCb(task_id=task_id, zam_id=0, action="all").pack())
    b.button(text="▫️ Снять все", callback_data=ZamZpPaySelCb(task_id=task_id, zam_id=0, action="none").pack())
    if selected:
        b.button(
            text=f"💳 Выплатить выбранные ({_fmt_rub(total)} ₽)",
            callback_data=ZamZpPaySelCb(task_id=task_id, zam_id=0, action="go").pack(),
        )
    b.button(text="❌ Отмена", callback_data=ZamZpPayActCb(task_id=task_id, action="cancel").pack())
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


def _render_zam_zp_pay_confirm(
    task_id: int,
    total: float,
    n_zam: int,
    has_receipt: bool,
) -> tuple[str, Any]:
    """Экран подтверждения выплаты ЗП замерщика (платёжка опциональна)."""
    receipt_line = "  Платёжка: <b>прикреплена</b>\n" if has_receipt else ""
    tail = (
        "и замерщик получит уведомление + платёжку."
        if has_receipt
        else "и замерщик получит уведомление о выплате."
    )
    text = (
        f"💰 <b>Выплата ЗП замерщика</b>\n\n"
        f"  Замеров: <b>{n_zam}</b>\n"
        f"  Сумма: <b>{_fmt_rub(total)} ₽</b>\n"
        f"{receipt_line}\n"
        f"Платёжка опциональна. После подтверждения замеры отмечаются оплаченными, "
        f"платёж заносится во взаиморасчёты (долг −{_fmt_rub(total)} ₽), {tail}"
    )
    b = InlineKeyboardBuilder()
    if has_receipt:
        b.button(
            text="✅ Выплатить и отправить",
            callback_data=ZamZpPayActCb(task_id=task_id, action="submit").pack(),
        )
    else:
        b.button(
            text="✅ Выплатить без платёжки",
            callback_data=ZamZpPayActCb(task_id=task_id, action="submit").pack(),
        )
        b.button(
            text="📎 С платёжкой",
            callback_data=ZamZpPayActCb(task_id=task_id, action="attach").pack(),
        )
    b.button(
        text="❌ Отмена",
        callback_data=ZamZpPayActCb(task_id=task_id, action="cancel").pack(),
    )
    b.adjust(1)
    return text, b.as_markup()


@router.callback_query(ZamZpPayCb.filter())
async def gd_zam_zp_pay(
    cb: CallbackQuery,
    callback_data: ZamZpPayCb,
    db: Database,
    state: FSMContext,
) -> None:
    """ГД: «✅ Выплатить» → экран тумблер-выбора замеров к оплате (FSM ZamZpPaySG.select).

    По умолчанию выбраны все неоплаченные замеры запроса. Уже оплаченные (частичная
    выплата ранее) в список не попадают. Сама выплата — в zam_zp_pay_submit.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    task_id = callback_data.task_id
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    payload = _parse_task_payload(task)
    zam_ids = payload.get("zam_ids") or []
    surveyor_id = int(payload.get("surveyor_id") or 0)
    if not zam_ids or not surveyor_id:
        await cb.answer("Список замеров пуст", show_alert=True)
        return
    # Оставляем только НЕоплаченные замеры (paid_amount IS NULL).
    zamery: list[dict] = []
    for zid in zam_ids:
        z = await db.get_zamery_request(int(zid))
        if not z:
            continue
        if z.get("paid_amount") is not None:
            continue  # уже оплачен ранее
        zamery.append({
            "id": int(z["id"]),
            "address": z.get("address"),
            "total_cost": float(z.get("total_cost") or 0),
        })
    if not zamery:
        await cb.answer("Все замеры уже оплачены", show_alert=True)
        return
    selected = {int(z["id"]) for z in zamery}  # по умолчанию выбраны все
    await state.clear()
    await state.set_state(ZamZpPaySG.select)
    await state.update_data(
        task_id=task_id,
        surveyor_id=surveyor_id,
        zamery=zamery,
        selected=list(selected),
        receipt_file_id=None,
        receipt_file_type=None,
    )
    text, kb = _render_zam_zp_select(task_id, zamery, selected)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
    await cb.answer()


@router.callback_query(ZamZpPaySelCb.filter(), ZamZpPaySG.select)
async def zam_zp_pay_select(
    cb: CallbackQuery,
    callback_data: ZamZpPaySelCb,
    state: FSMContext,
) -> None:
    """Тумблер-выбор замеров к оплате: toggle/all/none — перерисовка; go — к confirm."""
    data = await state.get_data()
    zamery = data.get("zamery") or []
    selected = {int(x) for x in (data.get("selected") or [])}
    task_id = int(data.get("task_id") or callback_data.task_id or 0)
    all_ids = {int(z.get("id") or 0) for z in zamery}
    act = callback_data.action
    if act == "toggle":
        zid = int(callback_data.zam_id)
        selected.discard(zid) if zid in selected else selected.add(zid)
    elif act == "all":
        selected = set(all_ids)
    elif act == "none":
        selected = set()
    elif act == "go":
        if not selected:
            await cb.answer("Выберите хотя бы один замер", show_alert=True)
            return
        sel = [z for z in zamery if int(z.get("id") or 0) in selected]
        total = sum(float(z.get("total_cost") or 0) for z in sel)
        await state.update_data(selected=list(selected), total=total, n_zam=len(sel))
        await state.set_state(ZamZpPaySG.confirm)
        text, kb = _render_zam_zp_pay_confirm(task_id, total, len(sel), has_receipt=False)
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
        await cb.answer()
        return
    await state.update_data(selected=list(selected))
    text, kb = _render_zam_zp_select(task_id, zamery, selected)
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()


@router.callback_query(ZamZpPayActCb.filter(F.action == "attach"), ZamZpPaySG.confirm)
async def zam_zp_pay_attach(cb: CallbackQuery, state: FSMContext) -> None:
    """ГД выбрал приложить платёжку → ждём фото/документ."""
    await cb.answer()
    data = await state.get_data()
    tid = int(data.get("task_id") or 0)
    await state.set_state(ZamZpPaySG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=ZamZpPayActCb(task_id=tid, action="cancel").pack())
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📎 Пришлите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )
    except Exception:
        await cb.message.answer(  # type: ignore[union-attr]
            "📎 Пришлите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )


@router.message(ZamZpPaySG.attach_receipt, F.content_type.in_({"document", "photo", "video"}))
async def zam_zp_pay_receipt(message: Message, state: FSMContext) -> None:
    """Получена платёжка → сохранить + вернуться к подтверждению (теперь с платёжкой)."""
    file_id: str | None = None
    file_type: str = ""
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
    if not file_id:
        await message.answer("❌ Не удалось получить файл. Прикрепите PDF, фото или документ.")
        return
    data = await state.get_data()
    await state.update_data(receipt_file_id=file_id, receipt_file_type=file_type)
    await state.set_state(ZamZpPaySG.confirm)
    tid = int(data.get("task_id") or 0)
    total = float(data.get("total") or 0)
    n_zam = int(data.get("n_zam") or 0)
    text, kb = _render_zam_zp_pay_confirm(tid, total, n_zam, has_receipt=True)
    await message.answer(text, reply_markup=kb)


@router.message(ZamZpPaySG.attach_receipt)
async def zam_zp_pay_receipt_invalid(message: Message) -> None:
    """Некорректный ввод вместо платёжки."""
    await message.answer("📎 Прикрепите платёжку (PDF, фото или документ), либо нажмите «❌ Отмена».")


@router.callback_query(ZamZpPayActCb.filter(F.action == "cancel"))
async def zam_zp_pay_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена выплаты ЗП замерщика на любом шаге (платёж ещё не проведён)."""
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Выплата ЗП замерщика отменена.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Выплата ЗП замерщика отменена.")  # type: ignore[union-attr]


@router.callback_query(ZamZpPayActCb.filter(F.action == "submit"), ZamZpPaySG.confirm)
@money_confirm_guard
async def zam_zp_pay_submit(
    cb: CallbackQuery,
    callback_data: ZamZpPayActCb,
    state: FSMContext,
    db: Database,
    notifier: Notifier,
) -> None:
    """Финал: mark_zamery_paid по выбранным + платёж в леджер (Σ ново-оплаченных).

    Идемпотентно: mark_zamery_paid платит лишь paid_amount IS NULL и возвращает
    (ids, Σ) фактически ново-оплаченных — платёж в леджер заносится ровно на эту Σ,
    поэтому повторный/гоночный submit не задваивает расход. Невыбранные замеры
    остаются в задаче ('requested'); задача → DONE лишь когда все её замеры оплачены.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    data = await state.get_data()
    task_id = int(data.get("task_id") or callback_data.task_id or 0)
    surveyor_id = int(data.get("surveyor_id") or 0)
    selected_ids = [int(x) for x in (data.get("selected") or [])]
    zamery = data.get("zamery") or []
    receipt_file_id = data.get("receipt_file_id")
    receipt_file_type = data.get("receipt_file_type")
    await state.clear()  # anti-replay (+ money_confirm_guard от конкурентного клика)
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    if not selected_ids or not surveyor_id:
        await cb.answer("Сессия потеряна — откройте задачу заново", show_alert=True)
        return
    today_iso = datetime.now().strftime("%Y-%m-%d")   # для БД/леджера (сортируемо)
    today_disp = datetime.now().strftime("%d.%m.%Y")  # для текста
    # 1. Отметить выбранные оплаченными — ИДЕМПОТЕНТНО. Возвращает Σ ново-оплаченных.
    paid_ids, paid_sum = await db.mark_zamery_paid(selected_ids, today_iso, surveyor_id)
    # 2. Платёж в леджер = Σ ново-оплаченных (долг −paid_sum). Только если что-то оплачено.
    zmap = {int(z.get("id") or 0): z for z in zamery}
    paid_lines: list[str] = []
    for zid in paid_ids:
        z = zmap.get(zid, {})
        addr = html.escape(str(z.get("address") or f"#{zid}"))
        paid_lines.append(f"   {_fmt_rub(z.get('total_cost') or 0):>7} ₽  {addr}")
    if paid_sum > 0:
        try:
            await db.add_zamery_settlement_entry(
                surveyor_id=surveyor_id,
                entry_date=today_iso,
                kind="payment",
                amount=paid_sum,
                comment=f"ЗП замерщика: {len(paid_ids)} замер(ов)",
                created_by=cb.from_user.id,
            )
        except Exception:
            log.exception("zam_zp_pay_submit: ledger entry failed surveyor=%s", surveyor_id)
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="zam_zp_payout",
                entity="surveyor",
                entity_id=str(surveyor_id),
                payload={"amount": paid_sum, "date": today_iso, "zam_ids": sorted(paid_ids), "task_id": task_id},
            )
        except Exception:
            log.exception("zam_zp_pay_submit: audit failed")
    # 3. Остаток = замеры задачи, всё ещё не оплаченные (снятые + не удалось). Есть
    #    остаток → задача остаётся открытой (ГД доплатит позже той же кнопкой). Нет →
    #    задача DONE (CAS от гонки — но платёж уже идемпотентен через mark_zamery_paid).
    all_ids = [int(z) for z in (_parse_task_payload(task).get("zam_ids") or [])]
    remaining: list[dict] = []
    for zid in all_ids:
        zz = await db.get_zamery_request(int(zid))
        if zz and zz.get("paid_amount") is None:
            remaining.append(zz)
    remaining_note = ""
    if remaining:
        rem_total = sum(float(z.get("total_cost") or 0) for z in remaining)
        rem_lines = "\n".join(
            f"   {html.escape(str(z.get('address') or ('#' + str(z.get('id')))))}"
            for z in remaining
        )
        remaining_note = (
            "\n   ┈┈┈ осталось к оплате ┈┈┈\n"
            + rem_lines
            + f"\n   Осталось  {_fmt_rub(rem_total)} ₽"
        )
    else:
        try:
            await db.update_task_status(
                task_id, TaskStatus.DONE.value,
                expected_statuses=(TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value),
            )
        except Exception:
            log.exception("zam_zp_pay_submit: close task failed task=%s", task_id)
    # 4. Notify замерщику — список оплаченных (+ остаток, если частично; + опц. платёжка).
    receipt_note = "\n   Платёжка              приложена" if (receipt_file_id and paid_lines) else ""
    if paid_lines:
        notify = (
            "<pre>✅ <b>Выплачено ЗП замерщика</b>\n"
            + "\n".join(paid_lines)
            + f"\n   Дата                  {today_disp}"
            + receipt_note
            + "\n   ━━━━━━━━━━━━━━━━"
            + f"\n   Итого  {_fmt_rub(paid_sum)} ₽"
            + remaining_note
            + "</pre>"
        )
        try:
            await notifier.safe_send(int(surveyor_id), notify)
        except Exception:
            log.exception("zam_zp_pay_submit: notify замерщику %s failed", surveyor_id)
        if receipt_file_id:
            try:
                await notifier.safe_send_media(
                    int(surveyor_id),
                    str(receipt_file_type or "document"),
                    str(receipt_file_id),
                    caption="💳 Платёжка по выплате ЗП замерщика",
                )
            except Exception:
                log.exception("zam_zp_pay_submit: receipt to замерщику %s failed", surveyor_id)
    # 5. Update GD's own card
    if paid_lines:
        gd_text = (
            "<pre>✅ <b>Выплачено</b>\n"
            + "\n".join(paid_lines)
            + f"\n   Дата                  {today_disp}"
            + receipt_note
            + "\n   ━━━━━━━━━━━━━━━━"
            + f"\n   Итого  {_fmt_rub(paid_sum)} ₽"
            + remaining_note
            + "</pre>"
        )
        answer_txt = "Выплачено частично" if remaining else "Выплачено"
    else:
        gd_text = "ℹ️ Нет замеров к выплате (возможно, уже оплачены)."
        answer_txt = "Нет замеров к выплате"
    try:
        await cb.message.edit_text(gd_text)  # type: ignore[union-attr]
    except Exception:
        log.exception("zam_zp_pay_submit: edit_text failed")
    await cb.answer(answer_txt)


@router.callback_query(ZamZpRejectCb.filter())
async def gd_zam_zp_reject(
    cb: CallbackQuery,
    callback_data: ZamZpRejectCb,
    db: Database,
    notifier: Notifier,
) -> None:
    """ГД: «❌ Отклонить» → неоплаченные замеры → «К оплате», task REJECTED, notify.

    Долг/paid_amount не меняются. CAS-claim task-статуса от гонки.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD, Role.TD]):
        return
    task_id = callback_data.task_id
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") in (TaskStatus.DONE.value, TaskStatus.REJECTED.value):
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    # CAS-claim: атомарно REJECTED — при гонке (2 ГД / reject vs pay) победит один.
    updated = await db.update_task_status(
        task_id, TaskStatus.REJECTED.value,
        expected_statuses=(TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value),
    )
    if updated is None:
        await cb.answer("Запрос уже обработан", show_alert=True)
        return
    payload = _parse_task_payload(task)
    zam_ids = [int(z) for z in (payload.get("zam_ids") or [])]
    surveyor_id = int(payload.get("surveyor_id") or 0)
    if zam_ids and surveyor_id:
        # Вернуть в «К оплате» только НЕоплаченные (оплаченные не трогаем).
        unpaid_ids: list[int] = []
        for zid in zam_ids:
            zz = await db.get_zamery_request(zid)
            if zz and zz.get("paid_amount") is None:
                unpaid_ids.append(zid)
        if unpaid_ids:
            await db.set_zamery_pay_status(unpaid_ids, "not_requested", surveyor_id)
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="zam_zp_reject",
                entity="surveyor",
                entity_id=str(surveyor_id),
                payload={"zam_ids": zam_ids, "task_id": task_id},
            )
        except Exception:
            log.exception("gd_zam_zp_reject: audit failed")
    if surveyor_id:
        try:
            await notifier.safe_send(
                int(surveyor_id),
                "<pre>❌ <b>Запрос ЗП замерщика отклонён</b>\nЗамеры возвращены в «К оплате».</pre>",
            )
        except Exception:
            log.exception("gd_zam_zp_reject: notify замерщику %s failed", surveyor_id)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "❌ <b>Отклонено</b> — замеры возвращены в «К оплате».",
        )
    except Exception:
        log.exception("gd_zam_zp_reject: edit_text failed")
    await cb.answer("Отклонено")
