"""
Handlers for Accounting (Бухгалтерия) role — new menu.

Covers:
- Входящие задачи (EDO requests)
- Не срочно ГД (reuses existing)
- Найти Счет №
- Закрытые Счета (list of ended invoices)
- EDO Response (EdoResponseSG)
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, Role, TaskStatus
from ..keyboards import (
    ACC_BTN_INVOICE_END,
    ACC_BTN_INVOICES_WORK,
    ACC_BTN_SYNC,
    main_menu,
)
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import AccRequestToManagerSG, EdoResponseSG
from ..utils import answer_service, get_initiator_label, private_only_reply_markup
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
# ВХОДЯЩИЕ ЗАДАЧИ (бухгалтерия — только непринятые)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие задачи"))
async def acc_inbox_tasks(message: Message, db: Database) -> None:
    """Входящие задачи бухгалтерии — только без подтверждения получения."""
    if not message.from_user:
        return
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)
    # Только непринятые (accepted_at IS NULL)
    unconfirmed = [t for t in tasks if not t.get("accepted_at")]
    if not unconfirmed:
        await answer_service(message, "📥 Нет новых задач ✅", delay_seconds=60)
        return

    await message.answer(f"📥 <b>Входящие задачи</b> ({len(unconfirmed)}):")

    # #13: Batch-загрузка счетов для карточек (вместо N+1 запросов)
    _payloads: list[dict] = []
    _inv_ids: set[int] = set()
    for t in unconfirmed[:15]:
        p = {}
        if t.get("payload_json"):
            try:
                p = json.loads(t["payload_json"]) if isinstance(t["payload_json"], str) else t["payload_json"]
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
        _payloads.append(p)
        iid = p.get("invoice_id")
        if iid:
            _inv_ids.add(int(iid))

    # Один запрос на все счета
    _inv_cache: dict[int, dict] = {}
    for iid in _inv_ids:
        inv_row = await db.get_invoice(iid)
        if inv_row:
            _inv_cache[iid] = inv_row

    for idx, t in enumerate(unconfirmed[:15]):
        tid = int(t["id"])
        payload = _payloads[idx]
        inv_num = payload.get("invoice_number", "")
        inv_id = payload.get("invoice_id")
        req_text = str(payload.get("request_text") or payload.get("comment") or "")[:100]

        # #13: Структурированная карточка с суммой и долгом на одной строке
        text = f"📋 <b>Задача #{tid}</b>\n"
        if inv_num:
            text += f"📄 Счёт: <b>{inv_num}</b>\n"
        # Данные счёта из кеша
        if inv_id and int(inv_id) in _inv_cache:
            _inv = _inv_cache[int(inv_id)]
            mgr = _inv.get("creator_role") or ""
            text += f"👤 Менеджер: {mgr}\n"
            rd = (_inv.get("receipt_date") or "")[:10]
            dd = _inv.get("deadline_days") or ""
            if rd:
                text += f"📅 Сроки: {rd}"
                if dd:
                    text += f" ({dd} дн.)"
                text += "\n"
            amt = float(_inv.get("amount") or 0)
            debt = float(_inv.get("outstanding_debt") or 0)
            text += f"💰 Сумма: {amt:,.0f}₽ | Долг: {debt:,.0f}₽\n"
            if _inv.get("edo_signed"):
                text += f"📝 ЭДО: подписан\n"
        text += f"💬 {req_text}\n" if req_text else ""
        text += f"📅 {(t.get('created_at') or '-')[:10]}"

        from ..callbacks import TaskCb
        b = InlineKeyboardBuilder()
        # Кнопка "Принято" + "Документы" если привязан к счёту
        b.button(text="✅ Принято", callback_data=TaskCb(task_id=tid, action="accept").pack())
        inv_id = payload.get("invoice_id")
        if inv_id:
            b.button(text="✏️ Документы", callback_data=f"acc_doc:menu:{inv_id}")
        b.adjust(2)
        await message.answer(text, reply_markup=b.as_markup())

    footer = InlineKeyboardBuilder()
    footer.button(text="⬅️ Назад", callback_data="nav:home")
    footer.adjust(1)
    await message.answer("—", reply_markup=footer.as_markup())


# =====================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ (обновление меню + сводка для бухгалтерии)
# =====================================================================

@router.message(F.text == ACC_BTN_SYNC)
async def acc_sync_data(message: Message, db: Database, config: Config) -> None:
    """Синхронизация данных для бухгалтерии — обновить меню + показать сводку."""
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]

    # Собираем статистику
    invoices_work = await db.list_invoices_in_work(limit=500, only_regular=True)
    invoices_ended = await db.list_invoices(status=InvoiceStatus.ENDED, limit=500, only_regular=True)
    tasks = await db.list_tasks_for_user(user_id, limit=100)
    unconfirmed = [t for t in tasks if not t.get("accepted_at")]

    # Документы без подписания
    docs_pending = sum(
        1 for inv in invoices_work
        if not (inv.get("docs_edo_signed") and inv.get("edo_signed")
                and inv.get("docs_originals_holder") and inv.get("closing_originals_holder"))
    )

    # Просроченные сроки
    overdue = 0
    for inv in invoices_work:
        dl = inv.get("deadline_end_date")
        if dl:
            try:
                end = datetime.fromisoformat(dl).date()
                if (end - date.today()).days < 0:
                    overdue += 1
            except (ValueError, TypeError):
                pass

    text = (
        "🔄 <b>Синхронизация данных</b>\n\n"
        f"📊 Счетов в работе: <b>{len(invoices_work)}</b>\n"
        f"🏁 Закрытых счетов: <b>{len(invoices_ended)}</b>\n"
        f"📥 Непринятых задач: <b>{len(unconfirmed)}</b>\n"
        f"📋 Документы не заполнены: <b>{docs_pending}</b>\n"
        f"🔴 Просрочено сроков: <b>{overdue}</b>\n\n"
        "✅ Меню обновлено."
    )

    is_admin = user_id in (config.admin_ids or set())
    unread = await db.count_unread_tasks(user_id)
    role, isolated_role = await _current_menu(db, user_id)
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=is_admin,
                unread=unread,
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# СЧЕТА В РАБОТЕ (list in-work invoices, excluding credit)
# =====================================================================

@router.message(F.text == ACC_BTN_INVOICES_WORK)
async def acc_invoices_work(message: Message, state: FSMContext, db: Database) -> None:
    """Список счетов в работе (без кредитных) с возможностью отправить запрос менеджеру."""
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    await state.clear()
    await _show_acc_invoices_work(message, db)


def _deadline_indicator(deadline_end_date: str | None) -> str:
    """Return deadline indicator string like ' | Срок: 20.03.2026 ✅' or '' if no date."""
    if not deadline_end_date:
        return ""
    try:
        end = datetime.fromisoformat(deadline_end_date).date()
    except (ValueError, TypeError):
        return ""
    delta = (end - date.today()).days
    if delta < 0:
        icon = "🔴"
    elif delta <= 7:
        icon = "⚠️"
    else:
        icon = "✅"
    return f" | Срок: {end.strftime('%d.%m.%Y')} {icon}"


def _doc_status_line(edo_signed: bool, originals_holder: str | None) -> str:
    """Format document status indicators (compact)."""
    edo = "✅" if edo_signed else "⏳"
    if originals_holder == "gd":
        orig = "✅ГД"
    elif originals_holder == "manager":
        orig = "✅Мнж"
    else:
        orig = "⏳"
    return f"{edo}эдо {orig}ориг"


async def _format_acc_card(inv: dict[str, Any], db: Database) -> str:
    """Build a compact card text for one invoice."""
    num = inv.get("invoice_number") or f"#{inv['id']}"
    date = (inv.get("created_at") or "-")[:10]
    status_icon = {
        "pending": "⏳", "in_progress": "🔄", "paid": "✅",
        "on_hold": "⏸", "closing": "📌",
    }.get(inv.get("status", ""), "❓")

    try:
        amt = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amt = "—"

    debt_val = inv.get("outstanding_debt")
    try:
        debt = f"{float(debt_val):,.0f}₽" if debt_val else "0₽"
    except (ValueError, TypeError):
        debt = "0₽"

    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))
    role_label = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(inv.get("creator_role", ""), inv.get("creator_role") or "")

    supplier = inv.get("supplier") or "—"

    primary = _doc_status_line(
        bool(inv.get("docs_edo_signed")),
        inv.get("docs_originals_holder"),
    )
    secondary = _doc_status_line(
        bool(inv.get("edo_signed")),
        inv.get("closing_originals_holder"),
    )

    dl = _deadline_indicator(inv.get("deadline_end_date"))

    return (
        f"{status_icon} <b>№{num}</b>\n"
        f"📅 {date}{dl}\n"
        f"👤 {creator_label} ({role_label})\n"
        f"🏢 {supplier}\n"
        f"💰 {amt} · 💳 Долг: {debt}\n"
        f"📋 П: {primary} | З: {secondary}"
    )


def _build_acc_summary(
    invoices: list[dict[str, Any]],
    title: str = "📊 Счета в работе — сводка",
) -> str:
    """Build accounting summary card."""
    cnt = len(invoices)
    total_amount = sum(float(inv.get("amount") or 0) for inv in invoices)
    total_debt = sum(float(inv.get("outstanding_debt") or 0) for inv in invoices)

    # Первичка
    prim_edo_ok = sum(1 for inv in invoices if inv.get("docs_edo_signed"))
    prim_edo_wait = cnt - prim_edo_ok
    prim_gd = sum(1 for inv in invoices if inv.get("docs_originals_holder") == "gd")
    prim_mgr = sum(1 for inv in invoices if inv.get("docs_originals_holder") == "manager")
    prim_none = cnt - prim_gd - prim_mgr

    # Вторичка
    clos_edo_ok = sum(1 for inv in invoices if inv.get("edo_signed"))
    clos_edo_wait = cnt - clos_edo_ok
    clos_gd = sum(1 for inv in invoices if inv.get("closing_originals_holder") == "gd")
    clos_mgr = sum(1 for inv in invoices if inv.get("closing_originals_holder") == "manager")
    clos_none = cnt - clos_gd - clos_mgr

    # Просрочки
    overdue = 0
    for inv in invoices:
        dl = inv.get("deadline_end_date")
        if dl:
            try:
                end = datetime.fromisoformat(dl).date()
                if (end - date.today()).days < 0:
                    overdue += 1
            except (ValueError, TypeError):
                pass

    # Документы не заполнены
    docs_pending = sum(
        1 for inv in invoices
        if not (inv.get("docs_edo_signed") and inv.get("edo_signed")
                and inv.get("docs_originals_holder") and inv.get("closing_originals_holder"))
    )

    lines = [
        f"<b>{title}</b> ({cnt})",
        "",
        "<pre>",
        f"{'Всего счетов':22s} {cnt:>5}",
        f"{'Сумма':22s} {total_amount:>12,.0f}₽",
        f"{'Долг':22s} {total_debt:>12,.0f}₽",
        f"{'─' * 30}",
        "Первичка",
        f"  {'ЭДО ✅':22s} {prim_edo_ok:>5}",
        f"  {'ЭДО ⏳':22s} {prim_edo_wait:>5}",
        f"  {'Ориг. у ГД':22s} {prim_gd:>5}",
        f"  {'Ориг. у менеджера':22s} {prim_mgr:>5}",
        f"  {'Без оригиналов':22s} {prim_none:>5}",
        f"{'─' * 30}",
        "Вторичка",
        f"  {'ЭДО ✅':22s} {clos_edo_ok:>5}",
        f"  {'ЭДО ⏳':22s} {clos_edo_wait:>5}",
        f"  {'Закр. у ГД':22s} {clos_gd:>5}",
        f"  {'Закр. у менеджера':22s} {clos_mgr:>5}",
        f"  {'Без закрывающих':22s} {clos_none:>5}",
        f"{'─' * 30}",
        f"{'🔴 Просрочено':22s} {overdue:>5}",
        f"{'📋 Док. не заполнены':22s} {docs_pending:>5}",
        "</pre>",
    ]
    return "\n".join(lines)


async def _show_acc_invoices_work(
    target: Message | CallbackQuery,
    db: Database,
) -> None:
    """Показать сводку + inline-список счетов в работе для бухгалтерии."""
    invoices = await db.list_invoices_in_work(limit=50, only_regular=True)

    msg = target.message if isinstance(target, CallbackQuery) else target
    if not invoices:
        await msg.answer("✅ Нет счетов в работе.")  # type: ignore[union-attr]
        return

    # Сводная карточка
    summary = _build_acc_summary(invoices)
    await msg.answer(summary)  # type: ignore[union-attr]

    # Inline-кнопки счетов
    b = InlineKeyboardBuilder()
    for inv in invoices[:20]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        status_icon = {
            "pending": "⏳", "in_progress": "🔄", "paid": "✅",
            "on_hold": "⏸", "closing": "📌",
        }.get(inv.get("status", ""), "❓")
        try:
            amt = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amt = ""
        b.button(
            text=f"{status_icon} {num} · {amt}",
            callback_data=f"acc_work:card:{inv['id']}",
        )
    b.adjust(1)
    await msg.answer(  # type: ignore[union-attr]
        "Нажмите на счёт для просмотра:",
        reply_markup=b.as_markup(),
    )

    footer = InlineKeyboardBuilder()
    footer.button(text="🔄 Обновить", callback_data="acc_work:refresh")
    footer.button(text="⬅️ Назад", callback_data="nav:home")
    footer.adjust(2)
    await msg.answer("—", reply_markup=footer.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "acc_work:refresh")
async def acc_invoices_work_refresh(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer("🔄 Обновлено")
    await _show_acc_invoices_work(cb, db)


@router.callback_query(F.data.regexp(r"^acc_work:card:\d+$"))
async def acc_invoices_work_card(cb: CallbackQuery, db: Database) -> None:
    """Карточка конкретного счёта — бухгалтерия."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    text = await _format_acc_card(inv, db)
    b = InlineKeyboardBuilder()
    b.button(text="📨 Запрос менеджеру", callback_data=f"acc_work:req:{inv['id']}")
    b.button(text="✏️ Документы", callback_data=f"acc_doc:menu:{inv['id']}")
    b.adjust(2)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^acc_work:view:\d+$"))
async def acc_invoices_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта в работе — бухгалтерия (legacy fallback)."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    text = await _format_acc_card(inv, db)
    b = InlineKeyboardBuilder()
    b.button(text="📨 Запрос менеджеру", callback_data=f"acc_work:req:{inv['id']}")
    b.button(text="⬅️ Назад к списку", callback_data="acc_work:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# РЕДАКТИРОВАНИЕ СТАТУСА ДОКУМЕНТОВ (✏️ Документы)
# =====================================================================

_HOLDER_LABELS = {"gd": "у ГД", "manager": "у менеджера"}


def _build_doc_menu_text(inv: dict[str, Any]) -> str:
    """Text for the document editing menu."""
    num = inv.get("invoice_number") or f"#{inv['id']}"
    prim_edo = "✅" if inv.get("docs_edo_signed") else "⏳"
    prim_h = inv.get("docs_originals_holder")
    prim_orig = f"✅({_HOLDER_LABELS.get(prim_h, '?')})" if prim_h else "⏳"
    clos_edo = "✅" if inv.get("edo_signed") else "⏳"
    clos_h = inv.get("closing_originals_holder")
    clos_orig = f"✅({_HOLDER_LABELS.get(clos_h, '?')})" if clos_h else "⏳"
    return (
        f"📋 <b>Статус документов №{num}</b>\n\n"
        f"📋 Первичка: {prim_edo}ЭДО  {prim_orig}Ориг\n"
        f"📋 Вторичка: {clos_edo}ЭДО  {clos_orig}Ориг\n\n"
        "Нажмите для изменения:"
    )


def _build_doc_menu_kb(inv: dict[str, Any]) -> InlineKeyboardMarkup:
    """Inline keyboard for the document editing menu."""
    inv_id = inv["id"]
    prim_edo = "✅" if inv.get("docs_edo_signed") else "⏳"
    prim_h = inv.get("docs_originals_holder")
    prim_orig = f"✅{_HOLDER_LABELS.get(prim_h, '')}" if prim_h else "⏳"
    clos_edo = "✅" if inv.get("edo_signed") else "⏳"
    clos_h = inv.get("closing_originals_holder")
    clos_orig = f"✅{_HOLDER_LABELS.get(clos_h, '')}" if clos_h else "⏳"
    b = InlineKeyboardBuilder()
    b.button(text=f"📋 Первичка ЭДО: {prim_edo}", callback_data=f"acc_doc:prim_edo:{inv_id}")
    b.button(text=f"📁 Первичка Ориг: {prim_orig}", callback_data=f"acc_doc:prim_orig:{inv_id}")
    b.button(text=f"📋 Вторичка ЭДО: {clos_edo}", callback_data=f"acc_doc:clos_edo:{inv_id}")
    b.button(text=f"📁 Вторичка Ориг: {clos_orig}", callback_data=f"acc_doc:clos_orig:{inv_id}")
    b.button(text="⬅️ Назад", callback_data="acc_work:refresh")
    b.adjust(1)
    return b.as_markup()


async def _notify_manager_doc_change(
    db: Database, notifier: Notifier, inv: dict[str, Any], field: str, new_label: str,
) -> None:
    """Send notification to the invoice manager about doc status change.
    Also auto-closes any pending doc-status tasks (EDO_REQUEST) for this invoice."""
    manager_id = inv.get("created_by")
    if not manager_id:
        return
    num = inv.get("invoice_number") or f"#{inv['id']}"
    inv_id = inv.get("id") or inv.get("invoice_id")
    try:
        await notifier.safe_send(
            int(manager_id),
            f"📋 <b>Статус документов изменён</b>\n"
            f"Счёт №{num}\n"
            f"Бухгалтерия: {field} → {new_label}\n\n"
            f"✅ Задача по документам выполнена.",
        )
    except Exception:
        log.exception("Failed to notify manager %s about doc change", manager_id)
    # Auto-close pending EDO_REQUEST tasks for this invoice
    if inv_id:
        try:
            from ..enums import TaskType
            await db.close_tasks_by_invoice(int(inv_id), TaskType.EDO_REQUEST)
        except Exception:
            log.exception("Failed to close doc tasks for invoice %s", inv_id)


@router.callback_query(F.data.regexp(r"^acc_doc:menu:\d+$"))
async def acc_doc_menu(cb: CallbackQuery, db: Database) -> None:
    """Show document status editing menu."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    await cb.message.answer(  # type: ignore[union-attr]
        _build_doc_menu_text(inv),
        reply_markup=_build_doc_menu_kb(inv),
    )


@router.callback_query(F.data.regexp(r"^acc_doc:prim_edo:\d+$"))
async def acc_doc_toggle_prim_edo(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Toggle primary docs EDO status."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
    new_val = 0 if inv.get("docs_edo_signed") else 1
    await db.update_invoice(inv_id, docs_edo_signed=new_val)
    inv = await db.get_invoice(inv_id)
    label = "✅ подписано" if new_val else "⏳ не подписано"
    await _notify_manager_doc_change(db, notifier, inv, "Первичка ЭДО", label)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            _build_doc_menu_text(inv), reply_markup=_build_doc_menu_kb(inv),
        )
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^acc_doc:clos_edo:\d+$"))
async def acc_doc_toggle_clos_edo(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Toggle closing docs EDO status."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
    new_val = 0 if inv.get("edo_signed") else 1
    await db.update_invoice(inv_id, edo_signed=new_val)
    inv = await db.get_invoice(inv_id)
    label = "✅ подписано" if new_val else "⏳ не подписано"
    await _notify_manager_doc_change(db, notifier, inv, "Вторичка ЭДО", label)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            _build_doc_menu_text(inv), reply_markup=_build_doc_menu_kb(inv),
        )
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^acc_doc:prim_orig:\d+$"))
async def acc_doc_prim_orig_choose(cb: CallbackQuery, db: Database) -> None:
    """Show originals holder choice for primary docs."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    b = InlineKeyboardBuilder()
    b.button(text="У ГД", callback_data=f"acc_doc:prim_orig_set:{inv_id}:gd")
    b.button(text="У менеджера", callback_data=f"acc_doc:prim_orig_set:{inv_id}:manager")
    b.button(text="Нет", callback_data=f"acc_doc:prim_orig_set:{inv_id}:none")
    b.button(text="⬅️ Назад", callback_data=f"acc_doc:menu:{inv_id}")
    b.adjust(3, 1)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📁 <b>Оригиналы первичных документов</b>\nУ кого находятся?",
            reply_markup=b.as_markup(),
        )
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^acc_doc:clos_orig:\d+$"))
async def acc_doc_clos_orig_choose(cb: CallbackQuery, db: Database) -> None:
    """Show originals holder choice for closing docs."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    b = InlineKeyboardBuilder()
    b.button(text="У ГД", callback_data=f"acc_doc:clos_orig_set:{inv_id}:gd")
    b.button(text="У менеджера", callback_data=f"acc_doc:clos_orig_set:{inv_id}:manager")
    b.button(text="Нет", callback_data=f"acc_doc:clos_orig_set:{inv_id}:none")
    b.button(text="⬅️ Назад", callback_data=f"acc_doc:menu:{inv_id}")
    b.adjust(3, 1)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📁 <b>Оригиналы закрывающих документов</b>\nУ кого находятся?",
            reply_markup=b.as_markup(),
        )
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^acc_doc:prim_orig_set:\d+:(gd|manager|none)$"))
async def acc_doc_prim_orig_set(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Set originals holder for primary docs."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    inv_id = int(parts[3])
    val = parts[4]
    holder = None if val == "none" else val
    await db.update_invoice(inv_id, docs_originals_holder=holder)
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
    label = _HOLDER_LABELS.get(val, "нет")
    await _notify_manager_doc_change(db, notifier, inv, "Первичка оригинал", label)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            _build_doc_menu_text(inv), reply_markup=_build_doc_menu_kb(inv),
        )
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^acc_doc:clos_orig_set:\d+:(gd|manager|none)$"))
async def acc_doc_clos_orig_set(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Set originals holder for closing docs."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    inv_id = int(parts[3])
    val = parts[4]
    holder = None if val == "none" else val
    await db.update_invoice(inv_id, closing_originals_holder=holder)
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
    label = _HOLDER_LABELS.get(val, "нет")
    await _notify_manager_doc_change(db, notifier, inv, "Вторичка оригинал", label)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            _build_doc_menu_text(inv), reply_markup=_build_doc_menu_kb(inv),
        )
    except Exception:
        pass


# =====================================================================
# ЗАПРОС МЕНЕДЖЕРУ
# =====================================================================

@router.callback_query(F.data.regexp(r"^acc_work:req:\d+$"))
async def acc_work_request_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Начать отправку запроса менеджеру счёта."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    manager_id = inv.get("created_by")
    if not manager_id:
        await cb.message.answer("⚠️ У счёта нет привязанного менеджера.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(AccRequestToManagerSG.text)
    await state.update_data(
        invoice_id=invoice_id,
        manager_id=int(manager_id),
        attachments=[],
    )

    num = inv.get("invoice_number") or f"#{invoice_id}"
    mgr_label = await get_initiator_label(db, int(manager_id))
    await cb.message.answer(  # type: ignore[union-attr]
        f"📨 <b>Запрос менеджеру</b>\n"
        f"Счёт: №{num}\n"
        f"Менеджер: {mgr_label}\n\n"
        "Введите текст запроса:"
    )


@router.message(AccRequestToManagerSG.text)
async def acc_work_request_text(message: Message, state: FSMContext) -> None:
    """Получить текст запроса."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите текст запроса:")
        return
    await state.update_data(request_text=text)
    await state.set_state(AccRequestToManagerSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить запрос", callback_data="acc_req:send")
    b.button(text="❌ Отмена", callback_data="acc_req:cancel")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы (необязательно) или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(AccRequestToManagerSG.attachments)
async def acc_work_request_attach(message: Message, state: FSMContext) -> None:
    """Получить вложения."""
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
    elif message.video:
        attachments.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("📎 Прикрепите файл/фото или нажмите кнопку отправки.")
        return

    await state.update_data(attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Отправить ({len(attachments)} файл.)", callback_data="acc_req:send")
    b.button(text="❌ Отмена", callback_data="acc_req:cancel")
    b.adjust(1)
    await message.answer(
        f"📎 Файлов: <b>{len(attachments)}</b>. Ещё файлы или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "acc_req:send")
async def acc_work_request_send(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Отправить запрос менеджеру."""
    await cb.answer()
    data = await state.get_data()
    await state.clear()

    invoice_id = data.get("invoice_id")
    manager_id = data.get("manager_id")
    request_text = data.get("request_text", "")
    attachments: list[dict[str, Any]] = data.get("attachments", [])

    if not manager_id:
        await cb.message.answer("⚠️ Менеджер не найден.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(invoice_id) if invoice_id else None
    num = (inv.get("invoice_number") if inv else None) or f"#{invoice_id}"

    from ..enums import TaskType
    from ..utils import utcnow, to_iso
    from datetime import timedelta

    sender_id = cb.from_user.id if cb.from_user else 0
    task = await db.create_task(
        project_id=inv.get("project_id") if inv else None,
        type_=TaskType.EDO_REQUEST,
        status=TaskStatus.OPEN,
        created_by=sender_id,
        assigned_to=int(manager_id),
        due_at_iso=to_iso(utcnow() + timedelta(hours=24)),
        payload={
            "invoice_id": invoice_id,
            "invoice_number": num,
            "request_text": request_text,
            "sender_id": sender_id,
            "source": "accounting_request",
            "assigned_role": inv.get("creator_role") if inv else None,
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

    initiator = await get_initiator_label(db, sender_id)
    mgr_text = (
        f"📨 <b>Запрос от бухгалтерии</b>\n"
        f"👤 От: {initiator}\n"
        f"📄 Счёт: №{num}\n"
    )
    if inv and inv.get("object_address"):
        mgr_text += f"📍 {inv['object_address'][:50]}\n"
    mgr_text += f"\n💬 {request_text}\n"
    if attachments:
        mgr_text += f"\n📎 Вложений: {len(attachments)}"

    from ..keyboards import task_actions_kb
    await notifier.safe_send(int(manager_id), mgr_text, reply_markup=task_actions_kb(task))
    for a in attachments:
        try:
            if a.get("file_type") == "document":
                await notifier.bot.send_document(int(manager_id), a["file_id"])
            elif a.get("file_type") == "photo":
                await notifier.bot.send_photo(int(manager_id), a["file_id"])
        except Exception:
            log.warning("Failed to send attachment to manager %s", manager_id, exc_info=True)

    u = cb.from_user
    role, isolated_role = await _current_menu(db, u.id) if u else (Role.ACCOUNTING, False)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос отправлен менеджеру (счёт №{num}).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()) if u else False,
                unread=await db.count_unread_tasks(u.id) if u else 0,
                isolated_role=isolated_role,
            ),
        ),
    )


@router.callback_query(F.data == "acc_req:cancel")
async def acc_work_request_cancel(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    await cb.answer("❌ Отменено")
    await state.clear()
    u = cb.from_user
    role, isolated_role = await _current_menu(db, u.id) if u else (Role.ACCOUNTING, False)
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ Запрос отменён.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()) if u else False,
                unread=await db.count_unread_tasks(u.id) if u else 0,
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# ЗАКРЫТЫЕ СЧЕТА (list ended invoices)
# =====================================================================

@router.message(F.text == ACC_BTN_INVOICE_END)
async def acc_invoice_end(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.ENDED, limit=50, only_regular=True)
    if not invoices:
        await answer_service(message, "🏁 Нет закрытых счетов.", delay_seconds=60)
        return

    # Сводная карточка
    summary = _build_acc_summary(invoices, title="🏁 Закрытые Счета — сводка")
    await message.answer(summary)

    # Inline-кнопки счетов
    b = InlineKeyboardBuilder()
    for inv in invoices[:20]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        try:
            amt = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amt = ""
        b.button(
            text=f"🏁 {num} · {amt}",
            callback_data=f"acc_work:card:{inv['id']}",
        )
    b.adjust(1)
    await message.answer(
        "Нажмите на счёт для просмотра:",
        reply_markup=b.as_markup(),
    )

    footer = InlineKeyboardBuilder()
    footer.button(text="⬅️ Назад", callback_data="nav:home")
    footer.adjust(1)
    await message.answer("—", reply_markup=footer.as_markup())


# =====================================================================
# НАЙТИ СЧЕТ № — обрабатывается в manager_new.py (принимает Role.ACCOUNTING)
# =====================================================================


# =====================================================================
# EDO RESPONSE (бухгалтер отвечает на ЭДО-запрос)
# =====================================================================

@router.callback_query(F.data.regexp(r"^edo_respond:\d+$"))
async def edo_respond_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Accountant starts responding to an EDO request."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(EdoResponseSG.response_type)
    await state.update_data(task_id=task_id, attachments=[])

    b = InlineKeyboardBuilder()
    b.button(text="✅ Подписано", callback_data="edo_resp_type:signed")
    b.button(text="⏳ Ожидание", callback_data="edo_resp_type:waiting")
    b.button(text="📨 Запрос документов", callback_data="edo_resp_type:docs_needed")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "📄 <b>Ответ на ЭДО-запрос</b>\n\n"
        "Выберите статус:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("edo_resp_type:"))
async def edo_respond_type(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    resp_type = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(response_type=resp_type)
    await state.set_state(EdoResponseSG.comment)
    await cb.message.answer(  # type: ignore[union-attr]
        "Добавьте <b>комментарий</b> (или «—» для пропуска):"
    )


@router.message(EdoResponseSG.comment)
async def edo_respond_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""
    await state.update_data(comment=comment)
    await state.set_state(EdoResponseSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="edo_respond:send")
    b.button(text="⏭ Без вложений", callback_data="edo_respond:send")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(EdoResponseSG.attachments)
async def edo_respond_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.video:
        attachments.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "edo_respond:send")
async def edo_respond_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    task_id = data["task_id"]
    response_type = data["response_type"]
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])

    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    payload = json.loads(task.get("payload_json", "{}"))
    edo_id = payload.get("edo_id")
    requester_id = payload.get("requester_id")
    invoice_number = payload.get("invoice_number", "")
    edo_type = payload.get("edo_type", "")

    # Complete EDO request
    if edo_id:
        await db.complete_edo_request(
            edo_id=edo_id,
            response_type=response_type,
            responder_id=u.id,
            response_comment=comment,
            response_attachments_json=json.dumps(attachments, ensure_ascii=False) if attachments else None,
        )

    # Mark task as done
    await db.update_task_status(task_id, TaskStatus.DONE)

    resp_label = {
        "signed": "✅ Подписано",
        "waiting": "⏳ Ожидание",
        "docs_needed": "📨 Запрос документов",
    }.get(response_type, response_type)

    # Время обработки
    processing_time_str = ""
    if edo_id:
        edo_rec = await db.get_edo_request(edo_id)
        if edo_rec and edo_rec.get("processing_time_minutes") is not None:
            mins = edo_rec["processing_time_minutes"]
            if mins < 60:
                processing_time_str = f"⏱ Время обработки: {mins} мин.\n"
            else:
                h, m = divmod(mins, 60)
                processing_time_str = f"⏱ Время обработки: {h}ч {m}мин.\n"

    # Notify requester
    if requester_id:
        initiator = await get_initiator_label(db, u.id)
        msg = (
            f"📄 <b>Ответ бухгалтерии на ЭДО</b>\n"
            f"👤 От: {initiator}\n\n"
            f"Статус: {resp_label}\n"
        )
        if invoice_number:
            msg += f"Счёт №: <code>{invoice_number}</code>\n"
        if comment:
            msg += f"Комментарий: {comment}\n"
        if processing_time_str:
            msg += processing_time_str

        await notifier.safe_send(int(requester_id), msg)
        for a in attachments:
            await notifier.safe_send_media(int(requester_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    # Update invoice EDO flags so downstream close conditions and originals flow stay consistent.
    if response_type == "signed" and invoice_number:
        inv = await db.get_invoice_by_number(invoice_number)
        if inv:
            if edo_type == "sign_closing":
                await db.set_invoice_edo_signed(inv["id"], True)
            elif edo_type == "sign_invoice":
                await db.update_invoice(inv["id"], docs_edo_signed=1)

            if edo_type in {"sign_closing", "sign_invoice"}:
                signed_label = (
                    "Закрывающие по ЭДО подписаны"
                    if edo_type == "sign_closing"
                    else "Первичные документы по ЭДО подписаны"
                )
                notify_msg = f"✅ <b>{signed_label}</b>\n\nСчёт №: <code>{invoice_number}</code>"
                gd_id = await resolve_default_assignee(db, config, Role.GD)
                rp_id = await resolve_default_assignee(db, config, Role.RP)
                seen_targets: set[int] = set()
                for target in [gd_id, rp_id, requester_id]:
                    if target and int(target) not in seen_targets:
                        seen_targets.add(int(target))
                        await notifier.safe_send(int(target), notify_msg)

    role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Ответ отправлен ({resp_label}).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )
