from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import ProjectCb, RpSalaryCb, RpSalaryTaskCb, TaskCb
from ..config import Config
from ..db import Database, OkladAlreadyPaidError
from ..enums import Role, TaskStatus, TaskType
from ..integrations.minio_storage import MinioStorage
from ..keyboards import GD_BTN_INVOICE_END_GD, GD_BTN_SUPPLIER_PAY, main_menu, projects_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..states import GdZpInstAdjustSG, GdZpPaymentSG, InvoicePaymentSG, RpSalaryPaySG, RpSalaryRejectSG, SupplierPaymentSG
from ..utils import answer_service, fmt_project_card, format_card_section, format_plan_fact_card, format_rp_oklad_lines, parse_amount, private_only_reply_markup, refresh_recipient_keyboard, try_json_loads
from ._mirror import collect_attachment
from .auth import require_role_callback, require_role_message
from .money_guard import money_confirm_guard

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

GD_ACCESS_ROLES = [Role.GD, Role.TD]

# B5 TZ v8: фиксированный месячный оклад РП. Изменяется только правкой кода.
RP_SALARY_MONTHLY = 66_000


def _fmt_rub_td(value: float | int | None) -> str:
    """5500 → '5 500'. Без знака валюты."""
    return f"{float(value or 0):,.0f}".replace(",", " ")


# ==================== СЧЁТ END (двойной функционал: статистика + задачи) ====================

@router.message(F.text.startswith(GD_BTN_INVOICE_END_GD))
async def gd_invoice_end_combined(message: Message, db: Database) -> None:
    """Show two sub-options: ended stats and active tasks."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]

    # Counts for badges (include credit invoices in the END view)
    n_ended = await db.count_ended_invoices(include_credit=True)
    tasks_pc = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.PAYMENT_CONFIRM)
    tasks_ie = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.INVOICE_END_REQUEST)
    n_tasks = len(tasks_pc) + len(tasks_ie)

    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(
        text=f"📊 Счета end: {n_ended}",
        callback_data="gd_end:stats",
    )])
    rows.append([InlineKeyboardButton(
        text=f"📋 Задачи Счёт End: {n_tasks}",
        callback_data="gd_end:tasks",
    )])
    rows.append([InlineKeyboardButton(text="📊 Статистика по лидам", callback_data="gd_lead_stats")])
    rows.append([InlineKeyboardButton(text="🔀 Расхождения РП", callback_data="gd_discrepancy")])
    rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="gd_end:close")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await message.answer(
        f"🏁 <b>Счёт END</b>\n\n"
        f"📊 Закрытых счетов: <b>{n_ended}</b>\n"
        f"📋 Задач в работе: <b>{n_tasks}</b>",
        reply_markup=kb,
    )


async def _render_invoice_end_menu(cb: CallbackQuery, db: Database) -> None:
    """Render root «Счёт END» menu in place of the current callback message."""
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    user_id = cb.from_user.id
    n_ended = await db.count_ended_invoices(include_credit=True)
    tasks_pc = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.PAYMENT_CONFIRM)
    tasks_ie = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.INVOICE_END_REQUEST)
    n_tasks = len(tasks_pc) + len(tasks_ie)
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=f"📊 Счета end: {n_ended}", callback_data="gd_end:stats")],
        [InlineKeyboardButton(text=f"📋 Задачи Счёт End: {n_tasks}", callback_data="gd_end:tasks")],
        [InlineKeyboardButton(text="📊 Статистика по лидам", callback_data="gd_lead_stats")],
        [InlineKeyboardButton(text="🔀 Расхождения РП", callback_data="gd_discrepancy")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="gd_end:close")],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    text = (
        f"🏁 <b>Счёт END</b>\n\n"
        f"📊 Закрытых счетов: <b>{n_ended}</b>\n"
        f"📋 Задач в работе: <b>{n_tasks}</b>"
    )
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data == "gd_end:menu")
async def gd_end_menu(cb: CallbackQuery, db: Database) -> None:
    """Return to root «Счёт END» menu."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    await _render_invoice_end_menu(cb, db)


@router.callback_query(F.data == "gd_end:close")
async def gd_end_close(cb: CallbackQuery, db: Database) -> None:
    """Close inline menu by removing its keyboard."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer("Закрыто")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass


_MONTH_NAMES = {
    "01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
    "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
    "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь",
}


@router.callback_query(F.data == "gd_end:stats")
async def gd_end_stats(cb: CallbackQuery, db: Database) -> None:
    """Show monthly ended summary + month buttons. Includes credit invoices."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    invoices = await db.list_invoices(statuses=["ended", "credit"], limit=200)
    # Только счета, реально получившие «статус Счёт END»:
    # обычный ended ИЛИ credit с пройденным invoice_end этапом.
    invoices = [
        inv for inv in invoices
        if inv.get("status") == "ended"
        or (inv.get("status") == "credit" and inv.get("montazh_stage") == "invoice_end")
    ]
    if not invoices:
        await cb.answer("Закрытых счетов нет", show_alert=True)
        return
    await cb.answer()
    # Monthly summary card (ended + закрытые кредитные, по требованию ГД)
    from ..utils import format_monthly_ended_summary
    months_data = await db.get_ended_monthly_summary()
    # ТЗ 2026-05-19 блок B: pre-compute «План» для ЗП менеджера и Налогов.
    # SQL get_ended_monthly_summary не агрегирует план — собираем через
    # get_plan_fact_card по invoice_ids месяца.
    for m in months_data:
        est_mgr = 0.0
        est_tax = 0.0
        cur_m = await db.conn.execute(
            "SELECT id FROM invoices "
            "WHERE strftime('%Y-%m', COALESCE(receipt_date, created_at)) = ? "
            "  AND (status = 'ended' "
            "       OR (status = 'credit' AND montazh_stage = 'invoice_end')) "
            "  AND parent_invoice_id IS NULL",
            (m["month"],),
        )
        inv_ids = [r[0] for r in await cur_m.fetchall()]
        for inv_id in inv_ids:
            pf = await db.get_plan_fact_card(inv_id)
            est_mgr += float(pf.get("manager_zp") or 0)
            amt = float(pf.get("amount") or 0)
            nv = float(pf.get("net_vat") or 0)
            et = float(pf.get("estimated_total_cost") or 0)
            if amt > 0:
                est_tax += nv + max(0.0, (amt - et - nv) * 0.20)
        m["est_manager_zp"] = est_mgr
        m["est_taxes"] = est_tax
    summary_text = format_monthly_ended_summary(months_data)
    await cb.message.answer(summary_text)  # type: ignore[union-attr]
    # Group invoices by month (credit учитываются наравне с ended)
    from collections import OrderedDict
    by_month: OrderedDict[str, int] = OrderedDict()
    for inv in invoices:
        rd = inv.get("receipt_date") or inv.get("created_at") or ""
        ym = str(rd)[:7]  # "2026-03"
        by_month[ym] = by_month.get(ym, 0) + 1
    b = InlineKeyboardBuilder()
    # Кнопки месяцев: от старого к новому (ASC) — январь сверху, текущий снизу.
    for ym, cnt in sorted(by_month.items()):
        mm = ym[5:7] if len(ym) >= 7 else "?"
        name = _MONTH_NAMES.get(mm, ym)
        year = ym[:4] if len(ym) >= 4 else ""
        b.button(text=f"{name} {year}: {cnt}", callback_data=f"gd_end:month:{ym}")
    b.button(text="⬅️ К меню Счёт END", callback_data="gd_end:menu")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"<b>✅ Счета end</b> ({len(invoices)})\n\nВыберите месяц:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("gd_end:month:"))
async def gd_end_month(cb: CallbackQuery, db: Database) -> None:
    """Show ended (incl. credit) invoices for a specific month."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    ym = cb.data.split(":", 2)[2]  # "2026-03"  # type: ignore[union-attr]
    invoices = await db.list_invoices(statuses=["ended", "credit"], limit=200)
    filtered = [
        inv for inv in invoices
        if (inv.get("receipt_date") or inv.get("created_at") or "")[:7] == ym
        and (inv.get("status") == "ended"
             or (inv.get("status") == "credit" and inv.get("montazh_stage") == "invoice_end"))
    ]
    if not filtered:
        await cb.answer("Нет счетов за этот месяц", show_alert=True)
        return
    await cb.answer()
    mm = ym[5:7] if len(ym) >= 7 else "?"
    name = _MONTH_NAMES.get(mm, ym)
    year = ym[:4] if len(ym) >= 4 else ""
    b = InlineKeyboardBuilder()
    for inv in filtered:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = inv.get("object_address") or ""
        label = f"{num} — {addr}"[:60]
        b.button(text=label, callback_data=f"gd_work:view:{inv['id']}")
    b.button(text="⬅️ Назад к месяцам", callback_data="gd_end:stats")
    b.button(text="🏁 К меню Счёт END", callback_data="gd_end:menu")
    b.adjust(1)
    await cb.message.edit_text(  # type: ignore[union-attr]
        f"<b>✅ {name} {year}</b> ({len(filtered)})\n\nВыберите счёт:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "gd_end:tasks")
async def gd_end_tasks(cb: CallbackQuery, db: Database) -> None:
    """Show PAYMENT_CONFIRM + INVOICE_END_REQUEST tasks."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    user_id = cb.from_user.id
    tasks_pc = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.PAYMENT_CONFIRM)
    tasks_ie = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.INVOICE_END_REQUEST)
    tasks = tasks_pc + tasks_ie
    tasks.sort(key=lambda t: t.get("created_at") or "", reverse=True)
    if not tasks:
        await cb.answer("Нет задач «Счёт END» и подтверждений оплат.", show_alert=True)
        return
    await cb.answer()
    n_pc = len(tasks_pc)
    n_ie = len(tasks_ie)
    parts = []
    if n_pc:
        parts.append(f"💰 Подтв.оплат: {n_pc}")
    if n_ie:
        parts.append(f"🏁 Счёт End: {n_ie}")
    summary = " | ".join(parts)

    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    rows: list[list[InlineKeyboardButton]] = []
    for t in tasks:
        tid = int(t.get("id", 0) or 0)
        ttype = t.get("type", "")
        prefix = "💰" if ttype == TaskType.PAYMENT_CONFIRM else "🏁"
        payload = t.get("payload") or try_json_loads(t.get("payload_json")) or {}
        label = payload.get("invoice_number") or payload.get("supplier") or f"#{tid}"
        rows.append([InlineKeyboardButton(
            text=f"{prefix} {label}",
            callback_data=TaskCb(task_id=tid, action="open").pack(),
        )])
    rows.append([InlineKeyboardButton(text="⬅️ К меню Счёт END", callback_data="gd_end:menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Задачи Счёт END</b> ({len(tasks)})\n{summary}\n\n"
        "Выберите задачу:",
        reply_markup=kb,
    )


@router.callback_query(F.data == "gd_lead_stats")
async def gd_lead_stats_handler(cb: CallbackQuery, db: Database, config: Config) -> None:
    """Show lead statistics for GD: менеджер (RP-приоритет) + воронка РП + источники."""
    await cb.answer()
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    from ..utils import format_lead_stats_card
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К меню Счёт END", callback_data="gd_end:menu")],
    ])
    user_map = getattr(config, "amocrm_user_map", {}) or {}
    stats = await db.get_lead_stats_v2(user_map)
    if not (stats.get("totals") or {}).get("total"):
        await cb.message.answer("📊 Лидов пока нет.", reply_markup=back_kb)  # type: ignore[union-attr]
        return
    await cb.message.answer(format_lead_stats_card(stats), reply_markup=back_kb)  # type: ignore[union-attr]


@router.callback_query(F.data == "gd_discrepancy")
async def gd_discrepancy_handler(cb: CallbackQuery, db: Database) -> None:
    """Отчёт расхождений «РП ↔ счета» для ГД/ТД (номера/суммы → роль-гейт gd/td)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    from ..utils import format_discrepancy_card
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К меню Счёт END", callback_data="gd_end:menu")],
    ])
    disc = await db.get_rp_discrepancies()
    await cb.message.answer(format_discrepancy_card(disc), reply_markup=back_kb)  # type: ignore[union-attr]


# ==================== ОПЛАТА ПОСТАВЩИКУ — ДАШБОРД + ЗП ====================

@router.message(F.text.startswith(GD_BTN_SUPPLIER_PAY))
async def gd_supplier_pay_dashboard(message: Message, state: FSMContext, db: Database) -> None:
    """Dashboard: папки ЗП + оплата поставщику."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    await state.clear()

    zp_installer = await db.list_pending_zp_requests("installer")
    zp_zamery = await db.list_pending_zp_requests("zamery")
    zp_manager = await db.list_pending_zp_requests("manager")
    total_zp = len(zp_installer) + len(zp_zamery) + len(zp_manager)

    # Считаем задачи оплаты поставщику
    user_id = message.from_user.id  # type: ignore[union-attr]
    invoice_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.INVOICE_PAYMENT,
        limit=100,
    )
    supplier_tasks = await db.list_tasks_open_by_types(["supplier_payment"])
    n_pay = len(invoice_tasks) + len(supplier_tasks)

    lines = ["💸 <b>Оплата поставщику</b>\n"]
    b = InlineKeyboardBuilder()

    # Папка: Монтаж ЗП
    b.button(
        text=f"🔧 Монтаж ЗП ({len(zp_installer)})",
        callback_data="gd_pay:folder:montazh_zp",
    )
    # Папка: Прочие ЗП (замерщик + менеджер + cart-style ЗП РП).
    # Видна всегда — ГД может инициировать оклад РП без pending-запроса.
    zp_rp_tasks_dash = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.ZP_RP,
        limit=50,
    )
    n_other_zp = len(zp_zamery) + len(zp_manager) + len(zp_rp_tasks_dash)
    b.button(
        text=f"💼 Прочие ЗП ({n_other_zp})",
        callback_data="gd_pay:folder:other_zp",
    )
    # Финансы (ГД-инициированный депозит/аванс).
    b.button(text="💸 Финансы", callback_data="gd_deposit:start")
    # Папка: Оплата поставщику
    b.button(
        text=f"💸 Оплата поставщику ({n_pay})",
        callback_data="gd_pay:folder:supplier",
    )
    b.adjust(1)

    if total_zp:
        lines.append(f"🔧 Монтаж ЗП: {len(zp_installer)}")
    if n_other_zp:
        lines.append(f"💼 Прочие ЗП: {n_other_zp}")
    if n_pay:
        lines.append(f"💸 Оплата: {n_pay}")
    if not total_zp and not n_pay:
        lines.append("✅ Нет входящих запросов")

    await message.answer("\n".join(lines), reply_markup=b.as_markup())


def _montazh_zp_list_card(
    inv: dict, adv_offset: float = 0.0, adv_date: str | None = None,
) -> str:
    """Карточка задачи «Монтаж ЗП» для списка ГД (эталон-v2 движок).

    ТЗ user 08.06: вместо плоской кнопки «адрес — сумма» — карточка
    установленного дизайна. Поля и источники (feedback_use_only_specified_sources):
      • Счёт           = invoice_number
      • Адрес          = rp_start_card._addr_cell(object_address) — как в «этапах работ»:
                          Москва → сокр. улица, НЕ Москва → город (единое правило
                          карточек ГД, owner 30.07; был _street — город терялся)
      • Долг по счёту  = outstanding_debt
      • Сумма счёта    = amount
      • Расчётная уст. = estimated_installation (сырое «Установка» из сметы ОП)
      • Согласовано     = montazh_agreed_amount (Invoices BJ) — финальная сумма ЗП монтажа,
                          введённая монтажником/РП (для б/н +10% уже включён). Показывается
                          КАК ЕСТЬ, без ×1,35: монтажник вносит уже финальную стоимость,
                          надбавка «официальной выплаты» на согласование не нужна (owner 23.07).
                          Кредит так же (owner 21.07 — «кредит как есть»).
      • Аванс          = зачтённый аванс монтажника по счёту (CG-конвенция: б/н ×1.10,
                          кредит как есть) + дата зачёта, МСК (ТЗ user 08.06pm)
      • Поступила      = zp_installer_requested_at (дата прихода задачи, МСК)
      • к выплате (footer) = BJ-остаток = Согласовано − max(AN, аванс) (ТЗ user 08.06pm)
    """
    from ..rp_start_card import _addr_cell
    from .installer_new import _is_credit

    def _f(n: Any) -> str:
        if n is None:
            return "—"
        try:
            return f"{float(n):,.0f}₽".replace(",", " ")
        except (ValueError, TypeError):
            return "—"

    def _agreed_str(n: Any) -> str:
        # «Согласовано» = montazh_agreed_amount КАК ЕСТЬ — финальная сумма ЗП монтажа,
        # введённая монтажником/РП (для б/н +10% уже включён в agreed). Без ×1,35:
        # монтажник вносит уже финальную стоимость, надбавка «официальной выплаты»
        # на согласование ошибочна (owner 23.07). Кредит и б/н — одинаково как есть
        # (кредит исправлен ранее, owner 21.07). [[feedback_installer_advance_spend_scope]]
        if not n:
            return "—"
        try:
            return _f(float(n))
        except (ValueError, TypeError):
            return "—"

    def _fmt_dt(raw: Any) -> str:
        if not raw:
            return "—"
        try:
            d = datetime.fromisoformat(str(raw))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            if ZoneInfo is not None:
                d = d.astimezone(ZoneInfo("Europe/Moscow"))
            return d.strftime("%d.%m %H:%M")
        except (ValueError, TypeError):
            return "—"

    num = inv.get("invoice_number") or f"#{inv.get('id', '?')}"
    street = _addr_cell(inv.get("object_address"), 22)

    # Аванс монтажника (CG-конвенция: б/н ×1.10, кредит как есть) + к выплате = BJ-остаток.
    # BJ = Согласовано − Выплачено; для pending-запросов Выплачено = max(AN, аванс)
    # (бот ещё не платил) — совпадает с листом Invoices.BJ. [[feedback_bs_immutable]]
    adv_cg = adv_offset * 1.10 if (adv_offset > 0 and not _is_credit(inv)) else adv_offset
    _agreed = float(inv.get("montazh_agreed_amount") or 0)
    _an = float(inv.get("montazh_fact_op") or 0)
    # Объединение платежей (owner 15.07): Согласовано включает ЗП, выплаченную ПРОШЛЫМ
    # монтажным группам. Без вычета footer показал бы «к выплате 220 000» рядом с заявкой
    # на 130 000 — а под карточкой живёт «✏️ Изменить сумму», и ГД, сверяя одно с другим,
    # переплатил бы в один тап. Форма — как в _montazh_money_state (rp_new.py): AN
    # накапливает ВСЕ ноги, поэтому с paid_prev не суммируется, а конкурирует по максимуму
    # (иначе старая нога вычлась бы дважды). paid_prev=0 → формула ровно прежняя.
    _paid_prev = float(inv.get("montazh_paid_prev") or 0)
    _bj = (
        max(0.0, _agreed - max(_an, _paid_prev + adv_cg)) if _agreed > 0 else 0.0
    )
    adv_str = f"{_f(adv_cg)} ({_fmt_dt(adv_date)})" if adv_offset > 0 else "—"
    bj_str = _f(_bj) if _agreed > 0 else "—"

    items: list[tuple[str, str]] = [
        ("Счёт", f"№{num}"),
        ("Адрес", street),
        ("Долг по счёту", _f(inv.get("outstanding_debt"))),
        ("Сумма счёта", _f(inv.get("amount"))),
        ("Расчётная уст.", _f(inv.get("estimated_installation"))),
        ("Согласовано", _agreed_str(inv.get("montazh_agreed_amount"))),
        ("Аванс", adv_str),
        ("Поступила", _fmt_dt(inv.get("zp_installer_requested_at"))),
    ]
    # Динамическая ширина: правый край значений выравнивается по самой длинной
    # строке (вкл. footer «к выплате»), чтобы столбцы визуально сходились
    # (user 26.06). INDENT в format_card_section = 3 символа; +1 чтобы у самой
    # длинной строки тоже был зазор pad_n=1 (иначе она вылезает на 1 правее).
    _rows = items + [("к выплате", bj_str)]
    _w = max(30, max(3 + len(lbl) + len(val) for lbl, val in _rows) + 1)
    return format_card_section(
        emoji="🔧",
        title="ЗП монтажника",
        items=items,
        width=_w,
        compact=False,
        footer=("к выплате", bj_str),
    )


@router.callback_query(F.data == "gd_pay:folder:montazh_zp")
async def gd_pay_montazh_zp(cb: CallbackQuery, db: Database) -> None:
    """Папка: Монтаж ЗП — карточки запросов от монтажника (эталон-v2, ТЗ 08.06).

    Вместо плоского списка кнопок «адрес — сумма» каждая задача = карточка
    установленного дизайна с 7 полями. Кнопка «👁 Открыть» и детальный
    план/факт-разворот убраны по ТЗ user 08.06 — показываем только карточку.
    """
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    zp_installer = await db.list_pending_zp_requests("installer")
    if not zp_installer:
        await cb.message.answer("🔧 <b>Монтаж ЗП</b>\n\nНет запросов ✅")  # type: ignore[union-attr]
        return

    # Заголовок секции + «Назад»
    hb = InlineKeyboardBuilder()
    hb.button(text="⬅️ Назад", callback_data="gd_pay:back")
    await cb.message.answer(  # type: ignore[union-attr]
        f"🔧 <b>Монтаж ЗП</b> ({len(zp_installer)})",
        reply_markup=hb.as_markup(),
    )

    # Каждая задача — карточка эталон-v2 (детальный «Открыть»/план-факт убраны по
    # ТЗ user 08.06) + кнопки действий ПРЯМО под карточкой (user 11.06: «верни их
    # на место»). Набор зеркалит прежний view, удалённый 09.06 («rm-gdzp-view»):
    # pending → ЗП ОК / Отклонить / Изменить; approved → Отправить платёжку /
    # Изменить. Обработчики gdzp_inst:ok|no|edit|pdf живы (td.py). Промежуточного
    # «Открыть» нет — действия доступны сразу (user 08.06 не хотел лишний шаг).
    # Аванс + дата зачёта — для строки «Аванс» и расчёта «к выплате» (BJ-остаток).
    for inv in zp_installer:
        # Аванс ТЕКУЩЕЙ группы: аванс прошлой уже внутри montazh_paid_prev (объединение
        # платежей, owner 15.07) — иначе он вычелся бы дважды.
        _adv = max(
            0.0,
            await db.get_installer_advance_for_invoice(int(inv["id"]))
            - float(inv.get("montazh_adv_prev") or 0),
        )
        _adv_dt = await db.get_installer_advance_date_for_invoice(int(inv["id"]))
        invoice_id = int(inv["id"])
        ab = InlineKeyboardBuilder()
        if (inv.get("zp_installer_status") or "") == "approved":
            ab.button(text="📎 Отправить платёжку", callback_data=f"gdzp_inst:pdf:{invoice_id}")
            # owner 15.07: платёжка для ГД ВСЕГДА опциональна — платёж вносится при
            # закрытии задачи, а не по факту PDF.
            ab.button(text="✅ Выплачено без платёжки", callback_data=f"gdzp_inst:nopdf:{invoice_id}")
            ab.button(text="✏️ Изменить сумму", callback_data=f"gdzp_inst:edit:{invoice_id}")
            ab.adjust(1)
        else:
            ab.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
            ab.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
            ab.button(text="✏️ Изменить сумму", callback_data=f"gdzp_inst:edit:{invoice_id}")
            ab.adjust(2, 1)
        await cb.message.answer(  # type: ignore[union-attr]
            _montazh_zp_list_card(inv, _adv, _adv_dt),
            reply_markup=ab.as_markup(),
        )


@router.callback_query(F.data == "gd_pay:folder:other_zp")
async def gd_pay_other_zp(cb: CallbackQuery, db: Database) -> None:
    """Папка: Прочие ЗП — замерщик + менеджер."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    zp_zamery = await db.list_pending_zp_requests("zamery")
    zp_manager = await db.list_pending_zp_requests("manager")

    # B2 cart-rework: pending групповые запросы ЗП РП (один task per ГД)
    user_id = cb.from_user.id if cb.from_user else 0
    zp_rp_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.ZP_RP,
        limit=50,
    )
    # B5 v2 request-based TZ 27.05: pending запросы оклада 60К от РП
    rp_salary_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.RP_SALARY,
        limit=20,
    )

    if not zp_zamery and not zp_manager and not zp_rp_tasks and not rp_salary_tasks:
        await cb.message.answer("💼 <b>Прочие ЗП</b>\n\nНет запросов ✅")  # type: ignore[union-attr]
        return

    # user 04.07: один view-элемент → сразу его карточка (без пикера «Выберите
    # для просмотра»). Оклад РП (rp_salary) исключён — owner: оставить кнопкой,
    # т.к. его открытие сразу втягивает в FSM подтверждения выплаты.
    _single = len(zp_zamery) + len(zp_manager) + len(zp_rp_tasks) + len(rp_salary_tasks) == 1
    if _single and not rp_salary_tasks:
        if zp_zamery:
            await gd_zp_zamery_view(cb, db, invoice_id=int(zp_zamery[0]["id"]))
        elif zp_manager:
            await gd_zp_manager_view(cb, db, invoice_id=int(zp_manager[0]["id"]))
        elif zp_rp_tasks:
            await gd_pay_rpzp_open(cb, db, task_id=int(zp_rp_tasks[0]["id"]))
        return

    b = InlineKeyboardBuilder()
    for inv in zp_zamery:
        amt = inv.get("zp_zamery_total") or 0
        b.button(
            text=f"📐 №{inv['invoice_number'] or '—'} — {amt:,.0f}₽",
            callback_data=f"gdzp_zam:view:{inv['id']}",
        )
    for inv in zp_manager:
        amt = inv.get("zp_manager_amount") or 0
        b.button(
            text=f"💼 №{inv['invoice_number'] or '—'} — {amt:,.0f}₽",
            callback_data=f"gdzp_mgr:view:{inv['id']}",
        )
    # B2 cart-rework: групповые ЗП РП (10% от прибыли, мульти-счёт)
    for t in zp_rp_tasks:
        try:
            payload = json.loads(t.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        total = float(payload.get("total") or 0)
        n_inv = len(payload.get("invoice_ids") or [])
        rp_name = payload.get("rp_name") or "РП"
        b.button(
            text=f"💰 ЗП РП {rp_name} — {_fmt_rub_td(total)}₽ ({n_inv} сч.)"[:60],
            callback_data=f"gd_pay:rpzp:open:{t['id']}",
        )
    # B5 v2 request-based TZ 27.05: task'и запроса оклада 60К — task_id-based кнопки
    for t in rp_salary_tasks:
        try:
            payload = json.loads(t.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        rp_name = payload.get("rp_name") or "РП"
        amount = float(payload.get("amount") or RP_SALARY_MONTHLY)
        month = payload.get("month") or ""
        # На кнопке — сумма К ВЫПЛАТЕ (оклад минус зачтённый аванс, ТЗ owner 31.07),
        # считаем живьём: у задач до правки в payload аванса нет.
        try:
            _rp_id = int(payload.get("rp_id") or 0)
            if _rp_id:
                amount = float((await db.get_rp_oklad_advance_offset(_rp_id))["payout"])
        except Exception:
            log.exception("gd_pay_rpzp: расчёт аванса РП не удался task=%s", t.get("id"))
        b.button(
            text=f"💼 Оклад {rp_name} — {_fmt_rub_td(amount)}₽ ({month})"[:60],
            callback_data=RpSalaryTaskCb(task_id=int(t["id"]), action="open").pack(),
        )
    b.button(text="⬅️ Назад", callback_data="gd_pay:back")
    b.adjust(1)

    total_cnt = len(zp_zamery) + len(zp_manager) + len(zp_rp_tasks) + len(rp_salary_tasks)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💼 <b>Прочие ЗП</b> ({total_cnt})\n\nВыберите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("gd_pay:rpzp:open:"))
async def gd_pay_rpzp_open(cb: CallbackQuery, db: Database, task_id: int | None = None) -> None:
    """B2 cart-rework: открыть карточку группового запроса ЗП РП."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    if task_id is None:
        task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return
    try:
        payload = json.loads(task.get("payload_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        payload = {}
    total = float(payload.get("total") or 0)
    rp_name = payload.get("rp_name") or "РП"
    invoices_info = payload.get("invoices") or []
    lines = [f"<pre>💰 <b>Запрос ЗП РП</b>"]
    lines.append(f"   От                    {rp_name}")
    for it in invoices_info:
        num = it.get("invoice_number") or "?"
        amt = float(it.get("amount") or 0)
        lines.append(f"   №{num:<18s} {_fmt_rub_td(amt):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub_td(total)} ₽")
    lines.append("</pre>")
    from ..callbacks import RpZpPayCb, RpZpRejectCb
    b = InlineKeyboardBuilder()
    if task.get("status") in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        b.button(text="✅ Выплатить", callback_data=RpZpPayCb(task_id=task_id).pack())
        b.button(text="❌ Отклонить", callback_data=RpZpRejectCb(task_id=task_id).pack())
        b.adjust(2)
    else:
        lines.append(f"\n<i>Статус: {task.get('status')}</i>")
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "gd_pay:folder:supplier")
async def gd_pay_supplier(cb: CallbackQuery, db: Database) -> None:
    """Папка: Оплата поставщику — только supplier_payment.

    B4 TZ 27.05: INVOICE_PAYMENT (счета от менеджеров/РП) убраны отсюда —
    они доступны через отдельную reply-кнопку «Счета на Оплату».
    """
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    supplier_tasks = await db.list_tasks_open_by_types(["supplier_payment"])
    all_tasks = list(supplier_tasks)

    if not all_tasks:
        await cb.message.answer("💸 <b>Оплата поставщику</b>\n\nНет запросов ✅")  # type: ignore[union-attr]
        return

    b = InlineKeyboardBuilder()
    for t in all_tasks[:20]:
        try:
            payload = json.loads(t.get("payload_json") or "{}") if t.get("payload_json") else {}
        except (json.JSONDecodeError, TypeError):
            payload = {}
        inv_num = payload.get("invoice_number") or ""
        amt = payload.get("amount") or ""
        label = f"💰 №{inv_num}" if inv_num else f"💰 #{t['id']}"
        if amt:
            try:
                label += f" — {float(amt):,.0f}₽"
            except (ValueError, TypeError):
                pass
        b.button(text=label[:60], callback_data=TaskCb(task_id=int(t["id"]), action="open").pack())
    b.button(text="➕ Новая оплата", callback_data="supplier_pay_start")
    b.button(text="⬅️ Назад", callback_data="gd_pay:back")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 <b>Оплата поставщику</b> ({len(all_tasks)})\n\nВыберите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "gd_pay:back")
async def gd_pay_back(cb: CallbackQuery, db: Database) -> None:
    """Назад к папкам оплат."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()

    zp_installer = await db.list_pending_zp_requests("installer")
    zp_zamery = await db.list_pending_zp_requests("zamery")
    zp_manager = await db.list_pending_zp_requests("manager")

    user_id = cb.from_user.id if cb.from_user else 0
    # B4 TZ 27.05: INVOICE_PAYMENT убраны — счета от менеджеров доступны через
    # reply-кнопку «Счета на Оплату» (GD_BTN_INVOICES).
    supplier_tasks = await db.list_tasks_open_by_types(["supplier_payment"])
    n_pay = len(supplier_tasks)
    # cart-style ЗП РП + B5 v2 оклад РП — учитываем в counter «💼 Прочие ЗП»
    zp_rp_tasks_back = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.ZP_RP,
        limit=50,
    )
    rp_salary_tasks_back = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.RP_SALARY,
        limit=20,
    )
    n_other_zp = len(zp_zamery) + len(zp_manager) + len(zp_rp_tasks_back) + len(rp_salary_tasks_back)

    # B3 TZ 27.05: badge «🔴N» вместо «(N)» — если есть pending запросы
    def _badge(n: int) -> str:
        return f" 🔴{n}" if n > 0 else ""

    b = InlineKeyboardBuilder()
    b.button(text=f"🔧 Монтаж ЗП{_badge(len(zp_installer))}", callback_data="gd_pay:folder:montazh_zp")
    # Видна всегда — ГД может инициировать оклад РП без pending-запроса
    b.button(text=f"💼 Прочие ЗП{_badge(n_other_zp)}", callback_data="gd_pay:folder:other_zp")
    b.button(text="💸 Финансы", callback_data="gd_deposit:start")
    b.button(text=f"💸 Оплата поставщику{_badge(n_pay)}", callback_data="gd_pay:folder:supplier")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "💸 <b>Оплата поставщику</b>\n\nВыберите раздел:",
        reply_markup=b.as_markup(),
    )


# --- GD ZP approve/reject handlers ---

@router.callback_query(F.data.startswith("gdzp_inst:ok:"))
async def gd_zp_installer_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Одобрять можно ТОЛЬКО активную заявку. Push-сообщение с этой кнопкой живёт в чате
    # ГД вечно, а заявку мог отозвать РП («🔁 Изменить Монтажников» → снятие запроса от
    # старой группы) — без гарда клик по старому сообщению воскрешал бы её со СТАРОЙ
    # zp_installer_amount (сеттер её не чистит) и жёг зачёт аванса. Тот же гард ловит
    # двойной клик: повторный approve прогонял apply_advance_offsets второй раз.
    # [[feedback_fsm_old_buttons_trap]], [[feedback_money_confirm_idempotent_gate]]
    if (inv.get("zp_installer_status") or "not_requested") != "requested":
        await cb.answer(
            "⚠️ Запрос ЗП не активен (отозван РП или уже обработан).", show_alert=True,
        )
        return
    await db.set_invoice_zp_installer_status(invoice_id, "approved")
    await integrations.sync_invoice_row(invoice_id)
    amt = float(inv.get("zp_installer_amount") or 0)
    actor_id = cb.from_user.id if cb.from_user else 0  # type: ignore[union-attr]
    # Часть 2 (2026-06-08): заявка-ОСТАТОК (zp_installer_remainder=1) — аванс уже
    # вычтен в сумме остатка → закрываем earmark БЕЗ повторного зачёта (бот платит
    # весь остаток amt). Старая семантика (бот платит всю согласованную) — прежний
    # auto-offset, где зачёт аванса уменьшает доплату.
    if inv.get("zp_installer_remainder"):
        closed = await db.close_open_advance_items_for_invoice(
            invoice_id, zp_id=invoice_id, actor_id=actor_id,
        )
        advance_offset = 0.0
        offset_remaining = amt
        if closed > 0:
            try:
                await integrations.sync_advances_journal()
            except Exception as e:
                log.warning("sync_advances_journal after zp approve (remainder) failed: %s", e)
    else:
        # ТЗ 2026-05-19 блок C: auto-offset открытых авансовых items этого счёта.
        offset_remaining = await db.apply_advance_offsets_on_zp_approve(
            invoice_id, zp_id=invoice_id, zp_amount=amt, actor_id=actor_id,
        )
        advance_offset = amt - offset_remaining
        if advance_offset > 0:
            try:
                await integrations.sync_advances_journal()
            except Exception as e:
                log.warning("sync_advances_journal after zp approve failed: %s", e)
    # Пересобрать строку счёта ПОСЛЕ зачёта аванса: offset проставил дату аванса (CH),
    # метрики DB–DF и возможный статус ЗП 'confirmed'; синк после set_invoice_zp_installer_status
    # был ДО зачёта, поэтому эти изменения иначе отстают в листе (особенно у б/н счетов).
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception as e:
        log.warning("sync_invoice_row after zp approve offset failed: %s", e)
    # НЕ закрываем задачу — она закроется после выплаты (платёжкой или без неё)
    b = InlineKeyboardBuilder()
    b.button(text="📎 Отправить платёжку", callback_data=f"gdzp_inst:pdf:{invoice_id}")
    b.button(text="✅ Выплачено без платёжки", callback_data=f"gdzp_inst:nopdf:{invoice_id}")
    b.adjust(1)
    extra = ""
    if advance_offset > 0:
        extra = (
            f"\n💸 Зачёт аванса: {advance_offset:,.0f}₽\n"
            f"🟢 К доплате: {offset_remaining:,.0f}₽"
        )
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП монтажника утверждена.\n"
        f"Счёт №{inv['invoice_number']}, сумма: {amt:,.0f}₽{extra}\n\n"
        f"📎 Прикрепите платёжку когда будет готова — или отметьте выплату без неё.",
        reply_markup=b.as_markup(),
    )
    # Notify installer — ЗП утверждена, ожидайте платёжку
    requested_by = inv.get("zp_installer_requested_by")
    if requested_by:
        inst_msg = (
            f"✅ <b>ЗП утверждена</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
            f"Сумма: {amt:,.0f}₽\n"
        )
        if advance_offset > 0:
            inst_msg += (
                f"💸 Зачёт аванса: {advance_offset:,.0f}₽\n"
                f"🟢 К доплате: {offset_remaining:,.0f}₽\n"
            )
        inst_msg += "⏳ Ожидайте платёжку."
        await notifier.safe_send(int(requested_by), inst_msg)
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))
    if cb.from_user:
        await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)


@router.callback_query(F.data.startswith("gdzp_inst:no:"))
async def gd_zp_installer_reject(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # ТЗ owner 16.07 (инцидент 26331-1НПН): данные в BJ — только при подтверждении ГД.
    # Отклонение ОТКАТЫВАЕТ Согласовано к выплаченному прошлым группам (paid_prev;
    # обычный счёт → 0) → BJ на листе очищается при sync. Иначе отклонённая сумма
    # оставалась в agreed и BJ показывал её как долг монтажнику.
    _paid_prev = float(inv.get("montazh_paid_prev") or 0)
    _agreed_old = float(inv.get("montazh_agreed_amount") or 0)
    if _agreed_old != _paid_prev:
        from datetime import datetime as _dt
        await db.conn.execute(
            "UPDATE invoices SET montazh_agreed_amount = ?, updated_at = ? WHERE id = ?",
            (_paid_prev, _dt.now().isoformat(), invoice_id),
        )
        await db.conn.commit()
        try:
            await db.audit(
                actor_id=cb.from_user.id if cb.from_user else None,
                action="montazh_zp_reject_rollback", entity="invoice",
                entity_id=str(invoice_id),
                payload={"agreed_old": _agreed_old, "agreed_new": _paid_prev},
            )
        except Exception:
            log.debug("zp reject rollback: audit failed inv=%s", invoice_id, exc_info=True)
    # amount=0 обязателен: сеттер при None сумму не трогает — stale zp_installer_amount
    # продолжил бы считаться «Выплачено» при следующих статусах.
    await db.set_invoice_zp_installer_status(invoice_id, "not_requested", amount=0)
    await integrations.sync_invoice_row(invoice_id)
    await _close_zp_tasks(db, invoice_id, TaskType.ZP_INSTALLER)
    await cb.message.answer(f"❌ ЗП монтажника по счёту №{inv['invoice_number']} отклонена.")  # type: ignore[union-attr]
    requested_by = inv.get("zp_installer_requested_by")
    if requested_by:
        await notifier.safe_send(
            int(requested_by),
            f"❌ <b>ЗП отклонена</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
            "Свяжитесь с ГД для уточнения.",
        )
        # Заявитель-РП (наёмный флоу): сумма откачена — сразу даём карточку
        # «💰 ЗП монтаж» с кнопкой, чтобы внести согласованную сумму заново
        # (иначе после отката у РП нет пути, кроме «Изменить Монтажников»).
        try:
            _ru = await db.get_user_optional(int(requested_by))
            if _ru and "rp" in str(getattr(_ru, "role", "") or "").split(","):
                from .rp_new import _build_montazh_zp_card  # lazy: circular import
                _built = await _build_montazh_zp_card(db, invoice_id)
                if _built:
                    await notifier.safe_send(
                        int(requested_by), _built[0], reply_markup=_built[1],
                    )
        except Exception:
            log.debug("zp reject: rp card failed inv=%s", invoice_id, exc_info=True)
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))


# ---------- ЗП монтажника: ГД меняет сумму ---------- #


@router.callback_query(F.data.startswith("gdzp_inst:edit:"))
async def gd_zp_installer_edit_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД нажал «✏️ Изменить сумму» — спросить новую сумму."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    cur_amt = float(inv.get("zp_installer_amount") or 0)
    await state.clear()
    await state.update_data(zpedit_invoice_id=invoice_id, zpedit_prev=cur_amt)
    await state.set_state(GdZpInstAdjustSG.amount)

    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="gdzp_inst:edit:cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✏️ <b>Изменение ЗП монтажника</b>\n\n"
        f"🔢 Счёт: №{inv['invoice_number']}\n"
        f"📍 Адрес: {inv.get('object_address') or '—'}\n"
        f"💵 Текущая сумма: <b>{cur_amt:,.0f}₽</b>\n\n"
        f"Введите новую сумму ЗП (₽):",
        reply_markup=b.as_markup(),
    )


@router.message(GdZpInstAdjustSG.amount)
async def gd_zp_installer_edit_amount(message: Message, state: FSMContext) -> None:
    """Шаг 2: парсинг суммы → confirm."""
    raw = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        val = float(raw)
        if val <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число больше 0:")
        return

    data = await state.get_data()
    invoice_id = data.get("zpedit_invoice_id")
    prev_amt = float(data.get("zpedit_prev") or 0)
    new_amt = int(val)
    await state.update_data(zpedit_new=new_amt)
    await state.set_state(GdZpInstAdjustSG.confirm)

    diff = new_amt - prev_amt
    sign = "+" if diff > 0 else ""
    text = (
        f"📋 <b>Подтверждение изменения ЗП</b>\n\n"
        f"💵 Было: {prev_amt:,.0f}₽\n"
        f"💵 Станет: <b>{new_amt:,.0f}₽</b>\n"
        f"   ({sign}{diff:,.0f}₽)\n\n"
        f"Подтвердить?"
    )
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="gdzp_inst:edit:confirm")
    b.button(text="❌ Отмена", callback_data="gdzp_inst:edit:cancel")
    b.adjust(2)
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "gdzp_inst:edit:confirm", GdZpInstAdjustSG.confirm)
async def gd_zp_installer_edit_finalize(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Подтверждение: update_invoice + sync + audit + уведомление монтажника."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    data = await state.get_data()
    invoice_id = data.get("zpedit_invoice_id")
    new_amt = data.get("zpedit_new")
    prev_amt = float(data.get("zpedit_prev") or 0)
    if not invoice_id or new_amt is None:
        await cb.message.answer("⚠️ Данные сессии утеряны, начните заново.")  # type: ignore[union-attr]
        await state.clear()
        return

    await db.update_invoice(int(invoice_id), zp_installer_amount=float(new_amt))
    await integrations.sync_invoice_row(int(invoice_id))

    inv = await db.get_invoice(int(invoice_id))
    inv_num = inv.get("invoice_number") if inv else "—"

    try:
        await db.audit(
            actor_id=u.id if u else None,
            action="invoice_zp_installer_amount_changed",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "invoice_id": int(invoice_id),
                "invoice_number": inv_num,
                "prev_amount": prev_amt,
                "new_amount": float(new_amt),
            },
        )
    except Exception:
        log.exception("gd_zp_installer_edit: audit failed for inv=%s", invoice_id)

    # Уведомить монтажника
    requested_by = (inv or {}).get("zp_installer_requested_by")
    if requested_by:
        try:
            await notifier.safe_send(
                int(requested_by),
                f"✏️ <b>ГД скорректировал ЗП</b>\n\n"
                f"Счёт №: <code>{inv_num}</code>\n"
                f"Было: {prev_amt:,.0f}₽\n"
                f"Стало: <b>{int(new_amt):,}₽</b>",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(requested_by))
        except Exception:
            log.exception("gd_zp_installer_edit: notify installer failed")

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП монтажника обновлена.\n"
        f"Счёт №{inv_num}: {prev_amt:,.0f}₽ → <b>{int(new_amt):,}₽</b>",
    )
    if u:
        await refresh_recipient_keyboard(notifier, db, config, u.id)


@router.callback_query(F.data == "gdzp_inst:edit:cancel")
async def gd_zp_installer_edit_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    """Отмена FSM на любом шаге."""
    await state.clear()
    await cb.answer("Отменено")
    try:
        await cb.message.edit_text("❌ Изменение ЗП отменено.")  # type: ignore[union-attr]
    except Exception:
        pass


# ---------- ЗП монтажника: выплата (платёжкой / без платёжки) ---------- #


async def _montazh_zp_payment_to_dr(
    db: Database, integrations: IntegrationHub, inv: dict[str, Any],
    closed_task_ids: list[int], actor_id: int | None,
) -> float:
    """Платёж ЗП монтажа → DR («Затр. Монтаж», cost_montazh) ПРИ ЗАКРЫТИИ ЗАДАЧИ.

    Owner 15.07 (решение №3): «для ГД добавление платёжки всегда опционально, а не
    обязательно. Платёж должен вноситься при закрытии задачи». До этого выплата
    платёжкой в DR не попадала вовсе (на 15.07 — 20 прод-счетов с выплаченной ЗП и
    DR=0), а выплата из кредит-кошелька попадала (apply_credit_wallet_spend, utils).

    Сумма = zp_installer_amount. Заявка ВСЕГДА остаток (is_remainder=True жёстко в
    обоих путях запроса, installer_new.py) → бот платит ровно её. Проверено на 3/3
    кошелёчных прецедентах прода (КВ 6/7/8: DR == zp_installer_amount при зачтённом
    авансе 25 000 / 5 000 / 0).
    ⛔ Зачтённый аванс НЕ вычитать: у заявки-остатка он уже вычтен в самой сумме
    (КВ 6 дал бы −17 800), а offset_zp_id указывает на СЧЁТ, а не на монтажную группу
    — на объединённом счёте вычлись бы и авансы прошлой группы. Для объединения
    (часть А) zp_installer_amount = доплата текущей группы → DR = выплаченное прошлой
    + доплата (90 000 + 130 000 = 220 000).

    ⛔ Гард задвоения: платёж создаётся ТОЛЬКО если этим действием реально закрыта
    открытая задача ЗП. Кошелёк закрывает задачу сам (resolve_installer_zp_by_wallet_
    payment) и сам пишет DR → клик по вечно живущему push-сообщению «платёжка» после
    кошелёчной выплаты открытой задачи уже не найдёт [[feedback_fsm_old_buttons_trap]].
    Поэтому хук висит на путях ВЫПЛАТЫ, а не внутри _close_zp_tasks: отклонение ЗП и
    снятие запроса при смене монтажной группы тоже закрывают задачу, но денег не
    двигают — фантомный DR там недопустим.
    """
    amount = float(inv.get("zp_installer_amount") or 0)
    if not closed_task_ids or amount <= 0:
        return 0.0
    invoice_id = int(inv["id"])
    # Ключ идемпотентности — id закрытой задачи ЗП: один платёж на одну задачу.
    # money_confirm_guard держит двойной клик по ОДНОМУ сообщению, а кнопка живёт и в
    # карточке «Монтаж ЗП», и в push после «ЗП ОК» — два разных message_id мимо него
    # проходят и оба успевают снять ещё открытую задачу [[feedback_money_confirm_idempotent_gate]].
    try:
        cur = await db.conn.execute(
            "SELECT id FROM supplier_payments WHERE parent_invoice_id = ? "
            "AND material_type = 'montazh' AND task_id = ? LIMIT 1",
            (invoice_id, closed_task_ids[0]),
        )
        if await cur.fetchone():
            log.info(
                "montazh zp→DR: платёж по задаче %s уже есть, пропуск inv=%s",
                closed_task_ids[0], invoice_id,
            )
            return 0.0
    except Exception:
        log.warning("montazh zp→DR: idempotency check failed inv=%s", invoice_id, exc_info=True)
        return 0.0
    try:
        sp_id = await db.create_supplier_payment(
            parent_invoice_id=invoice_id, amount=amount, material_type="montazh",
            invoice_number=str(inv.get("invoice_number") or ""),
            task_id=closed_task_ids[0], created_by=actor_id,
        )
    except Exception:
        log.warning(
            "montazh zp→DR: create_supplier_payment failed inv=%s", invoice_id, exc_info=True,
        )
        return 0.0
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception:
        log.warning("montazh zp→DR: sync_invoice_row failed inv=%s", invoice_id, exc_info=True)
    try:
        await db.audit(
            actor_id=actor_id, action="montazh_zp_supplier_payment",
            entity="invoice", entity_id=str(invoice_id),
            payload={
                "amount": amount, "supplier_payment_id": sp_id,
                "closed_task_ids": closed_task_ids,
            },
        )
    except Exception:
        log.debug("montazh zp→DR: audit failed inv=%s", invoice_id, exc_info=True)
    return amount


async def _cancel_paired_credit_requests(
    db: Database, notifier: Notifier, invoice_id: int, invoice_number: str,
    actor_id: int | None,
) -> None:
    """Анти-задвоение: ЗП монтажа выплачена штатно ГД → отменить парную ОТКРЫТУЮ
    кредит-заявку монтажа по тому же счёту, иначе кошелёк спишет ту же ЗП повторно.
    Помечаем DONE + флаг (исполнение заблокируется гардом credit_exec)."""
    try:
        _dupes = await db.list_open_credit_payment_requests_for_invoice(
            invoice_id, cost_type="montazh"
        )
        for _d in _dupes:
            _did = int(_d["id"])
            try:
                await db.update_task_payload(_did, {"cancelled_by_system": "zp_paid_direct"})
            except Exception:
                log.debug("gd_zp_pay: payload mark failed tid=%s", _did, exc_info=True)
            await db.update_task_status(_did, TaskStatus.DONE)
            _aid = _d.get("assigned_to")
            if _aid:
                try:
                    await notifier.safe_send(
                        int(_aid),
                        f"ℹ️ Кредит-заявка по ЗП монтажника (счёт №{invoice_number}) "
                        "отменена: ЗП уже выплачена напрямую ГД. Списывать кошелёк не нужно.",
                    )
                except Exception:
                    log.debug("gd_zp_pay: notify dupe assignee failed", exc_info=True)
        if _dupes:
            try:
                await db.audit(
                    actor_id=actor_id,
                    action="credit_request_cancelled_zp_paid_direct",
                    entity="invoice", entity_id=str(invoice_id),
                    payload={"task_ids": [int(d["id"]) for d in _dupes]},
                )
            except Exception:
                log.debug("gd_zp_pay: audit dupe-cancel failed", exc_info=True)
    except Exception:
        log.warning(
            "gd_zp_pay: anti-double credit-cancel hook failed inv=%s", invoice_id, exc_info=True
        )


async def _finalize_installer_zp_payment(
    db: Database, notifier: Notifier, integrations: IntegrationHub, config: Config,
    inv: dict[str, Any], actor_id: int | None,
    *, file_id: str | None = None, file_type: str | None = None,
) -> tuple[float, float]:
    """Общий хвост выплаты ЗП монтажника — платёжкой и без неё (owner 15.07 №3:
    платёжка опциональна). Один код на оба пути, чтобы они не разъехались.

    Порядок: статус payment_sent → [файл платёжки] → sync → закрыть задачу ЗП (её id
    нужны как гард для DR) → платёж в DR → отмена парной кредит-заявки → уведомление
    монтажника (+ платёжка вложением, если была) → до-закрытие кредитного счёта.
    Возвращает (сумма ЗП, сумма, попавшая в DR).
    """
    invoice_id = int(inv["id"])
    # id открытых задач ЗП снимаем ДО закрытия — они идут в платёж как ключ.
    try:
        _open_zp = await db.list_open_tasks_by_invoice(invoice_id, TaskType.ZP_INSTALLER)
        _ids = [int(t["id"]) for t in _open_zp]
    except Exception:
        log.warning("gd_zp_pay: list open zp tasks failed inv=%s", invoice_id, exc_info=True)
        _ids = []

    await db.set_invoice_zp_installer_status(invoice_id, "payment_sent")
    if file_id:
        await db.update_invoice(invoice_id, zp_installer_payment_file_id=file_id)
    await integrations.sync_invoice_row(invoice_id)

    # 🔑 Победителя гонки определяет АТОМАРНОЕ закрытие задачи: close_tasks_by_invoice —
    # один UPDATE ... WHERE status IN ('open','in_progress'), возвращает rowcount. Кто
    # закрыл строку, тот и платит; второй кликер получит 0 и в DR не пойдёт. Кнопка живёт
    # в двух сообщениях (карточка «Монтаж ЗП» + push после «ЗП ОК»), а money_confirm_guard
    # ключуется (user_id, message_id) и разные message_id не сериализует; проверки
    # «статус == approved» и SELECT-перед-INSERT сами по себе не атомарны — между их
    # await'ами вклинивается второй хендлер [[feedback_money_confirm_idempotent_gate]].
    closed_n = await db.close_tasks_by_invoice(invoice_id, TaskType.ZP_INSTALLER)
    closed_ids = _ids if closed_n > 0 else []
    dr_amount = await _montazh_zp_payment_to_dr(db, integrations, inv, closed_ids, actor_id)
    await _cancel_paired_credit_requests(
        db, notifier, invoice_id, str(inv.get("invoice_number") or ""), actor_id,
    )

    amt = float(inv.get("zp_installer_amount") or 0)
    requested_by = inv.get("zp_installer_requested_by")
    if requested_by:
        # Наёмная группа (edo_task_id=2): адресат карточки — РП, а денег он не
        # получает, их получают наёмники. Кнопка «✅ ЗП получено» просила бы его
        # подтвердить получение того, чего он не получал → у наёмных карточка
        # ЧИСТО ИНФОРМАЦИОННАЯ (owner 07.08). У штатного монтажника адресат сам
        # получатель — там кнопка остаётся, поведение прежнее.
        _is_naem = inv.get("edo_task_id") == 2
        _head = "💰 <b>Платёжка по ЗП</b>" if file_id else "💰 <b>ЗП выплачена</b>"
        if _is_naem:
            _kb = None
            _foot = "Наёмная группа 2️⃣ — выплата проведена."
        else:
            _b = InlineKeyboardBuilder()
            _b.button(text="✅ ЗП получено", callback_data=f"instzp_done:{invoice_id}")
            _kb = _b.as_markup()
            _foot = "Подтвердите получение ЗП."
        await notifier.safe_send(
            int(requested_by),
            f"{_head}\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
            f"Сумма: {amt:,.0f}₽\n\n"
            f"{_foot}",
            reply_markup=_kb,
        )
        if file_id and file_type:
            await notifier.safe_send_media(int(requested_by), file_type, file_id)
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))

    # owner 2026-07-03: ЗП монтаж выплачена → если это кредитный счёт с заполненной
    # «Дата Факт», чьё закрытие в «Счет End» откладывалось (гейт credit_zp_montazh_unpaid),
    # теперь до-закрываем его — симметрично авто-закрытию по «Дата Факт».
    try:
        inv_after = await db.get_invoice(invoice_id)
        _fd = inv_after.get("actual_completion_date") if inv_after else None
        if inv_after and inv_after.get("is_credit") and _fd:
            from ..services.sheet_commands import _auto_close_credit_invoice
            await _auto_close_credit_invoice(
                invoice=inv_after, fact_date=str(_fd),
                db=db, integrations=integrations, notifier=notifier, config=config,
            )
    except Exception:
        log.warning("gd_zp_pay: credit close-after-payment failed inv=%s", invoice_id, exc_info=True)

    return amt, dr_amount


@router.callback_query(F.data.startswith("gdzp_inst:nopdf:"))
@money_confirm_guard
async def gd_zp_paid_no_pdf(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """ГД: «✅ Выплачено без платёжки» — выплата ЗП монтажника без прикрепления PDF.

    Owner 15.07 (решение №3): «для ГД добавление платёжки всегда опционально, а не
    обязательно». Тот же хвост, что и у платёжки (_finalize_installer_zp_payment),
    включая платёж в DR при закрытии задачи.
    """
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Платить можно только утверждённую заявку. Кнопка живёт в чате вечно: ЗП мог уже
    # выплатить кошелёк (payment_sent) или РП снять запрос при смене монтажной группы
    # (not_requested) [[feedback_fsm_old_buttons_trap]], [[feedback_money_confirm_idempotent_gate]].
    if (inv.get("zp_installer_status") or "not_requested") != "approved":
        await cb.answer(
            "⚠️ ЗП не в статусе «утверждена» (уже выплачена, отклонена или отозвана).",
            show_alert=True,
        )
        return
    amt, dr_amount = await _finalize_installer_zp_payment(
        db, notifier, integrations, config, inv,
        cb.from_user.id if cb.from_user else None,
    )
    _dr_line = f"\n📉 Затраты монтаж: +{dr_amount:,.0f}₽" if dr_amount > 0 else ""
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП монтажника отмечена выплаченной (без платёжки).\n"
        f"Счёт №{inv['invoice_number']}, сумма: {amt:,.0f}₽{_dr_line}\n"
        f"Задача закрыта.",
    )
    if cb.from_user:
        await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)


@router.callback_query(F.data.startswith("gdzp_inst:pdf:"))
async def gd_zp_payment_start(
    cb: CallbackQuery, db: Database, state: FSMContext,
) -> None:
    """ГД нажал '📎 Отправить платёжку' — входим в FSM."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Платить можно только УТВЕРЖДЁННУЮ заявку — зеркало гарда nopdf-пути (выше).
    # Кнопка «✅ Завершить» карточки задачи ZP_INSTALLER (keyboards.py:774, приходит
    # ГД в 15-мин reminder-push) ведёт сюда напрямую ДО «✅ ЗП ОК»: без гарда выплата
    # шла в обход одобрения и зачёта аванса — оба живут только в gdzp_inst:ok.
    # [[feedback_fsm_old_buttons_trap]], [[feedback_money_confirm_idempotent_gate]]
    if (inv.get("zp_installer_status") or "not_requested") != "approved":
        await cb.answer(
            "⚠️ ЗП не в статусе «утверждена» — сначала «✅ ЗП ОК» "
            "(или уже выплачена, отклонена, отозвана).",
            show_alert=True,
        )
        return
    amt = inv.get("zp_installer_amount") or 0
    await state.update_data(zp_payment_invoice_id=invoice_id)
    await state.set_state(GdZpPaymentSG.waiting_pdf)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📎 Отправьте платёжку по ЗП монтажника.\n\n"
        f"🔢 Счёт: №{inv['invoice_number']}\n"
        f"💵 Сумма: {amt:,.0f}₽\n\n"
        f"<i>Прикрепите PDF, фото или скриншот платёжки.</i>",
    )


@router.message(GdZpPaymentSG.waiting_pdf, F.document | F.photo)
async def gd_zp_payment_upload(
    message: Message, state: FSMContext, db: Database,
    config: Config, notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """ГД отправил платёжку — пересылаем монтажнику, закрываем задачу."""
    data = await state.get_data()
    invoice_id = data.get("zp_payment_invoice_id")
    if not invoice_id:
        await message.answer("❌ Не найден счёт. Попробуйте заново.")
        await state.clear()
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        await state.clear()
        return

    # Повторный гард перед финализацией: FSM живёт сколь угодно долго — статус мог
    # уйти, пока ГД готовил платёжку (кошелёк выплатил ЗП, РП отозвал запрос при
    # смене монтажников). Клик-гард в gd_zp_payment_start это не ловит.
    # [[feedback_money_confirm_idempotent_gate]]
    if (inv.get("zp_installer_status") or "not_requested") != "approved":
        await message.answer(
            "⚠️ ЗП уже не в статусе «утверждена» (выплачена, отклонена или отозвана) — "
            "платёжка не отправлена.",
        )
        await state.clear()
        return

    # Определяем file_id и тип
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    else:
        file_id = message.photo[-1].file_id  # type: ignore[index]
        file_type = "photo"

    # Статус, файл, закрытие задачи, платёж в DR, анти-задвоение, уведомление
    # монтажника и до-закрытие кредитного счёта — общий хвост с путём «без платёжки».
    amt, dr_amount = await _finalize_installer_zp_payment(
        db, notifier, integrations, config, inv,
        message.from_user.id if message.from_user else None,
        file_id=file_id, file_type=file_type,
    )

    # Подтверждение ГД
    _dr_line = f"\n📉 Затраты монтаж: +{dr_amount:,.0f}₽" if dr_amount > 0 else ""
    await message.answer(
        f"✅ Платёжка отправлена монтажнику.\n"
        f"Счёт №{inv['invoice_number']}, сумма: {amt:,.0f}₽{_dr_line}\n"
        f"Задача закрыта.",
    )

    await state.clear()
    if message.from_user:
        await refresh_recipient_keyboard(notifier, db, config, message.from_user.id)


@router.callback_query(F.data.startswith("gdzp_zam:view:"))
async def gd_zp_zamery_view(cb: CallbackQuery, db: Database, invoice_id: int | None = None) -> None:
    """View zamery ZP request card."""
    await cb.answer()
    if invoice_id is None:
        invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    amt = inv.get("zp_zamery_total") or 0
    b = InlineKeyboardBuilder()
    b.button(text="✅ ЗП ОК", callback_data=f"zamzp_approve:yes:{invoice_id}")
    b.button(text="❌ Отклонить", callback_data=f"zamzp_approve:no:{invoice_id}")
    b.adjust(2)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📐 <b>ЗП замерщика</b>\n\n"
        f"🔢 Счёт: №{inv['invoice_number']}\n"
        f"📍 Адрес: {inv.get('object_address') or '—'}\n"
        f"💵 Сумма: {amt:,.0f}₽",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("gdzp_mgr:view:"))
async def gd_zp_manager_view(cb: CallbackQuery, db: Database, invoice_id: int | None = None) -> None:
    """View manager ZP request card with Plan/Fact comparison."""
    await cb.answer()
    if invoice_id is None:
        invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    amt = inv.get("zp_manager_amount") or 0

    # Plan/Fact card
    pf = await db.get_plan_fact_card(invoice_id)
    pf_text = ""
    if pf.get("has_estimated"):
        pf_text = "\n\n" + format_plan_fact_card(inv, pf)

    # Блок «🔴 ПЕРЕРАСЧЕТ ПРИБЫЛИ» убран из карточки ГД (owner 27.06).
    # План/Факт-карта (pf_text) остаётся.

    # owner 09.07 (вариант A): приём ≠ выплата. requested → «✅ ЗП ОК» (одобрить);
    # одобренная невыплаченная (approved & AN=0) → «💳 Выплатить».
    status = (inv.get("zp_manager_status") or "")
    paid = float(inv.get("zp_manager_payout") or 0)
    b = InlineKeyboardBuilder()
    if status == "approved" and paid <= 0:
        head = "💼 <b>ЗП отд.продаж</b> — ✅ одобрено, ожидает выплаты"
        b.button(text="💳 Выплатить", callback_data=f"gdzp_mgr:pay:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_mgr:no:{invoice_id}")
    else:
        head = "💼 <b>ЗП отд.продаж</b>"
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_mgr:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_mgr:no:{invoice_id}")
    b.adjust(2)
    await cb.message.answer(  # type: ignore[union-attr]
        f"{head}\n\n"
        f"🔢 Счёт: №{inv['invoice_number']}\n"
        f"📍 Адрес: {inv.get('object_address') or '—'}\n"
        f"💵 Запрос ЗП: {amt:,.0f}₽"
        f"{pf_text}",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("gdzp_mgr:ok:"))
async def gd_zp_manager_approve(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """ГД «✅ ЗП ОК» → ОДОБРИТЬ заявку ЗП менеджера (owner 09.07, вариант A: приём
    задачи ≠ выплата). Пишет только zp_manager_status='approved' — БЕЗ AN/AO, без
    зачёта аванса, без закрытия задачи. Реальная выплата — отдельным шагом
    «💳 Выплатить» (gd_zp_manager_pay). Идемпотентно по статусу."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Идемпотентность: уже одобрено/оплачено — повторно не одобряем.
    if (inv.get("zp_manager_status") or "") in ("approved", "payment_sent", "confirmed"):
        await cb.message.answer(  # type: ignore[union-attr]
            f"ℹ️ ЗП по счёту №{inv['invoice_number']} уже одобрена — нажмите «💳 Выплатить»."
        )
        return
    await state.clear()
    amt = inv.get("zp_manager_amount") or 0
    # Одобряем: статус approved, БЕЗ денег (AN/AO пусты до выплаты).
    await db.set_invoice_zp_manager_status(invoice_id, "approved", approved_by=cb.from_user.id)
    # Синк листа (статус). Задачу НЕ закрываем — счёт остаётся в «Прочие ЗП» с
    # кнопкой «💳 Выплатить», пока ГД не проведёт перевод.
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception as e:
        log.warning("sync_invoice_row after manager zp approve failed: %s", e)
    # Уведомить менеджера: одобрено, но ещё не выплачено.
    requested_by = inv.get("zp_manager_requested_by")
    if requested_by:
        await notifier.safe_send(
            int(requested_by),
            f"✅ <b>ЗП одобрена</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
            f"Сумма: {amt:,.0f}₽\n\n"
            "Ожидает выплаты — ГД проведёт перевод отдельно.",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))
    # Обновить карточку ГД → кнопка «💳 Выплатить».
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    b = InlineKeyboardBuilder()
    b.button(text="💳 Выплатить", callback_data=f"gdzp_mgr:pay:{invoice_id}")
    b.button(text="❌ Отклонить", callback_data=f"gdzp_mgr:no:{invoice_id}")
    b.adjust(2)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП отд.продаж одобрена — №{inv['invoice_number']}, {amt:,.0f}₽.\n"
        "Деньги ещё не переведены. Нажмите «💳 Выплатить», когда проведёте платёж.",
        reply_markup=b.as_markup(),
    )
    await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)


@router.callback_query(F.data.startswith("gdzp_mgr:pay:"))
async def gd_zp_manager_pay(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД «💳 Выплатить» (owner 09.07, вариант A) → экран вложения платёжки
    (ОПЦИОНАЛЬНА, как у ЗП РП): «✅ Подтвердить» / «✅ Без вложения» / «❌ Отмена».
    Реальная запись выплаты (AN/AO + зачёт аванса + close) — в
    _finalize_zp_manager_pay. Доступно только для одобренной невыплаченной ЗП."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    status = (inv.get("zp_manager_status") or "")
    if float(inv.get("zp_manager_payout") or 0) > 0 or status in ("payment_sent", "confirmed"):
        await cb.message.answer(  # type: ignore[union-attr]
            f"ℹ️ ЗП по счёту №{inv['invoice_number']} уже выплачена."
        )
        return
    if status != "approved":
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Сначала одобрите ЗП по счёту №{inv['invoice_number']} («✅ ЗП ОК»)."
        )
        return
    amt = float(inv.get("zp_manager_amount") or 0)
    # Аванс, уже выданный этому менеджеру и привязанный к ЭТОМУ счёту (open+closed).
    # ГД показываем НЕТТО к переводу, чтобы не задвоить: полная ЗП = аванс (выдан
    # ранее) + остаток наличными. Display-only: деньги-логику не трогаем —
    # zp_manager_payout фиксируется ПОЛНОЙ суммой в _finalize, а зачёт аванса
    # закрывает кошелёк. Правка нужна лишь чтобы ГД перевёл нетто и не заплатил дважды.
    adv_paid = min(await db.get_manager_advance_for_invoice(invoice_id), amt)
    net_pay = max(0.0, amt - adv_paid)
    # Переиспользуем generic-сборщик файлов InvoicePaymentSG.attaching_pp
    # (tasks.invoice_pp_collect: копит pp_files/pp_comment). Финал —
    # zpmgr_send/zpmgr_skip → _finalize_zp_manager_pay.
    await state.clear()
    await state.set_state(InvoicePaymentSG.attaching_pp)
    await state.update_data(zpmgr_inv=invoice_id)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data=f"zpmgr_send:{invoice_id}")
    b.button(text="✅ Без вложения", callback_data=f"zpmgr_skip:{invoice_id}")
    b.button(text="❌ Отмена", callback_data=f"zpmgr_acancel:{invoice_id}")
    b.adjust(1)
    if adv_paid > 0:
        sum_block = (
            f"Сумма ЗП: {amt:,.0f}₽\n"
            f"Аванс уже выдан: −{adv_paid:,.0f}₽\n"
            f"💵 К выплате сейчас: {net_pay:,.0f}₽\n\n"
        )
    else:
        sum_block = f"Сумма: {amt:,.0f}₽\n\n"
    await cb.message.answer(  # type: ignore[union-attr]
        f"💳 <b>Выплата ЗП отд.продаж — №{inv['invoice_number']}</b>\n"
        f"{sum_block}"
        "Прикрепите платёжку (PDF/фото) и/или комментарий, затем «✅ Подтвердить».\n"
        "Если платёжки нет — «✅ Без вложения».",
        reply_markup=b.as_markup(),
    )


async def _finalize_zp_manager_pay(
    invoice_id: int, actor_id: int, msg: Message,
    pp_files: list[dict[str, Any]], pp_comment: str,
    db: Database, config: Config, notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Запись ВЫПЛАТЫ ЗП менеджера (owner 09.07, вариант A): AN/AO прямым UPDATE
    (как AR/AS у ЗП РП, статус остаётся approved — «выплачено» = AN>0) + авто-зачёт
    ГД-авансов + sync + сохранение платёжки в ZP_MANAGER-задачу + пересылка
    менеджеру + close. Идемпотентно по AN. Выплата только для одобренной ЗП."""
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await msg.answer("❌ Счёт не найден.")
        return
    status = (inv.get("zp_manager_status") or "")
    if float(inv.get("zp_manager_payout") or 0) > 0 or status in ("payment_sent", "confirmed"):
        await msg.answer(f"ℹ️ ЗП по счёту №{inv['invoice_number']} уже выплачена ранее.")
        return
    if status != "approved":
        await msg.answer(f"⚠️ ЗП по счёту №{inv['invoice_number']} не одобрена — выплата невозможна.")
        return
    amt = float(inv.get("zp_manager_amount") or 0)
    # 1. Фиксируем выплату: AN/AO прямым UPDATE. Статус остаётся approved —
    #    «выплачено» определяется по AN>0 (как AR у ЗП РП). Дата DD.MM.YYYY —
    #    формат 1:1 с зачётом аванса (apply_advance_offsets... step3).
    pay_date = datetime.now().strftime("%d.%m.%Y")
    await db.update_invoice(
        invoice_id, zp_manager_payout=amt, zp_manager_payout_date=pay_date,
    )
    try:
        await db.audit(
            actor_id=actor_id, action="manager_zp_payout", entity="invoice",
            entity_id=str(invoice_id), payload={"amount": amt, "date": pay_date},
        )
    except Exception:
        log.debug("zpmgr pay: audit failed inv=%s", invoice_id, exc_info=True)
    # 2. Авто-зачёт ГД-авансов под этот счёт (зачёт аванса = форма выплаты; при
    #    полном покрытии step3 переведёт статус в confirmed).
    if amt and actor_id:
        try:
            await db.apply_advance_offsets_on_zp_approve(
                invoice_id, zp_id=invoice_id, zp_amount=float(amt),
                actor_id=actor_id, role="manager",
            )
        except Exception as e:
            log.warning("apply_advance_offsets (manager pay) failed for inv %s: %s", invoice_id, e)
    # 3. Пересобрать строку счёта ПОСЛЕ выплаты + зачёта (иначе лист отстаёт).
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception as e:
        log.warning("sync_invoice_row after manager zp pay failed: %s", e)
    # Сохранить вложения ГД в открытые ZP_MANAGER-задачи этого счёта ПЕРЕД закрытием.
    if pp_files:
        try:
            import json as _json
            cur = await db.conn.execute(
                "SELECT id, payload_json FROM tasks "
                "WHERE type = ? AND status IN ('open','in_progress')",
                (TaskType.ZP_MANAGER.value,),
            )
            for row in await cur.fetchall():
                try:
                    pl = _json.loads(row["payload_json"]) if row["payload_json"] else {}
                except (json.JSONDecodeError, TypeError):
                    pl = {}
                if pl.get("invoice_id") != invoice_id:
                    continue
                for a in pp_files:
                    try:
                        await db.add_attachment(
                            task_id=int(row["id"]),
                            file_id=a["file_id"],
                            file_unique_id=a.get("file_unique_id"),
                            file_type=a["file_type"],
                            caption=a.get("caption") or (pp_comment or None),
                            minio_object_key=a.get("minio_object_key"),
                        )
                    except Exception:
                        log.debug("zpmgr finalize: add_attachment failed", exc_info=True)
        except Exception:
            log.warning("zpmgr finalize: save attachments failed inv=%s", invoice_id, exc_info=True)
    await _close_zp_tasks(db, invoice_id, TaskType.ZP_MANAGER)
    # Уведомить менеджера + переслать платёжку (если есть).
    requested_by = inv.get("zp_manager_requested_by")
    if requested_by:
        # Тот же вычет аванса, что в карточке ГД (get_manager_advance_for_invoice):
        # менеджер видит полную ЗП, сколько выдано авансом ранее и сколько переведено
        # сейчас. Display-only — суммой платежа управляет _finalize выше.
        adv_paid = min(await db.get_manager_advance_for_invoice(invoice_id), amt)
        note = (
            f"💳 <b>ЗП выплачена</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
        )
        if adv_paid > 0:
            note += (
                f"Сумма ЗП: {amt:,.0f}₽\n"
                f"Аванс уже выдан: −{adv_paid:,.0f}₽\n"
                f"💵 Переведено сейчас: {max(0.0, amt - adv_paid):,.0f}₽"
            )
        else:
            note += f"Сумма: {amt:,.0f}₽"
        if pp_comment:
            note += f"\n📝 {html.escape(pp_comment)}"
        await notifier.safe_send(int(requested_by), note)
        for a in (pp_files or []):
            try:
                await notifier.safe_send_media(
                    int(requested_by), a["file_type"], a["file_id"], caption=a.get("caption"),
                )
            except Exception:
                log.debug("zpmgr pay: forward media failed", exc_info=True)
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))
    # Подтверждение ГД.
    suffix = f" (вложений: {len(pp_files)})" if pp_files else ""
    await msg.answer(
        f"💳 ЗП отд.продаж выплачена.{suffix}\n"
        f"Счёт №{inv['invoice_number']}, сумма: {amt:,.0f}₽",
    )
    if actor_id:
        await refresh_recipient_keyboard(notifier, db, config, int(actor_id))


@router.callback_query(F.data.startswith("zpmgr_send:"), InvoicePaymentSG.attaching_pp)
async def gd_zp_manager_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """ГД «✅ Подтвердить» — выплатить ЗП менеджера с приложенной платёжкой."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        await state.clear()
        return
    data = await state.get_data()
    invoice_id = int(data.get("zpmgr_inv") or 0) or int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "")
    await state.clear()
    await cb.answer()
    await _finalize_zp_manager_pay(
        invoice_id, cb.from_user.id, cb.message, pp_files, pp_comment,
        db, config, notifier, integrations,
    )


@router.callback_query(F.data.startswith("zpmgr_skip:"), InvoicePaymentSG.attaching_pp)
async def gd_zp_manager_skip(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """ГД «✅ Без вложения» — выплатить ЗП менеджера без платёжки (уже приложенные
    файлы, если есть, всё равно сохраняем — не теряем)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        await state.clear()
        return
    data = await state.get_data()
    invoice_id = int(data.get("zpmgr_inv") or 0) or int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "")
    await state.clear()
    await cb.answer()
    await _finalize_zp_manager_pay(
        invoice_id, cb.from_user.id, cb.message, pp_files, pp_comment,
        db, config, notifier, integrations,
    )


@router.callback_query(F.data.startswith("zpmgr_acancel:"), InvoicePaymentSG.attaching_pp)
async def gd_zp_manager_attach_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """ГД «❌ Отмена» на экране выплаты ЗП менеджера — выплата НЕ записывается,
    ЗП остаётся одобренной (вернуться к выплате можно через «Прочие ЗП»)."""
    await state.clear()
    await cb.answer("Отменено")
    _msg = "Отменено. Выплата не проведена — ЗП одобрена, вернитесь к «💳 Выплатить» в «Прочие ЗП»."
    try:
        await cb.message.edit_text(_msg)  # type: ignore[union-attr]
    except Exception:
        try:
            await cb.message.answer(_msg)  # type: ignore[union-attr]
        except Exception:
            pass


@router.callback_query(F.data.startswith("gdzp_mgr:no:"))
async def gd_zp_manager_reject(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    await db.set_invoice_zp_manager_status(invoice_id, "not_requested")
    await _close_zp_tasks(db, invoice_id, TaskType.ZP_MANAGER)
    await cb.message.answer(f"❌ ЗП отд.продаж по счёту №{inv['invoice_number']} отклонена.")  # type: ignore[union-attr]
    requested_by = inv.get("zp_manager_requested_by")
    if requested_by:
        await notifier.safe_send(
            int(requested_by),
            f"❌ <b>ЗП отклонена</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n"
            "Свяжитесь с ГД для уточнения.",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(requested_by))


async def _close_zp_tasks(db: Database, invoice_id: int, task_type: str) -> None:
    """Close all open ZP tasks related to this invoice."""
    import json
    cur = await db.conn.execute(
        "SELECT id, payload_json FROM tasks "
        "WHERE type = ? AND status IN ('open', 'in_progress')",
        (task_type,),
    )
    rows = await cur.fetchall()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except (json.JSONDecodeError, TypeError):
            payload = {}
        if payload.get("invoice_id") == invoice_id:
            await db.update_task_status(int(row["id"]), TaskStatus.DONE)


# --- Outgoing supplier payment flow (via callback from dashboard) ---

@router.callback_query(F.data == "supplier_pay_start")
async def start_supplier_payment(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Start existing 8-step supplier payment flow from dashboard button."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(SupplierPaymentSG.project)
    await cb.message.answer(  # type: ignore[union-attr]
        "💸 <b>Оплата поставщику</b>\n"
        "Шаг 1/8: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="suppl_pay"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "suppl_pay"))
async def supplier_pay_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))

    # Show parent invoice picker. include_credit=True (owner 10.08): ГД привязывает
    # оплату поставщику и к кредитному счёту — кредит скрыт только от бухгалтерии
    # ([[feedback_credit_filter_accounting_only]]).
    # ⚠️ limit 15 → 30 обязателен вместе с флагом: выборка идёт «свежие первыми», и
    # на боевых данных четыре кредитных счёта вытесняли из пикера ВОСЕМЬ обычных
    # (замер 11.08). Тот же приём, что в gd.py:1180.
    from ..keyboards import invoice_select_kb
    invoices = await db.list_invoices_for_selection(limit=30, only_regular=True, include_credit=True)
    if invoices:
        await state.set_state(SupplierPaymentSG.parent_invoice)
        await cb.message.answer(  # type: ignore
            "Шаг 2/8: привязка к счёту объекта (или пропустите):",
            reply_markup=invoice_select_kb(invoices, prefix="suppl_parent", back_callback="nav:home"),
        )
    else:
        await state.update_data(parent_invoice_id=None)
        from ..keyboards import material_type_kb
        await state.set_state(SupplierPaymentSG.material_type)
        await cb.message.answer(  # type: ignore
            "Шаг 3/8: тип материала/услуги:",
            reply_markup=material_type_kb(prefix="suppl_mat"),
        )


@router.callback_query(
    SupplierPaymentSG.parent_invoice,
    lambda cb: cb.data and cb.data.startswith("suppl_parent:"),
)
async def supplier_pay_pick_parent(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick parent invoice for supplier payment."""
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    parent_id = None if val == "skip" else int(val)
    await state.update_data(parent_invoice_id=parent_id)

    from ..keyboards import material_type_kb
    await state.set_state(SupplierPaymentSG.material_type)
    await cb.message.answer(  # type: ignore
        "Шаг 3/8: тип материала/услуги:",
        reply_markup=material_type_kb(prefix="suppl_mat"),
    )


@router.callback_query(
    SupplierPaymentSG.material_type,
    lambda cb: cb.data and cb.data.startswith("suppl_mat:"),
)
async def supplier_pay_pick_material(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick material type for supplier payment."""
    await cb.answer()
    mat_code = (cb.data or "").split(":", 1)[1]
    await state.update_data(material_type=mat_code)

    await state.set_state(SupplierPaymentSG.supplier)
    await cb.message.answer("Шаг 4/8: поставщик (название компании):")  # type: ignore


@router.message(SupplierPaymentSG.supplier)
async def supplier_pay_supplier(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 2:
        await message.answer("Укажите название поставщика:")
        return
    await state.update_data(supplier=t)
    await state.set_state(SupplierPaymentSG.amount)
    await message.answer("Шаг 5/8: сумма оплаты (например 50000 или 50k):")


@router.message(SupplierPaymentSG.amount)
async def supplier_pay_amount(message: Message, state: FSMContext) -> None:
    amount = parse_amount((message.text or "").strip())
    if amount is None:
        await message.answer("Не понял сумму. Пример: 50000 или 50k.")
        return
    await state.update_data(amount=amount)
    await state.set_state(SupplierPaymentSG.invoice_number)
    await message.answer("Шаг 6/8: номер счёта поставщика (или «-»):")


@router.message(SupplierPaymentSG.invoice_number)
async def supplier_pay_invoice(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(invoice_number=t)
    await state.set_state(SupplierPaymentSG.comment)
    await message.answer("Комментарий (или «-»):")


@router.message(SupplierPaymentSG.comment)
async def supplier_pay_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(SupplierPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ПП", callback_data="supplpay:create")
    b.button(text="⏭ Без вложений", callback_data="supplpay:create")
    # Шаг 8/8: вложения
    b.adjust(1)
    await message.answer(
        "Приложите платёжное поручение / скрин оплаты (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.message(SupplierPaymentSG.attachments)
async def supplier_pay_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"supplier_pay/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить ПП».")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "supplpay:create")
@money_confirm_guard
async def supplier_pay_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await cb.message.answer("Не выбран проект. Начните заново.")  # type: ignore
        await state.clear()
        return

    project = await db.get_project(int(project_id))
    supplier = data.get("supplier") or ""
    amount = data.get("amount")
    invoice_number = data.get("invoice_number") or ""
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []
    parent_invoice_id = data.get("parent_invoice_id")
    material_type = data.get("material_type")

    # Задачу назначаем РП для информирования
    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)

    source_order_task_id = data.get("source_order_task_id")

    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.SUPPLIER_PAYMENT,
        status=TaskStatus.DONE,  # Оплата уже произведена
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=None,
        payload={
            "supplier": supplier,
            "amount": amount,
            "invoice_number": invoice_number,
            "comment": comment,
            "td_id": u.id,
            "td_username": u.username,
            "parent_invoice_id": parent_invoice_id,
            "material_type": material_type,
        },
    )

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    # Вписать расход в БД с привязкой к материнскому счёту → себестоимость
    # (supplier_payments + cost_*/DP–DV). Зеркалит авто-путь оплаты счёта от РП
    # (tasks.py invoice_pp_finalize), НО без авто-списания кредит-кошелька —
    # решение user 08.07: ручной ГД-флоу пишет ТОЛЬКО расход в себестоимость.
    # Money-хендлер: сбой записи/синка НЕ должен ломать финализацию/уведомление РП.
    if parent_invoice_id and isinstance(amount, (int, float)) and amount:
        try:
            sp_id = await db.create_supplier_payment(
                parent_invoice_id=int(parent_invoice_id),
                amount=float(amount),
                material_type=material_type or "extra_mat",
                invoice_number=invoice_number,
                supplier=supplier,
                task_id=int(task["id"]) if task else None,
                created_by=u.id,
            )
            try:
                await integrations.sync_invoice_row(int(parent_invoice_id))
            except Exception:
                log.warning(
                    "supplpay manual: sync_invoice_row failed inv=%s sp=%s",
                    parent_invoice_id, sp_id, exc_info=True,
                )
        except Exception:
            log.warning(
                "supplpay manual: create_supplier_payment failed inv=%s",
                parent_invoice_id, exc_info=True,
            )

    amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
    msg = (
        "💸 <b>Оплата поставщику произведена</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"🏭 Поставщик: <b>{supplier}</b>\n"
        f"💰 Сумма: <b>{amount_s}</b>\n"
    )
    if invoice_number:
        msg += f"🧾 Счёт №: <b>{invoice_number}</b>\n"
    if parent_invoice_id:
        parent_inv = await db.get_invoice(parent_invoice_id)
        if parent_inv:
            msg += f"📋 Объект: Счёт №{parent_inv.get('invoice_number', '?')} — {(parent_inv.get('object_address') or '')[:40]}\n"
    if material_type:
        from ..enums import MATERIAL_TYPE_LABELS
        msg += f"📦 Материал: {MATERIAL_TYPE_LABELS.get(material_type, material_type)}\n"
    if comment:
        msg += f"📝 Комментарий: {comment}\n"
    msg += f"\nОт ГД: <code>{u.id}</code> @{u.username or '-'}"

    # Уведомляем РП и рабочий чат
    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
    await notifier.notify_workchat(msg)

    # Отправляем ПП
    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if rp_id:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))
    if rp_id:
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await integrations.sync_task(task, project_code=project.get("code", ""))

    if source_order_task_id:
        try:
            src_task = await db.get_task(int(source_order_task_id))
            if (
                src_task.get("project_id") == int(project_id)
                and src_task.get("type") in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS}
                and src_task.get("status") in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}
            ):
                src_task = await db.update_task_status(int(source_order_task_id), TaskStatus.DONE)
                await integrations.sync_task(src_task, project_code=project.get("code", ""))
        except Exception:
            log.exception("Failed to auto-close source order task id=%s", source_order_task_id)

    user_now = await db.get_user_optional(u.id)
    role_now, isolated_role = resolve_menu_scope(u.id, user_now.role if user_now else Role.GD)
    await cb.message.answer(
        (
            f"✅ Оплата поставщику «{supplier}» зафиксирована. "
            + ("РП уведомлён." if rp_id else "⚠️ РП не назначен (role=rp), уведомление не отправлено.")
        ),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role_now,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                unread_channels=await db.count_unread_by_channel(u.id),
                gd_inbox_unread=await db.count_gd_inbox_tasks(u.id),
                gd_invoice_unread=await db.count_gd_invoice_tasks(u.id),
                gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(u.id),
                gd_total_open_tasks=await db.count_gd_more_total_open_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )  # type: ignore
    await state.clear()


# ==================== B5 TZ v8: Выплата оклада РП (60К/мес) с платёжкой ====================

def _render_rp_salary_confirm(
    rp_id: int, rp_label: str, month_str: str, has_receipt: bool,
    advance: dict[str, float] | None = None,
) -> tuple[str, Any]:
    """Экран подтверждения выплаты оклада РП (платёжка опциональна).

    advance — db.get_rp_oklad_advance_offset: выданный аванс зачитывается в оклад, ГД
    видит остаток к выплате (ТЗ owner 31.07). Аванса нет → экран как был.
    """
    receipt_line = "  Платёжка: <b>прикреплена</b>\n" if has_receipt else ""
    tail = (
        "запись в «Баланс компании» и платёжка уйдёт РП."
        if has_receipt
        else "запись в «Баланс компании», РП получит уведомление."
    )
    adv = float((advance or {}).get("deduct") or 0)
    if adv > 0:
        sum_block = (
            f"  Оклад: <b>{_fmt_rub_td(RP_SALARY_MONTHLY)} ₽</b>\n"
            f"  Аванс зачтён: <b>−{_fmt_rub_td(adv)} ₽</b>\n"
            f"  К выплате: <b>{_fmt_rub_td((advance or {}).get('payout'))} ₽</b>\n"
        )
    else:
        sum_block = f"  Сумма: <b>{_fmt_rub_td(RP_SALARY_MONTHLY)} ₽</b>\n"
    text = (
        f"💼 <b>Выплата оклада РП</b>\n\n"
        f"  Сотрудник: <b>{html.escape(rp_label)}</b>\n"
        f"{sum_block}"
        f"  Месяц: <b>{month_str}</b>\n"
        f"{receipt_line}\n"
        f"После подтверждения: {tail}"
    )
    b = InlineKeyboardBuilder()
    if has_receipt:
        b.button(
            text="✅ Записать в БК и отправить РП",
            callback_data=RpSalaryCb(rp_id=rp_id, action="confirm").pack(),
        )
    else:
        b.button(
            text="✅ Выплатить без платёжки",
            callback_data=RpSalaryCb(rp_id=rp_id, action="confirm").pack(),
        )
        b.button(
            text="📎 С платёжкой",
            callback_data=RpSalaryCb(rp_id=rp_id, action="attach").pack(),
        )
    b.button(text="❌ Отмена", callback_data=f"rp_salary_cancel:{rp_id}")
    b.adjust(1)
    return text, b.as_markup()


@router.callback_query(RpSalaryCb.filter(F.action == "start"))
async def rp_salary_start(
    cb: CallbackQuery,
    callback_data: RpSalaryCb,
    state: FSMContext,
    db: Database,
) -> None:
    """B5 entry: ГД жмёт «💼 Оклад {name} 66К» → экран подтверждения (платёжка опциональна)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    rp = await db.get_user_optional(callback_data.rp_id)
    if not rp:
        await cb.answer("РП не найден", show_alert=True)
        return
    rp_label = rp.full_name or (f"@{rp.username}" if rp.username else f"id{rp.telegram_id}")
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    month_str = now_msk.strftime("%Y-%m")
    # A2 взаимоисключение «один оклад в месяц»: если оклад уже переведён РП в аванс — запрет
    okl_st = await db.get_rp_oklad_advance_status(now_msk.year, now_msk.month)
    if okl_st.get("to_advance", 0) > 0:
        await cb.answer(
            "Оклад за этот месяц уже переведён РП в аванс — выплата невозможна.",
            show_alert=True,
        )
        return
    await state.clear()
    await state.set_state(RpSalaryPaySG.confirm)
    await state.update_data(
        rp_id=callback_data.rp_id,
        rp_label=rp_label,
        month=month_str,
        year=now_msk.year,
        month_num=now_msk.month,
        date_display=now_msk.strftime("%d.%m.%Y"),
        receipt_file_id=None,
        receipt_file_type=None,
    )
    advance = await db.get_rp_oklad_advance_offset(callback_data.rp_id)
    text, kb = _render_rp_salary_confirm(
        callback_data.rp_id, rp_label, month_str, has_receipt=False, advance=advance,
    )
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
    await cb.answer()


@router.message(RpSalaryPaySG.attach_receipt, F.content_type.in_({"document", "photo", "video"}))
async def rp_salary_receipt(message: Message, state: FSMContext, db: Database) -> None:
    """B5 step 2: получена платёжка → сохранить + preview + confirm.

    db — для пересчёта зачёта аванса РП в карточке подтверждения (ТЗ owner 31.07).
    """
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
    rp_id = int(data.get("rp_id") or 0)
    rp_label = data.get("rp_label") or "РП"
    month_str = data.get("month") or ""
    await state.update_data(receipt_file_id=file_id, receipt_file_type=file_type)
    await state.set_state(RpSalaryPaySG.confirm)
    advance = await db.get_rp_oklad_advance_offset(rp_id) if rp_id else None
    text, kb = _render_rp_salary_confirm(
        rp_id, rp_label, month_str, has_receipt=True, advance=advance,
    )
    await message.answer(text, reply_markup=kb)


@router.message(RpSalaryPaySG.attach_receipt)
async def rp_salary_receipt_invalid(message: Message) -> None:
    """B5: некорректный ввод вместо платёжки."""
    await message.answer("📎 Прикрепите платёжку (PDF, фото или документ).")


@router.callback_query(RpSalaryCb.filter(F.action == "attach"), RpSalaryPaySG.confirm)
async def rp_salary_attach(
    cb: CallbackQuery,
    callback_data: RpSalaryCb,
    state: FSMContext,
) -> None:
    """ГД выбрал приложить платёжку к окладу → ждём фото/документ (опционально)."""
    await cb.answer()
    await state.set_state(RpSalaryPaySG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=f"rp_salary_cancel:{callback_data.rp_id}")
    b.adjust(1)
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📎 Прикрепите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )
    except Exception:
        await cb.message.answer(  # type: ignore[union-attr]
            "📎 Прикрепите платёжку (PDF, фото или документ) одним сообщением.",
            reply_markup=b.as_markup(),
        )


@router.callback_query(RpSalaryCb.filter(F.action == "confirm"), RpSalaryPaySG.confirm)
@money_confirm_guard
async def rp_salary_confirm(
    cb: CallbackQuery,
    callback_data: RpSalaryCb,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """B5 final: INSERT op_company_entries + sync БК + audit + notify РП с платёжкой."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    data = await state.get_data()
    rp_id = int(data.get("rp_id") or callback_data.rp_id)
    rp_label = data.get("rp_label") or "РП"
    year = int(data.get("year") or 0)
    month_num = int(data.get("month_num") or 0)
    date_display = data.get("date_display") or ""
    month_str = data.get("month") or ""
    receipt_file_id = data.get("receipt_file_id")
    receipt_file_type = data.get("receipt_file_type")
    if not year or not month_num:
        await cb.answer("Данные потерялись, начните заново", show_alert=True)
        await state.clear()
        return
    # B5 v2 race-guard: если открыт через task — проверить, что ещё не закрыт другим ГД
    task_id_close = data.get("task_id")
    if task_id_close:
        try:
            cur_task = await db.get_task(int(task_id_close))
            cur_status = cur_task.get("status") or ""
            if cur_status not in (TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value):
                await cb.answer(f"Запрос уже обработан (status={cur_status})", show_alert=True)
                await state.clear()
                return
        except KeyError:
            log.warning("rp_salary_confirm: task %s not found (race-guard)", task_id_close)
    # A2 взаимоисключение «один оклад в месяц» (финал, обязательный барьер): если РП уже
    # перевёл оклад в аванс за этот месяц — выплату не проводим (последняя линия защиты).
    okl_st = await db.get_rp_oklad_advance_status(year, month_num)
    if okl_st.get("gd_paid"):
        # Оклад за этот месяц уже выплачен (есть запись «Оклад РП%» в op_company_entries) —
        # анти-двойная-выплата 66К. Завершает инвариант «один оклад в месяц» на стороне
        # ВЫПЛАТЫ (раньше барьер ловил только перевод-в-аванс, не повторную выплату ГД —
        # двойной тап pre-card давал второй op_company_entry; ревью 30.06).
        await cb.answer(
            "Оклад за этот месяц уже выплачен — повторная выплата невозможна.",
            show_alert=True,
        )
        await state.clear()
        return
    if okl_st.get("to_advance", 0) > 0:
        await cb.answer(
            "Оклад за этот месяц уже переведён РП в аванс — выплата невозможна.",
            show_alert=True,
        )
        await state.clear()
        return
    # 1. Запись в «Баланс компании» + гашение зачтённого аванса — ОДНОЙ транзакцией
    #    (ТЗ owner 31.07). В БК уходит ФАКТИЧЕСКИ выплаченное (66 000 − аванс×1,1), а не
    #    полный оклад: выдача аванса уже прошла расходом раньше (credit_wallet_spend),
    #    и полная сумма задвоила бы расход компании. Величина считается ВНУТРИ транзакции.
    #    date_iso внутри = реальная дата платёжки (B5 v2 TZ 28.05): year/month — период ЗП
    #    (следующий месяц), а платёжка сегодняшняя.
    try:
        paid = await db.record_rp_salary_payment(
            rp_id=rp_id,
            year=year,
            month=month_num,
            month_str=month_str,
            date_display=date_display,
            rp_label=rp_label,
            actor_id=cb.from_user.id,
        )
    except OkladAlreadyPaidError:
        await cb.answer(
            "Оклад за этот месяц уже выплачен — повторная выплата невозможна.",
            show_alert=True,
        )
        await state.clear()
        return
    except Exception:
        log.exception("rp_salary_confirm: record_rp_salary_payment failed rp=%s", rp_id)
        await cb.answer("❌ Ошибка записи в БД", show_alert=True)
        return
    entry_id = int(paid["entry_id"])
    amount = float(paid["payout"])
    adv_deduct = float(paid["deduct"])
    # 2. sync лист «Баланс компании»
    sync_note = ""
    if integrations.sheets:
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception as ex:
            log.warning("rp_salary_confirm: sync_balance_company_sheet failed: %s", ex)
            sync_note = "\n⚠️ Лист «Баланс компании» не пересинхронизирован (ошибка)."
    # 3. audit
    try:
        await db.audit(
            actor_id=cb.from_user.id,
            action="rp_salary_paid",
            entity="op_company_entries",
            entity_id=str(entry_id),
            payload={
                "rp_id": rp_id,
                "rp_name": rp_label,
                "amount": amount,
                "month": month_str,
                "receipt_file_id": receipt_file_id,
                "receipt_file_type": receipt_file_type,
                # Зачёт аванса (31.07): amount здесь = ФАКТИЧЕСКИ выплаченное, поэтому
                # оклад и вычет пишем явно, иначе по аудиту не восстановить.
                "oklad_full": float(RP_SALARY_MONTHLY),
                "advance_offset": adv_deduct,
                "advance_carry": float(paid["carry"]),
            },
        )
    except Exception:
        log.exception("rp_salary_confirm: audit failed entry_id=%s", entry_id)
    # 4. Notify РП: pre-card + платёжка вложением
    caption_card = (
        f"<pre>✅ <b>Оклад РП выплачен</b>\n"
        f"   Месяц                 {month_str}\n"
        f"   Дата                  {date_display}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        + "\n".join(format_rp_oklad_lines(
            {"deduct": adv_deduct, "payout": amount, "carry": float(paid["carry"])},
            float(RP_SALARY_MONTHLY),
        ))
        + "</pre>"
    )
    try:
        await notifier.safe_send(rp_id, caption_card)
    except Exception:
        log.exception("rp_salary_confirm: notify РП %s text failed", rp_id)
    if receipt_file_id:
        try:
            await notifier.safe_send_media(
                rp_id,
                str(receipt_file_type or "document"),
                str(receipt_file_id),
                caption="💳 Платёжка по выплате оклада",
            )
        except Exception:
            log.exception("rp_salary_confirm: notify РП %s receipt failed", rp_id)
    # 5. Close task RP_SALARY (B5 v2 request-based — если открыт через task)
    if task_id_close:
        try:
            await db.update_task_status(
                int(task_id_close),
                TaskStatus.DONE.value,
                expected_statuses=(TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value),
            )
        except Exception:
            log.exception("rp_salary_confirm: close task %s failed", task_id_close)
    # 6. Update ГД card
    await state.clear()
    receipt_card_line = "  Платёжка → отправлена РП" if receipt_file_id else "  Без платёжки"
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ <b>Оклад выплачен</b> (запись #{entry_id})\n\n"
            f"  Сотрудник: <b>{html.escape(rp_label)}</b>\n"
            f"  Сумма: <b>{_fmt_rub_td(amount)} ₽</b>\n"
            f"  Месяц: <b>{month_str}</b>\n"
            f"{receipt_card_line}"
            + sync_note,
        )
    except Exception:
        log.exception("rp_salary_confirm: edit_text failed")
    await cb.answer("Записано и отправлено")


@router.callback_query(F.data.startswith("rp_salary_cancel:"))
async def rp_salary_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """B5: отмена выплаты оклада на любом шаге."""
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Выплата оклада отменена.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Выплата оклада отменена.")  # type: ignore[union-attr]


# ==================== B5 v2: REQUEST-BASED — ГД-handler'ы task'а оклада ====================


@router.callback_query(RpSalaryTaskCb.filter(F.action == "open"))
async def rp_salary_task_open(
    cb: CallbackQuery,
    callback_data: RpSalaryTaskCb,
    state: FSMContext,
    db: Database,
) -> None:
    """B5 v2 entry: ГД жмёт «✅ Выплатить» в pre-card task'а → переход в RpSalaryPaySG.

    Берёт task_id, парсит payload, инициализирует state и переходит в confirm
    (confirm-first: ✅ без платёжки / 📎 С платёжкой / ❌ Отмена; платёжка опциональна).
    """
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    try:
        task = await db.get_task(callback_data.task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") not in (TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value):
        await cb.answer(f"Запрос уже обработан (status={task.get('status')})", show_alert=True)
        return
    try:
        payload = json.loads(task.get("payload_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        payload = {}
    rp_id = int(payload.get("rp_id") or 0)
    rp_label = payload.get("rp_name") or "РП"
    month_str = payload.get("month") or ""
    if not rp_id or not month_str:
        await cb.answer("Некорректный payload task'а", show_alert=True)
        return
    # Распарсим month YYYY-MM → year, month_num
    try:
        year_s, mon_s = month_str.split("-")
        year_n = int(year_s)
        month_num = int(mon_s)
    except (ValueError, AttributeError):
        await cb.answer("Некорректный month в payload", show_alert=True)
        return
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    date_display = now_msk.strftime("%d.%m.%Y")
    # A2 взаимоисключение «один оклад в месяц»: если оклад уже переведён РП в аванс — запрет
    okl_st = await db.get_rp_oklad_advance_status(year_n, month_num)
    if okl_st.get("to_advance", 0) > 0:
        await cb.answer(
            "Оклад за этот месяц уже переведён РП в аванс — выплата невозможна.",
            show_alert=True,
        )
        return
    await state.clear()
    await state.set_state(RpSalaryPaySG.confirm)
    await state.update_data(
        rp_id=rp_id,
        rp_label=rp_label,
        month=month_str,
        year=year_n,
        month_num=month_num,
        date_display=date_display,
        task_id=callback_data.task_id,
        receipt_file_id=None,
        receipt_file_type=None,
    )
    advance = await db.get_rp_oklad_advance_offset(rp_id)
    text, kb = _render_rp_salary_confirm(
        rp_id, rp_label, month_str, has_receipt=False, advance=advance,
    )
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]
    await cb.answer()


@router.callback_query(RpSalaryTaskCb.filter(F.action == "reject_start"))
async def rp_salary_task_reject_start(
    cb: CallbackQuery,
    callback_data: RpSalaryTaskCb,
    state: FSMContext,
    db: Database,
) -> None:
    """B5 v2: ГД жмёт «❌ Отклонить» → запрос причины (RpSalaryRejectSG.reason)."""
    if not await require_role_callback(cb, db, roles=GD_ACCESS_ROLES):
        return
    try:
        task = await db.get_task(callback_data.task_id)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        return
    if task.get("status") not in (TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value):
        await cb.answer(f"Запрос уже обработан (status={task.get('status')})", show_alert=True)
        return
    await state.clear()
    await state.set_state(RpSalaryRejectSG.reason)
    await state.update_data(task_id=callback_data.task_id)
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ <b>Отклонение запроса оклада</b>\n\n"
        "Напишите причину отклонения (текстом):"
    )
    await cb.answer()


@router.message(RpSalaryRejectSG.reason, F.text)
async def rp_salary_reject_submit(
    message: Message,
    state: FSMContext,
    db: Database,
    notifier: Notifier,
) -> None:
    """B5 v2 final отклонения: REJECTED + комментарий в payload + notify РП."""
    if not await require_role_message(message, db, roles=GD_ACCESS_ROLES):
        return
    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Причина не может быть пустой. Напишите текст:")
        return
    data = await state.get_data()
    task_id = data.get("task_id")
    if not task_id:
        await message.answer("❌ Сессия потеряна. Начните заново через карточку.")
        await state.clear()
        return
    try:
        task = await db.get_task(int(task_id))
    except KeyError:
        await message.answer("❌ Запрос не найден.")
        await state.clear()
        return
    try:
        payload = json.loads(task.get("payload_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        payload = {}
    rp_id = int(payload.get("rp_id") or 0)
    rp_label = payload.get("rp_name") or "РП"
    month_str = payload.get("month") or ""
    # 1. Update task status REJECTED + reason в payload
    payload["reject_reason"] = reason
    payload["rejected_by"] = message.from_user.id if message.from_user else 0
    try:
        await db.conn.execute(
            "UPDATE tasks SET status = ?, payload_json = ?, updated_at = ? WHERE id = ?",
            (
                TaskStatus.REJECTED.value,
                json.dumps(payload, ensure_ascii=False),
                datetime.now().isoformat(timespec="seconds"),
                int(task_id),
            ),
        )
        await db.conn.commit()
    except Exception:
        log.exception("rp_salary_reject_submit: UPDATE task %s failed", task_id)
        await message.answer("❌ Ошибка БД при отклонении.")
        await state.clear()
        return
    # 2. audit
    try:
        await db.audit(
            actor_id=message.from_user.id if message.from_user else 0,
            action="rp_salary_rejected",
            entity="task",
            entity_id=str(task_id),
            payload={"rp_id": rp_id, "rp_name": rp_label, "month": month_str, "reason": reason},
        )
    except Exception:
        log.exception("rp_salary_reject_submit: audit failed task=%s", task_id)
    # 3. Notify РП
    if rp_id:
        notify_text = (
            f"<pre>❌ <b>Запрос оклада отклонён</b>\n"
            f"   Месяц                {month_str}\n"
            f"   Причина              {html.escape(reason)[:60]}</pre>\n\n"
            f"Можно повторить запрос за этот же месяц."
        )
        try:
            await notifier.safe_send(rp_id, notify_text)
        except Exception:
            log.exception("rp_salary_reject_submit: notify РП %s failed", rp_id)
    await state.clear()
    await message.answer(
        f"✅ Запрос оклада <b>отклонён</b>.\n"
        f"  Сотрудник: <b>{html.escape(rp_label)}</b>\n"
        f"  Месяц: <b>{month_str}</b>\n"
        f"  Причина: {html.escape(reason)[:120]}"
    )

