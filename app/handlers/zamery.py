"""
Handlers for Zamery (Замерщик) role.

Covers:
- Замеры (incoming requests, respond with blanks)
- Мои объекты (list, ZP status)
- Расчёт ЗП (ZameryZpSG) — Дополнение 3
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
from ..enums import InvoiceStatus, Role, TaskStatus
from ..keyboards import (
    ZAM_BTN_MY_OBJECTS,
    ZAM_BTN_ZAMERY,
    main_menu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.notifier import Notifier
from ..states import ZameryWorkSG, ZameryZpSG
from ..utils import private_only_reply_markup
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return user.role if user else None


# =====================================================================
# ЗАМЕРЫ (incoming requests)
# =====================================================================

@router.message(F.text == ZAM_BTN_ZAMERY)
async def zamery_inbox(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return

    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    if not tasks:
        await message.answer("📐 Нет входящих заявок на замеры ✅")
        return

    await message.answer(
        f"📐 <b>Замеры</b> ({len(tasks)}):\n\n"
        "Нажмите на заявку для просмотра:",
        reply_markup=tasks_kb(tasks),
    )


# =====================================================================
# МОИ ОБЪЕКТЫ (с кнопками «Расчёт ЗП» для подходящих счетов)
# =====================================================================

@router.message(F.text == ZAM_BTN_MY_OBJECTS)
async def zamery_my_objects(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return

    # Show invoices assigned to this zamery worker with relevant statuses
    user_id = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    active = [i for i in invoices if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
        InvoiceStatus.CLOSING, InvoiceStatus.ENDED,
    )]

    if not active:
        await message.answer("📌 Нет активных объектов.")
        return

    lines = []
    b = InlineKeyboardBuilder()
    has_zp_buttons = False

    for inv in active[:20]:
        zp = inv.get("zp_status", "not_requested")
        zp_emoji = {"approved": "✅", "requested": "⏳"}.get(zp, "—")
        status_emoji = {
            "in_progress": "🔄", "paid": "✅",
            "closing": "📌", "ended": "🏁",
        }.get(inv["status"], "❓")
        lines.append(
            f"{status_emoji} №{inv['invoice_number']} — {inv.get('object_address', '-')[:30]} "
            f"[ЗП: {zp_emoji}]"
        )
        # Кнопка «Расчёт ЗП» для счетов без утверждённой ЗП
        if zp not in ("approved", "requested"):
            inv_num_short = inv["invoice_number"][:15]
            b.button(
                text=f"💰 ЗП: №{inv_num_short}",
                callback_data=f"zamzp:start:{inv['id']}",
            )
            has_zp_buttons = True

    b.adjust(1)
    text = f"📋 <b>Мои замеры</b> ({len(active)}):\n\n" + "\n".join(lines)

    if has_zp_buttons:
        text += "\n\n💰 Нажмите для расчёта ЗП:"
        await message.answer(text, reply_markup=b.as_markup())
    else:
        await message.answer(text)


# =====================================================================
# РАСЧЁТ ЗП ЗАМЕРЩИКА (ZameryZpSG) — Дополнение 3
# =====================================================================

@router.callback_query(F.data.startswith("zamzp:start:"))
async def zamery_zp_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Start ZP calculation for a specific invoice."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(ZameryZpSG.cost_per_zamery)
    await state.update_data(
        invoice_id=invoice_id,
        invoice_number=inv["invoice_number"],
        address=inv.get("object_address", "-"),
    )

    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Расчёт ЗП — Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n\n"
        "Введите <b>стоимость замера</b> (число, ₽):\n"
        "Для отмены: <code>/cancel</code>",
    )


@router.message(ZameryZpSG.cost_per_zamery)
async def zamery_zp_cost(message: Message, state: FSMContext) -> None:
    """Zamery enters cost per measurement."""
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        cost = float(text)
        if cost <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите положительное число (стоимость замера в ₽):")
        return

    await state.update_data(cost_per_zamery=cost)
    await state.set_state(ZameryZpSG.all_same_price)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Да, все по одной цене", callback_data="zamzp:same:yes")
    b.button(text="❌ Нет, цены разные", callback_data="zamzp:same:no")
    b.adjust(1)

    await message.answer(
        f"Стоимость: <b>{cost:,.0f}₽</b>\n\n"
        "Все замеры в отчёте оплачиваются по этой цене?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("zamzp:same:"))
async def zamery_zp_same_price(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    """Zamery answers: same or different prices."""
    await cb.answer()
    answer = cb.data.split(":")[-1]  # type: ignore[union-attr]

    if answer == "yes":
        # Все по одной цене — спрашиваем количество замеров
        await state.set_state(ZameryZpSG.confirm)
        await state.update_data(all_same=True)
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите <b>количество выполненных замеров</b> (целое число):"
        )
    else:
        # Разные цены — ввод каждого замера отдельно
        await state.set_state(ZameryZpSG.custom_prices)
        await state.update_data(all_same=False, custom_entries=[])
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите стоимость каждого замера отдельной строкой.\n\n"
            "Формат: <code>Описание — сумма</code>\n\n"
            "Пример:\n"
            "<code>ул. Ленина 5 — 3000</code>\n"
            "<code>ул. Мира 10 — 4500</code>\n\n"
            "Когда закончите, нажмите /done",
        )


@router.message(ZameryZpSG.confirm)
async def zamery_zp_confirm_count(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Same-price path: zamery enters count of measurements, then summary is sent to GD."""
    if not message.from_user:
        return

    data = await state.get_data()

    if not data.get("all_same"):
        # Shouldn't reach here if all_same=False, but handle gracefully
        await message.answer("Используйте /done для завершения ввода цен.")
        return

    text = (message.text or "").strip()
    try:
        count = int(text)
        if count <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите положительное целое число (количество замеров):")
        return

    cost = data["cost_per_zamery"]
    total = cost * count
    invoice_number = data["invoice_number"]
    invoice_id = data["invoice_id"]
    address = data.get("address", "-")

    details = [{"description": f"Замер x{count}", "cost": cost, "count": count}]

    # Сохраняем в БД
    await db.update_invoice(
        invoice_id,
        zp_zamery_details_json=json.dumps(details, ensure_ascii=False),
        zp_zamery_total=total,
    )
    await db.set_invoice_zp_status(invoice_id, "requested")

    # Отправляем ГД
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    summary = (
        f"💰 <b>Расчёт ЗП замерщика</b>\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {address}\n\n"
        f"Замеров: <b>{count}</b>\n"
        f"Цена за замер: <b>{cost:,.0f}₽</b>\n"
        f"<b>Итого: {total:,.0f}₽</b>\n\n"
        f"От: @{message.from_user.username or '-'}"
    )

    if gd_id:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Утвердить", callback_data=f"zamzp_approve:yes:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"zamzp_approve:no:{invoice_id}")
        b.adjust(1)
        await notifier.safe_send(int(gd_id), summary, reply_markup=b.as_markup())

    role = await _current_role(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Расчёт ЗП отправлен ГД.\n"
        f"Замеров: {count}, итого: {total:,.0f}₽",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set())),
        ),
    )


@router.message(ZameryZpSG.custom_prices)
async def zamery_zp_custom(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Custom-prices path: zamery enters individual measurement costs."""
    if not message.from_user:
        return

    text = (message.text or "").strip()

    # /done — завершить ввод
    if text == "/done":
        data = await state.get_data()
        entries: list[dict[str, Any]] = data.get("custom_entries", [])
        if not entries:
            await message.answer("Вы не ввели ни одного замера. Введите хотя бы один:")
            return

        invoice_id = data["invoice_id"]
        invoice_number = data["invoice_number"]
        address = data.get("address", "-")
        total = sum(e["cost"] for e in entries)

        # Сохраняем в БД
        await db.update_invoice(
            invoice_id,
            zp_zamery_details_json=json.dumps(entries, ensure_ascii=False),
            zp_zamery_total=total,
        )
        await db.set_invoice_zp_status(invoice_id, "requested")

        # Формируем карточку для ГД
        lines = []
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['description']} — {e['cost']:,.0f}₽")
        details_text = "\n".join(lines)

        summary = (
            f"💰 <b>Расчёт ЗП замерщика</b>\n\n"
            f"📄 Счёт №: <code>{invoice_number}</code>\n"
            f"📍 Адрес: {address}\n\n"
            f"Замеры:\n{details_text}\n\n"
            f"<b>Итого: {total:,.0f}₽</b>\n\n"
            f"От: @{message.from_user.username or '-'}"
        )

        gd_id = await resolve_default_assignee(db, config, Role.GD)
        if gd_id:
            b = InlineKeyboardBuilder()
            b.button(text="✅ Утвердить", callback_data=f"zamzp_approve:yes:{invoice_id}")
            b.button(text="❌ Отклонить", callback_data=f"zamzp_approve:no:{invoice_id}")
            b.adjust(1)
            await notifier.safe_send(int(gd_id), summary, reply_markup=b.as_markup())

        role = await _current_role(db, message.from_user.id)
        await state.clear()
        await message.answer(
            f"✅ Расчёт ЗП отправлен ГД.\n"
            f"Замеров: {len(entries)}, итого: {total:,.0f}₽",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set())),
            ),
        )
        return

    # Парсим строку: "описание — сумма" или просто число
    if "—" in text or " - " in text:
        sep = "—" if "—" in text else " - "
        parts = text.split(sep, 1)
        desc = parts[0].strip()
        cost_str = parts[1].strip().replace(",", ".").replace(" ", "")
    else:
        data = await state.get_data()
        entry_num = len(data.get("custom_entries", [])) + 1
        desc = f"Замер #{entry_num}"
        cost_str = text.replace(",", ".").replace(" ", "")

    try:
        cost = float(cost_str)
        if cost <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer(
            "Не удалось разобрать. Формат: <code>описание — сумма</code>\n"
            "Или просто число (сумма)."
        )
        return

    data = await state.get_data()
    entries = data.get("custom_entries", [])
    entries.append({"description": desc, "cost": cost})
    await state.update_data(custom_entries=entries)

    total_so_far = sum(e["cost"] for e in entries)
    await message.answer(
        f"✅ Принял: {desc} — {cost:,.0f}₽\n"
        f"Замеров: {len(entries)}, промежуточный итог: {total_so_far:,.0f}₽\n\n"
        "Введите следующий замер или нажмите /done для завершения."
    )


# =====================================================================
# GD УТВЕРЖДАЕТ / ОТКЛОНЯЕТ ЗП ЗАМЕРЩИКА
# =====================================================================

@router.callback_query(F.data.startswith("zamzp_approve:"))
async def zamery_zp_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """GD approves or rejects zamery ZP calculation."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    decision = parts[1]  # yes or no
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    zamery_id = await resolve_default_assignee(db, config, Role.ZAMERY)

    if decision == "yes":
        await db.set_invoice_zp_status(invoice_id, "approved")
        total = inv.get("zp_zamery_total", 0) or 0
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ ЗП замерщика по счёту №{inv['invoice_number']} утверждена.\n"
            f"Сумма: {total:,.0f}₽"
        )
        # Уведомляем замерщика
        if zamery_id:
            await notifier.safe_send(
                int(zamery_id),
                f"✅ <b>ЗП утверждена</b>\n\n"
                f"Счёт №: <code>{inv['invoice_number']}</code>\n"
                f"Сумма: {total:,.0f}₽",
            )
    else:
        await db.set_invoice_zp_status(invoice_id, "not_requested")
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ ЗП замерщика по счёту №{inv['invoice_number']} отклонена."
        )
        if zamery_id:
            await notifier.safe_send(
                int(zamery_id),
                f"❌ <b>ЗП отклонена</b>\n\n"
                f"Счёт №: <code>{inv['invoice_number']}</code>\n"
                "Свяжитесь с ГД для уточнения.",
            )
