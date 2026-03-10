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
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, Role, TaskStatus
from ..keyboards import (
    ACC_BTN_INVOICE_END,
    ACC_BTN_INVOICES_WORK,
    invoice_list_kb,
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
# ВХОДЯЩИЕ ЗАДАЧИ — обработчик перенесён в common.py (универсальный)
# =====================================================================


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


async def _show_acc_invoices_work(
    target: Message | CallbackQuery,
    db: Database,
) -> None:
    """Общий хелпер: показать список счетов в работе для бухгалтерии."""
    invoices = await db.list_invoices_in_work(limit=50, exclude_no_digit=True)

    if not invoices:
        msg = target.message if isinstance(target, CallbackQuery) else target
        await msg.answer("✅ Нет счетов в работе.")  # type: ignore[union-attr]
        return

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
        b.button(text=label[:60], callback_data=f"acc_work:view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="acc_work:refresh")
    b.adjust(1)

    n_total = len(invoices)
    msg = target.message if isinstance(target, CallbackQuery) else target
    await msg.answer(  # type: ignore[union-attr]
        f"📊 <b>Счета в работе</b> ({n_total})\n\n"
        "Нажмите на счёт для просмотра / отправки запроса менеджеру:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "acc_work:refresh")
async def acc_invoices_work_refresh(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer("🔄 Обновлено")
    await _show_acc_invoices_work(cb, db)


@router.callback_query(F.data.regexp(r"^acc_work:view:\d+$"))
async def acc_invoices_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта в работе — бухгалтерия."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
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
        "on_hold": "⏸ Отложен", "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

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
        f"👤 Менеджер: {creator_label} ({creator_role_label})\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    b = InlineKeyboardBuilder()
    b.button(text="📨 Запрос менеджеру", callback_data=f"acc_work:req:{invoice_id}")
    b.button(text="📊 Себестоимость", callback_data=f"acc_cost:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="acc_work:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


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
            if a["file_type"] == "document":
                await notifier.bot.send_document(int(manager_id), a["file_id"])
            elif a["file_type"] == "photo":
                await notifier.bot.send_photo(int(manager_id), a["file_id"])
        except Exception:
            pass

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
    invoices = await db.list_invoices(status=InvoiceStatus.ENDED, limit=30, exclude_no_digit=True)
    if not invoices:
        await answer_service(message, "🏁 Нет закрытых счетов.", delay_seconds=60)
        return
    await message.answer(
        f"🏁 <b>Закрытые Счета</b> ({len(invoices)}):",
        reply_markup=invoice_list_kb(invoices, action_prefix="accinv"),
    )


@router.callback_query(F.data.startswith("accinv:view:"))
async def acc_invoice_view(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n"
        f"📊 Статус: 🏁 Закрытые Счета\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )
    b = InlineKeyboardBuilder()
    b.button(text="📊 Себестоимость", callback_data=f"acc_cost:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^acc_cost:\d+$"))
async def acc_invoice_cost(cb: CallbackQuery, db: Database) -> None:
    """Бухгалтерия: карточка себестоимости по закрытому счёту."""
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return
    cost = await db.get_full_invoice_cost_card(inv_id)
    from ..utils import format_cost_card
    await cb.message.answer(format_cost_card(inv, cost))  # type: ignore[union-attr]


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
