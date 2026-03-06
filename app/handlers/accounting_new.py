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
    invoice_list_kb,
    main_menu,
)
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import EdoResponseSG
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
# ЗАКРЫТЫЕ СЧЕТА (list ended invoices)
# =====================================================================

@router.message(F.text == ACC_BTN_INVOICE_END)
async def acc_invoice_end(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.ENDED, limit=30)
    if not invoices:
        await message.answer("🏁 Нет закрытых счетов.")
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
    await cb.message.answer(text)  # type: ignore[union-attr]


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
