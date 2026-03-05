"""
Handlers for Installer (Монтажник) role — new menu.

Covers:
- Заказ материалов (ORDER_MATERIALS to RP)
- Счет ок (InstallerInvoiceOkSG)
- Заказ доп.материалов (InstallerOrderMaterialsSG)
- Мои объекты (list invoices)
- Отчёт за день (InstallerDailyReportSG — text to RP via chat-proxy)
- В Работу (accept tasks from RP)
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
    INST_BTN_DAILY_REPORT,
    INST_BTN_IN_WORK,
    INST_BTN_INVOICE_OK,
    INST_BTN_MY_OBJECTS,
    INST_BTN_ORDER_EXTRA,
    INST_BTN_ORDER_MAT,
    invoice_list_kb,
    main_menu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.notifier import Notifier
from ..states import (
    InstallerDailyReportSG,
    InstallerInvoiceOkSG,
    InstallerOrderMaterialsSG,
)
from ..utils import get_initiator_label, private_only_reply_markup, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return user.role if user else None


# =====================================================================
# ЗАКАЗ МАТЕРИАЛОВ (to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_MAT)
async def start_order_materials(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    await state.set_state(InstallerOrderMaterialsSG.description)
    await message.answer(
        "📦 <b>Заказ материалов</b>\n\n"
        "Шаг 1/3: Опишите, какие материалы нужны (объект, размеры и т.д.).\n"
        "Для отмены: <code>/cancel</code>."
    )


@router.message(InstallerOrderMaterialsSG.description)
async def order_mat_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(InstallerOrderMaterialsSG.comment)
    await message.answer("Шаг 2/3: Добавьте <b>комментарий</b> (или «—» для пропуска):")


@router.message(InstallerOrderMaterialsSG.comment)
async def order_mat_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""
    await state.update_data(comment=comment, attachments=[])
    await state.set_state(InstallerOrderMaterialsSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить РП", callback_data="inst_order:create")
    b.button(text="⏭ Без вложений", callback_data="inst_order:create")
    b.adjust(1)
    await message.answer(
        "Шаг 3/3: Прикрепите фото/документы с размерами или нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerOrderMaterialsSG.attachments)
async def order_mat_attachments(message: Message, state: FSMContext) -> None:
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
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "inst_order:create")
async def order_mat_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    description = data["description"]
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ РП не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    task = await db.create_task(
        project_id=None,
        type_=TaskType.ORDER_MATERIALS,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(rp_id),
        due_at_iso=None,
        payload={
            "description": description,
            "comment": comment,
            "source": "installer",
            "sender_id": u.id,
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

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📦 <b>Заказ материалов от монтажника</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

    from ..keyboards import task_actions_kb
    await notifier.safe_send(int(rp_id), msg, reply_markup=task_actions_kb(task))
    for a in attachments:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    role = await _current_role(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Заказ материалов отправлен РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set())),
        ),
    )


# =====================================================================
# ЗАКАЗ ДОП.МАТЕРИАЛОВ (same as above, to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_EXTRA)
async def start_order_extra(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    await state.set_state(InstallerOrderMaterialsSG.description)
    await message.answer(
        "📦 <b>Заказ доп.материалов</b>\n\n"
        "Опишите, что нужно (объект, материалы, размеры).\n"
        "Для отмены: <code>/cancel</code>."
    )


# =====================================================================
# СЧЕТ ОК (InstallerInvoiceOkSG)
# =====================================================================

@router.message(F.text == INST_BTN_INVOICE_OK)
async def start_invoice_ok(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()

    # Show invoices in IN_PROGRESS state assigned to this installer
    user_id = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_invoices(assigned_to=user_id, status=InvoiceStatus.IN_PROGRESS)
    if not invoices:
        await message.answer("Нет счетов «В работе» для подтверждения.")
        return

    await state.set_state(InstallerInvoiceOkSG.select_invoice)
    await message.answer(
        "✅ <b>Счет ОК</b>\n\n"
        "Выберите счёт, по которому работы выполнены:",
        reply_markup=invoice_list_kb(invoices, action_prefix="instok"),
    )


@router.callback_query(F.data.startswith("instok:view:"))
async def invoice_ok_select(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.update_data(invoice_id=invoice_id)
    await state.set_state(InstallerInvoiceOkSG.comment)

    await cb.message.answer(  # type: ignore[union-attr]
        f"Счёт №{inv['invoice_number']} — подтверждение выполнения.\n"
        "Добавьте <b>комментарий</b> (или «—»):"
    )


@router.message(InstallerInvoiceOkSG.comment)
async def invoice_ok_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    invoice_id = data["invoice_id"]

    # Set installer_ok condition
    await db.set_invoice_installer_ok(invoice_id, True)

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        await state.clear()
        return

    # Create task
    task = await db.create_task(
        project_id=None,
        type_=TaskType.INSTALLER_INVOICE_OK,
        status=TaskStatus.DONE,
        created_by=message.from_user.id,
        assigned_to=inv.get("created_by", 0),
        due_at_iso=None,
        payload={
            "invoice_id": invoice_id,
            "invoice_number": inv["invoice_number"],
            "comment": comment,
            "installer_id": message.from_user.id,
        },
    )

    initiator = await get_initiator_label(db, message.from_user.id)
    msg = (
        f"✅ <b>Монтажник — Счет ОК</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Счёт №{inv['invoice_number']}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

    # Notify manager + RP
    manager_id = inv.get("created_by")
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    for target in [manager_id, rp_id]:
        if target:
            await notifier.safe_send(int(target), msg)

    role = await _current_role(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Подтверждение отправлено по счёту №{inv['invoice_number']}.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set())),
        ),
    )


# =====================================================================
# МОИ ОБЪЕКТЫ (list invoices)
# =====================================================================

@router.message(F.text == INST_BTN_MY_OBJECTS)
async def installer_my_objects(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return

    # Show all invoices assigned to this installer with status IN_PROGRESS, PAID, ENDED
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
    for inv in active[:20]:
        zp = inv.get("zp_status", "not_requested")
        zp_emoji = "✅" if zp == "approved" else "⏳"
        status_emoji = {
            "in_progress": "🔄", "paid": "✅",
            "closing": "📌", "ended": "🏁",
        }.get(inv["status"], "❓")
        lines.append(
            f"{status_emoji} №{inv['invoice_number']} — {inv.get('object_address', '-')[:30]} "
            f"[ЗП: {zp_emoji}]"
        )

    text = f"📌 <b>Мои объекты</b> ({len(active)}):\n\n" + "\n".join(lines)
    await message.answer(text)


# =====================================================================
# ОТЧЁТ ЗА ДЕНЬ (text to RP via chat-proxy)
# =====================================================================

@router.message(F.text == INST_BTN_DAILY_REPORT)
async def start_daily_report(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    await state.set_state(InstallerDailyReportSG.text)
    await message.answer(
        "📝 <b>Отчёт за день</b>\n\n"
        "Заполните:\n"
        "• Объект\n"
        "• Что сделано\n"
        "• Проблемы\n"
        "• Простой\n\n"
        "Напишите одним сообщением:"
    )


@router.message(InstallerDailyReportSG.text)
async def daily_report_text(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("Напишите подробнее:")
        return
    await state.update_data(text=text, attachments=[])
    await state.set_state(InstallerDailyReportSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить РП", callback_data="inst_report:send")
    b.button(text="⏭ Без вложений", callback_data="inst_report:send")
    b.adjust(1)
    await message.answer(
        "Прикрепите фото/файлы или нажмите «Отправить РП»:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerDailyReportSG.attachments)
async def daily_report_attachments(message: Message, state: FSMContext) -> None:
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
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "inst_report:send")
async def daily_report_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    text = data["text"]
    attachments = data.get("attachments", [])

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ РП не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Save as chat message
    await db.save_chat_message(
        channel="montazh",
        sender_id=u.id,
        direction="outgoing",
        text=f"[Отчёт за день]\n{text}",
        receiver_id=int(rp_id),
        has_attachment=bool(attachments),
    )

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📝 <b>Отчёт за день от монтажника</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{text}\n"
    )

    await notifier.safe_send(int(rp_id), msg)
    for a in attachments:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    role = await _current_role(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Отчёт отправлен РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set())),
        ),
    )


# =====================================================================
# В РАБОТУ (accept tasks from RP)
# =====================================================================

@router.message(F.text == INST_BTN_IN_WORK)
async def installer_in_work(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    new_tasks = [t for t in tasks if t.get("status") == TaskStatus.OPEN]
    if not new_tasks:
        await message.answer("🔨 Нет новых задач для принятия ✅")
        return
    await message.answer(
        f"🔨 <b>В Работу</b> ({len(new_tasks)}):\n\n"
        "Нажмите на задачу для просмотра и принятия:",
        reply_markup=tasks_kb(new_tasks),
    )
