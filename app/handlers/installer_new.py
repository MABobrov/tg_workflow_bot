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

import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, MontazhStage, Role, TaskStatus, TaskType
from ..keyboards import (
    INST_BTN_DAILY_REPORT,
    INST_BTN_IN_WORK,
    INST_BTN_INVOICE_OK,
    INST_BTN_MY_OBJECTS,
    INST_BTN_ORDER_EXTRA,
    INST_BTN_ORDER_MAT,
    INST_BTN_RAZMERY_OK,
    INST_BTN_ZP,
    invoice_list_kb,
    main_menu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import (
    InstallerDailyReportSG,
    InstallerInvoiceOkSG,
    InstallerOrderMaterialsSG,
    InstallerRazmerySG,
    InstallerZpSG,
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
# ЗАКАЗ МАТЕРИАЛОВ (to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_MAT)
async def start_order_materials(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    invoices = await db.list_installer_confirmed_invoices(message.from_user.id)
    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"№{num} — {addr}",
            callback_data=f"inst_order_inv:{inv['id']}",
        )
    b.button(text="⏩ Без привязки", callback_data="inst_order_inv:skip")
    b.adjust(1)
    await state.set_state(InstallerOrderMaterialsSG.invoice_pick)
    await message.answer(
        "📦 <b>Заказ материалов</b>\n\n"
        "Выберите счёт для привязки заказа или пропустите:\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(
    InstallerOrderMaterialsSG.invoice_pick,
    lambda cb: cb.data and cb.data.startswith("inst_order_inv:"),
)
async def order_mat_pick_invoice(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    invoice_id = None if val == "skip" else int(val)
    await state.update_data(invoice_id=invoice_id)
    await state.set_state(InstallerOrderMaterialsSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 1/3: Опишите, какие материалы нужны (объект, размеры и т.д.)."
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
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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
            "invoice_id": data.get("invoice_id"),
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
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Заказ материалов отправлен РП.",
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


# =====================================================================
# ЗАКАЗ ДОП.МАТЕРИАЛОВ (same as above, to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_EXTRA)
async def start_order_extra(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    invoices = await db.list_installer_confirmed_invoices(message.from_user.id)  # type: ignore[union-attr]
    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"№{num} — {addr}",
            callback_data=f"inst_order_inv:{inv['id']}",
        )
    b.button(text="⏩ Без привязки", callback_data="inst_order_inv:skip")
    b.adjust(1)
    await state.set_state(InstallerOrderMaterialsSG.invoice_pick)
    await message.answer(
        "📦 <b>Заказ доп.материалов</b>\n\n"
        "Выберите счёт для привязки или пропустите:\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=b.as_markup(),
    )


# =====================================================================
# СЧЕТ ОК (InstallerInvoiceOkSG)
# =====================================================================

@router.message(F.text == INST_BTN_INVOICE_OK)
async def start_invoice_ok(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()

    user_id = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_installer_confirmed_invoices(user_id)
    if not invoices:
        await answer_service(message, "Нет подтверждённых счетов «В работе».", delay_seconds=60)
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

    # Update montazh stage → invoice_ok
    from ..enums import MontazhStage
    await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_OK)

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        await state.clear()
        return

    # Create task
    await db.create_task(
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

    # Notify manager + RP (deduplicated to avoid double-sending when same person)
    manager_id = inv.get("created_by")
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    seen_targets: set[int] = set()
    for target in [manager_id, rp_id]:
        if target and int(target) not in seen_targets:
            seen_targets.add(int(target))
            await notifier.safe_send(int(target), msg)
            await refresh_recipient_keyboard(notifier, db, config, int(target))

    role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Подтверждение отправлено по счёту №{inv['invoice_number']}.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# РАЗМЕРЫ ОК — workflow проверки размеров стекла
# =====================================================================

@router.message(F.text == INST_BTN_RAZMERY_OK)
async def start_razmery_ok(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка «Размеры ОК»: два раздела — отправить бланк / проверить форму."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    user_id = message.from_user.id  # type: ignore[union-attr]

    # Счета in_work БЕЗ активного razmery_request → можно отправить бланк
    confirmed = await db.list_installer_confirmed_invoices(user_id)
    send_list = []
    check_list = []
    for inv in confirmed:
        stage = inv.get("montazh_stage", "")
        if stage != "in_work":
            continue
        req = await db.get_active_razmery_request(inv["id"])
        if not req:
            send_list.append(inv)
        elif req["status"] == "verification_sent":
            check_list.append((inv, req))

    if not send_list and not check_list:
        await answer_service(message, "📐 Нет счетов для отправки размеров.", delay_seconds=60)
        return

    b = InlineKeyboardBuilder()
    if send_list:
        for inv in send_list:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "")[:20]
            b.button(
                text=f"📤 №{num} — {addr}"[:55],
                callback_data=f"razmok_new:send:{inv['id']}",
            )
    if check_list:
        for inv, req in check_list:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "")[:20]
            b.button(
                text=f"📋 №{num} — проверить"[:55],
                callback_data=f"razmok_new:check:{req['id']}",
            )
    b.adjust(1)

    text = "📐 <b>Размеры ОК</b>\n\n"
    if send_list:
        text += f"📤 Отправить бланк ({len(send_list)})\n"
    if check_list:
        text += f"📋 На проверке ({len(check_list)})\n"
    await message.answer(text, reply_markup=b.as_markup())


# --- Шаг 1: отправка бланка размеров РП ---

@router.callback_query(F.data.startswith("razmok_new:send:"))
async def razmery_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.comment)
    await state.update_data(razmery_invoice_id=invoice_id, razmery_attachments=[])
    await cb.message.answer(  # type: ignore[union-attr]
        "📐 <b>Бланк размеров стекла</b>\n\n"
        "Добавьте комментарий к бланку размеров\n"
        "(или «-» для пропуска, «❌ Отмена» для отмены):",
    )


@router.message(InstallerRazmerySG.comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.result_comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.result_attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
async def razmery_cancel(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    u = message.from_user
    await state.clear()
    role, isolated_role = await _current_menu(db, u.id)  # type: ignore[union-attr]
    await message.answer(
        "❌ Отменено.",
        reply_markup=main_menu(
            role, is_admin=u.id in (config.admin_ids or set()),  # type: ignore[union-attr]
            unread=await db.count_unread_tasks(u.id),  # type: ignore[union-attr]
            isolated_role=isolated_role,
        ),
    )


@router.message(InstallerRazmerySG.comment)
async def razmery_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = None if text == "-" else text
    await state.update_data(razmery_comment=comment)
    await state.set_state(InstallerRazmerySG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить РП", callback_data="razmok_new:create")
    b.button(text="⏭ Без вложений", callback_data="razmok_new:create")
    b.adjust(1)
    await message.answer(
        "Прикрепите бланк размеров (фото/документ).\n"
        "Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerRazmerySG.attachments)
async def razmery_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("razmery_attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id})
    elif message.photo:
        attachments.append({"file_type": "photo", "file_id": message.photo[-1].file_id})
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(razmery_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "razmok_new:create")
async def razmery_send_to_rp(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Финализация: создать razmery_request + уведомить РП."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    invoice_id = data.get("razmery_invoice_id")
    comment = data.get("razmery_comment")
    attachments = data.get("razmery_attachments", [])

    req_id = await db.create_razmery_request(invoice_id, u.id, comment)

    inv = await db.get_invoice(invoice_id)
    inv_num = inv["invoice_number"] if inv else "?"
    initiator = await get_initiator_label(db, u.id)

    # Уведомить РП
    b = InlineKeyboardBuilder()
    b.button(text="✅ ОК (принял)", callback_data=f"razmok_rp:received:{req_id}")
    b.adjust(1)

    msg = (
        f"📐 <b>Бланк размеров стекла</b>\n"
        f"👤 От: {initiator}\n"
        f"🧾 Счёт: №{inv_num}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        await notifier.safe_send(int(rp_id), msg, reply_markup=b.as_markup())
        for a in attachments:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await state.clear()
    role, isolated_role = await _current_menu(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Бланк размеров отправлен РП по счёту №{inv_num}.",
        reply_markup=main_menu(
            role, is_admin=u.id in (config.admin_ids or set()),
            unread=await db.count_unread_tasks(u.id),
            isolated_role=isolated_role,
        ),
    )


# --- Шаг 3: проверка формы поставщика от РП ---

@router.callback_query(F.data.startswith("razmok_new:check:"))
async def razmery_check_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр формы поставщика от РП."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    rp_label = await get_initiator_label(db, req["rp_id"]) if req.get("rp_id") else "РП"

    text = (
        f"📐 <b>Проверка размеров</b>\n\n"
        f"🧾 Счёт: №{inv_num}\n"
        f"👤 Форма от: {rp_label}\n"
    )
    if req.get("rp_comment"):
        text += f"💬 {req['rp_comment']}\n"
    text += "\nПроверьте форму и выберите действие:"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Размеры ОК", callback_data=f"razmok_inst:ok:{req_id}")
    b.button(text="❌ Ошибка", callback_data=f"razmok_inst:error:{req_id}")
    b.adjust(2)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("razmok_inst:ok:"))
async def razmery_respond_ok(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.result_comment)
    await state.update_data(
        razmery_req_id=req_id, razmery_result="ok", razmery_result_attachments=[],
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ <b>Размеры ОК</b>\n\n"
        "Добавьте комментарий (или «-» для пропуска):",
    )


@router.callback_query(F.data.startswith("razmok_inst:error:"))
async def razmery_respond_error(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.result_comment)
    await state.update_data(
        razmery_req_id=req_id, razmery_result="error", razmery_result_attachments=[],
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ <b>Ошибка в размерах</b>\n\n"
        "Опишите ошибку (обязательно):",
    )


@router.message(InstallerRazmerySG.result_comment)
async def razmery_result_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    if data.get("razmery_result") == "error" and (not text or text == "-"):
        await message.answer("Опишите ошибку — комментарий обязателен:")
        return
    comment = None if text == "-" else text
    await state.update_data(razmery_result_comment=comment)
    await state.set_state(InstallerRazmerySG.result_attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить", callback_data="razmok_inst:result_send")
    b.button(text="⏭ Без вложений", callback_data="razmok_inst:result_send")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы (опционально). Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerRazmerySG.result_attachments)
async def razmery_result_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("razmery_result_attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id})
    elif message.photo:
        attachments.append({"file_type": "photo", "file_id": message.photo[-1].file_id})
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(razmery_result_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "razmok_inst:result_send")
async def razmery_result_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Финализация ответа: Размеры ОК или Ошибка."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    req_id = data.get("razmery_req_id")
    result = data.get("razmery_result", "ok")
    comment = data.get("razmery_result_comment")
    attachments = data.get("razmery_result_attachments", [])

    from ..utils import to_iso, utcnow
    now = to_iso(utcnow())

    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    initiator = await get_initiator_label(db, u.id)

    if result == "ok":
        await db.update_razmery_request(
            req_id, status="approved", result="ok",
            result_comment=comment, result_at=now,
        )
        await db.update_montazh_stage(req["invoice_id"], MontazhStage.RAZMERY_OK)

        rp_id = await resolve_default_assignee(db, config, Role.RP)
        if rp_id:
            msg = (
                f"✅ <b>Размеры ОК</b>\n"
                f"👤 От: {initiator}\n"
                f"🧾 Счёт: №{inv_num}\n"
                f"Размеры проверены ✅"
            )
            if comment:
                msg += f"\n💬 {comment}"
            await notifier.safe_send(int(rp_id), msg)
            for a in attachments:
                await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

        await state.clear()
        role, isolated_role = await _current_menu(db, u.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Размеры ОК подтверждены по счёту №{inv_num}.",
            reply_markup=main_menu(
                role, is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        )
    else:
        # Ошибка → возврат к rp_received (РП исправляет)
        await db.update_razmery_request(
            req_id, status="rp_received", result="error",
            result_comment=comment, result_at=now,
        )

        rp_id = await resolve_default_assignee(db, config, Role.RP)
        if rp_id:
            b = InlineKeyboardBuilder()
            b.button(
                text="📐 Отправить исправление",
                callback_data=f"razmok_rp:send_form:{req_id}",
            )
            b.adjust(1)
            msg = (
                f"❌ <b>Ошибка в размерах</b>\n"
                f"👤 От: {initiator}\n"
                f"🧾 Счёт: №{inv_num}\n"
                f"💬 {comment or '-'}"
            )
            await notifier.safe_send(int(rp_id), msg, reply_markup=b.as_markup())
            for a in attachments:
                await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

        await state.clear()
        role, isolated_role = await _current_menu(db, u.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Ошибка отправлена РП по счёту №{inv_num}.",
            reply_markup=main_menu(
                role, is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        )


# =====================================================================
# МОИ ОБЪЕКТЫ (list invoices)
# =====================================================================

@router.message(F.text == INST_BTN_MY_OBJECTS)
async def installer_my_objects(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    all_inv = [i for i in invoices if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
        InvoiceStatus.CLOSING, InvoiceStatus.ENDED,
    )]

    if not all_inv:
        await answer_service(message, "📌 Нет объектов.", delay_seconds=60)
        return

    in_work = [i for i in all_inv if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID, InvoiceStatus.CLOSING,
    )]
    ended = [i for i in all_inv if i["status"] == InvoiceStatus.ENDED]

    _STAGE_ORDER = {"in_work": 0, "razmery_ok": 1, "invoice_ok": 2}
    _STAGE_LABEL = {
        "in_work": "🔨 В работе",
        "razmery_ok": "📐 Размеры ОК",
        "invoice_ok": "✅ Счёт ОК",
        "none": "⏳ Ожидает",
    }

    def _fmt_line(inv: dict) -> str:
        zp = inv.get("zp_installer_status") or inv.get("zp_status", "not_requested")
        zp_emoji = "✅" if zp == "approved" else ("⏳" if zp == "requested" else "—")
        stage = inv.get("montazh_stage") or "none"
        stage_lbl = _STAGE_LABEL.get(stage, stage)
        return (
            f"• №{inv['invoice_number']} — "
            f"{inv.get('object_address', '-')[:25]}\n"
            f"  {stage_lbl} [ЗП: {zp_emoji}]"
        )

    text = f"📌 <b>Мои объекты</b> ({len(all_inv)})\n\n"

    if in_work:
        # Сортировка по montazh_stage
        in_work.sort(key=lambda i: _STAGE_ORDER.get(i.get("montazh_stage") or "none", 99))
        text += f"<b>🔄 В работе ({len(in_work)}):</b>\n"
        text += "\n".join(_fmt_line(i) for i in in_work[:15]) + "\n\n"

    if ended:
        text += f"<b>🏁 Счет End ({len(ended)}):</b>\n"
        text += "\n".join(_fmt_line(i) for i in ended[:10]) + "\n"

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
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Отчёт отправлен РП.",
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


# =====================================================================
# В РАБОТУ (accept tasks from RP)
# =====================================================================

@router.message(F.text == INST_BTN_IN_WORK)
async def installer_in_work(message: Message, state: FSMContext, db: Database) -> None:
    """Список неподтверждённых счетов для принятия в работу."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    user_id = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_installer_unconfirmed_invoices(user_id)

    if not invoices:
        await answer_service(message, "🔨 Нет новых счетов для принятия в работу ✅", delay_seconds=60)
        return

    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"📄 №{num} — {addr}"[:55],
            callback_data=f"inst_work:view:{inv['id']}",
        )
    b.adjust(1)

    await message.answer(
        f"🔨 <b>В Работу</b> ({len(invoices)})\n\n"
        "Счета, назначенные вам. Нажмите для просмотра и подтверждения:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("inst_work:view:"))
async def installer_work_view_card(
    cb: CallbackQuery, db: Database,
) -> None:
    """Карточка счёта для подтверждения «В работу»."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '—')}\n"
    )
    # Площадь (м²)
    area = inv.get("area_m2")
    if area:
        try:
            text += f"📐 Площадь: {float(area):,.1f} м²\n"
        except (ValueError, TypeError):
            pass
    # Расчётная стоимость монтажа (монтажнику показываем −30%)
    est_install = inv.get("estimated_installation")
    if est_install:
        try:
            text += f"🔧 Расч. стоимость монтажа: {float(est_install) * 0.7:,.0f}₽\n"
        except (ValueError, TypeError):
            pass
    text += f"📅 Создан: {(inv.get('created_at') or '—')[:10]}\n"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Ок (получил)", callback_data=f"inst_work:ack:{invoice_id}")
    b.button(text="🔨 В работу", callback_data=f"inst_work:confirm:{invoice_id}")
    b.adjust(2)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("inst_work:ack:"))
async def installer_work_acknowledge(cb: CallbackQuery, db: Database) -> None:
    """Подтверждение получения (мягкое, без смены статуса)."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer("✅ Принято")
    await cb.message.answer("✅ Получение счёта подтверждено.")  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("inst_work:confirm:"))
async def installer_work_confirm(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Монтажник подтверждает «В работу» → montazh_stage=IN_WORK."""
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

    await db.update_montazh_stage(invoice_id, MontazhStage.IN_WORK)

    # Уведомить РП
    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"🔨 <b>Монтажник — В работу</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Счёт №{inv['invoice_number']} принят в работу ✅"
    )
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Счёт №{inv['invoice_number']} принят в работу.",
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


# =====================================================================
# ЗАПРОС ЗП МОНТАЖНИКА (InstallerZpSG)
# =====================================================================

@router.message(F.text == INST_BTN_ZP)
async def installer_zp_start(message: Message, state: FSMContext, db: Database) -> None:
    """Show invoices eligible for ZP request (installer_ok=True, zp_installer_status='not_requested')."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    cur = await db.conn.execute(
        "SELECT * FROM invoices "
        "WHERE installer_ok = 1 "
        "  AND (zp_installer_status IS NULL OR zp_installer_status = 'not_requested') "
        "  AND assigned_to = ? "
        "  AND status NOT IN ('ended', 'rejected') "
        "ORDER BY id DESC LIMIT 20",
        (user_id,),
    )
    rows = await cur.fetchall()
    invoices = [dict(r) for r in rows]
    if not invoices:
        await message.answer("✅ Нет счетов, по которым можно запросить ЗП.")
        return
    b = InlineKeyboardBuilder()
    for inv in invoices:
        label = f"№{inv['invoice_number'] or '—'} / {(inv.get('object_address') or '—')[:30]}"
        b.button(text=label, callback_data=f"instzp:pick:{inv['id']}")
    b.adjust(1)
    await state.set_state(InstallerZpSG.select_invoice)
    await message.answer(
        "💰 <b>Запрос ЗП</b>\n\nВыберите счёт:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("instzp:pick:"), InstallerZpSG.select_invoice)
async def installer_zp_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.update_data(zp_invoice_id=invoice_id)
    await state.set_state(InstallerZpSG.amount)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 Счёт: <b>№{inv['invoice_number']}</b>\n"
        f"📍 Адрес: {inv.get('object_address') or '—'}\n\n"
        "Введите сумму ЗП (число):",
    )


@router.message(InstallerZpSG.amount)
async def installer_zp_amount(message: Message, state: FSMContext, db: Database) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите корректную сумму (положительное число):")
        return
    data = await state.get_data()
    invoice_id = data["zp_invoice_id"]
    inv = await db.get_invoice(invoice_id)
    await state.update_data(zp_amount=amount)
    await state.set_state(InstallerZpSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="instzp:confirm")
    b.button(text="❌ Отмена", callback_data="instzp:cancel")
    b.adjust(2)
    await message.answer(
        f"💰 <b>Подтверждение запроса ЗП</b>\n\n"
        f"🔢 Счёт: №{inv['invoice_number'] if inv else '—'}\n"
        f"💵 Сумма: {amount:,.0f}₽\n\n"
        "Отправить запрос ГД?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "instzp:cancel")
async def installer_zp_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("Отменено")
    await state.clear()
    await cb.message.answer("❌ Запрос ЗП отменён.")  # type: ignore[union-attr]


@router.callback_query(F.data == "instzp:confirm", InstallerZpSG.confirm)
async def installer_zp_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    invoice_id = data["zp_invoice_id"]
    amount = data["zp_amount"]

    # Update invoice
    await db.set_invoice_zp_installer_status(invoice_id, "requested", amount=amount, requested_by=u.id)

    inv = await db.get_invoice(invoice_id)
    inv_number = inv["invoice_number"] if inv else "—"

    # Create task for GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.ZP_INSTALLER,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=int(gd_id),
            due_at_iso=None,
            payload={
                "invoice_id": invoice_id,
                "invoice_number": inv_number,
                "amount": amount,
                "source": "installer_zp",
            },
        )
        initiator = await get_initiator_label(db, u.id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
        b.adjust(2)
        await notifier.safe_send(
            int(gd_id),
            f"💰 <b>Запрос ЗП монтажника</b>\n\n"
            f"👤 От: {initiator}\n"
            f"🔢 Счёт: №{inv_number}\n"
            f"📍 Адрес: {inv.get('object_address') or '—' if inv else '—'}\n"
            f"💵 Сумма: {amount:,.0f}₽",
            reply_markup=b.as_markup(),
        )
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос ЗП отправлен ГД.\n"
        f"Счёт: №{inv_number}, сумма: {amount:,.0f}₽",
    )
