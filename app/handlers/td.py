from __future__ import annotations

import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import ProjectCb
from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import GD_BTN_INVOICE_END_GD, main_menu, projects_kb, tasks_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..states import SupplierPaymentSG
from ..utils import answer_service, fmt_project_card, parse_amount, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


# ==================== СЧЁТ END (объединяет подтверждение оплат + Счет End) ====================

@router.message(F.text == GD_BTN_INVOICE_END_GD)
async def gd_invoice_end_combined(message: Message, db: Database) -> None:
    """Show both PAYMENT_CONFIRM and INVOICE_END_REQUEST tasks for GD."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    tasks_pc = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.PAYMENT_CONFIRM)
    tasks_ie = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.INVOICE_END_REQUEST)
    tasks = tasks_pc + tasks_ie
    tasks.sort(key=lambda t: t.get("created_at") or "", reverse=True)
    if not tasks:
        await answer_service(message, "✅ Нет задач «Счёт END» и подтверждений оплат.", delay_seconds=60)
        return
    n_pc = len(tasks_pc)
    n_ie = len(tasks_ie)
    parts = []
    if n_pc:
        parts.append(f"💰 Подтв.оплат: {n_pc}")
    if n_ie:
        parts.append(f"🏁 Счёт End: {n_ie}")
    summary = " | ".join(parts)
    await message.answer(
        f"🏁 <b>Счёт END</b> ({len(tasks)})\n{summary}\n\n"
        "Выберите задачу:",
        reply_markup=tasks_kb(tasks),
    )


# ==================== ОПЛАТА ПОСТАВЩИКУ (ТД/Сергей -> поставщик) ====================

@router.message(F.text == "💸 Оплата поставщику")
async def start_supplier_payment(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(SupplierPaymentSG.project)
    await message.answer(
        "💸 <b>Оплата поставщику</b>\n"
        "Шаг 1/6: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="suppl_pay"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "suppl_pay"))
async def supplier_pay_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(SupplierPaymentSG.supplier)
    await cb.message.answer("Поставщик (название компании):")  # type: ignore


@router.message(SupplierPaymentSG.supplier)
async def supplier_pay_supplier(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 2:
        await message.answer("Укажите название поставщика:")
        return
    await state.update_data(supplier=t)
    await state.set_state(SupplierPaymentSG.amount)
    await message.answer("Сумма оплаты (например 50000 или 50k):")


@router.message(SupplierPaymentSG.amount)
async def supplier_pay_amount(message: Message, state: FSMContext) -> None:
    amount = parse_amount((message.text or "").strip())
    if amount is None:
        await message.answer("Не понял сумму. Пример: 50000 или 50k.")
        return
    await state.update_data(amount=amount)
    await state.set_state(SupplierPaymentSG.invoice_number)
    await message.answer("Номер счёта поставщика (или «-»):")


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
    b.adjust(1)
    await message.answer(
        "Приложите платёжное поручение / скрин оплаты (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.message(SupplierPaymentSG.attachments)
async def supplier_pay_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить ПП».")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "supplpay:create")
async def supplier_pay_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.GD]):
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

    amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
    msg = (
        "💸 <b>Оплата поставщику произведена</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"🏭 Поставщик: <b>{supplier}</b>\n"
        f"💰 Сумма: <b>{amount_s}</b>\n"
    )
    if invoice_number:
        msg += f"🧾 Счёт №: <b>{invoice_number}</b>\n"
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
                isolated_role=isolated_role,
            ),
        ),
    )  # type: ignore
    await state.clear()
