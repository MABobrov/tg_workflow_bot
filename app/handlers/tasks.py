from __future__ import annotations

import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import TaskCb
from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import main_menu, manager_project_actions_kb, task_actions_kb
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import InvoicePaymentSG, MontazhCommentSG, SupplierPaymentSG, TaskCompleteSG
from ..utils import answer_service, fmt_task_card, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, task_type_label, try_json_loads

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _can_manage_task(cb: CallbackQuery, db: Database, config: Config, task: dict[str, Any]) -> bool:
    """Allow assigned user or admin."""
    if not cb.from_user:
        return False
    user_id = cb.from_user.id
    user = await db.get_user_optional(user_id)
    if user and not user.is_active:
        return False
    if user_id in (config.admin_ids or set()):
        return True
    assigned_to = task.get("assigned_to")
    if assigned_to and int(assigned_to) == user_id:
        return True
    return False


async def _current_role(db: Database, user_id: int) -> str | None:
    u = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, u.role if u else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


async def _maybe_mark_lead_tracking_response(db: Database, task: dict[str, Any] | None) -> None:
    if not task or task.get("type") != TaskType.LEAD_TO_PROJECT:
        return
    payload = try_json_loads(task.get("payload_json"))
    lead_id = payload.get("lead_id")
    try:
        lead_tracking_id = int(lead_id)
    except (TypeError, ValueError):
        return
    await db.update_lead_tracking_response(lead_tracking_id)


def _invoice_task_sender_id(payload: dict[str, Any]) -> int | None:
    sender_id = payload.get("sender_id") or payload.get("manager_id")
    if sender_id is None:
        return None
    try:
        return int(sender_id)
    except (TypeError, ValueError):
        return None


def _invoice_task_details(payload: dict[str, Any]) -> tuple[int | None, str, str, str]:
    invoice_id_raw = payload.get("invoice_id")
    try:
        invoice_id = int(invoice_id_raw) if invoice_id_raw is not None else None
    except (TypeError, ValueError):
        invoice_id = None

    invoice_number = str(payload.get("invoice_number") or "")
    supplier = str(payload.get("supplier") or "")
    amount = str(payload.get("amount") or "")
    return invoice_id, invoice_number, supplier, amount


def _task_take_text(task: dict[str, Any], project: dict[str, Any] | None) -> str:
    """Build a short human-readable confirmation for 'take in work'."""
    task_id = task.get("id")
    task_type = task.get("type")
    payload = try_json_loads(task.get("payload_json"))

    lines = [f"⏳ Взял в работу: #{task_id} — {task_type_label(task_type)}"]

    if project:
        code = str(project.get("code") or "").strip()
        title = str(project.get("title") or "").strip()
        project_label = " • ".join(part for part in (code, title) if part)
        if project_label:
            lines.append(f"📁 Проект: {project_label}")
        return "\n".join(lines)

    invoice_number = str(payload.get("invoice_number") or "").strip()
    if invoice_number:
        lines.append(f"📄 Счёт: {invoice_number}")

    address = str(payload.get("address") or payload.get("object_address") or "").strip()
    if address:
        lines.append(f"📍 Адрес: {address}")

    supplier = str(payload.get("supplier") or "").strip()
    if supplier:
        lines.append(f"🏢 Поставщик: {supplier}")

    comment = str(payload.get("comment") or payload.get("description") or "").strip()
    if comment:
        preview = comment if len(comment) <= 120 else f"{comment[:117]}..."
        lines.append(f"📝 {preview}")

    return "\n".join(lines)


@router.callback_query(TaskCb.filter())
async def task_actions(
    cb: CallbackQuery,
    callback_data: TaskCb,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    state: FSMContext,
) -> None:
    task_id = int(callback_data.task_id)
    action = callback_data.action

    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена или была удалена.", show_alert=True)
        return
    active_statuses = {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}

    if not await _can_manage_task(cb, db, config, task):
        await cb.answer("Эта задача назначена другому человеку", show_alert=True)
        return

    if action == "accept":
        if task.get("status") != TaskStatus.OPEN or task.get("accepted_at"):
            await cb.answer("Эта задача уже подтверждена или закрыта.", show_alert=True)
            return
        await db.accept_task(task_id)
        await cb.answer("✅ Принято")
        # Update the inline keyboard to remove the "Принято" button
        task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)
        if task:
            try:
                await cb.message.edit_reply_markup(reply_markup=task_actions_kb(task))
            except Exception:
                pass
        # Notify task creator
        created_by = task.get("created_by") if task else None
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(created_by),
                f"✅ Задача #{task_id} принята\n👤 Исполнитель: {initiator}"
            )
        return

    await cb.answer()

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None

    # OPEN: show card + actions
    if action == "open":
        text = fmt_task_card(task, project, config.timezone)
        await cb.message.answer(text, reply_markup=task_actions_kb(task))  # type: ignore

        # send attachments, if any
        attaches = await db.list_attachments(task_id)
        if attaches:
            await cb.message.answer(f"📎 Вложения: {len(attaches)}")  # type: ignore
            for a in attaches[:10]:
                await notifier.safe_send_media(cb.from_user.id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        return

    # TAKE
    if action == "take":
        status = task.get("status")
        if status == TaskStatus.IN_PROGRESS:
            await cb.answer("Эта задача уже взята в работу.", show_alert=True)
            return
        if status not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        if not task.get("accepted_at"):
            await db.accept_task(task_id)
            task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        try:
            await cb.message.edit_reply_markup(reply_markup=task_actions_kb(task))  # type: ignore[union-attr]
        except Exception:
            pass
        await answer_service(cb.message, _task_take_text(task, project))  # type: ignore[arg-type]
        return

    # REJECT
    if action == "reject":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        await _maybe_mark_lead_tracking_response(db, task)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        _uid = cb.from_user.id if cb.from_user else 0
        await cb.message.answer(
            "❌ Задача отклонена.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    (await _current_role(db, _uid)) if cb.from_user else None,
                    is_admin=bool(cb.from_user and _uid in (config.admin_ids or set())),
                    unread=await db.count_unread_tasks(_uid),
                ),
            ),
        )  # type: ignore

        # notify creator
        created_by = task.get("created_by")
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(created_by),
                f"❌ Ваша задача #{task_id} отклонена\n"
                f"👤 Исполнитель: {initiator}",
            )
        return

    # PAYMENT CONFIRM actions (TD)
    if action in {"pay_ok", "pay_need"} and task.get("type") == TaskType.PAYMENT_CONFIRM:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        if not project:
            await cb.message.answer("Проект не найден для этой задачи.")  # type: ignore
            return

        payload = try_json_loads(task.get("payload_json"))
        manager_id = payload.get("manager_id") or project.get("manager_id")
        rp_id = project.get("rp_id") or (await db.get_project_rp_id(int(project["id"])))

        if action == "pay_ok":
            task = await db.update_task_status(task_id, TaskStatus.DONE)
            project = await db.update_project_status(int(project["id"]), ProjectStatus.IN_WORK)

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "✅ <b>Оплата подтверждена</b> — можно запускать закупки и монтаж.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('code','')} • {project.get('title','')}"
            )
            if manager_id:
                await notifier.safe_send(
                    int(manager_id),
                    text,
                    reply_markup=manager_project_actions_kb(int(project["id"])),
                )
            if rp_id:
                await notifier.safe_send(int(rp_id), text)
            await notifier.notify_workchat(text)

        else:
            task = await db.update_task_status(task_id, TaskStatus.REJECTED)
            project = await db.update_project_status(int(project["id"]), ProjectStatus.WAITING_PAYMENT)

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "⚠️ <b>Оплата не подтверждена</b>: нужна доплата/уточнение.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('code','')} • {project.get('title','')}"
            )
            if manager_id:
                await notifier.safe_send(
                    int(manager_id),
                    text,
                    reply_markup=manager_project_actions_kb(int(project["id"])),
                )
            if rp_id:
                await notifier.safe_send(int(rp_id), text)
            await notifier.notify_workchat(text)

        await integrations.sync_project(project, manager_label="")
        await integrations.sync_task(task, project_code=project.get("code", ""))
        role_now = (await _current_role(db, cb.from_user.id)) if cb.from_user else Role.GD

        await state.clear()
        await cb.message.answer(
            "Готово.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    role_now,
                    is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set())),
                    unread=await db.count_unread_tasks(cb.from_user.id) if cb.from_user else 0,
                ),
            ),
        )  # type: ignore
        return

    # ORDER actions (TD) -> open supplier payment flow with project preselected
    if action == "pay_supplier" and task.get("type") in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS}:
        if not project:
            await cb.message.answer("Проект не найден для этой задачи.")  # type: ignore
            return
        await state.clear()
        await state.update_data(project_id=int(project["id"]), source_order_task_id=int(task_id))
        await state.set_state(SupplierPaymentSG.supplier)
        await cb.message.answer(
            "💸 <b>Оплата поставщику</b>\n"
            f"Проект: <b>{project.get('code','')} • {project.get('title','')}</b>\n\n"
            "Укажите поставщика (название компании):"
        )  # type: ignore
        return

    # INVOICE_PAYMENT — подтверждение получения (GD)
    if action == "inv_received" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") != TaskStatus.OPEN:
            await cb.answer("Этот счёт уже подтверждён.", show_alert=True)
            return
        # OPEN -> IN_PROGRESS
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        payload = try_json_loads(task.get("payload_json"))
        invoice_id, invoice_number, supplier, amount = _invoice_task_details(payload)
        if invoice_id is not None:
            await db.update_invoice_status(invoice_id, InvoiceStatus.IN_PROGRESS)
        # Уведомить отправителя (РП)
        sender_id = _invoice_task_sender_id(payload)
        if sender_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                "✅ <b>Счёт получен ГД</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        # Показать обновлённую карточку с новыми кнопками (Оплатить/Отложить/Отклонить)
        _uid_rcv = cb.from_user.id if cb.from_user else 0
        card_text = fmt_task_card(task, project)
        kb = task_actions_kb(task_id, TaskType.INVOICE_PAYMENT, TaskStatus.IN_PROGRESS)
        await cb.message.answer(  # type: ignore
            f"✅ Получение подтверждено.\n\n{card_text}",
            reply_markup=kb,
        )
        # Обновить main_menu (badge counters)
        await cb.message.answer(  # type: ignore
            "📋 Счёт принят в работу.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(Role.GD, is_admin=bool(cb.from_user and _uid_rcv in (config.admin_ids or set())),
                           unread=await db.count_unread_tasks(_uid_rcv), unread_channels=await db.count_unread_by_channel(_uid_rcv), gd_inbox_unread=await db.count_gd_inbox_tasks(_uid_rcv), gd_invoice_unread=await db.count_gd_invoice_tasks(_uid_rcv), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(_uid_rcv)),
            ),
        )
        return

    # INVOICE_PAYMENT actions (GD)
    if action == "inv_pay" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        # GD wants to pay — ask for payment order attachment
        await state.clear()
        await state.set_state(InvoicePaymentSG.attaching_pp)
        await state.update_data(invoice_task_id=task_id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ Отправить ПП", callback_data=f"inv_pp_done:{task_id}")
        b.button(text="❌ Отмена", callback_data=f"inv_pp_cancel:{task_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore
            "💳 <b>Оплата счёта</b>\n\n"
            "Прикрепите платёжное поручение (файл/фото).\n"
            "Когда готовы — нажмите «✅ Отправить ПП».",
            reply_markup=b.as_markup(),
        )
        return

    if action == "inv_hold" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        # Mark as in_progress (on hold)
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        payload = try_json_loads(task.get("payload_json"))
        invoice_id, invoice_number, supplier, amount = _invoice_task_details(payload)
        if invoice_id is not None:
            await db.update_invoice_status(invoice_id, InvoiceStatus.ON_HOLD)
        sender_id = _invoice_task_sender_id(payload)
        if sender_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                "⏸ <b>Счёт отложен</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        _uid_hold = cb.from_user.id if cb.from_user else 0
        await cb.message.answer(  # type: ignore
            "⏸ Счёт отложен.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(Role.GD, is_admin=bool(cb.from_user and _uid_hold in (config.admin_ids or set())),
                           unread=await db.count_unread_tasks(_uid_hold), unread_channels=await db.count_unread_by_channel(_uid_hold), gd_inbox_unread=await db.count_gd_inbox_tasks(_uid_hold), gd_invoice_unread=await db.count_gd_invoice_tasks(_uid_hold), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(_uid_hold)),
            ),
        )
        return

    if action == "inv_reject" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        payload = try_json_loads(task.get("payload_json"))
        invoice_id, invoice_number, supplier, amount = _invoice_task_details(payload)
        if invoice_id is not None:
            await db.update_invoice_status(invoice_id, InvoiceStatus.REJECTED)
        sender_id = _invoice_task_sender_id(payload)
        if sender_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                "❌ <b>Счёт отклонён</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        _uid_rej = cb.from_user.id if cb.from_user else 0
        await cb.message.answer(  # type: ignore
            "❌ Счёт отклонён. РП уведомлён.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(Role.GD, is_admin=bool(cb.from_user and _uid_rej in (config.admin_ids or set())),
                           unread=await db.count_unread_tasks(_uid_rej), unread_channels=await db.count_unread_by_channel(_uid_rej), gd_inbox_unread=await db.count_gd_inbox_tasks(_uid_rej), gd_invoice_unread=await db.count_gd_invoice_tasks(_uid_rej), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(_uid_rej)),
            ),
        )
        return

    # MONTAZH — подтверждение задачи (Да/Нет/Комментарий)
    if action == "montazh_yes" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.DONE)
        payload = try_json_loads(task.get("payload_json"))
        comment_text = payload.get("comment", "")
        gd_id = task.get("created_by")
        user_label = await get_initiator_label(db, cb.from_user.id)
        if gd_id:
            await notifier.safe_send(
                int(gd_id),
                f"✅ <b>Задача подтверждена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}\n\n"
                f"📋 {comment_text}" if comment_text else
                f"✅ <b>Задача подтверждена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(gd_id))
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        await cb.message.edit_text(  # type: ignore[union-attr]
            "✅ Задача подтверждена.",
        )
        return

    if action == "montazh_no" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        payload = try_json_loads(task.get("payload_json"))
        comment_text = payload.get("comment", "")
        gd_id = task.get("created_by")
        user_label = await get_initiator_label(db, cb.from_user.id)
        if gd_id:
            await notifier.safe_send(
                int(gd_id),
                f"❌ <b>Задача отклонена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}\n\n"
                f"📋 {comment_text}" if comment_text else
                f"❌ <b>Задача отклонена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(gd_id))
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        await cb.message.edit_text(  # type: ignore[union-attr]
            "❌ Задача отклонена.",
        )
        return

    if action == "montazh_comment" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        await state.clear()
        await state.set_state(MontazhCommentSG.text)
        await state.update_data(montazh_task_id=task_id)
        await cb.message.answer(  # type: ignore[union-attr]
            "💬 Введите комментарий к задаче:",
        )
        await cb.answer()
        return

    # DONE (generic)
    if action == "done":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        # For request/closing tasks we can optionally collect and send attachments to manager
        if task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST, TaskType.CLOSING_DOCS} and project:
            target_user_id = project.get("manager_id")
            if task.get("type") == TaskType.CLOSING_DOCS:
                target_user_id = project.get("manager_id")
            await state.clear()
            await state.set_state(TaskCompleteSG.attachments)
            await state.update_data(task_id=task_id, target_user_id=target_user_id)

            b = InlineKeyboardBuilder()
            b.button(text="✅ Отправить и закрыть", callback_data="taskcomplete:send")
            b.button(text="⏭ Закрыть без отправки", callback_data="taskcomplete:skip")
            b.adjust(1)

            await cb.message.answer(
                "Пришлите готовые документы (файлы/фото) несколькими сообщениями.\n"
                "Когда закончите — нажмите «✅ Отправить и закрыть».\n"
                "Или можно «⏭ Закрыть без отправки».",
                reply_markup=b.as_markup(),
            )  # type: ignore
            return

        # simple close
        task = await db.update_task_status(task_id, TaskStatus.DONE)
        await _maybe_mark_lead_tracking_response(db, task)

        # project status transitions
        if project and task.get("type") == TaskType.ISSUE:
            # issue solved: no status change
            pass
        if project and task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
            project = await db.update_project_status(int(project["id"]), ProjectStatus.INVOICE_SENT)
            await integrations.sync_project(project)
        if project and task.get("type") in {TaskType.CLOSING_DOCS, TaskType.PROJECT_END}:
            project = await db.update_project_status(int(project["id"]), ProjectStatus.ARCHIVE)
            await integrations.sync_project(project)

        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        _uid_done = cb.from_user.id if cb.from_user else 0
        await cb.message.answer(
            "✅ Закрыл задачу.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    (await _current_role(db, _uid_done)) if cb.from_user else None,
                    is_admin=bool(cb.from_user and _uid_done in (config.admin_ids or set())),
                    unread=await db.count_unread_tasks(_uid_done),
                ),
            ),
        )  # type: ignore
        return


@router.message(TaskCompleteSG.attachments)
async def taskcomplete_collect(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append(
            {
                "file_type": "document",
                "file_id": message.document.file_id,
                "file_unique_id": message.document.file_unique_id,
                "caption": message.caption,
            }
        )
    elif message.photo:
        ph = message.photo[-1]
        attachments.append(
            {
                "file_type": "photo",
                "file_id": ph.file_id,
                "file_unique_id": ph.file_unique_id,
                "caption": message.caption,
            }
        )
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку «✅ Отправить и закрыть».")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.")

@router.callback_query(F.data.in_({"taskcomplete:send", "taskcomplete:skip"}))
async def taskcomplete_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    data = await state.get_data()
    task_id = data.get("task_id")
    if not task_id:
        await cb.message.answer("Не вижу задачу. /cancel")  # type: ignore
        await state.clear()
        return

    try:
        task = await db.get_task(int(task_id))
    except KeyError:
        await cb.message.answer("Задача не найдена.")  # type: ignore
        await state.clear()
        return
    project = await db.get_project(int(task["project_id"])) if task.get("project_id") else None
    target_user_id = data.get("target_user_id")

    # Save attachments to DB (for history)
    attachments = data.get("attachments", []) if cb.data == "taskcomplete:send" else []
    for a in attachments:
        await db.add_attachment(
            task_id=int(task_id),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    # Send attachments to target (manager)
    if cb.data == "taskcomplete:send" and target_user_id:
        manager_markup = (
            manager_project_actions_kb(int(project["id"]))
            if project and task.get("project_id")
            else None
        )
        initiator = await get_initiator_label(db, cb.from_user.id)
        await notifier.safe_send(
            int(target_user_id),
            f"📄 <b>Документы по задаче #{task_id} готовы</b>\n"
            f"👤 От: {initiator}\n\n"
            f"См. вложения.",
            reply_markup=manager_markup,
        )
        # send actual files
        for a in attachments:
            await notifier.safe_send_media(int(target_user_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    # Close task and update project status
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)
    await _maybe_mark_lead_tracking_response(db, task)
    if project and task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.INVOICE_SENT)
        await integrations.sync_project(project)
    if project and task.get("type") in {TaskType.CLOSING_DOCS, TaskType.PROJECT_END}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.ARCHIVE)
        await integrations.sync_project(project)

    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")

    _uid_fin = cb.from_user.id if cb.from_user else 0
    await cb.message.answer(
        "✅ Готово.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                (await _current_role(db, _uid_fin)) if cb.from_user else None,
                is_admin=bool(cb.from_user and _uid_fin in (config.admin_ids or set())),
                unread=await db.count_unread_tasks(_uid_fin),
            ),
        ),
    )  # type: ignore
    await state.clear()



# ---------------------------------------------------------------------------
# Invoice payment: attach payment order (PP) and send to RP
# ---------------------------------------------------------------------------

@router.message(InvoicePaymentSG.attaching_pp)
async def invoice_pp_collect(message: Message, state: FSMContext) -> None:
    """Collect payment order attachments from GD."""
    data = await state.get_data()
    pp_files: list[dict[str, Any]] = data.get("pp_files", [])

    if message.document:
        pp_files.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        pp_files.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Прикрепите файл/фото платёжного поручения.")
        return

    await state.update_data(pp_files=pp_files)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(pp_files)}</b>.")


@router.callback_query(F.data.startswith("inv_pp_done:"))
async def invoice_pp_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Send payment order to RP and close invoice task."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    task_id = data.get("invoice_task_id")
    pp_files = data.get("pp_files", [])

    if not task_id:
        await state.clear()
        return
    if not pp_files:
        await cb.message.answer(  # type: ignore[union-attr]
            "Сначала прикрепите платёжное поручение, потом отправляйте результат."
        )
        return

    task = await db.get_task(int(task_id))
    if task.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        await state.clear()
        await cb.message.answer("Этот счёт уже обработан.")  # type: ignore[union-attr]
        return
    payload = try_json_loads(task.get("payload_json"))
    sender_id = _invoice_task_sender_id(payload)
    invoice_id, inv_num, supplier, amount = _invoice_task_details(payload)

    if invoice_id is not None:
        invoice_row = await db.get_invoice(invoice_id)
        if not invoice_row:
            await cb.message.answer("Не удалось найти счёт для обновления статуса.")  # type: ignore[union-attr]
            return
        await db.update_invoice_status(invoice_id, InvoiceStatus.PAID)

    # Mark task as done
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            pass

    # Save PP attachments to task
    for a in pp_files:
        await db.add_attachment(
            task_id=int(task_id),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    # Notify RP
    if sender_id:
        initiator = await get_initiator_label(db, u.id)
        msg = (
            "✅ <b>Счёт оплачен</b>\n"
            f"👤 От: {initiator}\n\n"
            f"🔢 № счёта: {inv_num}\n"
            f"🏢 Поставщик: {supplier}\n"
            f"💰 Сумма: {amount}\n\n"
            "Платёжное поручение прикреплено ниже."
        )
        await notifier.safe_send(int(sender_id), msg)
        for a in pp_files:
            await notifier.safe_send_media(
                int(sender_id), a["file_type"], a["file_id"], caption=a.get("caption"),
            )

    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
    await state.clear()

    is_admin = u.id in (config.admin_ids or set())
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Счёт оплачен. Платёжка отправлена РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(u.id), unread_channels=await db.count_unread_by_channel(u.id), gd_inbox_unread=await db.count_gd_inbox_tasks(u.id), gd_invoice_unread=await db.count_gd_invoice_tasks(u.id), gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(u.id)),
        ),
    )


@router.callback_query(F.data.startswith("inv_pp_cancel:"))
async def invoice_pp_cancel(cb: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    """Cancel payment order attachment."""
    await cb.answer()
    await state.clear()
    u = cb.from_user
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    role, isolated_role = (await _current_menu(db, u.id)) if u else (None, False)
    await cb.message.answer(  # type: ignore[union-attr]
        "Отменено.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=is_admin,
                unread=await db.count_unread_tasks(u.id) if u else 0,
                isolated_role=isolated_role,
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Montazh — ввод комментария к задаче
# ---------------------------------------------------------------------------

@router.message(MontazhCommentSG.text, F.text)
async def montazh_comment_text(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Получен комментарий к задаче монтажной группы."""
    u = message.from_user
    if not u:
        return
    data = await state.get_data()
    task_id = data.get("montazh_task_id")
    if not task_id:
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите текст комментария:")
        return

    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задача не найдена.")
        await state.clear()
        return

    # Уведомить ГД о комментарии
    gd_id = task.get("created_by")
    user_label = await get_initiator_label(db, u.id)
    payload = try_json_loads(task.get("payload_json"))
    task_comment = payload.get("comment", "")

    if gd_id:
        await notifier.safe_send(
            int(gd_id),
            f"💬 <b>Комментарий к задаче (Монтажная гр.)</b>\n"
            f"👤 От: {user_label}\n\n"
            f"📋 Задача: {task_comment}\n\n"
            f"💬 Комментарий: {text}",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await message.answer(
        "✅ Комментарий отправлен ГД.",
        reply_markup=task_actions_kb(task),
    )
