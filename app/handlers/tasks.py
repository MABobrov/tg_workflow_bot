from __future__ import annotations

import logging
from typing import Any

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import TaskCb
from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import main_menu, manager_project_actions_kb, task_actions_kb
from ..services.integration_hub import IntegrationHub
from ..services.assignment import resolve_default_assignee
from ..services.menu_context import build_main_menu_for_user
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..states import DeliveryPaymentSG, InvoicePaymentSG, MontazhCommentSG, SupplierPaymentSG, TaskCancelReasonSG, TaskCompleteSG
from ..utils import answer_service, fmt_task_card, get_initiator_label, parse_roles, private_only_reply_markup, refresh_recipient_keyboard, task_type_label, try_json_loads

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


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


def _ignorable_markup_error(exc: TelegramBadRequest) -> bool:
    text = str(exc).lower()
    return any(
        needle in text
        for needle in (
            "message is not modified",
            "message can't be edited",
            "message to edit not found",
            "there is no reply markup in the message",
        )
    )


async def _safe_edit_task_markup(
    message: Message | None,
    *,
    reply_markup: Any | None,
) -> None:
    if not message:
        return
    try:
        await message.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if _ignorable_markup_error(exc):
            return
        log.debug("Failed to refresh task callback markup", exc_info=True)
    except Exception:
        log.debug("Failed to refresh task callback markup", exc_info=True)


async def _answer_with_menu(
    message: Message | None,
    db: Database,
    config: Config,
    user_id: int,
    text: str,
    *,
    role: str | None,
    isolated_role: bool = False,
) -> None:
    if not message:
        return
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            await build_main_menu_for_user(
                db,
                config,
                user_id,
                role,
                isolated_role=isolated_role,
            ),
        ),
    )


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


async def _notify_task_creator_done(
    db: Database,
    notifier: Notifier,
    actor_id: int | None,
    task: dict[str, Any] | None,
) -> None:
    if not task:
        return
    created_by = task.get("created_by")
    if not created_by:
        return
    try:
        created_by_int = int(created_by)
    except (TypeError, ValueError):
        return
    if actor_id and created_by_int == actor_id:
        return
    initiator = await get_initiator_label(db, actor_id) if actor_id else "Исполнитель"
    await notifier.safe_send(
        created_by_int,
        f"✅ Ваша задача #{task['id']} выполнена\n👤 Исполнитель: {initiator}",
    )


async def _apply_done_side_effects(
    db: Database,
    integrations: IntegrationHub,
    task: dict[str, Any],
    project: dict[str, Any] | None,
) -> dict[str, Any] | None:
    await _maybe_mark_lead_tracking_response(db, task)
    if project and task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.INVOICE_SENT)
        await integrations.sync_project(project)
    if project and task.get("type") in {TaskType.CLOSING_DOCS, TaskType.PROJECT_END}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.ARCHIVE)
        await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
    return project


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
        title = str(project.get("title") or "").strip()
        if title:
            lines.append(f"📁 Проект: {title}")
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

    # DELETE — GD (admin) and RP
    if action == "delete":
        u = await db.get_user_optional(cb.from_user.id)
        user_roles = set(parse_roles(u.role if u else None))
        is_authorized = cb.from_user.id in (config.admin_ids or set()) or Role.RP in user_roles
        if not is_authorized:
            await cb.answer("⛔️ Удаление доступно только ГД и РП", show_alert=True)
            return
        await db.delete_task(task_id)
        await cb.answer(f"🗑 Задача #{task_id} удалена", show_alert=True)
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"🗑 <s>Задача #{task_id}</s> — удалена",
            )
        except Exception:
            pass
        return

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
            await _safe_edit_task_markup(cb.message, reply_markup=task_actions_kb(task))
        # Notify task creator
        created_by = task.get("created_by") if task else None
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(created_by),
                f"✅ Задача #{task_id} принята\n👤 Исполнитель: {initiator}"
            )
        return

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None

    # OPEN: show card + actions
    if action == "open":
        await cb.answer()
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
        task = await db.update_task_status(
            task_id, TaskStatus.IN_PROGRESS,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        if not task.get("accepted_at"):
            await db.accept_task(task_id)
            task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)

        # Update montazh_stage for installer tasks
        try:
            user_row = await db.get_user_optional(cb.from_user.id)
            if user_row and Role.INSTALLER in (user_row.role or ""):
                payload = task.get("payload_json") or {}
                if isinstance(payload, str):
                    payload = try_json_loads(payload)
                inv_id = payload.get("invoice_id")
                if inv_id:
                    from ..enums import MontazhStage
                    await db.update_montazh_stage(int(inv_id), MontazhStage.IN_WORK)
                    inv_row = await db.get_invoice(int(inv_id))
                    if inv_row:
                        await integrations.sync_invoice_status(
                            inv_row["invoice_number"], inv_row.get("status", ""), MontazhStage.IN_WORK,
                        )
        except Exception:
            log.exception("Failed to update montazh_stage on take")
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=task_actions_kb(task))
        await answer_service(cb.message, _task_take_text(task, project))  # type: ignore[arg-type]
        return

    # REJECT
    if action == "reject":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(
            task_id, TaskStatus.REJECTED,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        await _maybe_mark_lead_tracking_response(db, task)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "❌ Задача отклонена.",
                role=role_now,
                isolated_role=isolated_role,
            )

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

    # CANCEL (снять задачу) — available to assigned user, creator, and admin
    if action == "cancel":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        # Allow creator to cancel too
        user_id = cb.from_user.id
        created_by = task.get("created_by")
        try:
            is_creator = created_by is not None and int(created_by) == user_id
        except (ValueError, TypeError):
            is_creator = False
        try:
            is_assigned = task.get("assigned_to") is not None and int(task["assigned_to"]) == user_id
        except (ValueError, TypeError):
            is_assigned = False
        is_admin = user_id in (config.admin_ids or set())
        if not (is_creator or is_assigned or is_admin):
            await cb.answer("Снять задачу может только автор, исполнитель или администратор.", show_alert=True)
            return
        # #33/#48: Если задача уже подтверждена — запросить причину отмены
        if task.get("accepted_at") and not is_admin:
            from ..states import TaskCancelReasonSG
            await state.clear()
            await state.set_state(TaskCancelReasonSG.reason)
            await state.update_data(cancel_task_id=task_id)
            await cb.message.answer(  # type: ignore[union-attr]
                f"⚠️ Задача #{task_id} уже была подтверждена получателем.\n\n"
                "Для отмены укажите <b>причину</b>:",
            )
            return
        # Atomic update — prevent race condition
        task = await db.update_task_status(
            task_id, TaskStatus.REJECTED,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "🚫 Задача снята.",
                role=role_now,
                isolated_role=isolated_role,
            )
        # Notify the other party (creator or assigned)
        initiator = await get_initiator_label(db, cb.from_user.id)
        task_label = task_type_label(task.get("type") or "")
        inv_num = ""
        payload = try_json_loads(task.get("payload_json"))
        if payload:
            inv_num = payload.get("invoice_number", "")
        cancel_detail = f"📋 {task_label}"
        if inv_num:
            cancel_detail += f" | Счёт: {inv_num}"

        notified_ids: set[int] = {user_id}
        if is_creator and task.get("assigned_to"):
            try:
                tid_assigned = int(task["assigned_to"])
                await notifier.safe_send(
                    tid_assigned,
                    f"🚫 Задача #{task_id} снята автором\n{cancel_detail}\n👤 {initiator}",
                )
                notified_ids.add(tid_assigned)
            except (ValueError, TypeError):
                pass
        elif is_assigned and created_by:
            try:
                tid_creator = int(created_by)
                await notifier.safe_send(
                    tid_creator,
                    f"🚫 Ваша задача #{task_id} снята исполнителем\n{cancel_detail}\n👤 {initiator}",
                )
                notified_ids.add(tid_creator)
            except (ValueError, TypeError):
                pass
        elif is_admin:
            for notify_id in filter(None, [created_by, task.get("assigned_to")]):
                try:
                    nid = int(notify_id)
                except (ValueError, TypeError):
                    continue
                if nid != user_id:
                    await notifier.safe_send(
                        nid,
                        f"🚫 Задача #{task_id} снята администратором\n{cancel_detail}\n👤 {initiator}",
                    )
                    notified_ids.add(nid)

        # Always notify RP and GD about cancellation
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        gd_id = await resolve_default_assignee(db, config, Role.GD)
        cancel_msg_rp_gd = (
            f"🚫 Задача #{task_id} снята\n{cancel_detail}\n👤 Инициатор: {initiator}"
        )
        for mgmt_id in filter(None, [rp_id, gd_id]):
            if mgmt_id not in notified_ids:
                await notifier.safe_send(mgmt_id, cancel_msg_rp_gd)
                notified_ids.add(mgmt_id)
        return

# #33/#48: Обработка причины отмены задачи (после подтверждения)
@router.message(TaskCancelReasonSG.reason)
async def task_cancel_with_reason(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Принять причину отмены и отменить задачу."""
    reason = (message.text or "").strip()
    if len(reason) < 3:
        await message.answer("Укажите причину отмены (минимум 3 символа):")
        return

    data = await state.get_data()
    task_id = data.get("cancel_task_id")
    if not task_id or not isinstance(task_id, int) or task_id <= 0:
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача не найдена.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    task = await db.get_task(task_id)
    if not task or task.get("status") not in ("open", "in_progress"):
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача уже закрыта или обработана.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    task = await db.update_task_status(
        task_id, TaskStatus.REJECTED,
        expected_statuses=("open", "in_progress"),
    )
    if task is None:
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача уже была обработана другим пользователем.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    await state.clear()

    initiator = await get_initiator_label(db, message.from_user.id)
    task_label = task_type_label(task.get("type") or "")
    payload = try_json_loads(task.get("payload_json"))
    inv_num = payload.get("invoice_number", "") if payload else ""
    cancel_detail = f"📋 {task_label}"
    if inv_num:
        cancel_detail += f" | Счёт: {inv_num}"

    cancel_msg = (
        f"🚫 Задача #{task_id} снята\n"
        f"{cancel_detail}\n"
        f"👤 {initiator}\n"
        f"📝 Причина: {reason}"
    )

    # Уведомить все стороны
    user_id = message.from_user.id
    notified: set[int] = {user_id}
    for nid_raw in [task.get("assigned_to"), task.get("created_by")]:
        if nid_raw:
            try:
                nid = int(nid_raw)
                if nid not in notified:
                    await notifier.safe_send(nid, cancel_msg)
                    notified.add(nid)
            except (ValueError, TypeError):
                pass

    role_now, isolated_role = await _current_menu(db, user_id)
    await message.answer(
        f"🚫 Задача #{task_id} снята.\n📝 Причина: {reason}",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role_now,
                is_admin=user_id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(user_id),
                isolated_role=isolated_role,
            ),
        ),
    )


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
            task = await db.update_task_status(
                task_id, TaskStatus.DONE,
                expected_statuses=tuple(active_statuses),
            )
            if task is None:
                await cb.answer("Задача уже была обработана.", show_alert=True)
                return
            project = await db.update_project_status(int(project["id"]), ProjectStatus.IN_WORK)
            # Обновить статус подтверждения оплаты на счетах проекта
            await db.conn.execute(
                "UPDATE invoices SET payment_confirm_status = 'Подтверждена' WHERE project_id = ?",
                (int(project["id"]),),
            )
            await db.conn.commit()

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "✅ <b>Оплата подтверждена</b> — можно запускать закупки и монтаж.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('title','')}"
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
            task = await db.update_task_status(
                task_id, TaskStatus.REJECTED,
                expected_statuses=tuple(active_statuses),
            )
            if task is None:
                await cb.answer("Задача уже была обработана.", show_alert=True)
                return
            project = await db.update_project_status(int(project["id"]), ProjectStatus.WAITING_PAYMENT)
            # Обновить статус подтверждения оплаты на счетах проекта
            await db.conn.execute(
                "UPDATE invoices SET payment_confirm_status = 'Нужна доплата' WHERE project_id = ?",
                (int(project["id"]),),
            )
            await db.conn.commit()

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "⚠️ <b>Оплата не подтверждена</b>: нужна доплата/уточнение.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('title','')}"
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
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "Готово.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    # ORDER actions (TD) -> open supplier payment flow with project preselected
    if action == "pay_supplier" and task.get("type") in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS}:
        if not project:
            await cb.message.answer("Проект не найден для этой задачи.")  # type: ignore
            return
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        await state.update_data(project_id=int(project["id"]), source_order_task_id=int(task_id))
        await state.set_state(SupplierPaymentSG.supplier)
        await cb.message.answer(
            "💸 <b>Оплата поставщику</b>\n"
            f"Проект: <b>{project.get('title','')}</b>\n\n"
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
            if invoice_number:
                await integrations.sync_invoice_status(invoice_number, InvoiceStatus.IN_PROGRESS)
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
        # Уведомить РП: ГД принял счёт в работу
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        if rp_id:
            rp_text = (
                f"✅ <b>Счёт принят ГД в работу</b>\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}\n\n"
                f"📊 Статус: <b>В работе</b>"
            )
            await notifier.safe_send(int(rp_id), rp_text)
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        # Показать обновлённую карточку с новыми кнопками (Оплатить/Отложить/Отклонить)
        card_text = fmt_task_card(task, project, config.timezone)
        kb = task_actions_kb(task)
        await cb.message.answer(  # type: ignore
            f"✅ Получение подтверждено.\n\n{card_text}",
            reply_markup=kb,
        )
        # Обновить main_menu (badge counters)
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "📋 Счёт принят в работу.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    # INVOICE_PAYMENT actions (GD)
    if action == "inv_pay" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        # GD wants to pay — ask for payment order attachment
        await _safe_edit_task_markup(cb.message, reply_markup=None)
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
            if invoice_number:
                await integrations.sync_invoice_status(invoice_number, InvoiceStatus.ON_HOLD)
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
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "⏸ Счёт отложен.",
                role=role_now,
                isolated_role=isolated_role,
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
            if invoice_number:
                await integrations.sync_invoice_status(invoice_number, InvoiceStatus.REJECTED)
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
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "❌ Счёт отклонён. РП уведомлён.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    # DELIVERY_REQUEST — ГД принял заявку (в работу)
    if action == "del_accept" and task.get("type") == TaskType.DELIVERY_REQUEST:
        if task.get("status") != TaskStatus.OPEN:
            await cb.answer("Заявка уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        await db.accept_task(task_id)
        # Notify RP
        rp_id = task.get("created_by")
        if rp_id:
            await notifier.safe_send(
                int(rp_id),
                f"✅ <b>Оплата доставки — принято ГД</b>\n"
                f"Задача #{task_id} в работе.",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))
        # Show updated card
        task_kb = task_actions_kb(task)
        try:
            await cb.message.edit_reply_markup(reply_markup=task_kb)  # type: ignore
        except TelegramBadRequest:
            pass
        await cb.answer("Принято, статус: в работе")
        return

    # DELIVERY_REQUEST — ГД оплачивает доставку (FSM: сумма → комментарий → платёжка)
    if action == "del_pay" and task.get("type") == TaskType.DELIVERY_REQUEST:
        if task.get("status") != TaskStatus.IN_PROGRESS:
            await cb.answer("Задача не в работе.", show_alert=True)
            return
        await state.clear()
        await state.update_data(delivery_task_id=task_id)
        await state.set_state(DeliveryPaymentSG.amount)
        payload = try_json_loads(task.get("payload_json"))
        est = payload.get("estimated_logistics") or "—"
        await cb.message.answer(  # type: ignore
            f"💳 <b>Оплата доставки</b>\n"
            f"Задача #{task_id}\n"
            f"🚚 Расч. логистика: {est}\n\n"
            "Введите фактическую стоимость доставки (число):",
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
            reply_markup=None,
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
            reply_markup=None,
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
            await _safe_edit_task_markup(cb.message, reply_markup=None)
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
        task = await db.update_task_status(
            task_id, TaskStatus.DONE,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        project = await _apply_done_side_effects(db, integrations, task, project)
        await _notify_task_creator_done(
            db,
            notifier,
            cb.from_user.id if cb.from_user else None,
            task,
        )
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "✅ Закрыл задачу.",
                role=role_now,
                isolated_role=isolated_role,
            )
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
    elif message.video:
        attachments.append(
            {
                "file_type": "video",
                "file_id": message.video.file_id,
                "file_unique_id": message.video.file_unique_id,
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
    if task.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        await cb.message.answer("Эта задача уже закрыта.")  # type: ignore[union-attr]
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
    project = await _apply_done_side_effects(db, integrations, task, project)
    await _notify_task_creator_done(
        db,
        notifier,
        cb.from_user.id if cb.from_user else None,
        task,
    )

    await _safe_edit_task_markup(cb.message, reply_markup=None)
    if cb.from_user:
        role_now, isolated_role = await _current_menu(db, cb.from_user.id)
        await _answer_with_menu(
            cb.message,
            db,
            config,
            cb.from_user.id,
            "✅ Готово.",
            role=role_now,
            isolated_role=isolated_role,
        )
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
    elif message.video:
        pp_files.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
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
        if inv_num:
            await integrations.sync_invoice_status(inv_num, InvoiceStatus.PAID)

    # Mark task as done
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)

    # Auto-create SUPPLIER_PAYMENT for cost tracking
    _parent_inv_id = payload.get("parent_invoice_id") or payload.get("invoice_id")
    _sp_amount = payload.get("amount")
    if _parent_inv_id is not None and _sp_amount:
        _sp_mat_type = payload.get("material_type") or "extra_mat"
        _sp_supplier = payload.get("supplier") or ""
        _sp_inv_num = payload.get("invoice_number") or ""
        try:
            sp_task = await db.create_task(
                project_id=int(task.get("project_id") or 0) or None,
                type_=TaskType.SUPPLIER_PAYMENT,
                status=TaskStatus.DONE,
                created_by=u.id,
                assigned_to=int(sender_id) if sender_id else u.id,
                due_at_iso=None,
                payload={
                    "supplier": _sp_supplier,
                    "amount": float(_sp_amount),
                    "invoice_number": _sp_inv_num,
                    "material_type": _sp_mat_type,
                    "parent_invoice_id": int(_parent_inv_id),
                    "td_id": u.id,
                    "td_username": u.username or "",
                    "auto_from_invoice_payment": int(task_id),
                },
            )
            # Also write to supplier_payments table
            await db.create_supplier_payment(
                parent_invoice_id=int(_parent_inv_id),
                amount=float(_sp_amount),
                material_type=_sp_mat_type,
                invoice_number=_sp_inv_num,
                supplier=_sp_supplier,
                task_id=sp_task["id"] if sp_task else None,
                created_by=u.id,
            )
        except Exception:
            log.warning("Failed to auto-create SUPPLIER_PAYMENT from task %s", task_id, exc_info=True)

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            log.warning("Failed to get project %s for task %s", task.get("project_id"), task_id, exc_info=True)

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

    # Уведомить монтажника о поступлении оплаты (если счёт привязан)
    if invoice_id is not None:
        inv = await db.get_invoice(invoice_id)
        if inv and inv.get("assigned_to"):
            installer_id = inv["assigned_to"]
            inst_msg = (
                f"💰 <b>Оплата поступила</b>\n"
                f"📄 Счёт №{inv.get('invoice_number', inv_num)}\n"
                f"📍 {inv.get('object_address', '—')}\n"
            )
            await notifier.safe_send(int(installer_id), inst_msg)
            await refresh_recipient_keyboard(notifier, db, config, int(installer_id))

    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
    await _safe_edit_task_markup(cb.message, reply_markup=None)
    await state.clear()

    role_now, isolated_role = await _current_menu(db, u.id)
    await _answer_with_menu(
        cb.message,
        db,
        config,
        u.id,
        "✅ Счёт оплачен. Платёжка отправлена РП.",
        role=role_now,
        isolated_role=isolated_role,
    )


@router.callback_query(F.data.startswith("inv_pp_cancel:"))
async def invoice_pp_cancel(cb: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    """Cancel payment order attachment."""
    await cb.answer()
    await _safe_edit_task_markup(cb.message, reply_markup=None)
    await state.clear()
    u = cb.from_user
    if u:
        role, isolated_role = await _current_menu(db, u.id)
        await _answer_with_menu(
            cb.message,
            db,
            config,
            u.id,
            "Отменено.",
            role=role,
            isolated_role=isolated_role,
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


# ==================== ОПЛАТА ДОСТАВКИ — FSM ГД ====================


@router.message(DeliveryPaymentSG.amount)
async def delivery_payment_amount(message: Message, state: FSMContext) -> None:
    """GD enters actual delivery cost."""
    t = (message.text or "").strip().replace(" ", "").replace("\u00a0", "")
    # Parse number (supports 50000, 50k, 50К)
    raw = t.lower().replace("к", "000").replace("k", "000")
    try:
        amount = float(raw)
    except ValueError:
        await message.answer("Введите число (пример: 15000 или 15к):")
        return
    await state.update_data(delivery_amount=amount)
    await state.set_state(DeliveryPaymentSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без комментария", callback_data="delpay_gd:nocomment")
    b.adjust(1)
    await message.answer(
        f"Сумма: <b>{amount:.0f}₽</b>\n\n"
        "Комментарий (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "delpay_gd:nocomment", DeliveryPaymentSG.comment)
async def delivery_payment_no_comment(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(delivery_comment="", delivery_attachments=[])
    await state.set_state(DeliveryPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Завершить без файла", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await cb.message.answer(  # type: ignore
        "Прикрепите платёжку (PDF/фото) или завершите без файла:",
        reply_markup=b.as_markup(),
    )


@router.message(DeliveryPaymentSG.comment)
async def delivery_payment_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    await state.update_data(delivery_comment=t, delivery_attachments=[])
    await state.set_state(DeliveryPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Завершить без файла", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await message.answer(
        "Прикрепите платёжку (PDF/фото) или завершите без файла:",
        reply_markup=b.as_markup(),
    )


@router.message(DeliveryPaymentSG.attachments)
async def delivery_payment_attachment(message: Message, state: FSMContext) -> None:
    file_id = None
    file_type = None
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
        b = InlineKeyboardBuilder()
        b.button(text="✅ Завершить", callback_data="delpay_gd:finalize")
        b.adjust(1)
        await message.answer("Прикрепите файл или завершите:", reply_markup=b.as_markup())
        return

    data = await state.get_data()
    attachments = data.get("delivery_attachments", [])
    attachments.append({"file_id": file_id, "type": file_type})
    await state.update_data(delivery_attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Завершить (файлов: {len(attachments)})", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await message.answer(
        f"📎 Файл добавлен ({len(attachments)}). Ещё или завершить:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "delpay_gd:finalize")
async def delivery_payment_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    task_id = data.get("delivery_task_id")
    amount = data.get("delivery_amount", 0)
    comment = data.get("delivery_comment", "")
    attachments = data.get("delivery_attachments", [])

    if not task_id:
        await cb.message.answer("Ошибка: задача не найдена.")  # type: ignore
        await state.clear()
        return

    task = await db.get_task(int(task_id))
    payload = try_json_loads(task.get("payload_json"))
    inv_id = payload.get("invoice_id")
    inv_num = payload.get("invoice_number", "")

    # Save actual delivery cost to invoice
    if inv_id:
        await db.update_invoice(
            int(inv_id),
            actual_logistics=amount,
        )
        # Write to Google Sheets if available
        if integrations.sheets:
            try:
                await integrations.sheets.write_field_to_op(
                    inv_num, "estimated_logistics", amount,
                )
            except Exception:
                log.warning("Failed to write delivery cost to ОП sheet")

    # Update task payload with payment info
    payload["gd_amount"] = amount
    payload["gd_comment"] = comment
    payload["gd_attachments"] = attachments
    import json as _json
    from ..utils import to_iso, utcnow
    await db.conn.execute(
        "UPDATE tasks SET payload_json = ?, updated_at = ? WHERE id = ?",
        (_json.dumps(payload, ensure_ascii=False), to_iso(utcnow()), int(task_id)),
    )
    await db.conn.commit()

    # Close task
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)

    # Notify RP
    rp_id = task.get("created_by")
    if rp_id:
        msg = (
            f"💳 <b>Доставка оплачена</b>\n"
            f"Счёт: {inv_num}\n"
            f"💰 Сумма: <b>{amount:.0f}₽</b>\n"
        )
        if comment:
            msg += f"📝 Комментарий: {comment}\n"
        await notifier.safe_send(int(rp_id), msg)
        # Send payment attachments to RP
        for att in attachments:
            await notifier.safe_send_media(
                int(rp_id), att.get("type", "document"), att["file_id"],
            )
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await state.clear()
    await cb.message.answer(  # type: ignore
        f"✅ Доставка оплачена: {amount:.0f}₽. Задача закрыта.",
    )
