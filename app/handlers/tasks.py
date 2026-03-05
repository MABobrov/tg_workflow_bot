from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import TaskCb
from ..config import Config
from ..db import Database
from ..enums import ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import main_menu, manager_project_actions_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import InvoicePaymentSG, SupplierPaymentSG, TaskCompleteSG
from ..utils import fmt_task_card, private_only_reply_markup, to_iso, try_json_loads, utcnow
from .auth import require_role_callback

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
    return u.role if u else None


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
    await cb.answer()
    task_id = int(callback_data.task_id)
    action = callback_data.action

    task = await db.get_task(task_id)

    if not await _can_manage_task(cb, db, config, task):
        await cb.answer("Эта задача назначена другому человеку", show_alert=True)
        return

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
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await cb.message.answer("⏳ Взял в работу.", reply_markup=task_actions_kb(task))  # type: ignore
        return

    # REJECT
    if action == "reject":
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await cb.message.answer(
            "❌ Задача отклонена.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    (await _current_role(db, cb.from_user.id)) if cb.from_user else None,
                    is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set())),
                ),
            ),
        )  # type: ignore

        # notify creator
        created_by = task.get("created_by")
        if created_by:
            await notifier.safe_send(int(created_by), f"❌ Ваша задача #{task_id} отклонена исполнителем.")
        return

    # PAYMENT CONFIRM actions (TD)
    if action in {"pay_ok", "pay_need"} and task.get("type") == TaskType.PAYMENT_CONFIRM:
        if not project:
            await cb.message.answer("Проект не найден для этой задачи.")  # type: ignore
            return

        payload = try_json_loads(task.get("payload_json"))
        manager_id = payload.get("manager_id") or project.get("manager_id")
        rp_id = project.get("rp_id") or (await db.get_project_rp_id(int(project["id"])))

        if action == "pay_ok":
            task = await db.update_task_status(task_id, TaskStatus.DONE)
            project = await db.update_project_status(int(project["id"]), ProjectStatus.IN_WORK)

            text = (
                "✅ <b>Оплата подтверждена</b> — можно запускать закупки и монтаж.\n\n"
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

            text = (
                "⚠️ <b>Оплата не подтверждена</b>: нужна доплата/уточнение.\n\n"
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

        await cb.message.answer(
            "Готово.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    role_now,
                    is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set())),
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

    # INVOICE_PAYMENT actions (GD)
    if action == "inv_pay" and task.get("type") == TaskType.INVOICE_PAYMENT:
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
        # Mark as in_progress (on hold)
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        payload = try_json_loads(task.get("payload_json"))
        sender_id = payload.get("sender_id")
        if sender_id:
            await notifier.safe_send(int(sender_id), f"⏸ Счёт #{task_id} отложен ГД.")
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await cb.message.answer(  # type: ignore
            "⏸ Счёт отложен.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(Role.GD, is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set()))),
            ),
        )
        return

    if action == "inv_reject" and task.get("type") == TaskType.INVOICE_PAYMENT:
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        payload = try_json_loads(task.get("payload_json"))
        sender_id = payload.get("sender_id")
        if sender_id:
            await notifier.safe_send(
                int(sender_id),
                f"❌ Счёт #{task_id} отклонён ГД.",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await cb.message.answer(  # type: ignore
            "❌ Счёт отклонён. РП уведомлён.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(Role.GD, is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set()))),
            ),
        )
        return

    # DONE (generic)
    if action == "done":
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
        await cb.message.answer(
            "✅ Закрыл задачу.",
            reply_markup=private_only_reply_markup(
                cb.message,
                main_menu(
                    (await _current_role(db, cb.from_user.id)) if cb.from_user else None,
                    is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set())),
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
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.")

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

    task = await db.get_task(int(task_id))
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
        await notifier.safe_send(
            int(target_user_id),
            f"📄 Документы по задаче #{task_id} готовы. См. вложения.",
            reply_markup=manager_markup,
        )
        # send actual files
        for a in attachments:
            await notifier.safe_send_media(int(target_user_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    # Close task and update project status
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)
    if project and task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.INVOICE_SENT)
        await integrations.sync_project(project)
    if project and task.get("type") in {TaskType.CLOSING_DOCS, TaskType.PROJECT_END}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.ARCHIVE)
        await integrations.sync_project(project)

    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")

    await cb.message.answer(
        "✅ Готово.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                (await _current_role(db, cb.from_user.id)) if cb.from_user else None,
                is_admin=bool(cb.from_user and cb.from_user.id in (config.admin_ids or set())),
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
    await message.answer(f"📎 Принял. Файлов: <b>{len(pp_files)}</b>.")


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

    task = await db.get_task(int(task_id))
    payload = try_json_loads(task.get("payload_json"))
    sender_id = payload.get("sender_id")

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
        inv_num = payload.get("invoice_number", "")
        supplier = payload.get("supplier", "")
        amount = payload.get("amount", "")
        msg = (
            "✅ <b>Счёт оплачен</b>\n\n"
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
            main_menu(Role.GD, is_admin=is_admin),
        ),
    )


@router.callback_query(F.data.startswith("inv_pp_cancel:"))
async def invoice_pp_cancel(cb: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    """Cancel payment order attachment."""
    await cb.answer()
    await state.clear()
    u = cb.from_user
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    role = (await _current_role(db, u.id)) if u else None
    await cb.message.answer(  # type: ignore[union-attr]
        "Отменено.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=is_admin),
        ),
    )
