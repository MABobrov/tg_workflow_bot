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
from ..enums import ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import main_menu, projects_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import TintingDoneSG
from ..utils import fmt_project_card, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


# ==================== ТОНИРОВКА ВЫПОЛНЕНА (Тонировщик -> РП) ====================

@router.message(F.text == "✅ Тонировка выполнена")
async def start_tinting_done(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.TINTER]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(TintingDoneSG.project)
    await message.answer(
        "✅ <b>Тонировка выполнена</b>\n"
        "Шаг 1/3: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="tinting_done"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "tinting_done"))
async def tinting_done_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.TINTER]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(TintingDoneSG.comment)
    await cb.message.answer("Комментарий по работе (или «-»):")  # type: ignore


@router.message(TintingDoneSG.comment)
async def tinting_done_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(TintingDoneSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить тонировку", callback_data="tintingdone:create")
    b.button(text="⏭ Без фото", callback_data="tintingdone:create")
    b.adjust(1)
    await message.answer("Приложите фото результата (или нажмите кнопку):", reply_markup=b.as_markup())


@router.message(TintingDoneSG.attachments)
async def tinting_done_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите фото или нажмите «✅ Подтвердить тонировку».")
        return
    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "tintingdone:create")
async def tinting_done_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.TINTER]):
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
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)

    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.TINTING_DONE,
        status=TaskStatus.DONE,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=None,
        payload={
            "comment": comment,
            "tinter_id": u.id,
            "tinter_username": u.username,
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
        "🎨 <b>Тонировка выполнена</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}"

    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))
    await notifier.notify_workchat(msg)

    # Auto-close latest open tinting request for this project and tinter.
    source_tinting_task: dict[str, Any] | None = None
    for t in await db.list_tasks_for_project(int(project_id), limit=100):
        if (
            t.get("type") == TaskType.TINTING_REQUEST
            and t.get("status") in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}
            and (not t.get("assigned_to") or int(t.get("assigned_to")) == u.id)
        ):
            source_tinting_task = t
            break
    if source_tinting_task:
        source_tinting_task = await db.update_task_status(int(source_tinting_task["id"]), TaskStatus.DONE)
        await integrations.sync_task(source_tinting_task, project_code=project.get("code", ""))

    if project.get("status") == ProjectStatus.TINTING:
        project = await db.update_project_status(int(project_id), ProjectStatus.INSTALLATION)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if rp_id:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", ""))

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.TINTER
    await cb.message.answer(
        "✅ Тонировка подтверждена. "
        + ("РП уведомлён." if rp_id else "⚠️ РП не назначен (role=rp), уведомление не отправлено."),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set())),
        ),
    )  # type: ignore
    await state.clear()
