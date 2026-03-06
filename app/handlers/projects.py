from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.types import CallbackQuery

from ..callbacks import ProjectCb
from ..config import Config
from ..db import Database
from ..enums import MANAGER_ROLES, Role
from ..keyboards import manager_project_actions_kb
from ..utils import fmt_project_card, parse_roles, task_status_label, task_type_label
from .auth import require_role_callback

log = logging.getLogger(__name__)
router = Router()
router.callback_query.filter(F.message.chat.type == "private")


@router.callback_query(ProjectCb.filter(F.ctx == "view"))
async def view_project(cb: CallbackQuery, callback_data: ProjectCb, db: Database, config: Config) -> None:
    if not await require_role_callback(
        cb,
        db,
        roles=[
            Role.MANAGER,
            Role.MANAGER_KV,
            Role.MANAGER_KIA,
            Role.MANAGER_NPN,
            Role.RP,
            Role.TD,
            Role.ACCOUNTING,
            Role.INSTALLER,
            Role.GD,
            Role.DRIVER,
            Role.TINTER,
            Role.ZAMERY,
        ],
    ):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    text = fmt_project_card(project, config.timezone)
    if cb.from_user:
        user = await db.get_user_optional(cb.from_user.id)
        roles = set(parse_roles(user.role if user else None))
        is_owner_manager = bool(roles & (MANAGER_ROLES | {Role.MANAGER})) and int(project.get("manager_id") or 0) == cb.from_user.id
        if is_owner_manager:
            tasks = await db.list_tasks_for_project(int(project["id"]), limit=8)
            if tasks:
                lines = ["", "<b>Задачи проекта</b>"]
                for t in tasks:
                    lines.append(
                        f"• #{t['id']} — {task_type_label(t.get('type'))} — <b>{task_status_label(t.get('status'))}</b>"
                    )
                text += "\n" + "\n".join(lines)
            else:
                text += "\n\n<b>Задачи проекта</b>\n• Пока нет задач."
            await cb.message.answer(text, reply_markup=manager_project_actions_kb(int(project["id"])))  # type: ignore[arg-type]
            return
    await cb.message.answer(text)  # type: ignore
