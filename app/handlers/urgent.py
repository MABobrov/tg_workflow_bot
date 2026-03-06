from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import main_menu, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import NotUrgentGDSG, UrgentGDSG
from ..utils import get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

ALLOWED_ROLES = [
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
    Role.LOADER,
    Role.TINTER,
    Role.ZAMERY,
]


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return user.role if user else None


@router.message(F.text == "🚨 Срочно ГД")
async def start_urgent_gd(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALLOWED_ROLES):
        return
    await state.clear()
    await state.set_state(UrgentGDSG.description)
    await message.answer(
        "🚨 <b>Срочно ГД</b>\n"
        "Шаг 1/2: опишите срочный вопрос для ГД.\n"
        "Для отмены: <code>/cancel</code>."
    )


@router.message(UrgentGDSG.description)
async def urgent_gd_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Слишком коротко. Опишите вопрос подробнее:")
        return
    await state.update_data(description=text, attachments=[])
    await state.set_state(UrgentGDSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="urgentgd:create")
    b.button(text="⏭ Без вложений", callback_data="urgentgd:create")
    b.adjust(1)
    await message.answer("При необходимости приложите файл/скрин. Когда готовы — нажмите кнопку:", reply_markup=b.as_markup())


@router.message(UrgentGDSG.attachments)
async def urgent_gd_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.text and message.text.strip() and message.text.strip() != "❌ Отмена":
        note = message.text.strip()
        prev = data.get("description", "")
        data["description"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить ГД».")
        return

    await state.update_data(attachments=attachments, description=data.get("description", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "urgentgd:create")
async def urgent_gd_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=ALLOWED_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    description = data.get("description") or ""
    attachments = data.get("attachments") or []

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer(
            "⚠️ ГД не найден. Назначьте пользователя ролью <code>gd</code> "
            "или укажите дефолт через /setdefaults gd=@username."
        )  # type: ignore
        await state.clear()
        return

    due = utcnow() + timedelta(hours=1)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.URGENT_GD,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(gd_id),
        due_at_iso=to_iso(due),
        payload={
            "comment": description,
            "source": "urgent_gd",
            "sender_id": u.id,
            "sender_username": u.username,
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
        "🚨 <b>СРОЧНО ГД</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}"
    )

    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(gd_id), msg, reply_markup=task_kb)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(int(gd_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await integrations.sync_task(task, project_code="")

    role = await _current_role(db, u.id)
    await cb.message.answer(
        "✅ Срочный запрос отправлен ГД.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
        ),
    )  # type: ignore
    await state.clear()



# ---------------------------------------------------------------------------
# "Не срочно ГД" — задача с пониженным приоритетом
# ---------------------------------------------------------------------------

@router.message(F.text == "📩 Не срочно ГД")
async def start_not_urgent_gd(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALLOWED_ROLES):
        return
    await state.clear()
    await state.set_state(NotUrgentGDSG.description)
    await message.answer(
        "📩 <b>Не срочно ГД</b>\n"
        "Шаг 1/2: опишите задачу / вопрос для ГД.\n"
        "Для отмены: <code>/cancel</code>."
    )


@router.message(NotUrgentGDSG.description)
async def not_urgent_gd_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Слишком коротко. Опишите подробнее:")
        return
    await state.update_data(description=text, attachments=[])
    await state.set_state(NotUrgentGDSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="noturggd:create")
    b.button(text="⏭ Без вложений", callback_data="noturggd:create")
    b.adjust(1)
    await message.answer(
        "При необходимости приложите файл/скрин. Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(NotUrgentGDSG.attachments)
async def not_urgent_gd_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.text and message.text.strip():
        note = message.text.strip()
        prev = data.get("description", "")
        data["description"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return

    await state.update_data(attachments=attachments, description=data.get("description", ""))
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "noturggd:create")
async def not_urgent_gd_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=ALLOWED_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    description = data.get("description") or ""
    attachments = data.get("attachments") or []

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ ГД не найден. Назначьте роль GD."
        )
        await state.clear()
        return

    from datetime import timedelta as _td
    due = utcnow() + _td(days=7)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.NOT_URGENT_GD,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(gd_id),
        due_at_iso=to_iso(due),
        payload={
            "comment": description,
            "source": "not_urgent_gd",
            "sender_id": u.id,
            "sender_username": u.username,
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
        "📩 <b>Не срочно ГД</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}"
    )

    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(gd_id), msg, reply_markup=task_kb)

    for a in attachments:
        await notifier.safe_send_media(
            int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"),
        )
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await integrations.sync_task(task, project_code="")

    role = await _current_role(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Задача отправлена ГД (не срочно).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
        ),
    )
    await state.clear()
