from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from aiogram import Router, F, html
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import main_menu, task_actions_kb, tasks_kb
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import ManagerInfoRequestSG
from ..utils import private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


def _manager_label(manager: Any) -> tuple[str, str]:
    plain_name = (manager.full_name or "").strip()
    uname = (manager.username or "").strip()
    if plain_name and uname:
        plain = f"{plain_name} (@{uname})"
    elif plain_name:
        plain = plain_name
    elif uname:
        plain = f"@{uname}"
    else:
        plain = str(manager.telegram_id)
    return plain, html.quote(plain)


@router.message(F.text == "📄 Закрывающие")
async def closing_tasks(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30, type_filter=TaskType.CLOSING_DOCS)  # type: ignore
    if not tasks:
        await message.answer("Нет запросов на закрывающие ✅")
        return
    await message.answer(
        f"📄 Запросы на закрывающие: <b>{len(tasks)}</b>\n"
        "Нажмите на задачу, чтобы открыть детали и действия.",
        reply_markup=tasks_kb(tasks),
    )


@router.message(F.text == "📨 Менеджеру (Имя)")
async def start_manager_info_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ACCOUNTING]):
        return
    await state.clear()

    managers = await db.find_users_by_role(Role.MANAGER, limit=30)
    if not managers:
        await message.answer("Не найдено менеджеров с ролью manager.")
        return

    b = InlineKeyboardBuilder()
    for m in managers:
        label, _ = _manager_label(m)
        b.button(text=label[:64], callback_data=f"accmgr:pick:{m.telegram_id}")
    b.adjust(1)

    await state.set_state(ManagerInfoRequestSG.manager)
    await message.answer(
        "📨 <b>Запрос недостающей информации</b>\n"
        "Шаг 1/3: выберите менеджера.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("accmgr:pick:"))
async def pick_manager_for_request(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()

    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.message.answer("Не удалось выбрать менеджера. Попробуйте ещё раз.")  # type: ignore
        return

    manager_id = int(parts[2])
    manager = await db.get_user_optional(manager_id)
    if not manager:
        await cb.message.answer("Менеджер не найден в базе. Попросите его написать боту /start.")  # type: ignore
        return

    label_plain, label_html = _manager_label(manager)
    await state.update_data(manager_id=manager_id, manager_label=label_html, manager_label_plain=label_plain)
    await state.set_state(ManagerInfoRequestSG.description)
    await cb.message.answer(f"Введите запрос недостающей информации для <b>{label_html}</b>:")  # type: ignore


@router.message(ManagerInfoRequestSG.description)
async def manager_request_description(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 3:
        await message.answer("Слишком коротко. Опишите запрос подробнее:")
        return
    await state.update_data(description=t, attachments=[])
    await state.set_state(ManagerInfoRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить менеджеру", callback_data="accmgr:create")
    b.button(text="⏭ Без вложений", callback_data="accmgr:create")
    b.adjust(1)
    await message.answer(
        "При необходимости приложите файлы/скриншоты. Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(ManagerInfoRequestSG.attachments)
async def manager_request_attachments(message: Message, state: FSMContext) -> None:
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
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить менеджеру».")
        return

    await state.update_data(attachments=attachments, description=data.get("description", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "accmgr:create")
async def manager_request_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.ACCOUNTING]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    manager_id = data.get("manager_id")
    if not manager_id:
        await cb.message.answer("Не выбран менеджер. Начните заново: «📨 Менеджеру (Имя)».")  # type: ignore
        await state.clear()
        return

    description = data.get("description") or ""
    attachments = data.get("attachments") or []
    manager_label = data.get("manager_label") or f"<code>{manager_id}</code>"

    due = utcnow() + timedelta(hours=4)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.MANAGER_INFO_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(manager_id),
        due_at_iso=to_iso(due),
        payload={
            "comment": description,
            "source": "accounting",
            "accounting_id": u.id,
            "accounting_username": u.username,
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

    msg = (
        "🧾 <b>Запрос недостающей информации</b>\n\n"
        f"Кому: {manager_label}\n"
        f"📝 {description}\n\n"
        f"От бухгалтерии: <code>{u.id}</code> @{u.username or '-'}"
    )

    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(manager_id), msg, reply_markup=task_kb)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(int(manager_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    await integrations.sync_task(task, project_code="")
    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.ACCOUNTING

    await cb.message.answer(
        "✅ Запрос отправлен менеджеру.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
        ),
    )  # type: ignore
    await state.clear()
