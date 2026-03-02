from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from ..callbacks import ProjectCb
from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import main_menu, projects_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import DailyReportSG, InstallationDoneSG, IssueSG
from ..utils import fmt_project_card, parse_date, private_only_reply_markup, project_status_label, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

# -------------------- DAILY REPORT --------------------

@router.message(F.text == "📝 Отчёт за день")
async def start_daily_report(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(DailyReportSG.project)
    await message.answer(
        "📝 <b>Отчёт за день</b>\n"
        "Шаг 1/5: выберите объект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="daily"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "daily"))
async def daily_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(DailyReportSG.done)
    await cb.message.answer("Что сделали сегодня? (кратко)")  # type: ignore

@router.message(DailyReportSG.done)
async def daily_done(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 3:
        await message.answer("Напишите чуть подробнее (минимум 3 символа):")
        return
    await state.update_data(done=t)
    await state.set_state(DailyReportSG.hours)
    await message.answer("Сколько часов отработали? (например 8 или 7.5)")

@router.message(DailyReportSG.hours)
async def daily_hours(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip().replace(",", ".")
    try:
        hours = float(t)
    except ValueError:
        await message.answer("Не понял. Пример: 8 или 7.5")
        return
    if hours <= 0 or hours > 24:
        await message.answer("Часы выглядят странно. Введите значение от 0 до 24.")
        return
    await state.update_data(hours=hours)
    await state.set_state(DailyReportSG.issues)
    await message.answer("Проблемы/простой/ошибка? (или «-» если нет)")

@router.message(DailyReportSG.issues)
async def daily_issues(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(issues=t, attachments=[])
    await state.set_state(DailyReportSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить отчёт", callback_data="daily:send")
    b.button(text="⏭ Без вложений", callback_data="daily:send")
    b.adjust(1)

    await message.answer("Можно приложить фото/файл. Когда готовы — нажмите кнопку:", reply_markup=b.as_markup())

@router.message(DailyReportSG.attachments)
async def daily_collect_attachments(message: Message, state: FSMContext) -> None:
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
        await message.answer("Пришлите фото/файл или нажмите «✅ Отправить отчёт».")
        return

    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Отправить отчёт».")

@router.callback_query(F.data == "daily:send")
async def daily_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
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
    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)

    done = data.get("done") or ""
    hours = data.get("hours") or 0
    issues = data.get("issues") or ""
    attachments = data.get("attachments") or []

    # store as DONE task for history (not to spam inbox)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.DAILY_REPORT,
        status=TaskStatus.DONE,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=None,
        payload={"done": done, "hours": hours, "issues": issues, "installer_id": u.id, "installer_username": u.username},
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
        "🔵 <b>Ежедневный отчёт (20:00)</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"✅ Сделано: {done}\n"
        f"⏱ Часы: <b>{hours}</b>\n"
    )
    if issues:
        msg += f"⚠️ Проблемы: {issues}\n"
    msg += f"👷 От: <code>{u.id}</code> @{u.username or '-'}"

    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
    await notifier.notify_workchat(msg)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if rp_id:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_task(task, project_code=project.get("code", ""))
    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.INSTALLER

    await cb.message.answer(
        "✅ Отчёт отправлен. "
        + ("РП уведомлён." if rp_id else "⚠️ РП не назначен (role=rp), отчёт отправлен только в рабочий чат."),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set())),
        ),
    )  # type: ignore
    await state.clear()

# -------------------- INSTALLATION DONE (installer -> RP + admins) --------------------

@router.message(F.text == "✅ Счёт ОК")
async def start_installation_done(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(InstallationDoneSG.project)
    await message.answer(
        "✅ <b>Счёт ОК</b>\n"
        "Шаг 1/3: выберите объект, где монтаж завершён.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="install_done"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "install_done"))
async def installation_done_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(InstallationDoneSG.end_date)
    await cb.message.answer("Введите дату окончания работ (ДД.ММ.ГГГГ) или «сегодня».")  # type: ignore

@router.message(InstallationDoneSG.end_date)
async def installation_done_end_date(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    dt = parse_date(t, config.timezone)
    if not dt:
        await message.answer("Не понял дату. Пример: 25.03.2026 или «сегодня».")
        return
    await state.update_data(end_date=to_iso(dt))
    await state.set_state(InstallationDoneSG.comment)
    await message.answer("Комментарий по допработам: причины и стоимость (или «-»):")

@router.message(InstallationDoneSG.comment)
async def installation_done_finalize(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    u = message.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await message.answer("Не выбран объект. Начните заново: «✅ Счёт ОК».")
        await state.clear()
        return

    extra_comment = (message.text or "").strip()
    if extra_comment == "-":
        extra_comment = ""

    project = await db.get_project(int(project_id))
    end_date = data.get("end_date")
    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await message.answer("⚠️ Не найден РП (role=rp).")
        await state.clear()
        return

    due = utcnow() + timedelta(hours=8)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.INSTALLATION_DONE,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload={
            "end_date": end_date,
            "comment": extra_comment,
            "installer_id": u.id,
            "installer_username": u.username,
        },
    )

    msg = (
        "✅ <b>Счёт ОК / монтаж завершён</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"📅 Дата окончания: <b>{end_date[:10] if isinstance(end_date, str) else end_date}</b>\n"
        f"👷 От: <code>{u.id}</code> @{u.username or '-'}"
    )
    if extra_comment:
        msg += f"\n📝 Допработы: {extra_comment}"

    await notifier.safe_send(int(rp_id), msg, reply_markup=task_actions_kb(task))
    for admin_id in sorted(config.admin_ids or set()):
        if admin_id != int(rp_id):
            await notifier.safe_send(int(admin_id), msg, reply_markup=task_actions_kb(task))

    await integrations.sync_task(task, project_code=project.get("code", ""))
    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.INSTALLER

    await message.answer(
        "✅ Уведомление «Счёт ОК» отправлено.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set())),
        ),
    )
    await state.clear()

# -------------------- ISSUE (installer -> RP) --------------------

@router.message(F.text.in_({"🆘 Проблема / простой", "🆘 Проблема / вопрос", "🆘 Дозаказ/простой/ошибка"}))
async def start_issue(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(IssueSG.project)
    await message.answer(
        "🆘 <b>Проблема / вопрос</b>\n"
        "Шаг 1/4: выберите объект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="inst_issue"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "inst_issue"))
async def issue_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(IssueSG.issue_type)

    kb = ReplyKeyboardBuilder()
    kb.button(text="Дозаказ")
    kb.button(text="Ошибка/несостыковка")
    kb.button(text="Простой")
    kb.button(text="Вопрос")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2, 1)
    await cb.message.answer(
        "Тип проблемы:",
        reply_markup=private_only_reply_markup(cb.message, kb.as_markup(resize_keyboard=True)),
    )  # type: ignore

@router.message(IssueSG.issue_type)
async def issue_type(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t in {"", "❌ Отмена"}:
        return
    await state.update_data(issue_type=t)
    await state.set_state(IssueSG.description)
    await message.answer("Опишите кратко что случилось и что нужно:")

@router.message(IssueSG.description)
async def issue_desc(message: Message, state: FSMContext) -> None:
    d = (message.text or "").strip()
    if len(d) < 5:
        await message.answer("Опишите чуть подробнее (минимум 5 символов):")
        return
    await state.update_data(description=d, attachments=[])
    await state.set_state(IssueSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="instissue:send")
    b.button(text="⏭ Без вложений", callback_data="instissue:send")
    b.adjust(1)
    await message.answer("Можно приложить фото/файл. Когда готовы — нажмите кнопку:", reply_markup=b.as_markup())

@router.message(IssueSG.attachments)
async def issue_attach(message: Message, state: FSMContext) -> None:
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
        await message.answer("Пришлите фото/файл или нажмите «✅ Отправить».")
        return

    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Отправить».")

@router.callback_query(F.data == "instissue:send")
async def issue_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
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
    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ Не найден РП (role=rp).")  # type: ignore
        await state.clear()
        return

    issue_type = data.get("issue_type") or "Проблема"
    description = data.get("description") or ""
    attachments = data.get("attachments") or []

    due = utcnow() + timedelta(hours=2)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.ISSUE,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload={"issue_type": issue_type, "comment": description, "installer_id": u.id, "installer_username": u.username},
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
        "🟠 <b>Сигнал с объекта</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"⚠️ Тип: <b>{issue_type}</b>\n"
        f"📝 Описание: {description}\n"
        f"👷 От: <code>{u.id}</code> @{u.username or '-'}"
    )
    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(rp_id), msg, reply_markup=task_kb)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_task(task, project_code=project.get("code", ""))
    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.INSTALLER

    await cb.message.answer(
        "✅ Отправил РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set())),
        ),
    )  # type: ignore
    await state.clear()

@router.message(F.text == "📌 Мои объекты")
async def my_objects(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    projects = await db.list_recent_projects(limit=20)
    if not projects:
        await message.answer("Проектов нет.")
        return
    lines = [f"<b>Последние объекты:</b> <b>{len(projects)}</b>"]
    for p in projects[:20]:
        lines.append(
            f"• <b>{p.get('code','')}</b> — {p.get('title','')} — <i>{project_status_label(p.get('status'))}</i>"
        )
    await message.answer("\n".join(lines))
