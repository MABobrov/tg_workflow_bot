from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from ..callbacks import ManagerProjectCb, ProjectCb
from ..config import Config
from ..db import Database
from ..enums import MANAGER_ROLES, ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import manager_project_actions_kb, projects_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    ClosingDocsSG,
    DocsRequestSG,
    IssueSG,
    PaymentReportSG,
    ProjectEndSG,
    QuoteRequestSG,
)
from ..utils import (
    fmt_project_card,
    get_initiator_label,
    parse_amount,
    parse_date,
    parse_roles,
    private_only_reply_markup,
    refresh_recipient_keyboard,
    task_status_label,
    task_type_label,
    to_iso,
    utcnow,
)
from .auth import require_role_message, require_role_callback

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")
MANAGER_FLOW_START_BUTTON = "Проверить КП/Запросить документы"
MANAGER_ACCESS_ROLES = (
    Role.MANAGER,
    Role.MANAGER_KV,
    Role.MANAGER_KIA,
    Role.MANAGER_NPN,
)


def _manager_can_access_project(project: dict[str, Any], user_id: int, config: Config) -> bool:
    if user_id in (config.admin_ids or set()):
        return True
    manager_id = int(project.get("manager_id") or 0)
    if manager_id == 0:
        return True
    return manager_id == user_id


def _has_manager_access(role_value: str | None) -> bool:
    return bool(set(parse_roles(role_value)) & (MANAGER_ROLES | {Role.MANAGER}))


async def _project_thread_text(db: Database, project: dict[str, Any], config: Config, limit: int = 8) -> str:
    lines = [fmt_project_card(project, config.timezone), "", "<b>Задачи проекта</b>"]
    tasks = await db.list_tasks_for_project(int(project["id"]), limit=limit)
    if not tasks:
        lines.append("• Пока нет задач.")
        return "\n".join(lines)
    for t in tasks:
        lines.append(
            f"• #{t['id']} — {task_type_label(t.get('type'))} — <b>{task_status_label(t.get('status'))}</b>"
        )
    return "\n".join(lines)


async def _show_manager_project_thread(
    target: Message | CallbackQuery,
    db: Database,
    config: Config,
    project_id: int,
) -> None:
    project = await db.get_project(project_id)
    text = await _project_thread_text(db, project, config, limit=10)
    kb = manager_project_actions_kb(project_id)
    if isinstance(target, CallbackQuery):
        if target.message:
            await target.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]
    else:
        await target.answer(text, reply_markup=kb)


async def _start_docs_request_flow(target: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(DocsRequestSG.title)
    await target.answer(
        "🧾 <b>Запрос документов/счёта</b>\n"
        "Шаг 1/8\n\n"
        "Введите <b>название проекта</b> (например: «Ленина 45»).\n"
        "Для отмены в любой момент: <code>/cancel</code>."
    )


async def _start_quote_request_flow(target: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(QuoteRequestSG.title)
    await target.answer(
        "📐 <b>Запрос / КП /</b>\n"
        "Шаг 1/7\n\n"
        "Введите <b>название проекта</b> (например: «Ленина 45»).\n"
        "Для отмены в любой момент: <code>/cancel</code>."
    )


async def _start_payment_flow_for_project(target: Message, state: FSMContext, project_id: int) -> None:
    await state.clear()
    await state.update_data(project_id=project_id)
    await state.set_state(PaymentReportSG.amount)
    await target.answer(
        "💰 <b>Оплата поступила</b>\n"
        "Шаг 2/7: введите сумму поступившей оплаты (например 50000 или 50k)."
    )


async def _start_closing_flow_for_project(target: Message, state: FSMContext, project_id: int) -> None:
    await state.clear()
    await state.update_data(project_id=project_id)
    await state.set_state(ClosingDocsSG.doc_type)
    kb = ReplyKeyboardBuilder()
    kb.button(text="Первичка")
    kb.button(text="Акт сверки")
    kb.button(text="КС")
    kb.button(text="Другое")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2, 1)
    await target.answer(
        "📄 <b>Док. / ЭДО</b>\nВыберите тип документов:",
        reply_markup=private_only_reply_markup(target, kb.as_markup(resize_keyboard=True)),
    )


async def _start_issue_flow_for_project(target: Message, state: FSMContext, project_id: int) -> None:
    await state.clear()
    await state.update_data(project_id=project_id)
    await state.set_state(IssueSG.issue_type)
    kb = ReplyKeyboardBuilder()
    kb.button(text="Дозаказ")
    kb.button(text="Ошибка/несостыковка")
    kb.button(text="Простой")
    kb.button(text="Вопрос")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2, 1)
    await target.answer(
        "🆘 <b>Проблема / вопрос</b>\nВыберите тип:",
        reply_markup=private_only_reply_markup(target, kb.as_markup(resize_keyboard=True)),
    )


async def _start_project_end_flow_for_project(target: Message, state: FSMContext, project_id: int) -> None:
    await state.clear()
    await state.update_data(project_id=project_id)
    await state.set_state(ProjectEndSG.invoice_number)
    await target.answer("🏁 <b>Счёт End</b>\nВведите <b>№ счёта</b>:")


@router.message(F.text == MANAGER_FLOW_START_BUTTON)
async def start_manager_kp_docs_flow(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    # Unified flow: no extra branching for manager.
    await _start_docs_request_flow(message, state)


@router.callback_query(F.data.in_({"mgrflow:quote", "mgrflow:docs"}))
async def pick_manager_kp_docs_flow(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.message:
        return
    # Backward compatibility for old messages with 2 buttons:
    # both paths are now treated as one unified action.
    await _start_docs_request_flow(cb.message, state)


@router.callback_query(ProjectCb.filter(F.ctx == "manager_project"))
async def open_manager_project_thread(
    cb: CallbackQuery,
    callback_data: ProjectCb,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user:
        return
    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return
    await _show_manager_project_thread(cb, db, config, int(project["id"]))


@router.callback_query(ManagerProjectCb.filter())
async def manager_project_action(
    cb: CallbackQuery,
    callback_data: ManagerProjectCb,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user or not cb.message:
        return

    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return

    action = callback_data.action
    project_id = int(project["id"])
    if action in {"open", "refresh"}:
        await _show_manager_project_thread(cb, db, config, project_id)
        return
    if action == "tasks":
        await _show_manager_project_thread(cb, db, config, project_id)
        return
    if action == "payment":
        await _start_payment_flow_for_project(cb.message, state, project_id)
        return
    if action == "closing":
        await _start_closing_flow_for_project(cb.message, state, project_id)
        return
    if action == "issue":
        await _start_issue_flow_for_project(cb.message, state, project_id)
        return
    if action == "end":
        await _start_project_end_flow_for_project(cb.message, state, project_id)
        return

# -------------------- DOCS REQUEST (manager -> RP) --------------------

@router.message(F.text == "➕ Запросить документы/счёт")
@router.message(Command("invoice"))
@router.message(F.text.regexp(r"^/(?:счет|счёт)(?:/|\b)"))
async def start_docs_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await _start_docs_request_flow(message, state)

@router.message(DocsRequestSG.title)
async def docs_title(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Название слишком короткое. Попробуйте ещё раз:")
        return
    await state.update_data(title=title)
    await state.set_state(DocsRequestSG.address)
    await message.answer("Введите <b>адрес</b> (или отправьте «-» чтобы пропустить):")

@router.message(DocsRequestSG.address)
async def docs_address(message: Message, state: FSMContext) -> None:
    address = (message.text or "").strip()
    if address == "-":
        address = ""
    await state.update_data(address=address)
    await state.set_state(DocsRequestSG.client)
    await message.answer("Введите <b>клиента/компанию</b> (или «-» чтобы пропустить):")

@router.message(DocsRequestSG.client)
async def docs_client(message: Message, state: FSMContext) -> None:
    client = (message.text or "").strip()
    if client == "-":
        client = ""
    await state.update_data(client=client)
    await state.set_state(DocsRequestSG.amount)
    await message.answer("Введите <b>сумму проекта</b> (например 150000 или 150k) или «-» чтобы пропустить:")

@router.message(DocsRequestSG.amount)
async def docs_amount(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        await state.update_data(amount=None)
        await state.set_state(DocsRequestSG.deadline)
        await message.answer("Введите <b>дедлайн</b> (ДД.ММ.ГГГГ) или «-» чтобы пропустить:")
        return
    amount = parse_amount(t)
    if amount is None:
        await message.answer("Не понял сумму. Пример: 150000 или 150k. Или «-» чтобы пропустить:")
        return
    await state.update_data(amount=amount)
    await state.set_state(DocsRequestSG.deadline)
    await message.answer("Введите <b>дедлайн</b> (ДД.ММ.ГГГГ или «сегодня/завтра») или «-» чтобы пропустить:")

@router.message(DocsRequestSG.deadline)
async def docs_deadline(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t == "-":
        await state.update_data(deadline=None)
        await state.set_state(DocsRequestSG.measurements)
        await message.answer("Опишите <b>проект с размерами</b> (или «-» чтобы пропустить):")
        return
    dt = parse_date(t, config.timezone)
    if not dt:
        await message.answer("Не понял дату. Формат: 25.03.2026 или 2026-03-25. Или «-» чтобы пропустить:")
        return
    await state.update_data(deadline=to_iso(dt))
    await state.set_state(DocsRequestSG.measurements)
    await message.answer("Опишите <b>проект с размерами</b> (или «-» чтобы пропустить):")

@router.message(DocsRequestSG.measurements)
async def docs_measurements(message: Message, state: FSMContext) -> None:
    measurements = (message.text or "").strip()
    if measurements == "-":
        measurements = ""
    await state.update_data(measurements=measurements)
    await state.set_state(DocsRequestSG.comment)
    await message.answer("Комментарий/особенности (или «-» чтобы пропустить):")

@router.message(DocsRequestSG.comment)
async def docs_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(DocsRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать запрос", callback_data="docs:create")
    b.button(text="⏭ Пропустить файлы", callback_data="docs:create")
    b.adjust(1)
    await message.answer(
        "Теперь можете <b>прикрепить КП/чертежи/замеры</b> (файлы/фото) несколькими сообщениями.\n"
        "Когда закончите — нажмите кнопку ниже.",
        reply_markup=b.as_markup(),
    )

@router.message(DocsRequestSG.attachments)
async def docs_collect_attachments(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])

    # Accept documents/photos with optional caption
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
    elif message.text and message.text.strip() and message.text.strip() != "❌ Отмена":
        # treat as additional note
        note = message.text.strip()
        prev = data.get("comment", "")
        data["comment"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать запрос».")
        return

    await state.update_data(attachments=attachments, comment=data.get("comment", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Создать запрос».")

@router.callback_query(F.data == "docs:create")
async def docs_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()

    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    title = data.get("title")
    if not title:
        await cb.message.answer(f"Не вижу данных проекта. Начните заново: «{MANAGER_FLOW_START_BUTTON}».")
        await state.clear()
        return

    address = data.get("address") or ""
    client = data.get("client") or ""
    amount = data.get("amount")
    deadline = data.get("deadline")
    measurements = data.get("measurements") or ""
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ Не найден РП (role=rp). Админ должен назначить роль хотя бы одному пользователю.")
        await state.clear()
        return

    # 1) create project
    project = await db.create_project(
        title=title,
        address=address,
        client=client,
        amount=amount,
        deadline_iso=deadline,
        status=ProjectStatus.DOCS_REQUEST,
        manager_id=u.id,
        rp_id=rp_id,
    )

    # 2) create task for RP
    due = utcnow() + timedelta(hours=24)
    task_payload = {
        "comment": comment,
        "measurements": measurements,
        "source": "telegram",
    }
    task = await db.create_task(
        project_id=int(project["id"]),
        type_=TaskType.DOCS_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload=task_payload,
    )

    # 3) store attachments (linked to task)
    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    # 4) notify RP + work chat
    initiator = await get_initiator_label(db, u.id)
    project_card = fmt_project_card(project, config.timezone)
    msg_to_rp = (
        "🟢 <b>Новый запрос: документы/счёт</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{project_card}\n\n"
    )
    if comment:
        msg_to_rp += f"📝 Комментарий: {comment}"
    if measurements:
        msg_to_rp += f"\n📐 Размеры/ТЗ: {measurements}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(rp_id, msg_to_rp, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, rp_id)
    await notifier.notify_workchat(msg_to_rp, reply_markup=task_kb)

    # resend attachments to RP
    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(rp_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    # 5) auto-link amoCRM lead if manager has an unconverted one
    unconverted_lead = await db.get_unconverted_lead_for_manager(u.id)
    if unconverted_lead and unconverted_lead.get("amo_lead_id"):
        await db.set_project_amo_lead(int(project["id"]), int(unconverted_lead["amo_lead_id"]))
        log.info(
            "Auto-linked amoCRM lead %s to project %s (docs_request, manager=%s)",
            unconverted_lead["amo_lead_id"], project["id"], u.id,
        )
    else:
        await integrations.maybe_create_lead(int(project["id"]))

    # 6) sync integrations
    await integrations.sync_project(project, manager_label=f"@{u.username or ''} ({u.id})")
    await integrations.sync_task(task, project_code=project.get("code", ""))
    await cb.message.answer("✅ Проект создан и отправлен РП.")
    await _show_manager_project_thread(cb, db, config, int(project["id"]))
    await state.clear()

# -------------------- QUOTE REQUEST (manager -> RP) --------------------

@router.message(F.text == "➕ Запросить / КП /")
@router.message(Command("kp"))
@router.message(F.text.regexp(r"^/(?:кп|kp)(?:/|\b)"))
async def start_quote_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await _start_quote_request_flow(message, state)

@router.message(QuoteRequestSG.title)
async def quote_title(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Название слишком короткое. Попробуйте ещё раз:")
        return
    await state.update_data(title=title)
    await state.set_state(QuoteRequestSG.address)
    await message.answer("Введите <b>адрес</b> (или отправьте «-» чтобы пропустить):")

@router.message(QuoteRequestSG.address)
async def quote_address(message: Message, state: FSMContext) -> None:
    address = (message.text or "").strip()
    if address == "-":
        address = ""
    await state.update_data(address=address)
    await state.set_state(QuoteRequestSG.client)
    await message.answer("Введите <b>клиента/компанию</b> (или «-» чтобы пропустить):")

@router.message(QuoteRequestSG.client)
async def quote_client(message: Message, state: FSMContext) -> None:
    client = (message.text or "").strip()
    if client == "-":
        client = ""
    await state.update_data(client=client)
    await state.set_state(QuoteRequestSG.deadline)
    await message.answer("Введите <b>дедлайн</b> (ДД.ММ.ГГГГ или «сегодня/завтра») или «-» чтобы пропустить:")

@router.message(QuoteRequestSG.deadline)
async def quote_deadline(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t == "-":
        await state.update_data(deadline=None)
        await state.set_state(QuoteRequestSG.measurements)
        await message.answer("Опишите <b>проект с размерами</b> (или «-» чтобы пропустить):")
        return
    dt = parse_date(t, config.timezone)
    if not dt:
        await message.answer("Не понял дату. Формат: 25.03.2026 или 2026-03-25. Или «-» чтобы пропустить:")
        return
    await state.update_data(deadline=to_iso(dt))
    await state.set_state(QuoteRequestSG.measurements)
    await message.answer("Опишите <b>проект с размерами</b> (или «-» чтобы пропустить):")

@router.message(QuoteRequestSG.measurements)
async def quote_measurements(message: Message, state: FSMContext) -> None:
    measurements = (message.text or "").strip()
    if measurements == "-":
        measurements = ""
    await state.update_data(measurements=measurements)
    await state.set_state(QuoteRequestSG.comment)
    await message.answer("Комментарий (или «-» чтобы пропустить):")

@router.message(QuoteRequestSG.comment)
async def quote_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(QuoteRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать запрос КП", callback_data="quote:create")
    b.button(text="⏭ Пропустить файлы", callback_data="quote:create")
    b.adjust(1)
    await message.answer(
        "Теперь можете <b>прикрепить фото/чертежи/скрин переписки</b> несколькими сообщениями.\n"
        "Когда закончите — нажмите кнопку ниже.",
        reply_markup=b.as_markup(),
    )

@router.message(QuoteRequestSG.attachments)
async def quote_collect_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.text and message.text.strip() and message.text.strip() != "❌ Отмена":
        note = message.text.strip()
        prev = data.get("comment", "")
        data["comment"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать запрос КП».")
        return

    await state.update_data(attachments=attachments, comment=data.get("comment", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Создать запрос КП».")

@router.callback_query(F.data == "quote:create")
async def quote_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    title = data.get("title")
    if not title:
        await cb.message.answer(f"Не вижу данных проекта. Начните заново: «{MANAGER_FLOW_START_BUTTON}».")
        await state.clear()
        return

    address = data.get("address") or ""
    client = data.get("client") or ""
    deadline = data.get("deadline")
    measurements = data.get("measurements") or ""
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ Не найден РП (role=rp). Админ должен назначить роль хотя бы одному пользователю.")
        await state.clear()
        return

    project = await db.create_project(
        title=title,
        address=address,
        client=client,
        amount=None,
        deadline_iso=deadline,
        status=ProjectStatus.QUOTE_REQUEST,
        manager_id=u.id,
        rp_id=rp_id,
    )

    due = utcnow() + timedelta(hours=24)
    task = await db.create_task(
        project_id=int(project["id"]),
        type_=TaskType.QUOTE_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload={"comment": comment, "measurements": measurements, "source": "telegram"},
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
    project_card = fmt_project_card(project, config.timezone)
    msg = (
        "🟢 <b>Новый запрос: КП</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{project_card}\n\n"
    )
    if measurements:
        msg += f"📐 Размеры/ТЗ: {measurements}\n"
    if comment:
        msg += f"📝 Комментарий: {comment}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(rp_id, msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, rp_id)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(rp_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    # auto-link amoCRM lead if manager has an unconverted one
    unconverted_lead = await db.get_unconverted_lead_for_manager(u.id)
    if unconverted_lead and unconverted_lead.get("amo_lead_id"):
        await db.set_project_amo_lead(int(project["id"]), int(unconverted_lead["amo_lead_id"]))
        log.info(
            "Auto-linked amoCRM lead %s to project %s (quote_request, manager=%s)",
            unconverted_lead["amo_lead_id"], project["id"], u.id,
        )
    else:
        await integrations.maybe_create_lead(int(project["id"]))

    await integrations.sync_project(project, manager_label=f"@{u.username or ''} ({u.id})")
    await integrations.sync_task(task, project_code=project.get("code", ""))
    await cb.message.answer("✅ Запрос КП создан и отправлен РП.")
    await _show_manager_project_thread(cb, db, config, int(project["id"]))
    await state.clear()

# -------------------- PAYMENT REPORT (manager -> TD) --------------------

@router.message(F.text == "💰 Оплата поступила")
async def start_payment_report(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await state.clear()
    projects = await db.list_projects_for_manager(message.from_user.id, limit=20)  # type: ignore
    if not projects:
        await message.answer("У вас пока нет проектов. Сначала создайте проект через «Проверить КП/Запросить документы».")
        return
    await state.set_state(PaymentReportSG.project)
    await message.answer(
        "💰 <b>Оплата поступила</b>\n"
        "Шаг 1/7: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="payment"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "payment"))
async def payment_pick_project(
    cb: CallbackQuery,
    callback_data: ProjectCb,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user:
        return
    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return
    await _start_payment_flow_for_project(cb.message, state, int(project["id"]))  # type: ignore[arg-type]

@router.message(PaymentReportSG.amount)
async def payment_amount(message: Message, state: FSMContext) -> None:
    amount = parse_amount((message.text or "").strip())
    if amount is None:
        await message.answer("Не понял сумму. Пример: 50000 или 50k.")
        return
    await state.update_data(payment_amount=amount)
    await state.set_state(PaymentReportSG.payment_method)

    kb = ReplyKeyboardBuilder()
    kb.button(text="б/н")
    kb.button(text="кред")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 1)
    await message.answer(
        "Выберите <b>тип оплаты</b>:",
        reply_markup=private_only_reply_markup(message, kb.as_markup(resize_keyboard=True)),
    )

@router.message(PaymentReportSG.payment_method)
async def payment_method(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip().lower()
    if t not in {"б/н", "кред"}:
        await message.answer("Выберите вариант кнопкой: б/н или кред")
        return
    await state.update_data(payment_method=t)
    await state.set_state(PaymentReportSG.payment_type)

    kb = ReplyKeyboardBuilder()
    kb.button(text="Предоплата")
    kb.button(text="Окончательный")
    kb.button(text="Другое")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2)
    await message.answer(
        "Выберите этап оплаты:",
        reply_markup=private_only_reply_markup(message, kb.as_markup(resize_keyboard=True)),
    )

@router.message(PaymentReportSG.payment_type)
async def payment_type(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t not in {"Предоплата", "Окончательный", "Другое"}:
        await message.answer("Выберите вариант кнопкой: Предоплата / Окончательный / Другое")
        return
    await state.update_data(payment_type=t)
    await state.set_state(PaymentReportSG.payment_date)
    await message.answer(
        "Введите дату оплаты (ДД.ММ.ГГГГ) или «сегодня/завтра».\n"
        "Можно «-» — тогда поставлю сегодняшнюю дату."
    )

@router.message(PaymentReportSG.payment_date)
async def payment_date(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t == "-":
        dt = parse_date("сегодня", config.timezone)
        if not dt:
            dt = utcnow()
        await state.update_data(payment_date=to_iso(dt))
    else:
        dt = parse_date(t, config.timezone)
        if not dt:
            await message.answer("Не понял дату. Пример: 25.03.2026 или «сегодня». Или «-».")
            return
        await state.update_data(payment_date=to_iso(dt))
    await state.set_state(PaymentReportSG.comment)
    await message.answer("Комментарий (например: «поступило по счёту №...») или «-»:")

@router.message(PaymentReportSG.comment)
async def payment_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(PaymentReportSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить на подтверждение", callback_data="pay:create")
    b.button(text="⏭ Без вложений", callback_data="pay:create")
    b.adjust(1)
    await message.answer(
        "Если есть — прикрепите <b>платёжку/скрин</b>.\n"
        "Когда готовы — нажмите кнопку ниже.",
        reply_markup=b.as_markup(),
    )

@router.message(PaymentReportSG.attachments)
async def payment_collect_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.text and message.text.strip() and message.text.strip() != "❌ Отмена":
        note = message.text.strip()
        prev = data.get("comment", "")
        data["comment"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить на подтверждение».")
        return

    await state.update_data(attachments=attachments, comment=data.get("comment", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Отправить на подтверждение».")

@router.callback_query(F.data == "pay:create")
async def payment_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()

    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await cb.message.answer("Не выбран проект. Начните заново: «💰 Оплата поступила».")  # type: ignore
        await state.clear()
        return

    project = await db.get_project(int(project_id))
    td_id = await resolve_default_assignee(db, config, Role.GD)
    if not td_id:
        await cb.message.answer("⚠️ Не найден ГД (role=gd). Админ должен назначить роль хотя бы одному пользователю.")
        await state.clear()
        return

    payment_amount = data.get("payment_amount")
    payment_method = data.get("payment_method")
    payment_type = data.get("payment_type")
    payment_date = data.get("payment_date")
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    # update project status: payment reported
    project = await db.update_project_status(int(project_id), ProjectStatus.PAYMENT_REPORTED)

    # create confirmation task
    due = utcnow() + timedelta(hours=2)
    task_payload = {
        "payment_amount": payment_amount,
        "payment_method": payment_method,
        "payment_type": payment_type,
        "payment_stage": payment_type,
        "payment_date": payment_date,
        "comment": comment,
        "manager_id": u.id,
        "manager_username": u.username,
    }
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.PAYMENT_CONFIRM,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=td_id,
        due_at_iso=to_iso(due),
        payload=task_payload,
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
    project_card = fmt_project_card(project, config.timezone)
    msg = (
        "🟡 <b>Требуется подтверждение оплаты</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{project_card}\n\n"
        f"💰 Сумма: <b>{payment_amount}</b>\n"
        f"💳 Тип: <b>{payment_method}</b>\n"
        f"🧾 Этап: <b>{payment_type}</b>\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(td_id, msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, td_id)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(td_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_project(project, manager_label=f"@{u.username or ''} ({u.id})")
    await integrations.sync_task(task, project_code=project.get("code", ""))
    await cb.message.answer("✅ Передал на подтверждение ТД.")  # type: ignore
    await _show_manager_project_thread(cb, db, config, int(project["id"]))
    await state.clear()

# -------------------- CLOSING DOCS REQUEST (manager -> accounting) --------------------

@router.message(F.text.in_({"📄 Док. / ЭДО", "📄 Закрывающие / ЭДО"}))
async def start_closing_docs(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await state.clear()
    projects = await db.list_projects_for_manager(message.from_user.id, limit=20)  # type: ignore
    if not projects:
        await message.answer("У вас пока нет проектов. Сначала создайте проект через «Проверить КП/Запросить документы».")
        return
    await state.set_state(ClosingDocsSG.project)
    await message.answer(
        "📄 <b>Док. / ЭДО</b>\n"
        "Шаг 1/6: выберите проект для закрывающих.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="closing"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "closing"))
async def closing_pick_project(
    cb: CallbackQuery,
    callback_data: ProjectCb,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user:
        return
    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return
    await _start_closing_flow_for_project(cb.message, state, int(project["id"]))  # type: ignore[arg-type]

@router.message(ClosingDocsSG.doc_type)
async def closing_doc_type(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t in {"❌ Отмена", ""}:
        return
    await state.update_data(doc_type=t)
    await state.set_state(ClosingDocsSG.details)
    await message.answer("Уточнение по документам (контрагент / № счёта / что подписать) или «-»:")

@router.message(ClosingDocsSG.details)
async def closing_details(message: Message, state: FSMContext) -> None:
    details = (message.text or "").strip()
    if details == "-":
        details = ""
    await state.update_data(details=details)
    await state.set_state(ClosingDocsSG.due_date)
    await message.answer("Срок (ДД.ММ.ГГГГ) или «-» (по умолчанию 2 дня):")

@router.message(ClosingDocsSG.due_date)
async def closing_due_date(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t == "-":
        dt = parse_date("сегодня", config.timezone) or utcnow()
        due = dt + timedelta(days=2)
        await state.update_data(due_at=to_iso(due))
    else:
        d = parse_date(t, config.timezone)
        if not d:
            await message.answer("Не понял дату. Пример: 25.03.2026 или «-».")
            return
        due = d + timedelta(hours=18)
        await state.update_data(due_at=to_iso(due))
    await state.set_state(ClosingDocsSG.comment)
    await message.answer("Комментарий (кому/как отправить) или «-»:")

@router.message(ClosingDocsSG.comment)
async def closing_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment, attachments=[])
    await state.set_state(ClosingDocsSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить в бухгалтерию", callback_data="closing:create")
    b.button(text="⏭ Без вложений", callback_data="closing:create")
    b.adjust(1)
    await message.answer(
        "Если есть — приложите документы (счёт, договор, приложение, скрин ЭДО и т.д.).\n"
        "Когда готовы — нажмите кнопку ниже.",
        reply_markup=b.as_markup(),
    )

@router.message(ClosingDocsSG.attachments)
async def closing_collect_attachments(message: Message, state: FSMContext) -> None:
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
    elif message.text and message.text.strip() and message.text.strip() != "❌ Отмена":
        note = message.text.strip()
        prev = data.get("comment", "")
        data["comment"] = (prev + "\n" + note).strip() if prev else note
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить в бухгалтерию».")
        return

    await state.update_data(attachments=attachments, comment=data.get("comment", ""))
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Отправить в бухгалтерию».")

@router.callback_query(F.data == "closing:create")
async def closing_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await cb.message.answer("Не выбран проект. Начните заново: «📄 Док. / ЭДО».")  # type: ignore
        await state.clear()
        return

    comment = data.get("comment") or ""
    doc_type = data.get("doc_type") or "Закрывающие"
    details = data.get("details") or ""
    due_at = data.get("due_at")
    attachments = data.get("attachments") or []

    acc_id = await resolve_default_assignee(db, config, Role.ACCOUNTING)
    if not acc_id:
        await cb.message.answer("⚠️ Не найдена бухгалтерия (role=accounting). Админ должен назначить роль.")  # type: ignore
        await state.clear()
        return

    # update project status
    project = await db.update_project_status(int(project_id), ProjectStatus.CLOSING_DOCS)

    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.CLOSING_DOCS,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=acc_id,
        due_at_iso=due_at,
        payload={
            "doc_type": doc_type,
            "details": details,
            "comment": comment,
            "manager_id": u.id,
            "manager_username": u.username,
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
    project_card = fmt_project_card(project, config.timezone)
    msg = (
        "🟣 <b>Запрос закрывающих</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{project_card}\n\n"
        f"📄 Документы: <b>{doc_type}</b>\n"
    )
    if details:
        msg += f"ℹ️ Уточнение: {details}\n"
    if comment:
        msg += f"📝 Комментарий: {comment}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(acc_id, msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, acc_id)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(acc_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_project(project, manager_label=f"@{u.username or ''} ({u.id})")
    await integrations.sync_task(task, project_code=project.get("code", ""))
    await cb.message.answer("✅ Запрос отправлен в бухгалтерию.")  # type: ignore
    await _show_manager_project_thread(cb, db, config, int(project["id"]))
    await state.clear()

# -------------------- PROJECT END (manager -> RP + admins) --------------------

@router.message(F.text == "🏁 Счёт End")
async def start_project_end(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await state.clear()
    projects = await db.list_projects_for_manager(message.from_user.id, limit=20)  # type: ignore
    if not projects:
        await message.answer("У вас пока нет проектов. Сначала создайте проект через «Проверить КП/Запросить документы».")
        return
    await state.set_state(ProjectEndSG.project)
    await message.answer(
        "🏁 <b>Счёт End</b>\n"
        "Шаг 1/4: выберите проект для финального закрытия.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="project_end"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "project_end"))
async def project_end_pick_project(
    cb: CallbackQuery,
    callback_data: ProjectCb,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user:
        return
    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return
    await _start_project_end_flow_for_project(cb.message, state, int(project["id"]))  # type: ignore[arg-type]

@router.message(ProjectEndSG.invoice_number)
async def project_end_invoice(message: Message, state: FSMContext) -> None:
    invoice = (message.text or "").strip()
    if len(invoice) < 2:
        await message.answer("Введите корректный № счёта (минимум 2 символа):")
        return
    await state.update_data(invoice_number=invoice)
    await state.set_state(ProjectEndSG.sign_type)

    kb = ReplyKeyboardBuilder()
    kb.button(text="ЭДО")
    kb.button(text="Оригиналы")
    kb.button(text="Другое")
    kb.button(text="❌ Отмена")
    kb.adjust(2, 2)
    await message.answer(
        "Выберите тип подписания:",
        reply_markup=private_only_reply_markup(message, kb.as_markup(resize_keyboard=True)),
    )

@router.message(ProjectEndSG.sign_type)
async def project_end_sign_type(message: Message, state: FSMContext) -> None:
    sign_type = (message.text or "").strip()
    if sign_type not in {"ЭДО", "Оригиналы", "Другое"}:
        await message.answer("Выберите вариант кнопкой: ЭДО / Оригиналы / Другое")
        return
    await state.update_data(sign_type=sign_type)
    await state.set_state(ProjectEndSG.comment)
    await message.answer("Комментарий (для «Оригиналы» укажите где находятся) или «-»:")

@router.message(ProjectEndSG.comment)
async def project_end_finalize(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    u = message.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await message.answer("Не выбран проект. Начните заново: «🏁 Счёт End».")
        await state.clear()
        return

    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""

    invoice_number = data.get("invoice_number")
    sign_type = data.get("sign_type")
    project = await db.get_project(int(project_id))

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await message.answer("⚠️ Не найден РП (role=rp).")
        await state.clear()
        return

    due = utcnow() + timedelta(hours=24)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.PROJECT_END,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload={
            "invoice_number": invoice_number,
            "sign_type": sign_type,
            "comment": comment,
            "manager_id": u.id,
            "manager_username": u.username,
        },
    )

    initiator = await get_initiator_label(db, u.id)
    msg = (
        "🏁 <b>Счёт End</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"🧾 Счёт: <b>{invoice_number}</b>\n"
        f"✍️ Подписание: <b>{sign_type}</b>"
    )
    if comment:
        msg += f"\n📝 Комментарий: {comment}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(rp_id), msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))
    for admin_id in sorted(config.admin_ids or set()):
        if admin_id != int(rp_id):
            await notifier.safe_send(int(admin_id), msg, reply_markup=task_kb)
            await refresh_recipient_keyboard(notifier, db, config, int(admin_id))
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    await integrations.sync_task(task, project_code=project.get("code", ""))
    await message.answer("✅ Сигнал «Счёт End» отправлен.")
    await _show_manager_project_thread(message, db, config, int(project["id"]))
    await state.clear()

# -------------------- VIEW MANAGER PROJECTS --------------------

@router.message(F.text == "📌 Мои проекты")
async def my_projects(message: Message, db: Database, config: Config) -> None:
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    projects = await db.list_projects_for_manager(message.from_user.id, limit=20)  # type: ignore
    if not projects:
        await message.answer(f"Пока нет проектов. Создайте: «{MANAGER_FLOW_START_BUTTON}».")
        return
    await message.answer(
        f"📁 Ваши проекты: <b>{len(projects)}</b>\n"
        "Нажмите на проект — откроется карточка и кнопки всех следующих шагов.",
        reply_markup=projects_kb(projects, ctx="manager_project"),
    )

# -------------------- ISSUE (manager) --------------------

@router.message(F.text == "🆘 Проблема / вопрос")
async def start_issue(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user:
        return
    user = await db.get_user_optional(message.from_user.id)
    if not _has_manager_access(user.role if user else None):
        # Leave chance for other handlers with the same text (e.g. installer legacy button).
        return
    if not await require_role_message(message, db, roles=MANAGER_ACCESS_ROLES):
        return
    await state.clear()
    projects = await db.list_projects_for_manager(message.from_user.id, limit=20)  # type: ignore
    if not projects:
        await message.answer("У вас пока нет проектов. Сначала создайте проект через «Проверить КП/Запросить документы».")
        return
    await state.set_state(IssueSG.project)
    await message.answer(
        "🆘 <b>Проблема / вопрос</b>\n"
        "Шаг 1/4: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="issue"),
    )

@router.callback_query(ProjectCb.filter(F.ctx == "issue"))
async def issue_pick_project(
    cb: CallbackQuery,
    callback_data: ProjectCb,
    state: FSMContext,
    db: Database,
    config: Config,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
        return
    await cb.answer()
    if not cb.from_user:
        return
    project = await db.get_project(int(callback_data.project_id))
    if not _manager_can_access_project(project, cb.from_user.id, config):
        await cb.answer("Это не ваш проект", show_alert=True)
        return
    await _start_issue_flow_for_project(cb.message, state, int(project["id"]))  # type: ignore[arg-type]

@router.message(IssueSG.issue_type)
async def issue_type(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t in {"", "❌ Отмена"}:
        return
    await state.update_data(issue_type=t)
    await state.set_state(IssueSG.description)
    await message.answer("Опишите проблему/вопрос текстом:")

@router.message(IssueSG.description)
async def issue_description(message: Message, state: FSMContext) -> None:
    d = (message.text or "").strip()
    if len(d) < 5:
        await message.answer("Опишите чуть подробнее (минимум 5 символов):")
        return
    await state.update_data(description=d, attachments=[])
    await state.set_state(IssueSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="issue:create")
    b.button(text="⏭ Без вложений", callback_data="issue:create")
    b.adjust(1)
    await message.answer("Можно приложить фото/файл. Когда готовы — нажмите кнопку:", reply_markup=b.as_markup())

@router.message(IssueSG.attachments)
async def issue_collect_attachments(message: Message, state: FSMContext) -> None:
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
        await message.answer("Пришлите фото/файл или нажмите «✅ Отправить».")
        return

    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>. Можно отправить ещё или нажать «✅ Отправить».")

@router.callback_query(F.data == "issue:create")
async def issue_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=MANAGER_ACCESS_ROLES):
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

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ Не найден РП (role=rp).")  # type: ignore
        await state.clear()
        return

    issue_type = data.get("issue_type") or "Проблема"
    description = data.get("description") or ""
    attachments = data.get("attachments") or []

    due = utcnow() + timedelta(hours=4)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.ISSUE,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=to_iso(due),
        payload={"issue_type": issue_type, "comment": description, "manager_id": u.id, "manager_username": u.username},
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
        "🟠 <b>Проблема/вопрос</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"⚠️ Тип: <b>{issue_type}</b>\n"
        f"📝 Описание: {description}"
    )
    task_kb = task_actions_kb(task)
    await notifier.safe_send(rp_id, msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, rp_id)
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        await notifier.safe_send_media(rp_id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_task(task, project_code=project.get("code", ""))
    await cb.message.answer("✅ Отправил РП.")  # type: ignore
    await _show_manager_project_thread(cb, db, config, int(project["id"]))
    await state.clear()
