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
from ..enums import ProjectStatus, Role, TaskStatus, TaskType
from ..keyboards import main_menu, projects_kb, tasks_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    AssignLeadSG,
    DeliveryRequestSG,
    InvoiceCreateSG,
    OrderMaterialSG,
    TintingRequestSG,
)
from ..utils import fmt_project_card, get_initiator_label, parse_date, parse_roles, private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _list_managers(db: Database, limit_per_role: int = 30) -> list[Any]:
    managers_by_id: dict[int, Any] = {}
    for role in (Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN):
        for manager in await db.find_users_by_role(role, limit=limit_per_role):
            managers_by_id.setdefault(manager.telegram_id, manager)
    return sorted(
        managers_by_id.values(),
        key=lambda manager: (
            (manager.full_name or "").strip().lower(),
            (manager.username or "").strip().lower(),
            manager.telegram_id,
        ),
    )


# ==================== ВХОДЯЩИЕ ЗАДАЧИ ====================

@router.message(F.text == "📥 Входящие задачи")
async def inbox_tasks(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN, Role.RP, Role.TD, Role.ACCOUNTING, Role.GD, Role.DRIVER, Role.LOADER, Role.TINTER, Role.ZAMERY]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore
    if not tasks:
        await message.answer("Входящих задач нет ✅")
        return
    await message.answer(
        f"📥 Ваши задачи: <b>{len(tasks)}</b>\n"
        "Нажмите на задачу, чтобы открыть карточку и доступные действия.",
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


@router.message(F.text == "🗂 Проекты")
async def list_projects(message: Message, db: Database, config: Config) -> None:
    if not await require_role_message(message, db, roles=[Role.RP, Role.TD, Role.ACCOUNTING, Role.GD]):
        return
    projects = await db.list_recent_projects(limit=20)
    if not projects:
        await message.answer("Проектов нет.")
        return
    await message.answer(
        f"🗂 Последние проекты: <b>{len(projects)}</b>\n"
        "Нажмите на проект, чтобы открыть карточку.",
        reply_markup=projects_kb(projects, ctx="view"),
    )


# ==================== ЗАКАЗ МАТЕРИАЛОВ (РП -> Поставщик) ====================

@router.message(F.text == "📦 Заказ материалов")
async def start_order_material(message: Message, state: FSMContext, db: Database) -> None:
    # Skip if the user has installer role — let installer_new handle it
    if message.from_user:
        _u = await db.get_user_optional(message.from_user.id)
        if _u and _u.role and Role.INSTALLER in set(parse_roles(_u.role)):
            return
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(OrderMaterialSG.project)
    await message.answer(
        "📦 <b>Заказ материалов</b>\n"
        "Шаг 1/6: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="order_mat"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "order_mat"))
async def order_mat_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(OrderMaterialSG.material_type)

    kb = ReplyKeyboardBuilder()
    kb.button(text="Профиль")
    kb.button(text="Стекло")
    kb.button(text="ЛДСП")
    kb.button(text="ГКЛ")
    kb.button(text="Сэндвич")
    kb.button(text="Нестандарт")
    kb.button(text="❌ Отмена")
    kb.adjust(3, 3, 1)
    await cb.message.answer(
        "Тип материала:",
        reply_markup=private_only_reply_markup(cb.message, kb.as_markup(resize_keyboard=True)),
    )  # type: ignore


@router.message(OrderMaterialSG.material_type)
async def order_mat_type(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t in {"", "❌ Отмена"}:
        return
    await state.update_data(material_type=t)
    await state.set_state(OrderMaterialSG.supplier)
    await message.answer("Укажите поставщика (название компании или «-» если стандартный):")


@router.message(OrderMaterialSG.supplier)
async def order_mat_supplier(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(supplier=t)
    await state.set_state(OrderMaterialSG.description)
    await message.answer("Спецификация заказа (размеры, количество, артикулы):")


@router.message(OrderMaterialSG.description)
async def order_mat_description(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 5:
        await message.answer("Опишите подробнее (минимум 5 символов):")
        return
    await state.update_data(description=t)
    await state.set_state(OrderMaterialSG.comment)
    await message.answer("Комментарий (или «-» чтобы пропустить):")


@router.message(OrderMaterialSG.comment)
async def order_mat_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(OrderMaterialSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать заказ", callback_data="ordermat:create")
    b.button(text="⏭ Без вложений", callback_data="ordermat:create")
    b.adjust(1)
    await message.answer(
        "Приложите чертежи / спецификации / бланк заказа (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.message(OrderMaterialSG.attachments)
async def order_mat_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать заказ».")
        return
    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "ordermat:create")
async def order_mat_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
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

    material_type = data.get("material_type") or "Материал"
    supplier = data.get("supplier") or ""
    description = data.get("description") or ""
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    # Определяем тип задачи по типу материала
    type_map = {
        "Профиль": TaskType.ORDER_PROFILE,
        "Стекло": TaskType.ORDER_GLASS,
    }
    task_type = type_map.get(material_type, TaskType.ORDER_MATERIALS)

    project = await db.get_project(int(project_id))

    # Обновляем статус проекта если он в IN_WORK
    if project.get("status") == ProjectStatus.IN_WORK:
        project = await db.update_project_status(int(project_id), ProjectStatus.ORDERING)

    # Задача назначается на ТД (для оплаты) или ГД (для контроля)
    td_id = await resolve_default_assignee(db, config, Role.GD)

    due = utcnow() + timedelta(hours=24)
    task = await db.create_task(
        project_id=int(project_id),
        type_=task_type,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=td_id,
        due_at_iso=to_iso(due),
        payload={
            "material_type": material_type,
            "supplier": supplier,
            "description": description,
            "comment": comment,
            "rp_id": u.id,
            "rp_username": u.username,
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
        f"📦 <b>Заказ: {material_type}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"🏭 Поставщик: <b>{supplier or '—'}</b>\n"
        f"📋 Спецификация: {description}\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}\n"

    task_kb = task_actions_kb(task)
    if td_id:
        await notifier.safe_send(int(td_id), msg, reply_markup=task_kb)
        await refresh_recipient_keyboard(notifier, db, config, int(td_id))
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    # Отправляем вложения
    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if td_id:
            await notifier.safe_send_media(int(td_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", ""))

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.RP
    await cb.message.answer(
        (
            f"✅ Заказ «{material_type}» создан."
            + (" Отправлен ТД на оплату." if td_id else " ⚠️ ТД не назначен (role=td), заявка ушла только в рабочий чат.")
        ),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
        ),
    )  # type: ignore
    await state.clear()


# ==================== ЗАЯВКА НА ДОСТАВКУ (РП -> Водитель) ====================

@router.message(F.text == "🚚 Заявка на доставку")
async def start_delivery_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(DeliveryRequestSG.project)
    await message.answer(
        "🚚 <b>Заявка на доставку</b>\n"
        "Шаг 1/6: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="delivery_req"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "delivery_req"))
async def delivery_req_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(DeliveryRequestSG.address_from)
    await cb.message.answer("Откуда забрать? (адрес склада/поставщика):")  # type: ignore


@router.message(DeliveryRequestSG.address_from)
async def delivery_req_from(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 3:
        await message.answer("Укажите адрес подробнее:")
        return
    await state.update_data(address_from=t)
    await state.set_state(DeliveryRequestSG.address_to)
    await message.answer("Куда доставить? (адрес объекта):")


@router.message(DeliveryRequestSG.address_to)
async def delivery_req_to(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 3:
        await message.answer("Укажите адрес подробнее:")
        return
    await state.update_data(address_to=t)
    await state.set_state(DeliveryRequestSG.delivery_date)
    await message.answer("Дата доставки (ДД.ММ.ГГГГ или «сегодня/завтра»):")


@router.message(DeliveryRequestSG.delivery_date)
async def delivery_req_date(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    dt = parse_date(t, config.timezone)
    if not dt:
        await message.answer("Не понял дату. Пример: 25.03.2026 или «сегодня».")
        return
    await state.update_data(delivery_date=to_iso(dt))
    await state.set_state(DeliveryRequestSG.cargo_description)
    await message.answer("Что везём? (профиль / стекло / другое — кратко):")


@router.message(DeliveryRequestSG.cargo_description)
async def delivery_req_cargo(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 3:
        await message.answer("Опишите груз подробнее:")
        return
    await state.update_data(cargo_description=t)
    await state.set_state(DeliveryRequestSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать заявку", callback_data="deliveryreq:create")
    b.adjust(1)
    await message.answer("Комментарий (или нажмите кнопку для создания заявки):", reply_markup=b.as_markup())


@router.message(DeliveryRequestSG.comment)
async def delivery_req_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать заявку", callback_data="deliveryreq:create")
    b.adjust(1)
    await message.answer("Готово. Нажмите кнопку для создания заявки:", reply_markup=b.as_markup())


@router.callback_query(F.data == "deliveryreq:create")
async def delivery_req_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
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

    # Обновляем статус проекта
    if project.get("status") in {ProjectStatus.IN_WORK, ProjectStatus.ORDERING}:
        project = await db.update_project_status(int(project_id), ProjectStatus.DELIVERY)

    driver_id = await resolve_default_assignee(db, config, Role.DRIVER)

    address_from = data.get("address_from") or ""
    address_to = data.get("address_to") or ""
    delivery_date = data.get("delivery_date")
    cargo = data.get("cargo_description") or ""
    comment = data.get("comment") or ""

    due = utcnow() + timedelta(hours=24)
    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.DELIVERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=driver_id,
        due_at_iso=to_iso(due),
        payload={
            "address_from": address_from,
            "address_to": address_to,
            "delivery_date": delivery_date,
            "cargo": cargo,
            "comment": comment,
            "rp_id": u.id,
            "rp_username": u.username,
        },
    )

    initiator = await get_initiator_label(db, u.id)
    msg = (
        "🚚 <b>Заявка на доставку</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"📍 Откуда: <b>{address_from}</b>\n"
        f"📍 Куда: <b>{address_to}</b>\n"
        f"📅 Дата: <b>{delivery_date[:10] if isinstance(delivery_date, str) else '—'}</b>\n"
        f"📦 Груз: <b>{cargo}</b>\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}\n"

    task_kb = task_actions_kb(task)
    if driver_id:
        await notifier.safe_send(int(driver_id), msg, reply_markup=task_kb)
        await refresh_recipient_keyboard(notifier, db, config, int(driver_id))
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", ""))

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.RP
    await cb.message.answer(
        "✅ Заявка на доставку создана." + (" Водитель уведомлён." if driver_id else " ⚠️ Водитель не назначен (role=driver)."),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
        ),
    )  # type: ignore
    await state.clear()


# ==================== РАСПРЕДЕЛЕНИЕ ЛИДА (РП -> Менеджер) ====================

@router.message(F.text == "🎯 Распределить лид")
async def start_assign_lead(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    managers = await _list_managers(db, limit_per_role=30)
    if not managers:
        await message.answer("Не найдено менеджеров с активной ролью отдела продаж.")
        return

    b = InlineKeyboardBuilder()
    for m in managers:
        label = (m.full_name or "").strip() or (m.username or str(m.telegram_id))
        if m.username:
            label = f"{label} (@{m.username})"
        b.button(text=label[:64], callback_data=f"assignlead:pick:{m.telegram_id}")
    b.adjust(1)

    await state.set_state(AssignLeadSG.manager)
    await message.answer(
        "🎯 <b>Распределение лида</b>\n"
        "Шаг 1/3: выберите менеджера.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("assignlead:pick:"))
async def assign_lead_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    parts = (cb.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await cb.message.answer("Ошибка выбора. Попробуйте ещё раз.")  # type: ignore
        return
    manager_id = int(parts[2])
    manager = await db.get_user_optional(manager_id)
    if not manager:
        await cb.message.answer("Менеджер не найден.")  # type: ignore
        return
    label = (manager.full_name or "") or f"@{manager.username or manager_id}"
    await state.update_data(manager_id=manager_id, manager_label=label)
    await state.set_state(AssignLeadSG.description)
    await cb.message.answer(f"Опишите лид для <b>{label}</b> (источник, контакт, суть запроса):")  # type: ignore


@router.message(AssignLeadSG.description)
async def assign_lead_desc(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 5:
        await message.answer("Опишите подробнее (минимум 5 символов):")
        return
    await state.update_data(description=t)
    await state.set_state(AssignLeadSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить менеджеру", callback_data="assignlead:create")
    b.adjust(1)
    await message.answer("Комментарий (или нажмите кнопку):", reply_markup=b.as_markup())


@router.message(AssignLeadSG.comment)
async def assign_lead_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить менеджеру", callback_data="assignlead:create")
    b.adjust(1)
    await message.answer("Готово. Нажмите кнопку:", reply_markup=b.as_markup())


@router.callback_query(F.data == "assignlead:create")
async def assign_lead_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    manager_id = data.get("manager_id")
    if not manager_id:
        await cb.message.answer("Не выбран менеджер. Начните заново.")  # type: ignore
        await state.clear()
        return

    description = data.get("description") or ""
    comment = data.get("comment") or ""
    manager_label = data.get("manager_label") or str(manager_id)

    due = utcnow() + timedelta(hours=4)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.ASSIGN_LEAD,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(manager_id),
        due_at_iso=to_iso(due),
        payload={
            "description": description,
            "comment": comment,
            "rp_id": u.id,
            "rp_username": u.username,
        },
    )

    initiator = await get_initiator_label(db, u.id)
    msg = (
        "🎯 <b>Новый лид</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Менеджер: <b>{manager_label}</b>\n"
        f"📝 Описание: {description}\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}\n"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(manager_id), msg, reply_markup=task_kb)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    await integrations.sync_task(task, project_code="")

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.RP
    await cb.message.answer(
        f"✅ Лид отправлен менеджеру ({manager_label}).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
        ),
    )  # type: ignore
    await state.clear()


# ==================== ЗАЯВКА НА ТОНИРОВКУ (РП -> Тонировщик) ====================

@router.message(F.text == "🎨 Заявка на тонировку")
async def start_tinting_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(TintingRequestSG.project)
    await message.answer(
        "🎨 <b>Заявка на тонировку</b>\n"
        "Шаг 1/5: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="tinting_req"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "tinting_req"))
async def tinting_req_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))
    await state.set_state(TintingRequestSG.description)
    await cb.message.answer("Опишите что нужно затонировать (площадь, тип плёнки, особенности):")  # type: ignore


@router.message(TintingRequestSG.description)
async def tinting_req_desc(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 5:
        await message.answer("Опишите подробнее (минимум 5 символов):")
        return
    await state.update_data(description=t)
    await state.set_state(TintingRequestSG.deadline)
    await message.answer("Срок выполнения (ДД.ММ.ГГГГ или «-» — 3 дня по умолчанию):")


@router.message(TintingRequestSG.deadline)
async def tinting_req_deadline(message: Message, state: FSMContext, config: Config) -> None:
    t = (message.text or "").strip()
    if t == "-":
        dt = utcnow() + timedelta(days=3)
    else:
        dt = parse_date(t, config.timezone)
        if not dt:
            await message.answer("Не понял дату. Пример: 25.03.2026 или «-».")
            return
    await state.update_data(deadline=to_iso(dt))
    await state.set_state(TintingRequestSG.comment)
    await message.answer("Комментарий (или «-»):")


@router.message(TintingRequestSG.comment)
async def tinting_req_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(TintingRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать заявку", callback_data="tintingreq:create")
    b.button(text="⏭ Без вложений", callback_data="tintingreq:create")
    b.adjust(1)
    await message.answer("Приложите фото/чертёж (или нажмите кнопку):", reply_markup=b.as_markup())


@router.message(TintingRequestSG.attachments)
async def tinting_req_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать заявку».")
        return
    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "tintingreq:create")
async def tinting_req_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
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
    description = data.get("description") or ""
    deadline = data.get("deadline")
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []

    # Обновляем статус
    if project.get("status") in {ProjectStatus.IN_WORK, ProjectStatus.INSTALLATION}:
        project = await db.update_project_status(int(project_id), ProjectStatus.TINTING)

    tinter_id = await resolve_default_assignee(db, config, Role.TINTER)

    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.TINTING_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=tinter_id,
        due_at_iso=deadline,
        payload={
            "description": description,
            "comment": comment,
            "rp_id": u.id,
            "rp_username": u.username,
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
        "🎨 <b>Заявка на тонировку</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"📋 Описание: {description}\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}\n"

    task_kb = task_actions_kb(task)
    if tinter_id:
        await notifier.safe_send(int(tinter_id), msg, reply_markup=task_kb)
        await refresh_recipient_keyboard(notifier, db, config, int(tinter_id))
    await notifier.notify_workchat(msg, reply_markup=task_kb)

    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if tinter_id:
            await notifier.safe_send_media(int(tinter_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))

    await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", ""))

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.RP
    await cb.message.answer(
        "✅ Заявка на тонировку создана." + (" Тонировщик уведомлён." if tinter_id else " ⚠️ Тонировщик не назначен (role=tinter)."),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
        ),
    )  # type: ignore
    await state.clear()


# ---------------------------------------------------------------------------
# Invoice creation flow (РП -> ГД): "Создать счёт на оплату"
# ---------------------------------------------------------------------------

@router.message(F.text == "💳 Счёт на оплату ГД")
async def start_invoice_create(message: Message, state: FSMContext, db: Database) -> None:
    """RP starts creating an invoice payment task for GD."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return

    projects = await db.list_recent_projects(limit=30)
    if not projects:
        await message.answer("Нет проектов. Сначала создайте проект.")
        return

    from ..keyboards import projects_kb
    await state.clear()
    await state.set_state(InvoiceCreateSG.project)
    await message.answer(
        "💳 <b>Счёт на оплату ГД</b>\n"
        "Шаг 1/7: выберите проект:",
        reply_markup=projects_kb(projects, ctx="invoice"),
    )


@router.callback_query(
    InvoiceCreateSG.project,
    lambda cb: cb.data and cb.data.startswith("proj:"),
)
async def invoice_pick_project(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Pick project for invoice → show parent invoice picker."""
    await cb.answer()
    from ..callbacks import ProjectCb
    data = ProjectCb.unpack(cb.data)
    project = await db.get_project(data.project_id)
    await state.update_data(project_id=data.project_id, project_code=project.get("code", ""))

    # Show parent invoice picker
    from ..keyboards import invoice_select_kb
    invoices = await db.list_invoices_for_selection(limit=15)
    if invoices:
        await state.set_state(InvoiceCreateSG.parent_invoice)
        await cb.message.answer(  # type: ignore[union-attr]
            "Шаг 1: привязка к счёту объекта (или пропустите):",
            reply_markup=invoice_select_kb(invoices, prefix="inv_create_parent"),
        )
    else:
        # No invoices — skip to material type
        await state.update_data(parent_invoice_id=None)
        from ..keyboards import material_type_kb
        await state.set_state(InvoiceCreateSG.material_type)
        await cb.message.answer(  # type: ignore[union-attr]
            "Шаг 2: тип материала/услуги:",
            reply_markup=material_type_kb(prefix="inv_create_mat"),
        )


@router.callback_query(
    InvoiceCreateSG.parent_invoice,
    lambda cb: cb.data and cb.data.startswith("inv_create_parent:"),
)
async def invoice_pick_parent(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Pick parent invoice for the new invoice payment."""
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    parent_id = None if val == "skip" else int(val)
    await state.update_data(parent_invoice_id=parent_id)

    # If project_id not set (simplified flow), extract from parent invoice
    data = await state.get_data()
    if not data.get("project_id") and parent_id:
        parent_inv = await db.get_invoice(parent_id)
        if parent_inv and parent_inv.get("project_id"):
            await state.update_data(
                project_id=parent_inv["project_id"],
                project_code=parent_inv.get("invoice_number", ""),
            )

    from ..keyboards import material_type_kb
    await state.set_state(InvoiceCreateSG.material_type)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 2: тип материала/услуги:",
        reply_markup=material_type_kb(prefix="inv_create_mat"),
    )


@router.callback_query(
    InvoiceCreateSG.material_type,
    lambda cb: cb.data and cb.data.startswith("inv_create_mat:"),
)
async def invoice_pick_material(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick material type for the new invoice payment."""
    await cb.answer()
    mat_code = (cb.data or "").split(":", 1)[1]
    await state.update_data(material_type=mat_code)

    await state.set_state(InvoiceCreateSG.supplier)
    await cb.message.answer("Шаг 3: укажите поставщика:")  # type: ignore[union-attr]


@router.message(InvoiceCreateSG.supplier)
async def invoice_supplier(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Укажите поставщика:")
        return
    await state.update_data(supplier=text)
    await state.set_state(InvoiceCreateSG.amount)
    await message.answer("Шаг 4: укажите сумму:")


@router.message(InvoiceCreateSG.amount)
async def invoice_amount(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
    except ValueError:
        await message.answer("Укажите сумму числом:")
        return
    await state.update_data(amount=amount)
    await state.set_state(InvoiceCreateSG.invoice_number)
    await message.answer("Шаг 5: укажите номер счёта:")


@router.message(InvoiceCreateSG.invoice_number)
async def invoice_number(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Укажите номер счёта:")
        return
    await state.update_data(invoice_number=text)
    await state.set_state(InvoiceCreateSG.comment)
    await message.answer("Шаг 6: комментарий (или напишите «-» для пропуска):")


@router.message(InvoiceCreateSG.comment)
async def invoice_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = text if text != "-" else ""
    await state.update_data(comment=comment, attachments=[])

    from ..keyboards import urgency_kb
    await state.set_state(InvoiceCreateSG.urgency)
    await message.answer(
        "Шаг 7: срочность оплаты:",
        reply_markup=urgency_kb(prefix="inv_urgency"),
    )


@router.callback_query(
    InvoiceCreateSG.urgency,
    lambda cb: cb.data and cb.data.startswith("inv_urgency:"),
)
async def invoice_urgency(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick urgency for the invoice payment."""
    await cb.answer()
    code = (cb.data or "").split(":", 1)[1]  # 1h / 7h / 24h
    await state.update_data(urgency=code)

    await state.set_state(InvoiceCreateSG.attachments)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать счёт", callback_data="invoice_create:finalize")
    b.button(text="⏭ Без вложений", callback_data="invoice_create:finalize")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 8: прикрепите файлы (счёт, скан). Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InvoiceCreateSG.attachments)
async def invoice_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("attachments", [])
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
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "invoice_create:finalize")
async def invoice_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: "Notifier",
    integrations: "IntegrationHub",
) -> None:
    """Create INVOICE_PAYMENT task and notify GD."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    supplier = data.get("supplier", "")
    amount = data.get("amount", 0)
    invoice_number = data.get("invoice_number", "")
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])
    parent_invoice_id = data.get("parent_invoice_id")
    material_type = data.get("material_type")

    from ..services.assignment import resolve_default_assignee
    from ..enums import TaskType, TaskStatus
    from ..utils import utcnow, to_iso
    from datetime import timedelta

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer("⚠️ ГД не найден. Настройте роль GD.")  # type: ignore[union-attr]
        await state.clear()
        return

    urgency = data.get("urgency", "1h")
    _URGENCY_DELTA = {"1h": timedelta(hours=1), "7h": timedelta(hours=7), "24h": timedelta(hours=24)}
    due = utcnow() + _URGENCY_DELTA.get(urgency, timedelta(hours=1))
    task = await db.create_task(
        project_id=project_id,
        type_=TaskType.INVOICE_PAYMENT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(gd_id),
        due_at_iso=to_iso(due),
        payload={
            "supplier": supplier,
            "amount": amount,
            "invoice_number": invoice_number,
            "comment": comment,
            "sender_id": u.id,
            "sender_username": u.username,
            "parent_invoice_id": parent_invoice_id,
            "material_type": material_type,
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
    project_code = data.get("project_code", "")
    msg = (
        "💳 <b>Новый счёт на оплату</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📋 Проект: {project_code}\n"
        f"🏢 Поставщик: {supplier}\n"
        f"💰 Сумма: {amount}\n"
        f"🔢 № счёта: {invoice_number}\n"
    )
    if parent_invoice_id:
        parent_inv = await db.get_invoice(parent_invoice_id)
        if parent_inv:
            msg += f"📋 Объект: Счёт №{parent_inv.get('invoice_number', '?')} — {(parent_inv.get('object_address') or '')[:40]}\n"
    if material_type:
        from ..enums import MATERIAL_TYPE_LABELS
        msg += f"📦 Материал: {MATERIAL_TYPE_LABELS.get(material_type, material_type)}\n"
    if comment:
        msg += f"💬 {comment}\n"
    _URGENCY_LABEL = {"1h": "⚡ В течение 1 часа", "7h": "🕐 В течение 7 часов", "24h": "📅 В течение 24 часов"}
    msg += f"⏰ Срочность: {_URGENCY_LABEL.get(urgency, urgency)}\n"

    from ..keyboards import task_actions_kb
    await notifier.safe_send(int(gd_id), msg, reply_markup=task_actions_kb(task))
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    for a in attachments:
        await notifier.safe_send_media(int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    await integrations.sync_task(task, project_code=project_code)
    await state.clear()

    from ..keyboards import main_menu
    role_raw = None
    user_row = await db.get_user_optional(u.id)
    if user_row:
        role_raw = user_row.role
    is_admin = u.id in (config.admin_ids or set())
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Счёт на оплату отправлен ГД.",
        reply_markup=main_menu(role_raw, is_admin=is_admin, unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
    )
