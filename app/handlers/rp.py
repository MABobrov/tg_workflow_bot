from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from ..callbacks import ProjectCb, RpOkladAdvCb, RpOkladRecvCb, RpSalaryRequestCb, RpSalaryTaskCb, TaskCb
from ..config import Config
from ..db import Database, OkladAlreadyPaidError, OkladAmountExceedsRemainingError
from ..enums import ProjectStatus, Role, TaskStatus, TaskType
from ..integrations.minio_storage import MinioStorage
from ..keyboards import RP_BTN_SALARY_HUB, RP_BTN_SALARY_REQUEST, RP_BTN_ZP_RP, main_menu, projects_kb, tasks_kb, task_actions_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    AssignLeadSG,
    DeliveryRequestSG,
    InvoiceCreateSG,
    ManagerWithdrawSG,
    OrderMaterialSG,
    RpAdvanceFillSG,
    RpAdvDistributeSG,
    RpOkladReceivedSG,
    RpOkladToAdvanceSG,
    RpSalaryRequestSG,
    RpZpRequestSG,
    TintingRequestSG,
)
from ..utils import fmt_project_card, format_rp_oklad_lines, get_initiator_label, parse_amount, parse_date, parse_roles, private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from ._mirror import mirror_attachment
from .auth import require_role_callback, require_role_message
from .money_guard import money_confirm_guard

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
    # If user is in installer menu, delegate to installer handler
    if message.from_user:
        _u = await db.get_user_optional(message.from_user.id)
        if _u and _u.role and Role.INSTALLER in set(parse_roles(_u.role)):
            from ..services.menu_scope import resolve_active_menu_role
            _menu_role = resolve_active_menu_role(message.from_user.id, _u.role)
            if _menu_role == Role.INSTALLER:
                from .installer_new import start_order_materials
                return await start_order_materials(message, state, db)
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
async def order_mat_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    uid = message.from_user.id if message.from_user else "anon"
    att = await mirror_attachment(message, storage, prefix=f"rp/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать заказ».")
        return
    attachments.append(att)
    await state.update_data(attachments=attachments)
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await message.answer(f"📎 Принял. Сейчас файлов: <b>{len(attachments)}</b>.{suffix}")


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
            minio_object_key=a.get("minio_object_key"),
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


# ==================== ОПЛАТА ДОСТАВКИ (РП -> ГД) ====================

@router.message(F.text == "🚚 Оплата доставки")
async def start_delivery_payment(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    # Показать счета в работе (для привязки)
    invoices = await db.list_invoices(status="in_work", limit=30, only_regular=True, include_credit=True)
    if not invoices:
        invoices = await db.list_invoices(limit=30, only_regular=True, include_credit=True)
    if not invoices:
        await message.answer("Нет счетов для привязки доставки.")
        return

    b = InlineKeyboardBuilder()
    for inv in invoices[:20]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        label = f"📄 {num}"
        if addr:
            label += f" · {addr}"
        b.button(text=label, callback_data=f"delpay:inv:{inv['id']}")
    b.adjust(1)
    await state.set_state(DeliveryRequestSG.invoice)
    await message.answer(
        "🚚 <b>Оплата доставки</b>\n"
        "Шаг 1/3: выберите счёт.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("delpay:inv:"), DeliveryRequestSG.invoice)
async def delivery_pay_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    invoice = await db.get_invoice(inv_id)
    if not invoice:
        await cb.message.answer("Счёт не найден.")  # type: ignore
        await state.clear()
        return

    await state.update_data(invoice_id=inv_id, invoice_number=invoice.get("invoice_number", ""))
    await state.set_state(DeliveryRequestSG.comment)

    inv_num = invoice.get("invoice_number") or f"#{inv_id}"
    addr = invoice.get("object_address") or "—"
    amount = invoice.get("amount") or "—"
    est_logistics = invoice.get("estimated_logistics") or "—"

    await cb.message.answer(  # type: ignore
        f"📄 Счёт: <b>{inv_num}</b>\n"
        f"📍 Адрес: {addr}\n"
        f"💰 Сумма: {amount}\n"
        f"🚚 Расч. логистика: {est_logistics}\n\n"
        "Шаг 2/3: напишите комментарий к заявке на оплату доставки:",
    )


@router.message(DeliveryRequestSG.comment)
async def delivery_pay_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 2:
        await message.answer("Напишите комментарий подробнее:")
        return
    await state.update_data(comment=t, attachments=[])
    await state.set_state(DeliveryRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="delpay:send")
    b.adjust(1)
    await message.answer(
        "Шаг 3/3: прикрепите файлы (фото, PDF, Excel) или нажмите кнопку:\n"
        "Можно прикрепить несколько файлов по одному.",
        reply_markup=b.as_markup(),
    )


@router.message(DeliveryRequestSG.attachments)
async def delivery_pay_attachment(message: Message, state: FSMContext) -> None:
    """Collect attachments (photos, documents)."""
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
        b.button(text="✅ Отправить ГД", callback_data="delpay:send")
        b.adjust(1)
        await message.answer("Прикрепите файл (фото/PDF/Excel) или нажмите кнопку:", reply_markup=b.as_markup())
        return

    data = await state.get_data()
    attachments = data.get("attachments", [])
    attachments.append({"file_id": file_id, "type": file_type})
    await state.update_data(attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Отправить ГД (файлов: {len(attachments)})", callback_data="delpay:send")
    b.adjust(1)
    await message.answer(
        f"📎 Файл добавлен ({len(attachments)}). Ещё файл или отправить:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "delpay:send")
async def delivery_pay_finalize(
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
    inv_id = data.get("invoice_id")
    inv_num = data.get("invoice_number", "")
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])

    if not inv_id:
        await cb.message.answer("Не выбран счёт. Начните заново.")  # type: ignore
        await state.clear()
        return

    invoice = await db.get_invoice(int(inv_id))

    gd_id = await resolve_default_assignee(db, config, Role.GD)

    due = utcnow() + timedelta(hours=24)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.DELIVERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=gd_id,
        due_at_iso=to_iso(due),
        payload={
            "invoice_id": inv_id,
            "invoice_number": inv_num,
            "object_address": (invoice or {}).get("object_address", ""),
            "estimated_logistics": (invoice or {}).get("estimated_logistics"),
            "comment": comment,
            "attachments": attachments,
            "rp_id": u.id,
            "rp_username": u.username,
        },
    )

    initiator = await get_initiator_label(db, u.id)
    addr = (invoice or {}).get("object_address") or "—"
    est_log = (invoice or {}).get("estimated_logistics") or "—"
    msg = (
        "🚚 <b>Оплата доставки</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт: <b>{inv_num}</b>\n"
        f"📍 Адрес: {addr}\n"
        f"🚚 Расч. логистика: {est_log}\n"
    )
    if comment:
        msg += f"📝 Комментарий: {comment}\n"

    task_kb = task_actions_kb(task)
    if gd_id:
        await notifier.safe_send(int(gd_id), msg, reply_markup=task_kb)
        # Send attachments
        for att in attachments:
            try:
                if att.get("type") == "photo":
                    await notifier.bot.send_photo(int(gd_id), att["file_id"])
                else:
                    await notifier.bot.send_document(int(gd_id), att["file_id"])
            except Exception:
                log.warning("Failed to send delivery attachment to GD")
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await notifier.notify_workchat(msg, reply_markup=task_kb)
    await integrations.sync_task(task, project_code="")

    user_now = await db.get_user_optional(u.id)
    role_now = user_now.role if user_now else Role.RP
    await cb.message.answer(  # type: ignore
        "✅ Заявка на оплату доставки отправлена ГД." + (" ГД уведомлён." if gd_id else " ⚠️ ГД не назначен."),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role_now, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id), rp_tasks=await db.count_rp_role_tasks(u.id), rp_messages=await db.count_rp_role_messages(u.id)),
        ),
    )
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
    manager_roles = [role for role in parse_roles(manager.role) if role in {Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN}]
    await state.update_data(
        manager_id=manager_id,
        manager_label=label,
        manager_role=manager_roles[0] if manager_roles else None,
    )
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
    manager_role = data.get("manager_role")

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
            "assigned_role": manager_role,
        },
    )

    initiator = await get_initiator_label(db, u.id)
    from ..utils import build_manager_task_card
    try:
        msg = await build_manager_task_card(
            db, task, config.timezone,
            header_emoji="🎯", header_title="Новый лид",
            actor_label=initiator,
        )
    except Exception:
        log.exception("assign_lead: card render failed, fallback")
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
async def tinting_req_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    uid = message.from_user.id if message.from_user else "anon"
    att = await mirror_attachment(message, storage, prefix=f"rp/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите «✅ Создать заявку».")
        return
    attachments.append(att)
    await state.update_data(attachments=attachments)
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.{suffix}")


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
            minio_object_key=a.get("minio_object_key"),
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
    invoices = await db.list_invoices_for_selection(limit=15, only_regular=True, include_credit=True)
    if invoices:
        await state.set_state(InvoiceCreateSG.parent_invoice)
        await cb.message.answer(  # type: ignore[union-attr]
            "Шаг 1: привязка к счёту объекта (или пропустите):",
            reply_markup=invoice_select_kb(invoices, prefix="inv_create_parent", back_callback="nav:home"),
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

    from aiogram.utils.keyboard import InlineKeyboardBuilder as _IKB
    _b = _IKB()
    _b.button(text="🏢 Безналичный (юрлицо)", callback_data="inv_credit:0")
    _b.button(text="💰 Кредитный (физлицо)", callback_data="inv_credit:1")
    _b.adjust(1)
    await state.set_state(InvoiceCreateSG.credit_type)
    await message.answer(
        "Шаг 7: тип оплаты клиента:",
        reply_markup=_b.as_markup(),
    )




@router.callback_query(
    InvoiceCreateSG.credit_type,
    lambda cb: cb.data and cb.data.startswith("inv_credit:"),
)
async def invoice_credit_type(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    is_credit = int((cb.data or "").split(":", 1)[1])
    await state.update_data(is_credit=is_credit)
    label = "💰 Кредитный" if is_credit else "🏢 Безналичный"
    await cb.message.edit_text(f"✅ Тип оплаты: <b>{label}</b>")  # type: ignore[union-attr]
    data = await state.get_data()
    # §E: кредитный + есть привязка к счёту-объекту → развилка маршрута оплаты
    # (к ГД как раньше / к кредит-кошельку менеджера КВ·КИА·НПН). Без привязки —
    # только ГД: расход кошелька менеджера должен лечь в конкретный счёт (DP-DV).
    if is_credit and data.get("parent_invoice_id"):
        from aiogram.utils.keyboard import InlineKeyboardBuilder as _IKB
        _b = _IKB()
        _b.button(text="🧑‍💼 На оплату к ГД", callback_data="inv_route:gd")
        _b.button(text="🏦 К КВ", callback_data="inv_route:manager_kv")
        _b.button(text="🏦 К КИА", callback_data="inv_route:manager_kia")
        _b.button(text="🏦 К НПН", callback_data="inv_route:manager_npn")
        _b.adjust(1, 3)
        await state.set_state(InvoiceCreateSG.credit_route)
        await cb.message.answer(  # type: ignore[union-attr]
            "Кому отправить на оплату?",
            reply_markup=_b.as_markup(),
        )
        return
    from ..keyboards import urgency_kb
    await state.set_state(InvoiceCreateSG.urgency)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 8: срочность оплаты:",
        reply_markup=urgency_kb(prefix="inv_urgency"),
    )


@router.callback_query(
    InvoiceCreateSG.credit_route,
    lambda cb: cb.data and cb.data.startswith("inv_route:"),
)
async def invoice_credit_route(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """§E: маршрут оплаты кредитного счёта — к ГД (старый путь) или к кредит-
    кошельку менеджера КВ/КИА/НПН (новая ветка через §C-машинерию)."""
    await cb.answer()
    route = (cb.data or "").split(":", 1)[1]  # gd / manager_kv / manager_kia / manager_npn
    if route == "gd":
        try:
            await cb.message.edit_text("✅ Маршрут: <b>На оплату к ГД</b>")  # type: ignore[union-attr]
        except Exception:
            pass
        from ..keyboards import urgency_kb
        await state.set_state(InvoiceCreateSG.urgency)
        await cb.message.answer(  # type: ignore[union-attr]
            "Шаг 8: срочность оплаты:",
            reply_markup=urgency_kb(prefix="inv_urgency"),
        )
        return
    # Ветка «к менеджеру» → мост в §C-машинерию (расход кредит-кошелька с привязкой
    # к материнскому счёту, отложенная задача владельцу + 2-шаговое подтверждение).
    from .manager_new import rp_start_credit_to_manager
    await rp_start_credit_to_manager(cb, state, db, wallet_role=route)

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
async def invoice_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    data = await state.get_data()
    attachments = data.get("attachments", [])
    uid = message.from_user.id if message.from_user else "anon"
    att = await mirror_attachment(message, storage, prefix=f"rp/{uid}")
    if att is None:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    attachments.append(att)
    await state.update_data(attachments=attachments)
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.{suffix}")


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
            "is_credit": data.get("is_credit", 0),
        },
    )

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
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
    _credit_label = "💰 Кредитный (физлицо)" if data.get("is_credit") else "🏢 Безналичный (юрлицо)"
    msg += f"💳 Тип: {_credit_label}\n"
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


# ==================== B2 TZ v8 (cart-rework): Запрос ЗП РП (мульти-выбор ended-счетов) ====================

def _fmt_rub(value: float | int | None) -> str:
    """5500 → '5 500'. Без знака валюты (его подставляем рядом)."""
    return f"{float(value or 0):,.0f}".replace(",", " ")


def _next_month_msk() -> tuple[int, int, str]:
    """(year, month, 'YYYY-MM') следующего месяца по МСК — период ЗП (B5 v2)."""
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    ny = now_msk.year + (1 if now_msk.month == 12 else 0)
    nm = 1 if now_msk.month == 12 else now_msk.month + 1
    return ny, nm, f"{ny:04d}-{nm:02d}"


def _current_month_msk() -> tuple[int, int, str]:
    """(year, month, 'YYYY-MM') текущего месяца по МСК — для фиксации факта получения оклада."""
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    return now_msk.year, now_msk.month, f"{now_msk.year:04d}-{now_msk.month:02d}"


async def _has_pending_rp_salary(db: Database, user_id: int, month_str: str) -> bool:
    """Есть ли открытый (OPEN/IN_PROGRESS) запрос оклада РП у ГД за месяц (task RP_SALARY)."""
    try:
        existing = await db.list_tasks_by_creator_and_type(
            created_by=user_id,
            type_filter=TaskType.RP_SALARY.value,
            statuses=[TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value],
            limit=20,
        )
    except Exception:
        return False
    for t in existing:
        try:
            payload = json.loads(t.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        if payload.get("month") == month_str:
            return True
    return False


async def _list_rp_zp_eligible_invoices(db: Database) -> list[dict[str, Any]]:
    """Закрытые счета (status='ended') БЕЗ выплаченной ЗП РП, без активного запроса
    и не забранные в аванс.

    §4 гард (07.06): rp_payout_advance_at IS NULL исключает счета, чью 10%-ЗП РП уже
    забрал в кошелёк аванса (credit_rp_zp_to_advance) — иначе одна ЗП попала бы и в
    налив, и в ГД-выплату (двойной учёт). «Одна ЗП учитывается один раз».
    + гард rp-offset (user 2026-06-13): счета, чьё 10% уже зачтено из аванса РП
    (apply_rp_advance_to_invoice_now), тоже исключаем из ГД-выплаты.
    + гард AS (user 29.06): «выдана ЗП РП» = AR «Выдано РП» (rp_payout_op, сумма) И/ИЛИ
    AS «Дата РП» (rp_payout_date_op, дата) ← Импорт ОП AV/AW. Исключаем счёт, если
    заполнено ЛЮБОЕ из двух (defense-in-depth, как AN+AO у менеджера).
    """
    cur = await db.conn.execute(
        "SELECT * FROM invoices "
        "WHERE status = 'ended' "
        "  AND (rp_payout_op IS NULL OR rp_payout_op = 0) "
        "  AND (rp_payout_date_op IS NULL OR rp_payout_date_op = '') "
        "  AND (rp_request_op IS NULL OR rp_request_op = 0) "
        "  AND rp_payout_advance_at IS NULL "
        "  AND id NOT IN ("
        "      SELECT it.invoice_id FROM installer_advance_items it "
        "      JOIN installer_advance_requests r ON r.id = it.request_id "
        "      WHERE r.wallet_role = 'rp' AND it.invoice_id IS NOT NULL "
        "        AND it.offset_zp_id IS NOT NULL) "
        "ORDER BY receipt_date DESC, id DESC LIMIT 50"
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


def _render_rp_zp_cart(invoices: list[dict[str, Any]], cart: dict[int, float]) -> tuple[str, Any]:
    """Отрисовать список ended-счетов с подсветкой выбранных + корзина + кнопки."""
    lines = ["💰 <b>Запрос ЗП РП</b>\n"]
    lines.append("Выберите закрытые счета, по которым ещё не получали ЗП (10% от прибыли).")
    lines.append("Нажмите на счёт → введите сумму ЗП.")
    lines.append("Можно объединить несколько счетов одним запросом.\n")
    if cart:
        lines.append("<b>Выбрано:</b>")
        for inv_id, amount in cart.items():
            inv = next((i for i in invoices if int(i["id"]) == int(inv_id)), None)
            if not inv:
                continue
            num = inv.get("invoice_number") or "?"
            lines.append(f"  ✓ №{num} — {_fmt_rub(amount)} ₽")
        total = sum(float(v) for v in cart.values())
        lines.append(f"\n<b>Итого: {_fmt_rub(total)} ₽</b>")
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = int(inv["id"])
        is_selected = inv_id in cart
        num = inv.get("invoice_number") or "?"
        addr = (inv.get("object_address") or "").strip()
        prefix = "✓ " if is_selected else ""
        if is_selected:
            label = f"{prefix}№{num} — {_fmt_rub(cart[inv_id])}₽"
        else:
            label = f"{prefix}№{num}" + (f" — {addr[:20]}" if addr else "")
        b.button(text=label[:60], callback_data=f"rpzp:pick:{inv_id}")
    if cart:
        total = sum(float(v) for v in cart.values())
        b.button(text=f"📤 Отправить ГД ({_fmt_rub(total)} ₽)", callback_data="rpzp:send")
    b.button(text="❌ Отмена", callback_data="rpzp:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


def _build_rp_zp_cart_from_source(
    invoices: list[dict[str, Any]],
) -> tuple[dict[int, float], int]:
    """Авто-корзина 10% строго из источника (Импорт ОП · столбец T «РП - 10 %» → rp_10_pct_op).

    Все eligible-счета с заполненным 10% попадают в одну корзину; счета без
    значения в источнике пропускаются (возвращается их количество).
    """
    cart: dict[int, float] = {}
    skipped = 0
    for inv in invoices:
        amount = float(inv.get("rp_10_pct_op") or 0)
        if amount > 0:
            cart[int(inv["id"])] = round(amount, 2)
        else:
            skipped += 1
    return cart, skipped


# ==================== РАЗДЕЛЕНИЕ ЗП РП ПО ТИПУ ОПЛАТЫ (кредит / б/н) ====================
# ТЗ 30.06 (owner): экран «ЗП от завершённых счетов» делит eligible-счета на
# 🏦 кредитные (is_credit=1) и 💳 б/н (is_credit=0). Каждый тип запрашивается
# ОТДЕЛЬНО → отдельная задача ГД. Внутри типа РП тумблером выбирает счета
# (по умолчанию выбраны все, полные 10% из источника Импорт ОП T), бот суммирует.

PAY_TYPE_CREDIT = "credit"
PAY_TYPE_BEZNAL = "beznal"


def _pay_type_label(payment_type: str | None) -> str:
    return "🏦 Кредитные" if payment_type == PAY_TYPE_CREDIT else "💳 Б/н"


def _split_rp_zp_by_type(
    invoices: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """eligible-счета с 10%>0, разбитые на кредитные / б/н (по is_credit)."""
    out: dict[str, list[dict[str, Any]]] = {PAY_TYPE_CREDIT: [], PAY_TYPE_BEZNAL: []}
    for inv in invoices:
        if float(inv.get("rp_10_pct_op") or 0) <= 0:
            continue
        key = PAY_TYPE_CREDIT if inv.get("is_credit") else PAY_TYPE_BEZNAL
        out[key].append(inv)
    return out


def _render_rp_zp_type_menu(invoices: list[dict[str, Any]]) -> tuple[str, Any]:
    """Экран 1: выбор типа оплаты с Итого по каждому (кнопка только для непустого типа)."""
    groups = _split_rp_zp_by_type(invoices)
    cr = groups[PAY_TYPE_CREDIT]
    bn = groups[PAY_TYPE_BEZNAL]
    cr_total = sum(float(i.get("rp_10_pct_op") or 0) for i in cr)
    bn_total = sum(float(i.get("rp_10_pct_op") or 0) for i in bn)
    lines = ["<pre>💰 <b>Запрос ЗП РП</b>"]
    lines.append(f"   🏦 Кредитные  {len(cr):>2d} сч  {_fmt_rub(cr_total):>10s} ₽")
    lines.append(f"   💳 Б/н        {len(bn):>2d} сч  {_fmt_rub(bn_total):>10s} ₽")
    lines.append("</pre>")
    lines.append("\nКредит и б/н запрашиваются раздельно. Выберите тип:")
    b = InlineKeyboardBuilder()
    if cr:
        b.button(text=f"🏦 Кредитные — {_fmt_rub(cr_total)} ₽",
                 callback_data=f"rpzp:type:{PAY_TYPE_CREDIT}")
    if bn:
        b.button(text=f"💳 Б/н — {_fmt_rub(bn_total)} ₽",
                 callback_data=f"rpzp:type:{PAY_TYPE_BEZNAL}")
    b.button(text="❌ Отмена", callback_data="rpzp:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


def _render_rp_zp_type_cart(
    type_invoices: list[dict[str, Any]],
    cart: dict[int, float],
    payment_type: str,
) -> tuple[str, Any]:
    """Экран 2: корзина одного типа с тумблерами (✓ = выбран). Итого = сумма выбранных."""
    label = _pay_type_label(payment_type)
    lines = [f"<pre>💰 <b>Запрос ЗП РП</b> · {label}"]
    for inv in type_invoices:
        inv_id = int(inv["id"])
        num = inv.get("invoice_number") or "?"
        amount = float(inv.get("rp_10_pct_op") or 0)
        mark = "✓" if inv_id in cart else " "
        lines.append(f" {mark} №{num:<14s} {_fmt_rub(amount):>10s} ₽")
    total = sum(float(v) for v in cart.values())
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append("\nОтметьте счета (тап — вкл/выкл). По умолчанию выбраны все.")
    b = InlineKeyboardBuilder()
    for inv in type_invoices:
        inv_id = int(inv["id"])
        num = inv.get("invoice_number") or "?"
        amount = float(inv.get("rp_10_pct_op") or 0)
        mark = "✓ " if inv_id in cart else "▫️ "
        b.button(text=f"{mark}№{num} — {_fmt_rub(amount)}₽",
                 callback_data=f"rpzp:pick:{inv_id}")
    if cart:
        b.button(text=f"📤 Отправить ГД ({_fmt_rub(total)} ₽)", callback_data="rpzp:send")
    b.button(text="❌ Отмена", callback_data="rpzp:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


async def _show_rp_zp_type_menu(target: Message, state: FSMContext, db: Database) -> None:
    """Общий вход обеих кнопок запроса ЗП РП → экран выбора типа оплаты (кредит/б/н)."""
    await state.clear()
    invoices = await _list_rp_zp_eligible_invoices(db)
    groups = _split_rp_zp_by_type(invoices)
    if not groups[PAY_TYPE_CREDIT] and not groups[PAY_TYPE_BEZNAL]:
        await target.answer(
            "📭 Нет счетов «Счёт End» с невыплаченным 10%.\n\n"
            "Счёт должен быть «Счёт End», 10% ещё не выплачен, и в таблице "
            "(Импорт ОП · «РП - 10 %») должна стоять сумма."
        )
        return
    await state.update_data(invoices=invoices)
    await state.set_state(RpZpRequestSG.list_invoices)
    text, kb = _render_rp_zp_type_menu(invoices)
    await target.answer(text, reply_markup=kb)


def _render_rp_zp_summary(
    invoices: list[dict[str, Any]],
    cart: dict[int, float],
    skipped: int = 0,
) -> tuple[str, Any]:
    """Сводка: все ended-счета с невыплаченным 10% одной суммой (10% из источника T)."""
    total = sum(float(v) for v in cart.values())
    lines = [f"<pre>💰 <b>Запрос ЗП РП</b>"]
    for inv_id, amount in cart.items():
        inv = next((i for i in invoices if int(i["id"]) == int(inv_id)), None)
        num = (inv or {}).get("invoice_number") or "?"
        lines.append(f"   №{num:<18s} {_fmt_rub(amount):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append(
        "\n10% по всем счетам «Счёт End» с невыплаченным РП — "
        "из таблицы (Импорт ОП · «РП - 10 %»)."
    )
    if skipped:
        lines.append(f"⚠️ Без 10% в таблице (пропущено): {skipped} счёт(ов).")
    b = InlineKeyboardBuilder()
    b.button(text=f"📤 Запросить одной суммой ({_fmt_rub(total)} ₽)", callback_data="rpzp:send")
    b.button(text="❌ Отмена", callback_data="rpzp:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


@router.message(F.text == RP_BTN_ZP_RP)
async def rp_zp_start(message: Message, state: FSMContext, db: Database) -> None:
    """B2 entry: РП жмёт «💰 Запрос ЗП РП» → выбор типа оплаты (кредит/б/н)."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await _show_rp_zp_type_menu(message, state, db)


@router.callback_query(F.data.startswith("rpzp:type:"), RpZpRequestSG.list_invoices)
async def rp_zp_type(cb: CallbackQuery, state: FSMContext) -> None:
    """Экран 1 → выбор типа: строим корзину этого типа (все 10%) и показываем тумблеры."""
    await cb.answer()
    payment_type = (cb.data or "").split(":")[-1]
    if payment_type not in (PAY_TYPE_CREDIT, PAY_TYPE_BEZNAL):
        return
    data = await state.get_data()
    invoices = data.get("invoices") or []
    type_invoices = _split_rp_zp_by_type(invoices).get(payment_type) or []
    if not type_invoices:
        await cb.answer("По этому типу нет счетов.", show_alert=True)
        return
    cart = {int(i["id"]): round(float(i.get("rp_10_pct_op") or 0), 2) for i in type_invoices}
    await state.update_data(cart=cart, payment_type=payment_type)
    text, kb = _render_rp_zp_type_cart(type_invoices, cart, payment_type)
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("rpzp:pick:"), RpZpRequestSG.list_invoices)
async def rp_zp_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Тумблер: тап по счёту → вкл/выкл в корзине (полные 10% из источника)."""
    await cb.answer()
    inv_id = int((cb.data or "").split(":")[-1])
    data = await state.get_data()
    invoices = data.get("invoices") or []
    payment_type = data.get("payment_type") or PAY_TYPE_BEZNAL
    cart = {int(k): float(v) for k, v in (data.get("cart") or {}).items()}
    type_invoices = _split_rp_zp_by_type(invoices).get(payment_type) or []
    inv = next((i for i in type_invoices if int(i["id"]) == inv_id), None)
    if not inv:
        await cb.answer("Счёт не найден в текущей сессии", show_alert=True)
        return
    if inv_id in cart:
        del cart[inv_id]
    else:
        cart[inv_id] = round(float(inv.get("rp_10_pct_op") or 0), 2)
    await state.update_data(cart=cart)
    text, kb = _render_rp_zp_type_cart(type_invoices, cart, payment_type)
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.message(RpZpRequestSG.enter_amount, F.text)
async def rp_zp_amount(message: Message, state: FSMContext) -> None:
    """Cart: сумма введена → возврат к списку с обновлённой корзиной."""
    raw = (message.text or "").strip()
    if raw in ("❌ Отмена", "⬅️ Назад"):
        data = await state.get_data()
        invoices = data.get("invoices") or []
        cart = data.get("cart") or {}
        await state.set_state(RpZpRequestSG.list_invoices)
        text, kb = _render_rp_zp_cart(invoices, cart)
        await message.answer(text, reply_markup=kb)
        return
    t = raw.replace(",", ".").replace(" ", "").replace("₽", "")
    try:
        amount = float(t)
        if amount <= 0:
            raise ValueError
    except (TypeError, ValueError):
        await message.answer("Введите положительное число, например: 5500")
        return
    data = await state.get_data()
    inv_id = data.get("picking_inv_id")
    if not inv_id:
        await message.answer("❌ Сессия выбора потеряна — начните заново.")
        await state.clear()
        return
    cart = dict(data.get("cart") or {})
    cart[int(inv_id)] = amount
    invoices = data.get("invoices") or []
    await state.update_data(cart=cart, picking_inv_id=None)
    await state.set_state(RpZpRequestSG.list_invoices)
    text, kb = _render_rp_zp_cart(invoices, cart)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "rpzp:back_list", RpZpRequestSG.enter_amount)
async def rp_zp_back_list(cb: CallbackQuery, state: FSMContext) -> None:
    """Cart: вернуться к списку без сохранения суммы."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("invoices") or []
    cart = data.get("cart") or {}
    await state.set_state(RpZpRequestSG.list_invoices)
    text, kb = _render_rp_zp_cart(invoices, cart)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data == "rpzp:cancel")
async def rp_zp_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Cart: отмена запроса ЗП РП на любом шаге."""
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Запрос ЗП РП отменён.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Запрос ЗП РП отменён.")  # type: ignore[union-attr]


@router.callback_query(F.data == "rpzp:send", RpZpRequestSG.list_invoices)
async def rp_zp_send(cb: CallbackQuery, state: FSMContext) -> None:
    """Cart: «📤 Отправить» → confirm-диалог."""
    await cb.answer()
    data = await state.get_data()
    cart = data.get("cart") or {}
    invoices = data.get("invoices") or []
    if not cart:
        await cb.answer("Корзина пуста — выберите хотя бы один счёт.", show_alert=True)
        return
    payment_type = data.get("payment_type") or PAY_TYPE_BEZNAL
    total = sum(float(v) for v in cart.values())
    lines = [f"<pre>💰 <b>Запрос ЗП РП</b> · {_pay_type_label(payment_type)}"]
    for inv_id, amount in cart.items():
        inv = next((i for i in invoices if int(i["id"]) == int(inv_id)), None)
        num = (inv or {}).get("invoice_number") or "?"
        lines.append(f"   №{num:<18s} {_fmt_rub(amount):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append("\nПодтвердить отправку запроса ГД?")
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data="rpzp:confirm:yes")
    b.button(text="❌ Нет", callback_data="rpzp:confirm:no")
    b.adjust(2)
    await state.set_state(RpZpRequestSG.confirm)
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("rpzp:confirm:"), RpZpRequestSG.confirm)
@money_confirm_guard
async def rp_zp_confirm(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Cart final: UPDATE rp_request_op per-invoice + ONE task per ГД + notify."""
    answer = cb.data.split(":")[-1] if cb.data else "no"  # type: ignore[union-attr]
    if answer != "yes":
        await state.clear()
        try:
            await cb.message.edit_text("❌ Отменено.")  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]
        await cb.answer()
        return
    data = await state.get_data()
    cart = data.get("cart") or {}
    invoices = data.get("invoices") or []
    payment_type = data.get("payment_type") or PAY_TYPE_BEZNAL
    if not cart:
        await cb.answer("Корзина пуста", show_alert=True)
        await state.clear()
        return
    total = sum(float(v) for v in cart.values())
    rp_user = await db.get_user_optional(cb.from_user.id)
    rp_label = (
        f"@{rp_user.username}" if rp_user and rp_user.username
        else (rp_user.full_name if rp_user and rp_user.full_name else "РП")
    )
    # Mirror менеджерского Фикса 2 (29.06): durable-маркер rp_request_op пишем
    # ТОЛЬКО ПОСЛЕ успешного создания задачи ГД. Иначе при «нет ГД»/сбое create_task
    # счёт ушёл бы из списка «ЗП-счета» навсегда (фильтр _list_rp_zp_eligible_invoices
    # исключает rp_request_op<>0), а РП увидел бы ложное «✅ отправлено».
    # 1. inv_payload — чистая сборка из cart (без записи в БД)
    inv_payload: list[dict[str, Any]] = []
    for inv_id, amount in cart.items():
        inv_id_int = int(inv_id)
        amount_f = float(amount)
        inv = next((i for i in invoices if int(i["id"]) == inv_id_int), None)
        inv_number = (inv or {}).get("invoice_number") or "?"
        inv_payload.append({"invoice_id": inv_id_int, "invoice_number": inv_number, "amount": amount_f})
    # 2. Резолв ГД ПЕРВЫМ — нет ГД → честная ошибка, маркеры НЕ пишем, счета в списке
    gd_users = await db.find_users_by_role(Role.GD, limit=10)
    if not gd_users:
        await state.clear()
        msg = "❌ В системе нет пользователей ГД. Запрос НЕ отправлен — обратитесь к админу."
        try:
            await cb.message.edit_text(msg)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(msg)  # type: ignore[union-attr]
        await cb.answer()
        return
    # 3. Создаём ONE task per ГД с группой invoice_ids + notify
    import uuid as _uuid
    group_id = _uuid.uuid4().hex[:12]
    notify_lines = [f"<pre>💰 <b>Запрос ЗП РП</b> · {_pay_type_label(payment_type)}"]
    notify_lines.append(f"   От                    {rp_label}")
    for it in inv_payload:
        notify_lines.append(f"   №{it['invoice_number']:<18s} {_fmt_rub(it['amount']):>10s} ₽")
    notify_lines.append("   ━━━━━━━━━━━━━━━━")
    notify_lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    notify_lines.append("</pre>")
    notify_text = "\n".join(notify_lines)
    created = 0
    for gd in gd_users:
        gd_chat_id = int(gd.telegram_id)
        # Создаём task сначала, чтобы знать task_id для callback'ов
        try:
            task = await db.create_task(
                project_id=None,
                type_=TaskType.ZP_RP.value,
                status=TaskStatus.OPEN.value,
                created_by=cb.from_user.id,
                assigned_to=gd_chat_id,
                due_at_iso=None,
                payload={
                    "invoice_ids": [it["invoice_id"] for it in inv_payload],
                    "invoices": inv_payload,
                    "total": total,
                    "rp_id": cb.from_user.id,
                    "rp_name": rp_label,
                    "source": "rp_zp_request",
                    "group_id": group_id,
                    "payment_type": payment_type,
                },
            )
            task_id = int(task["id"])
            created += 1  # задача durable у ГД — даже если notify ниже упадёт
        except Exception:
            log.exception("rp_zp_confirm: create_task ZP_RP failed for gd=%s", gd_chat_id)
            continue
        kb_gd = InlineKeyboardBuilder()
        from ..callbacks import RpZpPayCb, RpZpRejectCb
        kb_gd.button(text="✅ Выплатить", callback_data=RpZpPayCb(task_id=task_id).pack())
        kb_gd.button(text="❌ Отклонить", callback_data=RpZpRejectCb(task_id=task_id).pack())
        kb_gd.adjust(2)
        try:
            await notifier.bot.send_message(
                chat_id=gd_chat_id,
                text=notify_text,
                reply_markup=kb_gd.as_markup(),
            )
        except Exception:
            log.exception("rp_zp_confirm: notify ГД %s failed", gd_chat_id)
    # 4. Ни одной задачи не создано → маркеры НЕ пишем, счета остаются в списке (повтор возможен)
    if created == 0:
        await state.clear()
        msg = "❌ Не удалось создать задачу ГД. Запрос НЕ отправлен — попробуйте позже."
        try:
            await cb.message.edit_text(msg)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(msg)  # type: ignore[union-attr]
        await cb.answer()
        return
    # 5. Задача(и) ГД созданы → durable-маркер rp_request_op + sync + audit per-invoice
    for it in inv_payload:
        inv_id_int = int(it["invoice_id"])
        amount_f = float(it["amount"])
        try:
            await db.update_invoice(inv_id_int, rp_request_op=amount_f)
            await integrations.sync_invoice_row(inv_id_int)
        except Exception:
            log.exception("rp_zp_confirm: update/sync failed for inv_id=%s", inv_id_int)
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="rp_zp_request",
                entity="invoice",
                entity_id=str(inv_id_int),
                payload={"amount": amount_f, "rp_name": rp_label, "rp_id": cb.from_user.id, "group_total": total},
            )
        except Exception:
            log.exception("rp_zp_confirm: audit() failed for inv_id=%s", inv_id_int)
    await state.clear()
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ Запрос ЗП РП ({_pay_type_label(payment_type)}) отправлен ГД.\n"
            f"Сумма: <b>{_fmt_rub(total)} ₽</b> ({len(cart)} счёт(ов))",
        )
    except Exception:
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Запрос ЗП РП ({_pay_type_label(payment_type)}) отправлен ГД.\n"
            f"Сумма: <b>{_fmt_rub(total)} ₽</b>",
        )
    await cb.answer()


# ==================== B5 v2: REQUEST-BASED ЗАПРОС ОКЛАДА РП ====================
# TZ 27.05: правило [[feedback_zp_request_initiated_by_role]] — РП сам инициирует
# запрос оклада 60К, ГД одобряет с платёжкой. Кнопка-инициатива у ГД запрещена.

RP_SALARY_MONTHLY_AMOUNT = 66_000  # синхронно с td.py:42 RP_SALARY_MONTHLY


@router.message(F.text == RP_BTN_SALARY_REQUEST)
async def rp_salary_request_start(message: Message, state: FSMContext, db: Database) -> None:
    """B5 v2 entry: РП жмёт «💼 Запрос оклада» → confirm-карточка за следующий месяц.

    Идемпотентность: если за этот месяц уже есть OPEN/IN_PROGRESS/DONE task — отказ.
    Если предыдущий был REJECTED — разрешить повтор.
    """
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    user_id = message.from_user.id if message.from_user else 0
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    # B5 v2 TZ 28.05: оклад запрашивается на СЛЕДУЮЩИЙ месяц (Павел: «авансом»)
    next_year = now_msk.year + (1 if now_msk.month == 12 else 0)
    next_month = 1 if now_msk.month == 12 else now_msk.month + 1
    month_str = f"{next_year:04d}-{next_month:02d}"
    # Идемпотентность: ищем active/done task за этот месяц
    existing = await db.list_tasks_by_creator_and_type(
        created_by=user_id,
        type_filter=TaskType.RP_SALARY.value,
        statuses=[TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value, TaskStatus.DONE.value],
        limit=20,
    )
    for t in existing:
        try:
            payload = json.loads(t.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        if payload.get("month") == month_str:
            status = t.get("status") or ""
            if status == TaskStatus.DONE.value:
                await message.answer(
                    f"❌ Оклад за <b>{month_str}</b> уже выплачен.\n"
                    f"Если нужно повторить запрос — обратитесь к ГД."
                )
            else:
                await message.answer(
                    f"❌ Запрос оклада за <b>{month_str}</b> уже отправлен и ожидает решения ГД.\n"
                    f"Повторный запрос за тот же месяц разрешён только если предыдущий отклонён."
                )
            return
    # Confirm card
    await state.clear()
    await state.set_state(RpSalaryRequestSG.confirm)
    await state.update_data(month=month_str)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data=RpSalaryRequestCb(action="submit").pack())
    b.button(text="❌ Отмена", callback_data=RpSalaryRequestCb(action="cancel").pack())
    b.adjust(2)
    # ТЗ owner 31.07: выданный ГД аванс зачитывается в оклад — РП видит остаток сразу,
    # ещё до отправки запроса. Аванса нет → карточка ровно как была.
    calc = await db.get_rp_oklad_advance_offset(user_id)
    await message.answer(
        f"<pre>💼 <b>Запрос оклада</b>\n"
        f"   Месяц                {month_str}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        + "\n".join(format_rp_oklad_lines(calc, RP_SALARY_MONTHLY_AMOUNT))
        + "</pre>\n\n"
        f"Отправить запрос ГД?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(RpSalaryRequestCb.filter(F.action == "cancel"), RpSalaryRequestSG.confirm)
async def rp_salary_request_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """B5 v2: РП отменил запрос на стадии confirm."""
    await state.clear()
    try:
        await cb.message.edit_text("❌ Отменено.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]
    await cb.answer()


@router.callback_query(RpSalaryRequestCb.filter(F.action == "submit"), RpSalaryRequestSG.confirm)
async def rp_salary_request_submit(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    notifier: Notifier,
) -> None:
    """B5 v2 final: создать task RP_SALARY assigned_to=ГД + notify pre-card."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    data = await state.get_data()
    month_str = data.get("month") or ""
    if not month_str:
        await cb.answer("Сессия потеряна — начните заново.", show_alert=True)
        await state.clear()
        return
    user_id = cb.from_user.id
    rp_user = await db.get_user_optional(user_id)
    rp_label = (
        rp_user.full_name if rp_user and rp_user.full_name
        else (f"@{rp_user.username}" if rp_user and rp_user.username else "РП")
    )
    gd_users = await db.find_users_by_role(Role.GD, limit=10)
    if not gd_users:
        await cb.message.answer("❌ В системе нет пользователей ГД. Обратитесь к админу.")  # type: ignore[union-attr]
        await state.clear()
        await cb.answer()
        return
    # Зачёт аванса (ТЗ owner 31.07). Снимок кладём в payload только для аудита — карточка
    # ГД пересчитывает величину живьём (tasks.send_task_open_card), т.к. между запросом и
    # выплатой аванс может измениться.
    calc = await db.get_rp_oklad_advance_offset(user_id)
    pre_card = (
        f"<pre>💼 <b>Запрос оклада</b>\n"
        f"   От                   {rp_label}\n"
        f"   Месяц                {month_str}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        + "\n".join(format_rp_oklad_lines(calc, RP_SALARY_MONTHLY_AMOUNT))
        + "</pre>"
    )
    created = 0
    for gd in gd_users:
        gd_chat_id = int(gd.telegram_id)
        try:
            task = await db.create_task(
                project_id=None,
                type_=TaskType.RP_SALARY.value,
                status=TaskStatus.OPEN.value,
                created_by=user_id,
                assigned_to=gd_chat_id,
                due_at_iso=None,
                payload={
                    "month": month_str,
                    "amount": RP_SALARY_MONTHLY_AMOUNT,
                    "rp_id": user_id,
                    "rp_name": rp_label,
                    "source": "rp_salary_request",
                    # Снимок на момент запроса (аудит). Смысл "amount" НЕ меняем — его
                    # читают карточка ГД и метка кнопки «Прочие ЗП».
                    "advance_offset": calc["deduct"],
                    "advance_raw": calc["raw"],
                    "payout": calc["payout"],
                },
            )
            task_id = int(task["id"])
        except Exception:
            log.exception("rp_salary_request_submit: create_task failed for gd=%s", gd_chat_id)
            continue
        kb_gd = InlineKeyboardBuilder()
        # «✅ Принято» (owner 01.08) — ровно то же, что уже есть на карточке задачи
        # (keyboards.py::task_actions_kb, ветка RP_SALARY): generic accept →
        # handlers/tasks.py::task_actions → db.accept_task ставит ТОЛЬКО accepted_at.
        # Статус остаётся open, денег не двигает, из бейджей ГД задача не исчезает —
        # они считают по status (db.py::count_gd_inbox_tasks и соседи).
        # Пуш — та поверхность, где ГД видит оклад ПЕРВЫМ, поэтому кнопка нужна и здесь.
        # Гард пишем явно, хотя задача создана строкой выше и условие истинно всегда:
        # он самодокументирует инвариант и переживёт перестановку кода.
        # ⚠️ Задача у каждого ГД СВОЯ (цикл по gd_users): «Принято» одного НЕ гасит
        # напоминания остальным — у accept нет группового закрытия по group_id,
        # в отличие от «Выплатить». Owner объём подтвердил, расширять не просил.
        _show_accept = task.get("status") == TaskStatus.OPEN and not task.get("accepted_at")
        if _show_accept:
            kb_gd.button(text="✅ Принято", callback_data=TaskCb(task_id=task_id, action="accept").pack())
        kb_gd.button(text="✅ Выплатить", callback_data=RpSalaryTaskCb(task_id=task_id, action="open").pack())
        kb_gd.button(text="❌ Отклонить", callback_data=RpSalaryTaskCb(task_id=task_id, action="reject_start").pack())
        if _show_accept:
            kb_gd.adjust(1, 2)
        else:
            kb_gd.adjust(2)
        try:
            await notifier.bot.send_message(
                chat_id=gd_chat_id,
                text=pre_card,
                reply_markup=kb_gd.as_markup(),
            )
            created += 1
        except Exception:
            log.exception("rp_salary_request_submit: notify ГД %s failed", gd_chat_id)
    try:
        await db.audit(
            actor_id=user_id,
            action="rp_salary_requested",
            entity="user",
            entity_id=str(user_id),
            payload={"month": month_str, "amount": RP_SALARY_MONTHLY_AMOUNT, "rp_name": rp_label, "gd_count": created},
        )
    except Exception:
        log.exception("rp_salary_request_submit: audit failed")
    await state.clear()
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ Запрос оклада за <b>{month_str}</b> отправлен ГД.\n"
            f"Сумма: <b>{_fmt_rub(calc['payout'])} ₽</b>"
            + (f" (оклад {_fmt_rub(RP_SALARY_MONTHLY_AMOUNT)} − аванс {_fmt_rub(calc['deduct'])})"
               if calc["deduct"] > 0 else "")
        )
    except Exception:
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Запрос оклада за <b>{month_str}</b> отправлен ГД."
        )
    await cb.answer()


# ==================== TZ 28.05: ХАБ «ЗАПРОС ЗП» — 4 ФУНКЦИИ В ОДНОМ МЕНЮ ====================
# Reply-кнопка RP_BTN_SALARY_HUB → inline-меню (Оклад / ЗП-счета / Аванс / Депозит).
# Аванс и Депозит используют существующие manager-FSM (Павел = rp,manager_npn → whitelist + role
# проверены, БД-методы role-agnostic; колонки zp_manager_* подходят).


@router.message(F.text == RP_BTN_SALARY_HUB)
async def rp_salary_hub_start(message: Message, state: FSMContext, db: Database) -> None:
    """Хаб «Запрос ЗП»: показывает inline-меню с 4 вариантами + балансы аванса/депозита."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    user_id = message.from_user.id if message.from_user else 0
    try:
        adv_balance = await db.get_advance_balance(user_id, wallet_role="rp")
    except Exception:
        adv_balance = 0.0
    try:
        depo_balance = await db.get_deposit_balance(user_id, wallet_role="rp")
    except Exception:
        depo_balance = 0.0
    text = (
        f"<pre>💰 <b>Запрос ЗП</b>\n"
        f"   Аванс              {_fmt_rub(adv_balance)} ₽\n"
        f"   Депозит            {_fmt_rub(depo_balance)} ₽</pre>\n\n"
        f"Выберите действие:"
    )
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    _in_wl = user_id in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    b = InlineKeyboardBuilder()
    b.button(text="💼 Оклад 66К (на следующий месяц)", callback_data="rp_hub:b5")
    b.button(text="✅ Оклад получен (фиксация факта)", callback_data="rp_hub:oklad_recv")
    b.button(text="💰 ЗП от завершённых счетов", callback_data="rp_hub:b2")
    # «💵 Аванс (забрать ЗП по счетам)» ОТКЛЮЧЕНО 2026-06-30: кошелёк наполняет
    # только ГД. РП — кошелёк только расход в счёт ЗП («💰 Аванс в счёт ЗП»).
    # «Аванс в счёт ЗП» (зачёт аванса в 10% счёта, user 2026-06-13; переименовано
    # из «Распределить аванс» 30.06, callback rp_hub:adv_dist без изменений) — как у
    # менеджера (funds:advdist): whitelist + есть свободный аванс. Зачёт сразу.
    if _in_wl and adv_balance > 0:
        b.button(text="💰 Аванс в счёт ЗП", callback_data="rp_hub:adv_dist")
    b.button(text="💸 Расход депозита", callback_data="rp_hub:depo")
    b.adjust(1)
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "rp_hub:b5")
async def rp_hub_b5_oklad(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Хаб → B5 v2 оклад на следующий месяц (логика rp_salary_request_start)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    user_id = cb.from_user.id
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    next_year = now_msk.year + (1 if now_msk.month == 12 else 0)
    next_month = 1 if now_msk.month == 12 else now_msk.month + 1
    month_str = f"{next_year:04d}-{next_month:02d}"
    # A2: оклад «один на месяц» — если переведён в аванс / выплачен ГД, запрос недоступен
    okl_st = await db.get_rp_oklad_advance_status(next_year, next_month)
    if okl_st["gd_paid"] or okl_st["to_advance"] > 0:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Оклад за <b>{month_str}</b> уже "
            f"{'выплачен ГД' if okl_st['gd_paid'] else 'переведён в аванс'}. Запрос недоступен."
        )
        return
    existing = await db.list_tasks_by_creator_and_type(
        created_by=user_id,
        type_filter=TaskType.RP_SALARY.value,
        statuses=[TaskStatus.OPEN.value, TaskStatus.IN_PROGRESS.value, TaskStatus.DONE.value],
        limit=20,
    )
    for t in existing:
        try:
            payload = json.loads(t.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        if payload.get("month") == month_str:
            status = t.get("status") or ""
            if status == TaskStatus.DONE.value:
                await cb.message.answer(  # type: ignore[union-attr]
                    f"❌ Оклад за <b>{month_str}</b> уже выплачен.\n"
                    f"Если нужно повторить запрос — обратитесь к ГД."
                )
            else:
                await cb.message.answer(  # type: ignore[union-attr]
                    f"❌ Запрос оклада за <b>{month_str}</b> уже отправлен и ожидает решения ГД."
                )
            return
    await state.clear()
    await state.set_state(RpSalaryRequestSG.confirm)
    await state.update_data(month=month_str)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да", callback_data=RpSalaryRequestCb(action="submit").pack())
    kb.button(text="❌ Отмена", callback_data=RpSalaryRequestCb(action="cancel").pack())
    kb.adjust(2)
    await cb.message.answer(  # type: ignore[union-attr]
        f"<pre>💼 <b>Запрос оклада</b>\n"
        f"   Месяц                {month_str}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(RP_SALARY_MONTHLY_AMOUNT)} ₽</pre>\n\n"
        f"Отправить запрос ГД?",
        reply_markup=kb.as_markup(),
    )


@router.callback_query(F.data == "rp_hub:b2")
async def rp_hub_b2_invoices(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Хаб → ЗП РП 10%: выбор типа оплаты (кредит/б/н), затем тумблер-выбор счетов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await _show_rp_zp_type_menu(cb.message, state, db)  # type: ignore[arg-type]


@router.callback_query(F.data == "rp_hub:adv")
async def rp_hub_adv_distribute(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Устар.: кнопка «Распределить аванс» убрана из хаба РП (TZ 2026-05-29 wallet-sep).

    Аванс РП гасится против ЗП РП (оклад/10%), а не распределяется по счетам
    по-менеджерски. Хендлер оставлен как защита от старых сообщений с кнопкой
    (FSM-trap): не запускает менеджерское распределение на смешанные деньги.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "ℹ️ Распределение аванса больше не используется в меню РП.\n"
        "Аванс РП учитывается при выплате оклада и 10%."
    )


@router.callback_query(F.data == "rp_hub:depo")
async def rp_hub_depo_withdraw(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Хаб → расход депозита (логика manager_withdraw_start; state-based FSM далее)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    await cb.answer()
    user_id = cb.from_user.id
    whitelist_ids = {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    if user_id not in whitelist_ids:
        await cb.message.answer(  # type: ignore[union-attr]
            "⛔ Функция недоступна.\nЗапросите ГД добавить вас в whitelist."
        )
        return
    balance = await db.get_deposit_balance(user_id, wallet_role="rp")
    if balance <= 0:
        await cb.message.answer("❌ На депозите нет средств для расхода.")  # type: ignore[union-attr]
        return
    await state.set_state(ManagerWithdrawSG.enter_amount)
    await state.update_data(mgr_withdraw_balance=balance, mgr_withdraw_wallet_role="rp")
    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 <b>Расход с депозита</b>\n"
        f"Доступно: <b>{_fmt_rub(balance)} ₽</b>\n\n"
        f"Введите сумму расхода (₽, ≤ {_fmt_rub(balance)}):"
    )


# ==================== ХАБ → «💵 АВАНС»: НАПОЛНЕНИЕ КОШЕЛЬКА ИЗ ЗП ПО СЧЕТАМ ====================
# ТЗ advance-wallet-fill 30.05: РП забирает свою незабранную ЗП (10% НПН) по выбранным
# счетам в кошелёк аванса. По счёту ставится durable-метка rp_payout_advance_at:=now
# (НЕ rp_payout_op/AR — импорт-безопасно, 07.06; одна ЗП учитывается один раз: налитый
# счёт исключён из ГД-выплаты, выплаченный ГД — из налива). Σ сумм → topup кошелька
# wallet_role='rp'. Инфо-уведомление ГД при зачислении.


def _render_rp_adv_fill(
    invoices: list[dict[str, Any]], selected: set[int],
    oklad: dict[str, Any] | None = None,
) -> tuple[str, Any]:
    """Экран наполнения аванса: счета с незабранной ЗП (10%, toggle) + перевод оклада (A2)."""
    sel_total = sum(float(i.get("npn_amount") or 0) for i in invoices if int(i["id"]) in selected)
    avail_total = sum(float(i.get("npn_amount") or 0) for i in invoices)
    lines = [f"<pre>💵 <b>Наполнить аванс</b>"]
    lines.append(f"   Доступно ЗП          {_fmt_rub(avail_total):>10s} ₽")
    lines.append(f"   Выбрано счетов       {len(selected):>10d} / {len(invoices)}")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(sel_total)} ₽</pre>")
    if invoices:
        lines.append("\nВыберите счета, по которым забрать свою ЗП (10%) в кошелёк аванса.")
        lines.append("Выбранная ЗП уйдёт в кошелёк аванса (отразится в журнале авансирования).")
    if oklad:
        lines.append(
            f"\n💼 Доступен перевод оклада {oklad['month_str']} "
            f"(остаток {_fmt_rub(oklad['remaining'])} ₽) — кнопка ниже."
        )
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = int(inv["id"])
        num = inv.get("invoice_number") or "?"
        amount = float(inv.get("npn_amount") or 0)
        mark = "✅" if inv_id in selected else "▫️"
        b.button(text=f"{mark} №{num} — {_fmt_rub(amount)}₽", callback_data=f"rpadv:pick:{inv_id}")
    if invoices:
        if len(selected) < len(invoices):
            b.button(text="☑️ Выбрать все", callback_data="rpadv:all")
        else:
            b.button(text="⬜ Снять все", callback_data="rpadv:none")
        if selected:
            b.button(text=f"💵 Зачислить в аванс ({_fmt_rub(sel_total)} ₽)", callback_data="rpadv:credit")
    if oklad:
        b.button(
            text=f"💼 Оклад {oklad['month_str']} → аванс (остаток {_fmt_rub(oklad['remaining'])} ₽)",
            callback_data=RpOkladAdvCb(action="start").pack(),
        )
    b.button(text="❌ Отмена", callback_data="rpadv:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


async def _edit_or_send(cb: CallbackQuery, text: str, markup: Any = None) -> None:
    """edit_text текущего сообщения; при ошибке (не изменено / нет прав) — новое сообщение."""
    try:
        await cb.message.edit_text(text, reply_markup=markup)  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=markup)  # type: ignore[union-attr]


@router.callback_query(F.data == "rp_hub:adv_fill")
async def rp_hub_adv_fill_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Хаб → наполнение кошелька аванса РП — ОТКЛЮЧЕНО 2026-06-30 (только ГД).

    Гард от устаревшей inline-кнопки: наполнение кошелька сотрудником запрещено
    (включая перевод оклада), кошелёк используется только в счёт ЗП.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "⛔ Наполнение аванса доступно только ГД.\n"
        "Авансовый кошелёк используется только в счёт ЗП.")
    return
    user_id = cb.from_user.id
    invoices = await db.list_rp_advance_fill_invoices()
    # A2: доступность перевода оклада за следующий месяц
    oklad: dict[str, Any] | None = None
    try:
        ny, nm, okl_month = _next_month_msk()
        st = await db.get_rp_oklad_advance_status(ny, nm)
        if st["remaining"] > 0 and not st["gd_paid"] and not await _has_pending_rp_salary(db, user_id, okl_month):
            oklad = {"month_str": okl_month, "remaining": st["remaining"]}
    except Exception:
        log.exception("rp_hub_adv_fill_start: oklad status failed")
    if not invoices and not oklad:
        await cb.message.answer(  # type: ignore[union-attr]
            "📭 Нет ни счетов с незабранной ЗП РП (10%), ни доступного оклада.\n\n"
            "Счета появляются, когда в таблице есть сумма «НПН 10%» и ЗП ещё не выдана "
            "(столбец «Выдано РП» пуст)."
        )
        return
    selected = {int(i["id"]) for i in invoices}  # по умолчанию выбраны все
    await state.set_state(RpAdvanceFillSG.list_invoices)
    await state.update_data(adv_invoices=invoices, adv_selected=list(selected), adv_oklad=oklad)
    text, kb = _render_rp_adv_fill(invoices, selected, oklad)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("rpadv:pick:"), RpAdvanceFillSG.list_invoices)
async def rp_adv_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Toggle одного счёта в выборке наполнения аванса."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    invoices = data.get("adv_invoices") or []
    selected = set(data.get("adv_selected") or [])
    if inv_id in selected:
        selected.discard(inv_id)
    else:
        selected.add(inv_id)
    await state.update_data(adv_selected=list(selected))
    text, kb = _render_rp_adv_fill(invoices, selected, data.get("adv_oklad"))
    await _edit_or_send(cb, text, kb)


@router.callback_query(F.data == "rpadv:all", RpAdvanceFillSG.list_invoices)
async def rp_adv_all(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбрать все счета."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("adv_invoices") or []
    selected = {int(i["id"]) for i in invoices}
    await state.update_data(adv_selected=list(selected))
    text, kb = _render_rp_adv_fill(invoices, selected, data.get("adv_oklad"))
    await _edit_or_send(cb, text, kb)


@router.callback_query(F.data == "rpadv:none", RpAdvanceFillSG.list_invoices)
async def rp_adv_none(cb: CallbackQuery, state: FSMContext) -> None:
    """Снять выбор со всех счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("adv_invoices") or []
    await state.update_data(adv_selected=[])
    text, kb = _render_rp_adv_fill(invoices, set(), data.get("adv_oklad"))
    await _edit_or_send(cb, text, kb)


@router.callback_query(F.data == "rpadv:cancel")
async def rp_adv_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена наполнения аванса на любом шаге."""
    await cb.answer()
    await state.clear()
    await _edit_or_send(cb, "❌ Наполнение аванса отменено.")


@router.callback_query(F.data == "rpadv:credit", RpAdvanceFillSG.list_invoices)
async def rp_adv_credit_confirm(cb: CallbackQuery, state: FSMContext) -> None:
    """«💵 Зачислить» → подтверждение со списком выбранных счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("adv_invoices") or []
    selected = set(data.get("adv_selected") or [])
    if not selected:
        await cb.answer("Выберите хотя бы один счёт.", show_alert=True)
        return
    chosen = [i for i in invoices if int(i["id"]) in selected]
    total = sum(float(i.get("npn_amount") or 0) for i in chosen)
    lines = [f"<pre>💵 <b>Зачислить в аванс</b>"]
    for inv in chosen:
        num = inv.get("invoice_number") or "?"
        lines.append(f"   №{str(num):<18s} {_fmt_rub(float(inv.get('npn_amount') or 0)):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽")
    lines.append("</pre>")
    lines.append(f"\nЗачислить ЗП по {len(chosen)} счёт(ам) в кошелёк аванса?")
    lines.append("ЗП по ним уйдёт в кошелёк аванса (журнал авансирования; 10% по ним считается забранным).")
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data="rpadv:confirm:yes")
    b.button(text="❌ Нет", callback_data="rpadv:confirm:no")
    b.adjust(2)
    await state.set_state(RpAdvanceFillSG.confirm)
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("rpadv:confirm:"), RpAdvanceFillSG.confirm)
@money_confirm_guard
async def rp_adv_credit_apply(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Финал: credit_rp_zp_to_advance (атомарно) + sync счетов + инфо-уведомление ГД.

    ОТКЛЮЧЕНО 2026-06-30: наполнение кошелька — только ГД. Гард от устаревшего
    FSM-состояния/кнопки «✅ Да» (денежная запись не выполняется).
    """
    await state.clear()
    await cb.answer("⛔ Наполнение аванса отключено (только ГД).", show_alert=True)
    return
    answer = cb.data.split(":")[-1] if cb.data else "no"  # type: ignore[union-attr]
    if answer != "yes":
        await state.clear()
        await _edit_or_send(cb, "❌ Отменено.")
        await cb.answer()
        return
    data = await state.get_data()
    selected = list({int(x) for x in (data.get("adv_selected") or [])})
    if not selected:
        await cb.answer("Список пуст", show_alert=True)
        await state.clear()
        return
    rp_id = cb.from_user.id
    try:
        req_id, total, credited = await db.credit_rp_zp_to_advance(rp_id, selected)
    except Exception:
        log.exception("rp_adv_credit_apply: credit_rp_zp_to_advance failed rp_id=%s", rp_id)
        await state.clear()
        await cb.answer("Ошибка зачисления, попробуйте позже.", show_alert=True)
        await cb.message.answer("❌ Не удалось зачислить в аванс. Попробуйте позже.")  # type: ignore[union-attr]
        return
    # sync каждого затронутого счёта в Sheets (после успешной БД-транзакции)
    for c in credited:
        try:
            await integrations.sync_invoice_row(int(c["invoice_id"]))
        except Exception:
            log.exception("rp_adv_credit_apply: sync_invoice_row failed inv=%s", c.get("invoice_id"))
    await state.clear()
    if not credited:
        await _edit_or_send(cb, "ℹ️ Выбранные счета уже забраны ранее — изменений нет.")
        await cb.answer()
        return
    try:
        new_balance = await db.get_advance_balance(rp_id, wallet_role="rp")
    except Exception:
        new_balance = 0.0
    # Карточка РП
    lines = [f"<pre>✅ <b>Зачислено в аванс</b>"]
    for c in credited:
        lines.append(f"   №{str(c['invoice_number']):<18s} {_fmt_rub(float(c['amount'])):>10s} ₽")
    lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(total)} ₽</pre>")
    await _edit_or_send(cb, "\n".join(lines))
    await cb.answer("Зачислено")
    # Инфо-уведомление ГД (п.10 ТЗ — при использовании баланса аванса)
    rp_user = await db.get_user_optional(rp_id)
    rp_label = (
        f"@{rp_user.username}" if rp_user and rp_user.username
        else (rp_user.full_name if rp_user and rp_user.full_name else "РП")
    )
    gd_lines = [f"<pre>ℹ️ <b>РП пополнил аванс</b>"]
    gd_lines.append(f"   Кто                  {rp_label}")
    gd_lines.append(f"   Источник             ЗП РП 10% · {len(credited)} счёт(ов)")
    gd_lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    gd_lines.append("   ━━━━━━━━━━━━━━━━")
    gd_lines.append(f"   Итого  {_fmt_rub(total)} ₽</pre>")
    gd_text = "\n".join(gd_lines)
    try:
        gd_users = await db.find_users_by_role(Role.GD, limit=10)
        for gd in gd_users:
            try:
                await notifier.safe_send(int(gd.telegram_id), gd_text)
            except Exception:
                log.exception("rp_adv_credit_apply: notify ГД %s failed", gd.telegram_id)
    except Exception:
        log.exception("rp_adv_credit_apply: GD lookup failed")


# ==================== A2: перевод оклада РП (60К/часть) в кошелёк аванса ====================
# Оклад показывается кнопкой в экране наполнения аванса (rp_hub:adv_fill). Перевод пишет
# op_company_entries('ЗП РП Нижельченко', сумма) [= расход компании, кол. E БК] + ОДИН topup
# кошелька аванса (wallet_role='rp'). Взаимоисключение «один оклад в месяц» с ГД-выплатой
# (td.py rp_salary_confirm) и запросом оклада (rp_hub:b5). ТЗ advance-wallet-fill 30.05.


def _render_rp_oklad_choose(remaining: float, month_str: str) -> tuple[str, Any]:
    """Экран выбора: весь остаток / ввести часть."""
    text = (
        f"<pre>💼 <b>Оклад в аванс</b>\n"
        f"   Месяц                {month_str}\n"
        f"   Остаток оклада       {_fmt_rub(remaining):>10s} ₽\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(remaining)} ₽</pre>\n\n"
        f"Сколько перевести в кошелёк аванса?"
    )
    b = InlineKeyboardBuilder()
    b.button(text=f"💵 Весь остаток ({_fmt_rub(remaining)} ₽)", callback_data=RpOkladAdvCb(action="whole").pack())
    b.button(text="✏️ Часть (ввести сумму)", callback_data=RpOkladAdvCb(action="part").pack())
    b.button(text="❌ Отмена", callback_data=RpOkladAdvCb(action="cancel").pack())
    b.adjust(1)
    return text, b.as_markup()


def _render_rp_oklad_confirm(amount: float, remaining: float, month_str: str) -> tuple[str, Any]:
    """Экран подтверждения перевода оклада."""
    text = (
        f"<pre>💼 <b>Перевести оклад</b>\n"
        f"   Месяц                {month_str}\n"
        f"   Останется оклада     {_fmt_rub(max(0.0, remaining - amount)):>10s} ₽\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(amount)} ₽</pre>\n\n"
        f"Перевести {_fmt_rub(amount)} ₽ из оклада в кошелёк аванса?"
    )
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data=RpOkladAdvCb(action="submit").pack())
    b.button(text="❌ Нет", callback_data=RpOkladAdvCb(action="cancel").pack())
    b.adjust(2)
    return text, b.as_markup()


@router.callback_query(RpOkladAdvCb.filter(F.action == "start"))
async def rp_oklad_adv_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Открыть перевод оклада в аванс — ОТКЛЮЧЕНО 2026-06-30 (кошелёк наполняет только ГД).

    Гард от устаревшей inline-кнопки: перевод оклада в кошелёк сотрудником запрещён.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "⛔ Наполнение аванса доступно только ГД.\n"
        "Авансовый кошелёк используется только в счёт ЗП.")
    return
    user_id = cb.from_user.id
    ny, nm, month_str = _next_month_msk()
    st = await db.get_rp_oklad_advance_status(ny, nm)
    if (st["gd_paid"] or st["remaining"] <= 0
            or await _has_pending_rp_salary(db, user_id, month_str)):
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Оклад за {month_str} сейчас недоступен для перевода в аванс "
            f"(уже выплачен, переведён полностью или ожидает решения ГД)."
        )
        return
    remaining = st["remaining"]
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    await state.set_state(RpOkladToAdvanceSG.choose)
    await state.update_data(
        okl_year=ny, okl_month=nm, okl_month_str=month_str,
        okl_remaining=remaining, okl_date=now_msk.strftime("%d.%m.%Y"),
    )
    text, kb = _render_rp_oklad_choose(remaining, month_str)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(RpOkladAdvCb.filter(F.action == "whole"), RpOkladToAdvanceSG.choose)
async def rp_oklad_adv_whole(cb: CallbackQuery, state: FSMContext) -> None:
    """Перевести весь остаток оклада → подтверждение."""
    await cb.answer()
    data = await state.get_data()
    remaining = float(data.get("okl_remaining") or 0)
    month_str = data.get("okl_month_str") or ""
    if remaining <= 0:
        await state.clear()
        await cb.answer("Остаток оклада исчерпан.", show_alert=True)
        return
    await state.update_data(okl_amount=remaining)
    await state.set_state(RpOkladToAdvanceSG.confirm)
    text, kb = _render_rp_oklad_confirm(remaining, remaining, month_str)
    await _edit_or_send(cb, text, kb)


@router.callback_query(RpOkladAdvCb.filter(F.action == "part"), RpOkladToAdvanceSG.choose)
async def rp_oklad_adv_part(cb: CallbackQuery, state: FSMContext) -> None:
    """Запросить ввод части оклада."""
    await cb.answer()
    data = await state.get_data()
    remaining = float(data.get("okl_remaining") or 0)
    await state.set_state(RpOkladToAdvanceSG.enter_amount)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=RpOkladAdvCb(action="cancel").pack())
    await _edit_or_send(
        cb,
        f"Введите сумму для перевода в аванс — целое число, не больше {_fmt_rub(remaining)} ₽:",
        b.as_markup(),
    )


@router.message(RpOkladToAdvanceSG.enter_amount, F.text)
async def rp_oklad_adv_amount(message: Message, state: FSMContext) -> None:
    """Обработать ввод части оклада → подтверждение."""
    data = await state.get_data()
    remaining = float(data.get("okl_remaining") or 0)
    month_str = data.get("okl_month_str") or ""
    amount = parse_amount((message.text or "").strip())
    if amount is None or amount <= 0:
        await message.answer("❌ Введите положительное число (например, 30000).")
        return
    if amount != int(amount):
        await message.answer("❌ Оклад переводится без копеек — введите целое число.")
        return
    amount = float(int(amount))
    if amount > remaining + 1e-6:
        await message.answer(
            f"❌ Сумма больше остатка оклада ({_fmt_rub(remaining)} ₽). Введите меньше."
        )
        return
    await state.update_data(okl_amount=amount)
    await state.set_state(RpOkladToAdvanceSG.confirm)
    text, kb = _render_rp_oklad_confirm(amount, remaining, month_str)
    await message.answer(text, reply_markup=kb)


@router.callback_query(RpOkladAdvCb.filter(F.action == "cancel"))
async def rp_oklad_adv_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена перевода оклада на любом шаге."""
    await cb.answer()
    await state.clear()
    await _edit_or_send(cb, "❌ Перевод оклада в аванс отменён.")


@router.callback_query(RpOkladAdvCb.filter(F.action == "submit"), RpOkladToAdvanceSG.confirm)
async def rp_oklad_adv_submit(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Финал: credit_rp_oklad_to_advance (атомарно) + sync БК + карточка РП + инфо ГД.

    ОТКЛЮЧЕНО 2026-06-30: наполнение кошелька — только ГД. Гард от устаревшего
    FSM-состояния/кнопки «✅ Да» (денежная запись не выполняется).
    """
    await state.clear()
    await cb.answer("⛔ Наполнение аванса отключено (только ГД).", show_alert=True)
    return
    await cb.answer()
    data = await state.get_data()
    await state.clear()  # anti-replay: сразу очищаем state (повторный submit не пройдёт)
    rp_id = cb.from_user.id
    ny = int(data.get("okl_year") or 0)
    nm = int(data.get("okl_month") or 0)
    month_str = data.get("okl_month_str") or ""
    amount = round(float(data.get("okl_amount") or 0), 2)
    date_display = data.get("okl_date") or datetime.now().strftime("%d.%m.%Y")
    if not ny or not nm or amount <= 0:
        await _edit_or_send(cb, "❌ Данные перевода потерялись, начните заново.")
        return
    try:
        entry_id, req_id, rem_after = await db.credit_rp_oklad_to_advance(
            rp_id, ny, nm, amount, month_str, date_display,
        )
    except OkladAlreadyPaidError:
        await _edit_or_send(cb, f"❌ Оклад за {month_str} уже выплачен ГД — перевод невозможен.")
        return
    except OkladAmountExceedsRemainingError as e:
        await _edit_or_send(
            cb, f"❌ Остаток оклада уменьшился до {_fmt_rub(e.remaining)} ₽. Откройте перевод заново."
        )
        return
    except Exception:
        log.exception("rp_oklad_adv_submit: credit_rp_oklad_to_advance failed rp=%s", rp_id)
        await _edit_or_send(cb, "❌ Не удалось перевести оклад. Попробуйте позже.")
        return
    # sync лист «Баланс компании» (НЕ sync_invoice_row — счетов в этой операции нет)
    sync_note = ""
    if integrations.sheets:
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception as ex:
            log.warning("rp_oklad_adv_submit: sync_balance_company_sheet failed: %s", ex)
            sync_note = "\n⚠️ Лист «Баланс компании» не пересинхронизирован (ошибка)."
    try:
        new_balance = await db.get_advance_balance(rp_id, wallet_role="rp")
    except Exception:
        new_balance = 0.0
    # карточка РП
    lines = [f"<pre>✅ <b>Оклад в аванс</b>"]
    lines.append(f"   Месяц                {month_str}")
    lines.append(f"   Остаток оклада       {_fmt_rub(rem_after):>10s} ₽")
    lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(amount)} ₽</pre>")
    await _edit_or_send(cb, "\n".join(lines) + sync_note)
    # инфо-уведомление ГД (п.10 ТЗ — при использовании баланса аванса)
    rp_user = await db.get_user_optional(rp_id)
    rp_label = (
        f"@{rp_user.username}" if rp_user and rp_user.username
        else (rp_user.full_name if rp_user and rp_user.full_name else "РП")
    )
    gd_lines = [f"<pre>ℹ️ <b>РП перевёл оклад в аванс</b>"]
    gd_lines.append(f"   Кто                  {rp_label}")
    gd_lines.append(f"   Месяц                {month_str}")
    gd_lines.append(f"   Остаток оклада       {_fmt_rub(rem_after):>10s} ₽")
    gd_lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    gd_lines.append("   ━━━━━━━━━━━━━━━━")
    gd_lines.append(f"   Итого  {_fmt_rub(amount)} ₽</pre>")
    gd_text = "\n".join(gd_lines)
    try:
        gd_users = await db.find_users_by_role(Role.GD, limit=10)
        for gd in gd_users:
            try:
                await notifier.safe_send(int(gd.telegram_id), gd_text)
            except Exception:
                log.exception("rp_oklad_adv_submit: notify ГД %s failed", gd.telegram_id)
    except Exception:
        log.exception("rp_oklad_adv_submit: GD lookup failed")


# ============ ХАБ → «✅ ОКЛАД ПОЛУЧЕН»: ФИКСАЦИЯ ФАКТА ОПЛАТЫ (user 2026-06-14) ============
# РП отмечает, что оклад за ТЕКУЩИЙ месяц получен (выплачен вне бота). Эффект = как
# ГД-выплата: маркер «Оклад РП …» в «Баланс компании» (расход) → месяц закрыт
# (блокирует b5-запрос и перевод-в-аванс). Сумма = остаток (66К − ушедшее в аванс):
# итог по месяцу = 66К без двойного учёта. Платёжка опциональна. Финальный submit под
# @money_confirm_guard (анти-двойной-клик). db.record_rp_oklad_received — БЕЗ topup
# кошелька (в отличие от перевода-в-аванс): деньги пришли вне бота, РП лишь отмечает факт.


def _render_rp_oklad_recv_confirm(
    amount: float, month_str: str, has_receipt: bool,
) -> tuple[str, Any]:
    """Карточка подтверждения фиксации факта получения оклада."""
    receipt_line = "   Платёжка             приложена\n" if has_receipt else ""
    text = (
        f"<pre>✅ <b>Оклад получен</b>\n"
        f"   Месяц                {month_str}\n"
        f"{receipt_line}"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(amount)} ₽</pre>\n\n"
        f"Зафиксировать получение оклада за {month_str}?\n"
        f"Запись уйдёт в «Баланс компании», месяц закроется для запроса у ГД и перевода в аванс."
    )
    b = InlineKeyboardBuilder()
    b.button(text="✅ Зафиксировать", callback_data=RpOkladRecvCb(action="submit").pack())
    if not has_receipt:
        b.button(text="📎 С платёжкой", callback_data=RpOkladRecvCb(action="attach").pack())
    b.button(text="✖️ Отмена", callback_data=RpOkladRecvCb(action="cancel").pack())
    b.adjust(1)
    return text, b.as_markup()


@router.callback_query(F.data == "rp_hub:oklad_recv")
async def rp_oklad_recv_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Открыть фиксацию факта получения оклада за ТЕКУЩИЙ месяц."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()
    user_id = cb.from_user.id
    cy, cm, month_str = _current_month_msk()
    st = await db.get_rp_oklad_advance_status(cy, cm)
    # Гейт 1: месяц уже закрыт ГД-выплатой или предыдущей фиксацией («Оклад РП%»)
    if st["gd_paid"]:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Оклад за {month_str} уже отмечен как полученный (или выплачен ГД)."
        )
        return
    # Гейт 2: открыт запрос оклада у ГД за этот месяц → иначе двойной учёт при выплате
    if await _has_pending_rp_salary(db, user_id, month_str):
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ По окладу за {month_str} открыт запрос у ГД. Дождитесь решения ГД "
            f"(или отмените запрос) — иначе возможен двойной учёт."
        )
        return
    # Гейт 3: остаток оклада исчерпан (весь переведён в аванс)
    remaining = float(st["remaining"])
    if remaining <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Оклад за {month_str} уже закрыт (полностью переведён в аванс)."
        )
        return
    now_msk = datetime.now(ZoneInfo("Europe/Moscow")) if ZoneInfo else datetime.now()
    await state.set_state(RpOkladReceivedSG.confirm)
    await state.update_data(
        okl_year=cy, okl_month=cm, okl_month_str=month_str,
        okl_amount=remaining, okl_date=now_msk.strftime("%d.%m.%Y"),
        recv_file_id=None, recv_file_type=None,
    )
    text, kb = _render_rp_oklad_recv_confirm(remaining, month_str, has_receipt=False)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(RpOkladRecvCb.filter(F.action == "attach"), RpOkladReceivedSG.confirm)
async def rp_oklad_recv_attach(cb: CallbackQuery, state: FSMContext) -> None:
    """РП выбрал приложить платёжку → ждём фото/документ."""
    await cb.answer()
    await state.set_state(RpOkladReceivedSG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="✖️ Отмена", callback_data=RpOkladRecvCb(action="cancel").pack())
    await _edit_or_send(
        cb,
        "📎 Пришлите платёжку (фото или документ) одним сообщением.",
        b.as_markup(),
    )


@router.message(RpOkladReceivedSG.attach_receipt, F.photo | F.document)
async def rp_oklad_recv_receipt(message: Message, state: FSMContext) -> None:
    """Принять файл платёжки → вернуться к подтверждению (теперь с платёжкой)."""
    if message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
    else:  # pragma: no cover — отфильтровано F.photo | F.document
        await message.answer("📎 Пришлите фото или документ платёжки.")
        return
    data = await state.get_data()
    amount = float(data.get("okl_amount") or 0)
    month_str = data.get("okl_month_str") or ""
    await state.update_data(recv_file_id=file_id, recv_file_type=file_type)
    await state.set_state(RpOkladReceivedSG.confirm)
    text, kb = _render_rp_oklad_recv_confirm(amount, month_str, has_receipt=True)
    await message.answer(text, reply_markup=kb)


@router.message(RpOkladReceivedSG.attach_receipt)
async def rp_oklad_recv_receipt_invalid(message: Message) -> None:
    """Некорректный ввод вместо платёжки."""
    await message.answer("📎 Пришлите фото или документ платёжки, либо нажмите «✖️ Отмена».")


@router.callback_query(RpOkladRecvCb.filter(F.action == "cancel"))
async def rp_oklad_recv_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена фиксации получения оклада на любом шаге."""
    await cb.answer()
    await state.clear()
    await _edit_or_send(cb, "❌ Фиксация получения оклада отменена.")


@router.callback_query(RpOkladRecvCb.filter(F.action == "submit"), RpOkladReceivedSG.confirm)
@money_confirm_guard
async def rp_oklad_recv_submit(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Финал: record_rp_oklad_received (атомарно) + sync БК + карточка РП + инфо ГД (+опц. платёжка)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    data = await state.get_data()
    await state.clear()  # anti-replay (+ @money_confirm_guard от конкурентного двойного клика)
    rp_id = cb.from_user.id
    cy = int(data.get("okl_year") or 0)
    cm = int(data.get("okl_month") or 0)
    month_str = data.get("okl_month_str") or ""
    amount = round(float(data.get("okl_amount") or 0), 2)
    date_display = data.get("okl_date") or datetime.now().strftime("%d.%m.%Y")
    file_id = data.get("recv_file_id")
    file_type = data.get("recv_file_type")
    if not cy or not cm or amount <= 0:
        await _edit_or_send(cb, "❌ Данные фиксации потерялись, начните заново.")
        return
    # rp_label = тот же формат, что у ГД-выплаты (td.py:1534) → строки БК единообразны
    rp_user = await db.get_user_optional(rp_id)
    rp_label = (
        (rp_user.full_name if rp_user and rp_user.full_name else None)
        or (f"@{rp_user.username}" if rp_user and rp_user.username else None)
        or f"id{rp_id}"
    )
    try:
        entry_id, _rem_after = await db.record_rp_oklad_received(
            rp_id, cy, cm, amount, month_str, date_display, rp_label,
            pp_file_id=file_id, pp_file_type=file_type,
        )
    except OkladAlreadyPaidError:
        await _edit_or_send(
            cb, f"❌ Оклад за {month_str} уже отмечен/выплачен — повторная фиксация не нужна."
        )
        return
    except OkladAmountExceedsRemainingError as e:
        await _edit_or_send(
            cb, f"❌ Остаток оклада изменился до {_fmt_rub(e.remaining)} ₽. Откройте фиксацию заново."
        )
        return
    except Exception:
        log.exception("rp_oklad_recv_submit: record_rp_oklad_received failed rp=%s", rp_id)
        await _edit_or_send(cb, "❌ Не удалось зафиксировать получение оклада. Попробуйте позже.")
        return
    # sync лист «Баланс компании» (счетов в операции нет — sync_invoice_row не нужен)
    sync_note = ""
    if integrations.sheets:
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception as ex:
            log.warning("rp_oklad_recv_submit: sync_balance_company_sheet failed: %s", ex)
            sync_note = "\n⚠️ Лист «Баланс компании» не пересинхронизирован (ошибка)."
    receipt_line = "   Платёжка             приложена\n" if file_id else ""
    rp_card = (
        f"<pre>✅ <b>Оклад получен</b>\n"
        f"   Месяц                {month_str}\n"
        f"{receipt_line}"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(amount)} ₽</pre>\n"
        f"Месяц {month_str} закрыт для запроса у ГД и перевода в аванс."
    )
    await _edit_or_send(cb, rp_card + sync_note)
    # инфо-уведомление ГД (+ опц. платёжка вложением)
    gd_text = (
        f"<pre>ℹ️ <b>РП отметил получение оклада</b>\n"
        f"   Кто                  {rp_label}\n"
        f"   Месяц                {month_str}\n"
        f"   Платёжка             {'приложена' if file_id else 'нет'}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(amount)} ₽</pre>"
    )
    try:
        gd_users = await db.find_users_by_role(Role.GD, limit=10)
        for gd in gd_users:
            try:
                await notifier.safe_send(int(gd.telegram_id), gd_text)
                if file_id:
                    await notifier.safe_send_media(
                        int(gd.telegram_id), str(file_type or "document"), str(file_id),
                        caption=f"💳 Платёжка по окладу РП ({month_str})",
                    )
            except Exception:
                log.exception("rp_oklad_recv_submit: notify ГД %s failed", gd.telegram_id)
    except Exception:
        log.exception("rp_oklad_recv_submit: GD lookup failed")


# ============ ХАБ → «💰 РАСПРЕДЕЛИТЬ АВАНС»: ЗАЧЁТ АВАНСА В ЗП-10% СЧЁТА ============
# user 2026-06-13: РП тратит кошелёк аванса как менеджер «Распределить аванс», но с
# НЕМЕДЛЕННЫМ зачётом (у РП нет шага «ЗП по счёту одобрена»). Выбор счёта → сумма
# (≤ свободный аванс и ≤ остаток 10% счёта) → db.apply_rp_advance_to_invoice_now:
# сразу ЗАКРЫТЫЙ offset-item (баланс аванса −= сумма). Наличие rp-offset исключает
# счёт из налива и ГД-выплаты (одна 10%-ЗП один раз). Whitelist-гейт как у менеджера.


def _render_rp_adv_dist_list(invoices: list[dict[str, Any]], unalloc: float) -> tuple[str, Any]:
    """Экран выбора счёта для зачёта аванса в его ЗП-10%."""
    lines = [f"<pre>💰 <b>Распределить аванс</b>"]
    lines.append(f"   Свободно аванса      {_fmt_rub(unalloc):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(unalloc)} ₽</pre>")
    lines.append("\nВыберите счёт, в ЗП-10% которого зачесть аванс:")
    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or "?"
        rem = float(inv.get("remaining") or 0)
        b.button(text=f"№{num} — 10% {_fmt_rub(rem)} ₽", callback_data=f"rpdist:inv:{int(inv['id'])}")
    b.button(text="❌ Отмена", callback_data="rpdist:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


@router.callback_query(F.data == "rp_hub:adv_dist")
async def rp_hub_adv_dist_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Хаб → распределение аванса: whitelist + свободный аванс → список счетов с 10%."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    await cb.answer()
    await state.clear()
    user_id = cb.from_user.id
    if user_id not in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}:
        await cb.message.answer(  # type: ignore[union-attr]
            "⛔ Функция недоступна.\nЗапросите ГД добавить вас в whitelist."
        )
        return
    unalloc = await db.get_advance_outstanding_unallocated(user_id, wallet_role="rp")
    if unalloc <= 0:
        await cb.message.answer("✅ Нечего распределять — свободного аванса нет.")  # type: ignore[union-attr]
        return
    invoices = await db.list_rp_advance_distribute_invoices()
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "📭 Нет счетов с непокрытой ЗП РП (10%) для зачёта аванса."
        )
        return
    await state.set_state(RpAdvDistributeSG.select_invoice)
    rem_map = {str(int(i["id"])): float(i.get("remaining") or 0) for i in invoices}
    num_map = {str(int(i["id"])): (i.get("invoice_number") or f"#{i['id']}") for i in invoices}
    await state.update_data(rpdist_unalloc=unalloc, rpdist_rem=rem_map, rpdist_num=num_map)
    text, kb = _render_rp_adv_dist_list(invoices, unalloc)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(RpAdvDistributeSG.select_invoice, F.data.startswith("rpdist:inv:"))
async def rp_adv_dist_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбран счёт → запрос суммы к зачёту (≤ свободный аванс и ≤ остаток 10%)."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    unalloc = float(data.get("rpdist_unalloc") or 0)
    rem = float((data.get("rpdist_rem") or {}).get(str(invoice_id)) or 0)
    num = (data.get("rpdist_num") or {}).get(str(invoice_id)) or f"#{invoice_id}"
    limit = min(unalloc, rem)
    if limit <= 0:
        await cb.answer("По этому счёту нечего зачитывать.", show_alert=True)
        return
    await state.update_data(rpdist_invoice_id=invoice_id, rpdist_limit=limit, rpdist_cur_num=num)
    await state.set_state(RpAdvDistributeSG.enter_amount)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="rpdist:cancel")
    await _edit_or_send(
        cb,
        f"<pre>📋 <b>Счёт №{num}</b>\n"
        f"   ЗП-10% (остаток)     {_fmt_rub(rem):>10s} ₽\n"
        f"   Свободно аванса      {_fmt_rub(unalloc):>10s} ₽\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   К зачёту ≤ {_fmt_rub(limit)} ₽</pre>\n\n"
        f"Введите сумму к зачёту (₽, ≤ {_fmt_rub(limit)}):",
        b.as_markup(),
    )


@router.message(RpAdvDistributeSG.enter_amount, F.text)
async def rp_adv_dist_amount(message: Message, state: FSMContext) -> None:
    """Ввод суммы зачёта → подтверждение."""
    data = await state.get_data()
    limit = float(data.get("rpdist_limit") or 0)
    num = data.get("rpdist_cur_num") or "?"
    amount = parse_amount((message.text or "").strip())
    if amount is None or amount <= 0:
        await message.answer("❌ Введите положительное число (например, 5000).")
        return
    if amount > limit + 1e-6:
        await message.answer(f"❌ Больше лимита. Доступно к зачёту: {_fmt_rub(limit)} ₽.")
        return
    await state.update_data(rpdist_amount=float(amount))
    await state.set_state(RpAdvDistributeSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data="rpdist:confirm")
    b.button(text="❌ Нет", callback_data="rpdist:cancel")
    b.adjust(2)
    await message.answer(
        f"<pre>💰 <b>Зачесть аванс в 10%</b>\n"
        f"   Счёт                 №{num}\n"
        f"   ━━━━━━━━━━━━━━━━\n"
        f"   Итого  {_fmt_rub(amount)} ₽</pre>\n\n"
        f"Зачесть {_fmt_rub(amount)} ₽ из аванса в ЗП-10% счёта №{num}?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(RpAdvDistributeSG.confirm, F.data == "rpdist:confirm")
@money_confirm_guard
async def rp_adv_dist_confirm(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
) -> None:
    """Финал: apply_rp_advance_to_invoice_now (зачёт сразу) + sync счёта/журнала + инфо ГД."""
    await cb.answer()
    data = await state.get_data()
    await state.clear()  # anti-replay: повторный submit не пройдёт
    rp_id = cb.from_user.id
    invoice_id = int(data.get("rpdist_invoice_id") or 0)
    amount = float(data.get("rpdist_amount") or 0)
    if not invoice_id or amount <= 0:
        await _edit_or_send(cb, "❌ Данные потерялись, начните заново.")
        return
    try:
        res = await db.apply_rp_advance_to_invoice_now(rp_id, invoice_id, amount, rp_id)
    except (ValueError, RuntimeError) as e:
        await _edit_or_send(cb, f"❌ {e}")
        return
    except Exception:
        log.exception("rp_adv_dist_confirm: apply failed rp=%s inv=%s", rp_id, invoice_id)
        await _edit_or_send(cb, "❌ Не удалось зачесть аванс. Попробуйте позже.")
        return
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception:
        log.exception("rp_adv_dist_confirm: sync_invoice_row failed inv=%s", invoice_id)
    try:
        await integrations.sync_advances_journal()
    except Exception:
        log.exception("rp_adv_dist_confirm: sync_advances_journal failed")
    try:
        new_balance = await db.get_advance_balance(rp_id, wallet_role="rp")
    except Exception:
        new_balance = 0.0
    inv = await db.get_invoice(invoice_id)
    num = inv.get("invoice_number") if inv else f"#{invoice_id}"
    closed_note = (
        "10% по счёту покрыто полностью."
        if res.get("full_closed")
        else f"10% покрыто частично, остаток {_fmt_rub(res.get('remaining'))} ₽."
    )
    lines = [f"<pre>✅ <b>Аванс зачтён в 10%</b>"]
    lines.append(f"   Счёт                 №{num}")
    lines.append(f"   Зачтено              {_fmt_rub(res.get('applied')):>10s} ₽")
    lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_rub(res.get('applied'))} ₽</pre>")
    await _edit_or_send(cb, "\n".join(lines) + f"\n{closed_note}")
    # инфо-уведомление ГД (как при наливе/окладе — использование баланса аванса)
    rp_user = await db.get_user_optional(rp_id)
    rp_label = (
        f"@{rp_user.username}" if rp_user and rp_user.username
        else (rp_user.full_name if rp_user and rp_user.full_name else "РП")
    )
    gd_lines = [f"<pre>ℹ️ <b>РП зачёл аванс в 10%</b>"]
    gd_lines.append(f"   Кто                  {rp_label}")
    gd_lines.append(f"   Счёт                 №{num}")
    gd_lines.append(f"   Зачтено              {_fmt_rub(res.get('applied')):>10s} ₽")
    gd_lines.append(f"   Баланс аванса        {_fmt_rub(new_balance):>10s} ₽")
    gd_lines.append("   ━━━━━━━━━━━━━━━━")
    gd_lines.append(f"   Итого  {_fmt_rub(res.get('applied'))} ₽</pre>")
    gd_text = "\n".join(gd_lines)
    try:
        gd_users = await db.find_users_by_role(Role.GD, limit=10)
        for gd in gd_users:
            try:
                await notifier.safe_send(int(gd.telegram_id), gd_text)
            except Exception:
                log.exception("rp_adv_dist_confirm: notify ГД %s failed", gd.telegram_id)
    except Exception:
        log.exception("rp_adv_dist_confirm: GD lookup failed")


@router.callback_query(F.data == "rpdist:cancel")
async def rp_adv_dist_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена распределения аванса на любом шаге."""
    await cb.answer()
    await state.clear()
    await _edit_or_send(cb, "❌ Распределение аванса отменено.")
