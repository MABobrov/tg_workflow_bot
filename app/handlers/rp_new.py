"""
New handlers for RP (Руководитель проектов) role.

Covers:
- Входящие Отд.Продаж
- Счета в Работу (мониторинг)
- Счет End (входящие условия)
- Проблема / Вопрос
- Менеджер 1 (КВ) / Менеджер 2 (КИА) — chat-proxy
- Монтажная гр. — chat-proxy
- Лид в проект (LeadToProjectSG)
- Смена роли (RoleSwitchSG)
- Поиск Счета
- Ответ на КП от менеджера (KpReviewResponseSG)
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, Role, TaskStatus, TaskType
from ..keyboards import (
    RP_BTN_INBOX_SALES,
    RP_BTN_INVOICE_END,
    RP_BTN_INVOICE_START,
    RP_BTN_INVOICES_PAY,
    RP_BTN_ISSUE,
    RP_BTN_LEAD,
    RP_BTN_MGR_KIA,
    RP_BTN_MGR_KV,
    RP_BTN_MONTAZH,
    RP_BTN_ROLE_RP,
    RP_BTN_ROLE_RP_INACTIVE,
    RP_BTN_ROLE_NPN,
    RP_BTN_ROLE_NPN_ACTIVE,
    invoice_list_kb,
    lead_pick_manager_kb,
    main_menu,
    rp_chat_submenu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.notifier import Notifier
from ..states import (
    KpReviewResponseSG,
    LeadToProjectSG,
    ManagerChatProxySG,
)
from ..utils import get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return user.role if user else None


# =====================================================================
# ВХОДЯЩИЕ ОТД.ПРОДАЖ
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие Отд.Продаж"))
async def rp_inbox_sales(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    if not tasks:
        await message.answer("📥 Входящих задач нет ✅")
        return
    await message.answer(
        f"📥 <b>Входящие Отд.Продаж</b> ({len(tasks)}):\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(tasks),
    )


# =====================================================================
# СЧЕТ В РАБОТУ (мониторинг для РП)
# =====================================================================

@router.message(F.text == RP_BTN_INVOICE_START)
async def rp_invoice_start_monitor(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS)
    if not invoices:
        await message.answer("💼 Нет счетов «В работе».")
        return
    await message.answer(
        f"💼 <b>Счета В Работе</b> ({len(invoices)}):\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(invoices, action_prefix="rpinv"),
    )


@router.callback_query(F.data.startswith("rpinv:view:"))
async def rp_invoice_view(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ожидает", "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен", "on_hold": "⏸ Отложен", "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )
    conditions = await db.check_close_conditions(invoice_id)
    c1 = "✅" if conditions["installer_ok"] else "⏳"
    c2 = "✅" if conditions["edo_signed"] else "⏳"
    c3 = "✅" if conditions["no_debts"] else "⏳"
    text += (
        f"\n<b>Условия:</b>\n"
        f"{c1} 1. Монтажник — Счет ОК\n"
        f"{c2} 2. ЭДО — подписано\n"
        f"{c3} 3. Долгов нет\n"
    )
    await cb.message.answer(text)  # type: ignore[union-attr]


# =====================================================================
# СЧЕТА НА ОПЛАТУ (💳 — мониторинг PENDING_PAYMENT + IN_PROGRESS)
# =====================================================================

@router.message(F.text == RP_BTN_INVOICES_PAY)
async def rp_invoices_pay(message: Message, db: Database) -> None:
    """Show invoices pending payment and in-progress for RP monitoring."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return

    pending = await db.list_invoices(status=InvoiceStatus.PENDING_PAYMENT, limit=30)
    in_progress = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS, limit=30)
    all_inv = list(pending) + list(in_progress)

    if not all_inv:
        await message.answer("💳 Нет счетов, ожидающих оплаты.")
        return

    n_pending = len(pending)
    n_progress = len(in_progress)

    header_parts = []
    if n_pending:
        header_parts.append(f"⏳ Ожидают: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")

    await message.answer(
        f"💳 <b>Счета на оплату</b>\n"
        f"{' | '.join(header_parts)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv"),
    )


# =====================================================================
# СЧЕТ END (входящие для РП)
# =====================================================================

@router.message(F.text == RP_BTN_INVOICE_END)
async def rp_invoice_end(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.CLOSING)
    ended = await db.list_invoices(status=InvoiceStatus.ENDED, limit=10)
    all_inv = list(invoices) + list(ended)

    if not all_inv:
        await message.answer("🏁 Нет счетов в процессе закрытия / закрытых.")
        return
    await message.answer(
        f"🏁 <b>Счет End</b> ({len(all_inv)}):\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv"),
    )


# =====================================================================
# ПРОБЛЕМА / ВОПРОС
# =====================================================================

@router.message(F.text == RP_BTN_ISSUE)
async def rp_issue(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    issues = [t for t in tasks if t.get("type") == TaskType.ISSUE]
    if not issues:
        await message.answer("🆘 Нет входящих проблем/вопросов.")
        return
    await message.answer(
        f"🆘 <b>Проблема / Вопрос</b> ({len(issues)}):",
        reply_markup=tasks_kb(issues),
    )


# =====================================================================
# МЕНЕДЖЕР 1 (КВ) — chat-proxy
# =====================================================================

@router.message(F.text == RP_BTN_MGR_KV)
async def rp_chat_mgr_kv(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kv")
    await message.answer(
        "👤 <b>Менеджер 1 (КВ)</b>\n\nВыберите действие:",
        reply_markup=rp_chat_submenu("⬅️ Назад"),
    )


@router.message(F.text == RP_BTN_MGR_KIA)
async def rp_chat_mgr_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kia")
    await message.answer(
        "👤 <b>Менеджер 2 (КИА)</b>\n\nВыберите действие:",
        reply_markup=rp_chat_submenu("⬅️ Назад"),
    )


# =====================================================================
# МОНТАЖНАЯ ГР. — chat-proxy
# =====================================================================

@router.message(F.text == RP_BTN_MONTAZH)
async def rp_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await message.answer(
        "🔧 <b>Монтажная гр.</b>\n\nВыберите действие:",
        reply_markup=rp_chat_submenu("⬅️ Назад"),
    )


# =====================================================================
# ЛИД В ПРОЕКТ (LeadToProjectSG)
# =====================================================================

@router.message(F.text == RP_BTN_LEAD)
async def start_lead_to_project(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(LeadToProjectSG.pick_manager)
    await message.answer(
        "🎯 <b>Лид в проект</b>\n\n"
        "Шаг 1/4: Выберите менеджера-получателя:",
        reply_markup=lead_pick_manager_kb(),
    )


@router.callback_query(F.data.startswith("lead_mgr:"))
async def lead_pick_manager(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    manager_role = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(manager_role=manager_role)
    await state.set_state(LeadToProjectSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 2/4: Опишите лид (описание + контактная информация):"
    )


@router.message(LeadToProjectSG.description)
async def lead_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(LeadToProjectSG.source)
    await message.answer("Шаг 3/4: Укажите <b>источник лида</b> (сайт / звонок / рекомендация / ...):")


@router.message(LeadToProjectSG.source)
async def lead_source(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Укажите источник:")
        return
    await state.update_data(source=text, attachments=[])
    await state.set_state(LeadToProjectSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="lead:create")
    b.button(text="⏭ Без вложений", callback_data="lead:create")
    b.adjust(1)
    await message.answer(
        "Шаг 4/4: Прикрепите файлы/фото или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(LeadToProjectSG.attachments)
async def lead_attachments(message: Message, state: FSMContext) -> None:
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
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "lead:create")
async def lead_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    manager_role = data["manager_role"]
    description = data["description"]
    source = data.get("source", "")
    attachments = data.get("attachments", [])

    # Find manager ID
    manager_id = config.get_role_id(manager_role)
    if not manager_id:
        # Try to find by role in DB
        manager_id_resolved = await resolve_default_assignee(db, config, manager_role)
        if manager_id_resolved:
            manager_id = int(manager_id_resolved)

    if not manager_id:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Менеджер {manager_role} не найден."
        )
        await state.clear()
        return

    lead_id = await db.create_lead_tracking(
        assigned_by=u.id,
        assigned_manager_id=manager_id,
        assigned_manager_role=manager_role,
        lead_source=source,
    )

    task = await db.create_task(
        project_id=None,
        type_=TaskType.LEAD_TO_PROJECT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=manager_id,
        due_at_iso=None,
        payload={
            "lead_id": lead_id,
            "description": description,
            "source": source,
            "manager_role": manager_role,
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

    role_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(manager_role, manager_role)

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"🎯 <b>Новый лид от РП</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}\n"
        f"📌 Источник: {source}\n"
    )

    from ..keyboards import task_actions_kb
    await notifier.safe_send(manager_id, msg, reply_markup=task_actions_kb(task))
    for a in attachments:
        await notifier.safe_send_media(manager_id, a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, manager_id)

    role = await _current_role(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Лид отправлен {role_label}.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
        ),
    )


# =====================================================================
# СМЕНА РОЛИ РП ↔ НПН (кнопки в первой строке меню)
# =====================================================================

@router.message(F.text.in_({RP_BTN_ROLE_NPN, RP_BTN_ROLE_RP_INACTIVE}))
async def role_switch_to_other(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Switch to the other role (RP->NPN or NPN->RP) when clicking the inactive role button."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return
    await state.clear()

    u = message.from_user
    if not u:
        return

    role = await _current_role(db, u.id)

    # Determine target role
    if role == Role.RP:
        target_role = Role.MANAGER_NPN
        role_label_str = "Менеджер НПН"
    else:
        target_role = Role.RP
        role_label_str = "РП"

    # Switch role in DB
    await db.switch_user_role(u.id, target_role)

    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        f"✅ Роль изменена на: <b>{role_label_str}</b>",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(target_role, is_admin=is_admin, unread=await db.count_unread_tasks(u.id)),
        ),
    )


@router.message(F.text.in_({RP_BTN_ROLE_RP, RP_BTN_ROLE_NPN_ACTIVE}))
async def role_switch_already_active(message: Message, db: Database, config: Config) -> None:
    """User clicked the already-active role button — just refresh the menu."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return

    u = message.from_user
    if not u:
        return

    role = await _current_role(db, u.id)
    is_admin = u.id in (config.admin_ids or set())
    role_label_str = "РП" if role == Role.RP else "Менеджер НПН"

    await message.answer(
        f"Вы уже в роли <b>{role_label_str}</b>.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=is_admin, unread=await db.count_unread_tasks(u.id)),
        ),
    )


# =====================================================================
# ПОИСК СЧЕТА — обрабатывается в manager_new.py (принимает Role.RP)
# =====================================================================


# =====================================================================
# ОТВЕТ НА КП ОТ МЕНЕДЖЕРА (KpReviewResponseSG)
# =====================================================================

@router.callback_query(F.data.regexp(r"^kp_review:\d+$"))
async def kp_review_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """RP initiates response to a CHECK_KP task."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(KpReviewResponseSG.documents)
    await state.update_data(task_id=task_id, documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "📋 <b>Ответ на КП</b>\n\n"
        "Прикрепите готовые документы:\n"
        "• Счёт\n"
        "• Договор\n"
        "• Приложение к договору\n\n"
        "Отправляйте файлы по одному."
    )


@router.message(KpReviewResponseSG.documents)
async def kp_review_documents(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    documents: list[dict[str, Any]] = data.get("documents", [])

    if message.document:
        documents.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        documents.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    else:
        if documents:
            # Text = move to comment step
            await state.update_data(documents=documents)
            await state.set_state(KpReviewResponseSG.comment)
            await message.answer("Добавьте <b>комментарий</b> (или «—» для пропуска):")
            return
        await message.answer("Пришлите файл или фото:")
        return

    await state.update_data(documents=documents)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Далее (комментарий)", callback_data="kp_review:next")
    b.adjust(1)
    await message.answer(
        f"📎 Принял. Документов: <b>{len(documents)}</b>.\n"
        "Ещё файлы или нажмите «Далее».",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "kp_review:next")
async def kp_review_next(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.set_state(KpReviewResponseSG.comment)
    await cb.message.answer(  # type: ignore[union-attr]
        "Добавьте <b>комментарий</b> (или «—» для пропуска):"
    )


@router.message(KpReviewResponseSG.comment)
async def kp_review_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    task_id = data["task_id"]
    documents = data.get("documents", [])

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json", "{}"))
    manager_id = payload.get("manager_id")
    invoice_number = payload.get("invoice_number", "?")

    # Mark task as done
    await db.update_task_status(task_id, TaskStatus.DONE)

    # Update invoice documents
    invoice_id = payload.get("invoice_id")
    if invoice_id:
        await db.update_invoice(
            invoice_id,
            documents_json=json.dumps(documents, ensure_ascii=False) if documents else None,
        )

    # Send documents to manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)
        msg = (
            f"📋 <b>Документы по счёту №{invoice_number}</b>\n"
            f"👤 От: {initiator}\n\n"
            f"РП проверил КП и подготовил:\n"
            f"• Счёт, Договор, Приложение\n"
        )
        if comment:
            msg += f"\n💬 Комментарий РП: {comment}"

        await notifier.safe_send(int(manager_id), msg)
        for doc in documents:
            await notifier.safe_send_media(
                int(manager_id), doc["file_type"], doc["file_id"], caption=doc.get("caption")
            )
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    role = await _current_role(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Документы отправлены менеджеру по счёту №{invoice_number}.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(message.from_user.id)),
        ),
    )
