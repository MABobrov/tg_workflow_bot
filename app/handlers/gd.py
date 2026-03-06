"""Handlers specific to the GD (Генеральный директор) role.

Phase 1:
- "Срочно для ГД" — shows list of open URGENT_GD + PAYMENT_CONFIRM tasks
- "Синхронизация данных" — triggers Google Sheets resync from GD main menu

Phase 2:
- Chat-proxy buttons: Чат с РП, Замеры, Бухгалтерия, Монтажная гр., Отд.Продаж,
  КВ Кред, КИА Кред, НПН Кред
"""

from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import (
    gd_sales_submenu,
    gd_sales_write_to_kb,
    GD_BTN_ACCOUNTING,
    GD_BTN_CHAT_RP,
    GD_BTN_INVOICES,
    GD_BTN_KIA_CRED,
    GD_BTN_NPN_CRED,
    GD_BTN_KV_CRED,
    GD_BTN_MONTAZH,
    GD_BTN_SALES,
    GD_BTN_SEARCH_INVOICE,
    GD_BTN_SYNC,
    GD_BTN_URGENT,
    GD_BTN_ZAMERY,
    main_menu,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import ChatProxySG, InvoiceSearchSG, SalesWriteSG
from ..utils import private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_message
from .chat_proxy import enter_chat_menu, resolve_channel_target, channel_label

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")


# ---------------------------------------------------------------------------
# "Срочно для ГД" — for GD role: show incoming URGENT_GD + PAYMENT_CONFIRM
# ---------------------------------------------------------------------------

@router.message(F.text == GD_BTN_URGENT)
async def gd_urgent_inbox(message: Message, db: Database, config: Config) -> None:
    """Show GD a combined list of open URGENT_GD and PAYMENT_CONFIRM tasks."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    urgent_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.URGENT_GD,
        limit=50,
    )

    payment_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.PAYMENT_CONFIRM,
        limit=50,
    )

    all_tasks = urgent_tasks + payment_tasks
    all_tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)

    is_admin = user_id in (config.admin_ids or set())

    if not all_tasks:
        await message.answer(
            "✅ Нет открытых срочных запросов и подтверждений оплат.",
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
        )
        return

    n_urgent = len(urgent_tasks)
    n_payment = len(payment_tasks)

    header_parts = []
    if n_urgent:
        header_parts.append(f"🚨 Срочных: {n_urgent}")
    if n_payment:
        header_parts.append(f"💰 Оплат: {n_payment}")

    text = (
        f"<b>Срочно для ГД</b>\n"
        f"{' | '.join(header_parts)}\n\n"
        "Выберите задачу для просмотра:"
    )

    await message.answer(text, reply_markup=tasks_kb(all_tasks))




# ---------------------------------------------------------------------------
# "Счета на Оплату" — show INVOICE_PAYMENT tasks for GD
# ---------------------------------------------------------------------------

@router.message(F.text == GD_BTN_INVOICES)
async def gd_invoices(message: Message, db: Database, config: Config) -> None:
    """Show list of open invoice payment tasks."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    invoice_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.INVOICE_PAYMENT,
        limit=50,
    )

    is_admin = user_id in (config.admin_ids or set())

    if not invoice_tasks:
        await message.answer(
            "✅ Нет открытых счетов на оплату.",
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
        )
        return

    await message.answer(
        f"<b>Счета на Оплату</b> ({len(invoice_tasks)}):\n\n"
        "Выберите счёт для просмотра:",
        reply_markup=tasks_kb(invoice_tasks),
    )


# ---------------------------------------------------------------------------
# "Поиск Счета" — search invoices by criteria
# ---------------------------------------------------------------------------

@router.message(F.text == GD_BTN_SEARCH_INVOICE)
async def gd_search_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    """Start invoice search flow."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    await state.clear()
    await state.set_state(InvoiceSearchSG.criteria)

    b = InlineKeyboardBuilder()
    b.button(text="По № счёта", callback_data="inv_search:invoice_number")
    b.button(text="По поставщику", callback_data="inv_search:supplier")
    b.button(text="По проекту", callback_data="inv_search:project")
    b.button(text="По сумме", callback_data="inv_search:amount")
    b.adjust(2)
    await message.answer(
        "<b>Поиск Счета</b>\n\nВыберите критерий поиска:",
        reply_markup=b.as_markup(),
    )


SEARCH_CRITERIA_LABELS = {
    "invoice_number": "№ счёта",
    "supplier": "поставщик",
    "project": "проект",
    "amount": "сумма",
}


@router.callback_query(F.data.startswith("inv_search:"))
async def gd_search_pick_criteria(cb: CallbackQuery, state: FSMContext) -> None:
    """User picked a search criterion."""
    await cb.answer()
    criteria = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.update_data(search_criteria=criteria)
    await state.set_state(InvoiceSearchSG.value)

    label = SEARCH_CRITERIA_LABELS.get(criteria, criteria)
    await cb.message.answer(  # type: ignore[union-attr]
        f"Введите значение для поиска по <b>{label}</b>:",
    )


@router.message(InvoiceSearchSG.value)
async def gd_search_execute(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Execute invoice search."""
    data = await state.get_data()
    criteria = data.get("search_criteria", "")
    value = (message.text or "").strip()

    if not value:
        await message.answer("Введите значение для поиска:")
        return

    # Search in tasks payload
    results = await db.search_tasks_by_payload(
        field=criteria,
        value=value,
        type_filter=[TaskType.INVOICE_PAYMENT, TaskType.SUPPLIER_PAYMENT],
        limit=20,
    )

    await state.clear()

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    if not results:
        await message.answer(
            "Ничего не найдено.",
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
        )
        return

    await message.answer(
        f"<b>Результаты поиска</b> ({len(results)}):",
        reply_markup=tasks_kb(results),
    )

# ---------------------------------------------------------------------------
# Chat-proxy buttons — each opens chat submenu with its channel
# ---------------------------------------------------------------------------

@router.message(F.text == GD_BTN_CHAT_RP)
async def gd_chat_rp(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="rp")


@router.message(F.text == GD_BTN_ZAMERY)
async def gd_chat_zamery(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="zamery")


@router.message(F.text == GD_BTN_ACCOUNTING)
async def gd_chat_accounting(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="accounting")


@router.message(F.text == GD_BTN_MONTAZH)
async def gd_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="montazh")


@router.message(F.text == GD_BTN_SALES)
async def gd_chat_sales(message: Message, state: FSMContext, db: Database) -> None:
    """Отд.Продаж — составной канал."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel="otd_prodazh")
    await message.answer(
        "💬 <b>Отд.Продаж</b>\n\nВыберите действие:",
        reply_markup=gd_sales_submenu(back_label="⬅️ Назад"),
    )


@router.message(F.text == GD_BTN_KV_CRED)
async def gd_chat_kv(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="manager_kv")


@router.message(F.text == GD_BTN_KIA_CRED)
async def gd_chat_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="manager_kia")


@router.message(F.text == GD_BTN_NPN_CRED)
async def gd_chat_npn(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await enter_chat_menu(message, state, channel="manager_npn")


# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Отд.Продаж — composite handlers
# ---------------------------------------------------------------------------

SALES_TARGET_MAP = {
    "➡️ РП (НПН)": "rp",
    "➡️ Менеджер КВ": "manager_kv",
    "➡️ Менеджер КИА": "manager_kia",
    "➡️ Менеджер НПН": "manager_npn",
}


@router.message(ChatProxySG.menu, F.text == "📨 Входящие")
async def sales_incoming(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Show NOT_URGENT_GD tasks from RP/managers."""
    data = await state.get_data()
    channel = data.get("channel", "")
    if channel != "otd_prodazh":
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        type_filter=TaskType.NOT_URGENT_GD,
        limit=50,
    )

    if not tasks:
        await message.answer(
            "✅ Нет входящих «Не срочно ГД».",
            reply_markup=gd_sales_submenu(),
        )
        return

    await message.answer(
        f"<b>Входящие «Не срочно»</b> ({len(tasks)}):",
        reply_markup=tasks_kb(tasks),
    )


@router.message(ChatProxySG.menu, F.text == "✏️ Написать")
async def sales_or_regular_write(message: Message, state: FSMContext) -> None:
    """Override 'Написать' for otd_prodazh — show 'Кому?' submenu."""
    data = await state.get_data()
    channel = data.get("channel", "")

    if channel == "otd_prodazh":
        await state.set_state(SalesWriteSG.pick_target)
        await message.answer(
            "✏️ <b>Написать → Отд.Продаж</b>\n\nВыберите адресата:",
            reply_markup=gd_sales_write_to_kb(),
        )
    else:
        # Default — delegate to chat_proxy enter_writing
        from .chat_proxy import enter_writing
        await enter_writing(message, state, channel)


@router.message(SalesWriteSG.pick_target)
async def sales_pick_target(message: Message, state: FSMContext) -> None:
    """User picked a target from the sales write submenu."""
    text = (message.text or "").strip()

    if text == "⬅️ Назад":
        await state.set_state(ChatProxySG.menu)
        await state.update_data(channel="otd_prodazh")
        await message.answer(
            "💬 <b>Отд.Продаж</b>\n\nВыберите действие:",
            reply_markup=gd_sales_submenu(),
        )
        return

    if text == "➡️ Всем в отдел":
        await state.set_state(SalesWriteSG.writing)
        await state.update_data(sales_targets=["rp", "manager_kv", "manager_kia", "manager_npn"])
        await message.answer(
            "✏️ <b>Написать → Всем в Отд.Продаж</b>\n\n"
            "Введите текст сообщения.\n"
            "Для отмены: /cancel",
        )
        return

    target_channel = SALES_TARGET_MAP.get(text)
    if not target_channel:
        await message.answer("Выберите адресата из кнопок.")
        return

    await state.set_state(SalesWriteSG.writing)
    await state.update_data(sales_targets=[target_channel])
    from .chat_proxy import channel_label
    label = channel_label(target_channel)
    await message.answer(
        f"✏️ <b>Написать → {label}</b>\n\n"
        "Введите текст сообщения.\n"
        "Для отмены: /cancel",
    )


@router.message(SalesWriteSG.writing)
async def sales_send_message(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Send message to selected sales targets."""
    data = await state.get_data()
    targets = data.get("sales_targets", [])
    u = message.from_user
    if not u:
        return

    text = (message.text or message.caption or "").strip()

    file_info = None
    if message.document:
        file_info = {"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id}
    elif message.photo:
        ph = message.photo[-1]
        file_info = {"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id}

    if not text and not file_info:
        await message.answer("Введите текст или прикрепите файл.")
        return

    from .chat_proxy import resolve_channel_target, channel_label, is_group_channel

    sent_count = 0
    for ch in targets:
        target_id = await resolve_channel_target(ch, db, config)
        if not target_id:
            continue

        # Save to DB
        await db.save_chat_message(
            channel=ch,
            sender_id=u.id,
            direction="outgoing",
            text=text or None,
            receiver_id=target_id if not is_group_channel(ch) else None,
            receiver_chat_id=target_id if is_group_channel(ch) else None,
            tg_message_id=message.message_id,
            has_attachment=bool(file_info),
        )

        label = channel_label(ch)
        header = f"📩 <b>От ГД</b> ({label}):\n\n"
        if text:
            await notifier.safe_send(target_id, header + text)
        if file_info:
            await notifier.safe_send_media(target_id, file_info["file_type"], file_info["file_id"], caption=message.caption)
        if not is_group_channel(ch):
            await refresh_recipient_keyboard(notifier, db, config, int(target_id))
        sent_count += 1

    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel="otd_prodazh")
    await message.answer(
        f"✅ Отправлено {sent_count} адресатам.",
        reply_markup=gd_sales_submenu(),
    )


# "Сообщение Всем" — broadcast to all channels
# ---------------------------------------------------------------------------
# "Синхронизация данных" — Google Sheets resync from GD main menu
# ---------------------------------------------------------------------------

@router.message(F.text == GD_BTN_SYNC)
async def gd_sync_data(message: Message, db: Database, config: Config, integrations: IntegrationHub) -> None:
    """Trigger Google Sheets resync from GD main menu."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())

    if not integrations.sheets:
        await message.answer(
            "⚠️ Интеграция Google Sheets выключена.",
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
        )
        return

    await message.answer("⏳ Запускаю синхронизацию данных с Google Sheets...")

    projects = await db.list_recent_projects(limit=10000)
    tasks = await db.list_recent_tasks(limit=50000)

    project_code_by_id: dict[int, str] = {}
    projects_ok = 0
    tasks_ok = 0

    for p in sorted(projects, key=lambda x: int(x["id"])):
        manager_label = ""
        manager_id = p.get("manager_id")
        if manager_id:
            manager = await db.get_user_optional(int(manager_id))
            if manager:
                manager_label = f"@{manager.username}" if manager.username else str(manager.telegram_id)
        await integrations.sheets.upsert_project(p, manager_label=manager_label)
        project_code = str(p.get("code") or "")
        if project_code:
            project_code_by_id[int(p["id"])] = project_code
        projects_ok += 1

    for t in sorted(tasks, key=lambda x: int(x["id"])):
        project_code = ""
        project_id = t.get("project_id")
        if project_id:
            project_code = project_code_by_id.get(int(project_id), "")
            if not project_code:
                try:
                    p = await db.get_project(int(project_id))
                    project_code = str(p.get("code") or "")
                    if project_code:
                        project_code_by_id[int(project_id)] = project_code
                except Exception:
                    project_code = ""
        await integrations.sheets.upsert_task(t, project_code=project_code)
        tasks_ok += 1

    await message.answer(
        "✅ Синхронизация завершена.\n"
        f"Проектов: <b>{projects_ok}</b>\n"
        f"Задач: <b>{tasks_ok}</b>",
        reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
    )
