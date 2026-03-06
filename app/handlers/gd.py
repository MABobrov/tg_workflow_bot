"""Handlers specific to the GD (Генеральный директор) role.

Phase 1:
- "Срочно для ГД" — shows list of open URGENT_GD + PAYMENT_CONFIRM tasks
- "Синхронизация данных" — triggers Google Sheets resync from GD main menu

Phase 2:
- Chat-proxy buttons: Чат с РП, Замеры, Бухгалтерия, Монтажная гр., Отд.Продаж,
  КВ Кред, КИА Кред, НПН Кред
"""

from __future__ import annotations

import html
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
    GD_BTN_INBOX_GD,
    GD_BTN_INVOICES,
    GD_BTN_KIA_CRED,
    GD_BTN_NPN_CRED,
    GD_BTN_KV_CRED,
    GD_BTN_MONTAZH,
    GD_BTN_SALES,
    GD_BTN_SEARCH_INVOICE,
    GD_BTN_SYNC,
    GD_BTN_ZAMERY,
    main_menu,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import ChatProxySG, InvoiceSearchSG, SalesWriteSG
from ..utils import (
    format_dt_iso,
    get_initiator_label,
    private_only_reply_markup,
    project_status_label,
    refresh_recipient_keyboard,
    task_status_label,
    task_type_label,
)
from .auth import require_role_message
from .chat_proxy import enter_chat_menu, resolve_channel_target, channel_label

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")


# ---------------------------------------------------------------------------
# "📥 Входящие для ГД" — all incoming tasks for GD
# ---------------------------------------------------------------------------

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие для ГД"))
async def gd_inbox_all(message: Message, db: Database, config: Config) -> None:
    """Show GD all open tasks (urgent, payment confirm, GD_TASK, etc.)."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    all_tasks = await db.list_tasks_for_user(
        assigned_to=user_id,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        limit=50,
    )

    is_admin = user_id in (config.admin_ids or set())

    if not all_tasks:
        await message.answer(
            "✅ Нет входящих задач.",
            reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
        )
        return

    # Count by type for summary
    n_urgent = sum(1 for t in all_tasks if t.get("type") == TaskType.URGENT_GD)
    n_payment = sum(1 for t in all_tasks if t.get("type") == TaskType.PAYMENT_CONFIRM)
    n_invoice = sum(1 for t in all_tasks if t.get("type") == TaskType.INVOICE_PAYMENT)
    n_other = len(all_tasks) - n_urgent - n_payment - n_invoice

    parts = []
    if n_urgent:
        parts.append(f"🚨 Срочных: {n_urgent}")
    if n_payment:
        parts.append(f"💰 Подтв.оплат: {n_payment}")
    if n_invoice:
        parts.append(f"📄 Счетов: {n_invoice}")
    if n_other:
        parts.append(f"📋 Прочих: {n_other}")

    text = (
        f"<b>📥 Входящие для ГД</b> ({len(all_tasks)})\n"
        f"{' | '.join(parts)}\n\n"
        "Выберите задачу:"
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
    """Trigger Google Sheets resync + show detailed task/project summary."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]
    is_admin = user_id in (config.admin_ids or set())
    tz = config.timezone

    # --- 1. Google Sheets sync (if enabled) ---
    if integrations.sheets:
        await message.answer("⏳ Запускаю синхронизацию данных с Google Sheets...")

        all_projects = await db.list_recent_projects(limit=10000)
        all_tasks_gs = await db.list_recent_tasks(limit=50000)

        project_code_by_id: dict[int, str] = {}
        projects_ok = 0
        tasks_ok = 0

        for p in sorted(all_projects, key=lambda x: int(x["id"])):
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

        for t in sorted(all_tasks_gs, key=lambda x: int(x["id"])):
            project_code = ""
            project_id = t.get("project_id")
            if project_id:
                project_code = project_code_by_id.get(int(project_id), "")
                if not project_code:
                    try:
                        proj = await db.get_project(int(project_id))
                        project_code = str(proj.get("code") or "")
                        if project_code:
                            project_code_by_id[int(project_id)] = project_code
                    except Exception:
                        project_code = ""
            await integrations.sheets.upsert_task(t, project_code=project_code)
            tasks_ok += 1

        await message.answer(
            "✅ Синхронизация Google Sheets завершена.\n"
            f"Проектов: <b>{projects_ok}</b> | Задач: <b>{tasks_ok}</b>",
        )

    # --- 2. Detailed task report ---
    active_tasks = await db.list_recent_tasks(limit=5000)
    active_tasks = [
        t for t in active_tasks
        if t.get("status") in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS)
    ]
    active_tasks.sort(key=lambda t: t.get("created_at") or "", reverse=True)

    # Pre-resolve user names
    user_cache: dict[int, str] = {}

    async def _user_label(uid: int | None) -> str:
        if not uid:
            return "—"
        uid = int(uid)
        if uid not in user_cache:
            user_cache[uid] = await get_initiator_label(db, uid)
        return user_cache[uid]

    if active_tasks:
        header = f"📋 <b>Активные задачи ({len(active_tasks)})</b>\n"
        chunks: list[str] = [header]
        current_chunk = header

        for t in active_tasks:
            created_by_label = await _user_label(t.get("created_by"))
            assigned_to_label = await _user_label(t.get("assigned_to"))
            ttype = task_type_label(t.get("type"))
            tstatus = task_status_label(t.get("status"))
            created_at = format_dt_iso(t.get("created_at"), tz)
            due_at = format_dt_iso(t.get("due_at"), tz) if t.get("due_at") else "—"

            line = (
                f"\n<b>#{t['id']}</b> {html.escape(ttype)}\n"
                f"  👤 От: {created_by_label}\n"
                f"  👉 Кому: {assigned_to_label}\n"
                f"  📌 Статус: <b>{html.escape(tstatus)}</b>\n"
                f"  🕒 Создана: {created_at}\n"
                f"  ⏰ Дедлайн: {due_at}\n"
            )

            if len(current_chunk) + len(line) > 3800:
                chunks.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += line

        if current_chunk and current_chunk != header:
            chunks.append(current_chunk)

        # Send first chunk as header, rest as continuations
        for i, chunk in enumerate(chunks):
            if i == 0:
                continue  # header was merged into first data chunk
            await message.answer(chunk)
    else:
        await message.answer("📋 Активных задач нет.")

    # --- 3. Active projects report ---
    all_projects_list = await db.list_recent_projects(limit=500)
    active_projects = [
        p for p in all_projects_list
        if p.get("status") and p.get("status") != "archive"
    ]
    active_projects.sort(key=lambda p: p.get("updated_at") or "", reverse=True)

    if active_projects:
        header_p = f"\n🏗 <b>Активные проекты ({len(active_projects)})</b>\n"
        chunks_p: list[str] = [header_p]
        current_chunk_p = header_p

        for p in active_projects:
            code = html.escape(p.get("code") or f"#{p['id']}")
            title = html.escape(p.get("title") or "—")
            client = html.escape(p.get("client") or "—")
            address = html.escape(p.get("address") or "—")
            pstatus = project_status_label(str(p.get("status") or ""))
            manager_label = await _user_label(p.get("manager_id"))
            rp_label = await _user_label(p.get("rp_id"))
            amount = p.get("amount")
            amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
            updated = format_dt_iso(p.get("updated_at"), tz)

            line = (
                f"\n<b>{code}</b> — {title}\n"
                f"  👤 Клиент: {client}\n"
                f"  📍 Адрес: {address}\n"
                f"  💰 Сумма: {amount_s}\n"
                f"  📌 Статус: <b>{html.escape(pstatus)}</b>\n"
                f"  👷 Менеджер: {manager_label}\n"
                f"  👔 РП: {rp_label}\n"
                f"  🔄 Обновлён: {updated}\n"
            )

            if len(current_chunk_p) + len(line) > 3800:
                chunks_p.append(current_chunk_p)
                current_chunk_p = line
            else:
                current_chunk_p += line

        if current_chunk_p and current_chunk_p != header_p:
            chunks_p.append(current_chunk_p)

        for i, chunk in enumerate(chunks_p):
            if i == 0:
                continue
            await message.answer(chunk)
    else:
        await message.answer("🏗 Активных проектов нет.")

    await message.answer(
        "✅ Синхронизация данных завершена.",
        reply_markup=private_only_reply_markup(message, main_menu(Role.GD, is_admin=is_admin, unread=await db.count_unread_tasks(user_id))),
    )
