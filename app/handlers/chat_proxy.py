"""Universal chat-proxy engine for GD ↔ employee/group communication.

Handles:
- Entering/exiting chat mode per channel
- Showing message history
- Sending messages (text + attachments) and forwarding them to the recipient
- Processing incoming replies from recipients back to GD
- Showing tasks related to a channel

Channels: rp, zamery, accounting, montazh, otd_prodazh, manager_kv, manager_kia, manager_npn
"""

from __future__ import annotations

import logging
import re
from typing import Any

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import (
    gd_chat_submenu,
    gd_chat_submenu_finance,
    main_menu,
    task_actions_kb,
    tasks_kb,
    GD_BTN_BACK_HOME,
)
from ..services.notifier import Notifier
from ..services.integration_hub import IntegrationHub
from ..states import ChatProxySG, GdTaskCreateSG, ReplyToGDSG
from ..utils import get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, utcnow, to_iso
from .auth import require_role_message

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")


# ---------------------------------------------------------------------------
# Channel resolution: map channel name → target user/chat id
# ---------------------------------------------------------------------------

async def resolve_channel_target(
    channel: str, db: Database, config: Config
) -> int | None:
    """Return the telegram_id (or chat_id for groups) for a given channel."""
    if channel == "rp":
        if config.default_rp_id:
            return config.default_rp_id
        if config.default_rp_username:
            u = await db.find_user_by_username(config.default_rp_username)
            return u.telegram_id if u else None
        users = await db.find_users_by_role(Role.RP, limit=1)
        return users[0].telegram_id if users else None

    if channel == "zamery":
        if config.default_zamery_id:
            return config.default_zamery_id
        if config.default_zamery_username:
            u = await db.find_user_by_username(config.default_zamery_username)
            return u.telegram_id if u else None
        return None

    if channel == "accounting":
        if config.default_accounting_id:
            return config.default_accounting_id
        if config.default_accounting_username:
            u = await db.find_user_by_username(config.default_accounting_username)
            return u.telegram_id if u else None
        users = await db.find_users_by_role(Role.ACCOUNTING, limit=1)
        return users[0].telegram_id if users else None

    if channel == "montazh":
        from ..services.assignment import get_work_chat_id
        return await get_work_chat_id(db, config)

    if channel == "manager_kv":
        if config.default_manager_kv_id:
            return config.default_manager_kv_id
        if config.default_manager_kv_username:
            u = await db.find_user_by_username(config.default_manager_kv_username)
            return u.telegram_id if u else None
        return None

    if channel == "manager_kia":
        if config.default_manager_kia_id:
            return config.default_manager_kia_id
        if config.default_manager_kia_username:
            u = await db.find_user_by_username(config.default_manager_kia_username)
            return u.telegram_id if u else None
        return None

    if channel == "manager_npn":
        if config.default_manager_npn_id:
            return config.default_manager_npn_id
        if config.default_manager_npn_username:
            u = await db.find_user_by_username(config.default_manager_npn_username)
            return u.telegram_id if u else None
        return None

    return None


def channel_label(channel: str) -> str:
    """Human-readable label for a channel."""
    labels = {
        "rp": "РП (НПН)",
        "zamery": "Замеры",
        "accounting": "Бухгалтерия",
        "montazh": "Монтажная гр.",
        "otd_prodazh": "Отд.Продаж",
        "manager_kv": "КВ Кред",
        "manager_kia": "КИА Кред",
        "manager_npn": "НПН Кред",
    }
    return labels.get(channel, channel)


def is_group_channel(channel: str) -> bool:
    """Whether this channel targets a group chat (not a user)."""
    return channel == "montazh"


def parse_amount_from_text(text: str) -> float | None:
    """Try to extract a monetary amount from text.

    Recognizes patterns like:
      - 150000
      - 150 000
      - 150000.50
      - 150 000,50
      - сумма: 150000
      - оплата 150000 руб
    Returns the first found amount or None.
    """
    if not text:
        return None
    # Pattern: optional label, then digits with optional spaces/dots as thousands sep, optional decimal
    pattern = r"(?:^|\s)(\d[\d\s.]*\d(?:[,.]\d{1,2})?)(?:\s|$|\s*(?:руб|р\b|₽))"
    matches = re.findall(pattern, text)
    if not matches:
        # Try standalone number
        pattern2 = r"(?:^|\s)(\d{4,}(?:[,.]\d{1,2})?)(?:\s|$)"
        matches = re.findall(pattern2, text)
    if not matches:
        return None
    raw = matches[0].replace(" ", "").replace(".", "").replace(",", ".")
    # If it ended with a dot after stripping, remove it
    if raw.endswith("."):
        raw = raw[:-1]
    try:
        return float(raw)
    except ValueError:
        return None


FINANCE_CHANNELS = {"manager_kv", "manager_kia", "manager_npn"}

# Composite channels: one button -> multiple underlying channels
COMPOSITE_CHANNELS = {
    "otd_prodazh": ["rp", "manager_kv", "manager_kia", "manager_npn"],
}



# ---------------------------------------------------------------------------
# Enter chat submenu
# ---------------------------------------------------------------------------

async def enter_chat_menu(
    message: Message,
    state: FSMContext,
    channel: str,
) -> None:
    """Show chat-proxy submenu for a given channel."""
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    label = channel_label(channel)
    if channel in FINANCE_CHANNELS:
        kb = gd_chat_submenu_finance(back_label="⬅️ Назад")
    else:
        kb = gd_chat_submenu(back_label="⬅️ Назад")
    await message.answer(
        f"💬 <b>{label}</b>\n\nВыберите действие:",
        reply_markup=kb,
    )


# ---------------------------------------------------------------------------
# Show message history
# ---------------------------------------------------------------------------

async def show_history(
    message: Message,
    db: Database,
    config: Config,
    channel: str,
) -> None:
    """Display last N messages for a channel."""
    limit = config.chat_history_limit

    if channel in COMPOSITE_CHANNELS:
        # Aggregate messages from all sub-channels
        all_msgs: list[dict] = []
        for sub_ch in COMPOSITE_CHANNELS[channel]:
            sub_msgs = await db.list_chat_messages(sub_ch, limit=limit)
            for m in sub_msgs:
                m["_channel"] = sub_ch
            all_msgs.extend(sub_msgs)
        all_msgs.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        messages = all_msgs[:limit]
    else:
        messages = await db.list_chat_messages(channel, limit=limit)

    label = channel_label(channel)

    if not messages:
        await message.answer(
            f"📖 <b>{label} — Переписка</b>\n\n"
            "Сообщений пока нет.",
            reply_markup=gd_chat_submenu(),
        )
        return

    lines = [f"📖 <b>{label} — Переписка</b> (последние {len(messages)}):\n"]
    for m in messages:
        direction = "➡️" if m["direction"] == "outgoing" else "⬅️"
        if channel in COMPOSITE_CHANNELS and m.get("_channel"):
            direction += f" [{channel_label(m['_channel'])}]"
        ts = m["created_at"][:16].replace("T", " ") if m.get("created_at") else ""
        text_preview = (m.get("text") or "📎 вложение")[:100]
        lines.append(f"{direction} <i>{ts}</i>  {text_preview}")

    text = "\n".join(lines)
    # Truncate if too long for Telegram
    if len(text) > 3800:
        text = text[:3800] + "\n\n... (обрезано)"

    await message.answer(text, reply_markup=gd_chat_submenu())


# ---------------------------------------------------------------------------
# Enter writing mode
# ---------------------------------------------------------------------------

async def enter_writing(
    message: Message,
    state: FSMContext,
    channel: str,
) -> None:
    """Switch to message input mode."""
    await state.set_state(ChatProxySG.writing)
    await state.update_data(channel=channel, pending_attachments=[])

    label = channel_label(channel)
    await message.answer(
        f"✏️ <b>Написать → {label}</b>\n\n"
        "Введите текст сообщения.\n"
        "Можно прикрепить файлы/фото.\n"
        "Для отмены: /cancel",
    )


# ---------------------------------------------------------------------------
# Handle outgoing message (GD writes text/attachment in writing state)
# ---------------------------------------------------------------------------

@router.message(ChatProxySG.writing)
async def handle_writing(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Process GD's outgoing message in chat-proxy."""
    data = await state.get_data()
    channel = data.get("channel", "")
    u = message.from_user
    if not u:
        return

    target_id = await resolve_channel_target(channel, db, config)
    if not target_id:
        await message.answer(
            f"⚠️ Адресат для канала «{channel_label(channel)}» не настроен.\n"
            "Попросите администратора настроить конфигурацию.",
        )
        await state.set_state(ChatProxySG.menu)
        return

    label = channel_label(channel)
    text = (message.text or message.caption or "").strip()
    has_attach = False

    # Handle file/photo attachments
    file_info: dict[str, Any] | None = None
    if message.document:
        file_info = {
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
        }
        has_attach = True
    elif message.photo:
        ph = message.photo[-1]
        file_info = {
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
        }
        has_attach = True

    if not text and not file_info:
        await message.answer("Введите текст или прикрепите файл.")
        return

    # Save to DB
    chat_msg = await db.save_chat_message(
        channel=channel,
        sender_id=u.id,
        direction="outgoing",
        text=text or None,
        receiver_id=target_id if not is_group_channel(channel) else None,
        receiver_chat_id=target_id if is_group_channel(channel) else None,
        tg_message_id=message.message_id,
        has_attachment=has_attach,
    )

    # Auto-detect sum for finance channels
    if channel in FINANCE_CHANNELS and text:
        amount = parse_amount_from_text(text)
        if amount is not None:
            await db.save_finance_entry(
                channel=channel,
                amount=amount,
                entered_by=u.id,
                chat_message_id=int(chat_msg["id"]),
                description=text[:200],
            )

    if file_info:
        await db.save_chat_attachment(
            chat_message_id=int(chat_msg["id"]),
            tg_file_id=file_info["file_id"],
            file_type=file_info["file_type"],
            tg_file_unique_id=file_info.get("file_unique_id"),
            caption=message.caption,
        )

    # Forward to recipient with reply button
    header = f"📩 <b>От ГД</b> ({label}):\n\n"
    reply_b = InlineKeyboardBuilder()
    reply_b.button(text="💬 Ответить ГД", callback_data=f"reply_to_gd:{channel}")
    reply_b.adjust(1)
    if text:
        await notifier.safe_send(target_id, header + text, reply_markup=reply_b.as_markup())
    if file_info:
        await notifier.safe_send_media(
            target_id,
            file_info["file_type"],
            file_info["file_id"],
            caption=message.caption,
        )

    await message.answer(
        f"✅ Сообщение отправлено → {label}",
        reply_markup=gd_chat_submenu(),
    )
    await state.set_state(ChatProxySG.menu)


# ---------------------------------------------------------------------------
# Show tasks for channel
# ---------------------------------------------------------------------------

async def show_channel_tasks(
    message: Message,
    db: Database,
    config: Config,
    channel: str,
    gd_user_id: int,
) -> None:
    """Show tasks related to a channel (incoming + outgoing)."""
    target_id = await resolve_channel_target(channel, db, config)
    label = channel_label(channel)

    all_tasks: list[dict[str, Any]] = []

    if target_id and not is_group_channel(channel):
        # Outgoing: GD created, assigned to target
        outgoing = await db.list_tasks_for_user(
            assigned_to=target_id,
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            limit=20,
        )
        # Filter to tasks created by GD
        outgoing = [t for t in outgoing if t.get("created_by") == gd_user_id]
        all_tasks.extend(outgoing)

        # Incoming: target created, assigned to GD
        incoming = await db.list_tasks_for_user(
            assigned_to=gd_user_id,
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            limit=20,
        )
        incoming = [t for t in incoming if t.get("created_by") == target_id]
        all_tasks.extend(incoming)

    # Deduplicate and sort
    seen_ids: set[int] = set()
    unique_tasks: list[dict[str, Any]] = []
    for t in all_tasks:
        tid = int(t["id"])
        if tid not in seen_ids:
            seen_ids.add(tid)
            unique_tasks.append(t)
    unique_tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)

    if not unique_tasks:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Создать задачу", callback_data=f"gd_task_create:{channel}")
        await message.answer(
            f"📋 <b>{label} — Задачи</b>\n\nОткрытых задач нет.",
            reply_markup=b.as_markup(),
        )
        return

    b = InlineKeyboardBuilder()
    b.button(text="➕ Создать задачу", callback_data=f"gd_task_create:{channel}")
    await message.answer(
        f"📋 <b>{label} — Задачи</b> ({len(unique_tasks)}):",
        reply_markup=tasks_kb(unique_tasks),
    )
    await message.answer("Или создайте новую:", reply_markup=b.as_markup())


# ---------------------------------------------------------------------------
# Chat submenu navigation (Переписка / Написать / Задачи / Назад)
# ---------------------------------------------------------------------------

@router.message(ChatProxySG.menu, F.text == "📖 Переписка")
async def chat_menu_history(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    await show_history(message, db, config, channel)


@router.message(ChatProxySG.menu, F.text == "✏️ Написать")
async def chat_menu_write(
    message: Message, state: FSMContext
) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    await enter_writing(message, state, channel)


@router.message(ChatProxySG.menu, F.text == "📋 Задачи")
async def chat_menu_tasks(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    u = message.from_user
    if u:
        await show_channel_tasks(message, db, config, channel, u.id)



@router.message(ChatProxySG.menu, F.text == "📊 Отчёт")
async def chat_menu_report(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    """Show finance summary for the current channel."""
    data = await state.get_data()
    channel = data.get("channel", "")

    if channel not in FINANCE_CHANNELS:
        await message.answer("Отчётность доступна только для КВ/КИА каналов.")
        return

    summary = await db.get_finance_summary(channel)
    total = summary["total"]
    entries = summary["entries"]

    label = channel_label(channel)
    lines = [f"📊 <b>{label} — Отчёт</b>\n"]
    lines.append(f"💰 Итого: <b>{total:,.2f}</b> руб.\n")

    if entries:
        lines.append("Последние записи:")
        for e in entries[:10]:
            ts = (e.get("created_at") or "")[:10]
            desc = (e.get("description") or "—")[:60]
            amt = e["amount"]
            sign = "+" if amt >= 0 else ""
            lines.append(f"  {ts}  {sign}{amt:,.2f}  {desc}")
    else:
        lines.append("Записей пока нет.")

    if channel in FINANCE_CHANNELS:
        kb = gd_chat_submenu_finance()
    else:
        kb = gd_chat_submenu()
    await message.answer("\n".join(lines), reply_markup=kb)


@router.message(ChatProxySG.menu, F.text == "⬅️ Назад")
async def chat_menu_back(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    """Return from chat submenu to main menu."""
    await state.clear()
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        "Главное меню.",
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
    )




# ---------------------------------------------------------------------------
# GD Task creation from chat-proxy
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("gd_task_create:"))
async def gd_task_create_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """GD starts creating a task for channel target."""
    await cb.answer()
    channel = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(GdTaskCreateSG.description)
    await state.update_data(task_channel=channel, task_attachments=[])

    label = channel_label(channel)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📝 <b>Новая задача → {label}</b>\n\n"
        "Шаг 1/3: опишите задачу:",
    )


@router.message(GdTaskCreateSG.description)
async def gd_task_create_desc(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите задачу подробнее (минимум 3 символа):")
        return
    await state.update_data(task_description=text)
    await state.set_state(GdTaskCreateSG.deadline)
    await message.answer(
        "Шаг 2/3: укажите срок (дд.мм.гггг) или напишите «-» без срока:",
    )


@router.message(GdTaskCreateSG.deadline)
async def gd_task_create_deadline(message: Message, state: FSMContext, config: Config) -> None:
    text = (message.text or "").strip()

    if text == "-":
        from datetime import timedelta
        due = utcnow() + timedelta(days=7)
    else:
        from ..utils import parse_date
        parsed = parse_date(text, config.timezone)
        if not parsed:
            await message.answer("Не удалось распознать дату. Укажите в формате дд.мм.гггг:")
            return
        due = parsed

    await state.update_data(task_due=to_iso(due))
    await state.set_state(GdTaskCreateSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать задачу", callback_data="gd_task_finalize")
    b.button(text="⏭ Без вложений", callback_data="gd_task_finalize")
    b.adjust(1)
    await message.answer(
        "Шаг 3/3: прикрепите файлы (по желанию). Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(GdTaskCreateSG.attachments)
async def gd_task_create_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("task_attachments", [])

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

    await state.update_data(task_attachments=attachments)
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "gd_task_finalize")
async def gd_task_create_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Create GD_TASK and notify the target."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    channel = data.get("task_channel", "")
    description = data.get("task_description", "")
    due_iso = data.get("task_due", to_iso(utcnow()))
    attachments = data.get("task_attachments", [])

    target_id = await resolve_channel_target(channel, db, config)
    if not target_id:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Адресат для {channel_label(channel)} не настроен.",
        )
        await state.clear()
        return

    task = await db.create_task(
        project_id=None,
        type_=TaskType.GD_TASK,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(target_id),
        due_at_iso=due_iso,
        payload={
            "comment": description,
            "source": f"chat_proxy:{channel}",
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

    label = channel_label(channel)
    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📝 <b>Новая задача от ГД</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📋 {description}"
    )

    from ..keyboards import task_actions_kb
    await notifier.safe_send(int(target_id), msg, reply_markup=task_actions_kb(task))

    for a in attachments:
        await notifier.safe_send_media(int(target_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(target_id))

    await integrations.sync_task(task, project_code="")
    await state.clear()

    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    is_admin = u.id in (config.admin_ids or set())
    if channel in FINANCE_CHANNELS:
        from ..keyboards import gd_chat_submenu_finance
        kb = gd_chat_submenu_finance()
    elif channel == "otd_prodazh":
        from ..keyboards import gd_sales_submenu
        kb = gd_sales_submenu()
    else:
        kb = gd_chat_submenu()

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Задача создана и отправлена → {label}.",
        reply_markup=kb,
    )

# ---------------------------------------------------------------------------
# Reply from employee to GD (incoming replies)
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("reply_to_gd:"))
async def reply_to_gd_start(cb: CallbackQuery, state: FSMContext) -> None:
    """Employee clicks 'Ответить ГД' button."""
    await cb.answer()
    channel = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(ReplyToGDSG.text)
    await state.update_data(reply_channel=channel)

    label = channel_label(channel)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💬 <b>Ответ ГД</b> (канал: {label})\n\n"
        "Введите текст ответа.\n"
        "Можно прикрепить файл.\n"
        "Для отмены: /cancel",
    )


@router.message(ReplyToGDSG.text)
async def reply_to_gd_send(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Forward employee reply to GD."""
    data = await state.get_data()
    channel = data.get("reply_channel", "")
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

    # Find GD user
    from ..services.assignment import resolve_default_assignee
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await message.answer("Не удалось найти ГД.")
        await state.clear()
        return

    # Save to DB as incoming message
    chat_msg = await db.save_chat_message(
        channel=channel,
        sender_id=u.id,
        direction="incoming",
        text=text or None,
        receiver_id=int(gd_id),
        tg_message_id=message.message_id,
        has_attachment=bool(file_info),
    )

    if file_info:
        await db.save_chat_attachment(
            chat_message_id=int(chat_msg["id"]),
            tg_file_id=file_info["file_id"],
            file_type=file_info["file_type"],
            tg_file_unique_id=file_info.get("file_unique_id"),
            caption=message.caption,
        )

    # Forward to GD
    label = channel_label(channel)
    header = f"💬 <b>Ответ от {label}</b> (@{u.username or u.id}):\n\n"
    if text:
        await notifier.safe_send(int(gd_id), header + text)
    if file_info:
        await notifier.safe_send_media(
            int(gd_id), file_info["file_type"], file_info["file_id"], caption=message.caption,
        )

    await state.clear()
    await message.answer("✅ Ответ отправлен ГД.")
