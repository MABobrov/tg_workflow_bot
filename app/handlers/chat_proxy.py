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
    gd_sales_submenu,
    invoice_select_kb,
    main_menu,
    task_actions_kb,
    tasks_kb,
)
from ..services.notifier import Notifier
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..states import ChatProxySG, GdTaskCreateSG, ReplyToGDSG
from ..utils import answer_service, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, utcnow, to_iso

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
    from ..services.assignment import get_work_chat_id, resolve_default_assignee

    role_by_channel = {
        "rp": Role.RP,
        "zamery": Role.ZAMERY,
        "accounting": Role.ACCOUNTING,
        "manager_kv": Role.MANAGER_KV,
        "manager_kia": Role.MANAGER_KIA,
        "manager_npn": Role.MANAGER_NPN,
    }
    if channel == "montazh":
        return await get_work_chat_id(db, config)
    target_role = role_by_channel.get(channel)
    if target_role:
        return await resolve_default_assignee(db, config, target_role)

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
    raw = matches[0].replace(" ", "")
    # Detect whether the last separator (. or ,) is a decimal marker
    # (followed by exactly 1-2 digits at the end), or a thousands separator.
    decimal_match = re.search(r"[,.](\d{1,2})$", raw)
    if decimal_match:
        decimal_part = decimal_match.group(1)
        integer_part = raw[: decimal_match.start()]
        integer_clean = integer_part.replace(".", "").replace(",", "")
        raw_clean = integer_clean + "." + decimal_part
    else:
        # No decimal part — strip all separators
        raw_clean = raw.replace(".", "").replace(",", "")
    if not raw_clean or raw_clean == ".":
        return None
    try:
        return float(raw_clean)
    except ValueError:
        return None


FINANCE_CHANNELS = {"manager_kv", "manager_kia", "manager_npn"}

# Composite channels: one button -> multiple underlying channels
COMPOSITE_CHANNELS = {
    "otd_prodazh": ["rp", "manager_kv", "manager_kia", "manager_npn"],
}

# Write targets: who can be written to in each GD channel
# Format: list of (channel_key, button_label)
CHANNEL_WRITE_TARGETS: dict[str, list[tuple[str, str]]] = {
    "rp": [("rp", "➡️ РП (НПН)")],
    "zamery": [("zamery", "➡️ Замерщик")],
    "accounting": [("accounting", "➡️ Бухгалтерия")],
    "montazh": [("montazh", "➡️ Монтажная гр.")],
    "otd_prodazh": [
        ("rp", "➡️ РП (НПН)"),
        ("manager_kv", "➡️ Менеджер КВ"),
        ("manager_kia", "➡️ Менеджер КИА"),
        ("manager_npn", "➡️ Менеджер НПН"),
    ],
    "manager_kv": [("manager_kv", "➡️ Менеджер КВ")],
    "manager_kia": [("manager_kia", "➡️ Менеджер КИА")],
    "manager_npn": [("manager_npn", "➡️ Менеджер НПН")],
}


def gd_channel_menu(channel: str):
    """Return the correct GD submenu keyboard for a channel."""
    if channel == "otd_prodazh":
        return gd_sales_submenu()
    if channel in FINANCE_CHANNELS:
        return gd_chat_submenu_finance()
    return gd_chat_submenu()



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
    await message.answer(
        f"💬 <b>{label}</b>\n\nВыберите действие:",
        reply_markup=gd_channel_menu(channel),
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
    viewer_id = message.from_user.id if message.from_user else None

    if channel in COMPOSITE_CHANNELS:
        # Aggregate messages from all sub-channels
        all_msgs: list[dict] = []
        for sub_ch in COMPOSITE_CHANNELS[channel]:
            sub_msgs = await db.list_chat_messages(sub_ch, limit=limit)
            for m in sub_msgs:
                m["_channel"] = sub_ch
            all_msgs.extend(sub_msgs)
            # Mark incoming messages as read for viewer
            if viewer_id:
                await db.mark_messages_read(viewer_id, sub_ch)
        all_msgs.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        messages = all_msgs[:limit]
    else:
        messages = await db.list_chat_messages(channel, limit=limit)
        if viewer_id:
            await db.mark_messages_read(viewer_id, channel)

    label = channel_label(channel)

    if not messages:
        await message.answer(
            f"📖 <b>{label} — Переписка</b>\n\n"
            "Сообщений пока нет.",
            reply_markup=gd_channel_menu(channel),
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

    await message.answer(text, reply_markup=gd_channel_menu(channel))


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

    # Forward to recipient with reply button (для всех каналов, включая группу)
    header = f"📩 <b>От ГД</b> ({label}):\n\n"
    reply_b = InlineKeyboardBuilder()
    reply_b.button(text="💬 Ответить ГД", callback_data=f"reply_to_gd:{channel}")
    reply_b.adjust(1)
    reply_markup = reply_b.as_markup()
    if text:
        await notifier.safe_send(target_id, header + text, reply_markup=reply_markup)
    if file_info:
        await notifier.safe_send_media(
            target_id,
            file_info["file_type"],
            file_info["file_id"],
            caption=message.caption,
        )
    if not is_group_channel(channel):
        await refresh_recipient_keyboard(notifier, db, config, int(target_id))

    await message.answer(
        f"✅ Сообщение отправлено → {label}",
        reply_markup=gd_channel_menu(channel),
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

    if is_group_channel(channel):
        # Для группового канала — ищем задачи по source в payload
        all_tasks = await db.list_tasks_by_source(
            source=f"chat_proxy:{channel}",
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            created_by=gd_user_id,
            limit=20,
        )
    elif target_id:
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

    await message.answer("\n".join(lines), reply_markup=gd_channel_menu(channel))


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
    role, isolated_role = resolve_menu_scope(u.id, user.role if user else None)
    is_admin = u.id in (config.admin_ids or set())
    unread = await db.count_unread_tasks(u.id)
    uc = await db.count_unread_by_channel(u.id)
    from ..enums import Role as _Role
    from ..utils import parse_roles as _parse_roles
    _parsed_cp = _parse_roles(role) if role else []
    gd_ur = await db.count_gd_inbox_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    gd_inv = await db.count_gd_invoice_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    gd_ie = await db.count_gd_invoice_end_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    _is_rp_cp = _Role.RP in _parsed_cp or _Role.MANAGER_NPN in _parsed_cp
    rp_t_cp = await db.count_rp_role_tasks(u.id) if _is_rp_cp else 0
    rp_m_cp = await db.count_rp_role_messages(u.id) if _is_rp_cp else 0
    await message.answer(
        "Главное меню.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=is_admin,
                unread=unread,
                unread_channels=uc,
                gd_inbox_unread=gd_ur,
                gd_invoice_unread=gd_inv,
                gd_invoice_end_unread=gd_ie,
                isolated_role=isolated_role,
                rp_tasks=rp_t_cp,
                rp_messages=rp_m_cp,
            ),
        ),
    )




# ---------------------------------------------------------------------------
# GD Task creation from chat-proxy
# ---------------------------------------------------------------------------

_GDTASK_INV_PREFIX = "gdtask_inv"


async def _show_task_invoice_picker_or_desc(
    source: CallbackQuery,
    state: FSMContext,
    db: Database,
    label: str,
) -> None:
    """Показать invoice picker перед описанием задачи, или пропустить."""
    invoices = await db.list_invoices_for_selection(limit=15)
    msg_target = source.message
    if invoices:
        await state.set_state(GdTaskCreateSG.invoice_pick)
        await msg_target.answer(  # type: ignore[union-attr]
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "По какому счёту задача?\n"
            "Для отмены: «❌ Отмена».",
            reply_markup=invoice_select_kb(invoices, prefix=_GDTASK_INV_PREFIX),
        )
    else:
        await state.update_data(linked_invoice_id=None)
        await state.set_state(GdTaskCreateSG.description)
        await msg_target.answer(  # type: ignore[union-attr]
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "Шаг 1/4: опишите задачу\n"
            "(«❌ Отмена» — отменить):",
        )


@router.callback_query(F.data.startswith(f"{_GDTASK_INV_PREFIX}:"))
async def gd_task_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """GD выбрал счёт для привязки к задаче."""
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    linked = None if val == "skip" else int(val)
    await state.update_data(linked_invoice_id=linked)
    await state.set_state(GdTaskCreateSG.description)

    data = await state.get_data()
    label = channel_label(data.get("task_channel", ""))

    inv_label = ""
    if linked:
        inv = await db.get_invoice(linked)
        if inv:
            inv_label = f"\n📋 Счёт: №{inv.get('invoice_number', '?')}"

    await cb.message.answer(  # type: ignore[union-attr]
        f"📝 <b>Новая задача → {label}</b>{inv_label}\n\n"
        "Шаг 1/4: опишите задачу\n"
        "(«❌ Отмена» — отменить):",
    )


@router.callback_query(F.data.startswith("gd_task_create:"))
async def gd_task_create_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """GD starts creating a task for channel target."""
    await cb.answer()
    channel = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.clear()
    await state.update_data(task_channel=channel, task_attachments=[])

    label = channel_label(channel)

    # Для montazh — сначала выбрать конкретного монтажника
    if channel == "montazh":
        installers = await db.find_users_by_role("installer")
        if not installers:
            await cb.message.answer("⚠️ Нет активных монтажников.")  # type: ignore[union-attr]
            return
        b = InlineKeyboardBuilder()
        for inst in installers:
            name = inst.full_name or inst.username or str(inst.telegram_id)
            b.button(text=name, callback_data=f"pick_installer:{inst.telegram_id}")
        b.adjust(1)
        await state.set_state(GdTaskCreateSG.pick_installer)
        await cb.message.answer(  # type: ignore[union-attr]
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "👷 Выберите монтажника:",
            reply_markup=b.as_markup(),
        )
        return

    # Для остальных каналов — показать invoice picker
    await _show_task_invoice_picker_or_desc(cb, state, db, label)


@router.callback_query(F.data.startswith("pick_installer:"), GdTaskCreateSG.pick_installer)
async def gd_task_pick_installer(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """GD picks a specific installer for montazh task."""
    await cb.answer()
    installer_id = int(cb.data.split(":", 1)[1])  # type: ignore[union-attr]
    await state.update_data(montazh_target_id=installer_id)

    data = await state.get_data()
    label = channel_label(data.get("task_channel", "montazh"))
    await _show_task_invoice_picker_or_desc(cb, state, db, label)


# --- Cancel task creation at any step ---
_CANCEL_TEXTS = {"❌ отмена", "отмена", "cancel", "/cancel", "❌отмена"}


@router.message(GdTaskCreateSG.pick_installer, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.invoice_pick, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.description, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.deadline, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.deadline_time, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.attachments, F.text.casefold().in_(_CANCEL_TEXTS))
async def gd_task_create_cancel(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Cancel task creation and return to chat submenu."""
    data = await state.get_data()
    channel = data.get("task_channel", "")
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await message.answer("❌ Создание задачи отменено.", reply_markup=gd_channel_menu(channel))


@router.message(GdTaskCreateSG.description)
async def gd_task_create_desc(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите задачу подробнее (минимум 3 символа):")
        return
    await state.update_data(task_description=text)
    await state.set_state(GdTaskCreateSG.deadline)
    await message.answer(
        "Шаг 2/4: укажите срок — например <b>07 марта</b> или <b>15.03.2026</b>\n"
        "Напишите «-» без срока, «❌ Отмена» для отмены:",
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
            await message.answer(
                "Не удалось распознать дату.\n"
                "Укажите в формате <b>07 марта</b> или <b>дд.мм.гггг</b>:"
            )
            return
        due = parsed

    await state.update_data(task_due=to_iso(due))
    await state.set_state(GdTaskCreateSG.deadline_time)
    await message.answer(
        "Укажите время дедлайна (например <b>14:00</b>)\n"
        "или «-» — конец рабочего дня (18:00):",
    )


@router.message(GdTaskCreateSG.deadline_time)
async def gd_task_create_time(message: Message, state: FSMContext, config: Config) -> None:
    import re as _re
    from ..utils import from_iso, tzinfo as _tzinfo

    text = (message.text or "").strip()

    if text == "-":
        hour, minute = 18, 0
    else:
        m = _re.fullmatch(r"(\d{1,2})[:\.](\d{2})", text)
        if not m:
            m = _re.fullmatch(r"(\d{1,2})", text)
            if m:
                hour, minute = int(m.group(1)), 0
            else:
                await message.answer(
                    "Не удалось распознать время.\n"
                    "Укажите в формате <b>14:00</b> или просто <b>14</b>:"
                )
                return
        else:
            hour, minute = int(m.group(1)), int(m.group(2))

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        await message.answer("Некорректное время. Укажите от 00:00 до 23:59:")
        return

    data = await state.get_data()
    due_iso = data.get("task_due", "")
    if due_iso:
        due_dt = from_iso(due_iso).astimezone(_tzinfo(config.timezone))
        due_dt = due_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        await state.update_data(task_due=to_iso(due_dt))

    await state.set_state(GdTaskCreateSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать задачу", callback_data="gd_task_finalize")
    b.button(text="⏭ Без вложений", callback_data="gd_task_finalize")
    b.button(text="❌ Отмена", callback_data="gd_task_cancel")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы (по желанию). Когда готовы — нажмите кнопку:",
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
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "gd_task_cancel")
async def gd_task_cancel_cb(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Cancel task creation via inline button."""
    await cb.answer("Отменено")
    data = await state.get_data()
    channel = data.get("task_channel", "")
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await cb.message.answer(  # type: ignore[union-attr]
        "❌ Создание задачи отменено.",
        reply_markup=gd_channel_menu(channel),
    )


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

    # Resolve target(s): composite channels → multiple recipients
    # Для montazh — использовать выбранного монтажника
    montazh_target = data.get("montazh_target_id")
    if channel == "montazh" and montazh_target:
        targets: list[tuple[str, int]] = [(channel, int(montazh_target))]
    else:
        sub_channels = COMPOSITE_CHANNELS.get(channel)
        if sub_channels:
            targets = []
            for sc in sub_channels:
                tid = await resolve_channel_target(sc, db, config)
                if tid:
                    targets.append((sc, int(tid)))
        else:
            tid = await resolve_channel_target(channel, db, config)
            targets = [(channel, int(tid))] if tid else []

    if not targets:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Адресат для {channel_label(channel)} не настроен.",
        )
        await state.clear()
        return

    label = channel_label(channel)
    initiator = await get_initiator_label(db, u.id)

    # Invoice label for notification
    linked_inv_id = data.get("linked_invoice_id")
    inv_label = ""
    if linked_inv_id:
        inv_row = await db.fetchone(
            "SELECT invoice_number, address FROM invoices WHERE id = ?",
            (linked_inv_id,),
        )
        if inv_row:
            inv_label = f"\n🧾 Счёт: {inv_row['invoice_number'] or '—'} / {inv_row['address'] or '—'}"

    for sc, target_id in targets:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.GD_TASK,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=target_id,
            due_at_iso=due_iso,
            payload={
                "comment": description,
                "source": f"chat_proxy:{channel}",
                "sender_id": u.id,
                "sender_username": u.username,
                "linked_invoice_id": data.get("linked_invoice_id"),
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
            f"📝 <b>Новая задача от ГД</b>\n"
            f"👤 От: {initiator}{inv_label}\n\n"
            f"📋 {description}"
        )
        await notifier.safe_send(target_id, msg, reply_markup=task_actions_kb(task))

        for a in attachments:
            await notifier.safe_send_media(target_id, a["file_type"], a["file_id"], caption=a.get("caption"))
        await refresh_recipient_keyboard(notifier, db, config, target_id)

        await integrations.sync_task(task, project_code="")

        # При назначении монтажника на счёт — привязать к счёту
        if channel == "montazh" and linked_inv_id:
            await db.assign_installer_to_invoice(int(linked_inv_id), target_id)

    await state.clear()

    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Задача создана и отправлена → {label}.",
        reply_markup=gd_channel_menu(channel),
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
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await answer_service(message, "✅ Ответ отправлен ГД.")
