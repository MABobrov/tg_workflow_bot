"""Manager ↔ Installer chat bound to a specific invoice.

Callbacks:
  inv_chat:history:{invoice_id}  — show chat history
  inv_chat:write:{invoice_id}    — enter writing mode
  inv_chat:back:{invoice_id}     — return to card

FSM state: InvoiceChatSG.writing
"""
from __future__ import annotations

import logging
from typing import Any

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role
from ..services.notifier import Notifier
from ..states import InvoiceChatSG
from ..utils import get_initiator_label, refresh_recipient_keyboard, utcnow, to_iso
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

CHANNEL = "mgr_installer"


def invoice_chat_button(invoice_id: int, label: str = "💬 Чат") -> tuple[str, str]:
    """Return (text, callback_data) for the chat button."""
    return label, f"inv_chat:menu:{invoice_id}"


def _chat_menu_kb(invoice_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="📖 Переписка", callback_data=f"inv_chat:history:{invoice_id}")
    b.button(text="✏️ Написать", callback_data=f"inv_chat:write:{invoice_id}")
    b.button(text="⬅️ Назад", callback_data=f"inv_chat:close:{invoice_id}")
    b.adjust(2, 1)
    return b.as_markup()


@router.callback_query(F.data.regexp(r"^inv_chat:menu:\d+$"))
async def inv_chat_menu(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Show chat menu for invoice."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    # Count messages
    msgs = await db.list_chat_messages_by_invoice(invoice_id)
    count = len(msgs)
    text = (
        f"💬 <b>Чат по счёту №{inv['invoice_number']}</b>\n"
        f"📍 {inv.get('object_address', '—')}\n\n"
        f"Сообщений: {count}"
    )
    await cb.message.answer(text, reply_markup=_chat_menu_kb(invoice_id))  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^inv_chat:history:\d+$"))
async def inv_chat_history(cb: CallbackQuery, db: Database) -> None:
    """Show chat message history for invoice."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    msgs = await db.list_chat_messages_by_invoice(invoice_id)
    u = cb.from_user
    if u:
        await db.mark_messages_read(u.id, f"{CHANNEL}:{invoice_id}")

    if not msgs:
        b = InlineKeyboardBuilder()
        b.button(text="✏️ Написать", callback_data=f"inv_chat:write:{invoice_id}")
        b.button(text="⬅️ Назад", callback_data=f"inv_chat:menu:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"💬 <b>Чат по счёту №{inv['invoice_number']}</b>\n\nСообщений пока нет.",
            reply_markup=b.as_markup(),
        )
        return

    lines = [f"💬 <b>Чат по счёту №{inv['invoice_number']}</b>\n"]
    for m in msgs:
        direction = "➡️" if m["sender_id"] == (u.id if u else 0) else "⬅️"
        ts = m["created_at"][:16].replace("T", " ") if m.get("created_at") else ""
        text_preview = (m.get("text") or "📎 вложение")[:100]
        lines.append(f"{direction} <i>{ts}</i>  {text_preview}")

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n\n... (обрезано)"

    b = InlineKeyboardBuilder()
    b.button(text="✏️ Написать", callback_data=f"inv_chat:write:{invoice_id}")
    b.button(text="⬅️ Назад", callback_data=f"inv_chat:menu:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^inv_chat:write:\d+$"))
async def inv_chat_start_write(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Enter writing mode for invoice chat."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.set_state(InvoiceChatSG.writing)
    await state.update_data(inv_chat_invoice_id=invoice_id)

    await cb.message.answer(  # type: ignore[union-attr]
        f"✏️ <b>Написать по счёту №{inv['invoice_number']}</b>\n\n"
        "Введите текст сообщения.\n"
        "Для отмены: /cancel",
    )


@router.message(InvoiceChatSG.writing)
async def inv_chat_send(
    message: Message, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Send message in invoice-bound chat."""
    text = message.text or ""
    if text.strip() in ("/cancel", "отмена"):
        await state.clear()
        await message.answer("❌ Отменено.")
        return

    data = await state.get_data()
    invoice_id = data.get("inv_chat_invoice_id")
    if not invoice_id:
        await state.clear()
        await message.answer("❌ Ошибка: счёт не найден.")
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await state.clear()
        await message.answer("❌ Счёт не найден.")
        return

    u = message.from_user
    if not u:
        return

    # Determine recipient
    sender_id = u.id
    installer_id = inv.get("assigned_to")
    manager_id = inv.get("created_by")

    # If sender is installer → recipient is manager; and vice versa
    if sender_id == installer_id:
        receiver_id = int(manager_id) if manager_id else None
    else:
        receiver_id = int(installer_id) if installer_id else None

    channel = f"{CHANNEL}:{invoice_id}"
    await db.save_chat_message(
        channel=channel,
        sender_id=sender_id,
        direction="outgoing",
        text=text,
        receiver_id=receiver_id,
        invoice_id=invoice_id,
    )

    await state.clear()
    await message.answer("✅ Сообщение отправлено.")

    # Notify recipient
    if receiver_id:
        initiator = await get_initiator_label(db, sender_id)
        notify_text = (
            f"💬 <b>Новое сообщение по счёту №{inv['invoice_number']}</b>\n"
            f"👤 От: {initiator}\n\n"
            f"{text[:300]}"
        )
        b = InlineKeyboardBuilder()
        b.button(text="💬 Открыть чат", callback_data=f"inv_chat:menu:{invoice_id}")
        b.adjust(1)
        await notifier.safe_send(receiver_id, notify_text, reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, receiver_id)


@router.callback_query(F.data.regexp(r"^inv_chat:close:\d+$"))
async def inv_chat_close(cb: CallbackQuery, state: FSMContext) -> None:
    """Close chat menu, return to previous view."""
    await cb.answer()
    await state.clear()
    await cb.message.answer("📋 Возврат в меню.")  # type: ignore[union-attr]
