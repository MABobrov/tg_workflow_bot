from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from ..middlewares.usage_audit import MENU_BUTTONS

router = Router()
router.message.filter(F.chat.type.in_({"group", "supergroup"}))
router.callback_query.filter(F.message.chat.type.in_({"group", "supergroup"}))


@router.message(F.text)
async def cleanup_group_reply_keyboard(message: Message) -> None:
    text = (message.text or "").strip()
    # In group chats remove legacy keyboards on command/button interactions.
    if text.startswith("/") or text in MENU_BUTTONS:
        await message.answer(
            "Кнопки в группах отключены. Используйте бота в личном чате.",
            reply_markup=ReplyKeyboardRemove(),
        )


@router.callback_query()
async def cleanup_group_inline_callbacks(cb: CallbackQuery) -> None:
    await cb.answer("Кнопки работают только в личном чате с ботом.", show_alert=True)
    try:
        if cb.message:
            await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

