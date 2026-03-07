from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from ..middlewares.usage_audit import MENU_BUTTONS
from ..states import ReplyToGDSG

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type.in_({"group", "supergroup"}))
router.callback_query.filter(F.message.chat.type.in_({"group", "supergroup"}))

_ALLOWED_GROUP_CALLBACK_PREFIXES = ("lead:", "leadassign:", "reply_to_gd:", "task:")


@router.message(F.text)
async def cleanup_group_reply_keyboard(message: Message) -> None:
    text = (message.text or "").strip()
    # In group chats remove legacy keyboards on command/button interactions.
    if text.startswith("/") or text in MENU_BUTTONS:
        await message.answer(
            "Кнопки в группах отключены. Используйте бота в личном чате.",
            reply_markup=ReplyKeyboardRemove(),
        )


# ---------------------------------------------------------------------------
# "Ответить ГД" из группового чата (montazh)
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("reply_to_gd:"))
async def group_reply_to_gd(cb: CallbackQuery, state: FSMContext) -> None:
    """Redirect group 'Ответить ГД' to private chat with bot."""
    user = cb.from_user
    if not user:
        await cb.answer("Ошибка.", show_alert=True)
        return

    channel = (cb.data or "").split(":", 1)[1]

    try:
        # Создаём FSMContext для личного чата пользователя
        private_key = StorageKey(bot_id=cb.bot.id, chat_id=user.id, user_id=user.id)
        private_state = FSMContext(storage=state.storage, key=private_key)
        await private_state.set_state(ReplyToGDSG.text)
        await private_state.update_data(reply_channel=channel)

        # Отправляем DM пользователю с промптом для ответа
        await cb.bot.send_message(
            user.id,
            "💬 <b>Ответ ГД</b> (Монтажная гр.)\n\n"
            "Введите текст ответа в этом чате.\n"
            "Можно прикрепить файл.\n"
            "Для отмены: /cancel",
        )
        await cb.answer("Перейдите в личный чат с ботом для ответа", show_alert=True)
    except Exception:
        log.exception("Failed to send DM for group reply_to_gd, user=%s", user.id)
        await cb.answer(
            "Не удалось отправить сообщение. Убедитесь, что вы начали чат с ботом (/start).",
            show_alert=True,
        )


# ---------------------------------------------------------------------------
# Catch-all: block all other group callbacks
# ---------------------------------------------------------------------------

@router.callback_query()
async def cleanup_group_inline_callbacks(cb: CallbackQuery) -> None:
    data = cb.data or ""
    if data.startswith(_ALLOWED_GROUP_CALLBACK_PREFIXES):
        return
    await cb.answer("Кнопки работают только в личном чате с ботом.", show_alert=True)
    try:
        if cb.message:
            await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
