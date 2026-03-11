from __future__ import annotations

import logging
from typing import Any

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

log = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self,
        bot: Bot,
        work_chat_id: int | None = None,
        *,
        workchat_events_enabled: bool = False,
    ):
        self.bot = bot
        self.work_chat_id = work_chat_id
        self.workchat_events_enabled = workchat_events_enabled

    async def safe_send(
        self,
        chat_id: int,
        text: str,
        reply_markup: Any | None = None,
        *,
        return_error: bool = False,
    ) -> bool | tuple[bool, str | None]:
        def _ret(ok: bool, err: str | None = None) -> bool | tuple[bool, str | None]:
            if return_error:
                return ok, err
            return ok

        try:
            await self.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, disable_web_page_preview=True)
            return _ret(True, None)
        except TelegramForbiddenError:
            # user didn't start bot or blocked it
            log.warning("Cannot send to chat_id=%s: forbidden", chat_id)
            return _ret(False, "forbidden")
        except TelegramRetryAfter as e:
            log.warning("Telegram rate limit: retry_after=%s", e.retry_after)
            return _ret(False, f"retry_after:{e.retry_after}")
        except TelegramBadRequest as e:
            err = str(e)
            # If user text accidentally breaks HTML entities, retry as plain text.
            if "can't parse entities" in err.lower():
                try:
                    await self.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=reply_markup,
                        disable_web_page_preview=True,
                        parse_mode=None,
                    )
                    log.warning("Retried message as plain text due to entity parsing error, chat_id=%s", chat_id)
                    return _ret(True, None)
                except Exception:
                    log.exception("Failed to resend plain-text message after parse error, chat_id=%s", chat_id)
                    return _ret(False, err)
            log.warning("Telegram bad request chat_id=%s: %s", chat_id, err)
            return _ret(False, err)
        except Exception:
            log.exception("Unexpected error sending message")
            return _ret(False, "unexpected_error")

    async def safe_send_document(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send document to chat_id=%s: forbidden", chat_id)
            return False
        except TelegramBadRequest as e:
            err = str(e)
            if caption and "can't parse entities" in err.lower():
                try:
                    await self.bot.send_document(chat_id=chat_id, document=file_id, caption=caption, parse_mode=None)
                    log.warning("Retried document as plain caption due to entity parsing error, chat_id=%s", chat_id)
                    return True
                except Exception:
                    log.exception("Failed to resend document after parse error, chat_id=%s", chat_id)
                    return False
            log.warning("Telegram bad request document chat_id=%s: %s", chat_id, err)
            return False
        except Exception:
            log.exception("Unexpected error sending document")
            return False

    async def safe_send_photo(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send photo to chat_id=%s: forbidden", chat_id)
            return False
        except TelegramBadRequest as e:
            err = str(e)
            if caption and "can't parse entities" in err.lower():
                try:
                    await self.bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption, parse_mode=None)
                    log.warning("Retried photo as plain caption due to entity parsing error, chat_id=%s", chat_id)
                    return True
                except Exception:
                    log.exception("Failed to resend photo after parse error, chat_id=%s", chat_id)
                    return False
            log.warning("Telegram bad request photo chat_id=%s: %s", chat_id, err)
            return False
        except Exception:
            log.exception("Unexpected error sending photo")
            return False

    async def safe_send_video(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send video to chat_id=%s: forbidden", chat_id)
            return False
        except Exception:
            log.exception("Error sending video to chat_id=%s", chat_id)
            return False

    async def safe_send_media(self, chat_id: int, file_type: str, file_id: str, caption: str | None = None) -> bool:
        if file_type == "photo":
            return await self.safe_send_photo(chat_id, file_id, caption=caption)
        if file_type == "video":
            return await self.safe_send_video(chat_id, file_id, caption=caption)
        return await self.safe_send_document(chat_id, file_id, caption=caption)

    async def notify_workchat(self, text: str, reply_markup: Any | None = None) -> None:
        if not self.workchat_events_enabled:
            return
        if not self.work_chat_id:
            return
        chat_id = int(self.work_chat_id)
        result = await self.safe_send(chat_id, text, reply_markup=reply_markup, return_error=True)
        ok, err = result if isinstance(result, tuple) else (bool(result), None)
        if not ok and err and "chat not found" in err.lower():
            self.work_chat_id = None
            log.error(
                "Work chat disabled: chat_id=%s not found. "
                "Set a valid chat via /setworkchat in bot private chat.",
                chat_id,
            )

    async def notify_workchat_media(self, file_type: str, file_id: str, caption: str | None = None) -> bool:
        if not self.workchat_events_enabled:
            return False
        if not self.work_chat_id:
            return False
        return await self.safe_send_media(self.work_chat_id, file_type, file_id, caption=caption)
