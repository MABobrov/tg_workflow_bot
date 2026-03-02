from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message, Update

log = logging.getLogger(__name__)


def _clip(value: str | None, max_len: int = 120) -> str:
    if not value:
        return ""
    flat = " ".join(str(value).split())
    if len(flat) <= max_len:
        return flat
    return f"{flat[:max_len]}..."


def _extract_context(update: Update) -> Dict[str, Any]:
    try:
        event = update.event
        event_type = update.event_type
    except Exception:
        event = None
        event_type = "unknown"

    user_id = None
    username = None
    chat_id = None
    payload = ""

    if isinstance(event, Message):
        user_id = event.from_user.id if event.from_user else None
        username = event.from_user.username if event.from_user else None
        chat_id = event.chat.id if event.chat else None
        payload = _clip(event.text or event.caption)
    elif isinstance(event, CallbackQuery):
        user_id = event.from_user.id if event.from_user else None
        username = event.from_user.username if event.from_user else None
        chat_id = event.message.chat.id if event.message and event.message.chat else None
        payload = _clip(event.data)
    elif isinstance(event, ChatMemberUpdated):
        user_id = event.from_user.id if event.from_user else None
        username = event.from_user.username if event.from_user else None
        chat_id = event.chat.id if event.chat else None
        old_status = getattr(event.old_chat_member, "status", "")
        new_status = getattr(event.new_chat_member, "status", "")
        payload = _clip(f"{old_status}->{new_status}")

    return {
        "update_id": update.update_id,
        "event_type": event_type,
        "user_id": user_id,
        "username": username,
        "chat_id": chat_id,
        "payload": payload,
    }


class UpdateLoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        ctx = _extract_context(event)
        started = perf_counter()
        log.info(
            "Update start id=%s type=%s user=%s @%s chat=%s payload=%r",
            ctx["update_id"],
            ctx["event_type"],
            ctx["user_id"],
            ctx["username"] or "-",
            ctx["chat_id"],
            ctx["payload"],
        )

        try:
            result = await handler(event, data)
        except Exception:
            elapsed_ms = int((perf_counter() - started) * 1000)
            log.exception(
                "Update failed id=%s type=%s user=%s @%s chat=%s duration_ms=%s payload=%r",
                ctx["update_id"],
                ctx["event_type"],
                ctx["user_id"],
                ctx["username"] or "-",
                ctx["chat_id"],
                elapsed_ms,
                ctx["payload"],
            )
            raise

        elapsed_ms = int((perf_counter() - started) * 1000)
        status = "unhandled" if result is UNHANDLED else "handled"
        level = log.warning if status == "unhandled" else log.info
        level(
            "Update done id=%s type=%s status=%s user=%s @%s chat=%s duration_ms=%s",
            ctx["update_id"],
            ctx["event_type"],
            status,
            ctx["user_id"],
            ctx["username"] or "-",
            ctx["chat_id"],
            elapsed_ms,
        )
        return result
