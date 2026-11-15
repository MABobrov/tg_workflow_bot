"""Middleware ленивого обновления карточки роли из таблицы знаний.

Регламент владельца 16.11.2026: когда роль заходит в чат с ботом, бот
перечитывает карточку ИМЕННО этой роли (если она не обновлялась с
последнего 9:00 МСК). Остальные роли не трогаются, пока не зайдут сами.

Обновление — fire-and-forget: сообщение пользователя не ждёт Гугл,
любая ошибка глотается (кэш в SQLite остаётся прежним).
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message

from ..services.knowledge_sync import KnowledgeSync

log = logging.getLogger(__name__)


class KnowledgeRoleMiddleware(BaseMiddleware):
    """dp.message outer middleware — фоновое обновление карточки роли."""

    def __init__(self, sync: KnowledgeSync | None):
        self.sync = sync

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if self.sync is not None:
            chat = event.chat
            u = event.from_user
            if chat and chat.type == "private" and u:
                try:
                    await self.sync.maybe_refresh_for_user(u.id)
                except Exception:
                    log.exception("KnowledgeRoleMiddleware: refresh check failed")
        return await handler(event, data)
