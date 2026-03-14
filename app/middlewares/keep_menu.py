"""Middleware that ensures the reply keyboard (main menu) never disappears.

After every message from a private-chat user, if the handler didn't
explicitly send a reply_markup, we send an invisible "refresh" message
with the current main_menu keyboard. This prevents the menu from
vanishing during FSM steps and info messages.

This works at the Dispatcher level (dp.message.outer_middleware) and
covers ALL routers/handlers.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

log = logging.getLogger(__name__)

# Roles that have their OWN auto-refresh middleware on their routers
# (to avoid double-refreshing)
_ROLES_WITH_OWN_REFRESH = {
    "gd", "installer", "zamery", "rp",
    "manager", "manager_kv", "manager_kia", "manager_npn",
}


class KeepMenuMiddleware(BaseMiddleware):
    """Outer middleware for dp.message — refreshes reply keyboard for all roles."""

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        result = await handler(event, data)

        # Only for private chats
        chat = event.chat
        if not chat or chat.type != "private":
            return result

        u = event.from_user
        if not u:
            return result

        # NOTE: НЕ пропускаем FSM-состояния!
        # Клавиатура должна восстанавливаться ВСЕГДА,
        # включая многошаговые формы, чтобы меню бота не исчезало.

        db = data.get("db")
        config = data.get("config")
        if not db or not config:
            return result

        try:
            from ..db import Database
            from ..services.menu_context import build_main_menu_for_user
            from ..services.menu_scope import resolve_menu_scope
            from ..utils import answer_service

            if not isinstance(db, Database):
                return result

            user = await db.get_user_optional(u.id)
            if not user or not user.role:
                return result

            # Determine current menu role
            menu_role, isolated = resolve_menu_scope(u.id, user.role)
            if not menu_role:
                return result

            # Skip roles that have their own auto-refresh
            role_str = str(menu_role).lower()
            if role_str in _ROLES_WITH_OWN_REFRESH:
                return result

            kb = await build_main_menu_for_user(
                db,
                config,
                u.id,
                menu_role,
                isolated_role=isolated,
            )

            await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
        except Exception:
            log.debug("keep_menu refresh failed for user %s", u.id, exc_info=True)

        return result
