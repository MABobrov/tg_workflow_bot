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
_ROLES_WITH_OWN_REFRESH = {"installer", "zamery", "rp"}


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

        # Don't refresh if user is in an active FSM state (would interfere with input)
        fsm: FSMContext | None = data.get("state")
        if fsm:
            try:
                cur_state = await fsm.get_state()
                if cur_state is not None:
                    return result
            except Exception:
                pass

        db = data.get("db")
        config = data.get("config")
        if not db or not config:
            return result

        try:
            from ..db import Database
            from ..enums import Role
            from ..keyboards import main_menu
            from ..services.menu_scope import resolve_menu_scope
            from ..utils import answer_service, parse_roles

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

            # Build fresh keyboard with badges
            parsed_roles = set(parse_roles(user.role))
            unread = await db.count_unread_tasks(u.id)
            uc = await db.count_unread_by_channel(u.id)
            is_admin = u.id in (config.admin_ids or set())

            # GD-specific counters
            gd_unread = await db.count_gd_inbox_tasks(u.id) if Role.GD in parsed_roles else None
            gd_inv = await db.count_gd_invoice_tasks(u.id) if Role.GD in parsed_roles else None
            gd_ie = await db.count_gd_invoice_end_tasks(u.id) if Role.GD in parsed_roles else None
            gd_sp = await db.count_gd_supplier_pay_tasks(u.id) if Role.GD in parsed_roles else None

            kb = main_menu(
                menu_role,
                is_admin=is_admin,
                unread=unread,
                unread_channels=uc,
                isolated_role=isolated,
                gd_inbox_unread=gd_unread,
                gd_invoice_unread=gd_inv,
                gd_invoice_end_unread=gd_ie,
                gd_supplier_pay_unread=gd_sp,
            )

            await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
        except Exception:
            log.debug("keep_menu refresh failed for user %s", u.id, exc_info=True)

        return result
