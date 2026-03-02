from __future__ import annotations

from typing import Iterable, Optional

from aiogram.types import CallbackQuery, Message

from ..config import Config
from ..db import Database
from ..utils import has_any_role, parse_roles


async def get_role(db: Database, user_id: int) -> str | None:
    u = await db.get_user_optional(user_id)
    return u.role if u else None


async def get_roles(db: Database, user_id: int) -> set[str]:
    u = await db.get_user_optional(user_id)
    return set(parse_roles(u.role if u else None))


async def is_active_user(db: Database, user_id: int) -> bool:
    u = await db.get_user_optional(user_id)
    if not u:
        return True
    return bool(u.is_active)


async def require_role_message(message: Message, db: Database, roles: Iterable[str]) -> bool:
    if not message.from_user:
        return False
    if not message.chat or message.chat.type != "private":
        await message.answer("Этот сценарий доступен только в личных сообщениях с ботом.")
        return False
    if not await is_active_user(db, message.from_user.id):
        await message.answer("⛔️ Ваш доступ к боту заблокирован. Обратитесь к администратору.")
        return False
    role_raw = await get_role(db, message.from_user.id)
    if not has_any_role(role_raw, set(roles)):
        await message.answer("⛔️ Нет доступа. Попросите администратора назначить роль или используйте правильный аккаунт.")
        return False
    return True


async def require_role_callback(cb: CallbackQuery, db: Database, roles: Iterable[str]) -> bool:
    if not cb.from_user:
        return False
    if not cb.message or not cb.message.chat or cb.message.chat.type != "private":
        await cb.answer("Доступно только в личном чате с ботом", show_alert=True)
        return False
    if not await is_active_user(db, cb.from_user.id):
        await cb.answer("Ваш доступ к боту заблокирован", show_alert=True)
        return False
    role_raw = await get_role(db, cb.from_user.id)
    if not has_any_role(role_raw, set(roles)):
        await cb.answer("Нет доступа", show_alert=True)
        return False
    return True


def is_admin(user_id: int, config: Config) -> bool:
    return user_id in (config.admin_ids or set())
