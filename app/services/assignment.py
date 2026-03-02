from __future__ import annotations

from typing import Optional

from ..config import Config
from ..db import Database
from ..enums import Role


async def get_work_chat_id(db: Database, config: Config) -> int | None:
    v = await db.get_setting("work_chat_id")
    if v:
        try:
            return int(v)
        except ValueError:
            return config.work_chat_id
    return config.work_chat_id


async def resolve_default_assignee(db: Database, config: Config, role: str) -> int | None:
    # 1) settings override
    key_map = {
        Role.RP: "default_rp_id",
        Role.TD: "default_td_id",
        Role.ACCOUNTING: "default_accounting_id",
        Role.GD: "default_gd_id",
        Role.DRIVER: "default_driver_id",
        Role.TINTER: "default_tinter_id",
    }
    key = key_map.get(role)
    if key:
        v = await db.get_setting(key)
        if v:
            try:
                return int(v)
            except ValueError:
                user = await db.find_user_by_username(v)
                if user:
                    return user.telegram_id

    # 2) env defaults
    if role == Role.RP and config.default_rp_id:
        return config.default_rp_id
    if role == Role.TD and config.default_td_id:
        return config.default_td_id
    if role == Role.ACCOUNTING and config.default_accounting_id:
        return config.default_accounting_id
    if role == Role.GD and config.default_gd_id:
        return config.default_gd_id
    if role == Role.DRIVER and config.default_driver_id:
        return config.default_driver_id
    if role == Role.TINTER and config.default_tinter_id:
        return config.default_tinter_id

    if role == Role.RP and config.default_rp_username:
        user = await db.find_user_by_username(config.default_rp_username)
        if user:
            return user.telegram_id
    if role == Role.TD and config.default_td_username:
        user = await db.find_user_by_username(config.default_td_username)
        if user:
            return user.telegram_id
    if role == Role.ACCOUNTING and config.default_accounting_username:
        user = await db.find_user_by_username(config.default_accounting_username)
        if user:
            return user.telegram_id
    if role == Role.GD and config.default_gd_username:
        user = await db.find_user_by_username(config.default_gd_username)
        if user:
            return user.telegram_id
    if role == Role.DRIVER and config.default_driver_username:
        user = await db.find_user_by_username(config.default_driver_username)
        if user:
            return user.telegram_id
    if role == Role.TINTER and config.default_tinter_username:
        user = await db.find_user_by_username(config.default_tinter_username)
        if user:
            return user.telegram_id

    # 3) first active user with role
    users = await db.find_users_by_role(role, limit=1)
    return users[0].telegram_id if users else None
