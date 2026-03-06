from __future__ import annotations

from ..config import Config
from ..db import Database
from ..enums import Role


def _role_resolution_order(role: str) -> tuple[str, ...]:
    """Return setting/config lookup order, preserving TD -> GD compatibility."""
    if role in {Role.GD, Role.TD}:
        return (Role.GD, Role.TD)
    return (role,)


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
        Role.MANAGER_KV: "default_manager_kv_id",
        Role.MANAGER_KIA: "default_manager_kia_id",
        Role.MANAGER_NPN: "default_manager_npn_id",
        Role.ZAMERY: "default_zamery_id",
    }
    for candidate_role in _role_resolution_order(role):
        key = key_map.get(candidate_role)
        if not key:
            continue
        v = await db.get_setting(key)
        if not v:
            continue
        try:
            return int(v)
        except ValueError:
            user = await db.find_user_by_username(v)
            if user:
                return user.telegram_id

    # 2) env defaults
    for candidate_role in _role_resolution_order(role):
        default_id = config.get_role_id(candidate_role)
        if default_id:
            return default_id

    for candidate_role in _role_resolution_order(role):
        default_username = config.get_role_username(candidate_role)
        if not default_username:
            continue
        user = await db.find_user_by_username(default_username)
        if user:
            return user.telegram_id

    # 3) first active user with role
    for candidate_role in _role_resolution_order(role):
        users = await db.find_users_by_role(candidate_role, limit=1)
        if users:
            return users[0].telegram_id
    return None
