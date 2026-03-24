from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..enums import Role
from ..keyboards import main_menu
from ..utils import parse_roles

if TYPE_CHECKING:
    from aiogram.types import ReplyKeyboardMarkup

    from ..config import Config
    from ..db import Database


_GD_LIKE_ROLES = {Role.GD, Role.TD}


async def build_menu_context(
    db: Database,
    user_id: int | None,
    role: str | None,
) -> dict[str, Any]:
    if not user_id:
        return {
            "unread": 0,
            "unread_channels": {},
            "gd_inbox_unread": None,
            "gd_invoice_unread": None,
            "gd_invoice_end_unread": None,
            "gd_supplier_pay_unread": None,
            "rp_tasks": 0,
            "rp_messages": 0,
            "npn_tasks": 0,
            "npn_messages": 0,
        }

    roles = set(parse_roles(role))
    is_rp = Role.RP in roles or Role.MANAGER_NPN in roles
    has_gd_access = bool(roles & _GD_LIKE_ROLES)
    unread = await db.count_unread_tasks(user_id)
    context: dict[str, Any] = {
        "unread": unread,
        "unread_channels": await db.count_unread_by_channel(user_id),
        "gd_inbox_unread": await db.count_gd_inbox_tasks(user_id) if has_gd_access else None,
        "gd_invoice_unread": await db.count_gd_invoice_tasks(user_id) if has_gd_access else None,
        "gd_invoice_end_unread": await db.count_gd_invoice_end_tasks(user_id) if has_gd_access else None,
        "gd_supplier_pay_unread": await db.count_gd_supplier_pay_tasks(user_id) if has_gd_access else None,
        "rp_tasks": await db.count_rp_role_tasks(user_id) if is_rp else 0,
        "rp_messages": await db.count_rp_role_messages(user_id) if is_rp else 0,
        "npn_tasks": unread if Role.MANAGER_NPN in roles else 0,
        "npn_messages": 0,
    }
    if Role.RP in roles:
        context["rp_check_kp"] = await db.count_rp_check_kp_tasks(user_id)
        context["rp_invoices_pay"] = await db.count_rp_invoice_pay_tasks(user_id)
        context["rp_ch_mgr_kv"] = await db.count_rp_channel_unread(user_id, "rp_to_manager_kv")
        context["rp_ch_mgr_kia"] = await db.count_rp_channel_unread(user_id, "rp_to_manager_kia")
        context["rp_ch_montazh"] = await db.count_rp_channel_unread(user_id, "montazh")
    return context


async def build_main_menu_for_user(
    db: Database,
    config: Config,
    user_id: int,
    role: str | None,
    *,
    isolated_role: bool = False,
) -> ReplyKeyboardMarkup:
    return main_menu(
        role,
        is_admin=user_id in (config.admin_ids or set()),
        isolated_role=isolated_role,
        **(await build_menu_context(db, user_id, role)),
    )
