from __future__ import annotations

import logging
from typing import Any, Iterable

from ..config import Config
from ..db import Database
from ..enums import MANAGER_ROLES, Role, TaskStatus, TaskType
from ..utils import parse_roles, try_json_loads

log = logging.getLogger(__name__)

_CHAT_PROXY_ROLE_BY_CHANNEL: dict[str, str] = {
    "rp": Role.RP,
    "zamery": Role.ZAMERY,
    "accounting": Role.ACCOUNTING,
    "manager_kv": Role.MANAGER_KV,
    "manager_kia": Role.MANAGER_KIA,
    "manager_npn": Role.MANAGER_NPN,
}

_FIXED_ASSIGNEE_ROLE_BY_TASK_TYPE: dict[str, str] = {
    TaskType.CHECK_KP: Role.RP,
    TaskType.CLOSING_DOCS: Role.ACCOUNTING,
    TaskType.DELIVERY_DONE: Role.RP,
    TaskType.DELIVERY_REQUEST: Role.GD,
    TaskType.INSTALLATION_DONE: Role.INSTALLER,
    TaskType.INVOICE_END_REQUEST: Role.GD,
    TaskType.INVOICE_PAYMENT: Role.GD,
    TaskType.NOT_URGENT_GD: Role.GD,
    TaskType.ORDER_GLASS: Role.GD,
    TaskType.ORDER_MATERIALS: Role.GD,
    TaskType.ORDER_PROFILE: Role.GD,
    TaskType.PROJECT_END: Role.RP,
    TaskType.SUPPLIER_INVOICE: Role.GD,
    TaskType.SUPPLIER_PAYMENT: Role.RP,
    TaskType.TINTING_DONE: Role.RP,
    TaskType.TINTING_REQUEST: Role.TINTER,
    TaskType.URGENT_GD: Role.GD,
    TaskType.ZAMERY_REQUEST: Role.ZAMERY,
    TaskType.ZP_INSTALLER: Role.GD,
    TaskType.ZP_MANAGER: Role.GD,
    TaskType.ZP_ZAMERY_BATCH: Role.GD,
}


def _role_resolution_order(role: str) -> tuple[str, ...]:
    """Return setting/config lookup order, preserving TD -> GD compatibility."""
    if role in {Role.GD, Role.TD}:
        return (Role.GD, Role.TD)
    return (role,)


def _normalized_role(value: str | None) -> str | None:
    parsed = parse_roles(value)
    return parsed[0] if parsed else None


def _pick_role_from_roles(role_value: str | None) -> str | None:
    roles = parse_roles(role_value)
    if len(roles) == 1:
        return roles[0]
    manager_roles = [role for role in roles if role in (MANAGER_ROLES | {Role.MANAGER})]
    if len(manager_roles) == 1:
        return manager_roles[0]
    return None


def _user_has_role(user_role_value: str | None, role: str) -> bool:
    user_roles = set(parse_roles(user_role_value))
    if not user_roles:
        return False
    return any(candidate in user_roles for candidate in _role_resolution_order(role))


async def _candidate_user_by_id(
    db: Database,
    candidate_id: int | None,
    *,
    required_role: str,
    exclude_user_ids: set[int],
) -> int | None:
    if candidate_id is None or candidate_id in exclude_user_ids:
        return None
    user = await db.get_user_optional(int(candidate_id))
    if not user or not user.is_active:
        return None
    if not _user_has_role(user.role, required_role):
        return None
    return int(candidate_id)


def infer_task_assignee_role(
    task: dict[str, Any],
    *,
    fallback_role_value: str | None = None,
) -> str | None:
    payload = try_json_loads(task.get("payload_json"))

    explicit_role = _normalized_role(str(payload.get("assigned_role") or ""))
    if explicit_role:
        return explicit_role

    task_type = str(task.get("type") or "")
    source = str(payload.get("source") or "")

    if task_type == TaskType.GD_TASK and source.startswith("chat_proxy:"):
        channel = source.split(":", 1)[1].strip().lower()
        target_role = _CHAT_PROXY_ROLE_BY_CHANNEL.get(channel)
        if target_role:
            return target_role

    if task_type in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
        if source.startswith("sheets_"):
            return _pick_role_from_roles(fallback_role_value)
        return Role.RP

    if task_type == TaskType.EDO_REQUEST:
        if source == "accounting_request":
            return _pick_role_from_roles(fallback_role_value)
        return Role.ACCOUNTING

    if task_type in {TaskType.MANAGER_INFO_REQUEST, TaskType.INSTALLER_INVOICE_OK, TaskType.ASSIGN_LEAD, TaskType.LEAD_TO_PROJECT}:
        return _pick_role_from_roles(fallback_role_value)

    fixed_role = _FIXED_ASSIGNEE_ROLE_BY_TASK_TYPE.get(task_type)
    if fixed_role:
        return fixed_role

    manager_role = _normalized_role(str(payload.get("manager_role") or ""))
    if manager_role:
        return manager_role

    return _pick_role_from_roles(fallback_role_value)


async def get_work_chat_id(db: Database, config: Config) -> int | None:
    v = await db.get_setting("work_chat_id")
    if v:
        try:
            return int(v)
        except ValueError:
            return config.work_chat_id
    return config.work_chat_id


async def resolve_default_assignee(
    db: Database,
    config: Config,
    role: str,
    *,
    exclude_user_ids: Iterable[int] | None = None,
) -> int | None:
    excluded = {int(user_id) for user_id in (exclude_user_ids or [])}

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
            candidate_id = int(v)
        except ValueError:
            user = await db.find_user_by_username(v)
            candidate_id = user.telegram_id if user else None
        candidate = await _candidate_user_by_id(
            db,
            candidate_id,
            required_role=candidate_role,
            exclude_user_ids=excluded,
        )
        if candidate is not None:
            return candidate

    # 2) env defaults
    for candidate_role in _role_resolution_order(role):
        default_id = config.get_role_id(candidate_role)
        candidate = await _candidate_user_by_id(
            db,
            default_id,
            required_role=candidate_role,
            exclude_user_ids=excluded,
        )
        if candidate is not None:
            return candidate

    for candidate_role in _role_resolution_order(role):
        default_username = config.get_role_username(candidate_role)
        if not default_username:
            continue
        user = await db.find_user_by_username(default_username)
        candidate = await _candidate_user_by_id(
            db,
            user.telegram_id if user else None,
            required_role=candidate_role,
            exclude_user_ids=excluded,
        )
        if candidate is not None:
            return candidate

    # 3) first active user with role
    for candidate_role in _role_resolution_order(role):
        users = await db.find_users_by_role(candidate_role, limit=50)
        eligible = [user for user in users if user.telegram_id not in excluded]
        if len(eligible) == 1:
            return eligible[0].telegram_id
        if len(eligible) > 1:
            log.warning(
                "Multiple active users found for role %s; configure explicit default assignee",
                candidate_role,
            )
    return None


async def apply_user_roles(
    db: Database,
    config: Config,
    telegram_id: int,
    new_roles: Iterable[str],
) -> list[int]:
    user = await db.get_user_optional(telegram_id)
    if not user:
        return []

    normalized_roles = set(parse_roles(",".join(str(role) for role in new_roles)))
    current_roles = set(parse_roles(user.role))
    removed_roles = current_roles - normalized_roles
    if not removed_roles:
        await db.set_user_roles(telegram_id, normalized_roles)
        return []

    active_tasks = await db.list_tasks_for_user(
        telegram_id,
        statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
        limit=1000,
    )
    reassigned_task_ids: list[int] = []

    for task in active_tasks:
        assigned_role = infer_task_assignee_role(task, fallback_role_value=user.role)
        if assigned_role not in removed_roles:
            continue

        replacement_role = assigned_role
        replacement_id = await resolve_default_assignee(
            db,
            config,
            replacement_role,
            exclude_user_ids={telegram_id},
        )
        if replacement_id is None and replacement_role != Role.RP:
            replacement_role = Role.RP
            replacement_id = await resolve_default_assignee(
                db,
                config,
                replacement_role,
                exclude_user_ids={telegram_id},
            )

        if replacement_id is None:
            log.warning(
                "Could not reassign active task %s after removing role %s from user %s",
                task.get("id"),
                assigned_role,
                telegram_id,
            )
            continue

        await db.update_task_assignee(int(task["id"]), int(replacement_id), assigned_role=replacement_role)
        reassigned_task_ids.append(int(task["id"]))

    await db.set_user_roles(telegram_id, normalized_roles)
    return reassigned_task_ids
