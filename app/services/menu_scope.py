from __future__ import annotations

from threading import Lock

from ..utils import parse_roles

_lock = Lock()
_active_menu_roles: dict[int, str] = {}


def set_active_menu_role(user_id: int | None, role: str | None) -> None:
    if not user_id:
        return
    with _lock:
        if role:
            _active_menu_roles[int(user_id)] = role
        else:
            _active_menu_roles.pop(int(user_id), None)


def clear_active_menu_role(user_id: int | None) -> None:
    if not user_id:
        return
    with _lock:
        _active_menu_roles.pop(int(user_id), None)


def get_active_menu_role(user_id: int | None) -> str | None:
    if not user_id:
        return None
    with _lock:
        return _active_menu_roles.get(int(user_id))


def resolve_active_menu_role(user_id: int | None, role_value: str | None) -> str | None:
    roles = parse_roles(role_value)
    if not roles:
        clear_active_menu_role(user_id)
        return role_value
    if len(roles) == 1:
        set_active_menu_role(user_id, roles[0])
        return roles[0]
    if not user_id:
        return None
    with _lock:
        active_role = _active_menu_roles.get(int(user_id))
    if active_role in roles:
        return active_role
    return None


def resolve_menu_scope(user_id: int | None, role_value: str | None) -> tuple[str | None, bool]:
    menu_role = resolve_active_menu_role(user_id, role_value)
    roles = parse_roles(role_value)
    isolated_role = bool(
        user_id
        and menu_role
        and len(roles) > 1
        and menu_role in roles
    )
    return menu_role or role_value, isolated_role
