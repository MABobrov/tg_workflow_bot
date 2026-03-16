"""Menu scope resolution — single-role mode.

Each user has exactly one role. Multi-role support removed.
Functions kept for backward compatibility but simplified.
"""
from __future__ import annotations

from ..utils import parse_roles


def set_active_menu_role(user_id: int | None, role: str | None) -> None:
    """No-op — kept for backward compat."""


def clear_active_menu_role(user_id: int | None) -> None:
    """No-op — kept for backward compat."""


def get_active_menu_role(user_id: int | None) -> str | None:
    """No-op — kept for backward compat."""
    return None


def resolve_active_menu_role(user_id: int | None, role_value: str | None) -> str | None:
    """Return the user's role (first if comma-separated for legacy data)."""
    roles = parse_roles(role_value)
    if not roles:
        return role_value
    return roles[0]


def resolve_menu_scope(user_id: int | None, role_value: str | None) -> tuple[str | None, bool]:
    """Return (role, False). isolated_role is always False — no multi-role."""
    menu_role = resolve_active_menu_role(user_id, role_value)
    return menu_role or role_value, False
