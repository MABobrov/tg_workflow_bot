from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Set


def _parse_bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(val: str | None) -> Optional[int]:
    if val is None:
        return None
    val = val.strip()
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _parse_int_set(val: str | None) -> Set[int]:
    if not val:
        return set()
    items = []
    for part in val.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except ValueError:
            continue
    return set(items)


def _parse_username(val: str | None) -> Optional[str]:
    if val is None:
        return None
    v = val.strip()
    if not v:
        return None
    return v.lstrip("@").lower()


@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_ids: Set[int]

    db_path: str
    timezone: str

    # Optional: ID of a group chat where the bot posts all notifications.
    work_chat_id: Optional[int]

    # Defaults for task assignment
    default_rp_id: Optional[int]
    default_td_id: Optional[int]
    default_accounting_id: Optional[int]
    default_gd_id: Optional[int]
    default_driver_id: Optional[int]
    default_tinter_id: Optional[int]
    default_rp_username: Optional[str]
    default_td_username: Optional[str]
    default_accounting_username: Optional[str]
    default_gd_username: Optional[str]
    default_driver_username: Optional[str]
    default_tinter_username: Optional[str]

    # GD chat-proxy: managers and zamery
    default_manager_kv_id: Optional[int]
    default_manager_kv_username: Optional[str]
    default_manager_kia_id: Optional[int]
    default_manager_kia_username: Optional[str]
    default_manager_npn_id: Optional[int]
    default_manager_npn_username: Optional[str]
    default_zamery_id: Optional[int]
    default_zamery_username: Optional[str]
    chat_history_limit: int

    # Google Sheets integration
    sheets_enabled: bool
    gsheet_spreadsheet_id: Optional[str]
    gsheet_projects_tab: str
    gsheet_tasks_tab: str
    gsheet_invoices_tab: str
    gsheet_sales_spreadsheet_id: Optional[str]
    gsheet_sales_tab: str
    google_sa_json: Optional[str]  # raw JSON or base64 JSON
    google_sa_file: Optional[str]  # path to json file (alternative)

    # amoCRM / Kommo integration (optional)
    amocrm_enabled: bool
    amocrm_base_url: Optional[str]  # e.g. https://subdomain.amocrm.ru OR https://subdomain.kommo.com
    amocrm_client_id: Optional[str]
    amocrm_client_secret: Optional[str]
    amocrm_redirect_uri: Optional[str]
    # initial tokens (can be stored in DB after refresh)
    amocrm_access_token: Optional[str]
    amocrm_refresh_token: Optional[str]
    # mapping: amo user_id → role code (e.g. {11316010: "КВ", 11317938: "НПН"})
    amocrm_user_map: dict[int, str]

    # Behaviour
    enable_webhook: bool
    webhook_url: Optional[str]
    webhook_secret: Optional[str]

    # Sheets webhook (real-time sync from Google Sheets)
    sheets_webhook_secret: Optional[str]
    sheets_webhook_port: int

    # Reminders
    reminders_enabled: bool
    remind_soon_minutes: int
    remind_overdue_minutes: int

    def get_role_id(self, role: str) -> int | None:
        """Return default telegram_id for a given role string."""
        mapping = {
            "rp": self.default_rp_id,
            "td": self.default_td_id,
            "accounting": self.default_accounting_id,
            "gd": self.default_gd_id,
            "driver": self.default_driver_id,
            "tinter": self.default_tinter_id,
            "manager_kv": self.default_manager_kv_id,
            "manager_kia": self.default_manager_kia_id,
            "manager_npn": self.default_manager_npn_id,
            "zamery": self.default_zamery_id,
        }
        return mapping.get(role)

    def get_role_username(self, role: str) -> str | None:
        """Return default username for a given role string."""
        mapping = {
            "rp": self.default_rp_username,
            "td": self.default_td_username,
            "accounting": self.default_accounting_username,
            "gd": self.default_gd_username,
            "driver": self.default_driver_username,
            "tinter": self.default_tinter_username,
            "manager_kv": self.default_manager_kv_username,
            "manager_kia": self.default_manager_kia_username,
            "manager_npn": self.default_manager_npn_username,
            "zamery": self.default_zamery_username,
        }
        return mapping.get(role)


def load_config() -> Config:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    admin_ids = _parse_int_set(os.getenv("ADMIN_IDS"))

    db_path = os.getenv("DB_PATH", "./data/bot.sqlite3").strip()
    timezone = os.getenv("TIMEZONE", "Europe/Moscow").strip()

    work_chat_id = _parse_int(os.getenv("WORK_CHAT_ID"))

    default_rp_id = _parse_int(os.getenv("DEFAULT_RP_ID"))
    default_td_id = _parse_int(os.getenv("DEFAULT_TD_ID"))
    default_accounting_id = _parse_int(os.getenv("DEFAULT_ACCOUNTING_ID"))
    default_gd_id = _parse_int(os.getenv("DEFAULT_GD_ID"))
    default_driver_id = _parse_int(os.getenv("DEFAULT_DRIVER_ID"))
    default_tinter_id = _parse_int(os.getenv("DEFAULT_TINTER_ID"))
    default_rp_username = _parse_username(os.getenv("DEFAULT_RP_USERNAME"))
    default_td_username = _parse_username(os.getenv("DEFAULT_TD_USERNAME"))
    default_accounting_username = _parse_username(os.getenv("DEFAULT_ACCOUNTING_USERNAME"))
    default_gd_username = _parse_username(os.getenv("DEFAULT_GD_USERNAME"))
    default_driver_username = _parse_username(os.getenv("DEFAULT_DRIVER_USERNAME"))
    default_tinter_username = _parse_username(os.getenv("DEFAULT_TINTER_USERNAME"))

    default_manager_kv_id = _parse_int(os.getenv("DEFAULT_MANAGER_KV_ID"))
    default_manager_kv_username = _parse_username(os.getenv("DEFAULT_MANAGER_KV_USERNAME"))
    default_manager_kia_id = _parse_int(os.getenv("DEFAULT_MANAGER_KIA_ID"))
    default_manager_kia_username = _parse_username(os.getenv("DEFAULT_MANAGER_KIA_USERNAME"))
    default_manager_npn_id = _parse_int(os.getenv("DEFAULT_MANAGER_NPN_ID"))
    default_manager_npn_username = _parse_username(os.getenv("DEFAULT_MANAGER_NPN_USERNAME"))
    default_zamery_id = _parse_int(os.getenv("DEFAULT_ZAMERY_ID"))
    default_zamery_username = _parse_username(os.getenv("DEFAULT_ZAMERY_USERNAME"))
    chat_history_limit = _parse_int(os.getenv("CHAT_HISTORY_LIMIT", "20")) or 20

    sheets_enabled = _parse_bool(os.getenv("SHEETS_ENABLED"), default=False)
    gsheet_spreadsheet_id = os.getenv("GSHEET_SPREADSHEET_ID")
    gsheet_projects_tab = os.getenv("GSHEET_PROJECTS_TAB", "Projects").strip()
    gsheet_tasks_tab = os.getenv("GSHEET_TASKS_TAB", "Tasks").strip()
    gsheet_invoices_tab = os.getenv("GSHEET_INVOICES_TAB", "Invoices").strip()
    gsheet_sales_spreadsheet_id = os.getenv("GSHEET_SALES_SPREADSHEET_ID")
    gsheet_sales_tab = os.getenv("GSHEET_SALES_TAB", "Отдел продаж").strip()
    google_sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    google_sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

    amocrm_enabled = _parse_bool(os.getenv("AMOCRM_ENABLED"), default=False)
    amocrm_base_url = os.getenv("AMOCRM_BASE_URL")
    amocrm_client_id = os.getenv("AMOCRM_CLIENT_ID")
    amocrm_client_secret = os.getenv("AMOCRM_CLIENT_SECRET")
    amocrm_redirect_uri = os.getenv("AMOCRM_REDIRECT_URI")
    amocrm_access_token = os.getenv("AMOCRM_ACCESS_TOKEN")
    amocrm_refresh_token = os.getenv("AMOCRM_REFRESH_TOKEN")
    # Parse AMOCRM_USER_MAP: "11316010:КВ,11317938:НПН,9720106:ИП"
    amocrm_user_map: dict[int, str] = {}
    for pair in (os.getenv("AMOCRM_USER_MAP") or "").split(","):
        pair = pair.strip()
        if ":" in pair:
            uid_str, role = pair.split(":", 1)
            try:
                amocrm_user_map[int(uid_str.strip())] = role.strip()
            except ValueError:
                pass

    enable_webhook = _parse_bool(os.getenv("WEBHOOK_ENABLED"), default=False)
    webhook_url = os.getenv("WEBHOOK_URL")
    webhook_secret = os.getenv("WEBHOOK_SECRET")

    sheets_webhook_secret = os.getenv("SHEETS_WEBHOOK_SECRET")
    sheets_webhook_port = _parse_int(os.getenv("SHEETS_WEBHOOK_PORT", "8443")) or 8443

    reminders_enabled = _parse_bool(os.getenv("REMINDERS_ENABLED"), default=True)
    remind_soon_minutes = _parse_int(os.getenv("REMIND_SOON_MINUTES", "60")) or 60
    remind_overdue_minutes = _parse_int(os.getenv("REMIND_OVERDUE_MINUTES", "10")) or 10

    return Config(
        bot_token=token,
        admin_ids=admin_ids,
        db_path=db_path,
        timezone=timezone,
        work_chat_id=work_chat_id,
        default_rp_id=default_rp_id,
        default_td_id=default_td_id,
        default_accounting_id=default_accounting_id,
        default_gd_id=default_gd_id,
        default_driver_id=default_driver_id,
        default_tinter_id=default_tinter_id,
        default_rp_username=default_rp_username,
        default_td_username=default_td_username,
        default_accounting_username=default_accounting_username,
        default_gd_username=default_gd_username,
        default_driver_username=default_driver_username,
        default_tinter_username=default_tinter_username,
        default_manager_kv_id=default_manager_kv_id,
        default_manager_kv_username=default_manager_kv_username,
        default_manager_kia_id=default_manager_kia_id,
        default_manager_kia_username=default_manager_kia_username,
        default_manager_npn_id=default_manager_npn_id,
        default_manager_npn_username=default_manager_npn_username,
        default_zamery_id=default_zamery_id,
        default_zamery_username=default_zamery_username,
        chat_history_limit=chat_history_limit,
        sheets_enabled=sheets_enabled,
        gsheet_spreadsheet_id=gsheet_spreadsheet_id,
        gsheet_projects_tab=gsheet_projects_tab,
        gsheet_tasks_tab=gsheet_tasks_tab,
        gsheet_invoices_tab=gsheet_invoices_tab,
        gsheet_sales_spreadsheet_id=gsheet_sales_spreadsheet_id,
        gsheet_sales_tab=gsheet_sales_tab,
        google_sa_json=google_sa_json,
        google_sa_file=google_sa_file,
        amocrm_enabled=amocrm_enabled,
        amocrm_base_url=amocrm_base_url,
        amocrm_client_id=amocrm_client_id,
        amocrm_client_secret=amocrm_client_secret,
        amocrm_redirect_uri=amocrm_redirect_uri,
        amocrm_access_token=amocrm_access_token,
        amocrm_refresh_token=amocrm_refresh_token,
        amocrm_user_map=amocrm_user_map,
        sheets_webhook_secret=sheets_webhook_secret,
        sheets_webhook_port=sheets_webhook_port,
        enable_webhook=enable_webhook,
        webhook_url=webhook_url,
        webhook_secret=webhook_secret,
        reminders_enabled=reminders_enabled,
        remind_soon_minutes=remind_soon_minutes,
        remind_overdue_minutes=remind_overdue_minutes,
    )
