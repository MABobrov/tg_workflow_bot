from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from .callbacks import ManagerProjectCb, ProjectCb, TaskCb
from .enums import MANAGER_ROLES, Role, TaskStatus, TaskType
from .utils import parse_roles, task_status_label, task_type_label, try_json_loads


BACK_TO_HOME = "⬅️ Назад в главное меню"
BACK_TO_ROLE_SELECTOR = "⬅️ К выбору роли"
OPEN_ACTIONS = "📂 Ещё действия"
OPEN_HELP = "📚 Справка"
OPEN_ADMIN_PANEL = "🛠 Адм.панель"
ADMIN_HELP_BUTTON = "🛠 Админ-инструкция"
ADMIN_EMPLOYEES_BUTTON = "👥 Сотрудники"
ADMIN_STATS_BUTTON = "📊 Статистика бота"
ADMIN_WORKCHAT_BUTTON = "💬 Рабочий чат"
ADMIN_WORKCHAT_TEST_BUTTON = "🧪 Тест рабочего чата"
ADMIN_RESYNC_BUTTON = "🔄 Синхронизация Sheets"

# --- GD-specific buttons ---
GD_BTN_INBOX_GD = "📥 Входящие для ГД"
GD_BTN_INVOICES = "Счета на Оплату"
GD_BTN_SEARCH_INVOICE = "🔍 Поиск счёта"
GD_BTN_CHAT_RP = "Чат с РП (НПН)"
GD_BTN_ZAMERY = "Замеры"
GD_BTN_ACCOUNTING = "Бухгалтерия"
GD_BTN_MONTAZH = "Монтажная гр."
GD_BTN_SALES = "Отд.Продаж"
GD_BTN_SYNC = "Синхронизация данных"
GD_BTN_MORE = "📂 Ещё"
GD_BTN_CRED = "💬 Кред"
GD_BTN_CANCEL = "❌ Отмена"
GD_BTN_ADMIN = "🛠 Адм.панель"
GD_BTN_KV_CRED = "КВ Кред"
GD_BTN_KIA_CRED = "КИА Кред"
GD_BTN_NPN_CRED = "НПН Кред"
GD_SUBBTN_KV_CRED = "Менеджер КВ (кредит)"
GD_SUBBTN_KIA_CRED = "Менеджер КИА (кредит)"
GD_SUBBTN_NPN_CRED = "Менеджер НПН (кредит)"
GD_BTN_INVOICES_WORK = "📊 Счета в работе"
GD_BTN_INVOICE_END_GD = "🏁 Счёт END"
GD_BTN_SUPPLIER_PAY = "💸 Оплата поставщику"
GD_BTN_BACK_HOME = BACK_TO_HOME
GD_BTN_REFRESH = "🔄 Обновить меню"
GD_BTN_HELP = "📚 Справка"
GD_BTN_DAILY_SUMMARY = "📊 Сводка дня"

# --- Manager-specific buttons (КВ / КИА / НПН) ---
MGR_BTN_INBOX = "Задачи / Лид в проект"
MGR_BTN_CHECK_KP = "Проверить КП / Счет"
MGR_BTN_INVOICE_START = "Счет в Работу"
MGR_BTN_INVOICE_END = "Счет End"
MGR_BTN_ZAMERY = "Замеры"
MGR_BTN_EDO = "Бухгалтерия (Док./ЭДО)"
MGR_BTN_MONTAZH = "Монтажная гр."
MGR_BTN_CHAT_RP = "Чат с РП"
MGR_BTN_MY_INVOICES = "Мои Счета"
MGR_BTN_MORE = "Еще"
MGR_BTN_SYNC = "Синхронизация данных"
MGR_BTN_CANCEL = "Отмена"
# --- Manager "Еще" submenu ---
MGR_BTN_CRED = "Менеджер (кред)"
MGR_BTN_NOT_URGENT = "Не срочно ГД"
MGR_BTN_SEARCH_INVOICE = "Поиск Счета КВ / КИА / НПН"
MGR_BTN_HELP = "Справка"
MGR_BTN_BACK_HOME = BACK_TO_HOME
# --- Legacy constants (kept for handler compatibility) ---
MGR_BTN_URGENT = "🚨 Срочно ГД"
MGR_BTN_ZP = "💰 Запрос ЗП"
MGR_BTN_ISSUE = "🆘 Проблема/Вопрос"

# --- RP-specific buttons (new layout March 2026) ---
RP_BTN_CHECK_KP = "Проверка КП / Выставление Счета"
RP_BTN_CHAT_GD = "Чат с ГД"
RP_BTN_INVOICES_WORK = "Счета в Работе"
RP_BTN_MGR_KV = "Менеджер 1 (КВ)"
RP_BTN_INVOICES_PAY = "Счета на оплату"
RP_BTN_MGR_KIA = "Менеджер 2 (КИА)"
RP_BTN_EDO = "Бухгалтерия (УПД)"
RP_BTN_MONTAZH = "Монтажная гр."
RP_BTN_INVOICE_CLOSED = "Счет закрыт"
RP_BTN_LEAD = "Лид на расчет"
RP_BTN_CANCEL = "❌ Отмена"
RP_BTN_MORE = "📂 Еще"
# --- RP/NPN role-switching buttons (top row of main menu) ---
RP_BTN_ROLE_RP = "✅ РП"
RP_BTN_ROLE_NPN = "👤 Менеджер НПН"
RP_BTN_ROLE_RP_INACTIVE = "📋 РП"
RP_BTN_ROLE_NPN_ACTIVE = "✅ Менеджер НПН"
RP_BTN_BACK_HOME = BACK_TO_HOME
# --- RP legacy aliases (kept for backward compat in handlers) ---
RP_BTN_INBOX_SALES = "📥 Входящие Отд.Продаж"   # legacy
RP_BTN_NOT_URGENT = "📩 Не срочно ГД"            # legacy
RP_BTN_INVOICE_START = "💼 Счета в Работу"        # legacy
RP_BTN_INVOICE_END = "🏁 Счет End"                # legacy
RP_BTN_ISSUE = "🆘 Проблема/Вопрос"              # legacy
RP_BTN_TEAM = "👥 Команда"                        # legacy
RP_SUBBTN_MGR_KV = "Менеджер КВ"                  # legacy
RP_SUBBTN_MGR_KIA = "Менеджер КИА"                # legacy
RP_SUBBTN_MONTAZH = "Монтаж"                      # legacy
RP_MONTAZH_BTN_RAZMERY = "📐 Размеры"
RP_BTN_URGENT = "🚨 Срочно ГД"                    # legacy
RP_BTN_SEARCH_INVOICE = "🔍 Поиск счёта"          # legacy
RP_BTN_SYNC = "🔄 Синхронизация данных"            # legacy
RP_BTN_HELP = "📚 Справка"                        # legacy

# --- Accounting buttons ---
ACC_BTN_INBOX = "📥 Входящие задачи"
ACC_BTN_INVOICES_WORK = "📊 Счета в работе"
ACC_BTN_SEARCH = "🔍 Поиск счёта"
ACC_BTN_INVOICE_END = "🏁 Закрытые Счета"
ACC_BTN_SYNC = "🔄 Синхронизация данных"
ACC_BTN_URGENT = "🚨 Срочно ГД"

# --- Installer buttons ---
INST_BTN_INBOX = "📥 Входящие задачи"
INST_BTN_ORDER_MAT = "📦 Заказ материалов"
INST_BTN_INVOICE_OK = "✅ Счет ок"
INST_BTN_RAZMERY_OK = "📐 Размеры ОК"
INST_BTN_ORDER_EXTRA = "📦 Заказ доп.материалов"
INST_BTN_MY_OBJECTS = "📌 Мои объекты"
INST_BTN_DAILY_REPORT = "📝 Отчёт за день"
INST_BTN_IN_WORK = "🔨 В Работу"
INST_BTN_NOT_URGENT = "📩 Не срочно ГД"
INST_BTN_URGENT = "🚨 Срочно ГД"
INST_BTN_ZP = "💰 Запрос ЗП"
INST_BTN_SYNC = "🔄 Синхронизация данных"

# --- Zamery buttons ---
ZAM_BTN_ZAMERY = "📋 Заявка на замер"
ZAM_BTN_MY_OBJECTS = "📋 Мои замеры"
ZAM_BTN_URGENT = "🚨 Срочно ГД"
ZAM_BTN_NOT_URGENT = "📩 Не срочно ГД"
ZAM_BTN_SYNC = "🔄 Синхронизация данных"
ZAM_BTN_PAYMENT = "💰 Оплата замеров"
ZAM_BTN_SCHEDULE = "📅 График замеров"

ROLE_SELECTOR_PREFIX = "🎭 "
ROLE_SELECTOR_LABELS: dict[str, str] = {
    Role.MANAGER: f"{ROLE_SELECTOR_PREFIX}Менеджер",
    Role.MANAGER_KV: f"{ROLE_SELECTOR_PREFIX}Менеджер КВ",
    Role.MANAGER_KIA: f"{ROLE_SELECTOR_PREFIX}Менеджер КИА",
    Role.MANAGER_NPN: f"{ROLE_SELECTOR_PREFIX}Менеджер НПН",
    Role.RP: f"{ROLE_SELECTOR_PREFIX}РП",
    Role.ACCOUNTING: f"{ROLE_SELECTOR_PREFIX}Бухгалтерия",
    Role.INSTALLER: f"{ROLE_SELECTOR_PREFIX}Монтажник",
    Role.ZAMERY: f"{ROLE_SELECTOR_PREFIX}Замерщик",
    Role.GD: f"{ROLE_SELECTOR_PREFIX}ГД",
    Role.DRIVER: f"{ROLE_SELECTOR_PREFIX}Водитель",
    Role.LOADER: f"{ROLE_SELECTOR_PREFIX}Грузчик",
    Role.TINTER: f"{ROLE_SELECTOR_PREFIX}Тонировщик",
}


def _build_reply_rows(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for row in rows:
        for label in row:
            kb.button(text=label)
    kb.adjust(*[len(row) for row in rows])
    return kb.as_markup(resize_keyboard=True, is_persistent=True)


def role_selector_label(role: str) -> str:
    return ROLE_SELECTOR_LABELS.get(role, f"{ROLE_SELECTOR_PREFIX}{role}")


def role_selector_choices(role_value: str | None) -> dict[str, str]:
    return {role_selector_label(role): role for role in parse_roles(role_value)}


def role_selector_menu(role_value: str | None, is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    role_buttons = [role_selector_label(role) for role in parse_roles(role_value)]
    for index in range(0, len(role_buttons), 2):
        rows.append(role_buttons[index:index + 2])
    rows.append([OPEN_HELP, "🔄 Обновить меню"])
    if is_admin:
        rows.append([OPEN_ADMIN_PANEL])
    rows.append(["❌ Отмена"])
    return _build_reply_rows(rows)


def _role_primary_action_rows(role: str | None) -> list[list[str]]:
    # --- 3 менеджера (КВ / КИА / НПН) — единое меню ---
    if role in MANAGER_ROLES or role == Role.MANAGER:
        return [
            [MGR_BTN_INBOX, MGR_BTN_CHECK_KP],
            [MGR_BTN_INVOICE_START, MGR_BTN_INVOICE_END],
            [MGR_BTN_ZAMERY, MGR_BTN_EDO],
            [MGR_BTN_MONTAZH, MGR_BTN_CHAT_RP],
            [MGR_BTN_MY_INVOICES, MGR_BTN_MORE],
            [MGR_BTN_SYNC, MGR_BTN_CANCEL],
        ]
    if role == Role.RP:
        return [
            [RP_BTN_CHECK_KP, RP_BTN_CHAT_GD],
            [RP_BTN_INVOICES_WORK, RP_BTN_MGR_KV],
            [RP_BTN_INVOICES_PAY, RP_BTN_MGR_KIA],
            [RP_BTN_EDO, RP_BTN_MONTAZH],
            [RP_BTN_INVOICE_CLOSED, RP_BTN_LEAD],
            [RP_BTN_CANCEL, RP_BTN_MORE],
        ]
    if role == Role.TD:
        # TD merged into GD — redirect to GD menu
        return [
            [GD_BTN_INBOX_GD, GD_BTN_INVOICES],
            [GD_BTN_INVOICE_END_GD, GD_BTN_SUPPLIER_PAY],
            [GD_BTN_CHAT_RP, GD_BTN_ACCOUNTING],
            [GD_BTN_MONTAZH, GD_BTN_SALES],
            [GD_BTN_SYNC, GD_BTN_SEARCH_INVOICE],
            [GD_BTN_CANCEL, GD_BTN_MORE],
        ]
    if role == Role.ACCOUNTING:
        return [
            [ACC_BTN_INBOX, ACC_BTN_INVOICES_WORK],
            [ACC_BTN_SEARCH, ACC_BTN_INVOICE_END],
            [ACC_BTN_SYNC, ACC_BTN_URGENT],
            ["📋 Все задачи"],
        ]
    if role == Role.INSTALLER:
        return [
            [INST_BTN_INBOX, INST_BTN_IN_WORK],
            [INST_BTN_ORDER_MAT, INST_BTN_ORDER_EXTRA],
            [INST_BTN_INVOICE_OK, INST_BTN_RAZMERY_OK],
            [INST_BTN_MY_OBJECTS, INST_BTN_DAILY_REPORT],
            [INST_BTN_ZP, INST_BTN_NOT_URGENT],
            [INST_BTN_URGENT, INST_BTN_SYNC],
            ["📋 Все задачи"],
        ]
    if role == Role.GD:
        return [
            [GD_BTN_INBOX_GD, GD_BTN_INVOICES],
            [GD_BTN_INVOICE_END_GD, GD_BTN_SUPPLIER_PAY],
            [GD_BTN_CHAT_RP, GD_BTN_ACCOUNTING],
            [GD_BTN_MONTAZH, GD_BTN_SALES],
            [GD_BTN_ZAMERY, GD_BTN_SEARCH_INVOICE],
            [GD_BTN_CANCEL, GD_BTN_MORE],
        ]
    if role == Role.DRIVER:
        return [
            ["📥 Входящие задачи", "✅ Доставка выполнена"],
        ]
    if role == Role.LOADER:
        return [
            ["📥 Входящие задачи"],
        ]
    if role == Role.TINTER:
        return [
            ["📥 Входящие задачи", "✅ Тонировка выполнена"],
        ]
    if role == Role.ZAMERY:
        return [
            [ZAM_BTN_ZAMERY, ZAM_BTN_MY_OBJECTS],
            [ZAM_BTN_SCHEDULE, ZAM_BTN_PAYMENT],
            [ZAM_BTN_URGENT, ZAM_BTN_SYNC],
            ["📋 Все задачи"],
            [OPEN_HELP, BACK_TO_HOME],
        ]
    return []


def _role_secondary_action_rows(role: str | None) -> list[list[str]]:
    # --- 3 менеджера (КВ / КИА / НПН) — подменю "Еще" ---
    if role in MANAGER_ROLES or role == Role.MANAGER:
        return [
            [MGR_BTN_CRED],
            [MGR_BTN_NOT_URGENT, MGR_BTN_SEARCH_INVOICE],
            ["📋 Все задачи", MGR_BTN_HELP],
            [MGR_BTN_CANCEL, MGR_BTN_BACK_HOME],
        ]
    if role == Role.RP:
        return [
            [RP_BTN_SEARCH_INVOICE, RP_BTN_SYNC],
            ["📋 Все задачи", RP_BTN_HELP],
            [RP_BTN_CANCEL, RP_BTN_BACK_HOME],
        ]
    if role == Role.TD:
        # TD merged into GD — use GD submenu
        return [
            [GD_SUBBTN_KV_CRED, GD_SUBBTN_KIA_CRED],
            [GD_SUBBTN_NPN_CRED],
            ["📋 Все задачи"],
        ]
    if role == Role.ACCOUNTING:
        return []  # Accounting has no submenu
    if role == Role.INSTALLER:
        return []  # Installer has no submenu (all buttons in primary)
    if role == Role.GD:
        return [
            [GD_SUBBTN_KV_CRED, GD_SUBBTN_KIA_CRED],
            [GD_SUBBTN_NPN_CRED],
            ["📋 Все задачи"],
        ]
    if role == Role.DRIVER:
        return [
            ["📌 Поиск проекта", "🚨 Срочно ГД"],
        ]
    if role == Role.LOADER:
        return [
            ["🚨 Срочно ГД"],
        ]
    if role == Role.TINTER:
        return [
            ["📌 Поиск проекта", "🚨 Срочно ГД"],
        ]
    if role == Role.ZAMERY:
        return []  # Zamery has no submenu
    return []


def _merge_rows_for_roles(role_value: str | None, row_provider: Any) -> list[list[str]]:
    roles = parse_roles(role_value)
    if not roles and role_value:
        roles = [role_value]
    seen: set[tuple[str, ...]] = set()
    rows: list[list[str]] = []
    for role in roles:
        for row in row_provider(role):
            row_key = tuple(row)
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append(list(row))
    return rows


def _is_pure_gd(role: str | None) -> bool:
    """Check if user has only the GD role (no combined roles)."""
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.GD]


def _is_pure_manager(role: str | None) -> bool:
    """Check if user has only a manager role."""
    if not role:
        return False
    roles = parse_roles(role)
    return len(roles) == 1 and roles[0] in MANAGER_ROLES


def _is_pure_rp(role: str | None) -> bool:
    """Check if user has only RP role."""
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.RP]


def _is_pure_manager_npn(role: str | None) -> bool:
    """Check if user has only the MANAGER_NPN role."""
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.MANAGER_NPN]


def _is_pure_accounting(role: str | None) -> bool:
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.ACCOUNTING]


def _is_pure_installer(role: str | None) -> bool:
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.INSTALLER]


def _is_pure_zamery(role: str | None) -> bool:
    if not role:
        return False
    roles = parse_roles(role)
    return roles == [Role.ZAMERY]


def _format_role_badge(base: str, tasks: int = 0, messages: int = 0) -> str:
    """Format role button with separate task/message badges.

    Rules:
    - Both: "✅ РП 🔴3/💬2"
    - Tasks only: "✅ РП 🔴3"
    - Messages only: "✅ РП 💬2"
    - Nothing: "✅ РП"
    """
    if tasks > 0 and messages > 0:
        return f"{base} 🔴{tasks}/💬{messages}"
    if tasks > 0:
        return f"{base} 🔴{tasks}"
    if messages > 0:
        return f"{base} 💬{messages}"
    return base


def main_menu(
    role: str | None,
    is_admin: bool = False,
    unread: int = 0,
    unread_channels: dict[str, int] | None = None,
    gd_inbox_unread: int | None = None,
    gd_invoice_unread: int | None = None,
    gd_invoice_end_unread: int | None = None,
    gd_supplier_pay_unread: int | None = None,
    isolated_role: bool = False,
    rp_tasks: int = 0,
    rp_messages: int = 0,
    npn_tasks: int = 0,
    npn_messages: int = 0,
    rp_check_kp: int = 0,
    rp_invoices_pay: int = 0,
    rp_ch_mgr_kv: int = 0,
    rp_ch_mgr_kia: int = 0,
    rp_ch_montazh: int = 0,
) -> ReplyKeyboardMarkup:
    parsed_roles = parse_roles(role)
    if not isolated_role and len(parsed_roles) > 1:
        return role_selector_menu(role, is_admin=is_admin)

    inbox_label = MGR_BTN_INBOX
    if unread > 0:
        inbox_label = f"{MGR_BTN_INBOX} 🔴{unread}"

    rp_inbox_label = RP_BTN_INBOX_SALES
    if unread > 0:
        rp_inbox_label += f" 🔴{unread}"

    # GD inbox uses separate counter (tasks only, excl. invoices & payment confirms)
    _gd_count = gd_inbox_unread if gd_inbox_unread is not None else unread
    gd_inbox_label = GD_BTN_INBOX_GD
    if _gd_count > 0:
        gd_inbox_label += f" 🔴{_gd_count}"

    # GD invoice payment badge
    gd_invoice_label = GD_BTN_INVOICES
    if gd_invoice_unread and gd_invoice_unread > 0:
        gd_invoice_label += f" 🔴{gd_invoice_unread}"

    # GD "Счёт END" badge (payment_confirm + invoice_end tasks)
    gd_invoice_end_label = GD_BTN_INVOICE_END_GD
    if gd_invoice_end_unread and gd_invoice_end_unread > 0:
        gd_invoice_end_label += f" 🔴{gd_invoice_end_unread}"

    # GD "Оплата поставщику" badge (pending ZP requests)
    gd_supplier_pay_label = GD_BTN_SUPPLIER_PAY
    if gd_supplier_pay_unread and gd_supplier_pay_unread > 0:
        gd_supplier_pay_label += f" 🔴{gd_supplier_pay_unread}"

    # Map channel names to chat button constants (for per-channel badge)
    _CHAN_BTN: dict[str, str] = {
        "rp": GD_BTN_CHAT_RP,
        "accounting": GD_BTN_ACCOUNTING,
        "zamery": GD_BTN_ZAMERY,
        "montazh": GD_BTN_MONTAZH,
        "otd_prodazh": GD_BTN_SALES,
        "cred": GD_BTN_MORE,
    }
    # Build labels with per-channel unread counts
    _uc = unread_channels or {}
    # For composite channel "otd_prodazh", sum manager sub-channels (without rp)
    _otd_sum = sum(_uc.get(sc, 0) for sc in ("manager_kv", "manager_kia", "manager_npn"))
    _cred_sum = sum(_uc.get(sc, 0) for sc in ("manager_kv", "manager_kia", "manager_npn"))
    _chan_labels: dict[str, str] = {}
    for chan, base_btn in _CHAN_BTN.items():
        if chan == "otd_prodazh":
            cnt = _otd_sum
        elif chan == "cred":
            cnt = _cred_sum
        else:
            cnt = _uc.get(chan, 0)
        _chan_labels[base_btn] = f"{base_btn} 🔴{cnt}" if cnt > 0 else base_btn

    def _patch_inbox(rows: list[list[str]]) -> None:
        """Replace hardcoded inbox/chat text with dynamic counter."""
        for row in rows:
            for i, btn in enumerate(row):
                if unread > 0:
                    if btn == MGR_BTN_INBOX or btn == ACC_BTN_INBOX or btn == INST_BTN_INBOX or btn == "📥 Входящие задачи":
                        row[i] = inbox_label
                    elif btn == RP_BTN_INBOX_SALES:
                        row[i] = rp_inbox_label
                    elif btn == GD_BTN_INBOX_GD:
                        row[i] = gd_inbox_label
                    elif btn == ZAM_BTN_ZAMERY:
                        row[i] = f"{ZAM_BTN_ZAMERY} 🔴{unread}"
                # GD invoice payment badge
                if btn == GD_BTN_INVOICES:
                    row[i] = gd_invoice_label
                # GD "Счёт END" badge
                if btn == GD_BTN_INVOICE_END_GD:
                    row[i] = gd_invoice_end_label
                # GD "Оплата поставщику" badge
                if btn == GD_BTN_SUPPLIER_PAY:
                    row[i] = gd_supplier_pay_label
                # Per-channel chat badges (GD)
                if btn in _chan_labels:
                    row[i] = _chan_labels[btn]
                # RP per-button badges
                if btn == RP_BTN_CHECK_KP and rp_check_kp > 0:
                    row[i] = f"{RP_BTN_CHECK_KP} 🔴{rp_check_kp}"
                if btn == RP_BTN_INVOICES_PAY and rp_invoices_pay > 0:
                    row[i] = f"{RP_BTN_INVOICES_PAY} 🔴{rp_invoices_pay}"
                if btn == RP_BTN_MGR_KV and rp_ch_mgr_kv > 0:
                    row[i] = f"{RP_BTN_MGR_KV} 🔴{rp_ch_mgr_kv}"
                if btn == RP_BTN_MGR_KIA and rp_ch_mgr_kia > 0:
                    row[i] = f"{RP_BTN_MGR_KIA} 🔴{rp_ch_mgr_kia}"
                if btn == RP_BTN_MONTAZH and rp_ch_montazh > 0:
                    row[i] = f"{RP_BTN_MONTAZH} 🔴{rp_ch_montazh}"

    # GD gets a custom layout — Отмена и Ещё уже в grid, админ в подменю
    if _is_pure_gd(role):
        rows: list[list[str]] = [list(r) for r in _role_primary_action_rows(Role.GD)]
        if isolated_role:
            rows.append([BACK_TO_ROLE_SELECTOR])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # MANAGER_NPN (switched from RP) — role-switching row + NPN manager menu
    # Must be checked BEFORE generic _is_pure_manager to add role-switch buttons
    if _is_pure_manager_npn(role) and not isolated_role:
        rp_label = _format_role_badge(RP_BTN_ROLE_RP_INACTIVE, rp_tasks, rp_messages)
        npn_label = _format_role_badge(RP_BTN_ROLE_NPN_ACTIVE, npn_tasks, npn_messages)
        rows = [[rp_label, npn_label]]
        rows.extend([list(row) for row in _role_primary_action_rows(Role.MANAGER_NPN)])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # Manager (КВ/КИА) — custom layout with built-in "Еще" button
    if _is_pure_manager(role):
        r = parse_roles(role)[0]
        rows = [list(row) for row in _role_primary_action_rows(r)]
        if isolated_role:
            rows.append([BACK_TO_ROLE_SELECTOR])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # RP — custom layout with role-switching row + built-in "Еще" button
    if _is_pure_rp(role) and not isolated_role:
        rp_label = _format_role_badge(RP_BTN_ROLE_RP, rp_tasks, rp_messages)
        npn_label = _format_role_badge(RP_BTN_ROLE_NPN, npn_tasks, npn_messages)
        rows: list[list[str]] = [[rp_label, npn_label]]
        rows.extend([list(row) for row in _role_primary_action_rows(Role.RP)])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    if isolated_role and role == Role.RP:
        rows = [list(row) for row in _role_primary_action_rows(Role.RP)]
        rows.append([BACK_TO_ROLE_SELECTOR])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # Accounting — compact layout, no submenu
    if _is_pure_accounting(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.ACCOUNTING)]
        rows.append(["❌ Отмена", OPEN_HELP])
        if isolated_role:
            rows.append([BACK_TO_ROLE_SELECTOR])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # Installer — compact layout, no submenu
    if _is_pure_installer(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.INSTALLER)]
        rows.append(["❌ Отмена", OPEN_HELP])
        if isolated_role:
            rows.append([BACK_TO_ROLE_SELECTOR])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # Zamery — compact layout, no submenu
    if _is_pure_zamery(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.ZAMERY)]
        if isolated_role:
            rows.append([BACK_TO_ROLE_SELECTOR])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        _patch_inbox(rows)
        return _build_reply_rows(rows)

    # Generic: combined roles or old roles
    rows = []
    if role:
        rows.extend(_merge_rows_for_roles(role, _role_primary_action_rows))
    secondary_rows = _merge_rows_for_roles(role, _role_secondary_action_rows)
    if secondary_rows:
        rows.append([OPEN_ACTIONS])
    rows.append([OPEN_HELP, "🔄 Обновить меню"])
    if isolated_role:
        rows.append([BACK_TO_ROLE_SELECTOR])
    if is_admin:
        rows.append([OPEN_ADMIN_PANEL])
    rows.append(["❌ Отмена"])
    _patch_inbox(rows)
    return _build_reply_rows(rows)


def actions_menu(
    role: str | None,
    is_admin: bool = False,
    show_role_selector_back: bool = False,
) -> ReplyKeyboardMarkup:
    rows = _merge_rows_for_roles(role, _role_secondary_action_rows)
    if is_admin:
        rows.append([OPEN_ADMIN_PANEL])
    if show_role_selector_back:
        rows.append([BACK_TO_HOME, BACK_TO_ROLE_SELECTOR])
    else:
        rows.append([BACK_TO_HOME])
    return _build_reply_rows(rows)


def admin_panel_menu() -> ReplyKeyboardMarkup:
    rows = [
        [ADMIN_HELP_BUTTON, ADMIN_EMPLOYEES_BUTTON],
        [ADMIN_STATS_BUTTON, ADMIN_WORKCHAT_BUTTON],
        [ADMIN_WORKCHAT_TEST_BUTTON, ADMIN_RESYNC_BUTTON],
        [BACK_TO_HOME],
    ]
    return _build_reply_rows(rows)


def projects_kb(projects: list[dict[str, Any]], ctx: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for p in projects:
        text = f"{p.get('code','')} • {p.get('title','')}"
        b.button(text=text[:60], callback_data=ProjectCb(project_id=int(p["id"]), ctx=ctx).pack())
    b.adjust(1)
    return b.as_markup()


_ROLE_SHORT: dict[str, str] = {
    "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    "rp": "РП", "zamery": "Замер", "accounting": "Бухг",
    "gd": "ГД", "installer": "Монт", "driver": "Вод", "tinter": "Тон",
}


def tasks_kb(tasks: list[dict[str, Any]], *, back_callback: str | None = None) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in tasks:
        role_short = _ROLE_SHORT.get(t.get("creator_role", ""), "")
        role_tag = f" ({role_short})" if role_short else ""
        text = f"#{t['id']}{role_tag} • {task_type_label(t.get('type'))} • {task_status_label(t.get('status'))}"
        b.button(text=text[:64], callback_data=TaskCb(task_id=int(t["id"]), action="open").pack())
    if back_callback:
        b.button(text="⬅️ Назад", callback_data=back_callback)
    b.adjust(1)
    return b.as_markup()


def task_actions_kb(task: dict[str, Any]) -> InlineKeyboardMarkup:
    ttype = task.get("type")
    status = task.get("status")
    b = InlineKeyboardBuilder()

    tid = int(task["id"])

    # accept button — only for open tasks not yet accepted
    if status == TaskStatus.OPEN and not task.get("accepted_at"):
        b.button(text="✅ Принято", callback_data=TaskCb(task_id=tid, action="accept").pack())

    if status == TaskStatus.OPEN:
        b.button(text="✅ Завершить", callback_data=TaskCb(task_id=tid, action="done").pack())
        b.button(text="⏳ Взять в работу", callback_data=TaskCb(task_id=tid, action="take").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())
    elif status == TaskStatus.IN_PROGRESS:
        b.button(text="✅ Завершить", callback_data=TaskCb(task_id=tid, action="done").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())

    # payment confirm special actions (TD)
    if ttype == TaskType.PAYMENT_CONFIRM and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Оплата подтверждена", callback_data=TaskCb(task_id=tid, action="pay_ok").pack())
        b.button(text="⚠️ Нужна доплата", callback_data=TaskCb(task_id=tid, action="pay_need").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())

    # Заказ материалов — кнопки для ТД (оплатить/отклонить)
    if ttype in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS} and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="💸 Оплатить поставщику", callback_data=TaskCb(task_id=tid, action="pay_supplier").pack())
        b.button(text="✅ Завершить", callback_data=TaskCb(task_id=tid, action="done").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())

    # Счёт на оплату — кнопки для ГД
    if ttype == TaskType.INVOICE_PAYMENT:
        if status == TaskStatus.OPEN:
            # Первый шаг — подтвердить получение
            b = InlineKeyboardBuilder()
            b.button(text="✅ Подтвердить оплату", callback_data=TaskCb(task_id=tid, action="inv_received").pack())
            b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="inv_reject").pack())
        elif status == TaskStatus.IN_PROGRESS:
            # После подтверждения — действия по оплате
            b = InlineKeyboardBuilder()
            b.button(text="✅ Оплатить", callback_data=TaskCb(task_id=tid, action="inv_pay").pack())
            b.button(text="⏸ Отложить", callback_data=TaskCb(task_id=tid, action="inv_hold").pack())
            b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="inv_reject").pack())

    # Оплата доставки — кнопки для ГД
    if ttype == TaskType.DELIVERY_REQUEST and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        if status == TaskStatus.OPEN:
            b.button(text="✅ Принял", callback_data=TaskCb(task_id=tid, action="del_accept").pack())
            b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())
        else:
            b.button(text="💳 Оплатить", callback_data=TaskCb(task_id=tid, action="del_pay").pack())
            b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=tid, action="reject").pack())

    # Пакетный запрос ЗП замерщика → ГД
    if ttype == TaskType.ZP_ZAMERY_BATCH and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"zampay_gd:ok:{tid}")
        b.button(text="❌ Отклонить", callback_data=f"zampay_gd:no:{tid}")
        b.adjust(2)
        return b.as_markup()

    # Задачи монтажной группы — Да/Нет/Комментарий
    if ttype == TaskType.GD_TASK and status == TaskStatus.OPEN:
        _payload = try_json_loads(task.get("payload_json"))
        if "montazh" in (_payload.get("source") or ""):
            b = InlineKeyboardBuilder()
            b.button(text="✅ Да", callback_data=TaskCb(task_id=tid, action="montazh_yes").pack())
            b.button(text="❌ Нет", callback_data=TaskCb(task_id=tid, action="montazh_no").pack())
            b.button(text="💬 Комментарий", callback_data=TaskCb(task_id=tid, action="montazh_comment").pack())
            b.adjust(2, 1)
            return b.as_markup()

    # «Снять задачу» — для всех активных задач
    if status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b.button(text="🚫 Снять задачу", callback_data=TaskCb(task_id=tid, action="cancel").pack())

    b.adjust(1)
    return b.as_markup()


def manager_project_actions_kb(project_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="💰 Оплата", callback_data=ManagerProjectCb(project_id=project_id, action="payment").pack())
    b.button(text="📄 Док. / ЭДО", callback_data=ManagerProjectCb(project_id=project_id, action="closing").pack())
    b.button(text="🆘 Проблема / вопрос", callback_data=ManagerProjectCb(project_id=project_id, action="issue").pack())
    b.button(text="🏁 Счёт End", callback_data=ManagerProjectCb(project_id=project_id, action="end").pack())
    b.button(text="📋 Задачи проекта", callback_data=ManagerProjectCb(project_id=project_id, action="tasks").pack())
    b.button(text="🔄 Обновить карточку", callback_data=ManagerProjectCb(project_id=project_id, action="refresh").pack())
    b.adjust(2, 2, 1, 1)
    return b.as_markup()



def gd_more_menu(
    is_admin: bool = False,
    show_role_selector_back: bool = False,
    unread_channels: dict[str, int] | None = None,
) -> ReplyKeyboardMarkup:
    """Подменю 'Ещё' для ГД с бейджами непрочитанных."""
    _uc = unread_channels or {}

    def _badge(base: str, chan: str) -> str:
        cnt = _uc.get(chan, 0)
        return f"{base} 🔴{cnt}" if cnt > 0 else base

    rows = [
        [_badge(GD_SUBBTN_KV_CRED, "manager_kv"), _badge(GD_SUBBTN_KIA_CRED, "manager_kia")],
        [_badge(GD_SUBBTN_NPN_CRED, "manager_npn"), GD_BTN_SYNC],
        [GD_BTN_DAILY_SUMMARY, "📋 Все задачи"],
    ]
    if is_admin:
        rows.append([GD_BTN_ADMIN, GD_BTN_REFRESH])
    else:
        rows.append([GD_BTN_REFRESH])
    if show_role_selector_back:
        rows.append([BACK_TO_ROLE_SELECTOR])
    rows.append([GD_BTN_CANCEL, GD_BTN_HELP])
    return _build_reply_rows(rows)


def gd_chat_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю для чат-прокси кнопок ГД."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", back_label],
    ]
    return _build_reply_rows(rows)


def gd_chat_submenu_finance(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю для чат-прокси с кнопкой отчёта (КВ/КИА)."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", "📊 Отчёт"],
        [back_label],
    ]
    return _build_reply_rows(rows)


def gd_sales_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю для Отд.Продаж (составной канал)."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", "📨 Входящие"],
        [back_label],
    ]
    return _build_reply_rows(rows)


def gd_sales_write_to_kb() -> ReplyKeyboardMarkup:
    """'Кому писать?' подменю для Отд.Продаж."""
    rows = [
        ["➡️ РП (НПН)", "➡️ Менеджер КВ"],
        ["➡️ Менеджер КИА", "➡️ Менеджер НПН"],
        ["➡️ Всем в отдел", "⬅️ Назад"],
    ]
    return _build_reply_rows(rows)


def gd_chat_write_to_kb_universal(
    targets: list[tuple[str, str]],
) -> ReplyKeyboardMarkup:
    """'Кому писать?' — универсальное подменю для всех каналов ГД.

    targets: список (channel_key, button_label).
    """
    rows: list[list[str]] = []
    # Размещаем адресатов по 2 в строку
    for i in range(0, len(targets), 2):
        row = [targets[i][1]]
        if i + 1 < len(targets):
            row.append(targets[i + 1][1])
        rows.append(row)
    rows.append(["➡️ Написать всем", "⬅️ Назад"])
    return _build_reply_rows(rows)


def manager_more_menu(show_role_selector_back: bool = False) -> ReplyKeyboardMarkup:
    """Подменю 'Еще' для менеджеров (КВ / КИА / НПН)."""
    rows: list[list[str]] = [
        [MGR_BTN_CRED],
        [MGR_BTN_NOT_URGENT, MGR_BTN_URGENT],
        [MGR_BTN_SEARCH_INVOICE, "📋 Все задачи"],
        [MGR_BTN_HELP],
    ]
    if show_role_selector_back:
        rows.append([BACK_TO_ROLE_SELECTOR])
    rows.append([MGR_BTN_CANCEL, MGR_BTN_BACK_HOME])
    return _build_reply_rows(rows)


def rp_more_menu(show_role_selector_back: bool = False) -> ReplyKeyboardMarkup:
    """Подменю 'Еще' для РП: Поиск, Синхронизация, Справка (#42/#43 убраны дубли)."""
    rows: list[list[str]] = [
        [RP_BTN_SEARCH_INVOICE, RP_BTN_SYNC],
        ["📋 Все задачи", RP_BTN_HELP],
    ]
    if show_role_selector_back:
        rows.append([BACK_TO_ROLE_SELECTOR])
    rows.append([RP_BTN_CANCEL, RP_BTN_BACK_HOME])
    return _build_reply_rows(rows)


def rp_team_menu(show_role_selector_back: bool = False) -> ReplyKeyboardMarkup:
    """Подменю 'Команда' для РП."""
    rows = [
        [RP_SUBBTN_MGR_KV, RP_SUBBTN_MGR_KIA],
        [RP_SUBBTN_MONTAZH],
    ]
    if show_role_selector_back:
        rows.append([BACK_TO_ROLE_SELECTOR])
    rows.append([RP_BTN_CANCEL, RP_BTN_BACK_HOME])
    return _build_reply_rows(rows)


def manager_chat_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю чат-прокси для менеджера (кред)."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📊 Отчёт", "📋 Задачи"],
        [back_label],
    ]
    return _build_reply_rows(rows)


def rp_chat_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю чат-прокси для РП ↔ Менеджер (КВ / КИА)."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", back_label],
    ]
    return _build_reply_rows(rows)


def rp_chat_gd_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю чат-прокси для РП ↔ ГД."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", back_label],
    ]
    return _build_reply_rows(rows)


def rp_montazh_submenu(back_label: str = "⬅️ Назад") -> ReplyKeyboardMarkup:
    """Подменю «Монтажная гр.» для РП: Чат / В работу / Размеры."""
    rows = [
        ["💬 Чат", "🔧 В работу"],
        ["📐 Размеры", back_label],
    ]
    return _build_reply_rows(rows)


def edo_type_kb() -> InlineKeyboardMarkup:
    """4 inline-кнопки для выбора типа ЭДО-запроса."""
    b = InlineKeyboardBuilder()
    b.button(text="1. Подписать по ЭДО (счет №_)", callback_data="edo:sign_invoice")
    b.button(text="2. Закрывающие по ЭДО (счет №_)", callback_data="edo:sign_closing")
    b.button(text="3. Подписать по ЭДО УПД поставщика", callback_data="edo:sign_upd")
    b.button(text="4. Другое: пояснить суть", callback_data="edo:other")
    b.adjust(1)
    return b.as_markup()


def zamery_source_kb() -> InlineKeyboardMarkup:
    """3 inline-кнопки выбора источника заявки на замер."""
    b = InlineKeyboardBuilder()
    b.button(text="🎯 Привязать к лиду", callback_data="zam_src:lead")
    b.button(text="👤 Свой клиент", callback_data="zam_src:own_client")
    b.button(text="🔄 Повторный", callback_data="zam_src:repeat")
    b.adjust(1)
    return b.as_markup()


def zamery_lead_pick_kb(leads: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    """Inline-пикер лидов от РП для привязки к замеру."""
    b = InlineKeyboardBuilder()
    for lead_task in leads:
        payload = try_json_loads(lead_task.get("payload_json"))
        desc = (payload.get("description") or "")[:35]
        label = f"🎯 #{lead_task['id']}"
        if desc:
            label += f" — {desc}"
        b.button(text=label[:55], callback_data=f"zam_lead:{lead_task['id']}")
    b.adjust(1)
    return b.as_markup()


def zamery_my_requests_kb(requests: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    """Менеджер: список своих заявок на замер."""
    b = InlineKeyboardBuilder()
    for req in requests:
        status_emoji = {
            "open": "⏳", "in_progress": "🔄", "done": "✅", "rejected": "❌",
        }.get(req.get("status", ""), "❓")
        addr = (req.get("address") or "")[:30]
        b.button(
            text=f"{status_emoji} #{req['id']} — {addr}"[:55],
            callback_data=f"zam_req:view:{req['id']}",
        )
    b.adjust(1)
    return b.as_markup()


def zamery_incoming_kb(requests: list[dict[str, Any]], *, back_callback: str | None = None) -> InlineKeyboardMarkup:
    """Замерщик: входящие заявки на замер."""
    b = InlineKeyboardBuilder()
    for req in requests:
        addr = (req.get("address") or "")[:25]
        role_short = _ROLE_SHORT.get(req.get("requester_role", ""), "")
        b.button(
            text=f"📐 #{req['id']} ({role_short}) — {addr}"[:55],
            callback_data=f"zam_in:view:{req['id']}",
        )
    if back_callback:
        b.button(text="⬅️ Назад", callback_data=back_callback)
    b.adjust(1)
    return b.as_markup()


def edo_invoice_pick_kb(invoices: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    """Inline-пикер счетов для ЭДО-запроса."""
    b = InlineKeyboardBuilder()
    for inv in invoices[:15]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        status_emoji = {
            "pending": "⏳", "in_progress": "🔄", "paid": "✅", "closing": "📌",
        }.get(inv.get("status", ""), "❓")
        label = f"{status_emoji} №{num}"
        if addr:
            label += f" — {addr}"
        b.button(text=label[:55], callback_data=f"edo_inv:{inv['id']}")
    b.button(text="✍️ Ввести номер вручную", callback_data="edo_inv:manual")
    b.adjust(1)
    return b.as_markup()


def invoice_list_kb(invoices: list[dict], action_prefix: str = "inv", *, back_callback: str | None = None, hide_amount: bool = False) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком счетов."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        status_emoji = {
            "new": "🆕", "pending": "⏳", "in_progress": "🔄",
            "paid": "✅", "on_hold": "⏸", "rejected": "❌",
            "closing": "📌", "ended": "🏁", "credit": "🏦",
        }.get(inv.get("status", ""), "❓")
        if hide_amount:
            text = f"{status_emoji} №{inv.get('invoice_number', '?')}"
        else:
            text = f"{status_emoji} №{inv.get('invoice_number', '?')} — {(inv.get('amount') or 0):.0f}₽"
        b.button(text=text[:60], callback_data=f"{action_prefix}:view:{inv['id']}")
    if back_callback:
        b.button(text="⬅️ Назад", callback_data=back_callback)
    b.adjust(1)
    return b.as_markup()


def invoice_end_conditions_kb(invoice_id: int, conditions: dict) -> InlineKeyboardMarkup:
    """Inline-кнопки состояния 4 условий для Счет End."""
    b = InlineKeyboardBuilder()
    c1 = "✅" if conditions.get("installer_ok") else "⏳"
    c2 = "✅" if conditions.get("edo_signed") else "⏳"
    c3 = "✅" if conditions.get("no_debts") else "⏳"
    c4 = "☐"
    b.button(text=f"{c1} 1. Монтажник — Счет ОК", callback_data=f"invend:cond1:{invoice_id}")
    b.button(text=f"{c2} 2. Бухгалтерия — Закр.ЭДО ок", callback_data=f"invend:cond2:{invoice_id}")
    b.button(text=f"{c3} 3. Долгов нет", callback_data=f"invend:cond3:{invoice_id}")
    b.button(text=f"{c4} 4. Пояснения (опционально)", callback_data=f"invend:comment:{invoice_id}")
    b.adjust(1)
    return b.as_markup()


def lead_pick_manager_kb() -> InlineKeyboardMarkup:
    """Inline-кнопки выбора менеджера для 'Лид в проект'."""
    b = InlineKeyboardBuilder()
    b.button(text="👤 КВ", callback_data="lead_mgr:manager_kv")
    b.button(text="👤 КИА", callback_data="lead_mgr:manager_kia")
    b.button(text="👤 НПН", callback_data="lead_mgr:manager_npn")
    b.adjust(1)
    return b.as_markup()


def kp_task_list_kb(
    tasks: list[dict[str, Any]],
    show_issued: bool = True,
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком входящих CHECK_KP задач для РП.

    Каждая задача показывает: №счёта — сумма (Менеджер).
    """
    b = InlineKeyboardBuilder()
    for t in tasks:
        payload = try_json_loads(t.get("payload_json"))
        inv_num = payload.get("invoice_number", "?")
        amount = payload.get("amount", 0)
        manager_role = payload.get("manager_role", "")
        mgr_label = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }.get(manager_role, "Менеджер")
        try:
            amount_str = f"{float(amount):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{amount}₽"
        text = f"📋 №{inv_num} — {amount_str} ({mgr_label})"
        b.button(text=text[:60], callback_data=f"kp_resp:view:{t['id']}")
    if show_issued:
        b.button(text="📑 Выставленные счета", callback_data="kp_resp:issued")
    b.adjust(1)
    return b.as_markup()


def kp_response_kb(task_id: int) -> InlineKeyboardMarkup:
    """✅ Да / ❌ Нет — inline-кнопки ответа РП на CHECK_KP."""
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data=f"kp_resp:yes:{task_id}")
    b.button(text="❌ Нет", callback_data=f"kp_resp:no:{task_id}")
    b.button(text="⬅️ Назад к списку", callback_data="kp_resp:back")
    b.adjust(2, 1)
    return b.as_markup()


def kp_payment_type_kb(task_id: int) -> InlineKeyboardMarkup:
    """Выбор системы оплаты: б/н (безналичный) или Кред (кредит)."""
    b = InlineKeyboardBuilder()
    b.button(text="💳 б/н (безналичный)", callback_data=f"kp_resp:bn:{task_id}")
    b.button(text="🏦 Кред (кредит)", callback_data=f"kp_resp:cred:{task_id}")
    b.button(text="⬅️ Назад", callback_data=f"kp_resp:view:{task_id}")
    b.adjust(1)
    return b.as_markup()


def kp_issued_list_kb(
    invoices: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки «Выставленные счета» для РП."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        is_credit = inv.get("is_credit") or inv.get("status") == "credit"
        status_emoji = {
            "new": "🆕", "pending": "⏳", "in_progress": "🔄",
            "paid": "✅", "on_hold": "⏸", "rejected": "❌",
            "closing": "📌", "ended": "🏁", "credit": "🏦",
        }.get(inv.get("status", ""), "❓")
        credit_mark = " 🏦" if is_credit and inv.get("status") != "credit" else ""
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        text = f"{status_emoji} №{inv.get('invoice_number', '?')} — {amount_str}{credit_mark}"
        b.button(text=text[:60], callback_data=f"kp_issued:view:{inv['id']}")
    b.button(text="⬅️ Назад", callback_data="kp_resp:back")
    b.adjust(1)
    return b.as_markup()


def invoices_work_list_kb(
    invoices: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки «Счета в Работе» с двойными индикаторами 💰/📄.

    💰 = статус оплаты: ⏳ ждёт подтверждения / 🔄 в работе / ✅ оплачен
    📄 = статус документов (ЭДО): ⏳ не подписано / ✅ подписано
    """
    b = InlineKeyboardBuilder()
    for inv in invoices:
        # 💰 payment status indicator
        pay_emoji = {
            "pending": "⏳", "in_progress": "🔄", "paid": "✅",
        }.get(inv.get("status", ""), "❓")

        # 📄 document signing (EDO) indicator
        doc_emoji = "✅" if inv.get("edo_signed") else "⏳"

        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"

        text = f"💰{pay_emoji} 📄{doc_emoji} №{inv.get('invoice_number', '?')} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_work:view:{inv['id']}")
    b.button(text="➕ Добавить из закрытых", callback_data="rp_work:add_ended")
    b.button(text="🔄 Обновить", callback_data="rp_work:refresh")
    b.adjust(1)
    return b.as_markup()


def invoice_select_kb(
    invoices: list[dict[str, Any]],
    prefix: str = "selinv",
    allow_skip: bool = True,
    *,
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    """Inline-пикер счетов «в работе» для привязки задач/сообщений к счёту."""
    b = InlineKeyboardBuilder()
    for inv in invoices[:15]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:28]
        label = f"№{num}"
        if addr:
            label += f" — {addr}"
        b.button(text=label, callback_data=f"{prefix}:{inv['id']}")
    if allow_skip:
        b.button(text="➡️ Без привязки к счёту", callback_data=f"{prefix}:skip")
    if back_callback:
        b.button(text="⬅️ Назад", callback_data=back_callback)
    b.adjust(1)
    return b.as_markup()


def material_type_kb(prefix: str = "mattype") -> InlineKeyboardMarkup:
    """Inline-пикер типа материала/услуги."""
    from .enums import MATERIAL_TYPE_LABELS
    b = InlineKeyboardBuilder()
    for code, label in MATERIAL_TYPE_LABELS.items():
        b.button(text=label, callback_data=f"{prefix}:{code}")
    b.adjust(2)
    return b.as_markup()


def urgency_kb(prefix: str = "inv_urgency") -> InlineKeyboardMarkup:
    """Inline-пикер срочности оплаты: 1ч / 7ч / 24ч."""
    b = InlineKeyboardBuilder()
    b.button(text="⚡ В течение 1 часа", callback_data=f"{prefix}:1h")
    b.button(text="🕐 В течение 7 часов", callback_data=f"{prefix}:7h")
    b.button(text="📅 В течение 24 часов", callback_data=f"{prefix}:24h")
    b.adjust(1)
    return b.as_markup()


def finish_kb(action_cb_data: str, cancel_cb_data: str | None = None, finish_text: str = "✅ Создать") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=finish_text, callback_data=action_cb_data)
    if cancel_cb_data:
        b.button(text="❌ Отмена", callback_data=cancel_cb_data)
    b.adjust(1)
    return b.as_markup()
