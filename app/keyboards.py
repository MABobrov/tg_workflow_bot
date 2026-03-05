from __future__ import annotations

from typing import Any, Iterable

from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from .callbacks import ManagerProjectCb, ProjectCb, TaskCb
from .enums import MANAGER_ROLES, Role, TaskStatus, TaskType
from .utils import parse_roles, task_status_label, task_type_label


BACK_TO_HOME = "⬅️ Назад в главное меню"
OPEN_ACTIONS = "📂 Ещё действия"
OPEN_HELP = "📚 Справка"
OPEN_ADMIN_PANEL = "🛠 Админ-панель"
ADMIN_HELP_BUTTON = "🛠 Админ-инструкция"
ADMIN_EMPLOYEES_BUTTON = "👥 Сотрудники"
ADMIN_STATS_BUTTON = "📊 Статистика бота"
ADMIN_WORKCHAT_BUTTON = "💬 Рабочий чат"
ADMIN_WORKCHAT_TEST_BUTTON = "🧪 Тест рабочего чата"
ADMIN_RESYNC_BUTTON = "🔄 Синхронизация Sheets"

# --- GD-specific buttons ---
GD_BTN_INVOICES = "Счета на Оплату"
GD_BTN_URGENT = "Срочно для ГД"
GD_BTN_SEARCH_INVOICE = "Поиск Счета"
GD_BTN_CHAT_RP = "Чат с РП (НПН)"
GD_BTN_ZAMERY = "Замеры"
GD_BTN_ACCOUNTING = "Бухгалтерия"
GD_BTN_MONTAZH = "Монтажная гр."
GD_BTN_SALES = "Отд.Продаж"
GD_BTN_SYNC = "Синхронизация данных"
GD_BTN_MORE = "Еще"
GD_BTN_CANCEL = "❌ Отмена"
GD_BTN_ADMIN = "🛠 Админ-панель"
GD_BTN_KV_CRED = "КВ Кред"
GD_BTN_KIA_CRED = "КИА Кред"
GD_BTN_NPN_CRED = "НПН Кред"
GD_BTN_PAYMENT_CONFIRM = "✅ Подтверждение оплат"
GD_BTN_SUPPLIER_PAY = "💸 Оплата поставщику"
GD_BTN_BACK_HOME = "Назад в Гл.меню"
GD_BTN_REFRESH = "🔄 Обновить меню"
GD_BTN_HELP = "📚 Справка"

# --- Manager-specific buttons (КВ / КИА / НПН) ---
MGR_BTN_INBOX = "📥 Входящие задачи"
MGR_BTN_NOT_URGENT = "📩 Не срочно ГД"
MGR_BTN_INVOICE_START = "💼 Счет в Работу"
MGR_BTN_INVOICE_END = "🏁 Счет End"
MGR_BTN_ZAMERY = "📐 Замеры"
MGR_BTN_EDO = "📄 Бухгалтерия (ЭДО)"
MGR_BTN_CHECK_KP = "📋 Проверить КП/Счет"
MGR_BTN_SYNC = "🔄 Синхронизация данных"
MGR_BTN_MORE = "Еще"
MGR_BTN_CANCEL = "❌ Отмена"
# --- Manager "Еще" submenu ---
MGR_BTN_CRED = "💬 Менеджер (кред)"
MGR_BTN_URGENT = "🚨 Срочно ГД"
MGR_BTN_MY_INVOICES = "📑 Мои Счета"
MGR_BTN_ISSUE = "🆘 Проблема/Вопрос"
MGR_BTN_SEARCH_INVOICE = "🔍 Поиск Счета"
MGR_BTN_HELP = "📚 Справка"
MGR_BTN_BACK_HOME = "Назад в Гл.меню"

# --- RP-specific buttons ---
RP_BTN_INBOX_SALES = "📥 Входящие Отд.Продаж"
RP_BTN_NOT_URGENT = "📩 Не срочно ГД"
RP_BTN_INVOICE_START = "💼 Счета в Работу"
RP_BTN_INVOICE_END = "🏁 Счет End"
RP_BTN_INVOICES_PAY = "💳 Счета на оплату"
RP_BTN_ISSUE = "🆘 Проблема/Вопрос"
RP_BTN_MGR_KV = "👤 Менеджер 1 (КВ)"
RP_BTN_MGR_KIA = "👤 Менеджер 2 (КИА)"
RP_BTN_MONTAZH = "🔧 Монтажная гр."
RP_BTN_EDO = "📄 Бухгалтерия (ЭДО)"
RP_BTN_MORE = "Еще"
RP_BTN_CANCEL = "❌ Отмена"
# --- RP "Еще" submenu ---
RP_BTN_LEAD = "🎯 Лид в проект"
RP_BTN_URGENT = "🚨 Срочно ГД"
RP_BTN_ROLE_SWITCH = "🔄 Смена роли"
# --- RP/NPN role-switching buttons (top row of main menu) ---
RP_BTN_ROLE_RP = "✅ РП"
RP_BTN_ROLE_NPN = "👤 Менеджер НПН"
RP_BTN_ROLE_RP_INACTIVE = "📋 РП"
RP_BTN_ROLE_NPN_ACTIVE = "✅ Менеджер НПН"
RP_BTN_SEARCH_INVOICE = "🔍 Поиск Счета"
RP_BTN_SYNC = "🔄 Синхронизация данных"
RP_BTN_HELP = "📚 Справка"
RP_BTN_BACK_HOME = "Назад в Гл.меню"

# --- Accounting buttons ---
ACC_BTN_INBOX = "📥 Входящие задачи"
ACC_BTN_NOT_URGENT = "📩 Не срочно ГД"
ACC_BTN_SEARCH = "🔍 Найти Счет №"
ACC_BTN_INVOICE_END = "🏁 Закрытые Счета"
ACC_BTN_SYNC = "🔄 Синхронизация данных"
ACC_BTN_URGENT = "🚨 Срочно ГД"

# --- Installer buttons ---
INST_BTN_ORDER_MAT = "📦 Заказ материалов"
INST_BTN_INVOICE_OK = "✅ Счет ок"
INST_BTN_ORDER_EXTRA = "📦 Заказ доп.материалов"
INST_BTN_MY_OBJECTS = "📌 Мои объекты"
INST_BTN_DAILY_REPORT = "📝 Отчёт за день"
INST_BTN_IN_WORK = "🔨 В Работу"
INST_BTN_NOT_URGENT = "📩 Не срочно ГД"
INST_BTN_URGENT = "🚨 Срочно ГД"
INST_BTN_SYNC = "🔄 Синхронизация данных"

# --- Zamery buttons ---
ZAM_BTN_ZAMERY = "📋 Заявка на замер"
ZAM_BTN_MY_OBJECTS = "📋 Мои замеры"
ZAM_BTN_URGENT = "🚨 Срочно ГД"
ZAM_BTN_NOT_URGENT = "📩 Не срочно ГД"
ZAM_BTN_SYNC = "🔄 Синхронизация данных"


def _build_reply_rows(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for row in rows:
        for label in row:
            kb.button(text=label)
    kb.adjust(*[len(row) for row in rows])
    return kb.as_markup(resize_keyboard=True)


def _role_primary_action_rows(role: str | None) -> list[list[str]]:
    # --- 3 менеджера (КВ / КИА / НПН) — единое меню ---
    if role in MANAGER_ROLES:
        return [
            [MGR_BTN_INBOX, MGR_BTN_NOT_URGENT],
            [MGR_BTN_INVOICE_START, MGR_BTN_INVOICE_END],
            [MGR_BTN_ZAMERY, MGR_BTN_EDO],
            [MGR_BTN_CHECK_KP, MGR_BTN_SYNC],
            [MGR_BTN_MORE, MGR_BTN_CANCEL],
        ]
    # --- legacy MANAGER (обратная совместимость) ---
    if role == Role.MANAGER:
        return [
            [MGR_BTN_INBOX, MGR_BTN_NOT_URGENT],
            [MGR_BTN_INVOICE_START, MGR_BTN_INVOICE_END],
            [MGR_BTN_ZAMERY, MGR_BTN_EDO],
            [MGR_BTN_CHECK_KP, MGR_BTN_SYNC],
            [MGR_BTN_MORE, MGR_BTN_CANCEL],
        ]
    if role == Role.RP:
        return [
            [RP_BTN_INBOX_SALES, RP_BTN_NOT_URGENT],
            [RP_BTN_INVOICE_START, RP_BTN_INVOICE_END],
            [RP_BTN_INVOICES_PAY, RP_BTN_ISSUE],
            [RP_BTN_MGR_KV, RP_BTN_MGR_KIA],
            [RP_BTN_MONTAZH, RP_BTN_EDO],
            [RP_BTN_SYNC, RP_BTN_MORE],
            [RP_BTN_CANCEL],
        ]
    if role == Role.TD:
        # TD merged into GD — redirect to GD menu
        return [
            [GD_BTN_INVOICES, GD_BTN_URGENT],
            [GD_BTN_PAYMENT_CONFIRM, GD_BTN_SUPPLIER_PAY],
            [GD_BTN_SEARCH_INVOICE, GD_BTN_CHAT_RP],
            [GD_BTN_ZAMERY, GD_BTN_ACCOUNTING],
            [GD_BTN_MONTAZH, GD_BTN_SALES],
            [GD_BTN_SYNC, GD_BTN_MORE],
        ]
    if role == Role.ACCOUNTING:
        return [
            [ACC_BTN_INBOX, ACC_BTN_NOT_URGENT],
            [ACC_BTN_SEARCH, ACC_BTN_INVOICE_END],
            [ACC_BTN_SYNC, ACC_BTN_URGENT],
        ]
    if role == Role.INSTALLER:
        return [
            [INST_BTN_ORDER_MAT, INST_BTN_INVOICE_OK],
            [INST_BTN_ORDER_EXTRA, INST_BTN_MY_OBJECTS],
            [INST_BTN_DAILY_REPORT, INST_BTN_IN_WORK],
            [INST_BTN_NOT_URGENT, INST_BTN_URGENT],
            [INST_BTN_SYNC],
        ]
    if role == Role.GD:
        return [
            [GD_BTN_INVOICES, GD_BTN_URGENT],
            [GD_BTN_PAYMENT_CONFIRM, GD_BTN_SUPPLIER_PAY],
            [GD_BTN_SEARCH_INVOICE, GD_BTN_CHAT_RP],
            [GD_BTN_ZAMERY, GD_BTN_ACCOUNTING],
            [GD_BTN_MONTAZH, GD_BTN_SALES],
            [GD_BTN_SYNC, GD_BTN_MORE],
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
            [ZAM_BTN_URGENT, ZAM_BTN_NOT_URGENT],
            [ZAM_BTN_SYNC],
        ]
    return []


def _role_secondary_action_rows(role: str | None) -> list[list[str]]:
    # --- 3 менеджера (КВ / КИА / НПН) — подменю "Еще" ---
    if role in MANAGER_ROLES or role == Role.MANAGER:
        return [
            [MGR_BTN_CRED, MGR_BTN_URGENT],
            [MGR_BTN_MY_INVOICES, MGR_BTN_ISSUE],
            [MGR_BTN_SEARCH_INVOICE, MGR_BTN_HELP],
            [MGR_BTN_CANCEL, MGR_BTN_BACK_HOME],
        ]
    if role == Role.RP:
        return [
            [RP_BTN_LEAD, RP_BTN_URGENT],
            [RP_BTN_SEARCH_INVOICE, RP_BTN_SYNC],
            [RP_BTN_HELP],
            [RP_BTN_CANCEL, RP_BTN_BACK_HOME],
        ]
    if role == Role.TD:
        # TD merged into GD — use GD submenu
        return [
            [GD_BTN_KV_CRED, GD_BTN_KIA_CRED],
        ]
    if role == Role.ACCOUNTING:
        return []  # Accounting has no submenu
    if role == Role.INSTALLER:
        return []  # Installer has no submenu (all buttons in primary)
    if role == Role.GD:
        return [
            [GD_BTN_KV_CRED, GD_BTN_KIA_CRED],
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


def main_menu(role: str | None, is_admin: bool = False) -> ReplyKeyboardMarkup:
    # GD gets a custom layout (no separate "Ещё действия" row, admin in grid)
    if _is_pure_gd(role):
        rows: list[list[str]] = [list(r) for r in _role_primary_action_rows(Role.GD)]
        last_row = [GD_BTN_CANCEL]
        if is_admin:
            last_row.append(GD_BTN_ADMIN)
        rows.append(last_row)
        return _build_reply_rows(rows)

    # MANAGER_NPN (switched from RP) — role-switching row + NPN manager menu
    # Must be checked BEFORE generic _is_pure_manager to add role-switch buttons
    if _is_pure_manager_npn(role):
        rows = [[RP_BTN_ROLE_RP_INACTIVE, RP_BTN_ROLE_NPN_ACTIVE]]
        rows.extend([list(row) for row in _role_primary_action_rows(Role.MANAGER_NPN)])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # Manager (КВ/КИА) — custom layout with built-in "Еще" button
    if _is_pure_manager(role):
        r = parse_roles(role)[0]
        rows = [list(row) for row in _role_primary_action_rows(r)]
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # RP — custom layout with role-switching row + built-in "Еще" button
    if _is_pure_rp(role):
        rows: list[list[str]] = [[RP_BTN_ROLE_RP, RP_BTN_ROLE_NPN]]
        rows.extend([list(row) for row in _role_primary_action_rows(Role.RP)])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # Accounting — compact layout, no submenu
    if _is_pure_accounting(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.ACCOUNTING)]
        rows.append(["❌ Отмена", OPEN_HELP])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # Installer — compact layout, no submenu
    if _is_pure_installer(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.INSTALLER)]
        rows.append(["❌ Отмена", OPEN_HELP])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # Zamery — compact layout, no submenu
    if _is_pure_zamery(role):
        rows = [list(row) for row in _role_primary_action_rows(Role.ZAMERY)]
        rows.append(["❌ Отмена", OPEN_HELP])
        if is_admin:
            rows.append([OPEN_ADMIN_PANEL])
        return _build_reply_rows(rows)

    # Generic: combined roles or old roles
    rows = []
    if role:
        rows.extend(_merge_rows_for_roles(role, _role_primary_action_rows))
    secondary_rows = _merge_rows_for_roles(role, _role_secondary_action_rows)
    if secondary_rows:
        rows.append([OPEN_ACTIONS])
    rows.append([OPEN_HELP, "🔄 Обновить меню"])
    if is_admin:
        rows.append([OPEN_ADMIN_PANEL])
    rows.append(["❌ Отмена"])
    return _build_reply_rows(rows)


def actions_menu(role: str | None, is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = _merge_rows_for_roles(role, _role_secondary_action_rows)
    if is_admin:
        rows.append([OPEN_ADMIN_PANEL])
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


def tasks_kb(tasks: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in tasks:
        text = f"#{t['id']} • {task_type_label(t.get('type'))} • {task_status_label(t.get('status'))}"
        b.button(text=text[:60], callback_data=TaskCb(task_id=int(t["id"]), action="open").pack())
    b.adjust(1)
    return b.as_markup()


def task_actions_kb(task: dict[str, Any]) -> InlineKeyboardMarkup:
    ttype = task.get("type")
    status = task.get("status")
    b = InlineKeyboardBuilder()

    # universal
    if status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b.button(text="✅ Завершить", callback_data=TaskCb(task_id=int(task["id"]), action="done").pack())
        b.button(text="⏳ Взять в работу", callback_data=TaskCb(task_id=int(task["id"]), action="take").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=int(task["id"]), action="reject").pack())

    # payment confirm special actions (TD)
    if ttype == TaskType.PAYMENT_CONFIRM and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Оплата подтверждена", callback_data=TaskCb(task_id=int(task["id"]), action="pay_ok").pack())
        b.button(text="⚠️ Нужна доплата", callback_data=TaskCb(task_id=int(task["id"]), action="pay_need").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=int(task["id"]), action="reject").pack())

    # Заказ материалов — кнопки для ТД (оплатить/отклонить)
    if ttype in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS} and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="💸 Оплатить поставщику", callback_data=TaskCb(task_id=int(task["id"]), action="pay_supplier").pack())
        b.button(text="✅ Завершить", callback_data=TaskCb(task_id=int(task["id"]), action="done").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=int(task["id"]), action="reject").pack())

    # Счёт на оплату — кнопки для ГД
    if ttype == TaskType.INVOICE_PAYMENT and status in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Оплатить", callback_data=TaskCb(task_id=int(task["id"]), action="inv_pay").pack())
        b.button(text="⏸ Отложить", callback_data=TaskCb(task_id=int(task["id"]), action="inv_hold").pack())
        b.button(text="❌ Отклонить", callback_data=TaskCb(task_id=int(task["id"]), action="inv_reject").pack())

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



def gd_more_menu() -> ReplyKeyboardMarkup:
    """Подменю 'Еще' для ГД."""
    rows = [
        [GD_BTN_KV_CRED, GD_BTN_KIA_CRED],
        [GD_BTN_NPN_CRED],
        [GD_BTN_BACK_HOME, GD_BTN_REFRESH],
        [GD_BTN_CANCEL, GD_BTN_HELP],
    ]
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


def manager_more_menu() -> ReplyKeyboardMarkup:
    """Подменю 'Еще' для менеджеров (КВ / КИА / НПН)."""
    rows = [
        [MGR_BTN_CRED, MGR_BTN_URGENT],
        [MGR_BTN_MY_INVOICES, MGR_BTN_ISSUE],
        [MGR_BTN_SEARCH_INVOICE, MGR_BTN_HELP],
        [MGR_BTN_CANCEL, MGR_BTN_BACK_HOME],
    ]
    return _build_reply_rows(rows)


def rp_more_menu() -> ReplyKeyboardMarkup:
    """Подменю 'Еще' для РП."""
    rows = [
        [RP_BTN_LEAD, RP_BTN_URGENT],
        [RP_BTN_SEARCH_INVOICE, RP_BTN_SYNC],
        [RP_BTN_HELP],
        [RP_BTN_CANCEL, RP_BTN_BACK_HOME],
    ]
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
    """Подменю чат-прокси для РП ↔ Менеджер."""
    rows = [
        ["📖 Переписка", "✏️ Написать"],
        ["📋 Задачи", back_label],
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


def invoice_list_kb(invoices: list[dict], action_prefix: str = "inv") -> InlineKeyboardMarkup:
    """Inline-кнопки со списком счетов."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        status_emoji = {
            "new": "🆕", "pending": "⏳", "in_progress": "🔄",
            "paid": "✅", "on_hold": "⏸", "rejected": "❌",
            "closing": "📌", "ended": "🏁",
        }.get(inv.get("status", ""), "❓")
        text = f"{status_emoji} №{inv.get('invoice_number', '?')} — {inv.get('amount', 0):.0f}₽"
        b.button(text=text[:60], callback_data=f"{action_prefix}:view:{inv['id']}")
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
    b.button(text="👤 КВ (конструкции ПВХ)", callback_data="lead_mgr:manager_kv")
    b.button(text="👤 КИА (комплектующие)", callback_data="lead_mgr:manager_kia")
    b.button(text="👤 НПН (непрофильная)", callback_data="lead_mgr:manager_npn")
    b.adjust(1)
    return b.as_markup()


def finish_kb(action_cb_data: str, cancel_cb_data: str | None = None, finish_text: str = "✅ Создать") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=finish_text, callback_data=action_cb_data)
    if cancel_cb_data:
        b.button(text="❌ Отмена", callback_data=cancel_cb_data)
    b.adjust(1)
    return b.as_markup()
