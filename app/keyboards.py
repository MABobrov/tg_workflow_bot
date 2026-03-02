from __future__ import annotations

from typing import Any, Iterable

from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from .callbacks import ManagerProjectCb, ProjectCb, TaskCb
from .enums import Role, TaskStatus, TaskType
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


def _build_reply_rows(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for row in rows:
        for label in row:
            kb.button(text=label)
    kb.adjust(*[len(row) for row in rows])
    return kb.as_markup(resize_keyboard=True)


def _role_primary_action_rows(role: str | None) -> list[list[str]]:
    if role == Role.MANAGER:
        return [
            ["Проверить КП/Запросить документы"],
            ["💰 Оплата поступила", "📥 Входящие задачи"],
        ]
    if role == Role.RP:
        return [
            ["📥 Входящие задачи", "🗂 Проекты"],
            ["📦 Заказ материалов", "🚚 Заявка на доставку"],
            ["📌 Поиск проекта"],
        ]
    if role == Role.TD:
        return [
            ["✅ Подтверждение оплат", "💸 Оплата поставщику"],
            ["📥 Входящие задачи"],
        ]
    if role == Role.ACCOUNTING:
        return [
            ["📄 Закрывающие", "📨 Менеджеру (Имя)"],
            ["📥 Входящие задачи"],
        ]
    if role == Role.INSTALLER:
        return [
            ["📝 Отчёт за день", "✅ Счёт ОК"],
            ["🆘 Проблема / простой"],
        ]
    if role == Role.GD:
        return [
            ["📥 Входящие задачи", "🚨 Срочно ГД"],
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
    return []


def _role_secondary_action_rows(role: str | None) -> list[list[str]]:
    if role == Role.MANAGER:
        return [
            ["📄 Док. / ЭДО", "🏁 Счёт End"],
            ["🆘 Проблема / вопрос", "🚨 Срочно ГД"],
            ["📌 Мои проекты", "📌 Поиск проекта"],
        ]
    if role == Role.RP:
        return [
            ["🎯 Распределить лид", "🎨 Заявка на тонировку"],
            ["🆘 Проблема / простой", "🚨 Срочно ГД"],
        ]
    if role == Role.TD:
        return [
            ["📌 Поиск проекта", "🚨 Срочно ГД"],
        ]
    if role == Role.ACCOUNTING:
        return [
            ["📌 Поиск проекта", "🚨 Срочно ГД"],
        ]
    if role == Role.INSTALLER:
        return [
            ["📌 Мои объекты", "📌 Поиск проекта"],
            ["🚨 Срочно ГД"],
        ]
    if role == Role.GD:
        return [
            ["📌 Поиск проекта"],
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


def main_menu(role: str | None, is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
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


def finish_kb(action_cb_data: str, cancel_cb_data: str | None = None, finish_text: str = "✅ Создать") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=finish_text, callback_data=action_cb_data)
    if cancel_cb_data:
        b.button(text="❌ Отмена", callback_data=cancel_cb_data)
    b.adjust(1)
    return b.as_markup()
