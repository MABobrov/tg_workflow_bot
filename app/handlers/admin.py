from __future__ import annotations

import logging
from datetime import timedelta

from aiogram import Router, F, html
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import AdminRoleCb, AdminUserCb, AdminUsersListCb, TaskCb
from ..config import Config
from ..db import Database
from ..enums import Role
from ..keyboards import (
    ADMIN_EMPLOYEES_BUTTON,
    ADMIN_HELP_BUTTON,
    ADMIN_RESYNC_BUTTON,
    ADMIN_STATS_BUTTON,
    ADMIN_WORKCHAT_BUTTON,
    ADMIN_WORKCHAT_TEST_BUTTON,
    OPEN_ADMIN_PANEL,
    admin_panel_menu,
    main_menu,
)
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..utils import (
    parse_roles,
    private_only_reply_markup,
    role_label,
    roles_to_storage,
    task_status_label,
    task_type_label,
    to_iso,
    utcnow,
)

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")
ADMIN_USERS_PAGE_SIZE = 12


def _is_admin(user_id: int, config: Config) -> bool:
    return user_id in (config.admin_ids or set())


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


async def _answer_chunked(message: Message, lines: list[str], chunk_limit: int = 3800) -> None:
    if not lines:
        return
    chunk: list[str] = []
    cur_len = 0
    for line in lines:
        line_len = len(line) + 1
        if chunk and cur_len + line_len > chunk_limit:
            await message.answer("\n".join(chunk))
            chunk = [line]
            cur_len = line_len
        else:
            chunk.append(line)
            cur_len += line_len
    if chunk:
        await message.answer("\n".join(chunk))


async def _resolve_user_id_by_ref(db: Database, ref: str) -> tuple[int | None, str | None]:
    raw = (ref or "").strip()
    if not raw:
        return None, None

    if raw.isdigit():
        user_id = int(raw)
        user = await db.get_user_optional(user_id)
        label = f"@{user.username}" if user and user.username else str(user_id)
        return user_id, label

    user = await db.find_user_by_username(_normalize_username(raw))
    if not user:
        return None, None
    label = f"@{user.username}" if user.username else str(user.telegram_id)
    return user.telegram_id, label


async def _push_menu_to_user(message: Message, user_id: int, role: str | None, is_admin: bool = False) -> bool:
    text = (
        f"✅ Вам назначена роль: <b>{role_label(role)}</b>\n"
        "Меню обновлено."
        if role
        else "ℹ️ Ваша роль снята. Доступно базовое меню."
    )
    try:
        await message.bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=main_menu(role, is_admin=is_admin),
        )
        return True
    except TelegramForbiddenError:
        return False
    except TelegramBadRequest:
        return False


def _employee_display(user_id: int, username: str | None, full_name: str | None) -> str:
    if full_name and username:
        return f"{html.quote(full_name)} (@{html.quote(username)})"
    if full_name:
        return html.quote(full_name)
    if username:
        return f"@{html.quote(username)}"
    return f"<code>{user_id}</code>"


async def _employee_card_text(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    if not user:
        return None
    counts = await db.task_counts_for_user(user.telegram_id)
    status_label = "✅ Активен" if user.is_active else "🚫 Заблокирован"
    display = _employee_display(user.telegram_id, user.username, user.full_name)
    lines = [
        "👤 <b>Карточка сотрудника</b>",
        f"Имя: {display}",
        f"ID: <code>{user.telegram_id}</code>",
        f"Роли: <b>{role_label(user.role)}</b>",
        f"Статус доступа: <b>{status_label}</b>",
        "",
        "<b>Задачи сотрудника</b>",
        f"• Новые: <b>{counts.get('open', 0)}</b>",
        f"• В работе: <b>{counts.get('in_progress', 0)}</b>",
        f"• Завершённые: <b>{counts.get('done', 0)}</b>",
        f"• Отклонённые: <b>{counts.get('rejected', 0)}</b>",
    ]
    return "\n".join(lines)


def _employee_actions_kb(user_id: int, is_active: bool) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить роль", callback_data=AdminUserCb(user_id=user_id, action="roles_add").pack())
    b.button(text="➖ Убрать роль", callback_data=AdminUserCb(user_id=user_id, action="roles_remove").pack())
    if is_active:
        b.button(text="🚫 Заблокировать", callback_data=AdminUserCb(user_id=user_id, action="block").pack())
    else:
        b.button(text="✅ Разблокировать", callback_data=AdminUserCb(user_id=user_id, action="unblock").pack())
    b.button(text="🧹 Снять все роли", callback_data=AdminRoleCb(user_id=user_id, action="set", role="none").pack())
    b.button(text="📥 Активные задачи", callback_data=AdminUserCb(user_id=user_id, action="tasks_active").pack())
    b.button(text="✅ Завершённые", callback_data=AdminUserCb(user_id=user_id, action="tasks_done").pack())
    b.button(text="❌ Отклонённые", callback_data=AdminUserCb(user_id=user_id, action="tasks_rejected").pack())
    b.button(text="🔁 Обновить карточку", callback_data=AdminUserCb(user_id=user_id, action="view").pack())
    b.button(text="👥 К списку сотрудников", callback_data=AdminUsersListCb(offset=0).pack())
    b.adjust(2, 2, 2, 2, 1)
    return b


def _employee_roles_kb(user_id: int, mode: str) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    roles = [
        (Role.MANAGER, "Менеджер"),
        (Role.RP, "РП"),
        (Role.ACCOUNTING, "Бухгалтерия"),
        (Role.INSTALLER, "Монтажник"),
        (Role.DRIVER, "Водитель"),
        (Role.LOADER, "Грузчик"),
        (Role.TINTER, "Тонировщик"),
        (Role.GD, "ГД"),
    ]
    prefix = "➕" if mode == "add" else ("➖" if mode == "remove" else "✅")
    for role, label in roles:
        b.button(text=f"{prefix} {label}", callback_data=AdminRoleCb(user_id=user_id, action=mode, role=role).pack())
    if mode == "set":
        b.button(text="🧹 Снять все роли", callback_data=AdminRoleCb(user_id=user_id, action="set", role="none").pack())
    b.button(text="⬅️ Назад к сотруднику", callback_data=AdminUserCb(user_id=user_id, action="view").pack())
    b.adjust(2, 2, 2, 2, 1, 1)
    return b


async def _users_page(db: Database, offset: int, page_size: int = ADMIN_USERS_PAGE_SIZE) -> tuple[list, int, int]:
    users = await db.list_users(limit=2000)
    total = len(users)
    if total <= page_size:
        return users, 0, total
    offset = max(0, min(offset, max(0, total - page_size)))
    return users[offset : offset + page_size], offset, total


def _users_page_kb(users: list, offset: int, total: int, page_size: int = ADMIN_USERS_PAGE_SIZE) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    for usr in users:
        marker = "✅" if usr.is_active else "🚫"
        name = f"@{usr.username}" if usr.username else (usr.full_name or str(usr.telegram_id))
        label = f"{marker} {name}"
        b.button(text=label[:60], callback_data=AdminUserCb(user_id=usr.telegram_id, action="view").pack())

    if total > page_size:
        prev_offset = max(0, offset - page_size)
        next_offset = min(offset + page_size, max(0, total - page_size))
        if offset > 0:
            b.button(text="⬅️ Назад", callback_data=AdminUsersListCb(offset=prev_offset).pack())
        if offset + page_size < total:
            b.button(text="Вперёд ➡️", callback_data=AdminUsersListCb(offset=next_offset).pack())
    b.adjust(1)
    return b


def _employee_tasks_kb(tasks: list[dict], user_id: int) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    for task in tasks:
        t_text = f"#{task['id']} • {task_type_label(task.get('type'))} • {task_status_label(task.get('status'))}"
        b.button(text=t_text[:60], callback_data=TaskCb(task_id=int(task["id"]), action="open").pack())
    b.button(text="⬅️ Назад к сотруднику", callback_data=AdminUserCb(user_id=user_id, action="view").pack())
    b.adjust(1)
    return b


async def _show_employee_card(cb: CallbackQuery, db: Database, user_id: int) -> None:
    user = await db.get_user_optional(user_id)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True)
        return
    text = await _employee_card_text(db, user_id)
    if not text:
        await cb.answer("Пользователь не найден", show_alert=True)
        return
    kb = _employee_actions_kb(user_id, bool(user.is_active)).as_markup()
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[arg-type]
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]


@router.message(Command("admin_help"))
async def cmd_admin_help(message: Message, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    text = (
        "<b>Инструкция администратора</b>\n\n"
        "<b>1) Первичная настройка</b>\n"
        "• Все сотрудники пишут боту <code>/start</code>.\n"
        "• Назначьте роли командой <code>/setrole @username role[,role2]</code>.\n"
        "• Доступные роли: manager, rp, td, accounting, installer, driver, loader, tinter, gd.\n"
        "• Снять роль: <code>/setrole @username none</code>.\n\n"
        "• Добавить роль к существующим: <code>/addrole @username role</code>\n"
        "• Убрать одну роль: <code>/removerole @username role</code>\n\n"
        "<b>2) Дефолтные исполнители</b>\n"
        "• Команда: <code>/setdefaults rp=@user td=@user acc=@user driver=@user tinter=@user gd=@user</code>.\n"
        "• Можно использовать username, а не только ID.\n\n"
        "<b>3) Рабочий чат</b>\n"
        "• Добавьте бота в чат — он отправит ID автоматически.\n"
        "• Привязка: <code>/setworkchat -100...</code>\n"
        "• Проверка: <code>/workchat</code>\n"
        "• Тест отправки: <code>/workchat_test</code>\n\n"
        "<b>4) Сотрудники и статистика</b>\n"
        "• <code>/users</code> или <code>/employees</code> — список заведённых сотрудников.\n"
        "• В списке сотрудников доступны inline-кнопки: роли, блокировка, задачи сотрудника.\n"
        "• <code>/stats</code> — статистика использования бота.\n\n"
        "<b>5) Интеграции</b>\n"
        "• <code>/resyncsheets</code> — полная пересинхронизация Google Sheets.\n"
    )
    await message.answer(text)


@router.message(F.text == OPEN_ADMIN_PANEL)
async def open_admin_panel(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return
    await message.answer(
        "Панель администратора:",
        reply_markup=private_only_reply_markup(message, admin_panel_menu()),
    )


@router.message(F.text == ADMIN_HELP_BUTTON)
async def menu_admin_help(message: Message, config: Config) -> None:
    await cmd_admin_help(message, config)


@router.message(Command("setrole"))
async def cmd_setrole(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer(
            "Использование: /setrole @username_or_telegram_id role[,role2,...]\n"
            "Примеры:\n"
            "• /setrole @ivan manager\n"
            "• /setrole @ivan manager,rp\n"
            "• /setrole @ivan none"
        )
        return

    ref = parts[1].strip()
    tg_id, label = await _resolve_user_id_by_ref(db, ref)
    if tg_id is None:
        await message.answer("Пользователь не найден. Убедитесь, что он писал боту /start, затем повторите команду.")
        return

    role_raw = ",".join(p.strip() for p in parts[2:]).lower()
    if role_raw == "none":
        await db.set_user_role(tg_id, None)
        pushed = await _push_menu_to_user(message, tg_id, None, is_admin=tg_id in (config.admin_ids or set()))
        note = "" if pushed else "\n⚠️ Не смог отправить меню пользователю (пусть напишет боту /start)."
        await message.answer(f"Роль пользователю {label or tg_id} очищена.{note}")
        return

    roles = parse_roles(role_raw)
    if not roles:
        await message.answer(
            "Неизвестная роль. Допустимо: manager,rp,td,accounting,installer,driver,loader,tinter,gd или none"
        )
        return

    # For numeric id we keep backward compatibility: create placeholder user row.
    user = await db.get_user_optional(tg_id)
    if not user and ref.isdigit():
        await db.upsert_user(tg_id, None, None)
    elif not user:
        await message.answer("Пользователь не найден в базе. Попросите его сначала написать боту /start.")
        return

    await db.set_user_roles(tg_id, roles)
    role_storage = roles_to_storage(set(roles))
    pushed = await _push_menu_to_user(message, tg_id, role_storage, is_admin=tg_id in (config.admin_ids or set()))
    note = "" if pushed else "\n⚠️ Не смог отправить меню пользователю (пусть напишет боту /start)."
    await message.answer(f"Роли пользователю {label or tg_id} установлены: <b>{role_label(role_storage)}</b>{note}")


@router.message(Command("addrole"))
async def cmd_addrole(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Использование: /addrole @username_or_telegram_id role[,role2,...]")
        return

    tg_id, label = await _resolve_user_id_by_ref(db, parts[1].strip())
    if tg_id is None:
        await message.answer("Пользователь не найден. Убедитесь, что он писал боту /start.")
        return

    add_roles = set(parse_roles(",".join(p.strip() for p in parts[2:]).lower()))
    if not add_roles:
        await message.answer("Не удалось разобрать роли. Пример: /addrole @ivan manager,rp")
        return

    user = await db.get_user_optional(tg_id)
    if not user and parts[1].strip().isdigit():
        await db.upsert_user(tg_id, None, None)
        user = await db.get_user_optional(tg_id)
    if not user:
        await message.answer("Пользователь не найден в базе. Попросите его сначала написать боту /start.")
        return
    current = set(parse_roles(user.role if user else None))
    merged = current | add_roles
    await db.set_user_roles(tg_id, merged)
    stored = roles_to_storage(merged)
    pushed = await _push_menu_to_user(message, tg_id, stored, is_admin=tg_id in (config.admin_ids or set()))
    note = "" if pushed else "\n⚠️ Не смог отправить меню пользователю."
    await message.answer(f"✅ Роли обновлены для {label or tg_id}: <b>{role_label(stored)}</b>{note}")


@router.message(Command("removerole"))
async def cmd_remove_role(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Использование: /removerole @username_or_telegram_id role[,role2,...]")
        return

    tg_id, label = await _resolve_user_id_by_ref(db, parts[1].strip())
    if tg_id is None:
        await message.answer("Пользователь не найден. Убедитесь, что он писал боту /start.")
        return

    remove_roles = set(parse_roles(",".join(p.strip() for p in parts[2:]).lower()))
    if not remove_roles:
        await message.answer("Не удалось разобрать роли. Пример: /removerole @ivan rp")
        return

    user = await db.get_user_optional(tg_id)
    if not user:
        await message.answer("Пользователь не найден в базе. Попросите его сначала написать боту /start.")
        return
    current = set(parse_roles(user.role if user else None))
    updated = current - remove_roles
    await db.set_user_roles(tg_id, updated)
    stored = roles_to_storage(updated)
    pushed = await _push_menu_to_user(message, tg_id, stored, is_admin=tg_id in (config.admin_ids or set()))
    note = "" if pushed else "\n⚠️ Не смог отправить меню пользователю."
    if stored:
        text = f"✅ Роли обновлены для {label or tg_id}: <b>{role_label(stored)}</b>{note}"
    else:
        text = f"✅ У пользователя {label or tg_id} больше нет ролей.{note}"
    await message.answer(text)

@router.message(Command("users"))
@router.message(Command("employees"))
async def cmd_users(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    parts = (message.text or "").split()
    limit = 1000
    if len(parts) > 1 and parts[1].isdigit():
        limit = max(1, min(int(parts[1]), 3000))

    users = await db.list_users(limit=limit)
    role_counts = await db.users_by_role()
    active_cnt = sum(1 for usr in users if usr.is_active)
    blocked_cnt = sum(1 for usr in users if not usr.is_active)

    lines = ["<b>Сотрудники в боте</b> (обзор)"]
    lines.append(f"Всего: <b>{len(users)}</b>")
    lines.append(f"Активные: <b>{active_cnt}</b>, заблокированные: <b>{blocked_cnt}</b>")
    lines.append("По ролям:")
    role_order = [
        Role.MANAGER,
        Role.RP,
        Role.ACCOUNTING,
        Role.INSTALLER,
        Role.DRIVER,
        Role.LOADER,
        Role.TINTER,
        Role.GD,
        "",
    ]
    for role in role_order:
        lines.append(f"• {role_label(role or None)}: <b>{role_counts.get(role, 0)}</b>")
    unknown_roles = [r for r in role_counts.keys() if r not in set(role_order)]
    for role in sorted(unknown_roles):
        lines.append(f"• {role_label(role)}: <b>{role_counts.get(role, 0)}</b>")
    await message.answer("\n".join(lines))

    page_users, offset, total = await _users_page(db, offset=0, page_size=ADMIN_USERS_PAGE_SIZE)
    if not page_users:
        await message.answer("Сотрудников пока нет.")
        return

    end_idx = min(offset + len(page_users), total)
    text = (
        "<b>Управление сотрудниками</b>\n"
        f"Показаны записи <b>{offset + 1}-{end_idx}</b> из <b>{total}</b>.\n"
        "Нажмите на сотрудника для управления ролями, блокировкой и задачами."
    )
    await message.answer(text, reply_markup=_users_page_kb(page_users, offset, total).as_markup())


@router.message(F.text == ADMIN_EMPLOYEES_BUTTON)
async def menu_users(message: Message, db: Database, config: Config) -> None:
    await cmd_users(message, db, config)


@router.callback_query(AdminUsersListCb.filter())
async def admin_users_page(cb: CallbackQuery, callback_data: AdminUsersListCb, db: Database, config: Config) -> None:
    u = cb.from_user
    if not u or not _is_admin(u.id, config):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer()

    page_users, offset, total = await _users_page(db, callback_data.offset, page_size=ADMIN_USERS_PAGE_SIZE)
    if not page_users:
        await cb.message.edit_text("Сотрудников пока нет.")  # type: ignore[arg-type]
        return

    end_idx = min(offset + len(page_users), total)
    text = (
        "<b>Управление сотрудниками</b>\n"
        f"Показаны записи <b>{offset + 1}-{end_idx}</b> из <b>{total}</b>.\n"
        "Нажмите на сотрудника для управления ролями, блокировкой и задачами."
    )
    kb = _users_page_kb(page_users, offset, total).as_markup()
    try:
        await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[arg-type]
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]


@router.callback_query(AdminUserCb.filter())
async def admin_user_action(cb: CallbackQuery, callback_data: AdminUserCb, db: Database, config: Config) -> None:
    u = cb.from_user
    if not u or not _is_admin(u.id, config):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer()

    user_id = int(callback_data.user_id)
    action = callback_data.action
    user = await db.get_user_optional(user_id)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True)
        return

    if action == "view":
        await _show_employee_card(cb, db, user_id)
        return

    if action == "roles_add":
        text = (
            "➕ <b>Добавление ролей</b>\n"
            f"Сотрудник: {_employee_display(user.telegram_id, user.username, user.full_name)}\n"
            f"Текущие роли: <b>{role_label(user.role)}</b>\n\n"
            "Выберите роль для добавления:"
        )
        kb = _employee_roles_kb(user_id, mode="add").as_markup()
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[arg-type]
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]
        return

    if action == "roles_remove":
        text = (
            "➖ <b>Снятие ролей</b>\n"
            f"Сотрудник: {_employee_display(user.telegram_id, user.username, user.full_name)}\n"
            f"Текущие роли: <b>{role_label(user.role)}</b>\n\n"
            "Выберите роль, которую нужно убрать:"
        )
        kb = _employee_roles_kb(user_id, mode="remove").as_markup()
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[arg-type]
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]
        return

    if action in {"block", "unblock"}:
        is_active = action == "unblock"
        await db.set_user_active(user_id, is_active=is_active)
        user = await db.get_user_optional(user_id)
        if not user:
            await cb.answer("Пользователь не найден", show_alert=True)
            return

        if is_active:
            pushed = await _push_menu_to_user(cb.message, user_id, user.role, is_admin=user_id in (config.admin_ids or set()))  # type: ignore[arg-type]
            if not pushed:
                await cb.message.answer("⚠️ Не смог отправить пользователю обновлённое меню.")  # type: ignore[arg-type]
        else:
            try:
                await cb.bot.send_message(
                    user_id,
                    "⛔️ Ваш доступ к боту заблокирован администратором.\nОбратитесь к администратору для разблокировки.",
                )
            except Exception:
                pass

        await _show_employee_card(cb, db, user_id)
        await cb.answer("Изменения сохранены")
        return

    if action in {"tasks_active", "tasks_done", "tasks_rejected"}:
        if action == "tasks_active":
            statuses = ("open", "in_progress")
            title = "📥 <b>Активные задачи сотрудника</b>"
        elif action == "tasks_done":
            statuses = ("done",)
            title = "✅ <b>Завершённые задачи сотрудника</b>"
        else:
            statuses = ("rejected",)
            title = "❌ <b>Отклонённые задачи сотрудника</b>"

        tasks = await db.list_tasks_for_user(user_id, statuses=statuses, limit=50)
        if not tasks:
            text = (
                f"{title}\n"
                f"Сотрудник: {_employee_display(user.telegram_id, user.username, user.full_name)}\n\n"
                "Задач в выбранном статусе нет."
            )
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅️ Назад к сотруднику", callback_data=AdminUserCb(user_id=user_id, action="view").pack())
            try:
                await cb.message.edit_text(text, reply_markup=kb.as_markup())  # type: ignore[arg-type]
            except TelegramBadRequest:
                await cb.message.answer(text, reply_markup=kb.as_markup())  # type: ignore[arg-type]
            return

        text = (
            f"{title}\n"
            f"Сотрудник: {_employee_display(user.telegram_id, user.username, user.full_name)}\n"
            f"Найдено задач: <b>{len(tasks)}</b>\n\n"
            "Откройте задачу, чтобы изменить её статус."
        )
        kb = _employee_tasks_kb(tasks, user_id).as_markup()
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[arg-type]
        except TelegramBadRequest:
            await cb.message.answer(text, reply_markup=kb)  # type: ignore[arg-type]
        return

    await cb.answer("Неизвестное действие", show_alert=True)


@router.callback_query(AdminRoleCb.filter())
async def admin_role_action(cb: CallbackQuery, callback_data: AdminRoleCb, db: Database, config: Config) -> None:
    u = cb.from_user
    if not u or not _is_admin(u.id, config):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer()

    user_id = int(callback_data.user_id)
    action = callback_data.action
    role = (callback_data.role or "").strip().lower()

    user = await db.get_user_optional(user_id)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True)
        return

    allowed_roles = {
        Role.MANAGER.value,
        Role.RP.value,
        Role.ACCOUNTING.value,
        Role.INSTALLER.value,
        Role.DRIVER.value,
        Role.LOADER.value,
        Role.TINTER.value,
        Role.GD.value,
    }
    current_roles = set(parse_roles(user.role))
    updated_roles = set(current_roles)

    if action == "set":
        if role == "none":
            updated_roles = set()
        elif role in allowed_roles:
            updated_roles = {role}
        else:
            await cb.answer("Неизвестная роль", show_alert=True)
            return
    elif action == "add":
        if role not in allowed_roles:
            await cb.answer("Неизвестная роль", show_alert=True)
            return
        updated_roles.add(role)
    elif action == "remove":
        if role not in allowed_roles:
            await cb.answer("Неизвестная роль", show_alert=True)
            return
        updated_roles.discard(role)
    else:
        await cb.answer("Неизвестное действие", show_alert=True)
        return

    await db.set_user_roles(user_id, updated_roles)
    updated = await db.get_user_optional(user_id)
    if not updated:
        await cb.answer("Пользователь не найден", show_alert=True)
        return

    if updated.is_active:
        pushed = await _push_menu_to_user(
            cb.message,  # type: ignore[arg-type]
            user_id,
            updated.role,
            is_admin=user_id in (config.admin_ids or set()),
        )
        if not pushed:
            await cb.message.answer("⚠️ Не смог отправить пользователю обновлённое меню.")  # type: ignore[arg-type]

    await _show_employee_card(cb, db, user_id)
    await cb.answer("Роли обновлены")


@router.message(Command("stats"))
async def cmd_stats(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    now = utcnow()
    since_24h = to_iso(now - timedelta(hours=24))
    since_7d = to_iso(now - timedelta(days=7))

    role_counts = await db.users_by_role()
    total_users = await db.count_users()

    projects_total = await db.count_projects()
    projects_24h = await db.count_projects(since_24h)
    projects_7d = await db.count_projects(since_7d)

    tasks_total = await db.count_tasks()
    tasks_24h = await db.count_tasks(since_24h)
    tasks_7d = await db.count_tasks(since_7d)
    task_status = await db.tasks_by_status()

    usage_all = await db.usage_metrics()
    usage_24h = await db.usage_metrics(since_24h)
    usage_7d = await db.usage_metrics(since_7d)

    top_cmd_7d = await db.top_usage_entities("command", since_iso=since_7d, limit=5)
    top_menu_7d = await db.top_usage_entities("menu_click", since_iso=since_7d, limit=5)

    lines = ["<b>Статистика использования бота</b>"]
    lines.append("")
    lines.append("<b>Пользователи</b>")
    lines.append(f"• Всего сотрудников в базе: <b>{total_users}</b>")
    role_order = [
        Role.MANAGER,
        Role.RP,
        Role.ACCOUNTING,
        Role.INSTALLER,
        Role.DRIVER,
        Role.LOADER,
        Role.TINTER,
        Role.GD,
        "",
    ]
    for role in role_order:
        lines.append(f"• {role_label(role or None)}: <b>{role_counts.get(role, 0)}</b>")
    unknown_roles = [r for r in role_counts.keys() if r not in set(role_order)]
    for role in sorted(unknown_roles):
        lines.append(f"• {role_label(role)}: <b>{role_counts.get(role, 0)}</b>")

    lines.append("")
    lines.append("<b>Проекты и задачи</b>")
    lines.append(f"• Проектов: <b>{projects_total}</b> (24ч: {projects_24h}, 7д: {projects_7d})")
    lines.append(f"• Задач: <b>{tasks_total}</b> (24ч: {tasks_24h}, 7д: {tasks_7d})")
    for status in ["open", "in_progress", "done", "rejected"]:
        lines.append(f"• Задачи {task_status_label(status)}: <b>{task_status.get(status, 0)}</b>")

    lines.append("")
    lines.append("<b>Активность (audit)</b>")
    lines.append(
        f"• За 24ч: событий <b>{usage_24h['total_events']}</b>, "
        f"активных пользователей <b>{usage_24h['unique_users']}</b>, "
        f"команд {usage_24h['commands']}, кнопок меню {usage_24h['menu_clicks']}, callback {usage_24h['callbacks']}"
    )
    lines.append(
        f"• За 7д: событий <b>{usage_7d['total_events']}</b>, "
        f"активных пользователей <b>{usage_7d['unique_users']}</b>, "
        f"команд {usage_7d['commands']}, кнопок меню {usage_7d['menu_clicks']}, callback {usage_7d['callbacks']}"
    )
    lines.append(
        f"• За всё время: событий <b>{usage_all['total_events']}</b>, "
        f"уникальных пользователей <b>{usage_all['unique_users']}</b>"
    )

    if top_cmd_7d:
        lines.append("")
        lines.append("<b>Топ команд за 7д</b>")
        for item in top_cmd_7d:
            lines.append(f"• /{item['entity_id']} — <b>{item['cnt']}</b>")

    if top_menu_7d:
        lines.append("")
        lines.append("<b>Топ кнопок меню за 7д</b>")
        for item in top_menu_7d:
            lines.append(f"• {item['entity_id']} — <b>{item['cnt']}</b>")

    lines.append("")
    lines.append("Примечание: подробная usage-статистика начинает заполняться с момента включения новой телеметрии.")
    await _answer_chunked(message, lines)


@router.message(F.text == ADMIN_STATS_BUTTON)
async def menu_stats(message: Message, db: Database, config: Config) -> None:
    await cmd_stats(message, db, config)

@router.message(Command("setworkchat"))
async def cmd_setworkchat(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /setworkchat chat_id. Например: /setworkchat -1001234567890")
        return

    try:
        chat_id = int(parts[1])
    except ValueError:
        await message.answer("chat_id должен быть числом")
        return

    await db.set_setting("work_chat_id", str(chat_id))
    notifier.work_chat_id = chat_id
    await message.answer(f"Work chat установлен: <code>{chat_id}</code>")


@router.message(Command("workchat"))
async def cmd_workchat(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    db_value = await db.get_setting("work_chat_id")
    runtime_value = notifier.work_chat_id
    env_value = config.work_chat_id

    lines = ["<b>Диагностика рабочего чата</b>"]
    lines.append(f"• DB setting: <code>{db_value or '—'}</code>")
    lines.append(f"• Runtime notifier: <code>{runtime_value if runtime_value is not None else '—'}</code>")
    lines.append(f"• .env WORK_CHAT_ID: <code>{env_value if env_value is not None else '—'}</code>")
    if runtime_value is None:
        lines.append("")
        lines.append("⚠️ Рабочий чат не настроен. Выполните: <code>/setworkchat -100...</code>")
    await message.answer("\n".join(lines))


@router.message(F.text == ADMIN_WORKCHAT_BUTTON)
async def menu_workchat(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    await cmd_workchat(message, db, config, notifier)


@router.message(Command("workchat_test"))
async def cmd_workchat_test(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    chat_id = notifier.work_chat_id
    if chat_id is None:
        db_value = await db.get_setting("work_chat_id")
        if db_value:
            try:
                chat_id = int(db_value)
                notifier.work_chat_id = chat_id
            except ValueError:
                chat_id = None

    if chat_id is None:
        await message.answer("⚠️ Рабочий чат не настроен. Сначала: <code>/setworkchat -100...</code>")
        return

    ok = await notifier.safe_send(
        chat_id,
        "🧪 Тест уведомления в рабочий чат.\nЕсли вы видите это сообщение — доставка работает.",
    )
    if ok:
        await message.answer(f"✅ Тест отправлен в <code>{chat_id}</code>.")
    else:
        await message.answer(
            "⚠️ Не удалось отправить тест.\n"
            "Проверьте, что бот добавлен в чат, не ограничен и chat_id корректный."
        )


@router.message(F.text == ADMIN_WORKCHAT_TEST_BUTTON)
async def menu_workchat_test(message: Message, db: Database, config: Config, notifier: Notifier) -> None:
    await cmd_workchat_test(message, db, config, notifier)


@router.message(Command("resyncsheets"))
async def cmd_resync_sheets(message: Message, db: Database, config: Config, integrations: IntegrationHub) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    if not integrations.sheets:
        await message.answer("⚠️ Интеграция Google Sheets выключена.")
        return

    await message.answer("⏳ Запускаю пересинхронизацию данных в Google Sheets...")

    projects = await db.list_recent_projects(limit=10000)
    tasks = await db.list_recent_tasks(limit=50000)

    project_code_by_id: dict[int, str] = {}
    projects_ok = 0
    tasks_ok = 0

    for p in sorted(projects, key=lambda x: int(x["id"])):
        manager_label = ""
        manager_id = p.get("manager_id")
        if manager_id:
            manager = await db.get_user_optional(int(manager_id))
            if manager:
                manager_label = f"@{manager.username}" if manager.username else str(manager.telegram_id)
        await integrations.sheets.upsert_project(p, manager_label=manager_label)
        project_code = str(p.get("code") or "")
        if project_code:
            project_code_by_id[int(p["id"])] = project_code
        projects_ok += 1

    for t in sorted(tasks, key=lambda x: int(x["id"])):
        project_code = ""
        project_id = t.get("project_id")
        if project_id:
            project_code = project_code_by_id.get(int(project_id), "")
            if not project_code:
                try:
                    p = await db.get_project(int(project_id))
                    project_code = str(p.get("code") or "")
                    if project_code:
                        project_code_by_id[int(project_id)] = project_code
                except Exception:
                    project_code = ""
        await integrations.sheets.upsert_task(t, project_code=project_code)
        tasks_ok += 1

    await message.answer(
        "✅ Пересинхронизация завершена.\n"
        f"Проектов: <b>{projects_ok}</b>\n"
        f"Задач: <b>{tasks_ok}</b>"
    )


@router.message(F.text == ADMIN_RESYNC_BUTTON)
async def menu_resync_sheets(message: Message, db: Database, config: Config, integrations: IntegrationHub) -> None:
    await cmd_resync_sheets(message, db, config, integrations)

@router.message(Command("setdefaults"))
async def cmd_setdefaults(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u or not _is_admin(u.id, config):
        return

    # Example: /setdefaults rp=@pasha td=@serga acc=@buh driver=@driver tinter=@tinter gd=@ceo
    parts = (message.text or "").split()[1:]
    if not parts:
        await message.answer(
            "Пример: /setdefaults rp=@pasha td=@serga acc=@buh driver=@driver tinter=@tinter gd=@ceo"
        )
        return

    changed = []
    failed = []
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if not v:
            continue
        user_id, label = await _resolve_user_id_by_ref(db, v)
        if user_id is None:
            failed.append(v)
            continue

        # Keep username in settings when provided, so defaults can be maintained by nickname.
        if v.isdigit():
            setting_value = str(user_id)
            value_label = label or str(user_id)
        else:
            uname = _normalize_username(v)
            setting_value = uname
            value_label = f"@{uname}"

        if k == "rp":
            await db.set_setting("default_rp_id", setting_value)
            changed.append(("default_rp_id", value_label))
        elif k in {"acc", "accounting"}:
            await db.set_setting("default_accounting_id", setting_value)
            changed.append(("default_accounting_id", value_label))
        elif k in {"driver", "drv"}:
            await db.set_setting("default_driver_id", setting_value)
            changed.append(("default_driver_id", value_label))
        elif k in {"tinter", "tint"}:
            await db.set_setting("default_tinter_id", setting_value)
            changed.append(("default_tinter_id", value_label))
        elif k == "gd":
            await db.set_setting("default_gd_id", setting_value)
            changed.append(("default_gd_id", value_label))

    if not changed:
        await message.answer(
            "Ничего не изменено. Пример: /setdefaults rp=@pasha td=@serga acc=@buh driver=@driver tinter=@tinter gd=@ceo"
        )
        return

    lines = ["✅ Обновил дефолтных исполнителей:"]
    for k, v in changed:
        lines.append(f"• {k} = <code>{v}</code>")
    if failed:
        lines.append("")
        lines.append("⚠️ Не удалось найти:")
        for item in failed:
            lines.append(f"• {item}")
    await message.answer("\n".join(lines))
