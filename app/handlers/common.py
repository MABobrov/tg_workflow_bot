from __future__ import annotations

import logging

from aiogram import Router
from aiogram import html
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram import F
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import AdminRoleCb, AdminUserCb
from ..config import Config
from ..db import Database
from ..enums import Role
from ..enums import MANAGER_ROLES
from ..keyboards import (
    BACK_TO_HOME, BACK_TO_ROLE_SELECTOR, OPEN_ACTIONS, OPEN_HELP, actions_menu, main_menu,
    gd_more_menu, GD_BTN_BACK_HOME, GD_BTN_MORE,
    manager_more_menu, MGR_BTN_MORE, MGR_BTN_BACK_HOME, MGR_BTN_SYNC,
    role_selector_choices, ROLE_SELECTOR_PREFIX,
    rp_more_menu, rp_team_menu, RP_BTN_MORE, RP_BTN_BACK_HOME, RP_BTN_TEAM,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.menu_context import build_menu_context
from ..services.menu_scope import (
    clear_active_menu_role,
    get_active_menu_role,
    resolve_active_menu_role,
    set_active_menu_role,
)
from ..services.sheets_sync import export_to_sheets, import_from_source_sheet
from ..utils import answer_service, parse_roles, private_only_reply_markup, role_label

log = logging.getLogger(__name__)

router = Router()


async def _is_blocked(db: Database, user_id: int) -> bool:
    user = await db.get_user_optional(user_id)
    return bool(user and not user.is_active)


async def _guard_blocked_message(message: Message, db: Database) -> bool:
    if not message.from_user:
        return False
    if await _is_blocked(db, message.from_user.id):
        await message.answer("⛔️ Ваш доступ к боту заблокирован. Обратитесь к администратору.")
        return False
    return True


async def _menu_context(db: Database, user_id: int | None, role: str | None) -> dict[str, object]:
    return await build_menu_context(db, user_id, role)


def _menu_scope(user_id: int | None, role_value: str | None) -> tuple[str | None, bool]:
    active_role = resolve_active_menu_role(user_id, role_value)
    isolated_role = bool(
        user_id
        and role_value
        and active_role
        and active_role != role_value
        and len(parse_roles(role_value)) > 1
    )
    return active_role or role_value, isolated_role


def _new_user_admin_kb(user_id: int) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    for role, label in (
        (Role.MANAGER_KV, "Менеджер КВ"),
        (Role.MANAGER_KIA, "Менеджер КИА"),
        (Role.MANAGER_NPN, "Менеджер НПН"),
        (Role.RP, "РП"),
        (Role.ACCOUNTING, "Бухгалтерия"),
        (Role.INSTALLER, "Монтажник"),
        (Role.ZAMERY, "Замерщик"),
        (Role.DRIVER, "Водитель"),
        (Role.LOADER, "Грузчик"),
        (Role.TINTER, "Тонировщик"),
        (Role.GD, "ГД"),
    ):
        b.button(text=f"✅ {label}", callback_data=AdminRoleCb(user_id=user_id, action="set", role=role).pack())
    b.button(text="🚫 Заблокировать", callback_data=AdminUserCb(user_id=user_id, action="block").pack())
    b.button(text="👤 Карточка сотрудника", callback_data=AdminUserCb(user_id=user_id, action="view").pack())
    b.adjust(2, 2, 2, 2, 2, 1)
    return b


async def _notify_admins_new_user_without_role(message: Message, config: Config) -> None:
    u = message.from_user
    if not u:
        return
    admin_ids = list(config.admin_ids or set())
    if not admin_ids:
        return

    username = f"@{u.username}" if u.username else "—"
    full_name = html.quote(u.full_name or "—")
    text = (
        "🆕 <b>Новый пользователь без роли</b>\n\n"
        f"👤 Имя: <b>{full_name}</b>\n"
        f"🔖 Username: <b>{html.quote(username)}</b>\n"
        f"🆔 ID: <code>{u.id}</code>\n\n"
        "Выберите действие на этом сообщении:"
    )
    kb = _new_user_admin_kb(u.id).as_markup()
    for admin_id in admin_ids:
        try:
            await message.bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            log.exception("Failed to notify admin=%s about new user=%s", admin_id, u.id)


def _role_guide(role: str | None) -> str:
    common = (
        "<b>Как пользоваться ботом</b>\n\n"
        "1. В главном меню выберите частое действие, а редкие — через «📂 Ещё действия».\n"
        "2. Ответьте на вопросы бота по шагам.\n"
        "3. В любой момент можно отменить ввод: <code>/cancel</code> или кнопка «❌ Отмена».\n"
        "4. Если кнопки не обновились после смены роли: кнопка «🔄 Обновить меню» или <code>/menu</code>.\n\n"
        "<b>Полезные команды</b>\n"
        "• <code>/menu</code> — обновить меню\n"
        "• <code>/id</code> — ваш Telegram ID\n"
        "• <code>/help</code> — эта инструкция\n"
        "• <code>/search текст</code> — быстрый поиск проекта\n"
    )

    roles = set(parse_roles(role))
    if not roles:
        return common + (
            "\n<b>Роль ещё не назначена</b>\n"
            "Попросите администратора назначить роль командой:\n"
            "<code>/setrole @username manager[,rp,...]</code>\n"
            "После назначения нажмите <code>/menu</code>."
        )

    sections: list[str] = []
    # ГД и РП видят справку по ВСЕМ ролям
    show_all = bool(roles & {Role.GD, Role.RP})
    # Manager roles (new)
    manager_roles_in_user = roles & MANAGER_ROLES
    if manager_roles_in_user or Role.MANAGER in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Менеджер)</b>\n"
            "• «📋 Проверить КП/Счет» — отправить КП на проверку РП, создать счёт в БД.\n"
            "• «💼 Счет в Работу» — отправить счёт ГД на оплату.\n"
            "• «🏁 Счет End» — инициировать закрытие счёта (проверка 4 условий).\n"
            "• «📐 Замеры» — запрос замерщику.\n"
            "• «📄 Бухгалтерия (ЭДО)» — запрос ЭДО в бухгалтерию.\n"
            "• «📩 Не срочно ГД» — задача ГД (пониженный приоритет).\n"
            "• Подменю «Ещё»: 💬 Менеджер (кред), 📑 Мои Счета, 🆘 Проблема/Вопрос, 🚨 Срочно ГД, 🔍 Поиск счёта.\n"
        )
    if Role.RP in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (РП)</b>\n"
            "• «📥 Входящие Отд.Продаж» — входящие задачи от менеджеров и ГД.\n"
            "• «💼 Счета в Работу» — мониторинг счетов (информационный).\n"
            "• «🏁 Счет End» — входящие условия закрытия.\n"
            "• «💳 Счета на оплату» — счёт на оплату ГД.\n"
            "• «🆘 Проблема/Вопрос» — входящие от ГД, менеджеров, монтажников.\n"
            "• «👥 Команда» — подменю чатов с менеджерами и монтажом.\n"
            "• «📄 Бухгалтерия (ЭДО)» — запрос ЭДО.\n"
            "• Подменю «Ещё»: 🎯 Лид в проект, 🚨 Срочно ГД, 🔍 Поиск счёта.\n"
        )
    if Role.ACCOUNTING in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Бухгалтерия)</b>\n"
            "• «📥 Входящие задачи» — запросы ЭДО от менеджеров и РП.\n"
            "• «📩 Не срочно ГД» — задача ГД (пониженный приоритет).\n"
            "• «🔍 Поиск счёта» — поиск счетов по критериям.\n"
            "• «🏁 Закрытые Счета» — список закрытых счетов.\n"
            "• «🚨 Срочно ГД» — срочный вопрос ГД.\n"
        )
    if Role.INSTALLER in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Монтажник)</b>\n"
            "• «📦 Заказ материалов» — запрос материалов у РП.\n"
            "• «✅ Счет ок» — подтверждение выполнения работ по счёту.\n"
            "• «📦 Заказ доп.материалов» — доп. запрос материалов у РП.\n"
            "• «📌 Мои объекты» — список объектов и статусы ЗП.\n"
            "• «📝 Отчёт за день» — текстовое сообщение РП.\n"
            "• «🔨 В Работу» — принять задачу от РП.\n"
            "• «📩 Не срочно ГД» / «🚨 Срочно ГД» — сообщения ГД.\n"
        )
    if Role.ZAMERY in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Замерщик)</b>\n"
            "• «📋 Заявка на замер» — входящие заявки от Отд.Продаж и ГД.\n"
            "• «📋 Мои замеры» — список замеров со статусом оплаты.\n"
            "• «🚨 Срочно ГД» — двустороннее сообщение с ГД.\n"
            "• «💰 Оплата замеров» — расчёт ЗП за выполненные замеры.\n"
        )
    if Role.GD in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (ГД)</b>\n"
            "• «Счета на Оплату» — входящие счета от РП.\n"
            "• «Срочно для ГД» — срочные запросы + подтверждения оплат.\n"
            "• «🔍 Поиск счёта» — поиск счетов по критериям.\n"
            "• «✅ Подтверждение оплат» — задачи на подтверждение оплаты клиентов.\n"
            "• По оплате: «✅ Оплата подтверждена» или «⚠️ Нужна доплата».\n"
            "• «💸 Оплата поставщику» — зафиксировать оплату поставщику.\n"
            "• «Чат с РП», «Замеры», «Бухгалтерия», «Монтажная гр.», «Отд.Продаж» — чаты с сотрудниками.\n"
            "• «Синхронизация данных» — синхронизация с Google Sheets.\n"
            "• «💬 Кред» — КВ, КИА, НПН.\n"
        )
    if Role.DRIVER in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Водитель)</b>\n"
            "• «📥 Входящие задачи» — задачи от РП.\n"
            "• «✅ Доставка выполнена» — подтвердить доставку.\n"
            "Заполните: проект, комментарий, фото разгрузки.\n"
        )
    if Role.TINTER in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Тонировщик)</b>\n"
            "• «📥 Входящие задачи» — заявки на тонировку от РП.\n"
            "• «✅ Тонировка выполнена» — подтвердить тонировку.\n"
            "Заполните: проект, комментарий, фото результата.\n"
        )
    if Role.LOADER in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Грузчик)</b>\n"
            "• «📥 Входящие задачи» — входящие задачи по объектам.\n"
            "• «🚨 Срочно ГД» — срочная эскалация ГД.\n"
        )

    return common + "".join(sections)


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database, config: Config) -> None:
    u = message.from_user
    if not u:
        return
    existed = await db.get_user_optional(u.id)
    user = await db.upsert_user(u.id, u.username, u.full_name)
    role = user.role
    if len(parse_roles(role)) > 1:
        clear_active_menu_role(u.id)
        active_role = None
    else:
        set_active_menu_role(u.id, role)
        active_role = role

    if not user.is_active:
        await message.answer("⛔️ Ваш доступ к боту заблокирован. Обратитесь к администратору.")
        return

    text = (
        "👋 Привет! Я бот для структурирования внутренних заявок и статусов проектов.\n\n"
        f"Ваш Telegram ID: <code>{u.id}</code>\n"
    )
    if role:
        text += f"Ваши роли: <b>{role_label(role)}</b>\n\nОткройте меню и выберите действие."
    else:
        text += (
            "Ваша роль пока не назначена.\n"
            "Администратор должен выдать роль командой: /setrole <code>@username</code> <code>manager[,rp,...]</code>\n"
            "Допускается и старый формат по ID.\n\n"
            "После назначения нажмите «🔄 Обновить меню»."
        )

    is_admin = u.id in (config.admin_ids or set())
    menu_context = await _menu_context(db, u.id, active_role or role)
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                active_role or role,
                is_admin=is_admin,
                isolated_role=bool(active_role and active_role != role),
                **menu_context,
            ),
        ),
    )
    if existed is None and not role and not is_admin:
        await _notify_admins_new_user_without_role(message, config)


@router.message(Command("id"))
async def cmd_id(message: Message, db: Database) -> None:
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    await answer_service(message, f"Ваш Telegram ID: <code>{u.id}</code>", delay_seconds=60)


@router.message(Command("menu"))
@router.message(lambda m: (m.text or "").strip() == "🔄 Обновить меню")
async def cmd_menu(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    is_admin = u.id in (config.admin_ids or set())
    roles = parse_roles(role)

    if len(roles) > 1:
        # Multi-role user: preserve active role if already selected
        active = get_active_menu_role(u.id)
        if active and active in roles:
            # Refresh menu for the active role (don't reset to selector)
            menu_context = await _menu_context(db, u.id, active)
            await message.answer(
                "✅ Меню обновлено.",
                reply_markup=private_only_reply_markup(
                    message,
                    main_menu(
                        active,
                        is_admin=is_admin,
                        isolated_role=True,
                        **menu_context,
                    ),
                ),
            )
            return
        # No active role — show role selector
        clear_active_menu_role(u.id)
        menu_context = await _menu_context(db, u.id, role)
        await message.answer(
            "🎭 <b>Выберите роль</b>\n\nСначала выберите, в каком контексте открыть меню.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(role, is_admin=is_admin, **menu_context),
            ),
        )
    else:
        set_active_menu_role(u.id, role)
        menu_context = await _menu_context(db, u.id, role)
        # Use message.answer() directly — NOT answer_service() which auto-deletes
        # the message after 60s, taking the reply keyboard with it.
        await message.answer(
            "✅ Меню обновлено.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(role, is_admin=is_admin, **menu_context),
            ),
        )


@router.message(Command("help"))
async def cmd_help(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    role = None
    is_admin = False
    if message.from_user:
        user = await db.get_user_optional(message.from_user.id)
        role = user.role if user else None
        is_admin = message.from_user.id in (config.admin_ids or set())
    menu_role, isolated_role = _menu_scope(message.from_user.id if message.from_user else None, role)
    # ГД и РП видят справку по всем ролям; остальные — только по активной роли меню
    guide_role = role if (menu_role and menu_role in (Role.GD, Role.RP)) else menu_role
    text = _role_guide(guide_role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    _uid_help = message.from_user.id if message.from_user else None
    menu_context = await _menu_context(db, _uid_help, menu_role)
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=is_admin,
                isolated_role=isolated_role,
                **menu_context,
            ),
        ),
    )


@router.message(lambda m: (m.text or "").strip() in {OPEN_ACTIONS, "🧭 Действия"})
async def menu_actions(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    menu_role, isolated_role = _menu_scope(u.id, role)
    if not role:
        unread = await db.count_unread_tasks(u.id)
        uc = await db.count_unread_by_channel(u.id)
        await answer_service(
            message,
            "Роль пока не назначена. Попросите администратора назначить роль и нажмите «🔄 Обновить меню».",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(
                message,
                main_menu(None, is_admin=u.id in (config.admin_ids or set()), unread=unread, unread_channels=uc, gd_inbox_unread=None),
            ),
        )
        return
    if len(parse_roles(role)) > 1 and not isolated_role:
        await answer_service(
            message,
            "🎭 <b>Выберите роль</b>\n\nСначала выберите роль, для которой хотите открыть действия:",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(
                message,
                main_menu(role, is_admin=u.id in (config.admin_ids or set())),
            ),
        )
        return
    is_admin = u.id in (config.admin_ids or set())
    await answer_service(
        message,
        "Выберите действие:",
        delay_seconds=60,
        reply_markup=private_only_reply_markup(
            message,
            actions_menu(
                menu_role,
                is_admin=is_admin,
                show_role_selector_back=isolated_role,
            ),
        ),
    )


@router.message(lambda m: (m.text or "").strip().startswith(ROLE_SELECTOR_PREFIX))
async def role_selector_pick(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    # Clear any lingering FSM state to avoid interference with role menus
    await state.clear()
    user = await db.get_user_optional(u.id)
    role_raw = user.role if user else None
    selected_role = role_selector_choices(role_raw).get((message.text or "").strip())
    if not selected_role:
        log.warning("role_selector_pick: no match for text=%r, role_raw=%r", (message.text or "").strip(), role_raw)
        # Fallback: re-show role selector so the user is not stuck
        is_admin = u.id in (config.admin_ids or set())
        menu_context = await _menu_context(db, u.id, role_raw)
        await message.answer(
            "🎭 <b>Выберите роль</b>\n\nСначала выберите, в каком контексте открыть меню.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(role_raw, is_admin=is_admin, **menu_context),
            ),
        )
        return
    set_active_menu_role(u.id, selected_role)

    menu_context = await _menu_context(db, u.id, selected_role)
    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        "🎭 <b>Выбрана роль</b>\n\nДоступны действия только этой роли.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                selected_role,
                is_admin=is_admin,
                isolated_role=True,
                **menu_context,
            ),
        ),
    )


@router.message(lambda m: (m.text or "").strip() == BACK_TO_HOME)
async def back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    menu_role, isolated_role = _menu_scope(u.id, role)
    if isolated_role:
        menu_context = await _menu_context(db, u.id, menu_role)
        await message.answer(
            "Главное меню выбранной роли.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(
                    menu_role,
                    is_admin=u.id in (config.admin_ids or set()),
                    isolated_role=True,
                    **menu_context,
                ),
            ),
        )
        return
    await cmd_menu(message, state, db, config)


@router.message(lambda m: (m.text or "").strip() == BACK_TO_ROLE_SELECTOR)
async def back_to_role_selector(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    if len(parse_roles(role)) <= 1:
        await cmd_menu(message, state, db, config)
        return
    clear_active_menu_role(u.id)
    menu_context = await _menu_context(db, u.id, role)
    await message.answer(
        "🎭 <b>Выберите роль</b>\n\nВыберите, в каком контексте продолжить работу.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()),
                **menu_context,
            ),
        ),
    )


@router.message(lambda m: (m.text or "").strip() in {GD_BTN_MORE, MGR_BTN_MORE, RP_BTN_MORE, "Еще"})
async def menu_more_universal(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Unified 'More/Ещё' handler — dispatches to correct submenu based on active role.

    GD_BTN_MORE == MGR_BTN_MORE == RP_BTN_MORE == "📂 Ещё", so a single handler
    is needed to avoid aiogram selecting whichever was registered first.
    """
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    _, isolated_role = _menu_scope(u.id, user.role if user else None)

    active_role = get_active_menu_role(u.id)
    if active_role is None and user and user.role:
        roles = parse_roles(user.role)
        active_role = roles[0] if roles else None

    if active_role == Role.GD:
        _is_adm = bool(u.id in (config.admin_ids or set()))
        _uc = await db.count_unread_by_channel(u.id)
        await message.answer(
            "Выберите действие:",
            reply_markup=private_only_reply_markup(
                message,
                gd_more_menu(
                    is_admin=_is_adm,
                    unread_channels=_uc,
                    show_role_selector_back=isolated_role,
                ),
            ),
        )
    elif active_role == Role.RP:
        await message.answer(
            "Выберите действие:",
            reply_markup=private_only_reply_markup(message, rp_more_menu(show_role_selector_back=isolated_role)),
        )
    else:
        # Manager roles (KV / KIA / NPN) and legacy Manager
        await message.answer(
            "Выберите действие:",
            reply_markup=private_only_reply_markup(message, manager_more_menu(show_role_selector_back=isolated_role)),
        )


@router.message(lambda m: (m.text or "").strip() in {GD_BTN_BACK_HOME, MGR_BTN_BACK_HOME, RP_BTN_BACK_HOME, "Назад в Гл.меню"})
async def role_back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Any role: return from 'Еще' submenu to main menu.

    For multi-role users: preserve the active role selection instead of
    resetting to the role selector every time.
    """
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    menu_role, isolated_role = _menu_scope(u.id, role)
    # If user has an active role selected, return to that role's menu
    if isolated_role and menu_role:
        menu_context = await _menu_context(db, u.id, menu_role)
        await message.answer(
            "Главное меню выбранной роли.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(
                    menu_role,
                    is_admin=u.id in (config.admin_ids or set()),
                    isolated_role=True,
                    **menu_context,
                ),
            ),
        )
        return
    # Fallback: full menu reset (single role or no active role)
    await cmd_menu(message, state, db, config)


@router.message(lambda m: (m.text or "").strip() == RP_BTN_TEAM)
async def rp_menu_team(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """RP: open 'Команда' submenu."""
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    user = await db.get_user_optional(message.from_user.id) if message.from_user else None
    _, isolated_role = _menu_scope(message.from_user.id if message.from_user else None, user.role if user else None)
    await message.answer(
        "Выберите сотрудника:",
        reply_markup=private_only_reply_markup(message, rp_team_menu(show_role_selector_back=isolated_role)),
    )


@router.message(lambda m: (m.text or "").strip() in {OPEN_HELP, "Справка"})
async def menu_help_shortcut(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    await cmd_help(message, db, config)


@router.message(lambda m: (m.text or "").strip() == "ℹ️ Инструкция")
async def menu_help(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    role = None
    is_admin = False
    if message.from_user:
        user = await db.get_user_optional(message.from_user.id)
        role = user.role if user else None
        is_admin = message.from_user.id in (config.admin_ids or set())
    menu_role, isolated_role = _menu_scope(message.from_user.id if message.from_user else None, role)
    # ГД и РП видят справку по всем ролям; остальные — только по активной роли меню
    guide_role = role if (menu_role and menu_role in (Role.GD, Role.RP)) else menu_role
    text = _role_guide(guide_role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    _uid_info = message.from_user.id if message.from_user else None
    unread = await db.count_unread_tasks(_uid_info) if _uid_info else 0
    uc = await db.count_unread_by_channel(_uid_info) if _uid_info else {}
    _parsed_info = parse_roles(role) if role else []
    gd_ur = await db.count_gd_inbox_tasks(_uid_info) if _uid_info and Role.GD in _parsed_info else None
    gd_inv = await db.count_gd_invoice_tasks(_uid_info) if _uid_info and Role.GD in _parsed_info else None
    gd_ie = await db.count_gd_invoice_end_tasks(_uid_info) if _uid_info and Role.GD in _parsed_info else None
    _is_rp_info = _uid_info and (Role.RP in _parsed_info or Role.MANAGER_NPN in _parsed_info)
    rp_t_info = await db.count_rp_role_tasks(_uid_info) if _is_rp_info else 0
    rp_m_info = await db.count_rp_role_messages(_uid_info) if _is_rp_info else 0
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=is_admin,
                unread=unread,
                unread_channels=uc,
                gd_inbox_unread=gd_ur,
                gd_invoice_unread=gd_inv,
                gd_invoice_end_unread=gd_ie,
                isolated_role=isolated_role,
                rp_tasks=rp_t_info,
                rp_messages=rp_m_info,
            ),
        ),
    )


# =====================================================================
# УНИВЕРСАЛЬНЫЙ CALLBACK «НАЗАД» — возврат в главное меню (любая роль)
# =====================================================================

@router.callback_query(F.data == "nav:home")
async def universal_back_home(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Возврат в главное меню из любого inline-меню (любая роль)."""
    await cb.answer()
    await state.clear()
    u = cb.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    menu_role, isolated_role = _menu_scope(u.id, role)
    menu_context = await _menu_context(db, u.id, menu_role)
    await cb.message.answer(  # type: ignore[union-attr]
        "📋 Главное меню",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                isolated_role=isolated_role,
                **menu_context,
            ),
        ),
    )


# =====================================================================
# ВХОДЯЩИЕ ЗАДАЧИ (универсальный обработчик для всех ролей)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие задачи") or (m.text or "").strip().startswith("Задачи / Лид в проект"))
async def inbox_tasks_universal(message: Message, db: Database) -> None:
    """Universal inbox handler for all roles that use inbox button."""
    if not message.from_user:
        return
    if not await _guard_blocked_message(message, db):
        return
    # Бухгалтерия обрабатывается в accounting_new.py
    _u = await db.get_user_optional(message.from_user.id)
    if _u and _u.role:
        _active = resolve_active_menu_role(message.from_user.id, _u.role)
        if _active == Role.ACCOUNTING:
            from .accounting_new import acc_inbox_tasks

            await acc_inbox_tasks(message, db)
            return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)
    if not tasks:
        await answer_service(message, "📥 Входящих задач нет ✅", delay_seconds=60)
        return
    await message.answer(
        f"📥 <b>Входящие задачи</b> ({len(tasks)}):\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


# =====================================================================
# ВСЕ ЗАДАЧИ (список для всех ролей)
# =====================================================================

@router.message(lambda m: (m.text or "").strip() == "📋 Все задачи")
async def all_tasks_list(message: Message, db: Database) -> None:
    """Show all tasks (active + recent closed) for the current user."""
    if not message.from_user:
        return
    if not await _guard_blocked_message(message, db):
        return
    uid = message.from_user.id

    # Active tasks (open + in_progress)
    active = await db.list_tasks_for_user(uid, statuses=("open", "in_progress"), limit=50)
    # Created by user (active)
    created = await db.list_tasks_created_by(uid, statuses=("open", "in_progress"), limit=20)
    # Recent closed (done + rejected, last 10)
    closed = await db.list_tasks_for_user(uid, statuses=("done", "rejected"), limit=10)

    all_tasks = []
    seen_ids: set[int] = set()
    for t in active:
        if int(t["id"]) not in seen_ids:
            all_tasks.append(t)
            seen_ids.add(int(t["id"]))
    for t in created:
        if int(t["id"]) not in seen_ids:
            all_tasks.append(t)
            seen_ids.add(int(t["id"]))
    for t in closed:
        if int(t["id"]) not in seen_ids:
            all_tasks.append(t)
            seen_ids.add(int(t["id"]))

    if not all_tasks:
        await answer_service(message, "📋 Задач нет.", delay_seconds=60)
        return

    active_count = sum(1 for t in all_tasks if t.get("status") in ("open", "in_progress"))
    closed_count = len(all_tasks) - active_count

    await message.answer(
        f"📋 <b>Все задачи</b>\n"
        f"Активных: {active_count} | Закрытых: {closed_count}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(all_tasks, back_callback="nav:home"),
    )


# =====================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ (для менеджеров, РП, бухгалтерии)
# =====================================================================

@router.message(
    lambda m: (m.text or "").strip() in {MGR_BTN_SYNC, "🔄 Синхронизация данных"}
    and get_active_menu_role(m.from_user.id if m.from_user else None) != Role.GD
)
async def sync_data_non_gd(
    message: Message, db: Database, config: Config, integrations: IntegrationHub,
) -> None:
    """Sync data with Google Sheets for non-GD roles (button text with emoji)."""
    if not message.from_user:
        return
    user = await db.get_user_optional(message.from_user.id)
    if not user:
        return

    role = user.role
    active_role = resolve_active_menu_role(message.from_user.id, role)
    if active_role == Role.GD:
        return
    is_admin = message.from_user.id in (config.admin_ids or set())
    menu_context = await _menu_context(db, message.from_user.id, active_role or role)

    if not integrations.sheets:
        await answer_service(
            message,
            "⚠️ Интеграция Google Sheets не настроена.",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(
                    active_role or role,
                    is_admin=is_admin,
                    isolated_role=bool(active_role and active_role != role),
                    **menu_context,
                ),
            ),
        )
        return

    await answer_service(message, "⏳ Запускаю синхронизацию данных с Google Sheets...")

    imported_ok = 0
    try:
        imported_ok = await import_from_source_sheet(
            db,
            integrations.sheets,
            log_prefix="manual_sync",
        )
    except Exception as e:
        log.error("read_op_sheet failed: %s", e)

    stats = await export_to_sheets(
        db,
        integrations.sheets,
        include_invoice_cost=False,
        sync_invoices=True,
    )

    menu_context = await _menu_context(db, message.from_user.id, active_role or role)
    await answer_service(
        message,
        "✅ Синхронизация завершена.\n"
        f"📥 Импорт из ОП: <b>{imported_ok}</b>\n"
        f"Проектов: <b>{stats['projects']}</b>\n"
        f"Задач: <b>{stats['tasks']}</b>\n"
        f"Счетов: <b>{stats['invoices']}</b>",
        delay_seconds=300,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                active_role or role,
                is_admin=is_admin,
                isolated_role=bool(active_role and active_role != role),
                **menu_context,
            ),
        ),
    )


@router.message(Command("cancel"))
@router.message(lambda m: (m.text or "").strip() in {"❌ Отмена", "Отмена"})
async def cmd_cancel(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    role = None
    if u:
        user = await db.get_user_optional(u.id)
        role = user.role if user else None
    active_role = resolve_active_menu_role(u.id if u else None, role)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    menu_context = await _menu_context(db, u.id if u else None, active_role or role)
    await answer_service(
        message,
        "Операция отменена. Выберите следующее действие в меню.",
        delay_seconds=60,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                active_role or role,
                is_admin=is_admin,
                isolated_role=bool(active_role and active_role != role),
                **menu_context,
            ),
        ),
    )


@router.my_chat_member()
async def on_bot_added_to_chat(event: ChatMemberUpdated) -> None:
    """When bot is added to a group/supergroup, post chat id for quick setup."""
    chat_type = getattr(event.chat, "type", "")
    if chat_type not in {"group", "supergroup"}:
        return

    old_status = (event.old_chat_member.status or "").lower()
    new_status = (event.new_chat_member.status or "").lower()
    if old_status not in {"left", "kicked"} or new_status not in {"member", "administrator"}:
        return

    chat_id = event.chat.id
    await event.bot.send_message(
        chat_id=chat_id,
        text=(
            "👋 Бот добавлен в рабочий чат.\n\n"
            f"ID этого чата: <code>{chat_id}</code>\n"
            "Для привязки администратор должен выполнить в личке бота:\n"
            f"<code>/setworkchat {chat_id}</code>"
        ),
        reply_markup=ReplyKeyboardRemove(),
    )
