from __future__ import annotations

import logging

from aiogram import Router
from aiogram import html
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import ChatMemberUpdated, Message, ReplyKeyboardRemove
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
from ..services.menu_scope import (
    clear_active_menu_role,
    get_active_menu_role,
    resolve_active_menu_role,
    set_active_menu_role,
)
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
    if not user_id:
        return {
            "unread": 0,
            "unread_channels": {},
            "gd_inbox_unread": None,
            "gd_invoice_unread": None,
            "gd_invoice_end_unread": None,
        }

    roles = set(parse_roles(role))
    return {
        "unread": await db.count_unread_tasks(user_id),
        "unread_channels": await db.count_unread_by_channel(user_id),
        "gd_inbox_unread": await db.count_gd_inbox_tasks(user_id) if Role.GD in roles else None,
        "gd_invoice_unread": await db.count_gd_invoice_tasks(user_id) if Role.GD in roles else None,
        "gd_invoice_end_unread": await db.count_gd_invoice_end_tasks(user_id) if Role.GD in roles else None,
    }


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
    # Manager roles (new)
    manager_roles_in_user = roles & MANAGER_ROLES
    if manager_roles_in_user or Role.MANAGER in roles:
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
    if Role.RP in roles:
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
    if Role.ACCOUNTING in roles:
        sections.append(
            "\n<b>Ваши сценарии (Бухгалтерия)</b>\n"
            "• «📥 Входящие задачи» — запросы ЭДО от менеджеров и РП.\n"
            "• «📩 Не срочно ГД» — задача ГД (пониженный приоритет).\n"
            "• «🔍 Поиск счёта» — поиск счетов по критериям.\n"
            "• «🏁 Закрытые Счета» — список закрытых счетов.\n"
            "• «🚨 Срочно ГД» — срочный вопрос ГД.\n"
        )
    if Role.INSTALLER in roles:
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
    if Role.ZAMERY in roles:
        sections.append(
            "\n<b>Ваши сценарии (Замерщик)</b>\n"
            "• «📋 Заявка на замер» — входящие заявки на замеры. Ответ: «ок» + бланк замера.\n"
            "• «📋 Мои замеры» — список объектов и статусы ЗП.\n"
            "• «🚨 Срочно ГД» / «📩 Не срочно ГД» — сообщения ГД.\n"
        )
    if Role.GD in roles:
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
    if Role.DRIVER in roles:
        sections.append(
            "\n<b>Ваши сценарии (Водитель)</b>\n"
            "• «📥 Входящие задачи» — заявки на доставку от РП.\n"
            "• «✅ Доставка выполнена» — подтвердить доставку.\n"
            "Заполните: проект, комментарий, фото разгрузки.\n"
        )
    if Role.TINTER in roles:
        sections.append(
            "\n<b>Ваши сценарии (Тонировщик)</b>\n"
            "• «📥 Входящие задачи» — заявки на тонировку от РП.\n"
            "• «✅ Тонировка выполнена» — подтвердить тонировку.\n"
            "Заполните: проект, комментарий, фото результата.\n"
        )
    if Role.LOADER in roles:
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
    unread = await db.count_unread_tasks(u.id)
    uc = await db.count_unread_by_channel(u.id)
    gd_ur = await db.count_gd_inbox_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    gd_inv = await db.count_gd_invoice_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    gd_ie = await db.count_gd_invoice_end_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                active_role or role,
                is_admin=is_admin,
                unread=unread,
                unread_channels=uc,
                gd_inbox_unread=gd_ur,
                gd_invoice_unread=gd_inv,
                gd_invoice_end_unread=gd_ie,
                isolated_role=bool(active_role and active_role != role),
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
    if len(parse_roles(role)) > 1:
        clear_active_menu_role(u.id)
    else:
        set_active_menu_role(u.id, role)
    is_admin = u.id in (config.admin_ids or set())
    unread = await db.count_unread_tasks(u.id)
    uc = await db.count_unread_by_channel(u.id)
    gd_ur = await db.count_gd_inbox_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    gd_inv = await db.count_gd_invoice_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    gd_ie = await db.count_gd_invoice_end_tasks(u.id) if role and Role.GD in parse_roles(role) else None
    if len(parse_roles(role)) > 1:
        text = "🎭 <b>Выберите роль</b>\n\nСначала выберите, в каком контексте открыть меню."
    else:
        text = "✅ Меню обновлено."
    await answer_service(
        message,
        text,
        delay_seconds=60,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=is_admin, unread=unread, unread_channels=uc, gd_inbox_unread=gd_ur, gd_invoice_unread=gd_inv, gd_invoice_end_unread=gd_ie),
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
    text = _role_guide(role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    unread = await db.count_unread_tasks(message.from_user.id) if message.from_user else 0
    uc = await db.count_unread_by_channel(message.from_user.id) if message.from_user else {}
    gd_ur = await db.count_gd_inbox_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
    gd_inv = await db.count_gd_invoice_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
    gd_ie = await db.count_gd_invoice_end_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
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
async def role_selector_pick(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role_raw = user.role if user else None
    selected_role = role_selector_choices(role_raw).get((message.text or "").strip())
    if not selected_role:
        return
    set_active_menu_role(u.id, selected_role)

    menu_context = await _menu_context(db, u.id, selected_role)
    is_admin = u.id in (config.admin_ids or set())
    await answer_service(
        message,
        "🎭 <b>Выбрана роль</b>\n\nДоступны действия только этой роли.",
        delay_seconds=60,
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
        await answer_service(
            message,
            "Главное меню выбранной роли.",
            delay_seconds=60,
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
    await answer_service(
        message,
        "🎭 <b>Выберите роль</b>\n\nВыберите, в каком контексте продолжить работу.",
        delay_seconds=60,
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
        await answer_service(
            message,
            "Выберите действие:",
            delay_seconds=60,
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
        await answer_service(
            message,
            "Выберите действие:",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, rp_more_menu(show_role_selector_back=isolated_role)),
        )
    else:
        # Manager roles (KV / KIA / NPN) and legacy Manager
        await answer_service(
            message,
            "Выберите действие:",
            delay_seconds=60,
            reply_markup=private_only_reply_markup(message, manager_more_menu(show_role_selector_back=isolated_role)),
        )


@router.message(lambda m: (m.text or "").strip() in {GD_BTN_BACK_HOME, "Назад в Гл.меню"})
async def gd_back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """GD: return from 'Еще' submenu to main menu."""
    await cmd_menu(message, state, db, config)


@router.message(lambda m: (m.text or "").strip() in {MGR_BTN_BACK_HOME, "Назад в Гл.меню"})
async def mgr_back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Manager: return from 'Еще' submenu."""
    await cmd_menu(message, state, db, config)


@router.message(lambda m: (m.text or "").strip() in {RP_BTN_BACK_HOME, "Назад в Гл.меню"})
async def rp_back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """RP: return from 'Еще' submenu."""
    await cmd_menu(message, state, db, config)


@router.message(lambda m: (m.text or "").strip() == RP_BTN_TEAM)
async def rp_menu_team(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """RP: open 'Команда' submenu."""
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    user = await db.get_user_optional(message.from_user.id) if message.from_user else None
    _, isolated_role = _menu_scope(message.from_user.id if message.from_user else None, user.role if user else None)
    await answer_service(
        message,
        "Выберите сотрудника:",
        delay_seconds=60,
        reply_markup=private_only_reply_markup(message, rp_team_menu(show_role_selector_back=isolated_role)),
    )


@router.message(lambda m: (m.text or "").strip() == OPEN_HELP)
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
    text = _role_guide(role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    unread = await db.count_unread_tasks(message.from_user.id) if message.from_user else 0
    uc = await db.count_unread_by_channel(message.from_user.id) if message.from_user else {}
    gd_ur = await db.count_gd_inbox_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
    gd_inv = await db.count_gd_invoice_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
    gd_ie = await db.count_gd_invoice_end_tasks(message.from_user.id) if message.from_user and role and Role.GD in parse_roles(role) else None
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
            ),
        ),
    )


# =====================================================================
# ВХОДЯЩИЕ ЗАДАЧИ (универсальный обработчик для всех ролей)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие задачи"))
async def inbox_tasks_universal(message: Message, db: Database) -> None:
    """Universal inbox handler for all roles that use '📥 Входящие задачи' button."""
    if not message.from_user:
        return
    if not await _guard_blocked_message(message, db):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)
    if not tasks:
        await answer_service(message, "📥 Входящих задач нет ✅", delay_seconds=60)
        return
    await message.answer(
        f"📥 <b>Входящие задачи</b> ({len(tasks)}):\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(tasks),
    )


# =====================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ (для менеджеров, РП, бухгалтерии)
# =====================================================================

@router.message(
    lambda m: (m.text or "").strip() == MGR_BTN_SYNC
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

    projects = await db.list_recent_projects(limit=10000)
    tasks = await db.list_recent_tasks(limit=50000)

    project_code_by_id: dict[int, str] = {}
    projects_ok = 0
    tasks_ok = 0

    for p in sorted(projects, key=lambda x: int(x["id"])):
        manager_label = ""
        manager_id = p.get("manager_id")
        if manager_id:
            manager_user = await db.get_user_optional(int(manager_id))
            if manager_user:
                manager_label = f"@{manager_user.username}" if manager_user.username else str(manager_user.telegram_id)
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
                    proj = await db.get_project(int(project_id))
                    project_code = str(proj.get("code") or "")
                    if project_code:
                        project_code_by_id[int(project_id)] = project_code
                except Exception:
                    project_code = ""
        await integrations.sheets.upsert_task(t, project_code=project_code)
        tasks_ok += 1

    menu_context = await _menu_context(db, message.from_user.id, active_role or role)
    await answer_service(
        message,
        "✅ Синхронизация завершена.\n"
        f"Проектов: <b>{projects_ok}</b>\n"
        f"Задач: <b>{tasks_ok}</b>",
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
@router.message(lambda m: (m.text or "").strip() == "❌ Отмена")
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
