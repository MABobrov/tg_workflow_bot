from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram import html
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import ChatMemberUpdated, Message, ReplyKeyboardRemove
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import AdminRoleCb, AdminUserCb
from ..config import Config
from ..db import Database
from ..enums import Role
from ..keyboards import BACK_TO_HOME, OPEN_ACTIONS, OPEN_HELP, actions_menu, main_menu
from ..utils import parse_roles, private_only_reply_markup, role_label

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


def _new_user_admin_kb(user_id: int) -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    for role, label in (
        (Role.MANAGER, "Менеджер"),
        (Role.RP, "РП"),
        (Role.TD, "ТД"),
        (Role.ACCOUNTING, "Бухгалтерия"),
        (Role.INSTALLER, "Монтажник"),
        (Role.DRIVER, "Водитель"),
        (Role.LOADER, "Грузчик"),
        (Role.TINTER, "Тонировщик"),
        (Role.GD, "Ген.дир"),
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
    if Role.MANAGER in roles:
        sections.append(
            "\n<b>Ваши сценарии (Менеджер)</b>\n"
            "• «Проверить КП/Запросить документы» — единый старт флоу.\n"
            "После создания проекта работайте через карточку проекта: все последующие шаги и задачи привязаны к нему.\n"
            "• «💰 Оплата поступила» — отправить оплату на подтверждение ТД.\n"
            "Заполните: проект, сумма, тип оплаты, этап, дата, комментарий, платёжка.\n"
            "• «📄 Док. / ЭДО» — запрос в бухгалтерию на документы.\n"
            "• «🆘 Проблема / вопрос» — сигнал РП по проекту.\n"
            "• «🏁 Счёт End» — финальное закрытие проекта.\n"
            "• «🚨 Срочно ГД» — срочный вопрос Ген.диру.\n"
        )
    if Role.RP in roles:
        sections.append(
            "\n<b>Ваши сценарии (РП)</b>\n"
            "• «📥 Входящие задачи» — открыть список задач.\n"
            "• Внутри задачи используйте кнопки:\n"
            "«⏳ Взять в работу», «✅ Завершить», «❌ Отклонить».\n"
            "• «🗂 Проекты» / «📌 Поиск проекта» — просмотр карточек проектов.\n"
            "• «📦 Заказ материалов» — заказать профиль/стекло/ЛДСП/ГКЛ.\n"
            "• «🚚 Заявка на доставку» — отправить заявку водителю.\n"
            "• «🎯 Распределить лид» — назначить лид менеджеру.\n"
            "• «🎨 Заявка на тонировку» — отправить тонировщику.\n"
            "• «🆘 Проблема / простой» — эскалация вопроса.\n"
            "• «🚨 Срочно ГД» — срочный вопрос Ген.диру.\n"
        )
    if Role.TD in roles:
        sections.append(
            "\n<b>Ваши сценарии (ТД)</b>\n"
            "• «✅ Подтверждение оплат» — задачи на подтверждение оплаты клиентов.\n"
            "• По оплате доступны спец-кнопки:\n"
            "«✅ Оплата подтверждена» или «⚠️ Нужна доплата».\n"
            "• «💸 Оплата поставщику» — зафиксировать оплату поставщику.\n"
            "Заполните: проект, поставщик, сумма, № счёта, платёжка.\n"
            "• «📥 Входящие задачи» — все прочие задачи.\n"
            "• «📌 Поиск проекта» — найти и открыть карточку проекта.\n"
            "• «🚨 Срочно ГД» — срочный вопрос Ген.диру.\n"
        )
    if Role.ACCOUNTING in roles:
        sections.append(
            "\n<b>Ваши сценарии (Бухгалтерия)</b>\n"
            "• «📄 Закрывающие» — входящие задачи по закрывающим документам.\n"
            "• «📨 Менеджеру (Имя)» — запросить у менеджера недостающую информацию.\n"
            "Можно приложить файлы/скриншоты.\n"
            "• «📥 Входящие задачи» — общий список задач.\n"
            "• «🚨 Срочно ГД» — срочный вопрос Ген.диру.\n"
        )
    if Role.INSTALLER in roles:
        sections.append(
            "\n<b>Ваши сценарии (Монтажник)</b>\n"
            "• «📝 Отчёт за день» — ежедневный отчёт по объекту.\n"
            "Заполните: объект, что сделано, часы, проблемы, вложения.\n"
            "• «✅ Счёт ОК» — подтверждение завершения монтажа.\n"
            "Заполните: объект, дата окончания, комментарий по допработам.\n"
            "• «🆘 Проблема / простой» — сигнал по объекту в РП.\n"
            "• «📌 Мои объекты» — список последних объектов.\n"
            "• «🚨 Срочно ГД» — срочный вопрос Ген.диру.\n"
        )
    if Role.GD in roles:
        sections.append(
            "\n<b>Ваши сценарии (Ген.дир)</b>\n"
            "• «📥 Входящие задачи» — срочные запросы и поручения.\n"
            "• «📌 Поиск проекта» — поиск карточек проектов.\n"
            "• «🚨 Срочно ГД» — быстрый канал срочных вопросов.\n"
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
            "• «🚨 Срочно ГД» — срочная эскалация Ген.диру.\n"
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
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
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
    await message.answer(f"Ваш Telegram ID: <code>{u.id}</code>")


@router.message(Command("menu"))
@router.message(lambda m: (m.text or "").strip() == "🔄 Обновить меню")
async def cmd_menu(message: Message, db: Database, config: Config) -> None:
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = user.role if user else None
    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        "✅ Меню обновлено.",
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
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
    text = _role_guide(role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
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
    if not role:
        await message.answer(
            "Роль пока не назначена. Попросите администратора назначить роль и нажмите «🔄 Обновить меню».",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(None, is_admin=u.id in (config.admin_ids or set())),
            ),
        )
        return
    is_admin = u.id in (config.admin_ids or set())
    await message.answer(
        "Выберите действие:",
        reply_markup=private_only_reply_markup(message, actions_menu(role, is_admin=is_admin)),
    )


@router.message(lambda m: (m.text or "").strip() == BACK_TO_HOME)
async def back_to_home(message: Message, db: Database, config: Config) -> None:
    await cmd_menu(message, db, config)


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
    text = _role_guide(role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
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
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    await message.answer(
        "Операция отменена. Выберите следующее действие в меню.",
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin)),
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
