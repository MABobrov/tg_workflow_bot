from __future__ import annotations

import asyncio
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
    BACK_TO_HOME, OPEN_ACTIONS, OPEN_HELP, actions_menu, main_menu,
    gd_more_menu, GD_BTN_BACK_HOME, GD_BTN_MORE,
    manager_more_menu, MGR_BTN_MORE, MGR_BTN_BACK_HOME, MGR_BTN_SYNC,
    rp_more_menu, rp_team_menu, RP_BTN_MORE, RP_BTN_BACK_HOME, RP_BTN_TEAM,
    tasks_kb,
)
from ..services.integration_hub import IntegrationHub
from ..services.menu_context import build_menu_context
from ..services.notifier import Notifier
from ..services.menu_scope import resolve_active_menu_role
from ..services.sheets_sync import export_to_sheets, import_from_source_sheet
from ..utils import answer_service, parse_roles, private_only_reply_markup, role_label

log = logging.getLogger(__name__)

router = Router()
# Все message-хендлеры работают только в личном чате с ботом.
# my_chat_member (добавление в группу) не затронут — это отдельный тип события.
router.message.filter(F.chat.type == "private")
_GD_LIKE_ROLES = {Role.GD, Role.TD}
_SILENT_MENU_TEXT = "\u2063"


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
    """Return (role, False). No multi-role support."""
    active_role = resolve_active_menu_role(user_id, role_value)
    return active_role or role_value, False


def _has_gd_like_access(role_value: str | None) -> bool:
    return bool(set(parse_roles(role_value)) & _GD_LIKE_ROLES)


def _can_delete_task_entries(role_value: str | None, *, is_admin: bool = False) -> bool:
    roles = set(parse_roles(role_value))
    return is_admin or bool(roles & (_GD_LIKE_ROLES | {Role.RP}))


async def _answer_menu_silent(message: Message, reply_markup: object) -> None:
    await message.answer(
        _SILENT_MENU_TEXT,
        reply_markup=private_only_reply_markup(message, reply_markup),
    )


async def _show_main_menu(
    message: Message,
    db: Database,
    config: Config,
    *,
    role: str | None,
    isolated_role: bool = False,
    silent: bool = False,
) -> None:
    if not message.from_user:
        return
    menu_context = await _menu_context(db, message.from_user.id, role)
    menu = main_menu(
        role,
        is_admin=message.from_user.id in (config.admin_ids or set()),
        isolated_role=isolated_role,
        **menu_context,
    )
    if silent:
        await _answer_menu_silent(message, menu)
        return
    await message.answer(
        "✅ Меню обновлено.",
        reply_markup=private_only_reply_markup(message, menu),
    )


async def _send_zamery_stats(message: Message, db: Database, for_user: int | None = None) -> None:
    """Шлёт ОТДЕЛЬНЫМ сообщением «👔 Замеры по менеджерам» (2 мес.) следом за стартовой
    картой РП/ГД/замерщика (owner 25.06). for_user=<id> — его замеры; None — агрегат
    всех замерщиков. Тихо при пусто/ошибке — не роняет основной поток."""
    try:
        from ..zamery_start_card import build_zamery_manager_stats_card
        txt = await build_zamery_manager_stats_card(db, for_user=for_user)
        if txt:
            await answer_service(message, txt, delay_seconds=0)
    except Exception:
        log.exception("zamery manager-stats followup failed")


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
        if show_all:
            # Краткий обзор для сводной справки ГД/РП по всем ролям —
            # детальная версия раздула бы сообщение за лимит Telegram 4096.
            sections.append(
                "\n<b>Ваши сценарии (Менеджер)</b>\n"
                "• «Проверить КП / Счет» — КП на проверку РП + ответы РП.\n"
                "• «Счет в Работу» — расчёт и отправка счёта ГД на оплату.\n"
                "• «Счет End» — закрытие счёта (Счёт ОК / ЭДО / долгов нет).\n"
                "• «Замеры» — заявки замерщику и его график.\n"
                "• «Бухгалтерия (Док./ЭДО)» — запросы бухгалтеру.\n"
                "• «Мои Счета», «Монтажная группа», «Чат с РП», «⏰ Напомнить».\n"
                "• «Ещё»: 💰 Финансы (ЗП/аванс/депозит), 🏦 Кредит, 📞 Связь с ГД, "
                "Поиск Счета, 📋 Все задачи, Справка.\n"
            )
        else:
            # Детальная инструкция менеджеру (ТЗ 17.07): каждая кнопка +
            # варианты сюжета, кратко и простым языком.
            sections.append(
                "\n<b>Ваши кнопки (Менеджер)</b>\n"
                "\n<b>Главное меню</b>\n"
                "• «Задачи / Лид в проект» — входящие задачи и лиды от РП; 🔴N — новые. "
                "Нажмите задачу — откроется карточка с действиями.\n"
                "• «Проверить КП / Счет» — отправить КП на проверку РП. Два пути: по своему "
                "лиду (файлы → сумма → тип клиента → комментарий) или «Новый клиент» "
                "(полная анкета: контрагент, адрес, сумма, тип оплаты, срок). Там же "
                "«Ответы РП»: ⌛ ждёт / 📥 счёт выставлен / ✅ подтверждено.\n"
                "• «Счет в Работу» — отправить счёт ГД на оплату: счёт из списка или номер "
                "вручную → чей клиент (свой 50/50 или лид ГД 75/25) → срок в днях → 4 суммы "
                "расчёта → сводка → документы (можно пропустить) → «Отправить ГД». "
                "«В работу» счёт перейдёт после подтверждения ГД.\n"
                "• «Счет End» — закрыть счёт: выберите из активных → бот покажет условия "
                "(монтажник «Счёт ОК», закрывающие по ЭДО, долгов нет; у кредитных ЭДО не "
                "требуется) → пояснение → решение ГД. Если ГД закрыл «с задачами» — "
                "доделайте их, до этого ЗП по счёту заблокирована.\n"
                "• «Замеры» — заявки на замер: список своих, «➕ Новая заявка» (адрес → "
                "работы → контакт → км от МКАД → дата → м²; стоимость бот посчитает сам, "
                "можно приложить фото) или «📅 График замерщика» — запись в свободный день.\n"
                "• «Бухгалтерия (Док./ЭДО)» — запрос бухгалтеру: счёт по ЭДО / закрывающие / "
                "УПД поставщика / другой вопрос; можно приложить файлы.\n"
                "• «Монтажная группа» — переписка с монтажниками: с привязкой к счёту или "
                "без; «Переписка» — история, «Написать» — текст/файл в рабочий чат.\n"
                "• «Чат с РП» — то же самое, канал РП.\n"
                "• «Мои Счета» — сводка и список ваших счетов; в карточке счёта — "
                "«Счёт End», «Материалы», чат.\n"
                "• «⏰ Напомнить» — самонапоминание: текст → когда (через 1/3 часа, сегодня "
                "18:00, завтра 10:00 или своя дата). Отмена — в «📋 Все задачи».\n"
                "• «Синхронизация данных» — обмен с Google-таблицей и ваша сводная карточка: "
                "счета, финансы, задачи.\n"
                "• «Отмена» или /cancel — сброс любого начатого ввода.\n"
                "\n<b>Подменю «Еще»</b>\n"
                "• «💰 Финансы» — ваши аванс и депозит. Внутри: «Запрос ЗП» (по счетам End, "
                "доля считается сама; 🔒 — по счёту долг, ЗП пока нельзя), «Расход депозита», "
                "«Наполнить ЗП из аванса», «Депо → Аванс», согласие с перерасчётом прибыли.\n"
                "• «🏦 Кредит» — кредит-кошелёк: остаток и движения. «Расход кредита»: "
                "с привязкой к счёту (ляжет в затраты счёта) или без (в баланс компании). "
                "«Кредит-чат» — переписка с ГД.\n"
                "• «📞 Связь с ГД» — несрочный вопрос ГД: можно привязать счёт и приложить "
                "файлы.\n"
                "• «Поиск Счета КВ / КИА / НПН» — любой счёт по номеру или части адреса → "
                "карточка.\n"
                "• «📋 Все задачи» — активные и завершённые задачи; здесь же отмена "
                "напоминаний.\n"
                "• «Справка» — эта инструкция.\n"
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
            "• «📞 Связь с ГД» — сообщение/вопрос ГД (не срочно).\n"
            "• Подменю «Ещё»: 🎯 Лид в проект, 🔍 Поиск счёта.\n"
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
            "• «📞 Связь с ГД» — сообщение/вопрос ГД.\n"
        )
    if Role.ZAMERY in roles or show_all:
        sections.append(
            "\n<b>Ваши сценарии (Замерщик)</b>\n"
            "• «📋 Заявка на замер» — входящие заявки на замер от Отд.Продаж и ГД (🔴 — новые).\n"
            "• «📋 Мои замеры» — список выполненных замеров со статусом оплаты.\n"
            "• «📅 График замеров» — расписание на 5 недель: бронь слотов и выходные.\n"
            "• «💰 Оплата замеров» — замеры по статусам «К оплате / На проверке / Оплачено»; отправка пакета ГД на выплату.\n"
            "• «🚨 Срочно ГД» — двустороннее сообщение с ГД.\n"
            "• «🔄 Синхронизация данных» — обновить журнал замеров в Google‑таблице.\n"
            "• «📋 Все задачи» — все ваши задачи (входящие и в работе).\n"
            "• Иногда бот присылает карточку «Кто отправлял тебя на замер?» — нажмите менеджера (Кирилл / Паша / Илья) или «🤷 Не помню».\n"
        )
    if _has_gd_like_access(role) or show_all:
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

    if not user.is_active:
        await message.answer("⛔️ Ваш доступ к боту заблокирован. Обратитесь к администратору.")
        return

    text = (
        "👋 Привет! Я бот для структурирования внутренних заявок и статусов проектов.\n\n"
        f"Ваш Telegram ID: <code>{u.id}</code>\n"
    )
    if role:
        text += f"Ваша роль: <b>{role_label(role)}</b>\n\nОткройте меню и выберите действие."
    else:
        text += (
            "Ваша роль пока не назначена.\n"
            "Администратор должен выдать роль командой: /setrole <code>@username</code> <code>role</code>\n\n"
            "После назначения нажмите «🔄 Обновить меню»."
        )

    is_admin = u.id in (config.admin_ids or set())
    menu_context = await _menu_context(db, u.id, role)
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=is_admin, **menu_context),
        ),
    )

    # Welcome-card для менеджеров (тот же формат что в /menu и после Sync).
    active_role_post_start = resolve_active_menu_role(u.id, role) if role else None
    if (active_role_post_start or role) in MANAGER_ROLES:
        try:
            metrics = await db.get_manager_dashboard_metrics(u.id)
            from ..utils import format_manager_sync_card, with_manager_tasks_section
            await answer_service(message, await with_manager_tasks_section(db, u.id, format_manager_sync_card(metrics)), delay_seconds=0)
        except Exception:
            log.exception("cmd_start: manager welcome-card failed")
    elif (active_role_post_start or role) == Role.RP:
        try:
            from ..rp_start_card import build_rp_start_card_text
            await answer_service(message, await build_rp_start_card_text(db, config, u.id), delay_seconds=0)
        except Exception:
            log.exception("cmd_start: rp welcome-card failed")
        await _send_zamery_stats(message, db)
    elif (active_role_post_start or role) == Role.INSTALLER:
        try:
            metrics = await db.get_installer_dashboard_metrics(u.id)
            from ..utils import format_installer_sync_card
            await answer_service(message, format_installer_sync_card(metrics), delay_seconds=0)
        except Exception:
            log.exception("cmd_start: installer welcome-card failed")
    elif (active_role_post_start or role) == Role.ACCOUNTING:
        try:
            from .accounting_new import build_acc_start_card_text
            await answer_service(message, await build_acc_start_card_text(db, u.id), delay_seconds=0)
        except Exception:
            log.exception("cmd_start: accounting welcome-card failed")
    elif (active_role_post_start or role) == Role.ZAMERY:
        try:
            from ..zamery_start_card import build_zamery_start_card_text
            await answer_service(message, await build_zamery_start_card_text(db, config, u.id), delay_seconds=0)
        except Exception:
            log.exception("cmd_start: zamery welcome-card failed")
        await _send_zamery_stats(message, db, for_user=u.id)

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
    is_refresh = (message.text or "").strip() == "🔄 Обновить меню"

    # Для менеджеров показываем welcome-card по образцу sync (без обращения к Sheets).
    active_role = resolve_active_menu_role(u.id, role) if role else None
    if (active_role or role) in MANAGER_ROLES:
        try:
            metrics = await db.get_manager_dashboard_metrics(u.id)
            from ..utils import format_manager_sync_card, with_manager_tasks_section
            text = await with_manager_tasks_section(db, u.id, format_manager_sync_card(metrics))
            is_admin = u.id in (config.admin_ids or set())
            menu_context = await _menu_context(db, u.id, active_role or role)
            await answer_service(
                message,
                text,
                delay_seconds=0,
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
        except Exception:
            log.exception("cmd_menu: manager welcome-card build failed, falling back")

    if (active_role or role) == Role.INSTALLER:
        try:
            metrics = await db.get_installer_dashboard_metrics(u.id)
            from ..utils import format_installer_sync_card
            text = format_installer_sync_card(metrics)
            is_admin = u.id in (config.admin_ids or set())
            menu_context = await _menu_context(db, u.id, active_role or role)
            await answer_service(
                message,
                text,
                delay_seconds=0,
                reply_markup=private_only_reply_markup(
                    message,
                    main_menu(active_role or role, is_admin=is_admin, **menu_context),
                ),
            )
            return
        except Exception:
            log.exception("cmd_menu: installer welcome-card build failed, falling back")

    if (active_role or role) == Role.ACCOUNTING:
        try:
            from .accounting_new import build_acc_start_card_text
            text = await build_acc_start_card_text(db, u.id)
            is_admin = u.id in (config.admin_ids or set())
            menu_context = await _menu_context(db, u.id, active_role or role)
            await answer_service(
                message,
                text,
                delay_seconds=0,
                reply_markup=private_only_reply_markup(
                    message,
                    main_menu(active_role or role, is_admin=is_admin, **menu_context),
                ),
            )
            return
        except Exception:
            log.exception("cmd_menu: accounting welcome-card build failed, falling back")

    if (active_role or role) == Role.ZAMERY:
        try:
            from ..zamery_start_card import build_zamery_start_card_text
            text = await build_zamery_start_card_text(db, config, u.id)
            is_admin = u.id in (config.admin_ids or set())
            menu_context = await _menu_context(db, u.id, active_role or role)
            await answer_service(
                message,
                text,
                delay_seconds=0,
                reply_markup=private_only_reply_markup(
                    message,
                    main_menu(active_role or role, is_admin=is_admin, **menu_context),
                ),
            )
            await _send_zamery_stats(message, db, for_user=u.id)
            return
        except Exception:
            log.exception("cmd_menu: zamery welcome-card build failed, falling back")

    if is_refresh:
        await _show_main_menu(message, db, config, role=role, silent=True)
        return
    await _show_main_menu(message, db, config, role=role, silent=False)


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
    guide_role = role if (menu_role and menu_role in (*_GD_LIKE_ROLES, Role.RP)) else menu_role
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
    await _answer_menu_silent(
        message,
        actions_menu(
            menu_role,
            is_admin=is_admin,
            show_role_selector_back=isolated_role,
        ),
    )


@router.message(lambda m: (m.text or "").strip() == BACK_TO_HOME)
async def back_to_home(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    user = await db.get_user_optional(message.from_user.id) if message.from_user else None
    await _show_main_menu(message, db, config, role=user.role if user else None, silent=True)


def _matches_more_button(text: str | None) -> bool:
    """Match «📂 Ещё» (and variants) with optional 🔴N badge suffix."""
    t = (text or "").strip()
    if not t:
        return False
    for base in (GD_BTN_MORE, MGR_BTN_MORE, RP_BTN_MORE, "Еще"):
        if t == base or t.startswith(f"{base} "):
            return True
    return False


@router.message(lambda m: _matches_more_button(m.text))
async def menu_more_universal(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Unified 'More/Ещё' handler — dispatches to correct submenu based on active role.

    GD_BTN_MORE == MGR_BTN_MORE == RP_BTN_MORE == "📂 Ещё", so a single handler
    is needed to avoid aiogram selecting whichever was registered first.

    Кнопка может приходить с бейджем «📂 Ещё 🔴N» — matcher это учитывает.
    """
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role = resolve_active_menu_role(u.id, user.role if user else None)

    if role in _GD_LIKE_ROLES:
        _is_adm = bool(u.id in (config.admin_ids or set()))
        _uc = await db.count_unread_by_channel(u.id)
        _gd_ie = await db.count_gd_invoice_end_tasks(u.id)
        _gd_tot = await db.count_gd_more_total_open_tasks(u.id)
        _gd_cred = await db.count_credit_payment_requests_by_channel()
        await _answer_menu_silent(
            message,
            gd_more_menu(
                is_admin=_is_adm,
                unread_channels=_uc,
                gd_invoice_end_unread=_gd_ie,
                gd_total_open_tasks=_gd_tot,
                credit_channels=_gd_cred,
            ),
        )
    elif role == Role.RP:
        await _answer_menu_silent(message, rp_more_menu())
    else:
        # Финансы-рефактор 02.06: балансы аванс/депозит ушли в сводную карточку
        # «💰 Финансы» (inline-действия), меню «Ещё» больше от них не зависит и
        # показывается всегда (не подменяется submenu).
        _mgr_cred = await db.count_credit_payment_requests_for_owner(u.id)
        _mgr_recalc = await db.count_recalc_confirm_tasks(u.id)
        await _answer_menu_silent(
            message,
            manager_more_menu(credit_count=_mgr_cred, recalc_count=_mgr_recalc),
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
        await _show_main_menu(
            message,
            db,
            config,
            role=menu_role,
            isolated_role=True,
            silent=True,
        )
        return
    # Fallback: full menu reset (single role or no active role)
    await _show_main_menu(message, db, config, role=role, silent=True)


@router.message(lambda m: (m.text or "").strip() == RP_BTN_TEAM)
async def rp_menu_team(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """RP: open 'Команда' submenu."""
    await state.clear()
    if not await _guard_blocked_message(message, db):
        return
    user = await db.get_user_optional(message.from_user.id) if message.from_user else None
    _, isolated_role = _menu_scope(message.from_user.id if message.from_user else None, user.role if user else None)
    await _answer_menu_silent(
        message,
        rp_team_menu(show_role_selector_back=isolated_role),
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
    guide_role = role if (menu_role and menu_role in (*_GD_LIKE_ROLES, Role.RP)) else menu_role
    text = _role_guide(guide_role)
    if is_admin:
        text += "\n\n<b>Админ-команды</b>\n• <code>/admin_help</code> — инструкция администратора\n• <code>/stats</code> — статистика\n• <code>/users</code> — сотрудники"
    _uid_info = message.from_user.id if message.from_user else None
    unread = await db.count_unread_tasks(_uid_info) if _uid_info else 0
    uc = await db.count_unread_by_channel(_uid_info) if _uid_info else {}
    _parsed_info = parse_roles(role) if role else []
    has_gd_access = bool(set(_parsed_info) & _GD_LIKE_ROLES)
    gd_ur = await db.count_gd_inbox_tasks(_uid_info) if _uid_info and has_gd_access else None
    gd_inv = await db.count_gd_invoice_tasks(_uid_info) if _uid_info and has_gd_access else None
    gd_ie = await db.count_gd_invoice_end_tasks(_uid_info) if _uid_info and has_gd_access else None
    gd_tot = await db.count_gd_more_total_open_tasks(_uid_info) if _uid_info and has_gd_access else None
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
                gd_total_open_tasks=gd_tot,
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
    uid = message.from_user.id
    # Req 34: исходящие не дублируются во входящие отправителя.
    # «Не срочно ГД» и подобные кнопки отправляют задачу адресату; инициатор
    # копию во входящие не получает. Здесь только входящие (assigned_to=uid).
    tasks_raw = await db.list_tasks_for_user(uid, limit=30)
    # Исключаем INVOICE_PAYMENT (только для ГД)
    # CHECK_KP исключаем только для менеджеров (они создают, а не принимают)
    from ..enums import TaskType
    _excluded = {TaskType.INVOICE_PAYMENT, TaskType.SELF_REMINDER}
    if _u and _u.role and ("manager" in (_u.role or "") or "rp" in (_u.role or "")):
        _excluded.add(TaskType.CHECK_KP)
        _excluded.add(TaskType.ORDER_MATERIALS)
        _excluded.add(TaskType.ORDER_PROFILE)
        _excluded.add(TaskType.ORDER_GLASS)
        _excluded.add(TaskType.EDO_REQUEST)
    tasks = [t for t in tasks_raw if t.get("type") not in _excluded]
    if not tasks:
        await answer_service(message, "📥 Задач нет ✅", delay_seconds=60)
        return
    header = f"📥 <b>Задачи</b> ({len(tasks)})\n\nНажмите на задачу для просмотра:"
    await message.answer(
        header,
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


# =====================================================================
# ВСЕ ЗАДАЧИ (список для всех ролей)
# =====================================================================

def _matches_all_tasks_button(text: str | None) -> bool:
    """Match «📋 Все задачи» с опциональным «🔴N» бейджем."""
    t = (text or "").strip()
    return t == "📋 Все задачи" or t.startswith("📋 Все задачи ")


@router.message(lambda m: _matches_all_tasks_button(m.text))
async def all_tasks_list(message: Message, db: Database, config: Config) -> None:
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

    # Для ГД исключаем типы покрытые отдельными кнопками главного меню
    # (invoice_payment / payment_confirm / invoice_end / zp_installer)
    # — чтобы «Все задачи» не дублировали их.
    _user = await db.get_user_optional(uid)
    _roles_str = (_user.role if _user else "") or ""
    is_gd_like = "gd" in _roles_str or "td" in _roles_str
    _gd_excluded_types = {"invoice_payment", "payment_confirm", "invoice_end", "zp_installer"}

    def _gd_filter(t: dict) -> bool:
        if not is_gd_like:
            return True
        # Скрываем только если задача назначена ГД (своя) и тип покрыт другой кнопкой.
        # Свои исходящие задачи (created_by=uid) показываем всегда.
        if int(t.get("assigned_to") or 0) != uid:
            return True
        return t.get("type") not in _gd_excluded_types

    active_tasks = []
    seen_ids: set[int] = set()
    for t in active:
        if int(t["id"]) not in seen_ids and _gd_filter(t):
            active_tasks.append(t)
            seen_ids.add(int(t["id"]))
    for t in created:
        if int(t["id"]) not in seen_ids and _gd_filter(t):
            active_tasks.append(t)
            seen_ids.add(int(t["id"]))

    closed_count = len(closed)

    if not active_tasks and not closed_count:
        # ГД: даже без задач показываем вход в самонапоминалку («⏰ Поставить
        # напоминание»), иначе точка входа пропадёт на пустом экране.
        if is_gd_like:
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            b = InlineKeyboardBuilder()
            b.button(text="⏰ Поставить напоминание", callback_data="selfrem_new")
            b.button(text="⬅️ Назад", callback_data="nav:home")
            b.adjust(1)
            await message.answer(
                "📋 <b>Все задачи</b>\nАктивных задач нет.\n\n"
                "Можно поставить себе напоминание:",
                reply_markup=b.as_markup(),
            )
            return
        await answer_service(message, "📋 Задач нет.", delay_seconds=60)
        return

    # GD and RP get delete buttons
    is_admin = uid in (config.admin_ids or set())
    user_role = (await db.get_user_optional(uid) or type("U", (), {"role": None})).role
    can_delete = _can_delete_task_entries(user_role, is_admin=is_admin)

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = tasks_kb(active_tasks, back_callback="nav:home", show_delete=can_delete)
    # Доп. пункты под списком: «Завершённые» (всем при наличии) +
    # «⏰ Поставить напоминание» — вход в самонапоминалку для ГД отдельным
    # пунктом внутри «Все задачи» (у менеджера/РП вход — reply-кнопка в меню).
    if closed_count or is_gd_like:
        b = InlineKeyboardBuilder(markup=kb.inline_keyboard)
        if closed_count:
            b.button(text=f"📦 Завершённые ({closed_count})", callback_data="all_tasks:closed")
        if is_gd_like:
            b.button(text="⏰ Поставить напоминание", callback_data="selfrem_new")
        b.adjust(1)
        kb = b.as_markup()

    await message.answer(
        f"📋 <b>Все задачи</b>\n"
        f"Активных: {len(active_tasks)} | Закрытых: {closed_count}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=kb,
    )


@router.callback_query(F.data == "all_tasks:closed")
async def all_tasks_closed(cb: CallbackQuery, db: Database, config: Config) -> None:
    """Подпапка: завершённые задачи."""
    if not cb.from_user:
        return
    await cb.answer()
    uid = cb.from_user.id
    closed = await db.list_tasks_for_user(uid, statuses=("done", "rejected"), limit=20)
    if not closed:
        await cb.message.answer("📦 Нет завершённых задач.")  # type: ignore[union-attr]
        return

    is_admin = uid in (config.admin_ids or set())
    user_role = (await db.get_user_optional(uid) or type("U", (), {"role": None})).role
    can_delete = _can_delete_task_entries(user_role, is_admin=is_admin)

    await cb.message.answer(  # type: ignore[union-attr]
        f"📦 <b>Завершённые задачи</b> ({len(closed)})",
        reply_markup=tasks_kb(closed, back_callback="nav:home", show_delete=can_delete),
    )


# =====================================================================
# СИНХРОНИЗАЦИЯ ДАННЫХ (для менеджеров, РП, бухгалтерии)
# =====================================================================

@router.message(
    lambda m: (m.text or "").strip() in {MGR_BTN_SYNC, "🔄 Синхронизация данных"}
)
async def sync_data_non_gd(
    message: Message, db: Database, config: Config, integrations: IntegrationHub, notifier: Notifier,
) -> None:
    """Sync data with Google Sheets for non-GD roles.

    GD users are handled by gd.py's gd_sync_data — we import and call it directly.
    """
    if not message.from_user:
        return
    user = await db.get_user_optional(message.from_user.id)
    if not user:
        return

    role = user.role
    active_role = resolve_active_menu_role(message.from_user.id, role)
    if active_role in _GD_LIKE_ROLES:
        # Delegate to GD-specific sync handler
        from .gd import gd_sync_data
        await gd_sync_data(message, db, config, integrations, notifier)
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

    # Typing-индикатор для всего флоу (как у ГД, без промежуточного сообщения).
    try:
        await message.bot.send_chat_action(message.chat.id, "typing")  # type: ignore[union-attr]
    except Exception:
        pass

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
        amocrm_user_map=getattr(config, "amocrm_user_map", None),
        amocrm=integrations.amocrm,
    )

    menu_context = await _menu_context(db, message.from_user.id, active_role or role)

    # Менеджеры (КВ / КИА / НПН) — карточка по образцу ГД sync_data_card_v2
    if (active_role or role) in MANAGER_ROLES:
        try:
            metrics = await db.get_manager_dashboard_metrics(message.from_user.id)
            from ..utils import format_manager_sync_card, with_manager_tasks_section
            text = await with_manager_tasks_section(db, message.from_user.id, format_manager_sync_card(metrics))
        except Exception:
            log.exception("manager_sync: build dashboard card failed")
            text = (
                "✅ Синхронизация завершена.\n"
                f"📥 Импорт из ОП: <b>{imported_ok}</b>\n"
                f"Проектов: <b>{stats['projects']}</b>\n"
                f"Задач: <b>{stats['tasks']}</b>\n"
                f"Счетов: <b>{stats['invoices']}</b>"
            )
        await answer_service(
            message,
            text,
            delay_seconds=0,
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

    # РП — полная стартовая карточка «Твоя Атмосфера» (как на /start; паритет с ГД,
    # запрос user'а 31.05). Авто-рассылка 09:00 в daily_sync — 3-секционная отдельно.
    if (active_role or role) == Role.RP:
        try:
            from ..rp_start_card import build_rp_start_card_text
            text = await build_rp_start_card_text(db, config, message.from_user.id)
        except Exception:
            log.exception("rp_sync: build dashboard card failed")
            text = (
                "✅ Синхронизация завершена.\n"
                f"📥 Импорт из ОП: <b>{imported_ok}</b>\n"
                f"Проектов: <b>{stats['projects']}</b>\n"
                f"Задач: <b>{stats['tasks']}</b>\n"
                f"Счетов: <b>{stats['invoices']}</b>"
            )
        await answer_service(
            message,
            text,
            delay_seconds=0,
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
        await _send_zamery_stats(message, db)
        return

    # Монтажник — карточка с Счёт End / В работе / ЗП помесячно / Баланс аванса.
    # Источники строго по [[project_installer_sync_card_deploy_20260522]].
    if (active_role or role) == Role.INSTALLER:
        try:
            metrics = await db.get_installer_dashboard_metrics(message.from_user.id)
            from ..utils import format_installer_sync_card
            text = format_installer_sync_card(metrics)
        except Exception:
            log.exception("installer_sync: build dashboard card failed")
            text = (
                "✅ Синхронизация завершена.\n"
                f"📥 Импорт из ОП: <b>{imported_ok}</b>\n"
                f"Проектов: <b>{stats['projects']}</b>\n"
                f"Задач: <b>{stats['tasks']}</b>\n"
                f"Счетов: <b>{stats['invoices']}</b>"
            )
        await answer_service(
            message,
            text,
            delay_seconds=0,
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

    # Замерщик — карточка-календарь «График замеров» (2 месяца) + зеркало на лист
    # leads (O:U, один месяц через month_marks). Статистика по менеджерам — отдельным
    # сообщением следом.
    if (active_role or role) == Role.ZAMERY:
        try:
            from ..zamery_start_card import month_marks, build_zamery_start_card_text
            _y, _m, _busy, _off = await month_marks(db, message.from_user.id)
            text = await build_zamery_start_card_text(db, config, message.from_user.id)
        except Exception:
            log.exception("zamery_sync: build calendar card failed")
            _y = _m = None
            _busy = _off = set()
            text = (
                "✅ Синхронизация завершена.\n"
                f"📥 Импорт из ОП: <b>{imported_ok}</b>"
            )
        if _y is not None and integrations.sheets:
            try:
                await asyncio.to_thread(
                    integrations.sheets.upsert_zamery_calendar_sync,
                    _y, _m, _busy, _off,
                )
            except Exception:
                log.exception("zamery_sync: write calendar to leads sheet failed")
        # Журнал заявок замерщика на лист leads (W:AG) — ВЕСЬ журнал (все заявки).
        # НЕ месячное окно: upsert_zamery_journal_sync перезаписывает блок целиком,
        # поэтому узкое окно затирало бы исторические замеры прошлых месяцев.
        if integrations.sheets:
            try:
                _d_from = "2000-01-01"
                _d_to = "2100-12-31"
                _journal = await db.list_zamery_journal(
                    message.from_user.id, _d_from, _d_to,
                )
                await asyncio.to_thread(
                    integrations.sheets.upsert_zamery_journal_sync, _journal,
                )
            except Exception:
                log.exception("zamery_sync: write journal to leads sheet failed")
        await answer_service(
            message,
            text,
            delay_seconds=0,
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
        await _send_zamery_stats(message, db, for_user=message.from_user.id)
        return

    await answer_service(
        message,
        "✅ Синхронизация завершена.\n"
        f"📥 Импорт из ОП: <b>{imported_ok}</b>\n"
        f"Проектов: <b>{stats['projects']}</b>\n"
        f"Задач: <b>{stats['tasks']}</b>\n"
        f"Счетов: <b>{stats['invoices']}</b>",
        delay_seconds=0,
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
