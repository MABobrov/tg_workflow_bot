"""Last-resort self-healing для «осиротевших» reply-кнопок меню.

Проблема. FSM-состояние в этом боте живёт в памяти процесса (`dp = Dispatcher()`
без storage → дефолтный MemoryStorage). При рестарте бота (деплой/краш/ребут VDS)
состояния ВСЕХ пользователей обнуляются. Reply-клавиатура при этом остаётся на
экране у пользователя, но её кнопки часто завязаны на FSM-состояние (напр.
подменю кредитного кошелька ГД требует `ChatProxySG.menu`). После сброса
состояния такие кнопки молча перестают работать — клавиатура «живая на вид,
мёртвая по факту», и пользователь застревает (даже «⬅️ Назад» не отвечает).

Решение. Этот роутер подключается ПОСЛЕДНИМ (после всех остальных в main.py).
Он ловит известные меню-кнопки, пришедшие БЕЗ активного состояния
(`StateFilter(None)`) и не обработанные никем выше, и мягко возвращает
пользователя в корневое меню вместо тишины. Активные FSM-потоки не затрагивает
(там состояние != None), поэтому не мешает штатной логике.

Здесь же (owner 26.07) закрыты два случая, когда текст пользователя пропадал
молча — см. блоки ниже.
"""
from __future__ import annotations

import html as _html
import logging

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..keyboards import (
    GD_BTN_CRED_BAL,
    GD_BTN_CRED_SPEND,
    main_menu,
    manager_chat_submenu,
    rp_chat_gd_submenu,
    rp_chat_submenu,
    rp_montazh_submenu,
)
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..states import ManagerChatProxySG

log = logging.getLogger(__name__)
router = Router(name="fallback")

# Reply-кнопки подменю чат-прокси / финанс-канала ГД (gd_chat_submenu*,
# gd_sales_submenu). Все требуют состояния ChatProxySG.menu и «умирают» после
# его сброса. Список держать в синхроне с keyboards.gd_chat_submenu_finance и др.
_ORPHAN_MENU_BUTTONS: frozenset[str] = frozenset({
    "📖 Переписка",
    "✏️ Написать",
    "📋 Задачи",
    "📨 Входящие",
    "📊 Отчёт",
    GD_BTN_CRED_BAL,      # 🏦 Баланс кошелька
    GD_BTN_CRED_SPEND,    # ➕ Расход кредита
    "⬅️ Назад",
    "◀️ Назад",           # защитно: иная стрелка при рендере клиента
})


@router.message(StateFilter(None), F.chat.type == "private", F.text)
async def stateless_menu_recovery(
    message: Message, db: Database, config: Config
) -> None:
    """Текст в личке без активного состояния → вернуть пользователя в меню.

    Срабатывает last-resort: только если сообщение не обработал ни один хендлер
    выше И у пользователя нет активного FSM-состояния (типично после рестарта
    бота).

    Две ветки:
      * известная меню-кнопка → «сессия сбросилась», открыть корневое меню;
      * любой другой текст → сказать, что не поняли, и открыть корневое меню.

    Вторая ветка — фикс owner 26.07. Раньше здесь стоял голый `return`, и текст
    пропадал МОЛЧА: хендлер уже совпал по фильтрам, поэтому aiogram считал
    апдейт обработанным и писал в лог `status=handled` — потерю нельзя было
    найти даже поиском. Прецедент: ответ менеджера с адресом по счёту
    26721-1НПН не дошёл ни до кого и не оставил следа.
    """
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return          # команды разбирают роутеры выше; неизвестные — не наш случай
    u = message.from_user
    if not u:
        return
    try:
        user = await db.get_user_optional(u.id)
        if not user or not user.role:
            return      # посторонний / не заведён в боте — не отвечаем
        menu_role, _ = resolve_menu_scope(u.id, user.role)
        if not menu_role:
            menu_role = user.role
        is_admin = u.id in (config.admin_ids or set())
        unread = await db.count_unread_tasks(u.id)
        kb = main_menu(menu_role, is_admin=is_admin, unread=unread)

        if text in _ORPHAN_MENU_BUTTONS:
            await message.answer(
                "⚠️ Похоже, сессия сбросилась (бот перезапускался) — кнопка потеряла контекст.\n"
                "Открыл главное меню, выберите раздел заново.",
                reply_markup=kb,
            )
            return

        log.info(
            "stateless free text from user %s (role=%s, %d chars) — показано меню",
            u.id, user.role, len(text),
        )
        await message.answer(
            "⚠️ Не понял сообщение — бот принимает команды только из меню.\n"
            "Выберите раздел ниже, а для переписки — раздел чата.",
            reply_markup=kb,
        )
    except Exception:
        log.warning("stateless_menu_recovery failed for user %s", u.id, exc_info=True)


# ---------------------------------------------------------------------------
# Свободный текст в меню канала у менеджера / РП (owner 26.07)
#
# Баг-зеркало фикса 25.07 для ГД (chat_proxy.chat_menu_freetext): в состоянии
# ManagerChatProxySG.menu ловятся только точные тексты кнопок, произвольный
# текст не обрабатывал НИКТО → бот молчал (в логах status=unhandled).
#
# Почему ИМЕННО ЗДЕСЬ, а не в manager_new/rp_new: состояние
# `ManagerChatProxySG.menu` обслуживают ОБА роутера, причём manager_new
# подключён раньше rp_new (main.py:276-277). Catch-all внутри manager_new
# перехватил бы кнопки РП («💬 Чат», «🔧 В работу», «📐 Размеры»,
# «✉️ Сообщение», «📋 Задача») и убил бы их. fallback.router подключён
# ПОСЛЕДНИМ, поэтому сюда доходит только то, что не разобрал никто.
# ---------------------------------------------------------------------------

_MGR_FREE_TEXT_KEY = "menu_free_text_mgr"


def _submenu_button_texts() -> frozenset[str]:
    """Тексты кнопок всех подменю канала (менеджер + РП).

    Собираются из самих клавиатур, чтобы список не разъезжался при их правке.
    Нужны только защитно: если кнопка дошла сюда, её хендлер сломан — такой
    текст нельзя молча превращать в сообщение собеседнику.
    """
    out: set[str] = set()
    for kb in (
        manager_chat_submenu(),
        rp_chat_submenu(),
        rp_chat_gd_submenu(),
        rp_montazh_submenu(),
    ):
        for row in kb.keyboard:
            for btn in row:
                out.add((btn.text or "").strip())
    out.add("◀️ Назад")     # иная стрелка при рендере клиента
    return frozenset(out)


_MENU_BUTTONS: frozenset[str] = _submenu_button_texts()


@router.message(ManagerChatProxySG.menu, F.chat.type == "private", F.text)
async def menu_freetext(message: Message, state: FSMContext) -> None:
    """Менеджер/РП набрал произвольный текст в меню канала → спросить, отправлять ли."""
    text = (message.text or "").strip()
    if len(text) < 2 or text.startswith("/"):
        return
    if text in _MENU_BUTTONS:
        log.warning(
            "menu_freetext: кнопка %r дошла до fallback в ManagerChatProxySG.menu "
            "— её хендлер не сработал", text,
        )
        return

    data = await state.get_data()
    channel = data.get("channel", "")
    await state.update_data(**{_MGR_FREE_TEXT_KEY: text})

    b = InlineKeyboardBuilder()
    b.button(text="📨 Отправить сообщением", callback_data="mgrfree:send")
    b.button(text="❌ Отмена", callback_data="mgrfree:cancel")
    b.adjust(1)

    preview = text if len(text) <= 200 else text[:200] + "…"
    await message.answer(
        f"💬 <b>{_channel_label(channel)}</b>\n\n"
        f"Ваш текст: <i>{_html.escape(preview)}</i>\n\n"
        "Отправить его собеседнику?",
        reply_markup=b.as_markup(),
    )


def _channel_label(channel: str) -> str:
    """Человекочитаемое имя канала (ленивый импорт — manager_new тяжёлый)."""
    from .manager_new import _CHAT_CHANNEL_LABEL

    return _CHAT_CHANNEL_LABEL.get(channel, channel or "Чат")


async def _take_free_text(cb: CallbackQuery, state: FSMContext) -> tuple[str, dict]:
    """Забрать набранный текст из state и СРАЗУ его погасить.

    Гашение до выполнения действия — синхронный гард от двойного клика
    ([[feedback_money_confirm_idempotent_gate]]), тем же приёмом, что и
    `chat_proxy._take_pending_free_text` у ГД.
    """
    data = await state.get_data()
    text = (data.get(_MGR_FREE_TEXT_KEY) or "").strip()
    if text:
        await state.update_data(**{_MGR_FREE_TEXT_KEY: None})
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    return text, data


@router.callback_query(F.data == "mgrfree:cancel")
async def menu_freetext_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("Отменено")
    await _take_free_text(cb, state)
    await cb.message.answer("❌ Текст отброшен.")  # type: ignore[union-attr]


@router.callback_query(F.data == "mgrfree:send")
async def menu_freetext_send(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Отправить набранный текст в канал — тем же путём, что «✏️ Написать»."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    text, data = await _take_free_text(cb, state)
    if not text:
        await cb.message.answer("⚠️ Текст уже обработан — наберите заново.")  # type: ignore[union-attr]
        return

    from .manager_new import deliver_manager_chat_message

    target_id = await deliver_manager_chat_message(
        sender_id=u.id,
        sender_name=u.full_name or "",
        sender_username=u.username,
        channel=data.get("channel", ""),
        text=text,
        db=db,
        config=config,
        notifier=notifier,
        invoice_id=data.get("invoice_id"),
    )
    if target_id:
        await cb.message.answer("✅ Сообщение отправлено.")  # type: ignore[union-attr]
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Адресат для этого канала не настроен — сообщение сохранено в переписке,\n"
            "но никому не ушло. Сообщите администратору.",
        )
