from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.types import CallbackQuery, Message, Update

from ..db import Database

log = logging.getLogger(__name__)


MENU_BUTTONS = {
    "📂 Ещё действия",
    "🧭 Действия",
    "📚 Справка",
    "🛠 Адм.панель",
    "⬅️ Назад в главное меню",
    "Проверить КП/Запросить документы",
    "➕ Запросить документы/счёт",
    "➕ Запросить / КП /",
    "💰 Оплата поступила",
    "📄 Док. / ЭДО",
    "📄 Закрывающие / ЭДО",
    "🏁 Счёт End",
    "🚨 Срочно ГД",
    "📥 Входящие задачи",
    "📌 Мои проекты",
    "🆘 Проблема / вопрос",
    "🗂 Проекты",
    "📌 Поиск проекта",
    "🆘 Проблема / простой",
    "✅ Подтверждение оплат",
    "📄 Закрывающие",
    "📨 Менеджеру (Имя)",
    "📝 Отчёт за день",
    "✅ Счёт ОК",
    "📌 Мои объекты",
    "🔄 Обновить меню",
    "ℹ️ Инструкция",
    "🛠 Админ-инструкция",
    "👥 Сотрудники",
    "📊 Статистика бота",
    "📊 Счета в работе",
    "💬 Рабочий чат",
    "🧪 Тест рабочего чата",
    "🔄 Синхронизация Sheets",
    "❌ Отмена",
    "📋 Заявка на замер",
    "📋 Мои замеры",
    "💰 Оплата замеров",
    "🔄 Синхронизация данных",
    # GD "Ещё" submenu — credit chat buttons
    "Менеджер КВ (кредит)",
    "Менеджер КИА (кредит)",
    "Менеджер НПН (кредит)",
    "📂 Ещё",
    "📊 Сводка дня",
    "📋 Все задачи",
    # RP buttons
    "📁 РП",
    "✅ Менеджер НПН",
    "Проверить КП / Счет",
    "Счет в Работу",
    "Счет End",
    "Замеры",
    "Бухгалтерия (Док./ЭДО)",
    "Монтажная гр.",
    "Чат с РП",
    "Менеджер (кред)",
}


def _clip(s: str | None, max_len: int = 80) -> str:
    if not s:
        return ""
    flat = " ".join(s.split())
    if len(flat) <= max_len:
        return flat
    return flat[:max_len]


def _extract_command(text: str) -> str:
    first = text.split()[0].split("@")[0]
    return first.lstrip("/").strip().lower()


class UsageAuditMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        result = await handler(event, data)
        if result is UNHANDLED:
            return result

        db = data.get("db")
        if not isinstance(db, Database):
            return result

        try:
            update_event = event.event
        except Exception:
            return result

        try:
            if isinstance(update_event, Message):
                user_id = update_event.from_user.id if update_event.from_user else None
                chat_id = update_event.chat.id if update_event.chat else None
                text = (update_event.text or "").strip()

                if text.startswith("/"):
                    await db.audit(
                        actor_id=user_id,
                        action="command",
                        entity="message",
                        entity_id=_extract_command(text),
                        payload={"chat_id": chat_id},
                    )
                elif text in MENU_BUTTONS:
                    await db.audit(
                        actor_id=user_id,
                        action="menu_click",
                        entity="message",
                        entity_id=text,
                        payload={"chat_id": chat_id},
                    )
                elif update_event.photo or update_event.document:
                    await db.audit(
                        actor_id=user_id,
                        action="media_message",
                        entity="message",
                        entity_id="photo" if update_event.photo else "document",
                        payload={"chat_id": chat_id},
                    )
                elif text:
                    # Free-form text is not persisted to audit to avoid noisy/sensitive telemetry.
                    pass

            elif isinstance(update_event, CallbackQuery):
                user_id = update_event.from_user.id if update_event.from_user else None
                chat_id = (
                    update_event.message.chat.id
                    if update_event.message and update_event.message.chat
                    else None
                )
                callback_data = (update_event.data or "").strip()
                callback_kind = callback_data.split(":", 1)[0] if callback_data else ""
                await db.audit(
                    actor_id=user_id,
                    action="callback",
                    entity="callback",
                    entity_id=callback_kind or _clip(callback_data),
                    payload={"chat_id": chat_id},
                )
        except Exception:
            # telemetry must not break business flow
            log.exception("Failed to write usage audit event")

        return result
