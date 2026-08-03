"""Session-level request middleware: «тихие часы» (пуши без звука ночью).

Глушит ЗВУК всех исходящих сообщений бота с 22:00 до 09:00 по Москве
(МСК = UTC+3, перехода на летнее время в РФ нет → фиксированный сдвиг +3 ч).
Сообщение всё равно ДОСТАВЛЯЕТСЯ (видно в чате, badge растёт) — гасится
только звук/вибро через Telegram-флаг ``disable_notification=True``. Ничего
не задерживается и не прячется.

В отличие от update-middleware (уровень dp/router, см. ``keep_menu.py``) этот
фильтр работает на уровне СЕССИИ бота (транспортный слой aiogram), поэтому
перехватывает ЛЮБУЮ отправку независимо от способа вызова: ``Notifier.safe_send*``,
прямые ``bot.send_message/photo/document/video`` в хендлерах, форвард вложений,
публикация лида в рабочий чат и т.д. Любые будущие новые пуши попадают под
правило автоматически — фильтр не «течёт».

Решение user (08.06): глушить ВСЁ ночью, включая «🚨 Срочно ГД» и эскалации —
исключений нет. Ответы на собственные действия юзера (он сам в боте и их видит)
ночью тоже без звука — это безвредно.

Регистрация: ``bot.session.middleware(QuietHoursMiddleware())`` в ``main.py``.
"""
from __future__ import annotations

from aiogram import Bot
from aiogram.client.session.middlewares.base import (
    BaseRequestMiddleware,
    NextRequestMiddlewareType,
)
from aiogram.methods import TelegramMethod
from aiogram.methods.base import Response, TelegramType

from ..utils import utcnow

# Окно тишины по Москве: [22:00, 09:00). 22:00 — уже тихо; 09:00 — снова звук.
# Граница 09:00 совпадает с утренней рассылкой daily_sync (services/daily_sync.py,
# 09:00 МСК) — карточка-«будильник» в 09:00 звучит штатно.
_MSK_UTC_OFFSET_H = 3   # МСК = UTC+3, DST в РФ отменён → фиксированный сдвиг
_QUIET_START_H = 22     # с 22:00 включительно — тихо
_QUIET_END_H = 9        # до 09:00 (не включая) — тихо; в 09:00 уже звук


def quiet_hours_now() -> bool:
    """True, если сейчас «тихие часы» по Москве (22:00–09:00)."""
    msk_hour = (utcnow().hour + _MSK_UTC_OFFSET_H) % 24
    return msk_hour >= _QUIET_START_H or msk_hour < _QUIET_END_H


class QuietHoursMiddleware(BaseRequestMiddleware):
    """В тихие часы проставляет ``disable_notification=True`` на send-методах."""

    async def __call__(
        self,
        make_request: NextRequestMiddlewareType[TelegramType],
        bot: Bot,
        method: TelegramMethod[TelegramType],
    ) -> Response[TelegramType]:
        # Поле disable_notification есть только у методов отправки (send_*,
        # copy_*, forward_*). У edit_* / answer_callback_query его нет —
        # такие методы пропускаем как есть.
        if (
            quiet_hours_now()
            and "disable_notification" in type(method).model_fields
            and getattr(method, "disable_notification", None) is not True
        ):
            method = method.model_copy(update={"disable_notification": True})
        return await make_request(bot, method)
