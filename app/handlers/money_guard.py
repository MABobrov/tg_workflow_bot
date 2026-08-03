"""Общий гард от двойного клика на денежных confirm-хендлерах (§9).

Проблема (инцидент 05.06): один платёж 18 000 ₽ записался 3 раза от повторных
кликов «✅ Записать», пока шла медленная запись с синком Google-таблиц, а сброс
FSM-состояния — лишь в конце хендлера. aiogram обрабатывает колбэки конкурентно,
поэтому повторные клики успевают пройти фильтр состояния и провести дубль-расход.

Решение — module-level set по (user_id, message_id) с СИНХРОННЫМ claim: между
проверкой `key in set` и `set.add(key)` нет await, поэтому два конкурентных колбэка
не могут оба пройти. Плюс снятие inline-клавиатуры сразу (defense-in-depth) и
release claim в `finally` (даже при исключении в хендлере).

Логика идентична проверенному на проде гарду `cw_confirm` (manager_new.py, фикс
05.06), вынесена в декоратор для переиспользования на остальных денежных confirm
(§9): installer/gd/rp/td/manager ЗП/аванс/депозит/вывод-подтверждения.

Бот — один процесс (single polling), поэтому module-level set достаточно (без
Redis/блокировок). Один общий набор на все декорированные хендлеры: клики по РАЗНЫМ
сообщениям не мешают друг другу (разный message_id), а повторные клики по ОДНОМУ
confirm-сообщению сериализуются.
"""
from __future__ import annotations

import functools
from typing import Any, Awaitable, Callable

from aiogram.types import CallbackQuery

# Ключ claim'а — (user_id, message_id). Module-level: процесс один (single polling).
_MONEY_CONFIRM_INFLIGHT: set[tuple[int, int]] = set()


def money_confirm_guard(
    func: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """Декоратор анти-двойного-клика для денежного confirm-callback-хендлера.

    Оборачиваемый хендлер — aiogram callback_query handler: первым ПОЗИЦИОННЫМ
    аргументом принимает CallbackQuery, остальные зависимости (state/db/notifier/
    integrations/config/callback_data) aiogram инжектит по ИМЕНИ (kwargs). Декоратор
    прозрачно их пробрасывает.

    `functools.wraps` сохраняет `__wrapped__`, поэтому aiogram (CallableObject делает
    `inspect.unwrap`) корректно резолвит ОРИГИНАЛЬНУЮ сигнатуру для dependency-
    injection — обёртка не меняет набор аргументов, который видит aiogram.

    Поведение:
      * повторный клик той же (user, message) пока хендлер ещё работает →
        `cb.answer("Уже обрабатываю, секунду…")` и ранний выход (тело не запускается);
      * на первом клике кнопки снимаются сразу (defense-in-depth: не приглашать к
        повторному нажатию в окне до того, как хендлер сам перерисует сообщение);
      * claim освобождается в `finally` — после завершения/исключения хендлера.
    """

    @functools.wraps(func)
    async def wrapper(cb: CallbackQuery, *args: Any, **kwargs: Any) -> Any:
        u = cb.from_user
        if u is None:
            # Колбэк без отправителя (на практике невозможно) — без гарда, как есть.
            return await func(cb, *args, **kwargs)
        key = (u.id, cb.message.message_id if cb.message else 0)
        if key in _MONEY_CONFIRM_INFLIGHT:
            await cb.answer("Уже обрабатываю, секунду…")
            return None
        # Синхронный claim — между `key in set` (выше) и `add` НЕТ await.
        _MONEY_CONFIRM_INFLIGHT.add(key)
        # Снять кнопки сразу — не приглашать к повторному нажатию (defense-in-depth).
        try:
            if cb.message is not None:
                await cb.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            return await func(cb, *args, **kwargs)
        finally:
            _MONEY_CONFIRM_INFLIGHT.discard(key)

    # Маркер для теста покрытия (проверяем, что все целевые confirm задекорированы).
    wrapper._money_confirm_guarded = True  # type: ignore[attr-defined]
    return wrapper
