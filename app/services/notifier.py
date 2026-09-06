from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)

log = logging.getLogger(__name__)

# =====================================================================
# Классификация сетевых сбоев отправки (06.09.2026)
# ---------------------------------------------------------------------
# ЗАМЕР прод-лога 03.09-06.09: 29 отказов safe_send из 29 —
#   15 x asyncio.IncompleteReadError (asyncio/streams.py readexactly, вызвана
#        из python_socks socks5_async.py:43 «ответ выбора метода», 2 байта,
#        и :75 _read_reply «CONNECT-reply», 3 байта);
#   14 x aiohttp_socks._errors.ProxyError (aiohttp_socks/connector.py:83).
# Оба рождаются ВНУТРИ aiohttp/connector.py connect(), то есть ДО того, как
# в сокет записан первый байт HTTP -> запрос до Telegram не доходил НИ РАЗУ,
# и повтор физически не может задвоить сообщение.
#
# 🔴 Ни один из них НЕ подкласс aiohttp.ClientError, поэтому aiogram их НЕ
# оборачивает (aiogram/client/session/aiohttp.py::make_request имеет ровно два
# except: asyncio.TimeoutError и ClientError -> TelegramNetworkError) и они
# падали в голый `except Exception` вместе с настоящими багами.
# Классификатор на `except TelegramNetworkError` починил бы 0 отказов из 29.
# =====================================================================

try:  # aiohttp-socks: жёсткая зависимость, но импорт защищён — модуль должен
      # оставаться пригодным на голом стенде без прокси-стека.
    from aiohttp_socks import (  # type: ignore[import-not-found]
        ProxyConnectionError as _SocksConnError,
        ProxyError as _SocksError,
        ProxyTimeoutError as _SocksTimeoutError,
    )
    _SOCKS_ERRORS: tuple[type[BaseException], ...] = (
        _SocksError, _SocksConnError, _SocksTimeoutError,
    )
except Exception:  # pragma: no cover
    _SOCKS_ERRORS = ()

# (1) ВРЕМЕННЫЕ и БЕЗОПАСНЫЕ К ПОВТОРУ: отказ строго на фазе установления
#     соединения, запрос не отправлен, дубля быть не может.
_RETRY_SAFE_ERRORS: tuple[type[BaseException], ...] = (
    (asyncio.IncompleteReadError,) + _SOCKS_ERRORS
)

# (2) ВРЕМЕННЫЕ, но ДВУСМЫСЛЕННЫЕ: соединение уже стояло, запрос мог дойти до
#     Telegram, а ответ потеряться. sendMessage не идемпотентен, ключа
#     идемпотентности у Bot API нет -> НЕ повторяем. Но и флаг напоминания по
#     ним не защёлкиваем: повтор произойдёт штатной каденцией петли.
#     За окно наблюдения на пути ОТПРАВКИ таких 0 из 29.
_TRANSIENT_NO_RETRY_ERRORS: tuple[type[BaseException], ...] = (
    TelegramNetworkError,
    TelegramServerError,
    asyncio.TimeoutError,
)

# Паузы между попытками, сек. ЗАМЕР: обрывов короче 11 с нет ни одного
# (64 эпизода, p50 42 с), поэтому ретрай закрывает ~8 % потерь и служит
# сглаживанием микро-блипов; основную работу делает повтор на следующем тике.
_RETRY_BACKOFF: tuple[float, ...] = (1.0, 3.0)

# Коды, при которых флаг напоминания ставить РАНО — повторим позже.
_TRANSIENT_ERR_CODES = frozenset({"network", "network_noretry"})


def is_transient_error(err: str | None) -> bool:
    """True, если отказ временный: повтор имеет смысл, флаг ставить рано.

    Единственный источник истины для фоновых петель. `unexpected_error` сюда
    НЕ входит намеренно: это настоящий баг в коде, и вечно его повторять —
    значит менять потерю уведомления на бесконечный пинг API.
    """
    if err is None:
        return False
    return err in _TRANSIENT_ERR_CODES or err.startswith("retry_after:")


class Notifier:
    def __init__(
        self,
        bot: Bot,
        work_chat_id: int | None = None,
        *,
        workchat_events_enabled: bool = False,
    ):
        self.bot = bot
        self.work_chat_id = work_chat_id
        self.workchat_events_enabled = workchat_events_enabled

    async def safe_send(
        self,
        chat_id: int,
        text: str,
        reply_markup: Any | None = None,
        *,
        return_error: bool = False,
        retries: int = 0,
    ) -> bool | tuple[bool, str | None]:
        """Отправка с классификацией отказа и опциональным повтором.

        retries=0 — ДЕФОЛТ и поведение БАЙТ В БАЙТ как до правки 06.09: все
        295 существующих вызовов не меняются, в том числе ~231 в хендлерах,
        где человек ждёт ответа на нажатие и лишние секунды протухят
        callback-query. Повтор включают ЯВНО только фоновые петли.

        Коды ошибок при return_error=True:
          None               — доставлено;
          "forbidden"        — ПЕРМАНЕНТНО (бот заблокирован / чат недоступен);
          "retry_after:N"    — временно, паузу назначил Telegram;
          "<текст>"          — TelegramBadRequest, ПЕРМАНЕНТНО;
          "network"          — сеть, повтор был безопасен и исчерпан;
          "network_noretry"  — сеть, повтор небезопасен (запрос мог дойти);
          "unexpected_error" — БАГ в коде, повторять нельзя.
        """

        def _ret(ok: bool, err: str | None = None) -> bool | tuple[bool, str | None]:
            if return_error:
                return ok, err
            return ok

        attempt = 0
        while True:
            try:
                await self.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, disable_web_page_preview=True)
                return _ret(True, None)
            except TelegramForbiddenError:
                # user didn't start bot or blocked it
                log.warning("Cannot send to chat_id=%s: forbidden", chat_id)
                return _ret(False, "forbidden")
            except TelegramRetryAfter as e:
                # НЕ спим здесь намеренно: sleep(retry_after) заблокировал бы тик
                # фоновой петли на назначенные Telegram секунды. Отдаём наверх
                # как временный отказ — повторим на следующем тике.
                log.warning("Telegram rate limit: retry_after=%s", e.retry_after)
                return _ret(False, f"retry_after:{e.retry_after}")
            except TelegramBadRequest as e:
                err = str(e)
                # If user text accidentally breaks HTML entities, retry as plain text.
                if "can't parse entities" in err.lower():
                    try:
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            reply_markup=reply_markup,
                            disable_web_page_preview=True,
                            parse_mode=None,
                        )
                        log.warning("Retried message as plain text due to entity parsing error, chat_id=%s", chat_id)
                        return _ret(True, None)
                    except Exception:
                        log.exception("Failed to resend plain-text message after parse error, chat_id=%s", chat_id)
                        return _ret(False, err)
                log.warning("Telegram bad request chat_id=%s: %s", chat_id, err)
                return _ret(False, err)
            except _RETRY_SAFE_ERRORS as e:
                # Отказ на фазе соединения — повтор безопасен (см. шапку модуля).
                if attempt < retries:
                    base = _RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)]
                    delay = base * (1.0 + random.random() * 0.25)   # джиттер
                    log.warning(
                        "Network error sending to chat_id=%s (%s: %s), retry %s/%s in %.1fs",
                        chat_id, type(e).__name__, e, attempt + 1, retries, delay,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1
                    continue
                log.warning(
                    "Network error sending to chat_id=%s after %s attempt(s): %s: %s",
                    chat_id, attempt + 1, type(e).__name__, e,
                )
                return _ret(False, "network")
            except _TRANSIENT_NO_RETRY_ERRORS as e:
                # Запрос мог дойти до Telegram — повтор здесь способен задвоить
                # сообщение, поэтому не повторяем; но и флаг не защёлкиваем.
                log.warning(
                    "Ambiguous network error sending to chat_id=%s: %s: %s (no retry)",
                    chat_id, type(e).__name__, e,
                )
                return _ret(False, "network_noretry")
            except Exception:
                # Настоящий баг в коде. Раньше сюда падали ВСЕ сетевые сбои,
                # из-за чего «сеть» и «баг» были неразличимы, а chat_id терялся.
                log.exception("Unexpected error sending message to chat_id=%s", chat_id)
                return _ret(False, "unexpected_error")

    async def safe_send_document(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send document to chat_id=%s: forbidden", chat_id)
            return False
        except TelegramBadRequest as e:
            err = str(e)
            if caption and "can't parse entities" in err.lower():
                try:
                    await self.bot.send_document(chat_id=chat_id, document=file_id, caption=caption, parse_mode=None)
                    log.warning("Retried document as plain caption due to entity parsing error, chat_id=%s", chat_id)
                    return True
                except Exception:
                    log.exception("Failed to resend document after parse error, chat_id=%s", chat_id)
                    return False
            log.warning("Telegram bad request document chat_id=%s: %s", chat_id, err)
            return False
        except Exception:
            log.exception("Unexpected error sending document")
            return False

    async def safe_send_photo(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send photo to chat_id=%s: forbidden", chat_id)
            return False
        except TelegramBadRequest as e:
            err = str(e)
            if caption and "can't parse entities" in err.lower():
                try:
                    await self.bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption, parse_mode=None)
                    log.warning("Retried photo as plain caption due to entity parsing error, chat_id=%s", chat_id)
                    return True
                except Exception:
                    log.exception("Failed to resend photo after parse error, chat_id=%s", chat_id)
                    return False
            log.warning("Telegram bad request photo chat_id=%s: %s", chat_id, err)
            return False
        except Exception:
            log.exception("Unexpected error sending photo")
            return False

    async def safe_send_video(self, chat_id: int, file_id: str, caption: str | None = None) -> bool:
        try:
            await self.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
            return True
        except TelegramForbiddenError:
            log.warning("Cannot send video to chat_id=%s: forbidden", chat_id)
            return False
        except Exception:
            log.exception("Error sending video to chat_id=%s", chat_id)
            return False

    async def safe_send_media(self, chat_id: int, file_type: str, file_id: str, caption: str | None = None) -> bool:
        if file_type == "photo":
            return await self.safe_send_photo(chat_id, file_id, caption=caption)
        if file_type == "video":
            return await self.safe_send_video(chat_id, file_id, caption=caption)
        return await self.safe_send_document(chat_id, file_id, caption=caption)

    async def notify_workchat(self, text: str, reply_markup: Any | None = None) -> None:
        if not self.workchat_events_enabled:
            return
        if not self.work_chat_id:
            return
        chat_id = int(self.work_chat_id)
        # В рабочем чате клавиатура не нужна — только текст уведомления
        result = await self.safe_send(chat_id, text, return_error=True)
        ok, err = result if isinstance(result, tuple) else (bool(result), None)
        if not ok and err and "chat not found" in err.lower():
            self.work_chat_id = None
            log.error(
                "Work chat disabled: chat_id=%s not found. "
                "Set a valid chat via /setworkchat in bot private chat.",
                chat_id,
            )

    async def notify_workchat_media(self, file_type: str, file_id: str, caption: str | None = None) -> bool:
        if not self.workchat_events_enabled:
            return False
        if not self.work_chat_id:
            return False
        return await self.safe_send_media(self.work_chat_id, file_type, file_id, caption=caption)
