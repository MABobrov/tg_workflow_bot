"""Синк таблицы знаний «Бот для Телеграмм» с прочным кэшем в SQLite.

Регламент владельца 16.11.2026:
- полный освежающий прогон — раз в сутки в 09:00 МСК (в daily_sync);
- ленивое обновление по роли — когда человек заходит в чат с ботом,
  бот перечитывает карточку ИМЕННО этой роли, если она не обновлялась
  с последнего 9:00; остальные роли не трогаются, пока не зайдут сами;
- карточки хранятся в таблице settings (ключи knowledge:role:<code>) —
  перезапуск бота и чистка памяти данных не теряют, а при недоступности
  Гугла бот отвечает из кэша (всегда без ошибок).

Вызовы gspread синхронные — из asyncio через to_thread.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ..db import Database
from ..integrations.knowledge_sheet import KnowledgeSheetService

log = logging.getLogger(__name__)

_MSK = ZoneInfo("Europe/Moscow")
_ROLE_KEY = "knowledge:role:{}"
_VM_DIGEST_KEY = "knowledge:vm_digest"
_DAILY_HOUR = 9


def _now_iso() -> str:
    return datetime.now(_MSK).isoformat()


def _stale_since_daily(fetched_at: str | None) -> bool:
    """Карточка устарела, если взята раньше сегодняшнего 09:00 МСК."""
    if not fetched_at:
        return True
    try:
        fetched = datetime.fromisoformat(fetched_at)
    except ValueError:
        return True
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=_MSK)
    now = datetime.now(_MSK)
    today_9 = now.replace(hour=_DAILY_HOUR, minute=0, second=0, microsecond=0)
    return fetched < today_9


class KnowledgeSync:
    """Прочный кэш карточек ролей + дайджеста ВМ из таблицы знаний."""

    def __init__(self, svc: KnowledgeSheetService, db: Database):
        self.svc = svc
        self.db = db
        self._bg_tasks: set[asyncio.Task] = set()

    # ---------- чтение из кэша (никогда не бросает) ----------

    async def get_role_card(
        self,
        telegram_id: int | str | None = None,
        code: str | None = None,
    ) -> dict[str, Any] | None:
        """Карточка роли из кэша; None — роли нет (в т.ч. вообще не заведена)."""
        try:
            if telegram_id is not None:
                # прямой ключ по коду роли из БД бота
                user = await self.db.get_user_optional(int(telegram_id))
                if user and user.role:
                    for role in (user.role or "").split(","):
                        role = role.strip()
                        if role:
                            card = await self._load_card(role)
                            if card is not None:
                                return card
                # fallback: поиск по telegram_id в кэше всех карточек
                return await self._find_cached_by_tg(telegram_id)
            if code:
                return await self._load_card(code.strip())
            return None
        except Exception:
            log.exception("KnowledgeSync: get_role_card failed")
            return None

    async def _load_card(self, code: str) -> dict[str, Any] | None:
        raw = await self.db.get_setting(_ROLE_KEY.format(code))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return None

    async def _find_cached_by_tg(self, telegram_id: int | str) -> dict[str, Any] | None:
        wanted = str(telegram_id).strip()
        for code in await self._cached_role_codes():
            card = await self._load_card(code)
            if card:
                tg = next(
                    (str(v).strip() for k, v in card.get("row", {}).items()
                     if "telegram" in str(k).lower() and str(v).strip()),
                    "",
                )
                if tg == wanted:
                    return card
        return None

    async def _cached_role_codes(self) -> list[str]:
        # коды ролей берём из карточек, уже записанных в кэш за сегодня
        # (полный список даёт refresh_all; перечень ключей не храним отдельно)
        rows = await self.db.conn.execute(
            "SELECT key FROM settings WHERE key LIKE 'knowledge:role:%'"
        )
        keys = [r[0] for r in (await rows.fetchall()) or []]
        return [k.split(":", 2)[2] for k in keys]

    async def get_vm_digest(self) -> str:
        """Дайджест правил ВМ из кэша (fallback при недоступности Гугла)."""
        try:
            return await self.db.get_setting(_VM_DIGEST_KEY) or ""
        except Exception:
            return ""

    # ---------- обновление ----------

    async def refresh_role_card(
        self,
        telegram_id: int | str | None = None,
        code: str | None = None,
    ) -> dict[str, Any] | None:
        """Перечитать карточку роли с листа и записать в кэш.

        Ошибка Гугла ≠ ошибка бота: при сбое остаётся прежний кэш.
        """
        try:
            card = await asyncio.to_thread(
                self.svc.find_role_card, telegram_id, code, True,
            )
        except Exception:
            log.exception(
                "KnowledgeSync: refresh_role_card(tg=%s, code=%s) — лист недоступен, кэш не тронут",
                telegram_id, code,
            )
            return None
        if card is None:
            return None
        card["fetched_at"] = _now_iso()
        await self.db.set_setting(
            _ROLE_KEY.format(card["code"]), json.dumps(card, ensure_ascii=False),
        )
        return card

    async def refresh_all(self) -> dict[str, int]:
        """Полный прогон: карта + все карточки ролей + дайджест ВМ → кэш."""
        refreshed = 0
        try:
            cards = await asyncio.to_thread(self.svc.read_role_cards, True)
        except Exception:
            log.exception("KnowledgeSync: refresh_all — карточки недоступны")
            cards = []
        for card in cards:
            card["fetched_at"] = _now_iso()
            await self.db.set_setting(
                _ROLE_KEY.format(card["code"]), json.dumps(card, ensure_ascii=False),
            )
            refreshed += 1

        digest = ""
        vm_sheets = getattr(self.svc.cfg, "vm_sheets", None) or []
        if vm_sheets:
            try:
                digest = await asyncio.to_thread(self.svc.rules_digest, vm_sheets)
            except Exception:
                log.exception("KnowledgeSync: refresh_all — дайджест ВМ недоступен")
        if digest:
            await self.db.set_setting(_VM_DIGEST_KEY, digest)

        log.info("KnowledgeSync: refresh_all — карточек %d, дайджест %d знаков",
                 refreshed, len(digest))
        return {"role_cards": refreshed, "digest_chars": len(digest)}

    # ---------- ленивое обновление по заходу роли ----------

    async def maybe_refresh_for_user(self, telegram_id: int) -> None:
        """Фоновое обновление карточки роли, если она старше сегодняшнего 9:00.

        Вызывается middleware на каждое сообщение: дёшево (проверка кэша),
        само чтение — fire-and-forget задачей, ошибки глотаются.
        """
        try:
            user = await self.db.get_user_optional(telegram_id)
            if not user or not user.role:
                return
            for role in (user.role or "").split(","):
                role = role.strip()
                if not role:
                    continue
                card = await self._load_card(role)
                if _stale_since_daily((card or {}).get("fetched_at")):
                    task = asyncio.create_task(
                        self.refresh_role_card(telegram_id=telegram_id, code=role),
                        name=f"knowledge_role_refresh:{role}",
                    )
                    self._bg_tasks.add(task)
                    task.add_done_callback(self._bg_tasks.discard)
        except Exception:
            log.exception("KnowledgeSync: maybe_refresh_for_user failed")

    async def startup_warmup(self) -> None:
        """Прогрев при старте: если кэш пуст — заполнить (не ждать 9:00)."""
        try:
            codes = await self._cached_role_codes()
            if codes:
                log.info("KnowledgeSync: кэш карточек уже есть (%d ролей)", len(codes))
                return
            await self.refresh_all()
        except Exception:
            log.exception("KnowledgeSync: startup warmup failed")
