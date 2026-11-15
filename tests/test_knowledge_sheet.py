# Таблица знаний «Бот для Телеграмм»: сервис чтения через «Карту таблицы».
# 1) карта парсится до первой пустой строки / блока «⬇️ БЛОК:»;
# 2) строки без корректного диапазона в карте пропускаются;
# 3) read() читает точный диапазон из карты, строка→dict по шапке;
# 4) листа нет в карте → None (наугад не читаем);
# 5) кэш: повторный read не ходит в API;
# 6) rules_digest собирает листы для промта виртуального менеджера;
# 7) parse_role_code / read_role_cards / find_role_card — карточки ролей;
# 8) KnowledgeSync: кэш в settings, fallback при сбое Гугла, устаревание;
# 9) _stale_since_daily — порог «сегодняшнее 09:00 МСК».
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.integrations.knowledge_sheet import (
    KnowledgeConfig,
    KnowledgeSheetService,
    parse_role_code,
)

ROLE_CARDS_HEADER = ["№", "Роль", "Telegram_id", "Правила роли"]

SHEET_ROWS = {
    "Правила": [
        ["№", "Суть правила"],        # строка 1 — шапка (канон)
    ],
    "Запреты и Не трогать": [
        ["№", "Запрет"],
    ],
    "Карточки ролей": [
        ROLE_CARDS_HEADER,
    ],
}

MAP_ROWS = [
    ["Правила", "№ / Суть", "A2:M11", "15.11.2026, main df4fdb0", ""],
    ["Лист без диапазона", "что-то", "см. где-то", "штамп", ""],
    ["Запреты и Не трогать", "№ / Запрет", "A2:M17", "15.11.2026, main df4fdb0", "прим"],
    ["Карточки ролей", "№ / Роль", "A2:D11", "16.11.2026", ""],
    ["", "", "", "", ""],
    ["⬇️ БЛОК: Правила чтения таблицы", "", "", "", ""],
    ["Не должно попасть в карту", "", "", "", ""],
]

SHEET_DATA = {
    "Правила": [
        ["1", "Первое правило"],
        ["2", ""],
        ["", "Строка с пустым №, но непустой сутью"],
    ],
    "Запреты и Не трогать": [
        ["1", "Нельзя называть цену"],
    ],
    "Карточки ролей": [
        ["1", "Менеджер НПН (manager_npn)", "111", "Правило А"],
        ["2", "РП (rp)", "222", "Правило Б"],
        ["3", "Установщик (installer)", "—", "—"],
    ],
}


class FakeKnowledgeService(KnowledgeSheetService):
    """Сервис с подменённой точкой API-чтения (_values)."""

    def __init__(self) -> None:
        super().__init__(KnowledgeConfig(
            enabled=True,
            spreadsheet_id="fake",
            map_tab="Карта таблицы",
            role_cards_tab="Карточки ролей",
            cache_ttl_seconds=600,
        ))
        self.calls: list[str] = []

    def _values(self, tab: str, a1_range: str) -> list[list[str]]:
        self.calls.append(f"{tab}!{a1_range}")
        if tab == self.cfg.map_tab:
            return MAP_ROWS
        start, end = a1_range.split(":")
        if start.upper() == "A1":  # строка 1 — шапка
            return SHEET_ROWS[tab]
        return SHEET_DATA[tab]


def test_map_parsing() -> None:
    svc = FakeKnowledgeService()
    entries = svc.load_map(refresh=True)
    names = [e.name for e in entries]
    assert names == ["Правила", "Запреты и Не трогать", "Карточки ролей"], names
    # Строка без диапазона пропущена, блок «⬇️» и пустая строка — конец карты
    assert entries[0].data_range == "A2:M11"
    assert entries[1].note == "прим"


def test_read_exact_range_and_rows() -> None:
    svc = FakeKnowledgeService()
    data = svc.read("Правила")
    assert data is not None
    # Шапка — из строки 1 листа (канон), данные — со строки 2
    assert data.header == ["№", "Суть правила"]
    assert data.rows == [
        {"№": "1", "Суть правила": "Первое правило"},
        {"№": "2", "Суть правила": ""},
        {"№": "", "Суть правила": "Строка с пустым №, но непустой сутью"},
    ]
    # Диапазон взят из карты, не наугад; шапка — из A1
    assert "Правила!A1:M1" in svc.calls
    assert "Правила!A2:M11" in svc.calls


def test_read_unknown_sheet_returns_none() -> None:
    svc = FakeKnowledgeService()
    assert svc.read("Нет такого листа") is None


def test_read_uses_cache() -> None:
    svc = FakeKnowledgeService()
    svc.read("Правила")
    before = len(svc.calls)
    svc.read("Правила")
    assert len(svc.calls) == before  # второй read — из кэша, без API


def test_rules_digest() -> None:
    svc = FakeKnowledgeService()
    digest = svc.rules_digest(["Правила", "Запреты и Не трогать", "Нет листа"])
    assert "— Лист «Правила»" in digest
    assert "— Лист «Запреты и Не трогать»" in digest
    assert "Нельзя называть цену" in digest
    # Отсутствующий лист не падает, просто пропущен
    assert "Нет листа" not in digest


def test_parse_role_code() -> None:
    assert parse_role_code("Менеджер НПН (manager_npn)") == "manager_npn"
    assert parse_role_code("РП (rp)") == "rp"
    assert parse_role_code("Без кода") == "без кода"  # кода нет — весь label


def test_read_role_cards() -> None:
    svc = FakeKnowledgeService()
    cards = svc.read_role_cards()
    assert [c["code"] for c in cards] == ["manager_npn", "rp", "installer"], cards
    # «—» (нет данных) из row убраны; у установщика остался минимум
    assert cards[0]["row"]["Правила роли"] == "Правило А"
    assert "Telegram_id" not in cards[2]["row"]
    # Блоков «⬇️» в карточках нет (пропускаются по label)


def test_find_role_card() -> None:
    svc = FakeKnowledgeService()
    card = svc.find_role_card(telegram_id=222)
    assert card is not None and card["code"] == "rp"
    card = svc.find_role_card(code="MANAGER_NPN")
    assert card is not None and card["code"] == "manager_npn"
    assert svc.find_role_card(telegram_id=999, code="нет такой") is None


# ---------- KnowledgeSync: кэш, fallback, устаревание ----------


class FakeDB:
    """Минимальный db: settings-таблица в памяти + get_user_optional."""

    def __init__(self, users: dict[int, "FakeUser"] | None = None) -> None:
        self.settings: dict[str, str] = {}
        self.users = users or {}

    async def get_setting(self, key: str) -> str | None:
        return self.settings.get(key)

    async def set_setting(self, key: str, value: str) -> None:
        self.settings[key] = value

    async def get_user_optional(self, telegram_id: int) -> "FakeUser | None":
        return self.users.get(telegram_id)


class FakeUser:
    def __init__(self, role: str) -> None:
        self.role = role


class _FakeRows:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    async def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self, settings: dict[str, str]) -> None:
        self._settings = settings

    async def execute(self, sql: str):
        keys = sorted(k for k in self._settings if k.startswith("knowledge:role:"))
        return _FakeRows([(k,) for k in keys])


class FakeDBWithConn(FakeDB):
    def __init__(self, users=None) -> None:
        super().__init__(users)
        self.conn = _FakeConn(self.settings)


def test_stale_since_daily() -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.services.knowledge_sync import _stale_since_daily
    msk = ZoneInfo("Europe/Moscow")
    now = datetime.now(msk)
    today_9 = now.replace(hour=9, minute=0, second=0, microsecond=0)
    assert _stale_since_daily(None) is True
    assert _stale_since_daily("мусор") is True
    assert _stale_since_daily((today_9 - __import__("datetime").timedelta(minutes=1)).isoformat()) is True
    assert _stale_since_daily((today_9 + __import__("datetime").timedelta(minutes=1)).isoformat()) is False


def test_knowledge_sync_cache_and_fallback() -> None:
    from app.services.knowledge_sync import KnowledgeSync, _ROLE_KEY
    db = FakeDBWithConn({111: FakeUser("manager_npn")})
    svc = FakeKnowledgeService()
    sync = KnowledgeSync(svc, db)

    # кэш пуст → get_role_card резолвит по user.role → None
    assert asyncio.run(sync.get_role_card(telegram_id=111)) is None

    # refresh_role_card пишет карточку в кэш
    card = asyncio.run(sync.refresh_role_card(telegram_id=111, code="manager_npn"))
    assert card is not None and card["code"] == "manager_npn"
    assert _ROLE_KEY.format("manager_npn") in db.settings

    # теперь get_role_card отдаёт из кэша, не трогая Гугл
    got = asyncio.run(sync.get_role_card(telegram_id=111))
    assert got is not None and got["code"] == "manager_npn"

    # сбой Гугла не рушит чтение: кэш остаётся, get_role_card не бросает
    def _boom(*args, **kwargs):
        raise RuntimeError("Google down")
    svc.find_role_card = _boom  # type: ignore[method-assign]
    got = asyncio.run(sync.get_role_card(telegram_id=111))
    assert got is not None and got["code"] == "manager_npn"
    assert asyncio.run(sync.refresh_role_card(code="rp")) is None  # сбой — кэш не тронут


def test_knowledge_sync_refresh_all_and_maybe_refresh() -> None:
    from app.services.knowledge_sync import KnowledgeSync, _ROLE_KEY, _VM_DIGEST_KEY
    db = FakeDBWithConn({111: FakeUser("manager_npn"), 222: FakeUser("rp")})
    svc = FakeKnowledgeService()
    svc.cfg.vm_sheets = ["Правила", "Запреты и Не трогать"]
    sync = KnowledgeSync(svc, db)

    stats = asyncio.run(sync.refresh_all())
    assert stats["role_cards"] == 3
    assert stats["digest_chars"] > 0
    assert _ROLE_KEY.format("rp") in db.settings
    assert "Нельзя называть цену" in db.settings[_VM_DIGEST_KEY]

    # свежие карточки (после 9:00 сегодня) — maybe_refresh не создаёт задачу
    asyncio.run(sync.maybe_refresh_for_user(111))
    # карточка без fetched_at считается устаревшей → фоновое обновление
    db.settings[_ROLE_KEY.format("rp")] = db.settings[_ROLE_KEY.format("rp")].replace(
        '"fetched_at"', '"fetched_at_x"', 1)
    asyncio.run(sync.maybe_refresh_for_user(222))
    # даём fire-and-forget задаче выполниться
    for _ in range(10):
        asyncio.run(asyncio.sleep(0.01))
    assert '"fetched_at"' in db.settings[_ROLE_KEY.format("rp")]


if __name__ == "__main__":
    test_map_parsing()
    test_read_exact_range_and_rows()
    test_read_unknown_sheet_returns_none()
    test_read_uses_cache()
    test_rules_digest()
    test_parse_role_code()
    test_read_role_cards()
    test_find_role_card()
    test_stale_since_daily()
    test_knowledge_sync_cache_and_fallback()
    test_knowledge_sync_refresh_all_and_maybe_refresh()
    print("OK: все проверки таблицы знаний прошли")
