# Таблица знаний «Бот для Телеграмм»: сервис чтения через «Карту таблицы».
# 1) карта парсится до первой пустой строки / блока «⬇️ БЛОК:»;
# 2) строки без корректного диапазона в карте пропускаются;
# 3) read() читает точный диапазон из карты, строка→dict по шапке;
# 4) листа нет в карте → None (наугад не читаем);
# 5) кэш: повторный read не ходит в API;
# 6) rules_digest собирает листы для промта виртуального менеджера.
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.integrations.knowledge_sheet import (
    KnowledgeConfig,
    KnowledgeSheetService,
)

MAP_ROWS = [
    ["Правила", "№ / Суть", "A2:M11", "15.11.2026, main df4fdb0", ""],
    ["Лист без диапазона", "что-то", "см. где-то", "штамп", ""],
    ["Запреты и Не трогать", "№ / Запрет", "A2:M17", "15.11.2026, main df4fdb0", "прим"],
    ["", "", "", "", ""],
    ["⬇️ БЛОК: Правила чтения таблицы", "", "", "", ""],
    ["Не должно попасть в карту", "", "", "", ""],
]

SHEET_ROWS = {
    "Правила": [
        ["№", "Суть правила"],        # строка 1 — шапка (канон)
    ],
    "Запреты и Не трогать": [
        ["№", "Запрет"],
    ],
}

SHEET_DATA = {
    "Правила": [
        ["1", "Первое правило"],
        ["2", ""],
        ["", "Строка с пустым №, но непустой сутью"],
    ],
    "Запреты и Не трогать": [
        ["1", "Нельзя называть цену"],
    ],
}


class FakeKnowledgeService(KnowledgeSheetService):
    """Сервис с подменённой точкой API-чтения (_values)."""

    def __init__(self) -> None:
        super().__init__(KnowledgeConfig(
            enabled=True,
            spreadsheet_id="fake",
            map_tab="Карта таблицы",
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
    assert names == ["Правила", "Запреты и Не трогать"], names
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


if __name__ == "__main__":
    test_map_parsing()
    test_read_exact_range_and_rows()
    test_read_unknown_sheet_returns_none()
    test_read_uses_cache()
    test_rules_digest()
    print("OK: все проверки таблицы знаний прошли")
