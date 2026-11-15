"""Таблица знаний «Бот для Телеграмм» — чтение через лист «Карта таблицы».

Канон структуры (лист «Карта таблицы», фиксация 15.11.2026): бот/машина
начинает чтение с карты — берёт из неё точный диапазон данных нужного листа
и читает только его. Листа нет в карте → чтение наугад запрещено.

Карта (строка 1 — шапка): A Лист · B Что содержит (шапка) · C Диапазон
данных · D Штамп актуальности · E Примечание. Данные карты — со строки 2
до первой пустой строки; строки-блоки «⬇️ БЛОК: …» в карту не входят.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import gspread

log = logging.getLogger(__name__)

# Таблица знаний «Бот для Телеграмм» (переопределяется env KNOWLEDGE_SHEET_ID).
KNOWLEDGE_DEFAULT_SPREADSHEET_ID = "1fJQYT_qBTu7lIc5yHH6TCzb7skspel0VXLMvAiR4MWk"

_RANGE_RE = re.compile(r"^[A-Za-z]+\d+:[A-Za-z]+\d+$")
_BLOCK_PREFIX = "⬇️"

# Ограничители выгрузки в промт виртуального менеджера.
# На лист: «Правила · Деньги и ЗП» один ~13 000 знаков — при общем лимите 8000
# лист «Запреты и Не трогать» обрезался почти в ноль (выкатка 15.11).
_DIGEST_MAX_CHARS = 16000
_DIGEST_SHEET_MAX_CHARS = 8000
_DIGEST_ROW_MAX_CHARS = 500


@dataclass
class KnowledgeConfig:
    enabled: bool
    spreadsheet_id: str
    map_tab: str = "Карта таблицы"
    role_cards_tab: str = "Карточки ролей"
    vm_sheets: list[str] = field(default_factory=list)
    cache_ttl_seconds: int = 600
    service_account_json: str | None = None
    service_account_file: str | None = None


@dataclass
class SheetMapEntry:
    name: str
    header: str
    data_range: str
    stamp: str
    note: str = ""


@dataclass
class SheetData:
    name: str
    header: list[str]
    rows: list[dict[str, Any]]
    data_range: str
    stamp: str


_ROLE_CODE_RE = re.compile(r"\(([^()]+)\)\s*$")


def parse_role_code(label: str) -> str:
    """«Менеджер НПН (manager_npn)» → «manager_npn»; без скобок — сам текст."""
    label = (label or "").strip()
    m = _ROLE_CODE_RE.search(label)
    return (m.group(1).strip() if m else label).lower()


class KnowledgeSheetError(RuntimeError):
    """Карта таблицы недоступна или нечитаема (сеть/доступ/структура)."""


class KnowledgeSheetService:
    """Чтение таблицы знаний: карта → точный диапазон листа.

    Вызовы синхронные (gspread); из асинхронного кода — через asyncio.to_thread.
    """

    def __init__(self, cfg: KnowledgeConfig):
        self.cfg = cfg
        self._gc: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._map_cache: list[SheetMapEntry] | None = None
        self._map_cache_ts: float = 0.0
        self._sheet_cache: dict[str, tuple[float, SheetData]] = {}

    # ---------- gspread plumbing ----------

    def _get_client(self) -> gspread.Client:
        if self._gc:
            return self._gc
        if self.cfg.service_account_json:
            import base64
            import json as _json

            raw = self.cfg.service_account_json.strip()
            try:
                parsed = _json.loads(raw)
            except ValueError:
                parsed = _json.loads(base64.b64decode(raw).decode("utf-8"))
            from google.oauth2.service_account import Credentials

            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = Credentials.from_service_account_info(parsed, scopes=scopes)
            self._gc = gspread.authorize(creds)
        elif self.cfg.service_account_file:
            self._gc = gspread.service_account(
                filename=self.cfg.service_account_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
        else:
            self._gc = gspread.service_account(
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
            )
        return self._gc

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet:
            return self._spreadsheet
        self._spreadsheet = self._get_client().open_by_key(self.cfg.spreadsheet_id)
        return self._spreadsheet

    def _values(self, tab: str, a1_range: str) -> list[list[str]]:
        """Точные значения диапазона одного листа (единственная точка API-чтения)."""
        resp = self._get_spreadsheet().values_get(f"'{tab}'!{a1_range}")
        return [list(row) for row in (resp.get("values") or [])]

    # ---------- карта таблицы ----------

    def load_map(self, refresh: bool = False) -> list[SheetMapEntry]:
        """Карта «лист → диапазон» из листа «Карта таблицы» (с кэшем по TTL)."""
        now = time.monotonic()
        if (
            not refresh
            and self._map_cache is not None
            and now - self._map_cache_ts < self.cfg.cache_ttl_seconds
        ):
            return self._map_cache

        # Читаем с запасом; карта заканчивается первой пустой строкой
        # (после неё идут блоки «⬇️ БЛОК: …», в карту не входящие).
        raw = self._values(self.cfg.map_tab, "A2:E200")
        entries: list[SheetMapEntry] = []
        for row in raw:
            if not row or not (row[0] or "").strip():
                break
            name = row[0].strip()
            if name.startswith(_BLOCK_PREFIX):
                break
            header = (row[1] if len(row) > 1 else "").strip()
            data_range = (row[2] if len(row) > 2 else "").strip()
            stamp = (row[3] if len(row) > 3 else "").strip()
            note = (row[4] if len(row) > 4 else "").strip()
            if not _RANGE_RE.match(data_range):
                log.warning(
                    "Knowledge map: строка «%s» без корректного диапазона («%s») — пропущена",
                    name, data_range,
                )
                continue
            entries.append(SheetMapEntry(
                name=name, header=header, data_range=data_range,
                stamp=stamp, note=note,
            ))

        if not entries:
            raise KnowledgeSheetError(
                f"Лист «{self.cfg.map_tab}» не отдал ни одной записи карты — "
                "проверьте доступ сервисного аккаунта к таблице знаний"
            )

        self._map_cache = entries
        self._map_cache_ts = now
        return entries

    def find_map_entry(self, sheet_name: str) -> SheetMapEntry | None:
        wanted = (sheet_name or "").strip()
        for entry in self.load_map():
            if entry.name.lower() == wanted.lower():
                return entry
        return None

    # ---------- чтение листа ----------

    def read(self, sheet_name: str) -> SheetData | None:
        """Читать лист по карте: точный диапазон из «Карты таблицы».

        None → листа нет в карте (наугад не читаем). Кэш по TTL на лист.
        """
        now = time.monotonic()
        cached = self._sheet_cache.get(sheet_name)
        if cached and now - cached[0] < self.cfg.cache_ttl_seconds:
            return cached[1]

        entry = self.find_map_entry(sheet_name)
        if entry is None:
            return None

        # Канон (правило 1): строка 1 листа — всегда шапка, данные — со строки 2.
        # Диапазон из карты — чистые данные; шапку берём из строки 1 тех же колонок.
        end_col = entry.data_range.split(":")[1]
        end_col_letters = re.match(r"^[A-Za-z]+", end_col).group(0).upper()
        header_raw = self._values(entry.name, f"A1:{end_col_letters}1")
        header = [(cell or "").strip() for cell in (header_raw[0] if header_raw else [])]

        data_raw = self._values(entry.name, entry.data_range)
        rows: list[dict[str, Any]] = []
        for row in data_raw:
            if not any((cell or "").strip() for cell in row):
                continue
            rows.append({
                header[i] if i < len(header) and header[i] else f"col{i + 1}":
                (row[i] if i < len(row) else "") or ""
                for i in range(max(len(header), len(row)))
            })
        data = SheetData(
            name=entry.name, header=header, rows=rows,
            data_range=entry.data_range, stamp=entry.stamp,
        )

        self._sheet_cache[sheet_name] = (now, data)
        return data

    # ---------- выгрузка для виртуального менеджера ----------

    def rules_digest(self, sheet_names: list[str]) -> str:
        """Компактный текстовый дайджест листов для промта виртуального менеджера."""
        parts: list[str] = []
        total = 0
        for name in sheet_names:
            name = (name or "").strip()
            if not name:
                continue
            try:
                data = self.read(name)
            except Exception:
                log.exception("Knowledge digest: лист «%s» не прочитан — пропущен", name)
                continue
            if data is None:
                log.warning("Knowledge digest: листа «%s» нет в карте — пропущен", name)
                continue
            lines = [f"— Лист «{data.name}» (штамп: {data.stamp or 'нет'}):"]
            for idx, row in enumerate(data.rows, start=1):
                cells = [f"{k}: {v}" for k, v in row.items() if (v or "").strip()]
                if not cells:
                    continue
                line = f"{idx}. " + "; ".join(cells)
                if len(line) > _DIGEST_ROW_MAX_CHARS:
                    line = line[: _DIGEST_ROW_MAX_CHARS - 1] + "…"
                lines.append(line)
            block = "\n".join(lines)
            if len(block) > _DIGEST_SHEET_MAX_CHARS:
                log.warning(
                    "Knowledge digest: лимит %d знаков на лист — лист «%s» обрезан",
                    _DIGEST_SHEET_MAX_CHARS, name,
                )
                block = block[: _DIGEST_SHEET_MAX_CHARS - 1] + "…"
            if total + len(block) > _DIGEST_MAX_CHARS:
                log.warning(
                    "Knowledge digest: общий лимит %d знаков — лист «%s» обрезан",
                    _DIGEST_MAX_CHARS, name,
                )
                room = _DIGEST_MAX_CHARS - total
                if room > 200:
                    block = block[: room - 1] + "…"
                else:
                    break
            parts.append(block)
            total += len(block) + 1
        return "\n\n".join(parts)

    # ---------- карточки ролей ----------

    def read_role_cards(self, refresh: bool = False) -> list[dict[str, Any]]:
        """Карточки ролей с листа «Карточки ролей» (одна роль = одна строка).

        Каждая карточка: {code, label, row{колонка: значение}}.
        Ключи «—» (нет данных) из row убираются — остаётся только суть.
        """
        if refresh:
            self._sheet_cache.pop(self.cfg.role_cards_tab, None)
        data = self.read(self.cfg.role_cards_tab)
        if data is None:
            raise KnowledgeSheetError(
                f"Листа «{self.cfg.role_cards_tab}» нет в «{self.cfg.map_tab}»"
            )
        cards: list[dict[str, Any]] = []
        for row in data.rows:
            label = next(
                (str(v).strip() for k, v in row.items()
                 if "роль" in str(k).lower() and str(v).strip()),
                "",
            )
            if not label or label.startswith(_BLOCK_PREFIX):
                continue
            cards.append({
                "code": parse_role_code(label),
                "label": label,
                "row": {k: v for k, v in row.items()
                        if str(v).strip() and str(v).strip() != "—"},
            })
        return cards

    def find_role_card(
        self,
        telegram_id: int | str | None = None,
        code: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any] | None:
        """Найти карточку роли по telegram_id или коду роли."""
        cards = self.read_role_cards(refresh=refresh)
        if telegram_id is not None:
            wanted = str(telegram_id).strip()
            for card in cards:
                tg = next(
                    (str(v).strip() for k, v in card["row"].items()
                     if "telegram" in str(k).lower() and str(v).strip()),
                    "",
                )
                if tg and tg == wanted:
                    return card
        if code:
            wanted = (code or "").strip().lower()
            for card in cards:
                if card["code"] == wanted or wanted in card["label"].lower():
                    return card
        return None
