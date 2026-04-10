from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from dataclasses import dataclass
from threading import RLock
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from ..utils import (
    encode_sa_json,
    format_date_iso,
    format_dt_iso,
    project_status_label,
    task_status_label,
    task_type_label,
    try_json_loads,
)


log = logging.getLogger(__name__)


PROJECTS_HEADER = [
    "Код",
    "Проект",
    "Адрес",
    "Клиент",
    "Сумма",
    "Дедлайн",
    "Статус",
    "Менеджер (ID)",
    "Менеджер",
    "Создан",
    "Обновлён",
    "amo_lead_id",
]

TASKS_HEADER = [
    "ID задачи",
    "Код проекта",
    "Тип задачи",
    "Статус",
    "Назначена (ID)",
    "Создал (ID)",
    "Срок",
    "Создана",
    "Обновлена",
    "Комментарий",
    "Размеры/ТЗ",
    "Тип проблемы",
    "Документы",
    "Уточнение",
    "Сумма оплаты",
    "Тип оплаты",
    "Этап оплаты",
    "Дата оплаты",
    "№ счёта",
    "Тип подписания",
    "Источник",
    "Отправитель",
]

# Bot leads header — written starting from column H (col 8)
LEADS_COL_START = 1  # column A (1-indexed)
LEADS_HEADER = [
    "Дата",           # A
    "Имя клиента",    # B
    "Имя",            # C — название лида
    "Телефон",        # D
    "Менеджер",       # E
    "Источник",       # F
    "Статус",         # G
]

INVOICES_HEADER = [
    # — Отдел продаж structure (0-45) —
    "№",            # 0
    "Роль",         # 1
    "Менеджер",     # 2
    "Бухг.ЭДО",    # 3
    "Контрагент",   # 4
    "Ист.трафика",  # 5  manual
    "Б.Н./Кред",    # 6
    "Свой/Атм",     # 7  manual
    "Номер счета",  # 8
    "Адрес",        # 9
    "Дата пост.",   # 10
    "Сроки",        # 11 manual
    "Дата оконч.",  # 12 FORMULA
    "Дата Факт",    # 13
    "Сумма",        # 14
    "Сумма 1пл",    # 15
    "Расч.мат.",    # 16
    "Установка",    # 17
    "Грузчики",     # 18 manual
    "Логистика",    # 19 manual
    "Прибыль",      # 20
    "НДС",          # 21 manual
    "Нал.приб.",    # 22 manual
    "Рент-ть расч", # 23
    "Рент-ть факт", # 24 manual
    "Сумма допл",   # 25 manual
    "Допл подтв",   # 26 manual
    "Дата допл",    # 27 manual
    "Оконч допл",   # 28 manual
    "Дата оконч",   # 29 manual
    "Долг",         # 30
    "Договор",      # 31 manual
    "Закр.док",     # 32
    "Пояснения",    # 33 manual
    "Агентское",    # 34 manual
    "Мен.ЗП",       # 35
    "Запрос",       # 36
    "тех",          # 37 manual
    "Выпл.Агент",   # 38 manual
    "Выпл.МенЗП",   # 39 manual
    "Дата выпл",    # 40 manual
    "НПН 10%",      # 41 manual
    "Запрос НПН",   # 42 manual
    "Выдано НПН",   # 43 manual
    "Дата НПН",     # 44 manual
    "Месяц",        # 45 FORMULA
    # — Бот-специфичные (46-60) —
    "Статус",               # 46
    "Роль менеджера",       # 47
    "Поставщик",            # 48
    "Тип материала",        # 49
    "Родит. счёт ID",       # 50
    "Этап монтажа",         # 51
    "Монтажник ОК",         # 52
    "Долгов нет",           # 53
    "",                     # 54 (перенесено в 74)
    "",                     # 55 (перенесено в 75)
    "ЗП Монтажник статус",  # 56
    "Оплаты пост. итого",   # 57
    "Расходы итого",        # 58
    "Создан",               # 59
    "Обновлён",             # 60
    # — Статусы жизненного цикла (61-73) —
    "ЗП Монтажник",              # 61
    "Расчетная прибыль",         # 62
    "Фактическая прибыль",       # 63
    "Разница расч. и факт.",     # 64
    "НДС факт",            # 65
    "Налог на приб. факт", # 66
    "В работе",            # 67
    "Счет END",            # 68
    "Грузчики факт",       # 69
    "Монтаж Факт",         # 70
    "Материалы Факт",      # 71
    "Логистика Факт",      # 72
    "Статус лида",         # 73
    # — Блок Замерщик (74-76, перенос из 54/55/69) —
    "ЗП Замерщик",         # 74 (перенос из 54)
    "ЗП Замерщик сумма",   # 75 (перенос из 55)
    "Замеры",              # 76 (перенос из 69)
    # — Аналитика (77-79) —
    "Расчет vs Факт",     # 77
    "Прибыль факт",       # 78
    "Перерасчет прибыли",  # 79
    # — Кредитный учёт (80-85) —
    "Кредит вход",         # 80 — сумма входящего кредита
    "Кредит вход коммент", # 81 — Менеджер, адрес
    "Кредит расход",       # 82 — накопительная сумма расходов
    "Дата расход кред",    # 83 — дата расхода кредитных средств
    "Кредит назначение",   # 84 — лог назначений расходов
    "Кредит баланс",       # 85 — формула: вход - расход
    # — Сквозная нумерация (86) —
    "№ п/п",                # 86
    # — Лиды и Счета по менеджерам (87-116) —
    # КВ (87-96)
    "Лид КВ №",             # 87
    "Лид КВ источник",      # 88
    "Лид КВ дата",          # 89
    "Лид КВ имя",           # 90
    "Лид КВ телефон",       # 91
    "Лид КВ адрес",         # 92
    "Счет КВ №",            # 93
    "Счет КВ телефон",      # 94
    "Счет КВ адрес",        # 95
    "Счет КВ дата",         # 96
    # КИА (97-106)
    "Лид КИА №",            # 97
    "Лид КИА источник",     # 98
    "Лид КИА дата",         # 99
    "Лид КИА имя",          # 100
    "Лид КИА телефон",      # 101
    "Лид КИА адрес",        # 102
    "Счет КИА №",           # 103
    "Счет КИА телефон",     # 104
    "Счет КИА адрес",       # 105
    "Счет КИА дата",        # 106
    # НПН (107-116)
    "Лид НПН №",            # 107
    "Лид НПН источник",     # 108
    "Лид НПН дата",         # 109
    "Лид НПН имя",          # 110
    "Лид НПН телефон",      # 111
    "Лид НПН адрес",        # 112
    "Счет НПН №",           # 113
    "Счет НПН телефон",     # 114
    "Счет НПН адрес",       # 115
    "Счет НПН дата",        # 116
]

# Column indices the bot NEVER overwrites (manual-only + formula)
# Removed 7 (Свой/Атм→client_source), 18,19,21,24 — now bot-managed (Plan/Fact)
_MANUAL_COLS = frozenset([5,
                          33, 34, 37])


@dataclass
class SheetsConfig:
    enabled: bool
    spreadsheet_id: str
    projects_tab: str
    tasks_tab: str
    invoices_tab: str = "Invoices"
    leads_tab: str = "Leads"
    timezone_name: str = "Europe/Moscow"
    service_account_json: str | None = None
    service_account_file: str | None = None
    # Source spreadsheet for importing (Отдел Продаж)
    source_spreadsheet_id: str | None = None
    source_sheet_name: str = "Отдел продаж"


class GoogleSheetsService:
    """Best-effort sync to Google Sheets.

    Calls are synchronous (gspread), so in the bot we call them via asyncio.to_thread().
    """

    def __init__(self, cfg: SheetsConfig):
        self.cfg = cfg
        self._gc: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._worksheets: dict[str, gspread.Worksheet] = {}
        self._headers_ready: set[str] = set()
        self._row_indexes: dict[str, dict[str, int]] = {}
        self._next_rows: dict[str, int] = {}
        self._sync_lock = RLock()
        # Invoices: key column = "Номер счета" at index 8 → gspread 1-indexed = 9
        self._KEY_COL: dict[str, int] = {cfg.invoices_tab: 9}

    def _fmt_amount(self, amount: Any) -> str:
        if isinstance(amount, (int, float)):
            return f"{amount:.0f}"
        if amount is None:
            return ""
        return str(amount)

    @staticmethod
    def _fmt_sheet_date(value: Any) -> str:
        """Format DB ISO date/datetime as =DATE() formula for Google Sheets.

        Returns =DATE(YYYY,M,D) so Sheets treats it as a real date —
        correct chronological sorting and locale-aware display (DD.MM.YYYY).
        """
        if value in (None, ""):
            return ""
        text = str(value).strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text)
            return f"=DATE({dt.year},{dt.month},{dt.day})"
        except ValueError:
            return text

    @staticmethod
    def _fmt_docs_primary(invoice: dict[str, Any]) -> str:
        """AF (Договор): contract_signed + docs_edo_signed."""
        contract = invoice.get("contract_signed") or ""
        edo = bool(invoice.get("docs_edo_signed"))
        if edo and contract:
            return f"{contract} ✅"
        if edo:
            return "ЭДО ✅"
        if contract:
            return f"{contract} ⏳"
        return "⏳"

    @staticmethod
    def _fmt_docs_closing(invoice: dict[str, Any]) -> str:
        """AG (Закр.док): edo_signed."""
        if bool(invoice.get("edo_signed")):
            return "ЭДО ✅"
        return "⏳"

    def _task_payload_fields(self, task: dict[str, Any]) -> dict[str, str]:
        payload = try_json_loads(task.get("payload_json"))
        sender = (
            payload.get("sender_username")
            or payload.get("manager_username")
            or payload.get("installer_username")
            or payload.get("accounting_username")
            or ""
        )
        if sender and not str(sender).startswith("@"):
            sender = f"@{sender}"

        return {
            "comment": str(payload.get("comment") or ""),
            "measurements": str(payload.get("measurements") or ""),
            "issue_type": str(payload.get("issue_type") or ""),
            "doc_type": str(payload.get("doc_type") or ""),
            "details": str(payload.get("details") or ""),
            "payment_amount": self._fmt_amount(payload.get("payment_amount")),
            "payment_method": str(payload.get("payment_method") or ""),
            "payment_type": str(payload.get("payment_type") or payload.get("payment_stage") or ""),
            "payment_date": format_dt_iso(payload.get("payment_date"), self.cfg.timezone_name)
            if payload.get("payment_date")
            else "",
            "invoice_number": str(payload.get("invoice_number") or ""),
            "sign_type": str(payload.get("sign_type") or ""),
            "source": str(payload.get("source") or ""),
            "sender": str(sender),
        }

    # ---------- internal sync methods (thread) ----------

    def _get_client(self) -> gspread.Client:
        if self._gc:
            return self._gc

        if self.cfg.service_account_file:
            self._gc = gspread.service_account(filename=self.cfg.service_account_file)
            return self._gc

        if not self.cfg.service_account_json:
            raise RuntimeError("Google Sheets enabled, but GOOGLE_SERVICE_ACCOUNT_JSON/FILE is not set")

        info = encode_sa_json(self.cfg.service_account_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        self._gc = gspread.authorize(creds)
        return self._gc

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet:
            return self._spreadsheet
        gc = self._get_client()
        self._spreadsheet = gc.open_by_key(self.cfg.spreadsheet_id)
        return self._spreadsheet

    def _get_or_create_ws(self, title: str, header: list[str]) -> gspread.Worksheet:
        ws = self._worksheets.get(title)
        if ws is None:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(title)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(header) + 2))
            self._worksheets[title] = ws

        if title not in self._headers_ready:
            values = ws.row_values(1)
            if values[: len(header)] != header:
                ws.update([header], "A1")
            self._headers_ready.add(title)
        return ws

    def _get_row_index(self, title: str, ws: gspread.Worksheet) -> dict[str, int]:
        row_index = self._row_indexes.get(title)
        if row_index is not None:
            return row_index

        key_col = self._KEY_COL.get(title, 1)
        col_values = ws.col_values(key_col)
        row_index = {}
        for row_num, value in enumerate(col_values[1:], start=2):
            key = str(value).strip()
            if key and key not in row_index:
                row_index[key] = row_num
        self._row_indexes[title] = row_index
        self._next_rows[title] = max(2, len(col_values) + 1)
        return row_index

    def _get_or_allocate_row(self, title: str, ws: gspread.Worksheet, key: Any) -> tuple[int, bool]:
        key_str = str(key).strip()
        if not key_str:
            raise ValueError("sheet row key is required")

        row_index = self._get_row_index(title, ws)
        existing = row_index.get(key_str)
        if existing is not None:
            return existing, False

        row = self._next_rows.get(title, 2)
        row_index[key_str] = row
        self._next_rows[title] = row + 1
        return row, True

    @staticmethod
    def _chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    def _flush_batch_update(
        self,
        ws: gspread.Worksheet,
        batch_data: list[dict[str, Any]],
        *,
        chunk_size: int = 200,
    ) -> None:
        if not batch_data:
            return
        for chunk in self._chunked(batch_data, chunk_size):
            ws.batch_update(chunk, value_input_option="USER_ENTERED")

    @staticmethod
    def _row_range(row: int, width: int) -> str:
        end_col = GoogleSheetsService._col_letter(width - 1)
        return f"A{row}:{end_col}{row}"

    def _project_row_values(self, project: dict[str, Any], manager_label: str = "") -> list[Any]:
        return [
            project.get("code") or "",
            project.get("title") or "",
            project.get("address") or "",
            project.get("client") or "",
            self._fmt_amount(project.get("amount")),
            format_date_iso(project.get("deadline"), self.cfg.timezone_name),
            project_status_label(str(project.get("status") or "")),
            project.get("manager_id") or "",
            manager_label,
            format_dt_iso(project.get("created_at"), self.cfg.timezone_name),
            format_dt_iso(project.get("updated_at"), self.cfg.timezone_name),
            project.get("amo_lead_id") or "",
        ]

    def _lead_row_values(
        self,
        lead: dict[str, Any],
        *,
        status_name: str = "",
        amo_user_map: dict[int, str] | None = None,
    ) -> list[Any]:
        # Дата: DD.MM.YYYY
        date_str = format_dt_iso(lead.get("created_at"), self.cfg.timezone_name)
        if date_str and date_str != "—":
            date_str = date_str[:10]  # "DD.MM.YYYY"

        # Имя клиента: из контакта amoCRM
        client_name = lead.get("contact_name") or ""

        # Имя: название лида
        name = lead.get("name") or ""

        # Телефон
        phone = lead.get("phone") or ""

        # Менеджер: amo responsible_user_id → role code
        manager = ""
        resp_id = lead.get("responsible_user_id")
        if resp_id and amo_user_map:
            manager = amo_user_map.get(int(resp_id), "")

        # Источник: from custom field "Источник", fallback to first tag
        source = lead.get("source") or ""
        if not source:
            tags_raw = lead.get("tags_json")
            if tags_raw:
                try:
                    import json
                    tags = json.loads(tags_raw)
                    if tags:
                        source = str(tags[0])
                except (json.JSONDecodeError, IndexError):
                    pass

        # Статус: mapped name or status_id fallback
        status = status_name or ""
        if not status:
            sid = lead.get("status_id")
            status = str(sid) if sid else ""

        return [date_str, client_name, name, phone, manager, source, status]

    def _task_row_values(self, task: dict[str, Any], project_code: str = "") -> list[Any]:
        payload = self._task_payload_fields(task)
        return [
            task.get("id") or "",
            project_code,
            task_type_label(task.get("type")),
            task_status_label(task.get("status")),
            task.get("assigned_to") or "",
            task.get("created_by") or "",
            format_dt_iso(task.get("due_at"), self.cfg.timezone_name),
            format_dt_iso(task.get("created_at"), self.cfg.timezone_name),
            format_dt_iso(task.get("updated_at"), self.cfg.timezone_name),
            payload["comment"],
            payload["measurements"],
            payload["issue_type"],
            payload["doc_type"],
            payload["details"],
            payload["payment_amount"],
            payload["payment_method"],
            payload["payment_type"],
            payload["payment_date"],
            payload["invoice_number"],
            payload["sign_type"],
            payload["source"],
            payload["sender"],
        ]

    def _invoice_cells(
        self,
        invoice: dict[str, Any],
        manager_label: str,
        cost: dict[str, Any] | None,
        *,
        row: int,
        is_new: bool,
    ) -> dict[int, Any]:
        _ROLE_LABELS = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }
        _c = cost or {}
        _li = invoice.get("_lead_info") or {}
        _inv_num = invoice.get("invoice_number") or ""

        _role_label = _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or "")

        # LEAD-строки: базовые колонки + лид-колонки (индексы 87-113)
        if str(_inv_num).startswith("LEAD-"):
            cells: dict[int, Any] = {
                0: row - 1,   # № — сквозная нумерация
                1: _role_label,  # Роль
                2: manager_label,
                8: _inv_num,
                86: row - 1,  # № п/п — сквозная нумерация
            }
            for _i, _suf in enumerate(("kv", "kia", "npn")):
                _base = 87 + _i * 10
                cells[_base]     = invoice.get(f"lead_{_suf}_num") or ""
                cells[_base + 1] = invoice.get(f"lead_{_suf}_source") or _li.get(f"source_{_suf}") or ""  # Источник
                cells[_base + 2] = self._fmt_sheet_date(invoice.get(f"lead_{_suf}_date"))
                cells[_base + 3] = invoice.get(f"lead_{_suf}_name") or ""
                cells[_base + 4] = invoice.get(f"lead_{_suf}_phone") or ""
                cells[_base + 5] = invoice.get(f"lead_{_suf}_address") or ""
                cells[_base + 6] = invoice.get(f"inv_{_suf}_num") or ""
                cells[_base + 7] = invoice.get(f"inv_{_suf}_phone") or ""
                cells[_base + 8] = invoice.get(f"inv_{_suf}_address") or ""
                cells[_base + 9] = self._fmt_sheet_date(invoice.get(f"inv_{_suf}_date"))
            return cells

        cells: dict[int, Any] = {
            0: row - 1,   # № п/п — сквозная нумерация
            1: _role_label,  # Роль
            2: manager_label,
            3: "Да" if invoice.get("edo_signed") else "",
            4: invoice.get("client_name") or "",
            6: "0" if invoice.get("is_credit") else "1",  # ОП convention: 0=кредит, 1=б.н.
            7: {"own": 1, "gd_lead": 2}.get(invoice.get("client_source", ""), "")
               or invoice.get("client_type") or "",
            8: invoice.get("invoice_number") or "",
            9: invoice.get("object_address") or "",
            10: self._fmt_sheet_date(invoice.get("receipt_date")),
            11: f'={int(invoice.get("deadline_days"))}' if invoice.get("deadline_days") else "",  # L Сроки (число дней)
            13: self._fmt_sheet_date(invoice.get("actual_completion_date")),
            14: self._fmt_amount(invoice.get("amount")),
            15: self._fmt_amount(invoice.get("first_payment_amount")),
            25: self._fmt_amount(invoice.get("surcharge_amount")),       # Z Сумма допл
            26: invoice.get("payment_confirm_status") or "",             # AA Допл подтв
            27: self._fmt_sheet_date(invoice.get("surcharge_date")),     # AB Дата допл
            28: self._fmt_amount(invoice.get("final_surcharge_amount")), # AC Оконч допл
            29: self._fmt_sheet_date(invoice.get("final_surcharge_date")), # AD Дата оконч
            30: f"=O{row}-P{row}-Z{row}-AC{row}",                          # AE Долг
            31: self._fmt_docs_primary(invoice),                        # AF Договор
            32: self._fmt_docs_closing(invoice),                        # AG Закр.док
            35: self._fmt_amount(invoice.get("manager_zp_blank")),   # AJ ← ОП AG
            36: invoice.get("zp_manager_status") or "",
            38: self._fmt_amount(invoice.get("agent_payout_op")),   # AM ← ОП AE
            39: self._fmt_amount(invoice.get("zp_manager_payout")), # AN ← ОП AI
            40: self._fmt_sheet_date(invoice.get("zp_manager_payout_date")),  # AO ← ОП AJ
            42: self._fmt_amount(invoice.get("npn_amount")),        # AQ ← ОП AT
            43: self._fmt_amount(invoice.get("npn_payout_op")),     # AR ← ОП AU
            44: self._fmt_sheet_date(invoice.get("npn_payout_date_op")),  # AS ← ОП AV
            46: invoice.get("status") or "",
            47: _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or ""),
            48: invoice.get("supplier") or "",
            49: invoice.get("material_type") or "",
            50: invoice.get("invoice_number") or "",
            51: invoice.get("montazh_stage") or "",
            52: "Да" if invoice.get("installer_ok") else "",
            53: "Да" if invoice.get("no_debts") else "",
            54: "",  # очистка (перенесено в 74)
            55: "",  # очистка (перенесено в 75)
            56: invoice.get("zp_installer_status") or "",
            59: format_dt_iso(invoice.get("created_at"), self.cfg.timezone_name),
            60: format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name),
            # — Статусы жизненного цикла —
            # 61-66: не используются
            67: "Да" if invoice.get("status") == "in_progress" else "", # BP В работе
            68: "Да" if invoice.get("status") in ("ended", "credit") else "",  # BQ Счет END
            69: self._fmt_amount(invoice.get("loaders_fact_op")),    # BR Грузчики факт ← ОП AP
            72: self._fmt_amount(invoice.get("logistics_fact_op") or invoice.get("actual_logistics")),  # BU Логистика Факт
            73: _li.get("lead_status", ""),   # BV Статус лида
            # — Блок Замерщик (перенос из 54/55/69) —
            74: invoice.get("zp_status") or "",                          # BW ЗП Замерщик
            75: self._fmt_amount(invoice.get("zp_zamery_total")),        # BX ЗП Замерщик сумма
            76: invoice.get("zamery_info_op") or invoice.get("_zamery_info") or "",  # BY Замеры ← ОП I (fallback: бот)
            # — Аналитика —
            77: invoice.get("_plan_fact_label") or "",                   # BZ Расчет vs Факт
            # 78, 79 заполняются ниже из cost_card
            # — Кредитный учёт —
            85: f"=IF(CC{row}=\"\",\"\",CC{row}-CE{row})",              # CH Кредит баланс
        }

        # — Сквозная нумерация (86) —
        cells[86] = row - 1  # № п/п

        # — Лиды и Счета по менеджерам (87-116) —
        for _i, _suf in enumerate(("kv", "kia", "npn")):
            _base = 87 + _i * 10
            cells[_base]     = invoice.get(f"lead_{_suf}_num") or ""
            cells[_base + 1] = invoice.get(f"lead_{_suf}_source") or _li.get(f"source_{_suf}") or ""  # Источник
            cells[_base + 2] = self._fmt_sheet_date(invoice.get(f"lead_{_suf}_date"))
            cells[_base + 3] = invoice.get(f"lead_{_suf}_name") or ""
            cells[_base + 4] = invoice.get(f"lead_{_suf}_phone") or ""
            cells[_base + 5] = invoice.get(f"lead_{_suf}_address") or ""
            cells[_base + 6] = invoice.get(f"inv_{_suf}_num") or ""
            cells[_base + 7] = invoice.get(f"inv_{_suf}_phone") or ""
            cells[_base + 8] = invoice.get(f"inv_{_suf}_address") or ""
            cells[_base + 9] = self._fmt_sheet_date(invoice.get(f"inv_{_suf}_date"))

        # Кредит входящий (80-81): is_credit=1 — единственный источник правды
        is_credit = bool(invoice.get("is_credit"))
        if is_credit:
            cells[80] = self._fmt_amount(invoice.get("amount"))          # CC Кредит вход
            mgr_label = manager_label or ""
            addr = invoice.get("object_address") or ""
            cells[81] = f"{mgr_label}, {addr}".strip(", ") if (mgr_label or addr) else ""
        else:
            cells[80] = ""
            cells[81] = ""

        # Кредит расход (82), дата (83), назначение (84): прямая сумма ВСЕХ расходов
        credit_exp = invoice.get("_credit_expenses") or {}
        credit_exp_total = credit_exp.get("total") or 0
        if is_credit:
            if invoice.get("status") in ("ended", "credit"):
                # Закрытый счёт — кредит полностью израсходован, баланс = 0
                cells[82] = cells[80]
            else:
                # ВСЕ затраты КВ (кредитные/наличные) без привязки к мат. счёту
                _cf_op = sum(float(invoice.get(f) or 0) for f in (
                    "materials_fact_op",      # Материалы
                    "montazh_fact_op",        # Монтаж
                    "logistics_fact_op",      # Логистика
                    "loaders_fact_op",        # Грузчики
                    "agent_payout_op",        # Агентское
                    "taxes_fact_op",          # Налоги
                    "npn_payout_op",          # НПН
                    "zp_manager_payout",      # ЗП менеджера (выплата)
                ))
                _cf_bot = 0.0
                if _c:
                    _cf_bot = _c.get("supplier_payments_total", 0) + _c.get("materials_total", 0)
                cf_total = _cf_op + _cf_bot + credit_exp_total
                cells[82] = self._fmt_amount(cf_total) if cf_total else ""
        elif credit_exp_total:
            cells[82] = self._fmt_amount(credit_exp_total)
        else:
            cells[82] = ""
        # Дата расхода кредитных средств
        credit_items = credit_exp.get("items") or []
        if credit_items:
            from datetime import datetime as _dt
            try:
                last_dt = _dt.fromisoformat(credit_items[-1]["created_at"])
                cells[83] = last_dt.strftime("%d.%m.%Y")
            except (ValueError, TypeError, KeyError):
                cells[83] = ""
        elif is_credit and invoice.get("updated_at"):
            cells[83] = format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name)[:10] if invoice.get("updated_at") else ""
        else:
            cells[83] = ""
        cells[84] = credit_exp.get("log") or ""

        # Расч.мат., Установка, Грузчики, Логистика — из БД
        est_glass = float(invoice.get("estimated_glass") or 0)
        est_profile = float(invoice.get("estimated_profile") or 0)
        est_mat_legacy = float(invoice.get("estimated_materials") or 0)
        est_inst = float(invoice.get("estimated_installation") or 0)
        est_load = float(invoice.get("estimated_loaders") or 0)
        est_log = float(invoice.get("estimated_logistics") or 0)
        materials_total = est_glass + est_profile + est_mat_legacy
        if any([est_glass, est_profile, est_mat_legacy, est_inst, est_load, est_log]):
            cells[16] = self._fmt_amount(materials_total)
            cells[17] = self._fmt_amount(est_inst)
            cells[18] = self._fmt_amount(est_load)
            cells[19] = self._fmt_amount(est_log)

        # Python-вычисления: НДС, Нал.приб., Прибыль, Рент-ть (вместо формул)
        _amount = float(invoice.get("amount") or 0)
        _est_total = materials_total + est_inst + est_load + est_log
        if is_credit:
            _nds = 0
            _profit_tax = 0
        else:
            _nds = (_amount * 22 / 122) - (materials_total * 22 / 122) if _amount else 0
            _profit_tax = ((_amount - _est_total - _nds) / 100 * 20) if _amount else 0
        _profit = _amount - _est_total - _nds - _profit_tax
        _rentability = (_profit / _amount * 100) if _amount > 0 else 0
        _npn_10 = _profit * 10 / 100

        cells[21] = self._fmt_amount(_nds)                                     # V НДС
        cells[22] = self._fmt_amount(_profit_tax)                              # W Нал.приб.
        cells[20] = self._fmt_amount(_profit)                                  # U Прибыль
        cells[62] = self._fmt_amount(_profit)                                  # BK Расчетная прибыль
        # X Рент-ть расч: из ОП (rentability_calc) если есть, иначе Python-расчёт
        _rent_op = invoice.get("rentability_calc")
        if _rent_op is not None and _rent_op != 0:
            cells[23] = f"{float(_rent_op):.0f}%"
        elif _amount > 0:
            cells[23] = f"{_rentability:.1f}%"
        else:
            cells[23] = ""
        cells[41] = self._fmt_amount(_npn_10)                                  # AP НПН 10%

        # Группировка supplier payments по категориям
        _sp_materials = 0.0  # profile, glass, ldsp, gkl, sandwich, other
        _sp_services = 0.0   # service → монтаж
        if _c:
            _SP_CAT = {"profile": "mat", "glass": "mat", "ldsp": "mat",
                       "gkl": "mat", "sandwich": "mat", "other": "mat",
                       "service": "svc"}
            for _sp in _c.get("supplier_payments_list", []):
                _cat = _SP_CAT.get(_sp.get("material_type", "other"), "mat")
                if _cat == "svc":
                    _sp_services += _sp.get("amount", 0)
                else:
                    _sp_materials += _sp.get("amount", 0)

        # Материалы Факт: ОП + дочерние счета + supplier payments (материалы)
        _mat_op = float(invoice.get("materials_fact_op") or 0)
        _mat_children = _c.get("materials_total", 0) if _c else 0
        _mat_combined = _mat_op + _mat_children + _sp_materials
        if _mat_combined:
            cells[71] = self._fmt_amount(_mat_combined)

        if _c:
            fact_pct = _c.get("margin_pct", 0)
            fact_margin = _c.get("margin", 0)
            cells[24] = f"{fact_pct:.1f}%" if fact_pct else ""
            cells[57] = self._fmt_amount(_c.get("supplier_payments_total"))
            cells[58] = self._fmt_amount(_c.get("total_cost"))
            # НДС факт (65) и Налог на приб. факт (66)
            cells[65] = self._fmt_amount(_c.get("nds_fact"))         # BN НДС факт
            cells[66] = self._fmt_amount(_c.get("profit_tax_fact"))  # BO Налог на приб. факт
            # Прибыль факт (78)
            cells[78] = self._fmt_amount(fact_margin) if fact_margin else ""  # CA Прибыль факт
            # BL-BM: Фактическая прибыль / Разница — только если есть фактические затраты
            _logist_f = float(invoice.get("logistics_fact_op") or invoice.get("actual_logistics") or 0)
            _has_fact_costs = _mat_combined and _mont_zp and _logist_f
            if _has_fact_costs:
                cells[63] = self._fmt_amount(fact_margin) if fact_margin else ""       # BL Фактическая прибыль
                _diff = fact_margin - _profit if fact_margin else 0
                cells[64] = self._fmt_amount(_diff) if _diff else ""                   # BM Разница расч. и факт.
            # Перерасчет прибыли (79): разница план-факт при перерасходе
            pf_label = invoice.get("_plan_fact_label") or ""
            if pf_label == "Перерасчет прибыли":
                est_total = (float(invoice.get("estimated_glass") or 0)
                             + float(invoice.get("estimated_profile") or 0)
                             + float(invoice.get("estimated_materials") or 0)
                             + float(invoice.get("estimated_installation") or 0)
                             + float(invoice.get("estimated_loaders") or 0)
                             + float(invoice.get("estimated_logistics") or 0))
                fact_total = _c.get("total_cost", 0)
                delta = fact_total - est_total
                cells[79] = self._fmt_amount(delta)                          # CB Перерасчет
            else:
                cells[79] = ""

        # BJ — ЗП всегда; BS Монтаж Факт — только после approved
        _mont_zp = float(invoice.get("zp_installer_amount") or 0)
        _zp_status = invoice.get("zp_installer_status") or ""
        if _mont_zp:
            cells[61] = self._fmt_amount(_mont_zp)              # BJ ЗП Монтажник
            if _zp_status == "approved":
                cells[70] = self._fmt_amount(_mont_zp)          # BS Монтаж Факт

        # M (12): Дата окончания = receipt_date + deadline_days
        _receipt = invoice.get("receipt_date")
        _deadline_d = invoice.get("deadline_days")
        if _receipt and _deadline_d:
            try:
                from datetime import datetime as _dt, timedelta as _td
                _rd = _dt.fromisoformat(str(_receipt).strip())
                _end = _rd + _td(days=int(_deadline_d))
                cells[12] = f"=DATE({_end.year},{_end.month},{_end.day})"
            except (ValueError, TypeError):
                pass

        # AT (45): Месяц из receipt_date
        if _receipt:
            _months = {1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
                       5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
                       9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"}
            try:
                from datetime import datetime as _dt
                _rd = _dt.fromisoformat(str(_receipt).strip())
                cells[45] = _months.get(_rd.month, "")
            except (ValueError, TypeError):
                pass

        return cells

    def _invoice_batch_ranges(self, row: int, cells: dict[int, Any]) -> list[dict[str, Any]]:
        ranges: list[dict[str, Any]] = []
        current_cols: list[int] = []
        current_values: list[Any] = []

        for col_idx in sorted(cells):
            value = cells[col_idx]
            if current_cols and col_idx != current_cols[-1] + 1:
                start = self._col_letter(current_cols[0])
                end = self._col_letter(current_cols[-1])
                ranges.append({
                    "range": f"{start}{row}:{end}{row}",
                    "values": [current_values],
                })
                current_cols = []
                current_values = []

            current_cols.append(col_idx)
            current_values.append(value)

        if current_cols:
            start = self._col_letter(current_cols[0])
            end = self._col_letter(current_cols[-1])
            ranges.append({
                "range": f"{start}{row}:{end}{row}",
                "values": [current_values],
            })
        return ranges

    def upsert_project_sync(self, project: dict[str, Any], manager_label: str = "") -> None:
        code = project.get("code")
        if not code:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.projects_tab, PROJECTS_HEADER)
            row, _ = self._get_or_allocate_row(self.cfg.projects_tab, ws, code)
            row_values = self._project_row_values(project, manager_label)
            ws.update([row_values], self._row_range(row, len(PROJECTS_HEADER)), value_input_option="USER_ENTERED")

    def upsert_task_sync(self, task: dict[str, Any], project_code: str = "") -> None:
        tid = task.get("id")
        if not tid:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.tasks_tab, TASKS_HEADER)
            row, _ = self._get_or_allocate_row(self.cfg.tasks_tab, ws, tid)
            row_values = self._task_row_values(task, project_code)
            ws.update([row_values], self._row_range(row, len(TASKS_HEADER)), value_input_option="USER_ENTERED")

    @staticmethod
    def _col_letter(idx: int) -> str:
        """0-based index → A1 column letter (0=A, 25=Z, 26=AA, ...)."""
        result = ""
        while True:
            result = chr(65 + idx % 26) + result
            idx = idx // 26 - 1
            if idx < 0:
                break
        return result

    def upsert_invoice_sync(
        self,
        invoice: dict[str, Any],
        manager_label: str = "",
        cost: dict[str, Any] | None = None,
    ) -> None:
        inv_num = invoice.get("invoice_number") or ""
        if not inv_num:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)
            row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_num)
            cells = self._invoice_cells(invoice, manager_label, cost, row=row, is_new=is_new)
            if not is_new:
                cells = {k: v for k, v in cells.items() if k not in _MANUAL_COLS}
            batch_data = self._invoice_batch_ranges(row, cells)
            self._flush_batch_update(ws, batch_data, chunk_size=200)

    def upsert_projects_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.projects_tab, PROJECTS_HEADER)
            batch_data: list[dict[str, Any]] = []
            count = 0
            for project, manager_label in items:
                code = project.get("code")
                if not code:
                    continue
                row, _ = self._get_or_allocate_row(self.cfg.projects_tab, ws, code)
                batch_data.append(
                    {
                        "range": self._row_range(row, len(PROJECTS_HEADER)),
                        "values": [self._project_row_values(project, manager_label)],
                    }
                )
                count += 1
            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return count

    def upsert_tasks_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.tasks_tab, TASKS_HEADER)
            batch_data: list[dict[str, Any]] = []
            count = 0
            for task, project_code in items:
                tid = task.get("id")
                if not tid:
                    continue
                row, _ = self._get_or_allocate_row(self.cfg.tasks_tab, ws, tid)
                batch_data.append(
                    {
                        "range": self._row_range(row, len(TASKS_HEADER)),
                        "values": [self._task_row_values(task, project_code)],
                    }
                )
                count += 1
            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return count

    @staticmethod
    def _normalize_phone(phone: str | None) -> str:
        """Normalize phone for matching: keep last 10 digits."""
        if not phone:
            return ""
        digits = re.sub(r"\D", "", str(phone))
        return digits[-10:] if len(digits) >= 10 else digits

    def upsert_leads_bulk_sync(
        self,
        items: list[dict[str, Any]],
        *,
        status_map: dict[int, str] | None = None,
        amo_user_map: dict[int, str] | None = None,
    ) -> int:
        with self._sync_lock:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(self.cfg.leads_tab)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(
                    title=self.cfg.leads_tab, rows=2000,
                    cols=len(LEADS_HEADER),
                )

            # Ensure enough columns
            needed = len(LEADS_HEADER)
            if ws.col_count < needed:
                ws.resize(cols=needed)

            # Clear entire sheet and write header A1:G1
            total_rows = ws.row_count
            col_letter = gspread.utils.rowcol_to_a1(1, needed).rstrip("1")
            ws.batch_clear([f"A1:{col_letter}{total_rows}"])

            hdr_end = gspread.utils.rowcol_to_a1(1, len(LEADS_HEADER))
            ws.update([LEADS_HEADER], f"A1:{hdr_end}")

            # Write all leads starting from row 2
            batch_data: list[dict[str, Any]] = []
            next_row = 2

            for lead in items:
                if not lead.get("amo_lead_id"):
                    continue

                status_name = ""
                sid = lead.get("status_id")
                if sid and status_map:
                    status_name = status_map.get(int(sid), "")

                cell_end = gspread.utils.rowcol_to_a1(next_row, len(LEADS_HEADER))
                batch_data.append({
                    "range": f"A{next_row}:{cell_end}",
                    "values": [self._lead_row_values(
                        lead, status_name=status_name, amo_user_map=amo_user_map,
                    )],
                })
                next_row += 1

            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return len(batch_data)

    def upsert_invoices_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str, dict[str, Any] | None]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)

            # Полная очистка данных (кроме заголовка) — гарантирует чистый лист
            try:
                total_rows = ws.row_count
                if total_rows > 1:
                    col_count = ws.col_count
                    col_letter = gspread.utils.rowcol_to_a1(1, col_count).rstrip("1")
                    ws.batch_clear([f"A2:{col_letter}{total_rows}"])
            except Exception:
                pass
            # Сброс кеша строк — все строки будут записаны заново
            self._row_indexes.pop(self.cfg.invoices_tab, None)
            self._next_rows.pop(self.cfg.invoices_tab, None)

            # --- Фаза 1: Инвойсы (колонки 0-85 + 86 № п/п) ---
            batch_data: list[dict[str, Any]] = []
            count = 0
            for invoice, manager_label, cost in items:
                inv_num = invoice.get("invoice_number") or ""
                if not inv_num:
                    continue
                row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_num)
                cells = self._invoice_cells(invoice, manager_label, cost, row=row, is_new=is_new)
                # Только инвойс-колонки (0-85) + 86 (№ п/п — сквозная нумерация)
                inv_cells = {k: v for k, v in cells.items() if k <= 86}
                if not is_new:
                    inv_cells = {k: v for k, v in inv_cells.items() if k not in _MANUAL_COLS}
                batch_data.extend(self._invoice_batch_ranges(row, inv_cells))
                count += 1

            # --- Фаза 2: Лиды (колонки 86-116) — подряд, независимо от инвойсов ---
            lead_row = 2  # начинаем с строки 2
            for invoice, manager_label, cost in items:
                cells = self._invoice_cells(invoice, manager_label, cost, row=lead_row, is_new=True)
                lead_cells = {k: v for k, v in cells.items() if k >= 86}
                # Проверяем, есть ли реальные данные (не пустые строки)
                has_data = any(
                    v for k, v in lead_cells.items()
                    if k != 86 and v  # k=86 — это № п/п, не считаем
                )
                if has_data:
                    lead_cells[86] = lead_row - 1  # № п/п
                    batch_data.extend(self._invoice_batch_ranges(lead_row, lead_cells))
                    lead_row += 1

            self._flush_batch_update(ws, batch_data, chunk_size=500)

            # Очистить лишние строки после последней записанной
            max_row = max(count + 1, lead_row)  # макс из инвойсов и лидов
            try:
                total_rows = ws.row_count
                if total_rows > max_row:
                    col_count = ws.col_count
                    col_letter = gspread.utils.rowcol_to_a1(1, col_count).rstrip("1")
                    clear_range = f"A{max_row + 1}:{col_letter}{total_rows}"
                    ws.batch_clear([clear_range])
            except Exception:
                pass

            # Сортировка: старые счета вверху, новые внизу.
            # Столбец K (index=10) = receipt_date, записан как =DATE() — корректная сортировка.
            try:
                self._sort_ws_by_date(ws, sort_col_index=10)
            except Exception:
                pass  # не критично если сортировка не удалась

            return count

    @staticmethod
    def _clear_lead_rows(ws: gspread.Worksheet) -> bool:
        """Remove rows where invoice_number (col I = index 9) starts with LEAD-."""
        col_i = ws.col_values(9)  # column I = invoice_number
        rows_to_delete: list[int] = []
        for row_num, val in enumerate(col_i[1:], start=2):  # skip header
            if str(val).strip().startswith("LEAD-"):
                rows_to_delete.append(row_num)
        # Delete from bottom to top so row indices stay valid
        for row_num in reversed(rows_to_delete):
            ws.delete_rows(row_num)
        return bool(rows_to_delete)

    def _sort_ws_by_date(self, ws: gspread.Worksheet, sort_col_index: int = 10) -> None:
        """Sort worksheet rows 2+ by column, ASCENDING (oldest dates first)."""
        sheet_id = ws._properties["sheetId"]  # noqa: SLF001
        row_count = ws.row_count
        col_count = ws.col_count
        body = {
            "requests": [{
                "sortRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # skip header
                        "endRowIndex": row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    },
                    "sortSpecs": [{
                        "dimensionIndex": sort_col_index,
                        "sortOrder": "ASCENDING",
                    }],
                }
            }]
        }
        ws.spreadsheet.batch_update(body)
        # Сбросить кеш строк после сортировки
        self._row_indexes.pop(self.cfg.invoices_tab, None)

    # ---------- async wrappers ----------

    async def upsert_project(self, project: dict[str, Any], manager_label: str = "") -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(self.upsert_project_sync, project, manager_label)

    async def upsert_task(self, task: dict[str, Any], project_code: str = "") -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(self.upsert_task_sync, task, project_code)

    async def upsert_invoice(
        self,
        invoice: dict[str, Any],
        manager_label: str = "",
        cost: dict[str, Any] | None = None,
    ) -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(self.upsert_invoice_sync, invoice, manager_label, cost)

    async def upsert_projects_bulk(self, items: list[tuple[dict[str, Any], str]]) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_projects_bulk_sync, items)

    async def upsert_tasks_bulk(self, items: list[tuple[dict[str, Any], str]]) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_tasks_bulk_sync, items)

    async def upsert_leads_bulk(
        self,
        items: list[dict[str, Any]],
        *,
        status_map: dict[int, str] | None = None,
        amo_user_map: dict[int, str] | None = None,
    ) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(
            self.upsert_leads_bulk_sync, items,
            status_map=status_map, amo_user_map=amo_user_map,
        )

    async def upsert_invoices_bulk(
        self,
        items: list[tuple[dict[str, Any], str, dict[str, Any] | None]],
    ) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_invoices_bulk_sync, items)

    # ---------- IMPORT from source spreadsheet (Отдел Продаж → SQLite) ----------

    # Column mapping: source sheet col index → field name
    _OP_COL_MAP: dict[int, str] = {
        0: "client_name",              # A: Контрагент
        1: "traffic_source",           # B: Ист.трафика
        2: "is_credit",                # C: Кред (0=кредит, 1=б.н.)
        3: "client_source",            # D: Свой/Атм (1=Свой, 2=Атм)
        4: "invoice_number",           # E: Номер счета (KEY)
        5: "object_address",           # F: Адрес
        6: "receipt_date",             # G: Дата пост.
        7: "deadline_days",            # H: Сроки (дни)
        8: "zamery_info_op",             # I: Замеры (из ОП)
        9: "actual_completion_date",   # J: Дата Факт
        10: "amount",                  # K: Сумма
        11: "first_payment_amount",    # L: Сумма 1пл
        12: "estimated_materials",     # M: Расч.мат.
        13: "estimated_installation",  # N: Установка
        14: "estimated_loaders",       # O: Грузчики
        15: "estimated_logistics",     # P: Логистика
        16: "profit_tax",              # Q: Прибыль кред.
        17: "nds_amount",              # R: НДС
        18: "profit_tax_op",            # S: Налог на приб.
        19: "rp_10_pct_op",             # T: РП - 10%
        20: "profit_calc_op",           # U: Прибыль расч
        21: "rentability_calc",        # V: Рент-ть расчетная
        22: "rentability_fact_op",      # W: Рент-ть факт
        23: "surcharge_amount",        # X: Сумма допл
        24: "surcharge_date",          # Y: Дата допл
        25: "final_surcharge_amount",  # Z: Финальный платеж
        26: "final_surcharge_date",    # AA: Дата Финал.пл.
        27: "outstanding_debt",        # AB: Сумма Долга
        28: "payment_terms",           # AC: Пояснения
        29: "agent_fee",               # AD: Агентское
        30: "agent_payout_op",           # AE: Выпл. Агент.
        31: "men_zp_payout_op",          # AF: Выпл.МенЗП
        32: "manager_zp_blank",        # AG: Мен. ЗП (по бланку)
        33: "zp_manager_request_text",   # AH: Запрос суммы на выплату
        34: "zp_manager_payout",         # AI: Выплата. Мен. ЗП
        35: "zp_manager_payout_date",    # AJ: Дата выпл. мен.
        # 36: AK (пустая колонка)
        37: "materials_fact_op",         # AL: Материалы Факт
        38: "montazh_fact_op",           # AM: Монтаж Факт
        39: "logistics_fact_op",         # AN: Логистика факт
        40: "logistics_fact_date",       # AO: Дата лог.
        41: "loaders_fact_op",           # AP: Грузчики факт
        42: "loaders_fact_date",         # AQ: Дата груз.
        # 43: AR — Команда боту (human-writable)
        44: "npn_request_op",            # AS: Запрос НПН
        45: "npn_amount",               # AT: Выдано НПН
        46: "npn_payout_op",            # AU: Выдано НПН (сумма)
        47: "npn_payout_date_op",        # AV: Дата НПН
        # 48: AW (Месяц — не импортируем)
        49: "taxes_fact_op",             # AX: Налоги факт
        50: "profit_fact_credit_op",     # AY: Фактическая прибыль по кредитным счетам
        51: "profit_fact_op",            # AZ: Фактическая прибыль по каждому счёту
    }

    def _parse_num(self, val: str) -> float | None:
        """Parse number from string, handling spaces/commas as thousand separators."""
        if not val or not val.strip():
            return None
        v = val.strip().replace("\u00a0", "").replace(" ", "").rstrip("%")
        # Strip currency suffixes: "1000р.", "1000 руб", "1000₽"
        v = re.sub(r'[р₽]\.?$|руб\.?$', '', v).strip()
        # Google Sheets uses comma as thousand separator (257,000 = 257000)
        # If comma exists AND digits after comma are exactly 3 → thousand separator
        if "," in v:
            parts = v.split(",")
            if all(len(p) == 3 for p in parts[1:]) and all(p.isdigit() for p in parts[1:]):
                # Thousand separator: "257,000" → "257000"
                v = v.replace(",", "")
            else:
                # Decimal comma: "26.5%" already stripped %, just replace
                v = v.replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return None

    def _parse_date_dmy(self, val: str) -> str | None:
        """Parse date string → YYYY-MM-DD ISO.

        Supported formats: DD.MM.YYYY, DD/MM/YYYY, DD.MM.YY,
        YYYY-MM-DD (ISO passthrough), Google Sheets serial number.
        """
        if not val or not val.strip():
            return None
        raw = val.strip()
        # ISO passthrough
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            return raw
        # Google Sheets serial number (integer or float like 46107 or 46107.0)
        try:
            serial = float(raw)
            if 30000 < serial < 60000:  # reasonable range: ~1982–2064
                from datetime import datetime, timedelta
                base = datetime(1899, 12, 30)
                dt = base + timedelta(days=int(serial))
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        # DD.MM.YYYY or DD/MM/YYYY or DD.MM.YY
        for sep in (".", "/"):
            parts = raw.split(sep)
            if len(parts) == 3:
                try:
                    d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
                    if y < 100:
                        y += 2000
                    return f"{y:04d}-{m:02d}-{d:02d}"
                except (ValueError, IndexError):
                    continue
        return None

    _OP_NUMERIC_FIELDS = frozenset(
        {
            "amount",
            "first_payment_amount",
            "estimated_materials",
            "estimated_installation",
            "estimated_loaders",
            "estimated_logistics",
            "nds_amount",
            "outstanding_debt",
            "surcharge_amount",
            "final_surcharge_amount",
            "agent_fee",
            "manager_zp_blank",
            "npn_amount",
            "profit_tax",
            "rentability_calc",
            "materials_fact_op",
            "montazh_fact_op",
            "zp_manager_payout",
            "logistics_fact_op",
            "loaders_fact_op",
            "agent_payout_op",
            "men_zp_payout_op",
            "npn_payout_op",
            "taxes_fact_op",
        }
    )
    _OP_DATE_FIELDS = frozenset(
        {
            "receipt_date",
            "actual_completion_date",
            "surcharge_date",
            "final_surcharge_date",
            "zp_manager_payout_date",
            "logistics_fact_date",
            "loaders_fact_date",
            "npn_payout_date_op",
        }
    )

    def _parse_op_row(self, row_values: list[str]) -> dict[str, Any] | None:
        inv_num = str(row_values[4]).strip() if len(row_values) > 4 else ""
        if not inv_num:
            return None

        parsed: dict[str, Any] = {"invoice_number": inv_num}
        for col_idx, field in self._OP_COL_MAP.items():
            if field == "invoice_number":
                continue

            raw_value = str(row_values[col_idx]).strip() if col_idx < len(row_values) else ""
            if not raw_value:
                parsed[field] = None
                continue

            if field in self._OP_NUMERIC_FIELDS:
                num = self._parse_num(raw_value)
                if num is not None:
                    parsed[field] = num
            elif field in self._OP_DATE_FIELDS:
                parsed_date = self._parse_date_dmy(raw_value)
                if parsed_date:
                    parsed[field] = parsed_date
                else:
                    log.warning("ОП import: cannot parse date field '%s' = '%s' (invoice %s)", field, raw_value, inv_num)
            elif field == "deadline_days":
                num = self._parse_num(raw_value)
                if num is not None:
                    parsed[field] = int(num)
            elif field == "is_credit":
                # Source: 0 = кредит, 1 = б.н.
                parsed[field] = 1 if raw_value == "0" else 0
            elif field == "client_source":
                # 1 = Свой (own), 2 = Атм (gd_lead)
                if raw_value == "1":
                    parsed[field] = "own"
                elif raw_value == "2":
                    parsed[field] = "gd_lead"
                else:
                    parsed[field] = raw_value
            else:
                parsed[field] = raw_value

        return parsed

    def _detect_op_sheet_start_row(self, all_data: list[list[str]]) -> int:
        """Detect the header row in the source sheet and return the first data row."""
        header_markers = ("номер счета", "контрагент", "адрес", "дата пост", "сумма")
        for idx, row in enumerate(all_data[:10]):
            normalized = [str(cell).strip().lower() for cell in row if str(cell).strip()]
            if not normalized:
                continue
            score = sum(
                1
                for marker in header_markers
                if any(marker in cell for cell in normalized)
            )
            if score >= 3:
                return idx + 1
        return 1 if all_data else 0

    def read_op_sheet_sync(self) -> list[dict[str, Any]]:
        """Read all rows from source 'Отдел продаж' sheet, return parsed dicts."""
        if not self.cfg.source_spreadsheet_id:
            return []

        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
        except Exception as e:
            log.error("Cannot open source spreadsheet: %s", e)
            return []

        try:
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except gspread.WorksheetNotFound:
            log.error("Sheet '%s' not found in source spreadsheet", self.cfg.source_sheet_name)
            return []

        all_data = ws.get_all_values()
        if len(all_data) < 2:
            return []

        start_row = self._detect_op_sheet_start_row(all_data)

        # Diagnostic: log first 3 non-empty values from col AA (index 26)
        aa_samples = []
        for row in all_data[start_row:]:
            if len(row) > 26 and str(row[26]).strip():
                aa_samples.append(str(row[26]).strip())
                if len(aa_samples) >= 3:
                    break
        if aa_samples:
            log.info("ОП col AA (final_surcharge_date) samples: %s", aa_samples)
        else:
            log.warning("ОП col AA (final_surcharge_date): all values empty")

        results: list[dict[str, Any]] = []
        for row_idx in range(start_row, len(all_data)):
            parsed = self._parse_op_row(all_data[row_idx])
            if parsed:
                results.append(parsed)

        log.info("Read %d invoices from source ОП sheet", len(results))
        return results

    def parse_op_row_from_webhook(self, row_values: list[str]) -> dict[str, Any] | None:
        """Parse a single row from webhook payload (same column order as ОП sheet).

        row_values: list of string cell values, index = column index.
        Returns parsed dict compatible with db.import_invoice_from_sheet(), or None.
        """
        return self._parse_op_row(row_values)

    async def read_op_sheet(self) -> list[dict[str, Any]]:
        """Async wrapper for reading source ОП sheet."""
        if not self.cfg.enabled:
            return []
        return await asyncio.to_thread(self.read_op_sheet_sync)

    def write_date_fact_to_op_sync(self, invoice_number: str, date_iso: str) -> bool:
        """Write Дата Факт (col J/9) back to source ОП sheet by invoice_number."""
        if not self.cfg.source_spreadsheet_id:
            return False
        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except Exception as e:
            log.error("Cannot open source sheet for write-back: %s", e)
            return False

        # Find row by invoice_number (col E, index 4, 1-based col 5)
        try:
            cell = ws.find(invoice_number, in_column=5)
        except gspread.CellNotFound:
            log.warning("Invoice %s not found in ОП sheet", invoice_number)
            return False

        if not cell:
            return False

        # Convert ISO date (possibly with time) to DD.MM.YYYY
        date_part = date_iso[:10]  # safely extract YYYY-MM-DD even if time/tz appended
        parts = date_part.split("-")
        if len(parts) == 3:
            date_dmy = f"{parts[2]}.{parts[1]}.{parts[0]}"
        else:
            date_dmy = date_iso

        # Col J = column 10 (1-based)
        ws.update_cell(cell.row, 10, date_dmy)
        log.info("Wrote Дата Факт %s for %s to ОП row %d", date_dmy, invoice_number, cell.row)
        return True

    async def write_date_fact_to_op(self, invoice_number: str, date_iso: str) -> bool:
        """Async wrapper."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(self.write_date_fact_to_op_sync, invoice_number, date_iso)

    # --- Generic field write-back to ОП ---

    _OP_FIELD_TO_COL: dict[str, int] = {
        "estimated_logistics": 16,  # col P (1-based) = index 15 → logistics
        "margin_pct": 21,           # col U (1-based) = "Рент-ть факт"
        "bot_status": 46,           # col AT (1-based) = Статус бота
        "montazh_stage": 47,        # col AU (1-based) = Стадия монтажа
    }

    def write_field_to_op_sync(self, invoice_number: str, field: str, value: Any) -> bool:
        """Write a single field back to ОП sheet by invoice_number."""
        col_1based = self._OP_FIELD_TO_COL.get(field)
        if col_1based is None:
            log.warning("write_field_to_op: unknown field %s", field)
            return False
        if not self.cfg.source_spreadsheet_id:
            return False
        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except Exception as e:
            log.error("Cannot open source sheet for write-back: %s", e)
            return False

        try:
            cell = ws.find(invoice_number, in_column=5)
        except gspread.CellNotFound:
            log.warning("Invoice %s not found in ОП sheet for field write", invoice_number)
            return False

        if not cell:
            return False

        ws.update_cell(cell.row, col_1based, value)
        log.info("Wrote %s=%s for %s to ОП row %d", field, value, invoice_number, cell.row)
        return True

    async def write_field_to_op(self, invoice_number: str, field: str, value: Any) -> bool:
        """Async wrapper for generic field write-back."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(self.write_field_to_op_sync, invoice_number, field, value)

    def write_cell_to_sheet_sync(
        self, sheet_name: str, row: int, col_1based: int, value: str,
    ) -> bool:
        """Write a value to a specific cell in the source spreadsheet."""
        if not self.cfg.source_spreadsheet_id:
            return False
        gc = self._get_client()
        try:
            sp = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = sp.worksheet(sheet_name)
        except Exception as e:
            log.error("Cannot open sheet %s for cell write: %s", sheet_name, e)
            return False
        ws.update_cell(row, col_1based, value)
        log.info("Wrote cell R%dC%d=%s in %s", row, col_1based, value[:50], sheet_name)
        return True

    async def write_cell_to_sheet(
        self, sheet_name: str, row: int, col_1based: int, value: str,
    ) -> bool:
        """Async wrapper for cell write."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(
            self.write_cell_to_sheet_sync, sheet_name, row, col_1based, value,
        )
