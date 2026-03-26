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

INVOICES_HEADER = [
    # — Отдел продаж structure (0-45) —
    "№",            # 0
    "В работу",     # 1  manual
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
    "Лид КВ",              # 61
    "Лид КИА",             # 62
    "Лид НПН",             # 63
    "Счет КВ",             # 64
    "Счет КИА",            # 65
    "Счет НПН",            # 66
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
    # — Аналитика (77-80) —
    "Расчет vs Факт",     # 77
    "Прибыль факт",       # 78
    "Рент-ть факт %",     # 79
    "Перерасчет прибыли",  # 80
    # — Кредитный учёт (81-85) —
    "Кредит вход",         # 81 — сумма входящего кредита
    "Кредит вход коммент", # 82 — Менеджер, адрес
    "Кредит расход",       # 83 — накопительная сумма расходов
    "Кредит назначение",   # 84 — лог назначений расходов
    "Кредит баланс",       # 85 — формула: вход - расход
]

# Column indices the bot NEVER overwrites (manual-only + formula)
# Removed 7 (Свой/Атм→client_source), 18,19,21,24 — now bot-managed (Plan/Fact)
_MANUAL_COLS = frozenset([1, 5, 11, 12,
                          33, 34, 37, 40, 45])


@dataclass
class SheetsConfig:
    enabled: bool
    spreadsheet_id: str
    projects_tab: str
    tasks_tab: str
    invoices_tab: str = "Invoices"
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

        first_col = ws.col_values(1)
        row_index = {}
        for row_num, value in enumerate(first_col[1:], start=2):
            key = str(value).strip()
            if key and key not in row_index:
                row_index[key] = row_num
        self._row_indexes[title] = row_index
        self._next_rows[title] = max(2, len(first_col) + 1)
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

        cells: dict[int, Any] = {
            0: invoice.get("id") or "",
            2: manager_label,
            3: "Да" if invoice.get("edo_signed") else "",
            4: invoice.get("client_name") or "",
            6: "0" if invoice.get("is_credit") else "1",  # ОП convention: 0=кредит, 1=б.н.
            7: {"own": 1, "gd_lead": 2}.get(invoice.get("client_source", ""), "")
               or invoice.get("client_type") or "",
            8: invoice.get("invoice_number") or "",
            9: invoice.get("object_address") or "",
            10: self._fmt_sheet_date(invoice.get("receipt_date")),
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
            39: self._fmt_amount(invoice.get("men_zp_payout_op")),  # AN ← ОП AF
            42: self._fmt_amount(invoice.get("npn_amount")),        # AQ ← ОП AT
            43: self._fmt_amount(invoice.get("npn_payout_op")),     # AR ← ОП AU
            44: invoice.get("npn_payout_date_op") or "",            # AS ← ОП AV
            46: "" if invoice.get("status") == "credit" else (invoice.get("status") or ""),
            47: _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or ""),
            48: invoice.get("supplier") or "",
            49: invoice.get("material_type") or "",
            50: invoice.get("parent_invoice_id") or "",
            51: invoice.get("montazh_stage") or "",
            52: "Да" if invoice.get("installer_ok") else "",
            53: "Да" if invoice.get("no_debts") else "",
            54: "",  # очистка (перенесено в 74)
            55: "",  # очистка (перенесено в 75)
            56: invoice.get("zp_installer_status") or "",
            59: format_dt_iso(invoice.get("created_at"), self.cfg.timezone_name),
            60: format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name),
            # — Статусы жизненного цикла —
            61: _li.get("kv", ""),            # BJ Лид КВ (дата получения лида)
            62: _li.get("kia", ""),           # BK Лид КИА
            63: _li.get("npn", ""),           # BL Лид НПН
            64: _li.get("inv_kv", ""),        # BM Счет КВ (дата выставления счёта)
            65: _li.get("inv_kia", ""),       # BN Счет КИА
            66: _li.get("inv_npn", ""),       # BO Счет НПН
            67: "Да" if invoice.get("status") == "in_progress" else "", # BP В работе
            68: "Да" if invoice.get("status") == "ended" else "",       # BQ Счет END
            69: self._fmt_amount(invoice.get("loaders_fact_op")),    # BR Грузчики факт ← ОП AP
            72: self._fmt_amount(invoice.get("logistics_fact_op") or invoice.get("actual_logistics")),  # BU Логистика Факт
            73: _li.get("lead_status", ""),   # BV Статус лида
            # — Блок Замерщик (перенос из 54/55/69) —
            74: invoice.get("zp_status") or "",                          # BW ЗП Замерщик
            75: self._fmt_amount(invoice.get("zp_zamery_total")),        # BX ЗП Замерщик сумма
            76: invoice.get("zamery_info_op") or invoice.get("_zamery_info") or "",  # BY Замеры ← ОП I (fallback: бот)
            # — Аналитика —
            77: invoice.get("_plan_fact_label") or "",                   # BZ Расчет vs Факт
            # 78, 79, 80 заполняются ниже из cost_card
            # — Кредитный учёт —
            85: f"=IF(CD{row}=\"\",\"\",CD{row}-CF{row})",              # CH Кредит баланс
        }

        # Кредит входящий (81-82): is_credit=1 — единственный источник правды
        is_credit = bool(invoice.get("is_credit"))
        if is_credit:
            cells[81] = self._fmt_amount(invoice.get("amount"))          # CD Кредит вход
            mgr_label = manager_label or ""
            addr = invoice.get("object_address") or ""
            cells[82] = f"{mgr_label}, {addr}".strip(", ") if (mgr_label or addr) else ""
        else:
            cells[81] = ""
            cells[82] = ""

        # Кредит расход (83) и назначение (84): из _credit_expenses
        credit_exp = invoice.get("_credit_expenses") or {}
        cells[83] = self._fmt_amount(credit_exp.get("total")) if credit_exp.get("total") else ""
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

        # Формулы: НДС (V), Нал.приб. (W), НПН (AP), Прибыль (U), Рент-ть (X)
        cells[21] = f"=((O{row}*22/122)-(Q{row}*22/122))*G{row}"              # V
        cells[22] = f"=((O{row}-Q{row}-R{row}-S{row}-T{row}-V{row})/100*20)*G{row}"  # W
        cells[41] = f"=(O{row}-Q{row}-R{row}-S{row}-T{row}-V{row}-W{row})*10/100"    # AP (НПН 10%)
        cells[20] = f"=O{row}-Q{row}-R{row}-S{row}-T{row}-V{row}-W{row}"              # U (Прибыль)
        cells[23] = f'=IF(O{row}>0,U{row}/O{row}*100,0)'                      # X (Рент-ть)

        # Материалы Факт: ОП (уже закупленные) + дочерние счета (новые)
        _mat_op = float(invoice.get("materials_fact_op") or 0)
        _mat_children = _c.get("materials_total", 0) if _c else 0
        _mat_combined = _mat_op + _mat_children
        if _mat_combined:
            cells[71] = self._fmt_amount(_mat_combined)

        if _c:
            fact_pct = _c.get("margin_pct", 0)
            fact_margin = _c.get("margin", 0)
            cells[24] = f"{fact_pct:.1f}%" if fact_pct else ""
            cells[57] = self._fmt_amount(_c.get("supplier_payments_total"))
            cells[58] = self._fmt_amount(_c.get("total_cost"))
            # Прибыль факт (78) и Рентабельность факт % (79)
            cells[78] = self._fmt_amount(fact_margin) if fact_margin else ""  # CA Прибыль факт
            cells[79] = f"{fact_pct:.1f}%" if fact_pct else ""               # CB Рент-ть факт %
            # Перерасчет прибыли (80): разница план-факт при перерасходе
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
                cells[80] = self._fmt_amount(delta)                          # CC Перерасчет
            else:
                cells[80] = ""

        # Монтаж Факт: ОП (уже оплаченный) + ЗП монтажника (новые)
        _mont_op = float(invoice.get("montazh_fact_op") or 0)
        _mont_zp = float(invoice.get("zp_installer_amount") or 0)
        _mont_combined = _mont_op + _mont_zp
        if _mont_combined:
            cells[70] = self._fmt_amount(_mont_combined)

        if is_new:
            cells[12] = (
                f'=IF(OR(K{row}="",L{row}=""),"",TEXT('
                f'DATEVALUE(MID(K{row},7,4)&"-"&MID(K{row},4,2)&"-"&LEFT(K{row},2))'
                f'+L{row},"DD.MM.YYYY"))'
            )
            cells[45] = (
                f'=IF(K{row}="","",SWITCH(VALUE(MID(K{row},4,2)),'
                f'1,"Январь",2,"Февраль",3,"Март",4,"Апрель",'
                f'5,"Май",6,"Июнь",7,"Июль",8,"Август",'
                f'9,"Сентябрь",10,"Октябрь",11,"Ноябрь",12,"Декабрь"))'
            )

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
        inv_id = invoice.get("id")
        if not inv_id:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)
            row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_id)
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

    def upsert_invoices_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str, dict[str, Any] | None]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)
            batch_data: list[dict[str, Any]] = []
            count = 0
            for invoice, manager_label, cost in items:
                inv_id = invoice.get("id")
                if not inv_id:
                    continue
                row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_id)
                cells = self._invoice_cells(invoice, manager_label, cost, row=row, is_new=is_new)
                if not is_new:
                    cells = {k: v for k, v in cells.items() if k not in _MANUAL_COLS}
                batch_data.extend(self._invoice_batch_ranges(row, cells))
                count += 1
            self._flush_batch_update(ws, batch_data, chunk_size=500)

            # Сортировка: старые счета вверху, новые внизу.
            # Столбец K (index=10) = receipt_date, записан как =DATE() — корректная сортировка.
            try:
                self._sort_ws_by_date(ws, sort_col_index=10)
            except Exception:
                pass  # не критично если сортировка не удалась

            return count

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
        # 18: Налог на приб. (не импортируем)
        # 19: РП - 10% (не импортируем)
        # 20: Прибыль расч (не импортируем)
        21: "rentability_calc",        # V: Рент-ть расчетная
        # 22: Рент-ть факт (не импортируем)
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
        # 48: AW (пусто)
        49: "taxes_fact_op",             # AX: Налоги факт
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
