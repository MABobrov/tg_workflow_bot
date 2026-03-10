from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
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
    "ЗП Замерщик",          # 54
    "ЗП Замерщик сумма",    # 55
    "ЗП Монтажник статус",  # 56
    "Оплаты пост. итого",   # 57
    "Расходы итого",        # 58
    "Создан",               # 59
    "Обновлён",             # 60
]

# Column indices the bot NEVER overwrites (manual-only + formula)
# Removed 7 (Свой/Атм→client_source), 18,19,21,24 — now bot-managed (Plan/Fact)
_MANUAL_COLS = frozenset([1, 5, 11, 12, 22, 25, 26, 27, 28, 29,
                          31, 33, 34, 37, 38, 39, 40, 41, 42, 43, 44, 45])


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


class GoogleSheetsService:
    """Best-effort sync to Google Sheets.

    Calls are synchronous (gspread), so in the bot we call them via asyncio.to_thread().
    """

    def __init__(self, cfg: SheetsConfig):
        self.cfg = cfg
        self._gc: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None

    def _fmt_amount(self, amount: Any) -> str:
        if isinstance(amount, (int, float)):
            return f"{amount:,.0f}".replace(",", " ")
        if amount is None:
            return ""
        return str(amount)

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
        sh = self._get_spreadsheet()
        try:
            ws = sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(header) + 2))
        # ensure header
        values = ws.row_values(1)
        if values[: len(header)] != header:
            ws.update([header], "A1")
        return ws

    def upsert_project_sync(self, project: dict[str, Any], manager_label: str = "") -> None:
        ws = self._get_or_create_ws(self.cfg.projects_tab, PROJECTS_HEADER)

        code = project.get("code")
        if not code:
            return

        row_values = [
            code,
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

        try:
            cell = ws.find(str(code), in_column=1)  # 1-based column index
        except gspread.CellNotFound:
            cell = None
        if cell:
            row = cell.row
            ws.update([row_values], f"A{row}")
        else:
            ws.append_row(row_values, value_input_option="USER_ENTERED")

    def upsert_task_sync(self, task: dict[str, Any], project_code: str = "") -> None:
        ws = self._get_or_create_ws(self.cfg.tasks_tab, TASKS_HEADER)

        tid = task.get("id")
        if not tid:
            return

        payload = self._task_payload_fields(task)
        row_values = [
            tid,
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

        try:
            cell = ws.find(str(tid), in_column=1)
        except gspread.CellNotFound:
            cell = None
        if cell:
            row = cell.row
            ws.update([row_values], f"A{row}")
        else:
            ws.append_row(row_values, value_input_option="USER_ENTERED")

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
        ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)

        inv_id = invoice.get("id")
        if not inv_id:
            return

        _ROLE_LABELS = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }
        _c = cost or {}

        # Build bot-managed cells: {col_index: value}
        cells: dict[int, Any] = {
            0: inv_id,                                                              # №
            2: manager_label,                                                        # Менеджер
            3: "Да" if invoice.get("edo_signed") else "",                           # Бухг.ЭДО
            6: "0" if invoice.get("is_credit") else "1",                            # Б.Н./Кред
            7: {"own": "Свой", "gd_lead": "Атм"}.get(invoice.get("client_source", ""), ""),  # Свой/Атм
            8: invoice.get("invoice_number") or "",                                  # Номер счета
            9: invoice.get("object_address") or "",                                  # Адрес
            14: self._fmt_amount(invoice.get("amount")),                             # Сумма
            30: self._fmt_amount(invoice.get("outstanding_debt")),                   # Долг
            32: invoice.get("closing_docs_status") or "",                            # Закр.док
            35: self._fmt_amount(invoice.get("zp_manager_amount")),                  # Мен.ЗП
            36: invoice.get("zp_manager_status") or "",                              # Запрос
            # Bot-specific (46+)
            46: invoice.get("status") or "",                                         # Статус
            47: _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or ""),
            48: invoice.get("supplier") or "",                                       # Поставщик
            49: invoice.get("material_type") or "",                                  # Тип материала
            50: invoice.get("parent_invoice_id") or "",                              # Родит. счёт
            51: invoice.get("montazh_stage") or "",                                  # Этап монтажа
            52: "Да" if invoice.get("installer_ok") else "",                         # Монтажник ОК
            53: "Да" if invoice.get("no_debts") else "",                             # Долгов нет
            54: invoice.get("zp_status") or "",                                      # ЗП Замерщик
            55: self._fmt_amount(invoice.get("zp_zamery_total")),                    # ЗП Замерщик сумма
            56: invoice.get("zp_installer_status") or "",                            # ЗП Монтажник статус
            59: format_dt_iso(invoice.get("created_at"), self.cfg.timezone_name),    # Создан
            60: format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name),    # Обновлён
        }

        # Conditional: write only if DB has a value
        if invoice.get("client_name"):
            cells[4] = invoice["client_name"]
        if invoice.get("receipt_date"):
            cells[10] = format_date_iso(invoice["receipt_date"], self.cfg.timezone_name)
        if invoice.get("actual_completion_date"):
            cells[13] = format_date_iso(invoice["actual_completion_date"], self.cfg.timezone_name)
        if invoice.get("first_payment_amount") is not None:
            cells[15] = self._fmt_amount(invoice["first_payment_amount"])

        # Estimated (plan) columns — from manager input
        amount = float(invoice.get("amount") or 0)
        est_glass = float(invoice.get("estimated_glass") or 0)
        est_profile = float(invoice.get("estimated_profile") or 0)
        est_mat_legacy = float(invoice.get("estimated_materials") or 0)
        est_inst = float(invoice.get("estimated_installation") or 0)
        est_load = float(invoice.get("estimated_loaders") or 0)
        est_log = float(invoice.get("estimated_logistics") or 0)
        materials_total = est_glass + est_profile + est_mat_legacy
        est_total = materials_total + est_inst + est_load + est_log

        # НДС с возвратным
        refundable_base = est_glass + est_profile
        output_vat = amount * 22 / 122 if amount > 0 else 0
        input_vat = refundable_base * 22 / 122 if refundable_base > 0 else 0
        net_vat = output_vat - input_vat
        est_profit = amount - est_total - net_vat
        est_pct = (est_profit / amount * 100) if amount > 0 else 0

        if any([est_glass, est_profile, est_mat_legacy, est_inst, est_load, est_log]):
            cells[16] = self._fmt_amount(materials_total)                            # Расч.мат. (стекло+профиль)
            cells[17] = self._fmt_amount(est_inst)                                   # Установка
            cells[18] = self._fmt_amount(est_load)                                   # Грузчики
            cells[19] = self._fmt_amount(est_log)                                    # Логистика
            cells[20] = self._fmt_amount(est_profit)                                 # Прибыль
            cells[21] = self._fmt_amount(net_vat)                                    # Чистый НДС (с возвратом)
            cells[23] = f"{est_pct:.1f}%"                                            # Рент-ть расч

        # Actual (fact) columns
        if _c:
            fact_pct = _c.get("margin_pct", 0)
            cells[24] = f"{fact_pct:.1f}%" if fact_pct else ""                       # Рент-ть факт
            cells[57] = self._fmt_amount(_c.get("supplier_payments_total"))           # Оплаты пост.
            cells[58] = self._fmt_amount(_c.get("total_cost"))                        # Расходы итого

        # Find or create row
        try:
            cell = ws.find(str(inv_id), in_column=1)
            row = cell.row
            is_new = False
        except gspread.CellNotFound:
            row = len(ws.get_all_values()) + 1
            is_new = True

        # Set formulas for new rows only
        if is_new:
            cells[12] = f'=IF(K{row}="","",K{row}+L{row})'
            cells[45] = f'=IF(K{row}="","",TEXT(K{row},"MMMM"))'

        # Build batch update with A1 notation
        batch_data = []
        for col_idx, value in cells.items():
            col_letter = self._col_letter(col_idx)
            batch_data.append({
                "range": f"{col_letter}{row}",
                "values": [[value]],
            })

        ws.batch_update(batch_data, value_input_option="USER_ENTERED")

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
