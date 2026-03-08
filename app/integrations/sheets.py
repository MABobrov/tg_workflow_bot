from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

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
    "ID",
    "№ Счёта",
    "Адрес",
    "Сумма",
    "Статус",
    "Тип (б/н/кред)",
    "Менеджер",
    "Роль менеджера",
    "Поставщик",
    "Тип материала",
    "Родит. счёт ID",
    "Этап монтажа",
    "Монтажник ОК",
    "ЭДО подписано",
    "Долгов нет",
    "ЗП Замерщик",
    "ЗП Замерщик сумма",
    "ЗП Монтажник",
    "ЗП Монтажник сумма",
    "ЗП Менеджер",
    "ЗП Менеджер сумма",
    "Материалы итого",
    "Оплаты пост. итого",
    "Расходы итого",
    "Маржа",
    "Маржа %",
    "Создан",
    "Обновлён",
]


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

        _c = cost or {}
        is_credit = "Кредит" if invoice.get("is_credit") else "б/н"

        _ROLE_LABELS = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }

        row_values = [
            inv_id,
            invoice.get("invoice_number") or "",
            invoice.get("object_address") or "",
            self._fmt_amount(invoice.get("amount")),
            invoice.get("status") or "",
            is_credit,
            manager_label,
            _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or ""),
            invoice.get("supplier") or "",
            invoice.get("material_type") or "",
            invoice.get("parent_invoice_id") or "",
            invoice.get("montazh_stage") or "",
            "Да" if invoice.get("installer_ok") else "",
            "Да" if invoice.get("edo_signed") else "",
            "Да" if invoice.get("no_debts") else "",
            invoice.get("zp_status") or "",
            self._fmt_amount(invoice.get("zp_zamery_total")),
            invoice.get("zp_installer_status") or "",
            self._fmt_amount(invoice.get("zp_installer_amount")),
            invoice.get("zp_manager_status") or "",
            self._fmt_amount(invoice.get("zp_manager_amount")),
            self._fmt_amount(_c.get("materials_total")) if _c else "",
            self._fmt_amount(_c.get("supplier_payments_total")) if _c else "",
            self._fmt_amount(_c.get("total_cost")) if _c else "",
            self._fmt_amount(_c.get("margin")) if _c else "",
            f"{_c.get('margin_pct', 0):.1f}%" if _c and _c.get("margin_pct") is not None else "",
            format_dt_iso(invoice.get("created_at"), self.cfg.timezone_name),
            format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name),
        ]

        try:
            cell = ws.find(str(inv_id), in_column=1)
        except gspread.CellNotFound:
            cell = None
        if cell:
            row = cell.row
            ws.update([row_values], f"A{row}")
        else:
            ws.append_row(row_values, value_input_option="USER_ENTERED")

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
