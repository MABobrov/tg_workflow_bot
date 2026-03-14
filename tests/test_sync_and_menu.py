from __future__ import annotations

import asyncio
from types import SimpleNamespace

from aiogram.exceptions import TelegramBadRequest

from app.db import Database
from app.enums import InvoiceStatus, Role
from app.handlers.tasks import _safe_edit_task_markup
from app.handlers.common import _menu_context
from app.integrations.sheets import GoogleSheetsService, SheetsConfig
from app.services.menu_context import build_main_menu_for_user


class _FakeCell:
    def __init__(self, row: int) -> None:
        self.row = row


class _FakeWorksheet:
    def __init__(self, row: int = 2, first_col_values: list[str] | None = None) -> None:
        self.row = row
        self.batch_data: list[dict[str, object]] = []
        self.update_calls: list[tuple[list[list[object]], str]] = []
        self.col_values_calls = 0
        self.first_col_values = first_col_values or []

    def find(self, value: str, in_column: int | None = None) -> _FakeCell:
        return _FakeCell(self.row)

    def update(
        self,
        values: list[list[object]],
        range_name: str,
        value_input_option: str | None = None,
    ) -> None:
        self.update_calls.append((values, range_name))

    def col_values(self, col: int) -> list[str]:
        self.col_values_calls += 1
        return list(self.first_col_values)

    def batch_update(
        self,
        batch_data: list[dict[str, object]],
        value_input_option: str | None = None,
    ) -> None:
        self.batch_data = batch_data


def _sheets_service() -> GoogleSheetsService:
    return GoogleSheetsService(
        SheetsConfig(
            enabled=True,
            spreadsheet_id="test-sheet",
            projects_tab="Projects",
            tasks_tab="Tasks",
        )
    )


def _expand_row_batch_updates(batch_data: list[dict[str, object]]) -> dict[str, object]:
    cells: dict[str, object] = {}
    for item in batch_data:
        range_name = str(item["range"])
        values = item["values"][0]
        start, _, end = range_name.partition(":")
        if not end:
            cells[start] = values[0]
            continue

        start_col = "".join(ch for ch in start if ch.isalpha())
        end_col = "".join(ch for ch in end if ch.isalpha())
        row_num = "".join(ch for ch in start if ch.isdigit())

        def col_to_idx(col: str) -> int:
            idx = 0
            for ch in col:
                idx = idx * 26 + (ord(ch) - 64)
            return idx - 1

        for offset, value in enumerate(values):
            col_idx = col_to_idx(start_col) + offset
            if col_idx > col_to_idx(end_col):
                break
            cells[f"{GoogleSheetsService._col_letter(col_idx)}{row_num}"] = value
    return cells


def test_parse_op_row_from_webhook_keeps_blank_cells_for_authoritative_sync() -> None:
    service = _sheets_service()

    parsed = service.parse_op_row_from_webhook(["", "", "", "", "A-100"])

    assert parsed is not None
    assert parsed["invoice_number"] == "A-100"
    assert parsed["client_name"] is None
    assert parsed["receipt_date"] is None
    assert parsed["amount"] is None
    assert parsed["outstanding_debt"] is None


def test_upsert_invoice_sync_writes_sheet_dates_and_clears_empty_cells(monkeypatch) -> None:
    service = _sheets_service()
    fake_ws = _FakeWorksheet(row=2, first_col_values=["№", "12"])
    monkeypatch.setattr(service, "_get_or_create_ws", lambda title, header: fake_ws)

    service.upsert_invoice_sync(
        {
            "id": 12,
            "invoice_number": "A-100",
            "client_name": None,
            "receipt_date": "2026-03-05",
            "actual_completion_date": "2026-03-10",
            "first_payment_amount": None,
            "created_at": "2026-03-01T10:00:00+00:00",
            "updated_at": "2026-03-02T10:00:00+00:00",
        }
    )

    updates = _expand_row_batch_updates(fake_ws.batch_data)
    assert updates["E2"] == ""
    assert updates["K2"] == "05.03.2026"
    assert updates["N2"] == "10.03.2026"
    assert updates["P2"] == ""


def test_read_op_sheet_sync_silently_skips_missing_source_sheet(caplog) -> None:
    service = _sheets_service()

    with caplog.at_level("WARNING"):
        result = service.read_op_sheet_sync()

    assert result == []
    assert not caplog.records


def test_upsert_tasks_bulk_reuses_cached_first_column(monkeypatch) -> None:
    service = _sheets_service()
    fake_ws = _FakeWorksheet(first_col_values=["ID задачи"])
    monkeypatch.setattr(service, "_get_or_create_ws", lambda title, header: fake_ws)

    first_batch = [
        (
            {
                "id": 1,
                "type": "issue",
                "status": "open",
                "created_at": "2026-03-01T10:00:00+00:00",
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
            "",
        ),
        (
            {
                "id": 2,
                "type": "issue",
                "status": "open",
                "created_at": "2026-03-01T10:00:00+00:00",
                "updated_at": "2026-03-01T10:00:00+00:00",
            },
            "",
        ),
    ]
    second_batch = [
        (
            {
                "id": 1,
                "type": "issue",
                "status": "done",
                "created_at": "2026-03-01T10:00:00+00:00",
                "updated_at": "2026-03-02T10:00:00+00:00",
            },
            "",
        )
    ]

    assert service.upsert_tasks_bulk_sync(first_batch) == 2
    assert service.upsert_tasks_bulk_sync(second_batch) == 1
    assert fake_ws.col_values_calls == 1
    assert len(fake_ws.batch_data) == 1


def test_menu_context_includes_gd_supplier_payment_badge(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(1, "gd_user", "GD User")
            await db.set_user_role(1, Role.GD)

            invoice_id = await db.create_invoice(
                invoice_number="GD-1",
                project_id=None,
                created_by=1,
                creator_role=Role.GD,
            )
            await db.set_invoice_zp_status(invoice_id, "requested")

            ctx = await _menu_context(db, 1, Role.GD)
        finally:
            await db.close()

        assert ctx["gd_supplier_pay_unread"] == 1

    asyncio.run(scenario())


def test_build_main_menu_for_user_uses_shared_badge_context(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(1, "gd_user", "GD User")
            await db.set_user_role(1, Role.GD)

            invoice_id = await db.create_invoice(
                invoice_number="GD-2",
                project_id=None,
                created_by=1,
                creator_role=Role.GD,
            )
            await db.set_invoice_zp_status(invoice_id, "requested")

            keyboard = await build_main_menu_for_user(
                db,
                SimpleNamespace(admin_ids=set()),
                1,
                Role.GD,
            )
        finally:
            await db.close()

        labels = [button.text for row in keyboard.keyboard for button in row]
        assert "💸 Оплата поставщику 🔴1" in labels

    asyncio.run(scenario())


class _FakeTaskMessage:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.calls: list[object | None] = []

    async def edit_reply_markup(self, reply_markup: object | None = None) -> None:
        self.calls.append(reply_markup)
        if self.exc:
            raise self.exc


def test_safe_edit_task_markup_clears_inline_keyboard() -> None:
    async def scenario() -> None:
        message = _FakeTaskMessage()

        await _safe_edit_task_markup(message, reply_markup=None)

        assert message.calls == [None]

    asyncio.run(scenario())


def test_safe_edit_task_markup_ignores_expected_bad_request() -> None:
    async def scenario() -> None:
        message = _FakeTaskMessage(
            TelegramBadRequest(method="editMessageReplyMarkup", message="message is not modified")
        )

        await _safe_edit_task_markup(message, reply_markup=None)

        assert message.calls == [None]

    asyncio.run(scenario())


def test_upsert_invoice_from_op_uses_common_sheet_import_logic(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()

            invoice_id, is_new = await db.upsert_invoice_from_op(
                {
                    "invoice_number": "КВ-99",
                    "actual_completion_date": "2026-03-05",
                    "outstanding_debt": 0.0,
                }
            )
            first = await db.get_invoice(invoice_id)

            updated_id, updated_is_new = await db.upsert_invoice_from_op(
                {
                    "invoice_number": "КВ-99",
                    "actual_completion_date": "2026-03-05",
                    "outstanding_debt": 150.0,
                }
            )
            second = await db.get_invoice(invoice_id)
        finally:
            await db.close()

        assert is_new is True
        assert updated_is_new is False
        assert updated_id == invoice_id
        assert first is not None
        assert second is not None
        assert first["creator_role"] == Role.MANAGER_KV
        assert first["status"] == InvoiceStatus.ENDED
        assert second["status"] == InvoiceStatus.PAID

    asyncio.run(scenario())


def test_assign_invoices_by_marker_updates_creator_role(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            invoice_id = await db.create_invoice(
                invoice_number="КИА-42",
                project_id=None,
                created_by=10,
                creator_role=Role.MANAGER_NPN,
            )

            updated = await db.assign_invoices_by_marker({"КИА": 777})
            invoice = await db.get_invoice(invoice_id)
        finally:
            await db.close()

        assert updated == 1
        assert invoice is not None
        assert invoice["created_by"] == 777
        assert invoice["creator_role"] == Role.MANAGER_KIA

    asyncio.run(scenario())
