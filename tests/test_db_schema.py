from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database


def test_init_schema_adds_client_contact_to_invoices(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            cur = await db.conn.execute("PRAGMA table_info(invoices)")
            columns = {row["name"] for row in await cur.fetchall()}
        finally:
            await db.close()

        assert "client_contact" in columns

    asyncio.run(scenario())


def test_import_zamery_invoices_persists_contact_and_skips_invalid_rows(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            inserted = await db.import_zamery_invoices(
                [
                    {
                        "invoice_number": "ЗМ-КВ-1",
                        "object_address": "Москва",
                        "client_contact": "Иван +79990000000",
                    },
                    {
                        "invoice_number": "   ",
                        "object_address": "Без номера",
                        "client_contact": "bad",
                    },
                ],
                zamery_user_id=123,
            )
            cur = await db.conn.execute(
                "SELECT invoice_number, object_address, client_contact, created_by, assigned_to "
                "FROM invoices ORDER BY id"
            )
            rows = [dict(row) for row in await cur.fetchall()]
        finally:
            await db.close()

        assert inserted == 1
        assert rows == [
            {
                "invoice_number": "ЗМ-КВ-1",
                "object_address": "Москва",
                "client_contact": "Иван +79990000000",
                "created_by": 123,
                "assigned_to": 123,
            }
        ]

    asyncio.run(scenario())


def test_import_invoice_from_sheet_updates_existing_invoice_fields(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            invoice_id = await db.import_invoice_from_sheet(
                invoice_number="A-100",
                created_by=1,
                creator_role="manager_kv",
                status="in_progress",
                object_address="Старый адрес",
                amount=1000.0,
                is_credit=False,
                client_name="Старый клиент",
                traffic_source="ads",
                receipt_date="2026-03-01",
                deadline_days=10,
                actual_completion_date=None,
                first_payment_amount=300.0,
                outstanding_debt=700.0,
                contract_type="old",
                closing_docs_status="waiting",
                payment_terms="old terms",
                description="old description",
            )
            updated_id = await db.import_invoice_from_sheet(
                invoice_number="A-100",
                created_by=999,
                creator_role="manager_npn",
                status="ended",
                object_address="Новый адрес",
                amount=2500.0,
                is_credit=True,
                client_name="Новый клиент",
                traffic_source=None,
                receipt_date=None,
                deadline_days=None,
                actual_completion_date="2026-03-05",
                first_payment_amount=None,
                outstanding_debt=None,
                contract_type=None,
                closing_docs_status=None,
                payment_terms=None,
                description="new description",
            )
            invoice = await db.get_invoice(invoice_id)
        finally:
            await db.close()

        assert updated_id == invoice_id
        assert invoice is not None
        assert invoice["status"] == "ended"
        assert invoice["is_credit"] == 1
        assert invoice["object_address"] == "Новый адрес"
        assert invoice["amount"] == 2500.0
        assert invoice["client_name"] == "Новый клиент"
        assert invoice["traffic_source"] is None
        assert invoice["receipt_date"] is None
        assert invoice["deadline_days"] is None
        assert invoice["actual_completion_date"] == "2026-03-05"
        assert invoice["first_payment_amount"] is None
        assert invoice["outstanding_debt"] is None
        assert invoice["contract_type"] is None
        assert invoice["closing_docs_status"] is None
        assert invoice["payment_terms"] is None
        assert invoice["description"] == "new description"

    asyncio.run(scenario())


def test_import_invoice_from_sheet_accepts_dict_payload_and_computes_status(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            invoice_id = await db.import_invoice_from_sheet(
                {
                    "invoice_number": "A-200",
                    "created_by": 55,
                    "creator_role": "manager_kv",
                    "object_address": "Москва",
                    "amount": 5000.0,
                    "is_credit": 0,
                    "actual_completion_date": "2026-03-10",
                    "outstanding_debt": 350.0,
                    "description": "dict import",
                }
            )
            invoice = await db.get_invoice(invoice_id)
        finally:
            await db.close()

        assert invoice is not None
        assert invoice["created_by"] == 55
        assert invoice["creator_role"] == "manager_kv"
        assert invoice["status"] == "paid"
        assert invoice["description"] == "dict import"

    asyncio.run(scenario())


def test_import_invoice_from_sheet_infers_owner_by_invoice_marker(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(777, "kia_user", "Kia User")
            await db.set_user_role(777, "manager_kia")
            invoice_id = await db.import_invoice_from_sheet(
                {
                    "invoice_number": "КИА-42",
                    "amount": 1200.0,
                }
            )
            invoice = await db.get_invoice(invoice_id)
        finally:
            await db.close()

        assert invoice is not None
        assert invoice["created_by"] == 777
        assert invoice["creator_role"] == "manager_kia"
        assert invoice["status"] == "in_progress"

    asyncio.run(scenario())
