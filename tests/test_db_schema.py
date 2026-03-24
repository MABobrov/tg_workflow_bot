from __future__ import annotations

import asyncio
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database
from app.enums import TaskStatus, TaskType
from app.services.assignment import apply_user_roles, resolve_default_assignee


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


def test_list_invoices_approaching_deadline_filters_active_top_level_invoices(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()

            today = date(2026, 3, 15)

            overdue_id = await db.create_invoice(
                invoice_number="DL-OVERDUE",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            upcoming_id = await db.create_invoice(
                invoice_number="DL-UPCOMING",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            far_id = await db.create_invoice(
                invoice_number="DL-FAR",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            ended_id = await db.create_invoice(
                invoice_number="DL-ENDED",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            credit_id = await db.create_invoice(
                invoice_number="DL-CREDIT",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            parent_id = await db.create_invoice(
                invoice_number="DL-PARENT",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )
            child_id = await db.create_invoice(
                invoice_number="DL-CHILD",
                project_id=None,
                created_by=1,
                creator_role="manager_kv",
            )

            await db.update_invoice(
                overdue_id,
                status="in_progress",
                deadline_end_date=(today - timedelta(days=1)).isoformat(),
            )
            await db.update_invoice(
                upcoming_id,
                status="paid",
                deadline_end_date=(today + timedelta(days=3)).isoformat(),
            )
            await db.update_invoice(
                far_id,
                status="closing",
                deadline_end_date=(today + timedelta(days=4)).isoformat(),
            )
            await db.update_invoice(
                ended_id,
                status="ended",
                deadline_end_date=today.isoformat(),
            )
            await db.update_invoice(
                credit_id,
                status="in_progress",
                is_credit=1,
                deadline_end_date=today.isoformat(),
            )
            await db.update_invoice(
                parent_id,
                status="in_progress",
                deadline_end_date=today.isoformat(),
            )
            await db.update_invoice(
                child_id,
                status="in_progress",
                parent_invoice_id=parent_id,
                deadline_end_date=today.isoformat(),
            )

            invoices = await db.list_invoices_approaching_deadline(today=today)
        finally:
            await db.close()

        assert [invoice["invoice_number"] for invoice in invoices] == [
            "DL-OVERDUE",
            "DL-PARENT",
            "DL-UPCOMING",
        ]

    asyncio.run(scenario())


def test_create_task_rejects_assignment_to_existing_user_without_role(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(100, "norole", "No Role")

            try:
                await db.create_task(
                    project_id=None,
                    type_=TaskType.ISSUE,
                    status=TaskStatus.OPEN,
                    created_by=None,
                    assigned_to=100,
                    due_at_iso=None,
                    payload={"source": "test"},
                )
            except ValueError as exc:
                assert "has no role" in str(exc)
            else:
                raise AssertionError("create_task must reject assignment to a user without role")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_create_task_rejects_assignment_when_assigned_role_does_not_match_user(tmp_path) -> None:
    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(100, "rp_user", "RP User")
            await db.set_user_role(100, "rp")

            try:
                await db.create_task(
                    project_id=None,
                    type_=TaskType.ASSIGN_LEAD,
                    status=TaskStatus.OPEN,
                    created_by=None,
                    assigned_to=100,
                    due_at_iso=None,
                    payload={"assigned_role": "manager_kia"},
                )
            except ValueError as exc:
                assert "does not have role manager_kia" in str(exc)
            else:
                raise AssertionError("create_task must reject assignment to a user with the wrong role")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_apply_user_roles_falls_back_to_rp_when_same_role_missing(tmp_path) -> None:
    class _Config(SimpleNamespace):
        def get_role_id(self, role: str) -> int | None:
            return None

        def get_role_username(self, role: str) -> str | None:
            return None

    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(1, "mgr", "Manager")
            await db.upsert_user(2, "rp", "RP")
            await db.set_user_role(1, "manager_kia")
            await db.set_user_role(2, "rp")

            task = await db.create_task(
                project_id=None,
                type_=TaskType.DOCS_REQUEST,
                status=TaskStatus.OPEN,
                created_by=2,
                assigned_to=1,
                due_at_iso=None,
                payload={"source": "sheets_op", "invoice_id": 10},
            )

            reassigned = await apply_user_roles(db, _Config(), 1, [])
            updated_task = await db.get_task(int(task["id"]))
            payload = json.loads(updated_task["payload_json"])
            user = await db.get_user_optional(1)
        finally:
            await db.close()

        assert reassigned == [int(task["id"])]
        assert updated_task["assigned_to"] == 2
        assert payload["assigned_role"] == "rp"
        assert user is not None
        assert user.role is None

    asyncio.run(scenario())


def test_apply_user_roles_prefers_same_role_before_rp_fallback(tmp_path) -> None:
    class _Config(SimpleNamespace):
        def get_role_id(self, role: str) -> int | None:
            return None

        def get_role_username(self, role: str) -> str | None:
            return None

    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(1, "mgr_old", "Manager Old")
            await db.upsert_user(2, "rp", "RP")
            await db.upsert_user(3, "mgr_new", "Manager New")
            await db.set_user_role(1, "manager_kia")
            await db.set_user_role(2, "rp")
            await db.set_user_role(3, "manager_kia")

            task = await db.create_task(
                project_id=None,
                type_=TaskType.DOCS_REQUEST,
                status=TaskStatus.OPEN,
                created_by=2,
                assigned_to=1,
                due_at_iso=None,
                payload={"source": "sheets_op", "invoice_id": 11},
            )

            reassigned = await apply_user_roles(db, _Config(), 1, [])
            updated_task = await db.get_task(int(task["id"]))
            payload = json.loads(updated_task["payload_json"])
        finally:
            await db.close()

        assert reassigned == [int(task["id"])]
        assert updated_task["assigned_to"] == 3
        assert payload["assigned_role"] == "manager_kia"

    asyncio.run(scenario())


def test_resolve_default_assignee_requires_explicit_default_when_role_has_multiple_users(tmp_path) -> None:
    class _Config(SimpleNamespace):
        def get_role_id(self, role: str) -> int | None:
            return None

        def get_role_username(self, role: str) -> str | None:
            return None

    async def scenario() -> None:
        db = Database(str(tmp_path / "bot.sqlite3"))
        await db.connect()
        try:
            await db.init_schema()
            await db.upsert_user(1, "mgr_a", "Manager A")
            await db.upsert_user(2, "mgr_b", "Manager B")
            await db.set_user_role(1, "manager_kia")
            await db.set_user_role(2, "manager_kia")

            assignee = await resolve_default_assignee(db, _Config(), "manager_kia")
        finally:
            await db.close()

        assert assignee is None

    asyncio.run(scenario())
