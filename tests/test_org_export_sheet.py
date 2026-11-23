# Лист «экспортОРГ объекты» (owner 20.11): бот выгружает из БД данные
# материнских счетов для Орг.Объектов — металл/стекло (суммы + даты оплат),
# заказы, монтаж, грузчики/логистика. Дочерние счета (РП/поставщики)
# не выгружаются.
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database
from app.services.sheets_sync import _export_org_objects

MGR_TG_ID = 700_333


class _StubSheets:
    def __init__(self) -> None:
        self.rows: list[list[Any]] = []

    async def upsert_org_export(self, rows: list[list[Any]]) -> int:
        self.rows = rows
        return len(rows)


async def _mk_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "bot.sqlite3"))
    await db.connect()
    await db.init_schema()
    await db.upsert_user(MGR_TG_ID, "mgr_npn", "Менеджер НПН")
    await db.set_user_role(MGR_TG_ID, "manager_npn")
    return db


def test_org_export_metal_glass_rows(tmp_path) -> None:
    """Материнский счёт с металл- и стеклоплатежами → одна строка с суммами
    и датами оплат; дочерние счета в выгрузку не попадают."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await db.create_invoice(
                invoice_number="27020-1НПН",
                project_id=None,
                created_by=MGR_TG_ID,
                creator_role="manager_npn",
                amount=300_000.0,
                object_address="Москва, Ленина 1",
            )
            await db.update_invoice(
                inv_id,
                client_name="ООО Ромашка",
                receipt_date="2026-11-10",
                estimated_loaders=5_000.0,
                estimated_logistics=7_000.0,
                metal_order_status="ordered",
                glass_order_status="blank_sent",
                montazh_assigned_at="2026-11-15T10:00:00",
            )
            child_id = await db.create_invoice(
                invoice_number="РП-1",
                project_id=None,
                created_by=MGR_TG_ID,
                creator_role="manager_npn",
                amount=50_000.0,
            )
            await db.update_invoice(child_id, parent_invoice_id=inv_id)

            await db.create_supplier_payment(
                parent_invoice_id=inv_id, amount=80_000.0,
                material_type="metal", supplier="Завод",
                created_by=MGR_TG_ID, payment_date="12.11.2026",
            )
            await db.create_supplier_payment(
                parent_invoice_id=inv_id, amount=40_000.0,
                material_type="profile",
                created_by=MGR_TG_ID, payment_date=None,  # fallback на created_at
            )
            await db.create_supplier_payment(
                parent_invoice_id=inv_id, amount=120_000.0,
                material_type="glass", supplier="Стекло",
                created_by=MGR_TG_ID, payment_date="14.11.2026",
            )

            parent = await db.get_invoice(inv_id)
            sheets = _StubSheets()
            count = await _export_org_objects(db, sheets, [(parent, "@mgr", None)])
            assert count == 1, count
            assert len(sheets.rows) == 1, sheets.rows
            row = sheets.rows[0]
            # A №счёта, B контрагент, C адрес, D дата, E сумма
            assert row[0] == "27020-1НПН", row
            assert row[1] == "ООО Ромашка", row
            assert row[2] == "Москва, Ленина 1", row
            assert row[3] == "2026-11-10", row
            assert abs(row[4] - 300_000.0) < 0.01, row
            # H металл = 80 000 + 40 000; I дата первой оплаты с payment_date
            assert abs(row[7] - 120_000.0) < 0.01, row
            assert row[8] == "2026-11-12", row
            # L стекло = 120 000; M дата
            assert abs(row[11] - 120_000.0) < 0.01, row
            assert row[12] == "2026-11-14", row
            # J/N статусы заказов, P монтаж назначен
            assert row[9] == "ordered", row
            assert row[13] == "blank_sent", row
            assert row[15] == "2026-11-15", row
            # K «Готовность металл» (owner 23.11): оплата 2026-11-12 (чт)
            # + 4 рабочих дня (пт 13, пн 16, вт 17, ср 18) = 2026-11-18;
            # O «Готовность стекло» — профиль знает только владелец, бот не пишет
            assert row[10] == "2026-11-18", row
            assert row[14] == "", row
        finally:
            await db.close()
    asyncio.run(scenario())


def test_org_export_skips_children_and_empty_dates(tmp_path) -> None:
    """Счёт без платежей → суммы/даты пустые (не 0-даты); дочерний счёт
    отфильтровывается ещё до выгрузки."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await db.create_invoice(
                invoice_number="27021-1НПН",
                project_id=None,
                created_by=MGR_TG_ID,
                creator_role="manager_npn",
                amount=100_000.0,
            )
            parent = await db.get_invoice(inv_id)
            child_id = await db.create_invoice(
                invoice_number="РП-2", project_id=None,
                created_by=MGR_TG_ID, creator_role="manager_npn", amount=1.0,
            )
            await db.update_invoice(child_id, parent_invoice_id=inv_id)
            child = await db.get_invoice(child_id)
            sheets = _StubSheets()
            count = await _export_org_objects(db, sheets, [(parent, "", None), (child, "", None)])
            assert count == 1, count
            row = sheets.rows[0]
            assert row[7] == "" and row[8] == "", row      # металл пусто
            assert row[11] == "" and row[12] == "", row    # стекло пусто
            assert row[15] == "", row                      # монтаж не назначен
        finally:
            await db.close()
    asyncio.run(scenario())
