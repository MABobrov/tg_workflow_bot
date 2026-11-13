# Пересчёт ЗП МЕНЕДЖЕРОВ (КВ, КИА, НПН) по ФАКТУ при закрытии счёта —
# правило owner 14.11 (распространение правила РП 13.11 на всех менеджеров):
# ЗП менеджера факт = (факт-прибыль − 10% РП факт) × 25% (лид ГД) / 50% (свой);
# невыплата → авто-уменьшение min(net-план, факт) в manager_zp_net_payout;
# выплата (AN zp_manager_payout) > факт-ЗП → переплата фиксируется (mgr_overpay)
# и авто-списывается из авансового кошелька менеджера (первичный кошелёк).
# Кредитные счета — те же правила. Поля бот-локальные: реимпорт ОП не затирает.
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database, MGR_OVERPAY_OFFSET_TYPE
from app.utils import manager_zp_net_payout

MGR_TG_ID = 700_222  # telegram_id тестового менеджера НПН


async def _mk_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "bot.sqlite3"))
    await db.connect()
    await db.init_schema()
    await db.upsert_user(MGR_TG_ID, "mgr_npn", "Менеджер НПН")
    await db.set_user_role(MGR_TG_ID, "manager_npn")
    return db


async def _mk_invoice(
    db: Database,
    *,
    number: str = "27001-1НПН",
    amount: float = 100_000.0,
    materials_fact: float = 20_000.0,
    blank: float = 10_000.0,
    client_source: str | None = None,
    is_credit: bool = False,
) -> int:
    inv_id = await db.create_invoice(
        invoice_number=number,
        project_id=None,
        created_by=MGR_TG_ID,
        creator_role="manager_npn",
        amount=amount,
    )
    fields = dict(
        manager_zp_blank=blank,
        amount=amount,
        materials_fact_op=materials_fact,
        montazh_fact_op=0.0,
        loaders_fact_op=0.0,
        logistics_fact_op=0.0,
        nds_amount=0.0,
        taxes_fact_op=0.0,
        agent_payout_op=0.0,
        is_credit=int(is_credit),
        receipt_date="2026-10-01",
    )
    if client_source is not None:
        fields["client_source"] = client_source
    await db.update_invoice(inv_id, **fields)
    return inv_id


async def _wallet_topup(db: Database, total: float) -> int:
    """Платное пополнение авансового кошелька менеджера (первичный кошелёк)."""
    cur = await db.conn.execute(
        "INSERT INTO installer_advance_requests "
        "(installer_id, total_amount, status, comment, requested_at, paid_at, "
        " paid_by, initiator, request_type, wallet_role) "
        "VALUES (?, ?, 'paid', 'тест-пополнение', '2026-10-01T00:00:00', "
        "        '2026-10-01T00:00:00', ?, 'gd', 'request', 'manager_npn')",
        (MGR_TG_ID, total, MGR_TG_ID),
    )
    await db.conn.commit()
    return int(cur.lastrowid)


async def _overpay_offset_sum(db: Database) -> float:
    cur = await db.conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM installer_advance_requests "
        "WHERE request_type = ? AND wallet_role != 'rp' AND status = 'paid'",
        (MGR_OVERPAY_OFFSET_TYPE,),
    )
    return float((await cur.fetchone())[0] or 0)


def test_fact_below_plan_unpaid_autodecrease(tmp_path) -> None:
    """План (бланк) > факт, ЗП не выплачена → mgr_zp_fact записан, списаний
    нет, net-выплата уменьшена до факт-ЗП (кап min(net, факт))."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            # факт-прибыль = 100 000 − 92 000 = 8 000; РП факт = 800;
            # остаток 7 200 × 50% (свой клиент) → ЗП менеджера факт = 3 600.
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["mgr_zp_fact"]) - 3_600.0) < 0.01
            assert inv["mgr_fact_recalc_at"] is not None
            assert float(inv["mgr_overpay"] or 0) == 0.0
            assert await _overpay_offset_sum(db) == 0.0
            # авто-уменьшение: бланк 10 000 → к выплате min(10 000, 3 600).
            assert abs(manager_zp_net_payout(inv) - 3_600.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_gd_lead_share_25(tmp_path) -> None:
    """Лид ГД → доля 25% от остатка факт-прибыли после 10% РП."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, materials_fact=92_000.0, client_source="gd_lead",
            )
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            # остаток 7 200 × 25% = 1 800.
            assert abs(float(inv["mgr_zp_fact"]) - 1_800.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_paid_via_an_overpay_debits_wallet(tmp_path) -> None:
    """Выплачено через AN (zp_manager_payout) при факте ниже плана →
    переплата фиксируется и авто-списывается из кошелька (в пределах баланса)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт 3 600
            await db.update_invoice(inv_id, zp_manager_payout=10_000.0)
            await _wallet_topup(db, 5_000.0)  # баланс меньше переплаты
            assert await db.get_advance_balance(MGR_TG_ID, "manager_npn") == 5_000.0
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            # переплата = 10 000 − 3 600 = 6 400; списано 5 000 (весь баланс),
            # неот списанного остатка 1 400 остаётся в mgr_overpay.
            assert abs(float(inv["mgr_overpay"]) - 1_400.0) < 0.01
            assert abs(float(inv["mgr_overpay_offset"] or 0) - 5_000.0) < 0.01
            assert abs(await _overpay_offset_sum(db) - 5_000.0) < 0.01
            assert await db.get_advance_balance(MGR_TG_ID, "manager_npn") == 0.0
            cur = await db.conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE action = 'manager_zp_overpay' "
                "AND entity_id = ?",
                (str(inv_id),),
            )
            assert int((await cur.fetchone())[0]) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_fact_at_or_above_payout_no_overpay(tmp_path) -> None:
    """Факт-ЗП ≥ Σ выплат → без переплаты и списаний, mgr_overpay = 0."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            # факт-прибыль = 100 000 → РП 10 000, остаток 90 000 × 50% = 45 000.
            inv_id = await _mk_invoice(db, materials_fact=0.0)
            await db.update_invoice(inv_id, zp_manager_payout=10_000.0)
            await _wallet_topup(db, 20_000.0)
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["mgr_zp_fact"]) - 45_000.0) < 0.01
            assert float(inv["mgr_overpay"] or 0) == 0.0
            assert await _overpay_offset_sum(db) == 0.0
            assert abs(await db.get_advance_balance(MGR_TG_ID, "manager_npn") - 20_000.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_repeat_end_idempotent(tmp_path) -> None:
    """Повторный End не списывает переплату вторично (трекер mgr_overpay_offset)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт 3 600
            await db.update_invoice(inv_id, zp_manager_payout=10_000.0)
            await _wallet_topup(db, 20_000.0)  # хватает на всю переплату 6 400
            await db.update_invoice_status(inv_id, "ended")
            await db.update_invoice_status(inv_id, "ended")  # повтор — без эффекта
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["mgr_overpay_offset"] or 0) - 6_400.0) < 0.01
            assert abs(await _overpay_offset_sum(db) - 6_400.0) < 0.01
            assert float(inv["mgr_overpay"] or 0) == 0.0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_credit_invoice_close_recalc(tmp_path) -> None:
    """Кредитный счёт: закрытие (invoice_end без status='ended') тоже даёт
    пересчёт по факту — те же правила; хук статуса не списывает вторично."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27010-1НПН", materials_fact=92_000.0, is_credit=True,
            )
            await db.update_invoice(inv_id, montazh_stage="invoice_end")
            summary = await db.recalc_manager_zp_on_end(inv_id)
            assert summary is not None
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["mgr_zp_fact"]) - 3_600.0) < 0.01
            await db.update_invoice_status(inv_id, "ended")
            assert await _overpay_offset_sum(db) == 0.0  # выплат не было
        finally:
            await db.close()

    asyncio.run(scenario())
