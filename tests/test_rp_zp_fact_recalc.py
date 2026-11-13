# Пересчёт ЗП РП 10% по ФАКТУ при закрытии счёта — правило owner 13.11:
# до End ЗП РП = 10% от расчётной (план) прибыли; при End — от ФАКТИЧЕСКОЙ
# прибыли и фактических налогов. Факт < плана: ЗП не выплачена → авто-уменьшение
# (корзины/«К выплате» берут min(план, факт)); выплачена → переплата
# фиксируется (rp_overpay) и авто-списывается из авансового кошелька РП.
# Кредитные счета — те же правила («закрытие» = status='ended' +
# montazh_stage='invoice_end'). Все новые поля бот-локальные: реимпорт ОП
# (import_invoice_from_sheet) их не затирает.
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database, RP_OVERPAY_OFFSET_TYPE
from app.handlers.rp import _build_rp_zp_cart_from_source, _list_rp_zp_eligible_invoices

RP_TG_ID = 700_111  # telegram_id тестового РП (Павел)


async def _mk_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "bot.sqlite3"))
    await db.connect()
    await db.init_schema()
    await db.upsert_user(RP_TG_ID, "rp_pavel", "Павел РП")
    await db.set_user_role(RP_TG_ID, "rp")
    return db


async def _mk_invoice(
    db: Database,
    *,
    number: str = "27001-1НПН",
    amount: float = 100_000.0,
    npn: float = 10_000.0,
    materials_fact: float = 20_000.0,
    montazh_fact: float = 0.0,
    is_credit: bool = False,
) -> int:
    inv_id = await db.create_invoice(
        invoice_number=number,
        project_id=None,
        created_by=RP_TG_ID,
        creator_role="manager_npn",
        amount=amount,
    )
    await db.update_invoice(
        inv_id,
        npn_amount=npn,
        rp_10_pct_op=npn,
        amount=amount,
        materials_fact_op=materials_fact,
        montazh_fact_op=montazh_fact,
        loaders_fact_op=0.0,
        logistics_fact_op=0.0,
        nds_amount=0.0,
        taxes_fact_op=0.0,
        agent_payout_op=0.0,
        is_credit=int(is_credit),
        receipt_date="2026-10-01",
    )
    return inv_id


async def _wallet_topup(db: Database, total: float) -> int:
    """Платное пополнение авансового кошелька РП (request_type='request')."""
    cur = await db.conn.execute(
        "INSERT INTO installer_advance_requests "
        "(installer_id, total_amount, status, comment, requested_at, paid_at, "
        " paid_by, initiator, request_type, wallet_role) "
        "VALUES (?, ?, 'paid', 'тест-пополнение', '2026-10-01T00:00:00', "
        "        '2026-10-01T00:00:00', ?, 'gd', 'request', 'rp')",
        (RP_TG_ID, total, RP_TG_ID),
    )
    await db.conn.commit()
    return int(cur.lastrowid)


async def _overpay_offset_sum(db: Database) -> float:
    cur = await db.conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM installer_advance_requests "
        "WHERE request_type = ? AND wallet_role = 'rp' AND status = 'paid'",
        (RP_OVERPAY_OFFSET_TYPE,),
    )
    return float((await cur.fetchone())[0] or 0)


def test_fact_below_plan_unpaid_autodecrease(tmp_path) -> None:
    """План > факт, ЗП не выплачена → rp_zp_fact записан, списаний нет,
    корзина запроса берёт min(план, факт)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            # факт-прибыль = 100 000 − 92 000 = 8 000 → ЗП факт = 800 (план 10 000).
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["rp_zp_fact"]) - 800.0) < 0.01
            assert inv["rp_fact_recalc_at"] is not None
            assert float(inv["rp_overpay"] or 0) == 0.0
            assert await _overpay_offset_sum(db) == 0.0
            # авто-уменьшение в корзине запроса: eligible + кап min(план, факт).
            eligible = await _list_rp_zp_eligible_invoices(db)
            cart, skipped = _build_rp_zp_cart_from_source(eligible)
            assert cart == {inv_id: 800.0}
            assert skipped == 0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_paid_via_gd_overpay_debits_wallet(tmp_path) -> None:
    """Выплачено через ГД (rp_payout_op) при факте ниже плана → переплата
    фиксируется и авто-списывается из кошелька (в пределах баланса)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт-ЗП 800
            await db.update_invoice(inv_id, rp_payout_op=10_000.0)
            await _wallet_topup(db, 5_000.0)  # баланс меньше переплаты
            balance_before = await db.get_advance_balance(RP_TG_ID, "rp")
            assert balance_before == 5_000.0
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            # переплата = 10 000 − 800 = 9 200; списано 5 000 (весь баланс),
            # неот списанного остатка 4 200 остаётся в rp_overpay.
            assert abs(float(inv["rp_overpay"]) - 4_200.0) < 0.01
            assert abs(float(inv["rp_overpay_offset"] or 0) - 5_000.0) < 0.01
            assert abs(await _overpay_offset_sum(db) - 5_000.0) < 0.01
            assert await db.get_advance_balance(RP_TG_ID, "rp") == 0.0
            # аудит переплаты записан
            cur = await db.conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE action = 'rp_zp_overpay' "
                "AND entity_id = ?",
                (str(inv_id),),
            )
            assert int((await cur.fetchone())[0]) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_paid_via_advance_fill_overpay_debits_wallet(tmp_path) -> None:
    """Выплачено наливом своей ЗП в аванс (rp_payout_advance_at) → переплата
    + авто-списание из кошелька."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт-ЗП 800
            await db.update_invoice(inv_id, rp_payout_advance_at="2026-10-05T00:00:00")
            await _wallet_topup(db, 20_000.0)
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            # Σ выплат = npn 10 000 (налив) → переплата 9 200, списана целиком.
            assert abs(float(inv["rp_overpay"]) - 0.0) < 0.01
            assert abs(float(inv["rp_overpay_offset"] or 0) - 9_200.0) < 0.01
            assert abs(await db.get_advance_balance(RP_TG_ID, "rp") - 10_800.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_paid_via_rp_offset_overpay_debits_wallet(tmp_path) -> None:
    """Выплачено зачётом аванса (apply_rp_advance_to_invoice_now) → rp-offset
    входит в Σ выплат, переплата списывается."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт-ЗП 800
            await _wallet_topup(db, 20_000.0)
            await db.apply_rp_advance_to_invoice_now(
                RP_TG_ID, inv_id, 10_000.0, RP_TG_ID,
            )
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            # Σ выплат = зачёт 10 000 → переплата 9 200, кошелька хватило.
            assert abs(float(inv["rp_overpay_offset"] or 0) - 9_200.0) < 0.01
            assert abs(float(inv["rp_overpay"]) - 0.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_fact_at_or_above_plan_no_changes(tmp_path) -> None:
    """Факт ≥ Σ выплат → без переплаты и списаний, rp_overpay = 0."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            # факт-прибыль = 100 000 → ЗП факт 10 000 == выплачено 10 000.
            inv_id = await _mk_invoice(db, materials_fact=0.0)
            await db.update_invoice(inv_id, rp_payout_op=10_000.0)
            await _wallet_topup(db, 20_000.0)
            await db.update_invoice_status(inv_id, "ended")
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["rp_zp_fact"]) - 10_000.0) < 0.01
            assert float(inv["rp_overpay"] or 0) == 0.0
            assert await _overpay_offset_sum(db) == 0.0
            assert abs(await db.get_advance_balance(RP_TG_ID, "rp") - 20_000.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())


def test_credit_invoice_close_recalc(tmp_path) -> None:
    """Кредитный счёт: закрытие (invoice_end без status='ended') тоже даёт
    пересчёт по факту — те же правила."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27010-1НПН", materials_fact=92_000.0, is_credit=True,
            )
            await db.update_invoice(inv_id, montazh_stage="invoice_end")
            # прямой вызов (статус ещё не 'ended'): кредитное закрытие = enough.
            summary = await db.recalc_rp_zp_on_end(inv_id)
            assert summary is not None
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["rp_zp_fact"]) - 800.0) < 0.01
            # и через хук статуса — то же самое, без двойного эффекта.
            await db.update_invoice_status(inv_id, "ended")
            assert await _overpay_offset_sum(db) == 0.0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_sheet_reimport_preserves_new_fields(tmp_path) -> None:
    """Реимпорт ОП не затирает бот-локальные поля (их нет в sheet_fields)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)
            await db.update_invoice_status(inv_id, "ended")
            await db.import_invoice_from_sheet({
                "invoice_number": "27001-1НПН",
                "amount": 100_000.0,
                "npn_amount": 10_000.0,
                "status": "ended",
            })
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["rp_zp_fact"] or 0) - 800.0) < 0.01
            assert inv["rp_fact_recalc_at"] is not None
            assert float(inv["rp_overpay"] or 0) == 0.0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_repeat_end_idempotent(tmp_path) -> None:
    """Повторный End не списывает переплату второй раз (трекер-дельта)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, materials_fact=92_000.0)  # факт-ЗП 800
            await db.update_invoice(inv_id, rp_payout_op=10_000.0)
            await _wallet_topup(db, 20_000.0)
            await db.update_invoice_status(inv_id, "ended")
            first = await db.get_invoice(inv_id)
            assert first is not None
            assert abs(float(first["rp_overpay_offset"] or 0) - 9_200.0) < 0.01
            # повторный End (ре-эмиссия/форс) — списание не дублируется.
            await db.update_invoice_status(inv_id, "ended")
            await db.recalc_rp_zp_on_end(inv_id)
            assert abs(await _overpay_offset_sum(db) - 9_200.0) < 0.01
            inv = await db.get_invoice(inv_id)
            assert inv is not None
            assert abs(float(inv["rp_overpay_offset"] or 0) - 9_200.0) < 0.01
            assert abs(float(inv["rp_overpay"]) - 0.0) < 0.01
            assert abs(await db.get_advance_balance(RP_TG_ID, "rp") - 10_800.0) < 0.01
        finally:
            await db.close()

    asyncio.run(scenario())
