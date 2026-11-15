# Канал ЗП через кредитные кошельки в Σ факт-пересчёта (решение owner 14.11
# №122/№124) + анти-дубль №125 (журнал-приёмник первичен, окно ±2 дня):
#   1. кредитная ЗП-трата, привязанная к счёту, входит в Σ выплат РП;
#   2. та же сумма уже в AR с датой в окне ±2 дня → дубль, НЕ входит;
#   3. групповой дубль: одна трата = Σ AR по нескольким счетам в окне;
#   4. неразнесённая трата (без номера счёта/привязки) НЕ приписывается счёту;
#   5. для менеджера: ЗП-трата своего кошелька входит в Σ; трата с маркером
#      РП («Отправь ЗП РП») в менеджерский канал НЕ попадает;
#   6. менеджер: та же сумма в AN с датой в окне ±2 дня → дубль, НЕ входит.
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database

MGR_TG_ID = 700_333
RP_TG_ID = 700_334


async def _mk_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "bot.sqlite3"))
    await db.connect()
    await db.init_schema()
    await db.upsert_user(RP_TG_ID, "rp_pavel", "Павел РП")
    await db.set_user_role(RP_TG_ID, "rp")
    await db.upsert_user(MGR_TG_ID, "mgr_kv", "Менеджер КВ")
    await db.set_user_role(MGR_TG_ID, "manager_kv")
    return db


async def _mk_invoice(db: Database, *, number: str, created_by: int, role: str) -> int:
    inv_id = await db.create_invoice(
        invoice_number=number,
        project_id=None,
        created_by=created_by,
        creator_role=role,
        amount=100_000.0,
    )
    await db.update_invoice(
        inv_id,
        manager_zp_blank=10_000.0,
        amount=100_000.0,
        materials_fact_op=20_000.0,
        montazh_fact_op=0.0,
        loaders_fact_op=0.0,
        logistics_fact_op=0.0,
        nds_amount=0.0,
        taxes_fact_op=0.0,
        agent_payout_op=0.0,
        receipt_date="2026-10-01",
    )
    return inv_id


async def _add_credit_spend(db: Database, *, wallet: str, amount: float,
                            desc: str, bound: int | None = None,
                            created_at: str = "2026-10-10T12:00:00") -> int:
    cur = await db.conn.execute(
        "INSERT INTO credit_spends "
        "(wallet_role, amount, description, bound_invoice_id, entered_by, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (wallet, amount, desc, bound, MGR_TG_ID, created_at),
    )
    await db.conn.commit()
    return int(cur.lastrowid)


def test_rp_credit_zp_channel_in_sum(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28001-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            # кредитная ЗП-трата КВ, привязанная к счёту → входит в Σ выплат РП
            await _add_credit_spend(db, wallet="manager_kv", amount=5_000.0,
                                    desc="ЗП РП по счёту 28001-1КВ", bound=inv_id)
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_rp_zp_on_end(inv_id)
            assert summary is not None
            assert summary["paid_channels"]["credit_zp_extra"] == 5_000.0
            assert summary["paid_sum_attributed"] == 5_000.0
            assert len(summary["credit_zp_items"]) == 1
            assert not summary["credit_zp_dupes"]
        finally:
            await db.close()
    asyncio.run(scenario())


def test_rp_credit_zp_dupe_excluded(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28002-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            # AR уже содержит ту же сумму, дата выплаты в окне ±2 дня → дубль
            await db.update_invoice(inv_id, rp_payout_op=5_000.0,
                                    rp_payout_date_op="10.10.2026")
            await _add_credit_spend(db, wallet="manager_kv", amount=5_000.0,
                                    desc="Снятие САБ. Отправь ЗП РП", bound=inv_id,
                                    created_at="2026-10-11T12:00:00")
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_rp_zp_on_end(inv_id)
            assert summary["paid_channels"]["credit_zp_extra"] == 0.0
            assert summary["paid_sum_attributed"] == 5_000.0  # только AR, без дубля
            assert len(summary["credit_zp_dupes"]) == 1
        finally:
            await db.close()
    asyncio.run(scenario())


def test_rp_group_dupe_excluded(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            # кейс 02.09: одна трата = Σ AR по нескольким счетам в окне ±2 дня
            ids = []
            for n in ("28003-1КВ", "28004-1КВ"):
                ids.append(await _mk_invoice(db, number=n,
                                             created_by=MGR_TG_ID, role="manager_kv"))
            for i in ids:
                await db.update_invoice(i, rp_payout_op=3_000.0,
                                        rp_payout_date_op="02.09.2026")
            # трата НЕ привязана к счёту, но номер счёта в описании
            await _add_credit_spend(db, wallet="manager_kv", amount=6_000.0,
                                    desc="Снятие САБ. Отправь ЗП РП 28003-1КВ",
                                    created_at="2026-09-02T10:00:00")
            await db.update_invoice(ids[0], status="ended")
            summary = await db.recalc_rp_zp_on_end(ids[0])
            assert summary["credit_zp_dupes"], \
                "групповой дубль (6 000 = 2×3 000) должен быть исключён"
            assert summary["paid_channels"]["credit_zp_extra"] == 0.0
        finally:
            await db.close()
    asyncio.run(scenario())


def test_rp_unattributed_spend_not_counted(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28005-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            # неразнесённая групповая трата без номера счёта → НЕ приписывается
            await _add_credit_spend(db, wallet="manager_kv", amount=25_000.0,
                                    desc="Снятие САБ. Отправь ЗП РП")
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_rp_zp_on_end(inv_id)
            assert summary["paid_channels"]["credit_zp_extra"] == 0.0
        finally:
            await db.close()
    asyncio.run(scenario())


def test_manager_credit_zp_channel_in_sum(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28006-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            # ЗП-трата своего кошелька КВ, привязанная к счёту → входит в Σ
            await _add_credit_spend(db, wallet="manager_kv", amount=8_000.0,
                                    desc="ЗП Кирилла по счёту 28006-1КВ", bound=inv_id)
            # трата с маркером РП — канал РП, НЕ менеджера
            await _add_credit_spend(db, wallet="manager_kv", amount=5_000.0,
                                    desc="ЗП РП по счёту 28006-1КВ", bound=inv_id)
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_manager_zp_on_end(inv_id)
            assert summary["paid_channels"]["credit_zp_extra"] == 8_000.0
            assert summary["paid_sum_attributed"] == 8_000.0
            assert len(summary["credit_zp_items"]) == 1
        finally:
            await db.close()
    asyncio.run(scenario())


def test_manager_dupe_excluded(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28007-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            # AN уже содержит сумму, дата в окне ±2 дня → трата = дубль
            await db.update_invoice(inv_id, zp_manager_payout=8_000.0,
                                    zp_manager_payout_date="10.10.2026")
            await _add_credit_spend(db, wallet="manager_kv", amount=8_000.0,
                                    desc="ЗП Кирилла по счёту 28007-1КВ", bound=inv_id,
                                    created_at="2026-10-12T12:00:00")
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_manager_zp_on_end(inv_id)
            assert summary["paid_channels"]["credit_zp_extra"] == 0.0
            assert summary["paid_sum_attributed"] == 8_000.0  # AN-ручная, без дубля
            assert len(summary["credit_zp_dupes"]) == 1
        finally:
            await db.close()
    asyncio.run(scenario())


def test_dupe_date_iso_format_detected(tmp_path) -> None:
    """Даты выплат в БД бывают ISO (реимпорт ОП) — анти-дубль обязан их читать."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(db, number="28008-1КВ",
                                       created_by=MGR_TG_ID, role="manager_kv")
            await db.update_invoice(inv_id, rp_payout_op=5_000.0,
                                    rp_payout_date_op="2026-09-02")
            await _add_credit_spend(db, wallet="manager_kv", amount=5_000.0,
                                    desc="Снятие САБ. Отправь ЗП РП 28008-1КВ",
                                    created_at="2026-09-02T08:27:19.707665+00:00")
            await db.update_invoice(inv_id, status="ended")
            summary = await db.recalc_rp_zp_on_end(inv_id)
            assert len(summary["credit_zp_dupes"]) == 1
            assert summary["paid_channels"]["credit_zp_extra"] == 0.0
        finally:
            await db.close()
    asyncio.run(scenario())


def test_number_in_desc_no_false_substring_match() -> None:
    """«КВ 1» НЕ должно находиться внутри «КВ 10» — только отдельным номером."""
    from app.db import Database as _Db
    assert _Db._number_in_desc("КВ 1", "ЗП РП по счетам КВ 9, КВ 10, КВ 13") is False
    assert _Db._number_in_desc("КВ 10", "ЗП РП по счетам КВ 9, КВ 10, КВ 13") is True
    assert _Db._number_in_desc("КВ 9", "ЗП РП по счетам КВ 9, КВ 10") is True
    assert _Db._number_in_desc("1КВ", "оплата 26112-1КВ") is False  # дефис перед
    assert _Db._number_in_desc("КВ 1", "КВ 1") is True
