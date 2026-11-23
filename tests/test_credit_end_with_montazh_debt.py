# Решение owner 20.11: кредитный счёт закрывается в «Счёт End» БЕЗ выплаты ЗП
# монтажу; невыплаченная часть ЗП (согласованная − выплаченная) входит в
# перерасчёт фактической прибыли — для кредитных и б/н счетов одинаково.
# Прежний гейт credit_zp_montazh_unpaid (owner 2026-07-03) снят.
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database

MGR_TG_ID = 700_333  # telegram_id тестового менеджера НПН


class _StubConfig:
    def get_role_id(self, role):
        return None

    def get_role_username(self, role):
        return None


class _StubNotifier:
    async def safe_send(self, *args, **kwargs):
        return True

    async def safe_send_media(self, *args, **kwargs):
        return True


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
    number: str,
    is_credit: bool,
    agreed: float,
    zp_status: str,
) -> int:
    inv_id = await db.create_invoice(
        invoice_number=number,
        project_id=None,
        created_by=MGR_TG_ID,
        creator_role="manager_npn",
        amount=200_000.0,
    )
    await db.update_invoice(
        inv_id,
        amount=200_000.0,
        materials_fact_op=50_000.0,
        montazh_fact_op=0.0,
        loaders_fact_op=0.0,
        logistics_fact_op=0.0,
        nds_amount=0.0,
        taxes_fact_op=0.0,
        agent_payout_op=0.0,
        is_credit=int(is_credit),
        receipt_date="2026-10-01",
        montazh_stage="invoice_ok",
        zp_installer_status=zp_status,
        montazh_agreed_amount=agreed,
    )
    return inv_id


def test_credit_pending_montazh_in_profit_fact(tmp_path) -> None:
    """Кредит, ЗП монтаж согласована (50 000) и НЕ выплачена → долг по монтажу
    входит в bg_cost и снижает факт-прибыль; без согласования — не входит."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27010-1КВ", is_credit=True,
                agreed=50_000.0, zp_status="approved",
            )
            inv = await db.get_invoice(inv_id)
            card = await db.get_full_invoice_cost_card(inv_id)
            assert card["pending_cost_total"] == 50_000.0, card["pending_cost_total"]
            # bg_cost = материалы 50 000 + долг монтажа 50 000
            assert abs(card["bg_cost"] - 100_000.0) < 0.01, card["bg_cost"]
            # контроль: та же карта без согласования — долга в прибыли нет
            await db.update_invoice(inv_id, zp_installer_status="not_requested",
                                    montazh_agreed_amount=0.0)
            inv2 = await db.get_invoice(inv_id)
            card2 = await db.get_full_invoice_cost_card(inv_id)
            assert card2["pending_cost_total"] == 0.0, card2["pending_cost_total"]
            assert abs(card2["bg_cost"] - 50_000.0) < 0.01, card2["bg_cost"]
            # прибыль ниже ровно на долг монтажа
            assert abs((card2["margin"] - card["margin"]) - 50_000.0) < 0.01, (
                card["margin"], card2["margin"])
        finally:
            await db.close()
    asyncio.run(scenario())


def test_credit_requested_not_counted(tmp_path) -> None:
    """Кредит, заявка подана, но НЕ согласована → сумма ещё не финальна,
    долг в перерасчёт не входит (симметрично б/н)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27011-1КВ", is_credit=True,
                agreed=0.0, zp_status="requested",
            )
            await db.update_invoice(inv_id, zp_installer_status="requested")
            inv = await db.get_invoice(inv_id)
            card = await db.get_full_invoice_cost_card(inv_id)
            assert card["pending_cost_total"] == 0.0, card["pending_cost_total"]
        finally:
            await db.close()
    asyncio.run(scenario())


def test_auto_close_credit_with_unpaid_zp(tmp_path) -> None:
    """Кредит с «Дата Факт», ЗП монтаж НЕ выплачена → авто-закрытие в
    «Счёт End» срабатывает (раньше гейт оставлял счёт на «Счёт ОК»)."""
    async def scenario() -> None:
        from app.services.sheet_commands import _auto_close_credit_invoice
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27012-1КВ", is_credit=True,
                agreed=50_000.0, zp_status="approved",
            )
            await db.update_invoice(inv_id, actual_completion_date="2026-11-20")
            inv = await db.get_invoice(inv_id)
            ok = await _auto_close_credit_invoice(
                invoice=inv, fact_date="2026-11-20",
                db=db, integrations=None, notifier=_StubNotifier(), config=_StubConfig(),
            )
            assert ok is True, "авто-закрытие кредитного счёта с невыплаченной ЗП"
            inv_after = await db.get_invoice(inv_id)
            assert inv_after["status"] == "ended", inv_after["status"]
            assert inv_after["montazh_stage"] == "invoice_end", inv_after["montazh_stage"]
        finally:
            await db.close()
    asyncio.run(scenario())


def test_fact_date_autofill_blocked_by_debt(tmp_path) -> None:
    """owner 24.11 (баги 24.08/16.11): «Дата Факт» не ставится счёту
    в работе — при непогашенном долге автозаполнение по цепочке дат
    (AZ «Счет End») пропускается; без долга — ставится как раньше."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27020-1КВ", is_credit=True,
                agreed=0.0, zp_status="not_requested",
            )
            await db.update_invoice(
                inv_id,
                outstanding_debt=95_100.0,
                final_surcharge_date="2026-11-16",
            )
            inv = await db.get_invoice(inv_id)
            res = await db.fill_fact_date_on_invoice_end(inv_id, inv)
            assert res is None, "при долге дата факта ставиться не должна"
            inv_after = await db.get_invoice(inv_id)
            assert not (inv_after.get("actual_completion_date") or ""), (
                inv_after.get("actual_completion_date"))

            # контроль: долг погашен — дата ставится по цепочке (AD)
            await db.update_invoice(inv_id, outstanding_debt=0.0)
            inv2 = await db.get_invoice(inv_id)
            res2 = await db.fill_fact_date_on_invoice_end(inv_id, inv2)
            assert res2 == "2026-11-16", res2
            inv_after2 = await db.get_invoice(inv_id)
            assert inv_after2["actual_completion_date"] == "2026-11-16"
        finally:
            await db.close()
    asyncio.run(scenario())


def test_bn_pending_montazh_unchanged(tmp_path) -> None:
    """б/н: долг по монтажу входил в перерасчёт и раньше — поведение
    не изменилось (регрессия)."""
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv_id = await _mk_invoice(
                db, number="27013-1НПН", is_credit=False,
                agreed=40_000.0, zp_status="approved",
            )
            inv = await db.get_invoice(inv_id)
            card = await db.get_full_invoice_cost_card(inv_id)
            assert card["pending_cost_total"] == 40_000.0, card["pending_cost_total"]
        finally:
            await db.close()
    asyncio.run(scenario())
