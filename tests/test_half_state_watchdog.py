# Сторож полу-состояний счёта — задача аудита №28 (owner 14.11):
# 1) status='ended' при стадии ниже invoice_end → стадия доводится до
#    invoice_end + audit (безопасно, без денежных хуков);
# 2) stage='invoice_end' при status не ended/closing → уведомление ГД,
#    не чаще раза в сутки по счёту; статус НЕ переводится (деньги);
# 3) closing + invoice_end — НЕ полу-состояние (ожидание ГД, задача №31).
from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import Database
from app.services.half_state_watchdog import half_state_watchdog_run_once

GD_TG_ID = 700_999


class _FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str]] = []

    async def safe_send(self, chat_id: int, text: str, *args, **kwargs) -> bool:
        self.sent.append((chat_id, text))
        return True


async def _mk_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "bot.sqlite3"))
    await db.connect()
    await db.init_schema()
    await db.upsert_user(GD_TG_ID, "gd_test", "ГД тест")
    await db.set_user_role(GD_TG_ID, "gd")
    return db


async def _mk_invoice(db: Database, number: str, status: str, stage: str | None) -> int:
    inv_id = await db.create_invoice(
        invoice_number=number,
        project_id=None,
        created_by=GD_TG_ID,
        creator_role="gd",
        amount=100_000.0,
    )
    await db.update_invoice(inv_id, status=status, montazh_stage=stage)
    return inv_id


def test_ended_low_stage_synced_with_audit(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv = await _mk_invoice(db, "29001-1КВ", "ended", "invoice_ok")
            notifier = _FakeNotifier()
            synced, alerted = await half_state_watchdog_run_once(db, notifier)
            row = await db.get_invoice(inv)
            assert synced == 1, synced
            assert alerted == 0, alerted
            assert row["montazh_stage"] == "invoice_end", row["montazh_stage"]
            cur = await db.conn.execute(
                "SELECT action, entity_id FROM audit_log "
                "WHERE action='half_state_watchdog_stage_sync'"
            )
            audits = [dict(r) for r in await cur.fetchall()]
            assert len(audits) == 1 and audits[0]["entity_id"] == str(inv), audits
        finally:
            await db.close()

    asyncio.run(scenario())


def test_open_end_stage_alerts_gd_once_per_day(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            inv = await _mk_invoice(db, "29002-1НПН", "in_progress", "invoice_end")
            notifier = _FakeNotifier()
            notified: dict = {}
            synced, alerted = await half_state_watchdog_run_once(db, notifier, notified)
            assert synced == 0 and alerted == 1, (synced, alerted)
            assert len(notifier.sent) == 1, notifier.sent
            chat_id, text = notifier.sent[0]
            assert chat_id == GD_TG_ID, chat_id
            assert "29002-1НПН" in text and "in_progress" in text, text
            # тот же день — повторного уведомления нет
            _synced2, alerted2 = await half_state_watchdog_run_once(db, notifier, notified)
            assert alerted2 == 0 and len(notifier.sent) == 1, (alerted2, notifier.sent)
            # статус не тронут сторожем
            row = await db.get_invoice(inv)
            assert row["status"] == "in_progress", row["status"]
        finally:
            await db.close()

    asyncio.run(scenario())


def test_closing_end_stage_is_not_half_state(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            await _mk_invoice(db, "29003-1КВ", "closing", "invoice_end")
            notifier = _FakeNotifier()
            synced, alerted = await half_state_watchdog_run_once(db, notifier)
            assert synced == 0 and alerted == 0, (synced, alerted)
            assert not notifier.sent, notifier.sent
        finally:
            await db.close()

    asyncio.run(scenario())


def test_clean_states_untouched(tmp_path) -> None:
    async def scenario() -> None:
        db = await _mk_db(tmp_path)
        try:
            await _mk_invoice(db, "29004-1КВ", "ended", "invoice_end")
            await _mk_invoice(db, "29005-1КВ", "in_progress", "in_work")
            notifier = _FakeNotifier()
            synced, alerted = await half_state_watchdog_run_once(db, notifier)
            assert synced == 0 and alerted == 0, (synced, alerted)
            assert not notifier.sent, notifier.sent
        finally:
            await db.close()

    asyncio.run(scenario())


def main() -> None:
    for tfn in (
        test_ended_low_stage_synced_with_audit,
        test_open_end_stage_alerts_gd_once_per_day,
        test_closing_end_stage_is_not_half_state,
        test_clean_states_untouched,
    ):
        with tempfile.TemporaryDirectory() as td:
            tfn(Path(td))
        print(f"PASS {tfn.__name__}")
    print("ALL PASS")


if __name__ == "__main__":
    main()
