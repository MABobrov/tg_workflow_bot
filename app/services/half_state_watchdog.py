"""Сторож полу-состояний счёта (задача аудита №28, owner 14.11).

Полу-состояние — расхождение montazh_stage и status у одного счёта
(enums.py INVOICE_END_READY_STAGES, кейс 26623-1КВ от 17.07):

1. stage='invoice_end' при status НЕ ended/closing (кейс 261013-1САБ:
   in_progress + invoice_end) — счёт дошёл до End, но закрытие не завершено;
   выпадает из бейджей/напоминаний и висит молча. Сторож НЕ переводит статус
   сам: переход в ended запускает денежные пересчёты ЗП (recalc_*_on_end),
   поэтому только уведомляет ГД (не чаще раза в сутки по счёту).
   closing + invoice_end НЕ считается полу-состоянием — это ожидание
   подтверждения ГД по процессу (зона задачи №31).

2. status='ended' при стадии НИЖЕ invoice_end (кейс КВ 8: ended + invoice_ok)
   — безопасное сведение: стадия доводится до invoice_end + audit. Денежных
   хуков нет (update_invoice, не update_invoice_status),End уже проведён.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from ..db import Database
from ..enums import Role
from .notifier import Notifier

log = logging.getLogger(__name__)

_INTERVAL_SECONDS = 6 * 3600  # проход раз в 6 часов; уведомления — раз в сутки


def _half_state_alert_text(rows: list[dict[str, Any]]) -> str:
    lines = [
        "⚠️ <b>Сторож полу-состояний</b>",
        "Счёт на стадии «Счёт End», но статус не закрыт — выпадает из списков и напоминаний:",
    ]
    for r in rows:
        lines.append(
            "• {} (id {}) — статус «{}»".format(
                r.get("invoice_number") or "без номера", r.get("id"), r.get("status")
            )
        )
    lines.append("Проверьте и завершите закрытие (или верните стадию).")
    return "\n".join(lines)


async def _sync_ended_stage(db: Database) -> int:
    """status=ended при стадии ниже invoice_end → довести стадию + audit."""
    cur = await db.conn.execute(
        """
        SELECT id, invoice_number, montazh_stage FROM invoices
        WHERE status = 'ended'
          AND (montazh_stage IS NULL OR montazh_stage <> 'invoice_end')
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        old = r.get("montazh_stage")
        await db.update_invoice(r["id"], montazh_stage="invoice_end")
        try:
            await db.audit(
                actor_id=None,
                action="half_state_watchdog_stage_sync",
                entity="invoice",
                entity_id=str(r["id"]),
                payload={
                    "old_stage": old,
                    "new_stage": "invoice_end",
                    "invoice_number": r.get("invoice_number"),
                },
            )
        except Exception:
            log.exception(
                "half_state_watchdog: audit failed invoice_id=%s", r["id"]
            )
        log.info(
            "half_state_watchdog: stage synced to invoice_end for invoice_id=%s (%s, was %s)",
            r["id"], r.get("invoice_number"), old,
        )
    return len(rows)


async def _alert_open_end_stage(
    db: Database,
    notifier: Notifier,
    notified_today: dict[int, str],
) -> int:
    """stage=invoice_end при status не ended/closing → уведомить ГД (1/сутки)."""
    cur = await db.conn.execute(
        """
        SELECT id, invoice_number, status FROM invoices
        WHERE montazh_stage = 'invoice_end'
          AND status NOT IN ('ended', 'closing')
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    if not rows:
        return 0
    today = date.today().isoformat()
    fresh = [r for r in rows if notified_today.get(r["id"]) != today]
    if not fresh:
        return 0
    text = _half_state_alert_text(fresh)
    recipients: list[int] = []
    try:
        for u in await db.find_users_by_role(Role.GD):
            tid = getattr(u, "telegram_id", None)
            if tid:
                recipients.append(int(tid))
    except Exception:
        log.exception("half_state_watchdog: GD role resolve failed")
    if not recipients:
        log.warning("half_state_watchdog: no GD users found — alert only in log")
    sent_any = False
    for chat_id in recipients:
        try:
            if await notifier.safe_send(chat_id, text):
                sent_any = True
        except Exception:
            log.exception("half_state_watchdog: send to %s failed", chat_id)
    if not sent_any:
        # Даже без доставки — не молчим в логе (класс задачи №29: глотать нельзя).
        log.error("half_state_watchdog: alert NOT delivered, %d half-states: %s",
                  len(fresh), [(r.get("invoice_number"), r.get("status")) for r in fresh])
    for r in fresh:
        notified_today[r["id"]] = today
    return len(fresh)


async def half_state_watchdog_run_once(
    db: Database, notifier: Notifier, notified_today: dict[int, str] | None = None
) -> tuple[int, int]:
    """Один проход сторожа: (сведено стадий, отправлено уведомлений)."""
    notified: dict[int, str] = notified_today if notified_today is not None else {}
    synced = await _sync_ended_stage(db)
    alerted = await _alert_open_end_stage(db, notifier, notified)
    return synced, alerted


async def half_state_watchdog_loop(
    db: Database,
    notifier: Notifier,
    interval_seconds: int = _INTERVAL_SECONDS,
) -> None:
    """Фоновый цикл: проход раз в interval_seconds (уведомления — 1/сутки)."""
    notified: dict[int, str] = {}
    while True:
        try:
            synced, alerted = await half_state_watchdog_run_once(db, notifier, notified)
            if synced or alerted:
                log.info(
                    "half_state_watchdog: pass done (stage synced=%d, alerts=%d)",
                    synced, alerted,
                )
        except asyncio.CancelledError:
            log.info("half_state_watchdog: loop cancelled")
            raise
        except Exception:
            log.exception("half_state_watchdog: error in loop iteration")
        await asyncio.sleep(interval_seconds)
