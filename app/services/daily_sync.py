"""Daily automatic sync at 09:00 Moscow time for all active users.

Sends updated reply keyboard with fresh badge counts to every
registered user, and runs Google Sheets import/export if enabled.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from ..db import Database
from ..utils import refresh_recipient_keyboard
from .integration_hub import IntegrationHub
from .notifier import Notifier
from .sheets_sync import export_to_sheets, import_from_source_sheet

log = logging.getLogger(__name__)

# Moscow timezone: UTC+3
_MSK = timezone(timedelta(hours=3))


async def daily_sync_loop(
    db: Database,
    notifier: Notifier,
    config: object,
    integrations: IntegrationHub,
    target_hour: int = 9,
    target_minute: int = 0,
) -> None:
    """Background loop: once per day at target_hour:target_minute MSK.

    1. Import from ОП sheet (Google Sheets) if enabled.
    2. Export projects/tasks/invoices to Sheets.
    3. Send refreshed keyboard to every active user.
    """
    while True:
        try:
            now_msk = datetime.now(_MSK)
            target = now_msk.replace(
                hour=target_hour, minute=target_minute, second=0, microsecond=0,
            )
            if now_msk >= target:
                target += timedelta(days=1)
            wait_seconds = (target - now_msk).total_seconds()
            log.info(
                "daily_sync: next run at %s MSK (in %.0f sec)",
                target.strftime("%Y-%m-%d %H:%M"),
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)

            log.info("daily_sync: starting scheduled sync…")
            await _run_sync(db, notifier, config, integrations)
            log.info("daily_sync: completed")

        except asyncio.CancelledError:
            log.info("daily_sync: loop cancelled")
            raise
        except Exception:
            log.exception("daily_sync: error in loop iteration")
            await asyncio.sleep(60)  # retry after 1 min on error


async def _run_sync(
    db: Database,
    notifier: Notifier,
    config: object,
    integrations: IntegrationHub,
) -> None:
    """Execute one full sync cycle."""

    # --- 1. Google Sheets import/export ---
    if integrations.sheets:
        try:
            ok = await import_from_source_sheet(
                db,
                integrations.sheets,
                log_prefix="daily_sync",
            )
            if ok:
                log.info("daily_sync: imported %d invoices from ОП", ok)
        except Exception as e:
            log.error("daily_sync: read_op_sheet failed: %s", e)

        try:
            stats = await export_to_sheets(
                db,
                integrations.sheets,
                include_invoice_cost=False,
                sync_invoices=True,
            )
            log.info(
                "daily_sync: exported %d projects, %d tasks, %d invoices",
                stats["projects"],
                stats["tasks"],
                stats["invoices"],
            )
        except Exception as e:
            log.error("daily_sync: sheets export failed: %s", e)

    # --- 2. Refresh keyboards for ALL active users ---
    all_users = await db.list_users(limit=10000)
    refreshed = 0
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        try:
            await refresh_recipient_keyboard(notifier, db, config, user.telegram_id)
            refreshed += 1
        except Exception:
            log.debug("daily_sync: refresh failed for user %s", user.telegram_id, exc_info=True)
        # Small delay to avoid Telegram rate limiting
        await asyncio.sleep(0.3)

    log.info("daily_sync: refreshed keyboards for %d users", refreshed)
