"""Daily automatic sync at 09:00 Moscow time for all active users.

Sends updated reply keyboard with fresh badge counts to every
registered user, and runs Google Sheets import/export if enabled.
Also sends deadline notifications for approaching/overdue invoices.
"""
from __future__ import annotations

import asyncio
import html
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

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

    # --- 3. Deadline notifications ---
    try:
        sent = await _send_deadline_notifications(db, notifier, config)
        log.info("daily_sync: sent %d deadline notifications", sent)
    except Exception:
        log.exception("daily_sync: deadline notifications failed")


async def _send_deadline_notifications(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """Отправляет уведомления по срокам договора.

    - За 3 дня до срока → менеджеру + РП
    - В день срока (0 дней) → ГД + менеджер
    - Просрочен (< 0) → ежедневно ГД
    """
    today = datetime.now(_MSK).date()
    invoices = await db.list_invoices_approaching_deadline(today=today)
    if not invoices:
        return 0

    admin_ids: set[int] = getattr(config, "admin_ids", None) or set()
    sent = 0

    for inv in invoices:
        raw = inv.get("deadline_end_date")
        if not raw:
            continue
        try:
            end = datetime.fromisoformat(str(raw)).date()
        except (ValueError, TypeError):
            continue

        delta = (end - today).days
        inv_num = html.escape(str(inv.get("invoice_number", "?")))
        address = html.escape(str(inv.get("object_address") or "—"))
        manager_id: int | None = inv.get("created_by")
        project_id: int | None = inv.get("project_id")

        # Определяем РП через проект
        rp_id: int | None = None
        if project_id:
            try:
                rp_id = await db.get_project_rp_id(project_id)
            except Exception:
                pass

        recipients: set[int] = set()
        if delta < 0:
            # Просрочен → ежедневно ГД
            icon = "🔴"
            label = f"просрочен на {abs(delta)} дн."
            recipients.update(admin_ids)
        elif delta == 0:
            # Сегодня → ГД + менеджер
            icon = "🔴"
            label = "срок сегодня!"
            recipients.update(admin_ids)
            if manager_id:
                recipients.add(manager_id)
        elif delta <= 3:
            # За 3 дня → менеджер + РП
            icon = "⚠️"
            label = f"до срока {delta} дн."
            if manager_id:
                recipients.add(manager_id)
            if rp_id:
                recipients.add(rp_id)
        else:
            continue

        text = (
            f"{icon} <b>Срок по договору</b>\n"
            f"Счёт <b>№{inv_num}</b> — {label}\n"
            f"📍 {address}\n"
            f"📅 Срок: {end.strftime('%d.%m.%Y')}"
        )

        for uid in recipients:
            try:
                if await notifier.safe_send(uid, text):
                    sent += 1
            except Exception:
                log.debug("deadline notify failed for user %s", uid, exc_info=True)
            await asyncio.sleep(0.1)

    return sent
