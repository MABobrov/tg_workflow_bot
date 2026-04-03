"""Daily automatic sync at 09:00 Moscow time for all active users.

Sends updated reply keyboard with fresh badge counts to every
registered user, and runs Google Sheets import/export if enabled.
Also sends deadline notifications for approaching/overdue invoices.
"""
from __future__ import annotations

import asyncio
import html
import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ..db import Database
from ..utils import refresh_recipient_keyboard
from .integration_hub import IntegrationHub
from .notifier import Notifier
from .sheets_sync import export_to_sheets, import_from_source_sheet

log = logging.getLogger(__name__)

# Moscow timezone (DST-safe, consistent with config.timezone)
_MSK = ZoneInfo("Europe/Moscow")


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
            if now_msk > target:
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
    # NOTE: import runs first, then export. Manual edits in ОП are preserved
    # (import reads them into DB, export writes back). If this becomes an issue,
    # add sheet_modified_at tracking to avoid overwriting concurrent manual edits.
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
                amocrm_user_map=getattr(config, "amocrm_user_map", None),
                amocrm=integrations.amocrm,
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

    # --- 3. Debt summaries for managers ---
    try:
        debt_sent = await _send_debt_summaries(db, notifier)
        log.info("daily_sync: sent debt summaries to %d managers", debt_sent)
    except Exception:
        log.exception("daily_sync: debt summaries failed")

    # --- 4. Deadline notifications ---
    try:
        sent = await _send_deadline_notifications(db, notifier, config)
        log.info("daily_sync: sent %d deadline notifications", sent)
    except Exception:
        log.exception("daily_sync: deadline notifications failed")


async def _send_debt_summaries(
    db: Database,
    notifier: Notifier,
) -> int:
    """Send morning debt summary to each manager with outstanding debts."""
    invoices = await db.list_invoices(limit=10000)
    by_manager: dict[int, list[tuple[str, float]]] = {}
    for inv in invoices:
        debt = float(inv.get("outstanding_debt") or 0)
        status = str(inv.get("status") or "")
        if debt > 0 and inv.get("created_by") and status not in ("ended", "cancelled"):
            mid = int(inv["created_by"])
            by_manager.setdefault(mid, []).append(
                (str(inv.get("invoice_number", "?")), debt),
            )

    sent = 0
    for manager_id, debts in by_manager.items():
        total = sum(d for _, d in debts)
        lines = [f"  • {num}: <b>{d:,.0f}₽</b>" for num, d in debts[:20]]
        text = (
            f"📊 <b>Сводка по долгам (утро)</b>\n"
            f"Всего долг: <b>{total:,.0f}₽</b> ({len(debts)} сч.)\n\n"
            + "\n".join(lines)
        )
        if len(debts) > 20:
            text += f"\n  … и ещё {len(debts) - 20} счетов"
        try:
            if await notifier.safe_send(manager_id, text):
                sent += 1
        except Exception:
            log.debug("debt summary failed for user %s", manager_id, exc_info=True)
        await asyncio.sleep(0.1)
    return sent


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
