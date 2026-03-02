from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from ..db import Database
from ..utils import from_iso, utcnow, fmt_task_card
from .notifier import Notifier

log = logging.getLogger(__name__)


async def reminders_loop(
    db: Database,
    notifier: Notifier,
    timezone_name: str,
    remind_soon_minutes: int = 60,
    remind_overdue_minutes: int = 10,
    interval_seconds: int = 60,
) -> None:
    """Background reminders loop.

    - soon reminder: due within N minutes
    - overdue reminder: overdue by N minutes
    """
    while True:
        try:
            now = utcnow()
            tasks = await db.list_tasks_for_reminders(now.isoformat())
            for t in tasks:
                due_iso = t.get("due_at")
                if not due_iso:
                    continue
                due = from_iso(due_iso)
                delta = due - now

                # soon reminder
                if not t.get("reminded_soon") and timedelta(0) < delta <= timedelta(minutes=remind_soon_minutes):
                    await _send_task_reminder(db, notifier, t, timezone_name, kind="soon")
                    await db.mark_task_reminded_soon(int(t["id"]))

                # overdue reminder
                if not t.get("reminded_overdue") and -delta >= timedelta(minutes=remind_overdue_minutes):
                    await _send_task_reminder(db, notifier, t, timezone_name, kind="overdue")
                    await db.mark_task_reminded_overdue(int(t["id"]))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Reminders loop iteration failed")

        await asyncio.sleep(interval_seconds)


async def _send_task_reminder(db: Database, notifier: Notifier, task: dict, tz_name: str, kind: str) -> None:
    assigned_to = task.get("assigned_to")
    if not assigned_to:
        return
    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None

    if kind == "soon":
        prefix = "⏰ Скоро дедлайн по задаче"
    else:
        prefix = "🔥 Просрочена задача"

    text = prefix + "\n\n" + fmt_task_card(task, project, tz_name)
    await notifier.safe_send(int(assigned_to), text)
    await notifier.notify_workchat(text)
