from __future__ import annotations

import asyncio
import html
import logging
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from ..db import Database
from ..enums import TaskStatus, TaskType
from ..keyboards import task_actions_kb
from ..utils import build_task_reminder_card, from_iso, parse_roles, try_json_loads, utcnow
from .notifier import Notifier

log = logging.getLogger(__name__)


def _in_quiet_hours(hour_msk: int, start_hour: int, end_hour: int) -> bool:
    """Whether the given MSK hour falls inside the configured quiet window.

    start==end disables quiet hours. start>end wraps midnight (e.g. 22..9 = night).
    """
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour_msk < end_hour
    return hour_msk >= start_hour or hour_msk < end_hour


async def reminders_loop(
    db: Database,
    notifier: Notifier,
    timezone_name: str,
    remind_soon_minutes: int = 60,
    remind_overdue_minutes: int = 10,
    interval_seconds: int = 60,
    self_reminder_giveup_minutes: int = 30,
) -> None:
    """Background reminders loop.

    - soon reminder: due within N minutes
    - overdue reminder: overdue by N minutes
    - self_reminder: fires exactly at due; closed only after successful delivery,
      with a give-up window (`self_reminder_giveup_minutes`) as a fail-safe.
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

                # Самонапоминалка (менеджер/РП сам себе): срабатывает ТОЧНО в срок
                # (один раз), затем задача закрывается. Не участвует в soon/overdue
                # и в acceptance-loops (исключена по типу в db-запросах).
                if t.get("type") == TaskType.SELF_REMINDER:
                    if now >= due:
                        delivered = await _send_self_reminder(db, notifier, t)
                        # Закрываем ТОЛЬКО при успешной доставке: транзиентный сбой
                        # (rate-limit/сеть) не должен «потерять» напоминание — повторим
                        # на следующем тике. Fail-safe: если доставка не удаётся дольше
                        # give-up-окна (перманентный сбой — forbidden / текст too-long),
                        # всё равно закрываем, чтобы loop не пинговал API бесконечно.
                        give_up = (now - due) >= timedelta(minutes=self_reminder_giveup_minutes)
                        if delivered or give_up:
                            if not delivered:
                                log.warning(
                                    "self_reminder #%s: giving up delivery after %s min",
                                    t.get("id"), self_reminder_giveup_minutes,
                                )
                            await db.update_task_status(
                                int(t["id"]), TaskStatus.DONE,
                                expected_statuses=("open", "in_progress"),
                            )
                    continue

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

    text = prefix + "\n\n" + await build_task_reminder_card(db, task, project, tz_name)
    await notifier.safe_send(int(assigned_to), text)
    # NOTE: workchat notification removed to avoid duplicate delivery
    # (assigned_to already receives the reminder in private chat)


async def _send_self_reminder(db: Database, notifier: Notifier, task: dict) -> bool:
    """Отправить самонапоминание пользователю (менеджер/РП сам себе).

    Только личка исполнителя (он же постановщик), без work-chat и без карточки
    задачи — это личная напоминалка, а не рабочая задача.

    Возвращает True, если сообщение доставлено (по результату safe_send). При
    False задачу закрывать НЕЛЬЗЯ — иначе транзиентный сбой (rate-limit/сеть)
    «потеряет» напоминание; loop повторит на следующем тике.
    """
    assigned_to = task.get("assigned_to")
    if not assigned_to:
        return True  # слать некому — повторять нечего, считаем обработанным
    payload = try_json_loads(task.get("payload_json")) or {}
    text = str(payload.get("comment") or "").strip()
    body = "🔔 <b>Напоминание</b>"
    if text:
        body += f"\n\n{html.escape(text)}"
    return bool(await notifier.safe_send(int(assigned_to), body))


async def _reminder_with_card(
    db: Database, task: dict, tz_name: str, header: str, hint: str = ""
) -> str:
    """Текст напоминания = заголовок + карточка задачи (эталон fmt_task_card) +
    опц. подсказка действия. Карточка строится в try/except — сбой НЕ должен
    сорвать отправку напоминания (fallback: короткая строка с #id)."""
    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None
    try:
        body = await build_task_reminder_card(db, task, project, tz_name)
    except Exception:
        log.exception("reminder: build_task_reminder_card failed for task %s", task.get("id"))
        body = f"Задача #{task.get('id')} ожидает обработки."
    text = header + "\n\n" + body
    if hint:
        text += "\n\n" + hint
    return text


# =====================================================================
# Acceptance reminders: 15-min repeat + 2h post-accept
# =====================================================================

async def acceptance_reminders_loop(
    db: Database,
    notifier: Notifier,
    timezone_name: str = "Europe/Moscow",
    interval_seconds: int = 60,
) -> None:
    """Background loop: remind every 15 min until accepted, then once after 2h.

    INSTALLER is handled by `installer_acceptance_reminders_loop` (separate cadence)
    and is skipped here to avoid duplicate pings.
    """
    while True:
        try:
            now_dt = datetime.now(timezone.utc)

            # Helper: resolve assigned user role for keyboard
            _role_cache: dict[int, str | None] = {}

            async def _assigned_role(uid: int) -> str | None:
                if uid not in _role_cache:
                    u = await db.get_user_optional(uid)
                    _role_cache[uid] = u.role if u else None
                return _role_cache[uid]

            # 1. Непринятые задачи — напоминание каждые 15 мин
            #    Для бухгалтерии: только однократно (первое напоминание),
            #    дальше только бейдж 🔴N на кнопке (без повторных push).
            cutoff_15m = (now_dt - timedelta(minutes=15)).isoformat()
            tasks_15m = await db.list_tasks_needing_15m_reminder(cutoff_15m)
            for task in tasks_15m:
                assigned = task.get("assigned_to")
                if not assigned:
                    continue
                tid = int(task["id"])
                role = await _assigned_role(int(assigned))

                # Installer: обрабатывается отдельным installer_acceptance_reminders_loop
                if role and "installer" in parse_roles(role):
                    continue

                # Бухгалтерия: пропускаем повторные напоминания
                if role == "accounting" and task.get("reminded_soon"):
                    await db.mark_task_reminded_15(tid)
                    continue

                # ГД: однократное напоминание (по аналогии с бухгалтерией).
                # DM с task-карточкой приходит при create_task; здесь шлём
                # ОДИН reminder, дальше визуальная индикация работает через
                # бейджи 🔴N в main_menu — повторов не шлём, чтобы не спамить.
                if role == "gd" and task.get("reminded_soon"):
                    await db.mark_task_reminded_15(tid)
                    continue

                await notifier.safe_send(
                    int(assigned),
                    await _reminder_with_card(
                        db, task, timezone_name,
                        "🔔 <b>Напоминание</b> — задача ожидает подтверждения.",
                        "Нажмите «✅ Принято» для подтверждения.",
                    ),
                    reply_markup=task_actions_kb(task, assigned_role=role),
                )
                await db.mark_task_reminded_15(tid)
                # Для ГД помечаем reminded_soon чтобы блокировать повторы
                # на следующих тиках (визуальная индикация остаётся в меню).
                if role == "gd":
                    await db.mark_task_reminded_soon(tid)

            # 2. Принятые задачи — одно напоминание через 2 часа
            cutoff_2h = (now_dt - timedelta(hours=2)).isoformat()
            tasks_2h = await db.list_tasks_needing_2h_reminder(cutoff_2h)
            for task in tasks_2h:
                assigned = task.get("assigned_to")
                if not assigned:
                    continue
                tid = int(task["id"])
                role = await _assigned_role(int(assigned))

                # Installer: повторные post-accept напоминания идут через installer-loop
                if role and "installer" in parse_roles(role):
                    continue

                # ГД: post-accept reminder пропускаем — индикация в меню
                # достаточна, дополнительный push не нужен.
                if role == "gd":
                    await db.mark_task_reminded_2h(tid)
                    continue

                await notifier.safe_send(
                    int(assigned),
                    await _reminder_with_card(
                        db, task, timezone_name,
                        "🔔 <b>Напоминание о задаче</b> — ожидает выполнения.",
                    ),
                    reply_markup=task_actions_kb(task, assigned_role=role),
                )
                await db.mark_task_reminded_2h(tid)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Acceptance reminders loop iteration failed")

        await asyncio.sleep(interval_seconds)


# =====================================================================
# Installer acceptance reminders: aggressive cadence + quiet hours
# =====================================================================

async def installer_acceptance_reminders_loop(
    db: Database,
    notifier: Notifier,
    timezone_name: str = "Europe/Moscow",
    not_accepted_interval_min: int = 10,
    post_accept_interval_min: int = 60,
    quiet_start_hour: int = 22,
    quiet_end_hour: int = 9,
    interval_seconds: int = 60,
) -> None:
    """Installer-only loop with shorter intervals and MSK quiet hours.

    - Not-accepted tasks: re-ping every `not_accepted_interval_min` minutes
      until the installer hits «✅ Принято».
    - Accepted-but-not-done tasks: re-ping every `post_accept_interval_min`
      minutes until status='done'.
    - Quiet window [`quiet_start_hour`, `quiet_end_hour`) in MSK skips all pings
      (start>end wraps midnight, e.g. 22..9 = night).
    """
    msk = ZoneInfo("Europe/Moscow") if ZoneInfo is not None else timezone(timedelta(hours=3))
    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            hour_msk = now_dt.astimezone(msk).hour
            quiet = _in_quiet_hours(hour_msk, quiet_start_hour, quiet_end_hour)

            if quiet:
                await asyncio.sleep(interval_seconds)
                continue

            # 1. Непринятые installer-задачи — повтор каждые N мин
            cutoff_not_accepted = (now_dt - timedelta(minutes=not_accepted_interval_min)).isoformat()
            tasks_not_accepted = await db.list_installer_tasks_needing_acceptance_reminder(
                cutoff_not_accepted
            )
            for task in tasks_not_accepted:
                assigned = task.get("assigned_to")
                if not assigned:
                    continue
                tid = int(task["id"])
                await notifier.safe_send(
                    int(assigned),
                    await _reminder_with_card(
                        db, task, timezone_name,
                        "🔔 <b>Срочное напоминание</b> — задача ожидает подтверждения.",
                        f"Нажмите «✅ Принято» — иначе будем повторять каждые {not_accepted_interval_min} мин.",
                    ),
                    reply_markup=task_actions_kb(task, assigned_role="installer"),
                )
                await db.mark_task_reminded_15(tid)

            # 2. Принятые installer-задачи — повторы до status='done'
            cutoff_post_accept = (now_dt - timedelta(minutes=post_accept_interval_min)).isoformat()
            tasks_post_accept = await db.list_installer_tasks_needing_post_accept_reminder(
                cutoff_post_accept
            )
            for task in tasks_post_accept:
                assigned = task.get("assigned_to")
                if not assigned:
                    continue
                tid = int(task["id"])
                await notifier.safe_send(
                    int(assigned),
                    await _reminder_with_card(
                        db, task, timezone_name,
                        "🔔 <b>Напоминание о задаче</b> — ожидает завершения работ.",
                        "Отметьте «✅ Готово» когда монтаж завершён.",
                    ),
                    reply_markup=task_actions_kb(task, assigned_role="installer"),
                )
                # Переиспользуем last_reminded_at как cursor для следующего тика
                await db.mark_task_reminded_15(tid)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Installer acceptance reminders loop iteration failed")

        await asyncio.sleep(interval_seconds)
