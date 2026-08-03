"""Daily automatic sync at 09:00 Moscow time for all active users.

Sends updated reply keyboard with fresh badge counts to every
registered user, and runs Google Sheets import/export if enabled.
Also sends deadline notifications for approaching/overdue invoices.
"""
from __future__ import annotations

import asyncio
import html
import logging
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ..db import Database
from ..enums import MANAGER_ROLES, Role
from ..utils import (
    build_gd_sync_card_text,
    fmt_money,
    format_card_section,
    format_installer_sync_card,
    format_manager_sync_card,
    format_rp_sync_card,
    refresh_recipient_keyboard,
    with_manager_tasks_section,
)
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
            result = await import_from_source_sheet(
                db,
                integrations.sheets,
                log_prefix="daily_sync",
                integrations=integrations,
                notifier=notifier,
                config=config,
            )
            log.info(
                "daily_sync: imported %d, auto-closed %d credit, errors %d",
                result["imported"], result["closed_credits"], result["errors"],
            )
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

        # ТЗ 2026-05-20: ежедневный sync журнала «Авансы монтажников».
        try:
            await integrations.sync_advances_journal()
            log.info("daily_sync: advances journal synced")
        except Exception as e:
            log.error("daily_sync: advances journal sync failed: %s", e)

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

    # --- 4. Deadline notifications (только менеджеры + РП, ГД получает sync-карточку в шаге 5) ---
    try:
        sent = await _send_deadline_notifications(db, notifier, config)
        log.info("daily_sync: sent %d deadline notifications", sent)
    except Exception:
        log.exception("daily_sync: deadline notifications failed")

    # --- 4.4 Финальный платёж: догоняющий проход — создать задачу для счетов,
    # что подпадают под условия, но без задачи (этап до 15.06 / долг появился позже) ---
    try:
        cu = await catch_up_missing_final_payment_tasks(db, notifier, config)
        log.info("daily_sync: final-payment catch-up: %d new tasks", cu)
    except Exception:
        log.exception("daily_sync: final-payment catch-up failed")

    # --- 4.5 Финальный платёж: трекинг долга, переходы overdue/paid (ТЗ 14.06) ---
    try:
        fp = await _send_final_payment_followups(db, notifier, config)
        log.info("daily_sync: final-payment followups: %d events", fp)
    except Exception:
        log.exception("daily_sync: final-payment followups failed")

    # --- 4.6 Финальный платёж: пере-пинг по «застрявшим» задачам (дата не введена) ---
    try:
        rp = await _remind_stuck_final_payment_tasks(db, notifier, config)
        log.info("daily_sync: final-payment reminders: %d pinged", rp)
    except Exception:
        log.exception("daily_sync: final-payment reminders failed")

    # --- 4.7 «Счёт готов к закрытию»: монтаж «Счёт ОК» + долга нет → напомнить
    # менеджеру закрыть счёт (Счет End) + подчистить устаревшие (ТЗ 18.06) ---
    try:
        ier = await catch_up_invoice_end_ready(db, notifier, config)
        log.info("daily_sync: invoice-end-ready catch-up: %d new prompts", ier)
    except Exception:
        log.exception("daily_sync: invoice-end-ready catch-up failed")

    # --- 4.8 «Счёт без документов»: активный б/н без первички → задача менеджеру
    # + уведомление бухгалтерии; подчистка закрытых (ТЗ 18.06, B) ---
    try:
        dm = await catch_up_missing_doc_tasks(db, notifier, config)
        log.info("daily_sync: docs-missing catch-up: %d new tasks", dm)
    except Exception:
        log.exception("daily_sync: docs-missing catch-up failed")

    # --- 4.9 «Счёт без документов»: пере-пинг застрявших (ТЗ 18.06, B) ---
    try:
        dmr = await _remind_stuck_doc_tasks(db, notifier, config)
        log.info("daily_sync: docs-missing reminders: %d pinged", dmr)
    except Exception:
        log.exception("daily_sync: docs-missing reminders failed")

    # --- 5. Sync-карточка для ГД (как при ручном нажатии «Синхронизация данных») ---
    try:
        gd_sent = await _send_gd_sync_cards(db, notifier, config, all_users)
        log.info("daily_sync: sent gd sync cards to %d GD/TD users", gd_sent)
    except Exception:
        log.exception("daily_sync: gd sync cards failed")

    # --- 6. Sync-карточка для каждого менеджера (КВ/КИА/НПН) ---
    try:
        mgr_sent = await _send_manager_sync_cards(db, notifier, all_users)
        log.info("daily_sync: sent manager sync cards to %d managers", mgr_sent)
    except Exception:
        log.exception("daily_sync: manager sync cards failed")

    # --- 7. Sync-карточка для каждого РП ---
    try:
        rp_sent = await _send_rp_sync_cards(db, notifier, all_users)
        log.info("daily_sync: sent rp sync cards to %d RPs", rp_sent)
    except Exception:
        log.exception("daily_sync: rp sync cards failed")

    # --- 8. Sync-карточка для каждого монтажника ---
    try:
        inst_sent = await _send_installer_sync_cards(db, notifier, all_users)
        log.info("daily_sync: sent installer sync cards to %d installers", inst_sent)
    except Exception:
        log.exception("daily_sync: installer sync cards failed")

    # --- 9. Разовое напоминание ГД: оплатить сервер с ботом (20.09, owner 21.07) ---
    try:
        spr = await _maybe_send_server_payment_reminder(db, notifier, all_users)
        if spr:
            log.info("daily_sync: server-pay reminder sent to %d GD/TD users", spr)
    except Exception:
        log.exception("daily_sync: server-pay reminder failed")


# Разовое напоминание ГД об оплате сервера (owner 21.07). Срабатывает в первый
# daily_sync с датой (МСК) >= 20.09.2026 — устойчиво к простою: если бот лежал
# 20-го, напомнит при первом же запуске после. Дедуп через settings-флаг → 1 раз.
_SERVER_PAY_REMINDER_DATE = date(2026, 9, 20)
_SERVER_PAY_REMINDER_FLAG = "server_pay_reminder_20260920_sent"


async def _maybe_send_server_payment_reminder(
    db: Database,
    notifier: Notifier,
    all_users: list[Any],
) -> int:
    """Разовая инфо-карточка ГД: 20 сентября оплатить сервер с ботом."""
    now_msk = datetime.now(_MSK)
    if now_msk.date() < _SERVER_PAY_REMINDER_DATE:
        return 0
    if await db.get_setting(_SERVER_PAY_REMINDER_FLAG):
        return 0

    card = format_card_section(
        emoji="🖥",
        title="Оплата сервера с ботом",
        items=[
            ("Дата оплаты", "20 сентября"),
            ("Что оплатить", "VDS-сервер (хостинг бота)"),
            ("Если пропустить", "бот и хранилище остановятся"),
        ],
        width=40, compact=True,
    )

    sent = 0
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        if not any(_user_has_role(user, r) for r in _GD_CARD_ROLES):
            continue
        try:
            if await notifier.safe_send(user.telegram_id, card):
                sent += 1
        except Exception:
            log.exception(
                "daily_sync: server-pay reminder failed for user %s", user.telegram_id,
            )

    # Флаг ставим только при успешной доставке хотя бы одному ГД — иначе повтор завтра.
    if sent > 0:
        await db.set_setting(_SERVER_PAY_REMINDER_FLAG, "1")
    return sent


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
        # Эталон-v2: секция <pre>, счета построчно, в теле под ━ — «Счетов» и
        # «Итого» (owner 27.06: «изменить дизайн на эталонный»).
        text = format_card_section(
            emoji="📊",
            title="Сводка по долгам (утро)",
            items=[(num, fmt_money(d)) for num, d in debts[:20]],
            footer=("Счетов", str(len(debts))),
            total=fmt_money(total),
            width=29,
            sep_ratio=0.5,
        )
        if len(debts) > 20:
            text += f"\n\n… и ещё {len(debts) - 20} счетов"
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

    # Группируем по получателю: один пользователь — одна карточка с секцией
    # «⏰ Дедлайны» (эталонный дизайн, без push-серий).
    # ГД (admin_ids) намеренно НЕ включаем — он получает sync-карточку.
    per_recipient: dict[int, list[tuple[str, str]]] = {}

    for inv in invoices:
        raw = inv.get("deadline_end_date")
        if not raw:
            continue
        try:
            end = datetime.fromisoformat(str(raw)).date()
        except (ValueError, TypeError):
            continue

        delta = (end - today).days
        inv_num = str(inv.get("invoice_number", "?"))
        manager_id: int | None = inv.get("created_by")
        project_id: int | None = inv.get("project_id")

        rp_id: int | None = None
        if project_id:
            try:
                rp_id = await db.get_project_rp_id(project_id)
            except Exception:
                pass

        if delta < 0:
            label = f"🔴 №{inv_num}"
            value = f"−{abs(delta)} дн."
        elif delta == 0:
            label = f"🔴 №{inv_num}"
            value = "сегодня"
        elif delta <= 3:
            label = f"№{inv_num}"
            value = f"{delta} дн."
        else:
            continue

        for uid in (manager_id, rp_id):
            if not uid:
                continue
            per_recipient.setdefault(int(uid), []).append((label, value))

    if not per_recipient:
        return 0

    sent = 0
    for uid, items in per_recipient.items():
        text = format_card_section(
            emoji="⏰",
            title="Дедлайны",
            items=items,
            total=str(len(items)),
        )
        try:
            if await notifier.safe_send(uid, text):
                sent += 1
        except Exception:
            log.debug("deadline notify failed for user %s", uid, exc_info=True)
        await asyncio.sleep(0.1)

    return sent


async def _send_final_payment_followups(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """ТЗ 14.06: трекинг финального платежа по долгу — переходы 1× на событие.

    - planned + намеченная дата прошла + долг>0 → overdue: ГД уведомить 1×
      (просрочка) + пере-создать менеджеру задачу на новую дату (дедуп: 1 задача).
    - planned/overdue + долг≤0 → paid: ГД уведомить 1× (доплата получена).
    Состояние paid терминальное — видно в GD-карте «Долги» с ✅.
    """
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from .assignment import resolve_default_assignee
    from ..enums import TaskType, TaskStatus
    from ..utils import prompt_invoice_end_ready

    today = datetime.now(_MSK).date()
    try:
        tracked = await db.list_invoices_tracking_final_payment()
    except Exception:
        log.exception("final_payment: list tracking failed")
        return 0
    if not tracked:
        return 0

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    events = 0
    for inv in tracked:
        inv_id = int(inv["id"])
        st = inv.get("final_payment_track_state") or ""
        debt = float(inv.get("outstanding_debt") or 0)
        num = str(inv.get("invoice_number") or inv_id)
        addr = inv.get("object_address") or "—"

        # --- доплата получена: planned/overdue + долг закрыт → paid (1×) ---
        if st in ("planned", "overdue") and debt <= 0:
            await db.set_final_payment_track_state(inv_id, "paid")
            if gd_id:
                try:
                    await notifier.safe_send(
                        int(gd_id),
                        f"✅ <b>Доплата получена</b>\n\n"
                        f"Счёт №{num} — {addr}\n"
                        "Долг по счёту погашен. Отражено в карточке «Долги».",
                    )
                except Exception:
                    log.debug("final_payment: GD paid-notify failed inv=%s", inv_id, exc_info=True)
            # ТЗ 18.06: долг погашен — если монтаж завершён («Счёт ОК»), счёт
            # готов к закрытию → напомнить менеджеру (дедуп внутри хелпера).
            try:
                await prompt_invoice_end_ready(db, notifier, inv_id, gd_id, config)
            except Exception:
                log.debug("final_payment: invend-ready prompt failed inv=%s", inv_id, exc_info=True)
            events += 1
            await asyncio.sleep(0.1)
            continue

        # --- просрочка: planned + дата прошла + долг>0 → overdue (1×) ---
        if st == "planned" and debt > 0:
            pd = (inv.get("planned_final_payment_date") or "")[:10]
            try:
                planned = datetime.fromisoformat(pd).date() if pd else None
            except (ValueError, TypeError):
                planned = None
            if not (planned and planned <= today):
                continue
            await db.set_final_payment_track_state(inv_id, "overdue")
            debt_str = f"{int(round(debt)):,}".replace(",", " ")
            d_human = f"{pd[8:10]}.{pd[5:7]}.{pd[0:4]}" if len(pd) == 10 else pd
            # ГД уведомить 1× о просрочке
            if gd_id:
                try:
                    await notifier.safe_send(
                        int(gd_id),
                        f"🔴 <b>Просрочена доплата</b>\n\n"
                        f"Счёт №{num} — {addr}\n"
                        f"Долг: {debt_str} ₽\n"
                        f"Намеченная дата {d_human} прошла, платёж не поступил.\n"
                        "Помечено в карточке «Долги», менеджеру отправлен повторный запрос.",
                    )
                except Exception:
                    log.debug("final_payment: GD overdue-notify failed inv=%s", inv_id, exc_info=True)
            # пере-создать менеджеру задачу на новую дату (1 задача — дедуп).
            # менеджера резолвим по creator_role, а не created_by (у счетов из
            # «Импорт ОП»/синка ГД created_by = ГД → задача ушла бы ГД).
            from ..utils import resolve_invoice_manager_id
            manager_id = await resolve_invoice_manager_id(db, config, inv)
            if manager_id and not await db.has_open_final_payment_eta_task(inv_id):
                try:
                    task = await db.create_task(
                        project_id=None,
                        type_=TaskType.FINAL_PAYMENT_ETA,
                        status=TaskStatus.OPEN,
                        created_by=int(gd_id) if gd_id else int(manager_id),
                        assigned_to=int(manager_id),
                        due_at_iso=None,
                        payload={"invoice_id": inv_id, "invoice_number": num},
                    )
                    b = InlineKeyboardBuilder()
                    b.button(text="📅 Указать новую ориентировочную дату", callback_data=f"fpeta:{inv_id}")
                    b.adjust(1)
                    from ..utils import build_fpeta_debt_card
                    try:
                        body = build_fpeta_debt_card(
                            inv,
                            header_emoji="🔴",
                            header_title="Просрочен платёж — нужна новая ориентировочная дата",
                        )
                    except Exception:
                        log.debug("final_payment re-task: card render failed inv=%s", inv_id, exc_info=True)
                        body = (
                            f"🔴 <b>Просрочен финальный платёж — счёт №{num}</b>\n\n"
                            f"📍 {addr}\n"
                            f"Остаток долга: <b>{debt_str} ₽</b>\n\n"
                            "Намеченная дата прошла. Укажите <b>новую</b> ориентировочную "
                            "дату финального платежа."
                        )
                    await notifier.safe_send(
                        int(manager_id),
                        body,
                        reply_markup=b.as_markup(),
                    )
                except Exception:
                    log.exception("final_payment: re-task failed inv=%s", inv_id)
            events += 1
            await asyncio.sleep(0.1)
    return events


# Пере-пинг по «застрявшим» задачам фин. платежа: если менеджер не ввёл дату
# в течение стольких дней — повторное уведомление (троттлинг через payload).
_FPETA_REMINDER_DAYS = 2


async def catch_up_missing_final_payment_tasks(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """Догоняющий проход: создать задачу фин. платежа для счетов, что
    подпадают под условия (parent + долг>0 + этап «Счёт ОК»/«Счёт End»), но
    ещё не в трекинге и без открытой задачи.

    Ловит счета, у которых этап наступил ДО запуска фичи (15.06) или долг
    появился ПОЗЖЕ смены этапа — триггер request_final_payment_eta срабатывает
    только в момент смены этапа монтажником/менеджером. Идемпотентно: сам
    request_final_payment_eta дедуплицирует (открытая задача/трекинг → пропуск).
    Возвращает число созданных задач.
    """
    from .assignment import resolve_default_assignee
    from ..utils import request_final_payment_eta

    try:
        candidates = await db.list_invoices_missing_final_payment_task()
    except Exception:
        log.exception("fpeta catch-up: list candidates failed")
        return 0
    if not candidates:
        return 0

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    created = 0
    for inv in candidates:
        inv_id = int(inv["id"])
        # actor = ГД (создатель догоняющей задачи); fallback — менеджер счёта
        actor_id = int(gd_id) if gd_id else int(inv.get("created_by") or 0)
        if not actor_id:
            continue
        try:
            if await request_final_payment_eta(db, notifier, config, inv_id, actor_id):
                created += 1
        except Exception:
            log.exception("fpeta catch-up: request failed inv=%s", inv_id)
        await asyncio.sleep(0.1)
    return created


async def catch_up_invoice_end_ready(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """ТЗ 18.06: догоняющий проход «счёт готов к закрытию».

    (1) Для материнских счетов, готовых к закрытию (монтаж «Счёт ОК» + долга
    нет + статус активный) без открытого напоминания — создать задачу + push
    менеджеру (дедуп внутри prompt_invoice_end_ready). Ловит счета, где долг
    погасился ПОЗЖЕ «Счёт ОК», либо этап наступил до запуска фичи.
    (2) Подчистка: открытые INVOICE_END_READY по счетам, что больше НЕ готовы
    (закрыт / появился долг / сменился этап) — закрыть (DONE), чтобы бейдж и
    список задач не висели. Возвращает число созданных напоминаний.
    """
    import json as _json
    from ..utils import prompt_invoice_end_ready
    from ..enums import TaskType, TaskStatus

    created = 0
    # (1) создать для готовых без задачи
    try:
        candidates = await db.list_invoices_ready_for_end()
    except Exception:
        log.exception("invend-ready catch-up: list candidates failed")
        candidates = []
    for inv in candidates:
        inv_id = int(inv["id"])
        actor_id = int(inv.get("created_by") or 0) or None
        try:
            if await prompt_invoice_end_ready(db, notifier, inv_id, actor_id, config):
                created += 1
        except Exception:
            log.exception("invend-ready catch-up: prompt failed inv=%s", inv_id)
        await asyncio.sleep(0.1)

    # (2) подчистка устаревших открытых напоминаний
    try:
        open_ready = await db.list_tasks_open_by_types([TaskType.INVOICE_END_READY], limit=200)
    except Exception:
        log.exception("invend-ready catch-up: list open failed")
        open_ready = []
    for t in open_ready:
        try:
            pl = _json.loads(t.get("payload_json") or "{}") or {}
        except Exception:
            pl = {}
        inv_id = int(pl.get("invoice_id") or 0)
        if not inv_id:
            continue
        inv = await db.get_invoice(inv_id)
        ready = bool(
            inv
            and inv.get("parent_invoice_id") is None
            and (inv.get("status") or "") in ("in_progress", "paid", "credit")
            and (inv.get("montazh_stage") or "") == "invoice_ok"
            and float(inv.get("outstanding_debt") or 0) <= 0
        )
        if not ready:
            try:
                await db.update_task_status(int(t["id"]), TaskStatus.DONE)
            except Exception:
                log.debug("invend-ready catch-up: close stale failed task=%s", t.get("id"), exc_info=True)
    return created


async def _remind_stuck_final_payment_tasks(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """Пере-пинг по «застрявшим» задачам фин. платежа.

    Активная задача (open/in_progress) + менеджер так и не ввёл дату
    (final_payment_track_state '' или 'overdue') + задача старше
    _FPETA_REMINDER_DAYS дней (с момента создания / прошлого напоминания) →
    повторное уведомление менеджеру с кнопкой. Если долг уже погашен — задача
    закрывается (DONE) вместо напоминания. Троттлинг — поле
    payload.last_reminded_at (не чаще раза в N дней).
    """
    import json as _json
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from ..enums import TaskType, TaskStatus

    today = datetime.now(_MSK).date()
    try:
        tasks = await db.list_tasks_open_by_types([TaskType.FINAL_PAYMENT_ETA], limit=200)
    except Exception:
        log.exception("fpeta reminder: list tasks failed")
        return 0
    if not tasks:
        return 0

    pinged = 0
    for t in tasks:
        try:
            payload = _json.loads(t.get("payload_json") or "{}")
        except Exception:
            payload = {}
        inv_id = int(payload.get("invoice_id") or 0)
        if not inv_id:
            continue
        inv = await db.get_invoice(inv_id)
        if not inv:
            continue
        debt = float(inv.get("outstanding_debt") or 0)
        # долг погашен — закрыть «висящую» задачу, не напоминать
        if debt <= 0:
            try:
                await db.update_task_status(int(t["id"]), TaskStatus.DONE)
            except Exception:
                log.debug("fpeta reminder: close paid task failed id=%s", t.get("id"), exc_info=True)
            continue
        state = inv.get("final_payment_track_state") or ""
        # 'planned' = дата уже введена (открытой задачи быть не должно) — пропуск
        if state not in ("", "overdue"):
            continue
        manager_id = t.get("assigned_to")
        if not manager_id:
            continue
        # троттлинг: не чаще раза в N дней (от создания или прошлого напоминания)
        ref = payload.get("last_reminded_at") or t.get("created_at") or ""
        try:
            ref_date = datetime.fromisoformat(str(ref)[:10]).date()
        except (ValueError, TypeError):
            ref_date = None
        if ref_date and (today - ref_date).days < _FPETA_REMINDER_DAYS:
            continue

        num = str(inv.get("invoice_number") or inv_id)
        addr = inv.get("object_address") or "—"
        debt_str = f"{int(round(debt)):,}".replace(",", " ")
        b = InlineKeyboardBuilder()
        b.button(text="📅 Указать ориентировочную дату", callback_data=f"fpeta:{inv_id}")
        b.adjust(1)
        try:
            await notifier.safe_send(
                int(manager_id),
                f"⏰ <b>Напоминание: дата фин. платежа — счёт №{num}</b>\n\n"
                f"📍 {addr}\n"
                f"Остаток долга: <b>{debt_str} ₽</b>\n\n"
                "Вы ещё не указали ориентировочную дату финального платежа. "
                "Пожалуйста, укажите — ГД ожидает.",
                reply_markup=b.as_markup(),
            )
            await db.update_task_payload(int(t["id"]), {"last_reminded_at": today.isoformat()})
            pinged += 1
        except Exception:
            log.exception("fpeta reminder: ping failed inv=%s", inv_id)
        await asyncio.sleep(0.1)
    return pinged


_DOCS_MISSING_REMINDER_DAYS = 2


async def catch_up_missing_doc_tasks(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """ТЗ 18.06 (B): догоняющий проход «счёт без документов».

    (1) Для активных материнских б/н счетов без первичных документов (нет ЭДО
    первички И нет оригиналов) без открытой задачи — создать задачу менеджеру
    (created_by) + push карточкой + уведомить бухгалтерию (дедуп внутри
    prompt_invoice_docs_missing).
    (2) Подчистка: открытые INVOICE_DOCS_MISSING, у которых первичка уже
    появилась (или счёт закрыт/стал кредитным/не активен) — закрыть (DONE).
    Возвращает число созданных задач.
    """
    import json as _json
    from ..utils import prompt_invoice_docs_missing
    from ..enums import TaskType, TaskStatus

    created = 0
    try:
        candidates = await db.list_invoices_missing_primary_docs()
    except Exception:
        log.exception("docs-missing catch-up: list candidates failed")
        candidates = []
    for inv in candidates:
        inv_id = int(inv["id"])
        actor_id = int(inv.get("created_by") or 0) or None
        try:
            if await prompt_invoice_docs_missing(db, notifier, inv_id, actor_id, config):
                created += 1
        except Exception:
            log.exception("docs-missing catch-up: prompt failed inv=%s", inv_id)
        await asyncio.sleep(0.1)

    # (2) подчистка устаревших открытых задач
    try:
        open_dm = await db.list_tasks_open_by_types([TaskType.INVOICE_DOCS_MISSING], limit=200)
    except Exception:
        log.exception("docs-missing catch-up: list open failed")
        open_dm = []
    for t in open_dm:
        try:
            pl = _json.loads(t.get("payload_json") or "{}") or {}
        except Exception:
            pl = {}
        inv_id = int(pl.get("invoice_id") or 0)
        if not inv_id:
            continue
        inv = await db.get_invoice(inv_id)
        still_missing = bool(
            inv
            and inv.get("parent_invoice_id") is None
            and int(inv.get("is_credit") or 0) == 0
            and (inv.get("status") or "") == "in_progress"
            and int(inv.get("docs_edo_signed") or 0) == 0
            and not (inv.get("docs_originals_holder") or "")
        )
        if not still_missing:
            try:
                await db.update_task_status(int(t["id"]), TaskStatus.DONE)
            except Exception:
                log.debug("docs-missing catch-up: close stale failed task=%s", t.get("id"), exc_info=True)
    return created


async def _remind_stuck_doc_tasks(
    db: Database,
    notifier: Notifier,
    config: object,
) -> int:
    """ТЗ 18.06 (B): пере-пинг по «застрявшим» задачам «нет документов».

    Активная задача (open/in_progress) + первичка так и не оформлена + задача
    старше _DOCS_MISSING_REMINDER_DAYS дней (с создания / прошлого напоминания) →
    повторное уведомление менеджеру. Если первичка появилась (или счёт уже не
    активен) — задача закрывается (DONE) вместо напоминания. Троттлинг — поле
    payload.last_reminded_at.
    """
    import json as _json
    from ..enums import TaskType, TaskStatus

    today = datetime.now(_MSK).date()
    try:
        tasks = await db.list_tasks_open_by_types([TaskType.INVOICE_DOCS_MISSING], limit=200)
    except Exception:
        log.exception("docs-missing reminder: list tasks failed")
        return 0
    if not tasks:
        return 0

    pinged = 0
    for t in tasks:
        try:
            payload = _json.loads(t.get("payload_json") or "{}")
        except Exception:
            payload = {}
        inv_id = int(payload.get("invoice_id") or 0)
        if not inv_id:
            continue
        inv = await db.get_invoice(inv_id)
        if not inv:
            continue
        # первичка появилась или счёт не активен — закрыть, не напоминать
        primary_done = (
            int(inv.get("docs_edo_signed") or 0) != 0
            or bool(inv.get("docs_originals_holder") or "")
        )
        if primary_done or (inv.get("status") or "") != "in_progress":
            try:
                await db.update_task_status(int(t["id"]), TaskStatus.DONE)
            except Exception:
                log.debug("docs-missing reminder: close done task failed id=%s", t.get("id"), exc_info=True)
            continue
        manager_id = t.get("assigned_to")
        if not manager_id:
            continue
        # троттлинг: не чаще раза в N дней
        ref = payload.get("last_reminded_at") or t.get("created_at") or ""
        try:
            ref_date = datetime.fromisoformat(str(ref)[:10]).date()
        except (ValueError, TypeError):
            ref_date = None
        if ref_date and (today - ref_date).days < _DOCS_MISSING_REMINDER_DAYS:
            continue

        num = str(inv.get("invoice_number") or inv_id)
        addr = inv.get("object_address") or "—"
        try:
            # Эталон-v2: секция <pre> с мета (Счёт/Адрес, compact — адрес длинный),
            # призыв обычным текстом ниже (owner 27.06: «эталонный дизайн»).
            _docs_section = format_card_section(
                "📄",
                "Оформление документов",
                [
                    ("Счёт", f"№{html.escape(num)}"),
                    ("Адрес", html.escape(addr)),
                ],
                compact=True,
            )
            await notifier.safe_send(
                int(manager_id),
                _docs_section
                + "\n\n"
                + "Первичные документы по счёту всё ещё не оформлены "
                "(ЭДО или оригиналы). Пожалуйста, оформите.",
            )
            await db.update_task_payload(int(t["id"]), {"last_reminded_at": today.isoformat()})
            pinged += 1
        except Exception:
            log.exception("docs-missing reminder: ping failed inv=%s", inv_id)
        await asyncio.sleep(0.1)
    return pinged


def _user_has_role(user: Any, target_role: str) -> bool:
    """True если у пользователя есть target_role в comma-separated user.role."""
    raw = (user.role or "")
    for r in raw.split(","):
        if r.strip() == target_role:
            return True
    return False


# Роли, имеющие право получать ГД-карточку (финансы компании). Должно совпадать
# с handlers/common._GD_LIKE_ROLES. Изменять синхронно с тем модулем.
_GD_CARD_ROLES: tuple[str, ...] = (Role.GD, Role.TD)


async def _send_gd_sync_cards(
    db: Database,
    notifier: Notifier,
    config: object,
    all_users: list[Any],
) -> int:
    """Утренняя sync-карточка ГД — отправляется ТОЛЬКО ролям GD/TD.

    Идентичная карточка приходит ГД при ручном нажатии «Синхронизация данных»
    (handlers/gd.py::gd_sync_data). Заменяет раздельные deadline-уведомления
    для ГД — все цифры по незакрытым/просроченным агрегированы в строке
    «В работе» + «Сумма долга б.н./кредит».

    ВАЖНО: фильтр по users.role, не по config.admin_ids. Админ без роли GD/TD
    карточку НЕ получает — финансы компании предназначены только для роли ГД.
    См. правило [[feedback_gd_info_isolation]].
    """
    admin_ids: set[int] = getattr(config, "admin_ids", None) or set()

    sent = 0
    skipped_admins: list[int] = []
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        if not any(_user_has_role(user, r) for r in _GD_CARD_ROLES):
            # Если этот не-ГД оказался в admin_ids — лог-предупреждение для аудита.
            if user.telegram_id in admin_ids:
                skipped_admins.append(int(user.telegram_id))
            continue
        try:
            text = await build_gd_sync_card_text(db, config, user.telegram_id)
            if await notifier.safe_send(user.telegram_id, text):
                sent += 1
        except Exception:
            log.exception(
                "daily_sync: gd sync card failed for user %s", user.telegram_id,
            )
        # Статистика по менеджерам — отдельным сообщением (owner 25.06, агрегат).
        try:
            from ..zamery_start_card import build_zamery_manager_stats_card
            _zstats = await build_zamery_manager_stats_card(db)
            if _zstats:
                await notifier.safe_send(user.telegram_id, _zstats)
        except Exception:
            log.exception("daily_sync: zamery stats followup failed for %s", user.telegram_id)
        await asyncio.sleep(0.3)

    if skipped_admins:
        log.warning(
            "daily_sync: admin_ids %s НЕ получили ГД-карточку — нет роли GD/TD "
            "(правило [[feedback_gd_info_isolation]])",
            skipped_admins,
        )
    return sent


async def _send_manager_sync_cards(
    db: Database,
    notifier: Notifier,
    all_users: list[Any],
) -> int:
    """Утренняя sync-карточка для каждого менеджера (КВ/КИА/НПН).

    Идентичная карточка приходит менеджеру при ручном нажатии
    «Синхронизация данных» (handlers/common.py::sync_data_non_gd).
    """
    sent = 0
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        if not any(_user_has_role(user, r) for r in MANAGER_ROLES):
            continue
        try:
            metrics = await db.get_manager_dashboard_metrics(user.telegram_id)
            text = await with_manager_tasks_section(db, user.telegram_id, format_manager_sync_card(metrics))
            if await notifier.safe_send(user.telegram_id, text):
                sent += 1
        except Exception:
            log.exception("daily_sync: manager sync card failed for user %s", user.telegram_id)
        await asyncio.sleep(0.3)
    return sent


async def _send_rp_sync_cards(
    db: Database,
    notifier: Notifier,
    all_users: list[Any],
) -> int:
    """Утренняя sync-карточка для каждого РП.

    Идентичная карточка приходит РП при ручном нажатии «Синхронизация данных».
    Структура карточки = менеджерская, но со всеми счетами компании и блоком
    «10% прибыли РП» вместо «ЗП-менеджер».
    """
    sent = 0
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        if not _user_has_role(user, Role.RP):
            continue
        try:
            metrics = await db.get_rp_dashboard_metrics(user.telegram_id)
            text = format_rp_sync_card(metrics)
            if await notifier.safe_send(user.telegram_id, text):
                sent += 1
        except Exception:
            log.exception("daily_sync: rp sync card failed for user %s", user.telegram_id)
        await asyncio.sleep(0.3)
    return sent


async def _send_installer_sync_cards(
    db: Database,
    notifier: Notifier,
    all_users: list[Any],
) -> int:
    """Утренняя sync-карточка для каждого монтажника.

    Идентичная карточка приходит монтажнику при ручном нажатии «Синхронизация
    данных» (handlers/common.py::sync_data_non_gd). На проде сейчас только
    Игорь Быканов (tg=1072734744). Звук по умолчанию (НЕ disable_notification) —
    карточка должна разбудить утром.

    Источники СТРОГО по спеке [[project_installer_sync_card_deploy_20260522]] —
    см. db.get_installer_dashboard_metrics. Правило [[feedback_use_only_specified_sources]].
    """
    sent = 0
    for user in all_users:
        if not user.is_active or not user.role:
            continue
        if not _user_has_role(user, Role.INSTALLER):
            continue
        try:
            metrics = await db.get_installer_dashboard_metrics(user.telegram_id)
            text = format_installer_sync_card(metrics)
            if await notifier.safe_send(user.telegram_id, text):
                sent += 1
        except Exception:
            log.exception(
                "daily_sync: installer sync card failed for user %s", user.telegram_id,
            )
        await asyncio.sleep(0.3)
    return sent
