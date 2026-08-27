from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..db import Database
from ..integrations.sheets import GoogleSheetsService
from .sheet_commands import _OP_SYNC_LOCK, import_op_row_with_autoclose

log = logging.getLogger(__name__)

# Счета, которых в «Импорт ОП» нет (заведены ботом), уходят в конец листа —
# сортировочный «бесконечный» индекс.
_OP_ROW_INDEX_LAST = 10**9


def invoice_export_sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    """Порядок строк листа Invoices = порядок строк листа «Импорт ОП».

    Решение owner 27.07: строка N Invoices = строка N ОП, независимо от дат.
    До этого ключом была receipt_date, и любая опечатка или пустая ячейка в
    «Дата пост.» давала ключ '9999-12-31' и уносила строку в конец листа —
    порядок разъезжался с ОП, и это читалось как «данные перемешались»
    (инцидент 27.07, см. ОТЧЁТ_перемешивание_Invoices_20260727.md).

    Счета без op_row_index (в ОП не попадали) идут после всех ОП-строк, между
    собой — в прежнем порядке: дата поступления, затем id.
    """
    op_idx = item.get("op_row_index")
    return (
        int(op_idx) if op_idx is not None else _OP_ROW_INDEX_LAST,
        str(item.get("receipt_date") or "9999-12-31"),
        int(item["id"]),
    )


async def _refresh_op_mirror(
    db: Database,
    sheets: GoogleSheetsService,
    notifier: Any | None,
    *,
    log_prefix: str,
) -> None:
    """Пересчитать детектор «зеркало ОП врёт» и толкнуть ГД НОВЫЕ находки.

    Зовётся ПОСЛЕ импорта, а не до: до записи значения в БД устаревшие, и поле,
    которое этот же импорт сейчас обновит, попало бы в находки зря.

    Лист повторно НЕ читается — берётся снимок пустых ячеек, собранный тем же
    `read_op_sheet_sync`, что уже отработал выше. Своего запроса к Google здесь
    нет вовсе, значит и под 503 детектор не попадает.

    Сбой детектора не должен ронять импорт — он вторичен по отношению к данным.
    """
    empties = getattr(sheets, "last_op_empty_fields", None) or {}
    if not empties:
        return
    try:
        result = await db.refresh_op_mirror_residue(
            empties, columns=sheets.op_column_letters(),
        )
    except Exception:
        log.exception("%s: op-mirror detector failed", log_prefix)
        return

    log.info(
        "%s: зеркало ОП — находок %d (новых %d), заглушено ручных правок %d, счетов %d",
        log_prefix, result["total"], len(result["new"]),
        result["suppressed"], result["checked"],
    )

    new_items = result.get("new") or []
    if not new_items or notifier is None:
        return

    # Пуш ТОЛЬКО на новые пары: находка, о которой уже сообщали, молчит — иначе
    # ГД получал бы один и тот же список каждую ночь и перестал бы его читать.
    from ..enums import Role
    from ..utils import format_op_mirror_new_push

    text = format_op_mirror_new_push(new_items)
    try:
        gd_users = await db.find_users_by_role(Role.GD, limit=10)
    except Exception:
        log.exception("%s: op-mirror push — не смог получить список ГД", log_prefix)
        return
    sent = False
    for u in gd_users:
        # 🔴 `safe_send` НЕ бросает исключений НИКОГДА — он ловит вплоть до
        # `except Exception` и возвращает False (`notifier.py:24-68`). Поэтому
        # судить об успехе по отсутствию исключения НЕЛЬЗЯ: `sent` был бы True
        # всегда, и `mark_op_mirror_notified` глушил бы находку навсегда, даже
        # когда пуш не ушёл никому. Читаем возвращаемое значение; форма разбора
        # — та же, что у `Notifier.notify_workchat` (`notifier.py:141-142`).
        try:
            res = await notifier.safe_send(u.telegram_id, text)
        except Exception:
            log.exception("%s: op-mirror push to %s raised", log_prefix, u.telegram_id)
            res = False
        ok = res[0] if isinstance(res, tuple) else bool(res)
        if ok:
            sent = True
        else:
            log.warning("%s: op-mirror push to %s NOT delivered", log_prefix, u.telegram_id)
        await asyncio.sleep(0.1)
    # Метку «сообщили» ставим, только если хоть кто-то ДЕЙСТВИТЕЛЬНО получил:
    # иначе находка молча замолчит навсегда из-за сбоя отправки.
    if sent:
        await db.mark_op_mirror_notified(new_items)
    else:
        log.error(
            "%s: зеркало ОП — %d новых находок НЕ доставлены ни одному ГД, "
            "метку «сообщили» не ставим (всплывут в следующий проход)",
            log_prefix, len(new_items),
        )


async def import_from_source_sheet(
    db: Database,
    sheets: GoogleSheetsService,
    *,
    log_prefix: str,
    integrations: Any | None = None,
    notifier: Any | None = None,
    config: Any | None = None,
) -> dict[str, int]:
    """Pull all rows from the «Импорт ОП» sheet into the bot DB.

    When integrations, notifier, and config are *all* provided, runs the
    full per-row pipeline (import + side-effects gate via
    sheet_commands.import_op_row_with_autoclose) under a module-level
    lock to serialize against concurrent cron + button + webhook callers.
    Without them, falls back to bare DB import (back-compat).

    Returns {"imported": N, "closed_credits": N, "errors": N}.
    """
    if not sheets.cfg.source_spreadsheet_id:
        return {"imported": 0, "closed_credits": 0, "errors": 0}

    op_rows = await sheets.read_op_sheet()

    full_pipeline = (
        integrations is not None and notifier is not None and config is not None
    )

    if not full_pipeline:
        imported = 0
        for row_data in op_rows:
            try:
                await db.import_invoice_from_sheet(row_data)
                imported += 1
            except Exception:
                log.warning(
                    "%s: failed to import invoice %s",
                    log_prefix,
                    row_data.get("invoice_number"),
                    exc_info=True,
                )
        await _refresh_op_mirror(db, sheets, None, log_prefix=log_prefix)
        return {"imported": imported, "closed_credits": 0, "errors": 0}

    imported = 0
    closed_credits = 0
    errors = 0

    async with _OP_SYNC_LOCK:
        for row_data in op_rows:
            result = await import_op_row_with_autoclose(
                row_data,
                db=db,
                integrations=integrations,
                notifier=notifier,
                config=config,
            )
            if result["imported"]:
                imported += 1
            if result["closed"]:
                closed_credits += 1
                # Spread Telegram notifies to avoid rate-limit on mass close.
                await asyncio.sleep(0.1)
            if result["error"]:
                errors += 1
                log.warning(
                    "%s: row %s error: %s",
                    log_prefix,
                    result["invoice_number"],
                    result["error"],
                )

    log.info(
        "%s: import done — imported=%d, closed_credits=%d, errors=%d",
        log_prefix, imported, closed_credits, errors,
    )

    # Детектор «зеркало ОП врёт» (18.08) — сразу после импорта, на свежей БД.
    await _refresh_op_mirror(db, sheets, notifier, log_prefix=log_prefix)

    # Помесячная аналитика компании (Доходы/Расходы из «Импорт ОП» BM-колонки).
    # Отдельный pass — читает те же rows, но интерпретирует их как монтлвью.
    try:
        monthly = await sheets.import_op_monthly_balance(db)
        log.info("%s: op_company_monthly upserted %d rows", log_prefix, monthly)
    except Exception:
        log.exception("%s: import_op_monthly_balance failed", log_prefix)

    return {"imported": imported, "closed_credits": closed_credits, "errors": errors}


async def export_to_sheets(
    db: Database,
    sheets: GoogleSheetsService,
    *,
    include_invoice_cost: bool,
    sync_invoices: bool = True,
    amocrm_user_map: dict[int, str] | None = None,
    amocrm=None,
) -> dict[str, int]:
    projects = sorted(await db.list_recent_projects(limit=10000), key=lambda item: int(item["id"]))
    tasks = sorted(await db.list_recent_tasks(limit=50000), key=lambda item: int(item["id"]))

    project_code_by_id: dict[int, str] = {}
    project_items: list[tuple[dict[str, Any], str]] = []
    for project in projects:
        manager_label = ""
        manager_id = project.get("manager_id")
        if manager_id:
            manager = await db.get_user_optional(int(manager_id))
            if manager:
                manager_label = f"@{manager.username}" if manager.username else str(manager.telegram_id)
        project_items.append((project, manager_label))
        project_code = str(project.get("code") or "")
        if project_code:
            project_code_by_id[int(project["id"])] = project_code

    task_items: list[tuple[dict[str, Any], str]] = []
    for task in tasks:
        project_code = ""
        project_id = task.get("project_id")
        if project_id:
            project_code = project_code_by_id.get(int(project_id), "")
            if not project_code:
                try:
                    project = await db.get_project(int(project_id))
                    project_code = str(project.get("code") or "")
                    if project_code:
                        project_code_by_id[int(project_id)] = project_code
                except Exception:
                    log.debug("Failed to get project %s for task", project_id, exc_info=True)
                    project_code = ""
        task_items.append((task, project_code))

    project_count = await sheets.upsert_projects_bulk(project_items)
    task_count = await sheets.upsert_tasks_bulk(task_items)

    invoice_count = 0
    if sync_invoices:
        all_invoices = await db.list_invoices(limit=10000)
        # LEAD-записи экспортируются — данные лида видны в колонках Лид КВ/КИА/НПН
        # Порядок строк — строго как в «Импорт ОП», см. invoice_export_sort_key.
        invoices = sorted(all_invoices, key=invoice_export_sort_key)
        invoice_items: list[tuple[dict[str, Any], str, dict[str, Any] | None]] = []
        for invoice in invoices:
            manager_label = ""
            if invoice.get("created_by"):
                user = await db.get_user_optional(int(invoice["created_by"]))
                if user:
                    manager_label = f"@{user.username}" if user.username else (user.full_name or str(user.telegram_id))
            # Fallback: менеджер из проекта
            if not manager_label and invoice.get("project_id"):
                try:
                    _proj = await db.get_project(int(invoice["project_id"]))
                    _mid = _proj.get("manager_id") if _proj else None
                    if _mid:
                        _mu = await db.get_user_optional(int(_mid))
                        if _mu:
                            manager_label = f"@{_mu.username}" if _mu.username else (_mu.full_name or str(_mu.telegram_id))
                except Exception:
                    pass

            # Fallback: определить creator_role из роли пользователя
            if not invoice.get("creator_role") and invoice.get("created_by"):
                try:
                    u = await db.get_user_optional(int(invoice["created_by"]))
                    if u and u.role and u.role.startswith("manager"):
                        invoice["creator_role"] = u.role
                except Exception:
                    log.debug("Failed to resolve creator_role for invoice %s", invoice.get("id"), exc_info=True)

            cost = None
            # Cost card нужен: 1) если явно запрошен, 2) для кредитных счетов (CF расход),
            # 3) для закрытых безналичных, 4) для АКТИВНЫХ безналичных (in_progress).
            # Пункт 4 добавлен 29.07: дневной синк зовёт экспорт с
            # include_invoice_cost=False (daily_sync.py), поэтому у активных счетов
            # BL/BM/BN/BO/EO/EP не обновлялись и пустели — 28.07 их пришлось дописывать
            # руками у четырёх счетов ([[project-20260728-fact-columns-stale-daily-sync]]).
            # Y «Рент-ть факт» от этого НЕ зависит: она зеркалит «Импорт ОП» W
            # и пишется в _invoice_cells безусловно (feedback_fact_columns_sources).
            _need_cost = (
                include_invoice_cost
                or bool(invoice.get("is_credit"))
                or invoice.get("status") in ("ended", "credit", "in_progress")
            ) and not invoice.get("parent_invoice_id")
            if _need_cost:
                try:
                    cost = await db.get_full_invoice_cost_card(int(invoice["id"]))
                except Exception:
                    log.debug("Failed to build invoice cost card for invoice %s", invoice.get("id"), exc_info=True)
                    cost = None

            # Обогатить lead_info и zamery_info для столбцов BJ-BP
            try:
                invoice["_lead_info"] = await db.get_lead_info_for_invoice(invoice)
            except Exception:
                log.debug("Failed to get lead_info for invoice %s", invoice.get("id"), exc_info=True)
                invoice["_lead_info"] = {}

            project_id = invoice.get("project_id")
            if project_id:
                try:
                    invoice["_zamery_info"] = await db.get_zamery_info_for_project(int(project_id))
                except Exception:
                    log.debug("Failed to get zamery_info for project %s", project_id, exc_info=True)
                    invoice["_zamery_info"] = ""

            # Расчет vs Факт ← связано с CN «Удержать из ЗП менеджера» (owner 2026-06-22).
            # Ручной CN (ОП CF → zp_manager_hold) управляет статусом BZ:
            #   есть удержание (≠0) → «Перерасчет прибыли», иначе → «Расчет ОК».
            # Заменяет прежнее авто-сравнение план/факт себестоимости.
            try:
                _hold = float(invoice.get("zp_manager_hold") or 0)
            except (TypeError, ValueError):
                _hold = 0.0
            invoice["_plan_fact_label"] = "Перерасчет прибыли" if _hold != 0 else "Расчет ОК"

            # Кредитные расходы (для столбцов CF-DJ)
            is_credit = bool(invoice.get("is_credit"))
            if is_credit:
                try:
                    invoice["_credit_expenses"] = await db.get_credit_expenses_summary(
                        int(invoice["id"])
                    )
                except Exception:
                    log.debug("Failed to get credit_expenses for invoice %s", invoice.get("id"), exc_info=True)
                    invoice["_credit_expenses"] = {}
                try:
                    invoice["_credit_carry_in"] = await db.get_credit_carry_in(int(invoice["id"]))
                except Exception:
                    log.debug("Failed to get credit_carry_in for invoice %s", invoice.get("id"), exc_info=True)
                    invoice["_credit_carry_in"] = 0.0
            else:
                invoice["_credit_expenses"] = {}
                invoice["_credit_carry_in"] = 0.0

            # EDO request stats for accounting columns 140-143
            try:
                invoice["_edo_stats"] = await db.get_edo_stats_for_invoice(int(invoice["id"]))
            except Exception:
                log.debug("Failed to get edo_stats for invoice %s", invoice.get("id"), exc_info=True)
                invoice["_edo_stats"] = {}

            # Installer telegram_id for Invoices.AW (fallback chain если assigned_to пуст)
            try:
                invoice["_installer_id"] = await db.get_installer_id_for_invoice(int(invoice["id"]))
            except Exception:
                log.debug("Failed to get installer_id for invoice %s", invoice.get("id"), exc_info=True)
                invoice["_installer_id"] = None

            invoice_items.append((invoice, manager_label, cost))

        invoice_count = await sheets.upsert_invoices_bulk(invoice_items)

        # owner 13.08: AZ = «Счет End» → фиксируем N «Дата Факт». Метку кладёт
        # _invoice_cells внутрь самого словаря счёта, поэтому после bulk-экспорта
        # она уже есть у каждого элемента. Дата пишется в БД; на лист попадёт
        # следующим синком либо точечным sync_invoice_row — здесь второй полный
        # экспорт не гоняем, он дорогой.
        _fd_filled = 0
        for invoice, _ml, _c in invoice_items:
            if invoice.get("_az_label") != "Счет End":
                continue
            try:
                if await db.fill_fact_date_on_invoice_end(int(invoice["id"]), invoice):
                    _fd_filled += 1
            except Exception:
                log.warning(
                    "fact_date autofill failed for invoice %s", invoice.get("id"), exc_info=True,
                )
        if _fd_filled:
            log.info("Дата Факт автозаполнена у %s счетов (полный синк)", _fd_filled)

    # --- Leads (amoCRM) ---
    # Webhook keeps DB live; full daily-sync just mirrors visible leads to Sheet.
    # Filter: год=2026 (бот живёт с янв 2026), все воронки, exclude status=143 (закрыт-не-реализован).
    filtered_leads = await db.list_leads_for_sheet(year=2026)

    # Fetch pipeline status names if amocrm service available
    status_map: dict[int, str] = {}
    if amocrm and filtered_leads:
        pipeline_ids = {int(l["pipeline_id"]) for l in filtered_leads if l.get("pipeline_id")}
        for pid in pipeline_ids:
            try:
                status_map.update(await amocrm.get_pipeline_statuses(pid))
            except Exception:
                log.warning("Failed to fetch pipeline %s statuses", pid)

    lead_count = await sheets.upsert_leads_bulk(
        filtered_leads,
        status_map=status_map,
        amo_user_map=amocrm_user_map,
    )

    # «Баланс компании» — отдельный лист с помесячной Доходы/Расходы.
    balance_count = 0
    try:
        balance_count = await sheets.sync_balance_company_sheet(db)
    except Exception:
        log.exception("sync_balance_company_sheet failed")

    return {
        "projects": project_count,
        "tasks": task_count,
        "invoices": invoice_count,
        "leads": lead_count,
        "balance_company": balance_count,
    }
