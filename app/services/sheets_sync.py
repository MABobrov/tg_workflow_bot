from __future__ import annotations

import logging
from typing import Any

from ..db import Database
from ..integrations.sheets import GoogleSheetsService

log = logging.getLogger(__name__)


async def import_from_source_sheet(
    db: Database,
    sheets: GoogleSheetsService,
    *,
    log_prefix: str,
) -> int:
    if not sheets.cfg.source_spreadsheet_id:
        return 0

    imported = 0
    op_rows = await sheets.read_op_sheet()
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
    return imported


async def export_to_sheets(
    db: Database,
    sheets: GoogleSheetsService,
    *,
    include_invoice_cost: bool,
    sync_invoices: bool = True,
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
                    project_code = ""
        task_items.append((task, project_code))

    project_count = await sheets.upsert_projects_bulk(project_items)
    task_count = await sheets.upsert_tasks_bulk(task_items)

    invoice_count = 0
    if sync_invoices:
        invoices = sorted(
            await db.list_invoices(limit=10000),
            key=lambda item: (item.get("receipt_date") or "9999-12-31", int(item["id"])),
        )
        invoice_items: list[tuple[dict[str, Any], str, dict[str, Any] | None]] = []
        for invoice in invoices:
            manager_label = ""
            if invoice.get("created_by"):
                user = await db.get_user_optional(int(invoice["created_by"]))
                if user:
                    manager_label = f"@{user.username}" if user.username else (user.full_name or str(user.telegram_id))

            # Fallback: определить creator_role из роли пользователя
            if not invoice.get("creator_role") and invoice.get("created_by"):
                try:
                    u = await db.get_user_optional(int(invoice["created_by"]))
                    if u and u.role and u.role.startswith("manager"):
                        invoice["creator_role"] = u.role
                except Exception:
                    pass

            cost = None
            if include_invoice_cost and not invoice.get("parent_invoice_id"):
                try:
                    cost = await db.get_full_invoice_cost_card(int(invoice["id"]))
                except Exception:
                    log.debug("Failed to build invoice cost card for invoice %s", invoice.get("id"), exc_info=True)
                    cost = None

            # Обогатить lead_info и zamery_info для столбцов BJ-BP
            try:
                invoice["_lead_info"] = await db.get_lead_info_for_invoice(invoice)
            except Exception:
                invoice["_lead_info"] = {}

            project_id = invoice.get("project_id")
            if project_id:
                try:
                    invoice["_zamery_info"] = await db.get_zamery_info_for_project(int(project_id))
                except Exception:
                    invoice["_zamery_info"] = ""

            # Расчет vs Факт — сравнение план/факт себестоимости
            plan_fact_label = ""
            if cost and not invoice.get("parent_invoice_id"):
                est_glass = float(invoice.get("estimated_glass") or 0)
                est_profile = float(invoice.get("estimated_profile") or 0)
                est_mat = float(invoice.get("estimated_materials") or 0)
                est_inst = float(invoice.get("estimated_installation") or 0)
                est_load = float(invoice.get("estimated_loaders") or 0)
                est_log = float(invoice.get("estimated_logistics") or 0)
                est_total = est_glass + est_profile + est_mat + est_inst + est_load + est_log
                if any([est_glass, est_profile, est_mat, est_inst, est_load, est_log]):
                    if cost["total_cost"] <= est_total:
                        plan_fact_label = "Расчет ОК"
                    else:
                        plan_fact_label = "Перерасчет прибыли"
            invoice["_plan_fact_label"] = plan_fact_label

            invoice_items.append((invoice, manager_label, cost))

        invoice_count = await sheets.upsert_invoices_bulk(invoice_items)

    return {
        "projects": project_count,
        "tasks": task_count,
        "invoices": invoice_count,
    }
