from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from ..db import Database
from ..integrations.amocrm import AmoCRMService
from ..integrations.sheets import GoogleSheetsService

log = logging.getLogger(__name__)


@dataclass
class IntegrationEvent:
    kind: str
    payload: dict[str, Any]


class IntegrationHub:
    """Single-threaded background worker for integrations (Sheets, amoCRM)."""

    def __init__(
        self,
        db: Database,
        sheets: GoogleSheetsService | None = None,
        amocrm: AmoCRMService | None = None,
    ):
        self.db = db
        self.sheets = sheets
        self.amocrm = amocrm
        self._q: asyncio.Queue[IntegrationEvent] = asyncio.Queue()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Integration worker terminated with error")
            self._task = None
        if self.amocrm:
            await self.amocrm.close()

    async def push(self, kind: str, payload: dict[str, Any]) -> None:
        if not self.sheets and not self.amocrm:
            return
        await self._q.put(IntegrationEvent(kind=kind, payload=payload))

    async def sync_project(self, project: dict[str, Any], manager_label: str = "") -> None:
        await self.push("project_upsert", {"project": project, "manager_label": manager_label})

    async def sync_task(self, task: dict[str, Any], project_code: str = "") -> None:
        await self.push("task_upsert", {"task": task, "project_code": project_code})

    async def sync_invoice_status(
        self,
        invoice_number: str,
        status: str,
        montazh_stage: str | None = None,
    ) -> None:
        """Write bot status / montazh stage back to ОП sheet (cols AR/AS)."""
        await self.push("invoice_status_writeback", {
            "invoice_number": invoice_number,
            "status": status,
            "montazh_stage": montazh_stage,
        })

    async def sync_invoice_row(self, invoice_id: int) -> None:
        """Re-export one invoice row to the Invoices sheet."""
        await self.push("invoice_row_upsert", {"invoice_id": invoice_id})

    async def maybe_create_lead(self, project_id: int) -> None:
        if not self.amocrm:
            return
        await self.push("amocrm_create_lead", {"project_id": project_id})

    async def _worker(self) -> None:
        while True:
            ev = await self._q.get()
            try:
                if ev.kind == "project_upsert" and self.sheets:
                    await self.sheets.upsert_project(
                        ev.payload["project"], manager_label=ev.payload.get("manager_label", "")
                    )
                elif ev.kind == "task_upsert" and self.sheets:
                    task = ev.payload["task"]
                    project_code = ev.payload.get("project_code", "")

                    # Safety net: if task belongs to a project, upsert project too.
                    project_id = task.get("project_id")
                    if project_id:
                        try:
                            project = await self.db.get_project(int(project_id))
                            if not project_code:
                                project_code = str(project.get("code") or "")

                            manager_label = ""
                            manager_id = project.get("manager_id")
                            if manager_id:
                                manager = await self.db.get_user_optional(int(manager_id))
                                if manager:
                                    manager_label = f"@{manager.username}" if manager.username else str(manager.telegram_id)

                            await self.sheets.upsert_project(project, manager_label=manager_label)
                        except Exception:
                            log.exception("Failed to upsert project from task event, task_id=%s", task.get("id"))

                    await self.sheets.upsert_task(task, project_code=project_code)
                elif ev.kind == "invoice_status_writeback" and self.sheets:
                    inv_num = ev.payload["invoice_number"]
                    status = ev.payload.get("status")
                    stage = ev.payload.get("montazh_stage")
                    if status:
                        await self.sheets.write_field_to_op(inv_num, "bot_status", status)
                    if stage:
                        await self.sheets.write_field_to_op(inv_num, "montazh_stage", stage)
                elif ev.kind == "invoice_row_upsert" and self.sheets:
                    inv_id = int(ev.payload["invoice_id"])
                    inv = await self.db.get_invoice(inv_id)
                    if inv:
                        manager_label = ""
                        creator = inv.get("created_by")
                        if creator:
                            mu = await self.db.get_user_optional(int(creator))
                            if mu:
                                manager_label = f"@{mu.username}" if mu.username else str(mu.telegram_id)
                        try:
                            inv["_edo_stats"] = await self.db.get_edo_stats_for_invoice(inv_id)
                        except Exception:
                            log.debug("Failed to get edo_stats for invoice %s", inv_id, exc_info=True)
                            inv["_edo_stats"] = {}
                        await self.sheets.upsert_invoice(inv, manager_label=manager_label)
                        log.info("Synced invoice row #%s (%s) to Invoices sheet", inv_id, inv.get("invoice_number"))
                elif ev.kind == "amocrm_create_lead" and self.amocrm:
                    pid = int(ev.payload["project_id"])
                    project = await self.db.get_project(pid)
                    if not project:
                        log.warning("Skip amoCRM lead create: project %s not found", pid)
                    elif project.get("amo_lead_id"):
                        log.info("Skip amoCRM lead create: project %s already has lead %s", pid, project.get("amo_lead_id"))
                    else:
                        lead_id = await self.amocrm.create_lead_for_project(project)
                        await self.db.set_project_amo_lead(pid, lead_id)
                        log.info("Created amoCRM lead %s for project %s", lead_id, pid)
                else:
                    # unknown or disabled
                    pass
            except Exception as e:
                if ev.kind in {"project_upsert", "task_upsert"} and self.sheets:
                    msg = str(e).lower()
                    is_sheets_config_error = isinstance(e, FileNotFoundError) or (
                        isinstance(e, RuntimeError)
                        and ("google_service_account" in msg or "service account" in msg)
                    )
                    if is_sheets_config_error:
                        log.error(
                            "Google Sheets integration disabled due to configuration error: %s. "
                            "Fix .env (GOOGLE_SERVICE_ACCOUNT_FILE/JSON, GSHEET_SPREADSHEET_ID) and restart bot.",
                            e,
                        )
                        self.sheets = None
                log.exception("Integration event failed: %s", ev.kind)
            finally:
                self._q.task_done()
