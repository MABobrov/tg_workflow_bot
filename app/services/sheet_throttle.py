"""Background task: throttled bulk-sync Sheet «Leads» when dirty flag is set.

Why throttle: amoCRM webhook may fire many lead-events per minute (bulk imports,
status migrations). Doing a Google Sheets `values.update` per event would burn
through the 60-write-per-minute quota. Instead the webhook handler only flips
`settings.sheet_dirty_leads='1'`; this loop wakes every 60s, does ONE bulk
upsert of all visible leads, and clears the flag.

Pipeline status names are cached for 10 min — they almost never change.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

from ..db import Database
from ..integrations.amocrm import AmoCRMService
from ..integrations.sheets import GoogleSheetsService

log = logging.getLogger(__name__)

THROTTLE_INTERVAL_SEC = 60
PIPELINE_STATUS_TTL_SEC = 600  # 10 min
RECONCILE_INTERVAL_SEC = 1800  # 30 min — re-read РП «Импорт ОП», сверить источник/менеджер


class _StatusCache:
    """Per-pipeline status_id → name cache with TTL."""

    def __init__(self, ttl: int = PIPELINE_STATUS_TTL_SEC) -> None:
        self._ttl = ttl
        self._data: dict[int, dict[int, str]] = {}
        self._fetched_at: dict[int, float] = {}

    async def get_all(self, amocrm: AmoCRMService, pipeline_ids: set[int]) -> dict[int, str]:
        merged: dict[int, str] = {}
        now = time.monotonic()
        for pid in pipeline_ids:
            fetched = self._fetched_at.get(pid, 0.0)
            if pid not in self._data or now - fetched > self._ttl:
                try:
                    self._data[pid] = await amocrm.get_pipeline_statuses(pid)
                    self._fetched_at[pid] = now
                except Exception:
                    log.warning("sheet_throttle: pipeline %s statuses fetch failed", pid, exc_info=True)
                    self._data.setdefault(pid, {})
            merged.update(self._data[pid])
        return merged


async def _sync_once(
    db: Database,
    sheets: GoogleSheetsService,
    amocrm: AmoCRMService | None,
    amocrm_user_map: dict[int, str],
    status_cache: _StatusCache,
) -> int:
    leads: list[dict[str, Any]] = await db.list_leads_for_sheet(year=2026)

    status_map: dict[int, str] = {}
    if amocrm and leads:
        pipeline_ids = {int(l["pipeline_id"]) for l in leads if l.get("pipeline_id")}
        status_map = await status_cache.get_all(amocrm, pipeline_ids)

    count = await sheets.upsert_leads_bulk(
        leads,
        status_map=status_map,
        amo_user_map=amocrm_user_map,
    )
    return count


def _norm_phone(s: Any) -> str:
    d = re.sub(r"\D", "", str(s or ""))
    return d[-10:] if len(d) >= 10 else d


def _norm_name(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


async def reconcile_leads_with_rp(db: Database, sheets: GoogleSheetsService) -> dict[str, int]:
    """Сверка источник/менеджер sheet-visible лидов с таблицей РП «Импорт ОП»
    (колонки BR-BY исходной таблицы). РП — источник истины для СМАТЧЕННЫХ лидов
    (матч по телефону, затем по имени). Пишет rp_source/rp_manager в БД; лиды,
    выпавшие из РП, получают rp_*=NULL (рендер откатывается на amoCRM). Возвращает stats.
    """
    src_id = sheets.cfg.source_spreadsheet_id
    if not src_id:
        return {"rp_rows": 0, "leads": 0, "matched": 0, "changed": 0}

    # gspread синхронный — читаем в thread под общим _sync_lock, чтобы НЕ блокировать
    # event loop (иначе /health таймаутит → watchdog видит health=000). См. паттерн
    # to_thread+lock во всех *_sync методах GoogleSheetsService.
    def _fetch_rp_range() -> list[list[str]]:
        with sheets._sync_lock:
            gc = sheets._get_client()
            ws = gc.open_by_key(src_id).worksheet(sheets.cfg.source_sheet_name)
            # BR Дата · BS Имя · BT Телефон · BU Сделка · BV Менеджер · BW Источник · BX Статус · BY №счёта
            return ws.get("BR1:BY300")

    rng = await asyncio.to_thread(_fetch_rp_range)

    rp_by_phone: dict[str, dict[str, str]] = {}
    rp_by_name: dict[str, dict[str, str]] = {}
    for r in rng:
        phone = _norm_phone(r[2] if len(r) > 2 else "")
        name = _norm_name(r[1] if len(r) > 1 else "")
        if name in ("имя", "") and not phone:
            continue  # header / empty row
        rec = {
            "src": (r[5] if len(r) > 5 else "").strip(),
            "mgr": (r[4] if len(r) > 4 else "").strip(),
            "status": (r[6] if len(r) > 6 else "").strip(),  # BX Статус
            "inv": (r[7] if len(r) > 7 else "").strip(),     # BY №счёта (мост лид↔счёт)
            "deal": (r[3] if len(r) > 3 else "").strip(),    # BU Сделка
        }
        if phone:
            rp_by_phone.setdefault(phone, rec)
        if name:
            rp_by_name.setdefault(name, rec)

    leads = await db.list_leads_for_sheet(year=2026)
    matched = changed = 0
    for lead in leads:
        rec = None
        lp = _norm_phone(lead.get("phone"))
        if lp and lp in rp_by_phone:
            rec = rp_by_phone[lp]
        else:
            for cand in (_norm_name(lead.get("contact_name")), _norm_name(lead.get("name"))):
                if cand and cand in rp_by_name:
                    rec = rp_by_name[cand]
                    break
        if rec:
            matched += 1
            new_src = rec["src"] or None
            new_mgr = rec["mgr"] or None
            new_status = rec.get("status") or None
            new_inv = rec.get("inv") or None
            new_deal = rec.get("deal") or None
        else:
            new_src = new_mgr = new_status = new_inv = new_deal = None
        if (
            (lead.get("rp_source") or None) != new_src
            or (lead.get("rp_manager") or None) != new_mgr
            or (lead.get("rp_status") or None) != new_status
            or (lead.get("rp_invoice_number") or None) != new_inv
            or (lead.get("rp_deal") or None) != new_deal
        ):
            await db.update_lead_rp(
                lead["amo_lead_id"], new_src, new_mgr,
                rp_status=new_status, rp_invoice_number=new_inv, rp_deal=new_deal,
            )
            changed += 1
    return {"rp_rows": len(rp_by_phone), "leads": len(leads), "matched": matched, "changed": changed}


async def sheet_throttle_loop(
    db: Database,
    sheets: GoogleSheetsService,
    amocrm: AmoCRMService | None,
    amocrm_user_map: dict[int, str],
    interval_sec: int = THROTTLE_INTERVAL_SEC,
) -> None:
    """Run forever. Cancel-safe."""
    log.info("sheet_throttle started (interval=%ss)", interval_sec)
    status_cache = _StatusCache()
    last_reconcile = 0.0  # 0 → reconcile on first tick (boot)
    while True:
        try:
            now = time.monotonic()
            if now - last_reconcile >= RECONCILE_INTERVAL_SEC:
                try:
                    stats = await reconcile_leads_with_rp(db, sheets)
                    last_reconcile = now  # advance ONLY on success → transient Google flap retries next tick
                    if stats["changed"]:
                        await db.mark_sheet_dirty("leads", True)
                        log.info(
                            "sheet_throttle: РП reconcile matched=%d changed=%d (rp_rows=%d, leads=%d)",
                            stats["matched"], stats["changed"], stats["rp_rows"], stats["leads"],
                        )
                except Exception:
                    log.exception("sheet_throttle: РП reconcile failed (will retry next tick)")

            if await db.is_sheet_dirty("leads"):
                count = await _sync_once(db, sheets, amocrm, amocrm_user_map, status_cache)
                # Reset only after successful upsert — if upsert raised we'll retry next tick.
                await db.mark_sheet_dirty("leads", False)
                log.info("sheet_throttle: synced %d leads to «Leads» sheet", count)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("sheet_throttle: iteration error (will retry next tick)")
        await asyncio.sleep(interval_sec)
