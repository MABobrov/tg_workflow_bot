"""amoCRM Lead webhook endpoint at POST /webhooks/amocrm/leads.

Wired into the existing aiohttp server in main.py.

Auth: path token validated by nginx location regex
  `location ~ ^/webhooks/amocrm/leads/[a-fA-F0-9]{64}$`.
Token = AMOCRM_LEADS_WEBHOOK_SECRET in .env (64-hex random, 256-bit entropy).
amoCRM Digital Pipeline webhooks do NOT sign requests (no HMAC, no headers) —
secrecy of the URL path is the only auth factor. Bot handler trusts that any
request reaching it has passed the nginx token check.

Responsibilities:
  - Parse events: leads[add|update|status|delete]
  - Upsert into DB and mark Sheet «Leads» dirty (background throttle writes Sheet)
  - Always return 200 on parse/process errors to prevent amoCRM retries
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from urllib.parse import parse_qs, unquote_plus

from aiohttp import web

from ..db import Database
from ..integrations.amocrm import AmoCRMService
from ..services.lead_poller import (
    EXCLUDED_STATUS_IDS,
    NOTIFY_STATUS_IDS,
    _extract_contact_info,
    _extract_source,
    _extract_tags,
    _publish_lead,
    _refresh_lead_note,
)
from ..services.notifier import Notifier

log = logging.getLogger(__name__)


def _decode_payload(body: bytes, content_type: str) -> dict[str, Any] | None:
    """amoCRM may send JSON OR x-www-form-urlencoded (legacy webhooks).

    Try both. Return None if neither parses.
    """
    text = body.decode("utf-8", errors="replace")
    ct = (content_type or "").lower()

    if "json" in ct or text.lstrip().startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass

    # form-encoded: leads[add][0][id]=123&leads[add][0][name]=Foo&...
    if "form" in ct or "urlencoded" in ct or "=" in text:
        try:
            return _unflatten_form(text)
        except Exception:
            log.exception("amoCRM leads webhook: form parse failed")
    return None


def _unflatten_form(text: str) -> dict[str, Any]:
    """Convert PHP-style form keys (`leads[add][0][id]=123`) into nested dict."""
    parsed = parse_qs(text, keep_blank_values=True)
    result: dict[str, Any] = {}
    for raw_key, values in parsed.items():
        value = values[0] if values else ""
        # split foo[bar][0][baz] into ['foo','bar','0','baz']
        parts: list[str] = []
        head, _, rest = raw_key.partition("[")
        parts.append(head)
        while rest:
            seg, _, rest = rest.partition("]")
            parts.append(seg)
            if rest.startswith("["):
                rest = rest[1:]
        _set_nested(result, parts, unquote_plus(value))
    _convert_int_keyed_dicts_to_lists(result)
    return result


def _set_nested(container: dict[str, Any], path: list[str], value: Any) -> None:
    cur: Any = container
    for i, key in enumerate(path):
        is_last = i == len(path) - 1
        if is_last:
            if isinstance(cur, dict):
                cur[key] = value
            return
        nxt_key = path[i + 1]
        if isinstance(cur, dict):
            if key not in cur or not isinstance(cur[key], (dict, list)):
                cur[key] = {}
            cur = cur[key]
        # arrays-as-dicts collapse later


def _convert_int_keyed_dicts_to_lists(node: Any) -> None:
    """Recurse: if a dict has only string-int keys '0','1','2'... convert to list."""
    if isinstance(node, dict):
        for k, v in list(node.items()):
            _convert_int_keyed_dicts_to_lists(v)
            if isinstance(v, dict) and v and all(kk.isdigit() for kk in v):
                ordered = [v[str(i)] for i in sorted(int(kk) for kk in v)]
                node[k] = ordered
    elif isinstance(node, list):
        for item in node:
            _convert_int_keyed_dicts_to_lists(item)


def _collect_lead_events(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Return list of (action, lead_dict) pairs.

    amoCRM payload shape:
      {"leads": {"add": [...], "update": [...], "status": [...], "delete": [...]}, "account": {...}}
    """
    events: list[tuple[str, dict[str, Any]]] = []
    leads = payload.get("leads")
    if not isinstance(leads, dict):
        return events
    for action in ("add", "update", "status", "delete"):
        items = leads.get(action) or []
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                events.append((action, item))
    return events


def _coerce_int(val: Any) -> int | None:
    try:
        return int(val) if val not in (None, "") else None
    except (TypeError, ValueError):
        return None


async def _process_event(
    db: Database,
    amocrm: AmoCRMService,
    notifier: Notifier,
    action: str,
    item: dict[str, Any],
) -> None:
    """Process one lead event. Always swallow exceptions (logged)."""
    try:
        amo_id = _coerce_int(item.get("id"))
        if not amo_id:
            log.warning("amoCRM leads webhook: event with no id: %s %r", action, item)
            return

        if action == "delete":
            await db.delete_lead_by_amo_id(amo_id)
            await db.mark_sheet_dirty("leads", True)
            log.info("amoCRM leads webhook: deleted lead amo_id=%s", amo_id)
            return

        # For add/update/status — fetch full lead from API to get contacts + custom fields.
        # Payload from amoCRM webhooks is sparse and inconsistent.
        try:
            full = await amocrm.get_lead(amo_id, with_=["contacts"])
        except Exception:
            log.exception("amoCRM leads webhook: get_lead %s failed; using payload only", amo_id)
            full = item

        status_id = _coerce_int(full.get("status_id")) or _coerce_int(item.get("status_id"))

        phone, contact_name = None, None
        try:
            phone, contact_name = await _extract_contact_info(amocrm, full)
        except Exception:
            log.exception("amoCRM leads webhook: contact extract failed amo_id=%s", amo_id)

        tags_json = _extract_tags(full)
        source = _extract_source(full)

        await db.upsert_lead_by_amo_id(
            amo_lead_id=amo_id,
            name=full.get("name") or item.get("name"),
            price=full.get("price") if full.get("price") is not None else item.get("price"),
            pipeline_id=_coerce_int(full.get("pipeline_id") or item.get("pipeline_id")),
            status_id=status_id,
            responsible_user_id=_coerce_int(
                full.get("responsible_user_id") or item.get("responsible_user_id")
            ),
            phone=phone,
            contact_name=contact_name,
            tags_json=tags_json,
            source=source,
        )

        # Mark sheet dirty only if lead is sheet-visible (not in EXCLUDED_STATUS_IDS).
        # Even for excluded leads we keep DB record up-to-date.
        if status_id is None or status_id not in EXCLUDED_STATUS_IDS:
            # Enrich with the responsible manager's latest amoCRM note before push.
            await _refresh_lead_note(
                db, amocrm, amo_id,
                _coerce_int(full.get("responsible_user_id") or item.get("responsible_user_id")),
            )
            await db.mark_sheet_dirty("leads", True)

        # webhook-notify-fix: publish lead card to work_chat for new leads in
        # NOTIFY_STATUS_IDS. Idempotent — skip if already published (workchat_message_id set).
        if action == "add" and status_id and status_id in NOTIFY_STATUS_IDS:
            try:
                lead_row = await db.get_lead_by_amo_id(amo_id)
                if lead_row and not lead_row.get("workchat_message_id"):
                    await _publish_lead(db, notifier, lead_row)
            except Exception:
                log.exception("amoCRM leads webhook: publish_lead failed amo_id=%s", amo_id)

        log.info(
            "amoCRM leads webhook: %s lead amo_id=%s status=%s",
            action, amo_id, status_id,
        )
    except Exception:
        log.exception("amoCRM leads webhook: _process_event crashed (action=%s)", action)


async def handle_amocrm_leads_webhook(
    request: web.Request,
    *,
    db: Database,
    amocrm: AmoCRMService,
    notifier: Notifier,
    secret: str,  # kept for future IP whitelist / signed events; unused (auth via nginx path token)
    bg_tasks: set[asyncio.Task],
) -> web.Response:
    """Public entry point. Wired in main.py."""
    body = await request.read()
    # Auth = path token (validated by nginx location regex).
    # If we got here, the URL had a valid 64-hex token.

    payload = _decode_payload(body, request.headers.get("Content-Type", ""))
    if payload is None:
        log.warning(
            "amoCRM leads webhook: cannot decode body (len=%d, ct=%r, head=%r)",
            len(body), request.headers.get("Content-Type"), body[:120],
        )
        return web.json_response({"status": "ignored", "reason": "bad payload"})

    events = _collect_lead_events(payload)
    if not events:
        log.info("amoCRM leads webhook: no lead events in payload (keys=%s)", list(payload.keys()))
        return web.json_response({"status": "ignored", "reason": "no events"})

    # Fire-and-forget so the HTTP response stays fast (<1s).
    async def _run_all() -> None:
        for action, item in events:
            await _process_event(db, amocrm, notifier, action, item)

    task = asyncio.create_task(_run_all(), name="amocrm_leads_webhook_process")
    bg_tasks.add(task)
    task.add_done_callback(bg_tasks.discard)

    return web.json_response({"status": "accepted", "events": len(events)})
