"""amoCRM Chat API webhook endpoint.

Wired into the existing aiohttp server in main.py at POST /webhooks/amocrm/chat.

Responsibilities:
  - HMAC-SHA1 signature verification (X-Signature header)
  - Parse + normalize incoming events into AmoChatEvent
  - Dispatch new_message events to VirtualManager (fire-and-forget)
  - Always return 200 to prevent amoCRM retries on transient errors
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from aiohttp import web

from ..integrations.amocrm import AmoCRMService
from ..services.virtual_manager import AmoChatEvent, VirtualManager

log = logging.getLogger(__name__)


def _extract_text(payload_msg: dict[str, Any]) -> str:
    """amoCRM Chat message can be of several types; we handle text only for now."""
    if not isinstance(payload_msg, dict):
        return ""
    msg_type = (payload_msg.get("type") or "").lower()
    if msg_type in {"", "text"}:
        return (payload_msg.get("text") or "").strip()
    return ""


def _detect_sender_type(sender: dict[str, Any]) -> str:
    """Best-effort classification: 'client' / 'manager' / 'bot' / unknown.

    amoCRM webhooks carry hints we can use — tune as the real shape becomes clearer.
    """
    if not isinstance(sender, dict):
        return ""
    explicit = (sender.get("type") or sender.get("role") or "").lower()
    if explicit in {"client", "user", "lead"}:
        return "client"
    if explicit in {"manager", "operator", "agent", "user_manager"}:
        return "manager"
    if explicit in {"bot", "system"}:
        return "bot"
    if "client_id" in sender:
        return "client"
    return ""


def _parse_event(payload: dict[str, Any]) -> AmoChatEvent | None:
    event_type = (payload.get("event_type") or payload.get("type") or "").lower()
    inner = payload.get("payload") or payload
    if not isinstance(inner, dict):
        return None

    chat_id = (
        inner.get("conversation_id")
        or inner.get("chat_id")
        or inner.get("conversation_ref_id")
        or ""
    )
    if not chat_id:
        return None

    sender = inner.get("sender") or inner.get("author") or {}
    sender_type = _detect_sender_type(sender if isinstance(sender, dict) else {})

    msg = inner.get("message") or {}
    text = _extract_text(msg if isinstance(msg, dict) else {})

    source = inner.get("source") or {}
    source_external_id = None
    if isinstance(source, dict):
        source_external_id = (
            source.get("external_id")
            or source.get("name")
            or source.get("type")
        )

    amo_lead_id_raw = (
        inner.get("lead_id")
        or (inner.get("conversation_meta") or {}).get("lead_id")
        if isinstance(inner.get("conversation_meta"), dict)
        else inner.get("lead_id")
    )
    try:
        amo_lead_id = int(amo_lead_id_raw) if amo_lead_id_raw else None
    except (TypeError, ValueError):
        amo_lead_id = None

    return AmoChatEvent(
        event_type=event_type or "new_message",
        chat_id=str(chat_id),
        conversation_ref_id=inner.get("conversation_ref_id"),
        msg_id=inner.get("msgid") or inner.get("id"),
        sender_type=sender_type,
        sender_name=(sender.get("name") if isinstance(sender, dict) else None),
        text=text,
        source_external_id=str(source_external_id) if source_external_id else None,
        amo_lead_id=amo_lead_id,
        raw=payload,
    )


async def handle_amocrm_chat_webhook(
    request: web.Request,
    *,
    vm: VirtualManager,
    channel_secret: str,
    bg_tasks: set[asyncio.Task],
) -> web.Response:
    body_bytes = await request.read()
    signature = request.headers.get("X-Signature", "")
    if not AmoCRMService.verify_chat_signature(channel_secret, body_bytes, signature):
        log.warning("amoCRM webhook: invalid signature (len=%d, sig=%r...)",
                    len(body_bytes), signature[:16])
        return web.Response(status=403, text="Invalid signature")

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        log.warning("amoCRM webhook: bad JSON body (len=%d)", len(body_bytes))
        return web.Response(status=400, text="Bad JSON")

    if not isinstance(payload, dict):
        return web.Response(status=400, text="Payload must be a JSON object")

    event = _parse_event(payload)
    if not event:
        log.info("amoCRM webhook: unparseable / unsupported payload, ignoring")
        return web.json_response({"status": "ignored"})

    if not event.text:
        # No textual content — could be typing / delivered / image-only / etc.
        log.debug("amoCRM webhook: no text in event %s, ignoring", event.event_type)
        return web.json_response({"status": "ignored"})

    # Fire-and-forget — keep webhook latency low so amoCRM doesn't retry.
    async def _process() -> None:
        try:
            await vm.handle_incoming_message(event)
        except Exception:
            log.exception("VM handler crashed")

    task = asyncio.create_task(_process(), name="vm_webhook_process")
    bg_tasks.add(task)
    task.add_done_callback(bg_tasks.discard)

    return web.json_response({"status": "accepted"})
