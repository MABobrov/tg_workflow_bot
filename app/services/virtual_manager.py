"""Virtual-manager orchestrator.

Receives normalized amoCRM Chat events from the webhook handler, debounces
multi-message bursts from the same client, then drives a Claude tool-use loop
to produce a reply + side-effects (lead field updates, hand-off, close).

Behavior (system prompt) is intentionally a placeholder — the user supplies
the real prompt later via VIRTUAL_MANAGER_SYSTEM_PROMPT env var or by editing
DEFAULT_SYSTEM_PROMPT below.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any

import anthropic

from ..config import Config
from ..integrations.amocrm import AmoCRMService
from ..integrations.anthropic_client import AnthropicClient, ToolCall
from ..services.conversation_store import ConversationStore
from ..services.notifier import Notifier

log = logging.getLogger(__name__)


DEFAULT_SYSTEM_PROMPT = (
    "You are a temporary placeholder for a virtual sales manager handling first-line "
    "client contact in an amoCRM chat for a Russian company. The full behavioral prompt "
    "has not yet been provided.\n\n"
    "Until the real prompt arrives, follow this minimal protocol:\n"
    "1. Greet the client politely in Russian.\n"
    "2. If the client has provided a name, record it via set_lead_field(field_code='client_name').\n"
    "3. Immediately call request_human_handoff with reason='qualification_complete' and a "
    "   one-sentence summary (in Russian) of what the client wrote.\n"
    "4. Do NOT attempt to quote prices, discuss timelines, or answer product questions.\n\n"
    "All replies to the client must be in Russian."
)


TOOLS: list[dict[str, Any]] = [
    {
        "name": "set_lead_field",
        "description": (
            "Save a piece of qualifying information about the client to the amoCRM lead. "
            "Call this each time the client provides structured data (name, phone, product type, "
            "size/dimensions, budget, region, desired installation date, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "field_code": {
                    "type": "string",
                    "description": (
                        "Logical field name. Must match a key configured in the "
                        "VIRTUAL_MANAGER_FIELDS env mapping (e.g. 'product_type', "
                        "'sizes', 'budget', 'region', 'install_date', 'client_name', 'phone'). "
                        "Unknown codes are ignored but recorded in the conversation log."
                    ),
                },
                "value": {
                    "type": "string",
                    "description": "The value to record (free-form string).",
                },
            },
            "required": ["field_code", "value"],
        },
    },
    {
        "name": "request_human_handoff",
        "description": (
            "Hand the conversation over to a human sales manager. Use when: "
            "(a) the client explicitly asks for a human, "
            "(b) qualification is complete and a manager should follow up with a quote, "
            "(c) the client asks something that requires sales judgement "
            "(price negotiation, complaints, complex configurations)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "enum": [
                        "qualification_complete",
                        "client_request",
                        "complex_question",
                        "complaint",
                        "off_topic",
                    ],
                },
                "summary": {
                    "type": "string",
                    "description": (
                        "Short summary of the conversation and collected information, "
                        "written in Russian for the human manager who will pick it up."
                    ),
                },
            },
            "required": ["reason", "summary"],
        },
    },
    {
        "name": "mark_conversation_complete",
        "description": (
            "Close the conversation when no further action is needed "
            "(client said goodbye, off-topic spam, repeated nonsense). "
            "The human manager will NOT be notified."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Brief reason (in Russian) for closing.",
                },
            },
            "required": ["reason"],
        },
    },
]


@dataclass
class AmoChatEvent:
    """Normalized incoming-message event from the amoCRM Chat webhook."""

    event_type: str
    chat_id: str
    conversation_ref_id: str | None
    msg_id: str | None
    sender_type: str  # 'client' | 'manager' | 'bot' (best-effort; controls anti-loop)
    sender_name: str | None
    text: str
    source_external_id: str | None  # 'avito' | 'whatsapp' | 'site' | None
    amo_lead_id: int | None  # may be None until amoCRM links the chat to a lead
    raw: dict[str, Any]


class VirtualManager:
    """Orchestrates the agentic loop for one bot deployment.

    One instance is created at startup and shared across all incoming webhooks.
    State (per-chat debounce timers, pending tasks) is held in instance dicts.
    """

    MAX_TOOL_LOOPS = 5  # safety cap on agentic loop iterations per turn

    def __init__(
        self,
        cfg: Config,
        amocrm: AmoCRMService,
        anthropic_client: AnthropicClient,
        store: ConversationStore,
        notifier: Notifier,
    ):
        self.cfg = cfg
        self.amocrm = amocrm
        self.anthropic = anthropic_client
        self.store = store
        self.notifier = notifier

        # Allow override via env without code change (keeps DEFAULT in code as a safe fallback)
        env_prompt = (os.getenv("VIRTUAL_MANAGER_SYSTEM_PROMPT") or "").strip()
        self.system_prompt = env_prompt or DEFAULT_SYSTEM_PROMPT

        # Per-chat debounce: chat_id -> Task that fires _process_after_debounce
        self._debounce_tasks: dict[str, asyncio.Task] = {}
        # Per-chat lock: prevents concurrent processing of the same conversation
        self._chat_locks: dict[str, asyncio.Lock] = {}

    # ---------- public entry point ----------

    async def handle_incoming_message(self, event: AmoChatEvent) -> None:
        """Webhook handler calls this on each parsed inbound event.

        Pipeline:
          1. anti-loop / sender-type filter
          2. idempotency check by amo_message_id
          3. get-or-create conversation row
          4. append user message
          5. reset debounce timer for chat_id
        """
        if event.event_type != "new_message":
            log.debug("VM: ignoring event_type=%s", event.event_type)
            return

        # Anti-loop: only respond to the actual end client.
        if event.sender_type and event.sender_type.lower() not in {"client", "user", ""}:
            log.debug("VM: skipping non-client sender (%s) in chat %s",
                      event.sender_type, event.chat_id)
            return

        # Idempotency
        if event.msg_id and await self.store.message_exists_by_amo_id(event.msg_id):
            log.debug("VM: duplicate msg_id %s — skip", event.msg_id)
            return

        conv = await self.store.get_or_create(
            amo_chat_id=event.chat_id,
            amo_lead_id=event.amo_lead_id,
            source_channel=event.source_external_id,
            amo_conversation_ref_id=event.conversation_ref_id,
            model=self.cfg.claude_model,
        )
        conv_id = int(conv["id"])

        # If conversation was previously closed/handed off, do not auto-resume.
        status = (conv.get("status") or "active").lower()
        if status in {"completed", "handoff"}:
            log.info(
                "VM: chat %s in status=%s — virtual manager will not respond. "
                "Human pickup expected.", event.chat_id, status,
            )
            return

        # Backfill amo_lead_id if it became known after creation
        if event.amo_lead_id and not conv.get("amo_lead_id"):
            await self.store.set_amo_lead_id(conv_id, event.amo_lead_id)

        await self.store.append_message(
            conversation_id=conv_id,
            role="user",
            content_blocks=[{"type": "text", "text": event.text}],
            amo_message_id=event.msg_id,
        )

        self._reschedule_debounce(event.chat_id, conv_id)

    def _reschedule_debounce(self, chat_id: str, conv_id: int) -> None:
        existing = self._debounce_tasks.get(chat_id)
        if existing and not existing.done():
            existing.cancel()
        task = asyncio.create_task(
            self._wait_then_process(chat_id, conv_id),
            name=f"vm_debounce:{chat_id}",
        )
        self._debounce_tasks[chat_id] = task
        task.add_done_callback(lambda _t: self._debounce_tasks.pop(chat_id, None))

    async def _wait_then_process(self, chat_id: str, conv_id: int) -> None:
        try:
            await asyncio.sleep(max(1, self.cfg.vm_debounce_seconds))
        except asyncio.CancelledError:
            return
        lock = self._chat_locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            try:
                await self._process_after_debounce(conv_id)
            except Exception:
                log.exception("VM: process_after_debounce crashed for chat %s", chat_id)

    # ---------- agentic loop ----------

    async def _process_after_debounce(self, conv_id: int) -> None:
        conv = await self.store.get(conv_id)
        if not conv:
            log.warning("VM: conversation %s vanished", conv_id)
            return

        chat_id = conv["amo_chat_id"]

        for loop_iter in range(self.MAX_TOOL_LOOPS):
            history = await self.store.list_messages(
                conv_id, limit=self.cfg.vm_history_limit,
            )
            if not history:
                log.warning("VM: empty history for conv %s", conv_id)
                return

            try:
                resp = await self.anthropic.send_turn(
                    system_prompt=self.system_prompt,
                    tools=TOOLS,
                    history=history,
                )
            except anthropic.APIError as exc:
                log.exception("VM: Anthropic API error on conv %s: %s", conv_id, exc)
                return

            await self.store.accumulate_usage(
                conv_id,
                input_tokens=resp.usage.input_tokens,
                cache_read_tokens=resp.usage.cache_read_input_tokens,
                output_tokens=resp.usage.output_tokens,
            )

            # Persist assistant turn (text + tool_use blocks together)
            await self.store.append_message(
                conversation_id=conv_id,
                role="assistant",
                content_blocks=resp.raw_content,
            )

            if resp.stop_reason == "tool_use" and resp.tool_calls:
                tool_results = await self._execute_tools(conv_id, resp.tool_calls)
                # Feed tool_result back as a "user" message (per Anthropic wire format)
                await self.store.append_message(
                    conversation_id=conv_id,
                    role="user",
                    content_blocks=tool_results,
                )
                # Refresh status — tools may have ended the conversation
                fresh = await self.store.get(conv_id)
                fresh_status = (fresh or {}).get("status", "active")
                if fresh_status in {"completed", "handoff"}:
                    # Send any final text (if model produced text alongside tool_use)
                    if resp.text.strip():
                        await self._send_to_client(chat_id, resp.text.strip())
                    return
                continue  # next loop iteration — let model react to tool results

            # No more tool calls: send text reply (if any) and stop.
            if resp.text.strip():
                await self._send_to_client(chat_id, resp.text.strip())
            return

        log.warning("VM: tool loop hit MAX_TOOL_LOOPS (%d) on conv %s",
                    self.MAX_TOOL_LOOPS, conv_id)

    # ---------- tool implementations ----------

    async def _execute_tools(
        self,
        conv_id: int,
        tool_calls: list[ToolCall],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for call in tool_calls:
            try:
                if call.name == "set_lead_field":
                    out = await self._tool_set_lead_field(conv_id, call.input)
                elif call.name == "request_human_handoff":
                    out = await self._tool_request_handoff(conv_id, call.input)
                elif call.name == "mark_conversation_complete":
                    out = await self._tool_mark_complete(conv_id, call.input)
                else:
                    out = {"error": f"Unknown tool: {call.name}"}
                results.append({
                    "type": "tool_result",
                    "tool_use_id": call.id,
                    "content": json.dumps(out, ensure_ascii=False),
                })
            except Exception as exc:
                log.exception("VM: tool %s crashed", call.name)
                results.append({
                    "type": "tool_result",
                    "tool_use_id": call.id,
                    "content": f"Tool execution error: {exc}",
                    "is_error": True,
                })
        return results

    async def _tool_set_lead_field(
        self,
        conv_id: int,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        field_code = (params.get("field_code") or "").strip()
        value = params.get("value")
        if not field_code or value is None:
            return {"ok": False, "error": "field_code and value are required"}

        # Always record into the conversation's collected_fields snapshot
        await self.store.update_status(
            conv_id,
            status="active",
            collected_fields={field_code: value},
        )

        conv = await self.store.get(conv_id) or {}
        amo_lead_id = conv.get("amo_lead_id")
        amo_field_id = self.cfg.vm_field_ids.get(field_code)

        if not amo_lead_id:
            return {"ok": True, "stored_locally": True,
                    "note": "amo_lead_id unknown — value saved in conversation only"}
        if not amo_field_id:
            return {"ok": True, "stored_locally": True,
                    "note": f"field_code '{field_code}' not configured in VIRTUAL_MANAGER_FIELDS"}

        try:
            await self.amocrm.update_lead_custom_fields(
                int(amo_lead_id), {amo_field_id: value},
            )
        except Exception as exc:
            log.exception("VM: amoCRM lead update failed")
            return {"ok": False, "error": f"amoCRM update failed: {exc}"}

        return {"ok": True, "amo_lead_id": amo_lead_id,
                "field_id": amo_field_id, "value": value}

    async def _tool_request_handoff(
        self,
        conv_id: int,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        reason = (params.get("reason") or "client_request").strip()
        summary = (params.get("summary") or "").strip()

        await self.store.update_status(conv_id, status="handoff", ended=True)

        conv = await self.store.get(conv_id) or {}
        collected = await self.store.get_collected_fields(conv_id)
        amo_lead_id = conv.get("amo_lead_id")

        # 1) Add a note to the lead so the human sees the summary in amoCRM UI
        if amo_lead_id:
            try:
                note_text = self._build_handoff_note(reason, summary, collected, conv)
                await self.amocrm.add_lead_note(int(amo_lead_id), note_text)
            except Exception:
                log.exception("VM: add_lead_note failed for lead %s", amo_lead_id)

            # 2) Optionally bump status to a "needs human" pipeline status
            if self.cfg.vm_handoff_status_id:
                try:
                    await self.amocrm.update_lead(
                        int(amo_lead_id),
                        {"status_id": int(self.cfg.vm_handoff_status_id)},
                    )
                except Exception:
                    log.exception("VM: lead status bump failed for lead %s", amo_lead_id)

        # 3) Notify managers in Telegram work chat
        await self._notify_handoff_telegram(conv, reason, summary, collected)

        return {"ok": True, "reason": reason, "amo_lead_id": amo_lead_id}

    async def _tool_mark_complete(
        self,
        conv_id: int,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        reason = (params.get("reason") or "").strip()
        await self.store.update_status(conv_id, status="completed", ended=True)
        return {"ok": True, "reason": reason}

    # ---------- helpers ----------

    def _build_handoff_note(
        self,
        reason: str,
        summary: str,
        collected: dict[str, Any],
        conv: dict[str, Any],
    ) -> str:
        lines = [
            "🤖 Виртуальный менеджер передаёт лид человеку.",
            f"Причина: {reason}",
            "",
            "Резюме разговора:",
            summary or "(нет)",
        ]
        if collected:
            lines.append("")
            lines.append("Собранная информация:")
            for k, v in collected.items():
                lines.append(f"• {k}: {v}")
        if conv.get("source_channel"):
            lines.append("")
            lines.append(f"Канал: {conv['source_channel']}")
        return "\n".join(lines)

    async def _notify_handoff_telegram(
        self,
        conv: dict[str, Any],
        reason: str,
        summary: str,
        collected: dict[str, Any],
    ) -> None:
        if not self.notifier or not self.notifier.work_chat_id:
            return
        amo_lead_id = conv.get("amo_lead_id")
        chat_id = conv.get("amo_chat_id")
        text_parts = [
            "🤖 <b>Виртуальный менеджер: лид готов к передаче</b>",
            "",
            f"Причина: <code>{reason}</code>",
        ]
        if amo_lead_id:
            text_parts.append(f"amoCRM лид: <code>{amo_lead_id}</code>")
        if chat_id:
            text_parts.append(f"Чат: <code>{chat_id}</code>")
        if conv.get("source_channel"):
            text_parts.append(f"Канал: {conv['source_channel']}")
        if summary:
            text_parts.append("")
            text_parts.append("<b>Резюме:</b>")
            text_parts.append(summary)
        if collected:
            text_parts.append("")
            text_parts.append("<b>Собрано:</b>")
            for k, v in collected.items():
                text_parts.append(f"• {k}: {v}")
        try:
            await self.notifier.bot.send_message(
                chat_id=int(self.notifier.work_chat_id),
                text="\n".join(text_parts),
            )
        except Exception:
            log.exception("VM: handoff Telegram notify failed")

    # ---------- outbound to amoCRM Chat (Mojo) ----------

    async def _send_to_client(self, chat_id: str, text: str) -> None:
        if not text:
            return
        if not all([
            self.cfg.amocrm_chat_channel_id,
            self.cfg.amocrm_chat_channel_secret,
            self.cfg.amocrm_chat_amojo_id,
        ]):
            log.warning(
                "VM: chat-channel credentials missing — cannot send reply to %s. "
                "Set AMOCRM_CHAT_CHANNEL_ID / _SECRET / _AMOJO_ID.",
                chat_id,
            )
            return

        scope_id = f"{self.cfg.amocrm_chat_channel_id}_{self.cfg.amocrm_chat_amojo_id}"
        now_ts = int(time.time())
        payload = {
            "event_type": "new_message",
            "payload": {
                "timestamp": now_ts,
                "msec_timestamp": now_ts * 1000,
                "msgid": str(uuid.uuid4()),
                "conversation_id": chat_id,
                "sender": {
                    "id": "virtual_manager_bot",
                    "name": "Виртуальный менеджер",
                },
                "message": {
                    "type": "text",
                    "text": text,
                },
            },
        }
        try:
            await self.amocrm.send_chat_event(
                amojo_url=self.cfg.amocrm_chat_amojo_url,
                scope_id=scope_id,
                channel_secret=self.cfg.amocrm_chat_channel_secret,
                event_payload=payload,
            )
        except Exception:
            log.exception("VM: send_chat_event failed for chat %s", chat_id)

    # ---------- shutdown ----------

    async def shutdown(self) -> None:
        for task in list(self._debounce_tasks.values()):
            if not task.done():
                task.cancel()
        if self._debounce_tasks:
            await asyncio.gather(*self._debounce_tasks.values(), return_exceptions=True)
        await self.anthropic.close()
