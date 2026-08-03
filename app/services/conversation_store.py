"""Persistence layer for virtual-manager conversations.

Two tables:
 - agent_conversations: one row per amoCRM chat (idempotency by amo_chat_id)
 - agent_messages: append-only log; content_json stores Anthropic content blocks verbatim
                   so the message stream can be re-fed to the API on the next turn

Roles stored in agent_messages.role match Anthropic's wire format:
  - "user" — both client messages AND tool_result blocks
  - "assistant" — Claude responses (text + tool_use blocks)
"""
from __future__ import annotations

import json
import logging
from typing import Any

from ..db import Database
from ..utils import to_iso, utcnow

log = logging.getLogger(__name__)


class ConversationStore:
    def __init__(self, db: Database):
        self.db = db

    async def get_or_create(
        self,
        *,
        amo_chat_id: str,
        amo_lead_id: int | None = None,
        source_channel: str | None = None,
        amo_conversation_ref_id: str | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        """Return existing conversation row by amo_chat_id, or create a new active one."""
        cur = await self.db.conn.execute(
            "SELECT * FROM agent_conversations WHERE amo_chat_id = ?",
            (amo_chat_id,),
        )
        row = await cur.fetchone()
        if row:
            return dict(row)

        now = to_iso(utcnow())
        cur = await self.db.conn.execute(
            """
            INSERT INTO agent_conversations
                (amo_lead_id, amo_chat_id, amo_conversation_ref_id, source_channel,
                 status, model, started_at, last_msg_at)
            VALUES (?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (amo_lead_id, amo_chat_id, amo_conversation_ref_id, source_channel,
             model, now, now),
        )
        await self.db.conn.commit()
        conv_id = cur.lastrowid
        cur = await self.db.conn.execute(
            "SELECT * FROM agent_conversations WHERE id = ?",
            (conv_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else {}

    async def get(self, conversation_id: int) -> dict[str, Any] | None:
        cur = await self.db.conn.execute(
            "SELECT * FROM agent_conversations WHERE id = ?",
            (int(conversation_id),),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def message_exists_by_amo_id(self, amo_message_id: str) -> bool:
        if not amo_message_id:
            return False
        cur = await self.db.conn.execute(
            "SELECT 1 FROM agent_messages WHERE amo_message_id = ? LIMIT 1",
            (amo_message_id,),
        )
        row = await cur.fetchone()
        return row is not None

    async def append_message(
        self,
        *,
        conversation_id: int,
        role: str,
        content_blocks: list[dict[str, Any]] | str,
        amo_message_id: str | None = None,
    ) -> int:
        """Append an Anthropic-format message. content_blocks is either:
        - list[dict] of content blocks (preferred), or
        - str (auto-wrapped as [{"type":"text","text":...}])
        """
        if isinstance(content_blocks, str):
            blocks = [{"type": "text", "text": content_blocks}]
        else:
            blocks = content_blocks
        content_json = json.dumps(blocks, ensure_ascii=False)
        now = to_iso(utcnow())

        cur = await self.db.conn.execute(
            """
            INSERT INTO agent_messages
                (conversation_id, role, content_json, amo_message_id, ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            (int(conversation_id), role, content_json, amo_message_id, now),
        )
        await self.db.conn.execute(
            "UPDATE agent_conversations SET last_msg_at = ? WHERE id = ?",
            (now, int(conversation_id)),
        )
        await self.db.conn.commit()
        return int(cur.lastrowid or 0)

    async def list_messages(
        self,
        conversation_id: int,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Return last N messages in chronological order, ready to feed to messages.create.

        The result is a list of {role, content} dicts; content is the deserialized list of blocks.
        """
        cur = await self.db.conn.execute(
            """
            SELECT role, content_json FROM agent_messages
            WHERE conversation_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(conversation_id), int(limit)),
        )
        rows = list(await cur.fetchall())
        rows.reverse()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                content = json.loads(row["content_json"])
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
            out.append({"role": row["role"], "content": content})
        return out

    async def set_amo_lead_id(self, conversation_id: int, amo_lead_id: int) -> None:
        await self.db.conn.execute(
            "UPDATE agent_conversations SET amo_lead_id = ? WHERE id = ?",
            (int(amo_lead_id), int(conversation_id)),
        )
        await self.db.conn.commit()

    async def update_status(
        self,
        conversation_id: int,
        status: str,
        *,
        collected_fields: dict[str, Any] | None = None,
        ended: bool = False,
    ) -> None:
        cur_row = await self.get(conversation_id)
        existing_collected: dict[str, Any] = {}
        if cur_row and cur_row.get("collected_fields_json"):
            try:
                existing_collected = json.loads(cur_row["collected_fields_json"]) or {}
            except json.JSONDecodeError:
                existing_collected = {}
        if collected_fields:
            existing_collected.update(collected_fields)
        collected_json = (
            json.dumps(existing_collected, ensure_ascii=False)
            if existing_collected else None
        )
        ended_at = to_iso(utcnow()) if ended else None

        if ended:
            await self.db.conn.execute(
                """
                UPDATE agent_conversations
                SET status = ?, collected_fields_json = ?, ended_at = ?
                WHERE id = ?
                """,
                (status, collected_json, ended_at, int(conversation_id)),
            )
        else:
            await self.db.conn.execute(
                """
                UPDATE agent_conversations
                SET status = ?, collected_fields_json = ?
                WHERE id = ?
                """,
                (status, collected_json, int(conversation_id)),
            )
        await self.db.conn.commit()

    async def accumulate_usage(
        self,
        conversation_id: int,
        *,
        input_tokens: int = 0,
        cache_read_tokens: int = 0,
        output_tokens: int = 0,
    ) -> None:
        await self.db.conn.execute(
            """
            UPDATE agent_conversations SET
                total_input_tokens      = total_input_tokens      + ?,
                total_cache_read_tokens = total_cache_read_tokens + ?,
                total_output_tokens     = total_output_tokens     + ?
            WHERE id = ?
            """,
            (int(input_tokens), int(cache_read_tokens), int(output_tokens),
             int(conversation_id)),
        )
        await self.db.conn.commit()

    async def get_collected_fields(self, conversation_id: int) -> dict[str, Any]:
        row = await self.get(conversation_id)
        if not row or not row.get("collected_fields_json"):
            return {}
        try:
            data = json.loads(row["collected_fields_json"])
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
