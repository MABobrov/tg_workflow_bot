"""Anthropic Claude API wrapper used by the virtual-manager service.

One-call helper: build messages.create request with prompt caching enabled,
return a normalized response (text + tool_use blocks + usage). The agentic
loop (handle tool_use → call again) lives in virtual_manager.py — this module
is intentionally thin.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import anthropic

log = logging.getLogger(__name__)


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class ClaudeUsage:
    input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class ClaudeResponse:
    stop_reason: str
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    raw_content: list[dict[str, Any]] = field(default_factory=list)
    usage: ClaudeUsage = field(default_factory=ClaudeUsage)


def _block_to_dict(block: Any) -> dict[str, Any]:
    """Convert SDK block object into a JSON-serializable dict for storage / re-feed."""
    block_type = getattr(block, "type", None)
    if block_type == "text":
        return {"type": "text", "text": block.text}
    if block_type == "tool_use":
        return {
            "type": "tool_use",
            "id": block.id,
            "name": block.name,
            "input": block.input,
        }
    if block_type == "thinking":
        thinking_text = getattr(block, "thinking", "") or ""
        out: dict[str, Any] = {"type": "thinking", "thinking": thinking_text}
        signature = getattr(block, "signature", None)
        if signature:
            out["signature"] = signature
        return out
    return {"type": block_type or "unknown", "raw": str(block)}


class AnthropicClient:
    def __init__(self, api_key: str, model: str, max_tokens: int = 1024):
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required")
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception:
            log.exception("AnthropicClient close failed")

    async def send_turn(
        self,
        system_prompt: str,
        tools: list[dict[str, Any]],
        history: list[dict[str, Any]],
        max_tokens: int | None = None,
    ) -> ClaudeResponse:
        """One round-trip to messages.create with prompt caching.

        - system_prompt is wrapped as a single text block with cache_control
        - tools are passed as-is; cache_control on the last tool to cache the tool list
        - history is the messages array (list of {role, content})
        """
        system_blocks = [
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]

        cached_tools: list[dict[str, Any]] = []
        if tools:
            cached_tools = [dict(t) for t in tools]
            cached_tools[-1]["cache_control"] = {"type": "ephemeral"}

        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens or self.max_tokens,
                system=system_blocks,
                tools=cached_tools or anthropic.NOT_GIVEN,
                messages=history,
            )
        except anthropic.APIError:
            raise

        text_parts: list[str] = []
        tool_calls: list[ToolCall] = []
        raw_blocks: list[dict[str, Any]] = []
        for block in response.content:
            raw_blocks.append(_block_to_dict(block))
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(
                    ToolCall(id=block.id, name=block.name, input=dict(block.input or {}))
                )

        usage = ClaudeUsage(
            input_tokens=getattr(response.usage, "input_tokens", 0) or 0,
            cache_creation_input_tokens=getattr(response.usage, "cache_creation_input_tokens", 0) or 0,
            cache_read_input_tokens=getattr(response.usage, "cache_read_input_tokens", 0) or 0,
            output_tokens=getattr(response.usage, "output_tokens", 0) or 0,
        )

        return ClaudeResponse(
            stop_reason=response.stop_reason or "",
            text="".join(text_parts),
            tool_calls=tool_calls,
            raw_content=raw_blocks,
            usage=usage,
        )
