"""Helper for mirroring Telegram attachments to MinIO.

Used by every handler that accepts `message.photo/document/video`.
Extracts the attachment metadata, optionally mirrors it to MinIO,
and returns a dict ready to be appended to an FSM `attachments` list
or passed directly to `db.add_attachment(...)` / `db.save_chat_attachment(...)`.

Backward-compatible: if `storage is None` or `storage.enabled == False`
the result still contains `file_type/file_id/file_unique_id/caption`
(just with `minio_object_key=None`), exactly like the pre-MinIO payload.
"""
from __future__ import annotations

import logging
from typing import Any, Optional, TYPE_CHECKING

from aiogram.types import Message

if TYPE_CHECKING:
    from ..integrations.minio_storage import MinioStorage

log = logging.getLogger(__name__)


async def mirror_attachment(
    message: Message,
    storage: "Optional[MinioStorage]",
    prefix: str,
) -> Optional[dict[str, Any]]:
    """Extract attachment from `message` and optionally mirror to MinIO.

    Args:
        message: aiogram Message with possibly attached photo/document/video/audio/voice.
        storage: MinioStorage instance or None. If None / disabled — no upload.
        prefix: object-key prefix in MinIO bucket, e.g. ``"urgent/12345"``.

    Returns:
        - dict with keys: `file_type`, `file_id`, `file_unique_id`, `caption`, `minio_object_key`
        - or None if the message has no supported attachment.
    """
    file_type: Optional[str] = None
    file_id: Optional[str] = None
    file_unique_id: Optional[str] = None
    original_name: Optional[str] = None
    content_type: Optional[str] = None

    if message.document:
        file_type = "document"
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        original_name = message.document.file_name
        content_type = message.document.mime_type
    elif message.photo:
        ph = message.photo[-1]
        file_type = "photo"
        file_id = ph.file_id
        file_unique_id = ph.file_unique_id
        content_type = "image/jpeg"
    elif message.video:
        file_type = "video"
        file_id = message.video.file_id
        file_unique_id = message.video.file_unique_id
        content_type = message.video.mime_type or "video/mp4"
    elif message.audio:
        file_type = "audio"
        file_id = message.audio.file_id
        file_unique_id = message.audio.file_unique_id
        content_type = message.audio.mime_type or "audio/ogg"
    elif message.voice:
        file_type = "voice"
        file_id = message.voice.file_id
        file_unique_id = message.voice.file_unique_id
        content_type = message.voice.mime_type or "audio/ogg"
    else:
        return None

    minio_object_key: Optional[str] = None
    if storage is not None and storage.enabled and file_id and file_unique_id:
        try:
            minio_object_key = await storage.store_telegram_file(
                bot=message.bot,
                file_id=file_id,
                file_unique_id=file_unique_id,
                file_type=file_type,
                prefix=prefix,
                original_name=original_name,
                content_type=content_type,
            )
        except Exception as exc:  # noqa: BLE001 — never raise from handler code
            log.warning("MinIO mirror failed (prefix=%s): %s", prefix, exc)

    return {
        "file_type": file_type,
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "caption": message.caption,
        "minio_object_key": minio_object_key,
    }
