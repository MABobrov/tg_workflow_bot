"""MinIO object storage for Telegram attachments.

Mirror strategy:
- Telegram CDN remains primary (tg_file_id stored in DB as before).
- MinIO holds an app-controlled mirror with stable keys.
- All MinIO operations are best-effort; if MinIO is unavailable the bot
  still works as before (graceful degradation).

Object key layout:
    {prefix}/{yyyy}/{mm}/{tg_file_unique_id}{ext}

Example:
    urgent/2026/04/AgADBLwAAjShUUg.jpg
"""
from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from aiogram import Bot

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MinioConfig:
    enabled: bool
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False


# Map Telegram MIME-ish types to file extensions for nicer object keys.
_DEFAULT_EXTS = {
    "photo": ".jpg",
    "document": "",  # keep original name extension
    "video": ".mp4",
    "audio": ".ogg",
    "voice": ".ogg",
}


class MinioStorage:
    """Thin async wrapper around the (synchronous) `minio` client.

    All blocking calls are dispatched to a thread executor so they don't
    block the aiogram event loop.
    """

    def __init__(self, cfg: MinioConfig) -> None:
        self.cfg = cfg
        self._client = None  # lazy

    @property
    def enabled(self) -> bool:
        return self.cfg.enabled

    def _get_client(self):
        if self._client is None:
            from minio import Minio  # local import: optional dep

            self._client = Minio(
                self.cfg.endpoint,
                access_key=self.cfg.access_key,
                secret_key=self.cfg.secret_key,
                secure=self.cfg.secure,
            )
        return self._client

    async def ensure_bucket(self) -> bool:
        """Create the bucket if it doesn't exist. Returns True on success."""
        if not self.cfg.enabled:
            return False
        try:
            client = self._get_client()
            exists = await asyncio.to_thread(client.bucket_exists, self.cfg.bucket)
            if not exists:
                await asyncio.to_thread(client.make_bucket, self.cfg.bucket)
                log.info("MinIO: created bucket %s", self.cfg.bucket)
            return True
        except Exception as exc:
            log.warning("MinIO ensure_bucket failed: %s", exc)
            return False

    @staticmethod
    def _build_key(prefix: str, file_unique_id: str, file_type: str, original_name: str | None) -> str:
        now = datetime.now(timezone.utc)
        ext = ""
        if file_type == "document" and original_name and "." in original_name:
            ext = "." + original_name.rsplit(".", 1)[-1].lower()
        else:
            ext = _DEFAULT_EXTS.get(file_type, "")
        prefix = prefix.strip("/")
        return f"{prefix}/{now:%Y}/{now:%m}/{file_unique_id}{ext}"

    async def store_telegram_file(
        self,
        bot: "Bot",
        file_id: str,
        file_unique_id: str,
        file_type: str,
        prefix: str,
        original_name: str | None = None,
        content_type: str | None = None,
    ) -> Optional[str]:
        """Download a Telegram file by ID and upload it to MinIO.

        Returns the MinIO object key on success, or None on any failure.
        Never raises - designed to be safely called from handlers.
        """
        if not self.cfg.enabled:
            return None
        try:
            tg_file = await bot.get_file(file_id)
            buf = io.BytesIO()
            await bot.download(tg_file, destination=buf)
            buf.seek(0)
            data_bytes = buf.getvalue()
            length = len(data_bytes)
            object_key = self._build_key(prefix, file_unique_id, file_type, original_name)
            client = self._get_client()
            ct = content_type or "application/octet-stream"

            def _put() -> None:
                client.put_object(
                    self.cfg.bucket,
                    object_key,
                    io.BytesIO(data_bytes),
                    length=length,
                    content_type=ct,
                )

            await asyncio.to_thread(_put)
            log.info("MinIO upload: %s (%d bytes)", object_key, length)
            return object_key
        except Exception as exc:
            log.warning("MinIO upload failed for file_id=%s: %s", file_id, exc)
            return None

    async def get_presigned_url(self, object_key: str, expires_seconds: int = 3600) -> Optional[str]:
        if not self.cfg.enabled:
            return None
        try:
            from datetime import timedelta

            client = self._get_client()
            url = await asyncio.to_thread(
                client.presigned_get_object,
                self.cfg.bucket,
                object_key,
                timedelta(seconds=expires_seconds),
            )
            return url
        except Exception as exc:
            log.warning("MinIO presign failed for %s: %s", object_key, exc)
            return None

    async def download_object(self, object_key: str) -> Optional[bytes]:
        if not self.cfg.enabled:
            return None
        try:
            client = self._get_client()

            def _get() -> bytes:
                resp = client.get_object(self.cfg.bucket, object_key)
                try:
                    return resp.read()
                finally:
                    resp.close()
                    resp.release_conn()

            return await asyncio.to_thread(_get)
        except Exception as exc:
            log.warning("MinIO download failed for %s: %s", object_key, exc)
            return None
