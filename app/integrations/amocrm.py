from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from ..db import Database
from ..utils import utcnow


log = logging.getLogger(__name__)


@dataclass
class AmoConfig:
    enabled: bool
    base_url: str  # e.g. https://subdomain.amocrm.ru OR https://subdomain.kommo.com
    client_id: str | None = None
    client_secret: str | None = None
    redirect_uri: str | None = None
    access_token: str | None = None
    refresh_token: str | None = None
    # Optional defaults:
    pipeline_id: int | None = None
    status_id: int | None = None


class AmoCRMService:
    """Minimal amoCRM/Kommo API client with OAuth2 refresh token support.

    Notes:
    - Access token is short-lived, refresh token rotates on each refresh (store both new tokens).
    """

    def __init__(self, cfg: AmoConfig, db: Database):
        self.cfg = cfg
        self.db = db
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _url(self, path: str) -> str:
        return self.cfg.base_url.rstrip("/") + path

    async def _get_tokens_from_db(self) -> tuple[str | None, str | None]:
        access = await self.db.get_setting("amocrm_access_token")
        refresh = await self.db.get_setting("amocrm_refresh_token")
        return access, refresh

    async def _save_tokens_to_db(self, access: str, refresh: str) -> None:
        await self.db.set_setting("amocrm_access_token", access)
        await self.db.set_setting("amocrm_refresh_token", refresh)
        await self.db.set_setting("amocrm_tokens_updated_at", utcnow().isoformat())

    async def _refresh_tokens(self, refresh_token: str) -> tuple[str, str]:
        """Refresh tokens via POST /oauth2/access_token"""
        if self._session is None:
            raise RuntimeError("amoCRM session not started; call start() first")
        if not self.cfg.client_id or not self.cfg.client_secret or not self.cfg.redirect_uri:
            raise RuntimeError(
                "amoCRM token refresh requires AMOCRM_CLIENT_ID/AMOCRM_CLIENT_SECRET/AMOCRM_REDIRECT_URI"
            )

        payload = {
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": self.cfg.redirect_uri,
        }
        url = self._url("/oauth2/access_token")
        async with self._session.post(url, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"amoCRM token refresh failed {resp.status}: {text}")
            data = json.loads(text)
            access = data["access_token"]
            refresh = data["refresh_token"]
            return access, refresh

    async def get_access_token(self) -> str:
        """Return a valid access token, refreshing if needed.

        We refresh lazily on first need or if API returns 401.
        """
        async with self._lock:
            access, refresh = await self._get_tokens_from_db()
            access = access or self.cfg.access_token
            refresh = refresh or self.cfg.refresh_token
            if access:
                return access
            if not refresh:
                raise RuntimeError(
                    "amoCRM enabled but no tokens configured. Set AMOCRM_ACCESS_TOKEN "
                    "or AMOCRM_REFRESH_TOKEN (+ OAuth credentials)."
                )
            access, refresh = await self._refresh_tokens(refresh)
            await self._save_tokens_to_db(access, refresh)
            return access

    async def _request(
        self,
        method: str,
        path: str,
        json_body: Any | None = None,
        params: Any | None = None,
    ) -> Any:
        if not self.cfg.enabled:
            raise RuntimeError("amoCRM integration disabled")
        await self.start()
        if self._session is None:
            raise RuntimeError("amoCRM session not started; call start() first")

        token = await self.get_access_token()
        url = self._url(path)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with self._session.request(method, url, json=json_body, params=params, headers=headers) as resp:
            if resp.status == 401:
                # try refresh once
                async with self._lock:
                    _, refresh = await self._get_tokens_from_db()
                    refresh = refresh or self.cfg.refresh_token
                    if not refresh:
                        raise RuntimeError(
                            "amoCRM returned 401 and auto-refresh is unavailable. "
                            "Set AMOCRM_REFRESH_TOKEN + AMOCRM_CLIENT_ID/AMOCRM_CLIENT_SECRET/AMOCRM_REDIRECT_URI."
                        )
                    access, new_refresh = await self._refresh_tokens(refresh)
                    await self._save_tokens_to_db(access, new_refresh)
                    token = access
                headers["Authorization"] = f"Bearer {token}"
                async with self._session.request(method, url, json=json_body, params=params, headers=headers) as resp2:
                    text2 = await resp2.text()
                    if resp2.status >= 400:
                        raise RuntimeError(f"amoCRM request failed {resp2.status}: {text2}")
                    return json.loads(text2) if text2 else None

            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"amoCRM request failed {resp.status}: {text}")
            return json.loads(text) if text else None

    # ----------- public helpers -----------

    async def create_lead_for_project(self, project: dict[str, Any]) -> int:
        """Create lead in amoCRM/Kommo: POST /api/v4/leads"""
        leads = [
            {
                "name": project.get("title") or project.get("code") or "Проект",
                "price": int(project.get("amount") or 0),
            }
        ]
        if self.cfg.pipeline_id:
            leads[0]["pipeline_id"] = self.cfg.pipeline_id
        if self.cfg.status_id:
            leads[0]["status_id"] = self.cfg.status_id

        data = await self._request("POST", "/api/v4/leads", json_body=leads)
        # Expected: {"_embedded": {"leads": [{"id": ...}]}}
        try:
            return int(data["_embedded"]["leads"][0]["id"])
        except Exception as e:
            raise RuntimeError(f"Unexpected lead create response: {data}") from e

    async def create_task_for_lead(self, lead_id: int, text: str, complete_till_ts: int) -> int:
        """Create task in amoCRM/Kommo: POST /api/v4/tasks"""
        body = [
            {
                "entity_id": lead_id,
                "entity_type": "leads",
                "text": text,
                "complete_till": complete_till_ts,
            }
        ]
        data = await self._request("POST", "/api/v4/tasks", json_body=body)
        try:
            return int(data["_embedded"]["tasks"][0]["id"])
        except Exception as e:
            raise RuntimeError(f"Unexpected task create response: {data}") from e

    # ----------- tasks API helpers (v4/tasks) -----------

    @staticmethod
    def _tasks_query_params(
        page: int | None = None,
        limit: int | None = None,
        filter_: dict[str, Any] | None = None,
        order: dict[str, str] | None = None,
    ) -> list[tuple[str, Any]]:
        params: list[tuple[str, Any]] = []
        if page is not None:
            params.append(("page", int(page)))
        if limit is not None:
            params.append(("limit", int(limit)))

        for key, value in (filter_ or {}).items():
            if isinstance(value, list):
                for item in value:
                    params.append((f"filter[{key}][]", item))
            elif isinstance(value, dict):
                for subkey, subval in value.items():
                    params.append((f"filter[{key}][{subkey}]", subval))
            else:
                params.append((f"filter[{key}]", value))

        for key, value in (order or {}).items():
            params.append((f"order[{key}]", value))
        return params

    async def list_tasks(
        self,
        page: int | None = None,
        limit: int | None = None,
        filter_: dict[str, Any] | None = None,
        order: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """GET /api/v4/tasks"""
        params = self._tasks_query_params(page=page, limit=limit, filter_=filter_, order=order)
        data = await self._request("GET", "/api/v4/tasks", params=params or None)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected tasks list response: {data}")
        return data

    async def get_task(self, task_id: int) -> dict[str, Any]:
        """GET /api/v4/tasks/{id}"""
        data = await self._request("GET", f"/api/v4/tasks/{int(task_id)}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected task response: {data}")
        return data

    async def create_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """POST /api/v4/tasks (batch create)"""
        data = await self._request("POST", "/api/v4/tasks", json_body=tasks)
        try:
            embedded = data.get("_embedded", {})
            return list(embedded.get("tasks", []))
        except Exception as e:
            raise RuntimeError(f"Unexpected create tasks response: {data}") from e

    async def update_tasks(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """PATCH /api/v4/tasks (batch update)"""
        data = await self._request("PATCH", "/api/v4/tasks", json_body=tasks)
        try:
            embedded = data.get("_embedded", {})
            return list(embedded.get("tasks", []))
        except Exception as e:
            raise RuntimeError(f"Unexpected update tasks response: {data}") from e

    async def update_task(self, task_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """PATCH /api/v4/tasks/{id}"""
        data = await self._request("PATCH", f"/api/v4/tasks/{int(task_id)}", json_body=payload)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected update task response: {data}")
        return data

    async def complete_task(self, task_id: int, result_text: str) -> dict[str, Any]:
        """PATCH /api/v4/tasks/{id} with is_completed + result[text]."""
        return await self.update_task(
            task_id,
            {
                "is_completed": True,
                "result": {"text": result_text},
            },
        )

    # ----------- leads API helpers (v4/leads) -----------

    async def list_leads(
        self,
        page: int = 1,
        limit: int = 50,
        filter_: dict[str, Any] | None = None,
        order: dict[str, str] | None = None,
        with_: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """GET /api/v4/leads — returns list of lead dicts (may be empty)."""
        params: list[tuple[str, Any]] = [("page", page), ("limit", limit)]
        for key, value in (filter_ or {}).items():
            if isinstance(value, list):
                for item in value:
                    params.append((f"filter[{key}][]", item))
            elif isinstance(value, dict):
                for subkey, subval in value.items():
                    params.append((f"filter[{key}][{subkey}]", subval))
            else:
                params.append((f"filter[{key}]", value))
        for key, value in (order or {}).items():
            params.append((f"order[{key}]", value))
        if with_:
            params.append(("with", ",".join(with_)))
        try:
            data = await self._request("GET", "/api/v4/leads", params=params)
        except RuntimeError as exc:
            # amoCRM returns 204 No Content when no leads match
            if "204" in str(exc):
                return []
            raise
        if not data or not isinstance(data, dict):
            return []
        embedded = data.get("_embedded", {})
        return list(embedded.get("leads", []))

    async def get_lead(self, lead_id: int, with_: list[str] | None = None) -> dict[str, Any]:
        """GET /api/v4/leads/{id}"""
        params: list[tuple[str, Any]] | None = None
        if with_:
            params = [("with", ",".join(with_))]
        data = await self._request("GET", f"/api/v4/leads/{int(lead_id)}", params=params)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected lead response: {data}")
        return data

    async def update_lead(self, lead_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """PATCH /api/v4/leads/{id} — e.g. set responsible_user_id."""
        data = await self._request("PATCH", f"/api/v4/leads/{int(lead_id)}", json_body=payload)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected update lead response: {data}")
        return data

    async def get_users(self, page: int = 1, limit: int = 50) -> list[dict[str, Any]]:
        """GET /api/v4/users — list amoCRM users (needed to map Telegram user to amo user)."""
        params = [("page", page), ("limit", limit)]
        data = await self._request("GET", "/api/v4/users", params=params)
        if not data or not isinstance(data, dict):
            return []
        return list(data.get("_embedded", {}).get("users", []))
