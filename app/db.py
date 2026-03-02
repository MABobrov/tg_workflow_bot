from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import aiosqlite

from .utils import parse_roles, roles_to_storage, to_iso, utcnow


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


@dataclass
class UserRow:
    telegram_id: int
    username: str | None
    full_name: str | None
    role: str | None
    is_active: int
    created_at: str
    updated_at: str


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._conn:
            raise RuntimeError("DB is not connected")
        return self._conn

    async def init_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                role TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE,
                    title TEXT NOT NULL,
                    address TEXT,
                    client TEXT,
                    amount REAL,
                    deadline TEXT,
                    status TEXT NOT NULL,
                    manager_id INTEGER,
                    rp_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    amo_lead_id INTEGER,
                    gs_row INTEGER,
                    FOREIGN KEY(manager_id) REFERENCES users(telegram_id) ON DELETE SET NULL,
                    FOREIGN KEY(rp_id) REFERENCES users(telegram_id) ON DELETE SET NULL
                );

            CREATE INDEX IF NOT EXISTS idx_projects_manager ON projects(manager_id);
            CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_by INTEGER,
                assigned_to INTEGER,
                due_at TEXT,
                payload_json TEXT,
                reminded_soon INTEGER NOT NULL DEFAULT 0,
                reminded_overdue INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY(created_by) REFERENCES users(telegram_id) ON DELETE SET NULL,
                FOREIGN KEY(assigned_to) REFERENCES users(telegram_id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_assigned_status ON tasks(assigned_to, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_at);

            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                tg_file_id TEXT NOT NULL,
                tg_file_unique_id TEXT,
                file_type TEXT NOT NULL,
                caption TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_attach_task ON attachments(task_id);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_id INTEGER,
                action TEXT NOT NULL,
                entity TEXT NOT NULL,
                entity_id TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity, entity_id);

            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amo_lead_id INTEGER UNIQUE NOT NULL,
                name TEXT,
                price REAL,
                pipeline_id INTEGER,
                status_id INTEGER,
                responsible_user_id INTEGER,
                claimed_by INTEGER,
                claimed_at TEXT,
                escalated INTEGER NOT NULL DEFAULT 0,
                workchat_message_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(claimed_by) REFERENCES users(telegram_id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_leads_amo ON leads(amo_lead_id);
            CREATE INDEX IF NOT EXISTS idx_leads_claimed ON leads(claimed_by);
            """
        )
        await self.conn.commit()

    # ------------------------- users -------------------------

    async def upsert_user(self, telegram_id: int, username: str | None, full_name: str | None) -> UserRow:
        now = to_iso(utcnow())
        await self.conn.execute(
            """
            INSERT INTO users (telegram_id, username, full_name, role, is_active, created_at, updated_at)
            VALUES (?, ?, ?, COALESCE((SELECT role FROM users WHERE telegram_id = ?), NULL),
                    COALESCE((SELECT is_active FROM users WHERE telegram_id = ?), 1),
                    COALESCE((SELECT created_at FROM users WHERE telegram_id = ?), ?), ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                updated_at=excluded.updated_at
            """,
            (telegram_id, username, full_name, telegram_id, telegram_id, telegram_id, now, now),
        )
        await self.conn.commit()
        return await self.get_user(telegram_id)

    async def get_user(self, telegram_id: int) -> UserRow:
        cur = await self.conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = await cur.fetchone()
        if not row:
            raise KeyError(f"user {telegram_id} not found")
        return UserRow(**dict(row))

    async def get_user_optional(self, telegram_id: int) -> UserRow | None:
        cur = await self.conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = await cur.fetchone()
        return UserRow(**dict(row)) if row else None

    async def list_users(self, limit: int = 200) -> list[UserRow]:
        cur = await self.conn.execute("SELECT * FROM users ORDER BY updated_at DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [UserRow(**dict(r)) for r in rows]

    async def set_user_role(self, telegram_id: int, role: str | None) -> None:
        role_norm = roles_to_storage([role]) if role else None
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE telegram_id = ?",
            (role_norm, now, telegram_id),
        )
        await self.conn.commit()

    async def set_user_roles(self, telegram_id: int, roles: list[str] | tuple[str, ...] | set[str]) -> None:
        roles_norm = roles_to_storage(roles)
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE telegram_id = ?",
            (roles_norm, now, telegram_id),
        )
        await self.conn.commit()

    async def set_user_active(self, telegram_id: int, is_active: bool) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE users SET is_active = ?, updated_at = ? WHERE telegram_id = ?",
            (1 if is_active else 0, now, telegram_id),
        )
        await self.conn.commit()

    async def find_users_by_role(self, role: str, limit: int = 50) -> list[UserRow]:
        role_norm = (role or "").strip().lower()
        if not role_norm:
            return []
        cur = await self.conn.execute(
            """
            SELECT * FROM users
            WHERE is_active = 1
              AND (',' || lower(COALESCE(role, '')) || ',') LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (f"%,{role_norm},%", limit),
        )
        rows = await cur.fetchall()
        return [UserRow(**dict(r)) for r in rows]

    async def find_user_by_username(self, username: str) -> UserRow | None:
        uname = (username or "").strip().lstrip("@").lower()
        if not uname:
            return None
        cur = await self.conn.execute(
            """
            SELECT * FROM users
            WHERE lower(COALESCE(username, '')) = ?
            ORDER BY is_active DESC, updated_at DESC
            LIMIT 1
            """,
            (uname,),
        )
        row = await cur.fetchone()
        return UserRow(**dict(row)) if row else None

    # ------------------------- settings -------------------------

    async def set_setting(self, key: str, value: str | None) -> None:
        await self.conn.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await self.conn.commit()

    async def get_setting(self, key: str) -> str | None:
        cur = await self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    # ------------------------- projects -------------------------

    async def _next_project_code(self, project_id: int) -> str:
        # Format: PRJ-2026-000123
        y = utcnow().astimezone(timezone.utc).year
        return f"PRJ-{y}-{project_id:06d}"

    async def create_project(
        self,
        title: str,
        address: str | None,
        client: str | None,
        amount: float | None,
        deadline_iso: str | None,
        status: str,
        manager_id: int | None,
        rp_id: int | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO projects(code, title, address, client, amount, deadline, status, manager_id, rp_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (None, title, address, client, amount, deadline_iso, status, manager_id, rp_id, now, now),
        )
        pid = cur.lastrowid
        code = await self._next_project_code(int(pid))
        await self.conn.execute(
            "UPDATE projects SET code = ?, updated_at = ? WHERE id = ?",
            (code, now, pid),
        )
        await self.conn.commit()
        return await self.get_project(pid)

    async def get_project(self, project_id: int) -> dict[str, Any]:
        cur = await self.conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = await cur.fetchone()
        if not row:
            raise KeyError(f"project {project_id} not found")
        return dict(row)

    async def list_projects_for_manager(self, manager_id: int, limit: int = 20) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT * FROM projects WHERE manager_id = ? ORDER BY updated_at DESC LIMIT ?",
            (manager_id, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_recent_projects(self, limit: int = 20) -> list[dict[str, Any]]:
        cur = await self.conn.execute("SELECT * FROM projects ORDER BY updated_at DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_recent_tasks(self, limit: int = 200) -> list[dict[str, Any]]:
        cur = await self.conn.execute("SELECT * FROM tasks ORDER BY updated_at DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_tasks_for_project(self, project_id: int, limit: int = 50) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE project_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (project_id, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def search_projects(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        q = f"%{query.strip()}%"
        cur = await self.conn.execute(
            """
            SELECT * FROM projects
            WHERE code LIKE ? OR title LIKE ? OR address LIKE ? OR client LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (q, q, q, q, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def update_project_status(self, project_id: int, status: str) -> dict[str, Any]:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE projects SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, project_id),
        )
        await self.conn.commit()
        return await self.get_project(project_id)

    async def set_project_amo_lead(self, project_id: int, amo_lead_id: int | None) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE projects SET amo_lead_id = ?, updated_at = ? WHERE id = ?",
            (amo_lead_id, now, project_id),
        )
        await self.conn.commit()

    async def get_project_rp_id(self, project_id: int) -> int | None:
        cur = await self.conn.execute("SELECT rp_id FROM projects WHERE id = ?", (project_id,))
        row = await cur.fetchone()
        if row and row["rp_id"]:
            return int(row["rp_id"])

        # fallback: first docs/quote request task assignee for this project
        cur = await self.conn.execute(
            """
            SELECT assigned_to FROM tasks
            WHERE project_id = ?
              AND type IN ('docs_request', 'quote_request')
              AND assigned_to IS NOT NULL
            ORDER BY id ASC
            LIMIT 1
            """,
            (project_id,),
        )
        row2 = await cur.fetchone()
        return int(row2["assigned_to"]) if row2 and row2["assigned_to"] else None

    # ------------------------- tasks -------------------------

    async def create_task(
        self,
        project_id: int | None,
        type_: str,
        status: str,
        created_by: int | None,
        assigned_to: int | None,
        due_at_iso: str | None,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        payload_json = _json_dumps(payload or {})
        cur = await self.conn.execute(
            """
            INSERT INTO tasks(project_id, type, status, created_by, assigned_to, due_at, payload_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (project_id, type_, status, created_by, assigned_to, due_at_iso, payload_json, now, now),
        )
        await self.conn.commit()
        tid = cur.lastrowid
        return await self.get_task(tid)

    async def get_task(self, task_id: int) -> dict[str, Any]:
        cur = await self.conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = await cur.fetchone()
        if not row:
            raise KeyError(f"task {task_id} not found")
        return dict(row)

    async def list_tasks_for_user(
        self,
        assigned_to: int,
        statuses: Iterable[str] = ("open", "in_progress"),
        limit: int = 30,
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [assigned_to, *statuses]
        where_type = ""
        if type_filter:
            where_type = " AND type = ?"
            params.append(type_filter)
        params.append(limit)
        cur = await self.conn.execute(
            f"""
            SELECT * FROM tasks
            WHERE assigned_to = ? AND status IN ({placeholders}) {where_type}
            ORDER BY COALESCE(due_at, created_at) ASC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def update_task_status(self, task_id: int, status: str) -> dict[str, Any]:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, task_id),
        )
        await self.conn.commit()
        return await self.get_task(task_id)

    async def mark_task_reminded_soon(self, task_id: int) -> None:
        await self.conn.execute("UPDATE tasks SET reminded_soon = 1 WHERE id = ?", (task_id,))
        await self.conn.commit()

    async def mark_task_reminded_overdue(self, task_id: int) -> None:
        await self.conn.execute("UPDATE tasks SET reminded_overdue = 1 WHERE id = ?", (task_id,))
        await self.conn.commit()

    async def list_tasks_for_reminders(self, now_iso: str) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE status IN ('open', 'in_progress')
              AND due_at IS NOT NULL
            ORDER BY due_at ASC
            """
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ------------------------- attachments -------------------------

    async def add_attachment(
        self,
        task_id: int,
        file_id: str,
        file_unique_id: str | None,
        file_type: str,
        caption: str | None,
    ) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            """
            INSERT INTO attachments(task_id, tg_file_id, tg_file_unique_id, file_type, caption, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (task_id, file_id, file_unique_id, file_type, caption, now),
        )
        await self.conn.commit()

    async def list_attachments(self, task_id: int) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT * FROM attachments WHERE task_id = ? ORDER BY id ASC", (task_id,)
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ------------------------- audit -------------------------

    async def audit(
        self,
        actor_id: int | None,
        action: str,
        entity: str,
        entity_id: str | None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            """
            INSERT INTO audit_log(actor_id, action, entity, entity_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (actor_id, action, entity, entity_id, _json_dumps(payload or {}), now),
        )
        await self.conn.commit()

    async def users_by_role(self) -> dict[str, int]:
        cur = await self.conn.execute("SELECT role FROM users")
        rows = await cur.fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            roles = parse_roles(row["role"])
            if not roles:
                counts[""] = counts.get("", 0) + 1
                continue
            for r in roles:
                counts[r] = counts.get(r, 0) + 1
        return counts

    async def count_projects(self, since_iso: str | None = None) -> int:
        if since_iso:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM projects WHERE created_at >= ?",
                (since_iso,),
            )
        else:
            cur = await self.conn.execute("SELECT COUNT(*) AS cnt FROM projects")
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def count_users(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) AS cnt FROM users")
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def count_tasks(self, since_iso: str | None = None) -> int:
        if since_iso:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM tasks WHERE created_at >= ?",
                (since_iso,),
            )
        else:
            cur = await self.conn.execute("SELECT COUNT(*) AS cnt FROM tasks")
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def tasks_by_status(self) -> dict[str, int]:
        cur = await self.conn.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM tasks
            GROUP BY status
            """
        )
        rows = await cur.fetchall()
        return {str(r["status"]): int(r["cnt"]) for r in rows}

    async def task_counts_for_user(self, user_id: int) -> dict[str, int]:
        cur = await self.conn.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM tasks
            WHERE assigned_to = ?
            GROUP BY status
            """,
            (user_id,),
        )
        rows = await cur.fetchall()
        out = {"open": 0, "in_progress": 0, "done": 0, "rejected": 0}
        for row in rows:
            status = str(row["status"] or "")
            out[status] = int(row["cnt"] or 0)
        return out

    async def usage_metrics(self, since_iso: str | None = None) -> dict[str, int]:
        if since_iso:
            cur = await self.conn.execute(
                """
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT actor_id) AS unique_users,
                    SUM(CASE WHEN action = 'command' THEN 1 ELSE 0 END) AS commands,
                    SUM(CASE WHEN action = 'menu_click' THEN 1 ELSE 0 END) AS menu_clicks,
                    SUM(CASE WHEN action = 'callback' THEN 1 ELSE 0 END) AS callbacks
                FROM audit_log
                WHERE created_at >= ?
                """,
                (since_iso,),
            )
        else:
            cur = await self.conn.execute(
                """
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT actor_id) AS unique_users,
                    SUM(CASE WHEN action = 'command' THEN 1 ELSE 0 END) AS commands,
                    SUM(CASE WHEN action = 'menu_click' THEN 1 ELSE 0 END) AS menu_clicks,
                    SUM(CASE WHEN action = 'callback' THEN 1 ELSE 0 END) AS callbacks
                FROM audit_log
                """
            )
        row = await cur.fetchone()
        if not row:
            return {
                "total_events": 0,
                "unique_users": 0,
                "commands": 0,
                "menu_clicks": 0,
                "callbacks": 0,
            }
        return {
            "total_events": int(row["total_events"] or 0),
            "unique_users": int(row["unique_users"] or 0),
            "commands": int(row["commands"] or 0),
            "menu_clicks": int(row["menu_clicks"] or 0),
            "callbacks": int(row["callbacks"] or 0),
        }

    # ------------------------- leads (amoCRM) -------------------------

    async def lead_exists(self, amo_lead_id: int) -> bool:
        cur = await self.conn.execute("SELECT 1 FROM leads WHERE amo_lead_id = ?", (amo_lead_id,))
        return (await cur.fetchone()) is not None

    async def create_lead(
        self,
        amo_lead_id: int,
        name: str | None,
        price: float | None,
        pipeline_id: int | None,
        status_id: int | None,
        responsible_user_id: int | None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO leads(amo_lead_id, name, price, pipeline_id, status_id,
                              responsible_user_id, claimed_by, claimed_at, escalated,
                              workchat_message_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL, ?, ?)
            """,
            (amo_lead_id, name, price, pipeline_id, status_id, responsible_user_id, now, now),
        )
        await self.conn.commit()
        return await self.get_lead(cur.lastrowid)

    async def get_lead(self, lead_id: int) -> dict[str, Any]:
        cur = await self.conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
        row = await cur.fetchone()
        if not row:
            raise KeyError(f"lead {lead_id} not found")
        return dict(row)

    async def get_lead_by_amo_id(self, amo_lead_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute("SELECT * FROM leads WHERE amo_lead_id = ?", (amo_lead_id,))
        row = await cur.fetchone()
        return dict(row) if row else None

    async def claim_lead(self, lead_id: int, telegram_id: int) -> bool:
        """Atomically claim a lead. Returns True if claimed, False if already taken."""
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            UPDATE leads SET claimed_by = ?, claimed_at = ?, updated_at = ?
            WHERE id = ? AND claimed_by IS NULL
            """,
            (telegram_id, now, now, lead_id),
        )
        await self.conn.commit()
        return cur.rowcount > 0

    async def assign_lead(self, lead_id: int, telegram_id: int) -> None:
        """Force-assign lead by RP/GD (overrides even if already claimed)."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET claimed_by = ?, claimed_at = ?, updated_at = ? WHERE id = ?",
            (telegram_id, now, now, lead_id),
        )
        await self.conn.commit()

    async def set_lead_workchat_msg(self, lead_id: int, message_id: int) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET workchat_message_id = ?, updated_at = ? WHERE id = ?",
            (message_id, now, lead_id),
        )
        await self.conn.commit()

    async def set_lead_escalated(self, lead_id: int) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET escalated = 1, updated_at = ? WHERE id = ?",
            (now, lead_id),
        )
        await self.conn.commit()

    async def list_unclaimed_leads(self, older_than_iso: str | None = None) -> list[dict[str, Any]]:
        """List leads that have not been claimed yet."""
        if older_than_iso:
            cur = await self.conn.execute(
                """
                SELECT * FROM leads
                WHERE claimed_by IS NULL AND created_at <= ?
                ORDER BY created_at ASC
                """,
                (older_than_iso,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM leads WHERE claimed_by IS NULL ORDER BY created_at ASC"
            )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_unescalated_leads(self, older_than_iso: str) -> list[dict[str, Any]]:
        """Unclaimed & not yet escalated leads older than given timestamp."""
        cur = await self.conn.execute(
            """
            SELECT * FROM leads
            WHERE claimed_by IS NULL AND escalated = 0 AND created_at <= ?
            ORDER BY created_at ASC
            """,
            (older_than_iso,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_lead_for_project_conversion(self, amo_lead_id: int) -> dict[str, Any] | None:
        """Get a claimed lead by amo_lead_id (for converting to project)."""
        cur = await self.conn.execute(
            "SELECT * FROM leads WHERE amo_lead_id = ? AND claimed_by IS NOT NULL",
            (amo_lead_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_unconverted_lead_for_manager(self, manager_telegram_id: int) -> dict[str, Any] | None:
        """Get the most recent claimed lead that hasn't been linked to a project yet."""
        cur = await self.conn.execute(
            """
            SELECT l.* FROM leads l
            LEFT JOIN projects p ON p.amo_lead_id = l.amo_lead_id
            WHERE l.claimed_by = ? AND p.id IS NULL
            ORDER BY l.claimed_at DESC
            LIMIT 1
            """,
            (manager_telegram_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def top_usage_entities(
        self,
        action: str,
        since_iso: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        if since_iso:
            cur = await self.conn.execute(
                """
                SELECT entity_id, COUNT(*) AS cnt
                FROM audit_log
                WHERE action = ? AND created_at >= ? AND entity_id IS NOT NULL AND entity_id != ''
                GROUP BY entity_id
                ORDER BY cnt DESC
                LIMIT ?
                """,
                (action, since_iso, limit),
            )
        else:
            cur = await self.conn.execute(
                """
                SELECT entity_id, COUNT(*) AS cnt
                FROM audit_log
                WHERE action = ? AND entity_id IS NOT NULL AND entity_id != ''
                GROUP BY entity_id
                ORDER BY cnt DESC
                LIMIT ?
                """,
                (action, limit),
            )
        rows = await cur.fetchall()
        return [{"entity_id": str(r["entity_id"]), "cnt": int(r["cnt"])} for r in rows]
