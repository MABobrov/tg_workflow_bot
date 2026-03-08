from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import aiosqlite

from .enums import Role
from .utils import parse_roles, roles_to_storage, to_iso, utcnow

log = logging.getLogger(__name__)


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

            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                sender_id INTEGER NOT NULL,
                receiver_id INTEGER,
                receiver_chat_id INTEGER,
                direction TEXT NOT NULL,
                text TEXT,
                tg_message_id INTEGER,
                forwarded_message_id INTEGER,
                has_attachment INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_chat_messages_channel ON chat_messages(channel, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_chat_messages_sender ON chat_messages(sender_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS chat_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_message_id INTEGER NOT NULL REFERENCES chat_messages(id),
                tg_file_id TEXT NOT NULL,
                tg_file_unique_id TEXT,
                file_type TEXT NOT NULL,
                caption TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_chat_attach_msg ON chat_attachments(chat_message_id);

            CREATE TABLE IF NOT EXISTS finance_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                chat_message_id INTEGER REFERENCES chat_messages(id),
                amount REAL NOT NULL,
                description TEXT,
                entered_by INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_finance_channel ON finance_entries(channel, created_at DESC);

            -- ======== НОВЫЕ ТАБЛИЦЫ (расширение на все роли) ========

            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT NOT NULL,
                project_id INTEGER REFERENCES projects(id),
                supplier TEXT,
                amount REAL,
                description TEXT,
                object_address TEXT,
                payment_deadline TEXT,

                created_by INTEGER NOT NULL,
                creator_role TEXT NOT NULL,
                assigned_to INTEGER,

                status TEXT NOT NULL DEFAULT 'new',
                is_credit INTEGER DEFAULT 0,

                installer_ok INTEGER DEFAULT 0,
                installer_ok_at TEXT,
                installer_ok_by INTEGER,

                edo_signed INTEGER DEFAULT 0,
                edo_signed_at TEXT,
                edo_task_id INTEGER,

                no_debts INTEGER DEFAULT 0,
                no_debts_at TEXT,

                close_comment TEXT,

                zp_status TEXT DEFAULT 'not_requested',
                zp_requested_at TEXT,
                zp_approved_at TEXT,

                task_id INTEGER REFERENCES tasks(id),

                payment_file_id TEXT,
                payment_comment TEXT,

                created_at TEXT NOT NULL,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_invoices_number ON invoices(invoice_number);
            CREATE INDEX IF NOT EXISTS idx_invoices_created_by ON invoices(created_by, status);
            CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status);

            CREATE TABLE IF NOT EXISTS edo_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_type TEXT NOT NULL,
                invoice_number TEXT,
                description TEXT,
                comment TEXT,

                requested_by INTEGER NOT NULL,
                requested_by_role TEXT NOT NULL,

                assigned_to INTEGER NOT NULL,
                task_id INTEGER REFERENCES tasks(id),

                status TEXT NOT NULL DEFAULT 'open',
                signed_at TEXT,

                received_at TEXT NOT NULL,
                processing_started_at TEXT,
                completed_at TEXT,
                processing_time_minutes INTEGER,

                updated_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_edo_req_status ON edo_requests(status);
            CREATE INDEX IF NOT EXISTS idx_edo_req_assigned ON edo_requests(assigned_to, status);

            CREATE TABLE IF NOT EXISTS lead_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_source TEXT,
                assigned_manager_role TEXT NOT NULL,
                assigned_manager_id INTEGER NOT NULL,

                assigned_by INTEGER NOT NULL,
                assigned_at TEXT NOT NULL,

                response_at TEXT,
                processing_time_minutes INTEGER,

                project_id INTEGER REFERENCES projects(id),
                task_id INTEGER REFERENCES tasks(id),

                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_lead_tracking_mgr ON lead_tracking(assigned_manager_id);
            """
        )
        await self.conn.commit()

        # --- Migrations: add columns if they don't exist yet ---
        migration_columns = [
            # Дополнение 1: подписание ЭДО / бумажные оригиналы при «Счёт в работу»
            ("invoices", "docs_edo_signed", "INTEGER DEFAULT 0"),
            ("invoices", "docs_paper_signed", "INTEGER DEFAULT 0"),
            ("invoices", "docs_originals_holder", "TEXT"),  # 'gd' | 'manager' | NULL
            ("invoices", "docs_originals_comment", "TEXT"),
            # Дополнение 2: оригиналы закрывающих при «Счёт End»
            ("invoices", "closing_originals_holder", "TEXT"),  # 'gd' | 'manager' | NULL
            ("invoices", "closing_originals_comment", "TEXT"),
            # Дополнение 3: Расчёт ЗП замерщика
            ("invoices", "zp_zamery_details_json", "TEXT"),  # JSON: [{address, cost}, ...]
            ("invoices", "zp_zamery_total", "REAL"),
            # EDO response columns (для complete_edo_request)
            ("edo_requests", "response_type", "TEXT"),
            ("edo_requests", "responded_by", "INTEGER"),
            ("edo_requests", "response_comment", "TEXT"),
            ("edo_requests", "response_attachments_json", "TEXT"),
            ("edo_requests", "updated_at", "TEXT"),
            # Дополнение: принятие задач и напоминания
            ("tasks", "accepted_at", "TEXT"),
            ("tasks", "last_reminded_at", "TEXT"),
            ("tasks", "reminder_2h_sent", "INTEGER DEFAULT 0"),
            # Отслеживание прочтения входящих сообщений
            ("chat_messages", "is_read", "INTEGER DEFAULT 0"),
            # --- Фаза расширения ГД: иерархия счетов, материалы, монтаж ---
            ("invoices", "parent_invoice_id", "INTEGER REFERENCES invoices(id)"),
            ("invoices", "material_type", "TEXT"),
            ("invoices", "montazh_stage", "TEXT DEFAULT 'none'"),
            ("chat_messages", "invoice_id", "INTEGER REFERENCES invoices(id)"),
            # --- ЗП менеджера (Отд.Продаж) ---
            ("invoices", "zp_manager_status", "TEXT DEFAULT 'not_requested'"),
            ("invoices", "zp_manager_amount", "REAL"),
            ("invoices", "zp_manager_requested_by", "INTEGER"),
            ("invoices", "zp_manager_requested_at", "TEXT"),
            ("invoices", "zp_manager_approved_at", "TEXT"),
            # --- ЗП монтажника ---
            ("invoices", "zp_installer_status", "TEXT DEFAULT 'not_requested'"),
            ("invoices", "zp_installer_amount", "REAL"),
            ("invoices", "zp_installer_requested_by", "INTEGER"),
            ("invoices", "zp_installer_requested_at", "TEXT"),
            ("invoices", "zp_installer_approved_at", "TEXT"),
            # --- Объединение с Отдел продаж ---
            ("invoices", "client_name", "TEXT"),
            ("invoices", "traffic_source", "TEXT"),
            ("invoices", "receipt_date", "TEXT"),
            ("invoices", "deadline_days", "INTEGER"),
            ("invoices", "actual_completion_date", "TEXT"),
            ("invoices", "first_payment_amount", "REAL"),
            ("invoices", "outstanding_debt", "REAL"),
            ("invoices", "contract_type", "TEXT"),
            ("invoices", "closing_docs_status", "TEXT"),
            ("invoices", "payment_terms", "TEXT"),
        ]
        async def _column_exists(table: str, column: str) -> bool:
            cur = await self.conn.execute(f"PRAGMA table_info({table})")
            rows = await cur.fetchall()
            return any(str(row["name"]) == column for row in rows)

        for table, col, col_type in migration_columns:
            if await _column_exists(table, col):
                continue
            await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        await self.conn.commit()

        cur = await self.conn.execute(
            """
            SELECT invoice_number
            FROM invoices
            WHERE invoice_number IS NOT NULL AND trim(invoice_number) != ''
            GROUP BY invoice_number
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )
        duplicate_invoice = await cur.fetchone()
        if duplicate_invoice:
            log.error(
                "Duplicate invoice_number detected (%s); unique index was not created. "
                "Clean duplicates to restore invoice uniqueness.",
                duplicate_invoice["invoice_number"],
            )
        else:
            await self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_invoices_number_unique ON invoices(invoice_number)"
            )
            await self.conn.commit()

        # --- Indexes for invoice hierarchy & chat-invoice linking ---
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_invoices_parent ON invoices(parent_invoice_id)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_messages_invoice ON chat_messages(invoice_id)"
        )
        await self.conn.commit()

        # --- Auto-migration: TD -> GD (role merge) ---
        await self.conn.execute(
            "UPDATE users SET role = 'gd' WHERE role = 'td'"
        )
        # Handle combined roles containing 'td'
        cur = await self.conn.execute(
            "SELECT telegram_id, role FROM users WHERE role LIKE '%td%'"
        )
        for row in await cur.fetchall():
            old_role = row["role"]
            parts = [p.strip() for p in old_role.replace(";", ",").split(",")]
            new_parts = [p for p in parts if p != "td"]
            if "gd" not in new_parts:
                new_parts.append("gd")
            new_role = ",".join(new_parts)
            if new_role != old_role:
                await self.conn.execute(
                    "UPDATE users SET role = ? WHERE telegram_id = ?",
                    (new_role, row["telegram_id"]),
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

    async def list_tasks_by_source(
        self,
        source: str,
        statuses: Iterable[str] = ("open", "in_progress"),
        created_by: int | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """List tasks by source in payload_json (e.g. 'chat_proxy:montazh')."""
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [*statuses, source]
        where_creator = ""
        if created_by is not None:
            where_creator = " AND created_by = ?"
            params.append(created_by)
        params.append(limit)
        cur = await self.conn.execute(
            f"""
            SELECT * FROM tasks
            WHERE status IN ({placeholders})
              AND json_extract(payload_json, '$.source') = ?
              {where_creator}
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

    async def close_tasks_by_invoice(self, invoice_id: int, task_type: str) -> int:
        """Set tasks matching invoice_id (in payload_json) and type to DONE. Returns count."""
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "UPDATE tasks SET status = 'done', updated_at = ? "
            "WHERE type = ? AND status IN ('open', 'in_progress') "
            "AND json_extract(payload_json, '$.invoice_id') = ?",
            (now, task_type, invoice_id),
        )
        await self.conn.commit()
        return cur.rowcount

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

    async def accept_task(self, task_id: int) -> None:
        """Mark task as accepted (user clicked 'Принято')."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE tasks SET accepted_at = ?, updated_at = ? WHERE id = ?",
            (now, now, task_id),
        )
        await self.conn.commit()

    async def mark_task_reminded_15(self, task_id: int) -> None:
        """Update last_reminded_at for 15-min acceptance reminders."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE tasks SET last_reminded_at = ? WHERE id = ?",
            (now, task_id),
        )
        await self.conn.commit()

    async def mark_task_reminded_2h(self, task_id: int) -> None:
        """Mark that the 2-hour post-acceptance reminder was sent."""
        await self.conn.execute(
            "UPDATE tasks SET reminder_2h_sent = 1 WHERE id = ?",
            (task_id,),
        )
        await self.conn.commit()

    async def count_unread_tasks(self, user_id: int) -> int:
        """Count tasks (OPEN/IN_PROGRESS) + unread incoming messages for user."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? AND status IN ('open', 'in_progress')",
            (user_id,),
        )
        row = await cur.fetchone()
        task_count = row[0] if row else 0

        cur2 = await self.conn.execute(
            "SELECT COUNT(*) FROM chat_messages "
            "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0",
            (user_id,),
        )
        row2 = await cur2.fetchone()
        msg_count = row2[0] if row2 else 0

        return task_count + msg_count

    async def count_gd_inbox_tasks(self, user_id: int) -> int:
        """Count tasks for GD inbox: OPEN/IN_PROGRESS, excluding invoice_payment, payment_confirm, invoice_end."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type NOT IN ('invoice_payment', 'payment_confirm', 'invoice_end')",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_gd_invoice_tasks(self, user_id: int) -> int:
        """Count OPEN/IN_PROGRESS invoice_payment tasks assigned to user."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type = 'invoice_payment'",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_gd_invoice_end_tasks(self, user_id: int) -> int:
        """Count OPEN/IN_PROGRESS payment_confirm + invoice_end tasks for GD."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type IN ('payment_confirm', 'invoice_end')",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_gd_supplier_pay_tasks(self, user_id: int) -> int:
        """Count pending ZP requests (zamery/manager/installer) for 'Оплата поставщику' badge."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices "
            "WHERE zp_status = 'requested' "
            "   OR zp_manager_status = 'requested' "
            "   OR zp_installer_status = 'requested'",
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_unread_by_channel(self, user_id: int) -> dict[str, int]:
        """Count unread incoming messages per channel for a user."""
        cur = await self.conn.execute(
            "SELECT channel, COUNT(*) FROM chat_messages "
            "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0 "
            "GROUP BY channel",
            (user_id,),
        )
        rows = await cur.fetchall()
        return {row[0]: row[1] for row in rows}

    # -------------------- RP role badge counters --------------------

    _RP_TASK_TYPES = (
        "check_kp",
        "invoice_payment",
        "gd_task",
        "urgent_gd",
        "not_urgent_gd",
        "lead_to_project",
        "order_materials",
        "order_profile",
        "order_glass",
        "delivery_request",
        "tinting_request",
        "issue",
    )

    _RP_CHANNELS = (
        "rp",
    )

    async def count_rp_role_tasks(self, user_id: int) -> int:
        """Count OPEN/IN_PROGRESS tasks assigned to user with RP-relevant types.

        RP task types: CHECK_KP, INVOICE_PAYMENT, GD_TASK, URGENT_GD,
        NOT_URGENT_GD, LEAD_TO_PROJECT, ORDER_MATERIALS, ORDER_PROFILE,
        ORDER_GLASS, DELIVERY_REQUEST, TINTING_REQUEST, ISSUE.

        Returns the total count (for the red-circle badge on role buttons).
        """
        placeholders = ",".join("?" for _ in self._RP_TASK_TYPES)
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks "
            "WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            f"AND type IN ({placeholders})",
            (user_id, *self._RP_TASK_TYPES),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_rp_role_messages(self, user_id: int) -> int:
        """Count unread incoming chat messages for user in RP-relevant channels.

        RP channels: 'rp' (messages directed to RP from GD and others).

        Returns the total count (for the speech-bubble badge on role buttons).
        """
        placeholders = ",".join("?" for _ in self._RP_CHANNELS)
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM chat_messages "
            "WHERE receiver_id = ? "
            "AND direction = 'incoming' "
            "AND is_read = 0 "
            f"AND channel IN ({placeholders})",
            (user_id, *self._RP_CHANNELS),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    # -------------------- CHECK_KP task helpers (Этап 5) --------------------

    async def list_check_kp_tasks(self, user_id: int, limit: int = 30) -> list[dict[str, Any]]:
        """List CHECK_KP tasks assigned to user (OPEN/IN_PROGRESS)."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE assigned_to = ? "
            "AND type = 'check_kp' "
            "AND status IN ('open', 'in_progress') "
            "ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_check_kp_tasks(self, user_id: int) -> int:
        """Count OPEN/IN_PROGRESS CHECK_KP tasks assigned to user."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND type = 'check_kp' "
            "AND status IN ('open', 'in_progress')",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def list_rp_issued_invoices(self, limit: int = 30) -> list[dict[str, Any]]:
        """List invoices reviewed/processed by RP (status NOT 'new', NOT 'rejected').

        These are the «Выставленные счета» — invoices where RP said «Да».
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE status NOT IN ('new', 'rejected') "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_credit_invoices(self, limit: int = 30) -> list[dict[str, Any]]:
        """List credit-based invoices (is_credit=1 OR status='credit')."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE is_credit = 1 OR status = 'credit' "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_credit_invoices(self) -> int:
        """Count credit-based invoices."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE is_credit = 1 OR status = 'credit'"
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def list_invoices_in_work(self, limit: int = 50) -> list[dict[str, Any]]:
        """List invoices 'in work' (pending/in_progress/paid, excluding credit).

        Used for RP «Счета в Работе» dashboard.
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid') "
            "AND (is_credit = 0 OR is_credit IS NULL) "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_invoices_in_work(self) -> int:
        """Count invoices 'in work' (pending/in_progress/paid, excluding credit)."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid') "
            "AND (is_credit = 0 OR is_credit IS NULL)"
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # Invoice hierarchy & cost statistics
    # ------------------------------------------------------------------

    async def list_invoices_for_selection(self, limit: int = 30) -> list[dict[str, Any]]:
        """Счета «в работе» + «Счёт End» для inline-пикера (NOT credit)."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid', 'ended') "
            "AND (is_credit = 0 OR is_credit IS NULL) "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_child_invoices(self, parent_invoice_id: int) -> list[dict[str, Any]]:
        """Список дочерних счетов поставщиков, привязанных к родительскому."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE parent_invoice_id = ? ORDER BY created_at DESC",
            (parent_invoice_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_invoice_cost_summary(self, parent_invoice_id: int) -> dict[str, Any]:
        """Агрегация расходов по родительскому счёту: итого, по material_type, credit/non-credit."""
        children = await self.list_child_invoices(parent_invoice_id)
        summary: dict[str, Any] = {
            "total": 0.0,
            "by_material": {},
            "credit_total": 0.0,
            "credit_by_material": {},
            "non_credit_total": 0.0,
            "non_credit_by_material": {},
            "count": len(children),
        }
        for ch in children:
            amt = float(ch.get("amount") or 0)
            mat = ch.get("material_type") or "other"
            is_credit = bool(ch.get("is_credit"))

            summary["total"] += amt
            summary["by_material"][mat] = summary["by_material"].get(mat, 0.0) + amt

            if is_credit:
                summary["credit_total"] += amt
                summary["credit_by_material"][mat] = summary["credit_by_material"].get(mat, 0.0) + amt
            else:
                summary["non_credit_total"] += amt
                summary["non_credit_by_material"][mat] = summary["non_credit_by_material"].get(mat, 0.0) + amt

        return summary

    async def list_supplier_payments_for_invoice(
        self, invoice_id: int,
    ) -> list[dict[str, Any]]:
        """SUPPLIER_PAYMENT tasks, привязанные к parent_invoice_id в payload."""
        rows = await self.search_tasks_by_payload(
            field="parent_invoice_id",
            value=str(invoice_id),
            type_filter=["supplier_payment"],
            limit=50,
        )
        result: list[dict[str, Any]] = []
        for r in rows:
            if r.get("status") != "done":
                continue
            payload = json.loads(r.get("payload_json") or "{}")
            # Точная проверка parent_invoice_id (LIKE может дать ложные совпадения)
            if payload.get("parent_invoice_id") != invoice_id:
                continue
            result.append({
                "supplier": payload.get("supplier", ""),
                "amount": float(payload.get("amount") or 0),
                "material_type": payload.get("material_type", ""),
                "invoice_number": payload.get("invoice_number", ""),
                "task_id": r["id"],
            })
        return result

    async def get_full_invoice_cost_card(self, invoice_id: int) -> dict[str, Any]:
        """
        Полная карточка себестоимости по родительскому счёту.
        Агрегирует:
          1) Дочерние счета (по material_type)
          2) Оплаты поставщикам (SUPPLIER_PAYMENT tasks)
          3) ЗП Замерщик / Менеджер / Монтажник
        """
        inv = await self.get_invoice(invoice_id)
        if not inv:
            return {
                "invoice_amount": 0, "materials_total": 0, "materials_by_type": {},
                "supplier_payments_total": 0, "supplier_payments_list": [],
                "zp_zamery": 0, "zp_manager": 0, "zp_installer": 0, "zp_total": 0,
                "total_cost": 0, "margin": 0, "margin_pct": 0,
            }

        invoice_amount = float(inv.get("amount") or 0)

        # 1) Дочерние счета (материалы)
        mat_summary = await self.get_invoice_cost_summary(invoice_id)
        materials_total = mat_summary["total"]
        materials_by_type = mat_summary["by_material"]

        # 2) Оплаты поставщикам
        sp_list = await self.list_supplier_payments_for_invoice(invoice_id)
        supplier_payments_total = sum(s["amount"] for s in sp_list)

        # 3) ЗП (только approved)
        zp_zamery = float(inv.get("zp_zamery_total") or 0) if inv.get("zp_status") == "approved" else 0.0
        zp_manager = float(inv.get("zp_manager_amount") or 0) if inv.get("zp_manager_status") == "approved" else 0.0
        zp_installer = float(inv.get("zp_installer_amount") or 0) if inv.get("zp_installer_status") == "approved" else 0.0
        zp_total = zp_zamery + zp_manager + zp_installer

        total_cost = materials_total + supplier_payments_total + zp_total
        margin = invoice_amount - total_cost
        margin_pct = (margin / invoice_amount * 100) if invoice_amount > 0 else 0.0

        return {
            "invoice_amount": invoice_amount,
            "materials_total": materials_total,
            "materials_by_type": materials_by_type,
            "supplier_payments_total": supplier_payments_total,
            "supplier_payments_list": sp_list,
            "zp_zamery": zp_zamery,
            "zp_manager": zp_manager,
            "zp_installer": zp_installer,
            "zp_total": zp_total,
            "total_cost": total_cost,
            "margin": margin,
            "margin_pct": margin_pct,
        }

    async def list_invoices_for_installer(self, user_id: int) -> list[dict[str, Any]]:
        """Счета, назначенные монтажнику (assigned_to = user_id), в работе."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE assigned_to = ? "
            "AND status IN ('in_progress', 'paid') "
            "AND parent_invoice_id IS NULL "
            "ORDER BY created_at DESC LIMIT 15",
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_chat_messages_by_invoice(
        self, invoice_id: int, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Все сообщения из ВСЕХ каналов, привязанные к конкретному счёту."""
        cur = await self.conn.execute(
            "SELECT * FROM chat_messages WHERE invoice_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (invoice_id, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_tasks_by_invoice(
        self, invoice_id: int, limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Все задачи, привязанные к счёту через payload_json (invoice_id или parent_invoice_id)."""
        inv_str = str(invoice_id)
        cur = await self.conn.execute(
            "SELECT * FROM tasks "
            "WHERE ("
            "  json_extract(payload_json, '$.invoice_id') = ? "
            "  OR json_extract(payload_json, '$.parent_invoice_id') = ? "
            "  OR json_extract(payload_json, '$.linked_invoice_id') = ? "
            ") "
            "ORDER BY created_at DESC LIMIT ?",
            (inv_str, inv_str, inv_str, limit),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            # Fallback: json_extract may return int — try with int
            cur2 = await self.conn.execute(
                "SELECT * FROM tasks "
                "WHERE ("
                "  json_extract(payload_json, '$.invoice_id') = ? "
                "  OR json_extract(payload_json, '$.parent_invoice_id') = ? "
                "  OR json_extract(payload_json, '$.linked_invoice_id') = ? "
                ") "
                "ORDER BY created_at DESC LIMIT ?",
                (invoice_id, invoice_id, invoice_id, limit),
            )
            rows = [dict(r) for r in await cur2.fetchall()]
        return rows

    async def update_montazh_stage(self, invoice_id: int, stage: str) -> None:
        """Обновить этап монтажа по счёту."""
        await self.conn.execute(
            "UPDATE invoices SET montazh_stage = ?, updated_at = ? WHERE id = ?",
            (stage, to_iso(utcnow()), invoice_id),
        )
        await self.conn.commit()

    async def list_ended_invoices(
        self,
        month_start: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List ENDED invoices. If month_start given, filter by updated_at >= month_start."""
        if month_start:
            cur = await self.conn.execute(
                "SELECT * FROM invoices "
                "WHERE status = 'ended' AND updated_at >= ? "
                "ORDER BY updated_at DESC LIMIT ?",
                (month_start, limit),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM invoices "
                "WHERE status = 'ended' "
                "ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_ended_invoices(self, month_start: str | None = None) -> int:
        """Count ENDED invoices. If month_start given, count only current month."""
        if month_start:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices "
                "WHERE status = 'ended' AND updated_at >= ?",
                (month_start,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices WHERE status = 'ended'"
            )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def mark_messages_read(self, user_id: int, channel: str) -> int:
        """Mark all incoming messages for user in channel as read. Returns count."""
        cur = await self.conn.execute(
            "UPDATE chat_messages SET is_read = 1 "
            "WHERE receiver_id = ? AND channel = ? AND direction = 'incoming' AND is_read = 0",
            (user_id, channel),
        )
        await self.conn.commit()
        return cur.rowcount

    async def list_tasks_needing_15m_reminder(self, cutoff_iso: str) -> list[dict]:
        """Tasks: OPEN, not accepted, last reminder > 15 min ago (or never reminded)."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE status = 'open' AND accepted_at IS NULL "
            "AND (last_reminded_at IS NULL OR last_reminded_at <= ?) "
            "AND created_at <= ?",
            (cutoff_iso, cutoff_iso),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_tasks_needing_2h_reminder(self, cutoff_iso: str) -> list[dict]:
        """Tasks: accepted, reminder_2h_sent=0, accepted_at > 2h ago."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE status IN ('open', 'in_progress') "
            "AND accepted_at IS NOT NULL AND accepted_at <= ? "
            "AND (reminder_2h_sent IS NULL OR reminder_2h_sent = 0)",
            (cutoff_iso,),
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

    # ------------------------- chat proxy -------------------------

    async def save_chat_message(
        self,
        channel: str,
        sender_id: int,
        direction: str,
        text: str | None = None,
        receiver_id: int | None = None,
        receiver_chat_id: int | None = None,
        tg_message_id: int | None = None,
        forwarded_message_id: int | None = None,
        has_attachment: bool = False,
        invoice_id: int | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO chat_messages
                (channel, sender_id, receiver_id, receiver_chat_id, direction, text,
                 tg_message_id, forwarded_message_id, has_attachment, invoice_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                channel,
                sender_id,
                receiver_id,
                receiver_chat_id,
                direction,
                text,
                tg_message_id,
                forwarded_message_id,
                int(has_attachment),
                invoice_id,
                now,
            ),
        )
        await self.conn.commit()
        row_id = cur.lastrowid
        cur2 = await self.conn.execute("SELECT * FROM chat_messages WHERE id = ?", (row_id,))
        row = await cur2.fetchone()
        return dict(row)

    async def list_chat_messages(self, channel: str, limit: int = 20) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            """
            SELECT * FROM chat_messages
            WHERE channel = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (channel, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in reversed(rows)]

    async def save_chat_attachment(
        self,
        chat_message_id: int,
        tg_file_id: str,
        file_type: str,
        tg_file_unique_id: str | None = None,
        caption: str | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO chat_attachments
                (chat_message_id, tg_file_id, tg_file_unique_id, file_type, caption, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_message_id, tg_file_id, tg_file_unique_id, file_type, caption, now),
        )
        await self.conn.commit()
        row_id = cur.lastrowid
        cur2 = await self.conn.execute("SELECT * FROM chat_attachments WHERE id = ?", (row_id,))
        row = await cur2.fetchone()
        return dict(row)

    async def list_chat_attachments(self, chat_message_id: int) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT * FROM chat_attachments WHERE chat_message_id = ? ORDER BY id",
            (chat_message_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ------------------------- finance entries -------------------------

    async def save_finance_entry(
        self,
        channel: str,
        amount: float,
        entered_by: int,
        chat_message_id: int | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO finance_entries (channel, chat_message_id, amount, description, entered_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (channel, chat_message_id, amount, description, entered_by, now),
        )
        await self.conn.commit()
        row_id = cur.lastrowid
        cur2 = await self.conn.execute("SELECT * FROM finance_entries WHERE id = ?", (row_id,))
        row = await cur2.fetchone()
        return dict(row)

    async def get_finance_summary(self, channel: str) -> dict[str, Any]:
        """Return total balance and last entries for a channel."""
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) as total FROM finance_entries WHERE channel = ?",
            (channel,),
        )
        row = await cur.fetchone()
        total = row["total"] if row else 0.0

        cur2 = await self.conn.execute(
            """
            SELECT * FROM finance_entries
            WHERE channel = ?
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (channel,),
        )
        rows = await cur2.fetchall()
        entries = [dict(r) for r in rows]

        return {"total": total, "entries": entries}

    # ------------------------- invoice search -------------------------

    async def search_tasks_by_payload(
        self,
        field: str,
        value: str,
        type_filter: list[str] | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search tasks by a field inside payload_json (using LIKE)."""
        types = type_filter or []
        if types:
            placeholders = ",".join("?" for _ in types)
            type_clause = f" AND type IN ({placeholders})"
        else:
            type_clause = ""

        # Use JSON extract or LIKE on payload_json
        like_pattern = f'%"{field}":%{value}%'
        params: list[Any] = [like_pattern, *types, limit]

        cur = await self.conn.execute(
            f"""
            SELECT * FROM tasks
            WHERE payload_json LIKE ? {type_clause}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # =====================================================================
    # INVOICES
    # =====================================================================

    async def create_invoice(
        self,
        invoice_number: str,
        project_id: int | None,
        created_by: int,
        creator_role: str,
        object_address: str = "",
        amount: float = 0.0,
        supplier: str | None = None,
        description: str | None = None,
        assigned_to: int | None = None,
        payment_deadline: str | None = None,
    ) -> int:
        """Create a new invoice record (status = NEW)."""
        invoice_number_normalized = (invoice_number or "").strip()
        if not invoice_number_normalized:
            raise ValueError("invoice_number is required")
        existing = await self.get_invoice_by_number(invoice_number_normalized)
        if existing:
            raise ValueError(f"invoice_number '{invoice_number_normalized}' already exists")

        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO invoices
                (invoice_number, project_id, created_by, creator_role,
                 object_address, amount, supplier, description,
                 assigned_to, payment_deadline, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
            """,
            (invoice_number_normalized, project_id, created_by, creator_role,
             object_address, amount, supplier, description,
             assigned_to, payment_deadline, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def import_invoice_from_sheet(
        self,
        invoice_number: str,
        created_by: int,
        creator_role: str,
        status: str,
        *,
        object_address: str = "",
        amount: float = 0.0,
        is_credit: bool = False,
        client_name: str | None = None,
        traffic_source: str | None = None,
        receipt_date: str | None = None,
        deadline_days: int | None = None,
        actual_completion_date: str | None = None,
        first_payment_amount: float | None = None,
        outstanding_debt: float | None = None,
        contract_type: str | None = None,
        closing_docs_status: str | None = None,
        payment_terms: str | None = None,
        description: str | None = None,
    ) -> int:
        """Import invoice from external sheet (arbitrary status, new fields)."""
        inv_num = (invoice_number or "").strip()
        if not inv_num:
            raise ValueError("invoice_number is required")
        existing = await self.get_invoice_by_number(inv_num)
        if existing:
            # update new fields only
            updates: dict[str, Any] = {}
            if client_name:
                updates["client_name"] = client_name
            if traffic_source:
                updates["traffic_source"] = traffic_source
            if receipt_date:
                updates["receipt_date"] = receipt_date
            if deadline_days is not None:
                updates["deadline_days"] = deadline_days
            if actual_completion_date:
                updates["actual_completion_date"] = actual_completion_date
            if first_payment_amount is not None:
                updates["first_payment_amount"] = first_payment_amount
            if outstanding_debt is not None:
                updates["outstanding_debt"] = outstanding_debt
            if contract_type:
                updates["contract_type"] = contract_type
            if closing_docs_status:
                updates["closing_docs_status"] = closing_docs_status
            if payment_terms:
                updates["payment_terms"] = payment_terms
            if updates:
                sets = ", ".join(f"{k} = ?" for k in updates)
                vals = list(updates.values())
                vals.append(existing["id"])
                await self.conn.execute(
                    f"UPDATE invoices SET {sets} WHERE id = ?", vals,
                )
                await self.conn.commit()
            return int(existing["id"])

        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO invoices
                (invoice_number, created_by, creator_role, status, is_credit,
                 object_address, amount, description,
                 client_name, traffic_source, receipt_date, deadline_days,
                 actual_completion_date, first_payment_amount,
                 outstanding_debt, contract_type, closing_docs_status,
                 payment_terms, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (inv_num, created_by, creator_role, status, int(is_credit),
             object_address, amount, description,
             client_name, traffic_source, receipt_date, deadline_days,
             actual_completion_date, first_payment_amount,
             outstanding_debt, contract_type, closing_docs_status,
             payment_terms, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE id = ?", (invoice_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_invoice_by_number(self, invoice_number: str) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            """
            SELECT * FROM invoices
            WHERE invoice_number = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ((invoice_number or "").strip(),),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def list_invoices(
        self,
        created_by: int | None = None,
        assigned_to: int | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if created_by is not None:
            clauses.append("created_by = ?")
            params.append(created_by)
        if assigned_to is not None:
            clauses.append("assigned_to = ?")
            params.append(assigned_to)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM invoices {where} ORDER BY created_at DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def update_invoice_status(
        self, invoice_id: int, new_status: str
    ) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE invoices SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now, invoice_id),
        )
        await self.conn.commit()

    async def update_invoice(
        self, invoice_id: int, **fields: Any
    ) -> None:
        """Generic update: pass column=value pairs."""
        if not fields:
            return
        fields["updated_at"] = to_iso(utcnow())
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [invoice_id]
        await self.conn.execute(
            f"UPDATE invoices SET {set_clause} WHERE id = ?",
            tuple(vals),
        )
        await self.conn.commit()

    async def set_invoice_installer_ok(
        self, invoice_id: int, ok: bool = True
    ) -> None:
        fields: dict[str, Any] = {"installer_ok": int(ok)}
        if ok:
            fields["installer_ok_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)

    async def set_invoice_edo_signed(
        self, invoice_id: int, signed: bool = True
    ) -> None:
        fields: dict[str, Any] = {"edo_signed": int(signed)}
        if signed:
            fields["edo_signed_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)

    async def set_invoice_no_debts(
        self, invoice_id: int, no_debts: bool = True
    ) -> None:
        fields: dict[str, Any] = {"no_debts": int(no_debts)}
        if no_debts:
            fields["no_debts_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)

    async def set_invoice_zp_status(
        self, invoice_id: int, zp_status: str
    ) -> None:
        await self.update_invoice(invoice_id, zp_status=zp_status)

    async def set_invoice_zp_manager_status(
        self,
        invoice_id: int,
        status: str,
        amount: float | None = None,
        requested_by: int | None = None,
    ) -> None:
        """Update manager (Отд.Продаж) ZP status on invoice."""
        fields: dict[str, Any] = {"zp_manager_status": status}
        if amount is not None:
            fields["zp_manager_amount"] = amount
        if requested_by is not None:
            fields["zp_manager_requested_by"] = requested_by
        if status == "requested":
            fields["zp_manager_requested_at"] = to_iso(utcnow())
        elif status == "approved":
            fields["zp_manager_approved_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)

    async def set_invoice_zp_installer_status(
        self,
        invoice_id: int,
        status: str,
        amount: float | None = None,
        requested_by: int | None = None,
    ) -> None:
        """Update installer ZP status on invoice."""
        fields: dict[str, Any] = {"zp_installer_status": status}
        if amount is not None:
            fields["zp_installer_amount"] = amount
        if requested_by is not None:
            fields["zp_installer_requested_by"] = requested_by
        if status == "requested":
            fields["zp_installer_requested_at"] = to_iso(utcnow())
        elif status == "approved":
            fields["zp_installer_approved_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)

    async def list_pending_zp_requests(
        self, zp_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Return invoices with pending ZP requests.

        zp_type: 'zamery' | 'manager' | 'installer' | None (all).
        """
        conditions = {
            "zamery": "zp_status = 'requested'",
            "manager": "zp_manager_status = 'requested'",
            "installer": "zp_installer_status = 'requested'",
        }
        if zp_type and zp_type in conditions:
            where = conditions[zp_type]
        else:
            where = " OR ".join(conditions.values())
        cur = await self.conn.execute(
            f"SELECT * FROM invoices WHERE {where} ORDER BY id DESC",
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def check_close_conditions(self, invoice_id: int) -> dict[str, bool]:
        """Return dict with close-condition flags."""
        inv = await self.get_invoice(invoice_id)
        if not inv:
            return {
                "installer_ok": False,
                "edo_signed": False,
                "no_debts": False,
                "zp_approved": False,
            }
        return {
            "installer_ok": bool(inv.get("installer_ok")),
            "edo_signed": bool(inv.get("edo_signed")),
            "no_debts": bool(inv.get("no_debts")),
            "zp_approved": inv.get("zp_status") == "approved",
        }

    async def search_invoices(
        self, query: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Search invoices by number or address (LIKE)."""
        pattern = f"%{query}%"
        cur = await self.conn.execute(
            """
            SELECT * FROM invoices
            WHERE invoice_number LIKE ? OR object_address LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (pattern, pattern, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # =====================================================================
    # EDO REQUESTS
    # =====================================================================

    async def create_edo_request(
        self,
        request_type: str,
        requested_by: int,
        requested_by_role: str,
        assigned_to: int,
        invoice_number: str | None = None,
        description: str | None = None,
        comment: str | None = None,
        task_id: int | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO edo_requests
                (request_type, requested_by, requested_by_role, assigned_to,
                 invoice_number, description, comment, task_id,
                 status, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (request_type, requested_by, requested_by_role, assigned_to,
             invoice_number, description, comment, task_id,
             now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_edo_request(self, edo_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM edo_requests WHERE id = ?", (edo_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def list_edo_requests(
        self,
        requested_by: int | None = None,
        assigned_to: int | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if requested_by is not None:
            clauses.append("requested_by = ?")
            params.append(requested_by)
        if assigned_to is not None:
            clauses.append("assigned_to = ?")
            params.append(assigned_to)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM edo_requests {where} ORDER BY created_at DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_edo_requests_by_user(self, user_id: int) -> dict[str, int]:
        """Count EDO requests created by user, grouped by status (open/done)."""
        cur = await self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM edo_requests "
            "WHERE requested_by = ? GROUP BY status",
            (user_id,),
        )
        rows = await cur.fetchall()
        result = {"open": 0, "done": 0}
        for row in rows:
            s = row["status"]
            if s in result:
                result[s] = row["cnt"]
            else:
                result[s] = row["cnt"]
        return result

    async def update_edo_request(
        self, edo_id: int, **fields: Any
    ) -> None:
        if not fields:
            return
        fields["updated_at"] = to_iso(utcnow())
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [edo_id]
        await self.conn.execute(
            f"UPDATE edo_requests SET {set_clause} WHERE id = ?",
            tuple(vals),
        )
        await self.conn.commit()

    async def complete_edo_request(
        self,
        edo_id: int,
        response_type: str,
        responder_id: int,
        response_comment: str | None = None,
        response_attachments_json: str | None = None,
    ) -> None:
        now = to_iso(utcnow())
        # Use generic update — response columns added via migration
        await self.update_edo_request(
            edo_id,
            status="done",
            response_type=response_type,
            responded_by=responder_id,
            response_comment=response_comment,
            response_attachments_json=response_attachments_json,
            completed_at=now,
        )

    # =====================================================================
    # LEAD TRACKING
    # =====================================================================

    async def create_lead_tracking(
        self,
        assigned_by: int,
        assigned_manager_id: int,
        assigned_manager_role: str,
        lead_source: str | None = None,
        task_id: int | None = None,
        project_id: int | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO lead_tracking
                (assigned_by, assigned_manager_id, assigned_manager_role,
                 lead_source, task_id, project_id, assigned_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (assigned_by, assigned_manager_id, assigned_manager_role,
             lead_source, task_id, project_id, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def update_lead_tracking_response(
        self, lead_id: int
    ) -> None:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "SELECT assigned_at, response_at FROM lead_tracking WHERE id = ?",
            (lead_id,),
        )
        row = await cur.fetchone()
        if not row or row["response_at"]:
            return

        processing_time_minutes: int | None = None
        assigned_at = row["assigned_at"]
        if assigned_at:
            try:
                assigned_dt = datetime.fromisoformat(str(assigned_at))
                processing_time_minutes = max(
                    0,
                    int((utcnow() - assigned_dt).total_seconds() // 60),
                )
            except ValueError:
                processing_time_minutes = None

        await self.conn.execute(
            "UPDATE lead_tracking SET response_at = ?, processing_time_minutes = ? WHERE id = ?",
            (now, processing_time_minutes, lead_id),
        )
        await self.conn.commit()

    async def link_lead_tracking(
        self,
        lead_id: int,
        *,
        task_id: int | None = None,
        project_id: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {}
        if task_id is not None:
            fields["task_id"] = task_id
        if project_id is not None:
            fields["project_id"] = project_id
        if not fields:
            return

        set_clause = ", ".join(f"{key} = ?" for key in fields)
        values = [*fields.values(), lead_id]
        await self.conn.execute(
            f"UPDATE lead_tracking SET {set_clause} WHERE id = ?",
            tuple(values),
        )
        await self.conn.commit()

    async def list_leads(
        self,
        assigned_manager_id: int | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if assigned_manager_id is not None:
            clauses.append("assigned_manager_id = ?")
            params.append(assigned_manager_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM lead_tracking {where} ORDER BY assigned_at DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_lead_stats(self) -> dict[str, Any]:
        """Get lead conversion statistics grouped by manager and source."""
        # By manager role
        cur = await self.conn.execute(
            "SELECT assigned_manager_role, COUNT(*) as total, "
            "AVG(processing_time_minutes) as avg_time "
            "FROM lead_tracking GROUP BY assigned_manager_role"
        )
        by_manager = [dict(r) for r in await cur.fetchall()]

        # By source
        cur = await self.conn.execute(
            "SELECT lead_source, COUNT(*) as total "
            "FROM lead_tracking GROUP BY lead_source ORDER BY total DESC LIMIT 10"
        )
        by_source = [dict(r) for r in await cur.fetchall()]

        # Total count
        cur = await self.conn.execute("SELECT COUNT(*) FROM lead_tracking")
        total = (await cur.fetchone())[0]

        # Responded count
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM lead_tracking WHERE response_at IS NOT NULL"
        )
        responded = (await cur.fetchone())[0]

        return {
            "total": total,
            "responded": responded,
            "by_manager": by_manager,
            "by_source": by_source,
        }

    async def count_leads_total(self) -> int:
        """Count all leads."""
        cur = await self.conn.execute("SELECT COUNT(*) FROM lead_tracking")
        row = await cur.fetchone()
        return row[0] if row else 0

    # =====================================================================
    # ROLE SWITCHING (РП ↔ НПН)
    # =====================================================================

    async def switch_user_role(
        self, telegram_id: int, new_role: str
    ) -> None:
        """Switch active RP/NPN role without dropping unrelated roles."""
        user = await self.get_user_optional(telegram_id)
        if not user:
            return

        roles = parse_roles(user.role)
        preserved_roles = [
            role
            for role in roles
            if role not in {Role.RP, Role.MANAGER_NPN}
        ]
        preserved_roles.append(new_role)
        role_value = roles_to_storage(preserved_roles)
        await self.conn.execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE telegram_id = ?",
            (role_value, to_iso(utcnow()), telegram_id),
        )
        await self.conn.commit()
