from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import aiosqlite

from .enums import InvoiceStatus, Role
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
    zp_init_done: int = 0
    razmery_init_done: int = 0


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

            -- finance_entries: общий финансовый журнал по каналам (channel-level).
            -- НЕ привязан к конкретному счёту. Для расходов по кредитным счетам используй credit_expenses.
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
                client_contact TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_invoices_is_credit ON invoices(is_credit);

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
            CREATE INDEX IF NOT EXISTS idx_edo_req_invoice ON edo_requests(invoice_id);

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

            CREATE TABLE IF NOT EXISTS zamery_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                lead_id INTEGER REFERENCES lead_tracking(id),
                lead_task_id INTEGER REFERENCES tasks(id),
                address TEXT NOT NULL,
                description TEXT,
                client_contact TEXT,
                attachments_json TEXT,
                requested_by INTEGER NOT NULL,
                requester_role TEXT NOT NULL,
                assigned_to INTEGER,
                task_id INTEGER REFERENCES tasks(id),
                status TEXT NOT NULL DEFAULT 'open',
                response_comment TEXT,
                responded_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_zamery_req_by ON zamery_requests(requested_by);
            CREATE INDEX IF NOT EXISTS idx_zamery_req_to ON zamery_requests(assigned_to, status);

            CREATE TABLE IF NOT EXISTS zamery_blackout_dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                blackout_date TEXT NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_zam_blackout_user
                ON zamery_blackout_dates(user_id, blackout_date);

            CREATE TABLE IF NOT EXISTS razmery_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id INTEGER NOT NULL,
                installer_id INTEGER NOT NULL,
                installer_comment TEXT,
                rp_id INTEGER,
                rp_comment TEXT,
                rp_sent_at TEXT,
                result TEXT,
                result_comment TEXT,
                result_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_razmery_req_inv ON razmery_requests(invoice_id, status);
            CREATE INDEX IF NOT EXISTS idx_razmery_req_inst ON razmery_requests(installer_id, status);

            -- credit_expenses: расходы по конкретному кредитному счёту (invoice_id).
            -- Авто-запись из каналов через _auto_credit_expense(). НЕ дублирует finance_entries.
            CREATE TABLE IF NOT EXISTS credit_expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id INTEGER NOT NULL REFERENCES invoices(id),
                amount REAL NOT NULL,
                description TEXT,
                entered_by INTEGER NOT NULL,
                chat_message_id INTEGER REFERENCES chat_messages(id),
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_credit_exp_inv ON credit_expenses(invoice_id);

            CREATE TABLE IF NOT EXISTS supplier_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_invoice_id INTEGER NOT NULL REFERENCES invoices(id),
                invoice_number TEXT,
                amount REAL NOT NULL DEFAULT 0,
                material_type TEXT NOT NULL DEFAULT 'extra_mat',
                supplier TEXT,
                task_id INTEGER,
                created_by INTEGER,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sp_parent ON supplier_payments(parent_invoice_id);
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
            ("invoices", "zp_installer_payment_file_id", "TEXT"),
            ("invoices", "zp_installer_payment_sent_at", "TEXT"),
            ("invoices", "zp_installer_confirmed_at", "TEXT"),
            # --- Объединение с Отдел продаж ---
            ("invoices", "client_contact", "TEXT"),
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
            # --- Расчётные данные менеджера (План/Факт) ---
            ("invoices", "estimated_materials", "REAL"),  # legacy, заменено на glass+profile
            ("invoices", "estimated_installation", "REAL"),
            ("invoices", "estimated_loaders", "REAL"),
            ("invoices", "estimated_logistics", "REAL"),
            ("invoices", "client_source", "TEXT"),  # 'own' | 'gd_lead'
            ("invoices", "estimated_glass", "REAL"),    # стекло (возвратный НДС)
            ("invoices", "estimated_profile", "REAL"),   # ал. профиль (возвратный НДС)
            # --- ЭДО: привязка к счёту ---
            ("edo_requests", "invoice_id", "INTEGER"),
            # --- Площадь (м²) для монтажника ---
            ("invoices", "area_m2", "REAL"),
            # --- Расширенные финансовые поля (из бланка) ---
            ("invoices", "client_type", "INTEGER"),            # Свой=1 / Атмосфера=2
            ("invoices", "deadline_end_date", "TEXT"),          # Дата окончания сроков
            ("invoices", "nds_amount", "REAL"),                 # НДС
            ("invoices", "profit_tax", "REAL"),                 # Налог на прибыль
            ("invoices", "rentability_calc", "REAL"),           # Рентабельность расч. %
            ("invoices", "surcharge_amount", "REAL"),           # Сумма доплаты
            ("invoices", "surcharge_date", "TEXT"),             # Дата ПП по доплате
            ("invoices", "final_surcharge_amount", "REAL"),     # Сумма окончательной доплаты
            ("invoices", "final_surcharge_date", "TEXT"),       # Дата ПП окончательной доплаты
            ("invoices", "contract_signed", "TEXT"),            # Подписан Договор: Эдо/Ориг/Нет
            ("invoices", "agent_fee", "REAL"),                  # Агентское вознаграждение
            ("invoices", "manager_zp_blank", "REAL"),           # Менеджер ЗП по бланку
            ("invoices", "npn_amount", "REAL"),                 # НПН с 10% налог
            ("invoices", "materials_fact_op", "REAL"),            # Материалы Факт из ОП (колонка AL)
            ("invoices", "montazh_fact_op", "REAL"),             # Монтаж Факт из ОП (колонка AM)
            # --- ЗП менеджера: выплаты из ОП ---
            ("invoices", "zp_manager_request_text", "TEXT"),    # AH: Запрос суммы на выплату тех
            ("invoices", "zp_manager_request_amount", "REAL"),  # AI: Запрос суммы на выплату (НОВЫЙ)
            ("invoices", "zp_manager_payout", "REAL"),          # AJ: Выплата. Мен. ЗП
            ("invoices", "zp_manager_payout_date", "TEXT"),     # AJ: Дата выпл. мен.
            # --- Факт данные из ОП ---
            ("invoices", "logistics_fact_op", "REAL"),          # AN: Логистика факт
            ("invoices", "logistics_fact_date", "TEXT"),        # AO: Дата лог.
            ("invoices", "loaders_fact_op", "REAL"),            # AP: Грузчики факт
            ("invoices", "loaders_fact_date", "TEXT"),          # AQ: Дата груз.
            # --- Новые поля из ОП (Импорт ОП) ---
            ("invoices", "zamery_info_op", "TEXT"),             # I: Замеры (из ОП)
            ("invoices", "agent_payout_op", "REAL"),            # AE: Выпл. Агент.
            ("invoices", "men_zp_payout_op", "REAL"),           # AF: Выпл.МенЗП
            ("invoices", "npn_request_op", "TEXT"),             # AS: Запрос НПН
            ("invoices", "npn_payout_op", "REAL"),              # AU: Выдано НПН (сумма)
            ("invoices", "npn_payout_date_op", "TEXT"),         # AV: Дата НПН
            ("invoices", "taxes_fact_op", "REAL"),              # AX: Налоги факт
            # --- Монтажник: инициализация ЗП и отслеживание материалов ---
            ("invoices", "materials_ordered", "INTEGER DEFAULT 0"),
            ("users", "zp_init_done", "INTEGER DEFAULT 0"),
            ("users", "razmery_init_done", "INTEGER DEFAULT 0"),
            # --- Замеры: расширенная карточка ---
            ("zamery_requests", "mkad_km", "REAL"),
            ("zamery_requests", "volume_m2", "REAL"),
            ("zamery_requests", "base_cost", "INTEGER"),
            ("zamery_requests", "mkad_surcharge", "INTEGER"),
            ("zamery_requests", "total_cost", "INTEGER"),
            # --- Замеры: полный цикл (принятие + завершение) ---
            ("zamery_requests", "scheduled_date", "TEXT"),
            ("zamery_requests", "scheduled_time_interval", "TEXT"),
            ("zamery_requests", "accept_comment", "TEXT"),
            ("zamery_requests", "accepted_at", "TEXT"),
            ("zamery_requests", "completion_comment", "TEXT"),
            ("zamery_requests", "completion_attachments_json", "TEXT"),
            ("zamery_requests", "completed_at", "TEXT"),
            # --- Фактическая стоимость доставки ---
            ("invoices", "actual_logistics", "REAL"),
            # --- Финансовые данные из ОП (S, T, U, W) ---
            ("invoices", "profit_tax_op", "REAL"),            # S: Налог на приб.
            ("invoices", "rp_10_pct_op", "REAL"),             # T: РП - 10%
            ("invoices", "profit_calc_op", "REAL"),            # U: Прибыль расч
            ("invoices", "rentability_fact_op", "REAL"),       # W: Рент-ть факт
            # --- Фактическая прибыль из ОП ---
            ("invoices", "profit_fact_credit_op", "REAL"),   # AY: Прибыль факт (кредитные)
            ("invoices", "profit_fact_op", "REAL"),           # AZ: Прибыль факт (по счёту)
            # --- Подтверждение оплаты ГД ---
            ("invoices", "payment_confirm_status", "TEXT DEFAULT ''"),
            # --- Лид/Счёт по менеджерам (данные из flow лида) ---
            ("invoices", "lead_kv_num", "TEXT"),
            ("invoices", "lead_kv_name", "TEXT"),
            ("invoices", "lead_kv_phone", "TEXT"),
            ("invoices", "lead_kv_city", "TEXT"),
            ("invoices", "lead_kv_date", "TEXT"),
            ("invoices", "lead_kia_num", "TEXT"),
            ("invoices", "lead_kia_name", "TEXT"),
            ("invoices", "lead_kia_phone", "TEXT"),
            ("invoices", "lead_kia_city", "TEXT"),
            ("invoices", "lead_kia_date", "TEXT"),
            ("invoices", "lead_npn_num", "TEXT"),
            ("invoices", "lead_npn_name", "TEXT"),
            ("invoices", "lead_npn_phone", "TEXT"),
            ("invoices", "lead_npn_city", "TEXT"),
            ("invoices", "lead_npn_date", "TEXT"),
            ("invoices", "inv_kv_num", "TEXT"),
            ("invoices", "inv_kv_name", "TEXT"),
            ("invoices", "inv_kv_phone", "TEXT"),
            ("invoices", "inv_kv_city", "TEXT"),
            ("invoices", "inv_kv_date", "TEXT"),
            ("invoices", "inv_kia_num", "TEXT"),
            ("invoices", "inv_kia_name", "TEXT"),
            ("invoices", "inv_kia_phone", "TEXT"),
            ("invoices", "inv_kia_city", "TEXT"),
            ("invoices", "inv_kia_date", "TEXT"),
            ("invoices", "inv_npn_num", "TEXT"),
            ("invoices", "inv_npn_name", "TEXT"),
            ("invoices", "inv_npn_phone", "TEXT"),
            ("invoices", "inv_npn_city", "TEXT"),
            ("invoices", "inv_npn_date", "TEXT"),
            # --- Lead lifecycle: статус лида + привязка к счёту ---
            ("lead_tracking", "status", "TEXT DEFAULT 'lead'"),
            ("lead_tracking", "invoice_id", "INTEGER"),
            ("lead_tracking", "invoice_issued_at", "TEXT"),
            # --- Адрес (вместо city) для лидов/счетов по менеджерам ---
            ("invoices", "lead_kv_address", "TEXT"),
            ("invoices", "lead_kia_address", "TEXT"),
            ("invoices", "lead_npn_address", "TEXT"),
            ("invoices", "inv_kv_address", "TEXT"),
            ("invoices", "inv_kia_address", "TEXT"),
            ("invoices", "inv_npn_address", "TEXT"),
            # amoCRM lead enrichment: phone, contact name, tags, source
            ("leads", "phone", "TEXT"),
            ("leads", "contact_name", "TEXT"),
            ("leads", "tags_json", "TEXT"),
            ("leads", "source", "TEXT"),
            # --- Агрегированные затраты по типам (суммы из supplier_payments) ---
            ("invoices", "cost_metal", "REAL DEFAULT 0"),
            ("invoices", "cost_glass", "REAL DEFAULT 0"),
            ("invoices", "cost_montazh", "REAL DEFAULT 0"),
            ("invoices", "cost_loaders", "REAL DEFAULT 0"),
            ("invoices", "cost_logistics", "REAL DEFAULT 0"),
            ("invoices", "cost_extra_mat", "REAL DEFAULT 0"),
            ("invoices", "cost_extra_svc", "REAL DEFAULT 0"),
            # Согласованная сумма монтажа (монтажник подтвердил/изменил при приёмке)
            ("invoices", "montazh_agreed_amount", "REAL"),
            # Комментарий монтажника при «Счёт ОК»
            ("invoices", "installer_ok_comment", "TEXT"),
            # Способ оплаты (нал/безнал)
            ("invoices", "payment_method", "TEXT"),
            # Вложения от РП при назначении монтажнику (JSON)
            ("invoices", "montazh_assign_attachments_json", "TEXT"),
            # Статус заказа материалов (заказано / бланк отправлен / размеры подтверждены)
            ("invoices", "glass_order_status", "TEXT"),
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

        # --- Миграция city → address (однократно) ---
        for suffix in ("kv", "kia", "npn"):
            for prefix in ("lead", "inv"):
                await self.conn.execute(
                    f"UPDATE invoices SET {prefix}_{suffix}_address = {prefix}_{suffix}_city "
                    f"WHERE {prefix}_{suffix}_city IS NOT NULL "
                    f"AND ({prefix}_{suffix}_address IS NULL OR {prefix}_{suffix}_address = '')"
                )
        await self.conn.commit()

        # --- Одноразовая миграция: все активные счета → montazh_stage='in_work' ---
        await self.conn.execute(
            "UPDATE invoices SET montazh_stage = 'in_work' "
            "WHERE (montazh_stage IS NULL OR montazh_stage = 'none') "
            "AND status IN ('in_progress', 'paid') "
            "AND parent_invoice_id IS NULL"
        )
        await self.conn.commit()

        # Drop legacy unique index — invoice_number is NOT unique
        # (e.g. multiple КВ invoices are valid).
        await self.conn.execute(
            "DROP INDEX IF EXISTS idx_invoices_number_unique"
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
        task_payload = dict(payload or {})
        if assigned_to is not None:
            assignee = await self.get_user_optional(int(assigned_to))
            if assignee and not assignee.is_active:
                raise ValueError(f"task assignee {assigned_to} is inactive")
            if assignee and not parse_roles(assignee.role):
                raise ValueError(f"task assignee {assigned_to} has no role")
            assigned_role = str(task_payload.get("assigned_role") or "").strip().lower()
            if assigned_role and assignee and assigned_role not in set(parse_roles(assignee.role)):
                raise ValueError(f"task assignee {assigned_to} does not have role {assigned_role}")

        now = to_iso(utcnow())
        payload_json = _json_dumps(task_payload)
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

    async def delete_task(self, task_id: int) -> None:
        """Permanently delete a task and its attachments."""
        await self.conn.execute("DELETE FROM attachments WHERE task_id = ?", (task_id,))
        await self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await self.conn.commit()

    async def list_tasks_for_user(
        self,
        assigned_to: int,
        statuses: Iterable[str] = ("open", "in_progress"),
        limit: int = 30,
        type_filter: str | None = None,
        exclude_created_by: int | None = None,
    ) -> list[dict[str, Any]]:
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [assigned_to, *statuses]
        where_type = ""
        if type_filter:
            where_type += " AND type = ?"
            params.append(type_filter)
        if exclude_created_by is not None:
            where_type += " AND (created_by IS NULL OR created_by != ?)"
            params.append(exclude_created_by)
        params.append(limit)
        cur = await self.conn.execute(
            f"""
            SELECT t.*, u.role AS creator_role FROM tasks t
            LEFT JOIN users u ON t.created_by = u.telegram_id
            WHERE t.assigned_to = ? AND t.status IN ({placeholders}) {where_type}
            ORDER BY COALESCE(t.due_at, t.created_at) ASC
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

    async def list_tasks_open_by_types(
        self,
        task_types: list[str],
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List open/in_progress tasks filtered by task_type (for GD summary drill-down)."""
        placeholders = ",".join("?" for _ in task_types)
        cur = await self.conn.execute(
            f"""
            SELECT t.*, u.role AS creator_role FROM tasks t
            LEFT JOIN users u ON t.created_by = u.telegram_id
            WHERE t.status IN ('open', 'in_progress')
              AND t.type IN ({placeholders})
            ORDER BY t.created_at DESC LIMIT ?
            """,
            (*task_types, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_zp_pending_invoices(self, limit: int = 50) -> list[dict[str, Any]]:
        """List invoices with any pending ZP request."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE zp_installer_status = 'requested' "
            "   OR zp_status = 'requested' "
            "   OR zp_manager_status = 'requested' "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_tasks_created_by(
        self,
        created_by: int,
        statuses: Iterable[str] = ("open", "in_progress"),
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """List tasks created by a specific user."""
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [created_by, *statuses, limit]
        cur = await self.conn.execute(
            f"""
            SELECT t.*, u.role AS creator_role FROM tasks t
            LEFT JOIN users u ON t.created_by = u.telegram_id
            WHERE t.created_by = ? AND t.status IN ({placeholders})
            ORDER BY COALESCE(t.due_at, t.created_at) ASC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def update_task_status(
        self,
        task_id: int,
        status: str,
        *,
        expected_statuses: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        """Update task status atomically.

        If *expected_statuses* is given, the UPDATE only touches rows whose
        current status is one of those values.  Returns ``None`` when the row
        was not updated (status already changed by another handler).
        """
        now = to_iso(utcnow())
        if expected_statuses:
            cur = await self.conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? "
                "WHERE id = ? AND status IN ({})".format(
                    ",".join("?" for _ in expected_statuses)
                ),
                (status, now, task_id, *expected_statuses),
            )
        else:
            cur = await self.conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
                (status, now, task_id),
            )
        await self.conn.commit()
        if expected_statuses and cur.rowcount == 0:
            return None
        return await self.get_task(task_id)

    async def update_task_assignee(
        self,
        task_id: int,
        assigned_to: int,
        *,
        assigned_role: str | None = None,
    ) -> dict[str, Any]:
        assignee = await self.get_user_optional(int(assigned_to))
        if assignee and not assignee.is_active:
            raise ValueError(f"task assignee {assigned_to} is inactive")
        if assignee and not parse_roles(assignee.role):
            raise ValueError(f"task assignee {assigned_to} has no role")

        task = await self.get_task(task_id)
        payload = {}
        try:
            payload = json.loads(task.get("payload_json") or "{}")
        except Exception:
            payload = {}
        if assigned_role:
            payload["assigned_role"] = assigned_role

        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE tasks SET assigned_to = ?, payload_json = ?, updated_at = ? WHERE id = ?",
            (int(assigned_to), _json_dumps(payload), now, task_id),
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
        """Count tasks for GD inbox: OPEN/IN_PROGRESS, excluding invoice_payment, payment_confirm, invoice_end.
        Also excludes tasks created by the GD user themselves (outgoing tasks)."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type NOT IN ('invoice_payment', 'payment_confirm', 'invoice_end', 'zp_installer') "
            "AND (created_by IS NULL OR created_by != ?)",
            (user_id, user_id),
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
        """Count pending ZP requests (zamery/manager/installer) for 'Оплата поставщику' badge.

        Note: ZP requests are global (not per-user), but the parameter is kept
        for API consistency with other count_gd_* methods.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices "
            "WHERE zp_status = 'requested' "
            "   OR zp_manager_status = 'requested' "
            "   OR zp_installer_status IN ('requested', 'approved', 'payment_sent')",
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

    async def count_rp_check_kp_tasks(self, user_id: int) -> int:
        """Count open/in_progress CHECK_KP tasks assigned to RP user."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks "
            "WHERE assigned_to = ? AND status IN ('open', 'in_progress') "
            "AND type = 'check_kp'",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_rp_invoice_pay_tasks(self, user_id: int) -> int:
        """Count open/in_progress ORDER_* tasks assigned to RP user."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks "
            "WHERE assigned_to = ? AND status IN ('open', 'in_progress') "
            "AND type IN ('order_materials', 'order_profile', 'order_glass', "
            "'delivery_request', 'tinting_request')",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_rp_channel_unread(self, user_id: int, channel: str) -> int:
        """Count unread incoming chat messages for RP in a specific channel."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM chat_messages "
            "WHERE receiver_id = ? AND direction = 'incoming' "
            "AND is_read = 0 AND channel = ?",
            (user_id, channel),
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
        """List credit-based invoices (is_credit=1).

        Note: _compute_lifecycle_status() ensures status='credit' when is_credit=1,
        so checking is_credit alone is sufficient.
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE is_credit = 1 "
            "ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_credit_invoices(self) -> int:
        """Count credit-based invoices."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE is_credit = 1"
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def list_invoices_in_work(
        self, limit: int = 50, *, only_regular: bool = False,
    ) -> list[dict[str, Any]]:
        """List invoices 'in work' (pending/in_progress/paid, excluding credit).

        Args:
            only_regular: if True, show only regular invoices whose number
                matches DDMMYY-N... format (6 digits + dash). Everything else
                is considered credit.
        """
        fmt_clause = (
            "AND invoice_number GLOB '[0-9]*-*' "
            if only_regular else
            "AND (is_credit = 0 OR is_credit IS NULL) "
        )
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid') "
            f"{fmt_clause}"
            "ORDER BY receipt_date ASC, updated_at ASC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_invoices_approaching_deadline(
        self,
        *,
        today: date | None = None,
        days_ahead: int = 3,
    ) -> list[dict[str, Any]]:
        """Return active top-level invoices whose contract deadline is near.

        Includes overdue invoices and invoices with deadline within ``days_ahead``.
        Excludes child invoices, credit invoices, and inactive lifecycle states.
        """
        if days_ahead < 0:
            raise ValueError("days_ahead must be >= 0")

        anchor = today or datetime.now(timezone.utc).date()
        deadline_upper = (anchor + timedelta(days=days_ahead)).isoformat()

        cur = await self.conn.execute(
            """
            SELECT * FROM invoices
            WHERE deadline_end_date IS NOT NULL
              AND TRIM(deadline_end_date) != ''
              AND status IN ('pending', 'in_progress', 'paid', 'closing')
              AND (is_credit = 0 OR is_credit IS NULL)
              AND parent_invoice_id IS NULL
              AND (actual_completion_date IS NULL OR TRIM(actual_completion_date) = '')
              AND date(substr(deadline_end_date, 1, 10)) <= date(?)
            ORDER BY date(substr(deadline_end_date, 1, 10)) ASC, updated_at DESC, id DESC
            """,
            (deadline_upper,),
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

    async def list_invoices_for_selection(self, limit: int = 30, *, only_regular: bool = False) -> list[dict[str, Any]]:
        """Счета «в работе» + «Счёт End» для inline-пикера.

        only_regular — показать только обычные счета (номер DDMMYY-N...),
        исключая кредитные и прочие.
        """
        fmt_clause = (
            "AND invoice_number GLOB '[0-9]*-*' "
            if only_regular else
            "AND (is_credit = 0 OR is_credit IS NULL) "
        )
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid', 'ended') "
            f"{fmt_clause}"
            "ORDER BY receipt_date ASC, updated_at ASC LIMIT ?",
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

    # Mapping material_type → invoices column name
    _COST_COL_MAP: dict[str, str] = {
        "metal": "cost_metal", "glass": "cost_glass",
        "montazh": "cost_montazh", "loaders": "cost_loaders",
        "logistics": "cost_logistics",
        "extra_mat": "cost_extra_mat", "extra_svc": "cost_extra_svc",
        # Legacy types → closest column
        "profile": "cost_metal",
        "service": "cost_extra_svc",
        "ldsp": "cost_extra_mat", "gkl": "cost_extra_mat",
        "sandwich": "cost_extra_mat", "other": "cost_extra_mat",
    }

    async def create_supplier_payment(
        self,
        parent_invoice_id: int,
        amount: float,
        material_type: str,
        invoice_number: str = "",
        supplier: str = "",
        task_id: int | None = None,
        created_by: int | None = None,
    ) -> int:
        """Insert a row into supplier_payments table + update cost_* in invoices."""
        now = datetime.now(timezone.utc).isoformat()
        cur = await self.conn.execute(
            "INSERT INTO supplier_payments "
            "(parent_invoice_id, invoice_number, amount, material_type, supplier, task_id, created_by, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (parent_invoice_id, invoice_number, amount, material_type, supplier, task_id, created_by, now),
        )
        # Update aggregated cost column in parent invoice
        cost_col = self._COST_COL_MAP.get(material_type)
        if cost_col:
            await self.conn.execute(
                f"UPDATE invoices SET {cost_col} = COALESCE({cost_col}, 0) + ? WHERE id = ?",
                (amount, parent_invoice_id),
            )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def list_supplier_payments_for_invoice(
        self, invoice_id: int,
    ) -> list[dict[str, Any]]:
        """Оплаты поставщикам из таблицы supplier_payments + legacy SUPPLIER_PAYMENT tasks."""
        result: list[dict[str, Any]] = []
        seen_task_ids: set[int] = set()

        # 1) Новая таблица supplier_payments
        cur = await self.conn.execute(
            "SELECT id, invoice_number, amount, material_type, supplier, task_id "
            "FROM supplier_payments WHERE parent_invoice_id = ? ORDER BY id",
            (invoice_id,),
        )
        for row in await cur.fetchall():
            r = dict(row)
            result.append({
                "supplier": r.get("supplier", ""),
                "amount": float(r.get("amount") or 0),
                "material_type": r.get("material_type", ""),
                "invoice_number": r.get("invoice_number", ""),
                "task_id": r.get("task_id"),
            })
            if r.get("task_id"):
                seen_task_ids.add(r["task_id"])

        # 2) Legacy: SUPPLIER_PAYMENT tasks (для данных до миграции)
        rows = await self.search_tasks_by_payload(
            field="parent_invoice_id",
            value=str(invoice_id),
            type_filter=["supplier_payment"],
            limit=50,
        )
        for r in rows:
            if r.get("status") != "done":
                continue
            if r["id"] in seen_task_ids:
                continue
            payload = json.loads(r.get("payload_json") or "{}")
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

    async def list_supplier_payments_grouped(
        self, invoice_id: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Supplier payments grouped by material category for invoice.

        Categories:
            metal   → metal, profile (legacy)
            glass   → glass
            additional → extra_mat, ldsp, gkl, sandwich, other (legacy)
            services → montazh, loaders, logistics, extra_svc, service (legacy)
        """
        payments = await self.list_supplier_payments_for_invoice(invoice_id)
        _CAT_MAP = {
            # New categories
            "metal": "metal",
            "glass": "glass",
            "montazh": "services",
            "loaders": "services",
            "logistics": "services",
            "extra_mat": "additional",
            "extra_svc": "services",
            # Legacy backward compatibility
            "profile": "metal",
            "ldsp": "additional", "gkl": "additional",
            "sandwich": "additional", "other": "additional",
            "service": "services",
        }
        grouped: dict[str, list[dict[str, Any]]] = {
            "metal": [], "glass": [], "additional": [], "services": [],
        }
        for p in payments:
            cat = _CAT_MAP.get(p.get("material_type", ""), "additional")
            grouped[cat].append(p)
        return grouped

    async def get_edo_upd_status_for_invoice(self, invoice_id: int) -> bool:
        """True if a sign_upd EDO request is completed for this invoice."""
        cur = await self.conn.execute(
            "SELECT id FROM edo_requests "
            "WHERE request_type = 'sign_upd' AND invoice_id = ? AND status = 'done' "
            "LIMIT 1",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return row is not None

    async def get_edo_stats_for_invoice(self, invoice_id: int) -> dict[str, Any]:
        """Aggregate EDO request stats for an invoice (for sheet export)."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) as cnt FROM edo_requests WHERE invoice_id = ?",
            (invoice_id,),
        )
        total = (await cur.fetchone())["cnt"]

        cur = await self.conn.execute(
            "SELECT COUNT(*) as cnt FROM edo_requests WHERE invoice_id = ? AND status = 'open'",
            (invoice_id,),
        )
        open_count = (await cur.fetchone())["cnt"]

        cur = await self.conn.execute(
            "SELECT response_type, completed_at FROM edo_requests "
            "WHERE invoice_id = ? AND status = 'done' "
            "ORDER BY completed_at DESC LIMIT 1",
            (invoice_id,),
        )
        last = await cur.fetchone()

        return {
            "total": total,
            "open": open_count,
            "last_response_type": last["response_type"] if last else None,
            "last_completed_at": last["completed_at"] if last else None,
        }

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

        # 3) ЗП (только approved) — для информации, НЕ входят в total_cost
        zp_zamery = float(inv.get("zp_zamery_total") or 0) if inv.get("zp_status") == "approved" else 0.0
        zp_manager = float(inv.get("zp_manager_amount") or 0) if inv.get("zp_manager_status") == "approved" else 0.0
        zp_installer = float(inv.get("zp_installer_amount") or 0) if inv.get("zp_installer_status") == "approved" else 0.0
        zp_total = zp_zamery + zp_manager + zp_installer

        # Материалы из ОП (уже закупленные) + дочерние счета (новые)
        materials_fact_op = float(inv.get("materials_fact_op") or 0)
        # Fallback: если materials_fact_op не заполнен — берём сумму cost_* полей
        if not materials_fact_op:
            materials_fact_op = sum(
                float(inv.get(f) or 0)
                for f in ("cost_metal", "cost_glass", "cost_extra_mat")
            )
        materials_combined = materials_fact_op + materials_total

        # Монтаж: приоритет ЗП монтажника (если согласовано), иначе из ОП
        montazh_fact_op = float(inv.get("montazh_fact_op") or 0)
        if zp_installer > 0:
            montazh_combined = zp_installer
        elif montazh_fact_op > 0:
            montazh_combined = montazh_fact_op
        else:
            montazh_combined = 0

        # Дедупликация: берём MAX(materials_combined, supplier_payments)
        # чтобы не считать одни и те же расходы дважды
        mat_and_suppliers = max(materials_combined, supplier_payments_total)

        # Вычитаемые позиции (отслеживаются в отдельных столбцах ОП)
        logistics_fact = float(inv.get("logistics_fact_op") or 0)
        loaders_fact = float(inv.get("loaders_fact_op") or 0)
        agent_payout = float(inv.get("agent_payout_op") or inv.get("agent_fee") or 0)
        taxes_fact = float(inv.get("taxes_fact_op") or 0)

        # Итого расходы
        # ЗП не входит — используем только факт из ОП
        total_cost = (mat_and_suppliers + montazh_combined
                      + logistics_fact + loaders_fact + agent_payout)

        # НДС факт = (Сумма × 22/122) − (Материалы_факт × 22/122)
        # Кредитные счета: налоги = 0
        if inv.get("is_credit"):
            nds_fact = 0.0
            profit_tax_fact = 0.0
        else:
            nds_fact = (invoice_amount * 22 / 122) - (mat_and_suppliers * 22 / 122) if invoice_amount else 0.0
            # Налог на прибыль факт = (Сумма − Расходы − НДС) × 20%
            profit_tax_fact = ((invoice_amount - total_cost - nds_fact) / 100 * 20) if invoice_amount else 0.0

        # Прибыль факт = сумма − расходы − НДС − налог на прибыль
        margin = invoice_amount - total_cost - nds_fact - profit_tax_fact
        margin_pct = (margin / invoice_amount * 100) if invoice_amount > 0 else 0.0

        return {
            "invoice_amount": invoice_amount,
            "materials_total": materials_total,
            "materials_by_type": materials_by_type,
            "materials_fact_op": materials_fact_op,
            "materials_combined": materials_combined,
            "montazh_fact_op": montazh_fact_op,
            "montazh_combined": montazh_combined,
            "supplier_payments_total": supplier_payments_total,
            "supplier_payments_list": sp_list,
            "zp_zamery": zp_zamery,
            "zp_manager": zp_manager,
            "zp_installer": zp_installer,
            "zp_total": zp_total,
            "mat_and_suppliers": mat_and_suppliers,
            "logistics_fact": logistics_fact,
            "loaders_fact": loaders_fact,
            "agent_payout": agent_payout,
            "taxes_fact": taxes_fact,
            "nds_fact": nds_fact,
            "profit_tax_fact": profit_tax_fact,
            "total_cost": total_cost,
            "margin": margin,
            "margin_pct": margin_pct,
        }

    async def get_plan_fact_card(self, invoice_id: int) -> dict[str, Any]:
        """
        Карточка «План / Факт» для сравнения расчётных и фактических данных.
        Расчётные данные вводятся менеджером при запуске счёта в работу.
        Фактические берутся из get_full_invoice_cost_card().

        НДС с учётом возвратного:
          output_vat = amount * 22 / 122
          input_vat  = (стекло + профиль) * 22 / 122  (возвратный)
          net_vat    = output_vat - input_vat

        Распределение прибыли:
          1) ЗП РП = 10% от прибыли (с вычетом НДС)
          2) Остаток: клиент менеджера → 50/50, лид от ГД → 75(ГД)/25(менеджер)
        """
        inv = await self.get_invoice(invoice_id)
        if not inv:
            return {
                "has_estimated": False,
                "estimated_glass": 0, "estimated_profile": 0,
                "estimated_installation": 0,
                "estimated_loaders": 0, "estimated_logistics": 0,
                "output_vat": 0, "input_vat": 0, "net_vat": 0,
                "estimated_total_cost": 0,
                "estimated_profit": 0, "estimated_profitability": 0,
                "actual_total_cost": 0, "actual_profit": 0,
                "actual_profitability": 0, "cost_delta": 0,
                "zp_allowed": False, "cost_card": {},
                "client_source": "own",
                "rp_zp": 0, "manager_zp": 0, "gd_profit": 0,
            }

        cost = await self.get_full_invoice_cost_card(invoice_id)
        amount = float(inv.get("amount") or 0)

        # План (расчётные данные менеджера)
        est_glass = float(inv.get("estimated_glass") or 0)
        est_profile = float(inv.get("estimated_profile") or 0)
        est_mat_legacy = float(inv.get("estimated_materials") or 0)  # backward compat
        est_inst = float(inv.get("estimated_installation") or 0)
        est_load = float(inv.get("estimated_loaders") or 0)
        est_log = float(inv.get("estimated_logistics") or 0)

        # Материалы = стекло + профиль (+ legacy если есть)
        materials_total = est_glass + est_profile + est_mat_legacy
        est_total = materials_total + est_inst + est_load + est_log

        # НДС с учётом возвратного
        refundable_base = est_glass + est_profile  # только стекло + профиль
        output_vat = amount * 22 / 122 if amount > 0 else 0.0
        input_vat = refundable_base * 22 / 122 if refundable_base > 0 else 0.0
        net_vat = output_vat - input_vat

        est_profit = amount - est_total - net_vat
        est_pct = (est_profit / amount * 100) if amount > 0 else 0.0

        has_estimated = any([est_glass, est_profile, est_mat_legacy,
                             est_inst, est_load, est_log])

        # Факт (из cost card)
        fact_total = cost["total_cost"]
        fact_profit = cost["margin"]

        # Распределение расчётной прибыли
        client_source = inv.get("client_source") or "own"
        rp_zp = est_profit * 0.10 if est_profit > 0 else 0.0
        remaining = est_profit - rp_zp
        if client_source == "gd_lead":
            # Лид от ГД: 75% ГД / 25% менеджер
            manager_zp = remaining * 0.25
            gd_profit = remaining * 0.75
        else:
            # Клиент менеджера: 50/50
            manager_zp = remaining * 0.50
            gd_profit = remaining * 0.50

        return {
            "has_estimated": has_estimated,
            "amount": amount,
            "estimated_glass": est_glass,
            "estimated_profile": est_profile,
            "estimated_materials_legacy": est_mat_legacy,
            "materials_total": materials_total,
            "estimated_installation": est_inst,
            "estimated_loaders": est_load,
            "estimated_logistics": est_log,
            "output_vat": output_vat,
            "input_vat": input_vat,
            "net_vat": net_vat,
            "estimated_total_cost": est_total,
            "estimated_profit": est_profit,
            "estimated_profitability": est_pct,
            "actual_total_cost": fact_total,
            "actual_profit": fact_profit,
            "actual_profitability": cost["margin_pct"],
            "cost_delta": fact_total - est_total,
            "zp_allowed": fact_total <= est_total,
            "cost_card": cost,
            # Факт по отдельным категориям (для РП карточки)
            "fact_glass": float(inv.get("cost_glass") or 0),
            "fact_metal": float(inv.get("cost_metal") or 0),
            # Распределение прибыли
            "client_source": client_source,
            "rp_zp": rp_zp,
            "manager_zp": manager_zp,
            "gd_profit": gd_profit,
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

    async def list_installer_confirmed_invoices(self, user_id: int | None = None) -> list[dict[str, Any]]:
        """Счета «В работу» (montazh_stage >= IN_WORK).
        Если user_id=None — все счета (для общего списка монтажников)."""
        if user_id is not None:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE assigned_to = ? "
                "AND montazh_stage IN ('in_work', 'razmery_ok', 'invoice_ok') "
                "AND status IN ('in_progress', 'paid') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY CASE montazh_stage "
                "  WHEN 'in_work' THEN 1 WHEN 'razmery_ok' THEN 2 "
                "  WHEN 'invoice_ok' THEN 3 ELSE 4 END, created_at DESC LIMIT 15",
                (user_id,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE "
                "montazh_stage IN ('in_work', 'razmery_ok', 'invoice_ok') "
                "AND status IN ('in_progress', 'paid') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY CASE montazh_stage "
                "  WHEN 'in_work' THEN 1 WHEN 'razmery_ok' THEN 2 "
                "  WHEN 'invoice_ok' THEN 3 ELSE 4 END, created_at DESC LIMIT 15",
            )
        return [dict(r) for r in await cur.fetchall()]

    async def list_installer_unconfirmed_invoices(self, user_id: int | None = None) -> list[dict[str, Any]]:
        """Счета, ещё НЕ подтверждённые «В работу».
        Если user_id=None — все неподтверждённые (для общего списка)."""
        if user_id is not None:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE assigned_to = ? "
                "AND (montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY created_at DESC LIMIT 15",
                (user_id,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE "
                "(montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY created_at DESC LIMIT 15",
            )
        return [dict(r) for r in await cur.fetchall()]

    async def list_invoices_with_deadline(self) -> list[dict[str, Any]]:
        """Backward-compatible alias for deadline dashboards and legacy callers."""
        return await self.list_invoices_approaching_deadline()

    async def assign_installer_to_invoice(
        self, invoice_id: int, installer_id: int,
    ) -> None:
        """Назначить монтажника на счёт + сбросить montazh_stage."""
        await self.update_invoice(
            invoice_id,
            assigned_to=installer_id,
            montazh_stage="none",
        )

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

    async def list_chat_messages_for_invoice_channel(
        self,
        channel: str,
        invoice_id: int,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Messages for a specific invoice-bound channel only."""
        cur = await self.conn.execute(
            "SELECT * FROM chat_messages WHERE channel = ? AND invoice_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (channel, invoice_id, limit),
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

    # --- Installer init helpers (ZP & materials) ---

    async def is_installer_zp_initialized(self, user_id: int) -> bool:
        """Проверить, прошёл ли монтажник инициализацию ЗП."""
        cur = await self.conn.execute(
            "SELECT zp_init_done FROM users WHERE telegram_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        return bool(row and row["zp_init_done"])

    async def set_installer_zp_initialized(self, user_id: int) -> None:
        """Пометить, что монтажник прошёл инициализацию ЗП."""
        await self.conn.execute(
            "UPDATE users SET zp_init_done = 1 WHERE telegram_id = ?",
            (user_id,),
        )
        await self.conn.commit()

    async def is_installer_razmery_initialized(self, user_id: int) -> bool:
        """Проверить, прошёл ли монтажник инициализацию «Размеры ОК»."""
        cur = await self.conn.execute(
            "SELECT razmery_init_done FROM users WHERE telegram_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        return bool(row and row["razmery_init_done"])

    async def set_installer_razmery_initialized(self, user_id: int) -> None:
        """Пометить, что монтажник прошёл инициализацию «Размеры ОК»."""
        await self.conn.execute(
            "UPDATE users SET razmery_init_done = 1 WHERE telegram_id = ?",
            (user_id,),
        )
        await self.conn.commit()

    async def set_invoice_materials_ordered(
        self, invoice_id: int, ordered: bool = True,
    ) -> None:
        """Пометить счёт: материал заказан."""
        await self.conn.execute(
            "UPDATE invoices SET materials_ordered = ?, updated_at = ? WHERE id = ?",
            (int(ordered), to_iso(utcnow()), invoice_id),
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

    async def get_ended_monthly_summary(self) -> list[dict[str, Any]]:
        """Агрегация ended-счетов по месяцам: количество, суммы, план/факт, налоги, прибыль."""
        cur = await self.conn.execute(
            """
            SELECT
                strftime('%Y-%m', COALESCE(receipt_date, created_at)) AS month,
                COUNT(*) AS cnt,
                SUM(COALESCE(amount, 0)) AS total_amount,
                SUM(COALESCE(estimated_glass, 0) + COALESCE(estimated_profile, 0)
                    + COALESCE(estimated_materials, 0)) AS est_materials,
                SUM(COALESCE(estimated_installation, 0)) AS est_installation,
                SUM(COALESCE(estimated_loaders, 0)) AS est_loaders,
                SUM(COALESCE(estimated_logistics, 0)) AS est_logistics,
                SUM(CASE WHEN COALESCE(materials_fact_op, 0) > 0 THEN materials_fact_op
                    ELSE COALESCE(cost_metal, 0) + COALESCE(cost_glass, 0) + COALESCE(cost_extra_mat, 0)
                    END) AS fact_materials,
                SUM(CASE WHEN zp_installer_status IN ('approved', 'confirmed')
                        AND COALESCE(zp_installer_amount, 0) > 0
                    THEN COALESCE(zp_installer_amount, 0)
                    ELSE COALESCE(montazh_fact_op, 0) END) AS fact_montazh,
                SUM(COALESCE(loaders_fact_op, 0)) AS fact_loaders,
                SUM(COALESCE(logistics_fact_op, 0)) AS fact_logistics,
                SUM(CASE WHEN zp_manager_status = 'approved'
                    THEN COALESCE(zp_manager_amount, 0) ELSE 0 END) AS zp_manager,
                SUM(CASE WHEN zp_installer_status IN ('approved', 'confirmed')
                    THEN COALESCE(zp_installer_amount, 0) ELSE 0 END) AS zp_installer,
                SUM(COALESCE(agent_payout_op, agent_fee, 0)) AS agent_payout
            FROM invoices
            WHERE status = 'ended' AND parent_invoice_id IS NULL
            GROUP BY month
            ORDER BY month DESC
            """
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_daily_summary(self) -> dict[str, Any]:
        """Агрегированная сводка дня для ГД."""
        month_start = date.today().replace(day=1).isoformat()

        # Счета по статусам
        cur = await self.conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "GROUP BY status"
        )
        inv_by_status: dict[str, int] = {}
        for r in await cur.fetchall():
            inv_by_status[str(r["status"])] = int(r["cnt"])

        # Счета в работе (pending/in_progress/paid, без кредитных)
        in_work = await self.count_invoices_in_work()

        # Закрытые за месяц
        ended_month = await self.count_ended_invoices(month_start)

        # Открытые задачи по типам
        cur = await self.conn.execute(
            "SELECT type, COUNT(*) AS cnt FROM tasks "
            "WHERE status IN ('open', 'in_progress') "
            "GROUP BY type"
        )
        tasks_open: dict[str, int] = {}
        for r in await cur.fetchall():
            tasks_open[str(r["type"])] = int(r["cnt"])

        # Просроченные / приближающиеся дедлайны
        deadlines = await self.list_invoices_with_deadline()
        overdue = 0
        today_dl = 0
        soon_dl = 0
        for inv in deadlines:
            raw = inv.get("deadline_end_date")
            if not raw:
                continue
            try:
                end = datetime.fromisoformat(str(raw)).date()
            except (ValueError, TypeError):
                continue
            delta = (end - date.today()).days
            if delta < 0:
                overdue += 1
            elif delta == 0:
                today_dl += 1
            elif delta <= 3:
                soon_dl += 1

        # Сумма активных счетов
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total, "
            "COALESCE(SUM(outstanding_debt), 0) AS total_debt "
            "FROM invoices "
            "WHERE status IN ('pending', 'in_progress', 'paid') "
            "AND parent_invoice_id IS NULL "
            "AND (is_credit = 0 OR is_credit IS NULL)"
        )
        fin = await cur.fetchone()

        # ЗП-запросы в ожидании
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices "
            "WHERE zp_installer_status = 'requested' "
            "OR zp_status = 'requested' "
            "OR zp_manager_status = 'requested'"
        )
        zp_pending = (await cur.fetchone())[0]

        return {
            "invoices_by_status": inv_by_status,
            "in_work": in_work,
            "ended_month": ended_month,
            "tasks_open": tasks_open,
            "overdue": overdue,
            "today_deadline": today_dl,
            "soon_deadline": soon_dl,
            "total_amount": fin["total"] if fin else 0,
            "total_debt": fin["total_debt"] if fin else 0,
            "zp_pending": zp_pending,
        }

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
        *,
        phone: str | None = None,
        contact_name: str | None = None,
        tags_json: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO leads(amo_lead_id, name, price, pipeline_id, status_id,
                              responsible_user_id, claimed_by, claimed_at, escalated,
                              workchat_message_id, created_at, updated_at,
                              phone, contact_name, tags_json, source)
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 0, NULL, ?, ?, ?, ?, ?, ?)
            """,
            (amo_lead_id, name, price, pipeline_id, status_id, responsible_user_id,
             now, now, phone, contact_name, tags_json, source),
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

    async def update_lead_status(self, amo_lead_id: int, status_id: int) -> None:
        """Update the amoCRM status_id for an existing lead."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET status_id = ?, updated_at = ? WHERE amo_lead_id = ?",
            (status_id, now, amo_lead_id),
        )
        await self.conn.commit()

    async def update_lead_source(self, amo_lead_id: int, source: str) -> None:
        """Update the source (Источник) for an existing lead."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET source = ?, updated_at = ? WHERE amo_lead_id = ?",
            (source, now, amo_lead_id),
        )
        await self.conn.commit()

    async def list_all_amo_leads(self, limit: int = 10000) -> list[dict[str, Any]]:
        """List all amoCRM leads for Sheets export."""
        cur = await self.conn.execute(
            "SELECT * FROM leads ORDER BY created_at ASC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cur.fetchall()]

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

    async def list_unescalated_leads(
        self, older_than_iso: str, status_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Unclaimed & not yet escalated leads older than given timestamp.

        If status_ids is given, only return leads whose status_id is in the set.
        """
        if status_ids:
            placeholders = ",".join("?" for _ in status_ids)
            cur = await self.conn.execute(
                f"""
                SELECT * FROM leads
                WHERE claimed_by IS NULL AND escalated = 0
                  AND created_at <= ? AND status_id IN ({placeholders})
                ORDER BY created_at ASC
                """,
                (older_than_iso, *status_ids),
            )
        else:
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
        client_name: str = "",
        payment_terms: str | None = None,
        deadline_days: int | None = None,
    ) -> int:
        """Create a new invoice record (status = NEW)."""
        invoice_number_normalized = (invoice_number or "").strip()
        if not invoice_number_normalized:
            raise ValueError("invoice_number is required")
        now = to_iso(utcnow())

        # Compute deadline_end_date from receipt_date + deadline_days
        deadline_end_date: str | None = None
        if deadline_days:
            from datetime import datetime, timedelta
            dt = datetime.strptime(now[:10], "%Y-%m-%d")
            deadline_end_date = (dt + timedelta(days=deadline_days)).strftime("%Y-%m-%d")

        cur = await self.conn.execute(
            """
            INSERT INTO invoices
                (invoice_number, project_id, created_by, creator_role,
                 object_address, amount, supplier, description,
                 assigned_to, payment_deadline, client_name, payment_terms,
                 deadline_days, deadline_end_date, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
            """,
            (invoice_number_normalized, project_id, created_by, creator_role,
             object_address, amount, supplier, description,
             assigned_to, payment_deadline, client_name, payment_terms,
             deadline_days, deadline_end_date, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def _infer_invoice_creator_role(self, invoice_number: str) -> str:
        number_upper = (invoice_number or "").upper()
        if "КИА" in number_upper:
            return Role.MANAGER_KIA
        if "КВ" in number_upper:
            return Role.MANAGER_KV
        return Role.MANAGER_NPN

    async def _get_invoice_for_sheet_import(self, invoice_number: str) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            """
            SELECT * FROM invoices
            WHERE invoice_number = ?
            ORDER BY CASE WHEN parent_invoice_id IS NULL THEN 0 ELSE 1 END, id DESC
            """,
            ((invoice_number or "").strip(),),
        )
        rows = [dict(row) for row in await cur.fetchall()]
        if not rows:
            return None
        if len(rows) > 1:
            log.warning(
                "Multiple invoices found for sheet import invoice_number=%s; updating id=%s",
                invoice_number,
                rows[0]["id"],
            )
        return rows[0]

    async def _resolve_invoice_import_owner(
        self,
        inv_num: str,
        data: dict[str, Any],
        existing: dict[str, Any] | None,
    ) -> tuple[int, str]:
        creator_role = str(
            data.get("creator_role")
            or (existing.get("creator_role") if existing else "")
            or self._infer_invoice_creator_role(inv_num)
        ).strip()
        created_by_raw = data.get("created_by")
        created_by: int | None
        try:
            created_by = int(created_by_raw) if created_by_raw not in (None, "") else None
        except (TypeError, ValueError):
            created_by = None
        if created_by is None and existing and existing.get("created_by") not in (None, ""):
            try:
                created_by = int(existing["created_by"])
            except (TypeError, ValueError):
                created_by = None
        if created_by is None and creator_role:
            users = await self.find_users_by_role(creator_role, limit=1)
            if users:
                created_by = users[0].telegram_id
        return created_by or 0, creator_role

    def _compute_deadline_end_date(
        self,
        receipt_date: Any,
        deadline_days: Any,
    ) -> str | None:
        if not receipt_date or deadline_days in (None, ""):
            return None
        from datetime import datetime, timedelta

        try:
            dt = datetime.strptime(str(receipt_date), "%Y-%m-%d")
            end = dt + timedelta(days=int(deadline_days))
        except (ValueError, TypeError):
            return None
        return end.strftime("%Y-%m-%d")

    def _compute_invoice_import_status(
        self,
        data: dict[str, Any],
        existing: dict[str, Any] | None,
    ) -> str:
        explicit_status = data.get("status")
        if explicit_status not in (None, ""):
            return str(explicit_status)

        if "is_credit" in data:
            is_credit = data.get("is_credit")
        else:
            is_credit = existing.get("is_credit") if existing else None
        if isinstance(is_credit, str):
            is_credit = is_credit.strip().lower() in {"1", "true", "yes", "y", "on"}
        if bool(is_credit):
            return InvoiceStatus.CREDIT

        if "actual_completion_date" in data:
            actual_completion_date = data.get("actual_completion_date")
        else:
            actual_completion_date = existing.get("actual_completion_date") if existing else None
        if "outstanding_debt" in data:
            outstanding_debt = data.get("outstanding_debt")
        else:
            outstanding_debt = existing.get("outstanding_debt") if existing else None

        if actual_completion_date:
            try:
                debt_value = float(outstanding_debt or 0)
            except (TypeError, ValueError):
                debt_value = 0.0
            return InvoiceStatus.PAID if debt_value > 0 else InvoiceStatus.ENDED

        if existing and existing.get("status"):
            return str(existing["status"])
        return InvoiceStatus.IN_PROGRESS

    async def import_invoice_from_sheet(
        self,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Import or update invoice data from sales sheet rows.

        Accepts either a dict payload or keyword arguments. Sheet-owned fields
        are synchronized bidirectionally: explicit ``None`` values clear the
        stored column, while bot-managed fields remain untouched.
        """
        if data is None:
            payload: dict[str, Any] = {}
        elif isinstance(data, dict):
            payload = dict(data)
        else:
            raise TypeError("data must be a dict when provided")
        if kwargs:
            payload.update(kwargs)

        inv_num = str(payload.get("invoice_number") or "").strip()
        if not inv_num:
            raise ValueError("invoice_number is required")

        existing = await self._get_invoice_for_sheet_import(inv_num)
        now = to_iso(utcnow())

        sheet_fields = {
            "client_name", "traffic_source", "is_credit", "client_source",
            "object_address", "receipt_date", "deadline_days",
            "actual_completion_date", "amount", "first_payment_amount",
            "estimated_materials", "estimated_installation",
            "estimated_loaders", "estimated_logistics",
            "nds_amount", "outstanding_debt", "surcharge_amount",
            "final_surcharge_amount", "surcharge_date",
            "final_surcharge_date", "agent_fee",
            "manager_zp_blank", "npn_amount",
            "profit_tax", "rentability_calc", "payment_terms",
            "description", "contract_type", "closing_docs_status",
            "materials_fact_op",
            "montazh_fact_op",
            "zp_manager_request_text",
            "zp_manager_payout",
            "zp_manager_payout_date",
            "logistics_fact_op",
            "logistics_fact_date",
            "loaders_fact_op",
            "loaders_fact_date",
            "zamery_info_op",
            "agent_payout_op",
            "men_zp_payout_op",
            "npn_request_op",
            "npn_payout_op",
            "npn_payout_date_op",
            "taxes_fact_op",
            "profit_calc_op",
        }

        created_by, creator_role = await self._resolve_invoice_import_owner(inv_num, payload, existing)
        status = self._compute_invoice_import_status(payload, existing)
        receipt_date = payload.get("receipt_date") if "receipt_date" in payload else (existing.get("receipt_date") if existing else None)
        deadline_days = payload.get("deadline_days") if "deadline_days" in payload else (existing.get("deadline_days") if existing else None)
        deadline_end_date = self._compute_deadline_end_date(receipt_date, deadline_days)

        if existing:
            updates: dict[str, Any] = {"updated_at": now, "status": status}
            for field in sheet_fields:
                if field in payload:
                    updates[field] = payload[field]
            if "created_by" in payload and payload.get("created_by") not in (None, ""):
                updates["created_by"] = created_by
            if "creator_role" in payload and payload.get("creator_role"):
                updates["creator_role"] = creator_role
            if "receipt_date" in payload or "deadline_days" in payload:
                updates["deadline_end_date"] = deadline_end_date

            sets = ", ".join(f"{k} = ?" for k in updates)
            vals = list(updates.values())
            vals.append(existing["id"])
            await self.conn.execute(f"UPDATE invoices SET {sets} WHERE id = ?", vals)
            await self.conn.commit()
            return int(existing["id"])

        fields_to_insert: dict[str, Any] = {
            "invoice_number": inv_num,
            "created_by": created_by,
            "creator_role": creator_role,
            "status": status,
            "created_at": now,
            "updated_at": now,
        }
        for field in sheet_fields:
            if field in payload:
                fields_to_insert[field] = payload[field]
        if deadline_end_date is not None:
            fields_to_insert["deadline_end_date"] = deadline_end_date

        cols = ", ".join(fields_to_insert.keys())
        placeholders = ", ".join("?" * len(fields_to_insert))
        # Re-check for duplicates right before insert (guard against concurrent webhooks)
        recheck = await self._get_invoice_for_sheet_import(inv_num)
        if recheck:
            # Another request inserted between our check and now — update instead
            sets = ", ".join(f"{k} = ?" for k in fields_to_insert if k != "invoice_number")
            vals = [v for k, v in fields_to_insert.items() if k != "invoice_number"]
            vals.append(recheck["id"])
            await self.conn.execute(f"UPDATE invoices SET {sets} WHERE id = ?", vals)
            await self.conn.commit()
            return int(recheck["id"])
        cur = await self.conn.execute(
            f"INSERT INTO invoices ({cols}) VALUES ({placeholders})",
            list(fields_to_insert.values()),
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
        marker: str | None = None,
        limit: int = 50,
        *,
        only_regular: bool = False,
        project_id: int | None = None,
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
        if project_id is not None:
            clauses.append("project_id = ?")
            params.append(project_id)
        if marker is not None:
            clauses.append("invoice_number LIKE ?")
            params.append(f"%{marker}%")
        if only_regular:
            clauses.append("invoice_number GLOB '[0-9]*-*'")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM invoices {where} ORDER BY receipt_date ASC, created_at ASC LIMIT ?",
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
        elif status == "payment_sent":
            fields["zp_installer_payment_sent_at"] = to_iso(utcnow())
        elif status == "confirmed":
            fields["zp_installer_confirmed_at"] = to_iso(utcnow())
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
            "installer": "zp_installer_status IN ('requested', 'approved', 'payment_sent')",
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

    async def upsert_invoice_from_op(self, data: dict) -> tuple[int, bool]:
        """Upsert invoice from ОП sheet data.

        Returns (invoice_id, is_new).
        ОП data is authoritative — overwrites DB values for mapped fields.
        """
        inv_num = str(data.get("invoice_number") or "").strip()
        if not inv_num:
            return (0, False)

        existing = await self._get_invoice_for_sheet_import(inv_num)
        invoice_id = await self.import_invoice_from_sheet(dict(data, invoice_number=inv_num))
        return invoice_id, existing is None

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

    async def get_lead_info_for_project(self, project_id: int) -> dict[str, str]:
        """Return lead info per role for Invoices sheet cols BJ-BL.

        Returns dict with keys: 'kv', 'kia', 'npn' — each a formatted string.
        Also includes task description (RP comment) via task payload.
        """
        cur = await self.conn.execute(
            "SELECT lt.assigned_manager_role, lt.assigned_at, lt.task_id "
            "FROM lead_tracking lt "
            "WHERE lt.project_id = ? "
            "ORDER BY lt.assigned_at ASC",
            (project_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            return {}

        return await self._format_lead_rows(rows)

    async def get_lead_info_for_invoice(self, invoice: dict) -> dict[str, str]:
        """Return lead info per role for Invoices sheet.

        Returns dict with keys:
        - kv, kia, npn: lead dates (Лид КВ/КИА/НПН)
        - inv_kv, inv_kia, inv_npn: invoice issued dates (Счет КВ/КИА/НПН)
        - lead_status: текущий статус лида

        Tries project_id first, then falls back to matching by
        created_by == assigned_manager_id + creator_role == assigned_manager_role.
        """
        rows: list[dict] = []

        # 1) По project_id (если есть)
        project_id = invoice.get("project_id")
        if project_id:
            cur = await self.conn.execute(
                "SELECT lt.assigned_manager_role, lt.assigned_at, lt.task_id, "
                "lt.status, lt.invoice_issued_at, lt.lead_source "
                "FROM lead_tracking lt "
                "WHERE lt.project_id = ? "
                "ORDER BY lt.assigned_at ASC",
                (int(project_id),),
            )
            rows = [dict(r) for r in await cur.fetchall()]

        # 2) Fallback: по менеджеру, создавшему счёт
        if not rows:
            created_by = invoice.get("created_by")
            creator_role = invoice.get("creator_role")
            if created_by and creator_role:
                cur = await self.conn.execute(
                    "SELECT lt.assigned_manager_role, lt.assigned_at, lt.task_id, "
                    "lt.status, lt.invoice_issued_at, lt.lead_source "
                    "FROM lead_tracking lt "
                    "WHERE lt.assigned_manager_id = ? AND lt.assigned_manager_role = ? "
                    "ORDER BY lt.assigned_at DESC LIMIT 1",
                    (int(created_by), creator_role),
                )
                rows = [dict(r) for r in await cur.fetchall()]

        if not rows:
            return {}

        return await self._format_lead_rows(rows)

    async def _format_lead_rows(self, rows: list[dict]) -> dict[str, str]:
        """Format lead_tracking rows into sheet-ready dict.

        Returns:
        - kv, kia, npn: lead dates grouped by day (count if >1)
        - inv_kv, inv_kia, inv_npn: invoice_issued dates per role
        - lead_status: latest status
        """
        from collections import defaultdict
        from datetime import datetime as _dt

        role_key_map = {"manager_kv": "kv", "manager_kia": "kia", "manager_npn": "npn"}
        # {role_key: {date_str: count}}
        lead_dates: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        inv_dates: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        lead_sources: dict[str, str] = {}  # {role_key: source}
        latest_status = "lead"

        for row in rows:
            role_raw = row.get("assigned_manager_role", "")
            key = role_key_map.get(role_raw)
            if not key:
                continue

            # Lead source (первый непустой)
            if key not in lead_sources:
                src = row.get("lead_source") or ""
                if src:
                    lead_sources[key] = src

            # Lead date
            at = row.get("assigned_at") or ""
            if at:
                try:
                    date_str = _dt.fromisoformat(at).strftime("%d.%m.%Y")
                except (ValueError, TypeError):
                    date_str = at[:10] if len(at) >= 10 else at
                if date_str:
                    lead_dates[key][date_str] += 1

            # Invoice issued date (grouped by day, count)
            inv_at = row.get("invoice_issued_at") or ""
            if inv_at:
                try:
                    inv_date_str = _dt.fromisoformat(inv_at).strftime("%d.%m.%Y")
                except (ValueError, TypeError):
                    inv_date_str = inv_at[:10] if len(inv_at) >= 10 else inv_at
                if inv_date_str:
                    inv_dates[f"inv_{key}"][inv_date_str] += 1

            # Status
            row_status = row.get("status") or "lead"
            if row_status == "invoice_issued":
                latest_status = "invoice_issued"

        result: dict[str, str] = {}

        # Lead dates (grouped by day, count)
        for key, dates in lead_dates.items():
            parts = []
            for dt_str, cnt in dates.items():
                if cnt > 1:
                    parts.append(f"{dt_str} ({cnt})")
                else:
                    parts.append(dt_str)
            result[key] = "\n".join(parts)

        # Invoice issued dates (grouped by day, count)
        for key, dates in inv_dates.items():
            parts = []
            for dt_str, cnt in dates.items():
                if cnt > 1:
                    parts.append(f"{dt_str} ({cnt})")
                else:
                    parts.append(dt_str)
            result[key] = "\n".join(parts)

        # Lead sources
        for key, src in lead_sources.items():
            result[f"source_{key}"] = src

        # Status
        result["lead_status"] = latest_status

        return result

    async def get_zamery_info_for_project(self, project_id: int) -> str:
        """Return zamery info string for Invoices sheet col BP."""
        from datetime import datetime as _dt
        cur = await self.conn.execute(
            "SELECT zr.address, zr.total_cost, zr.scheduled_date, zr.created_at "
            "FROM zamery_requests zr "
            "JOIN lead_tracking lt ON lt.id = zr.lead_id "
            "WHERE lt.project_id = ? "
            "ORDER BY zr.created_at ASC",
            (project_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            return ""
        parts = []
        for r in rows:
            date_raw = r.get("scheduled_date") or r.get("created_at") or ""
            date_str = ""
            if date_raw:
                try:
                    date_str = _dt.fromisoformat(date_raw).strftime("%d.%m.%Y")
                except (ValueError, TypeError):
                    date_str = str(date_raw)[:10]
            addr = r.get("address") or ""
            cost = r.get("total_cost")
            cost_str = f"{int(cost)}₽" if cost else ""
            line = " | ".join(p for p in [date_str, addr, cost_str] if p)
            if line:
                parts.append(line)
        return "\n".join(parts)

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
        invoice_id: int | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO edo_requests
                (request_type, requested_by, requested_by_role, assigned_to,
                 invoice_number, description, comment, task_id, invoice_id,
                 status, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (request_type, requested_by, requested_by_role, assigned_to,
             invoice_number, description, comment, task_id, invoice_id,
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
            result[row["status"]] = row["cnt"]
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
        # Авто-расчёт времени обработки
        processing_minutes: int | None = None
        edo = await self.get_edo_request(edo_id)
        if edo and edo.get("created_at"):
            from datetime import datetime
            try:
                created = datetime.fromisoformat(edo["created_at"])
                completed = datetime.fromisoformat(now)
                processing_minutes = int((completed - created).total_seconds() / 60)
            except (ValueError, TypeError):
                pass
        fields: dict[str, Any] = {
            "status": "done",
            "response_type": response_type,
            "responded_by": responder_id,
            "response_comment": response_comment,
            "response_attachments_json": response_attachments_json,
            "completed_at": now,
        }
        if processing_minutes is not None:
            fields["processing_time_minutes"] = processing_minutes
        await self.update_edo_request(edo_id, **fields)

    async def list_invoices_for_edo(
        self, created_by: int, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Счета менеджера для ЭДО: в работе, не ended, не дочерние, не кредитные (#22)."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE created_by = ? "
            "AND status NOT IN ('new', 'rejected', 'ended', 'credit') "
            "AND (is_credit = 0 OR is_credit IS NULL) "
            "AND parent_invoice_id IS NULL "
            "ORDER BY updated_at DESC LIMIT ?",
            (created_by, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

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

        # Atomic: only update rows where response_at IS NULL to avoid race conditions
        cur = await self.conn.execute(
            "SELECT assigned_at FROM lead_tracking WHERE id = ? AND response_at IS NULL",
            (lead_id,),
        )
        row = await cur.fetchone()
        if not row:
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
            "UPDATE lead_tracking SET response_at = ?, processing_time_minutes = ? "
            "WHERE id = ? AND response_at IS NULL",
            (now, processing_time_minutes, lead_id),
        )
        await self.conn.commit()

    async def link_lead_tracking(
        self,
        lead_id: int,
        *,
        task_id: int | None = None,
        project_id: int | None = None,
        invoice_id: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {}
        if task_id is not None:
            fields["task_id"] = task_id
        if project_id is not None:
            fields["project_id"] = project_id
        if invoice_id is not None:
            fields["invoice_id"] = invoice_id
        if not fields:
            return

        set_clause = ", ".join(f"{key} = ?" for key in fields)
        values = [*fields.values(), lead_id]
        await self.conn.execute(
            f"UPDATE lead_tracking SET {set_clause} WHERE id = ?",
            tuple(values),
        )
        await self.conn.commit()

    async def update_lead_to_invoice_issued(
        self, project_id: int, invoice_id: int,
        *,
        manager_id: int | None = None,
        manager_role: str | None = None,
    ) -> None:
        """Лид → 'счет выставлен': привязка к счёту, фиксация даты.

        Если записи lead_tracking нет — создаёт её (привязка менеджера
        к счёту на этапе выставления).
        """
        now = to_iso(utcnow())

        # Проверяем есть ли уже запись
        cur = await self.conn.execute(
            "SELECT id FROM lead_tracking WHERE project_id = ?",
            (project_id,),
        )
        existing = await cur.fetchone()

        if existing:
            # Обновить существующий лид
            await self.conn.execute(
                "UPDATE lead_tracking SET status = 'invoice_issued', "
                "invoice_id = ?, invoice_issued_at = ? "
                "WHERE project_id = ?",
                (invoice_id, now, project_id),
            )
        else:
            # Создать запись — привязка менеджера к счёту при выставлении
            await self.conn.execute(
                "INSERT INTO lead_tracking "
                "(project_id, assigned_manager_id, assigned_manager_role, "
                "assigned_at, status, invoice_id, invoice_issued_at) "
                "VALUES (?, ?, ?, ?, 'invoice_issued', ?, ?)",
                (project_id, manager_id, manager_role, now, invoice_id, now),
            )

        await self.conn.commit()

    # ---------- Кредитный учёт ----------

    async def add_credit_expense(
        self,
        invoice_id: int,
        amount: float,
        description: str,
        entered_by: int,
        chat_message_id: int | None = None,
    ) -> int:
        """Добавить расход кредитных средств по счёту."""
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO credit_expenses "
            "(invoice_id, amount, description, entered_by, chat_message_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (invoice_id, amount, description, entered_by, chat_message_id, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_credit_expenses_summary(self, invoice_id: int) -> dict[str, Any]:
        """Получить сводку расходов кредитных средств по счёту.

        Returns: {"total": float, "log": str, "items": list[dict]}
        """
        cur = await self.conn.execute(
            "SELECT amount, description, created_at "
            "FROM credit_expenses WHERE invoice_id = ? "
            "ORDER BY created_at ASC",
            (invoice_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            return {"total": 0, "log": "", "items": []}

        total = sum(r["amount"] for r in rows)
        from datetime import datetime as _dt
        log_parts = []
        for r in rows:
            dt_str = ""
            if r.get("created_at"):
                try:
                    dt_str = _dt.fromisoformat(r["created_at"]).strftime("%d.%m.%Y")
                except (ValueError, TypeError):
                    dt_str = r["created_at"][:10]
            desc = r.get("description") or "—"
            log_parts.append(f"{dt_str}: {r['amount']:,.0f}₽ — {desc}")

        return {"total": total, "log": "\n".join(log_parts), "items": rows}

    async def list_leads(
        self,
        assigned_manager_id: int | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if assigned_manager_id is not None:
            clauses.append("assigned_manager_id = ?")
            params.append(assigned_manager_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM lead_tracking {where} ORDER BY assigned_at DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_lead_tracking(self, lead_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM lead_tracking WHERE id = ?", (lead_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def cancel_lead(self, lead_id: int) -> dict[str, Any] | None:
        """Delete lead and all related entities (task, LEAD-invoice, project)."""
        lead = await self.get_lead_tracking(lead_id)
        if not lead:
            return None
        task_id = lead.get("task_id")
        project_id = lead.get("project_id")
        # delete task + attachments
        if task_id:
            await self.conn.execute("DELETE FROM attachments WHERE task_id = ?", (task_id,))
            await self.conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        # delete LEAD-invoice
        await self.conn.execute(
            "DELETE FROM invoices WHERE invoice_number = ?", (f"LEAD-{lead_id}",)
        )
        # delete project (only if status='lead')
        if project_id:
            await self.conn.execute(
                "DELETE FROM projects WHERE id = ? AND status = 'lead'", (project_id,)
            )
        # delete lead_tracking
        await self.conn.execute("DELETE FROM lead_tracking WHERE id = ?", (lead_id,))
        await self.conn.commit()
        return lead

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
    # ZAMERY REQUESTS
    # =====================================================================

    async def create_zamery_request(
        self,
        source_type: str,
        address: str,
        requested_by: int,
        requester_role: str,
        assigned_to: int,
        description: str | None = None,
        client_contact: str | None = None,
        lead_id: int | None = None,
        lead_task_id: int | None = None,
        task_id: int | None = None,
        attachments_json: str | None = None,
        mkad_km: float | None = None,
        volume_m2: float | None = None,
        base_cost: int | None = None,
        mkad_surcharge: int | None = None,
        total_cost: int | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO zamery_requests "
            "(source_type, address, description, client_contact, "
            " requested_by, requester_role, assigned_to, "
            " lead_id, lead_task_id, task_id, attachments_json, "
            " mkad_km, volume_m2, base_cost, mkad_surcharge, total_cost, "
            " status, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (source_type, address, description, client_contact,
             requested_by, requester_role, assigned_to,
             lead_id, lead_task_id, task_id, attachments_json,
             mkad_km, volume_m2, base_cost, mkad_surcharge, total_cost,
             "open", now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_zamery_request(self, zamery_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM zamery_requests WHERE id = ?", (zamery_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def list_zamery_requests(
        self,
        requested_by: int | None = None,
        assigned_to: int | None = None,
        status: str | None = None,
        limit: int = 30,
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
            f"SELECT * FROM zamery_requests {where} ORDER BY created_at DESC LIMIT ?",
            tuple(params),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def update_zamery_request(self, zamery_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = to_iso(utcnow())
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [zamery_id]
        await self.conn.execute(
            f"UPDATE zamery_requests SET {sets} WHERE id = ?", tuple(vals),
        )
        await self.conn.commit()

    async def get_zamery_stats_by_manager(
        self, assigned_to: int,
    ) -> list[dict[str, Any]]:
        """Статистика заявок на замер по ролям менеджеров."""
        cur = await self.conn.execute(
            "SELECT requester_role, "
            "  COUNT(*) AS total, "
            "  SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done, "
            "  SUM(CASE WHEN status IN ('open','in_progress') THEN 1 ELSE 0 END) AS active "
            "FROM zamery_requests WHERE assigned_to = ? "
            "GROUP BY requester_role ORDER BY total DESC",
            (assigned_to,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_zamery_conversion_stats(
        self, assigned_to: int,
    ) -> dict[str, Any]:
        """Конверсия замеров → счета.

        Возвращает:
        - total_done: всего завершённых замеров
        - total_with_invoice: из них привязаны к счёту в работе
        - conversion_pct: процент конверсии
        - by_role: [{requester_role, done, with_invoice, pct}]
        """
        # Общая статистика
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS total_done FROM zamery_requests "
            "WHERE assigned_to = ? AND status = 'done'",
            (assigned_to,),
        )
        row = await cur.fetchone()
        total_done = dict(row)["total_done"] if row else 0

        # Замеры привязанные к лиду → лид стал счётом (через project_id)
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM zamery_requests zr "
            "WHERE zr.assigned_to = ? AND zr.status = 'done' "
            "AND zr.lead_id IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM lead_tracking lt "
            "  JOIN invoices i ON i.project_id = lt.project_id "
            "  WHERE lt.id = zr.lead_id "
            "  AND lt.project_id IS NOT NULL "
            "  AND i.status IN ('in_progress','paid','closing','ended'))",
            (assigned_to,),
        )
        row = await cur.fetchone()
        total_with_invoice = dict(row)["cnt"] if row else 0

        conversion_pct = round(total_with_invoice / total_done * 100) if total_done else 0

        # По ролям менеджеров
        cur = await self.conn.execute(
            "SELECT requester_role, "
            "  COUNT(*) AS done, "
            "  SUM(CASE WHEN lead_id IS NOT NULL AND EXISTS ("
            "    SELECT 1 FROM lead_tracking lt JOIN invoices i ON i.project_id = lt.project_id "
            "    WHERE lt.id = zr.lead_id AND lt.project_id IS NOT NULL "
            "    AND i.status IN ('in_progress','paid','closing','ended')"
            "  ) THEN 1 ELSE 0 END) AS with_invoice "
            "FROM zamery_requests zr "
            "WHERE zr.assigned_to = ? AND zr.status = 'done' "
            "GROUP BY requester_role",
            (assigned_to,),
        )
        by_role = []
        for r in await cur.fetchall():
            rd = dict(r)
            rd["pct"] = round(rd["with_invoice"] / rd["done"] * 100) if rd["done"] else 0
            by_role.append(rd)

        return {
            "total_done": total_done,
            "total_with_invoice": total_with_invoice,
            "conversion_pct": conversion_pct,
            "by_role": by_role,
        }

    # ----- График замеров (schedule / blackout) ----- #

    async def list_zamery_for_schedule(
        self,
        assigned_to: int,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """Замеры с датой в диапазоне + имя менеджера."""
        cur = await self.conn.execute(
            "SELECT zr.*, u.full_name AS manager_name "
            "FROM zamery_requests zr "
            "LEFT JOIN users u ON u.telegram_id = zr.requested_by "
            "WHERE zr.assigned_to = ? "
            "  AND zr.scheduled_date IS NOT NULL "
            "  AND zr.scheduled_date BETWEEN ? AND ? "
            "  AND zr.status IN ('open', 'in_progress', 'done') "
            "ORDER BY zr.scheduled_date, zr.scheduled_time_interval",
            (assigned_to, date_from, date_to),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_zamery_blackout_dates(
        self,
        user_id: int,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT * FROM zamery_blackout_dates "
            "WHERE user_id = ? AND blackout_date BETWEEN ? AND ? "
            "ORDER BY blackout_date",
            (user_id, date_from, date_to),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def add_zamery_blackout_date(
        self,
        user_id: int,
        blackout_date: str,
        comment: str | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO zamery_blackout_dates (user_id, blackout_date, comment, created_at) "
            "VALUES (?, ?, ?, ?)",
            (user_id, blackout_date, comment, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def remove_zamery_blackout_date(self, blackout_id: int) -> None:
        await self.conn.execute(
            "DELETE FROM zamery_blackout_dates WHERE id = ?", (blackout_id,),
        )
        await self.conn.commit()

    async def get_zamery_schedule_summary(
        self,
        assigned_to: int,
        date_from: str,
        date_to: str,
    ) -> dict[str, Any]:
        """Сводка графика для менеджера: занятые слоты + blackout."""
        zamery = await self.list_zamery_for_schedule(assigned_to, date_from, date_to)
        # Resolve zamery user_id for blackouts
        blackouts = await self.list_zamery_blackout_dates(assigned_to, date_from, date_to)

        busy: dict[str, list[str]] = {}  # date → [intervals]
        for z in zamery:
            d = z["scheduled_date"]
            interval = z.get("scheduled_time_interval") or "—"
            busy.setdefault(d, []).append(interval)

        blackout_set = {b["blackout_date"] for b in blackouts}

        return {
            "busy": busy,
            "blackout_dates": blackout_set,
            "zamery": zamery,
            "blackouts": blackouts,
        }

    async def import_zamery_invoices(
        self,
        records: list[dict[str, str]],
        zamery_user_id: int,
    ) -> int:
        """One-time import of zamery as invoices with zp_status='not_requested'.

        Each record: {"invoice_number": ..., "object_address": ..., "client_contact": ...}
        Returns number of inserted rows.
        """
        now = to_iso(utcnow())
        count = 0
        for rec in records:
            invoice_number = str(rec.get("invoice_number", "")).strip()
            object_address = str(rec.get("object_address", "")).strip()
            client_contact = str(rec.get("client_contact", "")).strip() or None

            if not invoice_number or not object_address:
                log.warning(
                    "import_zamery_invoices: skip invalid record invoice_number=%r object_address=%r",
                    rec.get("invoice_number"),
                    rec.get("object_address"),
                )
                continue

            # Skip if already imported (by invoice_number)
            cur = await self.conn.execute(
                "SELECT id FROM invoices WHERE invoice_number = ?",
                (invoice_number,),
            )
            if await cur.fetchone():
                continue
            await self.conn.execute(
                "INSERT INTO invoices "
                "(invoice_number, object_address, client_contact, "
                " created_by, creator_role, assigned_to, "
                " status, zp_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'zamery', ?, 'ended', 'not_requested', ?, ?)",
                (
                    invoice_number,
                    object_address,
                    client_contact,
                    zamery_user_id,
                    zamery_user_id,
                    now,
                    now,
                ),
            )
            count += 1
        if count:
            await self.conn.commit()
            log.info("import_zamery_invoices: inserted %d zamery records", count)
        return count

    async def list_open_lead_tasks_for_manager(
        self, manager_id: int, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Открытые LEAD_TO_PROJECT задачи для этого менеджера."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks "
            "WHERE assigned_to = ? AND type = 'lead_to_project' "
            "AND status IN ('open', 'in_progress') "
            "ORDER BY created_at DESC LIMIT ?",
            (manager_id, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    # =====================================================================
    # ONE-TIME DATA FIX: assign invoices to managers by marker
    # =====================================================================

    async def assign_invoices_by_marker(self, marker_map: dict[str, int]) -> int:
        """Привязать счета к менеджерам по маркировке в номере счёта.

        marker_map: {"КИА": manager_kia_id, "КВ": manager_kv_id, "НПН": manager_npn_id}
        Returns number of updated rows.
        """
        total = 0
        now = to_iso(utcnow())
        for marker, manager_id in marker_map.items():
            if not manager_id:
                continue
            creator_role = self._infer_invoice_creator_role(marker)
            cur = await self.conn.execute(
                "UPDATE invoices SET created_by = ?, creator_role = ?, updated_at = ? "
                "WHERE invoice_number LIKE ? "
                "AND parent_invoice_id IS NULL "
                "AND (created_by IS NULL OR created_by != ? OR creator_role IS NULL OR creator_role != ?)",
                (manager_id, creator_role, now, f"%{marker}%", manager_id, creator_role),
            )
            total += cur.rowcount
        if total:
            await self.conn.commit()
            log.info("assign_invoices_by_marker: updated %d invoices", total)
        return total

    # =====================================================================
    # RAZMERY REQUESTS (проверка размеров стекла)
    # =====================================================================

    async def create_razmery_request(
        self,
        invoice_id: int,
        installer_id: int,
        comment: str | None = None,
    ) -> int:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO razmery_requests "
            "(invoice_id, installer_id, installer_comment, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?, ?)",
            (invoice_id, installer_id, comment, now, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_razmery_request(self, req_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM razmery_requests WHERE id = ?", (req_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_active_razmery_request(self, invoice_id: int) -> dict[str, Any] | None:
        """Последний не-approved razmery_request для счёта."""
        cur = await self.conn.execute(
            "SELECT * FROM razmery_requests "
            "WHERE invoice_id = ? AND status NOT IN ('approved') "
            "ORDER BY id DESC LIMIT 1",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def list_razmery_requests_for_rp(self, limit: int = 30) -> list[dict[str, Any]]:
        """Все активные razmery_requests (для РП inbox)."""
        cur = await self.conn.execute(
            "SELECT r.*, i.invoice_number, i.object_address "
            "FROM razmery_requests r "
            "JOIN invoices i ON i.id = r.invoice_id "
            "WHERE r.status NOT IN ('approved') "
            "ORDER BY CASE r.status "
            "  WHEN 'pending' THEN 1 "
            "  WHEN 'error' THEN 2 "
            "  WHEN 'rp_received' THEN 3 "
            "  WHEN 'verification_sent' THEN 4 "
            "  ELSE 5 END, r.created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def update_razmery_request(self, req_id: int, **fields: Any) -> None:
        fields["updated_at"] = to_iso(utcnow())
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [req_id]
        await self.conn.execute(
            f"UPDATE razmery_requests SET {sets} WHERE id = ?", vals,
        )
        await self.conn.commit()

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
