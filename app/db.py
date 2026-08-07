from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import aiosqlite

from .enums import InvoiceStatus, Role
from .utils import (
    ZP_FACT_STATUSES,
    compute_plan_profit,
    fact_installation,
    manager_zp_net_payout,
    parse_roles,
    roles_to_storage,
    to_iso,
    utcnow,
)

log = logging.getLogger(__name__)

# НДС/налог факт «вариант-2» (формула AZ «Налоги факт» «Импорт ОП») применяется к счетам
# с receipt_date >= этой даты — «строка 21 и ниже» (= счёт 2654, 2026-05-04) — у которых
# заполнены металл (DP cost_metal) и/или стекло (DQ cost_glass). Для них НДС = (Сумма −
# металл − стекло)×22/122, налог = (Сумма − МатериалыФакт − МонтажФакт − Грузчики −
# Логистика − НДС)×20% (точно по формуле AZ). Счета до отсечки и без металла/стекла
# (напр. 26423) — старая база (совпадает с AZ строк 1–20). user 2026-06-17 (скрин формулы).
NDS_V2_CUTOFF = "2026-05-04"


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _compute_expense_total_new(row: dict[str, Any]) -> float:
    """Аггрегат «Расходы Итого» для op_company_monthly.

    Семантика (журнал BH-BQ, ред. 2026-05-12 v2):
      expense_cashless (BJ) + expense_other (BP).
    НЕ включаются:
      - expense_taxes (BM)  — налоги ведутся отдельной статьёй;
      - expense_nds (BK)    — выделенный НДС из cashless;
      - loan_net (BN)       — финансирование, не P&L.

    Fallback: если все три новых поля NULL — берётся legacy expense_total.
    """
    cashless = row.get("expense_cashless")
    taxes = row.get("expense_taxes")
    other = row.get("expense_other")
    if cashless is None and taxes is None and other is None:
        return float(row.get("expense_total") or 0)
    return float(cashless or 0) + float(other or 0)


# ── РП оклад → аванс (A2): маркеры взаимоисключения «один оклад в месяц» ──
RP_SALARY_MONTHLY = 66_000  # оклад РП/мес (синхронно td.py:42, rp.py RP_SALARY_MONTHLY_AMOUNT)
RP_OKLAD_ADVANCE_DESC = "ЗП РП Нижельченко"  # op_company_entries.description (кол. E БК):
#   маркер «оклад РП за месяц переведён в кошелёк аванса». ГД-выплата оклада пишет
#   description LIKE 'Оклад РП%' — детекторы не пересекаются (префиксы «ЗП»/«Оклад»).

# ── Зачёт выданного аванса РП в оклад (ТЗ owner 31.07) ──
# Аванс лежит в кошельке «телом» (выдан из кред-кошелька, без надбавки), а оклад идёт
# б/н самозанятому и уже содержит +10% (66 000 = 60 000 + 10%). Чтобы вычитать одно из
# другого, тело аванса приводим к тому же виду: 30 000 → 33 000.
RP_OKLAD_ADVANCE_GROSSUP = 1.1
# request_type строки-гашения. Через installer_advance_items гасить НЕЛЬЗЯ: там
# invoice_id NOT NULL REFERENCES invoices(id) при PRAGMA foreign_keys=ON, а у оклада
# счёта нет; сентинелы -1/-2 из сумм зачётов исключены и баланс не гасят вовсе.
RP_OKLAD_OFFSET_TYPE = "oklad_offset"
RP_OKLAD_OFFSET_DESC = "Зачёт аванса в оклад РП"  # + ' {YYYY-MM}' — маркер идемпотентности


class OkladAlreadyPaidError(Exception):
    """Оклад за месяц уже выплачен ГД (есть op_company_entries 'Оклад РП%')."""


class OkladAmountExceedsRemainingError(Exception):
    """Сумма перевода превышает остаток оклада за месяц (хранит .remaining)."""

    def __init__(self, remaining: float) -> None:
        self.remaining = float(remaining)
        super().__init__(f"amount exceeds remaining {remaining}")


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
                edo_task_id INTEGER,  -- переназначен: метка монт. группы (2=Наёмники, иначе NULL). ЭДО это поле не использует.

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

            -- Взаиморасчёты с замерщиком: ручной леджер платежей + нач. долг.
            -- Начисления НЕ дублируем — они берутся из zamery_requests(status='done').
            -- kind: 'opening' (нач. долг, +), 'payment' (оплата, −), 'adjustment' (правка, ±).
            CREATE TABLE IF NOT EXISTS zamery_settlement_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                surveyor_id INTEGER NOT NULL,
                entry_date TEXT NOT NULL,
                kind TEXT NOT NULL,
                amount REAL NOT NULL,
                comment TEXT,
                created_by INTEGER,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_zam_settle_surveyor
                ON zamery_settlement_entries(surveyor_id, entry_date);

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

            -- credit_spends: реестр трат кредитного кошелька (TZ 02.06).
            -- Любая трата списывает кошелёк wallet_role (manager_kv/kia/npn):
            --   привязка к счёту   → cost_*/DP–DV (supplier_payment_id),
            --   без привязки       → «Баланс компании» I/J (op_entry_id).
            -- Остаток = Σ(amount−долг по кредит-счетам роли) − Σ credit_spends(role).
            CREATE TABLE IF NOT EXISTS credit_spends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_role TEXT NOT NULL,
                amount REAL NOT NULL,
                cost_type TEXT,
                description TEXT,
                bound_invoice_id INTEGER REFERENCES invoices(id),
                supplier_payment_id INTEGER,
                op_entry_id INTEGER,
                entered_by INTEGER NOT NULL,
                chat_message_id INTEGER REFERENCES chat_messages(id),
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_credit_spends_wallet ON credit_spends(wallet_role);

            -- credit_incomes: реестр приходов кредит-кошелька (п.3 2026-06-12).
            -- Гашение долга (AC «Оконч.доплата» → ↓outstanding_debt) по is_credit-
            -- счёту даёт DISTINCT строку в истории движений. НЕ участвует в расчёте
            -- остатка (остаток = Σ(amount−долг по кредит-счетам) − Σ credit_spends,
            -- растёт сам при ↓долга); таблица лишь разбивает агрегатный «приход»
            -- на base (оплачено при создании) + per-доплату. Σ строк = amount−долг.
            CREATE TABLE IF NOT EXISTS credit_incomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_role TEXT NOT NULL,
                invoice_id INTEGER REFERENCES invoices(id),
                amount REAL NOT NULL,
                kind TEXT,
                description TEXT,
                entered_by INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_credit_incomes_wallet ON credit_incomes(wallet_role);
            CREATE INDEX IF NOT EXISTS idx_credit_incomes_inv ON credit_incomes(invoice_id);

            -- credit_wallet_anchors: авторитетная сверка остатка кошелька (durable-фикс 19.06).
            -- total_da кошелька = последний anchor.amount + Σ(приходы после) − Σ(расходы после),
            -- независимо от того, какой кредит-счёт «последний открытый». Якорь хранится ОТДЕЛЬНО
            -- от credit_expenses (чтобы не раздувать ce_total открытого счёта → лист CV/CX/DA цел).
            CREATE TABLE IF NOT EXISTS credit_wallet_anchors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_role TEXT NOT NULL,
                amount REAL NOT NULL,
                note TEXT,
                entered_by INTEGER,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_credit_anchor_role ON credit_wallet_anchors(wallet_role, created_at);

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

            -- Помесячная аналитика компании (BI/BK/BL/BM в «Импорт ОП»: Доходы/Расходы по месяцам).
            -- Раньше эти значения хранились на «якорных» строках invoices через op_company_profit_*,
            -- что было архитектурной ошибкой (2026-05-12 миграция: переехали в отдельную таблицу).
            CREATE TABLE IF NOT EXISTS op_company_monthly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,            -- 1..12
                -- Legacy income/expense (старая схема BM=Доходы/Расходы, до 2026-05-12)
                income_cash REAL,                  -- BI у Доходы-строки (legacy)
                income_credit REAL,                -- BK у Доходы-строки (legacy)
                income_total REAL,                 -- BL у Доходы-строки (legacy)
                expense_cash REAL,                 -- BI у Расходы-строки (legacy)
                expense_credit REAL,               -- BK у Расходы-строки (legacy)
                expense_total REAL,                -- BL у Расходы-строки (legacy)
                source_invoice_income TEXT,
                source_invoice_expense TEXT,
                -- Новая схема: «Итого:»-row из журнала BH-BQ + «Итого налоги»-row
                expense_cashless REAL,             -- BJ из Итого: (расходы безнал)
                expense_nds REAL,                  -- BK из Итого: (НДС)
                expense_taxes REAL,                -- BM из «Итого налоги»-строки
                expense_other REAL,                -- BP из Итого: (наличка/прочее)
                loan_net REAL,                     -- BN из Итого: (займ нетто, ± знак)
                updated_at TEXT NOT NULL,
                UNIQUE(year, month)
            );
            CREATE INDEX IF NOT EXISTS idx_op_company_monthly_year_month
                ON op_company_monthly(year, month);

            -- Журнал per-line операционных расходов, отсутствующих в «Импорт ОП» BH-BQ.
            -- Дополняет (не дублирует) данные из листа: каждая строка соответствует
            -- ровно одному платежу/налогу/займу, отсутствующему в Импорт ОП.
            -- Семантика колонок повторяет BH-BQ листа «Импорт ОП».
            CREATE TABLE IF NOT EXISTS op_company_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                date_iso TEXT,                  -- YYYY-MM-DD для сортировки (null для агрегатов)
                date_display TEXT,              -- DD.MM.YYYY (BI или BO)
                cashless_amount REAL,           -- BJ
                nds REAL,                       -- BK
                description TEXT,               -- BL
                taxes REAL,                     -- BM (только на 'Итого налоги' rows)
                loan REAL,                      -- BN (со знаком; + входящий, − возврат)
                date_other_display TEXT,       -- BO
                other_amount REAL,              -- BP
                description_credit TEXT,        -- BQ
                source TEXT NOT NULL DEFAULT 'manual_bot_entry',  -- 'manual_bot_entry'|'system'
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_op_company_entries_year_month
                ON op_company_entries(year, month);

            CREATE TABLE IF NOT EXISTS agent_conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amo_lead_id INTEGER,
                amo_chat_id TEXT NOT NULL UNIQUE,
                amo_conversation_ref_id TEXT,
                source_channel TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                collected_fields_json TEXT,
                model TEXT,
                total_input_tokens INTEGER NOT NULL DEFAULT 0,
                total_cache_read_tokens INTEGER NOT NULL DEFAULT 0,
                total_output_tokens INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                last_msg_at TEXT,
                ended_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_agent_conv_lead ON agent_conversations(amo_lead_id);
            CREATE INDEX IF NOT EXISTS idx_agent_conv_status ON agent_conversations(status);

            CREATE TABLE IF NOT EXISTS agent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL REFERENCES agent_conversations(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                content_json TEXT NOT NULL,
                amo_message_id TEXT,
                ts TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_agent_msg_conv ON agent_messages(conversation_id, ts);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_msg_amo_id
                ON agent_messages(amo_message_id) WHERE amo_message_id IS NOT NULL;

            -- ТЗ 2026-05-19 блок C: авансы монтажника (whitelist Игорь tg=1072734744).
            -- requested → approved → paid; auto-offset при approve ZP по тому же счёту.
            CREATE TABLE IF NOT EXISTS installer_advance_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                installer_id INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('requested', 'approved', 'paid', 'rejected')),
                comment TEXT,
                requested_at TEXT NOT NULL,
                approved_at TEXT,
                approved_by INTEGER,
                paid_at TEXT,
                paid_by INTEGER,
                payment_file_id TEXT,
                rejected_at TEXT,
                rejected_by INTEGER,
                reject_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS installer_advance_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL REFERENCES installer_advance_requests(id) ON DELETE CASCADE,
                invoice_id INTEGER NOT NULL REFERENCES invoices(id),
                amount REAL NOT NULL,
                plan_zp_snapshot REAL,
                offset_zp_id INTEGER,
                offset_at TEXT,
                offset_amount REAL
            );
            CREATE INDEX IF NOT EXISTS idx_advance_items_invoice ON installer_advance_items(invoice_id);
            CREATE INDEX IF NOT EXISTS idx_advance_items_request ON installer_advance_items(request_id);
            CREATE INDEX IF NOT EXISTS idx_advance_requests_installer ON installer_advance_requests(installer_id);
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
            # Физический номер строки счёта в листе «Импорт ОП» (1-based).
            # Задаёт порядок строк листа Invoices: строка N Invoices = строка N
            # ОП, независимо от дат (решение owner 27.07). Раньше порядок шёл по
            # receipt_date, и одна опечатка/пустая ячейка в «Дата пост.» уносила
            # строку в конец листа — это выглядело как «данные перемешались».
            # Обновляется каждым импортом ОП (входит в sheet_fields).
            ("invoices", "op_row_index", "INTEGER"),
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
            ("invoices", "agent_payout_date_op", "TEXT"),       # AF: Дата выпл. Агент. (renamed from men_zp_payout_op 2026-05-12 — был misnamed)
            ("invoices", "rp_request_op", "REAL"),              # AQ: Запрос РП (сумма) — renamed 26.05 from npn_request_op
            ("invoices", "rp_payout_op", "REAL"),               # AR: Выдано РП (сумма) — renamed 26.05 from npn_payout_op
            ("invoices", "rp_payout_date_op", "TEXT"),          # AS: Дата РП — renamed 26.05 from npn_payout_date_op
            ("invoices", "rp_payout_advance_at", "TEXT"),       # бот-метка «10% забрано в аванс» (durable; НЕ в sheet_fields/_OP_COL_MAP → реимпорт ОП не затирает, в отличие от rp_payout_op) — фикс 07.06
            ("invoices", "taxes_fact_op", "REAL"),              # AX: Налоги факт
            # NB: op_company_profit_* / op_company_record_type были удалены 2026-05-12
            # (полная миграция «Баланс компании» → отдельная таблица op_company_monthly).
            # ALTER TABLE DROP COLUMN выполняется в migrate-скрипте; здесь миграция-добавление
            # больше не нужна. См. tz-bot-tg-balance-monthly-migration.md.
            # --- Монтажник: инициализация ЗП и отслеживание материалов ---
            ("invoices", "materials_ordered", "INTEGER DEFAULT 0"),
            # Метка монт. группы 2 (Наёмники) хранится в переназначенном edo_task_id
            # (уже есть в CREATE TABLE) — отдельный столбец montazh_group не заводим.
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
            # --- Замеры: фиксация оплаты замерщику за замер (витрина журнала Leads AH/AI) ---
            # paid_amount = сколько выплачено за этот замер (обычно = total_cost),
            # paid_date = дата платежа отчётного периода. Долг НЕ зависит от этих полей
            # (get_zamery_settlement_summary считает по total_cost + settlement_entries).
            ("zamery_requests", "paid_amount", "INTEGER"),
            ("zamery_requests", "paid_date", "TEXT"),
            # --- Оплата замеров (объединение с леджером, ТЗ 06.07): статус запроса
            # оплаты замера. 'not_requested' = в «К оплате» | 'requested' = отправлен
            # ГД («На проверке»). «Оплачено» определяется по paid_amount IS NOT NULL. ---
            ("zamery_requests", "pay_status", "TEXT DEFAULT 'not_requested'"),
            # --- График замерщика: тип отметки дня ('off'=выходной / 'busy'=день занят) ---
            ("zamery_blackout_dates", "kind", "TEXT DEFAULT 'off'"),
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
            # --- ТЗ 14.06: трекинг финального платежа по долгу ---
            ("invoices", "planned_final_payment_date", "TEXT"),            # ориент. дата фин. платежа (вводит менеджер)
            ("invoices", "final_payment_track_state", "TEXT DEFAULT ''"),  # '' | planned | overdue | paid
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
            # Последняя заметка ответственного менеджера из amoCRM (Sheet «Примечание»)
            ("leads", "last_note", "TEXT"),
            # Сверка с таблицей РП «Импорт ОП»: источник+менеджер (РП главнее для сматченных)
            ("leads", "rp_source", "TEXT"),
            ("leads", "rp_manager", "TEXT"),
            # Сверка с РП «Импорт ОП»: статус(BX) + №счёта(BY) + сделка(BU) — мост лид↔счёт
            ("leads", "rp_status", "TEXT"),
            ("leads", "rp_invoice_number", "TEXT"),
            ("leads", "rp_deal", "TEXT"),
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
            # База ЗП монтаж, которую ввёл РП ДО надбавки +10% (б/н). Хранится, чтобы
            # карточка ГД «Запрос ЗП монтажника» показала раздельно «Внёс РП» и
            # «С надбавкой +10%» точно, без потери на округлении (ТЗ owner 17.07).
            ("invoices", "montazh_base_amount", "REAL"),
            # Комментарий монтажника при «Счёт ОК»
            ("invoices", "installer_ok_comment", "TEXT"),
            # Способ оплаты (нал/безнал)
            ("invoices", "payment_method", "TEXT"),
            # Вложения от РП при назначении монтажнику (JSON)
            ("invoices", "montazh_assign_attachments_json", "TEXT"),
            # Статус заказа материалов (заказано / бланк отправлен / размеры подтверждены)
            ("invoices", "glass_order_status", "TEXT"),
            # --- Менеджерский блок: таймстемпы монтажных стадий ---
            ("invoices", "montazh_assigned_at", "TEXT"),
            ("invoices", "montazh_in_work_at", "TEXT"),
            ("invoices", "montazh_razmery_ok_at", "TEXT"),
            ("invoices", "montazh_invoice_ok_at", "TEXT"),
            # --- Аудит: подтверждение оплаты ---
            ("invoices", "payment_confirmed_by", "INTEGER"),
            ("invoices", "payment_confirmed_at", "TEXT"),
            # --- Аудит: ЗП менеджера ---
            ("invoices", "zp_manager_approved_by", "INTEGER"),
            # --- Аудит: ЭДО ---
            ("invoices", "docs_edo_signed_at", "TEXT"),
            ("invoices", "docs_edo_signed_by", "INTEGER"),
            # --- Статус заказа материалов (профиль/металл) ---
            ("invoices", "profile_order_status", "TEXT"),
            ("invoices", "metal_order_status", "TEXT"),
            # --- Связка лид→счёт ---
            ("invoices", "lead_tracking_id", "INTEGER"),
            # --- ОП CF/CG → лист Invoices CN/CO (owner 2026-06-22) ---
            # Ручной ввод owner в листе ОП (CF/CG) → парсинг → трансляция
            # на лист Invoices (CN/CO). Чистый перенос, без вычислений.
            ("invoices", "zp_manager_hold", "REAL"),       # CF: Удержать из ЗП менеджера
            ("invoices", "cost_diff_calc_fact", "REAL"),   # CG: Разница себестоимости расч/факт
            # --- Переплата ЗП менеджера → баланс аванса (owner 2026-06-23) ---
            # Сколько из |zp_manager_hold| (CN, удержание) уже перенесено на баланс
            # аванса менеджера sweep'ом (sweep_manager_overpay_to_advance). Поле
            # БОТ-локальное (НЕ пишется реимпортом ОП, в отличие от zp_manager_hold/CN)
            # → переживает реимпорт → идемпотентность: на каждом синке переносится
            # ТОЛЬКО дельта (|CN| − zp_hold_advanced). Как только перенесено — CN
            # перестаёт вычитаться пер-счётно из net-ЗП (бланк платится полностью), а
            # переплата гасится распределением аванса по объектам (правила авансир-я).
            ("invoices", "zp_hold_advanced", "REAL"),
            # Дата ПОСЛЕДНЕГО переноса переплаты по этому счёту (owner 07.08). Сумма
            # (zp_hold_advanced) на листе не показывалась вовсе — пара «сумма + дата»
            # заводится по образцу CG/CH «Аванс монтажника»/«Дата аванса». Пишется
            # ОБОИМИ каналами переноса тем же `now`, что и сама сумма, одной
            # транзакцией: sweep_manager_overpay_to_advance (авто) и
            # create_recalc_advance_topup (ручной, «С перерасчётом согласен»).
            # ⚠️ Дата ПОСЛЕДНЕГО, не первого: перенос идёт дельтами (|CN| −
            # zp_hold_advanced), и при доросшем CN сумма пополняется повторно.
            ("invoices", "zp_hold_advanced_at", "TEXT"),
            # --- MinIO mirror keys for attachments ---
            ("attachments", "minio_object_key", "TEXT"),
            ("chat_attachments", "minio_object_key", "TEXT"),
            # --- Депозит-кошелёк монтажника (TZ tingly-twirling-whistle 2026-05-25) ---
            # initiator: 'installer' (классика, запрос от Игоря) | 'gd' (депозит от ГД)
            ("installer_advance_requests", "initiator", "TEXT NOT NULL DEFAULT 'installer'"),
            # request_type: 'request' (классика) | 'deposit' (ГД пополняет) | 'withdraw' (Игорь снимает на личный расход)
            ("installer_advance_requests", "request_type", "TEXT NOT NULL DEFAULT 'request'"),
            # --- Разделение кошельков РП / Менеджер NPN (TZ 2026-05-29) ---
            # wallet_role: NULL = кошелёк роли-владельца (single-role монтажник/менеджер,
            # backward-compat); 'rp' = кошелёк РП Павла; 'manager_npn' = менеджерский
            # кошелёк Павла. Фильтр «первичного» кошелька = (NULL OR != 'rp'); РП = строго 'rp'.
            ("installer_advance_requests", "wallet_role", "TEXT"),
            # --- Кредит: тип затрат расхода (TZ кредит-журнал 2026-06-02) ---
            # metal/glass/extra_mat/install/loaders/logistics; NULL — старые/авто-записи.
            ("credit_expenses", "cost_type", "TEXT"),
            # --- ЗП монтаж: заявка = ОСТАТОК (Часть 2, user 2026-06-08) ---
            # 1 = выплата бота покрывает ОСТАТОК ЗП (аванс зачтён ОТДЕЛЬНЫМ каналом)
            #     → «Выплачено» = аванс×1.10 + бот (ADDITIVE, счёт закрывается полностью).
            # 0/NULL = старая семантика (бот платил ВСЮ согласованную, аванс «внутри»)
            #     → «Выплачено» = max(AN, аванс×1.10, бот). Старые счета не задваиваются.
            # См. _invoice_cells (sheets.py) и feedback_bs_immutable.
            ("invoices", "zp_installer_remainder", "INTEGER NOT NULL DEFAULT 0"),
            # --- Объединение платежей ЗП монтаж (owner 2026-07-15) ---
            # ЗП монтаж, выплаченная ПРОШЛЫМ монтажным группам этого счёта; учтена внутри
            # montazh_agreed_amount (Согласовано = выплаченное + новое, решение owner).
            # Нужна, потому что ячейка ЗП на счёте ОДНА: при смене группы она освобождается
            # под доплату новой группе, и факт старой выплаты жил бы только в AN (его
            # заполняют люди в «Импорт ОП», с лагом) и в DR (платёжкой не заполняется).
            # Доплата новой группе = montazh_agreed_amount − montazh_paid_prev.
            # На лист НЕ уходит: своей колонки не имеет (owner: «новый столбец не нужен»).
            ("invoices", "montazh_paid_prev", "REAL NOT NULL DEFAULT 0"),
            # Аванс монтажника (СЫРОЙ, без ×1.10), уже привязанный к счёту на момент
            # объединения. installer_advance_items копятся по СЧЁТУ, а не по группе, и
            # аванс прошлой группы уже сидит внутри montazh_paid_prev — без этой базы
            # формулы остатка вычли бы его ВТОРОЙ раз (недоплата новой группе).
            # Аванс текущей группы = get_installer_advance_for_invoice() − montazh_adv_prev.
            ("invoices", "montazh_adv_prev", "REAL NOT NULL DEFAULT 0"),
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

        # --- credit_spends: миграция истории ОТМЕНЕНА (модель carry-DA 02.06) ---
        # Под carry-DA баланс = get_credit_balance_summary (читает credit_expenses
        # напрямую). credit_spends — реестр ТОЛЬКО новых трат кошелька (назначение
        # DP–DV / «Баланс компании»), заполняется из cw_confirm. Историю
        # credit_expenses сюда НЕ копируем: данные КВ5/КВ6 ненадёжны (расходы >
        # CV, отсюда маркеры «Остаток»), назначений у них нет — в журнале они дали
        # бы события без destination и не сошлись бы с carry-DA. Таблица создаётся
        # в init_schema; на проде её ещё нет → появится пустой при первом деплое.

        # --- Миграция rp_request_op TEXT → REAL (однократно) ---
        # Столбец AQ «Запрос РП» (исторически «НПН», renamed 26.05) был TEXT, значения мешались:
        # "3520" / "3850.0" / "3 520,00". Приводим к REAL affinity + нормализация.
        cur = await self.conn.execute("PRAGMA table_info(invoices)")
        _cols_info = await cur.fetchall()
        _rp_col = next(
            (c for c in _cols_info if str(c["name"]) == "rp_request_op"),
            None,
        )
        if _rp_col and str(_rp_col["type"]).upper() != "REAL":
            await self.conn.execute(
                "ALTER TABLE invoices ADD COLUMN rp_request_op_new REAL"
            )
            await self.conn.execute(
                "UPDATE invoices SET rp_request_op_new = CASE "
                "WHEN rp_request_op IS NULL OR TRIM(rp_request_op) = '' THEN NULL "
                "ELSE CAST(REPLACE(REPLACE(rp_request_op, ' ', ''), ',', '.') AS REAL) "
                "END"
            )
            await self.conn.execute(
                "ALTER TABLE invoices DROP COLUMN rp_request_op"
            )
            await self.conn.execute(
                "ALTER TABLE invoices RENAME COLUMN rp_request_op_new TO rp_request_op"
            )
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

    async def mark_sheet_dirty(self, table: str, dirty: bool = True) -> None:
        """Set settings.sheet_dirty_<table> = '1' or '0'. Used by webhook → throttle handoff."""
        await self.set_setting(f"sheet_dirty_{table}", "1" if dirty else "0")

    async def is_sheet_dirty(self, table: str) -> bool:
        return (await self.get_setting(f"sheet_dirty_{table}")) == "1"

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

    async def list_tasks_by_creator_and_type(
        self,
        created_by: int,
        type_filter: str,
        statuses: Iterable[str] = ("open", "in_progress", "done"),
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List tasks created by a user with type filter (for idempotency checks).

        B5 v2 request-based TZ 27.05: используется в rp.py чтобы проверить,
        нет ли уже активного/выплаченного запроса оклада за текущий месяц.
        """
        statuses = list(statuses)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [created_by, type_filter, *statuses, limit]
        cur = await self.conn.execute(
            f"""
            SELECT * FROM tasks
            WHERE created_by = ? AND type = ? AND status IN ({placeholders})
            ORDER BY created_at DESC
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

    async def list_recent_tasks_all(self, limit: int = 15) -> list[dict[str, Any]]:
        """Последние задачи ВСЕХ ролей/типов/статусов — для блока «Задачи ролям»
        ГД-карточки (ТЗ 25.06): полная история. Одним запросом джойнит роль+имя
        создателя и исполнителя. Read-only витрина; новые сверху (created_at DESC).
        """
        cur = await self.conn.execute(
            """
            SELECT t.*,
                   cu.role AS creator_role, cu.full_name AS creator_name,
                   au.role AS assignee_role, au.full_name AS assignee_name
            FROM tasks t
            LEFT JOIN users cu ON t.created_by = cu.telegram_id
            LEFT JOIN users au ON t.assigned_to = au.telegram_id
            ORDER BY t.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_recent_tasks_for_user(self, user_id: int, limit: int = 9) -> list[dict[str, Any]]:
        """Последние задачи, КАСАЮЩИЕСЯ пользователя — он создал ИЛИ ему назначено
        (created_by = ? OR assigned_to = ?). Для блока «Задачи ролям» в карточке
        менеджера (ТЗ 25.06: «касаемо именно его роли»). Те же JOIN'ы роль+имя
        создателя/исполнителя, что list_recent_tasks_all; новые сверху. Read-only.
        """
        cur = await self.conn.execute(
            """
            SELECT t.*,
                   cu.role AS creator_role, cu.full_name AS creator_name,
                   au.role AS assignee_role, au.full_name AS assignee_name
            FROM tasks t
            LEFT JOIN users cu ON t.created_by = cu.telegram_id
            LEFT JOIN users au ON t.assigned_to = au.telegram_id
            WHERE t.created_by = ? OR t.assigned_to = ?
            ORDER BY t.created_at DESC
            LIMIT ?
            """,
            (user_id, user_id, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

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

    async def claim_group_tasks(
        self,
        group_id: str,
        task_type: str,
        new_status: str,
        *,
        expected_statuses: tuple[str, ...] = ("open", "in_progress"),
    ) -> int:
        """Atomically transition ALL group tasks to *new_status* (CAS over a group).

        Single-statement UPDATE over every task sharing *group_id* (same *task_type*)
        whose status is still in *expected_statuses*.  The first caller to run flips
        the rows and gets ``rowcount > 0``; a concurrent second caller finds nothing
        left in *expected_statuses* and gets ``0``.  Serializes group-level
        pay/reject across multiple GDs (см. ``claim_lead`` — тот же приём).

        Returns the number of tasks claimed (0 = already handled by another actor).
        """
        now = to_iso(utcnow())
        placeholders = ",".join("?" for _ in expected_statuses)
        cur = await self.conn.execute(
            "UPDATE tasks SET status = ?, updated_at = ? "
            "WHERE type = ? AND json_extract(payload_json, '$.group_id') = ? "
            "AND status IN ({})".format(placeholders),
            (new_status, now, task_type, group_id, *expected_statuses),
        )
        await self.conn.commit()
        return cur.rowcount

    async def reopen_group_tasks(
        self,
        group_id: str,
        task_type: str,
        payload_patch: dict[str, Any],
        *,
        fallback_task_id: int | None = None,
    ) -> int:
        """Частичная выплата ЗП РП: вернуть DONE-задачи группы в OPEN и урезать payload
        до неоплаченных счетов (merge-patch invoices/invoice_ids/total).

        Вызывается ПОСЛЕ ``claim_group_tasks(..., 'done')`` (который атомарно закрыл
        группу и сериализовал гонку ГД): если после выплаты выбранных остались
        неоплаченные счета — открываем группу заново с урезанным списком. Матчит
        строки со status='done' по group_id (или по одиночному fallback_task_id при
        пустом group_id). Возвращает число переоткрытых задач.
        """
        if group_id:
            cur = await self.conn.execute(
                "SELECT id, payload_json FROM tasks "
                "WHERE type = ? AND status = 'done' "
                "AND json_extract(payload_json, '$.group_id') = ?",
                (task_type, group_id),
            )
        elif fallback_task_id:
            cur = await self.conn.execute(
                "SELECT id, payload_json FROM tasks WHERE id = ? AND status = 'done'",
                (int(fallback_task_id),),
            )
        else:
            return 0
        rows = await cur.fetchall()
        now = to_iso(utcnow())
        n = 0
        for r in rows:
            try:
                p = json.loads(r["payload_json"] or "{}")
            except Exception:
                p = {}
            if not isinstance(p, dict):
                p = {}
            p.update(payload_patch or {})
            await self.conn.execute(
                "UPDATE tasks SET status = 'open', payload_json = ?, updated_at = ? WHERE id = ?",
                (_json_dumps(p), now, r["id"]),
            )
            n += 1
        await self.conn.commit()
        return n

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

    async def update_task_payload(
        self, task_id: int, patch: dict[str, Any]
    ) -> dict[str, Any]:
        """Merge-patch payload_json задачи (след applied/credit_spend_id и т.п.).

        Не трогает status/assignee. Используется отложенным flow §C для пометки
        исполнения (applied=True, credit_spend_id) после записи расхода.
        """
        task = await self.get_task(task_id)
        try:
            payload = json.loads(task.get("payload_json") or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.update(patch or {})
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE tasks SET payload_json = ?, updated_at = ? WHERE id = ?",
            (_json_dumps(payload), now, task_id),
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

    async def list_open_tasks_by_invoice(
        self, invoice_id: int, task_type: str
    ) -> list[dict[str, Any]]:
        """Открытые/в работе задачи типа task_type, привязанные к invoice_id
        (payload $.invoice_id). Для анти-задвоения ЗП монтажника (закрыть парную
        zp_installer при кредит-выплате)."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE type = ? AND status IN ('open', 'in_progress') "
            "AND json_extract(payload_json, '$.invoice_id') = ? ORDER BY id",
            (task_type, invoice_id),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_open_credit_payment_requests_for_invoice(
        self, invoice_id: int, cost_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Открытые кредит-заявки (type=invoice_payment, payload.kind=
        credit_payment_request), привязанные к invoice_id; опц. фильтр по cost_type.
        Для анти-задвоения: при штатной выплате ЗП монтажа отменить парную
        кредит-заявку кошелька по тому же счёту."""
        sql = (
            "SELECT * FROM tasks WHERE type = 'invoice_payment' "
            "AND status IN ('open', 'in_progress') "
            "AND json_extract(payload_json, '$.kind') = 'credit_payment_request' "
            "AND json_extract(payload_json, '$.invoice_id') = ?"
        )
        params: list[Any] = [invoice_id]
        if cost_type is not None:
            sql += " AND json_extract(payload_json, '$.cost_type') = ?"
            params.append(cost_type)
        cur = await self.conn.execute(sql + " ORDER BY id", tuple(params))
        return [dict(r) for r in await cur.fetchall()]

    async def list_open_credit_payment_requests_by_channel(
        self, channel: str
    ) -> list[dict[str, Any]]:
        """Открытые кредит-заявки (type=invoice_payment, payload.kind=
        credit_payment_request) канала менеджера — по payload.wallet_role
        ИЛИ payload.channel (COALESCE, тот же ключ, что в
        count_credit_payment_requests_by_channel). Нужен для показа в дрилл-дауне
        «Задачи» чат-меню ГД, чтобы список совпадал с 💳-бейджем на кнопке канала
        (user 04.07: «бейдж 1, а внутри пусто»)."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE type = 'invoice_payment' "
            "AND status IN ('open', 'in_progress') "
            "AND json_extract(payload_json, '$.kind') = 'credit_payment_request' "
            "AND COALESCE("
            "json_extract(payload_json, '$.wallet_role'), "
            "json_extract(payload_json, '$.channel')) = ? "
            "ORDER BY id",
            (channel,),
        )
        return [dict(r) for r in await cur.fetchall()]

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
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? AND status IN ('open', 'in_progress') "
            "AND type != 'self_reminder'",
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

    async def count_installer_deposit_tasks(self, user_id: int) -> int:
        """Кол-во открытых задач-запросов из депозита от ГД (GD_DEPOSIT_REQUEST),
        ещё не исполненных сотрудником — для бейджа 🔴 на «Запрос ЗП»/«Депозит».

        Считает OPEN (не прочитана) + IN_PROGRESS (прочитана, но не исполнена);
        DONE/REJECTED не входят — бейдж гаснет после исполнения/отклонения.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type = 'gd_deposit_request'",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def list_installer_deposit_tasks(self, user_id: int) -> list[dict[str, Any]]:
        """Открытые задачи-запросы из депозита (GD_DEPOSIT_REQUEST) для карточки «💳 Депозит».

        OPEN (не прочитана) + IN_PROGRESS (прочитана, ждёт исполнения), новые сверху.
        """
        cur = await self.conn.execute(
            "SELECT id, status, payload_json, created_by, assigned_to, created_at "
            "FROM tasks WHERE assigned_to = ? AND type = 'gd_deposit_request' "
            "AND status IN ('open', 'in_progress') ORDER BY id DESC",
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

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

    async def count_gd_more_total_open_tasks(self, user_id: int) -> int:
        """Count tasks shown in GD «📋 Все задачи» (submenu «📂 Ещё»).

        Исключаем типы, покрытые отдельными кнопками главного меню:
        - invoice_payment    → «Счета на Оплату»
        - payment_confirm    → «🏁 Счёт END»
        - invoice_end        → «🏁 Счёт END»
        - zp_installer       → «💸 Оплата поставщику»
        - self_reminder      → личная напоминалка (ставит сам ГД); не должна
                               накручивать бейдж «Все задачи» (аналогично
                               count_unread_tasks). В списке видна, в счётчике нет.

        В «Все задачи» попадают только «прочие» open задачи — чтобы избежать
        дублирования с бейджами на других кнопках.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type NOT IN ('invoice_payment', 'payment_confirm', 'invoice_end', 'zp_installer', 'self_reminder')",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_gd_supplier_pay_tasks(self, user_id: int) -> int:
        """Count pending ZP requests (zamery/manager/installer) for 'Оплата поставщику'.

        Считаем invoices с pending zp_*_status. Это покрывает и ZP_INSTALLER
        tasks (которые существуют только при pending zp_installer_status),
        и обычные ЗП-запросы замерщика/менеджера. Без double-count.

        Note: ZP requests are global (not per-user), but the parameter is kept
        for API consistency with other count_gd_* methods.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices "
            "WHERE zp_status = 'requested' "
            "   OR zp_manager_status = 'requested' "
            "   OR zp_installer_status IN ('requested', 'approved')",
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

    async def count_credit_payment_requests_for_owner(self, user_id: int) -> int:
        """Кол-во ожидающих кредит-заявок (💳N) на владельца кошелька-менеджера.

        Бейдж «💳N» на кнопке «🏦 Кредит» в меню «Ещё» менеджера КВ/КИА/НПН —
        задачи-платёжки кредита (kind=credit_payment_request), ждущие действия
        владельца. Фильтр по status (OPEN/IN_PROGRESS), НЕ по payload.applied:
        одна из двух точек создания (ГД-расход, chat_proxy) поле applied не пишет.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE assigned_to = ? "
            "AND status IN ('open', 'in_progress') "
            "AND type = 'invoice_payment' "
            "AND json_extract(payload_json, '$.kind') = 'credit_payment_request'",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def count_credit_payment_requests_by_channel(self) -> dict[str, int]:
        """Кол-во ожидающих кредит-заявок (💳N) по каналам КВ/КИА/НПН — для ГД.

        Бейдж «💳N» на ГД-субкнопках менеджеров (меню «Ещё» ГД). Ключ канала —
        payload.wallet_role (точка создания cw_confirm) ИЛИ payload.channel
        (точка создания chat_proxy, ГД-расход): COALESCE покрывает оба источника.
        Значения ключа: manager_kv / manager_kia / manager_npn (+ возможен rp).
        """
        cur = await self.conn.execute(
            "SELECT COALESCE("
            "json_extract(payload_json, '$.wallet_role'), "
            "json_extract(payload_json, '$.channel')) AS ch, COUNT(*) "
            "FROM tasks "
            "WHERE status IN ('open', 'in_progress') "
            "AND type = 'invoice_payment' "
            "AND json_extract(payload_json, '$.kind') = 'credit_payment_request' "
            "GROUP BY ch",
        )
        rows = await cur.fetchall()
        return {row[0]: row[1] for row in rows if row[0]}

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

    async def list_check_kp_history_for_manager(
        self, manager_id: int, limit: int = 30
    ) -> list[dict[str, Any]]:
        """Все CHECK_KP-задачи, отправленные менеджером РП (любой статус).

        Используется в меню «Проверить КП / Счет» → подсписок ответов РП.
        Сортировка: новые сверху, при равенстве — по id DESC.
        """
        cur = await self.conn.execute(
            "SELECT * FROM tasks WHERE created_by = ? "
            "AND type = 'check_kp' "
            "ORDER BY COALESCE(updated_at, created_at) DESC, id DESC LIMIT ?",
            (manager_id, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_check_kp_unconfirmed_for_manager(self, manager_id: int) -> int:
        """Сколько ответов РП по CHECK_KP менеджер ещё не подтвердил.

        Считаем DONE/REJECTED задачи без payload.manager_confirmed=True.
        """
        cur = await self.conn.execute(
            "SELECT payload_json FROM tasks WHERE created_by = ? "
            "AND type = 'check_kp' "
            "AND status IN ('done', 'rejected')",
            (manager_id,),
        )
        rows = await cur.fetchall()
        cnt = 0
        for r in rows:
            payload_raw = r[0] if not isinstance(r, dict) else r.get("payload_json")
            try:
                payload = json.loads(payload_raw or "{}")
            except (ValueError, TypeError):
                payload = {}
            if not payload.get("manager_confirmed"):
                cnt += 1
        return cnt

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
        self, limit: int = 50, *,
        only_regular: bool = False,
        include_credit: bool = False,
        creator_role: str | None = None,
    ) -> list[dict[str, Any]]:
        """List invoices 'in work' (pending/in_progress/paid).

        Args:
            only_regular: if True, show only regular invoices whose number
                matches DDMMYY-N... format (6 digits + dash). Everything else
                is considered credit.
            include_credit: if True, кредитные счета (is_credit=1) тоже
                включаются (даже когда only_regular=True). Используется в
                сценариях РП «подача счёта в оплату ГД», где нужны и
                обычные, и кредитные счета в работе.
        """
        # Для кредитных «в работе» = status IN (pending, in_progress, paid,
        # 'credit'). status='credit' — это открытый кредитный счёт после
        # «✅ Счёт ОК», ещё не закрытый («ended»).
        if only_regular:
            fmt_clause = (
                "AND (invoice_number GLOB '[0-9]*-*' OR is_credit = 1) "
                if include_credit else
                "AND invoice_number GLOB '[0-9]*-*' "
            )
        else:
            fmt_clause = (
                ""
                if include_credit else
                "AND (is_credit = 0 OR is_credit IS NULL) "
            )
        if include_credit:
            status_clause = (
                "(status IN ('pending', 'in_progress', 'paid') "
                "OR (is_credit = 1 AND status = 'credit'))"
            )
        else:
            status_clause = "status IN ('pending', 'in_progress', 'paid')"
        role_clause = "AND creator_role = ? " if creator_role else ""
        params: list[Any] = []
        if creator_role:
            params.append(creator_role)
        params.append(limit)
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            f"WHERE {status_clause} "
            f"{fmt_clause}"
            f"{role_clause}"
            "ORDER BY receipt_date ASC, updated_at ASC LIMIT ?",
            tuple(params),
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

    async def list_invoices_to_send_montazh(self, limit: int = 20) -> list[dict[str, Any]]:
        """Счета, доступные РП для отправки монтажнику («➕ Счёт в работу»).

        Включает кредитные (status='credit') наравне с обычными; уже
        отправленные монтажнику (montazh_stage in_work/...) исключены.
        Фильтр ДОЛЖЕН совпадать с count_invoices_to_send_montazh ниже.
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE "
            "(montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
            "AND status IN ('in_progress','paid','credit') "
            "AND parent_invoice_id IS NULL "
            # Наёмная гр. (edo_task_id=2) после ввода согласованной суммы получает
            # stage='assigned' → уходит из «Счёт в работу» (живёт в «Счета в работе»).
            # COALESCE — NULL-safe: счета Игоря (edo NULL) со stage='assigned' остаются.
            "AND NOT (COALESCE(edo_task_id, 0) = 2 AND montazh_stage = 'assigned') "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def count_invoices_to_send_montazh(self) -> int:
        """Кол-во счетов для бейджа 🔴 у кнопки «➕ Счёт в работу».

        Фильтр ДОЛЖЕН совпадать с list_invoices_to_send_montazh выше.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE "
            "(montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
            "AND status IN ('in_progress','paid','credit') "
            "AND parent_invoice_id IS NULL "
            # Синхронно с list_invoices_to_send_montazh: наёмные assigned не в бейдже 🔴N.
            "AND NOT (COALESCE(edo_task_id, 0) = 2 AND montazh_stage = 'assigned')"
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Invoice hierarchy & cost statistics
    # ------------------------------------------------------------------

    async def list_invoices_for_selection(self, limit: int = 30, *, only_regular: bool = False, include_credit: bool = False) -> list[dict[str, Any]]:
        """Счета «в работе» + «Счёт End» для inline-пикера.

        only_regular — показать только обычные счета (номер DDMMYY-N...),
        исключая кредитные и прочие.
        include_credit — пропускать кредитные (is_credit=1) даже при
        only_regular=True (у credit invoice_number обычно «КВ N» —
        без него отрезаются). Используется в РП-сценариях, где
        credit-родитель допустим.
        """
        if only_regular:
            fmt_clause = (
                "AND (invoice_number GLOB '[0-9]*-*' OR is_credit = 1) "
                if include_credit else
                "AND invoice_number GLOB '[0-9]*-*' "
            )
        else:
            fmt_clause = (
                ""
                if include_credit else
                "AND (is_credit = 0 OR is_credit IS NULL) "
            )
        # include_credit добавляет активный кредит (status='credit') к выборке —
        # иначе кредитные счета «в работе» отсекаются статус-фильтром (видны
        # только кредит-счета, дошедшие до ended). Без include_credit — без изменений.
        status_clause = (
            "WHERE status IN ('pending', 'in_progress', 'paid', 'ended', 'credit') "
            if include_credit else
            "WHERE status IN ('pending', 'in_progress', 'paid', 'ended') "
        )
        # Свежие первыми (user 2026-06-15): picker берёт LIMIT, поэтому при ASC
        # активные (в т.ч. кредитные) счета не влезали в выборку — теперь DESC.
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            f"{status_clause}"
            f"{fmt_clause}"
            "ORDER BY receipt_date DESC, updated_at DESC LIMIT ?",
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
        update_cost: bool = True,
    ) -> int:
        """Insert a row into supplier_payments table + update cost_* in invoices.

        update_cost=False — только строка платежа, БЕЗ прибавки к cost_*. Нужно
        для DS «Затр. Грузчики» (owner 25.07): столбец заполняется при ПРИНЯТИИ
        задачи ГД в работу, поэтому на шаге оплаты прибавлять второй раз нельзя.
        """
        now = datetime.now(timezone.utc).isoformat()
        cur = await self.conn.execute(
            "INSERT INTO supplier_payments "
            "(parent_invoice_id, invoice_number, amount, material_type, supplier, task_id, created_by, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (parent_invoice_id, invoice_number, amount, material_type, supplier, task_id, created_by, now),
        )
        # Update aggregated cost column in parent invoice
        cost_col = self._COST_COL_MAP.get(material_type) if update_cost else None
        if cost_col:
            await self.conn.execute(
                f"UPDATE invoices SET {cost_col} = COALESCE({cost_col}, 0) + ? WHERE id = ?",
                (amount, parent_invoice_id),
            )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def bump_invoice_cost(
        self, invoice_id: int, material_type: str, amount: float,
    ) -> bool:
        """Прибавить сумму к столбцу затрат (DP–DV) БЕЗ строки в supplier_payments.

        Для DS «Затр. Грузчики» (owner 25.07): затраты попадают в таблицу в момент
        принятия задачи ГД в работу, а строка платежа создаётся позже, при оплате.
        """
        cost_col = self._COST_COL_MAP.get(material_type)
        if not cost_col or not amount:
            return False
        await self.conn.execute(
            f"UPDATE invoices SET {cost_col} = COALESCE({cost_col}, 0) + ? WHERE id = ?",
            (float(amount), invoice_id),
        )
        await self.conn.commit()
        return True

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

    # ---------- op_company_monthly (помесячная аналитика компании) ----------

    async def upsert_monthly_op_company(
        self,
        year: int,
        month: int,
        *,
        income_cash: float | None = None,
        income_credit: float | None = None,
        income_total: float | None = None,
        expense_cash: float | None = None,
        expense_credit: float | None = None,
        expense_total: float | None = None,
        source_invoice_income: str | None = None,
        source_invoice_expense: str | None = None,
        expense_cashless: float | None = None,
        expense_nds: float | None = None,
        expense_taxes: float | None = None,
        expense_other: float | None = None,
        loan_net: float | None = None,
    ) -> None:
        """Upsert по (year, month). NULL-аргументы НЕ затирают существующие значения
        (COALESCE на стороне UPDATE). Legacy income_*/expense_* поля сохраняются
        для исторических данных (старая схема BM=Доходы/Расходы); новые поля
        expense_cashless/nds/taxes/other/loan_net приходят из «Итого:»-row журнала
        BH-BQ и «Итого налоги»-строки.
        """
        now = datetime.now(timezone.utc).isoformat()
        await self.conn.execute(
            """
            INSERT INTO op_company_monthly (
                year, month,
                income_cash, income_credit, income_total,
                expense_cash, expense_credit, expense_total,
                source_invoice_income, source_invoice_expense,
                expense_cashless, expense_nds, expense_taxes, expense_other, loan_net,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, month) DO UPDATE SET
                income_cash            = COALESCE(excluded.income_cash, income_cash),
                income_credit          = COALESCE(excluded.income_credit, income_credit),
                income_total           = COALESCE(excluded.income_total, income_total),
                expense_cash           = COALESCE(excluded.expense_cash, expense_cash),
                expense_credit         = COALESCE(excluded.expense_credit, expense_credit),
                expense_total          = COALESCE(excluded.expense_total, expense_total),
                source_invoice_income  = COALESCE(excluded.source_invoice_income, source_invoice_income),
                source_invoice_expense = COALESCE(excluded.source_invoice_expense, source_invoice_expense),
                expense_cashless       = COALESCE(excluded.expense_cashless, expense_cashless),
                expense_nds            = COALESCE(excluded.expense_nds, expense_nds),
                expense_taxes          = COALESCE(excluded.expense_taxes, expense_taxes),
                expense_other          = COALESCE(excluded.expense_other, expense_other),
                loan_net               = COALESCE(excluded.loan_net, loan_net),
                updated_at             = excluded.updated_at
            """,
            (
                year, month,
                income_cash, income_credit, income_total,
                expense_cash, expense_credit, expense_total,
                source_invoice_income, source_invoice_expense,
                expense_cashless, expense_nds, expense_taxes, expense_other, loan_net,
                now,
            ),
        )
        await self.conn.commit()

    # ---------- op_company_entries (per-line, дополняет «Импорт ОП») ----------

    async def add_op_company_entry(
        self,
        *,
        year: int,
        month: int,
        date_display: str | None = None,
        date_iso: str | None = None,
        cashless_amount: float | None = None,
        nds: float | None = None,
        description: str | None = None,
        taxes: float | None = None,
        loan: float | None = None,
        date_other_display: str | None = None,
        other_amount: float | None = None,
        description_credit: str | None = None,
        source: str = "manual_bot_entry",
    ) -> int:
        """Вставить per-line запись операционного расхода (НЕ дублирует Импорт ОП)."""
        now = datetime.now(timezone.utc).isoformat()
        cur = await self.conn.execute(
            """
            INSERT INTO op_company_entries (
                year, month, date_iso, date_display,
                cashless_amount, nds, description, taxes, loan,
                date_other_display, other_amount, description_credit,
                source, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                year, month, date_iso, date_display,
                cashless_amount, nds, description, taxes, loan,
                date_other_display, other_amount, description_credit,
                source, now,
            ),
        )
        await self.conn.commit()
        return cur.lastrowid or 0

    async def list_op_company_entries(
        self, year: int | None = None, month: int | None = None,
    ) -> list[dict[str, Any]]:
        """Все записи (опц. фильтр по году/месяцу), сортировка по дате."""
        if year is not None and month is not None:
            cur = await self.conn.execute(
                "SELECT * FROM op_company_entries WHERE year = ? AND month = ? "
                "ORDER BY COALESCE(date_iso, date_display), id",
                (year, month),
            )
        elif year is not None:
            cur = await self.conn.execute(
                "SELECT * FROM op_company_entries WHERE year = ? "
                "ORDER BY year, month, COALESCE(date_iso, date_display), id",
                (year,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM op_company_entries "
                "ORDER BY year, month, COALESCE(date_iso, date_display), id",
            )
        return [dict(r) for r in await cur.fetchall()]

    async def get_op_entries_monthly_agg(
        self, year: int, month: int,
    ) -> dict[str, float | None]:
        """Помесячные агрегаты из op_company_entries.

        Returns: dict с ключами cashless / nds / taxes / loan / other.
                 None если ни одной записи в (year, month) нет; иначе sum'ы.
        """
        cur = await self.conn.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                COALESCE(SUM(cashless_amount), 0) AS cashless,
                COALESCE(SUM(nds), 0)            AS nds,
                COALESCE(SUM(taxes), 0)          AS taxes,
                COALESCE(SUM(loan), 0)           AS loan,
                COALESCE(SUM(other_amount), 0)   AS other
            FROM op_company_entries
            WHERE year = ? AND month = ?
            """,
            (year, month),
        )
        r = await cur.fetchone()
        if r is None or (r["cnt"] or 0) == 0:
            return {"cashless": None, "nds": None, "taxes": None, "loan": None, "other": None}
        return {
            "cashless": float(r["cashless"]) if r["cashless"] else None,
            "nds":      float(r["nds"])      if r["nds"]      else None,
            "taxes":    float(r["taxes"])    if r["taxes"]    else None,
            "loan":     float(r["loan"])     if r["loan"]     else None,
            "other":    float(r["other"])    if r["other"]    else None,
        }

    async def delete_op_company_entry(self, entry_id: int) -> None:
        await self.conn.execute("DELETE FROM op_company_entries WHERE id = ?", (entry_id,))
        await self.conn.commit()

    async def get_monthly_op_company(self, year: int, month: int) -> dict[str, Any] | None:
        """Вернуть raw + expense_total_new, balance_month, balance_running_ytd."""
        cur = await self.conn.execute(
            "SELECT * FROM op_company_monthly WHERE year = ? AND month = ?",
            (year, month),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        item = dict(row)
        item["expense_total_new"] = _compute_expense_total_new(item)
        inc = float(item.get("income_total") or 0)
        item["balance_month"] = inc - item["expense_total_new"]

        cur2 = await self.conn.execute(
            "SELECT * FROM op_company_monthly WHERE year = ? AND month <= ? ORDER BY month",
            (year, month),
        )
        ytd = 0.0
        for r in await cur2.fetchall():
            d = dict(r)
            ytd += float(d.get("income_total") or 0) - _compute_expense_total_new(d)
        item["balance_running_ytd"] = ytd
        return item

    async def list_monthly_op_company(
        self, year: int | None = None,
    ) -> list[dict[str, Any]]:
        """Все месяцы (опционально фильтр по году), отсортированы (year, month) asc.

        Каждая строка дополняется вычисленными balance_month и balance_running_ytd
        (running считается на лету в рамках одного года).
        """
        if year is None:
            cur = await self.conn.execute(
                "SELECT * FROM op_company_monthly ORDER BY year, month",
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM op_company_monthly WHERE year = ? ORDER BY year, month",
                (year,),
            )
        rows = [dict(r) for r in await cur.fetchall()]
        running_by_year: dict[int, float] = {}
        for r in rows:
            r["expense_total_new"] = _compute_expense_total_new(r)
            inc = float(r.get("income_total") or 0)
            r["balance_month"] = inc - r["expense_total_new"]
            y = int(r["year"])
            running_by_year[y] = running_by_year.get(y, 0.0) + r["balance_month"]
            r["balance_running_ytd"] = running_by_year[y]
        return rows

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

        # 2a) credit_expenses — отдельная статья, в total_cost НЕ суммируется
        # с материалами/услугами (по решению 2026-05-14). Считаем только для
        # информации и отдаём отдельным полем credit_expenses_total.
        credit_exp_total = 0.0
        if inv.get("is_credit"):
            cur = await self.conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM credit_expenses WHERE invoice_id = ?",
                (invoice_id,),
            )
            row_ce = await cur.fetchone()
            credit_exp_total = float(row_ce[0] or 0) if row_ce else 0.0

        # 3) ЗП — для информации, НЕ входят в total_cost.
        # ТЗ 2026-05-19 A.2: ZP_FACT_STATUSES = (approved, payment_sent, confirmed) —
        # все три считаются «факт оплачено», иначе карточка показывает 0 для уже выплаченных.
        zp_zamery = float(inv.get("zp_zamery_total") or 0) \
            if inv.get("zp_status") in ZP_FACT_STATUSES else 0.0
        zp_manager = float(inv.get("zp_manager_amount") or 0) \
            if inv.get("zp_manager_status") in ZP_FACT_STATUSES else 0.0
        zp_installer = float(inv.get("zp_installer_amount") or 0) \
            if inv.get("zp_installer_status") in ZP_FACT_STATUSES else 0.0
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

        # Монтаж: ТЗ 2026-05-19 A.1 — единый helper fact_installation(inv).
        # AN (montazh_fact_op) приоритет, fallback на zp_installer_amount.
        # Hunk 3 (2026-05-28): доп. fallback на cost_montazh (DR) — для счетов
        # где AN=0 И ZP-монтажника не выплачена, но есть прямые supplier_payments
        # с material_type='montazh' (агрегаты в cost_montazh).
        montazh_fact_op = float(inv.get("montazh_fact_op") or 0)
        montazh_combined = fact_installation(inv)
        if not montazh_combined:
            montazh_combined = float(inv.get("cost_montazh") or 0)

        # ТЗ 2026-05-19 A.7: MAX(materials_combined, supplier_payments) —
        # защита от дубля, когда одна и та же оплата поставщику попадает И в
        # materials_fact_op (через выписку ОП), И в supplier_payments (ручная
        # привязка РП к invoice_id). Если источники разные — заменить на сумму
        # (минус явные дубли через JOIN по дате+сумме). Решение по умолчанию: MAX.
        mat_and_suppliers = max(materials_combined, supplier_payments_total)

        # Вычитаемые позиции (отслеживаются в отдельных столбцах ОП)
        # Hunk 3 (2026-05-28): fallback на DS/DT если AO/AP пустые.
        logistics_fact = float(inv.get("logistics_fact_op") or 0)
        if not logistics_fact:
            logistics_fact = float(inv.get("cost_logistics") or 0)
        loaders_fact = float(inv.get("loaders_fact_op") or 0)
        if not loaders_fact:
            loaders_fact = float(inv.get("cost_loaders") or 0)
        agent_payout = float(inv.get("agent_payout_op") or inv.get("agent_fee") or 0)
        taxes_fact = float(inv.get("taxes_fact_op") or 0)
        npn_10pct = float(inv.get("npn_amount") or 0)
        # Зачёт аванса монтажника (нужен ниже для монтаж-факта прибыли = BS).
        installer_advance_offset = await self.get_installer_advance_for_invoice(invoice_id)

        # Итого расходы (СТАРАЯ база) — оставлена ТОЛЬКО для НДС/налога факт.
        # ЗП не входит — используем только факт из ОП
        total_cost = (mat_and_suppliers + montazh_combined
                      + logistics_fact + loaders_fact + agent_payout)

        # НДС/налог факт. Кредитные счета: налоги = 0.
        if inv.get("is_credit"):
            nds_fact = 0.0
            profit_tax_fact = 0.0
        else:
            _v2_rdate = str(inv.get("receipt_date") or "")[:10]
            _v2_metal = float(inv.get("cost_metal") or 0)
            _v2_glass = float(inv.get("cost_glass") or 0)
            # «Новые» счета (receipt_date >= NDS_V2_CUTOFF = с 2654/2026-05-04, «строка 21+
            # Импорт ОП») с заполненными металлом/стеклом → НДС и налог ТОЧНО по формуле AZ
            # «Налоги факт» (user 2026-06-17, скрин формулы):
            #   НДС   = (Сумма − металл DP − стекло DQ) × 22/122
            #   Налог = (Сумма − МатериалыФакт AM − МонтажФакт AN − Грузчики AQ −
            #            Логистика AO − НДС) × 20%   (БЕЗ max(0), БЕЗ агентского)
            if _v2_rdate >= NDS_V2_CUTOFF and (_v2_metal > 0 or _v2_glass > 0) and invoice_amount:
                nds_fact = (invoice_amount - _v2_metal - _v2_glass) * 22 / 122
                _az_costs = (float(inv.get("materials_fact_op") or 0)
                             + float(inv.get("montazh_fact_op") or 0)
                             + float(inv.get("loaders_fact_op") or 0)
                             + float(inv.get("logistics_fact_op") or 0))
                profit_tax_fact = (invoice_amount - _az_costs - nds_fact) * 0.20
            else:
                # Старая база (счета до отсечки и без металла/стекла, напр. 26423) —
                # воспроизводит «Налоги факт» AZ строк 1–20.
                # НДС = (Сумма − mat_and_suppliers) × 22/122; налог как было (с max(0)).
                nds_fact = (invoice_amount * 22 / 122) - (mat_and_suppliers * 22 / 122) if invoice_amount else 0.0
                profit_tax_fact = max(0.0, (invoice_amount - total_cost - nds_fact) * 0.20) \
                    if invoice_amount else 0.0

        # ── Прибыль факт (user 2026-06-17): затраты = «факт»-столбцы листа
        #    BG = BR+BS+BT+BU (Грузчики+Монтаж+Материалы+Логистика факт) + агентское АМ.
        #    БД — источник истины: считаем ЗДЕСЬ, а _invoice_cells просто печатает
        #    margin в BL (feedback_db_first_no_direct_sheet_writes). НЕ MAX(материалы,
        #    оплаты пост.) и НЕ AN-монтаж — это завышало материалы, задваивало
        #    логистику/грузчиков (они и в оплатах пост., и в BR/BU) и занижало монтаж.
        #      • Материалы = materials_fact_op (BT);
        #      • Монтаж = реально ВЫПЛАЧЕННЫЙ (зеркало BS из sheets._invoice_cells):
        #        max(AN, аванс×1.10 [б/н], бот) и только при полной выплате (≥ Согласовано);
        #      • Логистика/Грузчики = logistics_fact_op / loaders_fact_op (BU/BR);
        #      • + агентское АМ (agent_payout); НДС/налог — прежние (от старой базы).
        _bt = float(inv.get("materials_fact_op") or 0)
        _bu = float(inv.get("logistics_fact_op") or 0)
        _br = float(inv.get("loaders_fact_op") or 0)
        _agreed = float(inv.get("montazh_agreed_amount") or 0)
        _mfo_bs = float(inv.get("montazh_fact_op") or 0)
        _inv_credit_bs = bool(inv.get("is_credit")) or str(inv.get("invoice_number") or "").upper().startswith("ЗМ")
        # Аванс ТЕКУЩЕЙ монтажной группы: аванс прошлых групп (montazh_adv_prev, снимок
        # при переброске — rp_new.py::_finalize_naem/_finalize_regroup) уже внутри
        # paid_prev через DR, второй раз в «Выплачено» входить не должен. Канон бота —
        # installer_new.py::_advance_raw_cur. ⚠️ Само installer_advance_offset НЕ трогаем:
        # оно уходит в cost-card и кормит колонку CG «Аванс монтажника» (sheets.py:1378),
        # а там аванс по счёту ЦЕЛИКОМ [[feedback_no_unauthorized_column_logic]].
        # adv_prev = 0 → поведение прежнее.
        _adv_raw_cur = max(0.0, installer_advance_offset - float(inv.get("montazh_adv_prev") or 0))
        _adv_cg = _adv_raw_cur * 1.10 \
            if (_adv_raw_cur > 0 and not _inv_credit_bs) else _adv_raw_cur
        _bot_paid = float(inv.get("zp_installer_amount") or 0) \
            if inv.get("zp_installer_status") in ("payment_sent", "confirmed") else 0.0
        # Нога прошлых групп (paid_prev) — зеркало sheets._invoice_cells: складывается
        # с текущей ногой, с AN конкурирует по max. paid_prev=0 → формула прежняя.
        _paid_prev_bs = float(inv.get("montazh_paid_prev") or 0)
        # Канал DR «Затр. Монтаж» (cost_montazh) — канон integrations/sheets.py:1274,
        # добавлен туда 28.07 и до 01.08 в это зеркало НЕ попал. ЗП наёмной монтажной
        # группе может идти НЕСКОЛЬКИМИ платежами; DR — их накопитель по МАТЕРИНСКОМУ
        # счёту (create_supplier_payment/bump_invoice_cost при material_type='montazh'),
        # тогда как zp_installer_amount хранит только ПОСЛЕДНИЙ платёж, а
        # montazh_paid_prev — снимок DR на момент назначения НОВОЙ группы. Без DR
        # реконструкция «paid_prev + последний платёж» не складывала транши ОДНОЙ
        # группе: BS на листе стоял, а здесь _montazh_bs падал в 0 → bg_cost занижался
        # и прибыль ЗАВЫШАЛАСЬ (BG и BL на одной строке противоречили друг другу).
        # Через max, НЕ сумму: DR уже включает выплаты, учтённые другими каналами —
        # сложение задвоило бы [[feedback_montazh_zp_multi_payment_sum]].
        _dr_paid_bs = float(inv.get("cost_montazh") or 0)
        if bool(inv.get("zp_installer_remainder")) and _bot_paid > 0:
            _paid = max(_mfo_bs, _paid_prev_bs + _adv_cg + _bot_paid, _dr_paid_bs)
        else:
            _paid = max(_mfo_bs, _paid_prev_bs + max(_adv_cg, _bot_paid), _dr_paid_bs)
        if _agreed > 0:
            _montazh_bs = _paid if _paid >= _agreed - 0.001 else 0.0
        elif _mfo_bs > 0:
            _montazh_bs = _mfo_bs
        else:
            _montazh_bs = 0.0
        bg_cost = _bt + _montazh_bs + _bu + _br + agent_payout

        # Прибыль факт = (Сумма − Долг) − BG − НДС − налог на прибыль − НПН 10%
        # (агентское АМ уже в bg_cost).
        # Revenue = фактически полученное от заказчика (Сумма − outstanding_debt «Долг» AE),
        # user 2026-06-17: прибыль считается только от реально полученных денег — для б/н И
        # кредита (выбор user). НДС/налог пока на полной сумме (их расчёт отложен отдельно).
        # Эквивалентно (прежняя margin) − Долг; для счетов с долгом=0 — без изменений.
        outstanding_debt = float(inv.get("outstanding_debt") or 0)
        margin = invoice_amount - outstanding_debt - bg_cost - nds_fact - profit_tax_fact - npn_10pct
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
            # bg_cost = новая база маржи (BR+BS+BT+BU+АМ); npn_10pct — вычет НПН.
            # Отдаём в карточку себестоимости для блока «сверка маржи» (хвост #3,
            # user 2026-06-17): display-only, значения уже посчитаны для margin.
            "bg_cost": bg_cost,
            "npn_10pct": npn_10pct,
            "credit_expenses_total": credit_exp_total,
            "margin": margin,
            "margin_pct": margin_pct,
            # ТЗ 2026-05-19 вечер: сумма installer_advance_items для этого
            # invoice (только владелец-монтажник, любой статус item'a). Используется в
            # _invoice_cells для определения «ЗП оплачена через аванс» →
            # BS = эта сумма, BJ = "" (не показывать как план).
            "installer_advance_offset": installer_advance_offset,
            # Дата зачёта аванса по счёту (для Invoices CH «Дата аванса», user 2026-06-08).
            "installer_advance_date": await self.get_installer_advance_date_for_invoice(invoice_id),
            # Аналог для МЕНЕДЖЕРА (user 2026-06-14): зачтённый аванс менеджера + дата
            # по счёту для Invoices EO «Аванс менеджера» / EP «Дата аванса мен.».
            "manager_advance_offset": await self.get_manager_advance_for_invoice(invoice_id),
            "manager_advance_date": await self.get_manager_advance_date_for_invoice(invoice_id),
        }

    async def get_installer_advance_for_invoice(self, invoice_id: int) -> float:
        """Сумма installer_advance_items монтажника для конкретного invoice.

        Используется в `_invoice_cells` для отображения CG/BS/BJ: если есть
        items.amount > 0 → ZP считается оплаченной авансом (BS = sum, BJ = "").
        Учитываются ВСЕ items независимо от offset_zp_id (NULL = резерв, NOT NULL = closed).

        ⚠️ Защита роли владельца = монтажник (2026-06-08): счёт-владелец заявки
        (installer_advance_requests.installer_id) обязан иметь роль 'installer'.
        Авансы РП/менеджера (общая таблица; wallet_role 'rp'/'manager_npn' ИЛИ
        single-role менеджер с wallet_role NULL) НЕ протекают в монтажный CG/BJ.
        Идиом мульти-роли — как в list_installer_tasks_needing_* (',role,' LIKE).
        На текущих данных no-op (per-invoice items только у монтажника), но
        страхует будущие items других ролей. [[feedback_advance_deposit_journal_only]]
        """
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "JOIN users u ON u.telegram_id = r.installer_id "
            "WHERE i.invoice_id = ? "
            "  AND (',' || COALESCE(u.role, '') || ',') LIKE '%,installer,%'",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return float(row[0] or 0) if row else 0.0

    async def get_installer_advance_date_for_invoice(self, invoice_id: int) -> str | None:
        """Дата зачёта аванса монтажника по счёту (для Invoices CH «Дата аванса»).

        MAX(COALESCE(item.offset_at, request.paid_at, request.requested_at)) по всем
        installer_advance_items этого счёта. None — если авансов по счёту нет.
        Для открытого earmark (offset_at=NULL) берётся дата заявки (выдача/запрос).

        Та же защита роли владельца = монтажник, что и get_installer_advance_for_invoice
        (2026-06-08): CH и CG/BJ согласованы — считают один и тот же набор items.
        """
        cur = await self.conn.execute(
            "SELECT MAX(COALESCE(i.offset_at, r.paid_at, r.requested_at)) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "JOIN users u ON u.telegram_id = r.installer_id "
            "WHERE i.invoice_id = ? "
            "  AND (',' || COALESCE(u.role, '') || ',') LIKE '%,installer,%'",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

    async def get_manager_advance_for_invoice(self, invoice_id: int) -> float:
        """Сумма installer_advance_items МЕНЕДЖЕРА для invoice (Invoices EO «Аванс
        менеджера»). Зеркало get_installer_advance_for_invoice, но владелец = менеджер
        (роль LIKE manager) И кошелёк != 'rp': двойная роль Павла (rp+manager_npn) —
        его RP-авансы (wallet_role='rp') в менеджерскую колонку НЕ течут
        ([[feedback_rp_npn_separate_wallets]]). Авансы КВ/КИА имеют wallet_role NULL,
        НПН — 'manager_npn'; оба проходят (NULL OR != 'rp'). Монтажные items
        исключены ролью владельца. read-only.
        """
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "JOIN users u ON u.telegram_id = r.installer_id "
            "WHERE i.invoice_id = ? "
            "  AND (',' || COALESCE(u.role, '') || ',') LIKE '%,manager%' "
            "  AND (r.wallet_role IS NULL OR r.wallet_role != 'rp')",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return float(row[0] or 0) if row else 0.0

    async def get_manager_advance_date_for_invoice(self, invoice_id: int) -> str | None:
        """Дата зачёта аванса менеджера по счёту (Invoices EP «Дата аванса мен.»).
        MAX(COALESCE(item.offset_at, request.paid_at, request.requested_at)). Та же
        защита владелец=менеджер + кошелёк != 'rp', что в get_manager_advance_for_invoice.
        """
        cur = await self.conn.execute(
            "SELECT MAX(COALESCE(i.offset_at, r.paid_at, r.requested_at)) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "JOIN users u ON u.telegram_id = r.installer_id "
            "WHERE i.invoice_id = ? "
            "  AND (',' || COALESCE(u.role, '') || ',') LIKE '%,manager%' "
            "  AND (r.wallet_role IS NULL OR r.wallet_role != 'rp')",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

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

        # ЕДИНЫЙ helper utils.compute_plan_profit — credit-aware НДС (0 для кредита,
        # зеркало факт-стороны) + гард распределения при прибыли ≤ 0. Источник истины,
        # чтобы копии расчёта (manager_new, format_estimated_summary, sheet_commands)
        # не расходились в будущем (user 2026-06-19, КВ-6 inv50).
        _pp = compute_plan_profit(
            amount=amount, est_glass=est_glass, est_profile=est_profile,
            est_mat_legacy=est_mat_legacy, est_inst=est_inst, est_load=est_load,
            est_log=est_log, is_credit=bool(inv.get("is_credit")),
            client_source=inv.get("client_source") or "own",
        )
        materials_total = _pp["materials_total"]
        est_total = _pp["est_total"]
        output_vat = _pp["output_vat"]
        input_vat = _pp["input_vat"]
        net_vat = _pp["net_vat"]
        est_profit = _pp["est_profit"]
        est_pct = _pp["est_pct"]
        client_source = inv.get("client_source") or "own"
        rp_zp = _pp["rp_zp"]
        manager_zp = _pp["manager_zp"]
        gd_profit = _pp["gd_profit"]

        has_estimated = any([est_glass, est_profile, est_mat_legacy,
                             est_inst, est_load, est_log])

        # Факт (из cost card)
        fact_total = cost["total_cost"]
        fact_profit = cost["margin"]

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
            # ЗП менеджера разрешена при status='ended' AND перерасход ≤ 10000 ₽
            # (допустимый порог по решению user'а 2026-05-16).
            # Для pending/in_progress/paid/credit — заблокирована для всех меню
            # и сценариев бота (UI, FSM, авто-апрув из ОП).
            "zp_allowed": (inv.get("status") == "ended") and ((fact_total - est_total) <= 10000.0),
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

    async def list_invoices_under_recalc(
        self, marker: str | None = None, created_by: int | None = None
    ) -> list[int]:
        """ID счетов под механизмом перерасчёта ЗП менеджера (owner 2026-06-23).

        Условие: есть переплата (zp_manager_hold/CN != 0) И долг погашен
        (outstanding_debt == 0 — финальный платёж сделан). Пока по материнскому
        счёту есть долг — НЕ входит в механизм. Родительские счета, не rejected.
        Для карточки «Перерасчёт прибыли» (кнопка ГД + авто после синка).

        Фикс 30.07 (двойное начисление): счёт выпадает из списка, как только вся
        переплата перенесена в аванс (|CN| − zp_hold_advanced ≤ 0). Раньше условия
        на zp_hold_advanced не было → отработанный свипом счёт продолжал висеть в
        карточке ГД, и связка «Отправить менеджеру» → «Согласен» начисляла аванс
        ВТОРОЙ раз (достижимо было по КВ 9 и КВ 10). Ручное обнуление CF в «Импорт
        ОП» больше не единственный способ снять счёт с механизма.

        marker/created_by (owner 2026-06-23) — опц. скоуп под «Мои Счета» менеджера:
        marker → invoice_number LIKE '%marker%' (КВ/КИА/НПН, зеркало list_invoices),
        created_by → автор счёта. Оба None → все счета (поведение ГД неизменно).
        """
        clauses = [
            "COALESCE(zp_manager_hold, 0) != 0",
            "ABS(COALESCE(outstanding_debt, 0)) < 1",
            "parent_invoice_id IS NULL",
            "COALESCE(status, '') != 'rejected'",
            # Остаток ещё не перенесённой переплаты (зеркало дельты в свипе).
            "ABS(COALESCE(zp_manager_hold, 0)) - COALESCE(zp_hold_advanced, 0) > 0.009",
        ]
        params: list[Any] = []
        if marker is not None:
            clauses.append("invoice_number LIKE ?")
            params.append(f"%{marker}%")
        if created_by is not None:
            clauses.append("created_by = ?")
            params.append(created_by)
        cur = await self.conn.execute(
            "SELECT id FROM invoices WHERE " + " AND ".join(clauses) + " ORDER BY id",
            tuple(params),
        )
        return [int(r["id"]) for r in await cur.fetchall()]

    async def list_invoices_for_installer(self, user_id: int) -> list[dict[str, Any]]:
        """Активные счета монтажника, у которых ЗП-installer ещё не выплачена.

        zp_installer_payout колонки в Invoices нет — суррогат: status NOT IN
        ('payment_sent','confirmed'). После payment_sent распределять под этот
        счёт смысла нет (деньги уже ушли монтажнику).
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE assigned_to = ? "
            "AND status IN ('in_progress', 'paid') "
            "AND parent_invoice_id IS NULL "
            "AND (zp_installer_status IS NULL "
            "     OR zp_installer_status NOT IN ('payment_sent', 'confirmed')) "
            "ORDER BY created_at DESC LIMIT 15",
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_invoices_for_manager(self, user_id: int) -> list[dict[str, Any]]:
        """Активные счета менеджера — цели РАСПРЕДЕЛЕНИЯ аванса (ЗП ещё не выплачена).

        Фильтр `COALESCE(zp_manager_payout, 0) = 0` (Invoices.AN) — ТЗ 25.05
        funds-2balances. ДОП. (TZ 06.06, наполнение аванса): исключаем
        zp_manager_status ∈ confirmed/payment_sent — confirmed ставит налив ЗП в
        аванс (credit_manager_zp_to_advance) при payout=0, и без этого фильтра
        счёт остался бы целью распределения → аванс распределился бы против уже
        забранной ЗП (двойной учёт). Зеркало installer-distribution (исключает
        confirmed/payment_sent). 'approved' оставляем — валидная цель (auto-offset).

        'credit' в статусах (owner 2026-07-04): кредит-счета — тип оплаты, стадия у
        них тоже есть (BP/BQ). Менеджер зарабатывает ЗП и по кредит-счетам → они
        валидные цели распределения аванса. На ЗАКРЫТОМ (End) счёте распределение
        сразу пишет AN/AO без ГД (apply_manager_advance_immediate).
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE created_by = ? "
            "AND status IN ('in_progress', 'paid', 'ended', 'credit') "
            "AND COALESCE(zp_manager_payout, 0) = 0 "
            "AND COALESCE(zp_manager_status, '') NOT IN ('confirmed', 'payment_sent') "
            "AND parent_invoice_id IS NULL "
            "ORDER BY created_at DESC LIMIT 15",
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_installer_confirmed_invoices(self, user_id: int | None = None) -> list[dict[str, Any]]:
        """Счета «В работу» (montazh_stage >= IN_WORK).
        Если user_id=None — все счета (для общего списка монтажников).
        status включает 'credit' — кредитные счета тоже обрабатывает монтажник (fix 02.06)."""
        if user_id is not None:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE assigned_to = ? "
                "AND montazh_stage IN ('in_work', 'razmery_ok', 'invoice_ok') "
                "AND status IN ('in_progress', 'paid', 'credit') "
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
                "AND status IN ('in_progress', 'paid', 'credit') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY CASE montazh_stage "
                "  WHEN 'in_work' THEN 1 WHEN 'razmery_ok' THEN 2 "
                "  WHEN 'invoice_ok' THEN 3 ELSE 4 END, created_at DESC LIMIT 15",
            )
        return [dict(r) for r in await cur.fetchall()]

    async def list_installer_unconfirmed_invoices(self, user_id: int | None = None) -> list[dict[str, Any]]:
        """Счета, ещё НЕ подтверждённые «В работу».
        Если user_id=None — все неподтверждённые (для общего списка).
        status включает 'credit' — кредитные счета тоже идут монтажнику на принятие (fix 02.06)."""
        if user_id is not None:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE assigned_to = ? "
                "AND (montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid', 'credit') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY created_at DESC LIMIT 15",
                (user_id,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT * FROM invoices WHERE "
                "(montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid', 'credit') "
                "AND parent_invoice_id IS NULL "
                "ORDER BY created_at DESC LIMIT 15",
            )
        return [dict(r) for r in await cur.fetchall()]

    async def count_installer_unconfirmed_invoices(self, user_id: int | None = None) -> int:
        """Кол-во счетов «на принятие» В работу (для бейджа кнопки «🔨 В Работу»).

        Зеркалит list_installer_unconfirmed_invoices (та же выборка), но COUNT без LIMIT.
        user_id=None → глобально (как в installer_in_work — общий пул на принятие).
        """
        if user_id is not None:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices WHERE assigned_to = ? "
                "AND (montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid', 'credit') "
                "AND parent_invoice_id IS NULL",
                (user_id,),
            )
        else:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices WHERE "
                "(montazh_stage IS NULL OR montazh_stage IN ('none','assigned')) "
                "AND status IN ('in_progress', 'paid', 'credit') "
                "AND parent_invoice_id IS NULL",
            )
        row = await cur.fetchone()
        return int(row[0] or 0) if row else 0

    async def get_installer_pending_zp(self, installer_id: int) -> list[dict[str, Any]]:
        """Принятые в работу счета монтажника с согласованной ЗП, ещё НЕ выплаченной.

        «Ожидаемая ЗП»: montazh_agreed_amount > 0 и zp_installer_status != 'confirmed'.
        Read-only витрина для карточки «Мой баланс» (НЕ часть наличного баланса).
        """
        cur = await self.conn.execute(
            "SELECT id, invoice_number, object_address, COALESCE(is_credit, 0) AS is_credit, "
            "       COALESCE(montazh_agreed_amount, 0) AS agreed, "
            "       COALESCE(zp_installer_status, 'not_requested') AS zp_status "
            "FROM invoices "
            "WHERE assigned_to = ? "
            "  AND COALESCE(montazh_agreed_amount, 0) > 0 "
            "  AND COALESCE(zp_installer_status, 'not_requested') != 'confirmed' "
            "  AND parent_invoice_id IS NULL "
            "  AND status IN ('in_progress', 'paid', 'credit', 'ended') "
            "ORDER BY id DESC",
            (installer_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_invoices_with_deadline(self) -> list[dict[str, Any]]:
        """Backward-compatible alias for deadline dashboards and legacy callers."""
        return await self.list_invoices_approaching_deadline()

    async def assign_installer_to_invoice(
        self, invoice_id: int, installer_id: int,
    ) -> None:
        """Назначить монтажника на счёт + сбросить montazh_stage.

        edo_task_id=None — снимает возможную метку Наёмников (2), если счёт ранее
        был назначен 2-й группе (метка монт. группы хранится в edo_task_id;
        читается только значение 2). update_invoice пишет None как NULL.
        """
        await self.update_invoice(
            invoice_id,
            assigned_to=installer_id,
            edo_task_id=None,
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
        """Обновить этап монтажа по счёту + записать таймстемп стадии."""
        now = to_iso(utcnow())
        _stage_ts_col = {
            "assigned": "montazh_assigned_at",
            "in_work": "montazh_in_work_at",
            "razmery_ok": "montazh_razmery_ok_at",
            "invoice_ok": "montazh_invoice_ok_at",
        }
        ts_col = _stage_ts_col.get(stage)
        if ts_col:
            await self.conn.execute(
                f"UPDATE invoices SET montazh_stage = ?, {ts_col} = ?, updated_at = ? WHERE id = ?",
                (stage, now, now, invoice_id),
            )
        else:
            await self.conn.execute(
                "UPDATE invoices SET montazh_stage = ?, updated_at = ? WHERE id = ?",
                (stage, now, invoice_id),
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
        *,
        include_credit: bool = False,
        creator_role: str | None = None,
    ) -> list[dict[str, Any]]:
        """List ENDED invoices. If month_start given, filter by updated_at >= month_start.

        include_credit: также включить полностью закрытые кредитные
        (status='credit' AND montazh_stage='invoice_end') — как в count_ended_invoices.
        creator_role: если задан — только счета этого создателя (creator_role);
        зеркалит list_invoices_in_work (скоуп пикера привязки кредит-расхода для
        менеджера: его собственные закрытые счета). ТЗ 19.06.
        """
        status_clause = (
            "(status = 'ended' OR (status = 'credit' AND montazh_stage = 'invoice_end'))"
            if include_credit else "status = 'ended'"
        )
        role_clause = "AND creator_role = ? " if creator_role else ""
        date_clause = "AND updated_at >= ? " if month_start else ""
        params: list[Any] = []
        if creator_role:
            params.append(creator_role)
        if month_start:
            params.append(month_start)
        params.append(limit)
        cur = await self.conn.execute(
            f"SELECT * FROM invoices WHERE {status_clause} "
            f"{role_clause}{date_clause}"
            "ORDER BY updated_at DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def count_ended_invoices(
        self,
        month_start: str | None = None,
        *,
        include_credit: bool = False,
    ) -> int:
        """Count ENDED invoices. If month_start given, count only current month.

        include_credit: when True, also count credit invoices that prošли invoice_end
        этап (montazh_stage='invoice_end'). Credit без stage='invoice_end' (например,
        carry-in КВ 5/КВ 6) исключаются — это «активные кредиты», не закрытые счета.
        """
        status_clause = (
            "(status = 'ended' OR (status = 'credit' AND montazh_stage = 'invoice_end'))"
            if include_credit else "status = 'ended'"
        )
        if month_start:
            cur = await self.conn.execute(
                f"SELECT COUNT(*) FROM invoices "
                f"WHERE {status_clause} AND updated_at >= ?",
                (month_start,),
            )
        else:
            cur = await self.conn.execute(
                f"SELECT COUNT(*) FROM invoices WHERE {status_clause}"
            )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def get_ended_monthly_summary(self) -> list[dict[str, Any]]:
        """Агрегация ended+credit счетов по месяцам.

        Включает status='ended' И credit-счета, прошедшие invoice_end этап
        (montazh_stage='invoice_end'). Активные кредиты (КВ 5/КВ 6 — status=credit
        без stage=invoice_end) исключаются: они ещё не «получили статус счёт энд».
        """
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
                -- ТЗ 2026-05-19 A.1: fact_montazh по правилу BS=AN —
                -- montazh_fact_op приоритет, fallback на zp_installer_amount.
                -- Дублирует логику app.utils.fact_installation (helper в Python).
                SUM(CASE
                    WHEN COALESCE(montazh_fact_op, 0) > 0 THEN montazh_fact_op
                    WHEN zp_installer_status IN ('approved', 'payment_sent', 'confirmed')
                         AND COALESCE(zp_installer_amount, 0) > 0
                    THEN COALESCE(zp_installer_amount, 0)
                    ELSE 0
                END) AS fact_montazh,
                SUM(COALESCE(loaders_fact_op, 0)) AS fact_loaders,
                SUM(COALESCE(logistics_fact_op, 0)) AS fact_logistics,
                -- ТЗ 2026-05-19 A.2: расширенные ZP_FACT_STATUSES (payment_sent добавлен).
                SUM(CASE WHEN zp_manager_status IN ('approved', 'payment_sent', 'confirmed')
                    THEN COALESCE(zp_manager_amount, 0) ELSE 0 END) AS zp_manager,
                SUM(CASE WHEN zp_installer_status IN ('approved', 'payment_sent', 'confirmed')
                    THEN COALESCE(zp_installer_amount, 0) ELSE 0 END) AS zp_installer,
                SUM(COALESCE(agent_payout_op, agent_fee, 0)) AS agent_payout,
                SUM(COALESCE(outstanding_debt, 0)) AS total_debt
            FROM invoices
            WHERE (status = 'ended'
                   OR (status = 'credit' AND montazh_stage = 'invoice_end'))
              AND parent_invoice_id IS NULL
            GROUP BY month
            ORDER BY month ASC
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

    async def get_gd_inwork_extra_stats(self, gd_user_id: int) -> dict[str, int]:
        """Доп. статистика 2-го столбца блока «Счета в работе» ГД-карточки.

        - paid_today_inv: invoice_payment-задачи, завершённые сегодня (МСК).
        - paid_today_sup: записи supplier_payments, созданные сегодня (МСК).
        - inv_year: б/н счета (parent, не rejected), созданные в текущем году.
        - credit_year: кредитные счета (parent, не rejected) за текущий год.
        - unread_msgs: непрочитанные incoming сообщения у ГД.
        Каждый счётчик устойчив к сбою (per-query try/except → дефолт 0).
        """
        from zoneinfo import ZoneInfo
        _now = datetime.now(ZoneInfo("Europe/Moscow"))
        today = _now.date().isoformat()
        year = str(_now.year)
        out = {
            "paid_today_inv": 0, "paid_today_sup": 0,
            "inv_year": 0, "credit_year": 0, "unread_msgs": 0,
        }
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM tasks "
                "WHERE type = 'invoice_payment' AND status = 'done' "
                "AND date(updated_at) = ?",
                (today,),
            )
            out["paid_today_inv"] = int((await cur.fetchone())[0] or 0)
        except Exception:
            pass
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM supplier_payments WHERE date(created_at) = ?",
                (today,),
            )
            out["paid_today_sup"] = int((await cur.fetchone())[0] or 0)
        except Exception:
            pass
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices "
                "WHERE parent_invoice_id IS NULL "
                "AND (is_credit = 0 OR is_credit IS NULL) "
                "AND COALESCE(status, '') != 'rejected' "
                "AND invoice_number NOT LIKE 'LEAD-%' "
                "AND strftime('%Y', created_at) = ?",
                (year,),
            )
            out["inv_year"] = int((await cur.fetchone())[0] or 0)
        except Exception:
            pass
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM invoices "
                "WHERE parent_invoice_id IS NULL AND is_credit = 1 "
                "AND COALESCE(status, '') != 'rejected' "
                "AND invoice_number NOT LIKE 'LEAD-%' "
                "AND strftime('%Y', created_at) = ?",
                (year,),
            )
            out["credit_year"] = int((await cur.fetchone())[0] or 0)
        except Exception:
            pass
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM chat_messages "
                "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0",
                (gd_user_id,),
            )
            out["unread_msgs"] = int((await cur.fetchone())[0] or 0)
        except Exception:
            pass
        return out

    async def get_manager_dashboard_metrics(self, manager_id: int) -> dict[str, Any]:
        """Сводка персонально для менеджера — для welcome-card при синхронизации.

        Источник: invoices где created_by=manager_id, tasks где assigned_to=manager_id.
        """
        from .enums import MANAGER_ROLES
        from zoneinfo import ZoneInfo
        _now = datetime.now(ZoneInfo("Europe/Moscow"))
        year = _now.year

        # Роль менеджера для лейбла (КВ / КИА / НПН)
        cur = await self.conn.execute(
            "SELECT role FROM users WHERE telegram_id = ?", (manager_id,)
        )
        urow = await cur.fetchone()
        roles = parse_roles(urow["role"]) if urow else []
        role_label = ""
        for r in roles:
            if r == Role.MANAGER_KV:
                role_label = "КВ"; break
            if r == Role.MANAGER_KIA:
                role_label = "КИА"; break
            if r == Role.MANAGER_NPN:
                role_label = "НПН"; break

        # Счета в работе (parent_invoice_id IS NULL — только родительские)
        ACTIVE_STATUSES = ("pending", "in_progress", "paid", "credit")
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt, "
            "       COALESCE(SUM(outstanding_debt), 0) AS total_debt "
            "FROM invoices "
            "WHERE created_by = ? "
            "  AND parent_invoice_id IS NULL "
            f"  AND status IN ({','.join('?' * len(ACTIVE_STATUSES))})",
            (manager_id, *ACTIVE_STATUSES),
        )
        row = await cur.fetchone()
        count_invoices = int(row["cnt"] or 0)
        sum_debt = float(row["total_debt"] or 0)

        # Сумма всех счетов за текущий год (по дате создания, любой статус кроме rejected)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS s FROM invoices "
            "WHERE created_by = ? "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(status, '') != 'rejected' "
            "  AND strftime('%Y', created_at) = ?",
            (manager_id, str(year)),
        )
        sum_invoices_year = float((await cur.fetchone())["s"] or 0)

        # Открытые задачи менеджера
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM tasks "
            "WHERE assigned_to = ? AND status IN ('open', 'in_progress')",
            (manager_id,),
        )
        count_tasks = int((await cur.fetchone())["cnt"] or 0)

        count_unread_tasks = await self.count_unread_tasks(manager_id)

        # Входящие сообщения — непрочитанные incoming в chat_messages.
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM chat_messages "
                "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0",
                (manager_id,),
            )
            count_unread_msgs = int((await cur.fetchone())["cnt"] or 0)
        except Exception:
            count_unread_msgs = 0

        # Невыплаченная ЗП-менеджер = SUM(manager_zp_blank) − SUM(zp_manager_payout)
        # по всем счетам менеджера (не rejected). Не зависит от zp_manager_status.
        cur = await self.conn.execute(
            "SELECT "
            "  COALESCE(SUM(manager_zp_blank), 0) AS plan, "
            "  COALESCE(SUM(zp_manager_payout), 0) AS paid, "
            "  COALESCE(SUM(CASE WHEN ABS(COALESCE(outstanding_debt, 0)) < 1 "
            "                    THEN COALESCE(zp_manager_hold, 0) ELSE 0 END), 0) AS hold "
            "FROM invoices "
            "WHERE created_by = ? "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(status, '') != 'rejected'",
            (manager_id,),
        )
        row_zp = await cur.fetchone()
        # Механизм перерасчёта (owner 23.06): удержание (CN/zp_manager_hold, знак −)
        # уменьшает невыплаченную ЗП, но ТОЛЬКО по счетам без долга (outstanding_debt=0).
        sum_zp_unpaid = max(0.0, float(row_zp["plan"] or 0)
                            + float(row_zp["hold"] or 0)
                            - float(row_zp["paid"] or 0))

        # ЗП-менеджер помесячно за текущий год.
        # Источник: AG «Мен. ЗП (по бланку)» = manager_zp_blank (расчётная).
        # Группировка по strftime('%m', created_at) — месяц создания счёта.
        # Не зависит от статуса счёта: как только появился новый счёт менеджера —
        # его ЗП прибавляется к текущему месяцу автоматически.
        cur = await self.conn.execute(
            """
            SELECT
                CAST(strftime('%m', created_at) AS INTEGER) AS m,
                COALESCE(SUM(manager_zp_blank), 0) AS s
            FROM invoices
            WHERE created_by = ?
              AND parent_invoice_id IS NULL
              AND COALESCE(manager_zp_blank, 0) > 0
              AND COALESCE(status, '') != 'rejected'
              AND strftime('%Y', created_at) = ?
            GROUP BY m
            ORDER BY m
            """,
            (manager_id, str(year)),
        )
        zp_monthly: dict[int, float] = {}
        for r in await cur.fetchall():
            if r["m"] is not None:
                zp_monthly[int(r["m"])] = float(r["s"] or 0)
        zp_total_year = sum(zp_monthly.values())

        return {
            "role_label": role_label,
            "count_invoices": count_invoices,
            "sum_invoices_year": sum_invoices_year,
            "sum_debt": sum_debt,
            "count_tasks": count_tasks,
            "count_unread_tasks": count_unread_tasks,
            "count_unread_msgs": count_unread_msgs,
            "sum_zp_unpaid": sum_zp_unpaid,
            "zp_monthly": zp_monthly,
            "zp_total_year": zp_total_year,
            "year": year,
        }

    async def get_rp_dashboard_metrics(self, rp_id: int) -> dict[str, Any]:
        """Сводка для РП — для welcome-card при синхронизации и daily_sync 09:00.

        Структура идентична get_manager_dashboard_metrics, но:
        - Счета берутся ВСЕ (без фильтра created_by) — РП курирует все объекты.
        - Задачи фильтруются по assigned_to=rp_id (его персональные задачи).
        - Вместо ЗП-менеджер — значение Invoices.AP помесячно
          (расчётная ЗП РП; читается как сумма колонки AP по месяцам).
        - Блок sum_zp_unpaid отсутствует (нерелевантно для РП).
        """
        from zoneinfo import ZoneInfo
        _now = datetime.now(ZoneInfo("Europe/Moscow"))
        year = _now.year

        # Все счета в работе по компании (parent_invoice_id IS NULL — только родительские).
        ACTIVE_STATUSES = ("pending", "in_progress", "paid", "credit")
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt, "
            "       COALESCE(SUM(outstanding_debt), 0) AS total_debt "
            "FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            f"  AND status IN ({','.join('?' * len(ACTIVE_STATUSES))})",
            ACTIVE_STATUSES,
        )
        row = await cur.fetchone()
        count_invoices = int(row["cnt"] or 0)
        sum_debt = float(row["total_debt"] or 0)

        # Сумма всех счетов за текущий год (все счета, любой статус кроме rejected).
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS s FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "  AND COALESCE(status, '') != 'rejected' "
            "  AND strftime('%Y', created_at) = ?",
            (str(year),),
        )
        sum_invoices_year = float((await cur.fetchone())["s"] or 0)

        # Открытые задачи РП.
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM tasks "
            "WHERE assigned_to = ? AND status IN ('open', 'in_progress')",
            (rp_id,),
        )
        count_tasks = int((await cur.fetchone())["cnt"] or 0)
        count_unread_tasks = await self.count_unread_tasks(rp_id)

        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM chat_messages "
                "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0",
                (rp_id,),
            )
            count_unread_msgs = int((await cur.fetchone())["cnt"] or 0)
        except Exception:
            count_unread_msgs = 0

        # «10% прибыли РП» помесячно за текущий год.
        # Источник: значение Invoices.AP помесячно (расчётная ЗП РП).
        # Технически читается из БД-зеркала AP-колонки.
        # Запрет: не вмешиваться в механизм заполнения Invoices.AP — только трансляция.
        # Группировка по receipt_date («Дата пост.», K) — совпадает с колонкой «Месяц»
        # (AT) листа Invoices. НЕ created_at: дата завода счёта в бот отличается от
        # месяца поступления, иначе Янв/Фев теряются (ЗП-монтажник ниже — тот же receipt_date).
        cur = await self.conn.execute(
            """
            SELECT
                CAST(strftime('%m', receipt_date) AS INTEGER) AS m,
                COALESCE(SUM(npn_amount), 0) AS s
            FROM invoices
            WHERE parent_invoice_id IS NULL
              AND COALESCE(npn_amount, 0) > 0
              AND COALESCE(status, '') != 'rejected'
              AND strftime('%Y', receipt_date) = ?
            GROUP BY m
            ORDER BY m
            """,
            (str(year),),
        )
        rp_monthly: dict[int, float] = {}
        for r in await cur.fetchall():
            if r["m"] is not None:
                rp_monthly[int(r["m"])] = float(r["s"] or 0)
        rp_total_year = sum(rp_monthly.values())

        return {
            "role_label": "РП",
            "count_invoices": count_invoices,
            "sum_invoices_year": sum_invoices_year,
            "sum_debt": sum_debt,
            "count_tasks": count_tasks,
            "count_unread_tasks": count_unread_tasks,
            "count_unread_msgs": count_unread_msgs,
            "rp_monthly": rp_monthly,
            "rp_total_year": rp_total_year,
            "year": year,
        }

    async def get_rp_invoices_in_work_breakdown(self) -> dict[str, int]:
        """Разбивка активных счетов компании для минимальной карточки РП на /start.

        pending / in_progress / credit (paid и ended исключены),
        parent_invoice_id IS NULL — только родительские счета по всей компании
        (РП курирует все объекты, без фильтра created_by). Итог = их сумма.
        """
        cur = await self.conn.execute(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS n_pending, "
            "  COALESCE(SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END), 0) AS n_in_progress, "
            "  COALESCE(SUM(CASE WHEN status = 'credit' THEN 1 ELSE 0 END), 0) AS n_credit "
            "FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "  AND status IN ('pending', 'in_progress', 'credit')"
        )
        row = await cur.fetchone()
        return {
            "pending": int(row["n_pending"] or 0),
            "in_progress": int(row["n_in_progress"] or 0),
            "credit": int(row["n_credit"] or 0),
        }

    async def get_installer_dashboard_metrics(self, installer_id: int) -> dict[str, Any]:
        """Сводка для монтажника — для welcome-card при синхронизации и daily_sync 09:00.

        Источники СТРОГО по спеке [[project_installer_sync_card_deploy_20260522]]:
        - Счёт End: montazh_stage='invoice_end' (AZ-колонка).
        - В работе / Не взято: assigned_to + installer_ok=1/0.
        - ЗП-монтажник: montazh_fact_op (BS-колонка), group by month(created_at).
        - ЗП в работе: zp_installer_amount (BJ-колонка) для не-ended/не-rejected.
        - Баланс аванса: каноническая db.get_advance_balance (single source of truth,
          = лист «Авансирование сотрудников».F «Кошелёк аванса»). Депозит/withdraw НЕ
          входят (feedback_installer_advance_spend_scope: «Депозит≠аванс»). До 11.06
          здесь был ошибочный инлайн (Σ все paid-типы − Σ offset_at) — завышал баланс
          на остаток депозита + withdraw (правило [[feedback_use_only_specified_sources]]).
        """
        from zoneinfo import ZoneInfo
        _now = datetime.now(ZoneInfo("Europe/Moscow"))
        year = _now.year

        # Счёт End за {year} — montazh_stage='invoice_end' (AZ-колонка).
        # Игорь мог быть assigned_to ИЛИ закрыть монтаж как installer_ok_by.
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM invoices "
            "WHERE (assigned_to = ? OR installer_ok_by = ?) "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(montazh_stage, '') = 'invoice_end' "
            "  AND strftime('%Y', created_at) = ?",
            (installer_id, installer_id, str(year)),
        )
        count_ended_year = int((await cur.fetchone())["cnt"] or 0)

        # В работе — installer_ok=1, status НЕ ended/rejected.
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM invoices "
            "WHERE assigned_to = ? "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(installer_ok, 0) = 1 "
            "  AND COALESCE(status, '') NOT IN ('ended', 'rejected')",
            (installer_id,),
        )
        count_in_work = int((await cur.fetchone())["cnt"] or 0)

        # Не взято в работу — всё активное (не ended/rejected, не invoice_end stage),
        # что НЕ закреплено за этим монтажником в активной работе.
        # Семантика: count_not_taken = total_active − count_ended − count_in_work.
        # Включает: assigned_to IS NULL, assigned_to=другой_монтажник,
        # assigned_to=Игорь+installer_ok=0.
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "  AND COALESCE(status, '') NOT IN ('ended', 'rejected') "
            "  AND COALESCE(montazh_stage, '') != 'invoice_end' "
            "  AND NOT (assigned_to = ? AND COALESCE(installer_ok, 0) = 1)",
            (installer_id,),
        )
        count_not_taken = int((await cur.fetchone())["cnt"] or 0)

        # Открытые задачи монтажника.
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM tasks "
            "WHERE assigned_to = ? AND status IN ('open', 'in_progress')",
            (installer_id,),
        )
        count_tasks = int((await cur.fetchone())["cnt"] or 0)
        count_unread_tasks = await self.count_unread_tasks(installer_id)

        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM chat_messages "
                "WHERE receiver_id = ? AND direction = 'incoming' AND is_read = 0",
                (installer_id,),
            )
            count_unread_msgs = int((await cur.fetchone())["cnt"] or 0)
        except Exception:
            count_unread_msgs = 0

        # ЗП-монтажник помесячно за {year}. Источник: R = estimated_installation
        # («Установка»), группировка по месяцу receipt_date (K «Дата пост.»).
        # Фильтр — ТОЛЬКО assigned_to (BD) = монтажник, БЕЗ доп.условий (user 11.06:
        # «R по K, BD=Игорь»). Намеренно учитываются счета в любой стадии монтажа
        # (in_work / installer_ok=0 тоже): это план ЗП по закреплённым за монтажником
        # счетам, а не факт принятых. Снят прежний строгий фильтр (installer_ok=1 +
        # status!=rejected + installer_ok_by OR) — он терял in_work-счета
        # (Игорь мар-май: 45203/372771/187491 → 280832/559886/331572).
        cur = await self.conn.execute(
            """
            SELECT
                CAST(strftime('%m', receipt_date) AS INTEGER) AS m,
                COALESCE(SUM(estimated_installation), 0) AS s
            FROM invoices
            WHERE assigned_to = ?
              AND strftime('%Y', receipt_date) = ?
            GROUP BY m
            ORDER BY m
            """,
            (installer_id, str(year)),
        )
        zp_monthly: dict[int, float] = {}
        for r in await cur.fetchall():
            if r["m"] is not None:
                zp_monthly[int(r["m"])] = float(r["s"] or 0)
        zp_total_year = sum(zp_monthly.values())

        # ЗП в работе (не выплачена) — R = estimated_installation, для счетов в работе.
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(estimated_installation), 0) AS s FROM invoices "
            "WHERE (assigned_to = ? OR installer_ok_by = ?) "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(montazh_stage, '') = 'in_work'",
            (installer_id, installer_id),
        )
        zp_in_work_unpaid = float((await cur.fetchone())["s"] or 0)

        # Баланс аванса — каноническая get_advance_balance (single source of truth,
        # та же, что лист «Авансирование сотрудников».F «Кошелёк аванса» и карточка
        # «Аванс — история»): Σ(paid type request/transfer) − Σ(offset→ZP), clamp ≥0.
        # Депозит и withdraw НЕ входят (feedback_installer_advance_spend_scope:
        # «Депозит≠аванс»; withdraw — отдельная колонка E листа). Прежняя инлайн-
        # формула (все paid-типы − offset_at) ошибочно завышала баланс на остаток
        # депозита + withdraw (Игорь: 80 300 ₽ вместо 0). Фикс 2026-06-11.
        balance_advance = await self.get_advance_balance(installer_id)

        return {
            "role_label": "Монтажник",
            "count_ended_year": count_ended_year,
            "count_in_work": count_in_work,
            "count_not_taken": count_not_taken,
            "count_tasks": count_tasks,
            "count_unread_tasks": count_unread_tasks,
            "count_unread_msgs": count_unread_msgs,
            "zp_monthly": zp_monthly,
            "zp_total_year": zp_total_year,
            "zp_in_work_unpaid": zp_in_work_unpaid,
            "balance_advance": balance_advance,
            "year": year,
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
            "SELECT * FROM tasks WHERE status = 'open' AND type != 'self_reminder' "
            "AND accepted_at IS NULL "
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
            "AND type != 'self_reminder' "
            "AND accepted_at IS NOT NULL AND accepted_at <= ? "
            "AND (reminder_2h_sent IS NULL OR reminder_2h_sent = 0)",
            (cutoff_iso,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_installer_tasks_needing_acceptance_reminder(self, cutoff_iso: str) -> list[dict]:
        """Installer-only: OPEN, not accepted, last reminder > cutoff."""
        cur = await self.conn.execute(
            "SELECT t.* FROM tasks t "
            "JOIN users u ON u.telegram_id = t.assigned_to "
            "WHERE t.status = 'open' AND t.accepted_at IS NULL "
            "AND (t.last_reminded_at IS NULL OR t.last_reminded_at <= ?) "
            "AND t.created_at <= ? "
            "AND (',' || COALESCE(u.role, '') || ',') LIKE '%,installer,%'",
            (cutoff_iso, cutoff_iso),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def list_installer_tasks_needing_post_accept_reminder(self, cutoff_iso: str) -> list[dict]:
        """Installer-only post-accept: accepted, status != done, last reminder (or accept) <= cutoff.

        Uses max(last_reminded_at, accepted_at) so the first post-accept tick fires N minutes
        after acceptance, and subsequent ticks fire N minutes after the prior reminder.
        """
        cur = await self.conn.execute(
            "SELECT t.* FROM tasks t "
            "JOIN users u ON u.telegram_id = t.assigned_to "
            "WHERE t.status IN ('open', 'in_progress') "
            "AND t.accepted_at IS NOT NULL "
            "AND (',' || COALESCE(u.role, '') || ',') LIKE '%,installer,%' "
            "AND max(coalesce(t.last_reminded_at, t.accepted_at), t.accepted_at) <= ?",
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
        minio_object_key: str | None = None,
    ) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            """
            INSERT INTO attachments(task_id, tg_file_id, tg_file_unique_id, file_type, caption, minio_object_key, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, file_id, file_unique_id, file_type, caption, minio_object_key, now),
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

    async def update_lead_note(self, amo_lead_id: int, text: str | None) -> None:
        """Store the responsible manager's latest amoCRM note (Sheet «Примечание»)."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET last_note = ?, updated_at = ? WHERE amo_lead_id = ?",
            (text, now, amo_lead_id),
        )
        await self.conn.commit()

    async def update_lead_rp(
        self, amo_lead_id: int, rp_source: str | None, rp_manager: str | None,
        rp_status: str | None = None, rp_invoice_number: str | None = None,
        rp_deal: str | None = None,
    ) -> None:
        """Store РП-table (Импорт ОП) override for a matched lead: источник/менеджер +
        статус(BX)/№счёта(BY)/сделка(BU). None clears the field → renderer falls back
        to the amoCRM value (для статуса — на amoCRM status_name)."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET rp_source = ?, rp_manager = ?, rp_status = ?, "
            "rp_invoice_number = ?, rp_deal = ?, updated_at = ? WHERE amo_lead_id = ?",
            (rp_source, rp_manager, rp_status, rp_invoice_number, rp_deal, now, amo_lead_id),
        )
        await self.conn.commit()

    async def list_leads_for_sheet(self, year: int = 2026) -> list[dict[str, Any]]:
        """Leads visible in Sheet «Leads» — given year, exclude status_id=143 (closed-lost).

        Returns newest-first (created_at DESC) so свежие лиды сверху листа.
        """
        cur = await self.conn.execute(
            """
            SELECT * FROM leads
            WHERE substr(created_at, 1, 4) = ?
              AND (status_id IS NULL OR status_id != 143)
            ORDER BY created_at DESC
            """,
            (str(year),),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def upsert_lead_by_amo_id(
        self,
        amo_lead_id: int,
        *,
        name: str | None = None,
        price: float | None = None,
        pipeline_id: int | None = None,
        status_id: int | None = None,
        responsible_user_id: int | None = None,
        phone: str | None = None,
        contact_name: str | None = None,
        tags_json: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """INSERT-or-UPDATE keyed by amo_lead_id (UNIQUE).

        On UPDATE: only overwrites fields where the new value is non-None
        (avoids clobbering data that wasn't in the webhook payload).
        """
        existing = await self.get_lead_by_amo_id(amo_lead_id)
        now = to_iso(utcnow())
        if existing is None:
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

        # UPDATE: COALESCE-style for None fields.
        await self.conn.execute(
            """
            UPDATE leads SET
                name = COALESCE(?, name),
                price = COALESCE(?, price),
                pipeline_id = COALESCE(?, pipeline_id),
                status_id = COALESCE(?, status_id),
                responsible_user_id = COALESCE(?, responsible_user_id),
                phone = COALESCE(?, phone),
                contact_name = COALESCE(?, contact_name),
                tags_json = COALESCE(?, tags_json),
                source = COALESCE(?, source),
                updated_at = ?
            WHERE amo_lead_id = ?
            """,
            (name, price, pipeline_id, status_id, responsible_user_id,
             phone, contact_name, tags_json, source, now, amo_lead_id),
        )
        await self.conn.commit()
        return await self.get_lead_by_amo_id(amo_lead_id) or existing

    async def delete_lead_by_amo_id(self, amo_lead_id: int) -> None:
        """Soft-delete: set status_id=143 (closed-lost). Keeps row for audit."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE leads SET status_id = 143, updated_at = ? WHERE amo_lead_id = ?",
            (now, amo_lead_id),
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
        minio_object_key: str | None = None,
    ) -> dict[str, Any]:
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            """
            INSERT INTO chat_attachments
                (chat_message_id, tg_file_id, tg_file_unique_id, file_type, caption, minio_object_key, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_message_id, tg_file_id, tg_file_unique_id, file_type, caption, minio_object_key, now),
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
        new_invoice_id = cur.lastrowid
        try:
            await self.audit(
                actor_id=None,
                action="invoice_created",
                entity="invoice",
                entity_id=str(new_invoice_id) if new_invoice_id is not None else None,
                payload={
                    "invoice_number": invoice_number_normalized,
                    "project_id": project_id,
                    "created_by": created_by,
                    "creator_role": creator_role,
                    "supplier": supplier,
                    "amount": amount,
                    "client_name": client_name,
                    "payment_terms": payment_terms,
                    "deadline_days": deadline_days,
                },
            )
        except Exception:
            log.exception("create_invoice: audit() failed for invoice_id=%s", new_invoice_id)
        return new_invoice_id  # type: ignore[return-value]

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
            # Если credit-счёт уже закрыт через _auto_close_credit_invoice
            # (status='ended'), не откатывать обратно к 'credit' при последующих
            # импортах из листа. Иначе ЗП-gate (status='ended') блокирует ЗП
            # менеджера навсегда (бизнес-правило 2026-05-16).
            if existing and existing.get("status") == InvoiceStatus.ENDED:
                return InvoiceStatus.ENDED
            return InvoiceStatus.CREDIT

        # ВАЖНО: автоматически НЕ выставляем status='ended' / 'paid' по
        # одному факту наличия actual_completion_date. По бизнес-семантике:
        #   - «Счёт End» — финальный статус, который менеджер ставит явно
        #     через InvoiceEndSG (после монтаж=invoice_end + долгов нет +
        #     документы подписаны).
        #   - Для credit-счетов закрытие происходит через
        #     _auto_close_credit_invoice (sheet_commands.py).
        # Раньше логика по completion_date+debt автоматически переводила
        # счёт в ENDED (или PAID), что приводило к half-state: status=ended,
        # но montazh_stage остался 'invoice_ok' и побочные эффекты не
        # отработали. Теперь оставляем существующий статус нетронутым.
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

        Exception — *preserve-if-empty* fields (``_PRESERVE_IF_EMPTY`` below):
        an empty value (``None``/``""``) never clears a non-empty stored value.
        A real value **including a literal ``0``** still overwrites — that is
        how «долг погашен» and «ЗП выплачена по ОП» keep working.
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
            "profit_tax", "profit_tax_op",            # S: Налог на приб. (был забыт)
            "rentability_calc", "rentability_fact_op",  # W: Рент-ть факт (был забыт)
            "payment_terms",
            "description", "contract_type", "closing_docs_status",
            "materials_fact_op",
            "montazh_fact_op",
            "zp_manager_request_text",
            "zp_manager_request_amount",
            "zp_manager_payout",
            "zp_manager_payout_date",
            "logistics_fact_op",
            "logistics_fact_date",
            "loaders_fact_op",
            "loaders_fact_date",
            "zamery_info_op",
            "agent_payout_op",
            "agent_payout_date_op",
            "rp_request_op",
            "rp_payout_op",
            "rp_payout_date_op",
            "rp_10_pct_op",
            "taxes_fact_op",
            "profit_calc_op",
            "zp_manager_hold",
            "cost_diff_calc_fact",
            "op_row_index",
        }

        created_by, creator_role = await self._resolve_invoice_import_owner(inv_num, payload, existing)
        status = self._compute_invoice_import_status(payload, existing)
        receipt_date = payload.get("receipt_date") if "receipt_date" in payload else (existing.get("receipt_date") if existing else None)
        deadline_days = payload.get("deadline_days") if "deadline_days" in payload else (existing.get("deadline_days") if existing else None)
        deadline_end_date = self._compute_deadline_end_date(receipt_date, deadline_days)

        # Cost-поля, влияющие на margin в карточке ГД (изменения через
        # webhook от Sheets/GAS-script должны быть видны в audit_log, иначе
        # «нагрузка ОП» падает в БД невидимо — инцидент 24.05 18:00 МСК,
        # когда loaders_fact_op заполнились между двумя smoke'ами без следов).
        COST_AUDIT_FIELDS = frozenset({
            "materials_fact_op", "montazh_fact_op",
            "logistics_fact_op", "loaders_fact_op",
            "agent_payout_op", "npn_amount", "rp_payout_op",
            "zp_manager_payout", "zp_manager_request_amount",
            "taxes_fact_op", "profit_calc_op", "manager_zp_blank",
            "outstanding_debt", "amount",
        })

        # Поля выплаты ЗП (fill-if-empty durable): пустая ячейка ОП (None/"") НЕ
        # затирает уже заполненное значение (бот-выплата на одобрении ГД или прежний
        # ОП). ОП с реальным значением (вкл. явный 0) перезаписывает — приоритет ОП
        # где данные есть. Остальные sheet-поля сохраняют контракт «None очищает».
        #   • ЗП менеджера AN/AO (zp_manager_payout/_date) — owner 19.06.
        #   • ЗП РП AR/AS (rp_payout_op/rp_payout_date_op) — owner 02.07: бот-выплаты
        #     ЗП РП (GD-flow gd.py) были ТРАНЗИЕНТНЫ — любой синк ОП стирал их в NULL
        #     при пустом источнике AW/AX, выплата жила только до ближайшего импорта.
        #     Теперь бот-выплата durable; ручной ввод owner'ом в «Импорт ОП» по-
        #     прежнему подхватывается. См. project_rp_ar_as_revert_20260702.
        _ZP_PAYOUT_PRESERVE = (
            "zp_manager_payout", "zp_manager_payout_date",
            "rp_payout_op", "rp_payout_date_op",
        )

        # Поля с ДЕНЕЖНОЙ побочкой — второй слой защиты (баг 9б, 05.08).
        # Первый слой — парс-слой sheets.py::_parse_op_row: с фикса 27.07 пустая
        # ячейка ключ вообще не кладёт. Слой ОДИН, и если он регрессирует снова,
        # обнуление долга не просто теряет число: db.py:5290-5303 зовёт
        # record_credit_debt_payment, тот пишет приход в credit_incomes и двигает
        # маркер переноса кошелька — бот выдумывает деньги, которых не было.
        # ⚠️ Литеральный 0 из ОП («долг погашен») — РЕАЛЬНОЕ значение и обязан
        #    перезаписывать: именно так пришли все 6 исторических приходов.
        #    Инцидент 273 000 (audit 8388) был как раз литеральным нулём из
        #    съехавшей строки, а НЕ пустой ячейкой — эта ветка его бы не поймала.
        # amount добавлен 06.08 по команде owner'а. В отличие от долга, у него есть
        # ДОКАЗАННЫЙ живой прецедент затирания пустой ячейкой: тот же audit 8388
        # (27.07 06:00) — 510 000 → None; вернулось только следующим импортом в 08:39.
        # ⚠️ Обратная сторона: намеренная очистка «Суммы счёта» через лист больше не
        #    пройдёт — только правкой в БД. Owner об этом предупреждён 06.08.
        _MONEY_PRESERVE = ("outstanding_debt", "amount")

        _PRESERVE_IF_EMPTY = _ZP_PAYOUT_PRESERVE + _MONEY_PRESERVE

        if existing:
            updates: dict[str, Any] = {"updated_at": now, "status": status}
            for field in sheet_fields:
                if field in payload:
                    val = payload[field]
                    if (
                        field in _PRESERVE_IF_EMPTY
                        and val in (None, "")
                        and existing.get(field) not in (None, "")
                    ):
                        continue  # пустая ОП-ячейка не стирает заполненный AN/AO и долг
                    updates[field] = val
            if "created_by" in payload and payload.get("created_by") not in (None, ""):
                updates["created_by"] = created_by
            if "creator_role" in payload and payload.get("creator_role"):
                updates["creator_role"] = creator_role
            if "receipt_date" in payload or "deadline_days" in payload:
                updates["deadline_end_date"] = deadline_end_date

            # Собрать diff cost-полей до UPDATE (для последующего audit_log).
            cost_diffs: dict[str, dict[str, Any]] = {}
            for k in COST_AUDIT_FIELDS:
                if k not in updates:
                    continue
                old_v = existing.get(k)
                new_v = updates[k]
                # 0 ↔ None считаются эквивалентом (numeric=0 == пусто).
                if (old_v or 0) != (new_v or 0):
                    cost_diffs[k] = {"old": old_v, "new": new_v}

            # Auto-promote zp_manager_status when payout proven from ОП.
            # Когда payout проставлен только в ОП (а в боте FSM не пройден —
            # legacy / offline-flow), это единственный путь синхронизировать
            # Invoices.AK на «Одобрено». Не трогаем уже не-«not_requested»
            # статусы, чтобы не сломать ручной flow менеджера.
            # Жёсткое условие: только если счёт уже в status='ended' (Счёт End).
            # Пока счёт «в работе» (pending/in_progress/paid/credit) — ЗП
            # менеджера заблокирована для всех сценариев включая авто-апрув.
            cur_status = (existing.get("zp_manager_status") or "")
            payout_val = updates.get("zp_manager_payout", existing.get("zp_manager_payout"))
            payout_date_val = updates.get("zp_manager_payout_date", existing.get("zp_manager_payout_date"))
            inv_status_eff = updates.get("status", existing.get("status"))
            if (
                cur_status in ("", "not_requested")
                and (payout_val or 0) > 0
                and payout_date_val
                and inv_status_eff == "ended"
            ):
                updates["zp_manager_status"] = "approved"

            sets = ", ".join(f"{k} = ?" for k in updates)
            vals = list(updates.values())
            vals.append(existing["id"])
            await self.conn.execute(f"UPDATE invoices SET {sets} WHERE id = ?", vals)

            if cost_diffs:
                await self.conn.execute(
                    """INSERT INTO audit_log(actor_id, action, entity, entity_id, payload_json, created_at)
                       VALUES (NULL, 'invoice_import_op_cost_change', 'invoice', ?, ?, ?)""",
                    (
                        str(existing["id"]),
                        json.dumps(
                            {"invoice_number": inv_num, "changes": cost_diffs},
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )

            # Кредит-приход (п.3 2026-06-12): гашение долга (AB «Долг» ↓ при оконч.
            # доплате AC) по is_credit-счёту → DISTINCT строка в истории движений
            # кошелька. cost_diffs хранит outstanding_debt только при old≠new, метод
            # сам гардит is_credit + только уменьшение (идемпотентно при ре-импорте).
            if "outstanding_debt" in cost_diffs:
                _dd = cost_diffs["outstanding_debt"]
                # is_credit смотрим по ОБОИМ снимкам (до и после UPDATE): косой
                # импорт может затереть признак тем же апдейтом, что двигает долг,
                # и обратный переход (реверс прихода) молча пропускался по старому
                # снимку — инцидент 273 000 27.07 (audit 8388/8389/8425).
                _inv_dp = dict(existing)
                if not _inv_dp.get("is_credit") and updates.get("is_credit"):
                    _inv_dp["is_credit"] = updates["is_credit"]
                await self.record_credit_debt_payment(
                    _inv_dp, _dd.get("old"), _dd.get("new"), source="op_import",
                )
                # ч.3.2: гашение долга через импорт ОП → авто-закрыть fixup «no_debts»
                await self.resolve_invoice_end_fixups(int(existing["id"]))

            if updates.get("zp_manager_status") == "approved" and cur_status != "approved":
                await self.conn.execute(
                    """INSERT INTO audit_log(actor_id, action, entity, entity_id, payload_json, created_at)
                       VALUES (NULL, 'invoice_zp_auto_approve', 'invoice', ?, ?, ?)""",
                    (
                        str(existing["id"]),
                        json.dumps(
                            {
                                "invoice_number": inv_num,
                                "old": cur_status or "not_requested",
                                "new": "approved",
                                "trigger": "import_invoice_from_sheet",
                                "zp_manager_payout": payout_val,
                                "zp_manager_payout_date": payout_date_val,
                            },
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )

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

        # Auto-promote on fresh INSERT (same logic as UPDATE branch above).
        # Жёсткое условие: только если новый счёт сразу импортируется с
        # status='ended' (Счёт End). Иначе ЗП менеджера остаётся
        # not_requested до перехода счёта в End.
        new_payout = fields_to_insert.get("zp_manager_payout") or 0
        new_payout_date = fields_to_insert.get("zp_manager_payout_date")
        new_status_eff = fields_to_insert.get("status")
        if new_payout > 0 and new_payout_date and new_status_eff == "ended":
            fields_to_insert["zp_manager_status"] = "approved"

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
        if fields_to_insert.get("zp_manager_status") == "approved":
            await self.conn.execute(
                """INSERT INTO audit_log(actor_id, action, entity, entity_id, payload_json, created_at)
                   VALUES (NULL, 'invoice_zp_auto_approve', 'invoice', ?, ?, ?)""",
                (
                    str(cur.lastrowid),
                    json.dumps(
                        {
                            "invoice_number": inv_num,
                            "old": "not_requested",
                            "new": "approved",
                            "trigger": "import_invoice_from_sheet (new)",
                            "zp_manager_payout": new_payout,
                            "zp_manager_payout_date": new_payout_date,
                        },
                        ensure_ascii=False,
                    ),
                    now,
                ),
            )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_invoice(self, invoice_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM invoices WHERE id = ?", (invoice_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_installer_id_for_invoice(self, invoice_id: int) -> int | None:
        """Telegram ID монтажника по счёту, с fallback цепочкой.

        1) invoices.assigned_to (если у пользователя есть role 'installer')
        2) invoices.installer_ok_by
        3) Последний installer_ok task — assigned_to (RP/ГД → монтажник)
        4) Последний zp_installer task — created_by (монтажник → ГД)

        Используется для трансляции в Invoices.AW. Возвращает None если ни один
        источник не сработал (новый счёт без активных монтажных task'ов).
        """
        cur = await self.conn.execute(
            "SELECT i.assigned_to, COALESCE(u.role, '') "
            "FROM invoices i "
            "LEFT JOIN users u ON u.telegram_id = i.assigned_to "
            "WHERE i.id = ?",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if row and row[0] and 'installer' in (row[1] or ''):
            return int(row[0])

        cur = await self.conn.execute(
            "SELECT installer_ok_by FROM invoices WHERE id = ?", (invoice_id,)
        )
        row = await cur.fetchone()
        if row and row[0]:
            return int(row[0])

        cur = await self.conn.execute(
            "SELECT assigned_to FROM tasks "
            "WHERE type = 'installer_ok' "
            "AND CAST(json_extract(payload_json, '$.invoice_id') AS INTEGER) = ? "
            "AND assigned_to IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 1",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if row and row[0]:
            return int(row[0])

        cur = await self.conn.execute(
            "SELECT created_by FROM tasks "
            "WHERE type = 'zp_installer' "
            "AND CAST(json_extract(payload_json, '$.invoice_id') AS INTEGER) = ? "
            "AND created_by IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 1",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if row and row[0]:
            return int(row[0])

        return None

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
        statuses: list[str] | None = None,
        include_credit: bool = False,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if created_by is not None:
            clauses.append("created_by = ?")
            params.append(created_by)
        if assigned_to is not None:
            clauses.append("assigned_to = ?")
            params.append(assigned_to)
        if statuses is not None and len(statuses) > 0:
            placeholders = ",".join("?" * len(statuses))
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        elif status is not None:
            clauses.append("status = ?")
            params.append(status)
        if project_id is not None:
            clauses.append("project_id = ?")
            params.append(project_id)
        if marker is not None:
            clauses.append("invoice_number LIKE ?")
            params.append(f"%{marker}%")
        if only_regular:
            if include_credit:
                clauses.append("(invoice_number GLOB '[0-9]*-*' OR is_credit = 1)")
            else:
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
        old_status: str | None = None
        try:
            cur = await self.conn.execute(
                "SELECT status FROM invoices WHERE id = ?", (invoice_id,)
            )
            row = await cur.fetchone()
            if row is not None:
                old_status = row[0] if not isinstance(row, dict) else row.get("status")
        except Exception:
            log.exception("update_invoice_status: SELECT old_status failed for invoice_id=%s", invoice_id)
        await self.conn.execute(
            "UPDATE invoices SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now, invoice_id),
        )
        await self.conn.commit()
        try:
            await self.audit(
                actor_id=None,
                action="invoice_status_changed",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"old_status": old_status, "new_status": new_status},
            )
        except Exception:
            log.exception("update_invoice_status: audit() failed for invoice_id=%s", invoice_id)

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
        try:
            await self.audit(
                actor_id=None,
                action="invoice_installer_ok",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"ok": bool(ok), "installer_ok_at": fields.get("installer_ok_at")},
            )
        except Exception:
            log.exception("set_invoice_installer_ok: audit() failed for invoice_id=%s", invoice_id)
        if ok:
            await self.resolve_invoice_end_fixups(invoice_id)

    async def set_invoice_edo_signed(
        self, invoice_id: int, signed: bool = True, actor_id: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {"edo_signed": int(signed)}
        if signed:
            fields["edo_signed_at"] = to_iso(utcnow())
        else:
            fields["edo_signed_at"] = None
        await self.update_invoice(invoice_id, **fields)
        try:
            await self.audit(
                actor_id=actor_id,
                action="invoice_edo_signed",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"signed": bool(signed), "edo_signed_at": fields.get("edo_signed_at")},
            )
        except Exception:
            log.exception("set_invoice_edo_signed: audit() failed for invoice_id=%s", invoice_id)
        if signed:
            await self.resolve_invoice_end_fixups(invoice_id)

    async def set_invoice_docs_edo_signed(
        self, invoice_id: int, signed: bool = True, actor_id: int | None = None,
    ) -> None:
        """Set primary-docs (первичка) EDO-signed flag with attribution + audit.

        Записывает docs_edo_signed_at/_by при signed=True, очищает при unsign.
        """
        fields: dict[str, Any] = {"docs_edo_signed": int(signed)}
        if signed:
            fields["docs_edo_signed_at"] = to_iso(utcnow())
            fields["docs_edo_signed_by"] = actor_id
        else:
            fields["docs_edo_signed_at"] = None
            fields["docs_edo_signed_by"] = None
        await self.update_invoice(invoice_id, **fields)
        try:
            await self.audit(
                actor_id=actor_id,
                action="invoice_docs_edo_signed",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"signed": bool(signed), "docs_edo_signed_at": fields.get("docs_edo_signed_at")},
            )
        except Exception:
            log.exception("set_invoice_docs_edo_signed: audit() failed for invoice_id=%s", invoice_id)
        if signed:
            await self.resolve_invoice_docs_missing(invoice_id)

    async def set_docs_originals_holder(
        self, invoice_id: int, holder: str | None, actor_id: int | None = None,
    ) -> None:
        """Set primary-docs (первичка) originals holder with audit. holder: 'gd'|'manager'|None."""
        await self.update_invoice(invoice_id, docs_originals_holder=holder)
        try:
            await self.audit(
                actor_id=actor_id,
                action="invoice_docs_originals_holder",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"holder": holder},
            )
        except Exception:
            log.exception("set_docs_originals_holder: audit() failed for invoice_id=%s", invoice_id)
        if holder:
            await self.resolve_invoice_docs_missing(invoice_id)

    async def set_closing_originals_holder(
        self, invoice_id: int, holder: str | None, actor_id: int | None = None,
    ) -> None:
        """Set closing-docs (закрывающие) originals holder with audit. holder: 'gd'|'manager'|None."""
        await self.update_invoice(invoice_id, closing_originals_holder=holder)
        try:
            await self.audit(
                actor_id=actor_id,
                action="invoice_closing_originals_holder",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={"holder": holder},
            )
        except Exception:
            log.exception("set_closing_originals_holder: audit() failed for invoice_id=%s", invoice_id)

    async def set_invoice_no_debts(
        self, invoice_id: int, no_debts: bool = True
    ) -> None:
        fields: dict[str, Any] = {"no_debts": int(no_debts)}
        if no_debts:
            fields["no_debts_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)
        if no_debts:
            await self.resolve_invoice_end_fixups(invoice_id)

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
        approved_by: int | None = None,
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
            if approved_by is not None:
                fields["zp_manager_approved_by"] = approved_by
            # Одобрение ГД ≠ выплата (owner 09.07, вариант A «как у ЗП РП»): статус
            # approved НЕ заполняет AN/AO. «Выплачено» = AN>0, а сама выплата —
            # отдельный явный шаг «💳 Выплатить» (td.gd_zp_manager_pay →
            # _finalize_zp_manager_pay пишет zp_manager_payout/_date прямым UPDATE,
            # как AR/AS у ЗП РП). Прежний Route-A auto-fill (owner 19.06) отменён:
            # приём задачи не переводит деньги без реальной выплаты (баг #49
            # Очаковское — approve без платёжки помечал ЗП выплаченной). Легитимные
            # писатели AN сохранены: импорт ОП (import_invoice_from_sheet), зачёт
            # аванса (apply_advance_offsets_on_zp_approve step3 /
            # apply_manager_advance_immediate).
        await self.update_invoice(invoice_id, **fields)

    async def set_invoice_zp_installer_status(
        self,
        invoice_id: int,
        status: str,
        amount: float | None = None,
        requested_by: int | None = None,
        is_remainder: bool | None = None,
    ) -> None:
        """Update installer ZP status on invoice.

        is_remainder (Часть 2, 2026-06-08): True — заявка = ОСТАТОК ЗП (бот платит
        остаток, аванс зачтён отдельно) → «Выплачено» на листе считается ADDITIVE
        (аванс×1.10 + бот). None — поле не трогаем. При status='not_requested'
        (отклонение ГД) флаг сбрасывается в 0 — следующий запрос проставит заново.
        """
        fields: dict[str, Any] = {"zp_installer_status": status}
        if amount is not None:
            fields["zp_installer_amount"] = amount
        if requested_by is not None:
            fields["zp_installer_requested_by"] = requested_by
        if is_remainder is not None:
            fields["zp_installer_remainder"] = 1 if is_remainder else 0
        elif status == "not_requested":
            fields["zp_installer_remainder"] = 0
        if status == "requested":
            fields["zp_installer_requested_at"] = to_iso(utcnow())
        elif status == "approved":
            fields["zp_installer_approved_at"] = to_iso(utcnow())
        elif status == "payment_sent":
            fields["zp_installer_payment_sent_at"] = to_iso(utcnow())
        elif status == "confirmed":
            fields["zp_installer_confirmed_at"] = to_iso(utcnow())
        await self.update_invoice(invoice_id, **fields)
        if status == "payment_sent":
            await self._auto_invoice_end_after_zp_payment(invoice_id)

    async def _auto_invoice_end_after_zp_payment(self, invoice_id: int) -> None:
        """«Счёт ОК» → «Счёт End» при выплате ЗП монтаж (owner 2026-07-26).

        Монтажник по окончании работ ставит «Счёт ОК»; выплата ЗП монтаж закрывает
        этап монтажа сама. Точка одна на все пути выплаты (ГД с платёжкой и без —
        `_finalize_installer_zp_payment`; закрытие тратой кред-кошелька —
        `resolve_installer_zp_by_wallet_payment`; авто-закрытие авансами).

        ⛔ Меняем ТОЛЬКО montazh_stage. Статус самого счёта (`ended`) не трогаем —
        его по-прежнему закрывает «Счёт End» менеджера/РП со своими проверками.
        Переводим лишь со стадии `invoice_ok`: на assigned/in_work/razmery_ok
        монтажник окончание работ ещё не подтвердил. Идемпотентно — повторный
        payment_sent со стадии invoice_end условия уже не проходит.
        """
        try:
            cur = await self.conn.execute(
                "SELECT montazh_stage FROM invoices WHERE id = ?", (invoice_id,)
            )
            row = await cur.fetchone()
        except Exception:
            log.exception("auto invoice_end: read stage failed inv=%s", invoice_id)
            return
        if row is None:
            return
        stage = row[0] if not isinstance(row, dict) else row.get("montazh_stage")
        if stage != "invoice_ok":
            return
        await self.update_montazh_stage(invoice_id, "invoice_end")
        try:
            await self.audit(
                actor_id=None,
                action="montazh_stage_auto_invoice_end",
                entity="invoice",
                entity_id=str(invoice_id),
                payload={
                    "from": "invoice_ok",
                    "to": "invoice_end",
                    "trigger": "zp_installer_payment_sent",
                },
            )
        except Exception:
            log.exception("auto invoice_end: audit failed inv=%s", invoice_id)

    async def list_pending_zp_requests(
        self, zp_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Return invoices with pending ZP requests.

        zp_type: 'zamery' | 'manager' | 'installer' | None (all).

        manager (owner 09.07, вариант A): двухступенчатая модель как у installer —
        показываем и 'requested' (ждёт одобрения), и 'approved' с невыплаченной AN
        (zp_manager_payout=0, ждёт нажатия «💳 Выплатить»). Approved с AN>0 —
        реальная выплата (из «Импорт ОП» или зачёта аванса) → из очереди уходит.
        """
        conditions = {
            "zamery": "zp_status = 'requested'",
            "manager": (
                "(zp_manager_status = 'requested' "
                " OR (zp_manager_status = 'approved' "
                "     AND COALESCE(zp_manager_payout, 0) = 0))"
            ),
            "installer": "zp_installer_status IN ('requested', 'approved')",
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
        # no_debts (ч.2 2026-06-12): авто из колонки «Долг» (AE = outstanding_debt) —
        # долг закрыт, если outstanding_debt<=0. Q1: ручной флаг ГД (no_debts)
        # сохранён как override на случай отставания листа.
        # Кредит-счёт: ЭДО-документооборота для закрытия нет (бухгалтерия в
        # credit-flow не участвует — sheet_commands.py:639). Условие ЭДО считаем
        # выполненным → «Счёт End» не блокируется им, ЭДО не упоминается в
        # сообщении об условиях и не порождает fixup-задачу менеджеру (owner 24.06).
        is_credit = bool(inv.get("is_credit")) or str(
            inv.get("invoice_number") or ""
        ).upper().startswith("ЗМ")
        return {
            "installer_ok": bool(inv.get("installer_ok")),
            "edo_signed": True if is_credit else bool(inv.get("edo_signed")),
            "no_debts": (float(inv.get("outstanding_debt") or 0) <= 0)
            or bool(inv.get("no_debts")),
            "zp_approved": inv.get("zp_status") == "approved",
        }

    async def has_open_invoice_end_fixups(self, invoice_id: int) -> bool:
        """Есть ли открытые задачи INVOICE_END_FIXUP по счёту.

        Блокировка ЗП менеджера после форс-закрытия счёта ГД (ч.3.3, Q4 — блок
        только на стороне менеджера). True пока хоть один пункт не устранён.
        """
        from .enums import TaskType, TaskStatus
        tasks = await self.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.INVOICE_END_FIXUP],
            limit=20,
        )
        for t in tasks:
            if t.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                continue
            try:
                payload = json.loads(t.get("payload_json") or "{}")
            except (ValueError, TypeError):
                continue
            # search_tasks_by_payload использует LIKE — отсекаем substring-совпадения
            if int(payload.get("invoice_id") or 0) == int(invoice_id):
                return True
        return False

    async def resolve_invoice_end_fixups(
        self, invoice_id: int
    ) -> list[dict[str, Any]]:
        """Авто-закрыть задачи INVOICE_END_FIXUP, чьё условие теперь выполнено (ч.3.2, Q3).

        Вызывается из всех точек, где меняются условия закрытия счёта
        (set_invoice_installer_ok / set_invoice_edo_signed / set_invoice_no_debts +
        пути записи outstanding_debt). Возвращает список закрытых задач.
        Дешёвый no-op, если открытых fixup-задач по счёту нет.
        """
        from .enums import TaskType, TaskStatus
        tasks = await self.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.INVOICE_END_FIXUP],
            limit=20,
        )
        open_tasks = [
            t
            for t in tasks
            if t.get("status") in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS)
        ]
        if not open_tasks:
            return []
        conditions = await self.check_close_conditions(invoice_id)
        closed: list[dict[str, Any]] = []
        for t in open_tasks:
            try:
                payload = json.loads(t.get("payload_json") or "{}")
            except (ValueError, TypeError):
                continue
            if int(payload.get("invoice_id") or 0) != int(invoice_id):
                continue  # LIKE substring guard
            key = payload.get("condition_key")
            if key and conditions.get(key):
                updated = await self.update_task_status(
                    int(t["id"]),
                    TaskStatus.DONE,
                    expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
                )
                if updated:
                    closed.append(updated)
        return closed

    # ------------------------------------------------------------------
    # ТЗ 14.06 — трекинг финального платежа по долгу (FINAL_PAYMENT_ETA)
    # ------------------------------------------------------------------
    async def has_open_final_payment_eta_task(self, invoice_id: int) -> bool:
        """Есть ли открытая задача FINAL_PAYMENT_ETA по счёту (дедуп — 1 задача)."""
        from .enums import TaskType, TaskStatus
        tasks = await self.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.FINAL_PAYMENT_ETA],
            limit=20,
        )
        for t in tasks:
            if t.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                continue
            try:
                payload = json.loads(t.get("payload_json") or "{}")
            except (ValueError, TypeError):
                continue
            # search_tasks_by_payload — LIKE; отсекаем substring-совпадения
            if int(payload.get("invoice_id") or 0) == int(invoice_id):
                return True
        return False

    async def invoice_end_prompt_blocked(self, invoice_id: int) -> bool:
        """Дедуп напоминания «счёт готов к закрытию» (ТЗ 18.06): True, если по
        счёту уже открыта задача INVOICE_END_READY ИЛИ INVOICE_END_REQUEST
        (менеджер уже начал закрытие) — тогда новое напоминание не плодим."""
        from .enums import TaskType, TaskStatus
        tasks = await self.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.INVOICE_END_READY, TaskType.INVOICE_END_REQUEST],
            limit=30,
        )
        for t in tasks:
            if t.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                continue
            try:
                payload = json.loads(t.get("payload_json") or "{}")
            except (ValueError, TypeError):
                continue
            if int(payload.get("invoice_id") or 0) == int(invoice_id):
                return True
        return False

    async def count_invoices_ready_for_end(self, manager_id: int) -> int:
        """ТЗ 18.06: число материнских счетов менеджера, готовых к закрытию
        (монтаж «Счёт ОК» + долга нет + статус активный) — для бейджа 🔴 на
        кнопке «Счет End». Совпадает со списком в start_invoice_end (created_by
        + status in_progress/paid) + доп. условия монтаж/долг."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS c FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "AND created_by = ? "
            "AND status IN ('in_progress', 'paid', 'credit') "
            "AND montazh_stage = 'invoice_ok' "
            "AND COALESCE(outstanding_debt, 0) <= 0",
            (manager_id,),
        )
        row = await cur.fetchone()
        return int((dict(row).get("c") if row else 0) or 0)

    async def count_recalc_confirm_tasks(self, user_id: int) -> int:
        """ТЗ 02.07: число открытых задач «Перерасчёт прибыли → согласие»,
        назначенных менеджеру (assigned_to=user_id) — для бейджа 🔴 на кнопке
        «💰 Финансы»/«Ещё». Задача создаётся ГД из карточки перерасчёта."""
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS c FROM tasks "
            "WHERE type = 'recalc_confirm' AND status = 'open' AND assigned_to = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        return int((dict(row).get("c") if row else 0) or 0)

    async def list_recalc_confirm_tasks(self, user_id: int) -> list[dict[str, Any]]:
        """ТЗ 02.07: открытые recalc_confirm-задачи менеджера (для экрана «Финансы»
        и agree-хендлера). Свежие — первыми."""
        cur = await self.conn.execute(
            "SELECT * FROM tasks "
            "WHERE type = 'recalc_confirm' AND status = 'open' AND assigned_to = ? "
            "ORDER BY id DESC",
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def invoice_recalc_already_sent(self, invoice_id: int) -> bool:
        """ТЗ 02.07: отправляли ли уже перерасчёт по этому счёту менеджеру.

        Блокирует ПОВТОРНУЮ отправку ГД, пока по счёту висит ОТКРЫТАЯ задача
        (ждём согласия менеджера).

        Фикс 30.07: раньше блокировал и по 'done'. Но задачу можно закрыть мимо
        зачисления — устаревшей generic-кнопкой «Завершить» из старого сообщения
        ([[feedback_fsm_old_buttons_trap]]); ровно так 03.07 закрылись три задачи
        (336/337/338), денег по ним не двигалось. Счёт после этого блокировался
        НАВСЕГДА при нулевом движении денег. Теперь защита от двойного аванса
        стоит на деньгах, а не на статусе задачи: полностью перенесённая переплата
        выпадает из list_invoices_under_recalc, а частичный остаток можно дослать."""
        cur = await self.conn.execute(
            "SELECT payload_json FROM tasks "
            "WHERE type = 'recalc_confirm' AND status = 'open'",
        )
        for r in await cur.fetchall():
            try:
                p = json.loads(r["payload_json"] or "{}")
            except (ValueError, TypeError):
                continue
            if int(p.get("invoice_id") or 0) == int(invoice_id):
                return True
        return False

    async def list_invoices_ready_for_end(self) -> list[dict[str, Any]]:
        """ТЗ 18.06: все материнские счета (любых менеджеров), готовые к закрытию
        — монтаж «Счёт ОК» + долга нет + статус активный. Догоняющий проход
        daily_sync создаёт напоминание (дедуп — invoice_end_prompt_blocked)."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "AND status IN ('in_progress', 'paid', 'credit') "
            "AND montazh_stage = 'invoice_ok' "
            "AND COALESCE(outstanding_debt, 0) <= 0 "
            "ORDER BY id ASC"
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_invoices_missing_primary_docs(self) -> list[dict[str, Any]]:
        """ТЗ 18.06 (B): активные материнские б/н счета без первичных документов
        (нет ЭДО первички И нет оригиналов первички). Догоняющий проход daily_sync
        ставит менеджеру (created_by) задачу INVOICE_DOCS_MISSING. Дедуп по открытой
        задаче — внутри prompt_invoice_docs_missing."""
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "AND COALESCE(is_credit, 0) = 0 "
            "AND status = 'in_progress' "
            "AND COALESCE(docs_edo_signed, 0) = 0 "
            "AND (docs_originals_holder IS NULL OR docs_originals_holder = '') "
            "ORDER BY id ASC"
        )
        return [dict(r) for r in await cur.fetchall()]

    async def invoice_docs_missing_blocked(self, invoice_id: int) -> bool:
        """Дедуп задачи «нет документов» (B): True, если по счёту уже открыта
        задача INVOICE_DOCS_MISSING."""
        from .enums import TaskType, TaskStatus
        tasks = await self.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.INVOICE_DOCS_MISSING],
            limit=30,
        )
        for t in tasks:
            if t.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                continue
            try:
                payload = json.loads(t.get("payload_json") or "{}")
            except (ValueError, TypeError):
                continue
            if int(payload.get("invoice_id") or 0) == int(invoice_id):
                return True
        return False

    async def resolve_invoice_docs_missing(self, invoice_id: int) -> int:
        """Закрыть открытые задачи INVOICE_DOCS_MISSING по счёту (первичка появилась)."""
        from .enums import TaskType
        return await self.close_tasks_by_invoice(invoice_id, TaskType.INVOICE_DOCS_MISSING)

    async def manager_zp_block_reason(
        self, invoice_id: int, inv: dict[str, Any] | None = None
    ) -> str | None:
        """Причина блокировки ЗП менеджера по счёту, или None если запрос разрешён.

        Блок: (1) непогашенный долг по счёту (outstanding_debt > 0) — ТЗ 14.06;
        (2) открытые задачи INVOICE_END_FIXUP (ч.3.3, форс-закрытие ГД).
        """
        if inv is None:
            inv = await self.get_invoice(invoice_id)
        if float((inv or {}).get("outstanding_debt") or 0) > 0:
            return "по счёту есть непогашенный долг"
        if await self.has_open_invoice_end_fixups(invoice_id):
            return "по счёту есть незакрытые задачи «Счёт-END»"
        return None

    async def set_final_payment_eta(self, invoice_id: int, date_iso: str) -> None:
        """Менеджер указал ориент. дату фин. платежа → дата + state='planned'."""
        await self.conn.execute(
            "UPDATE invoices SET planned_final_payment_date = ?, "
            "final_payment_track_state = 'planned' WHERE id = ?",
            (date_iso, invoice_id),
        )
        await self.conn.commit()

    async def set_final_payment_track_state(self, invoice_id: int, state: str) -> None:
        """Перевод трекинг-состояния фин. платежа ('', 'planned', 'overdue', 'paid')."""
        await self.conn.execute(
            "UPDATE invoices SET final_payment_track_state = ? WHERE id = ?",
            (state, invoice_id),
        )
        await self.conn.commit()

    async def list_invoices_tracking_final_payment(self) -> list[dict[str, Any]]:
        """Материнские счета в трекинге фин. платежа (state planned/overdue/paid).

        Используется daily_sync (переходы overdue/paid, 1× на событие) и
        GD-картой «Синхронизация данных» (секция «Долги»).
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE final_payment_track_state IN ('planned','overdue','paid') "
            "AND parent_invoice_id IS NULL "
            "ORDER BY date(substr(COALESCE(planned_final_payment_date,''),1,10)) ASC, id ASC"
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_invoices_missing_final_payment_task(self) -> list[dict[str, Any]]:
        """Материнские счета, подпадающие под условия задачи фин. платежа,
        но ещё НЕ в трекинге (state '' / NULL).

        Условия = те же, что у триггера request_final_payment_eta:
        parent_invoice_id IS NULL + долг (outstanding_debt) > 0 + этап монтажа
        «Счёт ОК»/«Счёт End». Используется догоняющим проходом daily_sync —
        ловит счета, у которых этап наступил ДО запуска фичи или долг появился
        позже смены этапа (триггер срабатывает только в момент смены этапа).
        Дедуп по открытой задаче делает сам request_final_payment_eta.
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "AND COALESCE(outstanding_debt, 0) > 0 "
            "AND montazh_stage IN ('invoice_ok', 'invoice_end') "
            "AND COALESCE(final_payment_track_state, '') = '' "
            "ORDER BY id ASC"
        )
        return [dict(r) for r in await cur.fetchall()]

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
        cost_type: str | None = None,
    ) -> int:
        """Добавить расход кредитных средств по счёту.

        cost_type — код типа затрат (metal/glass/extra_mat/install/loaders/
        logistics); None для авто-записей из чат-каналов.
        """
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO credit_expenses "
            "(invoice_id, amount, description, entered_by, chat_message_id, cost_type, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (invoice_id, amount, description, entered_by, chat_message_id, cost_type, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_credit_expenses_summary(self, invoice_id: int) -> dict[str, Any]:
        """Получить сводку расходов кредитных средств по счёту.

        Returns: {"total": float, "log": str, "items": list[dict]}

        log: только описания через запятую — дата и сумма пишутся в CX/CY отдельно.
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
        descriptions = [(r.get("description") or "—") for r in rows]

        return {"total": total, "log": ", ".join(descriptions), "items": rows}

    async def get_credit_carry_in(self, invoice_id: int) -> float:
        """Running carry-over (DA) от всех предыдущих закрытых is_credit invoices.

        Используется для CV «Кредит вход» текущего КВ-счёта: остатки от прошлых
        КВ суммируются и прибавляются к CV следующего. DA-маркер — credit_expenses
        entry с description начинающимся на «Остаток …»; полностью закрытые
        счета (status=ended) считаются с CX=CV → residual=0.
        """
        cur = await self.conn.execute(
            "SELECT created_at FROM invoices WHERE id = ? AND is_credit = 1",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if not row:
            return 0.0
        current_created = row["created_at"]

        cur = await self.conn.execute(
            "SELECT id, amount, outstanding_debt, status, montazh_stage "
            "FROM invoices "
            "WHERE is_credit = 1 AND id != ? AND created_at < ? "
            "ORDER BY created_at ASC",
            (invoice_id, current_created),
        )
        rows = [dict(r) for r in await cur.fetchall()]

        running = 0.0
        for r in rows:
            paid = float(r.get("amount") or 0) - float(r.get("outstanding_debt") or 0)
            cv_prev = paid + running

            # _credit_fully_closed (как в sheets.py:675-677): status=ended ИЛИ montazh_stage='invoice_end'.
            is_fully_closed = (
                r.get("status") == "ended"
                or r.get("montazh_stage") == "invoice_end"
            )
            if is_fully_closed:
                cx_prev = cv_prev
            else:
                # Маркер «Остаток …» в credit_expenses → CX = CV − Остаток
                summary = await self.get_credit_expenses_summary(int(r["id"]))
                items = summary.get("items") or []
                balance_remainder = None
                for it in reversed(items):
                    d = (it.get("description") or "").strip().lower()
                    if d.startswith("остаток"):
                        try:
                            balance_remainder = float(it.get("amount") or 0)
                        except (TypeError, ValueError):
                            balance_remainder = None
                        break
                if balance_remainder is not None:
                    cx_prev = cv_prev - balance_remainder
                else:
                    cx_prev = float(summary.get("total") or 0)

            # Перенос = DA самого свежего ОТКРЫТОГО кредит-счёта, НЕ накопительная
            # сумма. Каждый открытый счёт в cv_prev уже включает перенос предыдущего,
            # поэтому running ЗАМЕНЯЕТСЯ его DA (residual), а не суммируется — иначе
            # остатки прежних открытых КВ задваиваются на каждом новом счёте
            # (баг: КВ9 показывал 793 400 вместо 544 000). Закрытые счета (DA=0,
            # израсходованы полностью) перенос не меняют.
            residual = cv_prev - cx_prev
            if not is_fully_closed:
                running = residual if residual > 0 else 0.0

        return running

    async def get_active_credit_invoice_for_channel(
        self, channel: str
    ) -> dict[str, Any] | None:
        """Найти «активный» open credit-счёт для канала менеджера.

        Активный = самый новый (по created_at) open credit (status='credit',
        is_credit=1, creator_role совпадает с ролью канала) с DA > 0.

        Возвращает dict invoice или None если активного нет.

        channel: 'manager_kv' | 'manager_kia' | 'manager_npn'.
        """
        channel_to_role = {
            "manager_kv": "manager_kv",
            "manager_kia": "manager_kia",
            "manager_npn": "manager_npn",
        }
        role = channel_to_role.get(channel)
        if not role:
            return None

        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE is_credit = 1 AND status = 'credit' AND creator_role = ? "
            "ORDER BY created_at DESC, id DESC",
            (role,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        for inv in rows:
            amount = float(inv.get("amount") or 0)
            debt = float(inv.get("outstanding_debt") or 0)
            paid_credit = amount - debt
            carry = await self.get_credit_carry_in(int(inv["id"]))
            eff_cv = paid_credit + carry
            summary = await self.get_credit_expenses_summary(int(inv["id"]))
            items = summary.get("items") or []
            residue: float | None = None
            for it in reversed(items):
                d = (it.get("description") or "").strip().lower()
                if d.startswith("остаток"):
                    try:
                        residue = float(it.get("amount") or 0)
                    except (TypeError, ValueError):
                        residue = None
                    break
            if residue is not None:
                da = residue
            else:
                sum_ce = float(summary.get("total") or 0)
                da = eff_cv - sum_ce
            if da > 0:
                inv["_da"] = da
                inv["_eff_cv"] = eff_cv
                return inv
        return None

    async def get_credit_balance_summary(self, creator_role: str) -> dict[str, Any]:
        """Баланс кредитных счетов по менеджеру (КВ/КИА/НПН).

        Зеркалит логику sheets.py CV(99)/CX(101)/DA для каждого is_credit=1
        счёта данного creator_role. Carry-in (DA от прошлых закрытых КВ)
        вычисляется через get_credit_carry_in. Возвращает:
          {invoices: [{id, invoice_number, object_address, is_closed, cv, cx, da}],
           total_da: running баланс (для следующего КВ)}
        """
        cur = await self.conn.execute(
            "SELECT * FROM invoices "
            "WHERE creator_role = ? AND is_credit = 1 "
            "ORDER BY created_at ASC, id ASC",
            (creator_role,),
        )
        rows = [dict(r) for r in await cur.fetchall()]

        result: list[dict[str, Any]] = []
        for inv in rows:
            inv_id = int(inv["id"])
            amount = float(inv.get("amount") or 0)
            debt = float(inv.get("outstanding_debt") or 0)
            paid_credit = amount - debt
            carry_in = await self.get_credit_carry_in(inv_id)
            cv = paid_credit + carry_in

            summary = await self.get_credit_expenses_summary(inv_id)
            items = summary.get("items") or []
            balance_remainder: float | None = None
            for it in reversed(items):
                d = (it.get("description") or "").strip().lower()
                if d.startswith("остаток"):
                    try:
                        balance_remainder = float(it.get("amount") or 0)
                    except (TypeError, ValueError):
                        balance_remainder = None
                    break
            ce_total = float(summary.get("total") or 0)

            is_closed = (
                inv.get("status") == "ended"
                or inv.get("montazh_stage") == "invoice_end"
            )
            if is_closed:
                cx = cv
            elif balance_remainder is not None:
                cx = cv - balance_remainder
            else:
                cx = ce_total
            da = cv - cx

            result.append({
                "id": inv_id,
                "invoice_number": inv.get("invoice_number") or f"#{inv_id}",
                "object_address": inv.get("object_address") or "",
                "is_closed": is_closed,
                "cv": cv,
                "cx": cx,
                "da": da,
            })

        # total_da: остаток кошелька. Durable-модель (19.06): если есть
        # авторитетный якорь-сверка (credit_wallet_anchors) — котёл = якорь +
        # Σ(приходы после) − Σ(расходы после), НЕ зависит от «последнего открытого
        # счёта» (устойчиво к созданию нового КВ / закрытию якорь-счёта). Иначе —
        # legacy carry-DA последнего открытого КВ (ничего не ломается без якоря).
        # per-invoice CV/CX/DA в result[] НЕ трогаются → лист (sheets.py) цел.
        legacy_total_da = 0.0
        for r in result:
            if not r["is_closed"]:
                legacy_total_da = r["da"]
        total_da = await self._credit_wallet_pot(creator_role, legacy_total_da)

        return {"invoices": result, "total_da": total_da}

    async def _credit_wallet_pot(self, role: str, fallback: float) -> float:
        """Остаток кредит-кошелька от авторитетного якоря-сверки + движения после.

        pot = anchor.amount + Σ(IN после anchor.ts) − Σ(OUT после anchor.ts).
        DIRECT SQL — НЕ через list_all_credit_events (та зовёт
        get_credit_balance_summary → рекурсия). Зеркалит её события:
          IN  = base счёта (amount−долг − Σ доплат, ts=COALESCE(payment_confirmed_at,
                created_at)) + доплаты долга (credit_incomes.created_at);
          OUT = credit_spends.created_at.
        Нет якоря → fallback (legacy carry-DA). Счета/авансы, созданные ДО якоря
        (вкл. «армянский» КВ10, если якорь свежее) — в котёл не входят: их приход
        раньше якоря, а якорь уже отражает сверенный остаток на свой момент.
        """
        cur = await self.conn.execute(
            "SELECT amount, created_at FROM credit_wallet_anchors "
            "WHERE wallet_role = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (role,),
        )
        anchor = await cur.fetchone()
        if not anchor:
            return fallback
        anchor_amt = float(anchor["amount"] or 0)
        anchor_ts = str(anchor["created_at"] or "")

        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM credit_spends "
            "WHERE wallet_role = ? AND created_at > ?",
            (role, anchor_ts),
        )
        out_after = float((await cur.fetchone())[0] or 0)

        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM credit_incomes "
            "WHERE wallet_role = ? AND created_at > ?",
            (role, anchor_ts),
        )
        in_debt_after = float((await cur.fetchone())[0] or 0)

        cur = await self.conn.execute(
            "SELECT id, amount, outstanding_debt, "
            "       COALESCE(payment_confirmed_at, created_at) AS base_ts "
            "FROM invoices WHERE creator_role = ? AND is_credit = 1",
            (role,),
        )
        inv_rows = [dict(r) for r in await cur.fetchall()]
        cur = await self.conn.execute(
            "SELECT invoice_id, COALESCE(SUM(amount), 0) AS s FROM credit_incomes "
            "WHERE wallet_role = ? GROUP BY invoice_id",
            (role,),
        )
        debt_by_inv = {
            int(r["invoice_id"]): float(r["s"] or 0)
            for r in await cur.fetchall() if r["invoice_id"] is not None
        }
        in_base_after = 0.0
        for iv in inv_rows:
            paid = float(iv.get("amount") or 0) - float(iv.get("outstanding_debt") or 0)
            base = paid - debt_by_inv.get(int(iv["id"]), 0.0)
            if abs(base) >= 0.005 and str(iv.get("base_ts") or "") > anchor_ts:
                in_base_after += base

        return anchor_amt + in_base_after + in_debt_after - out_after

    async def set_credit_wallet_anchor(
        self,
        wallet_role: str,
        amount: float,
        *,
        note: str | None = None,
        entered_by: int | None = None,
    ) -> int:
        """Записать авторитетную сверку остатка кредит-кошелька (durable-якорь).

        Остаток кошелька (total_da) покатится от него + движения после. Хранится
        отдельно от credit_expenses → не влияет на per-invoice CV/CX/DA и лист.
        """
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO credit_wallet_anchors "
            "(wallet_role, amount, note, entered_by, created_at) VALUES (?, ?, ?, ?, ?)",
            (wallet_role, float(amount), note, entered_by, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_latest_credit_wallet_anchor(
        self, role: str
    ) -> dict[str, Any] | None:
        """Последняя авторитетная сверка остатка кошелька роли (или None)."""
        cur = await self.conn.execute(
            "SELECT id, amount, note, created_at FROM credit_wallet_anchors "
            "WHERE wallet_role = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (role,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_credit_wallet_balance(self, role: str) -> dict[str, float]:
        """Баланс кредитного кошелька менеджера (КВ/КИА/НПН).

        Модель кошелька (TZ 02.06): остаток = приход − все траты.
          in  = Σ(amount − outstanding_debt) по is_credit-счетам роли
                (оплаченная кредит-часть; carry между счетами НЕ считаем);
          out = Σ credit_spends.amount где wallet_role = role
                (все траты: привязанные к счёту + без привязки).
        """
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(COALESCE(amount, 0) - COALESCE(outstanding_debt, 0)), 0) "
            "FROM invoices WHERE creator_role = ? AND is_credit = 1",
            (role,),
        )
        total_in = float((await cur.fetchone())[0] or 0)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM credit_spends WHERE wallet_role = ?",
            (role,),
        )
        total_out = float((await cur.fetchone())[0] or 0)
        return {"in": total_in, "out": total_out, "balance": total_in - total_out}

    async def add_credit_income(
        self,
        wallet_role: str,
        invoice_id: int,
        amount: float,
        *,
        kind: str | None = None,
        description: str | None = None,
        entered_by: int | None = None,
    ) -> int:
        """Записать приход кредит-кошелька (гашение долга/оконч.доплата) — п.3 2026-06-12.

        Чистый INSERT в credit_incomes. Идемпотентность обеспечивает вызывающий
        (income пишется только на переходе old≠new долга — см.
        record_credit_debt_payment). Эта запись НЕ влияет на остаток кошелька
        (он = Σ(amount−долг) − Σ credit_spends и растёт сам при ↓долга), а лишь
        даёт DISTINCT строку в истории движений (list_all_credit_events разбивает
        агрегатный «приход» счёта на base + per-доплату; Σ строк = amount−долг).
        """
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO credit_incomes "
            "(wallet_role, invoice_id, amount, kind, description, entered_by, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (wallet_role, int(invoice_id), float(amount), kind, description, entered_by, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def record_credit_debt_payment(
        self,
        inv: dict[str, Any],
        old_debt: Any,
        new_debt: Any,
        *,
        source: str = "",
    ) -> int | None:
        """Отразить изменение долга кредит-счёта в кошельке менеджера (п.3 + авто-сверка 15.06).

        Вызывается из всех путей записи outstanding_debt (sheet_commands.py —
        ручная правка AE «Долг»; import_invoice_from_sheet — импорт ОП col AB) на
        ЛЮБОМ переходе old≠new. Гард: только is_credit-счёт.

        Двунаправленно (доплата авто-попадает в остаток кошелька, откат — авто-снимается):
          • долг ↓ (доплата, delta>0): приход в историю credit_incomes + сдвиг
            авто-маркера «Остаток» переноса на +delta (остаток кошелька растёт сам);
          • долг ↑ (откат/исправление, delta<0): снять ранее записанный приход по
            счёту на |delta| + сдвиг авто-маркера на delta (остаток падает сам).
        Маркер двигается только для НЕактивных счетов, чья оплаченная часть затёрта
        маркером переноса; активный счёт и счета между маркер-счётом и активным
        текут естественно через amount−долг (см. adjust_credit_carry_for_debt_change)
        — двойного счёта нет. Идемпотентно: бьётся только на реальном переходе долга.
        Возвращает id новой credit_incomes (при доплате) или None.
        """
        if not inv or not inv.get("is_credit"):
            return None
        role = inv.get("creator_role")
        if not role:
            return None
        try:
            delta = float(old_debt or 0) - float(new_debt or 0)
        except (TypeError, ValueError):
            return None
        if abs(delta) < 1:  # незначимое изменение — no-op
            return None

        inc_id: int | None = None
        if delta >= 1:
            # долг ↓ → доплата: приход в историю движений
            inc_id = await self.add_credit_income(
                str(role), int(inv["id"]), delta,
                kind="debt_payment",
                description="Оконч. доплата (гашение долга)",
            )
        else:
            # долг ↑ → откат/исправление: снять ранее записанный приход по счёту
            await self._reverse_credit_debt_income(str(role), int(inv["id"]), -delta)

        # Баланс: подвинуть авто-маркер переноса на delta (только неактивные счета).
        try:
            await self.adjust_credit_carry_for_debt_change(str(role), inv, delta)
        except Exception:
            log.exception("record_credit_debt_payment: carry adjust failed inv=%s", inv.get("id"))

        try:
            await self.audit(
                actor_id=None,
                action="credit_debt_payment_income" if delta >= 1 else "credit_debt_reversal",
                entity="invoice",
                entity_id=str(inv["id"]),
                payload={
                    "credit_income_id": inc_id, "amount": delta,
                    "old_debt": old_debt, "new_debt": new_debt,
                    "wallet_role": role, "source": source,
                },
            )
        except Exception:
            log.exception("record_credit_debt_payment: audit() failed inv=%s", inv.get("id"))
        return inc_id

    async def _reverse_credit_debt_income(
        self, role: str, invoice_id: int, amount: float
    ) -> None:
        """Снять/уменьшить ранее записанные приходы-доплаты по счёту на `amount`
        (откат долга вверх — менеджер исправил/перенёс доплату на другую строку).
        Гасит самые свежие debt_payment-приходы счёта; точное совпадение → удаление.
        """
        to_remove = float(amount or 0)
        if to_remove < 1:
            return
        cur = await self.conn.execute(
            "SELECT id, amount FROM credit_incomes "
            "WHERE wallet_role = ? AND invoice_id = ? AND kind = 'debt_payment' "
            "ORDER BY created_at DESC, id DESC",
            (role, int(invoice_id)),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        for r in rows:
            if to_remove < 0.005:
                break
            amt = float(r.get("amount") or 0)
            take = min(amt, to_remove)
            if take >= amt - 0.005:
                await self.conn.execute(
                    "DELETE FROM credit_incomes WHERE id = ?", (int(r["id"]),)
                )
            else:
                await self.conn.execute(
                    "UPDATE credit_incomes SET amount = amount - ? WHERE id = ?",
                    (take, int(r["id"])),
                )
            to_remove -= take
        await self.conn.commit()

    async def adjust_credit_carry_for_debt_change(
        self, role: str, inv: dict[str, Any], delta: float
    ) -> None:
        """Подвинуть авто-маркер «Остаток» переноса на `delta` при изменении долга
        НЕактивного кредит-счёта (доплата delta>0 / откат delta<0).

        Маркер переноса живёт на предпоследнем открытом кредит-счёте роли (его DA =
        carry_in активного, см. get_credit_carry_in). Оплаченная часть счетов ≤
        маркер-счёта затирается этим маркером, поэтому их доплаты доводятся до
        баланса через него. Активный счёт и счета МЕЖДУ маркер-счётом и активным
        текут естественно (amount−долг) → их пропускаем (нет двойного счёта). Пишет
        новую датированную строку «Остаток …» (берётся последняя; история цела).
        """
        if abs(delta) < 1:
            return
        cur = await self.conn.execute(
            "SELECT id, invoice_number, created_at, created_by FROM invoices "
            "WHERE creator_role = ? AND is_credit = 1 "
            "AND NOT (status = 'ended' OR montazh_stage = 'invoice_end') "
            "ORDER BY created_at ASC, id ASC",
            (role,),
        )
        opens = [dict(r) for r in await cur.fetchall()]
        if len(opens) < 2:
            return  # только активный (или ни одного) — доплата течёт через cv
        active = opens[-1]
        marker_inv = opens[-2]
        if int(inv["id"]) == int(active["id"]):
            return  # активный — течёт через свой amount−долг
        if str(inv.get("created_at") or "") > str(marker_inv.get("created_at") or ""):
            return  # между маркер-счётом и активным — естественный перенос
        summary = await self.get_credit_expenses_summary(int(marker_inv["id"]))
        cur_marker: float | None = None
        for it in reversed(summary.get("items") or []):
            d = (it.get("description") or "").strip().lower()
            if d.startswith("остаток"):
                try:
                    cur_marker = float(it.get("amount") or 0)
                except (TypeError, ValueError):
                    cur_marker = None
                break
        if cur_marker is None:
            # У маркер-счёта ещё нет маркера переноса (напр. сразу после открытия
            # нового активного счёта — rollover КВ9→КВ10): база = его ТЕКУЩИЙ
            # естественный DA, чтобы пиннинг маркером СОХРАНИЛ уже накопленный
            # перенос (а не обнулил его). Иначе доплата «съела» бы весь баланс.
            try:
                cs = await self.get_credit_balance_summary(role)
                cur_marker = next(
                    (float(r["da"]) for r in (cs.get("invoices") or [])
                     if int(r["id"]) == int(marker_inv["id"])),
                    0.0,
                )
            except Exception:
                cur_marker = 0.0
        new_marker = float(cur_marker) + float(delta)
        num = inv.get("invoice_number") or f"#{inv['id']}"
        sign = "+" if delta > 0 else "−"
        await self.add_credit_expense(
            int(marker_inv["id"]),
            new_marker,
            f"Остаток кред. кошелька (авто: доплата {num} {sign}{abs(int(round(delta)))})",
            int(inv.get("created_by") or marker_inv.get("created_by") or 0),
        )

    async def add_credit_spend(
        self,
        wallet_role: str,
        amount: float,
        entered_by: int,
        *,
        cost_type: str | None = None,
        description: str | None = None,
        bound_invoice_id: int | None = None,
        supplier_payment_id: int | None = None,
        op_entry_id: int | None = None,
        chat_message_id: int | None = None,
    ) -> int:
        """Записать трату кредитных средств в реестр кошелька (credit_spends).

        Чистый INSERT. Побочные эффекты делает вызывающий код:
          привязка     → create_supplier_payment (→ cost_*/DP–DV), id в supplier_payment_id;
          без привязки → add_op_company_entry (→ «Баланс компании» I/J), id в op_entry_id.
        wallet_role — кошелёк, с которого списывается (manager_kv/kia/npn).
        """
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO credit_spends "
            "(wallet_role, amount, cost_type, description, bound_invoice_id, "
            " supplier_payment_id, op_entry_id, entered_by, chat_message_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (wallet_role, amount, cost_type, description, bound_invoice_id,
             supplier_payment_id, op_entry_id, entered_by, chat_message_id, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_credit_spend(self, spend_id: int) -> dict[str, Any] | None:
        """Одна трата кошелька по id (None если нет)."""
        cur = await self.conn.execute(
            "SELECT * FROM credit_spends WHERE id = ?", (spend_id,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_credit_spend_audit(self, spend_id: int) -> dict[str, Any]:
        """Payload последней audit-записи 'credit_wallet_spend' для траты.

        apply_credit_wallet_spend пишет туда credit_expense_id /
        active_credit_invoice_id / cost_type / mode в момент записи — нужно для
        ТОЧНОГО реверса (credit_spends сам credit_expense_id не хранит). {} если нет.
        """
        cur = await self.conn.execute(
            "SELECT payload_json FROM audit_log "
            "WHERE action = 'credit_wallet_spend' AND entity = 'credit_spends' "
            "AND entity_id = ? ORDER BY id DESC LIMIT 1",
            (str(spend_id),),
        )
        row = await cur.fetchone()
        if not row or not row["payload_json"]:
            return {}
        try:
            return json.loads(row["payload_json"])
        except (ValueError, TypeError):
            return {}

    async def cancel_credit_spend(
        self,
        spend_id: int,
        actor_id: int,
        reason: str,
        *,
        action: str = "credit_spend_cancel",
    ) -> dict[str, Any]:
        """Полный реверс одной траты кошелька (инверсия apply_credit_wallet_spend).

        Удаляет credit_spends + (supplier_payments + invoices.cost_* −=, если bound)
        ИЛИ op_company_entries (если free) + credit_expenses (что и есть баланс
        кошелька — total_da пересчитается сам). Атомарно: ОДИН commit на все
        DELETE/UPDATE (без промежуточных), чтобы реверс был «всё или ничего».

        credit_expense_id берётся из audit-записи (apply туда его пишет); если нет —
        fallback по РОВНО одному кандидату (invoice+amount+entered_by, не-«остаток»),
        иначе CX НЕ трогается и credit_expense_reversed=False (деньги не угадываем —
        вызывающий обязан прервать перезапись). KeyError если траты нет.
        """
        row = await self.get_credit_spend(spend_id)
        if not row:
            raise KeyError(f"credit_spend {spend_id} not found")
        aud = await self.get_credit_spend_audit(spend_id)

        amount = float(row.get("amount") or 0)
        wallet_role = row.get("wallet_role") or ""
        sp_id = row.get("supplier_payment_id")
        op_id = row.get("op_entry_id")
        bound_invoice_id = row.get("bound_invoice_id")
        ce_id = aud.get("credit_expense_id")
        active_inv_id = aud.get("active_credit_invoice_id")
        reversed_cost_col: str | None = None

        # (1) Реверс привязки/безпривязки.
        if sp_id:
            cur = await self.conn.execute(
                "SELECT amount, material_type, parent_invoice_id "
                "FROM supplier_payments WHERE id = ?",
                (sp_id,),
            )
            sp = await cur.fetchone()
            if sp:
                sp_amount = float(sp["amount"] or 0)
                cost_col = self._COST_COL_MAP.get(sp["material_type"] or "")
                if cost_col and sp["parent_invoice_id"]:
                    await self.conn.execute(
                        f"UPDATE invoices SET {cost_col} = COALESCE({cost_col}, 0) - ? WHERE id = ?",
                        (sp_amount, sp["parent_invoice_id"]),
                    )
                    reversed_cost_col = cost_col
            await self.conn.execute("DELETE FROM supplier_payments WHERE id = ?", (sp_id,))
        elif op_id:
            await self.conn.execute("DELETE FROM op_company_entries WHERE id = ?", (op_id,))

        # (2) Реверс баланса (credit_expense). Маркер «остаток» НЕ трогаем.
        credit_expense_reversed = False
        if ce_id:
            cur = await self.conn.execute(
                "SELECT description FROM credit_expenses WHERE id = ?", (ce_id,)
            )
            ce = await cur.fetchone()
            if ce is not None:
                d = (ce["description"] or "").strip().lower()
                if d.startswith("остаток"):
                    log.warning(
                        "cancel_credit_spend: ce %s — маркер «остаток», НЕ удаляю", ce_id
                    )
                else:
                    await self.conn.execute("DELETE FROM credit_expenses WHERE id = ?", (ce_id,))
                    credit_expense_reversed = True
        elif active_inv_id:
            cur = await self.conn.execute(
                "SELECT id FROM credit_expenses "
                "WHERE invoice_id = ? AND amount = ? AND entered_by = ? "
                "AND LOWER(COALESCE(description, '')) NOT LIKE 'остаток%'",
                (active_inv_id, amount, row.get("entered_by")),
            )
            cands = await cur.fetchall()
            if len(cands) == 1:
                ce_id = cands[0]["id"]
                await self.conn.execute("DELETE FROM credit_expenses WHERE id = ?", (ce_id,))
                credit_expense_reversed = True
            else:
                log.warning(
                    "cancel_credit_spend: fallback CX — %d кандидатов для spend %s, НЕ трогаю",
                    len(cands), spend_id,
                )

        # (3) Удалить саму трату. Единственный commit на весь реверс (1)-(3).
        await self.conn.execute("DELETE FROM credit_spends WHERE id = ?", (spend_id,))
        await self.conn.commit()

        snapshot = {
            "spend_id": spend_id,
            "amount": amount,
            "wallet_role": wallet_role,
            "mode": "bound" if bound_invoice_id else "free",
            "purpose": row.get("description") or "",
            "cost_type": row.get("cost_type"),
            "bound_invoice_id": bound_invoice_id,
            "supplier_payment_id": sp_id,
            "op_entry_id": op_id,
            "credit_expense_id": ce_id,
            "active_credit_invoice_id": active_inv_id,
            "reversed_cost_col": reversed_cost_col,
            "credit_expense_reversed": credit_expense_reversed,
        }
        await self.audit(
            actor_id=actor_id, action=action,
            entity="credit_spends", entity_id=str(spend_id),
            payload={"reason": reason, **snapshot},
        )
        return snapshot

    async def reattribute_credit_spend(
        self,
        spend_id: int,
        actor_id: int,
        *,
        new_mode: str,
        new_invoice_id: int | None,
        new_cost_type: str | None,
        new_invoice_number: str = "",
        new_purpose: str = "",
    ) -> dict[str, Any]:
        """Перенос ПРИВЯЗКИ исполненной траты кошелька (режим D, TZ #3 Фаза 3).

        Сумма ФИКСИРОВАНА — меняется только назначение: привязка bound↔free /
        счёт / категория / текст. «Перенос согласно назначению»: снять сумму со
        старой привязки (supplier_payment → invoices.cost_* −= с DELETE, либо
        op_company_entries DELETE) и отдать новой (новый supplier_payment +
        cost_* +=, либо новый op_company_entries).

        credit_expenses НЕ трогаются (amount/wallet/активный счёт те же) → CX и
        total_da НЕ сдвигаются — это КЛЮЧЕВОЙ инвариант режима D. Атомарно: ОДИН
        commit на все DELETE/INSERT/UPDATE (без промежуточных). KeyError если
        траты нет. Возвращает снимок {amount, wallet_role, active_credit_invoice_id,
        old{}, new{}} для ресинка листов и карточки «было→стало».
        """
        row = await self.get_credit_spend(spend_id)
        if not row:
            raise KeyError(f"credit_spend {spend_id} not found")
        aud = await self.get_credit_spend_audit(spend_id)

        amount = float(row.get("amount") or 0)
        wallet_role = row.get("wallet_role") or ""
        old_sp_id = row.get("supplier_payment_id")
        old_op_id = row.get("op_entry_id")
        old_bound_invoice_id = row.get("bound_invoice_id")
        old_mode = "bound" if old_bound_invoice_id else "free"
        old_cost_type = row.get("cost_type")
        old_parent: int | None = None
        old_cost_col: str | None = None

        bound = bool(new_mode == "bound" and new_invoice_id)
        new_cost_type = (new_cost_type or "extra_mat") if bound else None
        new_cost_col = self._COST_COL_MAP.get(new_cost_type or "") if bound else None

        # (1) Снять СТАРУЮ привязку (зеркало cancel_credit_spend, но без credit_expenses).
        if old_sp_id:
            cur = await self.conn.execute(
                "SELECT amount, material_type, parent_invoice_id "
                "FROM supplier_payments WHERE id = ?",
                (old_sp_id,),
            )
            sp = await cur.fetchone()
            if sp:
                sp_amount = float(sp["amount"] or 0)
                cost_col = self._COST_COL_MAP.get(sp["material_type"] or "")
                old_parent = sp["parent_invoice_id"]
                if cost_col and old_parent:
                    await self.conn.execute(
                        f"UPDATE invoices SET {cost_col} = COALESCE({cost_col}, 0) - ? WHERE id = ?",
                        (sp_amount, old_parent),
                    )
                    old_cost_col = cost_col
            await self.conn.execute("DELETE FROM supplier_payments WHERE id = ?", (old_sp_id,))
        elif old_op_id:
            await self.conn.execute("DELETE FROM op_company_entries WHERE id = ?", (old_op_id,))

        # (2) Создать НОВУЮ привязку СЫРЫМ SQL (НЕ через коммитящие create_supplier_payment/
        #     add_op_company_entry — иначе промежуточный commit ломает атомарность).
        now_iso = datetime.now(timezone.utc).isoformat()
        new_sp_id: int | None = None
        new_op_id: int | None = None
        new_parent: int | None = None
        if bound:
            new_parent = int(new_invoice_id)  # type: ignore[arg-type]
            material_type = new_cost_type or "extra_mat"
            cur = await self.conn.execute(
                "INSERT INTO supplier_payments "
                "(parent_invoice_id, invoice_number, amount, material_type, supplier, "
                " task_id, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (new_parent, new_invoice_number, amount, material_type, "", None, actor_id, now_iso),
            )
            new_sp_id = cur.lastrowid
            if new_cost_col:
                await self.conn.execute(
                    f"UPDATE invoices SET {new_cost_col} = COALESCE({new_cost_col}, 0) + ? WHERE id = ?",
                    (amount, new_parent),
                )
        else:
            # op_entry: период берём из ДАТЫ исходной траты (атрибуция переносится —
            # экономический месяц расхода не сдвигается). Поля идентичны
            # apply_credit_wallet_spend. Fallback на now (МСК) если дата не парсится.
            from zoneinfo import ZoneInfo as _ZI
            try:
                _src = datetime.fromisoformat(str(row.get("created_at") or ""))
                if _src.tzinfo is None:
                    _src = _src.replace(tzinfo=timezone.utc)
                _msk = _src.astimezone(_ZI("Europe/Moscow"))
            except (ValueError, TypeError):
                _msk = datetime.now(_ZI("Europe/Moscow"))
            cur = await self.conn.execute(
                "INSERT INTO op_company_entries "
                "(year, month, date_iso, date_display, cashless_amount, nds, description, "
                " taxes, loan, date_other_display, other_amount, description_credit, "
                " source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (_msk.year, _msk.month, _msk.strftime("%Y-%m-%d"), None, None, None, None,
                 None, None, _msk.strftime("%d.%m.%Y"), amount, new_purpose,
                 "credit_wallet_spend", now_iso),
            )
            new_op_id = cur.lastrowid

        # (3) Перевести credit_spends на новую привязку/назначение.
        new_bound_invoice_id = new_parent if bound else None
        await self.conn.execute(
            "UPDATE credit_spends SET bound_invoice_id = ?, supplier_payment_id = ?, "
            "op_entry_id = ?, cost_type = ?, description = ? WHERE id = ?",
            (new_bound_invoice_id, new_sp_id, new_op_id, new_cost_type, new_purpose, spend_id),
        )

        # (4) credit_expenses НЕ трогаем — CX/total_da неизменны (инвариант режима D).
        # (5) Единственный commit на (1)-(3).
        await self.conn.commit()

        snapshot = {
            "spend_id": spend_id,
            "amount": amount,
            "wallet_role": wallet_role,
            "active_credit_invoice_id": aud.get("active_credit_invoice_id"),
            "old": {
                "mode": old_mode, "invoice_id": old_bound_invoice_id,
                "parent_invoice_id": old_parent, "cost_type": old_cost_type,
                "cost_col": old_cost_col,
                "supplier_payment_id": old_sp_id, "op_entry_id": old_op_id,
            },
            "new": {
                "mode": "bound" if bound else "free", "invoice_id": new_bound_invoice_id,
                "parent_invoice_id": new_parent, "cost_type": new_cost_type,
                "cost_col": new_cost_col,
                "invoice_number": new_invoice_number if bound else "",
                "supplier_payment_id": new_sp_id, "op_entry_id": new_op_id,
                "purpose": new_purpose,
            },
        }
        await self.audit(
            actor_id=actor_id, action="credit_spend_reattributed",
            entity="credit_spends", entity_id=str(spend_id),
            payload={"amount": amount, "wallet_role": wallet_role,
                     "old": snapshot["old"], "new": snapshot["new"]},
        )
        return snapshot

    async def list_all_credit_events(self, limit: int = 500) -> dict[str, Any]:
        """Бандл для кредит-блока листа «Авансирование сотрудников» (модель кошелька).

        По менеджерам КВ/КИА/НПН:
          приход-события — оплаченная часть кредит-счетов роли (amount − долг),
                           дата = payment_confirmed_at | created_at;
          расход-события — из credit_spends: привязка → счёт + категория,
                           без привязки → назначение; дата, кто внёс;
          running        — приход_до − расход_до (хронологически; без двойного
                           счёта, т.к. carry убран).
        Итоги (in/out/balance) — из get_credit_balance_summary (carry-DA), чтобы
        сводка совпадала с карточкой «Кредитный баланс»; balance = total_da,
        total_in = CV последнего открытого (с переносом), total_out = in − balance.
        Рендер строк делает sheets.sync_advances_journal_sheet.
        """
        manager_roles = (
            ("manager_kv", "КВ"),
            ("manager_kia", "КИА"),
            ("manager_npn", "НПН"),
        )
        managers: list[dict[str, Any]] = []
        for role, label in manager_roles:
            wallet = await self.get_credit_wallet_balance(role)
            events: list[dict[str, Any]] = []

            # Приход: оплаченная часть кредит-счетов роли. Разбивка (п.3 2026-06-12):
            #   base          = (amount−долг) − Σ(гашения долга по счёту), дата создания;
            #   debt_payment  = каждое гашение долга (credit_incomes) отдельной строкой.
            # Σ(base + доплаты) = amount−долг → остаток/итоги НЕ меняются, лишь детализация.
            inc_cur = await self.conn.execute(
                "SELECT invoice_id, amount, description, created_at FROM credit_incomes "
                "WHERE wallet_role = ? ORDER BY created_at ASC, id ASC",
                (role,),
            )
            inc_by_inv: dict[int, list[dict[str, Any]]] = {}
            for ir in [dict(x) for x in await inc_cur.fetchall()]:
                inc_by_inv.setdefault(int(ir["invoice_id"]), []).append(ir)

            cur = await self.conn.execute(
                "SELECT id, invoice_number, object_address, amount, outstanding_debt, "
                "       created_at, payment_confirmed_at "
                "FROM invoices WHERE creator_role = ? AND is_credit = 1 "
                "ORDER BY COALESCE(payment_confirmed_at, created_at) ASC, id ASC LIMIT ?",
                (role, limit),
            )
            for r in [dict(x) for x in await cur.fetchall()]:
                inv_id = int(r["id"])
                inv_num = r.get("invoice_number") or f"#{inv_id}"
                addr = r.get("object_address") or ""
                paid_credit = float(r.get("amount") or 0) - float(r.get("outstanding_debt") or 0)
                incs = inc_by_inv.get(inv_id, [])
                base = paid_credit - sum(float(i.get("amount") or 0) for i in incs)
                if abs(base) >= 0.005:
                    events.append({
                        "ts": r.get("payment_confirmed_at") or r.get("created_at"),
                        "kind": "in",
                        "amount": base,
                        "invoice_id": inv_id,
                        "invoice_number": inv_num,
                        "object_address": addr,
                        "cost_type": None,
                        "description": "",
                        "entered_by": None,
                        "income_kind": "base",
                    })
                for i in incs:
                    events.append({
                        "ts": i.get("created_at"),
                        "kind": "in",
                        "amount": float(i.get("amount") or 0),
                        "invoice_id": inv_id,
                        "invoice_number": inv_num,
                        "object_address": addr,
                        "cost_type": None,
                        "description": i.get("description") or "Оконч. доплата (долг)",
                        "entered_by": None,
                        "income_kind": "debt_payment",
                    })

            # Расход: реестр трат кошелька (привязанные + без привязки).
            cur = await self.conn.execute(
                "SELECT s.id, s.amount, s.cost_type, s.description, s.bound_invoice_id, "
                "       s.entered_by, s.created_at, "
                "       inv.invoice_number AS bound_number, inv.object_address AS bound_addr "
                "FROM credit_spends s "
                "LEFT JOIN invoices inv ON inv.id = s.bound_invoice_id "
                "WHERE s.wallet_role = ? "
                "ORDER BY s.created_at ASC, s.id ASC LIMIT ?",
                (role, limit),
            )
            for r in [dict(x) for x in await cur.fetchall()]:
                events.append({
                    "ts": r.get("created_at"),
                    "kind": "out",
                    "amount": float(r.get("amount") or 0),
                    "invoice_id": r.get("bound_invoice_id"),
                    "invoice_number": r.get("bound_number") or "",
                    "object_address": r.get("bound_addr") or "",
                    "cost_type": r.get("cost_type"),
                    "description": r.get("description") or "",
                    "entered_by": r.get("entered_by"),
                })

            # Running-остаток (хронологически; приход раньше расхода в ту же метку).
            events.sort(key=lambda e: (str(e.get("ts") or ""), 0 if e["kind"] == "in" else 1))
            run = 0.0
            for e in events:
                run += e["amount"] if e["kind"] == "in" else -e["amount"]
                e["running"] = run

            # Итоги сводки = carry-DA (get_credit_balance_summary), чтобы журнал
            # совпадал с карточкой «Кредитный баланс»: Вход = CV последнего открытого
            # (с переносом), Остаток = total_da, Расход = Вход − Остаток.
            # (Σ credit_spends не отражает carry/маркеры «Остаток» — для сводки не годится.)
            try:
                cs = await self.get_credit_balance_summary(role)
                _last_open = None
                for _r in cs.get("invoices") or []:
                    if not _r.get("is_closed"):
                        _last_open = _r
                bal = float(cs.get("total_da") or 0)
                cv_in = float((_last_open or {}).get("cv") or 0)
            except Exception:
                bal = float(wallet.get("balance") or 0)
                cv_in = float(wallet.get("in") or 0)

            managers.append({
                "role": role,
                "label": label,
                "total_in": cv_in,
                "total_out": cv_in - bal,
                "balance": bal,
                "events": events,
            })

        return {"managers": managers}

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

    async def get_rp_discrepancies(self) -> dict[str, Any]:
        """Read-only отчёт расхождений «РП-таблица Импорт ОП ↔ счета в БД».

        Вход — лиды с проставленным РП №счёта (rp_invoice_number). Два вида:
          • role_mismatch — лид сматчен со счётом, но менеджер по номеру счёта
            (_infer_invoice_creator_role: КВ/КИА/НПН) ≠ rp_manager (что говорит РП).
            Конфликт атрибуции — влияет на ЗП менеджера.
          • unlinked — у лида есть rp_invoice_number, но счёта с таким номером в БД
            нет (потерянная/опечатанная связь).

        Видимость — только gd/td (содержит номера счетов и суммы).
        Только чтение: ничего не пишет.
        """
        cur = await self.conn.execute(
            "SELECT id, name, contact_name, rp_invoice_number, rp_manager, "
            "rp_source, rp_status FROM leads "
            "WHERE rp_invoice_number IS NOT NULL AND TRIM(rp_invoice_number) <> ''"
        )
        leads = [dict(r) for r in await cur.fetchall()]

        def _mgr_key(label: str) -> str | None:
            u = (label or "").upper()
            if "КИА" in u:
                return "kia"
            if "КВ" in u:
                return "kv"
            if "НПН" in u:
                return "npn"
            return None

        _role_key = {
            Role.MANAGER_KV: "kv",
            Role.MANAGER_KIA: "kia",
            Role.MANAGER_NPN: "npn",
        }
        _key_label = {"kv": "КВ", "kia": "КИА", "npn": "НПН"}

        role_mismatch: list[dict[str, Any]] = []
        unlinked: list[dict[str, Any]] = []

        for ld in leads:
            inv_num = (ld.get("rp_invoice_number") or "").strip()
            who = (ld.get("contact_name") or ld.get("name") or "").strip()
            inv = await self._get_invoice_for_sheet_import(inv_num)
            if inv is None:
                unlinked.append({
                    "lead_id": ld["id"],
                    "name": who,
                    "invoice_number": inv_num,
                    "rp_manager": (ld.get("rp_manager") or "").strip(),
                    "rp_source": (ld.get("rp_source") or "").strip(),
                })
                continue
            rp_key = _mgr_key(ld.get("rp_manager") or "")
            inv_key = _role_key.get(self._infer_invoice_creator_role(inv_num))
            if rp_key and inv_key and rp_key != inv_key:
                role_mismatch.append({
                    "lead_id": ld["id"],
                    "name": who,
                    "invoice_number": inv_num,
                    "invoice_id": inv.get("id"),
                    "amount": float(inv.get("amount") or 0),
                    "object_address": (inv.get("object_address") or "").strip(),
                    "invoice_manager": _key_label.get(inv_key, inv_key),
                    "rp_manager": _key_label.get(rp_key, rp_key),
                })

        return {
            "role_mismatch": role_mismatch,
            "unlinked": unlinked,
            "checked": len(leads),
        }

    async def get_lead_stats_v2(
        self, user_map: dict[int, str] | None = None
    ) -> dict[str, Any]:
        """V2: статистика лидов из таблицы `leads` (реальный источник, 1600+ записей).

        Разбивка по периодам (сегодня/неделя/месяц) × менеджеру (AMO-attribution
        по суффиксу 'КВ'/'КИА'/'НПН' в contact_name). Отдельно — unclaimed
        (claimed_by IS NULL), это инструмент ГД-контроля.
        """
        # Дни массового импорта (≥100 лидов/день) — это AMO-bulk-импорты с
        # фиктивным created_at. Норма: 2-5 лидов/день. Самофильтрующаяся
        # эвристика для будущих импортов тоже.
        cur = await self.conn.execute(
            "SELECT date(created_at) AS d FROM leads "
            "GROUP BY d HAVING COUNT(*) >= 100"
        )
        exclude_dates = sorted({r["d"] for r in await cur.fetchall()})
        if exclude_dates:
            quoted = ", ".join(f"'{d}'" for d in exclude_dates)
            exc_clause = f"AND date(created_at) NOT IN ({quoted})"
        else:
            exc_clause = ""

        # Все периоды — календарные:
        #   today      = текущий день
        #   week       = с понедельника текущей ISO-недели до сейчас
        #   month      = с 1-го числа текущего месяца до сейчас
        #   prev_month = весь предыдущий календарный месяц
        # «start of week» в sqlite даёт воскресенье, поэтому понедельник
        # вычисляем через смещение: ((day_of_week + 6) % 7) days back.
        _period_clauses = (
            "SUM(CASE WHEN date(created_at) = date('now') THEN 1 ELSE 0 END) AS today, "
            "SUM(CASE WHEN date(created_at) >= "
            "    date('now', '-' || ((strftime('%w', 'now') + 6) % 7) || ' days') "
            "  THEN 1 ELSE 0 END) AS week, "
            "SUM(CASE WHEN date(created_at) >= date('now', 'start of month') THEN 1 ELSE 0 END) AS month, "
            "SUM(CASE WHEN date(created_at) >= date('now', 'start of month', '-1 month') "
            "         AND date(created_at) <  date('now', 'start of month') THEN 1 ELSE 0 END) AS prev_month, "
            "COUNT(*) AS total"
        )

        by_manager_sql = f"""
        SELECT
          CASE
            WHEN contact_name LIKE '% КВ' OR contact_name LIKE '%КВ %'
                 OR contact_name = 'КВ' THEN 'kv'
            WHEN contact_name LIKE '% КИА' OR contact_name LIKE '%КИА %'
                 OR contact_name = 'КИА' THEN 'kia'
            WHEN contact_name LIKE '% НПН' OR contact_name LIKE '%НПН %'
                 OR contact_name = 'НПН' THEN 'npn'
            ELSE 'other'
          END AS mgr,
          {_period_clauses}
        FROM leads
        WHERE 1=1 {exc_clause}
        GROUP BY mgr
        """
        cur = await self.conn.execute(by_manager_sql)
        by_manager = {r["mgr"]: dict(r) for r in await cur.fetchall()}

        # Разбивка по responsible_user_id (колонка E листа «Лиды» = AMO-ответственный;
        # id→метку маппит caller через config.amocrm_user_map). Отдельно от by_manager
        # (тот — эвристика по суффиксу contact_name). Тот же exc_clause масс-импорта.
        by_resp_sql = f"""
        SELECT COALESCE(responsible_user_id, 0) AS rid, {_period_clauses}
        FROM leads
        WHERE 1=1 {exc_clause}
        GROUP BY rid
        """
        cur = await self.conn.execute(by_resp_sql)
        by_responsible = {int(r["rid"]): dict(r) for r in await cur.fetchall()}

        unclaimed_sql = f"""
        SELECT {_period_clauses}
        FROM leads
        WHERE claimed_by IS NULL {exc_clause}
        """
        cur = await self.conn.execute(unclaimed_sql)
        row = await cur.fetchone()
        unclaimed = (
            dict(row) if row
            else {"today": 0, "week": 0, "month": 0, "prev_month": 0, "total": 0}
        )

        totals_sql = f"SELECT {_period_clauses} FROM leads WHERE 1=1 {exc_clause}"
        cur = await self.conn.execute(totals_sql)
        row = await cur.fetchone()
        totals = (
            dict(row) if row
            else {"today": 0, "week": 0, "month": 0, "prev_month": 0, "total": 0}
        )

        cur = await self.conn.execute(
            f"SELECT COUNT(*) FROM leads WHERE claimed_by IS NOT NULL {exc_clause}"
        )
        claimed_row = await cur.fetchone()
        claimed_total = claimed_row[0] if claimed_row else 0

        # Сколько лидов исключено как масс-импорт (для footer карточки)
        if exclude_dates:
            quoted_in = ", ".join(f"'{d}'" for d in exclude_dates)
            cur = await self.conn.execute(
                f"SELECT COUNT(*) FROM leads WHERE date(created_at) IN ({quoted_in})"
            )
            excluded_row = await cur.fetchone()
            excluded_import_count = excluded_row[0] if excluded_row else 0
        else:
            excluded_import_count = 0

        # Источники трафика — категоризация с приоритетом:
        # 1) leads.source (если заполнено) — нормализуем (Авито*/тон в одну категорию)
        # 2) тег 'Сайт' в tags_json
        # 3) префикс 'Сайт:' / 'Сделка #' в name
        # 4) иначе 'Другое'
        # Только ~5% лидов имеют source, остальные тянем из name/tags.
        cur = await self.conn.execute(
            "SELECT created_at, source, name, tags_json, "
            "       rp_manager, rp_source, rp_status, rp_invoice_number, "
            "       contact_name, responsible_user_id "
            "FROM leads"
        )
        rows = await cur.fetchall()

        def _norm_src(raw: str) -> str:
            low = (raw or "").strip().lower()
            if low.startswith("авито"):
                return "Авито"
            if low in ("тон", "тонн", "тонн."):
                return "тон"
            if low.startswith("от "):
                return "от партнёра"
            return (raw or "").strip()

        def _categorize(row: Any) -> str:
            raw_src = (row["source"] or "").strip()
            if raw_src:
                return _norm_src(raw_src)
            try:
                tags = json.loads(row["tags_json"] or "[]") or []
            except (ValueError, TypeError):
                tags = []
            if any(t == "Сайт" for t in tags):
                return "Сайт"
            name = row["name"] or ""
            if name.startswith("Сайт:"):
                return "Сайт"
            if name.startswith("Сделка #"):
                return "АМО (прямой)"
            return "Другое"

        # RP-приоритет (user: «РП — главный источник»): для статистики менеджера и
        # источника берём данные таблицы РП «Импорт ОП» (rp_manager BV / rp_source BW),
        # где они есть; иначе fallback на amoCRM. КИА — по суффиксу имени (своего
        # amoCRM-аккаунта у КИА нет: его лиды сидят под чужими responsible_user_id).
        def _norm_rp_mgr(v: str) -> str:
            u = (v or "").upper()
            if "КИА" in u:
                return "КИА"
            if "НПН" in u:
                return "НПН"
            if "КВ" in u:
                return "КВ"
            return (v or "").strip()

        def _is_kia_suffix(cn: str) -> bool:
            cn = (cn or "").strip()
            return cn.endswith(" КИА") or "КИА " in cn or cn == "КИА"

        umap = user_map or {}

        def _eff_manager(row: Any) -> str:
            rpm = (row["rp_manager"] or "").strip()
            if rpm:
                return _norm_rp_mgr(rpm) or "Без мен."
            if _is_kia_suffix(row["contact_name"]):
                return "КИА"
            rid = row["responsible_user_id"]
            return (umap.get(int(rid)) if rid else None) or "Без мен."

        def _eff_source(row: Any) -> str:
            rps = (row["rp_source"] or "").strip()
            return _norm_src(rps) if rps else _categorize(row)

        now = utcnow()
        today_d = now.date()
        # Понедельник текущей ISO-недели
        week_start = today_d - timedelta(days=today_d.weekday())
        # Текущий календарный месяц: первое число этого месяца
        cur_month_start = today_d.replace(day=1)
        # Предыдущий календарный месяц: первое число прошлого месяца
        if cur_month_start.month == 1:
            prev_month_start = cur_month_start.replace(
                year=cur_month_start.year - 1, month=12
            )
        else:
            prev_month_start = cur_month_start.replace(month=cur_month_start.month - 1)

        exclude_set = set(exclude_dates)  # YYYY-MM-DD strings

        def _new() -> dict[str, int]:
            return {"today": 0, "week": 0, "month": 0, "prev_month": 0, "total": 0}

        def _accum(bucket: dict[str, dict[str, int]], key: str, ts_date: Any) -> None:
            d = bucket.setdefault(key, _new())
            d["total"] += 1
            if ts_date >= cur_month_start:
                d["month"] += 1
            elif ts_date >= prev_month_start:
                d["prev_month"] += 1
            if ts_date >= week_start:
                d["week"] += 1
            if ts_date == today_d:
                d["today"] += 1

        src_bucket: dict[str, dict[str, int]] = {}      # legacy: amoCRM-категоризация
        src_eff_bucket: dict[str, dict[str, int]] = {}  # RP-приоритет источник
        mgr_eff_bucket: dict[str, dict[str, int]] = {}  # RP-приоритет менеджер
        for r in rows:
            try:
                ts_str = str(r["created_at"]).replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
            ts_date = ts.date()
            if ts_date.isoformat() in exclude_set:
                continue
            _accum(src_bucket, _categorize(r), ts_date)
            _accum(src_eff_bucket, _eff_source(r), ts_date)
            _accum(mgr_eff_bucket, _eff_manager(r), ts_date)

        def _src_list(bucket: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
            return sorted(
                [{"src": k, **v} for k, v in bucket.items()],
                key=lambda x: (x["month"], x["prev_month"], x["total"]),
                reverse=True,
            )[:12]

        by_source = _src_list(src_bucket)
        by_source_eff = _src_list(src_eff_bucket)

        # Снимок РП-сделок — по ВСЕМ лидам (RP-матч = реальная сделка; масс-импорт
        # НЕ исключаем: created_at фиктивный, но сама сделка настоящая).
        cur = await self.conn.execute(
            "SELECT rp_status AS s, COUNT(*) AS c FROM leads "
            "WHERE rp_status IS NOT NULL AND TRIM(rp_status) <> '' GROUP BY rp_status"
        )
        funnel_bucket = {r["s"]: int(r["c"]) for r in await cur.fetchall()}
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS c FROM leads "
            "WHERE rp_manager IS NOT NULL AND TRIM(rp_manager) <> ''"
        )
        _rm = await cur.fetchone()
        rp_matched = int((_rm["c"] if _rm else 0) or 0)
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS c FROM leads "
            "WHERE rp_invoice_number IS NOT NULL AND TRIM(rp_invoice_number) <> ''"
        )
        _ri = await cur.fetchone()
        rp_inv_linked = int((_ri["c"] if _ri else 0) or 0)

        # Воронка РП: фикс-порядок стадий (неизвестные — в конец).
        _FUNNEL_ORDER = ["В разработке", "ДА", "Запустили", "Закончили"]
        _ord = {s: i for i, s in enumerate(_FUNNEL_ORDER)}
        funnel_rp = [
            {"status": s, "count": funnel_bucket[s]}
            for s in sorted(funnel_bucket, key=lambda s: (_ord.get(s, 99), s))
        ]

        return {
            "by_manager": by_manager,
            "by_responsible": by_responsible,
            "by_manager_eff": mgr_eff_bucket,
            "unclaimed": unclaimed,
            "totals": totals,
            "claimed_total": claimed_total,
            "by_source": by_source,
            "by_source_eff": by_source_eff,
            "funnel_rp": funnel_rp,
            "rp_matched": rp_matched,
            "rp_inv_linked": rp_inv_linked,
            "excluded_import_count": excluded_import_count,
            "excluded_import_dates": exclude_dates,
        }

    async def get_gd_balance_section_data(
        self,
        *,
        is_credit: bool,
        year: int,
        current_month: int,
    ) -> dict[str, Any]:
        """Данные для секций «Баланс б/н» / «Баланс (кред)» карточки ГД.

        Источники (spec docs/specs/gd_sync_card_spec.md, секции 4/5):
        - **Доход (Р)** (U): SUM(profit_calc_op); для credit fallback NULLIF→profit_tax.
          Год не ограничен (вариант B): все года сложены вместе по месяцу.
        - **Доход** (BL): SUM(cost_card.margin) — считается через
          get_full_invoice_cost_card для каждого ended-счёта (N+1 для ≤15
          закрытых, на проде ~1 сек). Для credit считаются и status='ended',
          и montazh_stage='invoice_end'.
        - **Расход** (C/I листа «Баланс компании»): expense_cashless (б/н) или
          expense_other (кред) из op_company_monthly за указанный год.
        - **AN** (zp_manager_payout): SUM по месяцу за все года, с фильтром
          is_credit. Используется в формуле «Баланс = Доход − Расход − AN».
        - **open_count** для бейджа 🔴N: COUNT(*) WHERE status≠'ended'
          (+ montazh_stage≠'invoice_end' для credit) GROUP BY месяц.
        - **forecast** «Прибыль прогноз»: SUM(profit_calc_op − MAX(0, manager_zp_blank))
          по открытым (status≠ended) счетам с тем же фильтром is_credit.
          Бланк флорится нулём: отрицательный AG в «Импорт ОП» (перерасчёт/сторно)
          иначе ПРИБАВЛЯЛСЯ бы к прогнозу вместо вычитания (owner 01.08).

        Возвращает dict с ключами: income_p, income, expense, an, open_count,
        forecast — все int-key dicts кроме forecast (float).

        Все фильтры: parent_invoice_id IS NULL AND invoice_number NOT LIKE
        'LEAD-%' AND is_credit = <0|1> AND receipt_date IS NOT NULL.
        """
        g_credit = 1 if is_credit else 0
        income_p: dict[int, float] = {}
        income: dict[int, float] = {}
        expense: dict[int, float] = {}
        an: dict[int, float] = {}
        open_count: dict[int, int] = {}

        # 1) Доход (Р) — SUM(U) с fallback на profit_tax для credit.
        if is_credit:
            income_p_sql = (
                "SELECT CAST(strftime('%m', receipt_date) AS INTEGER) AS m, "
                "       COALESCE(SUM(COALESCE(NULLIF(profit_calc_op, 0), profit_tax, 0)), 0) AS s "
                "FROM invoices "
                "WHERE parent_invoice_id IS NULL "
                "  AND invoice_number NOT LIKE 'LEAD-%' "
                "  AND is_credit = 1 "
                "  AND receipt_date IS NOT NULL AND receipt_date != '' "
                "GROUP BY m"
            )
        else:
            income_p_sql = (
                "SELECT CAST(strftime('%m', receipt_date) AS INTEGER) AS m, "
                "       COALESCE(SUM(profit_calc_op), 0) AS s "
                "FROM invoices "
                "WHERE parent_invoice_id IS NULL "
                "  AND invoice_number NOT LIKE 'LEAD-%' "
                "  AND is_credit = 0 "
                "  AND receipt_date IS NOT NULL AND receipt_date != '' "
                "GROUP BY m"
            )
        cur = await self.conn.execute(income_p_sql)
        for row in await cur.fetchall():
            m = int(row[0]) if row[0] else 0
            if 1 <= m <= 12:
                income_p[m] = float(row[1] or 0)

        # 2) Доход (BL) = SUM(cost_card.margin) по закрытым счетам, GROUP BY месяц.
        if is_credit:
            ended_filter = "AND (status = 'ended' OR montazh_stage = 'invoice_end')"
        else:
            ended_filter = "AND status = 'ended'"
        cur = await self.conn.execute(
            f"SELECT id, CAST(strftime('%m', receipt_date) AS INTEGER) AS m "
            f"FROM invoices "
            f"WHERE parent_invoice_id IS NULL "
            f"  AND invoice_number NOT LIKE 'LEAD-%' "
            f"  AND is_credit = ? "
            f"  AND receipt_date IS NOT NULL AND receipt_date != '' "
            f"  {ended_filter}",
            (g_credit,),
        )
        ended_rows = await cur.fetchall()
        for ended_row in ended_rows:
            inv_id = int(ended_row[0])
            m = int(ended_row[1]) if ended_row[1] else 0
            if not (1 <= m <= 12):
                continue
            cost = await self.get_full_invoice_cost_card(inv_id)
            income[m] = income.get(m, 0.0) + float(cost.get("margin") or 0)

        # 3) Расход — из op_company_monthly за указанный год.
        monthly_rows = await self.list_monthly_op_company(year=year)
        for r in monthly_rows:
            m = int(r.get("month") or 0)
            if not (1 <= m <= 12):
                continue
            if is_credit:
                expense[m] = float(r.get("expense_other") or 0)
            else:
                expense[m] = float(r.get("expense_cashless") or 0)

        # 4) AN — SUM(zp_manager_payout) по месяцу.
        cur = await self.conn.execute(
            "SELECT CAST(strftime('%m', receipt_date) AS INTEGER) AS m, "
            "       COALESCE(SUM(zp_manager_payout), 0) AS s "
            "FROM invoices "
            "WHERE parent_invoice_id IS NULL "
            "  AND invoice_number NOT LIKE 'LEAD-%' "
            "  AND is_credit = ? "
            "  AND receipt_date IS NOT NULL AND receipt_date != '' "
            "GROUP BY m",
            (g_credit,),
        )
        for row in await cur.fetchall():
            m = int(row[0]) if row[0] else 0
            if 1 <= m <= 12:
                an[m] = float(row[1] or 0)

        # 5) open_count для бейджа 🔴N.
        if is_credit:
            open_filter = (
                "AND status != 'ended' "
                "AND (montazh_stage IS NULL OR montazh_stage != 'invoice_end')"
            )
        else:
            open_filter = "AND status != 'ended'"
        cur = await self.conn.execute(
            f"SELECT CAST(strftime('%m', receipt_date) AS INTEGER) AS m, COUNT(*) AS c "
            f"FROM invoices "
            f"WHERE parent_invoice_id IS NULL "
            f"  AND invoice_number NOT LIKE 'LEAD-%' "
            f"  AND is_credit = ? "
            f"  AND receipt_date IS NOT NULL AND receipt_date != '' "
            f"  {open_filter} "
            f"GROUP BY m",
            (g_credit,),
        )
        for row in await cur.fetchall():
            m = int(row[0]) if row[0] else 0
            if 1 <= m <= 12:
                open_count[m] = int(row[1] or 0)

        # 6) forecast «Прибыль прогноз» = SUM(U − MAX(0, AJ)) по открытым.
        # MAX(0, …) на бланке (owner 01.08): AJ «Мен. ЗП (по бланку)» — зеркало колонки
        # AG «Импорт ОП», а туда ОП может поставить МИНУС (перерасчёт/сторно). Вычитание
        # отрицательного превращалось в прибавку и раздувало прогноз: у 26525-1КВ бланк
        # −7 080 давал 22 829 − (−7 080) = 29 909. Флорим ТОЛЬКО бланк — прибыльную часть
        # флорить нельзя, иначе из прогноза пропадут законно убыточные счета.
        # Тот же паттерн уже принят в кандидатах на налив аванса (см. MAX(0, …) ниже
        # в list_manager_zp_topup_candidates); денежные пути бланк и так флорят
        # (manager_zp_net_payout в utils.py), поэтому правка на выплаты не влияет.
        if is_credit:
            forecast_sql = (
                "SELECT COALESCE(SUM("
                "    COALESCE(NULLIF(profit_calc_op, 0), profit_tax, 0) "
                "    - MAX(0, COALESCE(manager_zp_blank, 0))), 0) "
                "FROM invoices "
                "WHERE parent_invoice_id IS NULL "
                "  AND invoice_number NOT LIKE 'LEAD-%' "
                "  AND is_credit = 1 "
                "  AND status != 'ended' "
                "  AND (montazh_stage IS NULL OR montazh_stage != 'invoice_end')"
            )
        else:
            forecast_sql = (
                "SELECT COALESCE(SUM("
                "    COALESCE(profit_calc_op, 0) "
                "    - MAX(0, COALESCE(manager_zp_blank, 0))), 0) "
                "FROM invoices "
                "WHERE parent_invoice_id IS NULL "
                "  AND invoice_number NOT LIKE 'LEAD-%' "
                "  AND is_credit = 0 "
                "  AND status != 'ended'"
            )
        cur = await self.conn.execute(forecast_sql)
        row = await cur.fetchone()
        forecast = float(row[0] or 0) if row else 0.0

        return {
            "income_p": income_p,
            "income": income,
            "expense": expense,
            "an": an,
            "open_count": open_count,
            "forecast": forecast,
        }

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

    # ----- Оплата замеров (объединение с леджером, ТЗ 06.07) ----- #

    async def list_zamery_for_payment(
        self, surveyor_id: int,
    ) -> list[dict[str, Any]]:
        """Выполненные замеры замерщика для экрана «Оплата замеров».

        Единица = замер (status='done'). total_cost = начисление; paid_amount/paid_date
        = оплачено (NULL = не оплачен); pay_status = 'not_requested' («К оплате») |
        'requested' («На проверке», отправлен ГД). Источник истины — zamery_requests.
        """
        cur = await self.conn.execute(
            "SELECT id, address, scheduled_date, total_cost, paid_amount, paid_date, "
            "  COALESCE(pay_status, 'not_requested') AS pay_status "
            "FROM zamery_requests "
            "WHERE assigned_to = ? AND status = 'done' "
            "ORDER BY scheduled_date, id",
            (surveyor_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_zamery_attribution(
        self, surveyor_id: int,
    ) -> list[dict[str, Any]]:
        """Все done-замеры замерщика с атрибуцией менеджера (уточнение + сводка).

        requester_role: 'manager_kv'|'manager_npn'|'manager_kia' | '' (UNK, не
        распределён). Атрибуция аналитическая — на долг/total_cost НЕ влияет.
        """
        cur = await self.conn.execute(
            "SELECT id, address, scheduled_date, "
            "  COALESCE(requester_role, '') AS requester_role, "
            "  COALESCE(requested_by, 0) AS requested_by "
            "FROM zamery_requests "
            "WHERE assigned_to = ? AND status = 'done' "
            "ORDER BY scheduled_date, id",
            (surveyor_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def list_zamery_settlement_detail(
        self, surveyor_id: int,
    ) -> list[dict[str, Any]]:
        """Все done-замеры замерщика для ПОМЕСЯЧНОЙ карточки взаиморасчётов ГД (ТЗ 14.07).

        Поля: id, address (улица), scheduled_date (дата замера), requester_role
        (менеджер-заказчик: manager_kv/kia/npn | '' UNK), total_cost (начисление/
        стоимость), paid_amount/paid_date (оплата; NULL = не оплачен). Read-only
        (только SELECT). Источник — zamery_requests, сорт по дате замера.
        """
        cur = await self.conn.execute(
            "SELECT id, address, scheduled_date, "
            "  COALESCE(requester_role, '') AS requester_role, "
            "  total_cost, paid_amount, paid_date "
            "FROM zamery_requests "
            "WHERE assigned_to = ? AND status = 'done' "
            "ORDER BY scheduled_date, id",
            (surveyor_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def set_zamery_pay_status(
        self, ids: list[int], status: str, surveyor_id: int,
    ) -> None:
        """Проставить pay_status пакету замеров (guard: свои done-замеры).

        Не трогает начисления/оплату — только статус запроса оплаты
        ('not_requested' | 'requested').
        """
        if not ids:
            return
        ph = ",".join("?" for _ in ids)
        await self.conn.execute(
            f"UPDATE zamery_requests SET pay_status = ?, updated_at = ? "
            f"WHERE id IN ({ph}) AND assigned_to = ? AND status = 'done'",
            (status, to_iso(utcnow()), *ids, surveyor_id),
        )
        await self.conn.commit()

    async def mark_zamery_paid(
        self, ids: list[int], paid_date: str, surveyor_id: int,
    ) -> tuple[list[int], float]:
        """Отметить пакет замеров оплаченными (paid_amount=total_cost, paid_date=?).

        ИДЕМПОТЕНТНО: платит только замеры с paid_amount IS NULL (guard: свои done).
        Возвращает (список фактически ново-оплаченных id, Σ их total_cost) — на эту
        сумму вызывающий заносит ОДИН платёж в леджер. Повторный вызов вернёт
        ([], 0.0) → без дубля платежа (защита от гонки/двойного клика). paid_amount
        на долг НЕ влияет (get_zamery_settlement_summary считает по total_cost) —
        долг падает именно платежом в леджер.
        """
        if not ids:
            return [], 0.0
        ph = ",".join("?" for _ in ids)
        cur = await self.conn.execute(
            f"SELECT id, total_cost FROM zamery_requests "
            f"WHERE id IN ({ph}) AND assigned_to = ? AND status = 'done' "
            f"AND paid_amount IS NULL",
            (*ids, surveyor_id),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        if not rows:
            return [], 0.0
        paid_ids = [int(r["id"]) for r in rows]
        paid_sum = float(sum(r["total_cost"] or 0 for r in rows))
        ph2 = ",".join("?" for _ in paid_ids)
        await self.conn.execute(
            f"UPDATE zamery_requests "
            f"SET paid_amount = total_cost, paid_date = ?, "
            f"    pay_status = 'not_requested', updated_at = ? "
            f"WHERE id IN ({ph2})",
            (paid_date, to_iso(utcnow()), *paid_ids),
        )
        await self.conn.commit()
        return paid_ids, paid_sum

    # ----- Взаиморасчёты с замерщиком (леджер платежей) ----- #

    async def add_zamery_settlement_entry(
        self,
        surveyor_id: int,
        entry_date: str,
        kind: str,
        amount: float,
        comment: str | None = None,
        created_by: int | None = None,
    ) -> int:
        """Запись в леджер взаиморасчётов. kind: 'opening'|'payment'|'adjustment'."""
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO zamery_settlement_entries "
            "(surveyor_id, entry_date, kind, amount, comment, created_by, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (surveyor_id, entry_date, kind, float(amount), comment, created_by, now),
        )
        await self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def list_zamery_settlement_entries(
        self, surveyor_id: int, limit: int = 200,
    ) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT * FROM zamery_settlement_entries WHERE surveyor_id = ? "
            "ORDER BY entry_date DESC, id DESC LIMIT ?",
            (surveyor_id, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_zamery_settlement_summary(
        self, surveyor_id: int,
    ) -> dict[str, Any]:
        """Сводка взаиморасчётов с замерщиком.

        Начисления (charges) = SUM(total_cost) выполненных замеров (status='done')
        — единый источник истины zamery_requests, НЕ дублируется в леджере.
        Долг = opening + adjustment + charges − payments.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(total_cost), 0) AS charges "
            "FROM zamery_requests WHERE assigned_to = ? AND status = 'done'",
            (surveyor_id,),
        )
        row = await cur.fetchone()
        n_measurements = int(row["n"] or 0)
        charges = float(row["charges"] or 0)

        cur = await self.conn.execute(
            "SELECT kind, COALESCE(SUM(amount), 0) AS s "
            "FROM zamery_settlement_entries WHERE surveyor_id = ? GROUP BY kind",
            (surveyor_id,),
        )
        by_kind = {r["kind"]: float(r["s"] or 0) for r in await cur.fetchall()}
        opening = by_kind.get("opening", 0.0)
        adjustments = by_kind.get("adjustment", 0.0)
        paid = by_kind.get("payment", 0.0)
        debt = opening + adjustments + charges - paid

        payments = await self.list_zamery_settlement_entries(surveyor_id)
        return {
            "surveyor_id": surveyor_id,
            "n_measurements": n_measurements,
            "charges": charges,
            "opening": opening,
            "adjustments": adjustments,
            "paid": paid,
            "debt": debt,
            "entries": payments,
        }

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

    async def list_zamery_journal(
        self,
        assigned_to: int,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """Журнал заявок замерщика для листа Leads (блок W:AG).

        Та же выборка что list_zamery_for_schedule (активные/выполненные с датой в окне) +
        признак конверсии has_invoice: 1, если по лиду замера есть счёт в работе/оплате.
        Логика конверсии идентична get_zamery_conversion_stats (lead_id → project → invoice).
        Read-only — только SELECT.
        """
        cur = await self.conn.execute(
            "SELECT zr.*, u.full_name AS manager_name, "
            "  CASE WHEN zr.lead_id IS NOT NULL AND EXISTS ("
            "    SELECT 1 FROM lead_tracking lt JOIN invoices i ON i.project_id = lt.project_id "
            "    WHERE lt.id = zr.lead_id AND lt.project_id IS NOT NULL "
            "    AND i.status IN ('in_progress','paid','closing','ended')"
            "  ) THEN 1 ELSE 0 END AS has_invoice "
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
        kind: str = "off",
    ) -> int:
        """kind: 'off' = выходной (день недоступен), 'busy' = день занят
        (закрыт для новых замеров, но взятые остаются)."""
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO zamery_blackout_dates (user_id, blackout_date, comment, kind, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, blackout_date, comment, kind, now),
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

    async def list_manager_leads_with_invoice(
        self, manager_id: int, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Лиды менеджера с уже выставленным РП счётом, готовым к запуску в работу.

        Возвращает invoice'ы для кнопки «Счёт в работу»: только статусы
        PENDING_PAYMENT и CREDIT (счёт выставлен РП, но менеджер ещё не запросил
        у ГД подтверждение оплаты).

        Источник связи лид→счёт: lead_tracking.invoice_id, status='invoice_issued'.
        Кроме того, добавляем счета, созданные напрямую менеджером (created_by)
        без записи в lead_tracking — для случая «без лида».
        """
        cur = await self.conn.execute(
            """
            SELECT DISTINCT i.id, i.invoice_number, i.status, i.client_name,
                   i.object_address, i.amount, i.is_credit, i.created_at
            FROM invoices i
            LEFT JOIN lead_tracking lt ON lt.invoice_id = i.id
            WHERE i.created_by = ?
              AND i.status IN ('pending', 'credit')
              AND COALESCE(i.invoice_number, '') NOT LIKE 'LEAD-%'
            ORDER BY i.created_at DESC
            LIMIT ?
            """,
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
    # INSTALLER ADVANCES (ТЗ 2026-05-19 блок C — авансы Игоря Быканова)
    # =====================================================================

    async def assert_invoice_advance_eligible(
        self, installer_id: int, invoice_id: int,
    ) -> None:
        """Гард ТЗ 2026-06-04: монтажник может брать аванс по счёту ТОЛЬКО когда он
        (а) взял счёт В РАБОТУ (montazh_stage ∈ in_work/razmery_ok/invoice_ok) И
        (б) СОГЛАСОВАЛ стоимость монтажа (montazh_agreed_amount > 0).
        Иначе ValueError с понятной причиной (ловится хендлером).
        Единый чокпоинт для всех путей «взять аванс по счёту»."""
        cur = await self.conn.execute(
            "SELECT invoice_number, assigned_to, COALESCE(montazh_stage, ''), "
            "COALESCE(montazh_agreed_amount, 0) FROM invoices WHERE id = ?",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise ValueError(f"счёт #{invoice_id} не найден")
        num = row[0] or f"#{invoice_id}"
        if int(row[1] or 0) != int(installer_id):
            raise ValueError(f"счёт {num} не назначен на вас")
        if row[2] not in ("in_work", "razmery_ok", "invoice_ok"):
            raise ValueError(f"счёт {num}: ещё не взят в работу")
        if float(row[3] or 0) <= 0:
            raise ValueError(f"счёт {num}: стоимость монтажа не согласована")

    async def create_advance_request(
        self,
        installer_id: int,
        items: list[tuple[int, float, float]],
        comment: str | None = None,
        wallet_role: str | None = None,
    ) -> int:
        """Создать запрос аванса по списку счетов.

        items = [(invoice_id, amount, plan_zp_snapshot), ...].
        Возвращает request_id. Snapshot плана ЗП — внешний, чтобы не тянуть
        installer_new._calc_est_montazh в db.
        """
        # ТЗ 2026-06-04: жёсткий гард — аванс только по счетам, взятым в работу
        # и с согласованной стоимостью. Проверяем ДО любых записей (raise → нет вставки).
        for _inv_id, _a, _z in items:
            await self.assert_invoice_advance_eligible(installer_id, int(_inv_id))
        total = sum(a for _, a, _ in items)
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, wallet_role) "
            "VALUES (?, ?, 'requested', ?, ?, ?)",
            (installer_id, total, comment, now, wallet_role),
        )
        req_id = cur.lastrowid
        for inv_id, amount, plan_zp in items:
            await self.conn.execute(
                "INSERT INTO installer_advance_items "
                "(request_id, invoice_id, amount, plan_zp_snapshot) "
                "VALUES (?, ?, ?, ?)",
                (req_id, inv_id, amount, plan_zp),
            )
        await self.conn.commit()
        await self.audit(
            actor_id=installer_id,
            action="installer_advance_requested",
            entity="advance_request",
            entity_id=str(req_id),
            payload={"total": total, "items_count": len(items)},
        )
        return int(req_id)

    async def approve_advance_request(self, request_id: int, approved_by: int) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE installer_advance_requests SET status='approved', "
            "approved_at=?, approved_by=? WHERE id=?",
            (now, approved_by, request_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=approved_by,
            action="installer_advance_approved",
            entity="advance_request",
            entity_id=str(request_id),
            payload=None,
        )

    async def reject_advance_request(
        self, request_id: int, rejected_by: int, reason: str,
    ) -> None:
        # Был ли перенос переплаты ЗП уже отклонён — определяем ДО UPDATE, иначе
        # повторное отклонение откатило бы zp_hold_advanced второй раз (30.07).
        cur = await self.conn.execute(
            "SELECT COALESCE(status, '') FROM installer_advance_requests WHERE id = ?",
            (request_id,),
        )
        row = await cur.fetchone()
        was_rejected = str(row[0] if row else "") == "rejected"
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE installer_advance_requests SET status='rejected', "
            "rejected_at=?, rejected_by=?, reject_reason=? WHERE id=?",
            (now, rejected_by, reason, request_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=rejected_by,
            action="installer_advance_rejected",
            entity="advance_request",
            entity_id=str(request_id),
            payload={"reason": reason},
        )
        # Откат отметки «переплата перенесена» по счетам этой заявки (30.07).
        # Без него счёт навсегда остаётся с zp_hold_advanced, а удержание из ЗП
        # менеджера не применяется (utils.manager_zp_net_payout платит бланк).
        if was_rejected:
            return
        try:
            reverted = await self.rollback_overpay_advance(request_id)
        except Exception:
            log.exception(
                "reject_advance_request: rollback overpay failed req_id=%s", request_id,
            )
            return
        if reverted:
            await self.audit(
                actor_id=rejected_by,
                action="manager_overpay_rollback",
                entity="advance_request",
                entity_id=str(request_id),
                payload={"reason": reason, "invoices": reverted},
            )

    async def pay_advance_request(
        self,
        request_id: int,
        paid_by: int,
        payment_file_id: str | None = None,
    ) -> None:
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE installer_advance_requests SET status='paid', "
            "paid_at=?, paid_by=?, payment_file_id=? WHERE id=?",
            (now, paid_by, payment_file_id, request_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=paid_by,
            action="installer_advance_paid",
            entity="advance_request",
            entity_id=str(request_id),
            payload={"payment_file_id": payment_file_id},
        )
        # Auto-offset на pay: если item привязан к invoice с zp=approved → сразу
        # offset. Закрывает root-cause «item создан ПОСЛЕ approve» (КВ 4 кейс).
        items = await self.get_advance_request_items(request_id)
        offsets_applied: list[dict[str, Any]] = []
        for it in items:
            inv_id = int(it["invoice_id"])
            cur = await self.conn.execute(
                "SELECT zp_installer_status, zp_installer_amount FROM invoices WHERE id=?",
                (inv_id,),
            )
            row = await cur.fetchone()
            if not row:
                continue
            zp_status = row[0] or ""
            zp_amount = float(row[1] or 0)
            if zp_status == "approved" and zp_amount > 0:
                remaining = await self.apply_advance_offsets_on_zp_approve(
                    inv_id, zp_id=inv_id, zp_amount=zp_amount, actor_id=paid_by,
                )
                offsets_applied.append(
                    {"invoice_id": inv_id, "zp_amount": zp_amount, "remaining": remaining},
                )
        if offsets_applied:
            await self.audit(
                actor_id=paid_by,
                action="installer_advance_auto_offset_on_pay",
                entity="advance_request",
                entity_id=str(request_id),
                payload={"applied": offsets_applied},
            )

    async def get_advance_request(self, request_id: int) -> dict[str, Any] | None:
        cur = await self.conn.execute(
            "SELECT * FROM installer_advance_requests WHERE id = ?",
            (request_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_advance_request_items(self, request_id: int) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT i.*, inv.invoice_number, inv.object_address "
            "FROM installer_advance_items i "
            "JOIN invoices inv ON inv.id = i.invoice_id "
            "WHERE i.request_id = ? ORDER BY i.id ASC",
            (request_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_installer_outstanding(
        self, installer_id: int, wallet_role: str | None = None,
    ) -> float:
        """Долг по авансу = Sum(paid advance.total) − Sum(items.offset_amount).

        После 25.05 funds-2balances split: считается только advance pool
        (type='request' и 'transfer_depo_to_adv'), depo и withdraw — отдельный pool.
        Эквивалентно get_advance_balance — оставлено как alias для backward-compat
        (используется в existing distribute UI).
        """
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv')" + wclause,
            (installer_id, *wparams),
        )
        total_paid = float((await cur.fetchone())[0] or 0)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.offset_amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv') "
            "  AND i.offset_zp_id IS NOT NULL "
            "  AND i.offset_zp_id NOT IN (-1, -2)" + wclause,
            (installer_id, *wparams),
        )
        total_offset = float((await cur.fetchone())[0] or 0)
        # Зачёт аванса в оклад РП (31.07) — отдельной строкой, не item'ом (см. _sum_rp_oklad_offset).
        total_oklad = await self._sum_rp_oklad_offset(installer_id, wallet_role)
        return max(0.0, total_paid - total_offset - total_oklad)

    async def get_open_advance_items_for_invoice(
        self, invoice_id: int,
    ) -> list[dict[str, Any]]:
        """Открытые items по этому счёту (paid, без offset_zp_id)."""
        cur = await self.conn.execute(
            "SELECT i.id, i.amount, i.plan_zp_snapshot, r.installer_id, r.requested_at "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE i.invoice_id = ? AND r.status = 'paid' AND i.offset_zp_id IS NULL "
            "ORDER BY r.requested_at ASC",
            (invoice_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_advance_taken_for_invoice(
        self, invoice_id: int, statuses: tuple[str, ...] = ("approved", "paid"),
    ) -> float:
        """Сумма аванса, уже взятого по счёту (для расчёта доступного при запросе)."""
        ph = ",".join("?" * len(statuses))
        cur = await self.conn.execute(
            f"SELECT COALESCE(SUM(i.amount), 0) FROM installer_advance_items i "
            f"JOIN installer_advance_requests r ON r.id = i.request_id "
            f"WHERE i.invoice_id = ? AND r.status IN ({ph})",
            (invoice_id, *statuses),
        )
        row = await cur.fetchone()
        return float(row[0] or 0) if row else 0.0

    async def get_advance_paid_open_total_for_invoice(self, invoice_id: int) -> float:
        """Sum(amount) WHERE r.status='paid' AND offset_zp_id IS NULL.

        Используется для решения auto-close (paid_advances >= plan_zp).
        """
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE i.invoice_id = ? AND r.status = 'paid' AND i.offset_zp_id IS NULL",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return float(row[0] or 0) if row else 0.0

    async def get_manager_advance_for_invoice(self, invoice_id: int) -> float:
        """Σ аванса менеджера, привязанного к счёту (open+closed items).

        Для витрины «аванс уже выдан» в карточке и уведомлении выплаты ЗП менеджера
        (gd_zp_manager_pay / _finalize_zp_manager_pay). Считает И открытые, И закрытые
        (offset_zp_id) items — закрытие через зачёт ЗП не должно убирать сумму из показа
        «сколько выдано авансом». Display-only, деньги не двигает. wallet_role ограничен
        менеджерскими кошельками.
        """
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE i.invoice_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv') "
            "  AND r.wallet_role IN ('manager_kv', 'manager_kia', 'manager_npn')",
            (invoice_id,),
        )
        row = await cur.fetchone()
        return float(row[0] or 0) if row else 0.0

    async def apply_advance_offsets_on_zp_approve(
        self,
        invoice_id: int,
        zp_id: int,
        zp_amount: float,
        actor_id: int,
        role: str = "installer",
    ) -> float:
        """Auto-offset открытых items этого счёта.

        Шаги:
        1. Закрыть существующие open items этого invoice до zp_amount.
        2. Если invoice кредитный и осталось remaining > 0 — pull из unallocated
           (новый item с immediate offset). Только role='installer'
           (кредит-pull = специфика монтажника).
        3. Если суммарно покрыто полностью (remaining=0) и zp_*_status='approved'
           — поставить zp_*_status='confirmed' (installer или manager).

        role='installer' (default) → обновляет zp_installer_*.
        role='manager' → обновляет zp_manager_*. Step 2 пропускается.

        Возвращает zp_remaining (после зачёта).
        """
        items = await self.get_open_advance_items_for_invoice(invoice_id)
        remaining = float(zp_amount)
        for item in items:
            if remaining <= 0:
                break
            offset = min(float(item["amount"]), remaining)
            now = to_iso(utcnow())
            await self.conn.execute(
                "UPDATE installer_advance_items SET offset_zp_id=?, offset_at=?, offset_amount=? "
                "WHERE id=?",
                (zp_id, now, offset, item["id"]),
            )
            remaining -= offset
            await self.audit(
                actor_id=actor_id,
                action="installer_advance_offset",
                entity="advance_item",
                entity_id=str(item["id"]),
                payload={
                    "offset_amount": offset,
                    "zp_id": zp_id,
                    "invoice_id": invoice_id,
                },
            )
        # Step 2: credit pull from unallocated (только для is_credit invoice + role=installer).
        if remaining > 0 and role == "installer":
            cur = await self.conn.execute(
                "SELECT COALESCE(is_credit,0) AS is_credit, assigned_to FROM invoices WHERE id=?",
                (invoice_id,),
            )
            inv_row = await cur.fetchone()
            if inv_row and int(inv_row[0]) == 1:
                installer_id_for_credit = inv_row[1]
                if installer_id_for_credit:
                    unallocated = await self.get_advance_outstanding_unallocated(
                        int(installer_id_for_credit),
                    )
                    if unallocated > 0:
                        pull = min(remaining, unallocated)
                        cur = await self.conn.execute(
                            "SELECT id FROM installer_advance_requests "
                            "WHERE installer_id=? AND status='paid' "
                            "  AND request_type IN ('request','transfer_depo_to_adv') "
                            "ORDER BY paid_at DESC LIMIT 1",
                            (int(installer_id_for_credit),),
                        )
                        req_row = await cur.fetchone()
                        if req_row:
                            req_id = int(req_row[0])
                            cur_check = await self.conn.execute(
                                "SELECT id FROM installer_advance_items "
                                "WHERE invoice_id=? AND offset_zp_id=? "
                                "AND request_id=? AND offset_amount=?",
                                (invoice_id, zp_id, req_id, pull),
                            )
                            if not await cur_check.fetchone():
                                now = to_iso(utcnow())
                                cur2 = await self.conn.execute(
                                    "INSERT INTO installer_advance_items "
                                    "(request_id, invoice_id, amount, plan_zp_snapshot, "
                                    " offset_zp_id, offset_amount, offset_at) "
                                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (req_id, invoice_id, pull, zp_amount, zp_id, pull, now),
                                )
                                new_item_id = int(cur2.lastrowid)
                                remaining -= pull
                                await self.audit(
                                    actor_id=actor_id,
                                    action="installer_advance_auto_credit_topup",
                                    entity="advance_item",
                                    entity_id=str(new_item_id),
                                    payload={
                                        "invoice_id": invoice_id,
                                        "amount": pull,
                                        "from_unallocated": True,
                                        "trigger": "zp_approve_credit",
                                    },
                                )
        # Step 3: если полностью покрыто → ZP=confirmed (ветвление по роли).
        if zp_amount > 0 and remaining <= 0.001:
            now = to_iso(utcnow())
            if role == "manager":
                # ЗП менеджера полностью покрыта авансом → фиксируем выплату по счёту:
                # confirmed + сумма (zp_manager_payout/AN) + дата (zp_manager_payout_date/AO).
                # AN/AO уже в _ZP_PAYOUT_PRESERVE → бот-выплата durable (пустой синк
                # «Импорт ОП» не затрёт; ручной ввод owner'ом в ОП перезаписывает). Дату
                # не перетираем, если уже проставлена. Паттерн 1:1 с выплатой ЗП РП (AR/AS).
                pay_date = datetime.now().strftime("%d.%m.%Y")
                await self.conn.execute(
                    "UPDATE invoices SET zp_manager_status='confirmed', "
                    "    zp_manager_payout=?, "
                    "    zp_manager_payout_date="
                    "COALESCE(NULLIF(zp_manager_payout_date, ''), ?), "
                    "    updated_at=? "
                    "WHERE id=? AND zp_manager_status='approved'",
                    (zp_amount, pay_date, now, invoice_id),
                )
            else:
                await self.conn.execute(
                    "UPDATE invoices SET zp_installer_status='confirmed', "
                    "    zp_installer_confirmed_at=? "
                    "WHERE id=? AND zp_installer_status='approved'",
                    (now, invoice_id),
                )
        await self.conn.commit()
        return remaining

    async def close_open_advance_items_for_invoice(
        self, invoice_id: int, zp_id: int, actor_id: int,
    ) -> float:
        """Закрыть открытые earmark-авансы счёта БЕЗ вычета из доплаты (Часть 2).

        Для заявки-ОСТАТКА (zp_installer_remainder=1): аванс УЖЕ вычтен в сумме
        остатка (zp_installer_amount), поэтому повторный зачёт против остатка
        задвоил бы аванс. Здесь только помечаем открытые earmark как привязанные
        к этому ZP (offset_zp_id), чтобы баланс кошелька и журнал авансов были
        согласованы — но бот платит ВЕСЬ остаток (не уменьшаем доплату).
        НЕ делаем кредит-pull и НЕ ставим confirmed (это путь apply_advance_offsets_on_zp_approve
        для старой семантики «бот платит всю согласованную»). Возвращает Σ закрытого аванса.
        """
        items = await self.get_open_advance_items_for_invoice(invoice_id)
        total = 0.0
        now = to_iso(utcnow())
        for item in items:
            amt = float(item["amount"])
            await self.conn.execute(
                "UPDATE installer_advance_items SET offset_zp_id=?, offset_at=?, offset_amount=? "
                "WHERE id=?",
                (zp_id, now, amt, item["id"]),
            )
            total += amt
            await self.audit(
                actor_id=actor_id,
                action="installer_advance_offset_remainder",
                entity="advance_item",
                entity_id=str(item["id"]),
                payload={"offset_amount": amt, "zp_id": zp_id, "invoice_id": invoice_id},
            )
        await self.conn.commit()
        return total

    async def auto_close_montazh_by_advance(
        self, invoice_id: int, plan_zp_base: float, plan_zp_total: float,
        actor_id: int,
    ) -> bool:
        """При invoice_end: paid_advances >= БАЗА → авто-провести ЗП = ИТОГ.

        Авансы считаются от базы (×0.67 / ×0.05). Финальная ЗП = база + 10%
        (для б.н.) или = база (для кредита) — передаётся как plan_zp_total.
        Возвращает True если auto-close прошёл.
        """
        if plan_zp_base <= 0:
            return False
        paid_open = await self.get_advance_paid_open_total_for_invoice(invoice_id)
        if paid_open < plan_zp_base:
            return False
        # Auto-close: ставим payment_sent с amount = итоговая ЗП (с +10% для б.н.).
        await self.set_invoice_zp_installer_status(
            invoice_id, status="payment_sent", amount=plan_zp_total,
        )
        # Зачитываем авансы (≤ база); 10%-бонус останется к доплате как remaining.
        await self.apply_advance_offsets_on_zp_approve(
            invoice_id, zp_id=invoice_id, zp_amount=plan_zp_base, actor_id=actor_id,
        )
        await self.audit(
            actor_id=actor_id,
            action="invoice_montazh_closed_by_advance",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "plan_zp_base": plan_zp_base,
                "plan_zp_total": plan_zp_total,
                "paid_advances": paid_open,
            },
        )
        return True

    async def get_advance_journal(
        self, installer_id: int, limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Для карточки «💼 Мой баланс»: журнал выдач + зачётов.

        События двух типов: 'give' (создание/оплата запроса) и 'offset' (зачёт ZP).
        """
        # Выдачи
        cur1 = await self.conn.execute(
            "SELECT 'give' AS event_type, r.requested_at AS ts, r.id AS req_id, "
            "       NULL AS invoice_id, r.total_amount AS amount, r.status, "
            "       NULL AS offset_zp_id "
            "FROM installer_advance_requests r "
            "WHERE r.installer_id = ? "
            "ORDER BY r.requested_at DESC LIMIT ?",
            (installer_id, limit),
        )
        give_rows = [dict(r) for r in await cur1.fetchall()]
        # Зачёты
        cur2 = await self.conn.execute(
            "SELECT 'offset' AS event_type, i.offset_at AS ts, i.request_id AS req_id, "
            "       i.invoice_id, i.offset_amount AS amount, NULL AS status, i.offset_zp_id "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE r.installer_id = ? AND i.offset_zp_id IS NOT NULL "
            "ORDER BY i.offset_at DESC LIMIT ?",
            (installer_id, limit),
        )
        offset_rows = [dict(r) for r in await cur2.fetchall()]
        all_rows = give_rows + offset_rows
        all_rows.sort(key=lambda r: (r.get("ts") or ""), reverse=True)
        return all_rows[:limit]

    async def list_pending_advance_payouts(self) -> list[dict[str, Any]]:
        """approved авансы, ожидающие оплаты ГД.

        Возвращает список requests; для каждого можно подгрузить items через
        get_advance_request_items.
        """
        cur = await self.conn.execute(
            "SELECT r.* FROM installer_advance_requests r "
            "WHERE r.status = 'approved' "
            "ORDER BY r.approved_at ASC",
        )
        return [dict(r) for r in await cur.fetchall()]

    async def write_off_advance_item(
        self, item_id: int, actor_id: int, reason: str,
    ) -> None:
        """Ручное списание непогашенного аванса (например при уходе монтажника)."""
        now = to_iso(utcnow())
        await self.conn.execute(
            "UPDATE installer_advance_items SET offset_zp_id=-1, offset_at=?, "
            "offset_amount=amount WHERE id=?",
            (now, item_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_advance_writeoff",
            entity="advance_item",
            entity_id=str(item_id),
            payload={"reason": reason},
        )

    async def get_invoice_advance_metrics(self, invoice_id: int) -> dict[str, Any]:
        """Для Invoices DB-DF: paid / offset / open / last_paid_at / zp_to_pay.

        zp_to_pay рассчитывается caller'ом (нужны plan_base/plan_total из handler'а).
        Здесь возвращаем то, что можно посчитать из БД.
        """
        cur = await self.conn.execute(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN r.status='paid' THEN i.amount ELSE 0 END), 0) AS paid, "
            "  COALESCE(SUM(CASE WHEN i.offset_zp_id IS NOT NULL THEN COALESCE(i.offset_amount, 0) ELSE 0 END), 0) AS offset_total, "
            "  COALESCE(SUM(CASE WHEN r.status='paid' AND i.offset_zp_id IS NULL THEN i.amount ELSE 0 END), 0) AS open_amt, "
            "  MAX(CASE WHEN r.status='paid' THEN r.paid_at ELSE NULL END) AS last_paid_at "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE i.invoice_id = ?",
            (invoice_id,),
        )
        row = await cur.fetchone()
        if not row:
            return {"paid": 0.0, "offset": 0.0, "open": 0.0,
                    "last_paid_at": "", "zp_to_pay": 0.0}
        last_at = (row["last_paid_at"] or "")[:10]
        return {
            "paid": float(row["paid"] or 0),
            "offset": float(row["offset_total"] or 0),
            "open": float(row["open_amt"] or 0),
            "last_paid_at": last_at,
            "zp_to_pay": 0.0,  # заполняется caller'ом (нужен plan_total)
        }

    async def list_all_advance_events(
        self, limit: int = 500,
    ) -> dict[str, list[dict[str, Any]]]:
        """Бандл для листа «Авансы монтажников» в Google Sheets.

        Возвращает {"requests": [...], "items": [...]}; рендер строк делает
        sheets.sync_advances_journal по этим двум таблицам.

        Поля requests включают initiator (installer/gd) и request_type
        (request/deposit/withdraw) — sheets.py использует их для рендера новых
        event-типов 'deposit' и 'withdraw'.
        """
        cur1 = await self.conn.execute(
            "SELECT r.id AS req_id, r.installer_id, r.total_amount, r.status, "
            "       r.comment, r.requested_at, r.approved_at, r.paid_at, "
            "       r.rejected_at, r.reject_reason, "
            "       COALESCE(r.initiator, 'installer') AS initiator, "
            "       COALESCE(r.request_type, 'request') AS request_type, "
            "       r.payment_file_id, r.wallet_role "
            "FROM installer_advance_requests r "
            "ORDER BY r.requested_at DESC LIMIT ?",
            (limit,),
        )
        requests = [dict(r) for r in await cur1.fetchall()]
        cur2 = await self.conn.execute(
            "SELECT i.id, i.request_id, i.invoice_id, i.amount, i.offset_amount, "
            "       i.offset_at, i.offset_zp_id, i.plan_zp_snapshot, r.installer_id, "
            "       inv.invoice_number, inv.object_address "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "JOIN invoices inv ON inv.id = i.invoice_id "
            "ORDER BY i.id DESC LIMIT ?",
            (limit,),
        )
        items = [dict(r) for r in await cur2.fetchall()]
        return {"requests": requests, "items": items}

    # =====================================================================
    # Депозит-кошелёк монтажника (TZ tingly-twirling-whistle 2026-05-25)
    # =====================================================================

    @staticmethod
    def _wallet_clause(wallet_role: str | None, alias: str = "r") -> tuple[str, list[Any]]:
        """SQL-фрагмент фильтра кошелька для installer_advance_requests.

        wallet_role=None       → без фильтра (все строки сотрудника; legacy/single-role).
        wallet_role='rp'       → строго кошелёк РП (только tagged 'rp').
        иначе (primary/manager)→ первичный кошелёк: NULL OR != 'rp'
                                  (single-role NULL + менеджерский 'manager_npn').

        Возвращает (" AND ...", params) — фрагмент дописывается в конец WHERE,
        поэтому его параметр (если есть) идёт последним для своего alias.
        """
        if wallet_role is None:
            return "", []
        if wallet_role == "rp":
            return f" AND {alias}.wallet_role = ?", ["rp"]
        return f" AND ({alias}.wallet_role IS NULL OR {alias}.wallet_role != 'rp')", []

    async def _sum_rp_oklad_offset(
        self, employee_id: int, wallet_role: str | None = None,
    ) -> float:
        """Σ аванса, погашенного зачётом в оклад РП (request_type='oklad_offset').

        Гашение оформлено ОТДЕЛЬНОЙ строкой requests, а не item'ом: у оклада нет счёта,
        а items.invoice_id NOT NULL REFERENCES invoices(id) при foreign_keys=ON. Сумма
        хранится в ТЕЛЕ аванса (без ×1,1) — кошелёк ведётся в теле.

        Вычитается во ВСЕХ трёх формулах кошелька (get_installer_outstanding /
        get_advance_balance / get_advance_outstanding_unallocated). Пропустить хоть одну —
        и те же деньги уйдут второй раз через apply_rp_advance_to_invoice_now:
        аванс ЗАКРЫВАЕТ ЗП, а не суммируется с ней [[feedback_installer_advance_closes_zp]].
        """
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type = ?" + wclause,
            (employee_id, RP_OKLAD_OFFSET_TYPE, *wparams),
        )
        return float((await cur.fetchone())[0] or 0)

    async def get_deposit_balance(
        self, installer_id: int, wallet_role: str | None = None,
    ) -> float:
        """Баланс депозита (кошелёк) сотрудника. После split 25.05 — только deposit-rows.

        balance = SUM(paid type='deposit')
                − SUM(items.offset_amount where parent.type='deposit' AND offset_zp_id positive)
                − SUM(paid type='withdraw')
                − SUM(paid type='transfer_depo_to_adv')

        Writeoff (offset_zp_id=-1) и transfer-virtual-items (offset_zp_id=-2) не вычитаются —
        реальный расход депозита отражён через отдельные request_type rows.
        """
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type = 'deposit'" + wclause,
            (installer_id, *wparams),
        )
        total_deposit_in = float((await cur.fetchone())[0] or 0)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.offset_amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type = 'deposit' "
            "  AND i.offset_zp_id IS NOT NULL "
            "  AND i.offset_zp_id NOT IN (-1, -2)" + wclause,
            (installer_id, *wparams),
        )
        total_deposit_to_zp = float((await cur.fetchone())[0] or 0)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('withdraw', 'transfer_depo_to_adv')" + wclause,
            (installer_id, *wparams),
        )
        total_out = float((await cur.fetchone())[0] or 0)
        return max(0.0, total_deposit_in - total_deposit_to_zp - total_out)

    async def get_advance_balance(
        self, employee_id: int, wallet_role: str | None = None,
    ) -> float:
        """Баланс аванса сотрудника. Введено 25.05 funds-2balances split.

        balance = SUM(paid type='request')
                + SUM(paid type='transfer_depo_to_adv')
                − SUM(items.offset_amount where parent.type='request' AND offset_zp_id positive)

        NULL и -1 (writeoff), -2 (withdraw-маркер) — НЕ вычитаются.
        Items с положительным offset_zp_id = деньги ушли в ZP (закрыты).
        Items с NULL = распределение бронь, аванс ещё доступен сотруднику.
        """
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv')" + wclause,
            (employee_id, *wparams),
        )
        total_in = float((await cur.fetchone())[0] or 0)
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(i.offset_amount), 0) "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type = 'request' "
            "  AND i.offset_zp_id IS NOT NULL "
            "  AND i.offset_zp_id NOT IN (-1, -2)" + wclause,
            (employee_id, *wparams),
        )
        total_to_zp = float((await cur.fetchone())[0] or 0)
        # Зачёт аванса в оклад РП (31.07) — отдельной строкой, не item'ом (см. _sum_rp_oklad_offset).
        total_oklad = await self._sum_rp_oklad_offset(employee_id, wallet_role)
        return max(0.0, total_in - total_to_zp - total_oklad)

    async def get_unpaid_zp_for_summary(self, emp_id: int, role_key: str) -> float:
        """Σ невыплаченной ЗП сотрудника по его счетам — для столбца «Зачислено из ЗП»
        сводки листа «Авансирование сотрудников» (ТЗ Часть B/C, 30.05).

        ТОЛЬКО ОТОБРАЖЕНИЕ: показывает, сколько заработанной-но-ещё-не-выданной ЗП
        «накопилось» в кошельке аванса. НЕ влияет на реальный баланс/право вывода
        (get_advance_balance) и НЕ меняет порядок выплаты ЗП (запрос → одобрение ГД).

        Источники подтверждены read-only сверкой прода 30.05 (см. ТЗ):
          installer    → Σ zp_installer_amount, счета assigned_to=emp_id, статус НЕ в
                         ('approved','payment_sent','confirmed') (= ещё не выплачено);
          manager_*    → Σ manager_zp_blank (AJ), счета creator_role=role_key,
                         zp_manager_payout (AN) пусто/0;
          rp           → Σ npn_amount (10% НПН), rp_payout_op (AR) пусто/0 И
                         rp_payout_advance_at пусто (не забрано в аванс; гард 07.06;
                         override РП по всем брендам; единственный РП — Павел).
        """
        if role_key == "installer":
            sql = (
                "SELECT COALESCE(SUM(COALESCE(zp_installer_amount, 0)), 0) FROM invoices "
                "WHERE assigned_to = ? AND COALESCE(zp_installer_amount, 0) > 0 "
                "  AND COALESCE(zp_installer_status, '') "
                "      NOT IN ('approved', 'payment_sent', 'confirmed')"
            )
            args = (emp_id,)
        elif role_key in ("manager_kv", "manager_kia", "manager_npn"):
            # status-фильтр добавлен 06.06 (наполнение аванса менеджера): забранная
            # в аванс ЗП метится бот-полем zp_manager_status='confirmed' (импорт-
            # безопасно, в отличие от zp_manager_payout/AN, которое реимпорт ОП
            # затирает) → исключаем confirmed/approved/payment_sent, чтобы налитая
            # ЗП ушла из «ожидаемой» и не было двойного учёта. На текущих данных
            # эквивалентно прежнему payout=0 (approved/ps все с payout>0).
            sql = (
                "SELECT COALESCE(SUM(COALESCE(manager_zp_blank, 0)), 0) "
                "     + COALESCE(SUM(CASE WHEN ABS(COALESCE(outstanding_debt, 0)) < 1 "
                "                         THEN COALESCE(zp_manager_hold, 0) ELSE 0 END), 0) "
                "FROM invoices "
                "WHERE creator_role = ? AND COALESCE(manager_zp_blank, 0) > 0 "
                "  AND COALESCE(zp_manager_payout, 0) = 0 "
                "  AND COALESCE(zp_manager_status, 'not_requested') "
                "      NOT IN ('approved', 'payment_sent', 'confirmed')"
            )
            args = (role_key,)
        elif role_key == "rp":
            # rp_payout_advance_at-гард (07.06): забранная в аванс ЗП РП метится
            # durable бот-полем rp_payout_advance_at (НЕ rp_payout_op/AR, которое
            # реимпорт ОП затирает) → исключаем налитое, чтобы оно ушло из
            # «ожидаемой» суммы и не было двойного учёта.
            sql = (
                "SELECT COALESCE(SUM(COALESCE(npn_amount, 0)), 0) FROM invoices "
                "WHERE COALESCE(npn_amount, 0) > 0 AND COALESCE(rp_payout_op, 0) = 0 "
                "  AND rp_payout_advance_at IS NULL "
                "  AND id NOT IN ("
                "      SELECT it.invoice_id FROM installer_advance_items it "
                "      JOIN installer_advance_requests r ON r.id = it.request_id "
                "      WHERE r.wallet_role = 'rp' AND it.invoice_id IS NOT NULL "
                "        AND it.offset_zp_id IS NOT NULL)"
            )
            args = ()
        else:
            return 0.0
        cur = await self.conn.execute(sql, args)
        row = await cur.fetchone()
        # max(0): механизм перерасчёта может занулить (удержание ≥ бланка) —
        # невыплаченная ЗП не уходит в минус (флор как в dashboard_metrics).
        return max(0.0, float((row[0] if row else 0) or 0))

    async def list_rp_advance_fill_invoices(self) -> list[dict[str, Any]]:
        """Счета для наполнения кошелька аванса РП незабранной ЗП (10% НПН).

        Критерий (07.06, импорт-безопасный + «одна ЗП учитывается один раз»):
        npn_amount (AP) > 0 И НЕ забрано в аванс ранее (rp_payout_advance_at пусто)
        И НЕ выплачено через ГД (rp_payout_op/AR=0) И нет висящего запроса ГД-выплаты
        (rp_request_op/AQ=0) И НЕ зачтено из аванса РП (нет rp-offset по счёту —
        взаимоисключение с распределением apply_rp_advance_to_invoice_now, user
        2026-06-13) — взаимоисключение с _list_rp_zp_eligible_invoices (rp.py),
        чтобы одна и та же 10%-ЗП не попала ни в налив, ни в ГД-выплату, ни в
        распределение. По всем брендам и всем статусам (ended/credit/in_progress;
        единственный РП — Павел). На экране — адрес (J) + сумма ЗП (AP). read-only.
        """
        cur = await self.conn.execute(
            "SELECT id, invoice_number, object_address, status, "
            "       COALESCE(npn_amount, 0) AS npn_amount "
            "FROM invoices "
            "WHERE COALESCE(npn_amount, 0) > 0 "
            "  AND rp_payout_advance_at IS NULL "
            "  AND COALESCE(rp_payout_op, 0) = 0 "
            "  AND COALESCE(rp_request_op, 0) = 0 "
            "  AND id NOT IN ("
            "      SELECT it.invoice_id FROM installer_advance_items it "
            "      JOIN installer_advance_requests r ON r.id = it.request_id "
            "      WHERE r.wallet_role = 'rp' AND it.invoice_id IS NOT NULL "
            "        AND it.offset_zp_id IS NOT NULL) "
            "ORDER BY receipt_date DESC, id DESC"
        )
        return [dict(r) for r in await cur.fetchall()]

    async def credit_rp_zp_to_advance(
        self, rp_id: int, invoice_ids: list[int],
    ) -> tuple[int, float, list[dict[str, Any]]]:
        """РП забирает незабранную ЗП (10% НПН) по выбранным счетам в кошелёк аванса.

        Метка забранного = durable бот-поле rp_payout_advance_at := now (07.06,
        импорт-безопасно). НЕ трогаем rp_payout_op/AR «Выдано РП»: оно входит в
        sheet_fields и парсится из «Импорт ОП» (col AW) → реимпорт ОП затирал бы его
        в NULL, счёт вернулся бы в кандидаты → двойной налив (как у менеджеров до
        фикса 06.06). Аванс отражается ТОЛЬКО в журнале «Авансирование сотрудников»,
        не в колонках Invoices (feedback_advance_deposit_journal_only).

        Для каждого счёта пропуск (идемпотентно), если: npn_amount<=0 | уже выплачено
        ГД (rp_payout_op!=0) | уже забрано в аванс ранее (rp_payout_advance_at IS NOT
        NULL). «Одна ЗП учитывается ОДИН раз» (user 30.05): счёт, выплаченный ГД, в
        налив не попадёт (фильтр list_rp_advance_fill_invoices), а налитый счёт — в
        ГД-выплату (гард _list_rp_zp_eligible_invoices по rp_payout_advance_at).

        Σ сумм → ОДИН topup кошелька (request_type='request', wallet_role='rp',
        initiator='employee', без чека — собственная ЗП). Атомарно: все UPDATE
        invoices + INSERT topup в одной транзакции (один commit, rollback при сбое).
        Возвращает (request_id, total, credited[{invoice_id, invoice_number, amount}]).
        """
        if not invoice_ids:
            return (0, 0.0, [])
        now_iso = to_iso(utcnow())
        credited: list[dict[str, Any]] = []
        total = 0.0
        req_id = 0
        try:
            for inv_id in invoice_ids:
                cur = await self.conn.execute(
                    "SELECT invoice_number, COALESCE(npn_amount, 0) AS ap, "
                    "       COALESCE(rp_payout_op, 0) AS ar, "
                    "       COALESCE(rp_request_op, 0) AS req, "
                    "       rp_payout_advance_at AS adv "
                    "FROM invoices WHERE id = ?",
                    (int(inv_id),),
                )
                row = await cur.fetchone()
                if not row:
                    continue
                ap = float(row["ap"] or 0)
                ar = float(row["ar"] or 0)
                req = float(row["req"] or 0)
                # пропуск (идемпотентно + взаимоисключение «одна ЗП один раз»):
                # нет суммы | уже выплачено ГД (rp_payout_op) | висит запрос ГД-выплаты
                # (rp_request_op) | уже забрано в аванс ранее (rp_payout_advance_at)
                if ap <= 0 or ar != 0 or req != 0 or row["adv"] is not None:
                    continue
                await self.conn.execute(
                    "UPDATE invoices SET rp_payout_advance_at = ?, "
                    "updated_at = ? WHERE id = ?",
                    (now_iso, now_iso, int(inv_id)),
                )
                credited.append({
                    "invoice_id": int(inv_id),
                    "invoice_number": row["invoice_number"],
                    "amount": ap,
                })
                total += ap
            if not credited:
                return (0, 0.0, [])
            nums = ", ".join(f"№{c['invoice_number']}" for c in credited)
            cur = await self.conn.execute(
                "INSERT INTO installer_advance_requests "
                "(installer_id, total_amount, status, comment, requested_at, paid_at, "
                " paid_by, initiator, request_type, wallet_role) "
                "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'request', 'rp')",
                (rp_id, total, f"ЗП РП в аванс: {nums}", now_iso, now_iso, rp_id),
            )
            req_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        try:
            await self.audit(
                actor_id=rp_id,
                action="rp_zp_to_advance",
                entity="advance_request",
                entity_id=str(req_id),
                payload={"total": total, "invoices": credited},
            )
        except Exception:
            log.exception("credit_rp_zp_to_advance: audit failed req_id=%s", req_id)
        return (req_id, total, credited)

    async def get_rp_oklad_advance_status(
        self, year: int, month: int,
    ) -> dict[str, Any]:
        """Статус оклада РП за (year, month) — взаимоисключение «один оклад в месяц» (A2).

        Источник истины — op_company_entries (лист «Баланс компании» = её рендер):
          gd_paid    — ГД уже выплатил оклад (description LIKE 'Оклад РП%');
          to_advance — Σ уже переведённого в кошелёк аванса (description = RP_OKLAD_ADVANCE_DESC);
          remaining  — остаток к переводу (0 если gd_paid, иначе 66000 − to_advance).
        read-only.
        """
        cur = await self.conn.execute(
            "SELECT COUNT(*) FROM op_company_entries "
            "WHERE year = ? AND month = ? AND description LIKE 'Оклад РП%'",
            (int(year), int(month)),
        )
        gd_paid = int((await cur.fetchone())[0] or 0) > 0
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(cashless_amount), 0) FROM op_company_entries "
            "WHERE year = ? AND month = ? AND description = ?",
            (int(year), int(month), RP_OKLAD_ADVANCE_DESC),
        )
        to_advance = float((await cur.fetchone())[0] or 0)
        remaining = 0.0 if gd_paid else max(0.0, float(RP_SALARY_MONTHLY) - to_advance)
        return {"gd_paid": gd_paid, "to_advance": to_advance, "remaining": remaining}

    async def get_rp_oklad_advance_offset(self, rp_id: int) -> dict[str, float]:
        """Сколько выданного аванса РП зачитывается в оклад и что остаётся к выплате.

        ТЗ owner 31.07: «аванс РП должен автоматически вычитаться из оклада, и ГД должна
        приходить карточка задачи с суммой остатка». read-only, ничего не пишет.

          raw    — свободный остаток кошелька РП (ТЕЛО аванса, wallet_role='rp');
          gross  — он же в виде оклада: raw × 1,1 (оклад идёт б/н самозанятому);
          deduct — сколько реально вычитаем из 66 000 (не больше самого оклада);
          payout — к выплате = 66 000 − deduct;
          body   — тело аванса, которое гасим в кошельке = deduct / 1,1;
          carry  — непогашенный хвост (gross − deduct), остаётся в кошельке на следующий месяц.

        ⚠️ Не путать с get_rp_oklad_advance_status: там `remaining` про ДРУГОЙ канал —
        РП сам переводит оклад будущего месяца в кошелёк (A2), а не про выданный ГД аванс.

        Кошелёк строго 'rp': менеджерский кошелёк Павла (manager_npn) к окладу РП
        отношения не имеет [[feedback_rp_npn_separate_wallets]].
        """
        raw = await self.get_advance_balance(int(rp_id), "rp")
        gross = round(raw * RP_OKLAD_ADVANCE_GROSSUP, 2)
        deduct = round(min(gross, float(RP_SALARY_MONTHLY)), 2)
        return {
            "raw": raw,
            "gross": gross,
            "deduct": deduct,
            "payout": round(float(RP_SALARY_MONTHLY) - deduct, 2),
            "body": round(deduct / RP_OKLAD_ADVANCE_GROSSUP, 2),
            "carry": round(gross - deduct, 2),
        }

    async def record_rp_salary_payment(
        self, rp_id: int, year: int, month: int, month_str: str,
        date_display: str, rp_label: str, actor_id: int,
    ) -> dict[str, Any]:
        """ГД выплачивает оклад РП: запись в «Баланс компании» + гашение аванса — АТОМАРНО.

        До 31.07 хендлер писал в op_company_entries ровно 66 000 и про аванс не знал.
        Теперь в ОДНОЙ транзакции:
          (1) op_company_entries(cashless_amount=payout, description='Оклад РП {name} {YYYY-MM}')
              — форма description НЕ менялась, маркер «месяц закрыт» (LIKE 'Оклад РП%') цел;
          (2) строка гашения кошелька installer_advance_requests(request_type='oklad_offset',
              wallet_role='rp', total_amount=ТЕЛО аванса) — только если аванс был.

        В БК пишется ФАКТИЧЕСКИ выплаченное (payout), а не 66 000: выдача аванса уже прошла
        расходом раньше (op_company_entries source='credit_wallet_spend'), и полная сумма
        задвоила бы расход компании.

        Внутри транзакции — повторная проверка gd_paid (анти-гонка\двойной клик), как в
        credit_rp_oklad_to_advance / record_rp_oklad_received. Она же единственный барьер
        идемпотентности: два оклада за месяц невозможны, значит и два гашения тоже.
        НЕ использует add_op_company_entry (тот коммитит сам — разрыв атомарности).

        Возвращает {entry_id, offset_req_id, payout, deduct, body, raw, carry}. audit — после commit.
        """
        now_iso = to_iso(utcnow())
        date_iso = datetime.strptime(date_display, "%d.%m.%Y").strftime("%Y-%m-%d")
        description = f"Оклад РП {rp_label} {month_str}"
        offset_comment = f"{RP_OKLAD_OFFSET_DESC} {month_str}"
        try:
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM op_company_entries "
                "WHERE year = ? AND month = ? AND description LIKE 'Оклад РП%'",
                (int(year), int(month)),
            )
            if int((await cur.fetchone())[0] or 0) > 0:
                raise OkladAlreadyPaidError()
            # Считаем ВНУТРИ транзакции: между показом карточки и кликом ГД аванс мог
            # измениться (новый топап от ГД / зачёт РП в ЗП по счёту).
            calc = await self.get_rp_oklad_advance_offset(int(rp_id))
            payout = float(calc["payout"])
            body = float(calc["body"])
            cur = await self.conn.execute(
                "INSERT INTO op_company_entries "
                "(year, month, date_iso, date_display, cashless_amount, description, "
                " source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'manual_bot_entry', ?)",
                (int(year), int(month), date_iso, date_display, payout, description, now_iso),
            )
            entry_id = int(cur.lastrowid)
            offset_req_id: int | None = None
            if body > 0:
                cur = await self.conn.execute(
                    "INSERT INTO installer_advance_requests "
                    "(installer_id, total_amount, status, comment, requested_at, paid_at, "
                    " paid_by, initiator, request_type, wallet_role) "
                    "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'gd', ?, 'rp')",
                    (int(rp_id), body, offset_comment, now_iso, now_iso,
                     int(actor_id), RP_OKLAD_OFFSET_TYPE),
                )
                offset_req_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        result = {
            "entry_id": entry_id,
            "offset_req_id": offset_req_id,
            "payout": payout,
            "deduct": float(calc["deduct"]),
            "body": body,
            "raw": float(calc["raw"]),
            "carry": float(calc["carry"]),
        }
        try:
            await self.audit(
                actor_id=int(actor_id),
                action="rp_salary_paid_with_advance_offset",
                entity="op_company_entries",
                entity_id=str(entry_id),
                payload={"rp_id": int(rp_id), "month": month_str, **result},
            )
        except Exception:
            log.exception("record_rp_salary_payment: audit failed entry_id=%s", entry_id)
        return result

    async def credit_rp_oklad_to_advance(
        self, rp_id: int, year: int, month: int, amount: float,
        month_str: str, date_display: str,
    ) -> tuple[int, int, float]:
        """РП переводит оклад (60К/мес или часть) за (year, month) в кошелёк аванса (A2).

        Атомарно (паттерн credit_rp_zp_to_advance): в ОДНОЙ транзакции —
          (1) расход компании op_company_entries(cashless_amount=amount,
              description=RP_OKLAD_ADVANCE_DESC) → рендер «Баланс компании» E='ЗП РП
              Нижельченко', C=сумма; списывает amount с баланса компании;
          (2) ОДИН topup кошелька аванса installer_advance_requests
              (request_type='request', wallet_role='rp', initiator='employee', без чека).
        Внутри транзакции — повторная проверка (анти-гонка/двойной клик):
          gd_paid → OkladAlreadyPaidError; amount > остатка → OkladAmountExceedsRemainingError.
        НЕ использует add_op_company_entry (тот коммитит сам — разрыв атомарности).
        Возвращает (entry_id, req_id, remaining_after). audit после commit.
        """
        amount = round(float(amount), 2)
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        now_iso = to_iso(utcnow())
        date_iso = datetime.strptime(date_display, "%d.%m.%Y").strftime("%Y-%m-%d")
        try:
            # повторная проверка взаимоисключения ВНУТРИ транзакции (race/double-click guard)
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM op_company_entries "
                "WHERE year = ? AND month = ? AND description LIKE 'Оклад РП%'",
                (int(year), int(month)),
            )
            if int((await cur.fetchone())[0] or 0) > 0:
                raise OkladAlreadyPaidError()
            cur = await self.conn.execute(
                "SELECT COALESCE(SUM(cashless_amount), 0) FROM op_company_entries "
                "WHERE year = ? AND month = ? AND description = ?",
                (int(year), int(month), RP_OKLAD_ADVANCE_DESC),
            )
            to_advance = float((await cur.fetchone())[0] or 0)
            remaining = max(0.0, float(RP_SALARY_MONTHLY) - to_advance)
            if amount > remaining + 1e-6:
                raise OkladAmountExceedsRemainingError(remaining)
            # (1) расход компании = маркер «оклад в аванс» (кол. E БК)
            cur = await self.conn.execute(
                "INSERT INTO op_company_entries "
                "(year, month, date_iso, date_display, cashless_amount, description, "
                " source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'manual_bot_entry', ?)",
                (int(year), int(month), date_iso, date_display, amount,
                 RP_OKLAD_ADVANCE_DESC, now_iso),
            )
            entry_id = int(cur.lastrowid)
            # (2) topup кошелька аванса РП (та же схема, что A1 credit_rp_zp_to_advance)
            cur = await self.conn.execute(
                "INSERT INTO installer_advance_requests "
                "(installer_id, total_amount, status, comment, requested_at, paid_at, "
                " paid_by, initiator, request_type, wallet_role) "
                "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'request', 'rp')",
                (rp_id, amount, f"Оклад {month_str} в аванс", now_iso, now_iso, rp_id),
            )
            req_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        remaining_after = round(remaining - amount, 2)
        try:
            await self.audit(
                actor_id=rp_id,
                action="rp_oklad_to_advance",
                entity="op_company_entries",
                entity_id=str(entry_id),
                payload={"req_id": req_id, "amount": amount, "month": month_str,
                         "remaining_after": remaining_after},
            )
        except Exception:
            log.exception("credit_rp_oklad_to_advance: audit failed entry=%s", entry_id)
        return (entry_id, req_id, remaining_after)

    async def record_rp_oklad_received(
        self, rp_id: int, year: int, month: int, amount: float,
        month_str: str, date_display: str, rp_label: str,
        pp_file_id: str | None = None, pp_file_type: str | None = None,
    ) -> tuple[int, float]:
        """РП фиксирует ФАКТ получения оклада за (year, month) (user 2026-06-14).

        Эффект = «как выплата ГД» (выбор user 14.06): в ОДНОЙ транзакции INSERT
        op_company_entries(cashless_amount=amount, description=f'Оклад РП {rp_label}
        {month_str}') — тот же маркер, что ГД-выплата (td.py rp_salary_confirm),
        поэтому месяц АВТОМАТИЧЕСКИ закрывается: get_rp_oklad_advance_status.gd_paid
        станет True (LIKE 'Оклад РП%') → b5-запрос и перевод-в-аванс блокируются.

        amount = остаток (66000 − уже переведённое в аванс): итог по месяцу = 66000
        без двойного учёта (часть могла уйти в аванс через credit_rp_oklad_to_advance).
        В ОТЛИЧИЕ от credit_rp_oklad_to_advance — БЕЗ topup кошелька аванса (РП лишь
        отмечает факт получения, деньги пришли вне бота).

        Внутри транзакции — повторная проверка (анти-гонка/двойной клик, как A2):
          gd_paid → OkladAlreadyPaidError; amount > остатка → OkladAmountExceedsRemainingError.
        НЕ использует add_op_company_entry (тот коммитит сам — разрыв атомарности).
        pp_file_id/pp_file_type — опц. платёжка, кладётся в audit (трассируемость).
        Возвращает (entry_id, remaining_after=0.0). audit после commit.
        """
        amount = round(float(amount), 2)
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        now_iso = to_iso(utcnow())
        date_iso = datetime.strptime(date_display, "%d.%m.%Y").strftime("%Y-%m-%d")
        description = f"Оклад РП {rp_label} {month_str}"
        try:
            # повторная проверка взаимоисключения ВНУТРИ транзакции (race/double-click guard)
            cur = await self.conn.execute(
                "SELECT COUNT(*) FROM op_company_entries "
                "WHERE year = ? AND month = ? AND description LIKE 'Оклад РП%'",
                (int(year), int(month)),
            )
            if int((await cur.fetchone())[0] or 0) > 0:
                raise OkladAlreadyPaidError()
            cur = await self.conn.execute(
                "SELECT COALESCE(SUM(cashless_amount), 0) FROM op_company_entries "
                "WHERE year = ? AND month = ? AND description = ?",
                (int(year), int(month), RP_OKLAD_ADVANCE_DESC),
            )
            to_advance = float((await cur.fetchone())[0] or 0)
            remaining = max(0.0, float(RP_SALARY_MONTHLY) - to_advance)
            if amount > remaining + 1e-6:
                raise OkladAmountExceedsRemainingError(remaining)
            # расход компании = маркер «оклад выплачен» (LIKE 'Оклад РП%' → gd_paid)
            cur = await self.conn.execute(
                "INSERT INTO op_company_entries "
                "(year, month, date_iso, date_display, cashless_amount, description, "
                " source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'manual_bot_entry', ?)",
                (int(year), int(month), date_iso, date_display, amount,
                 description, now_iso),
            )
            entry_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        try:
            await self.audit(
                actor_id=rp_id,
                action="rp_oklad_received",
                entity="op_company_entries",
                entity_id=str(entry_id),
                payload={"amount": amount, "month": month_str,
                         "pp_file_id": pp_file_id, "pp_file_type": pp_file_type},
            )
        except Exception:
            log.exception("record_rp_oklad_received: audit failed entry=%s", entry_id)
        return (entry_id, 0.0)

    async def list_rp_advance_distribute_invoices(self) -> list[dict[str, Any]]:
        """Счета для РАСПРЕДЕЛЕНИЯ аванса РП: непокрытая ЗП-10% (npn) (user 2026-06-13).

        Кандидат: npn_amount(AP)>0, НЕ выплачено ГД (rp_payout_op/AR=0), нет запроса
        ГД-выплаты (rp_request_op/AQ=0), НЕ забрано в аванс целиком (rp_payout_advance_at
        пусто). Остаток 10% = npn − уже зачтённое из аванса РП (Σ rp-offset по счёту);
        возвращаем только счета с остатком > 0. Тот же rp-offset, что исключает счёт
        из налива (list_rp_advance_fill_invoices) и ГД-выплаты
        (_list_rp_zp_eligible_invoices) — одна 10%-ЗП учитывается один раз. read-only.
        """
        cur = await self.conn.execute(
            "SELECT i.id, i.invoice_number, i.object_address, i.status, "
            "       COALESCE(i.npn_amount, 0) AS npn_amount, "
            "       COALESCE(("
            "         SELECT SUM(it.offset_amount) FROM installer_advance_items it "
            "         JOIN installer_advance_requests r ON r.id = it.request_id "
            "         WHERE it.invoice_id = i.id AND r.wallet_role = 'rp' "
            "           AND it.offset_zp_id IS NOT NULL), 0) AS taken_rp "
            "FROM invoices i "
            "WHERE COALESCE(i.npn_amount, 0) > 0 "
            "  AND COALESCE(i.rp_payout_op, 0) = 0 "
            "  AND COALESCE(i.rp_request_op, 0) = 0 "
            "  AND i.rp_payout_advance_at IS NULL "
            "ORDER BY i.receipt_date DESC, i.id DESC"
        )
        out: list[dict[str, Any]] = []
        for r in await cur.fetchall():
            d = dict(r)
            d["remaining"] = max(0.0, float(d["npn_amount"] or 0) - float(d["taken_rp"] or 0))
            if d["remaining"] > 0.005:
                out.append(d)
        return out

    async def apply_rp_advance_to_invoice_now(
        self, rp_id: int, invoice_id: int, amount: float, actor_id: int,
    ) -> dict[str, Any]:
        """РП зачитывает часть кошелька аванса в ЗП-10% (npn) счёта НЕМЕДЛЕННО.

        Зеркало apply_advance_to_invoice_now (монтажник, 03.06) для роли РП (user
        2026-06-13, «как у менеджера, но зачёт сразу» — у РП нет шага одобрения ЗП):
          - создаёт СРАЗУ ЗАКРЫТЫЙ advance-item (offset_zp_id=invoice_id,
            offset_amount=amount, offset_at=now); родитель — последний paid topup
            кошелька РП (request_type='request', wallet_role='rp') → баланс аванса
            −= amount немедленно (get_advance_balance вычитает offset-item);
          - «ЗП-10%» счёта = npn_amount(AP). Наличие rp-offset исключает счёт из
            налива и ГД-выплаты — одна 10%-ЗП учитывается один раз.
        rp_payout_advance_at НЕ трогаем (это маркер НАЛИВА; источник истины здесь —
        сам offset-item). Cap: amount ≤ свободный аванс РП И ≤ (npn − зачтённое).
        Возвращает {applied, total_applied, remaining, full_closed}.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        cur = await self.conn.execute(
            "SELECT COALESCE(npn_amount, 0) AS npn, COALESCE(rp_payout_op, 0) AS ar, "
            "       COALESCE(rp_request_op, 0) AS req, rp_payout_advance_at AS adv "
            "FROM invoices WHERE id = ?",
            (int(invoice_id),),
        )
        inv = await cur.fetchone()
        if not inv:
            raise RuntimeError(f"invoice id={invoice_id} not found")
        npn = float(inv["npn"] or 0)
        if npn <= 0:
            raise RuntimeError(f"счёт {invoice_id}: нет ЗП РП 10% (npn_amount=0)")
        if float(inv["ar"] or 0) != 0 or float(inv["req"] or 0) != 0 or inv["adv"] is not None:
            raise RuntimeError(
                f"счёт {invoice_id}: 10% уже выплачено/запрошено ГД или забрано в аванс",
            )
        # Свободный (нераспределённый) аванс кошелька РП.
        unalloc = await self.get_advance_outstanding_unallocated(rp_id, "rp")
        if amount > unalloc + 0.001:
            raise ValueError(f"amount={amount} > свободный аванс={unalloc}")
        # Уже зачтённое из аванса РП по этому счёту (rp-offset).
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(it.offset_amount), 0) FROM installer_advance_items it "
            "JOIN installer_advance_requests r ON r.id = it.request_id "
            "WHERE it.invoice_id = ? AND r.wallet_role = 'rp' "
            "  AND it.offset_zp_id IS NOT NULL",
            (int(invoice_id),),
        )
        taken_rp = float((await cur.fetchone())[0] or 0)
        remaining_before = npn - taken_rp
        if amount > remaining_before + 0.001:
            raise ValueError(
                f"amount={amount} > остаток 10%={remaining_before} "
                f"(npn={npn}, зачтено={taken_rp})",
            )
        # Родитель — последний оплаченный topup типа 'request' кошелька РП.
        cur = await self.conn.execute(
            "SELECT id FROM installer_advance_requests "
            "WHERE installer_id = ? AND status = 'paid' AND request_type = 'request' "
            "  AND wallet_role = 'rp' ORDER BY paid_at DESC, id DESC LIMIT 1",
            (rp_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("нет оплаченного пополнения кошелька аванса РП")
        req_id = int(row[0])
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount, plan_zp_snapshot, "
            " offset_zp_id, offset_at, offset_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (req_id, int(invoice_id), amount, npn, int(invoice_id), now, amount),
        )
        item_id = int(cur.lastrowid)
        await self.conn.commit()
        total_applied = taken_rp + amount
        full_closed = total_applied >= npn - 0.001
        await self.audit(
            actor_id=actor_id,
            action="rp_advance_applied_now",
            entity="advance_item",
            entity_id=str(item_id),
            payload={"request_id": req_id, "invoice_id": int(invoice_id),
                     "amount": amount, "npn": npn, "total_applied": total_applied,
                     "full_closed": full_closed},
        )
        return {
            "applied": amount,
            "total_applied": total_applied,
            "remaining": max(0.0, npn - total_applied),
            "full_closed": full_closed,
        }

    async def list_installer_advance_fill_invoices(
        self, installer_id: int,
    ) -> list[dict[str, Any]]:
        """Счета для наполнения кошелька аванса монтажника согласованной ЗП (BJ).

        Зеркало list_rp_advance_fill_invoices. Критерий — тот же набор, что
        «🔨 Ожидаемая ЗП» / кандидаты распределения: assigned_to=монтажник,
        montazh_agreed_amount(BJ)>0, zp_installer_status ∈ (not_requested/requested/
        approved) = ЗП ещё не выплачена и «Монтаж Факт»(BS) пуст, не детский счёт,
        статус активный. На экране наполнения показываем адрес (J) + сумму ЗП (BJ),
        чтобы монтажник выбрал, что перевести в кошелёк аванса. read-only.
        """
        cur = await self.conn.execute(
            "SELECT id, invoice_number, object_address, status, "
            "       COALESCE(montazh_agreed_amount, 0) AS agreed "
            "FROM invoices "
            "WHERE assigned_to = ? AND COALESCE(montazh_agreed_amount, 0) > 0 "
            "  AND COALESCE(zp_installer_status, 'not_requested') "
            "      IN ('not_requested', 'requested', 'approved') "
            "  AND parent_invoice_id IS NULL "
            "  AND status IN ('in_progress', 'paid', 'credit', 'ended') "
            # Защита от двойного учёта ЗП: исключаем счета, по которым уже есть
            # ЛЮБАЯ позиция аванса из не-отклонённой заявки — открытый earmark
            # «Распределить аванс» (offset_zp_id IS NULL) ИЛИ закрытая позиция от
            # частичного «применить аванс сейчас» (apply_advance_to_invoice_now,
            # offset проставлен). Налив берёт ПОЛНУЮ agreed-сумму и не сверяется с
            # уже взятым → иначе ЗП по счёту учлась бы дважды. Такие счета монтажник
            # доводит обычным путём (распределение / ЗП-flow), не наливом.
            "  AND id NOT IN ("
            "      SELECT it.invoice_id FROM installer_advance_items it "
            "      JOIN installer_advance_requests rq ON rq.id = it.request_id "
            "      WHERE it.invoice_id IS NOT NULL "
            "        AND COALESCE(rq.status, '') NOT IN ('rejected', 'cancelled')) "
            "ORDER BY receipt_date DESC, id DESC",
            (installer_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def credit_installer_zp_to_advance(
        self, installer_id: int, invoice_ids: list[int],
    ) -> tuple[int, float, list[dict[str, Any]]]:
        """Монтажник забирает согласованную монтажную ЗП (BJ) по выбранным счетам
        в кошелёк аванса (целиком по счёту).

        Зеркало credit_rp_zp_to_advance. Для каждого счёта (assigned_to=монтажник,
        montazh_agreed_amount>0, zp_installer_status ∈ not_requested/requested/approved,
        иначе пропуск — идемпотентно): zp_installer_status:='confirmed',
        zp_installer_amount:=montazh_agreed_amount, zp_installer_confirmed_at:=now.
        Помечает ЗП выданной (как обычная выплата) → «Монтаж Факт»(BS) ветка-3 =
        montazh_agreed; счёт уходит из «Ожидаемой ЗП» и из кандидатов распределения
        (одна ЗП учитывается ОДИН раз). Σ сумм → ОДИН topup кошелька
        (request_type='request', wallet_role=NULL, initiator='employee', без чека —
        это собственная ЗП, не внешний взнос ГД). Атомарно: все UPDATE invoices +
        INSERT topup в одной транзакции (один commit, rollback при сбое). Возвращает
        (request_id, total, credited[{invoice_id, invoice_number, amount}]).
        sync_invoice_row вызывает хендлер (нужен IntegrationHub).
        """
        if not invoice_ids:
            return (0, 0.0, [])
        now_iso = to_iso(utcnow())
        credited: list[dict[str, Any]] = []
        total = 0.0
        req_id = 0
        try:
            for inv_id in invoice_ids:
                cur = await self.conn.execute(
                    # Объединение платежей (owner 15.07): монтажнику причитается ДОПЛАТА —
                    # Согласовано минус выплаченное прошлым группам, иначе он нальёт в
                    # кошелёк всю объединённую сумму. paid_prev=0 → как раньше.
                    "SELECT invoice_number, COALESCE(assigned_to, 0) AS assigned_to, "
                    "       COALESCE(montazh_agreed_amount, 0) "
                    "         - COALESCE(montazh_paid_prev, 0) AS agreed, "
                    "       COALESCE(zp_installer_status, 'not_requested') AS zst "
                    "FROM invoices WHERE id = ?",
                    (int(inv_id),),
                )
                row = await cur.fetchone()
                if not row:
                    continue
                agreed = float(row["agreed"] or 0)
                if (int(row["assigned_to"] or 0) != int(installer_id)
                        or agreed <= 0
                        or str(row["zst"]) not in ("not_requested", "requested", "approved")):
                    continue  # чужой счёт / нет суммы / уже выплачено — пропуск
                # Guard от двойного учёта ЗП: счёт, по которому есть ЛЮБАЯ позиция
                # аванса из не-отклонённой заявки (открытый earmark распределения
                # ИЛИ закрытая позиция частичного apply) — пропуск. Налив берёт
                # полную agreed и не сверяется с уже взятым (зеркало фильтра
                # list_installer_advance_fill_invoices, защита от устаревшего списка).
                curx = await self.conn.execute(
                    "SELECT 1 FROM installer_advance_items it "
                    "JOIN installer_advance_requests rq ON rq.id = it.request_id "
                    "WHERE it.invoice_id = ? "
                    "  AND COALESCE(rq.status, '') NOT IN ('rejected', 'cancelled') LIMIT 1",
                    (int(inv_id),),
                )
                if await curx.fetchone():
                    continue  # есть открытое распределение по счёту — пропуск
                await self.conn.execute(
                    "UPDATE invoices SET zp_installer_status = 'confirmed', "
                    "zp_installer_amount = ?, zp_installer_confirmed_at = ?, "
                    "updated_at = ? WHERE id = ?",
                    (agreed, now_iso, now_iso, int(inv_id)),
                )
                credited.append({
                    "invoice_id": int(inv_id),
                    "invoice_number": row["invoice_number"],
                    "amount": agreed,
                })
                total += agreed
            if not credited:
                return (0, 0.0, [])
            nums = ", ".join(f"№{c['invoice_number']}" for c in credited)
            cur = await self.conn.execute(
                "INSERT INTO installer_advance_requests "
                "(installer_id, total_amount, status, comment, requested_at, paid_at, "
                " paid_by, initiator, request_type, wallet_role) "
                "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'request', NULL)",
                (installer_id, total, f"ЗП монтаж в аванс: {nums}", now_iso, now_iso, installer_id),
            )
            req_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        try:
            await self.audit(
                actor_id=installer_id,
                action="installer_zp_to_advance",
                entity="advance_request",
                entity_id=str(req_id),
                payload={"total": total, "invoices": credited},
            )
        except Exception:
            log.exception("credit_installer_zp_to_advance: audit failed req_id=%s", req_id)
        return (req_id, total, credited)

    async def list_manager_advance_fill_invoices(
        self, role_key: str, manager_id: int,
    ) -> list[dict[str, Any]]:
        """Счета для наполнения кошелька аванса менеджера незабранной ЗП (AJ/
        manager_zp_blank). Зеркало list_installer_advance_fill_invoices, импорт-
        безопасный вариант для менеджеров КВ/КИА/НПН (TZ 06.06).

        Критерий — тот же набор, что display-сводка get_unpaid_zp_for_summary
        (role_key=manager_*): creator_role=role_key, manager_zp_blank(AJ)>0, ЗП ещё
        НЕ в пайплайне выплаты / не забрана (zp_manager_status ∉ approved/
        payment_sent/confirmed) И не выплачена через ОП (zp_manager_payout/AN=0),
        не детский счёт, статус активный. На экране — адрес (J) + сумма ЗП
        (manager_zp_blank), целиком по счёту.

        Защита от двойного учёта: исключаем счета, по которым у ЭТОГО менеджера уже
        есть позиция «Распределить аванс» (installer_advance_items под его не-
        отклонённой заявкой). Фильтр по rq.installer_id=manager_id, чтобы монтажные
        распределения по тому же счёту НЕ блокировали менеджерский налив. read-only.
        """
        cur = await self.conn.execute(
            "SELECT id, invoice_number, object_address, status, "
            "       MAX(0, COALESCE(manager_zp_blank, 0) "
            "              + CASE WHEN ABS(COALESCE(outstanding_debt, 0)) < 1 "
            "                     THEN COALESCE(zp_manager_hold, 0) ELSE 0 END) AS amount "
            "FROM invoices "
            "WHERE creator_role = ? AND COALESCE(manager_zp_blank, 0) > 0 "
            "  AND COALESCE(zp_manager_payout, 0) = 0 "
            "  AND COALESCE(zp_manager_status, 'not_requested') "
            "      NOT IN ('approved', 'payment_sent', 'confirmed') "
            "  AND parent_invoice_id IS NULL "
            "  AND status IN ('in_progress', 'paid', 'credit', 'ended') "
            "  AND id NOT IN ("
            "      SELECT it.invoice_id FROM installer_advance_items it "
            "      JOIN installer_advance_requests rq ON rq.id = it.request_id "
            "      WHERE it.invoice_id IS NOT NULL "
            "        AND rq.installer_id = ? "
            "        AND COALESCE(rq.status, '') NOT IN ('rejected', 'cancelled')) "
            "ORDER BY receipt_date DESC, id DESC",
            (role_key, manager_id),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def credit_manager_zp_to_advance(
        self, manager_id: int, role_key: str, wallet_role: str | None,
        invoice_ids: list[int],
    ) -> tuple[int, float, list[dict[str, Any]]]:
        """Менеджер забирает незабранную ЗП (manager_zp_blank/AJ) по выбранным
        счетам в кошелёк аванса (целиком по счёту). Импорт-безопасное зеркало
        credit_installer_zp_to_advance (TZ 06.06).

        Для каждого счёта (creator_role=role_key, manager_zp_blank>0,
        zp_manager_status ∉ approved/payment_sent/confirmed, нет открытой позиции
        распределения этого менеджера — иначе пропуск, идемпотентно):
        zp_manager_status:='confirmed', zp_manager_amount:=manager_zp_blank,
        zp_manager_approved_at/by:=now/manager. Пометка БОТ-ПОЛЕМ, НЕ
        zp_manager_payout(AN): AN парсится из «Импорт ОП» (sheets._OP_COL_MAP) и
        затиралось бы реимпортом → колонка выплаты ОП остаётся чистой, аванс идёт
        в журнал «Авансирование сотрудников» (feedback_advance_deposit_journal_only).
        Статус 'confirmed' — durable-guard: даже если ОП проставит AN, счёт не
        вернётся в кандидаты (одна ЗП учитывается ОДИН раз).

        Σ сумм → ОДИН topup кошелька (request_type='request', initiator='employee',
        без чека; wallet_role: НПН='manager_npn' [отделён от кошелька РП Павла],
        КВ/КИА=NULL [single-role]). Атомарно: все UPDATE invoices + INSERT topup в
        одной транзакции (один commit, rollback при сбое). Возвращает (request_id,
        total, credited[{invoice_id, invoice_number, amount}]). sync_invoice_row +
        sync_advances_journal вызывает хендлер.
        """
        if not invoice_ids:
            return (0, 0.0, [])
        now_iso = to_iso(utcnow())
        credited: list[dict[str, Any]] = []
        total = 0.0
        req_id = 0
        try:
            for inv_id in invoice_ids:
                cur = await self.conn.execute(
                    "SELECT invoice_number, COALESCE(creator_role, '') AS cr, "
                    "       COALESCE(manager_zp_blank, 0) AS blank, "
                    "       COALESCE(zp_manager_hold, 0) AS hold, "
                    "       COALESCE(outstanding_debt, 0) AS debt, "
                    "       COALESCE(zp_manager_status, 'not_requested') AS zst "
                    "FROM invoices WHERE id = ?",
                    (int(inv_id),),
                )
                row = await cur.fetchone()
                if not row:
                    continue
                blank = float(row["blank"] or 0)
                # net выплаты: удержание (CN/hold) применяется только при погашенном
                # долге (механизм перерасчёта, owner 23.06); флор 0 — нельзя забрать
                # минус. Пока есть долг — забирается полный бланк (без удержания).
                if abs(float(row["debt"] or 0)) < 1:
                    net = max(0.0, blank + float(row["hold"] or 0))
                else:
                    net = blank
                if (str(row["cr"]) != role_key
                        or net <= 0
                        or str(row["zst"]) in ("approved", "payment_sent", "confirmed")):
                    continue  # чужой бренд / нечего забирать (с уч. удержания) / уже забрано
                # Guard двойного учёта: открытая позиция «Распределить аванс» этого
                # менеджера по счёту (зеркало фильтра list_manager_advance_fill_invoices,
                # защита от устаревшего списка). installer_id=manager_id — чтобы
                # монтажные распределения по тому же счёту не мешали.
                curx = await self.conn.execute(
                    "SELECT 1 FROM installer_advance_items it "
                    "JOIN installer_advance_requests rq ON rq.id = it.request_id "
                    "WHERE it.invoice_id = ? AND rq.installer_id = ? "
                    "  AND COALESCE(rq.status, '') NOT IN ('rejected', 'cancelled') LIMIT 1",
                    (int(inv_id), int(manager_id)),
                )
                if await curx.fetchone():
                    continue  # есть открытое распределение по счёту — пропуск
                await self.conn.execute(
                    "UPDATE invoices SET zp_manager_status = 'confirmed', "
                    "zp_manager_amount = ?, zp_manager_approved_at = ?, "
                    "zp_manager_approved_by = ?, updated_at = ? WHERE id = ?",
                    (net, now_iso, int(manager_id), now_iso, int(inv_id)),
                )
                credited.append({
                    "invoice_id": int(inv_id),
                    "invoice_number": row["invoice_number"],
                    "amount": net,
                })
                total += net
            if not credited:
                return (0, 0.0, [])
            nums = ", ".join(f"№{c['invoice_number']}" for c in credited)
            cur = await self.conn.execute(
                "INSERT INTO installer_advance_requests "
                "(installer_id, total_amount, status, comment, requested_at, paid_at, "
                " paid_by, initiator, request_type, wallet_role) "
                "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'request', ?)",
                (manager_id, total, f"ЗП менеджер в аванс: {nums}", now_iso, now_iso,
                 manager_id, wallet_role),
            )
            req_id = int(cur.lastrowid)
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        try:
            await self.audit(
                actor_id=manager_id,
                action="manager_zp_to_advance",
                entity="advance_request",
                entity_id=str(req_id),
                payload={"total": total, "invoices": credited,
                         "role_key": role_key, "wallet_role": wallet_role},
            )
        except Exception:
            log.exception("credit_manager_zp_to_advance: audit failed req_id=%s", req_id)
        return (req_id, total, credited)

    async def sweep_manager_overpay_to_advance(self) -> list[dict[str, Any]]:
        """Перенос ПЕРЕПЛАТЫ ЗП менеджера на баланс аванса (owner 2026-06-23).

        Для каждого менеджера общая сумма удержаний (|zp_manager_hold|/CN) по его
        счетам под механизмом (CN≠0 И долг погашен, outstanding_debt==0) переносится
        ОДНОЙ строкой на баланс аванса (installer_advance_requests, request_type=
        'request', status='paid' → +get_advance_balance/+unallocated). Менеджер далее
        САМ распределяет этот аванс по объектам (кнопка «Распределение аванса»), и он
        гасится из ЗП по правилам авансирования (apply_advance_offsets_on_zp_approve).

        Идемпотентность: на каждом счёте трекается zp_hold_advanced — сколько |CN| уже
        перенесено. Переносится ТОЛЬКО дельта (|CN| − zp_hold_advanced > 0). Поле
        БОТ-локальное, реимпорт ОП его НЕ затирает (в отличие от zp_manager_hold/CN) →
        повторный синк НЕ дублирует. Cap распределения аванса (manager_zp_net_payout,
        helper) после переноса возвращает перенесённую часть через +zp_hold_advanced
        → менеджер может распределить переплату по объектам на полный бланк ЗП.
        Дашборд «Невыплаченная ЗП» и take-to-advance (B2/B3/B4) остаются net (residual)
        — без сюрприза. Двойного счёта нет: переплата на балансе аванса (распределяется
        и гасит ЗП) + остаток net (take-to-advance) = полный бланк.

        Атомарно по каждому менеджеру (топап + UPDATE счетов в одной транзакции).
        wallet_role: 'manager_npn' если у менеджера есть роль 'rp' (Павел, раздельные
        кошельки), иначе NULL (КВ/КИА). Зеркало _mgr_wallet_role. Возвращает список
        [{manager_id, role_key, wallet_role, req_id, total, invoices:[…]}] для журнала.
        """
        cur = await self.conn.execute(
            "SELECT id, invoice_number, created_by, COALESCE(creator_role, '') AS cr, "
            "       ABS(COALESCE(zp_manager_hold, 0)) AS cn_abs, "
            "       COALESCE(zp_hold_advanced, 0) AS advanced "
            "FROM invoices "
            "WHERE COALESCE(zp_manager_hold, 0) != 0 "
            "  AND ABS(COALESCE(outstanding_debt, 0)) < 1 "
            "  AND parent_invoice_id IS NULL "
            "  AND COALESCE(status, '') != 'rejected' "
            "  AND created_by IS NOT NULL "
            "ORDER BY created_by, id"
        )
        rows = await cur.fetchall()
        # Группировка непокрытых дельт по менеджеру (created_by, creator_role).
        by_mgr: dict[tuple[int, str], list[dict[str, Any]]] = {}
        for r in rows:
            delta = float(r["cn_abs"] or 0) - float(r["advanced"] or 0)
            if delta <= 0.009:
                continue  # уже перенесено целиком (идемпотентность)
            key = (int(r["created_by"]), str(r["cr"]))
            by_mgr.setdefault(key, []).append({
                "invoice_id": int(r["id"]),
                "invoice_number": r["invoice_number"],
                "delta": delta,
            })
        results: list[dict[str, Any]] = []
        if not by_mgr:
            return results
        now_iso = to_iso(utcnow())
        for (manager_id, role_key), invs in by_mgr.items():
            total = round(sum(float(i["delta"]) for i in invs), 2)
            if total <= 0:
                continue
            # wallet_role: Павел (rp+менеджер) → 'manager_npn' (раздельные кошельки),
            # иначе NULL (single-role КВ/КИА). Зеркало _mgr_wallet_role (handlers).
            cur = await self.conn.execute(
                "SELECT COALESCE(role, '') FROM users WHERE telegram_id = ?",
                (manager_id,),
            )
            urow = await cur.fetchone()
            role_str = str(urow[0] if urow else "").lower()
            roles = [x.strip() for x in role_str.split(",")]
            wallet_role = "manager_npn" if "rp" in roles else None
            nums = ", ".join(f"№{i['invoice_number']}" for i in invs)
            inv_payload = [{"invoice_id": i["invoice_id"],
                            "invoice_number": i["invoice_number"],
                            "amount": round(float(i["delta"]), 2)} for i in invs]
            try:
                cur = await self.conn.execute(
                    "INSERT INTO installer_advance_requests "
                    "(installer_id, total_amount, status, comment, requested_at, "
                    " paid_at, paid_by, initiator, request_type, wallet_role) "
                    "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'request', ?)",
                    (manager_id, total, f"Переплата ЗП в аванс: {nums}", now_iso,
                     now_iso, manager_id, wallet_role),
                )
                req_id = int(cur.lastrowid)
                for i in invs:
                    await self.conn.execute(
                        "UPDATE invoices SET "
                        "zp_hold_advanced = COALESCE(zp_hold_advanced, 0) + ?, "
                        "zp_hold_advanced_at = ?, "
                        "updated_at = ? WHERE id = ?",
                        (round(float(i["delta"]), 2), now_iso, now_iso,
                         i["invoice_id"]),
                    )
                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                log.exception(
                    "sweep_manager_overpay_to_advance: failed manager_id=%s", manager_id,
                )
                continue
            try:
                await self.audit(
                    actor_id=manager_id,
                    action="manager_overpay_to_advance",
                    entity="advance_request",
                    entity_id=str(req_id),
                    payload={"total": total, "role_key": role_key,
                             "wallet_role": wallet_role, "invoices": inv_payload},
                )
            except Exception:
                log.exception(
                    "sweep_manager_overpay_to_advance: audit failed req_id=%s", req_id,
                )
            results.append({
                "manager_id": manager_id, "role_key": role_key,
                "wallet_role": wallet_role, "req_id": req_id,
                "total": total, "invoices": inv_payload,
            })
        return results

    async def create_recalc_advance_topup(
        self,
        invoice_id: int,
        invoice_number: str,
        employee_id: int,
        amount: float,
        gd_id: int,
        wallet_role: str | None = None,
    ) -> int:
        """Согласие менеджера с перерасчётом → аванс + отметка на счёте (30.07).

        Ручной канал («📨 Отправить менеджеру» → «✅ С перерасчётом согласен»)
        раньше звал create_gd_advance_topup, который zp_hold_advanced НЕ пишет →
        свип на следующем синке ГД видел дельту |CN| − 0 и переносил ту же сумму
        ВТОРОЙ раз. Здесь INSERT аванса и UPDATE счёта идут ОДНОЙ транзакцией,
        как в sweep_manager_overpay_to_advance — оба канала пишут один и тот же
        трекер, поэтому дублей нет в любом порядке срабатывания.

        Аудит пишется тем же action, что и у свипа (manager_overpay_to_advance),
        чтобы журнал переносов был единым, а rollback_overpay_advance умел
        откатывать записи обоих каналов. Поле channel в payload различает их.
        """
        if amount <= 0:
            raise ValueError(f"Advance topup amount must be positive, got {amount}")
        now = to_iso(utcnow())
        try:
            cur = await self.conn.execute(
                "INSERT INTO installer_advance_requests "
                "(installer_id, total_amount, status, comment, requested_at, "
                " approved_at, approved_by, paid_at, paid_by, initiator, "
                " request_type, wallet_role) "
                "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, ?, 'gd', 'request', ?)",
                (employee_id, amount, f"Перерасчёт прибыли, счёт №{invoice_number}",
                 now, now, gd_id, now, gd_id, wallet_role),
            )
            req_id = int(cur.lastrowid)
            await self.conn.execute(
                "UPDATE invoices SET "
                "zp_hold_advanced = COALESCE(zp_hold_advanced, 0) + ?, "
                "zp_hold_advanced_at = ?, "
                "updated_at = ? WHERE id = ?",
                (round(float(amount), 2), now, now, invoice_id),
            )
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            raise
        await self.audit(
            actor_id=gd_id,
            action="manager_overpay_to_advance",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "total": round(float(amount), 2),
                "channel": "recalc_agree",
                "employee_id": employee_id,
                "wallet_role": wallet_role,
                "invoices": [{
                    "invoice_id": invoice_id,
                    "invoice_number": invoice_number,
                    "amount": round(float(amount), 2),
                }],
            },
        )
        return req_id

    async def rollback_overpay_advance(self, request_id: int) -> list[dict[str, Any]]:
        """Откат отметки zp_hold_advanced при отклонении переноса переплаты (30.07).

        reject_advance_request раньше трогал только строку аванса — счета оставались
        помеченными как «переплата перенесена», а manager_zp_net_payout из-за этого
        платил полный бланк, и удержание не применялось НИКОГДА. 27.07 при датафиксе
        (заявки #23/#24, съехавший лист ОП) откат делали руками; здесь он в коде.

        Связь «заявка → счета» берётся из payload аудита manager_overpay_to_advance
        (пишут оба канала: свип и create_recalc_advance_topup), а НЕ из текста
        комментария. Откат не уводит поле ниже нуля. Возвращает список отката."""
        cur = await self.conn.execute(
            "SELECT payload_json FROM audit_log "
            "WHERE action = 'manager_overpay_to_advance' AND entity = 'advance_request' "
            "  AND entity_id = ? ORDER BY id",
            (str(request_id),),
        )
        rows = await cur.fetchall()
        reverted: list[dict[str, Any]] = []
        for r in rows:
            try:
                payload = json.loads(r["payload_json"] or "{}")
            except (ValueError, TypeError):
                continue
            for item in payload.get("invoices") or []:
                try:
                    inv_id = int(item.get("invoice_id") or 0)
                    amt = round(float(item.get("amount") or 0), 2)
                except (TypeError, ValueError):
                    continue
                if inv_id <= 0 or amt <= 0:
                    continue
                reverted.append({
                    "invoice_id": inv_id,
                    "invoice_number": item.get("invoice_number"),
                    "amount": amt,
                })
        if not reverted:
            return reverted
        now = to_iso(utcnow())
        try:
            for item in reverted:
                await self.conn.execute(
                    # Дату гасим ТОЛЬКО когда откат обнулил сумму: иначе на листе
                    # осталась бы дата при пустой сумме. В одном UPDATE все SET
                    # читают СТАРОЕ значение строки, поэтому CASE считает по
                    # zp_hold_advanced ДО отката — как и нужно.
                    "UPDATE invoices SET "
                    "zp_hold_advanced = MAX(0, COALESCE(zp_hold_advanced, 0) - ?), "
                    "zp_hold_advanced_at = CASE "
                    "  WHEN MAX(0, COALESCE(zp_hold_advanced, 0) - ?) <= 0 THEN NULL "
                    "  ELSE zp_hold_advanced_at END, "
                    "updated_at = ? WHERE id = ?",
                    (item["amount"], item["amount"], now, item["invoice_id"]),
                )
            await self.conn.commit()
        except Exception:
            await self.conn.rollback()
            log.exception(
                "rollback_overpay_advance: failed request_id=%s", request_id,
            )
            raise
        return reverted

    async def create_gd_deposit(
        self,
        installer_id: int,
        amount: float,
        gd_id: int,
        payment_file_id: str | None = None,
        comment: str | None = None,
        wallet_role: str | None = None,
    ) -> tuple[int, list[int]]:
        """ГД-инициированный депозит на счёт монтажника.

        INSERT request со status='paid', initiator='gd', request_type='deposit'.
        Без items — депозит общий, без привязки к invoice. После INSERT —
        auto-offset на все approved-ZP invoices монтажника (если есть).

        payment_file_id ОПЦИОНАЛЕН: для ГД-вноса депозита чек не обязателен
        (ТЗ 30.05 — внутреннее пополнение от ГД, не внешний б/н-платёж).
        Пустая строка/None → NULL.
        """
        if amount <= 0:
            raise ValueError(f"Deposit amount must be positive, got {amount}")
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, "
            " approved_at, approved_by, paid_at, paid_by, payment_file_id, "
            " initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, ?, ?, 'gd', 'deposit', ?)",
            (installer_id, amount, comment, now, now, gd_id, now, gd_id, (payment_file_id or None), wallet_role),
        )
        req_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=gd_id,
            action="installer_advance_gd_deposit",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "installer_id": installer_id,
                "comment": comment,
                "payment_file_id": payment_file_id,
            },
        )
        # Auto-offset: для каждого approved ZP invoice монтажника — попытаться зачесть.
        cur = await self.conn.execute(
            "SELECT id, zp_installer_amount FROM invoices "
            "WHERE assigned_to=? AND zp_installer_status='approved' "
            "  AND COALESCE(zp_installer_amount, 0) > 0",
            (installer_id,),
        )
        invs = await cur.fetchall()
        offsets_applied: list[dict[str, Any]] = []
        for inv_row in invs:
            inv_id = int(inv_row[0])
            zp_amount = float(inv_row[1] or 0)
            remaining = await self.apply_advance_offsets_on_zp_approve(
                inv_id, zp_id=inv_id, zp_amount=zp_amount, actor_id=gd_id,
            )
            offsets_applied.append(
                {"invoice_id": inv_id, "zp_amount": zp_amount, "remaining": remaining},
            )
        if offsets_applied:
            await self.audit(
                actor_id=gd_id,
                action="installer_advance_gd_deposit_auto_offset",
                entity="advance_request",
                entity_id=str(req_id),
                payload={"applied": offsets_applied},
            )
        # Вернуть счета, по которым прошёл авто-зачёт — хендлер пересоберёт их строки
        # в листе Invoices (иначе CH/метрики/статус ЗП отстают, особенно у б/н).
        return req_id, [int(o["invoice_id"]) for o in offsets_applied]

    async def create_gd_advance(
        self,
        installer_id: int,
        invoice_id: int,
        amount: float,
        gd_id: int,
        payment_file_id: str,
        comment: str | None = None,
        role: str = "installer",
    ) -> int:
        """DEPRECATED после 25.05 funds-2balances. UI ГД больше не вызывает.

        ГД-инициированный аванс сотруднику (installer или manager) с привязкой к счёту.
        После split на 2 баланса ГД топит только баланс (create_gd_advance_topup), а
        привязку к счёту делает сам сотрудник (add_advance_item_for_distribution).
        Метод оставлен для backward-compat / admin-shortcut «ГД сам распределил».

        role='installer' (default): проверка assigned_to + auto-offset на zp_installer_*.
        role='manager': проверка created_by + auto-offset на zp_manager_*.

        Создаётся request (initiator='gd', status='paid', request_type='request') +
        1 item с invoice_id, offset_zp_id=NULL. При ZP approve этого invoice
        соответствующий hook вызовет apply_advance_offsets_on_zp_approve(role=...).
        Если ZP уже approved — offset сразу.

        payment_file_id обязателен (правило «б/н только с чеком»).
        """
        if amount <= 0:
            raise ValueError(f"Advance amount must be positive, got {amount}")
        if not payment_file_id:
            raise ValueError("payment_file_id (чек) обязателен для аванса")
        if role == "manager":
            ownership_col, zp_status_col, zp_amount_col = (
                "created_by", "zp_manager_status", "zp_manager_amount",
            )
        else:
            ownership_col, zp_status_col, zp_amount_col = (
                "assigned_to", "zp_installer_status", "zp_installer_amount",
            )
        cur = await self.conn.execute(
            f"SELECT id, {ownership_col}, {zp_status_col}, {zp_amount_col} "
            f"FROM invoices WHERE id=?",
            (invoice_id,),
        )
        inv_row = await cur.fetchone()
        if not inv_row:
            raise ValueError(f"Invoice {invoice_id} not found")
        if inv_row[1] != installer_id:
            raise ValueError(
                f"Invoice {invoice_id} {ownership_col}={inv_row[1]} "
                f"!= employee_id {installer_id}",
            )
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, "
            " approved_at, approved_by, paid_at, paid_by, payment_file_id, "
            " initiator, request_type) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, ?, ?, 'gd', 'request')",
            (installer_id, amount, comment, now, now, gd_id, now, gd_id, payment_file_id),
        )
        req_id = int(cur.lastrowid)
        await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount) "
            "VALUES (?, ?, ?)",
            (req_id, invoice_id, amount),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=gd_id,
            action="installer_advance_gd_advance",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "employee_id": installer_id,
                "role": role,
                "invoice_id": invoice_id,
                "comment": comment,
                "payment_file_id": payment_file_id,
            },
        )
        zp_status = inv_row[2]
        zp_amount = float(inv_row[3] or 0)
        if zp_status == "approved" and zp_amount > 0:
            await self.apply_advance_offsets_on_zp_approve(
                invoice_id, zp_id=invoice_id, zp_amount=zp_amount,
                actor_id=gd_id, role=role,
            )
        return req_id

    async def create_gd_advance_topup(
        self,
        employee_id: int,
        amount: float,
        gd_id: int,
        payment_file_id: str | None = None,
        comment: str | None = None,
        wallet_role: str | None = None,
    ) -> int:
        """ГД пополняет advance-баланс сотрудника (без привязки к счёту).

        После 25.05 funds-2balances ГД топит общий пул, а сотрудник сам потом
        выбирает счёт через create_employee_advance_distribute (или distribute UI).

        INSERT request со status='paid', initiator='gd', request_type='request',
        БЕЗ items. Items создаст сотрудник при distribute.

        payment_file_id ОПЦИОНАЛЕН: для ГД-вноса аванса чек не обязателен
        (ТЗ 30.05 — внутреннее пополнение кошелька от ГД, не внешний б/н-платёж).
        Пустая строка/None → NULL.
        """
        if amount <= 0:
            raise ValueError(f"Advance topup amount must be positive, got {amount}")
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, "
            " approved_at, approved_by, paid_at, paid_by, payment_file_id, "
            " initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, ?, ?, 'gd', 'request', ?)",
            (employee_id, amount, comment, now, now, gd_id, now, gd_id, (payment_file_id or None), wallet_role),
        )
        req_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=gd_id,
            action="installer_advance_gd_topup",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "employee_id": employee_id,
                "comment": comment,
                "payment_file_id": payment_file_id,
            },
        )
        return req_id

    async def create_employee_depo_to_adv_transfer(
        self,
        employee_id: int,
        amount: float,
        actor_id: int,
        comment: str | None = None,
        wallet_role: str | None = None,
    ) -> int:
        """Сотрудник переводит часть депозита на advance-баланс (односторонний).

        GUARD: deposit_balance >= amount, иначе ValueError.
        INSERT request_type='transfer_depo_to_adv', initiator='employee', status='paid'.

        В балансах: +amount к get_advance_balance, −amount от get_deposit_balance
        (обе формулы знают про этот request_type).
        """
        if amount <= 0:
            raise ValueError(f"Transfer amount must be positive, got {amount}")
        depo = await self.get_deposit_balance(employee_id, wallet_role)
        if amount > depo + 0.001:
            raise ValueError(
                f"Недостаточно средств на депозите: balance={depo:.2f} < {amount:.2f}",
            )
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, paid_at, "
            " paid_by, initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, 'employee', 'transfer_depo_to_adv', ?)",
            (employee_id, amount, comment, now, now, actor_id, wallet_role),
        )
        req_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_advance_depo_to_adv_transfer",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "employee_id": employee_id,
                "comment": comment,
                "deposit_balance_before": depo,
            },
        )
        return req_id

    async def create_installer_withdraw(
        self,
        installer_id: int,
        amount: float,
        comment: str,
        receipt_file_id: str | None = None,
        wallet_role: str | None = None,
    ) -> int:
        """Игорь снимает деньги с депозита на личные расходы.

        GUARD: deposit_balance >= amount, иначе ValueError.
        INSERT request со status='paid', initiator='installer', request_type='withdraw'.
        Без items (нет привязки к invoice).

        comment обязателен (на что потрачено). receipt_file_id опционален.
        """
        if amount <= 0:
            raise ValueError(f"Withdraw amount must be positive, got {amount}")
        if not (comment or "").strip():
            raise ValueError("comment обязателен (что куплено / куда потратил)")
        balance = await self.get_deposit_balance(installer_id, wallet_role)
        if amount > balance + 0.001:
            raise ValueError(
                f"Недостаточно средств на депозите: balance={balance:.2f} < {amount:.2f}",
            )
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, paid_at, paid_by, "
            " payment_file_id, initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, 'installer', 'withdraw', ?)",
            (installer_id, amount, comment, now, now, installer_id, receipt_file_id, wallet_role),
        )
        req_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=installer_id,
            action="installer_advance_withdraw",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "comment": comment,
                "receipt_file_id": receipt_file_id,
                "balance_after": max(0.0, balance - amount),
            },
        )
        return req_id

    async def create_gd_deposit_withdrawal(
        self,
        employee_id: int,
        amount: float,
        comment: str,
        gd_id: int,
        wallet_role: str | None = None,
        receipt_file_id: str | None = None,
    ) -> int:
        """Списание с депозита сотрудника по запросу ГД (ТЗ C 30.05).

        Вызывается ПОСЛЕ того, как сотрудник ПОДТВЕРДИЛ ИСПОЛНЕНИЕ входящей задачи
        GD_DEPOSIT_REQUEST (двухшаговый flow депозита 04.06). Депозит уменьшается
        только в этот момент (вариант B).

        GUARD: deposit_balance >= amount, иначе ValueError.
        INSERT request со status='paid', initiator='gd', request_type='withdraw'
        (та же семантика баланса, что и у обычного withdraw — get_deposit_balance
        вычитает 'withdraw' независимо от initiator). comment = назначение от ГД.
        receipt_file_id — опц. вложение исполнения от сотрудника (чек/фото),
        пишется в payment_file_id. Без items (нет привязки к invoice).
        """
        if amount <= 0:
            raise ValueError(f"Withdraw amount must be positive, got {amount}")
        if not (comment or "").strip():
            raise ValueError("comment (назначение) обязателен")
        balance = await self.get_deposit_balance(employee_id, wallet_role)
        if amount > balance + 0.001:
            raise ValueError(
                f"Недостаточно средств на депозите: balance={balance:.2f} < {amount:.2f}",
            )
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, paid_at, paid_by, "
            " payment_file_id, initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, 'gd', 'withdraw', ?)",
            (employee_id, amount, comment, now, now, gd_id, receipt_file_id, wallet_role),
        )
        req_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=gd_id,
            action="installer_advance_gd_deposit_request_withdraw",
            entity="advance_request",
            entity_id=str(req_id),
            payload={
                "amount": amount,
                "employee_id": employee_id,
                "comment": comment,
                "balance_after": max(0.0, balance - amount),
            },
        )
        return req_id

    # =====================================================================
    # ТЗ 2026-05-20: распределение outstanding аванса монтажником
    # =====================================================================

    async def get_advance_outstanding_unallocated(
        self, installer_id: int, wallet_role: str | None = None,
    ) -> float:
        """Свободный advance, ещё не привязанный к invoice (для distribute UI).

        После 25.05 funds-2balances split:
        unallocated = SUM(paid type IN ('request','transfer_depo_to_adv'))
                    − SUM(items.amount where parent.type IN ('request','transfer_depo_to_adv'))

        Deposit и withdraw — отдельный pool (get_deposit_balance), здесь не учитываются.
        Любой item (в т.ч. с offset_zp_id IS NULL) считается «распределённым» —
        UI должен показывать остаток после бронирования.
        """
        wclause2, wparams2 = self._wallet_clause(wallet_role, "r2")
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT COALESCE(SUM(r.total_amount), 0) - "
            "       COALESCE((SELECT SUM(i.amount) FROM installer_advance_items i "
            "                  JOIN installer_advance_requests r2 ON r2.id = i.request_id "
            "                  WHERE r2.installer_id = ? AND r2.status = 'paid' "
            "                    AND r2.request_type IN ('request','transfer_depo_to_adv')"
            + wclause2 + "), 0) "
            "FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request','transfer_depo_to_adv')" + wclause,
            (installer_id, *wparams2, installer_id, *wparams),
        )
        row = await cur.fetchone()
        unalloc = float(row[0] or 0) if row else 0.0
        # Зачёт аванса в оклад РП (31.07): свободный остаток тоже обязан упасть, иначе
        # distribute UI предложит потратить уже погашенные деньги второй раз.
        unalloc -= await self._sum_rp_oklad_offset(installer_id, wallet_role)
        return max(0.0, unalloc)

    async def get_open_advance_items_for_installer(
        self, installer_id: int,
    ) -> list[dict[str, Any]]:
        """Глобально: все paid items монтажника без offset (FIFO для кредит-авто)."""
        cur = await self.conn.execute(
            "SELECT i.id, i.request_id, i.invoice_id, i.amount, i.plan_zp_snapshot, "
            "       inv.invoice_number, inv.object_address, "
            "       COALESCE(inv.is_credit, 0) AS is_credit "
            "FROM installer_advance_items i "
            "JOIN installer_advance_requests r ON r.id = i.request_id "
            "LEFT JOIN invoices inv ON inv.id = i.invoice_id "
            "WHERE r.installer_id = ? AND r.status = 'paid' AND i.offset_zp_id IS NULL "
            "ORDER BY r.requested_at ASC, i.id ASC",
            (installer_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def add_advance_item_for_distribution(
        self, installer_id: int, invoice_id: int, amount: float,
        plan_zp_snapshot: float, actor_id: int, role: str = "installer",
        wallet_role: str | None = None,
    ) -> int:
        """Распределить часть аванса на ещё один invoice — INSERT new item.

        role='installer' (default): hook смотрит zp_installer_*.
        role='manager': hook смотрит zp_manager_* (для distribute от менеджеров).

        Проверяет: unallocated ≥ amount; amount ≤ plan_zp_snapshot − taken
        (защита от перерасхода через UI). Привязывает к самому свежему paid
        request. Если invoice уже zp=approved — сразу call auto-offset hook.
        Возвращает id нового item.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        unallocated = await self.get_advance_outstanding_unallocated(installer_id, wallet_role)
        if amount > unallocated + 0.001:
            raise ValueError(
                f"amount={amount} > unallocated={unallocated}",
            )
        # Guard: amount не должен превышать plan_zp_snapshot − taken по этому invoice.
        taken = await self.get_advance_taken_for_invoice(invoice_id)
        max_avail = plan_zp_snapshot - taken
        if amount > max_avail + 0.001:
            raise ValueError(
                f"amount={amount} > available={max_avail} "
                f"(plan={plan_zp_snapshot}, taken={taken})",
            )
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT r.id FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv')" + wclause + " "
            "ORDER BY r.paid_at DESC LIMIT 1",
            (installer_id, *wparams),
        )
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("no paid advance request found")
        req_id = int(row[0])
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount, plan_zp_snapshot) VALUES (?, ?, ?, ?)",
            (req_id, invoice_id, amount, plan_zp_snapshot),
        )
        item_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_advance_distributed",
            entity="advance_item",
            entity_id=str(item_id),
            payload={
                "request_id": req_id, "invoice_id": invoice_id,
                "amount": amount, "plan_zp_snapshot": plan_zp_snapshot,
                "role": role,
            },
        )
        # Hook: если invoice уже zp=approved → сразу auto-offset этого нового item.
        zp_status_col = "zp_manager_status" if role == "manager" else "zp_installer_status"
        zp_amount_col = "zp_manager_amount" if role == "manager" else "zp_installer_amount"
        cur = await self.conn.execute(
            f"SELECT {zp_status_col}, {zp_amount_col} FROM invoices WHERE id=?",
            (invoice_id,),
        )
        zp_row = await cur.fetchone()
        if zp_row and (zp_row[0] or "") == "approved":
            zp_amt = float(zp_row[1] or 0)
            if zp_amt > 0:
                await self.apply_advance_offsets_on_zp_approve(
                    invoice_id, zp_id=invoice_id, zp_amount=zp_amt,
                    actor_id=actor_id, role=role,
                )
        return item_id

    async def offset_approved_zp_with_advance(
        self, invoice_id: int, actor_id: int,
    ) -> dict[str, Any]:
        """Для approved-ZP по invoice: зачесть открытый item этого invoice в счёт ZP.

        Логика: ZP уже approved (ГД одобрил). Вместо реальной выплаты — закрываем
        как 'confirmed' и записываем offset на open advance item этого же invoice.
        BT (montazh_fact_op) НЕ заполняется (Игорь не получает деньги физически —
        зачёт против ранее выплаченного аванса). Возвращает {offset_amount, zp_amt}.
        """
        cur = await self.conn.execute(
            "SELECT zp_installer_amount, zp_installer_status "
            "FROM invoices WHERE id = ?",
            (invoice_id,),
        )
        inv = await cur.fetchone()
        if not inv:
            raise RuntimeError(f"invoice id={invoice_id} not found")
        zp_amount = float(inv[0] or 0)
        zp_status = inv[1] or ""
        if zp_status != "approved":
            raise RuntimeError(f"ZP status='{zp_status}', expected 'approved'")

        items = await self.get_open_advance_items_for_invoice(invoice_id)
        if not items:
            raise RuntimeError(f"no open advance items on invoice {invoice_id}")

        total_offset = 0.0
        now = to_iso(utcnow())
        remaining = zp_amount
        for it in items:
            if remaining <= 0:
                break
            off = min(float(it["amount"]), remaining)
            await self.conn.execute(
                "UPDATE installer_advance_items "
                "SET offset_zp_id = ?, offset_amount = ?, offset_at = ? WHERE id = ?",
                (invoice_id, off, now, it["id"]),
            )
            total_offset += off
            remaining -= off
        # ZP approved → confirmed (фактически — закрыто через offset, не через BT)
        await self.conn.execute(
            "UPDATE invoices SET zp_installer_status = 'confirmed', "
            "zp_installer_confirmed_at = ? WHERE id = ?",
            (now, invoice_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_zp_offset_by_advance",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={"zp_amount": zp_amount, "offset_amount": total_offset},
        )
        return {"offset_amount": total_offset, "zp_amt": zp_amount}

    async def apply_advance_to_invoice_now(
        self, installer_id: int, invoice_id: int, amount: float, actor_id: int,
        wallet_role: str | None = None,
    ) -> dict[str, Any]:
        """Применить аванс к счёту НЕМЕДЛЕННО, в счёт предстоящей ЗП-монтаж.

        Спек user 03.06 (аванс «применить сейчас», частично, без гейта approved):
          - создаёт СРАЗУ ЗАКРЫТЫЙ advance-item (offset_zp_id=invoice_id,
            offset_amount=amount, offset_at=now) → баланс кошелька −= amount немедленно;
            запись попадает в журнал «Авансирование сотрудников» (как offset).
          - при ПОЛНОМ закрытии (Σприменённого ≥ montazh_agreed) → ЗП по счёту
            помечается 'оплачено' (zp_installer_status='confirmed' + zp_installer_amount=
            montazh_agreed + zp_installer_confirmed_at=now) → BS заполнится через ветку 3,
            DO из confirmed_at. Частичное применение статус ЗП НЕ меняет (BS пуст, BJ=остаток).
        Объединение платежей (owner 15.07): «montazh_agreed» ниже везде = доплата ТЕКУЩЕЙ
        группы (montazh_agreed_amount − montazh_paid_prev), а Σприменённого — без авансов
        прошлых групп (− montazh_adv_prev). Обычный счёт: оба поля 0 → поведение прежнее.
        Старые add_advance_item_for_distribution / offset_approved_zp_with_advance НЕ трогаются.
        Возвращает {applied, total_applied, remaining, full_closed}.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        # ТЗ 2026-06-04: аванс по счёту только если взят в работу + согласована стоимость.
        await self.assert_invoice_advance_eligible(installer_id, int(invoice_id))
        # Счёт: назначен этому монтажнику + есть согласованная ЗП-монтаж.
        cur = await self.conn.execute(
            "SELECT assigned_to, montazh_agreed_amount, montazh_paid_prev, montazh_adv_prev "
            "FROM invoices WHERE id = ?",
            (invoice_id,),
        )
        inv = await cur.fetchone()
        if not inv:
            raise RuntimeError(f"invoice id={invoice_id} not found")
        if int(inv[0] or 0) != int(installer_id):
            raise RuntimeError(
                f"invoice {invoice_id} assigned_to={inv[0]} != installer {installer_id}",
            )
        # Объединение платежей (owner 15.07): montazh_agreed_amount включает ЗП, выплаченную
        # ПРОШЛЫМ группам. Этому монтажнику причитается только доплата, иначе он закрыл бы
        # авансом всю объединённую сумму и компания простила бы ему paid_prev даром.
        # Аванс прошлой группы (adv_prev) уже внутри paid_prev — из taken его вычитаем,
        # иначе вычтется дважды. Обычный счёт: оба поля 0 → арифметика ровно прежняя.
        agreed_total = float(inv[1] or 0)
        paid_prev = float(inv[2] or 0)
        adv_prev = float(inv[3] or 0)
        agreed = agreed_total - paid_prev
        if agreed <= 0:
            raise RuntimeError(f"invoice {invoice_id}: montazh_agreed_amount not set")
        # Свободный (нераспределённый) аванс кошелька.
        unallocated = await self.get_advance_outstanding_unallocated(installer_id, wallet_role)
        if amount > unallocated + 0.001:
            raise ValueError(f"amount={amount} > free advance={unallocated}")
        # Остаток ЗП по счёту = agreed − уже применённое (та же сумма, что в BS/BJ).
        taken = max(0.0, await self.get_installer_advance_for_invoice(invoice_id) - adv_prev)
        remaining_before = agreed - taken
        if amount > remaining_before + 0.001:
            raise ValueError(
                f"amount={amount} > remaining ZP={remaining_before} "
                f"(agreed={agreed}, taken={taken})",
            )
        # Родитель — последнее оплаченное пополнение типа 'request' (чтобы get_advance_balance
        # корректно вычел offset_amount: он считает только request-parent, не transfer_depo_to_adv).
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT r.id FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type = 'request'" + wclause + " "
            "ORDER BY r.paid_at DESC LIMIT 1",
            (installer_id, *wparams),
        )
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("no paid advance topup (request) found")
        req_id = int(row[0])
        now = to_iso(utcnow())
        # ЗАКРЫТЫЙ item сразу: offset проставлен → баланс −= amount немедленно.
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount, plan_zp_snapshot, "
            " offset_zp_id, offset_at, offset_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (req_id, invoice_id, amount, agreed, invoice_id, now, amount),
        )
        item_id = int(cur.lastrowid)
        total_applied = taken + amount
        full_closed = total_applied >= agreed - 0.001
        if full_closed:
            # Полное закрытие авансом → ЗП по счёту 'оплачено' (решение user 03.06).
            # BS заполнит ветка 3 (=montazh_agreed), DO из confirmed_at.
            await self.conn.execute(
                "UPDATE invoices SET zp_installer_status = 'confirmed', "
                "zp_installer_amount = ?, zp_installer_confirmed_at = ? WHERE id = ?",
                (agreed, now, invoice_id),
            )
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_advance_applied_now",
            entity="advance_item",
            entity_id=str(item_id),
            payload={
                "request_id": req_id, "invoice_id": invoice_id, "amount": amount,
                "agreed": agreed, "total_applied": total_applied,
                "full_closed": full_closed,
            },
        )
        return {
            "applied": amount,
            "total_applied": total_applied,
            "remaining": max(0.0, agreed - total_applied),
            "full_closed": full_closed,
        }

    async def record_installer_advance_offset_from_wallet(
        self, installer_id: int, invoice_id: int, amount: float, actor_id: int | None,
        *, comment: str, plan_zp_snapshot: float | None = None,
        wallet_role: str | None = None, initiator: str = "gd",
    ) -> dict[str, Any]:
        """Зачесть ЧАСТИЧНУЮ выплату ЗП монтажа из кредит-кошелька как аванс монтажника.

        Owner 25.07: «Выплачено по ЗП монтаж ВСЕГДА ≥ Согласовано; аванс/частичная
        выплата засчитывается ВНУТРЬ согласованной суммы». Трата кошелька с
        cost_type='montazh' МЕНЬШЕ причитающегося — это аванс, а не закрытие всей ЗП
        (инцидент 23.07, сч. КВ 9: трата 50 000 помечала выплаченной ЗП 120 465).

        Пишем ровно ту пару записей, которой инцидент правился РУКАМИ (эталон —
        заявка 22 + item 11 по сч. 60):
          • installer_advance_requests — сразу 'paid', request_type='request' (приход);
          • installer_advance_items    — сразу ЗАКРЫТЫЙ item (offset_zp_id/offset_at/
            offset_amount) → зачёт по счёту.
        Приход == зачёт ⟹ баланс авансового кошелька монтажника НЕ меняется (деньги
        ушли из кредит-кошелька, их учли add_credit_spend/add_credit_expense), а по
        счёту сумма видна в CG «Аванс монтажника» и входит в «Выплачено» (BS/BJ)
        [[feedback_bs_immutable]] — формулу листа не трогаем, пользуемся её каналом.

        ⚠️ Владелец заявки ОБЯЗАН иметь роль 'installer', иначе зачёт не попадёт в
        монтажный CG/BJ (гард get_installer_advance_for_invoice) и запись станет
        мусором → raise до любой вставки. Гарды apply_advance_to_invoice_now (свободный
        остаток аванса, assigned_to == installer) здесь НЕ применимы: источник денег —
        кредит-кошелёк, а не пополненный аванс, поэтому «свободного аванса» нет по
        определению. Ограничение «amount ≤ причитающегося» держит вызывающий
        (utils.resolve_installer_zp_by_wallet_payment: partial-ветка только при
        зачёте < причитающегося).
        Возвращает {"request_id": int, "item_id": int}.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        cur = await self.conn.execute(
            "SELECT COALESCE(role, '') FROM users WHERE telegram_id = ?", (installer_id,)
        )
        row = await cur.fetchone()
        _roles = [r.strip() for r in str((row or [""])[0]).split(",")]
        if not row or "installer" not in _roles:
            raise RuntimeError(
                f"user {installer_id} роль={_roles} — не монтажник, зачёт не попал бы в CG/BJ"
            )
        now = to_iso(utcnow())
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_requests "
            "(installer_id, total_amount, status, comment, requested_at, approved_at, "
            " approved_by, paid_at, paid_by, initiator, request_type, wallet_role) "
            "VALUES (?, ?, 'paid', ?, ?, ?, ?, ?, ?, ?, 'request', ?)",
            (installer_id, amount, comment, now, now, actor_id, now, actor_id,
             initiator, wallet_role),
        )
        req_id = int(cur.lastrowid)
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount, plan_zp_snapshot, "
            " offset_zp_id, offset_at, offset_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (req_id, invoice_id, amount, plan_zp_snapshot, invoice_id, now, amount),
        )
        item_id = int(cur.lastrowid)
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="installer_advance_offset_from_wallet",
            entity="advance_item",
            entity_id=str(item_id),
            payload={
                "request_id": req_id, "invoice_id": invoice_id, "amount": amount,
                "installer_id": installer_id, "comment": comment,
            },
        )
        return {"request_id": req_id, "item_id": item_id}

    async def apply_manager_advance_immediate(
        self, manager_id: int, invoice_id: int, amount: float, actor_id: int,
        wallet_role: str | None = None,
    ) -> dict[str, Any]:
        """Немедленный зачёт аванса менеджера в ЗП по ЗАКРЫТОМУ (End) счёту.

        Owner 2026-07-04: на закрытом счёте (BQ «Счёт END») распределение аванса
        менеджера сразу фиксирует выплату ЗП — пишет AN(zp_manager_payout) и
        AO(zp_manager_payout_date) БЕЗ ГД-approval. Зеркалит ручной бэкфилл КВ и
        installer-аналог apply_advance_to_invoice_now (для «в работе» остаётся
        резерв add_advance_item_for_distribution + offset при approve).

          - создаёт СРАЗУ ЗАКРЫТЫЙ advance-item (offset_zp_id=invoice_id,
            offset_amount=amount, offset_at=now) → баланс кошелька −= amount
            немедленно; запись попадает в журнал «Авансирование сотрудников».
          - сумма ЗП к выплате = manager_zp_net_payout(inv) (бланк − удержание,
            debt-aware), НЕ zp_manager_amount (часто NULL у КВ). Если net<=0 (план
            не задан) — cap только по свободному авансу.
          - AN = накопленный offset по счёту (taken+amount), с cap по net. AO =
            сегодня (DD.MM.YYYY, не перетираем существующую). AN/AO durable через
            _ZP_PAYOUT_PRESERVE (пустой синк «Импорт ОП» не затрёт).
          - при ПОЛНОМ покрытии (Σoffset ≥ net) → zp_manager_status='confirmed'.
            Частично — статус НЕ трогаем (счёт уходит из целей распределения, т.к.
            AN>0; остаток owner добивает вручную).
        Возвращает {applied, total_offset, an, net, full_closed}.
        """
        if amount <= 0:
            raise ValueError(f"amount must be > 0, got {amount}")
        inv = await self.get_invoice(invoice_id)
        if not inv:
            raise RuntimeError(f"invoice id={invoice_id} not found")
        if int(inv.get("created_by") or 0) != int(manager_id):
            raise RuntimeError(
                f"invoice {invoice_id} created_by={inv.get('created_by')} "
                f"!= manager {manager_id}",
            )
        # Гард: ЗП по счёту не должна быть уже выплачена/в оплате.
        if (inv.get("zp_manager_status") or "") in ("confirmed", "payment_sent"):
            raise RuntimeError(
                f"invoice {invoice_id}: ЗП менеджера уже выплачена "
                f"(status={inv.get('zp_manager_status')})",
            )
        net = manager_zp_net_payout(inv)
        # Свободный (нераспределённый) аванс кошелька.
        unallocated = await self.get_advance_outstanding_unallocated(manager_id, wallet_role)
        if amount > unallocated + 0.001:
            raise ValueError(f"amount={amount} > free advance={unallocated}")
        # Остаток ЗП к зачёту = net − уже зачтённое по счёту (защита от переплаты).
        taken = await self.get_advance_taken_for_invoice(invoice_id)
        if net > 0:
            remaining_before = net - taken
            if amount > remaining_before + 0.001:
                raise ValueError(
                    f"amount={amount} > remaining ZP={remaining_before} "
                    f"(net={net}, taken={taken})",
                )
        # Родитель — последнее оплаченное пополнение кошелька (request/depo→adv).
        wclause, wparams = self._wallet_clause(wallet_role, "r")
        cur = await self.conn.execute(
            "SELECT r.id FROM installer_advance_requests r "
            "WHERE r.installer_id = ? AND r.status = 'paid' "
            "  AND r.request_type IN ('request', 'transfer_depo_to_adv')" + wclause + " "
            "ORDER BY r.paid_at DESC LIMIT 1",
            (manager_id, *wparams),
        )
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("no paid advance topup found")
        req_id = int(row[0])
        now = to_iso(utcnow())
        snapshot = net if net > 0 else amount
        # ЗАКРЫТЫЙ item сразу: offset проставлен → баланс −= amount немедленно.
        cur = await self.conn.execute(
            "INSERT INTO installer_advance_items "
            "(request_id, invoice_id, amount, plan_zp_snapshot, "
            " offset_zp_id, offset_at, offset_amount) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (req_id, invoice_id, amount, snapshot, invoice_id, now, amount),
        )
        item_id = int(cur.lastrowid)
        total_offset = taken + amount
        # AN = накопленный offset, cap по net (если задан).
        an_value = min(total_offset, net) if net > 0 else total_offset
        full_closed = net > 0 and total_offset >= net - 0.001
        pay_date = datetime.now().strftime("%d.%m.%Y")
        if full_closed:
            # Полное покрытие → ЗП по счёту 'оплачено' (confirmed) + AN/AO.
            await self.conn.execute(
                "UPDATE invoices SET zp_manager_status='confirmed', "
                "    zp_manager_payout=?, "
                "    zp_manager_payout_date="
                "COALESCE(NULLIF(zp_manager_payout_date, ''), ?), "
                "    updated_at=? WHERE id=?",
                (an_value, pay_date, now, invoice_id),
            )
        else:
            # Частично: пишем AN/AO, статус ЗП НЕ трогаем.
            await self.conn.execute(
                "UPDATE invoices SET zp_manager_payout=?, "
                "    zp_manager_payout_date="
                "COALESCE(NULLIF(zp_manager_payout_date, ''), ?), "
                "    updated_at=? WHERE id=?",
                (an_value, pay_date, now, invoice_id),
            )
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="manager_advance_applied_now",
            entity="advance_item",
            entity_id=str(item_id),
            payload={
                "request_id": req_id, "invoice_id": invoice_id, "amount": amount,
                "net": net, "taken": taken, "total_offset": total_offset,
                "an": an_value, "full_closed": full_closed,
            },
        )
        return {
            "applied": amount, "total_offset": total_offset, "an": an_value,
            "net": net, "full_closed": full_closed,
        }

    async def credit_autoclose_with_advance(
        self, invoice_id: int, installer_id: int, plan_zp_total: float,
        actor_id: int,
    ) -> dict[str, Any]:
        """Кредитный счёт + Счёт ОК + есть open advance items монтажника (любые) →
        авто-confirm ZP + FIFO offset на open items монтажника.

        НЕ создаёт ZP-flow, не делает payment_sent (Игорь не получает физически
        деньги — они идут в счёт ранее взятого аванса).

        Возвращает {applied: bool, offset_total: float, items_count: int}.
        """
        if plan_zp_total <= 0:
            return {"applied": False, "offset_total": 0.0, "items_count": 0}
        items = await self.get_open_advance_items_for_installer(installer_id)
        outstanding = sum(float(i["amount"]) for i in items)
        # Pull from unallocated если open items < plan_zp_total: создать новый
        # item на этот invoice с amount=gap, потом FIFO offset захватит его.
        if outstanding < plan_zp_total:
            unallocated = await self.get_advance_outstanding_unallocated(installer_id)
            if unallocated > 0:
                gap = plan_zp_total - outstanding
                pull = min(gap, unallocated)
                if pull > 0:
                    cur = await self.conn.execute(
                        "SELECT id FROM installer_advance_requests "
                        "WHERE installer_id=? AND status='paid' "
                        "ORDER BY paid_at DESC LIMIT 1",
                        (installer_id,),
                    )
                    req_row = await cur.fetchone()
                    if req_row:
                        req_id = int(req_row[0])
                        cur2 = await self.conn.execute(
                            "INSERT INTO installer_advance_items "
                            "(request_id, invoice_id, amount, plan_zp_snapshot) "
                            "VALUES (?, ?, ?, ?)",
                            (req_id, invoice_id, pull, plan_zp_total),
                        )
                        new_item_id = int(cur2.lastrowid)
                        await self.conn.commit()
                        await self.audit(
                            actor_id=actor_id,
                            action="installer_advance_auto_credit_topup",
                            entity="advance_item",
                            entity_id=str(new_item_id),
                            payload={
                                "invoice_id": invoice_id, "amount": pull,
                                "from_unallocated": True,
                                "trigger": "credit_autoclose",
                            },
                        )
                        items = await self.get_open_advance_items_for_installer(installer_id)
                        outstanding = sum(float(i["amount"]) for i in items)
        if not items:
            return {"applied": False, "offset_total": 0.0, "items_count": 0}
        if outstanding <= 0:
            return {"applied": False, "offset_total": 0.0, "items_count": 0}

        now = to_iso(utcnow())
        remaining = plan_zp_total
        applied = 0.0
        items_touched = 0
        for it in items:
            if remaining <= 0:
                break
            off = min(float(it["amount"]), remaining)
            await self.conn.execute(
                "UPDATE installer_advance_items "
                "SET offset_zp_id = ?, offset_amount = ?, offset_at = ? WHERE id = ?",
                (invoice_id, off, now, it["id"]),
            )
            applied += off
            remaining -= off
            items_touched += 1
        # Установить ZP=plan_zp_total confirmed для счёта (всё ушло в зачёт).
        await self.conn.execute(
            "UPDATE invoices SET zp_installer_status = 'confirmed', "
            "    zp_installer_amount = ?, zp_installer_confirmed_at = ? "
            "WHERE id = ?",
            (plan_zp_total, now, invoice_id),
        )
        await self.conn.commit()
        await self.audit(
            actor_id=actor_id,
            action="credit_autoclose_zp_by_advance",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "plan_zp_total": plan_zp_total, "offset_total": applied,
                "items_touched": items_touched, "remaining_to_pay": remaining,
            },
        )
        return {
            "applied": applied > 0, "offset_total": applied,
            "items_count": items_touched, "remaining_to_pay": remaining,
        }

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
