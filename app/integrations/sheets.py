from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from dataclasses import dataclass
from threading import RLock
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from ..utils import (
    encode_sa_json,
    format_date_iso,
    format_dt_iso,
    project_status_label,
    role_label,
    task_status_label,
    task_type_label,
    try_json_loads,
)


log = logging.getLogger(__name__)


PROJECTS_HEADER = [
    "Код",
    "Проект",
    "Адрес",
    "Клиент",
    "Сумма",
    "Дедлайн",
    "Статус",
    "Менеджер (ID)",
    "Менеджер",
    "Создан",
    "Обновлён",
    "amo_lead_id",
]

TASKS_HEADER = [
    "ID задачи",
    "Код проекта",
    "Тип задачи",
    "Статус",
    "Назначена (ID)",
    "Создал (ID)",
    "Срок",
    "Создана",
    "Обновлена",
    "Комментарий",
    "Размеры/ТЗ",
    "Тип проблемы",
    "Документы",
    "Уточнение",
    "Сумма оплаты",
    "Тип оплаты",
    "Этап оплаты",
    "Дата оплаты",
    "№ счёта",
    "Тип подписания",
    "Источник",
    "Отправитель",
]

# Помесячная аналитика компании (Доходы/Расходы) — отдельный лист «Баланс компании»
BALANCE_COMPANY_HEADER = [
    "Год",                    # A
    "Месяц (1-12)",           # B
    "Месяц назв.",            # C
    "Доходы Итого",           # D  (income_total, legacy)
    "Расходы безнал",         # E  (expense_cashless = BJ из «Итого:»)
    "НДС",                    # F  (expense_nds = BK, в т.ч. в E, информация)
    "Налоги (ЕНП)",           # G  (expense_taxes = BM, информация; НЕ в I)
    "Расходы прочие",         # H  (expense_other = BP из «Итого:»)
    "Расходы Итого",          # I  (E + H; налоги и НДС не плюсуются)
    "Займ (нетто)",           # J  (loan_net, ± знак; финансирование, не P&L)
    "Баланс месяца",          # K  (D − I; без налогов и займа)
    "Running с начала года",  # L  (cumsum K в рамках года)
]

# Журнал операций — повторяет блок BH-BQ из «Импорт ОП» (10 колонок).
# Лист «Баланс компании» = структура BH-BQ (template-rows сохраняются) +
# auto-fill из op_company_entries в пустые template-rows своего месяца +
# auto-пересчёт «Итого:» по всем data rows месяца.
# Высота блока одного месяца на листе «Баланс компании» (owner 25.07: «по 20
# строк на каждый месяц»). Данных меньше — блок добивается пустыми строками
# своего месяца; больше — показываем все строки, данные не прячем.
BALANCE_MONTH_BLOCK_ROWS = 20

BALANCE_JOURNAL_HEADER = [
    "Месяц",          # A → BH (RU label либо 'Итого:')
    "Дата",           # B → BI (DD.MM.YYYY)
    "Сумма б/н",      # C → BJ
    "НДС",            # D → BK
    "Расходы б/н",    # E → BL
    "Налоги",         # F → BM
    "Займ",           # G → BN (± знак)
    "Дата",           # H → BO (DD.MM.YYYY)
    "Сумма",          # I → BP
    "Расходы кред",   # J → BQ
]

# ТЗ 2026-05-19 блок C: журнал авансов монтажников.
# 2026-05-20: расширен до 10 колонок под 4 роли (Монтажник/Менеджер НПН/КВ/РП)
# + детализация (Объект подробно, Остаток running balance per-сотрудник).
# Полная перезапись листа при каждом событии (give/approve/pay/offset/writeoff).
ADVANCES_JOURNAL_HEADER = [
    "Дата",             # A — request.requested_at / item.offset_at (DD.MM.YYYY)
    "Сотрудник",        # B — installer_id → @username или ФИО из users
    "Роль",             # C — Монтажник / Менеджер НПН / Менеджер КВ / РП (utils.role_label)
    "Тип",              # D — give/approved/paid/rejected/offset/writeoff
    "Объект",           # E — invoice_number для offset; «(пакет)» для request-level
    "Объект подробно",  # F — invoice_number · object_address (offset); comment (request-level)
    "Сумма ₽",          # G
    "Остаток",          # H — running balance unallocated per-сотрудник ПОСЛЕ события
    "Запрос #",         # I — request_id
    "Статус",           # J — текущий статус request'a
]

# ТЗ 30.05 Часть A: накопительная сводка по кошелькам (шапка листа, над журналом).
# Накопительная модель: кошелёк пополняется (ГД + незабранная ЗП), уменьшается выдачей.
# Кошелёк аванса/депозита = get_advance_balance / get_deposit_balance (те же числа,
# что бот показывает везде). «Зачислено из ЗП» наполняется Частью B/C (пока 0).
# Павел (rp,manager_npn) — 2 строки (РП / Менеджер НПН).
ADVANCES_SUMMARY_HEADER = [
    "Сотрудник",           # A
    "Роль",                # B
    "Внесено ГД ₽",        # C — Σ ГД-пополнений (initiator='gd': deposit + advance topup)
    "Зачислено из ЗП ₽",   # D — незабранная ЗП → кошелёк аванса (Часть B/C; пока 0)
    "Выдано / выведено ₽", # E — Σ снятий сотрудника (request_type='withdraw')
    "Кошелёк аванса ₽",    # F — get_advance_balance (накопительный остаток)
    "Кошелёк депозита ₽",  # G — get_deposit_balance
    "Статус",              # H — статус запроса аванса: на одобрении / одобрен, ждёт оплаты / нет активных
]

# TZ кредит-журнал 2026-06-02: перенос отображения CV–DA (Invoices, поз. 99–104)
# на детальный журнал на листе «Авансирование сотрудников». Источник —
# db.list_all_credit_events (зеркало get_credit_balance_summary; числа НЕ пересчитываются).
# Модель кошелька (TZ 02.06): carry убран; остаток = приход − расход.
CREDIT_SUMMARY_HEADER = [
    "Менеджер",            # A — КВ / КИА / НПН
    "Приход ₽",            # B — total_in: Σ(сумма−долг) по кредит-счетам роли
    "Расход ₽",            # C — total_out: Σ трат кошелька (привязка + без привязки)
    "Остаток ₽",           # D — balance = приход − расход
]

CREDIT_JOURNAL_HEADER = [
    "Дата",                    # A — дата события (DD.MM.YYYY)
    "Менеджер",                # B — КВ / КИА / НПН
    "Тип",                     # C — ⬆️ Приход / ⬇️ Расход
    "Счёт",                    # D — invoice_number (привязка; пусто без привязки)
    "Объект",                  # E — object_address
    "Назначение / Категория",  # F — приход: «оплата кредитного счёта» / «оконч. доплата»; расход: назначение + категория
    "Сумма ₽",                 # G
    "Остаток ₽",               # H — running остаток кошелька после события
    "Кто внёс",                # I — @username внёсшего расход
]

# Bot leads header — written starting from column H (col 8)
LEADS_COL_START = 1  # column A (1-indexed)
LEADS_HEADER = [
    "Дата",           # A
    "Имя клиента",    # B
    "Имя",            # C — название лида
    "Телефон",        # D
    "Менеджер",       # E
    "Источник",       # F
    "Статус",         # G
    "Сумма сделки",   # H — price из amoCRM
    "Примечание",     # I — последняя заметка ответственного из amoCRM
]

# Журнал заявок замерщика на листе Leads, блок W:AG (11 колонок, 1 строка = 1 заявка).
LEADS_ZAMERY_JOURNAL_HEADER = [
    "Адрес",          # W
    "Дата замера",    # X
    "Интервал",       # Y
    "Менеджер",       # Z
    "Статус",         # AA
    "Стоимость",      # AB
    "МКАД, км",       # AC
    "Объём, м²",      # AD
    "Конверсия",      # AE
    "Фото, шт",       # AF
    "Комментарий",    # AG
    "Оплачено",       # AH  сумма, выплаченная замерщику за этот замер
    "Дата оплаты",    # AI  дата платежа отчётного периода
]

INVOICES_HEADER = [
    # — Отдел продаж structure (0-45) —
    "№",            # 0
    "Роль",         # 1
    "Менеджер",     # 2
    "Бухг.ЭДО",    # 3
    "Контрагент",   # 4
    "Ист.трафика",  # 5  manual
    "Б.Н./Кред",    # 6
    "Свой/Атм",     # 7  manual
    "Номер счета",  # 8
    "Адрес",        # 9
    "Дата пост.",   # 10
    "Сроки",        # 11 manual
    "Дата оконч.",  # 12 FORMULA
    "Дата Факт",    # 13
    "Сумма",        # 14
    "Сумма 1пл",    # 15
    "Расч.мат.",    # 16
    "Установка",    # 17
    "Грузчики",     # 18 manual
    "Логистика",    # 19 manual
    "Прибыль",      # 20
    "НДС",          # 21 manual
    "Нал.приб.",    # 22 manual
    "Рент-ть расч", # 23
    "Рент-ть факт", # 24 manual
    "Сумма допл",   # 25 manual
    "Допл подтв",   # 26 manual
    "Дата допл",    # 27 manual
    "Оконч допл",   # 28 manual
    "Дата оконч",   # 29 manual
    "Долг",         # 30
    "Договор",      # 31 manual
    "Закр.док",     # 32
    "Пояснения",    # 33 manual
    "Агентское",    # 34 manual
    "Мен.ЗП",       # 35
    "Запрос",       # 36
    "тех",          # 37 manual
    "Выпл.Агент",   # 38 manual
    "Выпл.МенЗП",   # 39 manual
    "Дата выпл",    # 40 manual
    "НПН 10%",      # 41 manual
    "Запрос РП",    # 42 manual — renamed 26.05 from «Запрос НПН»
    "Выдано РП",    # 43 manual — renamed 26.05 from «Выдано НПН»
    "Дата РП",      # 44 manual — renamed 26.05 from «Дата НПН»
    "Месяц",        # 45 FORMULA
    # — Бот-специфичные (46-60) —
    "Статус",               # 46
    "Роль менеджера",       # 47
    "ID монтажника",        # 48 — AW: invoice.assigned_to (telegram_id)
    "Тип материала",        # 49
    "Родит. счёт ID",       # 50
    "Этап монтажа",         # 51
    "Монтажник ОК",         # 52
    "Долгов нет",           # 53
    "",                     # 54 (перенесено в 74)
    "ID монтажника",        # 55 — BD: дубль AW для удобного просмотра рядом с AZ/BA
    "ЗП Монтажник статус",  # 56
    "Оплаты пост. итого",   # 57
    "Расходы итого",        # 58
    "Создан",               # 59
    "Обновлён",             # 60
    # — Статусы жизненного цикла (61-73) —
    "ЗП Монтажник",              # 61
    "Расчетная прибыль",         # 62
    "Прибыль факт",              # 63
    "Перерасчет прибыли",        # 64
    "НДС факт",            # 65
    "Налог на приб. факт", # 66
    "В работе",            # 67
    "Счет END",            # 68
    "Грузчики факт",       # 69
    "Монтаж Факт",         # 70
    "Материалы Факт",      # 71
    "Логистика Факт",      # 72
    "Статус лида",         # 73
    # — Блок Замерщик (74-76, перенос из 54/55/69) —
    "ЗП Замерщик",         # 74 (перенос из 54)
    "ЗП Замерщик сумма",   # 75 (перенос из 55)
    "Замеры",              # 76 (перенос из 69)
    # — Аналитика (77-79) —
    "Расчет vs Факт",     # 77
    "Расч.мат. ост.",     # 78 (CA) — Q − (DP+DQ+DU)
    "Установка ост.",     # 79 (CB) — R − BS
    # — Остатки к закупке (80-82) — план Q-T минус факт. Отрицательное = перерасход —
    "Грузчики ост.",       # 80 (CC) — S − (cost_loaders + loaders_fact_op)
    "Логистика ост.",      # 81 (CD) — T − (cost_logistics + logistics_fact_op)
    "Итого осталось",      # 82 (CE) — CA+CB+CC+CD (сумма остатков по всем категориям)
    "",                    # 83 (резерв)
    "Аванс монтажника",    # 84 (CG) — зачтённый аванс по счёту (б/н +10%, кредит как есть)
    "Дата аванса",         # 85 (CH) — дата зачёта аванса (авто)
    # — Сквозная нумерация (86) —
    "№ п/п",                # 86
    # — Менеджерский блок (87-104) —
    # Таймстемпы монтажных стадий (87-90)
    "Монтаж назначен",       # 87 — montazh_assigned_at
    "Монтаж в работе",       # 88 — montazh_in_work_at
    "Размеры ОК дата",       # 89 — montazh_razmery_ok_at
    "Счёт ОК дата",          # 90 — montazh_invoice_ok_at
    # ОП CF/CG → CN/CO (owner 2026-06-22): ранее payment_confirmed_by/at (91-92),
    # убраны с листа по запросу owner (ГД-only ID + дата не нужны; в БД целы).
    "Удержать из ЗП менеджера",         # 91 (CN) — zp_manager_hold (ОП CF)
    "Разница себестоимости расч/факт",  # 92 (CO) — cost_diff_calc_fact (ОП CG)
    # Аудит ЗП менеджера (93)
    "ЗП мен. одобрил",       # 93 — zp_manager_approved_by
    # Аудит ЭДО (94-95)
    "ЭДО подпись дата",      # 94 — docs_edo_signed_at
    "ЭДО подпись кем",       # 95 — docs_edo_signed_by
    # Статус заказа материалов (96-97)
    "",                      # 96 — profile_order_status  (заголовок убран 15.06: лист не использует)
    "",                      # 97 — metal_order_status  (заголовок убран 15.06: лист не использует)
    # Связка лид→счёт (98)
    "ID лида",               # 98 — lead_tracking_id
    # Кредитный учёт (99-104) — вынесен на лист «Авансирование сотрудников»
    # (owner 2026-06-22). Заголовки убраны; бот очищает весь CV–DA (99-104)
    # при каждом sync (CV–CZ = значения, DA = бывшая формула «=CV-CX»).
    "",                      # 99 (CV)  — было «Кредит вход»
    "",                      # 100 (CW) — было «Кредит вход коммент»
    "",                      # 101 (CX) — было «Кредит расход»
    "",                      # 102 (CY) — было «Дата расход кред»
    "",                      # 103 (CZ) — было «Кредит назначение»
    "",                      # 104 (DA) — было «Кредит баланс» (формула листа)
    # — Авансы монтажника (105-109) — ТЗ 2026-05-19 блок C —
    "",                      # 105 (DB) — sum(paid items.amount)  (заголовок убран 15.06: мёртвый блок)
    "",                      # 106 (DC) — sum(offset_amount) — зачтено в ЗП  (заголовок убран 15.06: мёртвый блок)
    "",                      # 107 (DD) — paid AND NOT offset (open)  (заголовок убран 15.06: мёртвый блок)
    "",                      # 108 (DE) — last paid_at (DD.MM.YYYY)  (заголовок убран 15.06: мёртвый блок)
    "",                      # 109 (DF) — plan_total − offset (caller fills)  (заголовок убран 15.06: мёртвый блок)
    # Резерв (110-116)
    "",                      # 110 (резерв)
    "",                      # 111 (резерв)
    "",                      # 112 (резерв)
    "",                      # 113 (резерв)
    "",                      # 114 (резерв)
    "",                      # 115 (резерв)
    "",                      # 116 (резерв)
    # — ЗП Монтажник: платёжка / подтверждение (117-118) —
    "Платёжка ЗП дата",     # 117 — дата отправки платёжки ГД
    "ЗП подтверждено дата", # 118 — дата подтверждения монтажником
    # --- Затраты по типам (из supplier_payments бота) ---
    "Затр. Металл",        # 119
    "Затр. Стекло",        # 120
    "Затр. Монтаж",        # 121
    "Затр. Грузчики",      # 122
    "Затр. Логистика",     # 123
    "Затр. Доп мат.",      # 124
    "Затр. Доп усл.",      # 125
    # --- Доп. поля процессов ---
    "Коммент. монтажник ОК",  # 126
    "",                       # 127 — payment_method  (заголовок убран 15.06: лист не использует)
    "Статус заказа стекла",   # 128
    # --- Бухгалтерия (129-143) ---
    "Первичка ЭДО",           # 129
    "Первичка бумага",         # 130
    "Первичка оригиналы",     # 131
    "Первичка коммент",        # 132
    "Вторичка ЭДО",           # 133
    "Вторичка оригиналы",     # 134
    "Вторичка коммент",        # 135
    "Статус закр.док",         # 136
    "Дата ЭДО подписи",       # 137
    "Дата долгов нет",         # 138
    "Платёжка ЗП файл",       # 139
    "ЭДО запросов всего",     # 140
    "ЭДО открытых",           # 141
    "Последний ЭДО ответ",    # 142
    "Дата последнего ЭДО",    # 143
    # — Аванс менеджера (144-145) — аналог монтажных CG/CH, для роли менеджер, user 2026-06-14 —
    "Аванс менеджера",        # 144 (EO) — зачтённый аванс менеджера по счёту (б/н ×1.10, кредит как есть)
    "Дата аванса мен.",       # 145 (EP) — дата зачёта аванса менеджера (авто)
    # — Финальный платёж (146) — ориент. дата фин. платежа по долгу, ТЗ 14.06 —
    "Ориент. дата фин.платежа",  # 146 (EQ) — planned_final_payment_date (вводит менеджер при долге)
    # — Переплата ЗП менеджера, перенесённая в его авансовый кошелёк (147-148),
    #   owner 07.08. Пара «сумма + дата» по образцу монтажных CG/CH. Прежде сумма
    #   (zp_hold_advanced) жила ТОЛЬКО в БД и на листе не показывалась вовсе, а
    #   EO/EP её не видят: свип пополняет кошелёк, но installer_advance_items с
    #   привязкой к счёту не создаёт → get_manager_advance_for_invoice = 0.
    #   Только материнские счета — оба канала переноса это уже гарантируют.
    "Переплата в аванс",      # 147 (ER) — zp_hold_advanced (перенесено в кошелёк менеджера)
    "Дата переплаты",         # 148 (ES) — zp_hold_advanced_at (дата последнего переноса)
]

# Column indices the bot NEVER overwrites (manual-only + formula)
# Removed 7 (Свой/Атм→client_source), 18,19,21,24 — now bot-managed (Plan/Fact)
# Removed 5 (Ист.трафика) — now written from traffic_source DB field
# Removed 37 (AL «тех») — 2026-05-11: bot теперь сам пишет fallback
# zp_manager_request_amount || zp_manager_request_text, чтобы Invoices AL
# был не пустой когда менеджер заполнил «новую» AI «Запрос» в ОП.
_MANUAL_COLS = frozenset([33, 34])


# Human-readable RU labels for ZP enum statuses (col AK manager, BE installer, BW замерщик)
_ZP_STATUS_LABELS = {
    "not_requested": "—",
    "requested": "Запрошено",
    "approved": "Одобрено",
    "payment_sent": "Платёж отправлен",
    "confirmed": "Подтверждено",
    "rejected": "Отклонено",
}

# Human-readable RU labels for montazh_stage enum (cols AU/AZ)
_MONTAZH_STAGE_LABELS = {
    "none": "",
    "assigned": "Назначен",
    "in_work": "В работе",
    "razmery_ok": "Размеры ОК",
    "invoice_ok": "Счет ОК",
    "invoice_end": "Счет End",
}


@dataclass
class SheetsConfig:
    enabled: bool
    spreadsheet_id: str
    projects_tab: str
    tasks_tab: str
    invoices_tab: str = "Invoices"
    leads_tab: str = "Leads"
    balance_company_tab: str = "Баланс компании"
    advances_tab: str = "Авансирование сотрудников"
    timezone_name: str = "Europe/Moscow"
    service_account_json: str | None = None
    service_account_file: str | None = None
    # Source spreadsheet for importing (Отдел Продаж)
    source_spreadsheet_id: str | None = None
    source_sheet_name: str = "Отдел продаж"


class GoogleSheetsService:
    """Best-effort sync to Google Sheets.

    Calls are synchronous (gspread), so in the bot we call them via asyncio.to_thread().
    """

    def __init__(self, cfg: SheetsConfig):
        self.cfg = cfg
        self._gc: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._worksheets: dict[str, gspread.Worksheet] = {}
        self._headers_ready: set[str] = set()
        self._row_indexes: dict[str, dict[str, int]] = {}
        self._next_rows: dict[str, int] = {}
        self._sync_lock = RLock()
        # Invoices: key column = "Номер счета" at index 8 → gspread 1-indexed = 9
        self._KEY_COL: dict[str, int] = {cfg.invoices_tab: 9}

    def _fmt_amount(self, amount: Any) -> str:
        if isinstance(amount, (int, float)):
            return f"{amount:.0f}"
        if amount is None:
            return ""
        return str(amount)

    def _fmt_an_payout(self, invoice: dict[str, Any], row: int) -> str:
        """AN «Выпл.МенЗП» = значение + CN «Удержать из ЗП менеджера» (owner 08.08).

        AN (39) — зеркало ОП AJ (`zp_manager_payout`), CN (91) — удержание по
        перерасчёту прибыли (`zp_manager_hold`, хранится МИНУСОМ), пишется этой же
        функцией на той же строке. Пока удержания нет — ячейка остаётся ровно
        прежней; при непустом CN пишем формулу по образцу AE «Долг» (cells[30]),
        чтобы ячейка пересчиталась сама, когда ОП изменит CN.

        В БД правка не уходит: AN нет в карте `sheet_commands._handle_field_change`,
        обратного пути с листа у этой ячейки не существует. Денежные фильтры
        `COALESCE(zp_manager_payout,0)=0` читают поле БД, а не лист.
        """
        payout = invoice.get("zp_manager_payout")
        try:
            hold_val = float(invoice.get("zp_manager_hold") or 0)
        except (TypeError, ValueError):
            hold_val = 0.0
        if not hold_val:
            return self._fmt_amount(payout)
        try:
            payout_val = float(payout or 0)
        except (TypeError, ValueError):
            payout_val = 0.0
        return f"={payout_val:.0f}+CN{row}"

    @staticmethod
    def _fmt_sheet_date(value: Any) -> str:
        """Format DB ISO date/datetime as =DATE() formula for Google Sheets.

        Returns =DATE(YYYY,M,D) so Sheets treats it as a real date —
        correct chronological sorting and locale-aware display (DD.MM.YYYY).
        """
        if value in (None, ""):
            return ""
        text = str(value).strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text)
            return f"=DATE({dt.year};{dt.month};{dt.day})"
        except ValueError:
            return text

    @staticmethod
    def _fmt_docs_primary(invoice: dict[str, Any]) -> str:
        """AF (Договор): contract_signed + docs_edo_signed + docs_originals_holder."""
        contract = invoice.get("contract_signed") or ""
        edo = bool(invoice.get("docs_edo_signed"))
        if edo and contract:
            return f"{contract} ✅"
        if edo:
            return "ЭДО ✅"
        holder = invoice.get("docs_originals_holder")
        if holder:
            label = f"📁 Ориг. у {'ГД' if holder == 'gd' else 'мен.'}"
            return f"{contract} {label}" if contract else label
        if contract:
            return f"{contract} ⏳"
        return "⏳"

    @staticmethod
    def _fmt_docs_closing(invoice: dict[str, Any]) -> str:
        """AG (Закр.док): edo_signed + closing_originals_holder."""
        if bool(invoice.get("edo_signed")):
            return "ЭДО ✅"
        holder = invoice.get("closing_originals_holder")
        if holder:
            return f"📁 Ориг. у {'ГД' if holder == 'gd' else 'мен.'}"
        return "⏳"

    def _fmt_task_payment_date(self, val: Any) -> str:
        # payload.payment_date может прийти в ISO (`2026-02-06[T...]`) или в
        # DD.MM.YYYY (так писал backfill 2026-05-12). Возвращаем DD.MM.YYYY
        # без падений в обоих случаях; raw — последний fallback.
        if not val:
            return ""
        s = str(val).strip()
        if re.match(r"^\d{2}\.\d{2}\.\d{4}$", s):
            return s
        try:
            return format_dt_iso(s, self.cfg.timezone_name)
        except (ValueError, TypeError):
            return s

    def _task_payload_fields(self, task: dict[str, Any]) -> dict[str, str]:
        payload = try_json_loads(task.get("payload_json"))
        sender = (
            payload.get("sender_username")
            or payload.get("manager_username")
            or payload.get("installer_username")
            or payload.get("accounting_username")
            or ""
        )
        if sender and not str(sender).startswith("@"):
            sender = f"@{sender}"

        return {
            "comment": str(payload.get("comment") or ""),
            "measurements": str(payload.get("measurements") or ""),
            "issue_type": str(payload.get("issue_type") or ""),
            "doc_type": str(payload.get("doc_type") or ""),
            "details": str(payload.get("details") or ""),
            "payment_amount": self._fmt_amount(payload.get("payment_amount")),
            "payment_method": str(payload.get("payment_method") or ""),
            "payment_type": str(payload.get("payment_type") or payload.get("payment_stage") or ""),
            "payment_date": self._fmt_task_payment_date(payload.get("payment_date")),
            "invoice_number": str(payload.get("invoice_number") or ""),
            "sign_type": str(payload.get("sign_type") or ""),
            "source": str(payload.get("source") or ""),
            "sender": str(sender),
        }

    # ---------- internal sync methods (thread) ----------

    def _get_client(self) -> gspread.Client:
        if self._gc:
            return self._gc

        if self.cfg.service_account_file:
            self._gc = gspread.service_account(filename=self.cfg.service_account_file)
            return self._gc

        if not self.cfg.service_account_json:
            raise RuntimeError("Google Sheets enabled, but GOOGLE_SERVICE_ACCOUNT_JSON/FILE is not set")

        info = encode_sa_json(self.cfg.service_account_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        self._gc = gspread.authorize(creds)
        return self._gc

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet:
            return self._spreadsheet
        gc = self._get_client()
        self._spreadsheet = gc.open_by_key(self.cfg.spreadsheet_id)
        return self._spreadsheet

    def _get_or_create_ws(self, title: str, header: list[str]) -> gspread.Worksheet:
        ws = self._worksheets.get(title)
        if ws is None:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(title)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(header) + 2))
            self._worksheets[title] = ws

        if title not in self._headers_ready:
            # Расширяем лист если столбцов меньше чем заголовков
            needed_cols = len(header)
            if ws.col_count < needed_cols:
                ws.resize(cols=needed_cols + 2)
            values = ws.row_values(1)
            if values[: len(header)] != header:
                ws.update([header], "A1")
            self._headers_ready.add(title)
        return ws

    def _get_row_index(self, title: str, ws: gspread.Worksheet) -> dict[str, int]:
        row_index = self._row_indexes.get(title)
        if row_index is not None:
            return row_index

        key_col = self._KEY_COL.get(title, 1)
        col_values = ws.col_values(key_col)
        row_index = {}
        empty_rows: list[int] = []
        for row_num, value in enumerate(col_values[1:], start=2):
            key = str(value).strip()
            if key and key not in row_index:
                row_index[key] = row_num
            elif not key:
                empty_rows.append(row_num)
        self._row_indexes[title] = row_index
        # gap-fill: новые ключи сначала идут в пустые row'ы (если они есть),
        # затем — в конец листа. Это предотвращает «дыры» при удалении
        # invoice'ов или ручной правке листа.
        if not hasattr(self, "_empty_rows"):
            self._empty_rows = {}
        self._empty_rows[title] = empty_rows
        self._next_rows[title] = max(2, len(col_values) + 1)
        return row_index

    def _get_or_allocate_row(self, title: str, ws: gspread.Worksheet, key: Any) -> tuple[int, bool]:
        key_str = str(key).strip()
        if not key_str:
            raise ValueError("sheet row key is required")

        row_index = self._get_row_index(title, ws)
        existing = row_index.get(key_str)
        if existing is not None:
            return existing, False

        # gap-fill first
        empty_rows = getattr(self, "_empty_rows", {}).get(title, [])
        if empty_rows:
            row = empty_rows.pop(0)
            row_index[key_str] = row
            return row, True

        row = self._next_rows.get(title, 2)
        row_index[key_str] = row
        self._next_rows[title] = row + 1
        return row, True

    @staticmethod
    def _chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    def _flush_batch_update(
        self,
        ws: gspread.Worksheet,
        batch_data: list[dict[str, Any]],
        *,
        chunk_size: int = 200,
    ) -> None:
        if not batch_data:
            return
        for chunk in self._chunked(batch_data, chunk_size):
            ws.batch_update(chunk, value_input_option="USER_ENTERED")

    @staticmethod
    def _row_range(row: int, width: int) -> str:
        end_col = GoogleSheetsService._col_letter(width - 1)
        return f"A{row}:{end_col}{row}"

    def _project_row_values(self, project: dict[str, Any], manager_label: str = "") -> list[Any]:
        return [
            project.get("code") or "",
            project.get("title") or "",
            project.get("address") or "",
            project.get("client") or "",
            self._fmt_amount(project.get("amount")),
            format_date_iso(project.get("deadline"), self.cfg.timezone_name),
            project_status_label(str(project.get("status") or "")),
            project.get("manager_id") or "",
            manager_label,
            format_dt_iso(project.get("created_at"), self.cfg.timezone_name),
            format_dt_iso(project.get("updated_at"), self.cfg.timezone_name),
            project.get("amo_lead_id") or "",
        ]

    def _lead_row_values(
        self,
        lead: dict[str, Any],
        *,
        status_name: str = "",
        amo_user_map: dict[int, str] | None = None,
    ) -> list[Any]:
        # Дата: «DD.MM.YYYY HH:MM» (МСК). Время важно для отслеживания
        # когда пришла заявка — раньше обрезалось до DD.MM.YYYY.
        date_str = format_dt_iso(lead.get("created_at"), self.cfg.timezone_name)

        # Имя клиента: из контакта amoCRM
        client_name = lead.get("contact_name") or ""

        # Имя: название лида
        name = lead.get("name") or ""

        # Телефон: убираем все нецифровые символы (+, пробелы, дефис) —
        # иначе Google Sheets интерпретирует «+7 926 662-51-06» как формулу
        # и показывает #ERROR! (Formula parse error). Старые avito-лиды
        # шли как «79254800662» без + → формула норм. Webhook 25.05 начал
        # сохранять отформатированный номер → fix нормализуем при выводе.
        phone_raw = lead.get("phone") or ""
        phone = re.sub(r"\D", "", str(phone_raw)) if phone_raw else ""

        # Менеджер: РП «Импорт ОП» главнее (сматченные лиды), иначе amo responsible_user_id → role code
        manager = ""
        resp_id = lead.get("responsible_user_id")
        if resp_id and amo_user_map:
            manager = amo_user_map.get(int(resp_id), "")
        rp_manager = (lead.get("rp_manager") or "").strip()
        if rp_manager:
            manager = rp_manager

        # Источник: РП «Импорт ОП» главнее, иначе custom field "Источник", иначе первый тег
        source = (lead.get("rp_source") or "").strip() or (lead.get("source") or "")
        if not source:
            tags_raw = lead.get("tags_json")
            if tags_raw:
                try:
                    import json
                    tags = json.loads(tags_raw)
                    if tags:
                        source = str(tags[0])
                except (json.JSONDecodeError, IndexError):
                    pass

        # Статус: РП «Импорт ОП» главнее (сматченные лиды), иначе amoCRM mapped name, иначе status_id
        status = (lead.get("rp_status") or "").strip() or status_name or ""
        if not status:
            sid = lead.get("status_id")
            status = str(sid) if sid else ""

        # Сумма сделки
        price = lead.get("price")
        price_str = ""
        if price:
            try:
                price_str = f"{float(price):,.0f}"
            except (ValueError, TypeError):
                price_str = str(price)

        # Примечание: последняя заметка ответственного из amoCRM.
        # Переводы строк → пробел, чтобы строка листа не разъезжалась.
        note_raw = lead.get("last_note") or ""
        note = re.sub(r"\s+", " ", str(note_raw)).strip()

        return [date_str, client_name, name, phone, manager, source, status, price_str, note]

    def _task_row_values(self, task: dict[str, Any], project_code: str = "") -> list[Any]:
        payload = self._task_payload_fields(task)
        return [
            task.get("id") or "",
            project_code,
            task_type_label(task.get("type")),
            task_status_label(task.get("status")),
            task.get("assigned_to") or "",
            task.get("created_by") or "",
            format_dt_iso(task.get("due_at"), self.cfg.timezone_name),
            format_dt_iso(task.get("created_at"), self.cfg.timezone_name),
            format_dt_iso(task.get("updated_at"), self.cfg.timezone_name),
            payload["comment"],
            payload["measurements"],
            payload["issue_type"],
            payload["doc_type"],
            payload["details"],
            payload["payment_amount"],
            payload["payment_method"],
            payload["payment_type"],
            payload["payment_date"],
            payload["invoice_number"],
            payload["sign_type"],
            payload["source"],
            payload["sender"],
        ]

    def _invoice_cells(
        self,
        invoice: dict[str, Any],
        manager_label: str,
        cost: dict[str, Any] | None,
        *,
        row: int,
        is_new: bool,
        current_bs: str = "",
        current_br: str = "",
        current_bt: str = "",
        current_bu: str = "",
        advance: dict[str, Any] | None = None,
    ) -> dict[int, Any]:
        _ROLE_LABELS = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }
        _c = cost or {}
        _li = invoice.get("_lead_info") or {}
        _inv_num = invoice.get("invoice_number") or ""

        _role_label = _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or "")

        # LEAD-строки: базовые колонки + лид-колонки (индексы 87-113)
        if str(_inv_num).startswith("LEAD-"):
            cells: dict[int, Any] = {
                0: row - 1,   # № — сквозная нумерация
                1: _role_label,  # Роль
                2: manager_label,
                8: _inv_num,
                86: row - 1,  # № п/п — сквозная нумерация
            }
            for _i, _suf in enumerate(("kv", "kia", "npn")):
                _base = 87 + _i * 10
                cells[_base]     = invoice.get(f"lead_{_suf}_num") or ""
                cells[_base + 1] = invoice.get(f"lead_{_suf}_source") or _li.get(f"source_{_suf}") or ""  # Источник
                cells[_base + 2] = self._fmt_sheet_date(invoice.get(f"lead_{_suf}_date"))
                cells[_base + 3] = invoice.get(f"lead_{_suf}_name") or ""
                cells[_base + 4] = invoice.get(f"lead_{_suf}_phone") or ""
                cells[_base + 5] = invoice.get(f"lead_{_suf}_address") or ""
                cells[_base + 6] = invoice.get(f"inv_{_suf}_num") or ""
                cells[_base + 7] = invoice.get(f"inv_{_suf}_phone") or ""
                cells[_base + 8] = invoice.get(f"inv_{_suf}_address") or ""
                cells[_base + 9] = self._fmt_sheet_date(invoice.get(f"inv_{_suf}_date"))
            return cells

        # Credit fully closed: бухгалтерия в credit-flow не участвует, поэтому
        # edo_signed нерелевантно. Достаточно того, что монтаж прошёл финальную
        # стадию (montazh_stage='invoice_end') — остальные побочные эффекты
        # (no_debts, installer_ok) выставляются _auto_close_credit_invoice.
        _credit_fully_closed = bool(invoice.get("is_credit")) and (
            invoice.get("montazh_stage") == "invoice_end"
        )
        _is_status_ended = invoice.get("status") == "ended" or _credit_fully_closed
        _zp_installer_status = invoice.get("zp_installer_status") or ""
        # AZ «Этап монтажа» (col 51, отдельно от AU «Статус» 46):
        #  • после назначения ОБЕ монт. группы (montazh_stage='assigned') → «В работе»;
        #  • наёмная (edo_task_id=2) после «Монтаж ОК» (installer_ok) → «Счет End» —
        #    ТОЛЬКО отображение: стадию invoice_end НЕ ставим, иначе кредитный счёт
        #    пометится «полностью закрыт» (_credit_fully_closed) и спрячет столбцы.
        #  • кредитные — та же логика (отдельного ветвления нет).
        _mz_stage = invoice.get("montazh_stage", "") or ""
        if _is_status_ended:
            _az_montazh = "Счет End"
        elif invoice.get("edo_task_id") == 2 and invoice.get("installer_ok"):
            _az_montazh = "Счет End"
        elif _mz_stage == "assigned":
            _az_montazh = "В работе"
        else:
            _az_montazh = _MONTAZH_STAGE_LABELS.get(_mz_stage, _mz_stage)
        cells: dict[int, Any] = {
            0: row - 1,   # № п/п — сквозная нумерация
            1: _role_label,  # Роль
            2: manager_label,
            3: "Да" if invoice.get("edo_signed") else "",
            4: invoice.get("client_name") or "",
            5: invoice.get("traffic_source") or "",        # F Ист.трафика
            6: "0" if invoice.get("is_credit") else "1",  # ОП convention: 0=кредит, 1=б.н.
            7: {"own": 1, "gd_lead": 2}.get(invoice.get("client_source", ""), "")
               or invoice.get("client_type") or "",
            8: invoice.get("invoice_number") or "",
            9: invoice.get("object_address") or "",
            10: self._fmt_sheet_date(invoice.get("receipt_date")),
            11: f'={int(invoice.get("deadline_days"))}' if invoice.get("deadline_days") else "",  # L Сроки (число дней)
            13: self._fmt_sheet_date(invoice.get("actual_completion_date")),
            14: self._fmt_amount(invoice.get("amount")),
            15: self._fmt_amount(invoice.get("first_payment_amount")),
            25: self._fmt_amount(invoice.get("surcharge_amount")),       # Z Сумма допл
            26: invoice.get("payment_confirm_status") or "",             # AA Допл подтв
            27: self._fmt_sheet_date(invoice.get("surcharge_date")),     # AB Дата допл
            28: self._fmt_amount(invoice.get("final_surcharge_amount")), # AC Оконч допл
            29: self._fmt_sheet_date(invoice.get("final_surcharge_date")), # AD Дата оконч
            30: f"=O{row}-P{row}-Z{row}-AC{row}",                          # AE Долг
            31: self._fmt_docs_primary(invoice),                        # AF Договор
            32: self._fmt_docs_closing(invoice),                        # AG Закр.док
            35: self._fmt_amount(invoice.get("manager_zp_blank")),   # AJ ← ОП AG
            36: _ZP_STATUS_LABELS.get(invoice.get("zp_manager_status") or "", invoice.get("zp_manager_status") or ""),
            # AL «тех»: приоритет AI ОП («Запрос НОВЫЙ», zp_manager_request_amount)
            # → fallback на AH ОП («Запрос тех», zp_manager_request_text legacy).
            # NB: явно проверяем > 0, т.к. _fmt_amount(0)='0' — truthy.
            37: (
                self._fmt_amount(invoice.get("zp_manager_request_amount"))
                if (invoice.get("zp_manager_request_amount") or 0) > 0
                else (invoice.get("zp_manager_request_text") or "")
            ),
            38: self._fmt_amount(invoice.get("agent_payout_op")),   # AM ← ОП AE
            39: self._fmt_an_payout(invoice, row),  # AN ← ОП AJ + CN (удержание)
            40: self._fmt_sheet_date(invoice.get("zp_manager_payout_date")),  # AO ← ОП AK
            42: self._fmt_amount(invoice.get("rp_request_op")),     # AQ Запрос РП ← ОП AU
            43: self._fmt_amount(invoice.get("rp_payout_op")),     # AR Выдано РП ← ОП AV
            44: self._fmt_sheet_date(invoice.get("rp_payout_date_op")),  # AS Дата РП ← ОП AW
            # AU Статус: зеркалит AZ «Этап монтажа» (_az_montazh) — по запросу user'а
            # 31.05 (после назначения «В работе»; наёмная после «Монтаж ОК» «Счет End»).
            46: _az_montazh,
            47: _ROLE_LABELS.get(invoice.get("creator_role", ""), invoice.get("creator_role") or ""),
            48: invoice.get("supplier") or "",
            49: invoice.get("material_type") or "",
            50: invoice.get("invoice_number") or "",
            # AZ Этап монтажа: см. _az_montazh выше (после назначения «В работе»,
            # наёмная после «Монтаж ОК» → «Счет End»). AU (46) НЕ меняем.
            51: _az_montazh,
            # BA Монтажник ОК: счёт закрыт (status=ended) ИЛИ монтажник принял работу
            # (installer_ok), ИЛИ ЗП-цикл монтажника пошёл (approved/confirmed/payment_sent).
            52: "Да" if (
                _is_status_ended
                or invoice.get("installer_ok")
                or float(invoice.get("montazh_agreed_amount") or 0) > 0
                or _zp_installer_status in ("approved", "confirmed", "payment_sent")
                # монтажник ПРИНЯЛ работу: стадия in_work и выше → «Да».
                # НЕ 'assigned' (РП только назначил, монтажник ещё не нажал «принять») — user 16.06.
                or _mz_stage in ("in_work", "razmery_ok", "invoice_ok", "invoice_end")
            ) else "",
            53: "Да" if invoice.get("no_debts") else "",
            54: "кредит" if invoice.get("is_credit") else "б.н.",  # BC Система оплаты
            55: "",  # очистка (перенесено в 75)
            56: "",  # BE «Статус» монтажа — лестница, вычисляется в ЗП-блоке ниже (user 2026-06-05)
            59: format_dt_iso(invoice.get("created_at"), self.cfg.timezone_name),
            60: format_dt_iso(invoice.get("updated_at"), self.cfg.timezone_name),
            # — Статусы жизненного цикла —
            # 61-66: не используются
            67: "Да" if invoice.get("status") == "in_progress" or (invoice.get("status") == "credit" and not _credit_fully_closed) else "", # BP В работе
            68: "Да" if invoice.get("status") == "ended" or _credit_fully_closed else "",  # BQ Счет END
            # 69 (BR), 71 (BT), 72 (BU) заполняются ниже immutable-блоком
            # рядом с BS (правило feedback_br_bs_bt_bu_immutable_op_source).
            73: _li.get("lead_status", ""),   # BV Статус лида
            # — Блок Замерщик (перенос из 54/55/69) —
            74: _ZP_STATUS_LABELS.get(invoice.get("zp_status") or "", invoice.get("zp_status") or ""),  # BW ЗП Замерщик
            75: self._fmt_amount(invoice.get("zp_zamery_total")),        # BX ЗП Замерщик сумма
            76: invoice.get("zamery_info_op") or invoice.get("_zamery_info") or "",  # BY Замеры ← ОП I (fallback: бот)
            # — Аналитика —
            77: invoice.get("_plan_fact_label") or "",                   # BZ Расчет vs Факт
            # 78, 79 заполняются ниже из cost_card
            # Кредитный учёт (99-104, CV–DA) вынесен на лист «Авансирование
            # сотрудников» (owner 2026-06-22) — очищается ниже единым блоком.
        }

        # — Сквозная нумерация (86) —
        cells[86] = row - 1  # № п/п

        # — AW ID монтажника (48) — telegram_id монтажника по счёту.
        # Приоритет: invoice.assigned_to → _installer_id (fallback из tasks/installer_ok_by).
        # _installer_id обогащается в integration_hub и sheets_sync (см. db.get_installer_id_for_invoice).
        # Наёмники (монт. группа 2, без tg_id) → текстовая метка вместо ID.
        # Метка группы 2 хранится в edo_task_id (переназначенный пустой столбец).
        if invoice.get("edo_task_id") == 2:
            cells[48] = "Наёмники"
        else:
            cells[48] = invoice.get("assigned_to") or invoice.get("_installer_id") or ""
        cells[55] = cells[48]  # BD: дубль AW для удобства ручного просмотра

        # — Менеджерский блок (87-98) —
        # Таймстемпы монтажных стадий
        cells[87] = self._fmt_sheet_date(invoice.get("montazh_assigned_at"))
        cells[88] = self._fmt_sheet_date(invoice.get("montazh_in_work_at"))
        cells[89] = self._fmt_sheet_date(invoice.get("montazh_razmery_ok_at"))
        cells[90] = self._fmt_sheet_date(invoice.get("montazh_invoice_ok_at"))
        # CN/CO: ОП CF/CG (owner 2026-06-22). Ранее payment_confirmed_by/at —
        # убраны с листа (ГД-only ID + дата не нужны; БД-поля и расчёты целы).
        cells[91] = self._fmt_amount(invoice.get("zp_manager_hold"))
        cells[92] = self._fmt_amount(invoice.get("cost_diff_calc_fact"))
        # Аудит ЗП менеджера
        cells[93] = invoice.get("zp_manager_approved_by") or ""
        # Аудит ЭДО
        cells[94] = self._fmt_sheet_date(invoice.get("docs_edo_signed_at"))
        cells[95] = invoice.get("docs_edo_signed_by") or ""
        # Статус заказа материалов
        cells[96] = invoice.get("profile_order_status") or ""
        cells[97] = invoice.get("metal_order_status") or ""
        # Связка лид→счёт
        cells[98] = invoice.get("lead_tracking_id") or ""

        # Кредитный учёт (CV–DA, 99–104): owner 2026-06-22 — кредитный кошелёк
        # ведётся ТОЛЬКО на листе «Авансирование сотрудников» (CREDIT_SUMMARY/
        # CREDIT_JOURNAL, источник db.list_all_credit_events), а НЕ в Invoices.
        # Бот безусловно очищает эти колонки при каждом sync — старые значения
        # (CV–CZ = данные, DA = бывшая формула «=IF(CV;;CV-CX)») уходят, новых
        # не пишем (тот же паттерн, что блок авансов DB–DF ниже).
        # is_credit нужен ниже по методу (cost-блок ~1017/1029) — сохраняем.
        is_credit = bool(invoice.get("is_credit"))
        cells[99] = ""   # CV  (было «Кредит вход»)
        cells[100] = ""  # CW  (было «Кредит вход коммент»)
        cells[101] = ""  # CX  (было «Кредит расход»)
        cells[102] = ""  # CY  (было «Дата расход кред»)
        cells[103] = ""  # CZ  (было «Кредит назначение»)
        cells[104] = ""  # DA  (была формула «=IF(CV;;CV-CX)»)

        # DB-DF (cells[105..109]) — 2026-05-20 v6 (правило user'а):
        # Данные по авансированию идут на ОТДЕЛЬНЫЙ лист «Авансирование сотрудников»,
        # а не в Invoices. Бот безусловно очищает эти 5 колонок при каждом sync —
        # старые значения уходят, новых не пишем.
        cells[105] = ""  # DB
        cells[106] = ""  # DC
        cells[107] = ""  # DD
        cells[108] = ""  # DE
        cells[109] = ""  # DF

        # Расч.мат., Установка, Грузчики, Логистика — из БД
        est_glass = float(invoice.get("estimated_glass") or 0)
        est_profile = float(invoice.get("estimated_profile") or 0)
        est_mat_legacy = float(invoice.get("estimated_materials") or 0)
        est_inst = float(invoice.get("estimated_installation") or 0)
        est_load = float(invoice.get("estimated_loaders") or 0)
        est_log = float(invoice.get("estimated_logistics") or 0)
        materials_total = est_glass + est_profile + est_mat_legacy
        if any([est_glass, est_profile, est_mat_legacy, est_inst, est_load, est_log]):
            # Q «Расч.мат.» — ЗЕРКАЛО «Импорт ОП» M, один-в-один.
            # Стекло/профиль в ОП НЕТ (это поля бота), подмешивать их сюда
            # запрещено: «данные из импорт оп парсить не искажая их и не
            # перемешивая» (owner 2026-07-28, правило feedback_op_mirror_no_mixing).
            # Было: _fmt_amount(materials_total) — давало 239 200 вместо 124 000
            # у 26721-1НПН (единственный счёт со «стеклом»).
            # materials_total ниже НЕ трогаем — на нём считаются НДС/прибыль.
            cells[16] = self._fmt_amount(invoice.get("estimated_materials"))
            cells[17] = self._fmt_amount(est_inst)
            cells[18] = self._fmt_amount(est_load)
            cells[19] = self._fmt_amount(est_log)

        # Python-вычисления: НДС, Нал.приб., Прибыль, Рент-ть (вместо формул)
        _amount = float(invoice.get("amount") or 0)
        _est_total = materials_total + est_inst + est_load + est_log
        if is_credit:
            _nds = 0
            _profit_tax = 0
        else:
            _nds = (_amount * 22 / 122) - (materials_total * 22 / 122) if _amount else 0
            _profit_tax = ((_amount - _est_total - _nds) / 100 * 20) if _amount else 0
        _profit = _amount - _est_total - _nds - _profit_tax
        _rentability = (_profit / _amount * 100) if _amount > 0 else 0

        cells[21] = self._fmt_amount(_nds)                                     # V НДС
        cells[22] = self._fmt_amount(_profit_tax)                              # W Нал.приб.
        _profit_op = float(invoice.get("profit_calc_op") or 0)
        if is_credit and not _profit_op:
            _profit_op = float(invoice.get("profit_tax") or 0)                 # Q Прибыль кред. (из ОП)
        cells[62] = self._fmt_amount(_profit_op)                               # BK Расчетная прибыль (из ОП)
        cells[20] = cells[62]                                                  # U Прибыль = BK (дубль 1-в-1)
        # X Рент-ть расч: из ОП (rentability_calc) если есть, иначе Python-расчёт
        _rent_op = invoice.get("rentability_calc")
        if _rent_op is not None and _rent_op != 0:
            cells[23] = f"{float(_rent_op):.0f}%"
        elif _amount > 0:
            cells[23] = f"{_rentability:.1f}%"
        else:
            cells[23] = ""
        cells[41] = self._fmt_amount(invoice.get("npn_amount"))                  # AP НПН 10% ← ОП AT

        # BT «Материалы Факт» — пишется immutable-блоком ниже (рядом с BS).
        # Старая логика «_mat_op + _mat_children» отменена правилом
        # feedback_br_bs_bt_bu_immutable_op_source (2026-05-20): BT строго =
        # materials_fact_op 1:1 из Импорт ОП (AM), без суммирования с children.
        # sp-затраты по-прежнему идут в BG через sum(cost_*) — это не BT.

        # BJ — ЗП всегда; BS Монтаж Факт — только после approved
        _mont_zp = float(invoice.get("zp_installer_amount") or 0)
        _zp_status = invoice.get("zp_installer_status") or ""

        # Y «Рент-ть факт» — ЗЕРКАЛО «Импорт ОП» W (rentability_fact_op). Owner 28.07:
        # «эти данные бот должен парсить с листа импорт оп»; подтверждено 29.07.
        # Раньше сюда шёл margin_pct из cost-card — поэтому ЛЮБОЙ полный синк затирал
        # ОП-значение (лист чистится целиком, а защита read-before-clear есть только у
        # BR/BS/BT/BU). Инцидент 28.07 19:27: Y переписана расчётом на всех 33 строках.
        # Пишем безусловно, без гейта _fact_visible: это зеркало, а не «факт»-показатель,
        # искажать и обрезать его нельзя (feedback_op_mirror_no_mixing). ОП «0%» → «0%»,
        # пустое ОП (NULL) → пусто.
        # ⚠️ BL/BM/BN/BO ниже по-прежнему из cost-card — источники соседних колонок
        # РАЗНЫЕ намеренно, сверять их одним источником нельзя
        # (feedback_fact_columns_sources).
        _rent_fact_op = invoice.get("rentability_fact_op")
        cells[24] = f"{float(_rent_fact_op):.0f}%" if _rent_fact_op is not None else ""

        # BL/BM/BN/BO — «факт»-показатели по умолчанию пусты; заполняются только ниже
        # при наличии cost-card И статуса ended/credit.
        cells[63] = ""
        cells[64] = ""
        cells[65] = ""
        cells[66] = ""

        # BF «Оплаты пост. итого»: сумма уже оплаченная заказчиком —
        # одинаковая формула для credit и безналичных (для credit заказчик
        # платит товаром, но outstanding_debt всё равно отслеживает остаток).
        _bf = (invoice.get("amount") or 0) - (invoice.get("outstanding_debt") or 0)
        cells[57] = self._fmt_amount(_bf) if _bf > 0 else ""

        # BG «Расходы итого» (правка user 2026-06-15):
        #   • Видимость = прежний гейт _fact_visible: показываем только на «Счет ОК»/
        #     «Счет End»/закрытом; для счетов «в работе» — пусто (user 15.06, ответ 2).
        #   • Значение = СТРОГАЯ формула суммы 4 видимых столбцов:
        #     BR (Грузчики факт) + BS (Монтаж Факт) + BT (Материалы Факт) + BU (Логистика факт),
        #     одинаково для credit и non-credit (user 15.06, ответ A).
        #   • CF-гейта НЕТ: даже при пустом CF BG = формула (user 15.06, ответ 1 —
        #     отменяет прежний план «только где есть данные в CF»).
        #   • BR/BT/BU (ОП-факт, immutable) и BS не меняются (user 15.06, ответы B/C);
        #     пустые ячейки в +-формуле = 0; монтаж входит только через BS
        #     (пусто, пока ЗП монтажа не выплачена полностью).
        # _fact_visible нужен и для BG, и ниже для Y/BL-BO.
        _fact_visible = _is_status_ended or _mz_stage in ("invoice_ok", "invoice_end")
        if not _fact_visible:
            cells[58] = ""
        else:
            cells[58] = f"=BR{row}+BS{row}+BT{row}+BU{row}"

        # Y «Рент-ть факт» (col 24) + BL-BO (BL Прибыль / BM Перерасчёт / BN НДС / BO Налог факт):
        # считаются с этапа «Счет ОК» (montazh_stage='invoice_ok') и далее — по запросу
        # user'а 2026-06-08 (сначала BL-BO, затем тем же запросом и Y «Рент-ть факт»).
        # Раньше — только для полностью закрытых счетов (_is_status_ended).
        # Пересчёт на «Счет End»/ended происходит АВТОМАТИЧЕСКИ: лист каждый синк
        # пересобирается из свежей cost-card, значения не «замораживаются».
        # ⚠️ _is_status_ended менять нельзя — он шарится с BP/BQ и CA-CE; поэтому
        # отдельный гейт _fact_visible (определён выше, перед BG): invoice_ok входит,
        # invoice_end/ended тоже.

        if _c:
            fact_margin = _c.get("margin", 0)
            if _fact_visible:
                cells[65] = self._fmt_amount(_c.get("nds_fact"))         # BN НДС факт
                cells[66] = self._fmt_amount(_c.get("profit_tax_fact"))  # BO Налог на приб. факт
                if fact_margin:
                    cells[63] = self._fmt_amount(fact_margin)              # BL Прибыль факт
                    if _profit_op and _profit_op > fact_margin:
                        cells[64] = self._fmt_amount(fact_margin - _profit_op)  # BM Перерасчет
                    else:
                        cells[64] = ""
                else:
                    cells[63] = ""
                    cells[64] = ""
            else:
                cells[65] = ""
                cells[66] = ""
                cells[63] = ""
                cells[64] = ""

        # CA-CE: остатки к закупке (план Q-T минус факт). Отрицательное = перерасход.
        # Пишется для счетов реально «в работе»:
        #   status IN ('in_progress', 'credit') И NOT _is_status_ended
        # (т.е. для credit-счетов исключаем те, где montazh_stage='invoice_end' —
        # они фактически закрыты, висят credit только до auto-close).
        # Триггеры локирования: montazh_invoice_ok_at → CA=0; запрос ЗП → CB=0.
        _est_mat = float(invoice.get("estimated_materials") or 0)
        _est_inst = float(invoice.get("estimated_installation") or 0)
        _est_load = float(invoice.get("estimated_loaders") or 0)
        _est_log = float(invoice.get("estimated_logistics") or 0)
        _fact_mat = (
            float(invoice.get("cost_metal") or 0)
            + float(invoice.get("cost_glass") or 0)
            + float(invoice.get("cost_extra_mat") or 0)
        )
        # Факт монтажа: AN из «Импорт ОП» (montazh_fact_op) имеет ПРИОРИТЕТ
        # как источник истины. Если AN пусто — используем confirmed zp_amount.
        # approved/payment_sent в _fact_inst НЕ учитываются (см. BS блок и BG).
        _mfo_local = float(invoice.get("montazh_fact_op") or 0)
        _zia_local = float(invoice.get("zp_installer_amount") or 0)
        _zis_local = invoice.get("zp_installer_status")
        if _mfo_local:
            _fact_inst = _mfo_local
        elif _zis_local == "confirmed" and _zia_local > 0:
            _fact_inst = _zia_local
        else:
            _fact_inst = 0.0
        _fact_load = float(invoice.get("cost_loaders") or 0) + float(invoice.get("loaders_fact_op") or 0)
        _fact_log = float(invoice.get("cost_logistics") or 0) + float(invoice.get("logistics_fact_op") or 0)

        # Триггеры фиксации остатков в 0:
        # — Материал считается полностью закупленным: (а) монтажник нажал «Счёт OK»
        #   (montazh_invoice_ok_at), ЛИБО (б) факт. затраты CF (=DP+DQ+DU+DV) превысили
        #   65% расчётной стоимости материалов Q=estimated_materials (user-req 2026-06-16).
        #   До этого момента — обычный расчёт план−факт.
        # — Монтаж считается окончательно согласованным после запроса ЗП монтажника
        #   (zp_installer_status в requested/approved/confirmed/paid либо zp_installer_requested_at).
        _mat_locked = bool(invoice.get("montazh_invoice_ok_at"))
        # (б) CF = факт затрат на материалы (_fact_mat + cost_extra_svc/тонировка) > 65% от Q.
        # Порог строгий (>). При срабатывании материалы в предстоящих затратах (CA) не учитываем.
        _cf_spend = _fact_mat + float(invoice.get("cost_extra_svc") or 0)
        _mat_bought_by_spend = _est_mat > 0 and _cf_spend > 0.65 * _est_mat
        _inst_zp_requested = (
            (_zis_local in ("requested", "approved", "confirmed", "paid"))
            or bool(invoice.get("zp_installer_requested_at"))
        )
        if _mat_locked or _mat_bought_by_spend:
            _rem_mat = 0.0 if _est_mat > 0 else None
        else:
            _rem_mat = (_est_mat - _fact_mat) if _est_mat > 0 else None
        # CB «Установка ост.» — база = сумма СОГЛАСОВАНИЯ ЗП монтаж (montazh_agreed —
        # что показывается монтажнику и реально платится за установку), НЕ валовой
        # estimated_installation (user 2026-06-16). Fallback, если ещё не согласовано —
        # расчёт как у монтажника (installer_new._calc_est_montazh: R×0.67 б/н +10% /
        # R×0.95 кредит, округл. к ближайшей 1000).
        _inst_credit = bool(invoice.get("is_credit")) or str(invoice.get("invoice_number") or "").upper().startswith("ЗМ")
        _inst_agreed = float(invoice.get("montazh_agreed_amount") or 0)
        if _inst_agreed <= 0 and _est_inst > 0:
            _coef = 0.95 if _inst_credit else 0.67
            _base = int((_est_inst * _coef + 500) // 1000) * 1000
            _inst_agreed = _base if _inst_credit else int((_base * 1.10 + 500) // 1000) * 1000
        if _inst_zp_requested:
            _rem_inst = 0.0 if _inst_agreed > 0 else None
        else:
            _rem_inst = (_inst_agreed - _fact_inst) if _inst_agreed > 0 else None
        _rem_load = (_est_load - _fact_load) if _est_load > 0 else None
        _rem_log = (_est_log - _fact_log) if _est_log > 0 else None
        if invoice.get("status") in ("in_progress", "credit") and not _is_status_ended:
            cells[78] = self._fmt_amount(_rem_mat) if _rem_mat is not None else ""   # CA
            cells[79] = self._fmt_amount(_rem_inst) if _rem_inst is not None else ""  # CB
            cells[80] = self._fmt_amount(_rem_load) if _rem_load is not None else ""  # CC
            cells[81] = self._fmt_amount(_rem_log) if _rem_log is not None else ""    # CD
            # CE Итого осталось — сумма ненулевых остатков (с учётом знака: перерасход в одной
            # категории уменьшает итог, может стать отрицательным = общий перерасход)
            _rem_total = sum(x for x in (_rem_mat, _rem_inst, _rem_load, _rem_log) if x is not None)
            cells[82] = self._fmt_amount(_rem_total) if any(x is not None for x in (_rem_mat, _rem_inst, _rem_load, _rem_log)) else ""
        else:
            cells[78] = ""
            cells[79] = ""
            cells[80] = ""
            cells[81] = ""
            cells[82] = ""
        # CF (83) — формула суммы затрат: «Затр. Металл + Стекло + Доп.мат. + Доп.усл.»
        # DV (Затр. Доп усл. = cost_extra_svc) добавлен 15.06 по запросу user (КВ5/КВ7).
        cells[83] = f"=DP{row}+DQ{row}+DU{row}+DV{row}"

        # ── ЗП монтаж: Статус (BE) / Остаток (BJ) / Факт (BS) — user 2026-06-05 ──
        # Заменило прежние 5 веток BS и BJ=agreed−offset (feedback_bs_immutable).
        # «Выплачено» по счёту = НАИБОЛЬШЕЕ из каналов (НЕ сумма: выплата ботом
        # пишется на ВСЮ согласованную, аванс-зачёт учтён внутри неё, поэтому
        # суммирование задвоило бы — db.py set_invoice_zp_installer_status):
        #   • AN «Монтаж Факт» из «Импорт ОП» (montazh_fact_op);
        #   • аванс-зачёт Игоря (Σ offset, installer_advance_offset);
        #   • выплата ботом (zp_installer_amount при статусе payment_sent/confirmed —
        #     «Платёж отправлен» уже считается выплаченным, user 05.06).
        # BS «Монтаж Факт» = Выплачено, ТОЛЬКО если Выплачено ≥ Согласовано (вся
        #   сумма ИЛИ больше); иначе пусто.  BJ = Согласовано − Выплачено (остаток).
        #   ⟹ BS заполнен ⇒ BJ пусто (взаимоисключающие).
        # Ручной ввод BS на старых счетах без согласования в боте — сохраняем.
        _zp_installer_amount = float(invoice.get("zp_installer_amount") or 0)
        _zp_installer_status = invoice.get("zp_installer_status")
        _mfo_for_bs = float(invoice.get("montazh_fact_op") or 0)
        _advance_offset_for_bs = float(_c.get("installer_advance_offset") or 0)
        _current_bs_str = (current_bs or "").strip()
        _montazh_agreed = float(invoice.get("montazh_agreed_amount") or 0)
        # Нога ПРОШЛЫХ монтажных групп (объединение платежей 15.07 / «✏️ Внести сумму
        # ЗП монтаж» РП 16.07): Согласовано = paid_prev + доля текущей группы, поэтому
        # paid_prev СКЛАДЫВАЕТСЯ с текущей ногой, а с AN (накопитель ВСЕХ ног из
        # «Импорт ОП») конкурирует по max — суммирование с AN задвоило бы (канон
        # utils.py::_montazh_money_state / rp_new.py). paid_prev=0 → формула прежняя.
        _paid_prev = float(invoice.get("montazh_paid_prev") or 0)
        # База аванса ПРОШЛЫХ групп — снимок на момент назначения новой группы
        # (rp_new.py::_finalize_naem / _finalize_regroup, merged-ветка: adv_prev
        # пишется только при DR > 0). Этот аванс уже сидит внутри paid_prev через
        # DR, поэтому в канал «Выплачено» он второй раз входить не должен.
        _adv_prev = float(invoice.get("montazh_adv_prev") or 0)

        # CG «Аванс монтажника»: зачтённый аванс по счёту. Для б/н — с надбавкой
        # +10% (ЗП монтаж б/н = база+10%), для кредита — как есть (user 2026-06-08).
        _inv_is_credit = bool(invoice.get("is_credit")) or str(_inv_num).upper().startswith("ЗМ")
        _advance_cg = (
            _advance_offset_for_bs * 1.10
            if (_advance_offset_for_bs > 0 and not _inv_is_credit)
            else _advance_offset_for_bs
        )
        # Аванс ТЕКУЩЕЙ группы — ТОЛЬКО он идёт в «Выплачено». Канон бота:
        # installer_new.py::_advance_raw_cur + _advance_cg_amount (та же пара в
        # utils.py::resolve_installer_zp и td.py). Без вычета аванс прошлой группы
        # входил дважды — и через paid_prev (DR), и через CG — завышая BS и занижая BJ.
        # ⚠️ Колонка CG (cells[84]) показывает аванс по СЧЁТУ целиком, по всем
        # группам, и считается от _advance_cg — здесь она НЕ меняется
        # [[feedback_no_unauthorized_column_logic]].
        # adv_prev = 0 (обычный счёт, счёт без перебросок) → величины совпадают,
        # поведение прежнее.
        _advance_raw_cur = max(0.0, _advance_offset_for_bs - _adv_prev)
        _advance_cg_cur = (
            _advance_raw_cur * 1.10
            if (_advance_raw_cur > 0 and not _inv_is_credit)
            else _advance_raw_cur
        )

        _bot_paid = (
            _zp_installer_amount
            if _zp_installer_status in ("payment_sent", "confirmed")
            else 0.0
        )
        # Часть 2 (user 2026-06-08): если заявка ЗП = ОСТАТОК (zp_installer_remainder=1),
        # бот платит ОСТАТОК, а аванс зачтён ОТДЕЛЬНЫМ каналом → «Выплачено» =
        # аванс×1.10 + бот (ADDITIVE), счёт закрывается полностью (пример 2649-1КВ:
        # аванс 22 000 + бот 42 000 = 64 000 = Согласовано → BS заполнен, BJ пусто).
        # Старые заявки (флаг 0/NULL: бот платил ВСЮ согласованную, аванс «внутри») —
        # прежний max, без задвоения. ⛔ Меняет канон feedback_bs_immutable (флаг-гейт).
        _zp_remainder = bool(invoice.get("zp_installer_remainder"))
        # Канал DR «Затр. Монтаж» (cost_montazh) — owner 28.07: ЗП наёмной монтажной
        # группе может идти НЕСКОЛЬКИМИ платежами; они должны суммироваться по
        # МАТЕРИНСКОМУ счёту и после выплаты попадать в BS. DR и есть этот накопитель:
        # create_supplier_payment(material_type='montazh') прибавляет к нему КАЖДЫЙ
        # платёж (db.py::_COST_COL_MAP), тогда как zp_installer_amount — скаляр
        # ПОСЛЕДНЕГО платежа, а montazh_paid_prev — снимок DR на момент назначения
        # НОВОЙ группы (rp_new.py::_finalize_naem). Поэтому реконструкция
        # «paid_prev + последний платёж» не складывала транши ОДНОЙ группе.
        # Через max, НЕ сумму: DR уже включает выплаты, учтённые другими каналами,
        # сложение задвоило бы [[feedback_montazh_zp_multi_payment_sum]].
        # Инцидент: 26331-1НПН (Раушская) — две наёмные группы 48 000 + 32 100,
        # в BS попадало 64 200.
        _dr_paid = float(invoice.get("cost_montazh") or 0)
        if _zp_remainder and _bot_paid > 0:
            _paid = max(_mfo_for_bs, _paid_prev + _advance_cg_cur + _bot_paid, _dr_paid)
        else:
            _paid = max(_mfo_for_bs, _paid_prev + max(_advance_cg_cur, _bot_paid), _dr_paid)
        _fully_paid = _montazh_agreed > 0 and _paid >= _montazh_agreed - 0.001

        # BS «Монтаж Факт» (cells[70])
        if _montazh_agreed > 0:
            cells[70] = self._fmt_amount(_paid) if _fully_paid else ""
        elif _mfo_for_bs > 0:
            cells[70] = self._fmt_amount(_mfo_for_bs)   # legacy: AN без согласования в боте
        elif _current_bs_str and not _zp_installer_status:
            cells[70] = _current_bs_str                  # ручной ввод старых счетов
        else:
            cells[70] = ""

        # AZ «Этап монтажа» (cells[51]) + зеркальный AU «Статус» (cells[46]):
        # если в столбце BS «Монтаж Факт» положительная сумма (BS>0) → «Счет End»
        # (owner 23.06). Display-only override: стадию montazh_stage НЕ трогаем,
        # AZ/AU обратно с листа не читаются. BS-ячейка может быть ручным вводом с
        # разделителями («1 000») либо из _fmt_amount («1000») — парсим аккуратно.
        _bs_cell = str(cells.get(70) or "").strip()
        if _bs_cell:
            try:
                _bs_num = float(_bs_cell.replace("\xa0", "").replace(" ", "").replace(",", "."))
            except ValueError:
                _bs_num = 0.0
            if _bs_num > 0:
                cells[51] = "Счет End"   # AZ Этап монтажа
                cells[46] = "Счет End"   # AU Статус (зеркалит AZ)

        # BE «ЗП Монтажник статус» (cells[56]) — статус ПРОЦЕССА ЗП монтаж.
        # ⛔ НЕ этап работ — тот в AZ «Этап монтажа» (cells[51]). «В работе» здесь НЕ ставим
        #    (дублировало бы AZ). Только стадии ЗП (user 2026-06-05).
        # Лестница (позже перекрывает раньше):
        #   пусто (не взял в работу) → На согласовании → Согласовано → Запрошено → Оплачено.
        if _fully_paid or _zp_installer_status in ("payment_sent", "confirmed"):
            cells[56] = "Оплачено"
        elif _zp_installer_status in ("requested", "approved"):
            cells[56] = "Запрошено"
        elif _montazh_agreed > 0:
            cells[56] = "Согласовано"          # сумма ЗП согласована (rejected → откат сюда)
        elif invoice.get("installer_ok"):
            cells[56] = "На согласовании"      # взял в работу, сумма ЗП ещё НЕ согласована
        else:
            cells[56] = ""

        # BR/BT/BU ← Импорт ОП (БД = mirror Импорт ОП 1:1, без агрегации):
        #   BR ← AQ (loaders_fact_op), BT ← AM (materials_fact_op), BU ← AO (logistics_fact_op).
        # Никаких fallback на cost_*, materials_total, actual_logistics —
        # эти расчётные поля идут в BG/DP/DQ/DU, не в BR/BT/BU.
        # ⚠️ Все три db-first — источник (Импорт ОП) ПЕРЕЗАПИСЫВАЕТ значение листа;
        #   если источник пуст (=0) — значение листа СОХРАНЯЕМ (preserve, не затираем).
        #   Правило immutable-op-source feedback_br_bs_bt_bu_immutable_op_source ОТМЕНЕНО:
        #   BT 2026-06-16, BR/BU 2026-06-16 (ч.8, user-req «грузчики/логистика как BT»).
        _current_br_str = (current_br or "").strip()
        _loaders_for_br = float(invoice.get("loaders_fact_op") or 0)
        if _loaders_for_br > 0:
            cells[69] = self._fmt_amount(_loaders_for_br)
        elif _current_br_str:
            cells[69] = _current_br_str
        else:
            cells[69] = ""

        # BT — db-first: источник (materials_fact_op) перезаписывает лист (см. шапку выше).
        # owner 2026-08-04: если ОП AM пуст, а монтажники дали «Счёт ОК» — подставляем ФАКТ
        # из cost-card (metal + glass + extra_mat). И канон подстановки, и её приоритет взяты
        # из db.py::get_full_invoice_cost_card (там ровно тот же fallback: ОП AM главнее,
        # cost_* — только когда AM пуст), гейт — общий _fact_visible («Счёт ОК»/«Счёт End»/
        # закрыт), тот же, что уже стоит у BG/Y/BL-BO. Зеркало ОП не искажаем: при
        # заполненном AM ветка не срабатывает вовсе ([[feedback_op_mirror_no_mixing]]).
        # Смысл правки: BL «Прибыль факт» эти материалы уже вычитала через cost-card, а BT
        # показывала пусто — лист противоречил сам себе.
        # owner 2026-08-10, триггер 4: закупка считается состоявшейся и без «Счёт ОК»,
        # если траты (металл + стекло + доп. мат.) достигли >= 80% от Q «Расч.мат.»
        # (estimated_materials). Порог НЕстрогий (>=) — в отличие от строгого 65% у
        # CA «Расч.мат. ост.» (sheets.py:1184), там своя механика и её не трогаем.
        # _fact_mat (1153-1157) — ровно те же три статьи, что подставляет ветка ниже;
        # _est_mat (1149) — Q. Обе величины уже посчитаны выше в этой же функции.
        # Приоритет ОП AM не меняется: при заполненном AM ветка не срабатывает вовсе
        # ([[feedback_op_mirror_no_mixing]]).
        _current_bt_str = (current_bt or "").strip()
        _materials_for_bt = float(invoice.get("materials_fact_op") or 0)
        _mat_80_reached = _est_mat > 0 and _fact_mat >= 0.8 * _est_mat
        if not _materials_for_bt and (_fact_visible or _mat_80_reached):
            _materials_for_bt = (
                float(invoice.get("cost_metal") or 0)
                + float(invoice.get("cost_glass") or 0)
                + float(invoice.get("cost_extra_mat") or 0)
            )
        if _materials_for_bt > 0:
            cells[71] = self._fmt_amount(_materials_for_bt)
        elif _current_bt_str:
            cells[71] = _current_bt_str
        else:
            cells[71] = ""

        # BU — db-first: источник (logistics_fact_op) перезаписывает лист (см. шапку выше).
        _current_bu_str = (current_bu or "").strip()
        _logistics_for_bu = float(invoice.get("logistics_fact_op") or 0)
        if _logistics_for_bu > 0:
            cells[72] = self._fmt_amount(_logistics_for_bu)
        elif _current_bu_str:
            cells[72] = _current_bu_str
        else:
            cells[72] = ""

        # Остатки CB/CC/CD → 0, когда соответствующий ФАКТ внесён (user-req 2026-06-16):
        #   BS «Монтаж Факт» (cells[70])  → CB «Установка ост.» (cells[79]) = 0;
        #   BR «Грузчики Факт» (cells[69]) → CC «Грузчики ост.»  (cells[80]) = 0;
        #   BU «Логистика Факт» (cells[72]) → CD «Логистика ост.» (cells[81]) = 0.
        # Читаем уже посчитанные факт-ячейки (1-в-1 «значение внесено в BR/BS/BU»); перекрывает
        # обычный план−факт. CE «Итого осталось» (cells[82]) пересчитываем по всем категориям
        # (вкл. CA, обнулённую правилом материалов). Только открытые счета (как блок CA-CE).
        if invoice.get("status") in ("in_progress", "credit") and not _is_status_ended:
            if str(cells.get(70) or "").strip() and _inst_agreed > 0:
                _rem_inst = 0.0
                cells[79] = self._fmt_amount(_rem_inst)
            if str(cells.get(69) or "").strip() and _est_load > 0:
                _rem_load = 0.0
                cells[80] = self._fmt_amount(_rem_load)
            if str(cells.get(72) or "").strip() and _est_log > 0:
                _rem_log = 0.0
                cells[81] = self._fmt_amount(_rem_log)
            _rem_total = sum(
                x for x in (_rem_mat, _rem_inst, _rem_load, _rem_log) if x is not None
            )
            cells[82] = (
                self._fmt_amount(_rem_total)
                if any(x is not None for x in (_rem_mat, _rem_inst, _rem_load, _rem_log))
                else ""
            )

        # BJ «ЗП Монтажник» = НЕвыплаченный остаток = Согласовано − Выплачено (user 2026-06-05).
        # Выплачено = max(AN, аванс-зачёт, выплата ботом) — _paid из ЗП-блока выше.
        # Полностью выплачено (BS заполнен) → остаток ≤0 → пусто (BS и BJ взаимоисключающие).
        _bj_remaining = _montazh_agreed - _paid
        if _montazh_agreed > 0 and _bj_remaining > 0.001:
            cells[61] = self._fmt_amount(_bj_remaining)
        else:
            cells[61] = ""

        # CG «Аванс монтажника» (84) / CH «Дата аванса» (85) — user 2026-06-08.
        # CG = зачтённый аванс по счёту (б/н +10%, кредит как есть); CH = дата зачёта.
        # Пусто, если по счёту аванса нет.
        cells[84] = self._fmt_amount(_advance_cg) if _advance_offset_for_bs > 0 else ""
        cells[85] = self._fmt_sheet_date(_c.get("installer_advance_date")) if _advance_offset_for_bs > 0 else ""

        # EO «Аванс менеджера» (cells[144]) / EP «Дата аванса мен.» (cells[145]) —
        # зеркало CG/CH для роли менеджер (user 2026-06-14). Сумма с надбавкой +10%
        # на б/н (как монтажник), кредит как есть. Источник — get_manager_advance_for_invoice
        # (role-guard владелец=менеджер, кошелёк != rp; двойная роль Павла rp+npn разделена).
        _mgr_advance_offset = float(_c.get("manager_advance_offset") or 0)
        _advance_mgr = (
            _mgr_advance_offset * 1.10
            if (_mgr_advance_offset > 0 and not _inv_is_credit)
            else _mgr_advance_offset
        )
        cells[144] = self._fmt_amount(_advance_mgr) if _mgr_advance_offset > 0 else ""
        cells[145] = self._fmt_sheet_date(_c.get("manager_advance_date")) if _mgr_advance_offset > 0 else ""

        # EQ «Ориент. дата фин.платежа» (cells[146]) — planned_final_payment_date,
        # ТЗ 14.06: дату вводит менеджер при долге по счёту; пусто, если не задана.
        cells[146] = self._fmt_sheet_date(invoice.get("planned_final_payment_date"))

        # ER «Переплата в аванс» (147) / ES «Дата переплаты» (148) — owner 07.08.
        # Пара по образцу CG/CH: сколько переплаты ЗП менеджера (|CN|) перенесено в
        # его авансовый кошелёк и когда. ⚠️ Это НЕ дубль EO/EP: там зачтённый аванс,
        # привязанный к счёту через installer_advance_items, а перенос переплаты
        # items не создаёт — пополняется только кошелёк, поэтому EO/EP по таким
        # счетам пустые (проверено на боевых: у всех счетов с непустым CN зачёт = 0).
        # Пусто, если переноса не было. Оба канала переноса пишут только по
        # материнским счетам (sweep — WHERE parent_invoice_id IS NULL; ручной —
        # через list_invoices_under_recalc с тем же условием).
        _hold_adv = float(invoice.get("zp_hold_advanced") or 0)
        cells[147] = self._fmt_amount(_hold_adv) if _hold_adv > 0 else ""
        cells[148] = (
            self._fmt_sheet_date(invoice.get("zp_hold_advanced_at"))
            if _hold_adv > 0 else ""
        )

        # Платёжка ЗП даты
        _pay_sent = invoice.get("zp_installer_payment_sent_at")
        _confirmed = invoice.get("zp_installer_confirmed_at")
        if _pay_sent:
            cells[117] = format_dt_iso(_pay_sent, self.cfg.timezone_name)
        if _confirmed:
            cells[118] = format_dt_iso(_confirmed, self.cfg.timezone_name)

        # --- Затраты по типам (из supplier_payments бота) ---
        # Правило 4 (2026-05-19 вечер): cost_metal (DP) отображается только
        # если есть ≥1 supplier_payment с material_type='metal' (т.е. ПП от
        # ОПТИМА-ПРОФИЛЬ). Без ПП — DP пустой даже при cost_metal>0
        # (сумма счёта может быть известна, но в расходы не идёт).
        #
        # Правило 5 (2026-05-19; ПЕРЕОПРЕДЕЛЕНО user 15.06): DP-DV отображаются
        # для счетов «в работе» (in_progress / credit) ИЛИ для ЛЮБОГО счёта, по
        # которому прошли платежи через бота (любое cost_* > 0). Порог по дате
        # закрытия (был > / >= 2026-05-15) ОТМЕНЁН — все бот-платежи видны
        # независимо от даты и статуса. Обоснование: cost_* пишутся ТОЛЬКО из
        # supplier_payments (бот), импорт ОП их не трогает (аудит 15.06: 0 счетов
        # с cost>0 без бот-платежей) → «cost_* > 0» == «есть платёж через бота».
        _sp_list = (cost.get("supplier_payments_list") if cost else None) or []
        _has_metal_pp = any(
            (sp.get("material_type") or "").lower() == "metal" for sp in _sp_list
        )
        # Правило 5: гейт по факту бот-платежей (не по дате).
        _is_working_for_dp = (
            invoice.get("status") in ("in_progress", "credit")
            and not _is_status_ended
        )
        # «Есть платёж через бота» = любое поле cost_* > 0 (cost_* пишутся только
        # из supplier_payments). Показываем DP-DV независимо от даты/статуса.
        _has_bot_cost = any(
            float(invoice.get(_f) or 0) > 0
            for _f in ("cost_metal", "cost_glass", "cost_montazh", "cost_loaders",
                       "cost_logistics", "cost_extra_mat", "cost_extra_svc")
        )
        _show_dp_dv = _is_working_for_dp or _has_bot_cost
        for _ci, _cf in ((119, "cost_metal"), (120, "cost_glass"),
                         (121, "cost_montazh"), (122, "cost_loaders"),
                         (123, "cost_logistics"), (124, "cost_extra_mat"),
                         (125, "cost_extra_svc")):
            _cv = float(invoice.get(_cf) or 0)
            if not _show_dp_dv:
                # Правило 5: нет бот-платежей (cost_*=0) — DP-DV пусто.
                cells[_ci] = ""
            elif _ci == 119 and cost is not None and not _has_metal_pp:
                # Правило 4: без ПП от ОПТИМЫ — DP пустой.
                cells[_ci] = ""
            else:
                cells[_ci] = self._fmt_amount(_cv) if _cv else ""

        # --- Доп. поля процессов ---
        cells[126] = invoice.get("installer_ok_comment") or ""
        cells[127] = invoice.get("payment_method") or ""
        cells[128] = invoice.get("glass_order_status") or ""

        # --- Бухгалтерия (129-143) ---
        _holder_map = {"gd": "У ГД", "manager": "У менеджера"}
        cells[129] = "Да" if invoice.get("docs_edo_signed") else "Нет"
        cells[130] = "Да" if invoice.get("docs_paper_signed") else "Нет"
        cells[131] = _holder_map.get(invoice.get("docs_originals_holder") or "", "—")
        cells[132] = invoice.get("docs_originals_comment") or ""
        cells[133] = "Да" if invoice.get("edo_signed") else "Нет"
        cells[134] = _holder_map.get(invoice.get("closing_originals_holder") or "", "—")
        cells[135] = invoice.get("closing_originals_comment") or ""
        cells[136] = invoice.get("closing_docs_status") or ""
        cells[137] = self._fmt_sheet_date(invoice.get("edo_signed_at"))
        cells[138] = self._fmt_sheet_date(invoice.get("no_debts_at"))
        cells[139] = "Есть" if invoice.get("zp_installer_payment_file_id") else "Нет"
        _edo = invoice.get("_edo_stats") or {}
        cells[140] = _edo.get("total", 0) or ""
        cells[141] = _edo.get("open", 0) or ""
        cells[142] = _edo.get("last_response_type") or ""
        cells[143] = self._fmt_sheet_date(_edo.get("last_completed_at"))

        # M (12): Дата окончания = receipt_date + deadline_days
        _receipt = invoice.get("receipt_date")
        _deadline_d = invoice.get("deadline_days")
        if _receipt and _deadline_d:
            try:
                from datetime import datetime as _dt, timedelta as _td
                _rd = _dt.fromisoformat(str(_receipt).strip())
                _end = _rd + _td(days=int(_deadline_d))
                cells[12] = f"=DATE({_end.year};{_end.month};{_end.day})"
            except (ValueError, TypeError):
                pass

        # AT (45): Месяц из receipt_date
        if _receipt:
            _months = {1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
                       5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
                       9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"}
            try:
                from datetime import datetime as _dt
                _rd = _dt.fromisoformat(str(_receipt).strip())
                cells[45] = _months.get(_rd.month, "")
            except (ValueError, TypeError):
                pass

        return cells

    def _invoice_batch_ranges(self, row: int, cells: dict[int, Any]) -> list[dict[str, Any]]:
        ranges: list[dict[str, Any]] = []
        current_cols: list[int] = []
        current_values: list[Any] = []

        for col_idx in sorted(cells):
            value = cells[col_idx]
            if current_cols and col_idx != current_cols[-1] + 1:
                start = self._col_letter(current_cols[0])
                end = self._col_letter(current_cols[-1])
                ranges.append({
                    "range": f"{start}{row}:{end}{row}",
                    "values": [current_values],
                })
                current_cols = []
                current_values = []

            current_cols.append(col_idx)
            current_values.append(value)

        if current_cols:
            start = self._col_letter(current_cols[0])
            end = self._col_letter(current_cols[-1])
            ranges.append({
                "range": f"{start}{row}:{end}{row}",
                "values": [current_values],
            })
        return ranges

    def upsert_project_sync(self, project: dict[str, Any], manager_label: str = "") -> None:
        code = project.get("code")
        if not code:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.projects_tab, PROJECTS_HEADER)
            row, _ = self._get_or_allocate_row(self.cfg.projects_tab, ws, code)
            row_values = self._project_row_values(project, manager_label)
            ws.update([row_values], self._row_range(row, len(PROJECTS_HEADER)), value_input_option="USER_ENTERED")

    def upsert_task_sync(self, task: dict[str, Any], project_code: str = "") -> None:
        tid = task.get("id")
        if not tid:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.tasks_tab, TASKS_HEADER)
            row, _ = self._get_or_allocate_row(self.cfg.tasks_tab, ws, tid)
            row_values = self._task_row_values(task, project_code)
            ws.update([row_values], self._row_range(row, len(TASKS_HEADER)), value_input_option="USER_ENTERED")

    @staticmethod
    def _col_letter(idx: int) -> str:
        """0-based index → A1 column letter (0=A, 25=Z, 26=AA, ...)."""
        result = ""
        while True:
            result = chr(65 + idx % 26) + result
            idx = idx // 26 - 1
            if idx < 0:
                break
        return result

    def upsert_invoice_sync(
        self,
        invoice: dict[str, Any],
        manager_label: str = "",
        cost: dict[str, Any] | None = None,
        advance: dict[str, Any] | None = None,
    ) -> None:
        inv_num = invoice.get("invoice_number") or ""
        if not inv_num:
            return
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)
            row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_num)
            # BR/BS/BT/BU immutable: read-before-write
            # (правило feedback_br_bs_bt_bu_immutable_op_source).
            # Для существующих строк читаем текущие значения и передаём в
            # _invoice_cells, чтобы fill-only-if-empty логика их сохранила.
            #   col 70 = BR «Грузчики Факт»     ← Импорт ОП AQ (loaders_fact_op)
            #   col 71 = BS «Монтаж Факт»        ← Импорт ОП AN (montazh_fact_op)
            #   col 72 = BT «Материалы Факт»     ← Импорт ОП AM (materials_fact_op)
            #   col 73 = BU «Логистика Факт»     ← Импорт ОП AO (logistics_fact_op)
            current_br = current_bs = current_bt = current_bu = ""
            if not is_new:
                try:
                    _vals = ws.range(row, 70, row, 73)  # BR..BU one round-trip
                    if len(_vals) >= 4:
                        current_br = (_vals[0].value or "").strip()
                        current_bs = (_vals[1].value or "").strip()
                        current_bt = (_vals[2].value or "").strip()
                        current_bu = (_vals[3].value or "").strip()
                except Exception:
                    pass
            cells = self._invoice_cells(
                invoice, manager_label, cost,
                row=row, is_new=is_new,
                current_bs=current_bs, current_br=current_br,
                current_bt=current_bt, current_bu=current_bu,
                advance=advance,
            )
            if not is_new:
                cells = {k: v for k, v in cells.items() if k not in _MANUAL_COLS}
            batch_data = self._invoice_batch_ranges(row, cells)
            self._flush_batch_update(ws, batch_data, chunk_size=200)

    def upsert_projects_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.projects_tab, PROJECTS_HEADER)
            batch_data: list[dict[str, Any]] = []
            count = 0
            for project, manager_label in items:
                code = project.get("code")
                if not code:
                    continue
                row, _ = self._get_or_allocate_row(self.cfg.projects_tab, ws, code)
                batch_data.append(
                    {
                        "range": self._row_range(row, len(PROJECTS_HEADER)),
                        "values": [self._project_row_values(project, manager_label)],
                    }
                )
                count += 1
            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return count

    def upsert_tasks_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.tasks_tab, TASKS_HEADER)
            batch_data: list[dict[str, Any]] = []
            count = 0
            for task, project_code in items:
                tid = task.get("id")
                if not tid:
                    continue
                row, _ = self._get_or_allocate_row(self.cfg.tasks_tab, ws, tid)
                batch_data.append(
                    {
                        "range": self._row_range(row, len(TASKS_HEADER)),
                        "values": [self._task_row_values(task, project_code)],
                    }
                )
                count += 1
            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return count

    @staticmethod
    def _normalize_phone(phone: str | None) -> str:
        """Normalize phone for matching: keep last 10 digits."""
        if not phone:
            return ""
        digits = re.sub(r"\D", "", str(phone))
        return digits[-10:] if len(digits) >= 10 else digits

    def upsert_leads_bulk_sync(
        self,
        items: list[dict[str, Any]],
        *,
        status_map: dict[int, str] | None = None,
        amo_user_map: dict[int, str] | None = None,
    ) -> int:
        with self._sync_lock:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(self.cfg.leads_tab)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(
                    title=self.cfg.leads_tab, rows=2000,
                    cols=len(LEADS_HEADER),
                )

            # Ensure enough columns
            needed = len(LEADS_HEADER)
            if ws.col_count < needed:
                ws.resize(cols=needed)

            # Clear entire sheet and write header A1:G1
            total_rows = ws.row_count
            col_letter = gspread.utils.rowcol_to_a1(1, needed).rstrip("1")
            ws.batch_clear([f"A1:{col_letter}{total_rows}"])

            hdr_end = gspread.utils.rowcol_to_a1(1, len(LEADS_HEADER))
            ws.update([LEADS_HEADER], f"A1:{hdr_end}")

            # Write all leads starting from row 2
            batch_data: list[dict[str, Any]] = []
            next_row = 2

            for lead in items:
                if not lead.get("amo_lead_id"):
                    continue

                status_name = ""
                sid = lead.get("status_id")
                if sid and status_map:
                    status_name = status_map.get(int(sid), "")

                cell_end = gspread.utils.rowcol_to_a1(next_row, len(LEADS_HEADER))
                batch_data.append({
                    "range": f"A{next_row}:{cell_end}",
                    "values": [self._lead_row_values(
                        lead, status_name=status_name, amo_user_map=amo_user_map,
                    )],
                })
                next_row += 1

            self._flush_batch_update(ws, batch_data, chunk_size=200)
            return len(batch_data)

    def upsert_zamery_calendar_sync(
        self,
        year: int,
        month: int,
        busy_days: set[int] | list[int],
        off_days: set[int] | list[int],
    ) -> None:
        """Календарь замеров на лист leads, столбцы O:U (Пн…Вс) — «визуальная фиксация».

        Занят (есть замер) → ✅, выходной (blackout) → ❌, свободно → число.
        Пишем фиксированный блок O1:U8 (заголовок + шапка дней + 6 недель), поэтому
        строки прошлого месяца затираются без остатков. Колонки A:I (лиды) НЕ трогаются
        (lead-синк чистит только A:I); лист при необходимости дорастает до 21 колонки (U).
        """
        from calendar import monthrange
        from datetime import date

        busy = set(busy_days)
        off = set(off_days)
        months_nom = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                      "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
        weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

        ndays = monthrange(year, month)[1]
        start = date(year, month, 1).weekday()      # Пн=0 … Вс=6
        cells: list[str] = [""] * start
        for d in range(1, ndays + 1):
            if d in busy:
                cells.append("✅")
            elif d in off:
                cells.append("❌")
            else:
                cells.append(str(d))
        while len(cells) < 42:                       # 6 недель фикс → всегда блок O1:U8
            cells.append("")
        weeks = [cells[i:i + 7] for i in range(0, 42, 7)]

        title_row = [f"График замеров · {months_nom[month - 1]}"] + [""] * 6
        values = [title_row, weekdays] + weeks       # 8 строк × 7 колонок (O..U)

        with self._sync_lock:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(self.cfg.leads_tab)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=self.cfg.leads_tab, rows=2000, cols=21)
            if ws.col_count < 21:                     # U = 21-я колонка
                ws.resize(cols=21)
            ws.update(values, "O1:U8")

    # ----- Журнал заявок замерщика на листе Leads (блок W:AG) ----- #

    _ZJ_STATUS_RU = {
        "open": "Новая",
        "in_progress": "В работе",
        "done": "Выполнен",
        "rejected": "Отклонён",
    }
    _ZJ_ROLE_RU = {
        "manager_kv": "КВ",
        "manager_kia": "КИА",
        "manager_npn": "НПН",
    }

    @staticmethod
    def _zj_count_photos(*json_fields: Any) -> int:
        """Суммарное число вложений по JSON-полям заявки (безопасно к мусору)."""
        total = 0
        for raw in json_fields:
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, list):
                total += len(data)
        return total

    @staticmethod
    def _zj_fmt_date(iso: Any) -> str:
        """ISO YYYY-MM-DD → ДД.ММ.ГГГГ; иначе как есть."""
        s = str(iso or "")
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return f"{s[8:10]}.{s[5:7]}.{s[0:4]}"
        return s

    @staticmethod
    def _zj_fmt_num(v: Any) -> str:
        """Число для журнала: '' для пустого, целое без дробной части, иначе компактно."""
        if v is None or v == "":
            return ""
        try:
            f = float(v)
        except (ValueError, TypeError):
            return str(v)
        return str(int(f)) if f == int(f) else f"{f:g}"

    def _zamery_journal_row(self, r: dict[str, Any]) -> list[str]:
        """Одна заявка zamery_requests → 13 ячеек блока W:AI."""
        role = (r.get("requester_role") or "").strip()
        manager = self._ZJ_ROLE_RU.get(role) or r.get("manager_name") or role or ""
        status_raw = (r.get("status") or "").strip()
        status = self._ZJ_STATUS_RU.get(status_raw, status_raw)
        comment = (r.get("completion_comment") or r.get("accept_comment") or "").strip()
        return [
            r.get("address") or "",                                       # W  Адрес
            self._zj_fmt_date(r.get("scheduled_date")),                   # X  Дата замера
            r.get("scheduled_time_interval") or "",                       # Y  Интервал
            manager,                                                      # Z  Менеджер
            status,                                                       # AA Статус
            self._fmt_amount(r.get("total_cost")),                        # AB Стоимость
            self._zj_fmt_num(r.get("mkad_km")),                           # AC МКАД, км
            self._zj_fmt_num(r.get("volume_m2")),                         # AD Объём, м²
            "✅" if r.get("has_invoice") else "—",                         # AE Конверсия
            str(self._zj_count_photos(                                    # AF Фото, шт
                r.get("attachments_json"),
                r.get("completion_attachments_json"),
            )),
            comment,                                                      # AG Комментарий
            self._fmt_amount(r.get("paid_amount")),                       # AH Оплачено
            self._zj_fmt_date(r.get("paid_date")),                        # AI Дата оплаты
        ]

    def upsert_zamery_journal_sync(self, records: list[dict[str, Any]]) -> None:
        """Журнал заявок замерщика на лист Leads, блок W:AI (1 строка = 1 заявка).

        Заголовок в W1, данные с W2. Блок перезаписывается целиком (batch_clear → update),
        поэтому строки прошлого синка затираются без хвостов. Колонки A:I (лиды) и O:U
        (календарь) НЕ трогаются; лист при необходимости дорастает до 35 колонок (AI).
        AH «Оплачено» / AI «Дата оплаты» — из полей paid_amount/paid_date заявки.
        """
        rows = [self._zamery_journal_row(r) for r in (records or [])]
        values = [LEADS_ZAMERY_JOURNAL_HEADER] + rows
        with self._sync_lock:
            sh = self._get_spreadsheet()
            try:
                ws = sh.worksheet(self.cfg.leads_tab)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=self.cfg.leads_tab, rows=2000, cols=35)
            if ws.col_count < 35:                     # AI = 35-я колонка
                ws.resize(cols=35)
            ws.batch_clear([f"W1:AI{ws.row_count}"])
            ws.update(values, f"W1:AI{len(values)}")

    def upsert_invoices_bulk_sync(
        self,
        items: list[tuple[dict[str, Any], str, dict[str, Any] | None]],
    ) -> int:
        with self._sync_lock:
            ws = self._get_or_create_ws(self.cfg.invoices_tab, INVOICES_HEADER)

            # BR/BS/BT/BU immutable: read-before-clear
            # (правило feedback_br_bs_bt_bu_immutable_op_source).
            # Снимаем snapshot значений 4 fact-колонок по invoice_number (col I=9),
            # чтобы после batch_clear восстановить их через fill-only-if-empty
            # логику в _invoice_cells.
            #   col 70 = BR «Грузчики Факт»     ← Импорт ОП AQ
            #   col 71 = BS «Монтаж Факт»        ← Импорт ОП AN
            #   col 72 = BT «Материалы Факт»     ← Импорт ОП AM
            #   col 73 = BU «Логистика Факт»     ← Импорт ОП AO
            br_by_inv_num: dict[str, str] = {}
            bs_by_inv_num: dict[str, str] = {}
            bt_by_inv_num: dict[str, str] = {}
            bu_by_inv_num: dict[str, str] = {}
            try:
                all_inv_nums = ws.col_values(9)   # col I = invoice_number
                all_br_vals = ws.col_values(70)
                all_bs_vals = ws.col_values(71)
                all_bt_vals = ws.col_values(72)
                all_bu_vals = ws.col_values(73)
                _data_inv = all_inv_nums[1:]
                _data_br = all_br_vals[1:]
                _data_bs = all_bs_vals[1:]
                _data_bt = all_bt_vals[1:]
                _data_bu = all_bu_vals[1:]
                for _i, _inv_num in enumerate(_data_inv):
                    _inv_str = str(_inv_num).strip() if _inv_num else ""
                    if not _inv_str:
                        continue
                    if _i < len(_data_br):
                        _v = str(_data_br[_i]).strip() if _data_br[_i] else ""
                        if _v:
                            br_by_inv_num[_inv_str] = _v
                    if _i < len(_data_bs):
                        _v = str(_data_bs[_i]).strip() if _data_bs[_i] else ""
                        if _v:
                            bs_by_inv_num[_inv_str] = _v
                    if _i < len(_data_bt):
                        _v = str(_data_bt[_i]).strip() if _data_bt[_i] else ""
                        if _v:
                            bt_by_inv_num[_inv_str] = _v
                    if _i < len(_data_bu):
                        _v = str(_data_bu[_i]).strip() if _data_bu[_i] else ""
                        if _v:
                            bu_by_inv_num[_inv_str] = _v
            except Exception:
                pass  # не критично; пустые → бот заполнит из БД, если есть данные

            # AH/AI immutable: read-before-clear (owner 30.07)
            # Ручные колонки, которые бот НЕ считает и НЕ пишет (_MANUAL_COLS).
            # Фильтр `if not is_new` ниже их не спасал НИКОГДА: batch_clear сбрасывает
            # кеш строк → каждый счёт приходит is_new=True → ветка не выполняется,
            # и значения гибли при каждом полном экспорте (30.07: 23 ячейки).
            # Поэтому снимаем snapshot и возвращаем значения КАК ЕСТЬ после очистки.
            #   col 34 = AH «Пояснения»   (0-based cells[33])
            #   col 35 = AI «Агентское»   (0-based cells[34])
            # value_render_option=FORMULA — парный режим к USER_ENTERED в
            # _flush_batch_update: число вернётся числом, текст текстом, формула
            # формулой. FORMATTED отдал бы '29\xa0700' (неразрывный пробел), и
            # число легло бы на лист текстом.
            ah_by_inv_num: dict[str, Any] = {}
            ai_by_inv_num: dict[str, Any] = {}
            try:
                _fml = gspread.utils.ValueRenderOption.formula
                _man_inv_nums = ws.col_values(9)[1:]
                _ah_vals = ws.col_values(34, value_render_option=_fml)[1:]
                _ai_vals = ws.col_values(35, value_render_option=_fml)[1:]
                for _i, _inv_num in enumerate(_man_inv_nums):
                    _inv_str = str(_inv_num).strip() if _inv_num else ""
                    if not _inv_str:
                        continue
                    if _i < len(_ah_vals) and str(_ah_vals[_i]).strip() != "":
                        ah_by_inv_num[_inv_str] = _ah_vals[_i]
                    if _i < len(_ai_vals) and str(_ai_vals[_i]).strip() != "":
                        ai_by_inv_num[_inv_str] = _ai_vals[_i]
            except Exception:
                pass  # не критично: пустые → колонки останутся пустыми, как до правки

            # Полная очистка данных (кроме заголовка) — гарантирует чистый лист
            try:
                total_rows = ws.row_count
                if total_rows > 1:
                    col_count = ws.col_count
                    col_letter = gspread.utils.rowcol_to_a1(1, col_count).rstrip("1")
                    ws.batch_clear([f"A2:{col_letter}{total_rows}"])
            except Exception:
                pass
            # Сброс кеша строк — все строки будут записаны заново
            self._row_indexes.pop(self.cfg.invoices_tab, None)
            self._next_rows.pop(self.cfg.invoices_tab, None)

            # Убрать старые LEAD-строки из основной зоны (если попали ранее)
            self._clear_lead_rows(ws)

            # Compact rewrite: инвойс-cells (0-86 + 119+) И лид-cells (86-118)
            # пишутся в ОДНУ row для каждого invoice. Раньше Phase 2 писала
            # лид-секцию в отдельный lead_row counter — это создавало
            # рассинхрон. Плюс _sort_ws_by_date уезжал rows без receipt_date
            # в конец, образуя gap. Sort убран — порядок задаётся внешним
            # sorted() в export_to_sheets (invoice_export_sort_key: строка N
            # Invoices = строка N «Импорт ОП», owner 27.07).
            batch_data: list[dict[str, Any]] = []
            count = 0
            seq_no = 0
            for invoice, manager_label, cost in items:
                inv_num = invoice.get("invoice_number") or ""
                if not inv_num or str(inv_num).startswith("LEAD-"):
                    continue
                row, is_new = self._get_or_allocate_row(self.cfg.invoices_tab, ws, inv_num)
                _inv_key = str(inv_num).strip()
                _current_br = br_by_inv_num.get(_inv_key, "")
                _current_bs = bs_by_inv_num.get(_inv_key, "")
                _current_bt = bt_by_inv_num.get(_inv_key, "")
                _current_bu = bu_by_inv_num.get(_inv_key, "")
                cells = self._invoice_cells(
                    invoice, manager_label, cost,
                    row=row, is_new=is_new,
                    current_bs=_current_bs, current_br=_current_br,
                    current_bt=_current_bt, current_bu=_current_bu,
                )
                if not is_new:
                    cells = {k: v for k, v in cells.items() if k not in _MANUAL_COLS}
                # Вернуть стёртые batch_clear'ом ручные AH/AI ровно тем, что было
                # на листе (snapshot выше). Бот эти колонки не вычисляет — только
                # возвращает; ни одного нового источника данных здесь нет.
                _ah = ah_by_inv_num.get(_inv_key)
                if _ah is not None:
                    cells[33] = _ah
                _ai = ai_by_inv_num.get(_inv_key)
                if _ai is not None:
                    cells[34] = _ai
                seq_no += 1
                # № п/п в col 86: монотонная нумерация compact, без gap'ов
                if 86 not in _MANUAL_COLS:
                    cells[86] = seq_no
                batch_data.extend(self._invoice_batch_ranges(row, cells))
                count += 1

            self._flush_batch_update(ws, batch_data, chunk_size=500)

            # Очистить лишние строки после последней записанной (compact)
            try:
                total_rows = ws.row_count
                last_data_row = count + 1
                if total_rows > last_data_row:
                    col_count = ws.col_count
                    col_letter = gspread.utils.rowcol_to_a1(1, col_count).rstrip("1")
                    clear_range = f"A{last_data_row + 1}:{col_letter}{total_rows}"
                    ws.batch_clear([clear_range])
            except Exception:
                pass

            # SORT удалён — он смещал rows без receipt_date в конец, создавая gap.
            # Порядок инвойсов задаётся sorted() в export_to_sheets ДО bulk_write
            # (invoice_export_sort_key: op_row_index → строка ОП; fallback дата, id).

            return count

    @staticmethod
    def _clear_lead_rows(ws: gspread.Worksheet) -> bool:
        """Remove rows where invoice_number (col I = index 9) starts with LEAD-."""
        col_i = ws.col_values(9)  # column I = invoice_number
        rows_to_delete: list[int] = []
        for row_num, val in enumerate(col_i[1:], start=2):  # skip header
            if str(val).strip().startswith("LEAD-"):
                rows_to_delete.append(row_num)
        # Delete from bottom to top so row indices stay valid
        for row_num in reversed(rows_to_delete):
            ws.delete_rows(row_num)
        return bool(rows_to_delete)

    def _sort_ws_by_date(self, ws: gspread.Worksheet, sort_col_index: int = 10) -> None:
        """Sort worksheet rows 2+ by column, ASCENDING (oldest dates first)."""
        sheet_id = ws._properties["sheetId"]  # noqa: SLF001
        row_count = ws.row_count
        col_count = ws.col_count
        body = {
            "requests": [{
                "sortRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # skip header
                        "endRowIndex": row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    },
                    "sortSpecs": [{
                        "dimensionIndex": sort_col_index,
                        "sortOrder": "ASCENDING",
                    }],
                }
            }]
        }
        ws.spreadsheet.batch_update(body)
        # Сбросить кеш строк после сортировки
        self._row_indexes.pop(self.cfg.invoices_tab, None)

    # ---------- async wrappers ----------

    async def upsert_project(self, project: dict[str, Any], manager_label: str = "") -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(self.upsert_project_sync, project, manager_label)

    async def upsert_task(self, task: dict[str, Any], project_code: str = "") -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(self.upsert_task_sync, task, project_code)

    async def upsert_invoice(
        self,
        invoice: dict[str, Any],
        manager_label: str = "",
        cost: dict[str, Any] | None = None,
        advance: dict[str, Any] | None = None,
    ) -> None:
        if not self.cfg.enabled:
            return
        await asyncio.to_thread(
            self.upsert_invoice_sync, invoice, manager_label, cost, advance,
        )

    async def upsert_projects_bulk(self, items: list[tuple[dict[str, Any], str]]) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_projects_bulk_sync, items)

    async def upsert_tasks_bulk(self, items: list[tuple[dict[str, Any], str]]) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_tasks_bulk_sync, items)

    async def upsert_leads_bulk(
        self,
        items: list[dict[str, Any]],
        *,
        status_map: dict[int, str] | None = None,
        amo_user_map: dict[int, str] | None = None,
    ) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(
            self.upsert_leads_bulk_sync, items,
            status_map=status_map, amo_user_map=amo_user_map,
        )

    async def upsert_invoices_bulk(
        self,
        items: list[tuple[dict[str, Any], str, dict[str, Any] | None]],
    ) -> int:
        if not self.cfg.enabled or not items:
            return 0
        return await asyncio.to_thread(self.upsert_invoices_bulk_sync, items)

    # ---------- IMPORT from source spreadsheet (Отдел Продаж → SQLite) ----------

    # Column mapping: source sheet col index → field name
    _OP_COL_MAP: dict[int, str] = {
        0: "client_name",              # A: Контрагент
        1: "traffic_source",           # B: Ист.трафика
        2: "is_credit",                # C: Кред (0=кредит, 1=б.н.)
        3: "client_source",            # D: Свой/Атм (1=Свой, 2=Атм)
        4: "invoice_number",           # E: Номер счета (KEY)
        5: "object_address",           # F: Адрес
        6: "receipt_date",             # G: Дата пост.
        7: "deadline_days",            # H: Сроки (дни)
        8: "zamery_info_op",             # I: Замеры (из ОП)
        9: "actual_completion_date",   # J: Дата Факт
        10: "amount",                  # K: Сумма
        11: "first_payment_amount",    # L: Сумма 1пл
        12: "estimated_materials",     # M: Расч.мат.
        13: "estimated_installation",  # N: Установка
        14: "estimated_loaders",       # O: Грузчики
        15: "estimated_logistics",     # P: Логистика
        16: "profit_tax",              # Q: Прибыль кред.
        17: "nds_amount",              # R: НДС
        18: "profit_tax_op",            # S: Налог на приб.
        19: "rp_10_pct_op",             # T: РП - 10%
        20: "profit_calc_op",           # U: Прибыль расч
        21: "rentability_calc",        # V: Рент-ть расчетная
        22: "rentability_fact_op",      # W: Рент-ть факт
        23: "surcharge_amount",        # X: Сумма допл
        24: "surcharge_date",          # Y: Дата допл
        25: "final_surcharge_amount",  # Z: Финальный платеж
        26: "final_surcharge_date",    # AA: Дата Финал.пл.
        27: "outstanding_debt",        # AB: Сумма Долга
        28: "payment_terms",           # AC: Пояснения
        29: "agent_fee",               # AD: Агентское
        30: "agent_payout_op",           # AE: Выплаты. Агент.
        31: "agent_payout_date_op",      # AF: Дата выпл. Агент. (renamed 2026-05-12 — был misnamed men_zp_payout_op)
        32: "manager_zp_blank",        # AG: Мен. ЗП (по бланку)
        33: "zp_manager_request_text",   # AH: Запрос суммы на выплату тех
        34: "zp_manager_request_amount", # AI: Запрос суммы на выплату (НОВЫЙ)
        35: "zp_manager_payout",         # AJ: Выплата. Мен. ЗП
        36: "zp_manager_payout_date",    # AK: Дата выпл. мен.
        # 37: AL (пустая колонка)
        38: "materials_fact_op",         # AM: Материалы Факт
        39: "montazh_fact_op",           # AN: Монтаж Факт
        40: "logistics_fact_op",         # AO: Логистика факт
        41: "logistics_fact_date",       # AP: Дата лог.
        42: "loaders_fact_op",           # AQ: Грузчики факт
        43: "loaders_fact_date",         # AR: Дата груз.
        # 44: AS — Команда боту (human-writable)
        # 45: AT — Команда боту (human-writable)
        46: "npn_amount",               # AU: НПН с 10% налог
        47: "rp_request_op",            # AV: Запрос РП — renamed 26.05 from npn_request_op
        48: "rp_payout_op",             # AW: Выдано РП — renamed 26.05 from npn_payout_op
        49: "rp_payout_date_op",        # AX: Дата РП — renamed 26.05 from npn_payout_date_op
        # 50: AY (Месяц — не импортируем)
        51: "taxes_fact_op",             # AZ: Налоги факт (был сдвиг, было idx 50)
        # Ручной ввод owner (2026-06-22) → лист Invoices CN/CO. Чистый перенос.
        83: "zp_manager_hold",           # CF: Удержать из ЗП менеджера
        84: "cost_diff_calc_fact",       # CG: Разница себестоимости расч/факт
        # 51, 52 removed (2026-04-21): BA/BB in Импорт ОП are operator-entered
        # manually, bot should not copy them. Use cost_card.margin (BL) instead.
        # idx 60/62/63/64 (BI/BK/BL/BM, «Оперативная прибыль компании») — НЕ маплим
        # как поля invoice'а. Эти колонки — помесячная аналитика компании,
        # парсится отдельным методом `import_op_monthly_balance` в op_company_monthly.
    }

    def _parse_num(self, val: str) -> float | None:
        """Parse number from string, handling spaces/commas as thousand separators."""
        if not val or not val.strip():
            return None
        v = val.strip().replace("\u00a0", "").replace(" ", "").rstrip("%")
        # Strip currency suffixes: "1000р.", "1000 руб", "1000₽"
        v = re.sub(r'[р₽]\.?$|руб\.?$', '', v).strip()
        # Google Sheets uses comma as thousand separator (257,000 = 257000)
        # If comma exists AND digits after comma are exactly 3 → thousand separator
        if "," in v:
            parts = v.split(",")
            if all(len(p) == 3 for p in parts[1:]) and all(p.isdigit() for p in parts[1:]):
                # Thousand separator: "257,000" → "257000"
                v = v.replace(",", "")
            else:
                # Decimal comma: "26.5%" already stripped %, just replace
                v = v.replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return None

    @staticmethod
    def _valid_ymd(iso: str) -> bool:
        """Календарная проверка YYYY-MM-DD: месяц 27 или 31.02 → False."""
        from datetime import datetime
        try:
            datetime.strptime(iso, "%Y-%m-%d")
            return True
        except (ValueError, TypeError):
            return False

    def _parse_date_dmy(self, val: str) -> str | None:
        """Parse date string → YYYY-MM-DD ISO.

        Supported formats: DD.MM.YYYY, DD/MM/YYYY, DD.MM.YY, **M/D/YYYY**
        (дата-типизированная ячейка в US-локали Google Sheets),
        YYYY-MM-DD (ISO passthrough), Google Sheets serial number.

        ⛔ Фикс owner 25.07: раньше порядок считался ЖЁСТКО «день.месяц», а
        результат не проверялся — ячейка `'7/27/2026'` давала строку
        **`'2026-27-07'`** (месяц 27!). Мусор уходил в `receipt_date`, а
        `db._compute_deadline_end_date` на нём падал в ValueError → «Срок оконч.»
        оставался пустым, т.е. часть данных счёта «не переносилась» (инцидент:
        сч. КВ 11 из «Импорт ОП» 25.07). Теперь:
          • порядок разрешается по диапазону — компонент > 12 однозначно день
            (RU `15.04.2026` → DD.MM; US `7/27/2026` → MM/DD);
          • оба ≤ 12 неоднозначны → остаётся прежнее соглашение DD.MM (RU);
          • результат проверяется КАЛЕНДАРНО, нераспознанное → None (вызывающий
            пишет warning и поле НЕ трогает) вместо записи битой строки в БД.
        """
        if not val or not val.strip():
            return None
        raw = val.strip()
        # ISO passthrough (с календарной проверкой — битый ISO в БД не пускаем)
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            return raw if self._valid_ymd(raw) else None
        # Google Sheets serial number (integer or float like 46107 or 46107.0)
        try:
            serial = float(raw)
            if 30000 < serial < 60000:  # reasonable range: ~1982–2064
                from datetime import datetime, timedelta
                base = datetime(1899, 12, 30)
                dt = base + timedelta(days=int(serial))
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        # DD.MM.YYYY / DD/MM/YYYY / DD.MM.YY / M/D/YYYY
        for sep in (".", "/"):
            parts = raw.split(sep)
            if len(parts) != 3:
                continue
            try:
                a, b, y = int(parts[0]), int(parts[1]), int(parts[2])
            except (ValueError, IndexError):
                continue
            if y < 100:
                y += 2000
            if a > 12 >= b:
                d, m = a, b          # DD.MM (RU)
            elif b > 12 >= a:
                d, m = b, a          # MM/DD (US-локаль Google Sheets)
            else:
                d, m = a, b          # оба ≤12 — неоднозначно, соглашение DD.MM
            iso = f"{y:04d}-{m:02d}-{d:02d}"
            if self._valid_ymd(iso):
                return iso
        return None

    _OP_NUMERIC_FIELDS = frozenset(
        {
            "amount",
            "first_payment_amount",
            "estimated_materials",
            "estimated_installation",
            "estimated_loaders",
            "estimated_logistics",
            "nds_amount",
            "outstanding_debt",
            "surcharge_amount",
            "final_surcharge_amount",
            "agent_fee",
            "manager_zp_blank",
            "npn_amount",
            "profit_tax",
            "rentability_calc",
            "materials_fact_op",
            "montazh_fact_op",
            "zp_manager_request_amount",
            "zp_manager_payout",
            "logistics_fact_op",
            "loaders_fact_op",
            "agent_payout_op",
            "rp_request_op",
            "rp_payout_op",
            "rp_10_pct_op",
            "taxes_fact_op",
            "profit_calc_op",
            "profit_tax_op",        # S: Налог на приб. (был забыт — разводка S/W 2026-06-26)
            "rentability_fact_op",  # W: Рент-ть факт ('0%' → 0.0)
            "zp_manager_hold",
            "cost_diff_calc_fact",
        }
    )
    _OP_DATE_FIELDS = frozenset(
        {
            "receipt_date",
            "actual_completion_date",
            "surcharge_date",
            "final_surcharge_date",
            "zp_manager_payout_date",
            "logistics_fact_date",
            "loaders_fact_date",
            "rp_payout_date_op",
            "agent_payout_date_op",
        }
    )

    def _parse_op_row(
        self,
        row_values: list[str],
        op_row_index: int | None = None,
    ) -> dict[str, Any] | None:
        inv_num = str(row_values[4]).strip() if len(row_values) > 4 else ""
        if not inv_num:
            return None

        parsed: dict[str, Any] = {"invoice_number": inv_num}
        # Физическая строка счёта в листе «Импорт ОП» — задаёт порядок строк
        # Invoices (решение owner 27.07). None (webhook, где номера строки нет)
        # ключ не кладёт → import_invoice_from_sheet сохранит прежнее значение.
        if op_row_index is not None:
            parsed["op_row_index"] = int(op_row_index)
        skipped_empty: list[str] = []
        for col_idx, field in self._OP_COL_MAP.items():
            if field == "invoice_number":
                continue

            raw_value = str(row_values[col_idx]).strip() if col_idx < len(row_values) else ""
            if not raw_value:
                # ⛔ Пустая ячейка НЕ стирает значение в БД (инцидент 27.07).
                # Раньше здесь стояло `parsed[field] = None`, а
                # `import_invoice_from_sheet` трактует явный None как «очистить
                # колонку» → любой синк, поймавший лист в момент правки (или
                # строку, обрезанную ответом API), затирал живые данные:
                # 27.07 06:00 UTC ночной cron снёс «Сумма» 510 000 у КВ 11,
                # «Монтаж факт» 25 000 у КВ 7 и ещё 7 полей; в 08:39 — «Сумма»
                # 179 000 и «Дата пост.» у 26721-1НПН. Пустой receipt_date
                # дополнительно уносит строку в конец листа (ключ сортировки
                # '9999-12-31') → физический порядок Invoices разъезжается с
                # «Импорт ОП», и это выглядит как «данные перемешались».
                # Теперь пусто = «нет данных», поле не трогаем: ключ не кладём,
                # import_invoice_from_sheet оставит прежнее значение.
                skipped_empty.append(field)
                continue

            if field in self._OP_NUMERIC_FIELDS:
                num = self._parse_num(raw_value)
                if num is not None:
                    parsed[field] = num
            elif field in self._OP_DATE_FIELDS:
                parsed_date = self._parse_date_dmy(raw_value)
                if parsed_date:
                    parsed[field] = parsed_date
                else:
                    log.warning("ОП import: cannot parse date field '%s' = '%s' (invoice %s)", field, raw_value, inv_num)
            elif field == "deadline_days":
                num = self._parse_num(raw_value)
                if num is not None:
                    parsed[field] = int(num)
            elif field == "is_credit":
                # Source: 0 = кредит, 1 = б.н.
                parsed[field] = 1 if raw_value == "0" else 0
            elif field == "client_source":
                # 1 = Свой (own), 2 = Атм (gd_lead)
                if raw_value == "1":
                    parsed[field] = "own"
                elif raw_value == "2":
                    parsed[field] = "gd_lead"
                else:
                    parsed[field] = raw_value
            else:
                parsed[field] = raw_value

        # Сигнал «строка прочитана полупустой» — типичный признак чтения листа
        # в момент правки. Данные при этом уже не теряются (пустое не пишем),
        # но след в логах нужен, чтобы такие синки было видно.
        if len(skipped_empty) >= max(1, int(len(self._OP_COL_MAP) * 0.8)):
            log.warning(
                "ОП import: строка счёта %s пришла почти пустой (%d из %d полей без значения) — "
                "поля не трогаем, вероятна правка листа во время чтения",
                inv_num, len(skipped_empty), len(self._OP_COL_MAP),
            )

        return parsed

    def _detect_op_sheet_start_row(self, all_data: list[list[str]]) -> int:
        """Detect the header row in the source sheet and return the first data row."""
        header_markers = ("номер счета", "контрагент", "адрес", "дата пост", "сумма")
        for idx, row in enumerate(all_data[:10]):
            normalized = [str(cell).strip().lower() for cell in row if str(cell).strip()]
            if not normalized:
                continue
            score = sum(
                1
                for marker in header_markers
                if any(marker in cell for cell in normalized)
            )
            if score >= 3:
                return idx + 1
        return 1 if all_data else 0

    def read_op_sheet_sync(self) -> list[dict[str, Any]]:
        """Read all rows from source 'Отдел продаж' sheet, return parsed dicts."""
        if not self.cfg.source_spreadsheet_id:
            return []

        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
        except Exception as e:
            log.error("Cannot open source spreadsheet: %s", e)
            return []

        try:
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except gspread.WorksheetNotFound:
            log.error("Sheet '%s' not found in source spreadsheet", self.cfg.source_sheet_name)
            return []

        all_data = ws.get_all_values()
        if len(all_data) < 2:
            return []

        start_row = self._detect_op_sheet_start_row(all_data)

        # Diagnostic: log first 3 non-empty values from col AA (index 26)
        aa_samples = []
        for row in all_data[start_row:]:
            if len(row) > 26 and str(row[26]).strip():
                aa_samples.append(str(row[26]).strip())
                if len(aa_samples) >= 3:
                    break
        if aa_samples:
            log.info("ОП col AA (final_surcharge_date) samples: %s", aa_samples)
        else:
            log.warning("ОП col AA (final_surcharge_date): all values empty")

        results: list[dict[str, Any]] = []
        for row_idx in range(start_row, len(all_data)):
            # row_idx 0-based → номер строки листа 1-based
            parsed = self._parse_op_row(all_data[row_idx], op_row_index=row_idx + 1)
            if parsed:
                results.append(parsed)

        log.info("Read %d invoices from source ОП sheet", len(results))
        return results

    def parse_op_row_from_webhook(self, row_values: list[str]) -> dict[str, Any] | None:
        """Parse a single row from webhook payload (same column order as ОП sheet).

        row_values: list of string cell values, index = column index.
        Returns parsed dict compatible with db.import_invoice_from_sheet(), or None.
        """
        return self._parse_op_row(row_values)

    async def read_op_sheet(self) -> list[dict[str, Any]]:
        """Async wrapper for reading source ОП sheet."""
        if not self.cfg.enabled:
            return []
        return await asyncio.to_thread(self.read_op_sheet_sync)

    # ---------- op_company_monthly (Доходы/Расходы по месяцам из «Импорт ОП») ----------

    _MONTHS_RU_TO_NUM = {
        "январь": 1, "февраль": 2, "март": 3, "апрель": 4,
        "май": 5, "июнь": 6, "июль": 7, "август": 8,
        "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
    }

    def read_op_monthly_balance_sync(self) -> list[dict[str, Any]]:
        """Парсер журнала BH-BQ из «Импорт ОП» (структура с 2026-05-12).

        Структура листа в колонках BH-BQ:
          BH (59) — Месяц (RU-имя: Январь, Февраль, ...)
          BI (60) — Дата DD.MM.YYYY безналичного расхода
          BJ (61) — Расходы Сумма б/н
          BK (62) — НДС (выделенный из BJ, информационно)
          BL (63) — Описание расхода ('ЗП директор', 'Реклама ...', 'Итого налоги', 'Возврат займа ...')
          BM (64) — Налоги (заполнено ТОЛЬКО на строке BL='Итого налоги')
          BN (65) — Займ (нетто, со знаком; + = входящий, − = возврат)
          BO (66) — Дата DD.MM.YYYY наличной/прочей оплаты
          BP (67) — Сумма наличной/прочей оплаты
          BQ (68) — Описание наличной оплаты

        Блок месяца завершается строкой BH='Итого:' с агрегатами BJ/BK/BN/BP.
        State-machine: track (current_month, current_year). На «Итого:»-строке
        фиксируем агрегаты; на BL='Итого налоги' внутри блока — налоги. Год
        берётся из первой DD.MM.YYYY в BI или BO внутри блока месяца.

        Returns: список dict с year, month, expense_cashless, expense_nds,
                 expense_taxes, expense_other, loan_net.
        """
        if not self.cfg.source_spreadsheet_id:
            return []

        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except Exception as e:
            log.error("Cannot open source sheet for monthly balance: %s", e)
            return []

        all_data = ws.get_all_values()
        if len(all_data) < 2:
            return []
        start_row = self._detect_op_sheet_start_row(all_data)

        def _parse_year_from_date(s: str) -> int | None:
            s = (s or "").strip()
            if not s or "." not in s:
                return None
            parts = s.split(".")
            if len(parts) != 3:
                return None
            try:
                y = int(parts[2])
                return y + 2000 if y < 100 else y
            except ValueError:
                return None

        accum: dict[tuple[int, int], dict[str, Any]] = {}
        current_month: int | None = None
        current_year: int | None = None

        for r in all_data[start_row:]:
            bh = (r[59] if len(r) > 59 else "").strip()
            bl = (r[63] if len(r) > 63 else "").strip()
            if not bh:
                continue

            if bh == "Итого:":
                if current_year is not None and current_month is not None:
                    key = (current_year, current_month)
                    e = accum.setdefault(key, {"year": current_year, "month": current_month})
                    e["expense_cashless"] = self._parse_num(r[61] if len(r) > 61 else "")
                    e["expense_nds"] = self._parse_num(r[62] if len(r) > 62 else "")
                    e["loan_net"] = self._parse_num(r[65] if len(r) > 65 else "")
                    e["expense_other"] = self._parse_num(r[67] if len(r) > 67 else "")
                current_month = None
                current_year = None
                continue

            month_num = self._MONTHS_RU_TO_NUM.get(bh.lower())
            if month_num is None:
                continue

            if month_num != current_month:
                current_month = month_num
                current_year = None

            if current_year is None:
                y = _parse_year_from_date(r[60] if len(r) > 60 else "") \
                    or _parse_year_from_date(r[66] if len(r) > 66 else "")
                if y is not None:
                    current_year = y

            if bl == "Итого налоги" and current_year is not None and current_month is not None:
                taxes = self._parse_num(r[64] if len(r) > 64 else "")
                key = (current_year, current_month)
                e = accum.setdefault(key, {"year": current_year, "month": current_month})
                e["expense_taxes"] = taxes

        results = sorted(accum.values(), key=lambda x: (x["year"], x["month"]))
        log.info("op_monthly: parsed %d (year, month) blocks from BH-BQ journal", len(results))
        return results

    async def import_op_monthly_balance(self, db) -> int:
        """Пересобрать op_company_monthly из «Импорт ОП» (BH-BQ) + op_company_entries.

        Для каждого (year, month) итог = агрегаты листа («Итого:»-row даёт
        cashless/nds/loan/other, «Итого налоги»-row → taxes) ПЛЮС суммы ручных
        записей op_company_entries того же месяца. Пишутся АБСОЛЮТНЫЕ значения
        (0.0 при отсутствии источника), а не None — поэтому пересборка идемпотентна:
        повторные синки дают тот же результат.

        ⚠️ До 2026-06-18 второй pass ПРИБАВЛЯЛ записи к уже сохранённому значению,
        а пустой лист «Итого:» (None) из-за COALESCE в upsert не сбрасывал прошлый
        результат — за много автосинков месяц раздувался (июнь 2026 показывал
        −30,4 млн при реальных ~−247 тыс; май — налоги 25,5 млн). Теперь — пересборка
        начисто. income_* / legacy expense_cash/credit/total НЕ трогаем (приходят из
        импорта счетов и сохраняются COALESCE'ом, т.к. сюда не передаются).

        Returns: количество обработанных (year, month) ключей (объединение источников).
        """
        if not self.cfg.enabled:
            return 0
        sheet_entries = await asyncio.to_thread(self.read_op_monthly_balance_sync)
        sheet_by_key: dict[tuple[int, int], dict[str, Any]] = {
            (e["year"], e["month"]): e for e in sheet_entries
        }

        # Агрегаты ручных записей op_company_entries по (year, month).
        try:
            db_rows = await db.list_op_company_entries()
        except Exception as ex:
            log.warning("op_monthly: list_op_company_entries failed (table missing?): %s", ex)
            db_rows = []

        agg: dict[tuple[int, int], dict[str, float]] = {}
        for r in db_rows:
            key = (r["year"], r["month"])
            a = agg.setdefault(key, {
                "cashless": 0.0, "nds": 0.0, "taxes": 0.0, "loan": 0.0, "other": 0.0,
            })
            if r.get("cashless_amount") is not None:
                a["cashless"] += float(r["cashless_amount"])
            if r.get("nds") is not None:
                a["nds"] += float(r["nds"])
            if r.get("taxes") is not None:
                a["taxes"] += float(r["taxes"])
            if r.get("loan") is not None:
                a["loan"] += float(r["loan"])
            if r.get("other_amount") is not None:
                a["other"] += float(r["other_amount"])

        # Неаддитивная пересборка: итог = лист + ручные записи, пишем абсолют
        # (перезапись через COALESCE, т.к. значения всегда не-None) → мусор от
        # прошлых аддитивных синков сбрасывается, результат идемпотентен.
        merged_keys = set(sheet_by_key.keys()) | set(agg.keys())
        for (y, m) in merged_keys:
            s = sheet_by_key.get((y, m)) or {}
            a = agg.get((y, m)) or {}
            await db.upsert_monthly_op_company(
                y, m,
                expense_cashless=float(s.get("expense_cashless") or 0) + float(a.get("cashless") or 0),
                expense_nds=float(s.get("expense_nds") or 0) + float(a.get("nds") or 0),
                expense_taxes=float(s.get("expense_taxes") or 0) + float(a.get("taxes") or 0),
                expense_other=float(s.get("expense_other") or 0) + float(a.get("other") or 0),
                loan_net=float(s.get("loan_net") or 0) + float(a.get("loan") or 0),
            )

        log.info(
            "op_monthly: rebuilt %d (year,month) keys (sheet=%d, db-entries=%d)",
            len(merged_keys), len(sheet_by_key), len(agg),
        )
        return len(merged_keys)

    @staticmethod
    def _excel_serial_to_dmy(val: Any) -> str:
        """Excel-serial (int/float) → 'DD.MM.YYYY'. Строки и пустоту возвращает как есть."""
        s = str(val).strip()
        if not s:
            return ""
        try:
            serial = float(s.replace(",", "."))
        except (ValueError, TypeError):
            return s
        if 1 <= serial < 100000:
            from datetime import datetime, timedelta
            base = datetime(1899, 12, 30)
            try:
                return (base + timedelta(days=int(serial))).strftime("%d.%m.%Y")
            except (ValueError, OverflowError):
                return s
        return s

    def read_op_journal_rows_sync(self) -> list[dict[str, Any]]:
        """Построчное чтение журнала BH-BQ из «Импорт ОП» — 1-в-1 без фильтрации.

        Возвращает ВСЕ строки as-is (включая 'Итого:', 'Итого налоги' и пустые
        template-rows с заполненным только BH=имя_месяца) с привязкой к году
        (берётся из первой DD.MM.YYYY в блоке текущего месяца). Используется для
        записи журнала в лист «Баланс компании» построчно 1-в-1.

        Returns: list of dict с полями: year, month_name, month_num,
                 date_cashless, amount_cashless, nds, description,
                 taxes, loan, date_other, amount_other, description_credit.
        """
        if not self.cfg.source_spreadsheet_id:
            return []
        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except Exception as e:
            log.error("Cannot open source sheet for journal rows: %s", e)
            return []

        all_data = ws.get_all_values()
        if len(all_data) < 2:
            return []
        start_row = self._detect_op_sheet_start_row(all_data)

        def _parse_year_from_dmy(s: str) -> int | None:
            s = (s or "").strip()
            if not s or "." not in s:
                return None
            parts = s.split(".")
            if len(parts) != 3:
                return None
            try:
                y = int(parts[2])
                return y + 2000 if y < 100 else y
            except ValueError:
                return None

        results: list[dict[str, Any]] = []
        current_year: int | None = None
        current_month_num: int | None = None
        last_known_year: int | None = None   # для месяцев без единой даты (пустой шаблон)

        for r in all_data[start_row:]:
            bh = (r[59] if len(r) > 59 else "").strip()
            if not bh:
                continue

            if bh == "Итого:":
                month_name = "Итого:"
                # привязываем к текущему month_num/year, чтобы можно было сортировать в конец блока
                month_num = current_month_num
            else:
                month_num = self._MONTHS_RU_TO_NUM.get(bh.lower())
                if month_num is None:
                    continue
                month_name = bh
                if month_num != current_month_num:
                    current_month_num = month_num
                    # Старт нового блока месяца. По умолчанию наследуем последний
                    # известный год: месяц без единой даты (пустой шаблон текущего/
                    # будущего месяца) иначе получил бы year=None, и его БД-записи
                    # из op_company_entries не сматчились бы по ключу (year, month)
                    # в sync_balance_company_sheet → молча выпали бы из листа
                    # «Баланс компании». Реальная дата в блоке (ниже) переопределит.
                    current_year = last_known_year

            bi_dmy = self._excel_serial_to_dmy(r[60] if len(r) > 60 else "")
            bo_dmy = self._excel_serial_to_dmy(r[66] if len(r) > 66 else "")

            # Год блока берём из первой реальной даты DD.MM.YYYY (она всегда
            # приоритетнее унаследованного) и запоминаем как last_known_year.
            if month_name != "Итого:":
                y = _parse_year_from_dmy(bi_dmy) or _parse_year_from_dmy(bo_dmy)
                if y is not None:
                    current_year = y
                    last_known_year = y

            amount_cashless = self._parse_num(r[61] if len(r) > 61 else "")
            nds = self._parse_num(r[62] if len(r) > 62 else "")
            description = (r[63] if len(r) > 63 else "").strip()
            taxes = self._parse_num(r[64] if len(r) > 64 else "")
            loan = self._parse_num(r[65] if len(r) > 65 else "")
            amount_other = self._parse_num(r[67] if len(r) > 67 else "")
            description_credit = (r[68] if len(r) > 68 else "").strip()

            # Лист «Баланс компании» — 1-в-1 копия BH-BQ из «Импорт ОП».
            # Тащим ВСЕ rows построчно, включая пустые template-rows
            # (структура листа должна быть идентична источнику).

            results.append({
                "year": current_year,
                "month_name": month_name,
                "month_num": month_num,
                "date_cashless": bi_dmy,
                "amount_cashless": amount_cashless,
                "nds": nds,
                "description": description,
                "taxes": taxes,
                "loan": loan,
                "date_other": bo_dmy,
                "amount_other": amount_other,
                "description_credit": description_credit,
            })

            if bh == "Итого:":
                current_year = None
                current_month_num = None

        log.info("op_journal: read %d journal rows from BH-BQ", len(results))
        return results

    _MONTHS_NUM_TO_RU = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }

    # «Баланс компании» layout (с 2026-05-19 вечер):
    #   row 1       — header журнала (BALANCE_JOURNAL_HEADER, 11 колонок)
    #   rows 2..N   — структура BH-BQ Импорт ОП:
    #                   data rows из листа (с заполненными BI-BQ),
    #                   op_company_entries бот вставляет в первые свободные
    #                   template-rows своего месяца (BH=имя_месяца, BI-BQ пусто),
    #                   extra op_company_entries (если template-rows кончились)
    #                   — перед «Итого:» месяца,
    #                   блок месяца выравнивается до BALANCE_MONTH_BLOCK_ROWS,
    #                   «Итого:» автоматически пересчитывается с нуля.

    def sync_balance_company_sheet_sync(
        self,
        journal_rows: list[dict[str, Any]] | None = None,
    ) -> int:
        """Write detailed BH-BQ journal into «Баланс компании» sheet.

        Returns: количество записанных журнальных строк (без header).
        """
        if not self.cfg.enabled:
            return 0
        journal_rows = journal_rows or []
        with self._sync_lock:
            sh = self._get_spreadsheet()
            title = self.cfg.balance_company_tab

            needed_cols = len(BALANCE_JOURNAL_HEADER)        # 11
            needed_rows = 1 + len(journal_rows) + 5          # header + data + резерв

            try:
                ws = sh.worksheet(title)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(
                    title=title,
                    rows=max(200, needed_rows),
                    cols=needed_cols + 1,
                )

            if ws.col_count < needed_cols:
                ws.resize(cols=needed_cols + 1)
            if ws.row_count < needed_rows:
                ws.resize(rows=needed_rows + 20)

            # Полная очистка листа (в т.ч. остатки старой сводки в колонке L).
            total_rows = ws.row_count
            clear_cols = max(needed_cols, ws.col_count)
            last_col_letter = gspread.utils.rowcol_to_a1(1, clear_cols).rstrip("1")
            ws.batch_clear([f"A1:{last_col_letter}{total_rows}"])

            # --- 1) HEADER журнала (row 1) ---
            hdr_end = gspread.utils.rowcol_to_a1(1, needed_cols)
            ws.update([BALANCE_JOURNAL_HEADER], f"A1:{hdr_end}")

            # --- 2) ДАННЫЕ журнала (rows 2+). 10 колонок, точная копия BH-BQ. ---
            if journal_rows:
                fmt = lambda v: self._fmt_amount(v) if v is not None else ""
                journal_data: list[list[Any]] = []
                for j in journal_rows:
                    journal_data.append([
                        j.get("month_name") or "",                            # A → BH
                        j.get("date_cashless") or "",                         # B → BI
                        fmt(j.get("amount_cashless")),                        # C → BJ
                        fmt(j.get("nds")),                                    # D → BK
                        j.get("description") or "",                           # E → BL
                        fmt(j.get("taxes")),                                  # F → BM
                        fmt(j.get("loan")),                                   # G → BN
                        j.get("date_other") or "",                            # H → BO
                        fmt(j.get("amount_other")),                           # I → BP
                        j.get("description_credit") or "",                    # J → BQ
                    ])
                end_row = 1 + len(journal_data)
                range_end = gspread.utils.rowcol_to_a1(end_row, needed_cols)
                ws.update(
                    journal_data,
                    f"A2:{range_end}",
                    value_input_option="USER_ENTERED",
                )

            self._worksheets.pop(title, None)
            log.info("balance_company: wrote %d journal rows", len(journal_rows))
            return len(journal_rows)

    async def sync_balance_company_sheet(self, db) -> int:
        """Sync «Баланс компании» sheet — структура BH-BQ Импорт ОП + auto-fill из БД.

        Поведение:
          1. Лист построчно повторяет диапазон BH-BQ из «Импорт ОП» (структура
             с template-rows сохраняется).
          2. Записи из `op_company_entries` (UI ввод через FSM OpAddSG) бот
             автоматически вставляет в **первые свободные template-rows своего
             месяца** (пустые rows вида BH=имя_месяца, BI-BQ пусто). Дата
             берётся из `date_display` записи (FSM OpAddSG записывает её
             автоматически как `now()` МСК).
          3. Если template-rows для месяца кончились — extra записи вставляются
             ПЕРЕД строкой «Итого:» этого месяца (сдвигая её вниз).
          4. Блок каждого месяца выравнивается до BALANCE_MONTH_BLOCK_ROWS строк:
             не хватает — добивается пустыми строками своего месяца, данных
             больше — кладутся все (данные не прячем).
          5. Строка «Итого:» автоматически пересчитывается с нуля по всем data
             rows месяца (sheet + БД-вставки): amount_cashless / nds / taxes /
             loan / amount_other. Если итог = 0, ячейка остаётся пустой.

        В лист «Импорт ОП» бот не пишет — это read-only источник (см.
        feedback-import-op-readonly).
        """
        if not self.cfg.enabled:
            return 0
        await self.import_op_monthly_balance(db)
        sheet_journal = await asyncio.to_thread(self.read_op_journal_rows_sync)

        # БД-записи из op_company_entries, формат same-as read_op_journal_rows_sync
        try:
            db_entries = await db.list_op_company_entries()
        except Exception as ex:
            log.warning("balance_company: list_op_company_entries failed: %s", ex)
            db_entries = []

        from collections import defaultdict
        db_by_month: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
        for r in db_entries:
            key = (int(r["year"]), int(r["month"]))
            db_by_month[key].append({
                "year":               r["year"],
                "month_num":          r["month"],
                "month_name":         self._MONTHS_NUM_TO_RU.get(r["month"], ""),
                "date_cashless":      r.get("date_display") or "",
                "amount_cashless":    r.get("cashless_amount"),
                "nds":                r.get("nds"),
                "description":        r.get("description") or "",
                "taxes":              r.get("taxes"),
                "loan":               r.get("loan"),
                "date_other":         r.get("date_other_display") or "",
                "amount_other":       r.get("other_amount"),
                "description_credit": r.get("description_credit") or "",
            })
        # Сортируем БД-записи внутри месяца по дате (затем id вставки = order в list).
        for key in db_by_month:
            db_by_month[key].sort(
                key=lambda x: x.get("date_cashless") or x.get("date_other") or "",
            )

        def _is_empty_template(j: dict[str, Any]) -> bool:
            """True если row — пустая template (BH=имя_месяца, BI-BQ пусто)."""
            if (j.get("month_name") or "") == "Итого:":
                return False
            return not any([
                j.get("date_cashless"),
                j.get("amount_cashless") is not None,
                j.get("nds") is not None,
                (j.get("description") or "").strip(),
                j.get("taxes") is not None,
                j.get("loan") is not None,
                j.get("date_other"),
                j.get("amount_other") is not None,
                (j.get("description_credit") or "").strip(),
            ])

        # Walk 1: вставляем БД-записи в пустые template-rows + flush остатки перед «Итого:».
        output: list[dict[str, Any]] = []
        for j in sheet_journal:
            month_name = (j.get("month_name") or "")
            if month_name == "Итого:":
                # Перед «Итого:» — flush оставшиеся БД-записи этого месяца.
                key = (int(j.get("year") or 0), int(j.get("month_num") or 0))
                remaining = db_by_month.get(key) or []
                for e in remaining:
                    e["month_name"] = self._MONTHS_NUM_TO_RU.get(key[1], "")
                    output.append(e)
                db_by_month[key] = []
                output.append(j)  # placeholder для пересчёта на walk 2
                continue
            if _is_empty_template(j):
                key = (int(j.get("year") or 0), int(j.get("month_num") or 0))
                pending = db_by_month.get(key) or []
                if pending:
                    e = pending.pop(0)
                    e["month_name"] = j.get("month_name") or e["month_name"]
                    output.append(e)
                    continue
            output.append(j)

        # Walk 1.5: УКЛАДКА без пустых строк внутри блока месяца (owner 04.07:
        # «нельзя пропускать строки»). Записи op_company_entries односторонние
        # (либо б/н B–G, либо кредит H–J), поэтому без укладки каждая занимает
        # свою строку → «лесенка» (пусто в другой половине). Здесь два столбца
        # пакуются к верху и зиппуются построчно.
        # ГЕЙТ: блок месяца пакуется ТОЛЬКО если в нём есть И б/н, И кредит.
        # Месяцы с односторонним журналом (только б/н или только кредит) остаются
        # байт-в-байт без изменений — правка их не касается.
        # Строки «Итого:» проходят как есть (не модифицируются); пересчёт итогов —
        # ниже в Walk 2, он не трогается.
        def _has_bn(j: dict[str, Any]) -> bool:
            return any([
                j.get("date_cashless"),
                j.get("amount_cashless") is not None,
                j.get("nds") is not None,
                (j.get("description") or "").strip(),
                j.get("taxes") is not None,
                j.get("loan") is not None,
            ])

        def _has_credit(j: dict[str, Any]) -> bool:
            return any([
                j.get("date_other"),
                j.get("amount_other") is not None,
                (j.get("description_credit") or "").strip(),
            ])

        packed: list[dict[str, Any]] = []
        month_block: list[dict[str, Any]] = []

        def _flush_month() -> None:
            if not month_block:
                return
            bn = [b for b in month_block if _has_bn(b)]
            cr = [b for b in month_block if _has_credit(b)]
            # Гейт: пакуем только смешанный блок (есть обе стороны). Иначе — as-is.
            if not (bn and cr):
                packed.extend(month_block)
                month_block.clear()
                return
            for i in range(max(len(bn), len(cr))):
                src = (bn[i] if i < len(bn) else cr[i])
                row: dict[str, Any] = {
                    "year": src.get("year"),
                    "month_num": src.get("month_num"),
                    "month_name": src.get("month_name") or "",
                    "date_cashless": "", "amount_cashless": None, "nds": None,
                    "description": "", "taxes": None, "loan": None,
                    "date_other": "", "amount_other": None, "description_credit": "",
                }
                if i < len(bn):
                    b = bn[i]
                    row["date_cashless"]   = b.get("date_cashless") or ""
                    row["amount_cashless"] = b.get("amount_cashless")
                    row["nds"]             = b.get("nds")
                    row["description"]     = b.get("description") or ""
                    row["taxes"]           = b.get("taxes")
                    row["loan"]            = b.get("loan")
                if i < len(cr):
                    c = cr[i]
                    row["date_other"]         = c.get("date_other") or ""
                    row["amount_other"]       = c.get("amount_other")
                    row["description_credit"] = c.get("description_credit") or ""
                packed.append(row)
            month_block.clear()

        for j in output:
            if (j.get("month_name") or "") == "Итого:":
                _flush_month()
                packed.append(j)          # «Итого:» — как есть, без изменений
            else:
                month_block.append(j)
        _flush_month()                    # хвост (текущий месяц без «Итого:»)
        output = packed

        # Walk 1.7: блок месяца = ровно BALANCE_MONTH_BLOCK_ROWS строк (owner 25.07).
        # Данных меньше — добиваем пустыми строками своего месяца; больше — кладём
        # все data rows (данные не прячем, блок просто выше 20). Пустые строки в
        # суммы не входят, поэтому Walk 2 ниже не затрагивается.
        # Строки «Итого:» проходят как есть — они не часть блока.
        sized: list[dict[str, Any]] = []
        size_block: list[dict[str, Any]] = []

        def _blank_row(src: dict[str, Any]) -> dict[str, Any]:
            return {
                "year": src.get("year"),
                "month_num": src.get("month_num"),
                "month_name": src.get("month_name") or "",
                "date_cashless": "", "amount_cashless": None, "nds": None,
                "description": "", "taxes": None, "loan": None,
                "date_other": "", "amount_other": None, "description_credit": "",
            }

        def _flush_sized() -> None:
            if not size_block:
                return
            rows = [b for b in size_block if not _is_empty_template(b)]
            meta = rows[0] if rows else size_block[0]
            while len(rows) < BALANCE_MONTH_BLOCK_ROWS:
                rows.append(_blank_row(meta))
            sized.extend(rows)
            size_block.clear()

        for j in output:
            if (j.get("month_name") or "") == "Итого:":
                _flush_sized()
                sized.append(j)
            else:
                size_block.append(j)
        _flush_sized()                    # хвост (текущий месяц без «Итого:»)
        output = sized

        # Walk 2: пересчёт «Итого:» по всем data rows месяца.
        for i, j in enumerate(output):
            if (j.get("month_name") or "") != "Итого:":
                continue
            m = int(j.get("month_num") or 0)
            y = int(j.get("year") or 0)
            total = {
                "year": y, "month_name": "Итого:", "month_num": m,
                "date_cashless": "", "date_other": "",
                "amount_cashless": 0.0, "nds": 0.0, "description": "",
                "taxes": 0.0, "loan": 0.0,
                "amount_other": 0.0, "description_credit": "",
            }
            for r in output:
                if (r.get("month_name") or "") == "Итого:":
                    continue
                if int(r.get("month_num") or 0) != m:
                    continue
                total["amount_cashless"] += float(r.get("amount_cashless") or 0)
                total["nds"]             += float(r.get("nds") or 0)
                total["taxes"]           += float(r.get("taxes") or 0)
                total["loan"]            += float(r.get("loan") or 0)
                total["amount_other"]    += float(r.get("amount_other") or 0)
            # 0 → None: пустая ячейка вместо «0».
            for f in ("amount_cashless", "nds", "taxes", "loan", "amount_other"):
                if not total[f]:
                    total[f] = None
            output[i] = total

        return await asyncio.to_thread(
            self.sync_balance_company_sheet_sync, output,
        )

    # ТЗ 2026-05-19 блок C: лист «Авансирование сотрудников» — журнал всех событий
    # (give/approve/pay/offset/writeoff). Полная перезапись при каждом sync.
    # До 2026-05-19 вечер назывался «Авансы монтажников» — переименован.

    def sync_advances_journal_sheet_sync(
        self,
        events: list[list[Any]] | None = None,
    ) -> int:
        """Write advance events into «Авансирование сотрудников» sheet.

        events = pre-rendered rows (как строки таблицы). Возвращает количество
        записанных строк (без header).
        """
        if not self.cfg.enabled:
            return 0
        events = events or []
        with self._sync_lock:
            sh = self._get_spreadsheet()
            title = self.cfg.advances_tab
            needed_cols = max(
                len(ADVANCES_SUMMARY_HEADER), len(ADVANCES_JOURNAL_HEADER),
                len(CREDIT_SUMMARY_HEADER), len(CREDIT_JOURNAL_HEADER),
            )  # 10 (журнал авансов = кредит-журнал = 10 кол.; сводки уже)
            needed_rows = 1 + len(events) + 5
            try:
                ws = sh.worksheet(title)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(
                    title=title,
                    rows=max(200, needed_rows),
                    cols=needed_cols + 1,
                )
            if ws.col_count < needed_cols:
                ws.resize(cols=needed_cols + 1)
            if ws.row_count < needed_rows:
                ws.resize(rows=needed_rows + 20)
            total_rows = ws.row_count
            clear_cols = max(needed_cols, ws.col_count)
            last_col_letter = gspread.utils.rowcol_to_a1(1, clear_cols).rstrip("1")
            ws.batch_clear([f"A1:{last_col_letter}{total_rows}"])
            # Row 1 = накопительная сводка-шапка (ТЗ 30.05 Часть A). Журнал событий —
            # ниже, со своей под-шапкой (ADVANCES_JOURNAL_HEADER), которую кладёт билдер.
            hdr_cols = len(ADVANCES_SUMMARY_HEADER)
            hdr_end = gspread.utils.rowcol_to_a1(1, hdr_cols)
            ws.update([ADVANCES_SUMMARY_HEADER], f"A1:{hdr_end}")
            # Col A («Дата») — формат TEXT, чтобы Google Sheets не интерпретировал
            # DD.MM.YYYY как date-serial и не показывал YYYY-MM-DD.
            try:
                ws.format("A:A", {"numberFormat": {"type": "TEXT"}})
            except Exception as ex:
                log.warning("advances_journal: failed to set TEXT format on col A: %s", ex)
            # Header style: bold + light-grey background + center align + freeze top row.
            try:
                ws.format(f"A1:{hdr_end}", {
                    "textFormat": {"bold": True, "fontSize": 11},
                    "backgroundColor": {"red": 0.91, "green": 0.91, "blue": 0.91},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "wrapStrategy": "WRAP",
                })
            except Exception as ex:
                log.warning("advances_journal: failed to style header: %s", ex)
            try:
                ws.freeze(rows=1)
            except Exception as ex:
                log.warning("advances_journal: failed to freeze row 1: %s", ex)
            if events:
                end_row = 1 + len(events)
                range_end = gspread.utils.rowcol_to_a1(end_row, needed_cols)
                # value_input_option='RAW' — не интерпретировать DD.MM.YYYY как дату.
                ws.update(events, f"A2:{range_end}", value_input_option="RAW")
            self._worksheets.pop(title, None)
            log.info("advances_journal: wrote %d event rows", len(events))
            return len(events)

    async def sync_advances_journal_sheet(self, db) -> int:
        """Async wrapper: build events из db.list_all_advance_events → write.

        Структура events (10 элементов на row):
            [дата, сотрудник, роль, тип, объект, объект подробно, сумма,
             остаток, request_id, статус]

        Колонка C «Роль» — utils.role_label(users.role).
        Колонка F «Объект подробно» — invoice_number · object_address · creator_role
        для offset (creator_role через read-only db.get_invoice); comment из
        request для request-level.
        Колонка H «Остаток» — running balance unallocated per-сотрудник,
        двигают только `paid` (+total_amount) и `offset` (-offset_amount).

        Сортировка: ISO timestamp DESC + event_order DESC (give=1 < approved=2
        < paid=3 < rejected=4 < offset=5 < writeoff=6). В одну минуту paid
        отображается выше approved и give.

        В конце листа — пустая разделительная row и итоговые row per сотрудник
        (выдано / зачтено / остаток).
        """
        if not self.cfg.enabled:
            return 0
        bundle = await db.list_all_advance_events(limit=500)
        requests = bundle.get("requests", []) if isinstance(bundle, dict) else []
        items = bundle.get("items", []) if isinstance(bundle, dict) else []
        # Кэш installer_id → username/label
        installer_cache: dict[int, str] = {}
        # Кэш installer_id → role label (Монтажник / Менеджер НПН / ...)
        role_cache: dict[int, str] = {}
        # Кэш invoice_id → creator_role label (КВ / НПН / КИА / "")
        invoice_role_cache: dict[int, str] = {}

        async def _user_for(installer_id: int):
            try:
                return await db.get_user_optional(installer_id)
            except Exception:
                return None

        async def _installer_label(installer_id: int) -> str:
            if installer_id in installer_cache:
                return installer_cache[installer_id]
            user = await _user_for(installer_id)
            label = (
                f"@{user.username}" if user and getattr(user, "username", None)
                else (getattr(user, "full_name", "") or str(installer_id))
            )
            installer_cache[installer_id] = label
            return label

        async def _role_label(installer_id: int) -> str:
            if installer_id in role_cache:
                return role_cache[installer_id]
            user = await _user_for(installer_id)
            raw = (getattr(user, "role", "") or "").strip() if user else ""
            # role_label() сам обрабатывает составные роли ("manager_npn,manager_kv")
            # и возвращает "—" для пустых значений → нормализуем в "".
            label = role_label(raw) if raw else ""
            if label == "—":
                label = ""
            role_cache[installer_id] = label
            return label

        async def _wallet_role_label(installer_id: int, wallet_role: Any) -> str:
            """Метка роли для строки журнала (wallet-sep 29.05).

            Для двуролевого РП+Менеджер (Павел) берём роль из wallet_role записи
            ('rp' → РП, 'manager_npn' → Менеджер НПН), чтобы РП и НПН не сливались.
            Иначе (NULL) — общая роль сотрудника (users.role).
            """
            wr = (str(wallet_role).strip() if wallet_role else "")
            if wr:
                lbl = role_label(wr)
                if lbl and lbl != "—":
                    return lbl
            return await _role_label(installer_id)

        async def _invoice_creator_role_label(invoice_id: int) -> str:
            if invoice_id in invoice_role_cache:
                return invoice_role_cache[invoice_id]
            try:
                inv = await db.get_invoice(int(invoice_id))
            except Exception:
                inv = None
            raw = (inv.get("creator_role") if inv else "") or ""
            label = role_label(raw) if raw else ""
            if label == "—":
                label = ""
            invoice_role_cache[invoice_id] = label
            return label

        def _fmt_dt(ts: Any) -> str:
            """ISO 'YYYY-MM-DDTHH:MM:SS...' или 'YYYY-MM-DD' → 'DD.MM.YYYY' МСК.
            Если уже DD.MM.YYYY — возвращаем as-is."""
            if not ts:
                return ""
            s = str(ts)
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                try:
                    return f"{s[8:10]}.{s[5:7]}.{s[0:4]}"
                except Exception:
                    return s[:10]
            return s[:10] if len(s) >= 10 else s

        # event_type → sort order (внутри одной даты: paid отображается выше give).
        # TZ tingly-twirling-whistle: +deposit=7 (ГД пополнил), +withdraw=8 (Игорь снял).
        # funds-2balances 25.05: +transfer=9 (сотрудник перевёл депо→аванс).
        EVENT_ORDER = {
            "give": 1, "approved": 2, "paid": 3,
            "rejected": 4, "offset": 5, "writeoff": 6,
            "deposit": 7, "withdraw": 8, "transfer": 9,
        }

        # Pre-pass: per-installer running balance + per-installer aggregates для footer.
        balance_after: dict[tuple, float] = {}
        movements: list[tuple[int, str, str, str, float]] = []
        paid_total: dict[int, float] = {}        # выдано (request_type='request')
        deposit_total: dict[int, float] = {}     # внесено ГД (request_type='deposit')
        withdraw_total: dict[int, float] = {}    # списания Игоря (request_type='withdraw')
        transfer_total: dict[int, float] = {}    # переводы депо→аванс (funds-2balances 25.05)
        offset_total: dict[int, float] = {}      # зачёты в ZP
        for r in requests:
            paid_at = r.get("paid_at")
            if not paid_at:
                continue
            amt = float(r.get("total_amount") or 0)
            inst = int(r["installer_id"])
            req_type = (r.get("request_type") or "request").lower()
            if req_type == "withdraw":
                # withdraw: уменьшает баланс на amt.
                movements.append(
                    (inst, str(paid_at), "withdraw", f"req-{r.get('req_id')}", -amt),
                )
                withdraw_total[inst] = withdraw_total.get(inst, 0.0) + amt
            elif req_type == "deposit":
                # deposit от ГД: пополняет баланс.
                movements.append(
                    (inst, str(paid_at), "deposit", f"req-{r.get('req_id')}", amt),
                )
                deposit_total[inst] = deposit_total.get(inst, 0.0) + amt
            elif req_type == "transfer_depo_to_adv":
                # transfer: с депозита на advance — total баланс не меняется,
                # но в журнале фиксируется отдельной строкой (для аудита).
                movements.append(
                    (inst, str(paid_at), "transfer", f"req-{r.get('req_id')}", 0.0),
                )
                transfer_total[inst] = transfer_total.get(inst, 0.0) + amt
            else:
                # request (классика): paid пополняет баланс.
                movements.append(
                    (inst, str(paid_at), "paid", f"req-{r.get('req_id')}", amt),
                )
                paid_total[inst] = paid_total.get(inst, 0.0) + amt
        for it in items:
            off_at = it.get("offset_at")
            if not off_at:
                continue
            amt = float(it.get("offset_amount") or 0)
            inst = int(it["installer_id"])
            kind = "offset" if amt > 0 else "writeoff"
            delta = -amt if amt > 0 else 0.0
            movements.append((inst, str(off_at), kind, f"item-{it.get('id')}", delta))
            if amt > 0:
                offset_total[inst] = offset_total.get(inst, 0.0) + amt
        movements.sort(key=lambda m: (m[0], m[1]))  # ASC (installer_id, ts)
        per_installer: dict[int, float] = {}
        for inst_id, ts, kind, src, delta in movements:
            per_installer[inst_id] = per_installer.get(inst_id, 0.0) + delta
            balance_after[(inst_id, kind, src, ts)] = per_installer[inst_id]

        def _bal(key: tuple) -> str:
            v = balance_after.get(key)
            return self._fmt_amount(v) if v is not None else ""

        # tagged_events: (iso_ts_sort, event_order, row_data)
        tagged_events: list[tuple[str, int, list[Any]]] = []

        def _add(iso_ts: str, kind: str, row: list[Any]) -> None:
            tagged_events.append((str(iso_ts or ""), EVENT_ORDER.get(kind, 99), row))

        for r in requests:
            installer_id = int(r["installer_id"])
            inst_label = await _installer_label(installer_id)
            role_lbl = await _wallet_role_label(installer_id, r.get("wallet_role"))
            req_id = r.get("req_id")
            comment_detail = (r.get("comment") or "")[:200]
            amount_str = self._fmt_amount(r.get("total_amount") or 0)
            req_type = (r.get("request_type") or "request").lower()
            # TZ tingly-twirling-whistle: deposit (ГД пополнил) — 1 row вместо 4.
            if req_type == "deposit":
                if r.get("paid_at"):
                    _add(r["paid_at"], "deposit", [
                        _fmt_dt(r["paid_at"]), inst_label, role_lbl, "deposit",
                        "(депозит)", comment_detail, amount_str,
                        _bal((installer_id, "deposit", f"req-{req_id}", str(r["paid_at"]))),
                        str(req_id or ""), "по инициативе ГД",
                    ])
                continue
            # TZ tingly-twirling-whistle: withdraw (Игорь снял на расход) — 1 row.
            if req_type == "withdraw":
                if r.get("paid_at"):
                    _add(r["paid_at"], "withdraw", [
                        _fmt_dt(r["paid_at"]), inst_label, role_lbl, "withdraw",
                        "(расход)", comment_detail, amount_str,
                        _bal((installer_id, "withdraw", f"req-{req_id}", str(r["paid_at"]))),
                        str(req_id or ""), "расход",
                    ])
                continue
            # funds-2balances 25.05: transfer депо→аванс — 1 row.
            if req_type == "transfer_depo_to_adv":
                if r.get("paid_at"):
                    _add(r["paid_at"], "transfer", [
                        _fmt_dt(r["paid_at"]), inst_label, role_lbl, "transfer",
                        "(депо→аванс)", comment_detail, amount_str,
                        _bal((installer_id, "transfer", f"req-{req_id}", str(r["paid_at"]))),
                        str(req_id or ""), "перевод",
                    ])
                continue
            # request (классика): give → approved → paid → (rejected).
            req_at = r.get("requested_at")
            _add(req_at, "give", [
                _fmt_dt(req_at), inst_label, role_lbl, "give", "(пакет)",
                comment_detail, amount_str, "", str(req_id or ""),
                str(r.get("status") or ""),
            ])
            if r.get("approved_at"):
                _add(r["approved_at"], "approved", [
                    _fmt_dt(r["approved_at"]), inst_label, role_lbl, "approved",
                    "(пакет)", comment_detail, amount_str, "", str(req_id or ""),
                    "approved",
                ])
            if r.get("paid_at"):
                _add(r["paid_at"], "paid", [
                    _fmt_dt(r["paid_at"]), inst_label, role_lbl, "paid", "(пакет)",
                    comment_detail, amount_str,
                    _bal((installer_id, "paid", f"req-{req_id}", str(r["paid_at"]))),
                    str(req_id or ""), "paid",
                ])
            if r.get("rejected_at"):
                _add(r["rejected_at"], "rejected", [
                    _fmt_dt(r["rejected_at"]), inst_label, role_lbl, "rejected",
                    "(пакет)",
                    (r.get("reject_reason") or comment_detail or "")[:200],
                    amount_str, "", str(req_id or ""), "rejected",
                ])

        for it in items:
            if not it.get("offset_at"):
                continue
            installer_id = int(it["installer_id"])
            inst_label = await _installer_label(installer_id)
            role_lbl = await _role_label(installer_id)
            amt = float(it.get("offset_amount") or 0)
            kind = "offset" if amt > 0 else "writeoff"
            inv_num = it.get("invoice_number") or ""
            obj_addr = it.get("object_address") or ""
            inv_id = it.get("invoice_id")
            creator_role_lbl = (
                await _invoice_creator_role_label(int(inv_id)) if inv_id else ""
            )
            parts = [p for p in (inv_num, obj_addr, creator_role_lbl) if p]
            detailed = " · ".join(parts) if parts else (inv_num or obj_addr)
            _add(it.get("offset_at"), kind, [
                _fmt_dt(it.get("offset_at")), inst_label, role_lbl, kind, inv_num,
                detailed,
                self._fmt_amount(it.get("offset_amount") or 0),
                _bal((installer_id, kind, f"item-{it.get('id')}", str(it.get("offset_at")))),
                str(it.get("request_id") or ""), "",
            ])

        # Хронологическая сортировка DESC: (iso_ts, event_order).
        # Внутри одинаковой даты: paid (3) выше approved (2) выше give (1).
        tagged_events.sort(key=lambda t: (t[0], t[1]), reverse=True)
        events: list[list[Any]] = [t[2] for t in tagged_events]

        # ---- ТЗ 30.05 Часть A: накопительная сводка по кошелькам (шапка листа) ----
        # Заменяет прежний футер «ИТОГО». По каждому кошельку-блоку:
        #   Внесено ГД · Зачислено из ЗП (Часть B/C, пока 0) · Выдано/выведено ·
        #   Кошелёк аванса · Кошелёк депозита.
        # Павел (rp,manager_npn) — 2 блока (РП / Менеджер НПН). Балансы =
        # get_advance_balance/get_deposit_balance (те же числа, что бот показывает везде).
        def _wallet_match(row_wallet: Any, wr: str | None) -> bool:
            rw = (str(row_wallet).strip() if row_wallet else "")
            if wr is None:
                return True
            if wr == "rp":
                return rw == "rp"
            return rw == "" or rw != "rp"   # primary / manager_npn

        def _blocks_for(role_raw: str) -> list[tuple[str | None, str, str]]:
            roles = [x.strip().lower() for x in (role_raw or "").split(",") if x.strip()]
            blocks: list[tuple[str | None, str, str]] = []
            if "installer" in roles:
                blocks.append((None, role_label("installer"), "installer"))
            if "rp" in roles:
                blocks.append(("rp", role_label("rp"), "rp"))
            if "manager_npn" in roles:
                blocks.append(("manager_npn", role_label("manager_npn"), "manager_npn"))
            if "manager_kv" in roles:
                blocks.append((None, role_label("manager_kv"), "manager_kv"))
            if "manager_kia" in roles:
                blocks.append((None, role_label("manager_kia"), "manager_kia"))
            return blocks

        try:
            roster_users = await db.list_users(limit=500)
        except Exception:
            roster_users = []
        _ROLE_PRIO = {"Монтажник": 0, "РП": 1, "Менеджер НПН": 2,
                      "Менеджер КВ": 3, "Менеджер КИА": 4}
        summary_specs: list[tuple[int, int, str, Any, str]] = []
        for u in roster_users:
            uid = getattr(u, "telegram_id", None)
            if uid is None:
                continue
            for wr, rlbl, rkey in _blocks_for(getattr(u, "role", "") or ""):
                summary_specs.append((_ROLE_PRIO.get(rlbl, 9), int(uid), rlbl, wr, rkey))
        summary_specs.sort(key=lambda s: (s[0], s[1]))

        summary_rows: list[list[Any]] = []
        for _prio, emp_id, rlbl, wr, rkey in summary_specs:
            label = await _installer_label(emp_id)
            gd_in = 0.0
            withdraw_out = 0.0
            for r in requests:
                if int(r.get("installer_id") or 0) != emp_id or not r.get("paid_at"):
                    continue
                if not _wallet_match(r.get("wallet_role"), wr):
                    continue
                rtype = (r.get("request_type") or "request").lower()
                ini = (r.get("initiator") or "installer").lower()
                amt = float(r.get("total_amount") or 0)
                if ini == "gd" and rtype in ("deposit", "request"):
                    gd_in += amt
                elif rtype == "withdraw":
                    withdraw_out += amt
            # ТЗ Часть B/C (30.05): незабранная ЗП → ОТОБРАЖАЕТСЯ как накопленная
            # в кошельке аванса. Только показатель — НЕ влияет на реальный баланс
            # (adv_bal ниже) и право вывода; реальная выдача ЗП идёт прежним путём
            # (запрос → одобрение ГД). Σ невыплаченной ЗП по счетам, по роли.
            try:
                zp_credited = await db.get_unpaid_zp_for_summary(emp_id, rkey)
            except Exception:
                zp_credited = 0.0
            try:
                adv_bal = await db.get_advance_balance(emp_id, wr)
            except Exception:
                adv_bal = 0.0
            try:
                depo_bal = await db.get_deposit_balance(emp_id, wr)
            except Exception:
                depo_bal = 0.0
            # ТЗ user 29.05 (вариант «а»): статус запроса аванса по сотруднику.
            # Только request_type='request' (классика give→approved→paid→rejected).
            req_statuses = {
                (r.get("status") or "").lower()
                for r in requests
                if int(r.get("installer_id") or 0) == emp_id
                and _wallet_match(r.get("wallet_role"), wr)
                and (r.get("request_type") or "request").lower() == "request"
            }
            if "requested" in req_statuses:
                status_label = "на одобрении"
            elif "approved" in req_statuses:
                status_label = "одобрен, ждёт оплаты"
            else:
                status_label = "нет активных"
            # Столбец «Кошелёк аванса»: реальный остаток + накопленная (невыплаченная)
            # ЗП — по решению user'а 30.05 показываем «всё, что накопилось» одной цифрой
            # (ОТОБРАЖЕНИЕ; вывод ЗП по-прежнему через запрос → одобрение ГД).
            summary_rows.append([
                label, rlbl,
                self._fmt_amount(gd_in),
                self._fmt_amount(zp_credited),
                self._fmt_amount(withdraw_out),
                self._fmt_amount(adv_bal + zp_credited),
                self._fmt_amount(depo_bal),
                status_label,
            ])

        # Компоновка: [сводка-строки] · пусто · «Журнал событий» · [под-шапка] · [события].
        # Row 1 (сводка-шапка ADVANCES_SUMMARY_HEADER) кладёт writer.
        WIDTH = max(
            len(ADVANCES_SUMMARY_HEADER),
            len(ADVANCES_JOURNAL_HEADER),
            len(CREDIT_SUMMARY_HEADER),
            len(CREDIT_JOURNAL_HEADER),
        )

        def _pad(row: list[Any]) -> list[Any]:
            return list(row) + [""] * (WIDTH - len(row))

        composed: list[list[Any]] = [_pad(r) for r in summary_rows]
        composed.append([""] * WIDTH)
        composed.append(_pad(["📋 Журнал событий"]))
        composed.append(_pad(list(ADVANCES_JOURNAL_HEADER)))
        composed.extend(_pad(ev) for ev in events)

        # ---- Кредитный кошелёк (TZ 2026-06-02, модель кошелька) ----
        # Источник — db.list_all_credit_events: по менеджерам КВ/КИА/НПН
        # сводка (Приход/Расход/Остаток) + журнал событий с running-остатком.
        # Приход = оплаченная часть кредит-счёта роли; расход = трата кошелька
        # (привязка к счёту+категория DP–DV / без привязки → назначение).
        # Carry между счетами убран (модель кошелька).
        try:
            credit_bundle = await db.list_all_credit_events(limit=500)
        except Exception:
            log.warning("list_all_credit_events failed", exc_info=True)
            credit_bundle = {"managers": []}
        credit_managers = (
            credit_bundle.get("managers", []) if isinstance(credit_bundle, dict) else []
        )
        _COST_TYPE_LABELS = {
            "metal": "Металл", "glass": "Стекло", "montazh": "Монтаж",
            "loaders": "Грузчики", "logistics": "Логистика",
            "extra_mat": "Доп. мат", "extra_svc": "Доп. услуги",
        }

        credit_summary_rows: list[list[Any]] = []
        credit_journal_rows: list[list[Any]] = []
        for m in credit_managers:
            mgr = m.get("label") or ""
            credit_summary_rows.append([
                mgr,
                self._fmt_amount(m.get("total_in") or 0),
                self._fmt_amount(m.get("total_out") or 0),
                self._fmt_amount(m.get("balance") or 0),
            ])
            for e in m.get("events", []):
                inv_num = e.get("invoice_number") or ""
                addr = e.get("object_address") or ""
                run = self._fmt_amount(e.get("running") or 0)
                if e.get("kind") == "in":
                    # base-приход (оплата счёта) vs ретро-доплата (гашение долга, п.3) — разный лейбл
                    in_purpose = (
                        "оконч. доплата"
                        if e.get("income_kind") == "debt_payment"
                        else "оплата кредитного счёта"
                    )
                    credit_journal_rows.append([
                        _fmt_dt(e.get("ts")), mgr, "⬆️ Приход", inv_num, addr,
                        in_purpose,
                        self._fmt_amount(e.get("amount") or 0), run, "",
                    ])
                else:  # out — трата кошелька
                    ct = _COST_TYPE_LABELS.get(e.get("cost_type") or "", "")
                    desc = (e.get("description") or "").strip()
                    if inv_num and ct:
                        purpose = f"{desc} · {ct}" if desc else ct
                    else:
                        purpose = desc or ct or "—"
                    who = ""
                    eb = e.get("entered_by")
                    if eb:
                        try:
                            who = await _installer_label(int(eb))
                        except Exception:
                            who = str(eb)
                    credit_journal_rows.append([
                        _fmt_dt(e.get("ts")), mgr, "⬇️ Расход", inv_num, addr,
                        purpose, self._fmt_amount(e.get("amount") or 0), run, who,
                    ])

        if credit_summary_rows or credit_journal_rows:
            composed.append([""] * WIDTH)
            composed.append(_pad(["🏦 Кредитный кошелёк"]))
            composed.append(_pad(list(CREDIT_SUMMARY_HEADER)))
            composed.extend(_pad(r) for r in credit_summary_rows)
            composed.append([""] * WIDTH)
            composed.append(_pad(list(CREDIT_JOURNAL_HEADER)))
            composed.extend(_pad(r) for r in credit_journal_rows)

        return await asyncio.to_thread(self.sync_advances_journal_sheet_sync, composed)

    def write_date_fact_to_op_sync(self, invoice_number: str, date_iso: str) -> bool:
        """Write Дата Факт (col J/9) back to source ОП sheet by invoice_number.
        DISABLED: запись в Импорт ОП запрещена."""
        log.debug("write_date_fact_to_op BLOCKED for %s (Импорт ОП write disabled)", invoice_number)
        return False
        if not self.cfg.source_spreadsheet_id:
            return False
        gc = self._get_client()
        try:
            source_sh = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = source_sh.worksheet(self.cfg.source_sheet_name)
        except Exception as e:
            log.error("Cannot open source sheet for write-back: %s", e)
            return False

        # Find row by invoice_number (col E, index 4, 1-based col 5)
        try:
            cell = ws.find(invoice_number, in_column=5)
        except gspread.CellNotFound:
            log.warning("Invoice %s not found in ОП sheet", invoice_number)
            return False

        if not cell:
            return False

        # Convert ISO date (possibly with time) to DD.MM.YYYY
        date_part = date_iso[:10]  # safely extract YYYY-MM-DD even if time/tz appended
        parts = date_part.split("-")
        if len(parts) == 3:
            date_dmy = f"{parts[2]}.{parts[1]}.{parts[0]}"
        else:
            date_dmy = date_iso

        # Col J = column 10 (1-based)
        ws.update_cell(cell.row, 10, date_dmy)
        log.info("Wrote Дата Факт %s for %s to ОП row %d", date_dmy, invoice_number, cell.row)
        return True

    async def write_date_fact_to_op(self, invoice_number: str, date_iso: str) -> bool:
        """Async wrapper."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(self.write_date_fact_to_op_sync, invoice_number, date_iso)

    # --- Generic field write-back to ОП ---

    _OP_FIELD_TO_COL: dict[str, int] = {
        "estimated_logistics": 16,  # col P (1-based) = index 15 → logistics
        "margin_pct": 21,           # col U (1-based) = "Рент-ть факт"
        "bot_status": 47,           # col AU (1-based) = Статус бота (+1 после вставки AI)
        "montazh_stage": 48,        # col AV (1-based) = Стадия монтажа (+1)
    }

    def write_field_to_op_sync(self, invoice_number: str, field: str, value: Any) -> bool:
        """Write a single field back to ОП sheet by invoice_number.
        DISABLED: запись в Импорт ОП запрещена."""
        return False

    async def write_field_to_op(self, invoice_number: str, field: str, value: Any) -> bool:
        """Async wrapper for generic field write-back."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(self.write_field_to_op_sync, invoice_number, field, value)

    def write_cell_to_sheet_sync(
        self, sheet_name: str, row: int, col_1based: int, value: str,
    ) -> bool:
        """Write a value to a specific cell in the source spreadsheet."""
        if not self.cfg.source_spreadsheet_id:
            return False
        gc = self._get_client()
        try:
            sp = gc.open_by_key(self.cfg.source_spreadsheet_id)
            ws = sp.worksheet(sheet_name)
        except Exception as e:
            log.error("Cannot open sheet %s for cell write: %s", sheet_name, e)
            return False
        ws.update_cell(row, col_1based, value)
        log.info("Wrote cell R%dC%d=%s in %s", row, col_1based, value[:50], sheet_name)
        return True

    async def write_cell_to_sheet(
        self, sheet_name: str, row: int, col_1based: int, value: str,
    ) -> bool:
        """Async wrapper for cell write."""
        if not self.cfg.enabled:
            return False
        return await asyncio.to_thread(
            self.write_cell_to_sheet_sync, sheet_name, row, col_1based, value,
        )
