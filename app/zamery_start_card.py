"""Замерщик — стартовая карточка-календарь «График замеров».

Подключается из handlers/common.py (cmd_start / cmd_menu / sync_data_non_gd,
role=ZAMERY) — триггеры как у других ролей (/start, /menu, «🔄 Синхронизация данных»).

Эталон-дизайн: <b>заголовок</b> + моноширинный <pre> с сеткой месяца.
  • занят (есть замер)  → ✅ вместо числа
  • выходной (blackout) → ❌ вместо числа
  • свободный день      → число
Понедельник первый. Ширина сетки ≈ как у «Этапы работы»: vw-выравнивание,
эмодзи = 2 колонки (тот же приём, что rp_start_card.vw) → столбцы ровно под шапкой.

Данные «в БД» — таблицы zamery_requests (замеры по датам) + zamery_blackout_dates
(выходные); отдельная таблица-календарь не заводится (дубль/рассинхрон). Тот же
календарь зеркалится на лист leads (O:U) методом sheets.upsert_zamery_calendar_sync.
"""
from __future__ import annotations

import calendar
import logging
import unicodedata
from datetime import date, datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

INDENT = "   "
WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTHS_NOM = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]


def vw(s: str) -> int:
    """Видимая ширина строки в моноширинном Telegram (эмодзи ≈ 2 колонки).
    Идентична rp_start_card.vw — общий приём выравнивания эмодзи-сетки."""
    w = 0
    for ch in s:
        o = ord(ch)
        if o == 0xFE0F:                       # variation selector — нулевая ширина
            continue
        if unicodedata.combining(ch):
            continue
        if (0x1F000 <= o <= 0x1FAFF) or (0x2600 <= o <= 0x27BF) or (0x2B00 <= o <= 0x2BFF):
            w += 2
        elif unicodedata.east_asian_width(ch) in ("W", "F"):
            w += 2
        else:
            w += 1
    return w


def _day_cell(day: int, busy: bool, off: bool, full: bool = False) -> str:
    """Ячейка дня — ровно 2 видимых колонки.
    Выходной → ❌, день занят → 🔒, есть замер → ✅, иначе число.
    Приоритет: выходной > занят > замер (full c default — для sheet-вызова render_calendar)."""
    if off:
        return "❌"
    if full:
        return "🔒"
    if busy:
        return "✅"
    return f"{day:>2}"


def render_calendar(year: int, month: int, busy_days: set[int], off_days: set[int]) -> str:
    """Карточка-календарь (эталон): <b>заголовок</b> + <pre>сетка</pre> + легенда."""
    ndays = calendar.monthrange(year, month)[1]
    start = date(year, month, 1).weekday()    # Пн=0 … Вс=6
    cells: list[str] = ["  "] * start
    for d in range(1, ndays + 1):
        cells.append(_day_cell(d, d in busy_days, d in off_days and d not in busy_days))
    while len(cells) % 7 != 0:
        cells.append("  ")

    head_inner = " ".join(WEEKDAYS)
    width = vw(head_inner)
    lines = [INDENT + head_inner]
    for i in range(0, len(cells), 7):
        lines.append(INDENT + " ".join(cells[i:i + 7]))
    lines.append(INDENT + "━" * width)
    footer = f"{INDENT}📐 {len(busy_days)} замеров"
    if off_days:
        footer += f"  ·  🚫 {len(off_days)} вых."
    lines.append(footer)

    title = f"📅  График замеров · {MONTHS_NOM[month - 1]}"
    body = "<pre>" + "\n".join(lines) + "</pre>"
    return f"<b>{title}</b>\n{body}\n<i>✅ занят  ·  ❌ выходной</i>"


async def month_marks(db, user_id: int) -> tuple[int, int, set[int], set[int]]:
    """(year, month, busy_days, off_days) текущего месяца замерщика (МСК).

    busy = дни месяца с замерами (status open/in_progress/done),
    off  = blackout-дни. user_id = telegram_id (он же zamery_requests.assigned_to)."""
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    year, month = now.year, now.month
    ndays = calendar.monthrange(year, month)[1]
    d_from = date(year, month, 1).isoformat()
    d_to = date(year, month, ndays).isoformat()

    busy_days: set[int] = set()
    off_days: set[int] = set()
    try:
        for z in await db.list_zamery_for_schedule(user_id, d_from, d_to):
            ds = z.get("scheduled_date")
            if ds and len(ds) >= 10:
                busy_days.add(int(ds[8:10]))
    except Exception:
        logger.exception("zamery card: list_zamery_for_schedule failed")
    try:
        for b in await db.list_zamery_blackout_dates(user_id, d_from, d_to):
            ds = b.get("blackout_date")
            if ds and len(ds) >= 10:
                off_days.add(int(ds[8:10]))
    except Exception:
        logger.exception("zamery card: list_zamery_blackout_dates failed")
    return year, month, busy_days, off_days


# ── Календарь-окно «Графика замеров» + карточка статистики по менеджерам ──
# График замеров показывает ОКНО вокруг «сейчас»: последние 15 дней прошлого месяца +
# весь текущий + первые 15 дней следующего (owner 09.07; прежде — текущий+следующий
# целиком). Счётчики по менеджерам — ОТДЕЛЬНОЕ сообщение build_zamery_manager_stats_card
# (прошлый/текущий/следующий месяц, КВ/КИА/НПН). Применяется к картам РП, ГД и замерщика.
# render_calendar/month_marks выше НЕ тронуты (их использует синк на «Leads» O:U).
# Всё read-only (только SELECT).

W = 20  # видимая ширина колонки месяца (= vw("Пн Вт Ср Чт Пт Сб Вс"))
SW = 9  # ширина колонки в карточке «по менеджерам» (3 месяца в ряд, mobile-safe)
WINDOW_TAIL = 15  # сколько дней от пред./след. месяца показывать в «Графике замеров»
_MGR_ROLES = (("manager_kv", "КВ"), ("manager_kia", "КИА"), ("manager_npn", "НПН"))


def _pad_vw(s: str, w: int) -> str:
    """Дополнить строку пробелами справа до видимой ширины w (эмодзи = 2 колонки)."""
    return s + " " * max(0, w - vw(s))


def _join_cols_n(cols: list[list[str]], w: int, gap: str = " ") -> list[str]:
    """Склеить N колонок строк бок о бок: INDENT + каждая колонка до ширины w через gap."""
    n = max((len(c) for c in cols), default=0)
    out: list[str] = []
    for i in range(n):
        cells = [_pad_vw(c[i] if i < len(c) else "", w) for c in cols]
        out.append((INDENT + gap.join(cells)).rstrip())
    return out


def _prev_cur_next_months() -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
    """((год,мес) прошлый, текущий, следующий) по МСК."""
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    y1, m1 = now.year, now.month
    y0, m0 = (y1 - 1, 12) if m1 == 1 else (y1, m1 - 1)
    y2, m2 = (y1 + 1, 1) if m1 == 12 else (y1, m1 + 1)
    return (y0, m0), (y1, m1), (y2, m2)


def _month_grid_rows(
    year: int, month: int, busy: set[int], off: set[int], full: set[int] | None = None,
    min_day: int = 1, max_day: int = 31,
) -> list[str]:
    """Строки сетки месяца: [название, шапка дней, 6 недель]. Дни вне [min_day..max_day]
    рендерятся пустыми (окно «Графика замеров»). Без footer-счётчика (числа — в карточке
    «по менеджерам»). Всегда 6 недель → ровная высота колонок."""
    full = full or set()
    ndays = calendar.monthrange(year, month)[1]
    start = date(year, month, 1).weekday()       # Пн=0 … Вс=6
    cells: list[str] = ["  "] * start
    for d in range(1, ndays + 1):
        if d < min_day or d > max_day:
            cells.append("  ")
        else:
            cells.append(_day_cell(d, d in busy, d in off, d in full))
    while len(cells) < 42:                        # добить до 6 полных недель
        cells.append("  ")
    rows = [MONTHS_NOM[month - 1], " ".join(WEEKDAYS)]
    for i in range(0, 42, 7):
        rows.append(" ".join(cells[i:i + 7]))
    return rows


def _cal_block(year: int, month: int, m: dict, min_day: int, max_day: int,
               label: str) -> list[str]:
    """Один месяц-блок «Графика замеров» с окном [min_day..max_day]. Пустые недели
    сверху и снизу (вне окна) срезаются для компактности; шапка дней сохраняется."""
    rows = _month_grid_rows(year, month, m["busy"], m["off"], m.get("full"),
                            min_day, max_day)
    weeks = rows[2:]
    while weeks and not weeks[0].strip():        # убрать пустые недели сверху
        weeks.pop(0)
    while weeks and not weeks[-1].strip():        # ... и снизу
        weeks.pop()
    return [(INDENT + r).rstrip() for r in ([label, rows[1]] + weeks)]


def render_window_calendar(y0, m0, c0, y1, m1, c1, y2, m2, c2) -> str:
    """Календарь «График замеров» — окно вокруг «сейчас»: последние WINDOW_TAIL дней
    прошлого месяца + весь текущий + первые WINDOW_TAIL дней следующего (owner 09.07).
    Три блока друг под другом (вертикаль, mobile-safe). c0/c1/c2 — dict из _collect."""
    nd0 = calendar.monthrange(y0, m0)[1]
    b0 = _cal_block(y0, m0, c0, nd0 - WINDOW_TAIL + 1, nd0,
                    f"{MONTHS_NOM[m0 - 1]} · посл. {WINDOW_TAIL} дн.")
    b1 = _cal_block(y1, m1, c1, 1, 31, MONTHS_NOM[m1 - 1])
    b2 = _cal_block(y2, m2, c2, 1, WINDOW_TAIL,
                    f"{MONTHS_NOM[m2 - 1]} · первые {WINDOW_TAIL} дн.")
    lines = b0 + [""] + b1 + [""] + b2
    body = "<pre>" + "\n".join(lines) + "</pre>"
    return f"<b>📅  График замеров</b>\n{body}\n<i>✅ замер  ·  🔒 занят  ·  ❌ выходной</i>"


def _stats_rows(month: int, st: dict) -> list[str]:
    by = st["by_mgr"]
    rows = [MONTHS_NOM[month - 1]]
    for role, lbl in _MGR_ROLES:
        rows.append(f"{lbl:<3} {by.get(role, 0)}")
    rows.append("━" * 7)
    rows.append(f"Всего {st['total']}")
    return rows


def render_manager_stats(cols: list[tuple[int, dict]]) -> str:
    """Карточка «Замеры по менеджерам» — N месяцев в ряд (прошлый/текущий/следующий),
    КВ/КИА/НПН + Всего. cols = [(month, stat_dict), ...]."""
    lines = _join_cols_n([_stats_rows(m, st) for m, st in cols], SW)
    body = "<pre>" + "\n".join(lines) + "</pre>"
    return f"<b>👔  Замеры по менеджерам</b>\n{body}"


async def _zamery_user_ids(db, for_user: int | None) -> list[int]:
    """telegram_id замерщиков: [for_user] либо все роли zamery (агрегат ГД/РП)."""
    if for_user is not None:
        return [for_user]
    try:
        zs = await db.find_users_by_role("zamery", limit=50)
    except Exception:
        logger.exception("zamery: find_users_by_role failed")
        return []
    return [u.telegram_id for u in zs]


async def _collect(db, user_ids: list[int], year: int, month: int) -> dict:
    """За месяц по списку замерщиков: busy/off дни, всего замеров, разбивка по
    менеджеру-заказчику (requester_role). Read-only (только SELECT)."""
    ndays = calendar.monthrange(year, month)[1]
    d_from = date(year, month, 1).isoformat()
    d_to = date(year, month, ndays).isoformat()
    busy: set[int] = set()
    off: set[int] = set()
    full: set[int] = set()  # дни «занят» (kind='busy')
    total = 0
    by_mgr: dict[str, int] = {}
    for uid in user_ids:
        try:
            for z in await db.list_zamery_for_schedule(uid, d_from, d_to):
                ds = z.get("scheduled_date")
                if ds and len(ds) >= 10:
                    busy.add(int(ds[8:10]))
                total += 1
                role = str(z.get("requester_role") or "").split(",")[0].strip()
                by_mgr[role] = by_mgr.get(role, 0) + 1
        except Exception:
            logger.exception("zamery collect: schedule failed for %s", uid)
        try:
            for bdt in await db.list_zamery_blackout_dates(uid, d_from, d_to):
                ds = bdt.get("blackout_date")
                if ds and len(ds) >= 10:
                    if (bdt.get("kind") or "off") == "busy":
                        full.add(int(ds[8:10]))
                    else:
                        off.add(int(ds[8:10]))
        except Exception:
            logger.exception("zamery collect: blackout failed for %s", uid)
    return {"busy": busy, "off": off, "full": full, "total": total, "by_mgr": by_mgr}


async def build_zamery_window_calendar(db, for_user: int | None = None) -> str | None:
    """Блок «📅 График замеров» — окно [посл. 15 дней пред. + текущий + первые 15 след.].
    for_user=<id> — его данные (карта замерщика); None — агрегат всех замерщиков
    (карты ГД/РП). None если замерщиков нет."""
    uids = await _zamery_user_ids(db, for_user)
    if not uids:
        return None
    (y0, m0), (y1, m1), (y2, m2) = _prev_cur_next_months()
    c0 = await _collect(db, uids, y0, m0)
    c1 = await _collect(db, uids, y1, m1)
    c2 = await _collect(db, uids, y2, m2)
    return render_window_calendar(y0, m0, c0, y1, m1, c1, y2, m2, c2)


async def build_zamery_manager_stats_card(db, for_user: int | None = None) -> str | None:
    """Отдельное сообщение «👔 Замеры по менеджерам» — прошлый/текущий/следующий месяц,
    КВ/КИА/НПН + Всего. for_user=<id> — его замеры; None — агрегат всех. None если нет."""
    uids = await _zamery_user_ids(db, for_user)
    if not uids:
        return None
    (y0, m0), (y1, m1), (y2, m2) = _prev_cur_next_months()
    a = await _collect(db, uids, y0, m0)
    b = await _collect(db, uids, y1, m1)
    c = await _collect(db, uids, y2, m2)
    return render_manager_stats([(m0, a), (m1, b), (m2, c)])


async def build_zamery_start_card_text(db, config, user_id: int) -> str:
    """Стартовая карточка замерщика (/start, /menu) — календарь на 2 месяца, его
    данные. Статистика по менеджерам уходит ОТДЕЛЬНЫМ сообщением (handler шлёт следом
    build_zamery_manager_stats_card)."""
    cal = await build_zamery_window_calendar(db, for_user=user_id)
    return cal or "📅  График замеров"


async def build_zamery_calendar_section(db) -> str | None:
    """Блок «📅 График замеров» (окно вокруг «сейчас», агрегат всех замерщиков) для карт
    ГД/РП. None если замерщиков нет. Статистика по менеджерам — отдельным сообщением."""
    return await build_zamery_window_calendar(db, for_user=None)
