"""РП стартовая (welcome) карточка в стиле ГД.

Подключается из handlers/common.py (ветка cmd_start, role=RP).
Логика портирована из проверенного харнесса rp_card_render_v8 (отрабатывал read-only
на проде). utils НЕ трогаем — только импортируем готовые билдеры.

Блоки: Счета в работе · Лиды · Финансы·долги · В работе (матрица) · 10% прибыли РП.
Каждый блок в try/except: падение одного блока не роняет всю карточку.
"""
import asyncio
import logging
import re
import time
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from .utils import format_card, format_card_section, format_leads_section, _build_gd_tasks_section

logger = logging.getLogger(__name__)

IGOR = 1072734744          # 1-я монтажная группа (Игорь Быканов)
INDENT = "   "
OKLAD_MONTHLY = 66_000     # оклад РП +10% = «Сумма б/н» (C) строки «ЗП РП Нижельченко» БК
# Вторая форма выплаты (owner 12.08): наличными — тело без надбавки на налог, ложится в
# «Сумма» (I) / «Расходы кред» (J). Синхронно db/td RP_SALARY_MONTHLY_CASH.
OKLAD_MONTHLY_CASH = 60_000

MONTHS_NOM = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
              "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]


def _month_now():
    return MONTHS_NOM[datetime.now(ZoneInfo("Europe/Moscow")).month - 1]

CATS = [
    ("metal",   "🔩", "cost_metal",      "Металл"),
    ("glass",   "🔷", "cost_glass",      "Стекло"),
    ("loaders", "💪", "cost_loaders",    "Грузчики"),
    ("logist",  "🚚", "cost_logistics",  "Логистика"),
    ("emat",    "🧱", "cost_extra_mat",  "Доп.мат"),
    ("esvc",    "🧾", "cost_extra_svc",  "Доп.усл"),
]
MONT_ICON = "👷"
ZP_MONT_ICON = "💰"        # статус выплаты ЗП монтаж (owner 25.07)

# «г. Москва» / «г Москва» / «г.Москва» — город в начале адреса.
_CITY_RE = re.compile(r'^\s*г\.?\s*([А-ЯЁ][А-Яа-яЁё-]*)')


def vw(s):
    """Визуальная ширина строки в моноширинном Telegram (эмодзи ≈ 2 колонки)."""
    w = 0
    for ch in s:
        o = ord(ch)
        if o == 0xFE0F:          # variation selector — нулевая ширина
            continue
        if o == 0x20E3:          # combining keycap → '1️⃣' получает ширину 2
            w += 1
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


def _money(v):
    try:
        return f"{int(round(float(v or 0))):,}".replace(",", " ")
    except Exception:
        return "0"


def _mt_to_cat(mt):
    s = (mt or "").strip().lower()
    if any(k in s for k in ("профил", "метал", "metal", "оптим")):
        return "metal"
    if any(k in s for k in ("стекл", "glass")):
        return "glass"
    if any(k in s for k in ("грузчик", "loader", "разгруз")):
        return "loaders"
    if any(k in s for k in ("логист", "logist", "доставк", "перевоз")):
        return "logist"
    if any(k in s for k in ("услуг", "svc")):
        return "esvc"
    return "emat"


def _street(addr, width):
    a = (addr or "").strip()
    a = re.sub(r'^г\.?\s*[А-Яа-яЁё-]+\s*,?\s*', '', a)
    for w in ("ул.", "улица", "наб.", "набережная", "бул.", "бульвар",
              "проспект", "пр-кт", "шоссе", "пер.", "переулок"):
        a = a.replace(w, " ")
    # Отрезаем всё после первой запятой, затем «номерной хвост» дома
    # (13-42 / 89с2 / д.91 / стр.3) = первый ПОСЛЕДУЮЩИЙ токен с цифрой.
    # Было `re.split(r'[,\d]', a)[0]` — резало по ЛЮБОЙ первой цифре, поэтому
    # улицы с цифрой в НАЧАЛЕ («2-я Звенигородская 13-42») схлопывались в «?»
    # (баг 14.07; в карточке замеров лечился локальным _zam_street в utils.py).
    # Первый токен НЕ трогаем — он и несёт «2-я»/«1-я».
    # Дельта на 47 прод-адресах (15.07): 44 без изменений, 2 починено, 0 регрессов.
    a = a.split(",")[0]
    _keep: list[str] = []
    for _i, _tok in enumerate(a.split()):
        if _i and any(_ch.isdigit() for _ch in _tok):
            break
        _keep.append(_tok)
    a = " ".join(_keep).strip(' .')
    a = re.sub(r'\s+', ' ', a)
    return (a[:width]).strip() or "?"


def _addr_cell(addr, width):
    """Первая колонка карточки «Этапы работы» (owner 25.07).

    Москва → улица сокращённо, как было (_street). НЕ Москва → название ГОРОДА:
    по иногородним объектам важнее видеть город, чем улицу. Адрес без города
    («Красногорский бул. 4») — прежнее поведение, улицей.
    ⚠️ Отдельная функция, а НЕ правка _street: _street общий с другими карточками
    (keyboards.py, utils.py) — их вид не трогаем.
    """
    a = (addr or "").strip()
    m = _CITY_RE.match(a)
    city = m.group(1) if m else ""
    if city and city.lower() != "москва":
        return (city[:width]).strip() or "?"
    return _street(a, width)


def _mont(assigned, group=None, empty="⬛"):
    if group == 2:
        return "2️⃣"          # Наёмники (монтажная группа 2, без tg_id)
    a = int(assigned or 0)
    if a == IGOR:
        return "1️⃣"
    if a > 0:
        return "2️⃣"
    return empty


async def _load(db):
    cur = await db.conn.execute(
        "SELECT id, object_address, assigned_to, edo_task_id, status, "
        "  COALESCE(zp_installer_status,'') zp_installer_status, "
        "  COALESCE(cost_metal,0) cost_metal, COALESCE(cost_glass,0) cost_glass, "
        "  COALESCE(cost_loaders,0) cost_loaders, COALESCE(cost_logistics,0) cost_logistics, "
        "  COALESCE(cost_extra_mat,0) cost_extra_mat, COALESCE(cost_extra_svc,0) cost_extra_svc "
        "FROM invoices WHERE parent_invoice_id IS NULL "
        "  AND status IN ('in_progress','credit') ORDER BY id DESC")
    inv_rows = [dict(r) for r in await cur.fetchall()]
    cur = await db.conn.execute(
        "SELECT json_extract(payload_json,'$.parent_invoice_id') AS inv, "
        "       json_extract(payload_json,'$.material_type') AS mt "
        "FROM tasks WHERE type='invoice_payment' AND status IN ('open','in_progress')")
    ordered = {}
    for r in await cur.fetchall():
        inv = r["inv"]
        if inv is None:
            continue
        ordered.setdefault(int(inv), set()).add(_mt_to_cat(r["mt"]))
    # Открытые задачи «ЗП монтаж» — для жёлтого статуса в столбце 💰 (owner 25.07).
    cur = await db.conn.execute(
        "SELECT json_extract(payload_json,'$.invoice_id') AS inv "
        "FROM tasks WHERE type='zp_installer' AND status IN ('open','in_progress')")
    zp_open = {int(r["inv"]) for r in await cur.fetchall() if r["inv"] is not None}
    return inv_rows, ordered, zp_open


def _cell(inv, key, field, ords, empty="⬛"):
    if float(inv.get(field) or 0) > 0:
        return "✅"
    if key in ords:
        return "🟡"
    return empty


def _zp_mont(zp_status, has_open_task, empty="⬛"):
    """Столбец 💰 «ЗП монтаж» — принцип тот же, что у столбцов затрат (owner 25.07):
    ✅ выплачено · 🟡 в процессе (запрошена/одобрена либо висит задача) · ⬛ ничего."""
    s = (zp_status or "").strip()
    if s in ("payment_sent", "confirmed"):
        return "✅"
    if s in ("requested", "approved") or has_open_task:
        return "🟡"
    return empty


def _data_rows(inv_rows, ordered, zp_open, col_sep, name_w, empty="⬛"):
    """Строки данных. Зазор имя→1й столбец = col_sep (как между колонками).
    Каждая ячейка = ровно 2 виз.колонки → шапка и ряды совпадают идеально."""
    sep = " " * col_sep
    lines = []
    for inv in inv_rows:
        st = _addr_cell(inv.get("object_address"), name_w)
        ords = ordered.get(int(inv["id"]), set())
        cells = (
            [_cell(inv, k, f, ords, empty) for k, _e, f, _l in CATS]
            + [_mont(inv.get("assigned_to"), inv.get("edo_task_id"), empty=empty)]
            + [_zp_mont(inv.get("zp_installer_status"),
                        int(inv["id"]) in zp_open, empty=empty)]
        )
        lines.append(INDENT + f"{st:<{name_w}}" + sep + sep.join(cells))
    return lines


def _legend():
    # Компактная легенда: 1 строка (меньше высота) + курсив (визуально мельче).
    # Telegram не умеет менять размер шрифта — курсив максимально близок к «мельче».
    # User 31.05 вечер — легенда в одну строку. Описание ТОЛЬКО для столбцов;
    # статусы ячеек ✅/🟡/⬛ без расшифровки.
    return [
        f"{INDENT}<i>🔩Металл 🔷Стекло 💪Грузч. 🚚Лог. "
        f"🧱Доп.мат 🧾Доп.усл 👷1️⃣Игорь 2️⃣наёмники 💰ЗП монтаж</i>",
    ]


async def _matrix(db, col_sep=1, name_w=7, show_legend=True, empty="⬛"):
    """Матрица «В работе»: ОДИН <pre>-блок (канон, как «Лиды»).

    Б/Н (in_progress) и Кред. (credit) — подгруппы строкой-маркером ◎ ВНУТРИ
    одного <pre> (Telegram не рисует <b> внутри <pre>, поэтому иерархия — маркером,
    а не жирным). Шапка эмодзи-столбцов — один раз сверху; все ряды выровнены под неё.
    Ширина поджата под телефон РП (col_sep 3→1, name_w 8→7) — ряды не переносятся."""
    inv_rows, ordered, zp_open = await _load(db)
    bn = [r for r in inv_rows if (r.get("status") or "") == "in_progress"]
    kr = [r for r in inv_rows if (r.get("status") or "") == "credit"]

    title = f"<b>🔧  Этапы работы · {_month_now()}</b>"
    _leg = ("\n" + "\n".join(_legend())) if show_legend else ""
    if not inv_rows:
        return title + _leg

    sep = " " * col_sep
    header_emojis = [e for _, e, _, _ in CATS] + [MONT_ICON, ZP_MONT_ICON]
    head = INDENT + (" " * name_w) + sep + sep.join(header_emojis)

    body = [head]
    for label, rows in (("Б/Н", bn), ("Кред.", kr)):
        if not rows:
            continue
        body.append(f"{INDENT}◎ {label}")
        body.extend(_data_rows(rows, ordered, zp_open, col_sep, name_w, empty))

    sep_w = vw(head) - len(INDENT)
    body.append(INDENT + "━" * sep_w)
    body.append(f"{INDENT}Итого: {len(inv_rows)}")
    return title + "\n<pre>" + "\n".join(body) + "</pre>" + _leg


def _sub_bold(title, rows, width=32):
    body = []
    for label, value in rows:
        used = len(INDENT) + len(label) + len(value)
        body.append(f"{INDENT}{label}{' ' * max(1, width - used)}{value}")
    return f"<b>{INDENT}<u>{title}</u></b>\n<pre>" + "\n".join(body) + "</pre>"


async def _finance(db):
    cur = await db.conn.execute(
        "SELECT creator_role AS role, COALESCE(SUM(outstanding_debt),0) AS debt "
        "FROM invoices WHERE parent_invoice_id IS NULL AND COALESCE(status,'') != 'rejected' "
        "GROUP BY creator_role")
    by = {}
    for r in await cur.fetchall():
        by[(r["role"] or "")] = float(r["debt"] or 0)
    kv, kia, npn = by.get("manager_kv", 0), by.get("manager_kia", 0), by.get("manager_npn", 0)
    return format_card_section(
        emoji="💰", title=f"Финансы · долги · {_month_now()}",
        items=[("КВ", f"{_money(kv)}₽"), ("КИА", f"{_money(kia)}₽"), ("НПН", f"{_money(npn)}₽")],
        footer=("Итого", f"{_money(kv + kia + npn)}₽"))


async def _paid_invoices_this_month(db):
    """«Оплачено счетов» за текущий месяц: invoice_payment-задачи, закрытые ГД
    (status='done') в текущем месяце по МСК. РП мог не видеть платёжку — это
    напоминание сколько счетов уже оплачено за месяц. Read-only."""
    ym = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m")
    cur = await db.conn.execute(
        "SELECT COUNT(*) AS c FROM tasks "
        "WHERE type = 'invoice_payment' AND status = 'done' "
        "AND strftime('%Y-%m', COALESCE(updated_at, created_at)) = ?",
        (ym,))
    r = await cur.fetchone()
    return int((r["c"] if r else 0) or 0)


async def _block_inwork(db, cfg, uid):
    s = await db.get_daily_summary()
    in_progress = s["invoices_by_status"].get("in_progress", 0)
    x = await db.get_gd_inwork_extra_stats(uid)
    cv = 0
    for cr in ("manager_kv", "manager_kia", "manager_npn"):
        try:
            cs = await db.get_credit_balance_summary(cr)
        except Exception:
            continue
        op = [r for r in (cs.get("invoices") or []) if not r.get("is_closed")]
        cv += sum(1 for r in op if float(r.get("da") or 0) > 0)
    rp_tasks = await db.count_rp_role_tasks(uid)        # «Входящие задачи» (для РП)
    paid_month = await _paid_invoices_this_month(db)    # «Оплачено счетов» за месяц
    total = in_progress + cv

    W = 13
    # 2-колоночные строки (статус сейчас · накоплено за год); поджато под ширину телефона РП
    r2lines = [
        f"{INDENT}{'В работе':<{W}}{in_progress:>2} · {x['inv_year']:>2}/год",
        f"{INDENT}{'Кредит':<{W}}{cv:>2} · {x['credit_year']:>2}/год",
    ]
    width = max(len(l) for l in r2lines)

    def rfull(label, val):
        body_s = f"{INDENT}{label}"
        return body_s + " " * max(1, width - len(body_s) - len(str(val))) + str(val)

    body = [rfull("Входящие задачи", rp_tasks), rfull("Оплачено счетов", paid_month)]
    body += r2lines
    body.append(rfull("Итого", total))
    body.append(INDENT + "━" * (width - len(INDENT)))
    body.append(rfull("Входящие сообщения", x["unread_msgs"]))

    title = f"📋  Счета в работе · {_month_now()}"
    header = f"<b>{title}</b>"
    return header + "\n<pre>" + "\n".join(body) + "</pre>"


async def _block_leads(db, cfg):
    umap = getattr(cfg, "amocrm_user_map", {}) or {}
    ls = await db.get_lead_stats_v2(umap)
    tot = (ls or {}).get("totals") or {}
    lt, lm = int(tot.get("today") or 0), int(tot.get("month") or 0)
    # Менеджеры — эффективная атрибуция (RP-приоритет: rp_manager→суффикс КИА→карта), за месяц.
    mgr = {}
    for lbl, d in ((ls or {}).get("by_manager_eff") or {}).items():
        n = int((d or {}).get("month") or 0)
        if n <= 0:
            continue
        mgr[lbl] = mgr.get(lbl, 0) + n
    mgr_rows = [(l, str(n)) for l, n in sorted(mgr.items(), key=lambda kv: -kv[1])]
    # Источники — RP-приоритет (rp_source→amoCRM-категоризация), за месяц.
    src_rows = [(str(sx.get("src") or "—")[:16], str(int(sx.get("month") or 0)))
                for sx in ((ls or {}).get("by_source_eff") or []) if int(sx.get("month") or 0) > 0][:6]
    # Воронка РП — текущий срез сделок (rp_status), не за месяц.
    funnel_rows = [(str(f.get("status") or "—")[:16], str(int(f.get("count") or 0)))
                   for f in ((ls or {}).get("funnel_rp") or [])]
    inv_linked = int((ls or {}).get("rp_inv_linked") or 0)
    if inv_linked > 0:
        funnel_rows.append(("→ счёт", str(inv_linked)))
    return format_leads_section(emoji="⏰", title=f"Лиды · {_month_now()}",
                                today=lt, month=lm, mgr_rows=mgr_rows, src_rows=src_rows,
                                funnel_rows=funnel_rows, show_header_total=False)


def _rj(s, w):
    """Right-justify по ВИЗУАЛЬНОЙ ширине (vw) — эмодзи/₽ считаются верно."""
    s = str(s)
    return " " * max(0, w - vw(s)) + s


# Колонки блока: Мес · Оклад · 10% РП · К выплате (ширины под экран телефона РП).
_PROFIT_COLS = (3, 6, 7, 7)


def _profit_row(c0, c1, c2, c3):
    """Одна строка таблицы ЗП: 4 колонки, выравнивание по правому краю."""
    w0, w1, w2, w3 = _PROFIT_COLS
    return (INDENT + _rj(c0, w0) + " " + _rj(c1, w1)
            + " " + _rj(c2, w2) + " " + _rj(c3, w3))


async def _rp_to_pay_monthly(db, year):
    """«К выплате» помесячно: Invoices.AP (npn_amount) при AR (rp_payout_op)=0 И
    не забрано в аванс (rp_payout_advance_at пусто, гард 07.06).

    Невыплаченная ЗП РП (10%). Те же фильтры и группировка по receipt_date, что и
    «10% РП» (get_rp_dashboard_metrics), + условие невыплаты + исключение забранного
    в аванс (иначе налитая ЗП осталась бы в «к выплате»). Read-only трансляция —
    в заполнение колонок AP/AR не вмешиваемся.
    """
    cur = await db.conn.execute(
        """
        SELECT CAST(strftime('%m', receipt_date) AS INTEGER) AS m,
               COALESCE(SUM(npn_amount), 0) AS s
        FROM invoices
        WHERE parent_invoice_id IS NULL
          AND COALESCE(npn_amount, 0) > 0
          AND COALESCE(rp_payout_op, 0) = 0
          AND rp_payout_advance_at IS NULL
          AND COALESCE(status, '') != 'rejected'
          AND strftime('%Y', receipt_date) = ?
        GROUP BY m
        """,
        (str(year),),
    )
    out: dict[int, float] = {}
    for r in await cur.fetchall():
        if r["m"] is not None:
            out[int(r["m"])] = float(r["s"] or 0)
    return out


_OKLAD_CACHE = {"ts": 0.0, "months": {}}
_OKLAD_TTL = 600.0  # сек: «Баланс компании» меняется редко, кэшируем чтение Sheets


async def _oklad_paid_months(config) -> dict[int, float]:
    """{month_num: сумма C} для месяцев со строкой «ЗП РП Нижельченко» в Балансе компании.

    Источник — блок BH-BQ листа «Импорт ОП» (= лист «Баланс компании»): A=Месяц,
    C=«Сумма б/н» (amount_cashless, оклад +10%), E=«Расходы б/н» (description). В БД
    построчно НЕ зеркалится, поэтому читаем Google Sheets напрямую. Кэш 10 мин +
    graceful fallback: при ошибке/недоступности Sheets → stale/пусто, карточка цела.
    """
    if not getattr(config, "sheets_enabled", False):
        return {}
    now = time.time()
    if _OKLAD_CACHE["months"] and now - _OKLAD_CACHE["ts"] < _OKLAD_TTL:
        return _OKLAD_CACHE["months"]
    try:
        from .integrations.sheets import GoogleSheetsService, SheetsConfig
        svc = GoogleSheetsService(SheetsConfig(
            enabled=True, spreadsheet_id=config.gsheet_spreadsheet_id,
            projects_tab=config.gsheet_projects_tab, tasks_tab=config.gsheet_tasks_tab,
            invoices_tab=config.gsheet_invoices_tab, timezone_name=config.timezone,
            service_account_json=config.google_sa_json,
            service_account_file=config.google_sa_file,
            source_spreadsheet_id=config.gsheet_sales_spreadsheet_id,
            source_sheet_name=config.gsheet_sales_tab,
        ))
        rows = await asyncio.to_thread(svc.read_op_journal_rows_sync)
        months: dict[int, float] = {}
        for r in rows:
            if "Нижельченко" in (r.get("description") or "") and r.get("month_num"):
                months[int(r["month_num"])] = float(r.get("amount_cashless") or 0)
        _OKLAD_CACHE["ts"] = now
        _OKLAD_CACHE["months"] = months
        return months
    except Exception:
        logger.exception("RP card: чтение оклада из Баланса компании упало")
        return _OKLAD_CACHE.get("months") or {}


async def _oklad_bot_months(db, year: int) -> dict[int, float]:
    """{month_num: сумма оклада} за месяцы, где оклад РП провёл САМ БОТ.

    Зачем отдельно от _oklad_paid_months: тот читает журнал ОФИСА («Импорт ОП» BH-BQ),
    а строки бота туда не попадают ВООБЩЕ — op_company_entries сливается только в рендер
    листа «Баланс компании» (sheets.sync_balance_company_sheet), обратно в «Импорт ОП»
    не возвращается. Плюс офис подписывает строку «Нижельченко», а бот пишет
    rp_label = «Павел Инфоперегородки» — это ОДИН человек с двумя ролями (rp и manager_npn),
    owner 12.08. Из-за обоих расхождений колонка «Оклад» молчала и про безнал тоже.

    Матч по имени здесь не нужен: маркеры 'Оклад РП%' (выплата ГД / факт получения) и
    'ЗП РП%' (перевод оклада в кошелёк аванса) кроме оклада РП никому не принадлежат.
    Смотрим ОБЕ колонки подписи: б/н пишет description (E), наличные — description_credit (J).

    Сумму показываем ФИКСИРОВАННУЮ по форме выплаты (66 000 б/н, 60 000 наличными), а не
    фактическую строку: в БК лежит выплаченное за вычетом зачтённого аванса, и разнобой в
    колонке «Оклад» owner уже отклонял. Только чтение [[feedback_card_display_only]].
    """
    try:
        cur = await db.conn.execute(
            "SELECT month, "
            "  MAX(CASE WHEN description_credit LIKE 'Оклад РП%' "
            "            OR description_credit LIKE 'ЗП РП%' THEN 1 ELSE 0 END) AS is_cash "
            "FROM op_company_entries WHERE year = ? AND ("
            "     description LIKE 'Оклад РП%' OR description_credit LIKE 'Оклад РП%' "
            "  OR description LIKE 'ЗП РП%'    OR description_credit LIKE 'ЗП РП%') "
            "GROUP BY month",
            (int(year),),
        )
        rows = await cur.fetchall()
    except Exception:
        logger.exception("RP card: чтение оклада из op_company_entries упало")
        return {}
    return {
        int(r[0]): float(OKLAD_MONTHLY_CASH if int(r[1] or 0) else OKLAD_MONTHLY)
        for r in rows if r and r[0]
    }


async def _block_profit(db, config, uid):
    m = await db.get_rp_dashboard_metrics(uid)
    rpm = m.get("rp_monthly") or {}
    year = int(m.get("year") or 2026)
    mn = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    active = [mo for mo in range(1, 13) if float(rpm.get(mo) or 0) > 0]
    if not active:
        active = [datetime.now(ZoneInfo("Europe/Moscow")).month]
    active = sorted(active, reverse=True)         # текущий/новые сверху, Январь снизу
    to_pay = await _rp_to_pay_monthly(db, year)   # AP где AR=0, по receipt_date месяцу
    oklad_paid = await _oklad_paid_months(config)  # месяцы со строкой «ЗП РП Нижельченко»
    oklad_bot = await _oklad_bot_months(db, year)  # то же, но из выплат САМОГО бота
    # Шапка столбцов (Мес-колонка без заголовка).
    body = [_profit_row("", "Оклад", "10% РП", "К выпл.")]
    for mo in active:
        # Оклад: есть строка за месяц → фикс. сумма по форме выплаты; иначе пусто.
        # Два источника: журнал офиса (oklad_paid, всегда б/н 66 000) и выплаты самого
        # бота (oklad_bot, знает про наличные 60 000). Бот приоритетнее — он источник
        # истины по своим записям [[feedback_db_is_source_of_truth]].
        if mo in oklad_bot:
            okl_cell = _money(oklad_bot[mo])
        else:
            okl_cell = _money(OKLAD_MONTHLY) if mo in oklad_paid else ""
        pay = float(to_pay.get(mo) or 0)
        pay_cell = _money(pay) if pay > 0 else ""
        body.append(_profit_row(mn[mo - 1], okl_cell, _money(rpm.get(mo)), pay_cell))
    title = f"ЗП РП {year}"
    total = _money(m.get("rp_total_year"))
    tw = sum(_PROFIT_COLS) + 3
    body.append(INDENT + "━" * tw)
    body.append(INDENT + "Итого" + " " * max(1, tw - vw("Итого") - vw(total)) + total)
    header = f"<b>📈  {title}</b>"
    return header + "\n<pre>" + "\n".join(body) + "</pre>"


def _short_header(block: str) -> str:
    """Укорачивает хвост ━ в ЗАГОЛОВКЕ секции (часть до первого <pre>) до 1 штуки —
    чтобы заголовок не переносился на узком экране телефона. Данные и разделители
    внутри <pre> не трогаются. User 31.05 вечер — линия ровно 1 символ во всех блоках."""
    parts = block.split("<pre>", 1)
    if len(parts) == 2:
        return re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
    return block


async def build_rp_start_card_text(db, config, user_id):
    """Собирает полную РП-карточку. user_id = telegram_id (message.from_user.id)."""
    sections = []
    builders = (
        ("inwork",  lambda: _block_inwork(db, config, user_id)),
        ("leads",   lambda: _block_leads(db, config)),
        ("finance", lambda: _finance(db)),
        ("matrix",  lambda: _matrix(db)),
        ("profit",  lambda: _block_profit(db, config, user_id)),
    )
    for name, fn in builders:
        try:
            sections.append(_short_header(await fn()))
        except Exception:
            logger.exception("RP start card: блок %s упал", name)
    # «Задачи ролям» — та же полная история задач всех ролей, что в ГД-карточке
    # (owner 25.06: «как у гд»). LIMIT=9 как у ГД; РП-база компактнее, запас
    # под лимит Telegram 4096 большой (≈2836 UTF-16, +1260).
    try:
        _tasks = await _build_gd_tasks_section(db, user_id, limit=9)
        if _tasks:
            sections.append(_tasks)
    except Exception:
        logger.exception("RP start card: блок задач упал")
    # «📅 График замеров» — агрегат-календарь всех замерщиков внизу карты (owner 25.06).
    # Лимит задач РП не трогаем (9): база РП компактна, запас под 4096 большой.
    try:
        from .zamery_start_card import build_zamery_calendar_section
        _cal = await build_zamery_calendar_section(db)
        if _cal:
            sections.append(_cal)
    except Exception:
        logger.exception("RP start card: блок календаря замеров упал")
    return f"<b><i>Т в о я   А т м о с ф е р а</i></b>\n\n{format_card(sections)}"
