"""Golden-снимок денежных витрин — ПЕРЕИСПОЛЬЗУЕМАЯ регрессионная сетка.

Зачем. Функтесты проекта одноразовы: каждый прибит к паре «задеплоенная ветка
против патченой», и после деплоя невоспроизводим. Этот снимок — наоборот:
он привязан не к правке, а к КОДУ ЦЕЛИКОМ. Прогнал до правки, прогнал после,
сравнил побайтово. Расхождения обязаны быть ровно там, где правка задумана.

Как запускать (всё внутри прода, боевого ничего не трогает) — см. run_golden.sh.

Три вещи, на которых такие стенды обычно врут, и как здесь с ними:

  1. ЗАМОРОЖЕННЫЙ вход. БД — не боевая, а снимок /golden/golden.sqlite3, снятый
     через sqlite3.backup (атомарно, WAL внутри). Живая БД меняется каждую минуту,
     и по ней нельзя отличить «код изменился» от «данные изменились».
     Снимок открывается КОПИЕЙ: Database.connect() делает PRAGMA journal_mode=WAL,
     то есть ПИШЕТ в файл — эталон перестал бы быть эталоном после первого прогона.

  2. НЕДЕТЕРМИНИЗМ ловится, а не предполагается. Каждая проба считается ДВАЖДЫ
     в одном процессе. Разошлась — уходит в unstable.json с пометкой и в эталон
     НЕ попадает. Молча отбрасывать нельзя: «покрыто всё» тогда становится ложью.
     Двойной прогон ловит random/id/порядок, но НЕ ловит зависимость от даты:
     она проявится только назавтра. Пробы, где найден datetime.now, регистрировать
     с time_sensitive=True — они уходят в отдельный раздел, а не в эталон.

  3. ВЕРСИЯ КОДА доказывается inspect.getsource, а не тем, что файл подложен:
     неперезапущенный процесс держит старый код в памяти.

В конце обязателен os._exit(0): aiosqlite держит не-daemon тред, иначе контейнер
залипает в состоянии Up.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import os
import re
import shutil
import sys
import traceback
from typing import Any, Callable

GOLDEN_DB = "/golden/golden.sqlite3"
WORK_DB = "/tmp/golden_run.sqlite3"
OUT_DIR = "/out"
BASELINE = os.path.join(OUT_DIR, "baseline.json")

PROBES: list[dict[str, Any]] = []
VERSION_TARGETS: list[tuple[str, Callable[..., Any]]] = []


def probe(name: str, *, time_sensitive: bool = False, note: str = ""):
    """Регистрация пробы. Функция — асинхронный генератор, отдающий (ключ, значение).

    Ключ обязан быть устойчивым к появлению новых счетов: «inv:66», а не «строка 7».
    """
    def deco(fn):
        PROBES.append({"name": name, "fn": fn, "time_sensitive": time_sensitive, "note": note})
        return fn
    return deco


_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{6,}")


def _default(o: Any) -> Any:
    """Разложить объекты aiogram по СОДЕРЖАНИЮ, а не по repr.

    🔴 Наступлено 06.09: карточка ГД «Счета на оплату» два прогона подряд давала
    разный результат — и это была не находка в боте, а дефект замерщика:
    в вывод уезжал repr клавиатуры вместе с АДРЕСОМ В ПАМЯТИ
    («InlineKeyboardBuilder object at 0x7515cd353350»). Замерщик, который врёт
    про нестабильность, хуже отсутствующего: он приучает не верить красному.
    Заодно клавиатуры попадают в снимок содержательно — тексты кнопок и
    callback_data, то есть ровно то, что ломается при правках меню.
    """
    # 🔴 Множества сортируем. Порядок обхода set зависит от seed хэшей строк и
    # МЕНЯЕТСЯ ОТ ПРОЦЕССА К ПРОЦЕССУ — двойной прогон внутри одного процесса
    # этого не видит, а сверка с эталоном назавтра покажет ложное расхождение.
    # Поймано 06.09 на fallback._submenu_button_texts (frozenset кнопок подменю);
    # в самом боте порядок там не важен — множество нужно только для «text in».
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    b = getattr(o, "as_markup", None)
    if callable(b):
        try:
            o = b()
        except Exception:
            pass
    rows = getattr(o, "inline_keyboard", None) or getattr(o, "keyboard", None)
    if rows is not None:
        out = []
        for row in rows:
            out.append([{k: v for k, v in (
                ("text", getattr(btn, "text", None)),
                ("cb", getattr(btn, "callback_data", None)),
                ("url", getattr(btn, "url", None)),
            ) if v is not None} for btn in row])
        return {"клавиатура": out}
    dump = getattr(o, "model_dump", None)
    if callable(dump):
        try:
            return dump(exclude_none=True)
        except Exception:
            pass
    return _ADDR_RE.sub("0xАДРЕС", str(o))


def render(value: Any) -> str:
    if isinstance(value, str):
        return value
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=_default)
    # Страховка: адрес объекта не несёт смысла и не имеет права попасть в хэш.
    return _ADDR_RE.sub("0xАДРЕС", text)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


async def build_ctx() -> dict[str, Any]:
    for suf in ("", "-wal", "-shm"):
        if os.path.exists(GOLDEN_DB + suf):
            shutil.copy2(GOLDEN_DB + suf, WORK_DB + suf)

    from app.db import Database

    db = Database(WORK_DB)
    await db.connect()
    # init_schema НЕ зовём: он мигрирует файл, а снимок обязан остаться неизменным.

    ids = [r[0] for r in await (await db.conn.execute("SELECT id FROM invoices ORDER BY id")).fetchall()]
    # ⚠️ У users ключ называется telegram_id, колонки id там НЕТ ВООБЩЕ
    # ([[feedback_users_telegram_id_not_tg_user_id]]). Проверено PRAGMA table_info.
    uids = [r[0] for r in await (await db.conn.execute("SELECT telegram_id FROM users ORDER BY telegram_id")).fetchall()]
    tids = [r[0] for r in await (await db.conn.execute("SELECT id FROM tasks ORDER BY id")).fetchall()]

    invoices = {i: await db.get_invoice(i) for i in ids}
    costs = {i: await db.get_full_invoice_cost_card(i) for i in ids}

    pfs: dict[int, Any] = {}
    for i in ids:
        try:
            pfs[i] = await db.get_plan_fact_card(i)
        except Exception:
            pfs[i] = None

    tasks: dict[int, Any] = {}
    for t in tids:
        try:
            tasks[t] = await db.get_task(t)
        except Exception:
            tasks[t] = None
    tids = [t for t in tids if tasks.get(t) is not None]

    return {
        "db": db,
        "ids": ids,
        "user_ids": uids,
        "task_ids": tids,
        "invoices": invoices,
        "costs": costs,
        "pfs": pfs,
        "tasks": tasks,
    }


async def collect(ctx: dict[str, Any], only: str | None) -> tuple[dict[str, str], dict[str, str]]:
    """Возвращает (значения, ошибки).

    Падение ОДНОЙ пробы не роняет прогон: иначе одна опечатка обнуляет всю сетку.
    """
    values: dict[str, str] = {}
    errors: dict[str, str] = {}
    for p in PROBES:
        if only and only not in p["name"]:
            continue
        try:
            async for key, val in p["fn"](ctx):
                values[p["name"] + " :: " + str(key)] = render(val)
        except Exception:
            errors[p["name"]] = traceback.format_exc(limit=3)
    return values, errors


def version_markers() -> dict[str, str]:
    out = {}
    for label, fn in VERSION_TARGETS:
        try:
            out[label] = sha(inspect.getsource(fn))
        except Exception as e:
            out[label] = "НЕ ПРОЧИТАН: " + type(e).__name__
    return out


async def main() -> None:
    """Обёртка, которая ГАРАНТИРУЕТ выход из процесса на любом пути.

    🔴 Наступлено 06.09: при исключении внутри прогона трейсбек печатается,
    asyncio.run завершается — а контейнер остаётся Up НАВСЕГДА, потому что
    aiosqlite держит не-daemon тред. Мёртвый контейнер провисел час и был
    замечен только по docker ps. Поэтому os._exit обязан стоять в finally,
    а не в конце успешной ветки.
    """
    rc = 3
    try:
        rc = await _run()
    except Exception:
        traceback.print_exc()
        print("ПРОГОН УПАЛ — эталон не тронут")
        rc = 3
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(rc)


async def _run() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", action="store_true", help="записать эталон вместо сверки")
    ap.add_argument("--only", default=None, help="прогнать только пробы, чьё имя содержит подстроку")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    ctx = await build_ctx()

    first, err1 = await collect(ctx, args.only)
    second, _ = await collect(ctx, args.only)

    stable: dict[str, str] = {}
    unstable: dict[str, str] = {}
    for k, v in first.items():
        if second.get(k) == v:
            stable[k] = v
        else:
            unstable[k] = "значение разошлось между двумя прогонами В ОДНОМ ПРОЦЕССЕ"
            # Сохраняем ОБА варианта: без них «нестабильно» — приговор без улик,
            # и причину приходится угадывать.
            d = os.path.join(OUT_DIR, "unstable_detail")
            os.makedirs(d, exist_ok=True)
            safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in k)[:120]
            with open(os.path.join(d, safe + ".diff.txt"), "w", encoding="utf-8") as f:
                import difflib
                f.write("\n".join(difflib.unified_diff(
                    v.splitlines(), (second.get(k) or "").splitlines(),
                    "прогон1", "прогон2", lineterm="", n=2)))
    for name in {p["name"] for p in PROBES if p["time_sensitive"]}:
        for k in list(stable):
            if k.startswith(name + " ::"):
                unstable[k] = "проба зависит от текущей даты (time_sensitive) — в эталон не берём"
                stable.pop(k)

    hashes = {k: sha(v) for k, v in stable.items()}
    payload = {
        "markers": version_markers(),
        "counts": {
            "проб": len(PROBES),
            "значений": len(first),
            "стабильных": len(stable),
            "нестабильных": len(unstable),
            "проб_упало": len(err1),
        },
        "hashes": hashes,
    }

    with open(os.path.join(OUT_DIR, "dump.txt"), "w", encoding="utf-8") as f:
        for k in sorted(stable):
            f.write("=" * 100 + "\n" + k + "\n" + "=" * 100 + "\n" + stable[k] + "\n\n")
    with open(os.path.join(OUT_DIR, "unstable.json"), "w", encoding="utf-8") as f:
        json.dump(unstable, f, ensure_ascii=False, indent=2, sort_keys=True)
    with open(os.path.join(OUT_DIR, "errors.json"), "w", encoding="utf-8") as f:
        json.dump(err1, f, ensure_ascii=False, indent=2, sort_keys=True)
    # ⚠️ Что НЕ покрыто — записываем явно. Молчаливое сужение охвата превращает
    # зелёный вердикт в ложь: «сетка чиста» читается как «проверено всё».
    with open(os.path.join(OUT_DIR, "skipped.json"), "w", encoding="utf-8") as f:
        json.dump(SKIPPED, f, ensure_ascii=False, indent=2, sort_keys=True)

    print("проб зарегистрировано:", len(PROBES), "| функций не покрыто:", len(SKIPPED))
    print("значений собрано:", len(first), "| стабильных:", len(stable), "| нестабильных:", len(unstable))
    if err1:
        print("ПРОБЫ УПАЛИ:", ", ".join(err1))
    for k, why in sorted(unstable.items())[:10]:
        print("   нестабильно:", k, "—", why)
    if len(unstable) > 10:
        print("   ... ещё", len(unstable) - 10, "(полностью в unstable.json)")

    rc = 0
    if args.baseline:
        with open(BASELINE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        print("ЭТАЛОН ЗАПИСАН:", BASELINE)
    else:
        if not os.path.exists(BASELINE):
            print("ЭТАЛОНА НЕТ — сначала прогон с --baseline")
            rc = 2
        else:
            with open(BASELINE, encoding="utf-8") as f:
                base = json.load(f)
            bh = base.get("hashes", {})
            changed = sorted(k for k in hashes if k in bh and hashes[k] != bh[k])
            gone = sorted(k for k in bh if k not in hashes)
            new = sorted(k for k in hashes if k not in bh)
            print()
            print("=== СВЕРКА С ЭТАЛОНОМ ===")
            print("изменилось:", len(changed), "| пропало:", len(gone), "| появилось:", len(new))
            for k in changed[:40]:
                print("   ИЗМЕНИЛОСЬ:", k)
            if len(changed) > 40:
                print("   ... ещё", len(changed) - 40)
            for k in gone[:20]:
                print("   ПРОПАЛО:", k)
            for label, m in payload["markers"].items():
                if base.get("markers", {}).get(label) != m:
                    print("   версия кода изменилась:", label, base.get("markers", {}).get(label), "->", m)
            if changed or gone:
                print("РАСХОЖДЕНИЯ ЕСТЬ. Проверить, что они ровно там, где правка задумана.")
                rc = 1
            else:
                print("расхождений нет — правка ничего постороннего не задела")

    return rc


# ===========================================================================
# РЕЕСТР ПРОБ. Ниже — то, что снимок покрывает. Добавлять сюда.
# ===========================================================================

@probe("db.get_full_invoice_cost_card", note="ядро себестоимости и прибыли, все счета")
async def _p_cost_card(ctx):
    for i in ctx["ids"]:
        yield "inv:" + str(ctx["invoices"][i].get("invoice_number") or i), ctx["costs"][i]


@probe("db.get_plan_fact_card", note="план-факт, все счета")
async def _p_plan_fact(ctx):
    for i in ctx["ids"]:
        if ctx["pfs"].get(i) is not None:
            yield "inv:" + str(ctx["invoices"][i].get("invoice_number") or i), ctx["pfs"][i]


# ===========================================================================
# АВТО-ДРАЙВЕР. 230 чистых функций показа и расчёта, перечисленных в
# allowlist.json (снят разведкой 06.09 по 17 модулям). Писать 230 проб руками
# бессмысленно: аргументы у них однотипные. Вместо этого — резолвер по ИМЕНИ
# параметра, а всё, что резолвер не осилил, попадает в skipped.json с причиной.
#
# 🔑 Почему это безопасно, хотя вызывает функции пачкой: прогон идёт на КОПИИ
# замороженного снимка, а не на боевой БД. Даже если в allow-list по ошибке
# просочится пишущая функция, она испортит одноразовый файл в /tmp — и тут же
# выдаст себя расхождением между двумя прогонами.
#
# ⛔ Пополнять allow-list только функциями, у которых разведка подтвердила
# side_effects=нет. Список — данные, а не код: пересняли инвентарь, положили json.
# ===========================================================================

ALLOWLIST_PATH = "/tools/allowlist.json"
SKIPPED: dict[str, str] = {}

# Константы подобраны каноническими, а не случайными: ширина 34 — та же, что у
# объединённого блока ГД; пояс — дефолт config.py (TIMEZONE).
CONST_ARGS: dict[str, Any] = {
    "width": 34,
    "tz_name": "Europe/Moscow",
    "timezone_name": "Europe/Moscow",
    "role": "gd",
    "viewer_role": None,
    "compact": False,
    "text": "Проба снимка",
    "title": "Проба",
    "emoji": "📋",
    "name": "Проба",
    "label": "Проба",
    "prefix": "проба",
    "amount": 123456.78,
    "value": 123456.78,
    "v": 123456.78,
    "total": None,
    "items": [("Строка", "1")],
    "iso_s": "2026-09-01T12:00:00+00:00",
    "what": "проба",
    # Вторая волна провайдеров (06.09): имена взяты из фактического списка
    # непокрытых по частоте, а не выдуманы. Значения фиксированные и нейтральные.
    # ⚠️ Если форма аргумента не угадана, функция бросит исключение и уйдёт
    # в skipped как «все вызовы дали исключение» — фиктивного покрытия не будет.
    "year": 2026,
    "month": 8,
    "month_str": "2026-08",
    "addr": "г. Москва, ул. Тестовая, д. 1",
    "initiator": "Проба",
    "channel": "kv",
    "mode": "bn",
    "status": "open",
    "payment_type": "cash",
    "s": "проба",
    "w": 34,
    "n": 3,
    "z": 0.0,
    "c0": 0.0,
    "c1": 0.0,
    "base": 0.0,
    "requested": 0.0,
    "has_receipt": False,
    "expanded": False,
    "selected": [],
    "rows": [],
    "cart": [],
    "conditions": [],
    "payload": {},
    "req": {},
}

ENTITY = {
    "inv": "invoice", "invoice": "invoice", "invoice_row": "invoice",
    "cost": "cost", "cost_card": "cost", "card": "cost",
    "pf": "pf", "plan_fact": "pf",
    "invoice_id": "invoice_id",
    "task": "task", "task_id": "task_id",
    "tasks": "tasks",
    "remaining": "remaining",
    "user_id": "user", "telegram_id": "user", "actor_id": "user", "installer_id": "user",
    "rp_id": "user",
    "db": "db",
    "invoices": "invoices",
}


def _db_changes(ctx: dict[str, Any]) -> int:
    """Сколько строк изменено с момента открытия соединения. Считает сам sqlite."""
    c = ctx["db"].conn
    for attr in ("total_changes",):
        if hasattr(c, attr):
            return int(getattr(c, attr))
    inner = getattr(c, "_conn", None)
    return int(getattr(inner, "total_changes", 0)) if inner is not None else 0


def _has_provider(pname: str) -> bool:
    """Проверка ПО ИМЕНИ, без обращения к данным.

    🔴 Наступлено 06.09: предпроверка вызывала настоящий резолвер и подставляла
    id счёта в аргумент «task» — KeyError валил весь авто-драйвер целиком.
    Проверять наличие провайдера и доставать значение — разные операции.
    """
    return pname in ENTITY or pname in CONST_ARGS


def _resolve(pname: str, ctx: dict[str, Any], item: Any) -> Any:
    slot = ENTITY.get(pname)
    if slot == "invoice":
        return ctx["invoices"][item]
    if slot == "invoice_id":
        return item
    if slot == "cost":
        return ctx["costs"][item]
    if slot == "pf":
        return ctx["pfs"].get(item)
    if slot == "task":
        import copy
        # ⚠️ deepcopy обязателен: enrich_task_invoice_label переписывает
        # task['payload_json'] НА МЕСТЕ, и второй прогон пошёл бы другой веткой.
        return copy.deepcopy(ctx["tasks"][item])
    if slot == "task_id":
        return item
    if slot == "tasks":
        return [ctx["tasks"][t] for t in ctx["task_ids"][:30]]
    if slot == "remaining":
        from app.utils import _compute_remaining_to_buy
        return _compute_remaining_to_buy(ctx["invoices"][item])
    if slot == "user":
        return item
    if slot == "db":
        return ctx["db"]
    if slot == "invoices":
        return [ctx["invoices"][i] for i in ctx["ids"]]
    return CONST_ARGS[pname]


def _axis(names: list[str]) -> str | None:
    slots = {ENTITY.get(n) for n in names}
    if ("invoice" in slots or "cost" in slots or "pf" in slots
            or "invoice_id" in slots or "remaining" in slots):
        return "inv"
    if "task" in slots or "task_id" in slots:
        return "task"
    if "user" in slots:
        return "user"
    return None


@probe("sheets._invoice_cells", note="строка листа Invoices, ~130 колонок на счёт")
async def _p_invoice_cells(ctx):
    """Самая ценная цель сетки: одна строка листа — вся денежная витрина счёта.

    🔴 cost-card подаётся НАСТОЯЩАЯ ([[feedback_ab_sheet_probe_needs_cost_card]]):
    колонки BM/BL/Y/BN/BO живут внутри «if _c:», и с cost=None они пусты у ВСЕХ
    счетов в ОБЕИХ ветках сравнения — замерщик тогда показывает «изменений нет»
    там, где изменилось всё. Ровно эта ловушка дала ложный вывод 13.08.

    ⚠️ GoogleSheetsService конструируется БЕЗ сети: __init__ только раскладывает
    поля, клиент gspread ленивый (_gc=None). Ключей и secrets не требуется, поэтому
    прогон честно идёт с --network none.
    ⚠️ row=10 и current_* пустыми — фиксированные канонические значения: строка
    влияет только на номер в формулах, а current_* читаются с ЖИВОГО листа, то есть
    в замороженный снимок попасть не могут by design.
    ⚠️ advance=None — ветка с авансом снимком НЕ покрыта, это записано честно
    (см. skipped.json и отчёт), а не замолчано.
    """
    from app.integrations.sheets import GoogleSheetsService, SheetsConfig

    svc = GoogleSheetsService(SheetsConfig(
        enabled=False, spreadsheet_id="ПРОБА", projects_tab="Projects", tasks_tab="Tasks",
    ))
    for i in ctx["ids"]:
        inv = ctx["invoices"][i]
        num = str(inv.get("invoice_number") or i)
        label = str(inv.get("manager") or "")
        for is_new in (False, True):
            try:
                cells = svc._invoice_cells(inv, label, ctx["costs"][i], row=10, is_new=is_new)
            except Exception as e:
                cells = "ИСКЛЮЧЕНИЕ: " + type(e).__name__ + ": " + str(e)[:200]
            yield num + " | is_new=" + str(is_new), cells


@probe("auto", note="авто-драйвер по allowlist.json")
async def _p_auto(ctx):
    import importlib

    try:
        with open(ALLOWLIST_PATH, encoding="utf-8") as f:
            entries = json.load(f)
    except Exception as e:
        SKIPPED["allowlist.json"] = "не прочитан: " + repr(e)
        return

    mods: dict[str, Any] = {}
    for ent in entries:
        modname, fname = ent["mod"], ent["name"]
        tag = modname + "." + fname
        if modname not in mods:
            try:
                mods[modname] = importlib.import_module(modname)
            except Exception as e:
                mods[modname] = None
                SKIPPED["модуль " + modname] = "не импортируется: " + repr(e)[:160]
        mod = mods[modname]
        if mod is None:
            SKIPPED[tag] = "модуль не импортировался"
            continue
        fn = getattr(mod, fname, None)
        if fn is None:
            SKIPPED[tag] = "функции нет в модуле (переименована или удалена)"
            continue
        try:
            sig = inspect.signature(fn)
        except Exception as e:
            SKIPPED[tag] = "сигнатура не читается: " + repr(e)[:120]
            continue

        req = [p.name for p in sig.parameters.values()
               if p.default is inspect.Parameter.empty
               and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)]
        bad = [n for n in req if not _has_provider(n)]
        if bad:
            SKIPPED[tag] = "нет провайдера для аргументов: " + ", ".join(sorted(set(bad)))
            continue

        axis = _axis(req)
        if axis == "inv":
            items = ctx["ids"]
        elif axis == "task":
            items = ctx["task_ids"]
        elif axis == "user":
            items = ctx["user_ids"]
        else:
            items = [None]

        if tag not in {t for t, _ in VERSION_TARGETS}:
            VERSION_TARGETS.append((tag, fn))

        # 🔴 Проверка «функция ничего не пишет» — НЕ на доверии к классификации,
        # а фактом: sqlite считает изменённые строки сам (total_changes).
        # Наступлено 06.09: карточка ГД «Счета на оплату» разошлась между двумя
        # прогонами — не потому, что недетерминирована, а потому, что СОСЕДНЯЯ
        # функция из allow-list успела поменять данные под ней.
        buf: list[tuple[str, Any]] = []
        before = _db_changes(ctx)
        for it in items:
            try:
                kwargs = {n: _resolve(n, ctx, it) for n in req}
            except Exception as e:
                SKIPPED[tag] = "аргументы не собрались: " + repr(e)[:120]
                break
            key = tag if it is None else tag + " | " + (
                str(ctx["invoices"][it].get("invoice_number") or it) if axis == "inv" else str(it))
            try:
                res = fn(**kwargs)
                if inspect.isawaitable(res):
                    res = await res
            except Exception as e:
                res = "ИСКЛЮЧЕНИЕ: " + type(e).__name__ + ": " + str(e)[:200]
            buf.append((key, res))

        # Если ВСЕ вызовы дали исключение — провайдер подобрал аргумент не той формы.
        # Хэш строки «ИСКЛЮЧЕНИЕ: ...» стабилен, и такая проба молча выдавала бы себя
        # за покрытие, ничего не проверяя. Честнее записать в непокрытые.
        if buf and all(isinstance(r, str) and r.startswith("ИСКЛЮЧЕНИЕ:") for _k, r in buf):
            SKIPPED[tag] = "все вызовы дали исключение (" + buf[0][1][:80] + ") — аргумент не той формы"
            continue

        wrote = _db_changes(ctx) - before
        if wrote:
            SKIPPED[tag] = ("ИСКЛЮЧЕНА: изменила " + str(wrote) + " строк(и) БД — "
                            "это не витрина, а писатель; для неё нужен A/B-функтест")
            continue
        for key, res in buf:
            yield key, res


def _register_version_targets() -> None:
    from app.db import Database

    VERSION_TARGETS.append(("db.get_full_invoice_cost_card", Database.get_full_invoice_cost_card))


if __name__ == "__main__":
    _register_version_targets()
    asyncio.run(main())
