"""
New handlers for RP (Руководитель проектов) role.

Main menu (March 2026 layout):
- Проверка КП / Выставление Счета   (placeholder)
- Чат с ГД                          (placeholder)
- Счета в Работе                    (placeholder)
- Менеджер 1 (КВ) — chat-proxy
- Счета на оплату — мониторинг
- Менеджер 2 (КИА) — chat-proxy
- Бухгалтерия (УПД) — ЭДО запрос
- Монтажная гр. — submenu (Чат / В работу)
- Счет закрыт                       (placeholder)
- Лид на расчет (LeadToProjectSG)

Legacy (still handled for backward compat):
- Входящие Отд.Продаж
- Счета в Работу (мониторинг, legacy)
- Счет End (входящие условия, legacy)
- Проблема / Вопрос (legacy)

Other:
- Смена роли РП ↔ НПН
- Поиск Счета (в manager_new.py)
- Ответ на КП от менеджера (KpReviewSG)
"""
from __future__ import annotations

import json
import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, MontazhStage, Role, TaskStatus, TaskType
from ..integrations.minio_storage import MinioStorage
from ..keyboards import (
    RP_BTN_CHECK_KP,
    RP_BTN_CHAT_GD,
    RP_BTN_EDO,
    RP_BTN_INVOICE_CLOSED,
    RP_BTN_INVOICE_END,
    RP_BTN_INVOICE_START,
    RP_BTN_INVOICES_PAY,
    RP_BTN_INVOICES_WORK,
    RP_BTN_ISSUE,
    RP_BTN_LEAD,
    RP_BTN_SEARCH_INVOICE,
    RP_BTN_SYNC,
    RP_BTN_MGR_KIA,
    RP_BTN_MGR_KV,
    RP_BTN_MONTAZH,
    RP_BTN_ROLE_RP,
    RP_BTN_ROLE_RP_INACTIVE,
    RP_BTN_ROLE_NPN,
    RP_BTN_ROLE_NPN_ACTIVE,
    RP_MONTAZH_BTN_RAZMERY,
    RP_SUBBTN_MGR_KIA,
    RP_SUBBTN_MGR_KV,
    RP_SUBBTN_MONTAZH,
    edo_type_kb,
    invoice_list_kb,
    invoices_work_list_kb,
    kp_issued_list_kb,
    kp_payment_type_kb,
    kp_response_kb,
    kp_task_list_kb,
    lead_pick_manager_kb,
    main_menu,
    rp_chat_gd_submenu,
    rp_chat_submenu,
    rp_montazh_submenu,
    tasks_kb,
)
from ..services.assignment import apply_user_roles, resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    EdoRequestSG,
    KpReviewSG,
    LeadToProjectSG,
    ManagerChatProxySG,
    RpInvCancelSG,
    RpMontazhAssignSG,
    RpMontazhNaemSG,
    RpMontazhRegroupSG,
    RpMontazhZpSG,
    RpRazmerySG,
    RpSupplierInvoiceSG,
)
from ..utils import answer_service, build_invoice_section, close_condition_core_rows, fmt_money, format_invoice_card_standard, get_initiator_label, invoice_status_emoji, invoice_status_label, parse_roles, private_only_reply_markup, refresh_recipient_keyboard, try_json_loads
from ..rp_start_card import _matrix, _street
from ._mirror import collect_attachment
from .installer_new import (
    _advance_cg_amount,
    _advance_raw_cur,
    _gd_zp_request_card,
)
from .auth import RoleFilter, require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


@router.message.outer_middleware()
async def _rp_auto_refresh(handler, event: Message, data: dict):  # type: ignore[type-arg]
    """При каждом сообщении от РП — обновляем reply-клавиатуру с бейджами."""
    result = await handler(event, data)
    u = event.from_user
    if not u:
        return result
    db_rp: Database | None = data.get("db")
    cfg = data.get("config")
    if not db_rp or not cfg:
        return result
    try:
        user = await db_rp.get_user_optional(u.id)
        if not user or not user.role:
            return result
        menu_role = resolve_active_menu_role(u.id, user.role)
        if menu_role != Role.RP:
            return result
        unread = await db_rp.count_unread_tasks(u.id)
        uc = await db_rp.count_unread_by_channel(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        # RP-specific badge counts
        rp_t = await db_rp.count_rp_role_tasks(u.id)
        rp_m = await db_rp.count_rp_role_messages(u.id)
        rp_ckp = await db_rp.count_rp_check_kp_tasks(u.id)
        rp_ipay = await db_rp.count_rp_invoice_pay_tasks(u.id)
        rp_ch_kv = await db_rp.count_rp_channel_unread(u.id, "rp_to_manager_kv")
        rp_ch_kia = await db_rp.count_rp_channel_unread(u.id, "rp_to_manager_kia")
        rp_ch_mont = await db_rp.count_rp_channel_unread(u.id, "montazh")
        rp_ch_paid = await db_rp.count_rp_channel_unread(u.id, "rp_invoice_paid")
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
            unread_channels=uc,
            rp_tasks=rp_t, rp_messages=rp_m,
            rp_check_kp=rp_ckp, rp_invoices_pay=rp_ipay,
            rp_ch_mgr_kv=rp_ch_kv, rp_ch_mgr_kia=rp_ch_kia,
            rp_ch_montazh=rp_ch_mont,
            rp_invoice_paid=rp_ch_paid,
        )
        await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
    except Exception:
        log.debug("rp auto-refresh failed", exc_info=True)
    return result


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


async def _answer_or_edit(
    target: Message | CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(  # type: ignore[union-attr]
                text,
                reply_markup=reply_markup,
            )
            return
        except Exception:
            await target.message.answer(  # type: ignore[union-attr]
                text,
                reply_markup=reply_markup,
            )
            return

    await target.answer(text, reply_markup=reply_markup)


# =====================================================================
# ВХОДЯЩИЕ ОТД.ПРОДАЖ
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith("📥 Входящие Отд.Продаж"))
async def rp_inbox_sales(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    if not tasks:
        await message.answer("📥 Входящих задач нет ✅")
        return
    await message.answer(
        f"📥 <b>Входящие Отд.Продаж</b> ({len(tasks)}):\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=tasks_kb(tasks, back_callback="nav:home"),
    )


# =====================================================================
# СЧЕТ В РАБОТУ (мониторинг для РП)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_START))
async def rp_invoice_start_monitor(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await _show_invoices_work_dashboard(message, db)


@router.callback_query(F.data.startswith("rpinv:view:"))
async def rp_invoice_view(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    creator_label = "—"
    if inv.get("created_by"):
        try:
            creator_label = await get_initiator_label(db, int(inv["created_by"]))
        except (TypeError, ValueError):
            pass

    section = await build_invoice_section(db, inv, invoice_id)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
    )
    await cb.message.answer(text)  # type: ignore[union-attr]


# =====================================================================
# СЧЕТА НА ОПЛАТУ (💳 — мониторинг + создание, Этап 7)
#
# Мониторинг: PENDING_PAYMENT + IN_PROGRESS
# Создание: InvoiceCreateSG flow (handlers in legacy rp.py)
#
# Callbacks:
#   rp_inv_pay:create — начать создание счёта на оплату (→ InvoiceCreateSG)
#   rp_inv_pay:refresh[:1|0] — обновить список (хвост = состояние папки;
#                              голый вариант = старые кнопки, папка свёрнута)
#   rp_inv_pay:sent:1|0 — развернуть/свернуть папку «Отправлено в оплату»
#   rp_inv_pay:cancel:<task_id> — снять свой счёт с ГД (→ RpInvCancelSG.reason)
# =====================================================================


# Потолок показа папки «Отправлено в оплату» — ОДИН на карточку и на кнопки.
# Модульная константа, а не локальная в карточке: разъедься эти два числа, и
# кнопки отмены перестанут соответствовать блокам — РП снимал бы не тот счёт.
_SENT_MAX_BLOCKS = 20


def _sent_money(n: float) -> str:
    """Единый формат суммы для карточки папки и для меток кнопок отмены."""
    try:
        return f"{float(n):,.0f}₽".replace(",", " ")
    except (TypeError, ValueError):
        return f"{n}₽"


def _sent_folder_btn(
    b: InlineKeyboardBuilder,
    tasks: list[dict[str, Any]],
    expanded: bool,
) -> None:
    """Кнопка-вход в папку «Отправлено в оплату» + кнопки отмены в развёрнутой.

    Состояние разворота — в callback_data, а НЕ в FSM: кнопка из СТАРОГО
    сообщения обязана оставаться рабочей [[feedback_fsm_old_buttons_trap]].
    При нуле счетов кнопки нет вовсе — пустая папка не нужна.

    ⚠️ Кнопки отмены живут ЗДЕСЬ, а не у вызывающего, намеренно: дашборд рисуется
    в ДВУХ ветках (`all_inv` пуст и не пуст), и разложи мы их по местам вызова —
    пункт появлялся бы через раз. Ровно эту грабельку ловили на `invendgd`
    (td.py: меню «Счёт END» рисуется двумя функциями, одну легко пропустить).

    Порядок и потолок — те же, что у блоков карточки (`_SENT_MAX_BLOCKS`):
    иначе кнопка не соответствовала бы блоку, на который смотрит человек.
    `tasks` обязан приходить из `_rp_sent_invoice_tasks` — кредит-заявки там уже
    отфильтрованы, и кнопка снятия на них не появится.
    """
    count = len(tasks)
    if count <= 0:
        return
    if expanded:
        b.button(text=f"🔼 Свернуть отправленное ({count})", callback_data="rp_inv_pay:sent:0")
    else:
        b.button(text=f"💰 Отправлено в оплату: {count}", callback_data="rp_inv_pay:sent:1")
        return

    for t in tasks[:_SENT_MAX_BLOCKS]:
        payload = try_json_loads(t.get("payload_json") or "{}") or {}
        inv_num = payload.get("invoice_number") or f"#{t['id']}"
        label = f"✖️ №{inv_num} · {_sent_money(payload.get('amount') or 0)}"
        b.button(text=label[:60], callback_data=f"rp_inv_pay:cancel:{int(t['id'])}")


def _invoices_pay_kb(
    invoices: list[dict[str, Any]],
    sent_tasks: list[dict[str, Any]] | None = None,
    sent_expanded: bool = False,
) -> InlineKeyboardMarkup:
    """Inline-кнопки для «Счета на оплату»: папка + список + кнопка создания."""
    b = InlineKeyboardBuilder()
    # Папка ПЕРВОЙ кнопкой: ниже может быть до 90 кнопок счетов (три статуса
    # по limit=30), и вход в папку утонул бы под ними — ровно та жалоба owner'а.
    _sent_folder_btn(b, sent_tasks or [], sent_expanded)
    for inv in invoices:
        status_emoji = invoice_status_emoji(inv.get("status"))
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        text = f"{status_emoji} №{inv.get('invoice_number', '?')} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_work:view:{inv['id']}")
    b.button(text="➕ Создать счёт на оплату", callback_data="rp_inv_pay:create")
    b.button(text="🔄 Обновить", callback_data=f"rp_inv_pay:refresh:{1 if sent_expanded else 0}")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


async def _rp_sent_invoice_tasks(db: Database, user_id: int) -> list[dict[str, Any]]:
    """Активные счета ПОСТАВЩИКУ, отправленные этим РП на оплату ГД.

    🔴 Фильтр по `payload_json["kind"]` обязателен — `TaskType.INVOICE_PAYMENT`
    ПЕРЕГРУЖЕН. Под ним живут три разных потока, и различает их только `kind`
    (канон и предупреждение — `keyboards.py:813-834`):

        kind отсутствует        → счёт поставщику (rp.py:1181, rp_new.py:3326)
        credit_payment_request  → кредит-заявка (chat_proxy.py:893,
                                  manager_new.py:4832) → `_finalize_credit_execution`
        credit_spend_gd_confirm → трата хозяина кошелька (manager_new.py:7515)

    Выборка идёт по СОЗДАТЕЛЮ, а Павел числится сразу двумя ролями
    (`users.role='rp,manager_npn'`, единственный многоролевой в базе) — поэтому
    без фильтра в папку РП попадало то, что он создал в кредит-меню менеджера:
    на 19.08 там висела кредит-заявка #303. owner 19.08 — прятать.

    ⚠️ Фильтр нужен НЕ только показу. Кнопка снятия без него сняла бы
    кредит-заявку мимо кредит-флоу: теряется `cost_type` и задваивается ЗП
    монтажника (`keyboards.py:813-817`).
    """
    tasks = await db.list_tasks_by_creator_and_type(
        created_by=user_id,
        type_filter=TaskType.INVOICE_PAYMENT,
        statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
        limit=100,
    )
    out: list[dict[str, Any]] = []
    for t in tasks:
        payload = try_json_loads(t.get("payload_json") or "{}") or {}
        if isinstance(payload, dict) and payload.get("kind"):
            continue  # кредит-флоу — не счёт поставщику
        out.append(t)
    return out


async def _build_rp_sent_invoices_card(
    db: Database,
    tasks: list[dict[str, Any]],
) -> str | None:
    """Карточка «Отправлено в оплату» для РП — что он уже отправил ГД и статус.

    Зеркало ГД-карточки (`_build_gd_invoices_view` в gd.py): те же задачи
    INVOICE_PAYMENT и тот же дизайн, но выборка по СОЗДАТЕЛЮ, а не по
    исполнителю (задача создаётся РП с created_by=РП, assigned_to=ГД —
    rp.py invoice_finalize). Блок = 3 строки:

        {иконка} {Категория}                {сумма}
        №{номер счёта} · {улица}
        {статус} · мен. {КВ/КИА/НПН}

    У ГД третья строка — «от: {инициатор} ({роль})»; для РП инициатор всегда
    он сам, поэтому на её месте статус задачи. Формулировки — существующий
    generic `task_status_label` (Новая / В работе), user 15.07. Внизу «Итого».

    ⚠️ `tasks` принимает УЖЕ ОТФИЛЬТРОВАННЫЙ список — только из
    `_rp_sent_invoice_tasks`. Сама выборку не делает намеренно: свой запрос
    здесь вернул бы и кредит-заявки, то есть починку 19.08 пришлось бы делать
    дважды. Свёрнутая папка эту функцию не зовёт вовсе, отсюда и экономия на
    `get_invoice` за адресом.

    Только активные OPEN+IN_PROGRESS — как у ГД (user 15.07). Сама карточка
    display-only и боевых данных не пишет; generic-кнопок задач у неё НЕТ —
    задачи назначены ГД, а `_can_manage_task` (tasks.py) пускает только
    assigned_to/админа, так что «✅ Принять» у РП падала бы в «Эта задача
    назначена другому человеку».
    ⚠️ Кнопки «✖️ снять» с 20.08 всё же есть, но живут ОТДЕЛЬНО — в
    `_sent_folder_btn`, своим путём `rp_inv_pay:cancel:*` (owner 19.08,
    вариант B), общий гард не тронут. Здесь по-прежнему только текст.
    [[feedback_card_display_only_no_data_writes]] [[feedback_card_template_standard]]
    """
    import html

    from ..rp_start_card import CATS, _mt_to_cat, vw
    from ..utils import task_status_label

    if not tasks:
        return None

    # Потолок показа — модульный `_SENT_MAX_BLOCKS`. Блок = 3 строки (~46 знаков),
    # сообщение Telegram — 4096 символов на ВСЁ, включая шапку дашборда. Выборка
    # идёт с limit=100, и без потолка на трёх десятках счетов сообщение перестало
    # бы отправляться целиком: _answer_or_edit (:170) глотает падение edit_text и
    # пробует answer(), который упрётся в тот же лимит — экран «Счета на оплату»
    # лёг бы весь. Сейчас активных 15, запас четырёхкратный.
    # ⚠️ То же число режет и кнопки отмены (`_sent_folder_btn`) — они обязаны
    # соответствовать блокам один в один, поэтому константа общая, не локальная.
    _MAX_BLOCKS = _SENT_MAX_BLOCKS

    icon_by_cat = {k: ic for (k, ic, *_rest) in CATS}
    title_by_cat = {k: ttl for (k, _ic, _fld, ttl) in CATS}

    _money = _sent_money

    INDENT = "   "
    blocks: list[list[tuple[str, str]]] = []  # блок = [(label, value)]; value="" → без right-align
    total = 0.0
    for t in tasks:
        payload = try_json_loads(t.get("payload_json") or "{}") or {}
        inv_num = payload.get("invoice_number") or f"#{t['id']}"
        amount = float(payload.get("amount") or 0)
        total += amount
        if len(blocks) >= _MAX_BLOCKS:
            continue  # деньги в «Итого» уже посчитаны, блок не рисуем
        cat = _mt_to_cat(payload.get("material_type") or "")
        icon = icon_by_cat.get(cat, "🧱")
        cat_title = title_by_cat.get(cat, "Прочее")
        inv_id = payload.get("invoice_id") or payload.get("parent_invoice_id")
        addr = ""
        if inv_id:
            inv = await db.get_invoice(int(inv_id))
            addr = (inv or {}).get("object_address") or ""
        street = _street(addr, 14) if addr else "—"
        mgr = "КИА" if "КИА" in inv_num else ("НПН" if "НПН" in inv_num else "КВ")
        blocks.append([
            (f"{icon} {cat_title}", _money(amount)),
            (f"№{html.escape(str(inv_num))} · {html.escape(street)}", ""),
            (f"{html.escape(task_status_label(t.get('status')))} · мен. {mgr}", ""),
        ])

    foot = [("Итого", _money(total))]

    # Динамическая ширина: max визуальная по всем right-align и левым строкам —
    # чтобы суммы и footer сходились в один столбец (идентично ГД).
    raw: list[int] = []
    for blk in blocks:
        for lbl, val in blk:
            raw.append(vw(INDENT) + vw(lbl) + (1 + vw(val) if val else 0))
    for lbl, val in foot:
        raw.append(vw(INDENT) + vw(lbl) + 1 + vw(val))
    width = max(raw) if raw else 30

    def _rline(lbl: str, val: str) -> str:
        pad = max(1, width - vw(INDENT) - vw(lbl) - vw(val))
        return f"{INDENT}{lbl}{' ' * pad}{val}"

    body_lines: list[str] = []
    for i, blk in enumerate(blocks):
        if i:
            body_lines.append("")  # пустая строка-разделитель между блоками
        for lbl, val in blk:
            body_lines.append(_rline(lbl, val) if val else f"{INDENT}{lbl}")
    _hidden = len(tasks) - len(blocks)
    if _hidden > 0:
        body_lines.append("")
        body_lines.append(f"{INDENT}…и ещё {_hidden} — в «Итого» они учтены")
    body_lines.append(INDENT + "━" * max(3, width - vw(INDENT)))
    for lbl, val in foot:
        body_lines.append(_rline(lbl, val))

    body = "\n".join(body_lines)
    return f"<b>💰  Отправлено в оплату</b>\n<pre>{body}</pre>"


async def _show_invoices_pay_dashboard(
    target: Message | CallbackQuery,
    db: Database,
    *,
    show_sent: bool = False,
) -> None:
    pending = await db.list_invoices(status=InvoiceStatus.PENDING_PAYMENT, limit=30)
    in_progress = await db.list_invoices(status=InvoiceStatus.IN_PROGRESS, limit=30)
    credit = await db.list_invoices(status=InvoiceStatus.CREDIT, limit=30)
    all_inv = list(pending) + list(in_progress) + list(credit)

    # Папка «Отправлено в оплату» (ТЗ 15.07; вход кнопкой — owner 18.08).
    # В ТЕЛЕ этого же сообщения, а НЕ отдельным: и «🔄 Обновить», и разворот
    # идут через _answer_or_edit, который правит сообщение in-place — отдельное
    # сообщение плодило бы копию на каждом нажатии (та же грабля, из-за которой
    # у ГД убрали «Обновить» 26.06).
    # СВЁРНУТА по умолчанию: при 15 счетах развёрнутый блок — полсотни строк, в
    # которых тонул сам список счетов; owner 18.08 «папку не нашёл» именно из-за
    # этого. Свёрнутая ветка ещё и не ходит в get_invoice за адресом на КАЖДУЮ
    # задачу (было 15 лишних запросов на каждую отрисовку дашборда).
    _uid = target.from_user.id if target.from_user else None
    _sent_tasks = await _rp_sent_invoice_tasks(db, int(_uid)) if _uid else []
    _sent_block = ""
    if show_sent and _sent_tasks:
        _sent = await _build_rp_sent_invoices_card(db, _sent_tasks)
        _sent_block = f"\n\n{_sent}" if _sent else ""

    if not all_inv:
        b = InlineKeyboardBuilder()
        _sent_folder_btn(b, _sent_tasks, show_sent)
        b.button(text="➕ Создать счёт на оплату", callback_data="rp_inv_pay:create")
        b.button(text="🔄 Обновить", callback_data=f"rp_inv_pay:refresh:{1 if show_sent else 0}")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await _answer_or_edit(
            target,
            "💳 <b>Счета на оплату</b>\n\n"
            "Нет счетов, ожидающих оплаты ✅\n\n"
            "Можно создать новый счёт или обновить список."
            + _sent_block,
            reply_markup=b.as_markup(),
        )
        return

    header_parts: list[str] = []
    if pending:
        header_parts.append(f"⏳ Ожидают: {len(pending)}")
    if in_progress:
        header_parts.append(f"🔄 В работе: {len(in_progress)}")
    if credit:
        header_parts.append(f"💳 Кредит: {len(credit)}")

    await _answer_or_edit(
        target,
        f"💳 <b>Счета на оплату</b> ({len(all_inv)})\n"
        f"{' | '.join(header_parts)}"
        f"{_sent_block}\n\n"
        "Нажмите для просмотра или создайте новый:",
        reply_markup=_invoices_pay_kb(all_inv, _sent_tasks, show_sent),
    )


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICES_PAY))
async def rp_invoices_pay(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: Счета на оплату (мониторинг + создание)."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await _show_invoices_pay_dashboard(message, db)


def _sent_flag(data: str | None) -> bool:
    """Хвостовой флаг «папка развёрнута» из callback_data (`…:1` / `…:0`).

    Отсутствие хвоста = свёрнуто. Это и есть совместимость со СТАРЫМИ кнопками:
    до этой правки «Обновить» ходила голым `rp_inv_pay:refresh`, и она обязана
    остаться рабочей [[feedback_fsm_old_buttons_trap]].
    """
    return (data or "").rsplit(":", 1)[-1] == "1"


@router.callback_query(F.data.startswith("rp_inv_pay:refresh"))
async def rp_invoices_pay_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить список «Счета на оплату», сохранив состояние папки.

    Флаг едет в callback_data, потому что иначе «Обновить» схлопывала бы
    только что открытую папку — то есть ломала бы ровно то, ради чего её
    открывали.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await state.clear()
    await _show_invoices_pay_dashboard(cb, db, show_sent=_sent_flag(cb.data))


@router.callback_query(F.data.startswith("rp_inv_pay:sent:"))
async def rp_invoices_pay_sent_toggle(cb: CallbackQuery, db: Database) -> None:
    """Развернуть/свернуть папку «Отправлено в оплату» в ТОМ ЖЕ сообщении.

    Только показ, боевых данных не пишет [[feedback_card_display_only_no_data_writes]].

    ⚠️ FSM здесь НЕ чистится намеренно, в отличие от соседней «Обновить»:
    состояние разворота живёт в callback_data, а чужой поток рвать незачем —
    ровно на этом обожглись 04.08, когда `acc_q:cancel` безусловным
    `state.clear()` убивал начатый ответ по ЭДО.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await _show_invoices_pay_dashboard(cb, db, show_sent=_sent_flag(cb.data))


async def _rp_cancel_reject_text(db: Database, task_id: int, user_id: int) -> str:
    """Точная причина отказа в снятии — текст для алерта.

    Молчать на отказе нельзя: catch-all для callback'ов в проекте нет, и без
    ответа кнопка повиснет спиннером [[feedback_fsm_old_buttons_trap]]. Запрос
    в БД здесь идёт ТОЛЬКО на пути отказа — успешный путь его не платит.
    """
    try:
        raw = await db.get_task(task_id)
    except KeyError:
        return "Задача не найдена."
    if (raw.get("type") or "") != TaskType.INVOICE_PAYMENT:
        return "Это не счёт на оплату."
    payload = try_json_loads(raw.get("payload_json") or "{}") or {}
    if isinstance(payload, dict) and payload.get("kind"):
        # Кредит-заявка / трата хозяина кошелька: снимаются кредит-флоу, иначе
        # теряется cost_type и задваивается ЗП монтажника (keyboards.py:813-834).
        return "Эта заявка снимается через кредит-флоу, а не отсюда."
    try:
        if int(raw.get("created_by") or 0) != user_id:
            return "Снять можно только свой счёт."
    except (TypeError, ValueError):
        return "Снять можно только свой счёт."
    if (raw.get("status") or "") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        return "Задача уже закрыта или обработана."
    return "Счёт недоступен для снятия."


async def _rp_cancel_gd_card(db: Database, task: dict, reason: str) -> str:
    """Карточка ГД «Счёт снят автором» — по эталону, ОДНИМ блоком.

    Owner 23.08: прежний вид (плоский текст с эмодзи-метками `📋/💰/👤/📝`) —
    это **anti-pattern B** из [[feedback_card_template_standard]], запрещённый
    для всех ролей; заодно заказано добавить назначение (стекло/металл/…) и
    адрес «принятого образца», и держать всё ОДНОЙ карточкой.

    🔑 Источники и вид взяты 1:1 у экрана ГД «Счета на оплату»
    (`gd._build_gd_invoices_view`, деплой `gdcats` 15.08): payload задачи
    (`invoice_number`/`amount`/`material_type`), адрес счёта по
    `invoice_id`/`parent_invoice_id`, категория — `_mt_to_cat`+`CATS`, адрес —
    `_addr_cell` (Москва → улица, не Москва → город, owner 30.07), ширина —
    `vw` (эмодзи ≈ 2 колонки). Один и тот же счёт обязан выглядеть одинаково
    там и здесь, иначе ГД сверяет две разные витрины.

    ⚠️ Экранируем ВЕСЬ текст блока разом и на СЫРОМ считаем ширину: `vw` по
    экранированному дал бы `&amp;` пять колонок вместо одной.
    Только показ, боевых данных не пишет
    ([[feedback_card_display_only_no_data_writes]]).
    """
    import html as _html
    import textwrap

    from ..rp_start_card import CATS, _addr_cell, _mt_to_cat, vw

    payload = try_json_loads(task.get("payload_json") or "{}") or {}
    if not isinstance(payload, dict):
        payload = {}
    inv_num = str(payload.get("invoice_number") or f"#{task.get('id')}")
    money = _sent_money(payload.get("amount") or 0)

    cat = _mt_to_cat(payload.get("material_type") or "")
    icon = next((ic for (k, ic, *_r) in CATS if k == cat), "🧱")
    title = next((ttl for (k, _ic, _f, ttl) in CATS if k == cat), "Прочее")

    inv_id = payload.get("invoice_id") or payload.get("parent_invoice_id")
    addr = ""
    if inv_id:
        try:
            addr = (await db.get_invoice(int(inv_id)) or {}).get("object_address") or ""
        except (KeyError, TypeError, ValueError):
            addr = ""
    street = _addr_cell(addr, 14) if addr else "—"

    # Инициатор — ПЕРВЫМ именем и ролью, как задумано на экране ГД «Счета на
    # оплату» (`gd._build_gd_invoices_view::_creator`). Полное имя с @username,
    # которое отдаёт `get_initiator_label`, уводит ширину карточки за 50 колонок
    # и ломает её на телефоне — замерено стендом до правки (49 колонок в строке).
    # Снявший и создатель — один человек: снять чужой счёт не даёт гард
    # `_rp_sent_invoice_tasks` (выборка идёт по `created_by`).
    #
    # 🔴 Роль берём из `users.role`, а НЕ из `task["creator_role"]`, как сосед:
    # такой колонки в таблице `tasks` НЕТ ВООБЩЕ (проверено на боевой —
    # `no such column: creator_role`), поэтому у ГД роль не печаталась никогда.
    # Повторять мёртвую ветку незачем. `parse_roles` берёт первую роль по
    # бизнес-порядку — у Павла `role='rp,manager_npn'`, и это «РП».
    role_lbl = {
        "rp": "РП", "gd": "ГД", "td": "ТД",
        "manager": "менеджер", "manager_kv": "менеджер",
        "manager_kia": "менеджер", "manager_npn": "менеджер",
    }
    who = "—"
    cid = task.get("created_by")
    if cid:
        try:
            user = await db.get_user_optional(int(cid))
        except (TypeError, ValueError):
            user = None
        name = (user.full_name.split()[0] if (user and user.full_name) else "") or f"#{cid}"
        # ⚠️ При нескольких ролях берём ТУ, в которой человек выступает ЗДЕСЬ.
        # `parse_roles` сортирует по бизнес-порядку и у Павла отдаёт
        # ['manager_npn', 'rp'] — первый элемент дал бы «менеджер» на карточке
        # счёта, поданного им как РП (замерено на боевой).
        roles = parse_roles(getattr(user, "role", "") if user else "")
        role = "rp" if "rp" in roles else (roles[0] if roles else "")
        lbl = role_lbl.get(role, "")
        who = f"{name} ({lbl})" if lbl else name

    indent = "   "
    head = f"№{inv_num} · {street}"
    left = [f"{icon} {title}", f"👤 {who}"]

    # Пол ширины 38, а не 34: денежная строка тут одна, и при 34 сумма упиралась
    # в адрес через ОДИН пробел — правое выравнивание становилось фиктивным
    # (замерено стендом). 38 даёт видимый зазор и на телефон помещается.
    width = max(
        [vw(indent) + vw(head) + 1 + vw(money)]
        + [vw(indent) + vw(row) for row in left]
        + [38]
    )
    pad = max(1, width - vw(indent) - vw(head) - vw(money))
    lines = [f"{indent}{head}{' ' * pad}{money}"]
    lines += [f"{indent}{row}" for row in left]

    # Причина — свободный текст человека: переносим по ширине карточки, иначе
    # <pre> уедет в горизонтальную прокрутку и на телефоне будет нечитаем.
    # Слово «Причина» сохранено — оно было в прежнем тексте, утверждённом owner'ом
    # 22.08, и убирать его никто не просил [[feedback_do_exactly_asked_no_own_initiative]].
    chunks = textwrap.wrap(f"Причина: {reason}".strip(), max(20, width - vw(indent) - 3))
    for i, chunk in enumerate(chunks or ["Причина: —"]):
        lines.append(f"{indent}{'📝 ' if i == 0 else '   '}{chunk}")

    return "<b>🚫  Счёт снят автором</b>\n<pre>" + _html.escape("\n".join(lines)) + "</pre>"


@router.callback_query(F.data.startswith("rp_inv_pay:cancel:"))
async def rp_invoices_pay_cancel_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """РП снимает свой счёт на оплату с ГД — шаг 1: спросить причину.

    owner 20.08: «это он отправляет счета в оплату для ГД и ему нужна кнопка
    отмены этой задачи; соответственно у ГД эта отменённая задача тоже должна
    сняться, автоматически прийти информационное сообщение с причиной».

    🔑 Причину спрашиваем ВСЕГДА — без развилки по `accepted_at` и без поблажки
    админам. Иначе третий пункт требования невыполним: причины просто не будет.
    Штатная ветка (`tasks.py:609`) спрашивает её лишь при `accepted_at`, а он у
    задач `invoice_payment` пуст У ВСЕХ — и у 15 активных, и у 10 последних
    закрытых (замер на боевой 20.08). Копия штатного условия не сработала бы ни
    разу. ⚠️ И Павел, и ГД оба входят в `ADMIN_IDS` — поблажка админам обошла бы
    шаг причины ровно у того человека, ради которого он и заводится.

    ⛔ Общий гард `_can_manage_task` (tasks.py:34) и обе его точки вызова
    (:414, :820) НЕ трогаем — решение owner'а 19.08, вариант B: свой путь.
    """
    import html

    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    uid = cb.from_user.id if cb.from_user else None
    if uid is None:
        await cb.answer("Не удалось определить пользователя.", show_alert=True)
        return
    try:
        task_id = int((cb.data or "").rsplit(":", 1)[-1])
    except (TypeError, ValueError):
        await cb.answer("Не удалось разобрать кнопку.", show_alert=True)
        return

    # Гард — членство в СВОЕЙ выборке. Тогда «тип», «kind пуст», «мой» и
    # «активна» выполняются по построению и ровно тем же источником, которым
    # нарисован показ; фильтр kind не приходится дублировать вторым местом.
    task = next(
        (t for t in await _rp_sent_invoice_tasks(db, int(uid)) if int(t["id"]) == task_id),
        None,
    )
    if task is None:
        await cb.answer(await _rp_cancel_reject_text(db, task_id, int(uid)), show_alert=True)
        return

    payload = try_json_loads(task.get("payload_json") or "{}") or {}
    inv_num = payload.get("invoice_number") or f"#{task_id}"
    money = _sent_money(payload.get("amount") or 0)

    await cb.answer()
    await state.clear()
    await state.set_state(RpInvCancelSG.reason)
    # Свой ключ, а НЕ общий `cancel_task_id` из TaskCancelReasonSG: под одним
    # ключом уже жили два разных потока, и на этом обожглись 04.08 (acc_q:*
    # безусловным state.clear() убивал начатый ответ по ЭДО).
    await state.update_data(rp_cancel_task_id=task_id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✖️ Снять счёт №{html.escape(str(inv_num))} на {money} с ГД.\n\n"
        "Укажите <b>причину</b> — она уйдёт ГД:",
    )


@router.message(RpInvCancelSG.reason)
async def rp_invoices_pay_cancel_reason(
    message: Message,
    state: FSMContext,
    db: Database,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """РП снимает свой счёт — шаг 2: причина → запись → уведомление ГД."""
    import html

    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    reason = (message.text or "").strip()
    if len(reason) < 3:
        # Валидация как у штатной ветки (tasks.py:710-712) — state не рвём.
        await message.answer("Укажите причину отмены (минимум 3 символа):")
        return

    data = await state.get_data()
    task_id = data.get("rp_cancel_task_id")
    uid = message.from_user.id if message.from_user else None
    if not isinstance(task_id, int) or task_id <= 0 or uid is None:
        await state.clear()
        await message.answer("❌ Счёт не найден — начните снятие заново.")
        return

    # Перепроверка ПЕРЕД записью: между нажатием кнопки и вводом причины ГД мог
    # задачу забрать или закрыть, а FSM об этом не знает.
    task = next(
        (t for t in await _rp_sent_invoice_tasks(db, int(uid)) if int(t["id"]) == task_id),
        None,
    )
    if task is None:
        await state.clear()
        await message.answer(f"❌ {await _rp_cancel_reject_text(db, task_id, int(uid))}")
        await _show_invoices_pay_dashboard(message, db, show_sent=True)
        return

    # Атомарно и ровно как штатное снятие: статус именно `rejected`
    # (tasks.py:621 — расходиться нельзя), expected_statuses отбивает гонку.
    updated = await db.update_task_status(
        task_id, TaskStatus.REJECTED,
        expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
    )
    if updated is None:
        await state.clear()
        await message.answer("❌ Задача уже была обработана.")
        await _show_invoices_pay_dashboard(message, db, show_sent=True)
        return

    await state.clear()

    # 🔴 sync_task зовём ЯВНО. Штатный `task_cancel_with_reason`
    # (tasks.py:699-796) его НЕ делает: `integrations` там принят параметром
    # (:706) и в теле не используется НИ РАЗУ — то есть снятие С ПРИЧИНОЙ задачу
    # не синкает, а снятие без причины (:627) синкает. Наследовать этот дефект
    # своим путём незачем; чинить общий код — отдельный вопрос owner'у.
    project_code = ""
    if updated.get("project_id"):
        try:
            project_code = (await db.get_project(int(updated["project_id"]))).get("code") or ""
        except (KeyError, TypeError, ValueError):
            project_code = ""
    await integrations.sync_task(updated, project_code=project_code)

    payload = try_json_loads(updated.get("payload_json") or "{}") or {}
    inv_num = html.escape(str(payload.get("invoice_number") or f"#{task_id}"))
    reason_safe = html.escape(reason)

    # Уведомление ГД — пункт (3) требования owner'а: ОДНО сообщение и по эталону
    # (owner 23.08, прежний плоский вид = anti-pattern B). Экранирование живёт
    # внутри `_rp_cancel_gd_card`: parse_mode у бота HTML, а причина — свободный
    # текст человека; safe_send умеет откатиться в plain text, но тогда ГД
    # увидит сырую разметку вместо текста.
    gd_id = updated.get("assigned_to")
    if gd_id:
        try:
            await notifier.safe_send(
                int(gd_id),
                await _rp_cancel_gd_card(db, updated, reason),
            )
        except (TypeError, ValueError):
            log.warning("rp inv cancel: bad assigned_to on task %s", task_id)

    await message.answer(f"🚫 Счёт №{inv_num} снят с ГД.\n📝 Причина: {reason_safe}")
    # Дашборд — НОВЫМ сообщением: причина пришла message'ем, прежнее сообщение
    # дашборда отсюда не отредактировать. Осознанное отступление от «не плодить
    # сообщения» — то правило было про «🔄 Обновить», который ходит callback'ом.
    await _show_invoices_pay_dashboard(message, db, show_sent=True)


@router.callback_query(F.data == "rp_inv_pay:create")
async def rp_invoices_pay_create(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание счёта на оплату ГД (→ InvoiceCreateSG).

    Simplified: skip project selection, go directly to invoice picker.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    from ..states import InvoiceCreateSG
    from ..keyboards import invoice_select_kb

    # include_credit=True: при подаче счёта в оплату ГД в списке должны быть
    # и обычные, и кредитные счета (is_credit=1) со статусом в работе.
    invoices = await db.list_invoices_in_work(
        limit=20, only_regular=True, include_credit=True,
    )
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Нет счетов в работе."
        )
        return

    await state.clear()
    await state.set_state(InvoiceCreateSG.parent_invoice)
    await cb.message.answer(  # type: ignore[union-attr]
        "💳 <b>Счёт на оплату ГД</b>\n"
        "Шаг 1: выберите счёт объекта (№, адрес):",
        reply_markup=invoice_select_kb(invoices, prefix="inv_create_parent", allow_skip=True, back_callback="nav:home"),
    )


# =====================================================================
# СЧЕТ END (входящие для РП)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_END))
async def rp_invoice_end(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    invoices = await db.list_invoices(status=InvoiceStatus.CLOSING)
    ended = await db.list_invoices(status=InvoiceStatus.ENDED, limit=10)
    credit = await db.list_invoices(status=InvoiceStatus.CREDIT, limit=30)
    all_inv = list(invoices) + list(ended) + list(credit)

    if not all_inv:
        await answer_service(message, "🏁 Нет счетов в процессе закрытия / закрытых.", delay_seconds=60)
        return
    await message.answer(
        f"🏁 <b>Счет End</b> ({len(all_inv)}):\n\n"
        "Нажмите для просмотра:",
        reply_markup=invoice_list_kb(all_inv, action_prefix="rpinv", back_callback="nav:home"),
    )


# =====================================================================
# ПРОБЛЕМА / ВОПРОС
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_ISSUE))
async def rp_issue(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=30)  # type: ignore[union-attr]
    issues = [t for t in tasks if t.get("type") == TaskType.ISSUE]
    if not issues:
        await answer_service(message, "🆘 Нет входящих проблем/вопросов.", delay_seconds=60)
        return
    await message.answer(
        f"🆘 <b>Проблема / Вопрос</b> ({len(issues)}):",
        reply_markup=tasks_kb(issues, back_callback="nav:home"),
    )


# =====================================================================
# МЕНЕДЖЕР 1 (КВ) — chat-proxy
# =====================================================================

@router.message(lambda m: (
    ((m.text or "").strip().startswith(RP_BTN_MGR_KV)
     or (m.text or "").strip().startswith(RP_SUBBTN_MGR_KV))
    and "(кредит)" not in (m.text or "")
))
async def rp_chat_mgr_kv(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kv")
    # #38: Invoice picker перед чатом
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True, include_credit=True)
    kv_invoices = [i for i in invoices if i.get("creator_role") == "manager_kv"]
    if kv_invoices:
        b = InlineKeyboardBuilder()
        for inv in kv_invoices[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"rp_chat_inv:kv:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="rp_chat_inv:kv:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "👤 <b>Менеджер 1 (КВ)</b>\n\n"
            "Выберите счёт для привязки к переписке:",
            reply_markup=b.as_markup(),
        )
    else:
        await message.answer(
            "👤 <b>Менеджер 1 (КВ)</b>\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


@router.message(lambda m: (
    ((m.text or "").strip().startswith(RP_BTN_MGR_KIA)
     or (m.text or "").strip().startswith(RP_SUBBTN_MGR_KIA))
    and "(кредит)" not in (m.text or "")
))
async def rp_chat_mgr_kia(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_manager_kia")
    # #38: Invoice picker перед чатом
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True, include_credit=True)
    kia_invoices = [i for i in invoices if i.get("creator_role") == "manager_kia"]
    if kia_invoices:
        b = InlineKeyboardBuilder()
        for inv in kia_invoices[:10]:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "—")[:20]
            b.button(text=f"📄 №{num} — {addr}"[:45], callback_data=f"rp_chat_inv:kia:{inv['id']}")
        b.button(text="📝 Без привязки к счёту", callback_data="rp_chat_inv:kia:0")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        await message.answer(
            "👤 <b>Менеджер 2 (КИА)</b>\n\n"
            "Выберите счёт для привязки к переписке:",
            reply_markup=b.as_markup(),
        )
    else:
        await message.answer(
            "👤 <b>Менеджер 2 (КИА)</b>\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


# =====================================================================
# INVOICE PICKER FOR CHAT (#38/#39)
# =====================================================================

@router.callback_query(F.data.startswith("rp_chat_inv:"))
async def rp_chat_invoice_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП выбрал счёт для привязки к чату с менеджером (#38)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    mgr_key = parts[1]  # kv, kia, montazh
    inv_id = int(parts[2])

    channel_map = {"kv": "rp_to_manager_kv", "kia": "rp_to_manager_kia", "montazh": "montazh"}
    channel = channel_map.get(mgr_key, f"rp_to_manager_{mgr_key}")

    await state.update_data(channel=channel, linked_invoice_id=inv_id if inv_id else None)

    label_map = {"kv": "Менеджер 1 (КВ)", "kia": "Менеджер 2 (КИА)", "montazh": "Монтажная гр."}
    label = label_map.get(mgr_key, mgr_key)

    inv_text = ""
    if inv_id:
        inv = await db.get_invoice(inv_id)
        if inv:
            inv_text = f"\n📄 Привязан счёт: №{inv.get('invoice_number', '?')}"

    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"👤 <b>{label}</b>{inv_text}\n\nВыберите действие:",
        )
    except Exception:
        pass

    if mgr_key == "montazh":
        await cb.message.answer(  # type: ignore[union-attr]
            f"🔧 <b>Монтажная гр.</b>{inv_text}\n\nВыберите действие:",
            reply_markup=rp_montazh_submenu("⬅️ Назад"),
        )
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            f"👤 <b>{label}</b>{inv_text}\n\nВыберите действие:",
            reply_markup=rp_chat_submenu("⬅️ Назад"),
        )


# =====================================================================
# МОНТАЖНАЯ ГР. — chat-proxy + В работу (Этап 9)
#
# Submenu: 💬 Чат / 🔧 В работу
# - Чат → ManagerChatProxySG (standard chat-proxy with montazh channel)
# - В работу → список активных счетов с монтажниками
#
# Callbacks:
#   rp_montazh:work_view:\d+  — карточка счёта «В работу»
#   rp_montazh:work_refresh   — обновить список «В работу»
# =====================================================================

@router.message(
    lambda m: (m.text or "").strip().startswith(RP_BTN_MONTAZH) or (m.text or "").strip().startswith(RP_SUBBTN_MONTAZH),
    RoleFilter([Role.RP]),
)
async def rp_chat_montazh(message: Message, state: FSMContext, db: Database) -> None:
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")

    # Считаем счета в работе
    in_work_all = await db.list_invoices_in_work(limit=50, include_credit=True)
    in_work_montazh = [
        i for i in in_work_all
        if i.get("montazh_stage") in ("in_work", "razmery_ok", "invoice_ok")
    ]
    n_in_work = len(in_work_montazh)
    n_send = await db.count_invoices_to_send_montazh()

    b = InlineKeyboardBuilder()
    b.button(text=f"📋 Счета в работе ({n_in_work})", callback_data="rp_montazh:list_inwork")
    b.button(text=(f"➕ Счёт в монтаж 🔴{n_send}" if n_send else "➕ Счёт в монтаж"), callback_data="rp_montazh:send_to_work")
    b.button(text="💬 Чат", callback_data="rp_montazh:chat")
    b.button(text="📐 Размеры", callback_data="rp_montazh:razmery")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    await message.answer(
        "🔧 <b>Монтажная гр.</b>\n\nВыберите действие:",
        reply_markup=b.as_markup(),
    )


@router.message(ManagerChatProxySG.menu, F.text == "💬 Чат")
async def rp_montazh_chat(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Монтажная гр. → Чат: переписка с монтажниками."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return  # Only handle montazh context
    limit = getattr(config, "chat_history_limit", 20)
    messages_list = await db.list_chat_messages(channel, limit=limit)
    if not messages_list:
        await message.answer("💬 Пока нет сообщений в чате с монтажной группой.")
        return
    lines: list[str] = [f"💬 <b>Чат — Монтажная гр.</b> (последние {len(messages_list)}):\n"]
    for m in messages_list:
        sender_id = m.get("sender_id", 0)
        sender_label = await get_initiator_label(db, int(sender_id)) if sender_id else "?"
        text_msg = m.get("text", "")
        ts = m.get("created_at", "")[:16]
        direction = m.get("direction", "")
        arrow = "→" if direction == "outgoing" else "←"
        lines.append(f"<b>{sender_label}</b> {arrow} ({ts}):\n{text_msg}")
    await message.answer("\n\n".join(lines[-12:]))


@router.message(ManagerChatProxySG.menu, F.text == "🔧 В работу")
async def rp_montazh_in_work(message: Message, state: FSMContext, db: Database) -> None:
    """Монтажная гр. → В работу: два действия — список в работе / отправить в работу."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return  # Only handle montazh context

    # Считаем счета в работе (montazh_stage in_work+)
    in_work = await db.list_invoices_in_work(limit=50, include_credit=True)
    in_work_montazh = [
        i for i in in_work
        if i.get("montazh_stage") in ("in_work", "razmery_ok", "invoice_ok")
    ]
    n_in_work = len(in_work_montazh)
    n_send = await db.count_invoices_to_send_montazh()

    b = InlineKeyboardBuilder()
    b.button(
        text=f"📋 Счета в работе ({n_in_work})",
        callback_data="rp_montazh:list_inwork",
    )
    b.button(text=(f"➕ Счёт в монтаж 🔴{n_send}" if n_send else "➕ Счёт в монтаж"), callback_data="rp_montazh:send_to_work")
    b.adjust(1)

    await message.answer(
        "🔧 <b>Монтажная гр. — В работу</b>\n\n"
        "Выберите действие:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:list_inwork")
async def rp_montazh_list_inwork(cb: CallbackQuery, db: Database) -> None:
    """Список счетов, уже принятых монтажником в работу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_invoices_in_work(limit=50, include_credit=True)
    in_work = [
        i for i in invoices
        if i.get("montazh_stage") in ("in_work", "razmery_ok", "invoice_ok")
        or (i.get("edo_task_id") == 2
            and i.get("montazh_stage") in ("assigned", "in_work", "razmery_ok", "invoice_ok"))
    ]
    if not in_work:
        await cb.message.answer(  # type: ignore[union-attr]
            "📋 <b>Счета в работе</b>\n\nНет счетов в работе у монтажника ✅"
        )
        return

    b = InlineKeyboardBuilder()
    for inv in in_work:
        ok_emoji = "✅" if inv.get("installer_ok") else "⏳"
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        street = _street(inv.get("object_address"), 22)
        grp = "2️⃣" if inv.get("edo_task_id") == 2 else "1️⃣"
        text = f"{grp}{ok_emoji} {street} — {amount_str}"
        b.button(text=text[:60], callback_data=f"rp_montazh:work_view:{inv['id']}")
    b.button(text="🔄 Обновить", callback_data="rp_montazh:list_inwork")
    b.adjust(1)

    n_ok = sum(1 for inv in in_work if inv.get("installer_ok"))
    n_pending = len(in_work) - n_ok
    stats = []
    if n_ok:
        stats.append(f"✅ Счет ОК: {n_ok}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 <b>Счета в работе</b> ({len(in_work)})\n"
        f"{' | '.join(stats)}\n\n"
        "Нажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:send_to_work")
async def rp_montazh_send_to_work(cb: CallbackQuery, db: Database) -> None:
    """Список счетов, доступных для отправки монтажнику."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_invoices_to_send_montazh(limit=20)

    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "➕ <b>Счёт в монтаж</b>\n\nНет счетов без подтверждения монтажника ✅"
        )
        return

    b = InlineKeyboardBuilder()
    # Собираем строки, затем добиваем улицы невидимым пробелом (U+00A0) до одной
    # ширины: Telegram центрирует кнопки, равная ширина = ровный столбик (иконки в
    # один левый край). Шрифт кнопок пропорциональный — выравнивание приблизительное,
    # но зигзаг названий разной длины уходит. User 31.05 (левый край нельзя → подгон).
    rows = []
    for inv in invoices:
        stage = inv.get("montazh_stage") or ""
        if (inv.get("status") or "") == "credit":
            prefix = "🏦"
        elif inv.get("edo_task_id") == 2:
            prefix = "2️⃣"
        elif stage == "assigned":
            prefix = "📩"
        else:
            prefix = "📄"
        rows.append((inv, prefix, _street(inv.get("object_address"), 30)))
    pad_w = max((len(s) for _, _, s in rows), default=0)
    for inv, prefix, street in rows:
        text = f"{prefix} {street.ljust(pad_w, chr(0xA0))}"
        b.button(text=text[:60], callback_data=f"rp_montazh:assign:{inv['id']}")
    b.button(text="⬅️ Назад", callback_data="rp_montazh:back_menu")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"➕ <b>Счёт в монтаж</b> ({len(invoices)})\n\n"
        "Выберите счёт для отправки монтажнику:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("rp_montazh:assign:"))
async def rp_montazh_assign(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Выбор монтажной группы: штатный Игорь (1) или Наёмники (2)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    # Защита от повторного назначения
    stage = inv.get("montazh_stage") or ""
    if stage in ("assigned", "in_work", "razmery_ok", "invoice_ok", "invoice_end"):
        from ..enums import MONTAZH_STAGE_LABELS
        label = MONTAZH_STAGE_LABELS.get(stage, stage)
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Счёт №{inv.get('invoice_number', '?')} уже назначен (стадия: {label})",
        )
        return

    num = inv.get("invoice_number", "?")
    b = InlineKeyboardBuilder()
    b.button(text="1️⃣ Наша монтажная группа", callback_data=f"rp_montazh:grp_igor:{invoice_id}")
    b.button(text="2️⃣ Наёмная монтажная группа", callback_data=f"rp_montazh:grp_naem:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📄 <b>Счёт №{num}</b>\n\nКто выполняет монтаж?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("rp_montazh:grp_igor:"))
async def rp_montazh_grp_igor(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Группа 1 (штатный Игорь): выбор — прикрепить файлы или отправить сразу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    num = inv.get("invoice_number", "?")
    b = InlineKeyboardBuilder()
    b.button(text="📎 Прикрепить файлы", callback_data=f"rp_montazh:attach:{invoice_id}")
    b.button(text="➡️ Отправить без вложений", callback_data=f"rp_montazh:send_now:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📄 <b>Счёт №{num}</b>\n\nПрикрепить вложения для монтажника?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("rp_montazh:grp_naem:"))
async def rp_montazh_grp_naem(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Группа 2 (Наёмная монт. группа): спросить согласованную сумму ЗП монтажа.

    Поток: РП вводит согласованную сумму → (для б/н спрашиваем «+10%? Да/Нет», кредит —
    без вопроса) → фиксируем `montazh_agreed_amount`, метим счёт наёмным (`edo_task_id=2`,
    `assigned_to=NULL`, `montazh_stage='assigned'`) → счёт переезжает в «Счета в работе».
    Запрос ЗП к ГД — позже, по кнопке «✅ Монтаж ОК».
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Защита от повторной обработки уже работающего счёта (старая кнопка)
    stage = inv.get("montazh_stage") or ""
    if stage in ("in_work", "razmery_ok", "invoice_ok", "invoice_end") or inv.get("installer_ok"):
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Счёт №{inv.get('invoice_number', '?')} уже в работе у монтажной группы.",
        )
        return
    await state.set_state(RpMontazhNaemSG.amount)
    await state.update_data(naem_invoice_id=invoice_id)
    num = inv.get("invoice_number", "?")
    await cb.message.answer(  # type: ignore[union-attr]
        f"👥 <b>Счёт №{num} — Наёмная монтажная группа 2️⃣</b>\n\n"
        f"Введите <b>согласованную сумму ЗП монтажа</b> (₽):",
    )


async def _finalize_naem(
    db: Database, integrations: IntegrationHub, invoice_id: int,
    agreed: int, target_msg: Message, base: int | None = None,
    actor_id: int | None = None,
) -> None:
    """Зафиксировать наёмный счёт: согласованная сумма + метка 2️⃣ + переезд в «В работе».

    base — сумма, которую ввёл РП ДО надбавки +10% (для карточки ГД). None → =agreed.
    agreed — сумма ЗП ТЕКУЩЕЙ (наёмной) группы; в БД уходит объединённая (см. ниже).

    ⛔ Фикс owner 25.07 — объединение платежей (та же механика, что в _finalize_zp_edit,
    ТЗ owner 15.07): если по счёту ЗП монтажа уже выплачена ПРОШЛОЙ группе (DR =
    cost_montazh > 0), то Согласовано = DR + X, montazh_paid_prev = DR. Раньше этот путь
    писал montazh_agreed_amount = X и НЕ трогал montazh_paid_prev → вторая наёмная группа
    затирала первую и в BS попадала ЗП только последней (инцидент 25.07, сч. 41 Раушская:
    32 100 вместо 64 200 за две группы). Правило owner: «Выплачено ВСЕГДА ≥ Согласовано».
    Повторная запись НЕ накапливает: Согласовано всегда выводится заново из DR (как в
    _finalize_zp_edit).

    ⛔ Фикс owner 29.07 — база аванса. Раньше montazh_adv_prev здесь НЕ трогали
    («живые авансы текущей группы должны вычитаться из остатка»), но при DR > 0
    ТЕКУЩЕЙ группы ещё нет: всё, что привязано к счёту сейчас, — аванс ПРОШЛОЙ,
    и он уже внутри paid_prev (через DR). Без снимка _advance_raw_cur
    (installer_new.py:2293) приписывал его новой группе как свой, и
    _zp_remainder_for_invoice ужимал её остаток ровно на его размер — новая
    группа недополучала (функтест: остаток 15 000 вместо 40 000 при чужом
    авансе 25 000). Аванс не суммируется с ЗП монтаж, он её закрывает
    [[feedback_installer_advance_closes_zp_not_added]] → засчитывать дважды нельзя.
    Логика симметрична парному _finalize_regroup (ниже, merged-ветка).
    DR = 0 → поле как было: аванс принадлежит текущей группе, отнимать нечего.
    """
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await target_msg.answer("❌ Счёт не найден.")
        return
    # Идемпотентность: уже прошедший «assigned» счёт не сбрасываем (защита от старой кнопки)
    stage = inv.get("montazh_stage") or ""
    if stage in ("in_work", "razmery_ok", "invoice_ok", "invoice_end") or inv.get("installer_ok"):
        await target_msg.answer(
            f"⚠️ Счёт №{inv.get('invoice_number', '?')} уже в работе у монтажной группы.",
        )
        return
    dr = float(inv.get("cost_montazh") or 0)
    _merged = dr > 0.001
    agreed_total = int(round(dr + agreed))
    # База аванса — калька _finalize_regroup (merged-ветка): снимаем всё, что
    # привязано к счёту сейчас, иначе аванс прошлой группы уйдёт новой как её
    # собственный (см. docstring). DR = 0 → поле не трогаем.
    _adv_prev_col = (
        await db.get_installer_advance_for_invoice(invoice_id) if _merged
        else float(inv.get("montazh_adv_prev") or 0)
    )
    from datetime import datetime
    _now = datetime.now().isoformat()
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, montazh_base_amount = ?, "
        "montazh_paid_prev = ?, montazh_adv_prev = ?, edo_task_id = 2, assigned_to = NULL, "
        "montazh_stage = 'assigned', montazh_assigned_at = ?, updated_at = ? WHERE id = ?",
        (agreed_total, base if base is not None else agreed, dr, _adv_prev_col,
         _now, _now, invoice_id),
    )
    await db.conn.commit()
    try:
        await db.audit(
            actor_id=actor_id, action="rp_montazh_naem_amount_set",
            entity="invoice", entity_id=str(invoice_id),
            payload={"amount": agreed, "agreed": agreed_total, "paid_prev": dr,
                     "adv_prev": _adv_prev_col},
        )
    except Exception:
        log.debug("naem: audit failed inv=%s", invoice_id, exc_info=True)
    if integrations:
        await integrations.sync_invoice_row(invoice_id)
    num = inv.get("invoice_number", "?")
    lines = [
        f"✅ Счёт №{num} закреплён за <b>Наёмной монтажной группой</b> 2️⃣",
        f"💰 Согласованная сумма ЗП монтажа: <b>{agreed:,.0f}₽</b>",
    ]
    if dr > 0:
        lines.append(
            f"🔗 С учётом выплаченного прошлой группе ({dr:,.0f}₽): "
            f"<b>{agreed_total:,.0f}₽</b>"
        )
    lines.append("")
    lines.append(
        "Перенесён в «Счета в работе». Когда монтаж выполнен — нажмите «✅ Монтаж ОК»."
    )
    await target_msg.answer("\n".join(lines))
    # ТЗ owner 16.07: после назначения — карточка «💰 ЗП монтаж» (сумма уже введена
    # РП → карточка её отражает, кнопка = исправить).
    try:
        await _send_montazh_zp_card(db, target_msg, invoice_id)
    except Exception:
        log.warning("naem: montazh zp card failed inv=%s", invoice_id, exc_info=True)


@router.message(RpMontazhNaemSG.amount)
async def rp_montazh_naem_amount(
    message: Message, state: FSMContext, db: Database, integrations: IntegrationHub,
) -> None:
    """РП ввёл согласованную сумму ЗП монтажа для наёмной группы."""
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", "")
    try:
        amount = int(float(text))
    except (ValueError, TypeError):
        await message.answer("❌ Введите число:")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0:")
        return

    data = await state.get_data()
    invoice_id = data.get("naem_invoice_id")
    if not invoice_id:
        await state.clear()
        await message.answer("❌ Счёт не найден, начните заново.")
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await state.clear()
        await message.answer("❌ Счёт не найден.")
        return

    # Owner 22.08: надбавку +10% к сумме РП НЕ прибавляем — он вводит её уже с
    # учётом. Сумма пишется КАК ВВЕДЕНА, без округления до тысячи (ровно так же,
    # как до этой правки шёл кредит). Надбавка у МОНТАЖНИКА не тронута — она живёт
    # в installer_new._calc_est_montazh, owner подтвердил её 22.08 по Лобне.
    await state.clear()
    agreed = amount
    await _finalize_naem(
        db, integrations, invoice_id, agreed, message, base=amount,
        actor_id=message.from_user.id,
    )


@router.callback_query(F.data.startswith("rp_naem_bonus:"))
async def rp_montazh_naem_bonus(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """РП выбрал, прибавлять ли 10% к согласованной сумме (только б/н)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    try:
        _, choice, raw_id, raw_amount = cb.data.split(":")  # type: ignore[union-attr]
        invoice_id = int(raw_id)
        amount = int(raw_amount)
    except (ValueError, AttributeError):
        await cb.message.answer("❌ Ошибка данных, начните заново.")  # type: ignore[union-attr]
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Owner 22.08: надбавки к сумме РП нет — старая кнопка «+10%? Да» (сообщения
    # до 17.07) больше не может её начислить, обе ветки дают введённое число.
    agreed = amount
    await _finalize_naem(
        db, integrations, invoice_id, agreed, cb.message,  # type: ignore[arg-type]
        actor_id=cb.from_user.id if cb.from_user else None,
    )


# ---------------------------------------------------------------------------
# Карточка «💰 ЗП монтаж» после назначения группы + «✏️ Внести сумму ЗП монтаж»
# (ТЗ owner 16.07, переназначение монтажа часть 2). Показывается РП ВСЕГДА после
# назначения (обе ветки: 1️⃣ Игорь и 2️⃣ наёмная; в regroup НЕ показываем — там РП
# сумму только что ввёл сам). Запись кнопки — механика merge 15.07:
# Согласовано = cost_montazh (DR, выплачено прошлым) + X, montazh_paid_prev = DR →
# заявка ЗП потом уйдёт на доплату X (naem_ok/_zp_remainder_for_invoice вычитают
# paid_prev), на листе BJ = X, после выплаты BS = X + DR (paid_prev-нога в
# sheets._invoice_cells). В лист напрямую не пишем — только sync_invoice_row.
# ---------------------------------------------------------------------------


async def _build_montazh_zp_card(
    db: Database, invoice_id: int,
) -> tuple[str, Any] | None:
    """Текст + клавиатура карточки «💰 ЗП монтаж» (используется и в td.py при
    отклонении ГД — РП получает карточку для повторного внесения суммы)."""
    inv = await db.get_invoice(invoice_id)
    if not inv:
        return None
    # Owner 22.08: сумму ЗП монтажа РП вводит ТОЛЬКО под наёмную группу (метка
    # edo_task_id=2). Гард стоит в СБОРЩИКЕ, а не у вызывающих: карточку шлют три
    # места (наёмная ветка, назначение штатному, отклонение ГД в td.py) — гейт в
    # одной точке закрывает все, включая путь через td.py, который сейчас трогать
    # нельзя (в нём лежит непродеплоенный патч детектора зеркала ОП).
    if inv.get("edo_task_id") != 2:
        return None
    from ..utils import format_card_section
    from .installer_new import _calc_est_montazh  # lazy: circular import
    est = float(_calc_est_montazh(inv) or 0)
    dr = float(inv.get("cost_montazh") or 0)  # «Выплачено» = только DR (решение owner №2)

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    items: list[tuple[str, str]] = [
        ("Счёт", f"№{inv.get('invoice_number', '?')}"),
        ("Адрес", str(inv.get("object_address") or "—")),
        ("Расчётная ЗП", _f(est)),
        ("Выплачено", _f(dr)),
        ("Остаток расчёта", _f(max(est - dr, 0.0))),
    ]
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if agreed > 0:  # наёмная ветка: сумма уже введена РП — показать, кнопка = исправить
        items.append(("Согласовано", _f(agreed)))
    card_text = format_card_section(
        emoji="💰", title="ЗП монтаж", items=items, width=27, compact=True,
    )
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Внести сумму ЗП монтаж", callback_data=f"rp_montazh:zp_edit:{invoice_id}")
    b.adjust(1)
    return card_text, b.as_markup()


async def _send_montazh_zp_card(
    db: Database, target_msg: Message, invoice_id: int,
) -> None:
    """Карточка РП «💰 ЗП монтаж»: Расчётная ЗП (авто-смета) / Выплачено (DR) /
    Остаток расчёта + кнопка «✏️ Внести сумму ЗП монтаж»."""
    built = await _build_montazh_zp_card(db, invoice_id)
    if not built:
        return
    card_text, markup = built
    await target_msg.answer(card_text, reply_markup=markup)


# Заявка ЗП уже в работе/выплачена → «Согласовано» менять поздно
# [[feedback_fsm_old_buttons_trap]] — кнопка живёт в чате вечно.
_ZP_EDIT_BLOCKED_STATUSES = ("requested", "approved", "payment_sent", "confirmed")


@router.callback_query(F.data.startswith("rp_montazh:zp_edit:"))
async def rp_montazh_zp_edit(
    cb: CallbackQuery, db: Database, state: FSMContext,
) -> None:
    """РП нажал «✏️ Внести сумму ЗП монтаж» — вход в FSM ввода суммы."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.answer("❌ Счёт не найден", show_alert=True)
        return
    # Кнопка из старого сообщения живёт в чате вечно [[feedback_fsm_old_buttons_trap]]:
    # по штатным счетам карточку больше не шлём, но выданные до 22.08 надо отбить —
    # catch-all для callback-ов в проекте нет, молча висеть кнопка не должна.
    if inv.get("edo_task_id") != 2:
        await cb.answer(
            "⚠️ Сумму ЗП монтажа вводит только наёмная монтажная группа. "
            "По нашей группе сумму согласует монтажник.",
            show_alert=True,
        )
        return
    if (inv.get("zp_installer_status") or "not_requested") in _ZP_EDIT_BLOCKED_STATUSES:
        await cb.answer(
            "⚠️ По счёту уже есть заявка ЗП (или она выплачена) — сумму менять поздно.",
            show_alert=True,
        )
        return
    await cb.answer()
    await state.set_state(RpMontazhZpSG.amount)
    await state.update_data(zp_edit_invoice_id=invoice_id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 Введите сумму ЗП монтажа для счёта №{inv.get('invoice_number', '?')} (в рублях):",
    )


@router.message(RpMontazhZpSG.amount)
async def rp_montazh_zp_amount(
    message: Message, state: FSMContext, db: Database, integrations: IntegrationHub,
) -> None:
    """РП ввёл сумму ЗП монтажа (флоу «✏️ Внести сумму ЗП монтаж»)."""
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", "")
    try:
        amount = int(float(text))
    except (ValueError, TypeError):
        await message.answer("❌ Введите число:")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0:")
        return

    data = await state.get_data()
    invoice_id = data.get("zp_edit_invoice_id")
    if not invoice_id:
        await state.clear()
        await message.answer("❌ Счёт не найден, начните заново.")
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await state.clear()
        await message.answer("❌ Счёт не найден.")
        return

    # Owner 22.08: надбавку +10% к сумме РП не прибавляем (см. rp_montazh_naem_amount).
    await state.clear()
    x = amount
    await _finalize_zp_edit(
        db, integrations, invoice_id, x, message, message.from_user.id, base=amount,
    )


@router.callback_query(F.data.startswith("rp_zped_bonus:"))
async def rp_montazh_zp_bonus(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """РП выбрал, прибавлять ли 10% (флоу «✏️ Внести сумму ЗП монтаж», только б/н)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    try:
        _, choice, raw_id, raw_amount = cb.data.split(":")  # type: ignore[union-attr]
        invoice_id = int(raw_id)
        amount = int(raw_amount)
    except (ValueError, AttributeError):
        await cb.message.answer("❌ Ошибка данных, начните заново.")  # type: ignore[union-attr]
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    x = amount  # owner 22.08: надбавку к сумме РП не прибавляем
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # одноразовость кнопки
    except Exception:
        pass
    await _finalize_zp_edit(
        db, integrations, invoice_id, x, cb.message,  # type: ignore[arg-type]
        cb.from_user.id if cb.from_user else None,
    )


async def _finalize_zp_edit(
    db: Database, integrations: IntegrationHub, invoice_id: int,
    x: int, target_msg: Message, actor_id: int | None, base: int | None = None,
) -> None:
    """Запись суммы РП: Согласовано = DR + X, montazh_paid_prev = DR (механика merge
    15.07). Перезапуск кнопки легитимен (исправление): значения выводятся заново из
    cost_montazh, повторная запись не накапливает. montazh_adv_prev НЕ трогаем —
    живые авансы текущей группы должны вычитаться из остатка как раньше."""
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await target_msg.answer("❌ Счёт не найден.")
        return
    # Гард повторной проверки: пока РП вводил сумму, заявка могла уйти в работу.
    if (inv.get("zp_installer_status") or "not_requested") in _ZP_EDIT_BLOCKED_STATUSES:
        await target_msg.answer("⚠️ По счёту уже есть заявка ЗП — сумма не записана.")
        return
    # Гонка: пока РП вводил сумму, счёт могли перевести на штатную группу.
    if inv.get("edo_task_id") != 2:
        await target_msg.answer(
            "⚠️ Счёт не за наёмной монтажной группой — сумма не записана.",
        )
        return
    dr = float(inv.get("cost_montazh") or 0)
    agreed = int(round(dr + x))
    from datetime import datetime
    _now = datetime.now().isoformat()
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, montazh_base_amount = ?, "
        "montazh_paid_prev = ?, updated_at = ? WHERE id = ?",
        (agreed, base if base is not None else x, dr, _now, invoice_id),
    )
    await db.conn.commit()
    try:
        await db.audit(
            actor_id=actor_id, action="rp_montazh_zp_amount_set",
            entity="invoice", entity_id=str(invoice_id),
            payload={"amount": x, "agreed": agreed, "paid_prev": dr},
        )
    except Exception:
        log.debug("zp_edit: audit failed inv=%s", invoice_id, exc_info=True)
    if integrations:
        await integrations.sync_invoice_row(invoice_id)
    num = inv.get("invoice_number", "?")
    lines = [f"✅ ЗП монтаж по счёту №{num}: <b>{x:,.0f}₽</b>"]
    if dr > 0:
        lines.append(
            f"Согласовано с учётом выплаченного ({dr:,.0f}₽): <b>{agreed:,.0f}₽</b>"
        )
    await target_msg.answer("\n".join(lines))


# ---------------------------------------------------------------------------
# «🔁 Изменить Монтажников» (ТЗ owner 15.07) — смена монт. группы на счёте,
# который уже отдан в монтаж. РП выбирает группу 1️⃣/2️⃣ и ОБЯЗАТЕЛЬНО вводит новую
# согласованную сумму ЗП монтажа.
#
# В лист напрямую не пишем: у montazh_agreed_amount нет своей колонки, а BJ/BS/BE/CB
# пересобирает sync_invoice_row [[feedback_bs_immutable]], [[feedback_db_first_no_direct_sheet_writes]].
# Отличие от первичного назначения (rp_montazh:assign): гарды «уже назначено» и
# идемпотентности обойдены намеренно — в этом весь смысл механизма.
# _REGROUP_INFLIGHT — анти-двойной-клик [[feedback_money_confirm_idempotent_gate]].
# ---------------------------------------------------------------------------
_REGROUP_INFLIGHT: set[tuple[int, int]] = set()

_ZP_INST_LABELS = {
    "not_requested": "⏳ Не запрошен",
    "not_applicable": "— не применимо",
    "requested": "📤 Отправлен ГД",
    "approved": "✅ Одобрен ГД",
    "payment_sent": "💸 Платёжка отправлена",
    "confirmed": "✅ Получено монтажником",
}


def _grp_label(inv: dict[str, Any]) -> str:
    """Метка текущей монтажной группы счёта (edo_task_id=2 — наёмная)."""
    return "2️⃣ Наёмная" if inv.get("edo_task_id") == 2 else "1️⃣ Наша"


def _montazh_payout_done(inv: dict[str, Any]) -> bool:
    """Была ли по счёту РЕАЛЬНАЯ выплата ЗП монтажа — т.е. есть что объединять.

    Зачтённый аванс сам по себе объединение не запускает: он часть ЗП ТЕКУЩЕЙ группы,
    а не закрытый платёж прошлой (owner 15.07: «если по данному материнскому счёту ЗП
    монтаж уже было выплачено — прибавить две эти суммы»). Иначе аванс Игоря попал бы
    в «Согласовано» вторым слагаемым и раздул бы его.
    """
    return bool(
        (inv.get("zp_installer_status") or "not_requested") in ("payment_sent", "confirmed")
        or float(inv.get("montazh_fact_op") or 0) > 0
        or float(inv.get("montazh_paid_prev") or 0) > 0
    )


async def _montazh_money_state(db: Database, inv: dict[str, Any]) -> tuple[float, float]:
    """(Выплачено, зачтённый аванс CG) по ЗП монтажа — 1-в-1 с листом (sheets.py:1186-1219)
    плюс выплаченное ПРОШЛЫМ группам, которого лист знать не может.

    Источники «Выплачено»: AN «Монтаж Факт» из ОП, зачтённый аванс (CG, ×1.10 для б/н)
    и выплата ботом (только payment_sent/confirmed — approved деньгами ещё не является).
    zp_installer_remainder=1 → бот платил ОСТАТОК, поэтому аванс и выплата складываются;
    иначе берётся максимум.

    Аванс берём только за ТЕКУЩУЮ группу (_advance_raw_cur): аванс прошлой уже сидит
    внутри montazh_paid_prev, иначе он посчитался бы дважды.

    montazh_paid_prev (объединение платежей, owner 15.07) — ОТДЕЛЬНАЯ нога: выплаты
    прошлых групп. Лист видит их только через AN, а AN — накопитель, который заполняют
    люди в «Импорт ОП» с лагом; пока он пуст, это поле — единственный след старой выплаты.
    Без него повторное объединение посчитало бы «Выплачено» = 0 и потеряло бы её.
    AN накапливает ВСЕ ноги (решение owner), поэтому с ними не суммируется, а конкурирует
    по максимуму. paid_prev = 0 (обычный счёт) → формула ровно прежняя, лист 1-в-1.
    """
    adv_cg = _advance_cg_amount(await _advance_raw_cur(db, inv), inv)
    an = float(inv.get("montazh_fact_op") or 0)
    bot_paid = (
        float(inv.get("zp_installer_amount") or 0)
        if inv.get("zp_installer_status") in ("payment_sent", "confirmed")
        else 0.0
    )
    if inv.get("zp_installer_remainder") and bot_paid > 0:
        leg = adv_cg + bot_paid
    else:
        leg = max(adv_cg, bot_paid)
    paid_prev = float(inv.get("montazh_paid_prev") or 0)
    # Канал DR «Затр. Монтаж» (owner 01.08) — канон sheets.py:1274. Здесь он важнее,
    # чем в остальных трёх местах: величина уходит НЕ только в показ (строка
    # «💰 Выплачено» в предупреждении), но и в ЗАПИСЬ — rp_montazh_regroup_merge
    # передаёт её в _finalize_regroup(paid_prev=...), а тот пишет
    # montazh_agreed_amount = paid_prev + новая сумма. Без DR «Согласовано по счёту»
    # теряло транши, ушедшие через затраты (класс инцидента 26331-1НПН).
    # Соседние ветки того же механизма DR уже берут напрямую: _finalize_naem
    # (agreed_total = dr + agreed) и _finalize_zp_edit — правка выравнивает с ними.
    # ⚠️ Гард _montazh_payout_done (выше) про cost_montazh по-прежнему НЕ знает:
    # у счёта, где деньги шли ТОЛЬКО через DR, развилка «объединить» не предложится.
    # Расширение гарда owner не заказывал — вынесено вопросом.
    dr = float(inv.get("cost_montazh") or 0)
    return max(an, paid_prev + leg, dr), adv_cg


async def _regroup_picker(target_msg: Message, inv: dict[str, Any]) -> None:
    """Пикер новой монт. группы — разметка как при первичном назначении."""
    invoice_id = int(inv["id"])
    b = InlineKeyboardBuilder()
    b.button(text="1️⃣ Наша монтажная группа", callback_data=f"rp_montazh:regrp_igor:{invoice_id}")
    b.button(text="2️⃣ Наёмная монтажная группа", callback_data=f"rp_montazh:regrp_naem:{invoice_id}")
    b.button(text="❌ Отмена", callback_data="rp_montazh:work_refresh")
    b.adjust(1)
    await target_msg.answer(
        f"🔁 <b>Счёт №{inv.get('invoice_number', '?')} — смена монтажной группы</b>\n\n"
        f"Сейчас: {_grp_label(inv)}\n\n"
        f"Кто выполняет монтаж?",
        reply_markup=b.as_markup(),
    )


async def _regroup_warn_if_money(db: Database, inv: dict[str, Any], target_msg: Message) -> bool:
    """Показать карточку-предупреждение, если по счёту двигались деньги. True — показана.

    Вызывается ДВАЖДЫ: на входе и повторно при выборе группы. Второй вызов не избыточен —
    деньги могли двинуться уже после отрисовки пикера (монтажник взял аванс, ГД провёл
    выплату), а инлайн-пикер живёт в чате вечно [[feedback_fsm_old_buttons_trap]].
    """
    invoice_id = int(inv["id"])
    paid, adv_cg = await _montazh_money_state(db, inv)
    zp_st = inv.get("zp_installer_status") or "not_requested"
    if paid <= 0.001 and adv_cg <= 0.001 and zp_st in ("not_requested", "not_applicable"):
        return False

    agreed = float(inv.get("montazh_agreed_amount") or 0)
    # У счёта одна ячейка под ЗП монтажа — две выплаты по одному счёту не представимы.
    # Раньше это был тупик («новой группе бот ЗП не начислит»); теперь ячейка
    # освобождается под доплату, а старая выплата уходит в montazh_paid_prev —
    # объединение платежей (owner 15.07). Предлагаем его ПОСЛЕ ввода новой суммы:
    # тут ещё нечего складывать.
    merge_line = (
        "\n🔗 ЗП по счёту уже выплачена — после ввода новой суммы предложу "
        "<b>объединить платежи</b>.\n"
        if _montazh_payout_done(inv) else ""
    )
    b = InlineKeyboardBuilder()
    b.button(text="⚠️ Всё равно менять", callback_data=f"rp_montazh:regrp_go:{invoice_id}")
    b.button(text="❌ Отмена", callback_data="rp_montazh:work_refresh")
    b.adjust(1)
    await target_msg.answer(
        f"⚠️ <b>Счёт №{inv.get('invoice_number', '?')} — по счёту уже двигались деньги</b>\n\n"
        f"👥 Текущая группа: {_grp_label(inv)}\n"
        f"📊 Согласованная ЗП монтаж: <b>{agreed:,.0f}₽</b>\n"
        f"💰 Выплачено: <b>{paid:,.0f}₽</b>\n"
        # Аванс — ЧАСТЬ «Выплачено» (paid = max(AN, CG, бот) ≥ CG), не добавка к нему:
        # две строки подряд без «в т.ч.» читаются как сумма.
        f"🏦 в т.ч. зачтённый аванс: <b>{adv_cg:,.0f}₽</b>\n"
        f"💵 Статус ЗП: {_ZP_INST_LABELS.get(zp_st, zp_st)}\n"
        f"{merge_line}\n"
        f"Смена группы <b>не отменит</b> уже выплаченное. Всё равно менять?",
        reply_markup=b.as_markup(),
    )
    return True


@router.callback_query(F.data.regexp(r"^rp_montazh:regroup:\d+$"))
async def rp_montazh_regroup(cb: CallbackQuery, db: Database) -> None:
    """Старт смены монт. группы. Счёт с движением денег — только через предупреждение
    (решение owner 15.07: не блокировать, но и не менять молча)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if not await _regroup_warn_if_money(db, inv, cb.message):  # type: ignore[arg-type]
        await _regroup_picker(cb.message, inv)  # type: ignore[arg-type]


@router.callback_query(F.data.regexp(r"^rp_montazh:regrp_go:\d+$"))
async def rp_montazh_regroup_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """РП подтвердил смену на «денежном» счёте → пикер группы."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Отметка «предупреждение принято по ЭТОМУ счёту» — иначе повторная проверка в
    # regroup_pick снова покажет карточку и флоу зациклится.
    await state.update_data(regroup_ack_invoice_id=invoice_id)
    await _regroup_picker(cb.message, inv)  # type: ignore[arg-type]


@router.callback_query(F.data.regexp(r"^rp_montazh:regrp_(igor|naem):\d+$"))
async def rp_montazh_regroup_pick(
    cb: CallbackQuery, state: FSMContext, db: Database, integrations: IntegrationHub,
) -> None:
    """Новая группа выбрана → сумму ЗП монтажа спрашиваем ТОЛЬКО под наёмную.

    Owner 23.08: на НАШУ группу сумму называет сам монтажник — как при первичном
    назначении, где группа 1️⃣ ввода не имеет вовсе. Прежняя редакция (ТЗ owner 15.07)
    спрашивала для ОБЕИХ групп; после гейта 22.08 это стало единственным местом, где
    РП мог ввести сумму мимо наёмников.

    Выплаченное прошлой группе при этом НЕ теряется и НЕ спрашивается развилкой:
    owner 23.08 — «сумма Игоря и сумма наёмников суммируется». Механика та же, что у
    кнопки «🔗 Объединить»: выплаченное уходит в montazh_paid_prev, а монтажник своей
    суммой его ДОПОЛНЯЕТ (installer_new.installer_price_confirm прибавляет paid_prev).
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    _, kind, raw_id = cb.data.split(":")  # type: ignore[union-attr]
    invoice_id = int(raw_id)
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Пикер мог быть отрисован, когда счёт был «чистым», а деньги двинулись уже после —
    # тогда РП обязан увидеть предупреждение (решение owner 15.07), а не проскочить мимо.
    # Если РП уже подтвердил его по этому счёту (regrp_go) — не переспрашиваем.
    acked = (await state.get_data()).get("regroup_ack_invoice_id") == invoice_id
    if not acked and await _regroup_warn_if_money(db, inv, cb.message):  # type: ignore[arg-type]
        return
    group = "igor" if kind == "regrp_igor" else "naem"
    if group == "igor":
        # Гард от двойного клика обязателен: ветка стала ПИШУЩЕЙ, а _finalize_regroup
        # намеренно без идемпотентности — повторный клик переиграл бы смену группы.
        key = (cb.from_user.id if cb.from_user else 0, invoice_id)
        if key in _REGROUP_INFLIGHT:
            await cb.answer("Уже обрабатываю, секунду…")
            return
        _REGROUP_INFLIGHT.add(key)
        try:
            # Пара гардов — 1:1 с _maybe_offer_merge, а не своя формула: зачтённый аванс
            # выплатой НЕ считается (_montazh_payout_done), иначе аванс текущей группы
            # попал бы в «Согласовано» вторым слагаемым и раздул бы его.
            paid = 0.0
            if _montazh_payout_done(inv):
                paid, _adv = await _montazh_money_state(db, inv)
            await state.clear()
            await _finalize_regroup(
                db, integrations, invoice_id, group, 0, cb.message,  # type: ignore[arg-type]
                paid_prev=paid, base=0,
            )
        finally:
            _REGROUP_INFLIGHT.discard(key)
        return
    await state.set_state(RpMontazhRegroupSG.amount)
    await state.update_data(regroup_invoice_id=invoice_id, regroup_group=group)
    grp_name = "1️⃣ Наша монтажная группа" if group == "igor" else "2️⃣ Наёмная монтажная группа"
    cur = float(inv.get("montazh_agreed_amount") or 0)
    cur_line = f"Сейчас согласовано: {cur:,.0f}₽\n\n" if cur else ""
    await cb.message.answer(  # type: ignore[union-attr]
        f"👥 <b>Счёт №{inv.get('invoice_number', '?')} → {grp_name}</b>\n\n"
        f"{cur_line}"
        f"Введите <b>согласованную сумму ЗП монтажа</b> (₽):",
    )


@router.message(RpMontazhRegroupSG.amount)
async def rp_montazh_regroup_amount(
    message: Message, state: FSMContext, db: Database, integrations: IntegrationHub,
) -> None:
    """РП ввёл новую согласованную сумму ЗП монтажа при смене группы."""
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", "")
    try:
        amount = int(float(text))
    except (ValueError, TypeError):
        await message.answer("❌ Введите число:")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0:")
        return

    data = await state.get_data()
    invoice_id = data.get("regroup_invoice_id")
    group = data.get("regroup_group")
    if not invoice_id or not group:
        await state.clear()
        await message.answer("❌ Счёт не найден, начните заново.")
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await state.clear()
        await message.answer("❌ Счёт не найден.")
        return

    await state.clear()
    # Owner 23.08: под НАШУ группу сумму вводит монтажник, а не РП. Сюда можно попасть
    # только из состояния, поставленного ДО деплоя (кнопка старого сообщения) —
    # записывать ввод вопреки правилу нельзя [[feedback_fsm_old_buttons_trap]].
    if str(group) == "igor":
        paid = 0.0
        if _montazh_payout_done(inv):
            paid, _adv = await _montazh_money_state(db, inv)
        await message.answer(
            "ℹ️ Сумму ЗП монтажа для нашей монтажной группы называет сам монтажник — "
            "ваш ввод не записан.",
        )
        await _finalize_regroup(
            db, integrations, int(invoice_id), "igor", 0, message,
            paid_prev=paid, base=0,
        )
        return
    # Owner 22.08: надбавку +10% к сумме РП не прибавляем (см. rp_montazh_naem_amount).
    agreed = amount
    if await _maybe_offer_merge(
        db, int(invoice_id), str(group), agreed, message, base=amount,
    ):
        return
    await _finalize_regroup(
        db, integrations, int(invoice_id), str(group), agreed, message, base=amount,
    )


@router.callback_query(F.data.startswith("rp_regrp_bonus:"))
async def rp_montazh_regroup_bonus(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """РП выбрал, прибавлять ли 10% к новой согласованной сумме (только б/н)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    try:
        _, choice, raw_id, raw_amount, group = cb.data.split(":")  # type: ignore[union-attr]
        invoice_id = int(raw_id)
        amount = int(raw_amount)
    except (ValueError, AttributeError):
        await cb.answer()
        await cb.message.answer("❌ Ошибка данных, начните заново.")  # type: ignore[union-attr]
        return

    key = (cb.from_user.id, invoice_id)
    if key in _REGROUP_INFLIGHT:
        await cb.answer("Уже обрабатываю, секунду…")
        return
    _REGROUP_INFLIGHT.add(key)
    try:
        await cb.answer()
        inv = await db.get_invoice(invoice_id)
        if not inv:
            await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
            return
        # Кнопка одноразовая: _finalize_regroup намеренно без гарда идемпотентности,
        # поэтому повторный клик по старому сообщению переиграл бы всю смену группы
        # (сброс installer_ok/стадии на старую сумму) [[feedback_fsm_old_buttons_trap]].
        try:
            await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
        except Exception:
            pass
        agreed = amount  # owner 22.08: надбавку к сумме РП не прибавляем
        if await _maybe_offer_merge(db, invoice_id, group, agreed, cb.message):  # type: ignore[arg-type]
            return
        await _finalize_regroup(
            db, integrations, invoice_id, group, agreed, cb.message,  # type: ignore[arg-type]
        )
    finally:
        _REGROUP_INFLIGHT.discard(key)


async def _maybe_offer_merge(
    db: Database, invoice_id: int, group: str, agreed_new: int, target_msg: Message,
    base: int | None = None,
) -> bool:
    """Предложить объединить выплаченную ЗП монтаж с новой суммой. True — предложено.

    Owner 15.07: «бот должен предложить объединить платежи зп монтаж уже выплаченного и
    нового» — это РАЗВИЛКА КНОПКОЙ, молча складывать нельзя. Согласовано = выплаченное +
    новое (90 000 + 130 000 = 220 000), новой группе бот начислит доплату 130 000.
    Зовётся после ввода суммы (и вопроса о 10%) — до этого момента складывать нечего.
    """
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await target_msg.answer("❌ Счёт не найден.")
        return False
    if not _montazh_payout_done(inv):
        return False
    paid, _adv = await _montazh_money_state(db, inv)
    if paid <= 0.001:
        return False

    total = paid + agreed_new
    b = InlineKeyboardBuilder()
    b.button(
        text=f"🔗 Объединить — {total:,.0f}₽",
        callback_data=(
            f"rp_regrp_merge:{invoice_id}:{agreed_new}:{group}:"
            f"{base if base is not None else agreed_new}"
        ),
    )
    # Отмена здесь = отказ от ВСЕЙ смены группы, а не только от объединения: на
    # выплаченном счёте «сменить, но не объединять» — запрещённый исход (решение owner
    # №1: суммы обязательно складываются). РП должен видеть, что теряет ввод.
    b.button(text="❌ Отмена — не менять группу", callback_data="rp_montazh:work_refresh")
    b.adjust(1)
    await target_msg.answer(
        f"🔗 <b>Счёт №{inv.get('invoice_number', '?')} — объединить платежи ЗП монтаж?</b>\n\n"
        f"💰 Уже выплачено: <b>{paid:,.0f}₽</b>\n"
        f"➕ Новая группа: <b>{agreed_new:,.0f}₽</b>\n"
        f"━ Согласовано по счёту: <b>{total:,.0f}₽</b>\n\n"
        f"Новой группе бот начислит <b>доплату {agreed_new:,.0f}₽</b>.\n"
        f"Выплаченное прошлой группе остаётся на счёте.\n\n"
        f"<i>Отмена оставит счёт как есть — группа не сменится, сумму нужно будет "
        f"ввести заново.</i>",
        reply_markup=b.as_markup(),
    )
    return True


@router.callback_query(F.data.startswith("rp_regrp_merge:"))
async def rp_montazh_regroup_merge(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """РП подтвердил объединение платежей ЗП монтаж (owner 15.07)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    try:
        # rp_regrp_merge:{id}:{agreed_new}:{group}[:{base}] — база опциональна
        # (старые кнопки без неё → base=None, карточка ГД разбивку не покажет).
        parts = (cb.data or "").split(":")
        _, raw_id, raw_amount, group = parts[:4]
        invoice_id = int(raw_id)
        agreed_new = int(raw_amount)
        base_new = int(parts[4]) if len(parts) > 4 else None
    except (ValueError, AttributeError):
        await cb.answer()
        await cb.message.answer("❌ Ошибка данных, начните заново.")  # type: ignore[union-attr]
        return

    key = (cb.from_user.id, invoice_id)
    if key in _REGROUP_INFLIGHT:
        await cb.answer("Уже обрабатываю, секунду…")
        return
    _REGROUP_INFLIGHT.add(key)
    try:
        await cb.answer()
        inv = await db.get_invoice(invoice_id)
        if not inv:
            await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
            return
        # Кнопка одноразовая — калька rp_regrp_bonus [[feedback_fsm_old_buttons_trap]].
        try:
            await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
        except Exception:
            pass
        # Выплаченное пересчитываем ЗАНОВО, а не берём из callback_data: карточка живёт
        # в чате вечно, деньги могли двинуться после её отрисовки — иначе объединим по
        # устаревшей сумме и запишем в Согласовано неправду.
        paid, _adv = await _montazh_money_state(db, inv)
        await _finalize_regroup(
            db, integrations, invoice_id, group, agreed_new, cb.message,  # type: ignore[arg-type]
            paid_prev=paid, base=base_new,
        )
    finally:
        _REGROUP_INFLIGHT.discard(key)


async def _finalize_regroup(
    db: Database, integrations: IntegrationHub, invoice_id: int, group: str,
    agreed: int, target_msg: Message, paid_prev: float = 0.0, base: int | None = None,
) -> None:
    """Записать новую монт. группу + согласованную сумму. Гардов «уже назначено»/
    идемпотентности здесь НЕТ намеренно (ср. _finalize_naem) — счёт в работе меняют осознанно.

    Сумму пишем БЕЗУСЛОВНО: `or`-паттерн вида `agreed or _calc_est_montazh(inv)` вернул бы
    старую сумму и молча съел ввод РП. installer_ok сбрасываем — исполнитель сменился,
    старое подтверждение недействительно [[feedback_montazh_stage_in_work_requires_installer_ok]].

    paid_prev > 0 — ОБЪЕДИНЕНИЕ ПЛАТЕЖЕЙ (owner 15.07): Согласовано = выплаченное прошлым
    группам + новая сумма (`agreed` здесь — доля ТОЛЬКО новой группы), ячейка ЗП счёта
    освобождается под доплату новой группе.
    """
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await target_msg.answer("❌ Счёт не найден.")
        return

    merged = paid_prev > 0.001
    agreed_total = int(round(paid_prev + agreed)) if merged else agreed

    installer_uid: int | None = None
    if group == "igor":
        installers = await db.find_users_by_role("installer")
        if not installers:
            await target_msg.answer("❌ Нет активных монтажников.")
            return
        installer_uid = installers[0].telegram_id

    from datetime import datetime
    _now = datetime.now().isoformat()
    # paid_prev при обычной смене НЕ обнуляем, а переносим как есть: объединение могло
    # пройти раньше, и его след — единственное, что помнит выплату прошлым группам.
    _paid_prev_col = paid_prev if merged else float(inv.get("montazh_paid_prev") or 0)
    # База аванса: всё, что привязано к счёту СЕЙЧАС, относится к прошлым группам —
    # оно уже внутри paid_prev. Новой группе засчитываем только её собственные авансы.
    _adv_prev_col = (
        await db.get_installer_advance_for_invoice(invoice_id) if merged
        else float(inv.get("montazh_adv_prev") or 0)
    )
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, montazh_base_amount = ?, "
        "montazh_paid_prev = ?, "
        "montazh_adv_prev = ?, assigned_to = ?, edo_task_id = ?, "
        "montazh_stage = 'assigned', installer_ok = 0, installer_ok_by = NULL, "
        "installer_ok_at = NULL, montazh_assigned_at = ?, updated_at = ? WHERE id = ?",
        (agreed_total, base if base is not None else agreed, _paid_prev_col,
         _adv_prev_col, installer_uid,
         None if group == "igor" else 2, _now, _now, invoice_id),
    )
    await db.conn.commit()

    # Открытый запрос ЗП относился к СТАРОЙ группе → снять задачу у ГД и сбросить статус
    # (решение owner 15.07), иначе ГД выплатит старую сумму старому исполнителю.
    # Калька gdzp_inst:no (td.py:863-865). Одобренную/выплаченную ЗП НЕ трогаем: деньги
    # двинулись, зачёты авансов применены и назад не откатываются — такой счёт РП меняет
    # под предупреждением, факт выплаты остаётся на счёте.
    zp_st_now = inv.get("zp_installer_status") or "not_requested"
    zp_withdrawn = zp_st_now == "requested"
    if zp_withdrawn:
        await db.set_invoice_zp_installer_status(invoice_id, "not_requested")
        await db.close_tasks_by_invoice(invoice_id, TaskType.ZP_INSTALLER)
    elif merged and zp_st_now in ("approved", "payment_sent", "confirmed"):
        # Объединение: ячейка ЗП на счёте ОДНА — освобождаем её под доплату новой группе.
        # Это осознанно перекрывает решение owner №2 от 15.07 («выплаченную ЗП не трогаем»)
        # для кейса объединения: факт старой выплаты не теряется, он ушёл в
        # montazh_paid_prev (+ AN/DR). amount=0 обязателен — сеттер stale-сумму не чистит,
        # иначе старые 90 000 остались бы в расчёте «Выплачено» как нога текущей группы.
        await db.set_invoice_zp_installer_status(invoice_id, "not_requested", amount=0)
        await db.close_tasks_by_invoice(invoice_id, TaskType.ZP_INSTALLER)

    if integrations:
        await integrations.sync_invoice_row(invoice_id)

    num = inv.get("invoice_number", "?")
    grp_txt = "1️⃣ Наша монтажная группа" if group == "igor" else "2️⃣ Наёмная монтажная группа"
    tail = (
        "Ожидает принятия монтажником («🔨 В Работу»)."
        if group == "igor"
        else "Когда монтаж выполнен — нажмите «✅ Монтаж ОК»."
    )
    zp_line = "🚫 Запрос ЗП у ГД снят — запросите заново после монтажа.\n" if zp_withdrawn else ""
    # Сумма РП есть только у наёмной ветки. Под нашу группу её называет монтажник,
    # поэтому печатать «новая 0₽ / к выплате 0₽» нельзя — это читалось бы как обнуление.
    if agreed <= 0:
        money_lines = (
            f"💰 Выплачено прошлой группе: <b>{paid_prev:,.0f}₽</b> — сохранено.\n"
            f"👉 Сумму назовёт монтажник; бот прибавит выплаченное к ней.\n"
            if merged else
            "👉 Сумму ЗП монтажа назовёт монтажник при приёме счёта.\n"
        )
    else:
        money_lines = (
            f"🔗 Платежи объединены: выплачено <b>{paid_prev:,.0f}₽</b> + новая "
            f"<b>{agreed:,.0f}₽</b>\n"
            f"💰 Согласовано по счёту: <b>{agreed_total:,.0f}₽</b>\n"
            f"👉 Новой группе к выплате: <b>{agreed:,.0f}₽</b>\n"
            if merged else
            f"💰 Согласованная сумма ЗП монтажа: <b>{agreed_total:,.0f}₽</b>\n"
        )
    await target_msg.answer(
        f"✅ Счёт №{num} — монтажная группа изменена\n"
        f"👥 Группа: <b>{grp_txt}</b>\n"
        f"{money_lines}"
        f"{zp_line}\n"
        f"{tail}",
    )


@router.callback_query(F.data.startswith("rp_montazh:attach:"))
async def rp_montazh_start_attach(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать сбор файлов для монтажника."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.set_state(RpMontazhAssignSG.attachments)
    await state.update_data(assign_invoice_id=invoice_id, attachments=[])
    await cb.message.answer(  # type: ignore[union-attr]
        "📎 Отправьте файлы (PDF, фото, видео) или текстовый комментарий.\n"
        "Можно несколько — я соберу всё.",
    )


@router.message(RpMontazhAssignSG.attachments, F.content_type.in_({"photo", "document", "video"}))
async def rp_montazh_attach_file(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Принять файл от РП."""
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"rp/{uid}")
    if att is None:
        return

    b = InlineKeyboardBuilder()
    b.button(
        text=f"✅ Отправить монтажнику ({count} вл.)",
        callback_data="rp_montazh:finish_attach",
    )
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await message.answer(
        f"📎 Принял. Файлов: {count}.{suffix} Отправьте ещё или нажмите кнопку.",
        reply_markup=b.as_markup(),
    )


@router.message(RpMontazhAssignSG.attachments, F.text)
async def rp_montazh_attach_text(message: Message, state: FSMContext) -> None:
    """Принять текстовый комментарий от РП."""
    if not message.text:
        return
    data = await state.get_data()
    attachments: list[dict] = data.get("attachments", [])
    attachments.append({
        "file_type": "text",
        "file_id": "",
        "caption": message.text,
    })
    await state.update_data(attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(
        text=f"✅ Отправить монтажнику ({len(attachments)} вл.)",
        callback_data="rp_montazh:finish_attach",
    )
    await message.answer(
        f"📎 Комментарий сохранён. Вложений: {len(attachments)}.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:finish_attach")
async def rp_montazh_finish_attach(cb: CallbackQuery, state: FSMContext, db: Database, integrations: IntegrationHub) -> None:
    """Отправить счёт монтажнику с вложениями."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    data = await state.get_data()
    invoice_id = data.get("assign_invoice_id")
    attachments = data.get("attachments", [])
    await state.clear()

    if not invoice_id:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    await _do_montazh_assign(cb, db, int(invoice_id), attachments, integrations)


@router.callback_query(F.data.startswith("rp_montazh:send_now:"))
async def rp_montazh_send_now(cb: CallbackQuery, db: Database, integrations: IntegrationHub) -> None:
    """Отправить счёт монтажнику без вложений."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _do_montazh_assign(cb, db, invoice_id, [], integrations)


async def _do_montazh_assign(
    cb: CallbackQuery, db: Database, invoice_id: int, attachments: list[dict],
    integrations: IntegrationHub | None = None,
) -> None:
    """Общая логика назначения счёта монтажнику."""
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    installers = await db.find_users_by_role("installer")
    if not installers:
        await cb.message.answer("❌ Нет активных монтажников.")  # type: ignore[union-attr]
        return
    installer = installers[0]
    installer_uid = installer.telegram_id

    import json
    from datetime import datetime
    att_json = json.dumps(attachments, ensure_ascii=False) if attachments else None
    _now = datetime.now().isoformat()
    await db.conn.execute(
        "UPDATE invoices SET assigned_to = ?, edo_task_id = NULL, montazh_stage = ?, "
        "montazh_assign_attachments_json = ?, montazh_assigned_at = ?, updated_at = ? WHERE id = ?",
        (installer_uid, MontazhStage.ASSIGNED, att_json, _now, _now, invoice_id),
    )
    await db.conn.commit()
    if integrations:
        await integrations.sync_invoice_row(invoice_id)

    num = inv.get("invoice_number", "?")
    addr = inv.get("object_address") or "—"

    # Тип менеджера из номера
    if "КИА" in num:
        mgr_label = "КИА"
        lead_name = inv.get("lead_kia_name") or ""
        lead_phone = inv.get("lead_kia_phone") or ""
    elif "НПН" in num:
        mgr_label = "НПН"
        lead_name = inv.get("lead_npn_name") or ""
        lead_phone = inv.get("lead_npn_phone") or ""
    else:
        mgr_label = "КВ"
        lead_name = inv.get("lead_kv_name") or ""
        lead_phone = inv.get("lead_kv_phone") or ""
    if not lead_name:
        lead_name = inv.get("client_name") or ""

    # Монтаж — расч. сумма по коэффициентам (б/н ×0.67 +10% надбавка, кредит ×0.95
    # без надбавки), как в карточке монтажника «В работу». Lazy-import (circular import).
    from .installer_new import _calc_est_montazh, _calc_est_montazh_base, _is_credit
    est_val = _calc_est_montazh(inv)
    est_base = _calc_est_montazh_base(inv)

    # Дедлайн
    from datetime import date as _date, datetime as _dt
    dl_str = ""
    days_left_str = ""
    dl_raw = inv.get("deadline_end_date")
    if dl_raw:
        try:
            dl_date = _dt.fromisoformat(str(dl_raw)).date()
            days_left = (dl_date - _date.today()).days
            dl_str = dl_date.strftime("%d.%m.%Y")
            if days_left < 0:
                days_left_str = f"просрочен {-days_left} дн."
            elif days_left == 0:
                days_left_str = "сегодня"
            else:
                days_left_str = f"{days_left} дн."
        except (ValueError, TypeError):
            pass

    # Карточка — эталон-v2 через format_card_section (1-в-1 с карточкой монтажника
    # из списка «🔨 В Работу», installer_work_view_card): итог строкой «Итого» в
    # теле, разделитель пробелами, Монтаж двумя строками (б/н) / одной (кредит).
    from ..utils import format_card_section

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    items: list[tuple[str, str]] = [
        ("Менеджер", mgr_label),
        ("Адрес", addr),
    ]
    if lead_name:
        items.append(("Клиент", lead_name))
    # телефон скрыт для монтажника
    if dl_str:
        items.append(("Срок", dl_str))
    if days_left_str:
        items.append(("Осталось", days_left_str))
    if est_val:
        # б/н: база + «Монтаж+10%» (итог); кредит: одна сумма (без +10%).
        if _is_credit(inv):
            items.append(("Монтаж расч.", _f(est_val)))
        else:
            items.append(("Монтаж", _f(est_base)))
            items.append(("Монтаж+10%", _f(est_val)))
    card_text = format_card_section(
        emoji="🔨",
        title=f"Новый счёт: №{num}",
        items=items,
        total=_f(est_val) if est_val else None,
        width=27,
        compact=True,
    ) + "\n\nНажмите «🔨 В Работу» для подтверждения."

    # Уведомление монтажнику
    try:
        await cb.bot.send_message(installer_uid, card_text)  # type: ignore[union-attr]
        # Отправить вложения
        for a in attachments:
            try:
                ft = a.get("file_type", "")
                fid = a.get("file_id", "")
                cap = a.get("caption", "")
                if ft == "photo":
                    await cb.bot.send_photo(installer_uid, fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "video":
                    await cb.bot.send_video(installer_uid, fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "document":
                    await cb.bot.send_document(installer_uid, fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "text" and cap:
                    await cb.bot.send_message(installer_uid, f"💬 {cap}")  # type: ignore[union-attr]
            except Exception:
                pass
    except Exception:
        pass

    installer_name = installer.username or installer.full_name or str(installer_uid)
    att_note = f" с {len(attachments)} вложениями" if attachments else ""
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Счёт №{num} отправлен монтажнику @{installer_name}{att_note}",
    )
    # Owner 22.08: у штатной группы (Игорь) ввода суммы ЗП монтажа у РП нет и не
    # было — её согласует сам монтажник авто-сметой. Карточка «💰 ЗП монтаж» здесь
    # больше НЕ отправляется: редакция от 16.07 слала её после ЛЮБОГО назначения,
    # и через неё 06.08 по счёту 26721-1НПН прошёл ввод РП мимо правила
    # (audit_log 8786: введено 15 000, записано 17 000 с надбавкой).


@router.callback_query(F.data == "rp_montazh:back_menu")
async def rp_montazh_back_menu(cb: CallbackQuery, db: Database) -> None:
    """Вернуться к главному меню Монтажной гр."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    in_work_all = await db.list_invoices_in_work(limit=50, include_credit=True)
    in_work_montazh = [
        i for i in in_work_all
        if i.get("montazh_stage") in ("in_work", "razmery_ok", "invoice_ok")
    ]
    n_in_work = len(in_work_montazh)
    n_send = await db.count_invoices_to_send_montazh()

    b = InlineKeyboardBuilder()
    b.button(text=f"📋 Счета в работе ({n_in_work})", callback_data="rp_montazh:list_inwork")
    b.button(text=(f"➕ Счёт в монтаж 🔴{n_send}" if n_send else "➕ Счёт в монтаж"), callback_data="rp_montazh:send_to_work")
    b.button(text="💬 Чат", callback_data="rp_montazh:chat")
    b.button(text="📐 Размеры", callback_data="rp_montazh:razmery")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "🔧 <b>Монтажная гр.</b>\n\nВыберите действие:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:chat")
async def rp_montazh_chat_cb(cb: CallbackQuery, db: Database, config: "Config") -> None:
    """Монтажная гр. → Чат (inline)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    limit = getattr(config, "chat_history_limit", 20)
    messages_list = await db.list_chat_messages("montazh", limit=limit)
    if not messages_list:
        await cb.message.answer("💬 Пока нет сообщений в чате с монтажной группой.")  # type: ignore[union-attr]
        return
    lines: list[str] = [f"💬 <b>Чат — Монтажная гр.</b> (последние {len(messages_list)}):\n"]
    for m in messages_list:
        sender_id = m.get("sender_id", 0)
        sender_label = await get_initiator_label(db, int(sender_id)) if sender_id else "?"
        text_msg = m.get("text", "")
        ts = m.get("created_at", "")[:16]
        direction = m.get("direction", "")
        arrow = "→" if direction == "outgoing" else "←"
        lines.append(f"<b>{sender_label}</b> {arrow} ({ts}):\n{text_msg}")
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="rp_montazh:back_menu")
    await cb.message.answer("\n\n".join(lines[-12:]), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "rp_montazh:razmery")
async def rp_montazh_razmery_cb(cb: CallbackQuery, db: Database) -> None:
    """Монтажная гр. → Размеры (inline) — показываем список запросов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    cur = await db.conn.execute(
        "SELECT * FROM razmery_requests ORDER BY created_at DESC LIMIT 15",
    )
    rows = [dict(r) for r in await cur.fetchall()]
    if not rows:
        await cb.message.answer("📐 Нет запросов на размеры.")  # type: ignore[union-attr]
        return
    b = InlineKeyboardBuilder()
    for req in rows:
        inv = await db.get_invoice(req["invoice_id"]) if req.get("invoice_id") else None
        num = inv.get("invoice_number", "?") if inv else "?"
        status_map = {"new": "🆕", "pending_form": "📝", "error": "❌", "sent": "📤", "ok": "✅"}
        s = status_map.get(req.get("status", ""), "❓")
        b.button(text=f"{s} №{num}"[:50], callback_data=f"razmok_rp:view:{req['id']}")
    b.button(text="⬅️ Назад", callback_data="rp_montazh:back_menu")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📐 <b>Размеры</b> ({len(rows)})\n\nНажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_montazh:work_refresh")
async def rp_montazh_work_refresh(cb: CallbackQuery, db: Database) -> None:
    """Обновить список — перенаправляем на list_inwork."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await rp_montazh_list_inwork(cb, db)


@router.callback_query(F.data.regexp(r"^rp_montazh:work_view:\d+$"))
async def rp_montazh_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта «В работу» монтажной группы."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = invoice_status_label(inv.get("status"))

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    text = (
        f"🔧 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount_str}\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    # Installer OK status
    if inv.get("installer_ok"):
        ok_by = ""
        if inv.get("installer_ok_by"):
            ok_by = await get_initiator_label(db, int(inv["installer_ok_by"]))
            ok_by = f" ({ok_by})"
        ok_at = inv.get("installer_ok_at", "")[:10] if inv.get("installer_ok_at") else ""
        text += f"\n✅ <b>Монтажник — Счет ОК</b>{ok_by} {ok_at}\n"
    else:
        text += "\n⏳ Монтажник — ожидание «Счет ОК»\n"

    # ZP status
    zp_label = {
        "not_requested": "⏳ Не запрошен",
        "requested": "📤 Отправлен ГД",
        "approved": "✅ ЗП ОК",
    }.get(inv.get("zp_status", "not_requested"), inv.get("zp_status", ""))
    text += f"💵 Расчёт ЗП: {zp_label}\n"

    # EDO status
    if inv.get("edo_signed"):
        text += "📄 ЭДО: ✅ Подписано\n"
    else:
        text += "📄 ЭДО: ⏳ Не подписано\n"

    b = InlineKeyboardBuilder()
    # Наёмная группа (2️⃣): РП сам подтверждает готовность монтажа → авто-запрос ЗП ГД.
    if inv.get("edo_task_id") == 2 and not inv.get("installer_ok"):
        b.button(text="✅ Монтаж ОК", callback_data=f"rp_montazh:naem_ok:{invoice_id}")
    b.button(text="🔁 Изменить Монтажников", callback_data=f"rp_montazh:regroup:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_montazh:work_refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_montazh:naem_ok:\d+$"))
async def rp_montazh_naem_ok(
    cb: CallbackQuery, db: Database, config: Config,
    integrations: IntegrationHub, notifier: Notifier,
) -> None:
    """Наёмная группа: РП подтверждает «Монтаж ОК» → готовность + авто-запрос ЗП к ГД."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    u = cb.from_user
    if not u:
        return
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.answer("❌ Счёт не найден.", show_alert=True)
        return
    if inv.get("edo_task_id") != 2:
        await cb.answer("⚠️ Это не наёмный счёт.", show_alert=True)
        return
    if inv.get("installer_ok"):
        await cb.answer("✅ Монтаж уже подтверждён.", show_alert=True)
        return
    if (inv.get("zp_installer_status") or "not_requested") not in ("not_requested",):
        await cb.answer("⚠️ Запрос ЗП уже отправлен.", show_alert=True)
        return
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if not agreed:
        await cb.answer("⚠️ Сначала укажите согласованную сумму ЗП монтажа.", show_alert=True)
        return
    # Объединение платежей (owner 15.07): Согласовано включает ЗП, уже выплаченную ПРОШЛЫМ
    # монтажным группам, — этой группе причитается только доплата. Без вычета ГД выплатил
    # бы всю объединённую сумму (220 000 вместо 130 000) живыми деньгами.
    paid_prev = float(inv.get("montazh_paid_prev") or 0)
    due = agreed - paid_prev
    if due <= 0:
        await cb.answer("⚠️ Согласованная ЗП по счёту уже выплачена полностью.", show_alert=True)
        return
    await cb.answer(f"✅ Монтаж ОК: {due:,.0f}₽")

    # 1) Готовность: installer_ok + стадия «Счёт ОК» (паритет с Игорем)
    await db.set_invoice_installer_ok(invoice_id, True)
    await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_OK)
    inv_row = await db.get_invoice(invoice_id)
    if inv_row:
        await integrations.sync_invoice_status(
            inv_row["invoice_number"], inv_row.get("status", ""), MontazhStage.INVOICE_OK,
        )

    # 2) Авто-запрос ЗП монтажника к ГД на согласованную сумму (за вычетом выплаченного
    #    прошлым группам — см. due выше)
    await db.set_invoice_zp_installer_status(
        invoice_id, "requested", amount=due, requested_by=u.id,
    )
    await integrations.sync_invoice_row(invoice_id)

    inv_number = inv.get("invoice_number") or "—"
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await db.create_task(
            project_id=None,
            type_=TaskType.ZP_INSTALLER,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=int(gd_id),
            due_at_iso=None,
            payload={
                "invoice_id": invoice_id,
                "invoice_number": inv_number,
                "amount": due,
                "source": "rp_naem_montazh_ok",
            },
        )
        initiator = await get_initiator_label(db, u.id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
        b.adjust(2)
        # Owner 01.09: единый вид карточки «Запрос ЗП монтажника» во ВСЕХ местах.
        # Здесь лежал ad-hoc плоский текст — запрещённый anti-pattern B
        # [[feedback_card_template_standard]]: один и тот же счёт выглядел у ГД иначе,
        # чем присланный из installer_new. Кредит-пометку печатает сам
        # билдер, поэтому credit_warn здесь больше не нужен. initiator НЕ
        # передаём: билдер подписал бы его «(монтажник)», а здесь
        # заявку подаёт РП — подпись уходит в хвост.
        card = _gd_zp_request_card(inv, due, agreed=agreed)
        tail = f"\n👤 От: {initiator} (РП, наёмная группа 2️⃣)"
        if paid_prev > 0:
            # Без этой строки сумма выглядит как ошибка: она МЕНЬШЕ Согласованного.
            tail += (
                f"\n🔗 Согласовано {agreed:,.0f}₽, "
                f"выплачено прошлой группе {paid_prev:,.0f}₽"
            )
        await notifier.safe_send(int(gd_id), card + tail, reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ <b>Монтаж ОК.</b> Запрос ЗП на <b>{due:,.0f}₽</b> отправлен ГД.\n"
        f"Счёт: №{inv_number}",
    )


# =====================================================================
# РАЗМЕРЫ — workflow проверки размеров стекла (РП-сторона)
# =====================================================================

@router.message(ManagerChatProxySG.menu, F.text == RP_MONTAZH_BTN_RAZMERY)
async def rp_razmery_inbox(message: Message, state: FSMContext, db: Database) -> None:
    """Монтажная гр. → Размеры: inbox заявок на размеры."""
    data = await state.get_data()
    channel = data.get("channel", "montazh")
    if channel != "montazh":
        return

    reqs = await db.list_razmery_requests_for_rp()
    if not reqs:
        await message.answer(
            "📐 <b>Размеры</b>\n\nНет активных заявок ✅"
        )
        return

    _STATUS_LABEL = {
        "pending": "🆕 Новый",
        "rp_received": "📝 Ожидает формы",
        "error": "❌ Ошибка → исправить",
        "verification_sent": "📤 Отправлено",
    }

    b = InlineKeyboardBuilder()
    for req in reqs:
        inv_num = req.get("invoice_number") or f"#{req['invoice_id']}"
        sl = _STATUS_LABEL.get(req["status"], req["status"])
        b.button(
            text=f"{sl}: №{inv_num}"[:55],
            callback_data=f"razmok_rp:view:{req['id']}",
        )
    b.adjust(1)

    stats = {}
    for req in reqs:
        s = req["status"]
        stats[s] = stats.get(s, 0) + 1
    stats_line = " | ".join(f"{_STATUS_LABEL.get(k, k)}: {v}" for k, v in stats.items())

    await message.answer(
        f"📐 <b>Размеры</b> ({len(reqs)})\n"
        f"{stats_line}\n\n"
        "Нажмите для просмотра:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("razmok_rp:view:"))
async def rp_razmery_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка заявки на размеры."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    addr = inv.get("object_address", "—") if inv else "—"
    inst_label = await get_initiator_label(db, req["installer_id"])

    _STATUS_LABEL = {
        "pending": "🆕 Ожидает подтверждения",
        "rp_received": "📝 Ожидает формы поставщика",
        "error": "❌ Ошибка — нужно исправить",
        "verification_sent": "📤 Отправлено монтажнику",
    }

    text = (
        f"📐 <b>Заявка на размеры #{req['id']}</b>\n\n"
        f"🧾 Счёт: №{inv_num}\n"
        f"📍 Адрес: {addr}\n"
        f"👷 Монтажник: {inst_label}\n"
        f"📊 Статус: {_STATUS_LABEL.get(req['status'], req['status'])}\n"
    )
    if req.get("installer_comment"):
        text += f"💬 Комментарий: {req['installer_comment']}\n"
    if req.get("result") == "error" and req.get("result_comment"):
        text += f"\n❌ <b>Ошибка от монтажника:</b>\n{req['result_comment']}\n"

    b = InlineKeyboardBuilder()
    if req["status"] == "pending":
        b.button(text="✅ ОК (принял)", callback_data=f"razmok_rp:received:{req_id}")
    elif req["status"] in ("rp_received", "error"):
        b.button(text="📐 Отправить форму", callback_data=f"razmok_rp:send_form:{req_id}")
    elif req["status"] == "verification_sent":
        b.button(text="⏳ Ожидаем ответ", callback_data=f"razmok_rp:noop:{req_id}")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("razmok_rp:noop:"))
async def rp_razmery_noop(cb: CallbackQuery) -> None:
    await cb.answer("Ожидаем ответ монтажника")


@router.callback_query(F.data.startswith("razmok_rp:received:"))
async def rp_razmery_confirm_receipt(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """РП подтверждает получение бланка."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("✅ Принял")
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        return

    await db.update_razmery_request(req_id, status="rp_received", rp_id=cb.from_user.id)

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"

    # Уведомить монтажника
    await notifier.safe_send(
        req["installer_id"],
        f"✅ РП принял бланк размеров по счёту №{inv_num}.\n"
        "Ожидайте форму поставщика для проверки.",
    )
    await refresh_recipient_keyboard(notifier, db, config, req["installer_id"])

    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Получение подтверждено. Теперь заполните форму поставщика и отправьте монтажнику.\n"
        "Используйте кнопку «📐 Размеры» → выберите заявку → «Отправить форму».",
    )


# --- Шаг 2: РП отправляет форму поставщика ---

@router.callback_query(F.data.startswith("razmok_rp:send_form:"))
async def rp_razmery_start_form(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП начинает отправку формы поставщика."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    await state.update_data(rp_razmery_req_id=req_id, rp_razmery_attachments=[])
    await state.set_state(RpRazmerySG.comment)
    await cb.message.answer(  # type: ignore[union-attr]
        "📐 <b>Форма поставщика</b>\n\n"
        "Добавьте комментарий к форме\n"
        "(или «-» для пропуска, «❌ Отмена» для отмены):",
    )


@router.message(RpRazmerySG.comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(RpRazmerySG.attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
async def rp_razmery_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await message.answer(
        "❌ Отменено.",
        reply_markup=rp_montazh_submenu("⬅️ Назад"),
    )


@router.message(RpRazmerySG.comment)
async def rp_razmery_form_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = None if text == "-" else text
    await state.update_data(rp_razmery_comment=comment)
    await state.set_state(RpRazmerySG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить монтажнику", callback_data="razmok_rp:form_create")
    b.button(text="⏭ Без вложений", callback_data="razmok_rp:form_create")
    b.adjust(1)
    await message.answer(
        "Прикрепите форму поставщика (фото/документ).\n"
        "Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(RpRazmerySG.attachments)
async def rp_razmery_form_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(
        message, state, storage, prefix=f"rp/{uid}", key="rp_razmery_attachments"
    )
    if att is None:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "razmok_rp:form_create")
async def rp_razmery_form_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Финализация: отправить форму поставщика монтажнику."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    req_id = data.get("rp_razmery_req_id")
    comment = data.get("rp_razmery_comment")
    attachments = data.get("rp_razmery_attachments", [])

    from ..utils import to_iso, utcnow
    now = to_iso(utcnow())

    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    await db.update_razmery_request(
        req_id,
        status="verification_sent",
        rp_id=u.id,
        rp_comment=comment,
        rp_sent_at=now,
        result=None,
        result_comment=None,
    )

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    rp_label = await get_initiator_label(db, u.id)

    # Уведомить монтажника
    inst_b = InlineKeyboardBuilder()
    inst_b.button(text="✅ Размеры ОК", callback_data=f"razmok_inst:ok:{req_id}")
    inst_b.button(text="❌ Ошибка", callback_data=f"razmok_inst:error:{req_id}")
    inst_b.adjust(2)

    msg = (
        f"📐 <b>Проверка размеров</b>\n"
        f"👤 От: {rp_label}\n"
        f"🧾 Счёт: №{inv_num}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"
    msg += "\nПроверьте форму и подтвердите:"

    await notifier.safe_send(
        req["installer_id"], msg, reply_markup=inst_b.as_markup(),
    )
    for a in attachments:
        await notifier.safe_send_media(req["installer_id"], a["file_type"], a["file_id"])
    await refresh_recipient_keyboard(notifier, db, config, req["installer_id"])

    # Вернуть РП в подменю montazh
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="montazh")
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Форма поставщика отправлена монтажнику по счёту №{inv_num}.",
        reply_markup=rp_montazh_submenu("⬅️ Назад"),
    )


# =====================================================================
# ПРОВЕРКА КП / ВЫСТАВЛЕНИЕ СЧЕТА — полный flow (Этап 5)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_CHECK_KP))
async def rp_check_kp(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: показать входящие CHECK_KP задачи."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    u = message.from_user
    if not u:
        return

    tasks = await db.list_check_kp_tasks(u.id)

    if not tasks:
        await answer_service(
            message,
            "📋 <b>Проверка КП / Выставление Счета</b>\n\n"
            "Входящих запросов на проверку КП нет ✅\n\n"
            "Используйте «📑 Выставленные счета» для просмотра обработанных.",
            delay_seconds=60,
        )
        # Всё равно показываем кнопку «Выставленные счета»
        b = InlineKeyboardBuilder()
        b.button(text="📑 Выставленные счета", callback_data="kp_resp:issued")
        b.adjust(1)
        await message.answer("—", reply_markup=b.as_markup())
        return

    # Подсчёт по менеджерам
    mgr_counts: dict[str, int] = {}
    for t in tasks:
        payload = json.loads(t.get("payload_json") or "{}")
        mrole = payload.get("manager_role", "manager")
        lbl = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}.get(mrole, "Менеджер")
        mgr_counts[lbl] = mgr_counts.get(lbl, 0) + 1

    summary_parts = [f"{lbl}: {cnt}" for lbl, cnt in mgr_counts.items()]

    await message.answer(
        f"📋 <b>Проверка КП / Выставление Счета</b>\n\n"
        f"Входящих запросов: <b>{len(tasks)}</b>\n"
        f"По менеджерам: {', '.join(summary_parts)}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=kp_task_list_kb(tasks, show_issued=True),
    )


# =====================================================================
# ЧАТ С ГД — chat-proxy (RP ↔ GD)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_CHAT_GD))
async def rp_chat_gd(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="rp_to_gd")
    await message.answer(
        "👤 <b>Чат с ГД</b>\n\nВыберите действие:",
        reply_markup=rp_chat_gd_submenu("⬅️ Назад"),
    )


@router.message(ManagerChatProxySG.menu, F.text == "✉️ Сообщение")
async def rp_chat_gd_write(message: Message, state: FSMContext) -> None:
    """РП → ГД: ввод сообщения."""
    from .chat_proxy import enter_writing
    data = await state.get_data()
    channel = data.get("channel", "rp_to_gd")
    await state.set_state(ManagerChatProxySG.writing)
    await state.update_data(channel=channel, pending_attachments=[])
    label = "ГД"
    await message.answer(
        f"✏️ <b>Написать → {label}</b>\n\n"
        "Введите текст сообщения.\n"
        "Можно прикрепить файлы/фото.\n"
        "Для отмены: /cancel",
    )


@router.message(ManagerChatProxySG.menu, F.text == "📋 Задача")
async def rp_chat_gd_task(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """РП → ГД: создать задачу."""
    from .chat_proxy import show_channel_tasks
    data = await state.get_data()
    channel = data.get("channel", "rp_to_gd")
    u = message.from_user
    if u:
        await show_channel_tasks(message, db, config, channel, u.id)


# =====================================================================
# СЧЕТА В РАБОТЕ — дашборд (Этап 6)
#
# Показывает счета со статусами PENDING/IN_PROGRESS/PAID (исключая Кред)
# с двойными индикаторами:
#   💰 = оплата (⏳ ожидает / 🔄 в работе / ✅ оплачен)
#   📄 = документы ЭДО (⏳ не подписано / ✅ подписано)
#
# Callbacks:
#   rp_work:view:\d+  — карточка счёта
#   rp_work:refresh   — обновить список
# =====================================================================


async def _show_invoices_work_dashboard(
    target: Message | CallbackQuery,
    db: Database,
) -> None:
    """Общий хелпер: показать дашборд «Счета в Работе»."""
    all_invoices = await db.list_invoices_in_work(limit=50, include_credit=True)
    # RP sees only pending + in_progress (not paid — those are done)
    invoices = [inv for inv in all_invoices if inv.get("status") != "paid"]

    if not invoices:
        b = InlineKeyboardBuilder()
        b.button(text="🔄 Обновить", callback_data="rp_work:refresh")
        b.button(text="⬅️ Назад", callback_data="nav:home")
        b.adjust(1)
        text = (
            "💼 <b>Счета в Работе</b>\n\n"
            "Нет активных счетов ✅"
        )
        await _answer_or_edit(target, text, reply_markup=b.as_markup())
        return

    # Statistics by status
    n_pending = sum(1 for inv in invoices if inv.get("status") == "pending")
    n_progress = sum(1 for inv in invoices if inv.get("status") == "in_progress")
    n_paid = sum(1 for inv in invoices if inv.get("status") == "paid")

    # EDO signing stats
    n_edo_signed = sum(1 for inv in invoices if inv.get("edo_signed"))
    n_edo_pending = len(invoices) - n_edo_signed

    header_parts: list[str] = []
    if n_pending:
        header_parts.append(f"⏳ Ждёт подтверждения: {n_pending}")
    if n_progress:
        header_parts.append(f"🔄 В работе: {n_progress}")
    if n_paid:
        header_parts.append(f"✅ Оплачены: {n_paid}")

    edo_parts: list[str] = []
    if n_edo_signed:
        edo_parts.append(f"✅ Подписано: {n_edo_signed}")
    if n_edo_pending:
        edo_parts.append(f"⏳ Не подписано: {n_edo_pending}")

    # Карточка-блок «Этапы работы» — тот же матрица-блок, что в стартовой карточке РП
    # (запрос user 03.06). _matrix грузит свои in-work счета (Б/Н + Кред), легенда+⬛.
    try:
        stages = await _matrix(db)
    except Exception:
        stages = ""
    text = (
        f"💼 <b>Счета в Работе</b> ({len(invoices)})\n\n"
        f"<b>💰 Оплата:</b> {' | '.join(header_parts)}\n"
        f"<b>📄 ЭДО:</b> {' | '.join(edo_parts)}\n\n"
        + (f"{stages}\n\n" if stages else "")
        + "Нажмите на счёт для просмотра:"
    )

    await _answer_or_edit(
        target,
        text,
        reply_markup=invoices_work_list_kb(invoices),
    )


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICES_WORK))
async def rp_invoices_work(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Счета в Работе»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    # Открытие раздела гасит бейдж 🔴N «Счёт оплачен» (канал 'rp_invoice_paid').
    if message.from_user:
        await db.mark_messages_read(message.from_user.id, "rp_invoice_paid")
    await _show_invoices_work_dashboard(message, db)


@router.callback_query(F.data == "rp_work:refresh")
async def rp_invoices_work_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд «Счета в Работе»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await _show_invoices_work_dashboard(cb, db)


@router.callback_query(F.data.regexp(r"^rp_work:view:\d+$"))
async def rp_invoices_work_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта из дашборда «Счета в Работе»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    b = InlineKeyboardBuilder()
    b.button(text="💬 Переписка", callback_data=f"rp_work:msgs:{invoice_id}")
    b.button(text="📋 Задачи", callback_data=f"rp_work:tasks:{invoice_id}")
    b.button(text="📦 Расходы", callback_data=f"rp_work:expenses:{invoice_id}")
    b.button(text="📎 Счёт ГД", callback_data=f"rp_work:send_inv_gd:{invoice_id}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_work:refresh")
    b.adjust(2, 2, 1)

    # Plan/Fact card (same format as GD)
    pf = await db.get_plan_fact_card(invoice_id)
    if pf.get("has_estimated"):
        from ..utils import format_plan_fact_card
        text = format_plan_fact_card(inv, pf, role="rp")
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
        return

    # Fallback for invoices without estimated data — карточка по эталону.
    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    section = await build_invoice_section(db, inv, invoice_id)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
    )

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# --- Вложенные страницы карточки «Счета в Работе» ---


@router.callback_query(F.data.regexp(r"^rp_work:msgs:\d+$"))
async def rp_work_messages(cb: CallbackQuery, db: Database) -> None:
    """Переписка, привязанная к счёту."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    messages = await db.list_chat_messages_by_invoice(invoice_id, limit=30)

    num = inv.get("invoice_number") or f"#{inv.get('id')}"
    lines: list[str] = [f"💬 <b>Переписка — Счёт №{num}</b>\n"]

    if not messages:
        lines.append("Нет привязанных сообщений.")
    else:
        for msg in reversed(messages):  # chronological order
            channel = msg.get("channel", "—")
            text_content = (msg.get("text") or "")[:120]
            dt = (msg.get("created_at") or "")[:16]
            direction = "→" if msg.get("direction") == "outgoing" else "←"
            has_file = " 📎" if msg.get("has_attachment") else ""
            lines.append(f"{dt} [{channel}] {direction} {text_content}{has_file}")

    text_out = "\n".join(lines)
    if len(text_out) > 3800:
        text_out = text_out[:3800] + "\n\n... (обрезано)"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_work:tasks:\d+$"))
async def rp_work_tasks(cb: CallbackQuery, db: Database) -> None:
    """Задачи, привязанные к счёту."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    tasks = await db.list_tasks_by_invoice(invoice_id, limit=30)

    num = inv.get("invoice_number") or f"#{inv.get('id')}"
    lines: list[str] = [f"📋 <b>Задачи — Счёт №{num}</b>\n"]

    if not tasks:
        lines.append("Нет привязанных задач.")
    else:
        from ..enums import MATERIAL_TYPE_LABELS
        status_emoji = {
            "open": "🟡", "in_progress": "🔵", "done": "✅", "rejected": "❌",
        }
        status_label = {
            "open": "Ожидает", "in_progress": "Принят ГД",
            "done": "Оплачен", "rejected": "Отклонён",
        }
        for t in tasks:
            s_emoji = status_emoji.get(t.get("status", ""), "❓")
            t_type_raw = t.get("type") or t.get("task_type") or "—"
            payload = try_json_loads(t.get("payload_json"))

            if t_type_raw == "invoice_payment":
                # Детальная карточка для счетов на оплату
                _amount = payload.get("amount", "")
                _mat_type = payload.get("material_type", "")
                _mat_label = MATERIAL_TYPE_LABELS.get(_mat_type, _mat_type)
                _s_label = status_label.get(t.get("status", ""), t.get("status", ""))
                try:
                    _amount_s = f"{float(_amount):,.0f}₽"
                except (ValueError, TypeError):
                    _amount_s = str(_amount)
                dt = (t.get("created_at") or "")[:10]
                lines.append(
                    f"{s_emoji} <b>Счёт ГД</b> — {_amount_s} | {_mat_label}\n"
                    f"    Статус: {_s_label} | {dt}"
                )
                # Комментарий РП
                _comment = payload.get("comment", "")
                if _comment:
                    _comment_short = _comment if len(_comment) <= 80 else _comment[:77] + "..."
                    lines.append(f"    💬 РП: {_comment_short}")
                # Комментарий ГД (после оплаты)
                _pp_comment = payload.get("pp_comment", "")
                if _pp_comment:
                    _pp_short = _pp_comment if len(_pp_comment) <= 80 else _pp_comment[:77] + "..."
                    lines.append(f"    💬 ГД: {_pp_short}")
                lines.append("")  # пустая строка-разделитель
            else:
                # Общий формат для остальных задач
                t_label = t_type_raw.replace("_", " ").title()
                dt = (t.get("created_at") or "")[:10]
                lines.append(f"{s_emoji} {t_label} ({dt})")

    text_out = "\n".join(lines)
    if len(text_out) > 3800:
        text_out = text_out[:3800] + "\n\n... (обрезано)"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_work:expenses:\d+$"))
async def rp_work_expenses(cb: CallbackQuery, db: Database) -> None:
    """Расходы по счёту (расширенный доступ РП — с суммами, без маржи)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    from ..utils import format_rp_expenses

    children = await db.list_child_invoices(invoice_id)
    supplier_payments = await db.list_supplier_payments_for_invoice(invoice_id)

    text_out = format_rp_expenses(inv, children, supplier_payments)

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К карточке", callback_data=f"rp_work:view:{invoice_id}")
    b.button(text="⬅️ К списку", callback_data="rp_work:refresh")
    b.adjust(2)

    await cb.message.answer(text_out, reply_markup=b.as_markup())  # type: ignore[union-attr]


# --- Добавление закрытых счетов обратно в работу ---


@router.callback_query(F.data == "rp_work:add_ended")
async def rp_work_add_ended(cb: CallbackQuery, db: Database) -> None:
    """Показать список ended-счетов для возврата в работу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    ended = await db.list_ended_invoices(limit=20, include_credit=True)
    if not ended:
        await cb.message.answer(  # type: ignore[union-attr]
            "✅ Нет закрытых счетов для возврата."
        )
        return

    b = InlineKeyboardBuilder()
    for inv in ended:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:28]
        label = f"🏁 №{num}"
        if addr:
            label += f" — {addr}"
        b.button(text=label[:60], callback_data=f"rp_work:return:{inv['id']}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_work:refresh")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Закрытые счета</b> ({len(ended)})\n\n"
        "Выберите счёт для возврата в работу:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^rp_work:return:\d+$"))
async def rp_work_return_confirm(cb: CallbackQuery, db: Database) -> None:
    """Подтверждение возврата ended-счёта в работу."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    try:
        amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{inv.get('amount', 0)}₽"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Вернуть в работу", callback_data=f"rp_work:return_ok:{invoice_id}")
    b.button(text="❌ Отмена", callback_data="rp_work:add_ended")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"🔄 <b>Вернуть счёт в работу?</b>\n\n"
        f"📄 №{inv.get('invoice_number', '?')}\n"
        f"📍 {inv.get('object_address', '-')}\n"
        f"💰 {amount_str}\n"
        f"📊 Текущий статус: 🏁 Закрыт\n\n"
        "Статус изменится на «В работе».",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.regexp(r"^rp_work:return_ok:\d+$"))
async def rp_work_return_ok(
    cb: CallbackQuery, db: Database, integrations: IntegrationHub,
) -> None:
    """Вернуть ended-счёт в работу (status → in_progress)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("✅ Возвращён в работу")

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_invoice_status(invoice_id, InvoiceStatus.IN_PROGRESS)
    await integrations.sync_invoice_status(inv["invoice_number"], InvoiceStatus.IN_PROGRESS)

    # Refresh the dashboard
    await _show_invoices_work_dashboard(cb, db)


# =====================================================================
# ОТПРАВКА СЧЁТА ОТ ПОСТАВЩИКА → ГД (из карточки «Счета в работе»)
# =====================================================================


@router.callback_query(F.data.regexp(r"^rp_work:send_inv_gd:\d+$"))
async def rp_work_send_inv_gd_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Начать отправку счёта от поставщика для ГД."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(RpSupplierInvoiceSG.amount)
    await state.update_data(invoice_id=invoice_id, attachments=[])

    num = inv.get("invoice_number") or f"#{invoice_id}"
    await cb.message.answer(  # type: ignore[union-attr]
        f"📎 <b>Счёт от поставщика → ГД</b>\n"
        f"Счёт: №{num}\n\n"
        "💰 Введите сумму счёта на оплату:",
    )


@router.message(RpSupplierInvoiceSG.amount)
async def rp_sinv_amount(message: Message, state: FSMContext) -> None:
    """Получить сумму счёта."""
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("⚠️ Введите число (сумму счёта):")
        return
    await state.update_data(sinv_amount=amount)
    from ..keyboards import material_type_kb
    await state.set_state(RpSupplierInvoiceSG.material_type)
    await message.answer(
        "📦 Выберите тип материала/услуги:",
        reply_markup=material_type_kb(prefix="rp_sinv_mat"),
    )


@router.callback_query(
    RpSupplierInvoiceSG.material_type,
    lambda cb: cb.data and cb.data.startswith("rp_sinv_mat:"),
)
async def rp_sinv_material(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбор типа материала/услуги."""
    await cb.answer()
    mat_code = (cb.data or "").split(":", 1)[1]
    await state.update_data(sinv_material_type=mat_code)

    await state.set_state(RpSupplierInvoiceSG.attachments)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без файлов → Комментарий", callback_data="rp_sinv:skip_attach")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "📎 Прикрепите файл(ы) счёта (документ или фото).\n"
        "Когда все файлы прикреплены — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(RpSupplierInvoiceSG.attachments)
async def rp_sinv_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Получить файл(ы) от РП для отправки ГД."""
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"rp/{uid}")
    if att is None:
        await message.answer("📎 Прикрепите файл или фото. Для продолжения нажмите кнопку.")
        return

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Далее ({count} файл.)", callback_data="rp_sinv:skip_attach")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)
    await message.answer(
        f"📎 Принял. Файлов: <b>{count}</b>.\n"
        "Прикрепите ещё или нажмите «Далее»:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "rp_sinv:skip_attach")
async def rp_sinv_to_comment(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Перейти к вводу комментария."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    await state.set_state(RpSupplierInvoiceSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без комментария", callback_data="rp_sinv:send")
    b.button(text="❌ Отмена", callback_data="rp_sinv:cancel")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        "💬 Введите комментарий к счёту (или нажмите «Без комментария»):",
        reply_markup=b.as_markup(),
    )


@router.message(RpSupplierInvoiceSG.comment)
async def rp_sinv_comment_text(
    message: Message, state: FSMContext, db: Database,
    config: "Config", notifier: "Notifier",
) -> None:
    """Получить комментарий и отправить ГД."""
    comment = (message.text or "").strip()
    if comment == "-":
        comment = ""
    await state.update_data(comment=comment)
    await _rp_sinv_finalize(message, state, db, config, notifier, message.from_user)


@router.callback_query(F.data == "rp_sinv:send")
async def rp_sinv_send_no_comment(
    cb: CallbackQuery, state: FSMContext, db: Database,
    config: "Config", notifier: "Notifier",
) -> None:
    """Отправить без комментария."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.update_data(comment="")
    await _rp_sinv_finalize(cb.message, state, db, config, notifier, cb.from_user)  # type: ignore[arg-type]


@router.callback_query(F.data == "rp_sinv:cancel")
async def rp_sinv_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Отменить отправку счёта."""
    await cb.answer("❌ Отменено")
    await state.clear()
    u = cb.from_user
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Отправка счёта отменена.", reply_markup=kb)  # type: ignore[union-attr]


async def _sinv_duplicate_warning(
    db: Database, invoice_id: int, amount: float, material_type: str,
) -> str | None:
    """Текст предупреждения, если такой счёт уже отправлен/оплачен.

    Дубль = тот же счёт (invoice_id) + та же сумма (±1 коп) + тот же тип.
    """
    from ..enums import MATERIAL_TYPE_LABELS
    EPS = 0.01
    mt_label = MATERIAL_TYPE_LABELS.get(material_type, material_type)
    # 1) уже есть заявка на оплату (open/in_progress) с теми же параметрами
    try:
        pend = await db.search_tasks_by_payload(
            field="invoice_id", value=str(invoice_id),
            type_filter=[TaskType.INVOICE_PAYMENT], limit=30,
        )
    except Exception:
        pend = []
    for t in pend:
        if t.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
            continue
        p = try_json_loads(t.get("payload_json"))
        same_inv = invoice_id in (p.get("invoice_id"), p.get("parent_invoice_id"))
        if (same_inv and (p.get("material_type") or "") == material_type
                and abs(float(p.get("amount") or 0) - amount) < EPS):
            return (
                "⚠️ <b>Похоже, этот счёт уже отправлен на оплату</b>\n"
                f"Заявка #{t.get('id')} ещё ждёт ГД "
                f"({mt_label}, {fmt_money(amount)}).\n\n"
                "Отправить ещё раз?"
            )
    # 2) уже оплачен (есть supplier_payment с теми же параметрами)
    try:
        paid = await db.list_supplier_payments_for_invoice(invoice_id)
    except Exception:
        paid = []
    for sp in paid:
        if ((sp.get("material_type") or "") == material_type
                and abs(float(sp.get("amount") or 0) - amount) < EPS):
            return (
                "⚠️ <b>Этот счёт уже оплачен</b>\n"
                f"{mt_label}, {fmt_money(amount)} — оплата уже проведена.\n\n"
                "Отправить ещё раз?"
            )
    return None


@router.callback_query(F.data == "rp_sinv:force")
async def rp_sinv_send_force(
    cb: CallbackQuery, state: FSMContext, db: Database,
    config: "Config", notifier: "Notifier",
) -> None:
    """РП подтвердил отправку счёта несмотря на предупреждение о дубле."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await _rp_sinv_finalize(cb.message, state, db, config, notifier, cb.from_user, force=True)  # type: ignore[arg-type]


async def _rp_sinv_finalize(
    event_msg: Any,
    state: FSMContext,
    db: Database,
    config: Any,
    notifier: Any,
    from_user: Any,
    force: bool = False,
) -> None:
    """Создать задачу SUPPLIER_INVOICE и отправить ГД."""
    data = await state.get_data()

    invoice_id = data.get("invoice_id")
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    comment: str = data.get("comment", "")
    sinv_amount: float = float(data.get("sinv_amount") or 0)
    sinv_material_type: str = data.get("sinv_material_type") or "extra_mat"

    # Защита от дублей: тот же счёт + сумма + тип, уже отправленный или
    # оплаченный, требует явного подтверждения «Всё равно отправить».
    if not force and invoice_id:
        dup = await _sinv_duplicate_warning(db, int(invoice_id), sinv_amount, sinv_material_type)
        if dup:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Всё равно отправить", callback_data="rp_sinv:force")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="rp_sinv:cancel")],
            ])
            await event_msg.answer(dup, reply_markup=kb)
            return

    await state.clear()

    inv = await db.get_invoice(invoice_id) if invoice_id else None
    num = (inv.get("invoice_number") if inv else None) or f"#{invoice_id}"

    from ..services.assignment import resolve_default_assignee
    from ..enums import TaskType, TaskStatus, MATERIAL_TYPE_LABELS
    from ..utils import utcnow, to_iso
    from datetime import timedelta

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await event_msg.answer("⚠️ ГД не найден. Настройте роль GD.")
        return

    due = utcnow() + timedelta(hours=7)
    task = await db.create_task(
        project_id=inv.get("project_id") if inv else None,
        type_=TaskType.INVOICE_PAYMENT,
        status=TaskStatus.OPEN,
        created_by=from_user.id if from_user else 0,
        assigned_to=int(gd_id),
        due_at_iso=to_iso(due),
        payload={
            "invoice_id": invoice_id,
            "parent_invoice_id": invoice_id,
            "invoice_number": num,
            "amount": sinv_amount,
            "material_type": sinv_material_type,
            "comment": comment,
            "sender_id": from_user.id if from_user else 0,
            "sender_username": (from_user.username if from_user else ""),
        },
    )

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    initiator = await get_initiator_label(db, from_user.id) if from_user else "?"
    mat_label = MATERIAL_TYPE_LABELS.get(sinv_material_type, sinv_material_type)
    from ..utils import format_card_section
    _items: list[tuple[str, str]] = [
        ("От", initiator),
        ("Счёт", f"№{num}"),
        ("Тип", mat_label),
    ]
    if inv and inv.get("object_address"):
        _items.append(("Объект", inv["object_address"][:50]))
    if attachments:
        _items.append(("Вложений", str(len(attachments))))
    gd_text = format_card_section(
        emoji="💳",
        title="Счёт на оплату",
        total=f"{sinv_amount:,.0f}₽".replace(",", " "),
        items=_items,
        footer=("💬", comment) if comment else None,
        compact=True,
        width=40,
    )

    from ..keyboards import task_actions_kb
    await notifier.safe_send(
        int(gd_id), gd_text,
        reply_markup=task_actions_kb(task),
    )

    for a in attachments:
        try:
            if a.get("file_type") == "document":
                await notifier.bot.send_document(int(gd_id), a["file_id"])
            elif a.get("file_type") == "photo":
                await notifier.bot.send_photo(int(gd_id), a["file_id"])
        except Exception:
            log.warning("Failed to send attachment to GD %s", gd_id, exc_info=True)

    try:
        from ..utils import refresh_recipient_keyboard
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))
    except Exception:
        log.exception("Failed to refresh GD keyboard after invoice_payment task")

    await event_msg.answer(
        f"✅ Счёт от поставщика отправлен ГД (счёт №{num}).\n"
        f"📎 Файлов: {len(attachments)}"
    )


# =====================================================================
# БУХГАЛТЕРИЯ (УПД) — ЭДО-запрос от РП (Этап 8)
#
# Дашборд: список исходящих ЭДО-запросов + кнопка «Создать»
# Создание запроса → EdoRequestSG flow (handlers in manager_new.py)
# Просмотр карточки запроса с ответом бухгалтерии
#
# Callbacks:
#   rp_edo:create — начать создание нового запроса
#   rp_edo:view:\d+ — просмотр карточки запроса
#   rp_edo:refresh — обновить дашборд
# =====================================================================


def _edo_requests_list_kb(
    requests: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком ЭДО-запросов РП."""
    b = InlineKeyboardBuilder()
    for r in requests:
        status_emoji = {"open": "⏳", "done": "✅"}.get(r.get("status", ""), "❓")
        req_type_label = {
            "sign_invoice": "Подпись счёт",
            "sign_closing": "Закрывающие",
            "sign_upd": "УПД поставщика",
            "other": "Другое",
        }.get(r.get("request_type", ""), r.get("request_type", ""))
        inv_num = r.get("invoice_number") or ""
        text = f"{status_emoji} {req_type_label}"
        if inv_num:
            text += f" №{inv_num}"
        b.button(text=text[:60], callback_data=f"rp_edo:view:{r['id']}")
    b.button(text="➕ Новый запрос ЭДО", callback_data="rp_edo:create")
    # #40: Кнопки «Подписать УПД» для счетов в работе (если переданы)
    b.button(text="📝 Подписать УПД", callback_data="rp_edo:upd_list")
    b.button(text="🔄 Обновить", callback_data="rp_edo:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


async def _show_edo_dashboard(
    target: Message | CallbackQuery,
    db: Database,
    user_id: int,
) -> None:
    """Показать дашборд «Бухгалтерия (УПД)» для РП."""
    requests = await db.list_edo_requests(requested_by=user_id, limit=30)
    counts = await db.count_edo_requests_by_user(user_id)

    # #40: Счета в работе для подписания УПД
    work_invoices = await db.list_invoices_in_work(limit=20, only_regular=True, include_credit=True)

    if not requests:
        b = InlineKeyboardBuilder()
        # #40: Кнопки «Подписать УПД» для каждого счёта в работе
        for wi in work_invoices[:10]:
            wi_num = wi.get("invoice_number") or f"#{wi['id']}"
            b.button(text=f"📝 УПД: №{wi_num}"[:50], callback_data=f"rp_edo:upd:{wi['id']}")
        b.button(text="➕ Новый запрос ЭДО", callback_data="rp_edo:create")
        b.adjust(1)
        text = (
            "📄 <b>Бухгалтерия (УПД)</b>\n\n"
            "Нет ЭДО-запросов.\n\n"
            "Нажмите для создания нового:"
        )
        if isinstance(target, CallbackQuery):
            await target.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
        else:
            await target.answer(text, reply_markup=b.as_markup())
        return

    stats_parts: list[str] = []
    if counts.get("open", 0):
        stats_parts.append(f"⏳ Ожидают: {counts['open']}")
    if counts.get("done", 0):
        stats_parts.append(f"✅ Выполнено: {counts['done']}")

    text = (
        f"📄 <b>Бухгалтерия (УПД)</b> ({len(requests)})\n"
        f"{' | '.join(stats_parts)}\n\n"
        "Ваши ЭДО-запросы:"
    )

    if isinstance(target, CallbackQuery):
        await target.message.answer(text, reply_markup=_edo_requests_list_kb(requests))  # type: ignore[union-attr]
    else:
        await target.answer(text, reply_markup=_edo_requests_list_kb(requests))


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_EDO))
async def rp_edo_request(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Бухгалтерия (УПД)»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()
    u = message.from_user
    if not u:
        return
    await _show_edo_dashboard(message, db, u.id)


@router.callback_query(F.data == "rp_edo:create")
async def rp_edo_create(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание нового ЭДО-запроса."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(EdoRequestSG.request_type)
    await cb.message.answer(  # type: ignore[union-attr]
        "📄 <b>Новый запрос ЭДО</b>\n\n"
        "Выберите тип запроса:",
        reply_markup=edo_type_kb(),
    )


@router.callback_query(F.data == "rp_edo:refresh")
async def rp_edo_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд ЭДО-запросов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    u = cb.from_user
    if not u:
        return
    await _show_edo_dashboard(cb, db, u.id)


# #40: Список счетов для подписания УПД
@router.callback_query(F.data == "rp_edo:upd_list")
async def rp_edo_upd_list(cb: CallbackQuery, db: Database) -> None:
    """Показать счета в работе для подписания УПД."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    invoices = await db.list_invoices_in_work(limit=20, only_regular=True, include_credit=True)
    if not invoices:
        await cb.message.answer("✅ Нет счетов в работе.")  # type: ignore[union-attr]
        return
    b = InlineKeyboardBuilder()
    for inv in invoices[:10]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        label = f"📝 №{num}"
        if addr:
            label += f" — {addr}"
        b.button(text=label[:50], callback_data=f"rp_edo:upd:{inv['id']}")
    b.button(text="⬅️ Назад", callback_data="rp_edo:refresh")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "📝 <b>Подписать УПД</b>\n\nВыберите счёт:",
        reply_markup=b.as_markup(),
    )


# #40: Создать задачу «Подписать УПД» для бухгалтера
@router.callback_query(F.data.regexp(r"^rp_edo:upd:\d+$"))
async def rp_edo_upd_create(
    cb: CallbackQuery, db: Database, config: Config, notifier: "Notifier",
) -> None:
    """Создать задачу на подписание УПД для бухгалтера."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    from ..services.assignment import resolve_default_assignee
    acc_id = await resolve_default_assignee(db, config, Role.ACCOUNTING)
    if not acc_id:
        await cb.message.answer("❌ Бухгалтер не назначен.")  # type: ignore[union-attr]
        return

    await db.create_task(
        project_id=None,
        type_=TaskType.EDO_REQUEST,
        status=TaskStatus.OPEN,
        created_by=cb.from_user.id,
        assigned_to=int(acc_id),
        due_at_iso=None,
        payload={
            "invoice_id": inv_id,
            "invoice_number": inv["invoice_number"],
            "request_type": "sign_upd",
            "request_text": f"Подписать УПД по счёту №{inv['invoice_number']}",
        },
    )

    from .common import get_initiator_label, refresh_recipient_keyboard
    initiator = await get_initiator_label(db, cb.from_user.id)
    msg = (
        f"📝 <b>Запрос: Подписать УПД</b>\n"
        f"👤 От: {initiator}\n"
        f"📄 Счёт: №{inv['invoice_number']}\n"
    )
    await notifier.safe_send(int(acc_id), msg)
    await refresh_recipient_keyboard(notifier, db, config, int(acc_id))

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Задача «Подписать УПД» по счёту №{inv['invoice_number']} отправлена бухгалтеру.",
    )


@router.callback_query(F.data.regexp(r"^rp_edo:view:\d+$"))
async def rp_edo_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка ЭДО-запроса с ответом бухгалтерии."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    edo_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_edo_request(edo_id)
    if not req:
        await cb.message.answer("❌ Запрос не найден.")  # type: ignore[union-attr]
        return

    req_type_label = {
        "sign_invoice": "Подписать по ЭДО (счет)",
        "sign_closing": "Закрывающие по ЭДО",
        "sign_upd": "Подписать по ЭДО УПД поставщика",
        "other": "Другое",
    }.get(req.get("request_type", ""), req.get("request_type", ""))

    status_label = {
        "open": "⏳ Ожидает",
        "done": "✅ Выполнено",
    }.get(req.get("status", ""), req.get("status", ""))

    text = (
        f"📄 <b>ЭДО-запрос #{req['id']}</b>\n\n"
        f"📋 Тип: {req_type_label}\n"
    )
    if req.get("invoice_number"):
        text += f"🔢 Счёт №: {req['invoice_number']}\n"
    if req.get("description"):
        text += f"📝 Описание: {req['description']}\n"
    if req.get("comment"):
        text += f"💬 Комментарий: {req['comment']}\n"
    text += (
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {req.get('created_at', '-')[:10]}\n"
    )

    # Response from accounting
    if req.get("status") == "done":
        resp_type = {
            "signed": "✅ Подписано",
            "ok": "✅ ОК",
            "waiting": "⏳ Ожидание",
            "need_docs": "📄 Запрос документов",
        }.get(req.get("response_type", ""), req.get("response_type", ""))
        text += (
            f"\n<b>Ответ бухгалтерии:</b>\n"
            f"📋 Результат: {resp_type}\n"
        )
        if req.get("response_comment"):
            text += f"💬 Комментарий: {req['response_comment']}\n"
        if req.get("completed_at"):
            text += f"📅 Выполнено: {req['completed_at'][:10]}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_edo:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# СЧЕТ ЗАКРЫТ — дашборд (Этап 10)
#
# Показывает ENDED счета, сгруппированные по месяцам.
# Счётчик на кнопке: кол-во закрытых за текущий месяц.
# Поиск по номеру счёта / адресу.
#
# Callbacks:
#   rp_closed:view:\d+   — карточка закрытого счёта
#   rp_closed:refresh    — обновить список
#   rp_closed:all        — показать все (не только текущий месяц)
#   rp_closed:search     — поиск по номеру/адресу (inline → FSM)
# =====================================================================


def _ended_invoices_kb(
    invoices: list[dict[str, Any]],
    show_all: bool = False,
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком закрытых счетов."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        try:
            amount_str = f"{float(inv.get('amount', 0)):,.0f}₽"
        except (ValueError, TypeError):
            amount_str = f"{inv.get('amount', 0)}₽"
        closed_date = (inv.get("updated_at") or inv.get("created_at", ""))[:10]
        text = f"🏁 №{inv.get('invoice_number', '?')} — {amount_str} ({closed_date})"
        b.button(text=text[:60], callback_data=f"rp_closed:view:{inv['id']}")
    if not show_all:
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
    b.button(text="🔍 Поиск", callback_data="rp_closed:search")
    b.button(text="🔄 Обновить", callback_data="rp_closed:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    return b.as_markup()


def _current_month_start() -> str:
    """Return ISO date string for the 1st day of the current month."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_INVOICE_CLOSED))
async def rp_invoice_closed(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Счет закрыт»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    month_start = _current_month_start()
    invoices = await db.list_ended_invoices(month_start=month_start, limit=30, include_credit=True)
    total_this_month = await db.count_ended_invoices(month_start=month_start, include_credit=True)
    total_all = await db.count_ended_invoices(include_credit=True)

    if not invoices and total_all == 0:
        b = InlineKeyboardBuilder()
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await message.answer(
            "🏁 <b>Счет закрыт</b>\n\n"
            "Нет закрытых счетов.",
            reply_markup=b.as_markup(),
        )
        return

    if not invoices:
        # No invoices this month but there are older ones
        b = InlineKeyboardBuilder()
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await message.answer(
            f"🏁 <b>Счет закрыт</b>\n\n"
            f"За текущий месяц: <b>0</b>\n"
            f"Всего: <b>{total_all}</b>\n\n"
            "Нажмите «Все» для просмотра:",
            reply_markup=b.as_markup(),
        )
        return

    await message.answer(
        f"🏁 <b>Счет закрыт</b>\n\n"
        f"За текущий месяц: <b>{total_this_month}</b>\n"
        f"Всего: <b>{total_all}</b>\n\n"
        "Закрытые счета (текущий месяц):",
        reply_markup=_ended_invoices_kb(invoices),
    )


@router.callback_query(F.data == "rp_closed:refresh")
async def rp_invoice_closed_refresh(cb: CallbackQuery, db: Database) -> None:
    """Обновить список «Счет закрыт»."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")

    month_start = _current_month_start()
    invoices = await db.list_ended_invoices(month_start=month_start, limit=30, include_credit=True)
    total_this_month = await db.count_ended_invoices(month_start=month_start, include_credit=True)
    total_all = await db.count_ended_invoices(include_credit=True)

    if not invoices:
        b = InlineKeyboardBuilder()
        b.button(text="📋 Все закрытые счета", callback_data="rp_closed:all")
        b.button(text="🔍 Поиск", callback_data="rp_closed:search")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"🏁 За текущий месяц: <b>0</b> | Всего: <b>{total_all}</b>",
            reply_markup=b.as_markup(),
        )
        return

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Счет закрыт</b>\n\n"
        f"За месяц: <b>{total_this_month}</b> | Всего: <b>{total_all}</b>\n\n"
        "Закрытые счета (текущий месяц):",
        reply_markup=_ended_invoices_kb(invoices),
    )


@router.callback_query(F.data == "rp_closed:all")
async def rp_invoice_closed_all(cb: CallbackQuery, db: Database) -> None:
    """Показать все закрытые счета (не только текущий месяц)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_ended_invoices(limit=50, include_credit=True)
    if not invoices:
        await cb.message.answer("🏁 Нет закрытых счетов.")  # type: ignore[union-attr]
        return

    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Все закрытые счета</b> ({len(invoices)})\n\n"
        "Нажмите для просмотра:",
        reply_markup=_ended_invoices_kb(invoices, show_all=True),
    )


@router.callback_query(F.data.regexp(r"^rp_closed:view:\d+$"))
async def rp_invoice_closed_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка закрытого счёта."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    conditions = await db.check_close_conditions(invoice_id)
    cond_rows = close_condition_core_rows(inv, conditions)
    cond_rows.append(("✅" if conditions.get("zp_approved") else "⏳", "ЗП — утверждено"))
    section = ("Условия", [
        f"{i}. {mark} {label}" for i, (mark, label) in enumerate(cond_rows, 1)
    ])

    closed_at = (inv.get("updated_at") or "-")[:10]
    extra_meta = [f"📅 Закрыт: {closed_at}"]

    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("close_comment") or inv.get("description") or None,
        extra_meta=extra_meta,
    )

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="rp_closed:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "rp_closed:search")
async def rp_invoice_closed_search(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Поиск закрытого счёта → переход в FSM поиска."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    from ..states import InvoiceSearchSG
    await state.clear()
    await state.set_state(InvoiceSearchSG.value)
    await state.update_data(search_context="closed")
    await cb.message.answer(  # type: ignore[union-attr]
        "🔍 <b>Поиск счёта</b>\n\n"
        "Введите номер счёта или адрес для поиска:",
    )


# =====================================================================
# ПОИСК СЧЁТА (кнопка подменю «Ещё») — #44
# =====================================================================

@router.message(lambda m: (m.text or "").strip() in {RP_BTN_SEARCH_INVOICE, "🔍 Поиск счёта", "Поиск счёта"})
async def rp_search_invoice_btn(message: Message, state: FSMContext, db: Database) -> None:
    """#44: Хендлер кнопки «Поиск счёта» в подменю РП."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    from ..states import InvoiceSearchSG
    await state.clear()
    await state.set_state(InvoiceSearchSG.value)
    await message.answer(
        "🔍 <b>Поиск счёта</b>\n\n"
        "Введите номер счёта или адрес для поиска:",
    )


# =====================================================================
# СИНХРОНИЗАЦИЯ (кнопка подменю «Ещё»)
# =====================================================================

@router.message(lambda m: (m.text or "").strip() in {RP_BTN_SYNC, "🔄 Синхронизация данных"})
async def rp_sync_data(message: Message, db: Database, config: Config) -> None:
    """Хендлер кнопки «Синхронизация данных» в подменю РП."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    # Делегируем в общий обработчик синхронизации
    from .common import sync_data_non_gd
    await sync_data_non_gd(message, db, config)


# =====================================================================
# ЛИД НА РАСЧЕТ (Этап 11)
#
# Дашборд: список лидов + статистика + создание
# Создание: менеджер → описание → источник (inline) → вложения
# Источники: Свой клиент, Повторное обращение, Парсеры лидов, Другое
#
# Callbacks:
#   rp_lead:create    — начать создание нового лида
#   rp_lead:stats     — статистика конверсии
#   rp_lead:refresh   — обновить дашборд
#   rp_lead:view:\d+  — карточка лида
#   lead_src:*        — выбор источника лида (inline)
# =====================================================================

_LEAD_SOURCES = [
    ("Свой клиент", "own"),
    ("Повторное обращение", "repeat"),
    ("Парсеры лидов", "parsers"),
    ("Другое", "other"),
]


def _lead_source_kb() -> InlineKeyboardMarkup:
    """Inline-кнопки выбора источника лида."""
    b = InlineKeyboardBuilder()
    for label, key in _LEAD_SOURCES:
        b.button(text=label, callback_data=f"lead_src:{key}")
    b.button(text="❌ Отмена", callback_data="lead:cancel")
    b.adjust(2, 2, 1)
    return b.as_markup()


def _leads_list_kb(
    leads: list[dict[str, Any]],
) -> InlineKeyboardMarkup:
    """Inline-кнопки со списком лидов."""
    b = InlineKeyboardBuilder()
    for lead in leads:
        mgr_role = lead.get("assigned_manager_role", "")
        mgr_label = {
            "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        }.get(mgr_role, "Менеджер")
        source = lead.get("lead_source", "—") or "—"
        responded = "✅" if lead.get("response_at") else "⏳"
        date_str = (lead.get("assigned_at") or lead.get("created_at", ""))[:10]
        text = f"{responded} {mgr_label} | {source[:15]} ({date_str})"
        b.button(text=text[:60], callback_data=f"rp_lead:view:{lead['id']}")
    b.button(text="➕ Новый лид", callback_data="rp_lead:create")
    b.button(text="📊 Статистика", callback_data="rp_lead:stats")
    b.button(text="🔄 Обновить", callback_data="rp_lead:refresh")
    b.adjust(1)
    return b.as_markup()


@router.message(lambda m: (m.text or "").strip().startswith(RP_BTN_LEAD))
async def start_lead_to_project(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка главного меню: дашборд «Лид на расчет»."""
    if not await require_role_message(message, db, roles=[Role.RP]):
        return
    await state.clear()

    leads = await db.list_leads(limit=20)
    total = await db.count_leads_total()

    if not leads:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Новый лид", callback_data="rp_lead:create")
        b.adjust(1)
        await message.answer(
            "🎯 <b>Лид на расчет</b>\n\n"
            "Нет лидов.\n\n"
            "Нажмите для создания нового:",
            reply_markup=b.as_markup(),
        )
        return

    # Count responded
    n_responded = sum(1 for lead in leads if lead.get("response_at"))
    n_pending = len(leads) - n_responded

    stats = []
    if n_responded:
        stats.append(f"✅ Обработано: {n_responded}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await message.answer(
        f"🎯 <b>Лид на расчет</b> (всего: {total})\n"
        f"{' | '.join(stats)}\n\n"
        "Последние лиды:",
        reply_markup=_leads_list_kb(leads),
    )


@router.callback_query(F.data == "rp_lead:refresh")
async def lead_refresh(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Обновить дашборд лидов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer("🔄 Обновлено")
    await state.clear()

    leads = await db.list_leads(limit=20)
    total = await db.count_leads_total()

    if not leads:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Новый лид", callback_data="rp_lead:create")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            "🎯 Нет лидов.",
            reply_markup=b.as_markup(),
        )
        return

    n_responded = sum(1 for lead in leads if lead.get("response_at"))
    n_pending = len(leads) - n_responded
    stats = []
    if n_responded:
        stats.append(f"✅ Обработано: {n_responded}")
    if n_pending:
        stats.append(f"⏳ Ожидают: {n_pending}")

    await cb.message.answer(  # type: ignore[union-attr]
        f"🎯 <b>Лид на расчет</b> (всего: {total})\n"
        f"{' | '.join(stats)}\n\n"
        "Последние лиды:",
        reply_markup=_leads_list_kb(leads),
    )


@router.callback_query(F.data.regexp(r"^rp_lead:view:\d+$"))
async def lead_view(cb: CallbackQuery, db: Database) -> None:
    """Карточка лида."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    lead_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    # Get lead from lead_tracking table
    lead = await db.get_lead(lead_id)
    if not lead:
        await cb.message.answer("❌ Лид не найден.")  # type: ignore[union-attr]
        return

    mgr_role = lead.get("assigned_manager_role", "")
    mgr_label = {
        "manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(mgr_role, "Менеджер")

    # Get manager name
    mgr_name = mgr_label
    if lead.get("assigned_manager_id"):
        mgr_name = await get_initiator_label(db, int(lead["assigned_manager_id"]))
        mgr_name = f"{mgr_name} ({mgr_label})"

    responded_label = "✅ Обработан" if lead.get("response_at") else "⏳ Ожидает ответа"
    proc_time = ""
    if lead.get("processing_time_minutes"):
        minutes = lead["processing_time_minutes"]
        if minutes < 60:
            proc_time = f"\n⏱ Время отклика: {minutes} мин"
        else:
            hours = minutes // 60
            proc_time = f"\n⏱ Время отклика: {hours}ч {minutes % 60}мин"

    text = (
        f"🎯 <b>Лид #{lead['id']}</b>\n\n"
        f"👤 Менеджер: {mgr_name}\n"
        f"📌 Источник: {lead.get('lead_source', '—')}\n"
        f"📊 Статус: {responded_label}\n"
        f"📅 Назначен: {(lead.get('assigned_at') or '-')[:10]}\n"
    )
    if lead.get("response_at"):
        text += f"📅 Ответ: {lead['response_at'][:10]}\n"
    text += proc_time

    b = InlineKeyboardBuilder()
    if not lead.get("response_at"):
        b.button(text="❌ Отменить лид", callback_data=f"rp_lead:cancel:{lead['id']}")
    b.button(text="⬅️ Назад к списку", callback_data="rp_lead:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^rp_lead:cancel:\d+$"))
async def lead_cancel(cb: CallbackQuery, db: Database, config: Config) -> None:
    """РП отменяет отправленный лид."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    lead_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    lead = await db.cancel_lead(lead_id)
    if not lead:
        await cb.message.answer("❌ Лид не найден или уже удалён.")  # type: ignore[union-attr]
        return

    # Уведомить менеджера об отмене
    manager_id = lead.get("assigned_manager_id")
    if manager_id:
        notifier = Notifier(cb.bot, config)  # type: ignore[arg-type]
        await notifier.safe_send(
            int(manager_id),
            f"🚫 <b>Лид #{lead_id} отменён РП</b>\n"
            f"Задача по лиду удалена.",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Лид #{lead_id} удалён.",
        reply_markup=InlineKeyboardBuilder([
            [InlineKeyboardButton(text="⬅️ К лидам", callback_data="rp_lead:refresh")]
        ]).as_markup(),
    )


@router.callback_query(F.data == "rp_lead:stats")
async def lead_stats(cb: CallbackQuery, db: Database) -> None:
    """Статистика конверсии лидов."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    stats = await db.get_lead_stats()
    total = stats["total"]
    responded = stats["responded"]

    text = (
        f"📊 <b>Статистика лидов</b>\n\n"
        f"📋 Всего: <b>{total}</b>\n"
        f"✅ Обработано: <b>{responded}</b>\n"
        f"⏳ Ожидают: <b>{total - responded}</b>\n"
    )

    if stats["by_manager"]:
        text += "\n<b>По менеджерам:</b>\n"
        for entry in stats["by_manager"]:
            mgr_label = {
                "manager_kv": "КВ", "manager_kia": "КИА",
                "manager_npn": "НПН",
            }.get(entry.get("assigned_manager_role", ""), entry.get("assigned_manager_role", ""))
            avg_time = entry.get("avg_time")
            avg_str = ""
            if avg_time and avg_time > 0:
                if avg_time < 60:
                    avg_str = f" (ср. отклик: {int(avg_time)}мин)"
                else:
                    avg_str = f" (ср. отклик: {int(avg_time // 60)}ч)"
            text += f"  {mgr_label}: {entry['total']} лидов{avg_str}\n"

    if stats["by_source"]:
        text += "\n<b>По источникам:</b>\n"
        for entry in stats["by_source"]:
            source = entry.get("lead_source") or "—"
            text += f"  {source}: {entry['total']}\n"

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к лидам", callback_data="rp_lead:refresh")
    b.adjust(1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# ---------- Создание нового лида ----------

@router.callback_query(F.data == "rp_lead:create")
async def lead_create_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начать создание нового лида (Шаг 1: менеджер)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(LeadToProjectSG.pick_manager)
    await cb.message.answer(  # type: ignore[union-attr]
        "🎯 <b>Новый лид</b>\n\n"
        "Шаг 1/6: Выберите менеджера-получателя:",
        reply_markup=lead_pick_manager_kb(),
    )


@router.callback_query(F.data.startswith("lead_mgr:"))
async def lead_pick_manager(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    manager_role = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(manager_role=manager_role)
    await state.set_state(LeadToProjectSG.name)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 2/6: Введите <b>имя клиента</b>:"
    )


@router.message(LeadToProjectSG.name)
async def lead_enter_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Введите имя (минимум 2 символа):")
        return
    await state.update_data(name=text)
    await state.set_state(LeadToProjectSG.phone)
    await message.answer("Шаг 3/6: Введите <b>телефон</b>:")


@router.message(LeadToProjectSG.phone)
async def lead_enter_phone(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("Введите телефон (минимум 5 символов):")
        return
    await state.update_data(phone=text)
    await state.set_state(LeadToProjectSG.address)
    await message.answer("Шаг 4/6: Введите <b>адрес объекта</b>:")


@router.message(LeadToProjectSG.address)
async def lead_enter_address(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Введите адрес (минимум 2 символа):")
        return
    await state.update_data(address=text)
    await state.set_state(LeadToProjectSG.source)
    await message.answer(
        "Шаг 5/6: Выберите <b>источник лида</b>:",
        reply_markup=_lead_source_kb(),
    )


@router.callback_query(F.data == "lead:cancel")
async def lead_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Отмена создания лида."""
    await cb.answer("❌ Отменено")
    await state.clear()
    u = cb.from_user
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Создание лида отменено.", reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("lead_src:"))
async def lead_source_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Выбор источника лида из предустановленных."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    source_key = cb.data.split(":")[-1]  # type: ignore[union-attr]

    source_labels = {key: label for label, key in _LEAD_SOURCES}
    source_label = source_labels.get(source_key, source_key)

    if source_key == "other":
        await state.update_data(source_type="other")
        await cb.message.answer(  # type: ignore[union-attr]
            "Укажите <b>источник лида</b> вручную:"
        )
        return  # Stays in LeadToProjectSG.source, next text message will be handled

    await state.update_data(source=source_label, attachments=[])
    await state.set_state(LeadToProjectSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="lead:create")
    b.button(text="⏭ Без вложений", callback_data="lead:create")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📌 Источник: <b>{source_label}</b>\n\n"
        "Шаг 6/6: Прикрепите файлы/фото или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(LeadToProjectSG.source)
async def lead_source_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод источника лида (Другое)."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Укажите источник:")
        return
    await state.update_data(source=text, attachments=[])
    await state.set_state(LeadToProjectSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="lead:create")
    b.button(text="⏭ Без вложений", callback_data="lead:create")
    b.adjust(1)
    await message.answer(
        "Шаг 6/6: Прикрепите файлы/фото или нажмите «Отправить»:",
        reply_markup=b.as_markup(),
    )


@router.message(LeadToProjectSG.attachments)
async def lead_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"rp/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "lead:create")
async def lead_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    manager_role = data["manager_role"]
    lead_name = data["name"]
    lead_phone = data["phone"]
    lead_address = data["address"]
    source = data.get("source", "")
    attachments = data.get("attachments", [])

    # Find manager ID
    manager_id = config.get_role_id(manager_role)
    if not manager_id:
        # Try to find by role in DB
        manager_id_resolved = await resolve_default_assignee(db, config, manager_role)
        if manager_id_resolved:
            manager_id = int(manager_id_resolved)

    if not manager_id:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Менеджер {manager_role} не найден."
        )
        await state.clear()
        return

    # Create project to link lead → invoice chain
    project = await db.create_project(
        title=f"Лид: {lead_name}",
        address=None,
        client=None,
        amount=None,
        deadline_iso=None,
        status="lead",
        manager_id=manager_id,
        rp_id=u.id,
    )
    project_id = int(project["id"])

    lead_id = await db.create_lead_tracking(
        assigned_by=u.id,
        assigned_manager_id=manager_id,
        assigned_manager_role=manager_role,
        lead_source=source,
        project_id=project_id,
    )

    # Создать invoice для экспорта лида в Google Sheet
    from ..utils import utcnow
    role_suffix = {"manager_kv": "kv", "manager_kia": "kia", "manager_npn": "npn"}.get(manager_role, "npn")
    now_date = utcnow().strftime("%Y-%m-%d")

    invoice_id = await db.create_invoice(
        invoice_number=f"LEAD-{lead_id}",
        project_id=project_id,
        created_by=u.id,
        creator_role="rp",
        client_name=lead_name,
        description=f"{lead_name}, {lead_phone}, {lead_address}",
    )
    await db.update_invoice(invoice_id, **{
        f"lead_{role_suffix}_num": str(lead_id),
        f"lead_{role_suffix}_name": lead_name,
        f"lead_{role_suffix}_phone": lead_phone,
        f"lead_{role_suffix}_address": lead_address,
        f"lead_{role_suffix}_date": now_date,
        "lead_tracking_id": lead_id,
    })
    await db.link_lead_tracking(lead_id, invoice_id=invoice_id)

    task = await db.create_task(
        project_id=project_id,
        type_=TaskType.LEAD_TO_PROJECT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=manager_id,
        due_at_iso=None,
        payload={
            "lead_id": lead_id,
            "project_id": project_id,
            "name": lead_name,
            "phone": lead_phone,
            "address": lead_address,
            "source": source,
            "manager_role": manager_role,
            "assigned_role": manager_role,
        },
    )
    await db.link_lead_tracking(lead_id, task_id=int(task["id"]))

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    role_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(manager_role, manager_role)

    initiator = await get_initiator_label(db, u.id)
    from ..utils import build_manager_task_card
    try:
        msg = await build_manager_task_card(
            db, task, config.timezone,
            header_emoji="🎯", header_title="Новый лид от РП",
            actor_label=initiator,
        )
    except Exception:
        log.exception("lead_to_project: card render failed, fallback")
        msg = (
            f"🎯 <b>Новый лид от РП</b>\n"
            f"👤 От: {initiator}\n\n"
            f"👤 Имя: {lead_name}\n"
            f"📞 Телефон: {lead_phone}\n"
            f"📍 Адрес: {lead_address}\n"
            f"📌 Источник: {source}\n"
        )

    from ..keyboards import task_actions_kb
    await notifier.safe_send(manager_id, msg, reply_markup=task_actions_kb(task))
    for a in attachments:
        await notifier.safe_send_media(manager_id, a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, manager_id)

    menu_role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Лид отправлен {role_label}.\n"
        f"📌 Источник: {source}",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(u.id),
                rp_messages=await db.count_rp_role_messages(u.id),
            ),
        ),
    )


# =====================================================================
# СМЕНА РОЛИ РП ↔ НПН (кнопки в первой строке меню)
# =====================================================================

@router.message(lambda m: m.text and any(m.text.startswith(p) for p in (RP_BTN_ROLE_NPN, RP_BTN_ROLE_RP_INACTIVE)))
async def role_switch_to_other(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Switch to the other role (RP->NPN or NPN->RP) when clicking the inactive role button."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return
    await state.clear()

    u = message.from_user
    if not u:
        return

    role = await _current_role(db, u.id)

    # Determine target role
    if role == Role.RP:
        target_role = Role.MANAGER_NPN
        role_label_str = "Менеджер НПН"
    else:
        target_role = Role.RP
        role_label_str = "РП"

    # Switch role in DB — keep both RP+NPN, target first (= active menu)
    # Write directly to bypass parse_roles sorting which would reorder roles
    current_user = await db.get_user_optional(u.id)
    current_roles_set = set(parse_roles(current_user.role if current_user else None))
    other_role = Role.RP if target_role == Role.MANAGER_NPN else Role.MANAGER_NPN
    new_roles: list[str] = [target_role, other_role]
    for r in current_roles_set:
        if r not in {Role.RP, Role.MANAGER_NPN}:
            new_roles.append(r)
    role_str = ",".join(new_roles)
    await db.conn.execute(
        "UPDATE users SET role = ?, updated_at = datetime('now') WHERE telegram_id = ?",
        (role_str, u.id),
    )
    await db.conn.commit()

    is_admin = u.id in (config.admin_ids or set())
    full_role = ",".join(new_roles)  # pass full DB role for combined menu detection
    from ..services.menu_context import build_menu_context
    menu_ctx = await build_menu_context(db, u.id, full_role)
    await message.answer(
        f"✅ Роль изменена на: <b>{role_label_str}</b>",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(full_role, is_admin=is_admin, **menu_ctx),
        ),
    )


@router.message(lambda m: m.text and any(m.text.startswith(p) for p in (RP_BTN_ROLE_RP, RP_BTN_ROLE_NPN_ACTIVE)))
async def role_switch_already_active(message: Message, db: Database, config: Config) -> None:
    """User clicked the already-active role button — just refresh the menu."""
    if not await require_role_message(message, db, roles=[Role.RP, Role.MANAGER_NPN]):
        return

    u = message.from_user
    if not u:
        return

    current_user = await db.get_user_optional(u.id)
    full_role = current_user.role if current_user else None
    role = await _current_role(db, u.id)
    is_admin = u.id in (config.admin_ids or set())
    role_label_str = "РП" if role == Role.RP else "Менеджер НПН"

    from ..services.menu_context import build_menu_context
    menu_ctx = await build_menu_context(db, u.id, full_role)
    await message.answer(
        f"Вы уже в роли <b>{role_label_str}</b>.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(full_role, is_admin=is_admin, **menu_ctx),
        ),
    )


# =====================================================================
# ПОИСК СЧЕТА — обрабатывается в manager_new.py (принимает Role.RP)
# =====================================================================


# =====================================================================
# ОТВЕТ НА КП ОТ МЕНЕДЖЕРА — полный flow (Этап 5)
#
# Callback prefixes:
#   kp_review:\d+     — inline-кнопка из уведомления менеджера (открывает карточку)
#   kp_resp:view:\d+  — просмотр карточки задачи
#   kp_resp:yes:\d+   — Да → выбор типа оплаты
#   kp_resp:no:\d+    — Нет → FSM reject_comment
#   kp_resp:bn:\d+    — б/н → FSM documents
#   kp_resp:cred:\d+  — Кред → FSM comment (без документов)
#   kp_resp:back      — назад к списку CHECK_KP задач
#   kp_resp:issued    — Выставленные счета
#   kp_issued:view:\d+ — просмотр выставленного счёта
# =====================================================================


async def _show_kp_task_card(
    target: CallbackQuery,
    db: Database,
    task_id: int,
) -> None:
    """Показать карточку CHECK_KP задачи с кнопками Да/Нет."""
    task = await db.get_task(task_id)
    if not task:
        await target.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    payload = json.loads(task.get("payload_json") or "{}")
    invoice_number = payload.get("invoice_number", "?")
    address = payload.get("address", "—")
    amount = payload.get("amount", 0)
    comment = payload.get("comment", "")
    manager_role = payload.get("manager_role", "manager")
    manager_id = payload.get("manager_id")

    mgr_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
    }.get(manager_role, "Менеджер")

    # Get manager name
    mgr_name = mgr_label
    if manager_id:
        mgr_name = await get_initiator_label(db, int(manager_id))
        mgr_name = f"{mgr_name} ({mgr_label})"

    try:
        amount_str = f"{float(amount):,.0f}₽"
    except (ValueError, TypeError):
        amount_str = f"{amount}₽"

    client_name = payload.get("client_name", "")
    flow_type = payload.get("flow_type", "")
    lead_source = payload.get("lead_source", "")
    invoice_number = payload.get("invoice_number")
    flow_label = "📌 Лид" if flow_type == "lead" else "🆕 Новый клиент"

    text = f"📋 <b>Проверка КП — карточка</b> ({flow_label})\n\n"
    if invoice_number:
        text += f"📄 Счёт №: <code>{invoice_number}</code>\n"
    if client_name:
        text += f"🏢 Клиент: {client_name}\n"
    text += (
        f"📍 Адрес: {address}\n"
        f"💰 Сумма: {amount_str}\n"
        f"👤 От: {mgr_name}\n"
    )
    if lead_source:
        text += f"📌 Источник: {lead_source}\n"
    if comment:
        text += f"💬 Комментарий: {comment}\n"

    text += (
        f"\n📅 Создан: {task.get('created_at', '-')[:10]}\n"
        f"\n<b>Ваше решение:</b>"
    )

    await target.message.answer(  # type: ignore[union-attr]
        text,
        reply_markup=kp_response_kb(task_id),
    )

    # Show attached КП documents
    attachments = await db.list_attachments(int(task["id"]))
    if attachments:
        chat_id = target.from_user.id  # type: ignore[union-attr]
        bot = target.bot
        for att in attachments:
            fid = att["tg_file_id"]
            cap = att.get("caption")
            if att.get("file_type") == "photo":
                await bot.send_photo(chat_id=chat_id, photo=fid, caption=cap)
            elif att.get("file_type") == "video":
                await bot.send_video(chat_id=chat_id, video=fid, caption=cap)
            else:
                await bot.send_document(chat_id=chat_id, document=fid, caption=cap)


@router.callback_query(F.data.regexp(r"^kp_review:\d+$"))
async def kp_review_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Inline-кнопка из уведомления менеджера → показать карточку задачи."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _show_kp_task_card(cb, db, task_id)


@router.callback_query(F.data.regexp(r"^kp_resp:view:\d+$"))
async def kp_view_task(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Просмотр карточки CHECK_KP задачи из списка."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _show_kp_task_card(cb, db, task_id)


@router.callback_query(F.data == "kp_resp:back")
async def kp_back_to_list(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Назад к списку входящих CHECK_KP задач."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.clear()

    u = cb.from_user
    if not u:
        return

    tasks = await db.list_check_kp_tasks(u.id)
    if not tasks:
        await cb.message.answer(  # type: ignore[union-attr]
            "📋 Входящих запросов на проверку КП нет ✅",
        )
        return

    mgr_counts: dict[str, int] = {}
    for t in tasks:
        payload = json.loads(t.get("payload_json") or "{}")
        mrole = payload.get("manager_role", "manager")
        lbl = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}.get(mrole, "Менеджер")
        mgr_counts[lbl] = mgr_counts.get(lbl, 0) + 1
    summary_parts = [f"{lbl}: {cnt}" for lbl, cnt in mgr_counts.items()]

    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 <b>Проверка КП / Выставление Счета</b>\n\n"
        f"Входящих запросов: <b>{len(tasks)}</b>\n"
        f"По менеджерам: {', '.join(summary_parts)}\n\n"
        "Нажмите на задачу для просмотра:",
        reply_markup=kp_task_list_kb(tasks, show_issued=True),
    )


# ---------- ДА → Ввод номера счёта ----------

@router.callback_query(F.data.regexp(r"^kp_resp:yes:\d+$"))
async def kp_resp_yes(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП нажал Да → сразу нужная ветка по системе оплаты из задачи.

    Систему оплаты (б/н / кредит) менеджер уже указал при формировании задачи
    (CheckKpSG → payload is_credit). Повторно у РП НЕ спрашиваем — берём из payload
    и сразу ведём в соответствующую ветку: кред → номер счёта (банк оформляет доки),
    б/н → сбор документов.
    """
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.update_data(task_id=task_id)

    payload = json.loads(task.get("payload_json") or "{}")
    is_credit = bool(payload.get("is_credit"))

    if is_credit:
        # Кред: документы оформляет банк → сразу ввод номера счёта
        await state.set_state(KpReviewSG.invoice_number)
        await state.update_data(payment_type="cred", documents=[])
        await cb.message.answer(  # type: ignore[union-attr]
            "🏦 <b>Ответ на КП (Кред)</b>\n\n"
            "Документы не требуются (банк оформляет самостоятельно).\n\n"
            "Введите <b>номер счёта</b>:",
        )
    else:
        # б/н: сбор документов (Счёт, Договор, Приложение)
        await state.set_state(KpReviewSG.documents)
        await state.update_data(payment_type="bn", documents=[])
        await cb.message.answer(  # type: ignore[union-attr]
            "📋 <b>Ответ на КП (б/н)</b>\n\n"
            "Прикрепите готовые документы:\n"
            "• Счёт\n"
            "• Договор\n"
            "• Приложение к договору\n\n"
            "Отправляйте файлы по одному.",
        )


# ---------- б/н (безналичный) → Документы → Комментарий ----------

@router.callback_query(F.data.regexp(r"^kp_resp:bn:\d+$"))
async def kp_resp_bn(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """б/н выбран → FSM: сбор документов (Счёт, Договор, Приложение)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.set_state(KpReviewSG.documents)
    await state.update_data(payment_type="bn", documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "📋 <b>Ответ на КП (б/н)</b>\n\n"
        "Прикрепите готовые документы:\n"
        "• Счёт\n"
        "• Договор\n"
        "• Приложение к договору\n\n"
        "Отправляйте файлы по одному.",
    )


@router.message(KpReviewSG.documents)
async def kp_review_documents(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Сбор документов для б/н ответа на КП."""
    uid = message.from_user.id if message.from_user else "anon"
    att, doc_count = await collect_attachment(
        message, state, storage, prefix=f"rp/{uid}", key="documents"
    )
    if att is None:
        if doc_count:
            # Текстовое сообщение = переход к номеру счёта
            await state.set_state(KpReviewSG.invoice_number)
            await message.answer("Введите <b>номер счёта</b>:")
            return
        await message.answer("Пришлите файл или фото:")
        return
    b = InlineKeyboardBuilder()
    b.button(text="✅ Далее (комментарий)", callback_data="kp_review:next")
    b.adjust(1)
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await message.answer(
        f"📎 Принял. Документов: <b>{doc_count}</b>.{suffix}\n"
        "Ещё файлы или нажмите «Далее».",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "kp_review:next")
async def kp_review_next(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Кнопка 'Далее' → переход к номеру счёта."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()
    await state.set_state(KpReviewSG.invoice_number)
    await cb.message.answer(  # type: ignore[union-attr]
        "Введите <b>номер счёта</b>:"
    )


# ---------- Ввод номера счёта (после документов б/н или сразу для Кред) ----------

@router.message(KpReviewSG.invoice_number)
async def kp_review_invoice_number(message: Message, state: FSMContext, db: Database) -> None:
    """РП вводит номер счёта → проверка дублей → комментарий."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите номер счёта:")
        return

    existing = await db.get_invoice_by_number(text)
    if existing:
        await message.answer(
            f"⚠️ Счёт №{text} уже существует в базе.\n"
            "Введите другой номер:"
        )
        return

    await state.update_data(invoice_number=text)
    await state.set_state(KpReviewSG.comment)
    await message.answer(
        f"✅ Номер счёта: <b>№{text}</b>\n\n"
        "Добавьте <b>комментарий</b> (или «—» для пропуска):"
    )


# ---------- Кред (кредит) → Комментарий (без документов) ----------

@router.callback_query(F.data.regexp(r"^kp_resp:cred:\d+$"))
async def kp_resp_cred(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Кред выбран → FSM: номер счёта (документы не требуются)."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.set_state(KpReviewSG.invoice_number)
    await state.update_data(payment_type="cred", documents=[])

    await cb.message.answer(  # type: ignore[union-attr]
        "🏦 <b>Ответ на КП (Кред)</b>\n\n"
        "Документы не требуются (банк оформляет самостоятельно).\n\n"
        "Введите <b>номер счёта</b>:",
    )


# ---------- Комментарий (Да — б/н или Кред) → Финализация ----------

@router.message(KpReviewSG.comment)
async def kp_review_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Финализация ответа «Да» → РП создаёт invoice с введённым номером."""
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    task_id = data["task_id"]
    invoice_number = data.get("invoice_number", "")
    payment_type = data.get("payment_type", "bn")
    documents = data.get("documents", [])

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json") or "{}")
    manager_id = payload.get("manager_id")
    manager_role = payload.get("manager_role", "manager")
    client_name = payload.get("client_name", "")
    address = payload.get("address", "")
    amount = payload.get("amount", 0)
    p_payment_type = payload.get("payment_type", "")
    deadline_days = payload.get("deadline_days")

    is_credit = payment_type == "cred"

    # РП создаёт project + invoice
    project = await db.create_project(
        title=f"Счёт: {invoice_number}",
        address=address or None,
        client=client_name or None,
        amount=float(amount) if amount else None,
        deadline_iso=None,
        status="active",
        manager_id=int(manager_id) if manager_id else None,
        rp_id=message.from_user.id,
    )
    project_id = int(project["id"])

    inv_id: int | None = None
    if invoice_number:
        try:
            inv_id = await db.create_invoice(
                invoice_number=invoice_number,
                project_id=project_id,
                created_by=int(manager_id) if manager_id else message.from_user.id,
                creator_role=manager_role,
                client_name=client_name,
                object_address=address,
                amount=float(amount) if amount else 0.0,
                description=comment,
                payment_terms=p_payment_type,
                deadline_days=deadline_days,
            )
        except (ValueError, Exception) as exc:
            log.warning("create_invoice failed: %s", exc)
            await message.answer(
                f"⚠️ Ошибка при создании счёта №{invoice_number}: {exc}\n"
                "Попробуйте ещё раз."
            )
            await state.clear()
            return

    # Update invoice status
    if inv_id:
        upd: dict[str, Any] = {}
        if is_credit:
            upd["is_credit"] = 1
            upd["status"] = InvoiceStatus.CREDIT
        else:
            upd["is_credit"] = 0
            upd["status"] = InvoiceStatus.PENDING_PAYMENT

        # Фиксация inv_* полей по роли менеджера
        _role_suf = {"manager_kv": "kv", "manager_kia": "kia", "manager_npn": "npn"}.get(manager_role, "")
        if _role_suf:
            from ..utils import utcnow as _utcnow
            upd[f"inv_{_role_suf}_num"] = invoice_number
            upd[f"inv_{_role_suf}_name"] = client_name
            upd[f"inv_{_role_suf}_address"] = address
            upd[f"inv_{_role_suf}_date"] = _utcnow().strftime("%Y-%m-%d")
        await db.update_invoice(inv_id, **upd)

        try:
            await db.audit(
                actor_id=message.from_user.id,
                action="invoice_kp_finalized",
                entity="invoice",
                entity_id=str(inv_id),
                payload={
                    "invoice_number": invoice_number,
                    "project_id": project_id,
                    "manager_id": manager_id,
                    "manager_role": manager_role,
                    "payment_type": payment_type,
                    "is_credit": bool(is_credit),
                    "amount": amount,
                    "client_name": client_name,
                    "address": address,
                    "has_documents": bool(documents),
                    "source_task_id": task_id,
                    "comment_present": bool(comment),
                },
            )
        except Exception:
            log.exception("kp_review_comment: audit() failed for invoice=%s", inv_id)

        # Лид → "счет выставлен"
        try:
            await db.update_lead_to_invoice_issued(
                project_id, inv_id,
                manager_id=int(manager_id) if manager_id else None,
                manager_role=manager_role,
            )
        except Exception:
            log.warning("Failed to update lead status for project_id=%s", project_id)

    # Mark task as done, update payload with invoice info + ответ РП (для подсписка у менеджера)
    await db.update_task_status(task_id, TaskStatus.DONE)
    from ..utils import utcnow as _utcnow_payload, to_iso as _to_iso_payload
    payload["invoice_id"] = inv_id
    payload["invoice_number"] = invoice_number
    payload["response_documents"] = documents
    payload["response_comment"] = comment
    payload["response_payment_type"] = payment_type
    payload["response_is_credit"] = bool(is_credit)
    payload["response_finalized_at"] = _to_iso_payload(_utcnow_payload())
    payload["responder_id"] = message.from_user.id
    await db.conn.execute(
        "UPDATE tasks SET payload_json = ? WHERE id = ?",
        (json.dumps(payload, ensure_ascii=False), task_id),
    )
    await db.conn.commit()

    # Notify manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)

        if is_credit:
            msg = (
                f"🏦 <b>Счёт №{invoice_number} — Кред</b>\n"
                f"👤 От: {initiator}\n\n"
                f"РП одобрил КП и выставил счёт.\n"
                f"Система оплаты: <b>Кредит</b>\n"
                f"Документы оформляет банк.\n"
            )
        else:
            msg = (
                f"📋 <b>Счёт №{invoice_number} выставлен</b>\n"
                f"👤 От: {initiator}\n\n"
                f"РП проверил КП и подготовил документы.\n"
                f"Система оплаты: <b>б/н</b>\n"
            )

        if comment:
            msg += f"\n💬 Комментарий РП: {comment}"

        confirm_kb = InlineKeyboardBuilder()
        confirm_kb.button(
            text="✅ Задача ок",
            callback_data=f"mgr_kp_ok:{task_id}",
        )
        await notifier.safe_send(
            int(manager_id), msg, reply_markup=confirm_kb.as_markup(),
        )

        if not is_credit:
            for doc in documents:
                await notifier.safe_send_media(
                    int(manager_id), doc["file_type"], doc["file_id"],
                    caption=doc.get("caption"),
                )

        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    credit_label = " (Кред)" if is_credit else ""
    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Счёт №{invoice_number} выставлен{credit_label}. Менеджер уведомлён.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(message.from_user.id),
                rp_messages=await db.count_rp_role_messages(message.from_user.id),
            ),
        ),
    )


# ---------- НЕТ → Комментарий → Отклонение ----------

@router.callback_query(F.data.regexp(r"^kp_resp:no:\d+$"))
async def kp_resp_no(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """РП нажал Нет → FSM: ввод комментария к отклонению."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(KpReviewSG.reject_comment)
    await state.update_data(task_id=task_id)

    await cb.message.answer(  # type: ignore[union-attr]
        "❌ <b>Отклонение КП</b>\n\n"
        "Укажите <b>причину отклонения</b> (комментарий):",
    )


@router.message(KpReviewSG.reject_comment)
async def kp_reject_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Финализация отклонения (Нет)."""
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if not comment:
        await message.answer("Напишите причину отклонения:")
        return

    data = await state.get_data()
    task_id = data["task_id"]

    task = await db.get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await state.clear()
        return

    payload = json.loads(task.get("payload_json") or "{}")
    manager_id = payload.get("manager_id")
    invoice_number = payload.get("invoice_number", "?")
    invoice_id = payload.get("invoice_id")

    # Mark task as rejected
    await db.update_task_status(task_id, TaskStatus.REJECTED)

    # Save РП-response в payload (для подсписка у менеджера)
    from ..utils import utcnow as _utcnow_payload, to_iso as _to_iso_payload
    payload["response_documents"] = []
    payload["response_comment"] = comment
    payload["response_rejected"] = True
    payload["response_finalized_at"] = _to_iso_payload(_utcnow_payload())
    payload["responder_id"] = message.from_user.id
    await db.conn.execute(
        "UPDATE tasks SET payload_json = ? WHERE id = ?",
        (json.dumps(payload, ensure_ascii=False), task_id),
    )
    await db.conn.commit()

    # Update invoice status
    if invoice_id:
        await db.update_invoice(invoice_id, status=InvoiceStatus.REJECTED)

    # Notify manager
    if manager_id:
        initiator = await get_initiator_label(db, message.from_user.id)
        msg = (
            f"❌ <b>КП по счёту №{invoice_number} отклонён</b>\n"
            f"👤 От: {initiator}\n\n"
            f"💬 Причина: {comment}\n"
        )
        await notifier.safe_send(int(manager_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"❌ КП по счёту №{invoice_number} отклонён. Менеджер уведомлён.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
                rp_tasks=await db.count_rp_role_tasks(message.from_user.id),
                rp_messages=await db.count_rp_role_messages(message.from_user.id),
            ),
        ),
    )


# ---------- ВЫСТАВЛЕННЫЕ СЧЕТА ----------

@router.callback_query(F.data == "kp_resp:issued")
async def kp_issued_list(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Показать «Выставленные счета» — обработанные РП."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoices = await db.list_rp_issued_invoices(limit=30)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "📑 <b>Выставленные счета</b>\n\nСписок пуст.",
        )
        return

    # Count by type
    n_bn = sum(1 for inv in invoices if not inv.get("is_credit"))
    n_cred = sum(1 for inv in invoices if inv.get("is_credit"))

    header = f"📑 <b>Выставленные счета</b> ({len(invoices)})\n"
    if n_bn > 0:
        header += f"💳 б/н: {n_bn}"
    if n_cred > 0:
        header += f"  🏦 Кред: {n_cred}"
    header += "\n\nНажмите для просмотра:"

    await cb.message.answer(  # type: ignore[union-attr]
        header,
        reply_markup=kp_issued_list_kb(invoices),
    )


@router.callback_query(F.data.regexp(r"^kp_issued:view:\d+$"))
async def kp_issued_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр карточки выставленного счёта."""
    if not await require_role_callback(cb, db, roles=[Role.RP]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    creator_label = "—"
    if inv.get("created_by"):
        creator_label = await get_initiator_label(db, int(inv["created_by"]))

    # kp_issued — без ЗП-условия (счёт ещё не закрыт, ЗП не релевантна на этом этапе).
    section = await build_invoice_section(db, inv, invoice_id, include_zp=False)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
    )

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="kp_resp:issued")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
