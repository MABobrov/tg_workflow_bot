"""
Handlers for Zamery (Замерщик) role.

Covers:
- Замеры (incoming requests, respond with blanks)
- Мои объекты (list, ZP status)
- Расчёт ЗП (ZameryZpSG) — Дополнение 3
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import (
    InvoiceStatus, Role, TaskStatus,
    ZAMERY_SOURCE_LABELS,
)
from ..keyboards import (
    ZAM_BTN_MY_OBJECTS,
    ZAM_BTN_PAYMENT,
    ZAM_BTN_SCHEDULE,
    ZAM_BTN_ZAMERY,
    main_menu,
    zamery_incoming_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import ZameryAcceptSG, ZameryBlackoutSG, ZameryCompleteSG, ZameryCostEditSG, ZameryQuickBookSG, ZameryZpSG
from ..utils import answer_service, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


@router.message.outer_middleware()
async def _zamery_auto_refresh(handler, event: Message, data: dict):  # type: ignore[type-arg]
    """При каждом сообщении от замерщика — обновляем reply-клавиатуру."""
    result = await handler(event, data)
    u = event.from_user
    if not u:
        return result
    # Не обновлять меню, если замерщик сейчас в FSM-состоянии (ввод стоимости и т.п.)
    fsm: FSMContext | None = data.get("state")
    if fsm:
        cur_state = await fsm.get_state()
        if cur_state is not None:
            return result
    db_inst: Database | None = data.get("db")
    cfg = data.get("config")
    if not db_inst or not cfg:
        return result
    try:
        user = await db_inst.get_user_optional(u.id)
        if not user or not user.role:
            return result
        from ..enums import parse_roles
        if Role.ZAMERSCHIK not in parse_roles(user.role):
            return result
        menu_role, isolated = resolve_menu_scope(u.id, user.role)
        if menu_role != Role.ZAMERSCHIK:
            return result
        unread = await db_inst.count_unread_tasks(u.id)
        uc = await db_inst.count_unread_by_channel(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
            unread_channels=uc,
            isolated_role=isolated,
        )
        # Тихое обновление — сообщение удалится через 1сек, reply_markup обновится
        await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
    except Exception:
        log.debug("zamery auto-refresh failed", exc_info=True)
    return result


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


# =====================================================================
# ЗАМЕРЫ (incoming requests — структурированные заявки)
# =====================================================================

@router.message(F.text == ZAM_BTN_ZAMERY)
async def zamery_inbox(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return
    uid = message.from_user.id  # type: ignore[union-attr]

    reqs = await db.list_zamery_requests(
        assigned_to=uid, status="open", limit=30,
    )
    in_progress = await db.list_zamery_requests(
        assigned_to=uid, status="in_progress", limit=30,
    )
    all_reqs = reqs + in_progress

    if not all_reqs:
        await answer_service(message, "📐 Нет входящих заявок на замеры ✅", delay_seconds=60)
        return

    # Статистика по менеджерам
    stats = await db.get_zamery_stats_by_manager(uid)
    stat_lines = []
    role_short = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }
    for s in stats:
        rn = role_short.get(s.get("requester_role", ""), s.get("requester_role", "?"))
        stat_lines.append(f"  {rn}: {s['cnt']} заявок")

    text = f"📐 <b>Замеры</b> ({len(all_reqs)}):\n\n"
    if stat_lines:
        text += "<b>По менеджерам:</b>\n" + "\n".join(stat_lines) + "\n\n"
    text += "Нажмите на заявку для просмотра:"

    await message.answer(text, reply_markup=zamery_incoming_kb(all_reqs, back_callback="nav:home"))


@router.callback_query(F.data.startswith("zam_in:view:"))
async def zamery_view_request(
    cb: CallbackQuery, db: Database,
) -> None:
    """Замерщик: просмотр карточки заявки."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    source_label = ZAMERY_SOURCE_LABELS.get(req.get("source_type", ""), "—")
    role_short = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(req.get("requester_role", ""), "?")
    initiator = await get_initiator_label(db, req["requested_by"])
    status_label = {
        "open": "⏳ Новая", "in_progress": "🔄 В работе",
        "done": "✅ Выполнена", "rejected": "❌ Отклонена",
    }.get(req.get("status", ""), "❓")

    text = (
        f"📐 <b>Заявка на замер #{req['id']}</b>\n\n"
        f"👤 Менеджер: {initiator}"
    )
    if role_short:
        text += f" ({role_short})"
    text += f"\n📍 Адрес: {req.get('address', '—')}\n"
    if req.get("client_contact"):
        text += f"📞 Контакт: <code>{req['client_contact']}</code>\n"
    if req.get("volume_m2"):
        text += f"📊 Объём: {req['volume_m2']} м²\n"
    mkad_km = req.get("mkad_km") or 0
    mkad_surcharge = req.get("mkad_surcharge") or 0
    if mkad_km and mkad_km > 0:
        if mkad_surcharge:
            text += f"📍 МКАД: {mkad_km} км (наценка: {mkad_surcharge}₽)\n"
        else:
            text += f"📍 МКАД: {mkad_km} км\n"
    else:
        text += "📍 МКАД: внутри МКАД\n"
    total_cost = req.get("total_cost")
    if total_cost:
        text += f"💰 Стоимость замера: <b>{total_cost}₽</b>\n"
    if req.get("description"):
        text += f"\n📝 Описание: {req['description']}\n"
    text += f"📌 Источник: {source_label}\n"
    text += f"📊 Статус: {status_label}\n"
    text += f"📅 Создана: {req.get('created_at', '—')[:16]}\n"

    b = InlineKeyboardBuilder()
    if req.get("status") in ("open", "in_progress"):
        if req.get("status") == "open":
            b.button(text="✅ Принять", callback_data=f"zam_in:accept:{req_id}")
        b.button(text="❌ Отклонить", callback_data=f"zam_in:reject:{req_id}")
        b.adjust(2)

    # Показать вложения
    attachments = []
    if req.get("attachments_json"):
        try:
            attachments = json.loads(req["attachments_json"])
        except (json.JSONDecodeError, TypeError):
            pass

    await cb.message.answer(text, reply_markup=b.as_markup() if b.export() else None)  # type: ignore[union-attr]
    for a in attachments:
        ft = a.get("file_type", "document")
        fid = a.get("file_id")
        if fid and ft == "photo":
            await cb.message.answer_photo(fid)  # type: ignore[union-attr]
        elif fid:
            await cb.message.answer_document(fid)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zam_in:accept:"))
async def zamery_accept_request(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Замерщик: начало принятия заявки → выбор действия."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.update_data(accept_req_id=req_id)
    await state.set_state(ZameryAcceptSG.choose_action)

    b = InlineKeyboardBuilder()
    b.button(text="📅 Назначить дату", callback_data="zam_acc:date")
    b.button(text="💬 Комментарий", callback_data="zam_acc:comment")
    b.button(text="⏭ Принять без комментария", callback_data="zam_acc:skip")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📐 <b>Принятие заявки #{req_id}</b>\n\n"
        "Выберите действие:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryAcceptSG.choose_action, F.data == "zam_acc:date")
async def zamery_acc_pick_date(cb: CallbackQuery, state: FSMContext) -> None:
    """Показать 7 дней для выбора."""
    await cb.answer()
    from datetime import date, timedelta
    today = date.today()
    day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    b = InlineKeyboardBuilder()
    for i in range(7):
        d = today + timedelta(days=i)
        label = f"{day_names[d.weekday()]} {d.strftime('%d.%m')}"
        b.button(text=label, callback_data=f"zam_date:{d.isoformat()}")
    b.adjust(4, 3)
    await state.set_state(ZameryAcceptSG.pick_date)
    await cb.message.answer(  # type: ignore[union-attr]
        "📅 Выберите дату замера:", reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryAcceptSG.pick_date, F.data.startswith("zam_date:"))
async def zamery_acc_pick_time(cb: CallbackQuery, state: FSMContext) -> None:
    """Показать интервалы времени (2 часа)."""
    await cb.answer()
    chosen_date = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(scheduled_date=chosen_date)
    await state.set_state(ZameryAcceptSG.pick_time)

    b = InlineKeyboardBuilder()
    intervals = ["08:00-10:00", "10:00-12:00", "12:00-14:00", "14:00-16:00", "16:00-18:00"]
    for interval in intervals:
        b.button(text=interval, callback_data=f"zam_time:{interval}")
    b.adjust(3, 2)
    from datetime import date as date_cls
    d = date_cls.fromisoformat(chosen_date)
    day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    await cb.message.answer(  # type: ignore[union-attr]
        f"📅 {day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}\n\n"
        "⏰ Выберите временной интервал:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryAcceptSG.pick_time, F.data.startswith("zam_time:"))
async def zamery_acc_time_chosen(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Время выбрано → финализировать принятие."""
    await cb.answer()
    interval = cb.data.split(":", 1)[-1]  # type: ignore[union-attr]
    await state.update_data(scheduled_time_interval=interval)
    await _finalize_accept(cb, state, db, config, notifier)


@router.callback_query(ZameryAcceptSG.choose_action, F.data == "zam_acc:comment")
async def zamery_acc_enter_comment(cb: CallbackQuery, state: FSMContext) -> None:
    """Переход к вводу комментария."""
    await cb.answer()
    await state.set_state(ZameryAcceptSG.comment)
    await cb.message.answer("💬 Введите комментарий:")  # type: ignore[union-attr]


@router.message(ZameryAcceptSG.comment)
async def zamery_acc_comment_text(
    message: Message, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Получен комментарий → финализировать принятие."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите комментарий:")
        return
    await state.update_data(accept_comment=text)
    await _finalize_accept(message, state, db, config, notifier)


@router.callback_query(ZameryAcceptSG.choose_action, F.data == "zam_acc:skip")
async def zamery_acc_skip(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Принять без комментария."""
    await cb.answer()
    await _finalize_accept(cb, state, db, config, notifier)


async def _finalize_accept(
    event: Message | CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Общая функция финализации принятия заявки."""
    from ..utils import to_iso, utcnow
    data = await state.get_data()
    req_id = data["accept_req_id"]
    req = await db.get_zamery_request(req_id)
    if not req:
        msg = event.message if isinstance(event, CallbackQuery) else event
        await msg.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    update_fields: dict[str, Any] = {
        "status": "in_progress",
        "accepted_at": to_iso(utcnow()),
    }
    if data.get("scheduled_date"):
        update_fields["scheduled_date"] = data["scheduled_date"]
    if data.get("scheduled_time_interval"):
        update_fields["scheduled_time_interval"] = data["scheduled_time_interval"]
    if data.get("accept_comment"):
        update_fields["accept_comment"] = data["accept_comment"]

    await db.update_zamery_request(req_id, **update_fields)
    if req.get("task_id"):
        await db.accept_task(int(req["task_id"]))
        await db.update_task_status(req["task_id"], TaskStatus.IN_PROGRESS)

    msg_target = event.message if isinstance(event, CallbackQuery) else event
    await msg_target.answer(f"✅ Заявка #{req_id} принята в работу.")  # type: ignore[union-attr]

    # Уведомить менеджера
    notify_text = (
        f"✅ <b>Заявка на замер #{req_id} принята</b>\n\n"
        f"📍 {req.get('address', '—')}\n"
    )
    if data.get("scheduled_date"):
        from datetime import date as date_cls
        d = date_cls.fromisoformat(data["scheduled_date"])
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        notify_text += f"📅 Дата: {day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}\n"
    if data.get("scheduled_time_interval"):
        notify_text += f"⏰ Время: {data['scheduled_time_interval']}\n"
    if data.get("accept_comment"):
        notify_text += f"💬 Комментарий: {data['accept_comment']}\n"

    await notifier.safe_send(req["requested_by"], notify_text)
    await refresh_recipient_keyboard(notifier, db, config, req["requested_by"])

    # Обновить клавиатуру замерщика (бейдж на кнопке «Замеры»)
    uid = event.from_user.id if isinstance(event, (Message, CallbackQuery)) and event.from_user else None
    if uid:
        await refresh_recipient_keyboard(notifier, db, config, uid)
    await state.clear()


@router.callback_query(F.data.startswith("zam_in:reject:"))
async def zamery_reject_request(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Замерщик: отклонить заявку на замер."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer("❌ Отклонено")

    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    await db.update_zamery_request(req_id, status="rejected")
    if req.get("task_id"):
        await db.update_task_status(req["task_id"], TaskStatus.REJECTED)

    await cb.message.answer(  # type: ignore[union-attr]
        f"❌ Заявка #{req_id} отклонена."
    )

    # Уведомить менеджера
    await notifier.safe_send(
        req["requested_by"],
        f"❌ <b>Заявка на замер #{req_id} отклонена</b>\n\n"
        f"📍 {req.get('address', '—')}\n"
        "Свяжитесь с замерщиком для уточнения.",
    )
    await refresh_recipient_keyboard(notifier, db, config, req["requested_by"])
    # Обновить бейдж замерщика
    await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)


# =====================================================================
# МОИ ОБЪЕКТЫ (с кнопками «Расчёт ЗП» для подходящих счетов)
# =====================================================================

@router.message(F.text == ZAM_BTN_MY_OBJECTS)
async def zamery_my_objects(message: Message, db: Database) -> None:
    """📋 Мои замеры — дашборд: конверсия + подменю."""
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return

    user_id = message.from_user.id  # type: ignore[union-attr]

    # Конверсия
    conv = await db.get_zamery_conversion_stats(user_id)
    role_short = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}

    text = "📋 <b>Мои замеры</b>\n\n"
    text += f"📊 <b>Конверсия:</b> {conv['conversion_pct']}% "
    text += f"({conv['total_with_invoice']} счетов из {conv['total_done']} замеров)\n"
    if conv["by_role"]:
        parts = []
        for r in conv["by_role"]:
            rn = role_short.get(r.get("requester_role", ""), "?")
            parts.append(f"{rn}: {r['pct']}%")
        text += "По менеджерам: " + " | ".join(parts) + "\n"

    # Кол-во активных заявок и счетов
    active_reqs = await db.list_zamery_requests(assigned_to=user_id, status="in_progress", limit=50)
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    active_inv = [i for i in invoices if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
    )]

    b = InlineKeyboardBuilder()
    b.button(
        text=f"📐 Заявки на замер ({len(active_reqs)})",
        callback_data="zam_my:requests",
    )
    b.button(
        text=f"📋 Счета в работе ({len(active_inv)})",
        callback_data="zam_my:invoices",
    )
    b.adjust(1)
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "zam_my:requests")
async def zamery_my_requests(cb: CallbackQuery, db: Database) -> None:
    """Подменю: заявки на замер (in_progress)."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id
    reqs = await db.list_zamery_requests(assigned_to=user_id, status="in_progress", limit=30)
    # Также показать done для истории (последние 10)
    done_reqs = await db.list_zamery_requests(assigned_to=user_id, status="done", limit=10)

    all_reqs = reqs + done_reqs
    if not all_reqs:
        await cb.message.answer("📐 Нет активных заявок на замер.")  # type: ignore[union-attr]
        return

    b = InlineKeyboardBuilder()
    for req in all_reqs:
        icon = {"in_progress": "🔄", "done": "✅"}.get(req.get("status", ""), "❓")
        addr = (req.get("address") or "")[:25]
        b.button(
            text=f"{icon} #{req['id']} — {addr}"[:55],
            callback_data=f"zam_myreq:view:{req['id']}",
        )
    b.button(text="⬅️ Назад", callback_data="zam_my:back")
    b.adjust(1)
    text = f"📐 <b>Заявки на замер</b> (🔄 {len(reqs)} активных)"
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "zam_my:invoices")
async def zamery_my_invoices(cb: CallbackQuery, db: Database) -> None:
    """Подменю: счета в работе."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    active = [i for i in invoices if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
    )]
    if not active:
        await cb.message.answer("📋 Нет счетов в работе.")  # type: ignore[union-attr]
        return

    lines = []
    for inv in active[:20]:
        status_emoji = {"in_progress": "🔄", "paid": "✅"}.get(inv["status"], "❓")
        lines.append(
            f"{status_emoji} №{inv['invoice_number']} — "
            f"{(inv.get('object_address') or '-')[:30]}"
        )
    text = f"📋 <b>Счета в работе</b> ({len(active)}):\n\n" + "\n".join(lines)
    await cb.message.answer(text)  # type: ignore[union-attr]


@router.callback_query(F.data == "zam_my:back")
async def zamery_my_back(cb: CallbackQuery, db: Database) -> None:
    """Назад к дашборду «Мои замеры»."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    # Переиспользуем логику через фейковое сообщение нельзя — просто показываем заново
    user_id = cb.from_user.id
    conv = await db.get_zamery_conversion_stats(user_id)
    role_short = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}

    text = "📋 <b>Мои замеры</b>\n\n"
    text += f"📊 <b>Конверсия:</b> {conv['conversion_pct']}% "
    text += f"({conv['total_with_invoice']} счетов из {conv['total_done']} замеров)\n"
    if conv["by_role"]:
        parts = []
        for r in conv["by_role"]:
            rn = role_short.get(r.get("requester_role", ""), "?")
            parts.append(f"{rn}: {r['pct']}%")
        text += "По менеджерам: " + " | ".join(parts) + "\n"

    active_reqs = await db.list_zamery_requests(assigned_to=user_id, status="in_progress", limit=50)
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    active_inv = [i for i in invoices if i["status"] in (
        InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
    )]

    b = InlineKeyboardBuilder()
    b.button(text=f"📐 Заявки на замер ({len(active_reqs)})", callback_data="zam_my:requests")
    b.button(text=f"📋 Счета в работе ({len(active_inv)})", callback_data="zam_my:invoices")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zam_myreq:view:"))
async def zamery_myreq_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр заявки из «Мои замеры» + кнопка отправки."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    source_label = ZAMERY_SOURCE_LABELS.get(req.get("source_type", ""), "—")
    role_short = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(req.get("requester_role", ""), "?")
    initiator = await get_initiator_label(db, req["requested_by"])
    status_label = {
        "open": "⏳ Новая", "in_progress": "🔄 В работе",
        "done": "✅ Выполнена", "rejected": "❌ Отклонена",
    }.get(req.get("status", ""), "❓")

    text = f"📐 <b>Заявка на замер #{req['id']}</b>\n\n"
    text += f"👤 Менеджер: {initiator}"
    if role_short:
        text += f" ({role_short})"
    text += f"\n📍 Адрес: {req.get('address', '—')}\n"
    if req.get("client_contact"):
        text += f"📞 Контакт: <code>{req['client_contact']}</code>\n"
    if req.get("volume_m2"):
        text += f"📊 Объём: {req['volume_m2']} м²\n"
    mkad_km = req.get("mkad_km") or 0
    mkad_surcharge = req.get("mkad_surcharge") or 0
    if mkad_km and mkad_km > 0:
        if mkad_surcharge:
            text += f"📍 МКАД: {mkad_km} км (наценка: {mkad_surcharge}₽)\n"
        else:
            text += f"📍 МКАД: {mkad_km} км\n"
    else:
        text += "📍 МКАД: внутри МКАД\n"
    total_cost = req.get("total_cost")
    if total_cost:
        text += f"💰 Стоимость замера: <b>{total_cost}₽</b>\n"
    # Дата/время если назначены
    if req.get("scheduled_date"):
        from datetime import date as date_cls
        try:
            d = date_cls.fromisoformat(req["scheduled_date"])
            day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            text += f"📅 Дата: {day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}\n"
        except ValueError:
            pass
    if req.get("scheduled_time_interval"):
        text += f"⏰ Время: {req['scheduled_time_interval']}\n"
    if req.get("accept_comment"):
        text += f"💬 Комментарий: {req['accept_comment']}\n"
    if req.get("description"):
        text += f"\n📝 Описание: {req['description']}\n"
    text += f"📌 Источник: {source_label}\n"
    text += f"📊 Статус: {status_label}\n"
    text += f"📅 Создана: {req.get('created_at', '—')[:16]}\n"

    b = InlineKeyboardBuilder()
    if req.get("status") == "in_progress":
        b.button(text="📤 Отправить замер", callback_data=f"zam_complete:start:{req_id}")
    b.button(text="⬅️ Назад", callback_data="zam_my:requests")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# ОТПРАВИТЬ ЗАМЕР — завершение заявки
# =====================================================================

@router.callback_query(F.data.startswith("zam_complete:start:"))
async def zamery_complete_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Начало завершения замера — сбор вложений."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req or req.get("status") != "in_progress":
        await cb.message.answer("❌ Заявка не найдена или уже завершена.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.update_data(complete_req_id=req_id, complete_attachments=[])
    await state.set_state(ZameryCompleteSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="➡️ Далее", callback_data="zam_complete:next")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📤 <b>Отправка замера #{req_id}</b>\n\n"
        "📎 Прикрепите файлы (фото, видео, документы).\n"
        "Когда закончите — нажмите «Далее».",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryCompleteSG.attachments)
async def zamery_complete_file(message: Message, state: FSMContext) -> None:
    """Приём файлов: фото, видео, документы."""
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("complete_attachments", [])

    if message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    elif message.video:
        attachments.append({"file_type": "video", "file_id": message.video.file_id, "file_unique_id": message.video.file_unique_id, "caption": message.caption})
    elif message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл, фото или видео. Или нажмите «Далее».")
        return

    await state.update_data(complete_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(ZameryCompleteSG.attachments, F.data == "zam_complete:next")
async def zamery_complete_to_comment(cb: CallbackQuery, state: FSMContext) -> None:
    """Переход к комментарию."""
    await cb.answer()
    await state.set_state(ZameryCompleteSG.comment)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без комментария", callback_data="zam_complete:finish")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "💬 Добавьте комментарий к замеру:",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryCompleteSG.comment)
async def zamery_complete_comment_text(
    message: Message, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Получен комментарий → финализировать."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите комментарий или нажмите «Без комментария».")
        return
    await state.update_data(completion_comment=text)
    await _finalize_complete(message, state, db, config, notifier)


@router.callback_query(ZameryCompleteSG.comment, F.data == "zam_complete:finish")
async def zamery_complete_skip_comment(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Без комментария → финализировать."""
    await cb.answer()
    await _finalize_complete(cb, state, db, config, notifier)


async def _finalize_complete(
    event: Message | CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Общая функция завершения замера."""
    from ..utils import to_iso, utcnow
    from datetime import datetime
    data = await state.get_data()
    req_id = data["complete_req_id"]
    req = await db.get_zamery_request(req_id)
    msg_target = event.message if isinstance(event, CallbackQuery) else event

    if not req:
        await msg_target.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    now = utcnow()
    attachments = data.get("complete_attachments", [])
    comment = data.get("completion_comment")

    # Время выполнения
    time_label = ""
    if req.get("accepted_at"):
        try:
            accepted = datetime.fromisoformat(req["accepted_at"])
            delta = now - accepted
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            if hours > 0:
                time_label = f"{hours} ч {minutes} мин"
            else:
                time_label = f"{minutes} мин"
        except (ValueError, TypeError):
            pass

    # Обновить БД
    att_json = json.dumps([{"file_id": a["file_id"], "file_type": a["file_type"]} for a in attachments]) if attachments else None
    await db.update_zamery_request(
        req_id,
        status="done",
        completed_at=to_iso(now),
        completion_comment=comment,
        completion_attachments_json=att_json,
    )
    if req.get("task_id"):
        await db.update_task_status(req["task_id"], TaskStatus.DONE)

    await msg_target.answer(f"✅ Замер #{req_id} отправлен менеджеру.")  # type: ignore[union-attr]

    # Уведомить менеджера
    initiator = await get_initiator_label(db, req.get("assigned_to", 0))
    notify_text = (
        f"✅ <b>Замер #{req_id} выполнен</b>\n\n"
        f"📍 Адрес: {req.get('address', '—')}\n"
    )
    if time_label:
        notify_text += f"⏱ Время выполнения: {time_label}\n"
    if req.get("scheduled_date"):
        from datetime import date as date_cls
        try:
            d = date_cls.fromisoformat(req["scheduled_date"])
            day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            notify_text += f"📅 Дата замера: {day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}"
            if req.get("scheduled_time_interval"):
                notify_text += f" {req['scheduled_time_interval']}"
            notify_text += "\n"
        except ValueError:
            pass
    if comment:
        notify_text += f"💬 Комментарий: {comment}\n"

    await notifier.safe_send(req["requested_by"], notify_text)
    # Отправить вложения
    for a in attachments:
        ft = a.get("file_type", "document")
        fid = a.get("file_id")
        if fid:
            await notifier.safe_send_media(req["requested_by"], ft, fid, caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, req["requested_by"])
    # Обновить бейдж замерщика
    uid = event.from_user.id if isinstance(event, (Message, CallbackQuery)) and event.from_user else None
    if uid:
        await refresh_recipient_keyboard(notifier, db, config, uid)
    await state.clear()


# =====================================================================
# ОПЛАТА ЗАМЕРОВ (список неоплаченных + кнопки ЗП)
# =====================================================================

def _get_unpaid_invoices(invoices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Фильтр: неоплаченные замеры."""
    return [
        i for i in invoices
        if i.get("zp_status", "not_requested") != "approved"
        and i["status"] in (
            InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
            InvoiceStatus.CLOSING, InvoiceStatus.ENDED,
        )
    ]


async def _render_payment_list(
    target: Message | CallbackQuery,
    db: Database,
    user_id: int,
) -> None:
    """Отрисовка «Оплата замеров» — только карточка-статистика + кнопки."""
    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    unpaid = _get_unpaid_invoices(invoices)
    paid_list = [
        i for i in invoices
        if i.get("zp_status") == "approved"
        and i["status"] in (
            InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
            InvoiceStatus.CLOSING, InvoiceStatus.ENDED,
        )
    ]
    msg = target.message if isinstance(target, CallbackQuery) else target

    if not unpaid and not paid_list:
        await msg.answer("📭 Нет замеров для оплаты.")  # type: ignore[union-attr]
        return

    # --- Категоризация ---
    not_requested = [i for i in unpaid if i.get("zp_status") == "not_requested"]
    requested = [i for i in unpaid if i.get("zp_status") == "requested"]

    sum_not_req = sum(i.get("zp_zamery_total") or 0 for i in not_requested)
    sum_requested = sum(i.get("zp_zamery_total") or 0 for i in requested)
    sum_paid = sum(i.get("zp_zamery_total") or 0 for i in paid_list)
    total_all = sum_not_req + sum_requested + sum_paid

    no_cost = [i for i in not_requested if not i.get("zp_zamery_total")]

    # --- Карточка ---
    total_count = len(unpaid) + len(paid_list)
    text = f"💰 <b>Оплата замеров</b> · {total_count} шт."
    if total_all:
        text += f" · {int(total_all)}₽"
    text += "\n"

    parts = []
    if not_requested:
        s = f"❌ {len(not_requested)}"
        if sum_not_req:
            s += f" · {int(sum_not_req)}₽"
        parts.append(s)
    if requested:
        s = f"⏳ {len(requested)}"
        if sum_requested:
            s += f" · {int(sum_requested)}₽"
        parts.append(s)
    if paid_list:
        s = f"✅ {len(paid_list)}"
        if sum_paid:
            s += f" · {int(sum_paid)}₽"
        parts.append(s)
    if parts:
        text += " | ".join(parts) + "\n"
    if no_cost:
        text += f"⚠️ <i>Без стоимости: {len(no_cost)}</i>\n"

    # --- Кнопки: каждый замер → редактировать стоимость ---
    b = InlineKeyboardBuilder()
    for inv in unpaid[:20]:
        zp = inv.get("zp_status", "not_requested")
        cost = inv.get("zp_zamery_total")
        icon = {"requested": "⏳", "approved": "✅"}.get(zp, "❌")
        addr = (inv.get("object_address") or "—")[:18]
        cost_str = f"{int(cost)}₽" if cost else "—₽"
        b.button(
            text=f"{icon} №{inv['invoice_number']} · {addr} · {cost_str}",
            callback_data=f"zampay:view:{inv['id']}",
        )

    # Кнопка «Отправить в оплату»
    sendable = [
        i for i in unpaid
        if i.get("zp_zamery_total") and i.get("zp_zamery_total") > 0
        and i.get("zp_status") == "not_requested"
    ]
    if sendable:
        total_send = int(sum(i["zp_zamery_total"] for i in sendable))
        b.button(
            text=f"📤 Отправить в оплату · {len(sendable)} зам. · {total_send}₽",
            callback_data="zampay:send_batch",
        )

    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    await msg.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.message(F.text == ZAM_BTN_PAYMENT)
async def zamery_payment(message: Message, db: Database) -> None:
    """Замерщик: Оплата замеров — карточки с итогом."""
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    await _render_payment_list(message, db, user_id)


@router.callback_query(F.data == "zampay:back")
async def zamery_payment_back(cb: CallbackQuery, db: Database) -> None:
    """Назад к списку оплаты."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _render_payment_list(cb, db, cb.from_user.id)


@router.callback_query(F.data.startswith("zampay:view:"))
async def zamery_payment_card(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Карточка замера — нажатие сразу открывает ввод стоимости."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    zp = inv.get("zp_status", "not_requested")

    # Если уже оплачен или на проверке — показать инфо, без редактирования
    if zp in ("approved", "requested"):
        zp_label = {"requested": "⏳ На проверке", "approved": "✅ Оплачена"}.get(zp, "")
        cost = inv.get("zp_zamery_total")
        cost_str = f"<b>{int(cost)}₽</b>" if cost else "<b>—</b>"
        addr = inv.get("object_address", "—")
        text = f"💰 №{inv['invoice_number']} · {addr}\n💵 {cost_str} · {zp_label}"
        b = InlineKeyboardBuilder()
        b.button(text="⬅️ Назад", callback_data="zampay:back")
        b.adjust(1)
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
        return

    # Сразу открываем редактирование стоимости
    await state.clear()
    await state.update_data(edit_cost_inv_id=inv_id)
    await state.set_state(ZameryCostEditSG.enter_cost)

    cost = inv.get("zp_zamery_total")
    addr = inv.get("object_address") or "—"
    hint = f"<b>{int(cost)}₽</b>" if cost else "<i>—</i>"

    text = f"✏️ №{inv['invoice_number']} · {addr}\n💵 Сейчас: {hint}\n\nВведите новую стоимость (₽):"
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Отмена", callback_data="zampay:back")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.message(ZameryCostEditSG.enter_cost)
async def zamery_edit_cost_value(message: Message, state: FSMContext, db: Database) -> None:
    """Сохранить новую стоимость и вернуться к списку."""
    text = (message.text or "").strip().replace(",", ".")
    try:
        cost = float(text)
        if cost <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число > 0:")
        return

    data = await state.get_data()
    inv_id = data["edit_cost_inv_id"]
    await db.update_invoice(
        inv_id,
        zp_zamery_total=cost,
        zp_zamery_details_json=json.dumps([{"description": "Замер", "cost": cost, "count": 1}]),
    )
    await state.clear()

    inv = await db.get_invoice(inv_id)
    await message.answer(
        f"✅ №{inv['invoice_number'] if inv else inv_id} — <b>{int(cost)}₽</b>"
    )
    # Вернуть обновлённый список с пересчитанными итогами
    uid = message.from_user.id  # type: ignore[union-attr]
    await _render_payment_list(message, db, uid)


# --- Пакетная отправка в оплату ГД ---

@router.callback_query(F.data == "zampay:send_batch")
async def zamery_send_batch(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Отправить все неоплаченные замеры с ценой → задача ГД."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id

    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    unpaid = _get_unpaid_invoices(invoices)
    sendable = [
        i for i in unpaid
        if i.get("zp_zamery_total") and i.get("zp_zamery_total") > 0
        and i.get("zp_status") == "not_requested"
    ]

    if not sendable:
        await cb.message.answer("⚠️ Нет замеров для отправки.")  # type: ignore[union-attr]
        return

    # Показать итоговый расчёт карточкой
    total = sum(i["zp_zamery_total"] for i in sendable)

    text = f"📋 <b>Расчёт · {len(sendable)} зам. · {int(total)}₽</b>\n\n"
    for idx, inv in enumerate(sendable, 1):
        addr = (inv.get("object_address") or "—")[:25]
        text += f"{idx}. №{inv['invoice_number']} · {addr} · {int(inv['zp_zamery_total'])}₽\n"
    text += f"\nОтправить в оплату ГД?"

    b = InlineKeyboardBuilder()
    b.button(
        text=f"✅ Отправить · {len(sendable)} зам. · {int(total)}₽",
        callback_data="zampay:confirm_batch",
    )
    b.button(text="⬅️ Назад", callback_data="zampay:back")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "zampay:confirm_batch")
async def zamery_confirm_batch(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Подтверждение → создать задачу ГД, обновить статусы."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id

    invoices = await db.list_invoices(assigned_to=user_id, limit=50)
    unpaid = _get_unpaid_invoices(invoices)
    sendable = [
        i for i in unpaid
        if i.get("zp_zamery_total") and i.get("zp_zamery_total") > 0
        and i.get("zp_status") == "not_requested"
    ]
    if not sendable:
        await cb.message.answer("⚠️ Нет замеров для отправки.")  # type: ignore[union-attr]
        return

    total = sum(i["zp_zamery_total"] for i in sendable)
    inv_ids = [i["id"] for i in sendable]

    # Обновить статусы → requested
    for inv in sendable:
        await db.set_invoice_zp_status(inv["id"], "requested")

    # Создать задачу для ГД
    from ..enums import TaskType
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer("⚠️ ГД не найден.")  # type: ignore[union-attr]
        return

    initiator = await get_initiator_label(db, user_id)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZP_ZAMERY_BATCH,
        status=TaskStatus.OPEN,
        created_by=user_id,
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "invoice_ids": inv_ids,
            "total": total,
            "count": len(sendable),
            "zamery_user_id": user_id,
        },
    )

    # Уведомить ГД
    lines = []
    for inv in sendable:
        lines.append(f"• №{inv['invoice_number']} — {int(inv['zp_zamery_total'])}₽")
    gd_text = (
        f"💰 <b>ЗП Замерщика</b>\n"
        f"👤 От: {initiator}\n\n"
        + "\n".join(lines) + "\n\n"
        f"💵 <b>Итого: {int(total)}₽</b> ({len(sendable)} замеров)\n"
    )
    b = InlineKeyboardBuilder()
    b.button(text="✅ ЗП ОК", callback_data=f"zampay_gd:ok:{task['id']}")
    b.button(text="❌ Отклонить", callback_data=f"zampay_gd:no:{task['id']}")
    b.adjust(2)
    await notifier.safe_send(int(gd_id), gd_text, reply_markup=b.as_markup())
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос на оплату отправлен ГД.\n"
        f"💵 {len(sendable)} замеров на сумму {int(total)}₽"
    )


# --- ГД: одобрение / отклонение пакетной ЗП замерщика ---

@router.callback_query(F.data.startswith("zampay_gd:ok:"))
async def zamery_batch_gd_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """ГД: одобрить пакетную ЗП замерщика."""
    from .auth import require_role_callback as _rrc
    if not await _rrc(cb, db, roles=[Role.GD]):
        return
    await cb.answer("✅ ЗП ОК")

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    from ..utils import try_json_loads
    payload = try_json_loads(task.get("payload_json"))
    inv_ids = payload.get("invoice_ids", [])
    zamery_uid = payload.get("zamery_user_id")
    total = payload.get("total", 0)

    # Одобрить все
    for inv_id in inv_ids:
        await db.set_invoice_zp_status(inv_id, "approved")

    # Закрыть задачу
    await db.update_task_status(task_id, TaskStatus.DONE)

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП замерщика одобрена: {int(total)}₽ ({len(inv_ids)} замеров)"
    )

    # Обновить клавиатуру ГД (badge «Оплата поставщику»)
    await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)

    # Уведомить замерщика
    if zamery_uid:
        await notifier.safe_send(
            int(zamery_uid),
            f"✅ <b>ЗП одобрена!</b>\n\n"
            f"💵 Сумма: {int(total)}₽ ({len(inv_ids)} замеров)",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(zamery_uid))


@router.callback_query(F.data.startswith("zampay_gd:no:"))
async def zamery_batch_gd_reject(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """ГД: отклонить пакетную ЗП замерщика."""
    from .auth import require_role_callback as _rrc
    if not await _rrc(cb, db, roles=[Role.GD]):
        return
    await cb.answer("❌ Отклонено")

    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.message.answer("❌ Задача не найдена.")  # type: ignore[union-attr]
        return

    from ..utils import try_json_loads
    payload = try_json_loads(task.get("payload_json"))
    inv_ids = payload.get("invoice_ids", [])
    zamery_uid = payload.get("zamery_user_id")

    # Вернуть статусы
    for inv_id in inv_ids:
        await db.set_invoice_zp_status(inv_id, "not_requested")

    # Закрыть задачу
    await db.update_task_status(task_id, TaskStatus.REJECTED)

    await cb.message.answer(  # type: ignore[union-attr]
        f"❌ ЗП замерщика отклонена ({len(inv_ids)} замеров)"
    )

    # Обновить клавиатуру ГД
    await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)

    if zamery_uid:
        await notifier.safe_send(
            int(zamery_uid),
            f"❌ <b>ЗП отклонена</b>\n\n"
            f"Свяжитесь с ГД для уточнения.",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(zamery_uid))


# =====================================================================
# 📅 ГРАФИК ЗАМЕРОВ (Schedule)
# =====================================================================

_RU_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_RU_MONTHS = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _week_range(base: date, offset_weeks: int = 0) -> tuple[date, date]:
    """Return (monday, sunday) for the week containing base + offset_weeks."""
    monday = base - timedelta(days=base.weekday()) + timedelta(weeks=offset_weeks)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _format_date_short(d: date) -> str:
    return f"{d.day} {_RU_MONTHS[d.month]}"


@router.message(F.text == ZAM_BTN_SCHEDULE)
async def zamery_schedule_main(message: Message, db: Database) -> None:
    """📅 График замеров — главный экран с 5 неделями."""
    if not await require_role_message(message, db, roles=[Role.ZAMERY]):
        return
    uid = message.from_user.id  # type: ignore[union-attr]
    await _render_schedule_main(message, db, uid, edit_existing=False)


_RU_MONTH_NAMES = [
    "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


async def _get_monthly_stats(
    db: Database, uid: int, target_date: date,
) -> dict[str, int]:
    """Get zamery count for a given month."""
    first_day = target_date.replace(day=1)
    if target_date.month == 12:
        last_day = target_date.replace(year=target_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        last_day = target_date.replace(month=target_date.month + 1, day=1) - timedelta(days=1)
    zamery = await db.list_zamery_for_schedule(uid, first_day.isoformat(), last_day.isoformat())
    blackouts = await db.list_zamery_blackout_dates(uid, first_day.isoformat(), last_day.isoformat())
    return {"zamery": len(zamery), "blackouts": len(blackouts)}


async def _render_schedule_main(
    target: Message | CallbackQuery,
    db: Database,
    uid: int,
    edit_existing: bool = True,
) -> None:
    """Render schedule overview with monthly stats card + weeks."""
    today = date.today()

    # --- Помесячная статистика (текущий + 2 предыдущих) ---
    months_stats = []
    for offset in range(-2, 1):
        # Calculate month
        y = today.year
        m = today.month + offset
        if m <= 0:
            m += 12
            y -= 1
        elif m > 12:
            m -= 12
            y += 1
        d = date(y, m, 1)
        stats = await _get_monthly_stats(db, uid, d)
        months_stats.append((d, stats))

    # Current month stats
    cur_month = months_stats[-1]
    cur_d, cur_s = cur_month

    # --- Карточка статистики ---
    text = f"📅 <b>График замеров</b> · {_RU_MONTH_NAMES[cur_d.month]} {cur_d.year}\n"
    text += f"📐 {cur_s['zamery']} замеров · 🚫 {cur_s['blackouts']} выходных\n\n"
    for d, s in months_stats:
        is_cur = d.month == today.month and d.year == today.year
        icon = "▶️" if is_cur else "▫️"
        text += f"{icon} {_RU_MONTH_NAMES[d.month]}: <b>{s['zamery']}</b>"
        if s["blackouts"]:
            text += f" · 🚫{s['blackouts']}"
        text += "\n"
    text += "\n"

    # --- Недели ---
    weeks_data = []
    for w in range(5):
        mon, sun = _week_range(today, w)
        d_from = mon.isoformat()
        d_to = sun.isoformat()
        zamery = await db.list_zamery_for_schedule(uid, d_from, d_to)
        blackouts = await db.list_zamery_blackout_dates(uid, d_from, d_to)
        weeks_data.append((w, mon, sun, zamery, blackouts))

    b = InlineKeyboardBuilder()
    for w, mon, sun, zamery, blackouts in weeks_data:
        label = f"{_format_date_short(mon)} — {_format_date_short(sun)}"
        if w == 0:
            label = f"📍 {label}"

        cnt = len(zamery)
        bl = len(blackouts)
        badge = ""
        if cnt > 0:
            badge += f" · 📐{cnt}"
        if bl > 0:
            badge += f" · 🚫{bl}"
        if cnt == 0 and bl == 0:
            badge = " · свободна"

        b.button(
            text=f"{label}{badge}",
            callback_data=f"zamsched:week:{w}",
        )

    b.button(text="🚫 Добавить выходной", callback_data="zamsched:blackout:add")
    b.adjust(1)

    msg = target.message if isinstance(target, CallbackQuery) else target
    if edit_existing and isinstance(target, CallbackQuery):
        try:
            await msg.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
            return
        except Exception:
            pass
    await msg.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zamsched:week:"))
async def zamery_schedule_week(cb: CallbackQuery, db: Database) -> None:
    """Detailed week view — 7 days with zamery, blackouts, free."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    uid = cb.from_user.id
    week_offset = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    today = date.today()
    mon, sun = _week_range(today, week_offset)
    d_from = mon.isoformat()
    d_to = sun.isoformat()

    zamery = await db.list_zamery_for_schedule(uid, d_from, d_to)
    blackouts = await db.list_zamery_blackout_dates(uid, d_from, d_to)

    # Group zamery by date
    zam_by_date: dict[str, list[dict]] = {}
    for z in zamery:
        d = z["scheduled_date"]
        zam_by_date.setdefault(d, []).append(z)

    blackout_set = {bl["blackout_date"] for bl in blackouts}
    blackout_map = {bl["blackout_date"]: bl for bl in blackouts}

    text = f"📅 <b>{_format_date_short(mon)} — {_format_date_short(sun)}</b>\n\n"

    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        wd = _RU_WEEKDAYS[day.weekday()]
        day_label = f"{day.day} {_RU_MONTHS[day.month]} ({wd})"

        if ds in blackout_set:
            bl = blackout_map.get(ds)
            cmt = f" ({bl['comment']})" if bl and bl.get("comment") else ""
            text += f"🚫 <b>{day_label}</b> — выходной{cmt}\n"
        elif ds in zam_by_date:
            day_zamery = zam_by_date[ds]
            text += f"📐 <b>{day_label}</b> · {len(day_zamery)} замер(ов)\n"
            for z in day_zamery:
                interval = z.get("scheduled_time_interval") or ""
                mgr = z.get("manager_name") or "—"
                addr = z.get("address") or "—"
                text += f"  ⏰ {interval} · 👤 {mgr} · 📍 {addr}\n"
        else:
            if day < today:
                text += f"▫️ {day_label}\n"
            else:
                text += f"🟢 <b>{day_label}</b> — свободен\n"

    b = InlineKeyboardBuilder()
    # Кнопки свободных дней → заказать замер
    free_btns = 0
    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        if day >= today and ds not in blackout_set and ds not in zam_by_date:
            wd = _RU_WEEKDAYS[day.weekday()]
            b.button(
                text=f"🟢 {day.day} {_RU_MONTHS[day.month]} ({wd}) — записать",
                callback_data=f"zamsched:book:{ds}:{week_offset}",
            )
            free_btns += 1
    # Дни с замерами — тоже можно добавить ещё
    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        if day >= today and ds not in blackout_set and ds in zam_by_date:
            wd = _RU_WEEKDAYS[day.weekday()]
            cnt = len(zam_by_date[ds])
            b.button(
                text=f"📐 {day.day} {_RU_MONTHS[day.month]} ({wd}) — доп. замер",
                callback_data=f"zamsched:book:{ds}:{week_offset}",
            )

    # Nav buttons
    if week_offset > 0:
        b.button(text="⬅️ Пред. неделя", callback_data=f"zamsched:week:{week_offset - 1}")
    if week_offset < 8:
        b.button(text="След. неделя ➡️", callback_data=f"zamsched:week:{week_offset + 1}")
    b.button(text="🚫 Добавить выходной", callback_data="zamsched:blackout:add")
    # Show removable blackouts for this week
    for bl in blackouts:
        bd = date.fromisoformat(bl["blackout_date"])
        b.button(
            text=f"❌ Убрать выходной {bd.day} {_RU_MONTHS[bd.month]}",
            callback_data=f"zamsched:blackout:rm:{bl['id']}:{week_offset}",
        )
    b.button(text="⬅️ К списку недель", callback_data="zamsched:main")
    b.adjust(1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "zamsched:main")
async def zamery_schedule_back(cb: CallbackQuery, db: Database) -> None:
    """Back to main schedule view."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _render_schedule_main(cb, db, cb.from_user.id, edit_existing=True)


# --- Заказать замер из графика ---

_BOOK_INTERVALS = [
    "08:00–10:00", "10:00–12:00", "12:00–14:00",
    "14:00–16:00", "16:00–18:00", "18:00–20:00",
]


@router.callback_query(F.data.startswith("zamsched:book:"))
async def zamery_book_pick_time(cb: CallbackQuery, db: Database) -> None:
    """Выбор временного интервала для записи замера."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    parts = cb.data.split(":")  # type: ignore[union-attr]
    ds = parts[2]  # ISO date
    week_offset = int(parts[3])

    uid = cb.from_user.id
    d = date.fromisoformat(ds)
    wd = _RU_WEEKDAYS[d.weekday()]

    # Check which intervals are busy
    summary = await db.get_zamery_schedule_summary(uid, ds, ds)
    busy_intervals = summary["busy"].get(ds, [])

    text = f"📐 <b>Записать замер</b> · {d.day} {_RU_MONTHS[d.month]} ({wd})\n"
    if busy_intervals:
        text += f"⚠️ Занято: {', '.join(busy_intervals)}\n"
    text += "\nВыберите интервал:"

    b = InlineKeyboardBuilder()
    for interval in _BOOK_INTERVALS:
        if interval in busy_intervals:
            b.button(text=f"🔴 {interval}", callback_data=f"zamsched:booktime:{ds}:{interval}:{week_offset}")
        else:
            b.button(text=f"🟢 {interval}", callback_data=f"zamsched:booktime:{ds}:{interval}:{week_offset}")
    b.button(text="⬅️ Назад к неделе", callback_data=f"zamsched:week:{week_offset}")
    b.adjust(2, 2, 2, 1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zamsched:booktime:"))
async def zamery_book_enter_address(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Выбран интервал → ввод адреса (начало полного сбора данных)."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    parts = cb.data.split(":")  # type: ignore[union-attr]
    ds = parts[2]
    interval = parts[3]
    week_offset = int(parts[4])

    d = date.fromisoformat(ds)
    wd = _RU_WEEKDAYS[d.weekday()]

    await state.clear()
    await state.set_state(ZameryQuickBookSG.enter_address)
    await state.update_data(
        book_date=ds,
        book_interval=interval,
        book_week_offset=week_offset,
        book_source="zamery",
    )

    await cb.message.answer(  # type: ignore[union-attr]
        f"📐 <b>Запись замера</b>\n"
        f"📅 {d.day} {_RU_MONTHS[d.month]} ({wd})  ⏰ {interval}\n\n"
        f"<b>Шаг 1/6.</b> Введите <b>адрес</b> замера:",
    )


@router.message(ZameryQuickBookSG.enter_address)
async def zamery_book_address(message: Message, state: FSMContext) -> None:
    """Адрес → описание."""
    text = (message.text or "").strip()
    if not text:
        await message.answer("⚠️ Введите адрес:")
        return
    await state.update_data(book_address=text)
    await state.set_state(ZameryQuickBookSG.enter_description)

    b = InlineKeyboardBuilder()
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:desc")
    b.adjust(1)
    await message.answer(
        "📝 <b>Шаг 2/6.</b> Введите <b>описание работ</b> или пропустите:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryQuickBookSG.enter_description, F.data == "zamsched:skip:desc")
async def zamery_book_skip_desc(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(book_description=None)
    await state.set_state(ZameryQuickBookSG.enter_client_contact)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:contact")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "📞 <b>Шаг 3/6.</b> Введите <b>контакт клиента</b> (телефон/имя) или пропустите:",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryQuickBookSG.enter_description)
async def zamery_book_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip() or None
    await state.update_data(book_description=text)
    await state.set_state(ZameryQuickBookSG.enter_client_contact)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:contact")
    b.adjust(1)
    await message.answer(
        "📞 <b>Шаг 3/6.</b> Введите <b>контакт клиента</b> (телефон/имя) или пропустите:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryQuickBookSG.enter_client_contact, F.data == "zamsched:skip:contact")
async def zamery_book_skip_contact(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(book_client_contact=None)
    await state.set_state(ZameryQuickBookSG.enter_mkad_km)
    b = InlineKeyboardBuilder()
    b.button(text="0 (в пределах МКАД)", callback_data="zamsched:mkad:0")
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:mkad")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "🚗 <b>Шаг 4/6.</b> Введите <b>расстояние от МКАД</b> (км, число) или выберите:",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryQuickBookSG.enter_client_contact)
async def zamery_book_client_contact(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip() or None
    await state.update_data(book_client_contact=text)
    await state.set_state(ZameryQuickBookSG.enter_mkad_km)
    b = InlineKeyboardBuilder()
    b.button(text="0 (в пределах МКАД)", callback_data="zamsched:mkad:0")
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:mkad")
    b.adjust(1)
    await message.answer(
        "🚗 <b>Шаг 4/6.</b> Введите <b>расстояние от МКАД</b> (км, число) или выберите:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryQuickBookSG.enter_mkad_km, F.data == "zamsched:mkad:0")
async def zamery_book_mkad_zero(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(book_mkad_km=0)
    await _ask_volume(cb.message, state)  # type: ignore[arg-type]


@router.callback_query(ZameryQuickBookSG.enter_mkad_km, F.data == "zamsched:skip:mkad")
async def zamery_book_skip_mkad(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(book_mkad_km=0)
    await _ask_volume(cb.message, state)  # type: ignore[arg-type]


@router.message(ZameryQuickBookSG.enter_mkad_km)
async def zamery_book_mkad_km(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    try:
        km = float(text.replace(",", "."))
    except (ValueError, TypeError):
        await message.answer("⚠️ Введите число (км от МКАД):")
        return
    await state.update_data(book_mkad_km=km)
    await _ask_volume(message, state)


async def _ask_volume(target: Any, state: FSMContext) -> None:
    await state.set_state(ZameryQuickBookSG.enter_volume)
    b = InlineKeyboardBuilder()
    b.button(text="⏭ Пропустить", callback_data="zamsched:skip:volume")
    b.adjust(1)
    await target.answer(  # type: ignore[union-attr]
        "📐 <b>Шаг 5/6.</b> Введите <b>примерный объём</b> (м²) или пропустите:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryQuickBookSG.enter_volume, F.data == "zamsched:skip:volume")
async def zamery_book_skip_volume(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(book_volume=None)
    await _ask_attachments(cb.message, state)  # type: ignore[arg-type]


@router.message(ZameryQuickBookSG.enter_volume)
async def zamery_book_volume(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    try:
        vol = float(text.replace(",", "."))
    except (ValueError, TypeError):
        await message.answer("⚠️ Введите число (м²):")
        return
    await state.update_data(book_volume=vol)
    await _ask_attachments(message, state)


async def _ask_attachments(target: Any, state: FSMContext) -> None:
    await state.set_state(ZameryQuickBookSG.attachments)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Сохранить замер", callback_data="zamsched:book:finalize")
    b.adjust(1)
    await target.answer(  # type: ignore[union-attr]
        "📎 <b>Шаг 6/6.</b> Отправьте <b>фото/документы</b> или нажмите для сохранения:",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryQuickBookSG.attachments, F.content_type.in_({"photo", "document"}))
async def zamery_book_attachment(message: Message, state: FSMContext) -> None:
    """Получен файл — сохраняем и ждём ещё."""
    data = await state.get_data()
    attachments = data.get("book_attachments", [])
    if message.photo:
        biggest = message.photo[-1]
        attachments.append({
            "file_id": biggest.file_id,
            "file_unique_id": biggest.file_unique_id,
            "file_type": "photo",
            "caption": message.caption,
        })
    elif message.document:
        attachments.append({
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "file_type": "document",
            "caption": message.caption,
        })
    await state.update_data(book_attachments=attachments)
    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Сохранить замер ({len(attachments)} вл.)", callback_data="zamsched:book:finalize")
    b.adjust(1)
    await message.answer(
        f"📎 Добавлено: {len(attachments)}. Отправьте ещё или сохраните:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ZameryQuickBookSG.attachments, F.data == "zamsched:book:finalize")
async def zamery_book_finalize_cb(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    await cb.answer()
    await _finalize_schedule_book(cb, state, db, config, notifier)


async def _finalize_schedule_book(
    event: Message | CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Сохранить полную запись замера из графика."""
    import json as _json
    from ..enums import TaskType
    from ..keyboards import task_actions_kb

    data = await state.get_data()
    ds = data["book_date"]
    interval = data["book_interval"]
    address = data["book_address"]
    description = data.get("book_description")
    client_contact = data.get("book_client_contact")
    mkad_km = data.get("book_mkad_km", 0)
    volume_m2 = data.get("book_volume")
    attachments = data.get("book_attachments", [])
    week_offset = data.get("book_week_offset", 0)

    uid = event.from_user.id if event.from_user else 0
    d = date.fromisoformat(ds)
    wd = _RU_WEEKDAYS[d.weekday()]

    # Resolve zamery user
    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    assigned_to = int(zamery_uid) if zamery_uid else uid

    # Determine requester role
    user = await db.get_user_optional(uid)
    requester_role = resolve_active_menu_role(uid, user.role if user else None) or "zamery"

    # Cost calculation
    base_cost = 2500
    mkad_surcharge = int(mkad_km * 30) if mkad_km else 0
    total_cost = base_cost + mkad_surcharge

    # Create zamery request
    zam_req_id = await db.create_zamery_request(
        source_type="schedule",
        address=address,
        description=description,
        client_contact=client_contact,
        requested_by=uid,
        requester_role=requester_role,
        assigned_to=assigned_to,
        mkad_km=mkad_km,
        volume_m2=volume_m2,
        base_cost=base_cost,
        mkad_surcharge=mkad_surcharge,
        total_cost=total_cost,
        attachments_json=_json.dumps([{"file_id": a["file_id"], "file_type": a["file_type"]} for a in attachments]) if attachments else None,
    )
    # Set schedule
    await db.update_zamery_request(
        zam_req_id,
        scheduled_date=ds,
        scheduled_time_interval=interval,
    )

    # Create generic task for zamerschik inbox
    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZAMERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=uid,
        assigned_to=assigned_to,
        due_at_iso=None,
        payload={
            "zamery_request_id": zam_req_id,
            "source_type": "schedule",
            "address": address,
            "description": description,
            "client_contact": client_contact,
            "mkad_km": mkad_km,
            "volume_m2": volume_m2,
            "total_cost": total_cost,
        },
    )
    await db.update_zamery_request(zam_req_id, task_id=int(task["id"]))

    # Save attachments
    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    await state.clear()

    msg_target = event.message if isinstance(event, CallbackQuery) else event

    # Confirmation card
    text = f"✅ <b>Замер записан</b>\n"
    text += f"📅 {d.day} {_RU_MONTHS[d.month]} ({wd}) · ⏰ {interval}\n"
    text += f"📍 {address}\n"
    if description:
        text += f"📝 {description}\n"
    if client_contact:
        text += f"📞 {client_contact}\n"
    parts_line = []
    if mkad_km:
        parts_line.append(f"🚗 {mkad_km} км")
    if volume_m2:
        parts_line.append(f"📐 {volume_m2} м²")
    parts_line.append(f"💰 {total_cost} ₽")
    if attachments:
        parts_line.append(f"📎 {len(attachments)}")
    text += " · ".join(parts_line) + "\n\n"
    text += "Задача создана в «📐 Замеры»."

    await msg_target.answer(text)  # type: ignore[union-attr]

    # Send notification to zamerschik with action buttons
    initiator = await get_initiator_label(db, uid)
    notify_text = (
        f"📐 <b>Заявка на замер #{zam_req_id}</b>\n\n"
        f"📅 {d.day} {_RU_MONTHS[d.month]} ({wd})  ⏰ {interval}\n"
        f"📍 {address}\n"
    )
    if description:
        notify_text += f"📝 {description}\n"
    if client_contact:
        notify_text += f"📞 {client_contact}\n"
    if mkad_km:
        notify_text += f"🚗 {mkad_km} км от МКАД\n"
    if volume_m2:
        notify_text += f"📐 {volume_m2} м²\n"
    notify_text += f"💰 {total_cost} ₽\n"
    notify_text += f"\nОт: {initiator}"

    task_kb = task_actions_kb(task)
    await notifier.safe_send(assigned_to, notify_text, reply_markup=task_kb)
    await notifier.notify_workchat(notify_text)

    # Обновить бейдж
    await refresh_recipient_keyboard(notifier, db, config, assigned_to)
    if uid != assigned_to:
        await refresh_recipient_keyboard(notifier, db, config, uid)


# --- Blackout: добавить ---

@router.callback_query(F.data == "zamsched:blackout:add")
async def zamery_blackout_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Start blackout date input."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(ZameryBlackoutSG.pick_dates)

    # Show next 14 days as inline buttons for quick pick
    today = date.today()
    b = InlineKeyboardBuilder()
    for i in range(1, 15):
        d = today + timedelta(days=i)
        wd = _RU_WEEKDAYS[d.weekday()]
        b.button(
            text=f"{d.day} {_RU_MONTHS[d.month]} ({wd})",
            callback_data=f"zamsched:blackout:pick:{d.isoformat()}",
        )
    b.button(text="⬅️ Назад", callback_data="zamsched:blackout:cancel")
    b.adjust(2, 2, 2, 2, 2, 2, 2, 1)

    await cb.message.answer(  # type: ignore[union-attr]
        "🚫 <b>Добавить выходной</b>\n\n"
        "Выберите дату или введите вручную (ДД.ММ.ГГГГ):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("zamsched:blackout:pick:"))
async def zamery_blackout_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Quick-pick a blackout date from inline buttons."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    ds = cb.data.split(":")[-1]  # type: ignore[union-attr]
    uid = cb.from_user.id

    try:
        d = date.fromisoformat(ds)
    except ValueError:
        await cb.message.answer("❌ Некорректная дата.")  # type: ignore[union-attr]
        return

    await db.add_zamery_blackout_date(uid, ds)
    await state.clear()

    wd = _RU_WEEKDAYS[d.weekday()]
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Выходной добавлен: <b>{d.day} {_RU_MONTHS[d.month]} ({wd})</b>"
    )
    # Refresh main schedule
    await _render_schedule_main(cb, db, uid, edit_existing=False)


@router.message(ZameryBlackoutSG.pick_dates)
async def zamery_blackout_manual(message: Message, state: FSMContext, db: Database) -> None:
    """Manual date entry for blackout (DD.MM.YYYY)."""
    uid = message.from_user.id  # type: ignore[union-attr]
    text = (message.text or "").strip()

    # Parse DD.MM.YYYY
    try:
        d = datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer("⚠️ Формат: ДД.ММ.ГГГГ (например 15.03.2026)")
        return

    if d <= date.today():
        await message.answer("⚠️ Дата должна быть в будущем.")
        return

    await db.add_zamery_blackout_date(uid, d.isoformat())
    await state.clear()

    wd = _RU_WEEKDAYS[d.weekday()]
    await message.answer(
        f"✅ Выходной добавлен: <b>{d.day} {_RU_MONTHS[d.month]} ({wd})</b>"
    )
    await _render_schedule_main(message, db, uid, edit_existing=False)


@router.callback_query(F.data == "zamsched:blackout:cancel")
async def zamery_blackout_cancel(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Cancel blackout entry."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await state.clear()
    await _render_schedule_main(cb, db, cb.from_user.id, edit_existing=True)


# --- Blackout: удалить ---

@router.callback_query(F.data.startswith("zamsched:blackout:rm:"))
async def zamery_blackout_remove(cb: CallbackQuery, db: Database) -> None:
    """Remove a blackout date."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    parts = cb.data.split(":")  # type: ignore[union-attr]
    bl_id = int(parts[3])
    week_offset = int(parts[4]) if len(parts) > 4 else 0

    await db.remove_zamery_blackout_date(bl_id)

    # Refresh week view
    uid = cb.from_user.id
    today = date.today()
    mon, sun = _week_range(today, week_offset)

    # Re-render the week
    await cb.message.answer("✅ Выходной удалён.")  # type: ignore[union-attr]

    # Simulate going back to that week
    cb.data = f"zamsched:week:{week_offset}"  # type: ignore[assignment]
    await zamery_schedule_week(cb, db)


# =====================================================================
# РАСЧЁТ ЗП ЗАМЕРЩИКА (ZameryZpSG) — Дополнение 3
# =====================================================================

@router.callback_query(F.data.startswith("zamzp:start:"))
async def zamery_zp_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Start ZP calculation for a specific invoice."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.clear()
    await state.set_state(ZameryZpSG.cost_per_zamery)
    await state.update_data(
        invoice_id=invoice_id,
        invoice_number=inv["invoice_number"],
        address=inv.get("object_address", "-"),
    )

    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Расчёт ЗП — Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n\n"
        "Введите <b>стоимость замера</b> (число, ₽):\n"
        "По умолчанию: <b>2500₽</b>\n"
        "Для отмены: <code>/cancel</code>",
    )


@router.message(ZameryZpSG.cost_per_zamery)
async def zamery_zp_cost(message: Message, state: FSMContext) -> None:
    """Zamery enters cost per measurement."""
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        cost = float(text)
        if cost <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите положительное число (стоимость замера в ₽):")
        return

    await state.update_data(cost_per_zamery=cost)
    await state.set_state(ZameryZpSG.all_same_price)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Да, все по одной цене", callback_data="zamzp:same:yes")
    b.button(text="❌ Нет, цены разные", callback_data="zamzp:same:no")
    b.adjust(1)

    await message.answer(
        f"Стоимость: <b>{cost:,.0f}₽</b>\n\n"
        "Все замеры в отчёте оплачиваются по этой цене?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("zamzp:same:"))
async def zamery_zp_same_price(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    """Zamery answers: same or different prices."""
    await cb.answer()
    answer = cb.data.split(":")[-1]  # type: ignore[union-attr]

    if answer == "yes":
        # Все по одной цене — спрашиваем количество замеров
        await state.set_state(ZameryZpSG.confirm)
        await state.update_data(all_same=True)
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите <b>количество выполненных замеров</b> (целое число):"
        )
    else:
        # Разные цены — ввод каждого замера отдельно
        await state.set_state(ZameryZpSG.custom_prices)
        await state.update_data(all_same=False, custom_entries=[])
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите стоимость каждого замера отдельной строкой.\n\n"
            "Формат: <code>Описание — сумма</code>\n\n"
            "Пример:\n"
            "<code>ул. Ленина 5 — 3000</code>\n"
            "<code>ул. Мира 10 — 4500</code>\n\n"
            "Когда закончите, нажмите /done",
        )


@router.message(ZameryZpSG.confirm)
async def zamery_zp_confirm_count(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Same-price path: zamery enters count of measurements, then summary is sent to GD."""
    if not message.from_user:
        return

    data = await state.get_data()

    if not data.get("all_same"):
        # Shouldn't reach here if all_same=False, but handle gracefully
        await message.answer("Используйте /done для завершения ввода цен.")
        return

    text = (message.text or "").strip()
    try:
        count = int(text)
        if count <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("Введите положительное целое число (количество замеров):")
        return

    cost = data["cost_per_zamery"]
    total = cost * count
    invoice_number = data["invoice_number"]
    invoice_id = data["invoice_id"]
    address = data.get("address", "-")

    details = [{"description": f"Замер x{count}", "cost": cost, "count": count}]

    # Сохраняем в БД
    await db.update_invoice(
        invoice_id,
        zp_zamery_details_json=json.dumps(details, ensure_ascii=False),
        zp_zamery_total=total,
    )
    await db.set_invoice_zp_status(invoice_id, "requested")

    # Отправляем ГД
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    initiator = await get_initiator_label(db, message.from_user.id)
    summary = (
        f"💰 <b>Расчёт ЗП замерщика</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {address}\n\n"
        f"Замеров: <b>{count}</b>\n"
        f"Цена за замер: <b>{cost:,.0f}₽</b>\n"
        f"<b>Итого: {total:,.0f}₽</b>"
    )

    if gd_id:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Утвердить", callback_data=f"zamzp_approve:yes:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"zamzp_approve:no:{invoice_id}")
        b.adjust(1)
        await notifier.safe_send(int(gd_id), summary, reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Расчёт ЗП отправлен ГД.\n"
        f"Замеров: {count}, итого: {total:,.0f}₽",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
            ),
        ),
    )


@router.message(ZameryZpSG.custom_prices)
async def zamery_zp_custom(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Custom-prices path: zamery enters individual measurement costs."""
    if not message.from_user:
        return

    text = (message.text or "").strip()

    # /done — завершить ввод
    if text == "/done":
        data = await state.get_data()
        entries: list[dict[str, Any]] = data.get("custom_entries", [])
        if not entries:
            await message.answer("Вы не ввели ни одного замера. Введите хотя бы один:")
            return

        invoice_id = data["invoice_id"]
        invoice_number = data["invoice_number"]
        address = data.get("address", "-")
        total = sum(e["cost"] for e in entries)

        # Сохраняем в БД
        await db.update_invoice(
            invoice_id,
            zp_zamery_details_json=json.dumps(entries, ensure_ascii=False),
            zp_zamery_total=total,
        )
        await db.set_invoice_zp_status(invoice_id, "requested")

        # Формируем карточку для ГД
        lines = []
        for i, e in enumerate(entries, 1):
            lines.append(f"  {i}. {e['description']} — {e['cost']:,.0f}₽")
        details_text = "\n".join(lines)

        initiator = await get_initiator_label(db, message.from_user.id)
        summary = (
            f"💰 <b>Расчёт ЗП замерщика</b>\n"
            f"👤 От: {initiator}\n\n"
            f"📄 Счёт №: <code>{invoice_number}</code>\n"
            f"📍 Адрес: {address}\n\n"
            f"Замеры:\n{details_text}\n\n"
            f"<b>Итого: {total:,.0f}₽</b>"
        )

        gd_id = await resolve_default_assignee(db, config, Role.GD)
        if gd_id:
            b = InlineKeyboardBuilder()
            b.button(text="✅ Утвердить", callback_data=f"zamzp_approve:yes:{invoice_id}")
            b.button(text="❌ Отклонить", callback_data=f"zamzp_approve:no:{invoice_id}")
            b.adjust(1)
            await notifier.safe_send(int(gd_id), summary, reply_markup=b.as_markup())
            await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

        role, isolated_role = await _current_menu(db, message.from_user.id)
        await state.clear()
        await message.answer(
            f"✅ Расчёт ЗП отправлен ГД.\n"
            f"Замеров: {len(entries)}, итого: {total:,.0f}₽",
            reply_markup=private_only_reply_markup(
                message,
                main_menu(
                    role,
                    is_admin=message.from_user.id in (config.admin_ids or set()),
                    unread=await db.count_unread_tasks(message.from_user.id),
                    isolated_role=isolated_role,
                ),
            ),
        )
        return

    # Парсим строку: "описание — сумма" или просто число
    if "—" in text or " - " in text:
        sep = "—" if "—" in text else " - "
        parts = text.split(sep, 1)
        desc = parts[0].strip()
        cost_str = parts[1].strip().replace(",", ".").replace(" ", "")
    else:
        data = await state.get_data()
        entry_num = len(data.get("custom_entries", [])) + 1
        desc = f"Замер #{entry_num}"
        cost_str = text.replace(",", ".").replace(" ", "")

    try:
        cost = float(cost_str)
        if cost <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer(
            "Не удалось разобрать. Формат: <code>описание — сумма</code>\n"
            "Или просто число (сумма)."
        )
        return

    data = await state.get_data()
    entries = data.get("custom_entries", [])
    entries.append({"description": desc, "cost": cost})
    await state.update_data(custom_entries=entries)

    total_so_far = sum(e["cost"] for e in entries)
    await message.answer(
        f"✅ Принял: {desc} — {cost:,.0f}₽\n"
        f"Замеров: {len(entries)}, промежуточный итог: {total_so_far:,.0f}₽\n\n"
        "Введите следующий замер или нажмите /done для завершения."
    )


# =====================================================================
# GD УТВЕРЖДАЕТ / ОТКЛОНЯЕТ ЗП ЗАМЕРЩИКА
# =====================================================================

@router.callback_query(F.data.startswith("zamzp_approve:"))
async def zamery_zp_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """GD approves or rejects zamery ZP calculation."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    decision = parts[1]  # yes or no
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    zamery_id = await resolve_default_assignee(db, config, Role.ZAMERY)

    if decision == "yes":
        await db.set_invoice_zp_status(invoice_id, "approved")
        total = inv.get("zp_zamery_total", 0) or 0
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ ЗП замерщика по счёту №{inv['invoice_number']} утверждена.\n"
            f"Сумма: {total:,.0f}₽"
        )
        # Уведомляем замерщика
        if zamery_id:
            await notifier.safe_send(
                int(zamery_id),
                f"✅ <b>ЗП утверждена</b>\n\n"
                f"Счёт №: <code>{inv['invoice_number']}</code>\n"
                f"Сумма: {total:,.0f}₽",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(zamery_id))
    else:
        await db.set_invoice_zp_status(invoice_id, "not_requested")
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ ЗП замерщика по счёту №{inv['invoice_number']} отклонена."
        )
        if zamery_id:
            await notifier.safe_send(
                int(zamery_id),
                f"❌ <b>ЗП отклонена</b>\n\n"
                f"Счёт №: <code>{inv['invoice_number']}</code>\n"
                "Свяжитесь с ГД для уточнения.",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(zamery_id))
