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

from aiogram import Router, F, html
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import ZamAttrCb, ZamZpPayCb, ZamZpRejectCb
from ..config import Config
from ..db import Database
from ..enums import (
    InvoiceStatus, Role, TaskStatus,
    ZAMERY_SOURCE_LABELS,
)
from ..integrations.minio_storage import MinioStorage
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
from ..states import ZameryAcceptSG, ZameryBlackoutSG, ZameryCompleteSG, ZameryQuickBookSG, ZameryZpSG
from ..utils import answer_service, format_card_section, format_zamery_settlement_card, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard
from ._mirror import mirror_attachment
from .auth import require_role_callback, require_role_message
from .money_guard import money_confirm_guard

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
    db_inst: Database | None = data.get("db")
    cfg = data.get("config")
    if not db_inst or not cfg:
        return result
    try:
        user = await db_inst.get_user_optional(u.id)
        if not user or not user.role:
            return result
        menu_role = resolve_active_menu_role(u.id, user.role)
        if menu_role != Role.ZAMERY:
            return result
        unread = await db_inst.count_unread_tasks(u.id)
        uc = await db_inst.count_unread_by_channel(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
            unread_channels=uc,
        )
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

    # Сводка по менеджерам (эталон-карточка: счётчики + Всего в футере)
    stats = await db.get_zamery_stats_by_manager(uid)
    mgr_items = [
        (_REQ_ROLE_SHORT.get(s.get("requester_role", ""), s.get("requester_role", "?")), str(s["cnt"]))
        for s in stats
    ]
    text = format_card_section(
        "📐", "Замеры",
        items=mgr_items or [("Заявок", str(len(all_reqs)))],
        footer=("Всего", str(len(all_reqs))) if mgr_items else None,
    )
    await message.answer(text, reply_markup=zamery_incoming_kb(all_reqs, back_callback="nav:home"))


@router.callback_query(F.data.startswith("zam_in:view:"))
async def zamery_view_request(
    cb: CallbackQuery, db: Database,
) -> None:
    """Замерщик: просмотр карточки заявки."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    try:
        req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные заявки.")  # type: ignore[union-attr]
        return
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    initiator = await get_initiator_label(db, req["requested_by"])
    text = _format_request_card(req, initiator, with_schedule=False)

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

    try:
        req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные заявки.")  # type: ignore[union-attr]
        return
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
    try:
        d = date.fromisoformat(chosen_date)
    except ValueError:
        await cb.message.answer("❌ Некорректная дата.")  # type: ignore[union-attr]
        return
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
    req_id = data.get("accept_req_id")
    if not req_id:
        msg = event.message if isinstance(event, CallbackQuery) else event
        await msg.answer("❌ Ошибка состояния. Попробуйте снова.")  # type: ignore[union-attr]
        await state.clear()
        return
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
        try:
            d = date.fromisoformat(data["scheduled_date"])
            day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            notify_text += f"📅 Дата: {day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}\n"
        except (ValueError, TypeError):
            pass
    if data.get("scheduled_time_interval"):
        notify_text += f"⏰ Время: {data['scheduled_time_interval']}\n"
    if data.get("accept_comment"):
        notify_text += f"💬 Комментарий: {data['accept_comment']}\n"

    await notifier.safe_send(req["requested_by"], notify_text)
    await refresh_recipient_keyboard(notifier, db, config, req["requested_by"])

    # Уведомить РП о принятии замера
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id and rp_id != req["requested_by"]:
        rp_msg = (
            f"✅ <b>Замер #{req_id} принят замерщиком</b>\n"
            f"📍 {req.get('address', '—')}\n"
        )
        if data.get("scheduled_date"):
            rp_msg += f"📅 Дата: {data['scheduled_date']}\n"
        await notifier.safe_send(int(rp_id), rp_msg)

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

    try:
        req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные заявки.")  # type: ignore[union-attr]
        return
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
    await _render_my_objects(message, db, message.from_user.id)  # type: ignore[union-attr]


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

    items = []
    for inv in active[:20]:
        status_emoji = {"in_progress": "🔄", "paid": "✅"}.get(inv["status"], "❓")
        items.append((
            f"{status_emoji} №{inv['invoice_number']}",
            html.quote((inv.get("object_address") or "—")[:30]),
        ))
    text = format_card_section(
        "📋", "Счета в работе", items=items,
        footer=("Всего", str(len(active))), compact=True,
    )
    await cb.message.answer(text)  # type: ignore[union-attr]


@router.callback_query(F.data == "zam_my:back")
async def zamery_my_back(cb: CallbackQuery, db: Database) -> None:
    """Назад к дашборду «Мои замеры»."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _render_my_objects(cb.message, db, cb.from_user.id)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zam_myreq:view:"))
async def zamery_myreq_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр заявки из «Мои замеры» + кнопка отправки."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    try:
        req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные заявки.")  # type: ignore[union-attr]
        return
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    initiator = await get_initiator_label(db, req["requested_by"])
    text = _format_request_card(req, initiator, with_schedule=True)

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
    try:
        req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные заявки.")  # type: ignore[union-attr]
        return
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
async def zamery_complete_file(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Приём файлов: фото, видео, документы."""
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("complete_attachments", [])

    uid = message.from_user.id if message.from_user else "anon"
    att = await mirror_attachment(message, storage, prefix=f"zamery/{uid}")
    if att is None:
        await message.answer("Пришлите файл, фото или видео. Или нажмите «Далее».")
        return
    attachments.append(att)

    await state.update_data(complete_attachments=attachments)
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.{suffix}")


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
    data = await state.get_data()
    req_id = data.get("complete_req_id")
    msg_target = event.message if isinstance(event, CallbackQuery) else event
    if not req_id:
        await msg_target.answer("❌ Ошибка состояния. Попробуйте снова.")  # type: ignore[union-attr]
        await state.clear()
        return
    req = await db.get_zamery_request(req_id)

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

    # Кто выполнил замер (исполнитель)
    actor_uid = event.from_user.id if event.from_user else None
    initiator = await get_initiator_label(db, actor_uid) if actor_uid else None

    # Дата замера (человекочитаемо)
    scheduled_label = ""
    if req.get("scheduled_date"):
        try:
            d = date.fromisoformat(req["scheduled_date"])
            day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            scheduled_label = f"{day_names[d.weekday()]} {d.strftime('%d.%m.%Y')}"
            if req.get("scheduled_time_interval"):
                scheduled_label += f" {req['scheduled_time_interval']}"
        except ValueError:
            pass

    # Эталонная карточка-отчёт «замер выполнен» (единый вид для менеджера и РП)
    zam_items: list[tuple[str, str]] = [
        ("Статус", "Выполнен"),
        ("Адрес", html.quote(str(req.get("address") or "—"))),
    ]
    if scheduled_label:
        zam_items.append(("Дата замера", html.quote(scheduled_label)))
    if time_label:
        zam_items.append(("Время выполнения", html.quote(time_label)))
    if comment:
        zam_items.append(("Комментарий", html.quote(str(comment))))
    head = "✅ <b>Замер выполнен</b>"
    if initiator:
        head += f"\n👤 Исполнитель: {initiator}"
    notify_text = (
        f"{head}\n\n"
        + format_card_section(
            emoji="📐", title=f"Замер #{req_id}", items=zam_items, compact=True,
        )
    )

    # Уведомить менеджера
    await notifier.safe_send(req["requested_by"], notify_text)
    # Отправить вложения
    for a in attachments:
        ft = a.get("file_type", "document")
        fid = a.get("file_id")
        if fid:
            await notifier.safe_send_media(req["requested_by"], ft, fid, caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, req["requested_by"])

    # Уведомить РП о завершении замера (та же карточка)
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id and rp_id != req["requested_by"]:
        await notifier.safe_send(int(rp_id), notify_text)
        # Отправить бланк замера РП
        for a in attachments:
            ft = a.get("file_type", "document")
            fid = a.get("file_id")
            if fid:
                await notifier.safe_send_media(int(rp_id), ft, fid, caption=a.get("caption"))

    # Обновить бейдж замерщика
    uid = event.from_user.id if isinstance(event, (Message, CallbackQuery)) and event.from_user else None
    if uid:
        await refresh_recipient_keyboard(notifier, db, config, uid)
    await state.clear()


# =====================================================================
# ОПЛАТА ЗАМЕРОВ (список неоплаченных + кнопки ЗП)
# =====================================================================

def _fmt_money(x: float | int | None) -> str:
    """Целое со space-разделителем тысяч, без ₽ (для эталон-ячеек <pre>)."""
    return f"{int(x or 0):,}".replace(",", " ")


# Эталон-помощники экрана «Оплата замеров» (источник = zamery_requests, ТЗ 06.07).
_ZAM_PAY_STATUS_LABEL = {
    "not_requested": "К оплате",
    "requested": "На проверке",
    "paid": "Оплачено",
}
_ZAM_PAY_STATUS_ICON = {"not_requested": "🟡", "requested": "⏳", "paid": "✅"}


def _zam_ident(z: dict[str, Any]) -> str:
    """Опознавательный признак замера = адрес (fallback — #id), HTML-экранирован."""
    return html.quote(z.get("address") or f"#{z.get('id', '?')}")


# ---- Атрибуция замеров: бот спрашивает замерщика, кто из менеджеров направлял ----
# UNK-замеры (без счёта и без следа в чатах) атрибутирует только замерщик по памяти.
# Аналитика: пишет requester_role/requested_by, на долг/total_cost НЕ влияет.
_ATTR_MANAGERS: dict[str, dict[str, Any]] = {
    "kv":  {"role": "manager_kv",  "tg_id": 5641023011, "name": "Кирилл", "short": "КВ"},
    "npn": {"role": "manager_npn", "tg_id": 6546325840, "name": "Паша",   "short": "НПН"},
    "kia": {"role": "manager_kia", "tg_id": 495451226,  "name": "Илья",   "short": "КИА"},
}


def _attr_fmt_ddmm(iso: str | None) -> str:
    """ISO-дата → «дд.мм» для карточек атрибуции."""
    if not iso:
        return "—"
    try:
        return datetime.strptime(str(iso)[:10], "%Y-%m-%d").strftime("%d.%m")
    except (ValueError, TypeError):
        return str(iso)


def build_attr_question(req: dict[str, Any]) -> tuple[str, Any]:
    """Карточка-вопрос замерщику: кто из менеджеров направлял на этот замер."""
    zam_id = int(req["id"])
    text = (
        "❓ <b>Кто отправлял тебя на замер?</b>\n\n"
        f"📍 {html.quote(req.get('address') or '—')}\n"
        f"🗓 {_attr_fmt_ddmm(req.get('scheduled_date'))}"
    )
    b = InlineKeyboardBuilder()
    b.button(text="Кирилл", callback_data=ZamAttrCb(zam_id=zam_id, role="kv").pack())
    b.button(text="Паша", callback_data=ZamAttrCb(zam_id=zam_id, role="npn").pack())
    b.button(text="Илья", callback_data=ZamAttrCb(zam_id=zam_id, role="kia").pack())
    b.button(text="🤷 Не помню", callback_data=ZamAttrCb(zam_id=zam_id, role="unknown").pack())
    b.adjust(3, 1)
    return text, b.as_markup()


@router.callback_query(ZamAttrCb.filter())
async def zamery_attr_answer(
    cb: CallbackQuery, callback_data: ZamAttrCb,
    db: Database, config: Config, notifier: Notifier,
) -> None:
    """Замерщик отвечает на вопрос атрибуции: проставить менеджера / «не помню».

    Пишет requester_role+requested_by в zamery_requests (аналитика, долг не трогает),
    затем уведомляет ГД прогрессом. Гард: только свой done-замер.
    """
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    zam_id = callback_data.zam_id
    role_key = callback_data.role
    req = await db.get_zamery_request(zam_id)
    if (
        not req
        or req.get("assigned_to") != cb.from_user.id
        or req.get("status") != "done"
    ):
        await cb.answer("❌ Замер не найден.", show_alert=True)
        return

    # «↩️ Изменить» — вернуть карточку-вопрос с кнопками выбора
    if role_key == "change":
        text, kb = build_attr_question(req)
        try:
            await cb.message.edit_text(text, reply_markup=kb)  # type: ignore[union-attr]
        except Exception:
            pass
        await cb.answer()
        return

    addr = req.get("address") or "—"
    change_kb = InlineKeyboardBuilder()
    change_kb.button(
        text="↩️ Изменить",
        callback_data=ZamAttrCb(zam_id=zam_id, role="change").pack(),
    )

    if role_key == "unknown":
        # «Не помню» — оставить/вернуть без менеджера (UNK)
        if req.get("requester_role") or req.get("requested_by"):
            await db.update_zamery_request(zam_id, requester_role="", requested_by=0)
        await db.audit(
            actor_id=cb.from_user.id, action="zam_attr_unknown",
            entity="zamery_request", entity_id=str(zam_id),
            payload={"address": addr, "by": "surveyor"},
        )
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"🤷 <b>{html.quote(addr)}</b> — оставил без менеджера.",
                reply_markup=change_kb.as_markup(),
            )
        except Exception:
            pass
        await cb.answer("Ок")
        gd_note = f"🤷 {addr} — «не помню»"
    else:
        m = _ATTR_MANAGERS.get(role_key)
        if not m:
            await cb.answer("❌ Неизвестный выбор.", show_alert=True)
            return
        await db.update_zamery_request(
            zam_id, requester_role=m["role"], requested_by=m["tg_id"],
        )
        await db.audit(
            actor_id=cb.from_user.id, action="zam_attr_set",
            entity="zamery_request", entity_id=str(zam_id),
            payload={"role": m["role"], "requested_by": m["tg_id"],
                     "address": addr, "by": "surveyor"},
        )
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"✅ <b>{html.quote(addr)}</b> → {m['name']} ({m['short']})",
                reply_markup=change_kb.as_markup(),
            )
        except Exception:
            pass
        await cb.answer("Записал ✅")
        gd_note = f"✅ {addr} → {m['name']} ({m['short']})"

    # Уведомить ГД: прогресс + сколько замеров ещё без менеджера
    try:
        rows = await db.list_zamery_attribution(cb.from_user.id)
        remaining = sum(1 for r in rows if not r.get("requester_role"))
        gd_id = await resolve_default_assignee(db, config, Role.GD)
        if gd_id:
            tail = (
                "\n\n🎉 Все замеры распределены."
                if remaining == 0
                else f"\n\nОсталось без менеджера: {remaining}"
            )
            await notifier.safe_send(
                int(gd_id), f"🗺 <b>Атрибуция замеров</b>\n{gd_note}{tail}",
            )
    except Exception:
        log.exception("zamery_attr_answer: GD notify failed")


def _zam_pay_group(z: dict[str, Any]) -> str:
    """Группа замера на экране оплаты: paid | requested | not_requested.

    «Оплачено» = paid_amount проставлен; иначе — по pay_status.
    """
    if z.get("paid_amount") is not None:
        return "paid"
    return z.get("pay_status") or "not_requested"


_DOW = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_REQ_ROLE_SHORT = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}
_REQ_STATUS_LABEL = {
    "open": "⏳ Новая", "in_progress": "🔄 В работе",
    "done": "✅ Выполнена", "rejected": "❌ Отклонена",
}


def _format_request_card(
    req: dict[str, Any], initiator: str, *, with_schedule: bool,
) -> str:
    """Карточка заявки на замер в эталон-дизайне (compact: длинные кирилл.
    значения не выравниваются вправо — [[feedback_card_telegram_pre_alignment]])."""
    role_short = _REQ_ROLE_SHORT.get(req.get("requester_role", ""), "")
    source_label = ZAMERY_SOURCE_LABELS.get(req.get("source_type", ""), "—")
    status_label = _REQ_STATUS_LABEL.get(req.get("status", ""), "❓")

    # user 29.06: для замерщика показываем ТОЛЬКО дивизион менеджера (НПН/КВ/КИА),
    # без личного имени/username (initiator больше не выводим).
    mgr = role_short or "—"
    items: list[tuple[str, str]] = [
        ("Менеджер", mgr),
        ("Адрес", html.quote(req.get("address") or "—")),
    ]
    if req.get("client_contact"):
        items.append(("Контакт", html.quote(str(req["client_contact"]))))
    if req.get("volume_m2"):
        items.append(("Объём", f"{req['volume_m2']} м²"))
    mkad_km = req.get("mkad_km") or 0
    mkad_surcharge = req.get("mkad_surcharge") or 0
    if mkad_km and mkad_km > 0:
        items.append((
            "МКАД",
            f"{mkad_km} км" + (f" (+{_fmt_money(mkad_surcharge)})" if mkad_surcharge else ""),
        ))
    else:
        items.append(("МКАД", "внутри"))
    if req.get("total_cost"):
        items.append(("Стоимость", _fmt_money(req["total_cost"])))
    # Дата/время замера (указывает менеджер при создании заявки) — показываем ВСЕГДА
    # (вход. заявка + «Мои замеры»), user 29.06.
    if req.get("scheduled_date"):
        try:
            d = date.fromisoformat(req["scheduled_date"])
            items.append(("Дата", f"{_DOW[d.weekday()]} {d.strftime('%d.%m.%Y')}"))
        except (ValueError, TypeError):
            pass
    if req.get("scheduled_time_interval"):
        items.append(("Время", str(req["scheduled_time_interval"])))
    if with_schedule:
        if req.get("accept_comment"):
            items.append(("Комментарий", html.quote(str(req["accept_comment"]))))
    if req.get("description"):
        items.append(("Описание", html.quote(str(req["description"]))))
    items.append(("Источник", source_label))
    items.append(("Статус", status_label))
    items.append(("Создана", str(req.get("created_at") or "—")[:16]))
    return format_card_section(
        "📐", f"Заявка на замер #{req['id']}", items=items, compact=True,
    )


async def _render_my_objects(msg: Message, db: Database, user_id: int) -> None:
    """Дашборд «Мои замеры» (эталон) — конверсия + кнопки подменю.
    Общий для reply-входа и кнопки «⬅️ Назад»."""
    conv = await db.get_zamery_conversion_stats(user_id)
    items = [
        ("Конверсия", f"{conv['conversion_pct']}%"),
        ("Счетов", str(conv["total_with_invoice"])),
        ("Замеров", str(conv["total_done"])),
    ]
    for r in conv["by_role"]:
        items.append((_REQ_ROLE_SHORT.get(r.get("requester_role", ""), "?"), f"{r['pct']}%"))
    text = format_card_section("📋", "Мои замеры", items=items)

    active_reqs = await db.list_zamery_requests(assigned_to=user_id, status="in_progress", limit=50)
    b = InlineKeyboardBuilder()
    b.button(text=f"📐 Заявки на замер ({len(active_reqs)})", callback_data="zam_my:requests")
    b.button(text="💰 Взаиморасчёты", callback_data="zam_my:settle")
    b.adjust(1)
    await msg.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "zam_my:settle")
async def zamery_my_settlement(cb: CallbackQuery, db: Database) -> None:
    """Замерщик: Взаиморасчёты — read-only витрина своего долга/платежей."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    uid = cb.from_user.id
    summary = await db.get_zamery_settlement_summary(uid)
    me = await db.get_user_optional(uid)
    name = (me.full_name if me else None) or "Замерщик"
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад", callback_data="zam_my:back")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        format_zamery_settlement_card(summary, name), reply_markup=b.as_markup(),
    )


async def _render_payment_list(
    target: Message | CallbackQuery,
    db: Database,
    user_id: int,
) -> None:
    """Отрисовка «Оплата замеров» — источник zamery_requests (ТЗ 06.07)."""
    zamery = await db.list_zamery_for_payment(user_id)
    msg = target.message if isinstance(target, CallbackQuery) else target

    if not zamery:
        await msg.answer("📭 Нет выполненных замеров для оплаты.")  # type: ignore[union-attr]
        return

    to_pay = [z for z in zamery if _zam_pay_group(z) == "not_requested"]
    on_review = [z for z in zamery if _zam_pay_group(z) == "requested"]
    paid_list = [z for z in zamery if _zam_pay_group(z) == "paid"]
    total_all = sum(z.get("total_cost") or 0 for z in zamery)

    # --- Шапка-сводка (эталон-v2: счётчики + Сумма в футере) ---
    summary = format_card_section(
        "💳", "Оплата замеров",
        items=[
            ("Всего", str(len(zamery))),
            ("К оплате", str(len(to_pay))),
            ("На проверке", str(len(on_review))),
            ("Оплачено", str(len(paid_list))),
        ],
        footer=("Сумма", _fmt_money(total_all)),
    )
    await msg.answer(summary)  # type: ignore[union-attr]

    # --- Карточки: каждый замер отдельным сообщением (заголовок = адрес) ---
    for z in (to_pay + on_review + paid_list)[:30]:
        grp = _zam_pay_group(z)
        cost = z.get("total_cost")
        items = [("Стоимость", _fmt_money(cost) if cost else "—")]
        if grp == "paid" and z.get("paid_date"):
            items.append(("Дата оплаты", str(z["paid_date"])))
        items.append(("Статус", _ZAM_PAY_STATUS_LABEL.get(grp, "К оплате")))
        card = format_card_section(_ZAM_PAY_STATUS_ICON.get(grp, "🟡"), _zam_ident(z), items=items)
        await msg.answer(card)  # type: ignore[union-attr]

    # --- Кнопка «Отправить в оплату» (только «К оплате» с ценой) ---
    sendable = [z for z in to_pay if (z.get("total_cost") or 0) > 0]
    bottom_b = InlineKeyboardBuilder()
    if sendable:
        total_send = int(sum(z["total_cost"] for z in sendable))
        bottom_b.button(
            text=f"📤 Отправить в оплату · {len(sendable)} зам. · {total_send}₽",
            callback_data="zampay:send_batch",
        )
    bottom_b.button(text="⬅️ Назад", callback_data="nav:home")
    bottom_b.adjust(1)
    await msg.answer("⬇️", reply_markup=bottom_b.as_markup())  # type: ignore[union-attr]


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


# --- Пакетная отправка в оплату ГД ---

@router.callback_query(F.data == "zampay:send_batch")
async def zamery_send_batch(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Отправить все неоплаченные замеры «К оплате» с ценой → превью."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id

    zamery = await db.list_zamery_for_payment(user_id)
    sendable = [
        z for z in zamery
        if _zam_pay_group(z) == "not_requested" and (z.get("total_cost") or 0) > 0
    ]

    if not sendable:
        await cb.message.answer("⚠️ Нет замеров для отправки.")  # type: ignore[union-attr]
        return

    # Показать итоговый расчёт карточкой. Число СЛЕВА (моноширинно от фикс. старта),
    # адрес — в конце строки: при переменной кириллице колонка сумм не съезжает
    # ([[feedback_card_telegram_pre_alignment]]).
    total = sum(z["total_cost"] for z in sendable)
    indent = "   "
    rows = [
        f"{indent}{_fmt_money(z['total_cost']):>7}  {_zam_ident(z)}"
        for z in sendable
    ]
    body = "\n".join(
        rows + [indent + "━" * 14, f"{indent}Итого {_fmt_money(total)}"]
    )
    text = f"📤 <b>Отправить в оплату</b>\n<pre>{body}</pre>\n\nОтправить ГД?"

    b = InlineKeyboardBuilder()
    b.button(
        text=f"✅ Отправить · {len(sendable)} зам. · {_fmt_money(total)} ₽",
        callback_data="zampay:confirm_batch",
    )
    b.button(text="⬅️ Назад", callback_data="zampay:back")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "zampay:confirm_batch")
@money_confirm_guard
async def zamery_confirm_batch(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Подтверждение → создать задачу ГД (флоу как ЗП РП), пометить requested."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    user_id = cb.from_user.id

    zamery = await db.list_zamery_for_payment(user_id)
    sendable = [
        z for z in zamery
        if _zam_pay_group(z) == "not_requested" and (z.get("total_cost") or 0) > 0
    ]
    if not sendable:
        await cb.message.answer("⚠️ Нет замеров для отправки.")  # type: ignore[union-attr]
        return

    total = sum(z["total_cost"] for z in sendable)
    zam_ids = [z["id"] for z in sendable]

    # Создать задачу для ГД. resolve ДО смены статусов — если ГД не найден, не оставим
    # замеры «повисшими» в requested.
    from ..enums import TaskType
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer("⚠️ ГД не найден.")  # type: ignore[union-attr]
        return

    # Пометить замеры → requested («На проверке»)
    await db.set_zamery_pay_status(zam_ids, "requested", user_id)

    initiator = await get_initiator_label(db, user_id)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZP_ZAMERY_BATCH,
        status=TaskStatus.OPEN,
        created_by=user_id,
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "zam_ids": zam_ids,
            "total": total,
            "count": len(sendable),
            "surveyor_id": user_id,
        },
    )

    # Уведомить ГД (эталон: число слева / адрес справа, [[feedback_card_telegram_pre_alignment]]).
    # Кнопки = флоу выплаты ЗП замерщика (ZamZpPayCb → тумблер-выбор → платёж в леджер).
    indent = "   "
    rows = [
        f"{indent}{_fmt_money(z['total_cost']):>7}  {_zam_ident(z)}"
        for z in sendable
    ]
    body = "\n".join(rows + [indent + "━" * 14, f"{indent}Итого {_fmt_money(total)}"])
    gd_text = (
        f"💰 <b>ЗП Замерщика — {html.quote(initiator)}</b>\n"
        f"<pre>{body}</pre>\n"
        f"({len(sendable)} зам.)"
    )
    b = InlineKeyboardBuilder()
    b.button(text="✅ Выплатить", callback_data=ZamZpPayCb(task_id=task["id"]).pack())
    b.button(text="❌ Отклонить", callback_data=ZamZpRejectCb(task_id=task["id"]).pack())
    b.adjust(2)
    await notifier.safe_send(int(gd_id), gd_text, reply_markup=b.as_markup())
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос на оплату отправлен ГД.\n"
        f"💵 {len(sendable)} замеров на сумму {int(total)}₽"
    )


# ГД-сторона выплаты ЗП замерщика (одобрение/отклонение пакета) перенесена в
# handlers/gd.py — флоу ZamZp* (тумблер-выбор замеров → платёж в леджер +
# mark_zamery_paid), объединён с взаиморасчётами по образцу ЗП РП (ТЗ 06.07).


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


def _split_blackouts(blackouts: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    """(off_dates, busy_dates) по полю kind: 'busy' = «День занят» (закрыт для новых
    замеров, но взятые остаются), иначе — выходной (день полностью недоступен)."""
    off: set[str] = set()
    busy: set[str] = set()
    for bl in blackouts:
        d = bl["blackout_date"]
        if (bl.get("kind") or "off") == "busy":
            busy.add(d)
        else:
            off.add(d)
    return off, busy


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
    off, busy = _split_blackouts(blackouts)
    return {"zamery": len(zamery), "blackouts": len(off), "busy": len(busy)}


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
    text += f"📐 {cur_s['zamery']} замеров · 🚫 {cur_s['blackouts']} вых · 🔒 {cur_s.get('busy', 0)} занят\n\n"
    for d, s in months_stats:
        is_cur = d.month == today.month and d.year == today.year
        icon = "▶️" if is_cur else "▫️"
        text += f"{icon} {_RU_MONTH_NAMES[d.month]}: <b>{s['zamery']}</b>"
        if s["blackouts"]:
            text += f" · 🚫{s['blackouts']}"
        if s.get("busy"):
            text += f" · 🔒{s['busy']}"
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
        off_w, busy_w = _split_blackouts(blackouts)
        badge = ""
        if cnt > 0:
            badge += f" · 📐{cnt}"
        if off_w:
            badge += f" · 🚫{len(off_w)}"
        if busy_w:
            badge += f" · 🔒{len(busy_w)}"
        if cnt == 0 and not off_w and not busy_w:
            badge = " · свободна"

        b.button(
            text=f"{label}{badge}",
            callback_data=f"zamsched:week:{w}",
        )

    b.button(text="🚫 Добавить выходной", callback_data="zamsched:blackout:add")
    b.button(text="🔒 День занят", callback_data="zamsched:busy:add")
    b.button(text="🔄 Обновить", callback_data="zamsched:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    msg = target.message if isinstance(target, CallbackQuery) else target
    if edit_existing and isinstance(target, CallbackQuery):
        try:
            await msg.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
            return
        except Exception:
            pass
    await msg.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


async def _render_schedule_week(
    target: Message | CallbackQuery,
    db: Database,
    uid: int,
    week_offset: int,
    *,
    edit_existing: bool = True,
) -> None:
    """Render detailed week view and preserve the selected week context."""
    today = date.today()
    mon, sun = _week_range(today, week_offset)
    d_from = mon.isoformat()
    d_to = sun.isoformat()

    zamery = await db.list_zamery_for_schedule(uid, d_from, d_to)
    blackouts = await db.list_zamery_blackout_dates(uid, d_from, d_to)

    zam_by_date: dict[str, list[dict]] = {}
    for z in zamery:
        d = z["scheduled_date"]
        zam_by_date.setdefault(d, []).append(z)

    off_set, busy_set = _split_blackouts(blackouts)
    block_set = off_set | busy_set  # дни, закрытые для НОВЫХ замеров (вых + занят)
    blackout_map = {bl["blackout_date"]: bl for bl in blackouts}

    text = f"📅 <b>{_format_date_short(mon)} — {_format_date_short(sun)}</b>\n\n"

    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        wd = _RU_WEEKDAYS[day.weekday()]
        day_label = f"{day.day} {_RU_MONTHS[day.month]} ({wd})"

        if ds in off_set:
            bl = blackout_map.get(ds)
            cmt = f" ({bl['comment']})" if bl and bl.get("comment") else ""
            text += f"🚫 <b>{day_label}</b> — выходной{cmt}\n"
        elif ds in busy_set:
            text += f"🔒 <b>{day_label}</b> — занят (закрыт для новых)\n"
            for z in zam_by_date.get(ds, []):
                interval = z.get("scheduled_time_interval") or ""
                mgr = z.get("manager_name") or "—"
                addr = z.get("address") or "—"
                text += f"  ⏰ {interval} · 👤 {mgr} · 📍 {addr}\n"
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
    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        if day >= today and ds not in block_set and ds not in zam_by_date:
            wd = _RU_WEEKDAYS[day.weekday()]
            b.button(
                text=f"🟢 {day.day} {_RU_MONTHS[day.month]} ({wd}) — записать",
                callback_data=f"zamsched:book:{ds}:{week_offset}",
            )
    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        if day >= today and ds not in block_set and ds in zam_by_date:
            wd = _RU_WEEKDAYS[day.weekday()]
            b.button(
                text=f"📐 {day.day} {_RU_MONTHS[day.month]} ({wd}) — доп. замер",
                callback_data=f"zamsched:book:{ds}:{week_offset}",
            )

    if week_offset > 0:
        b.button(text="⬅️ Пред. неделя", callback_data=f"zamsched:week:{week_offset - 1}")
    if week_offset < 8:
        b.button(text="След. неделя ➡️", callback_data=f"zamsched:week:{week_offset + 1}")
    b.button(text="🚫 Добавить выходной", callback_data="zamsched:blackout:add")
    b.button(text="🔒 День занят", callback_data="zamsched:busy:add")
    for bl in blackouts:
        bd = date.fromisoformat(bl["blackout_date"])
        if (bl.get("kind") or "off") == "busy":
            rm_text = f"🔓 Убрать занят {bd.day} {_RU_MONTHS[bd.month]}"
        else:
            rm_text = f"❌ Убрать выходной {bd.day} {_RU_MONTHS[bd.month]}"
        b.button(
            text=rm_text,
            callback_data=f"zamsched:blackout:rm:{bl['id']}:{week_offset}",
        )
    b.button(text="⬅️ К списку недель", callback_data="zamsched:main")
    b.adjust(1)

    msg = target.message if isinstance(target, CallbackQuery) else target
    if edit_existing and isinstance(target, CallbackQuery):
        try:
            await msg.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
            return
        except Exception:
            pass
    await msg.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "zamsched:refresh")
async def zamery_schedule_refresh(cb: CallbackQuery, db: Database) -> None:
    """#15: Обновить график замеров."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer("🔄 Обновлено")
    uid = cb.from_user.id
    await _render_schedule_main(cb, db, uid, edit_existing=True)


@router.callback_query(F.data.startswith("zamsched:week:"))
async def zamery_schedule_week(cb: CallbackQuery, db: Database) -> None:
    """Detailed week view — 7 days with zamery, blackouts, free."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()

    uid = cb.from_user.id
    week_offset = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await _render_schedule_week(cb, db, uid, week_offset, edit_existing=True)


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
async def zamery_book_attachment(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Получен файл — сохраняем и ждём ещё."""
    data = await state.get_data()
    attachments = data.get("book_attachments", [])
    uid = message.from_user.id if message.from_user else "anon"
    att = await mirror_attachment(message, storage, prefix=f"zamery/{uid}")
    if att is not None:
        attachments.append(att)
    await state.update_data(book_attachments=attachments)
    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Сохранить замер ({len(attachments)} вл.)", callback_data="zamsched:book:finalize")
    b.adjust(1)
    suffix = " (☁️ зеркало)" if att and att.get("minio_object_key") else ""
    await message.answer(
        f"📎 Добавлено: {len(attachments)}.{suffix} Отправьте ещё или сохраните:",
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

    uid = event.from_user.id if event.from_user else 0
    d = date.fromisoformat(ds)
    wd = _RU_WEEKDAYS[d.weekday()]

    # Resolve zamery user
    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    assigned_to = int(zamery_uid) if zamery_uid else uid

    # Determine requester role
    user = await db.get_user_optional(uid)
    requester_role = resolve_active_menu_role(uid, user.role if user else None) or "zamery"

    # Cost calculation — единый тариф из config (общий хелпер с manager_new)
    from ..config import compute_zamery_cost
    base_cost, mkad_surcharge, total_cost = compute_zamery_cost(mkad_km)

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
            minio_object_key=a.get("minio_object_key"),
        )

    await state.clear()

    msg_target = event.message if isinstance(event, CallbackQuery) else event

    # Confirmation card
    text = "✅ <b>Замер записан</b>\n"
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

async def _render_day_mark_picker(
    cb: CallbackQuery, state: FSMContext, kind: str,
) -> None:
    """Пикер даты для отметки дня (kind='off' выходной / 'busy' день занят).
    Сохраняет kind в state для ручного ввода и формирует pick-кнопки нужного типа."""
    await state.clear()
    await state.set_state(ZameryBlackoutSG.pick_dates)
    await state.update_data(bo_kind=kind)

    pick_prefix = "zamsched:busy:pick" if kind == "busy" else "zamsched:blackout:pick"
    today = date.today()
    b = InlineKeyboardBuilder()
    for i in range(1, 15):
        d = today + timedelta(days=i)
        wd = _RU_WEEKDAYS[d.weekday()]
        b.button(
            text=f"{d.day} {_RU_MONTHS[d.month]} ({wd})",
            callback_data=f"{pick_prefix}:{d.isoformat()}",
        )
    b.button(text="⬅️ Назад", callback_data="zamsched:blackout:cancel")
    b.adjust(2, 2, 2, 2, 2, 2, 2, 1)

    title = (
        "🔒 <b>День занят</b>\n\nДень закроется для НОВЫХ замеров (взятые останутся).\n"
        if kind == "busy"
        else "🚫 <b>Добавить выходной</b>\n\n"
    )
    await cb.message.answer(  # type: ignore[union-attr]
        title + "Выберите дату или введите вручную (ДД.ММ.ГГГГ):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "zamsched:blackout:add")
async def zamery_blackout_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Start blackout (выходной) date input."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _render_day_mark_picker(cb, state, "off")


@router.callback_query(F.data == "zamsched:busy:add")
async def zamery_busy_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Start «день занят» date input."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _render_day_mark_picker(cb, state, "busy")


async def _finalize_day_mark(
    cb: CallbackQuery, state: FSMContext, db: Database, ds: str, kind: str,
) -> None:
    """Сохранить отметку дня (kind) по выбранной дате и вернуться к графику."""
    uid = cb.from_user.id
    try:
        d = date.fromisoformat(ds)
    except ValueError:
        await cb.message.answer("❌ Некорректная дата.")  # type: ignore[union-attr]
        return

    await db.add_zamery_blackout_date(uid, ds, kind=kind)
    await state.clear()

    wd = _RU_WEEKDAYS[d.weekday()]
    label = "День занят" if kind == "busy" else "Выходной"
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ {label} добавлен: <b>{d.day} {_RU_MONTHS[d.month]} ({wd})</b>"
    )
    await _render_schedule_main(cb, db, uid, edit_existing=False)


@router.callback_query(F.data.startswith("zamsched:blackout:pick:"))
async def zamery_blackout_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Quick-pick a выходной date from inline buttons."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _finalize_day_mark(cb, state, db, cb.data.split(":")[-1], "off")  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("zamsched:busy:pick:"))
async def zamery_busy_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Quick-pick a «день занят» date from inline buttons."""
    if not await require_role_callback(cb, db, roles=[Role.ZAMERY]):
        return
    await cb.answer()
    await _finalize_day_mark(cb, state, db, cb.data.split(":")[-1], "busy")  # type: ignore[union-attr]


@router.message(ZameryBlackoutSG.pick_dates)
async def zamery_blackout_manual(message: Message, state: FSMContext, db: Database) -> None:
    """Manual date entry for выходной/день занят (DD.MM.YYYY). Тип берётся из state."""
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

    data = await state.get_data()
    kind = "busy" if data.get("bo_kind") == "busy" else "off"
    await db.add_zamery_blackout_date(uid, d.isoformat(), kind=kind)
    await state.clear()

    wd = _RU_WEEKDAYS[d.weekday()]
    label = "День занят" if kind == "busy" else "Выходной"
    await message.answer(
        f"✅ {label} добавлен: <b>{d.day} {_RU_MONTHS[d.month]} ({wd})</b>"
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
    try:
        bl_id = int(parts[3])
        week_offset = int(parts[4]) if len(parts) > 4 else 0
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные.")  # type: ignore[union-attr]
        return

    await db.remove_zamery_blackout_date(bl_id)

    await cb.message.answer("✅ Отметка дня снята.")  # type: ignore[union-attr]

    await _render_schedule_week(cb, db, cb.from_user.id, week_offset, edit_existing=False)


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

    try:
        invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    except (ValueError, TypeError, IndexError):
        await cb.message.answer("❌ Некорректные данные.")  # type: ignore[union-attr]
        return
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

    from ..config import ZAMERY_BASE_COST
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Расчёт ЗП — Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n\n"
        "Введите <b>стоимость замера</b> (число, ₽):\n"
        f"По умолчанию: <b>{ZAMERY_BASE_COST}₽</b>\n"
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
        await message.answer("⚠️ Ошибка состояния. Начните заново.")
        await state.clear()
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
    summary = format_card_section(
        "💰", f"Расчёт ЗП — {html.quote(initiator)}",
        items=[
            ("Счёт", html.quote(str(invoice_number))),
            ("Адрес", html.quote(str(address))),
            ("Замеров", str(count)),
            ("Цена за замер", _fmt_money(cost)),
        ],
        footer=("Итого", _fmt_money(total)),
        compact=True,
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

        # Карточка для ГД (эталон: число слева / описание справа)
        initiator = await get_initiator_label(db, message.from_user.id)
        indent = "   "
        rows = [
            f"{indent}{_fmt_money(e['cost']):>7}  {html.quote(str(e['description']))}"
            for e in entries
        ]
        body = "\n".join(rows + [indent + "━" * 14, f"{indent}Итого {_fmt_money(total)}"])
        summary = (
            f"💰 <b>Расчёт ЗП — {html.quote(initiator)}</b>\n"
            f"Счёт {html.quote(str(invoice_number))} · {html.quote(str(address))}\n"
            f"<pre>{body}</pre>"
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
@money_confirm_guard
async def zamery_zp_approve(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """GD approves or rejects zamery ZP calculation."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    if len(parts) < 3:
        await cb.message.answer("❌ Некорректные данные.")  # type: ignore[union-attr]
        return
    decision = parts[1]  # yes or no
    try:
        invoice_id = int(parts[2])
    except (ValueError, TypeError):
        await cb.message.answer("❌ Некорректные данные.")  # type: ignore[union-attr]
        return

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
