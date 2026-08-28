"""
Handlers for Installer (Монтажник) role — new menu.

Covers:
- Заказ материалов (ORDER_MATERIALS to RP)
- Счет ок (InstallerInvoiceOkSG)
- Заказ доп.материалов (InstallerOrderMaterialsSG)
- Мои объекты (list invoices)
- Отчёт за день (InstallerDailyReportSG — text to RP via chat-proxy)
- В Работу (accept tasks from RP)
"""
from __future__ import annotations

import html
import logging
from datetime import datetime
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import (
    MATERIAL_TYPE_LABELS,
    InvoiceStatus,
    MaterialType,
    MontazhStage,
    Role,
    TaskStatus,
    TaskType,
)
from ..integrations.minio_storage import MinioStorage
from ..keyboards import (
    INST_BTN_DAILY_REPORT,
    INST_BTN_IN_WORK,
    INST_BTN_INVOICE_OK,
    INST_BTN_MY_OBJECTS,
    INST_BTN_ORDER_EXTRA,
    INST_BTN_ORDER_MAT,
    INST_BTN_RAZMERY_OK,
    INST_BTN_ZP,
    main_menu,
)
from ..services.integration_hub import IntegrationHub
from ..services.assignment import resolve_default_assignee
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import (
    AdvanceDistributeSG,
    AdvanceRequestSG,
    DepoReqExecuteSG,
    InstallerAdvanceFillSG,
    InstallerDailyReportSG,
    InstallerDepoToAdvSG,
    InstallerInvoiceOkSG,
    InstallerMatInitSG,
    InstallerOrderMaterialsSG,
    InstallerRazmerySG,
    InstallerWithdrawSG,
    InstallerWorkAcceptSG,
    InstallerZpAdjustSG,
    InstallerZpInitSG,
    InstallerZpSG,
)
from ..utils import answer_service, build_advance_history_card, build_deposit_history_card, build_installer_advance_card, build_installer_zp_invoiceok_card, build_task_done_card, fmt_money, format_card_section, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, try_json_loads
from ._mirror import collect_attachment
from .auth import require_role_callback, require_role_message
from .money_guard import money_confirm_guard

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


@router.message.outer_middleware()
async def _installer_auto_refresh(handler, event: Message, data: dict):  # type: ignore[type-arg]
    """При каждом сообщении от монтажника — обновляем reply-клавиатуру."""
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
        if menu_role != Role.INSTALLER:
            return result
        unread = await db_inst.count_unread_tasks(u.id)
        uc = await db_inst.count_unread_by_channel(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
            unread_channels=uc,
            inst_in_work=await db_inst.count_installer_unconfirmed_invoices(u.id),
            inst_zp_badge=await db_inst.count_installer_deposit_tasks(u.id),
        )
        await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
    except Exception:
        log.debug("installer auto-refresh failed", exc_info=True)
    return result


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


async def _ensure_reply_kb(cb: CallbackQuery, db: Database, config: Any) -> None:
    """Restore reply keyboard after inline callback so menu doesn't disappear."""
    u = cb.from_user
    if not u or not cb.message:
        return
    role, isolated_role = await _current_menu(db, u.id)
    kb = main_menu(
        role,
        is_admin=u.id in (config.admin_ids or set()),
        unread=await db.count_unread_tasks(u.id),
        isolated_role=isolated_role,
        inst_in_work=await db.count_installer_unconfirmed_invoices(u.id),
        inst_zp_badge=await db.count_installer_deposit_tasks(u.id),
    )
    await cb.message.answer("📋", reply_markup=private_only_reply_markup(cb.message, kb))  # type: ignore[arg-type]


async def _ensure_reply_kb_msg(message: Message, db: Database, config: Any) -> None:
    """Send a persistent message with reply keyboard before inline content."""
    u = message.from_user
    if not u:
        return
    role, isolated_role = await _current_menu(db, u.id)
    kb = main_menu(
        role,
        is_admin=u.id in (config.admin_ids or set()),
        unread=await db.count_unread_tasks(u.id),
        isolated_role=isolated_role,
        inst_in_work=await db.count_installer_unconfirmed_invoices(u.id),
        inst_zp_badge=await db.count_installer_deposit_tasks(u.id),
    )
    await message.answer("📋", reply_markup=private_only_reply_markup(message, kb))


# =====================================================================
# ОБЩИЙ CALLBACK «НАЗАД» — возврат в главное меню монтажника
# =====================================================================

@router.callback_query(F.data == "inst_nav:home")
async def installer_back_home(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Возврат в главное меню монтажника из любого inline-меню."""
    await cb.answer()
    await state.clear()
    u = cb.from_user
    if not u:
        return
    role, isolated_role = await _current_menu(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        "📋 Главное меню",
        reply_markup=main_menu(
            role,
            is_admin=u.id in (config.admin_ids or set()),
            unread=await db.count_unread_tasks(u.id),
            isolated_role=isolated_role,
            inst_in_work=await db.count_installer_unconfirmed_invoices(u.id),
            inst_zp_badge=await db.count_installer_deposit_tasks(u.id),
        ),
    )


# =====================================================================
# ЗАКАЗ МАТЕРИАЛОВ (to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_MAT)
async def start_order_materials(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    all_inv = await db.list_installer_confirmed_invoices()
    # Только счета в работе (не invoice_ok — работы завершены)
    invoices = [i for i in all_inv if i.get("montazh_stage") in ("in_work", "razmery_ok")]
    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"№{num} — {addr}",
            callback_data=f"inst_order_inv:{inv['id']}",
        )
    b.button(text="⏩ Без привязки", callback_data="inst_order_inv:skip")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await state.set_state(InstallerOrderMaterialsSG.invoice_pick)
    await message.answer(
        "📦 <b>Заказ материалов</b>\n\n"
        "Выберите счёт для привязки заказа или пропустите:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(
    InstallerOrderMaterialsSG.invoice_pick,
    lambda cb: cb.data and cb.data.startswith("inst_order_inv:"),
)
async def order_mat_pick_invoice(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    invoice_id = None if val == "skip" else int(val)
    await state.update_data(invoice_id=invoice_id)
    await state.set_state(InstallerOrderMaterialsSG.description)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 1/3: Опишите, какие материалы нужны (объект, размеры и т.д.)."
    )


@router.message(InstallerOrderMaterialsSG.description)
async def order_mat_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(InstallerOrderMaterialsSG.comment)
    await message.answer("Шаг 2/3: Добавьте <b>комментарий</b> (или «—» для пропуска):")


@router.message(InstallerOrderMaterialsSG.comment)
async def order_mat_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""
    await state.update_data(comment=comment, attachments=[])
    await state.set_state(InstallerOrderMaterialsSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить РП", callback_data="inst_order:create")
    b.button(text="⏭ Без вложений", callback_data="inst_order:create")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await message.answer(
        "Шаг 3/3: Прикрепите фото/документы с размерами или нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerOrderMaterialsSG.attachments)
async def order_mat_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"installer/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "inst_order:create")
async def order_mat_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    description = data["description"]
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ РП не найден. Попросите администратора назначить роль РП.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Resolve project_id from linked invoice
    invoice_id = data.get("invoice_id")
    project_id = None
    if invoice_id:
        try:
            inv = await db.get_invoice(int(invoice_id))
            project_id = inv.get("project_id") if inv else None
        except Exception:
            pass

    task = await db.create_task(
        project_id=project_id,
        type_=TaskType.ORDER_MATERIALS,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(rp_id),
        due_at_iso=None,
        payload={
            "description": description,
            "comment": comment,
            "source": "installer",
            "sender_id": u.id,
            "invoice_id": data.get("invoice_id"),
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

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📦 <b>Заказ материалов от монтажника</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📝 {description}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

    from ..keyboards import task_actions_kb
    await notifier.safe_send(int(rp_id), msg, reply_markup=task_actions_kb(task))
    for a in attachments:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Заказ материалов отправлен РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# ЗАКАЗ ДОП.МАТЕРИАЛОВ (same as above, to RP)
# =====================================================================

@router.message(F.text == INST_BTN_ORDER_EXTRA)
async def start_order_extra(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    all_inv = await db.list_installer_confirmed_invoices()
    # Только счета в работе (не invoice_ok — работы завершены)
    invoices = [i for i in all_inv if i.get("montazh_stage") in ("in_work", "razmery_ok")]
    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"№{num} — {addr}",
            callback_data=f"inst_order_inv:{inv['id']}",
        )
    b.button(text="⏩ Без привязки", callback_data="inst_order_inv:skip")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await state.set_state(InstallerOrderMaterialsSG.invoice_pick)
    await message.answer(
        "📦 <b>Заказ доп.материалов</b>\n\n"
        "Выберите счёт для привязки или пропустите:",
        reply_markup=b.as_markup(),
    )


# =====================================================================
# СЧЕТ ОК (InstallerInvoiceOkSG)
# =====================================================================

@router.message(F.text == INST_BTN_INVOICE_OK)
async def start_invoice_ok(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()

    # #2: Передаём user_id чтобы показывать только счета этого монтажника
    all_invoices = await db.list_installer_confirmed_invoices(user_id=message.from_user.id)
    # Fallback: если assigned_to не заполнен (старые данные) — показать все
    if not all_invoices:
        all_invoices = await db.list_installer_confirmed_invoices()
        # Но только те, где assigned_to не заполнен (не принадлежат другому монтажнику)
        all_invoices = [
            i for i in all_invoices
            if not i.get("assigned_to") or int(i.get("assigned_to", 0)) == message.from_user.id
        ]
    # Счета без actual_completion_date (ещё не завершены), стадии in_work/razmery_ok
    invoices = [
        i for i in all_invoices
        if i.get("montazh_stage") in ("in_work", "razmery_ok")
        and not i.get("actual_completion_date")
    ]
    if not invoices:
        await answer_service(message, "Нет счетов для завершения.", delay_seconds=60)
        return

    await state.set_state(InstallerInvoiceOkSG.select_invoice)
    for inv in invoices:
        card = _build_inst_detail_card(inv)
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Счёт ОК", callback_data=f"instok:view:{inv['id']}")
        kb.button(text="⬅️ Назад", callback_data="inst_nav:home")
        kb.adjust(1)
        await message.answer(card, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("instok:view:"))
async def invoice_ok_select(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Счёт ОК → показать согласованную сумму + возможность изменить."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Текущая согласованная сумма (или расчётная итог с +10% для б.н.)
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if not agreed:
        agreed = _calc_est_montazh(inv)

    await state.update_data(invoice_id=invoice_id)

    b = InlineKeyboardBuilder()
    if agreed:
        b.button(text=f"✅ Ок ({agreed:,.0f}₽)", callback_data=f"instok:price_ok:{invoice_id}")
    b.button(text="✏️ Изменить сумму", callback_data=f"instok:price_edit:{invoice_id}")
    b.adjust(1)

    await _ensure_reply_kb(cb, db, config)
    amount_str = f"<b>{agreed:,.0f}₽</b>" if agreed else "не указана"
    await cb.message.answer(  # type: ignore[union-attr]
        f"📄 Счёт №{inv['invoice_number']} — <b>Счёт ОК</b>\n\n"
        f"🔧 Стоимость монтажа: {amount_str}\n\n"
        "<b>Согласовать стоимость:</b>",
        reply_markup=b.as_markup(),
    )


# ---------------------------------------------------------------------
# Финализация «Счёт ОК» (фикс 16.07, инцидент КВ 9).
#
# Раньше ВСЯ запись в БД (installer_ok, stage→invoice_ok, дата факта, задача,
# уведомления) жила только в хендлере комментария — последнем шаге FSM.
# Монтажник нажал «✅ Ок (сумма)», комментарий не отправил → бот молча ничего
# не сохранил (а рестарт стирает MemoryStorage-state без следа). Теперь
# фиксация происходит СРАЗУ в момент согласия с суммой, комментарий —
# опциональное дополнение к уже созданной задаче (invoice_ok_comment).

_INSTOK_FINALIZE_INFLIGHT: set[int] = set()


async def _finalize_invoice_ok(
    *,
    target_msg: Message,
    installer_id: int,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    invoice_id: int,
    comment: str = "",
) -> int | None:
    """Полная фиксация «Счёт ОК»: статусы, дата факта, задача, уведомления.

    Возвращает id созданной задачи (для дописывания комментария) или None
    (счёт не найден / уже подтверждён / фиксация уже идёт). Идемпотентна:
    гард по состоянию БД + синхронный in-flight claim по invoice_id
    (повторный клик «✅ Ок» по старому сообщению не создаёт дублей).
    """
    inv_check = await db.get_invoice(invoice_id)
    if not inv_check:
        await target_msg.answer("❌ Счёт не найден.")
        await state.clear()
        return None
    if inv_check.get("installer_ok") and inv_check.get("montazh_stage") in (
        "invoice_ok", "invoice_end",
    ):
        await target_msg.answer(
            f"⚠️ Счёт №{inv_check.get('invoice_number', '?')} уже подтверждён (Счёт ОК).",
        )
        await state.clear()
        return None
    if invoice_id in _INSTOK_FINALIZE_INFLIGHT:
        return None
    # Синхронный claim — между проверкой и add нет await (ср. money_guard §9).
    _INSTOK_FINALIZE_INFLIGHT.add(invoice_id)
    try:
        # Set installer_ok condition
        await db.set_invoice_installer_ok(invoice_id, True)

        # Update montazh stage → invoice_ok
        await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_OK)
        inv_row = await db.get_invoice(invoice_id)
        if inv_row:
            await integrations.sync_invoice_status(
                inv_row["invoice_number"], inv_row.get("status", ""), MontazhStage.INVOICE_OK,
            )
            await integrations.sync_invoice_row(invoice_id)

        # ТЗ 14.06: «Счёт ОК» с непогашенным долгом по счёту → задача менеджеру
        # на ввод ориент. даты финального платежа (дедуп внутри хелпера).
        # 🔑 Автор задачи — ГД, а НЕ монтажник (owner 27.08: «монтажники никак не
        # относятся к этому процессу, в нём участвуют только менеджер и ГД»).
        # Монтажник лишь нажал «Счёт ОК»; из-за `created_by=installer_id` менеджеру
        # приходило «от Игоря». Конвенция не выдумана — она уже действует в ОБОИХ
        # путях daily_sync: `created_by=gd_id` (:511) и прямым комментарием
        # «actor = ГД (создатель догоняющей задачи)» (:583). Здесь было
        # единственное расхождение.
        # ⚠️ Fallback на монтажника оставлен намеренно: без автора задача не
        # создастся вовсе (`request_final_payment_eta` пишет `created_by=actor_id`),
        # а потерять напоминание о долге хуже, чем показать неверного инициатора.
        from ..utils import request_final_payment_eta
        _gd_actor = await resolve_default_assignee(db, config, Role.GD) or installer_id
        await request_final_payment_eta(db, notifier, config, invoice_id, int(_gd_actor))

        # ТЗ 18.06: «Счёт ОК» (монтаж завершён) + долга по счёту нет → напомнить
        # менеджеру закрыть счёт (Счет End) + бейдж 🔴 на кнопке (дедуп в хелпере).
        # 🔑 Автор — тот же ГД, что и у задачи выше (owner 27.08): напоминание
        # адресовано менеджеру и монтажника не касается, а из-за installer_id
        # оно висело в ЕГО списке исходящих (боевая задача #504). Конвенция та же,
        # что в daily_sync:468 — там уже передаётся gd_id.
        from ..utils import prompt_invoice_end_ready
        await prompt_invoice_end_ready(db, notifier, invoice_id, int(_gd_actor), config)

        # ТЗ 2026-05-19 блок C + user-уточнение: авто-закрытие ЗП если paid авансы
        # покрывают БАЗУ. ЗП ставится = итог (база + 10% для б.н.). 10%-бонус
        # остаётся к доплате как остаток (offset_remaining).
        inv_for_advance = await db.get_invoice(invoice_id)
        if inv_for_advance:
            plan_base = _calc_est_montazh_base(inv_for_advance)
            plan_total = _calc_est_montazh(inv_for_advance)
            closed = await db.auto_close_montazh_by_advance(
                invoice_id,
                plan_zp_base=plan_base,
                plan_zp_total=plan_total,
                actor_id=installer_id,
            )
            if closed:
                await integrations.sync_invoice_row(invoice_id)
                await integrations.sync_advances_journal()
                gd_id_notify = await resolve_default_assignee(db, config, Role.GD)
                if gd_id_notify:
                    bonus = max(0.0, float(plan_total) - float(plan_base))
                    bonus_line = (
                        f"\n🟢 К доплате 10%-бонус: {_fmt_money(bonus)} ₽" if bonus > 0 else ""
                    )
                    await notifier.safe_send(
                        int(gd_id_notify),
                        f"📍 <b>{inv_for_advance.get('object_address') or '—'}</b> "
                        f"(№{inv_for_advance.get('invoice_number') or '?'})\n"
                        f"Авансы покрыли базу ({_fmt_money(plan_base)} ₽), "
                        f"ЗП {_fmt_money(plan_total)} ₽ проведена в BT."
                        f"{bonus_line}",
                    )
            else:
                # ТЗ 2026-05-20: правило кредит-авто. Для is_credit=1 — глобальный
                # offset: ZP этого счёта (5% × est) переводится в счёт открытого
                # долга монтажника (любые open items, не только этого invoice).
                if inv_for_advance.get("is_credit") and plan_total > 0:
                    installer_id_adv = inv_for_advance.get("assigned_to")
                    if installer_id_adv:
                        res = await db.credit_autoclose_with_advance(
                            invoice_id=invoice_id,
                            installer_id=int(installer_id_adv),
                            plan_zp_total=plan_total,
                            actor_id=installer_id,
                        )
                        if res["applied"]:
                            await integrations.sync_invoice_row(invoice_id)
                            await integrations.sync_advances_journal()
                            gd_id_notify = await resolve_default_assignee(db, config, Role.GD)
                            if gd_id_notify:
                                remain = res.get("remaining_to_pay") or 0.0
                                remain_line = (
                                    f"\n🟢 Остаток к выплате: {_fmt_money(remain)} ₽" if remain > 0 else ""
                                )
                                await notifier.safe_send(
                                    int(gd_id_notify),
                                    f"🏦 <b>Кредит: авто-зачёт ZP в аванс</b>\n\n"
                                    f"📍 {inv_for_advance.get('object_address') or '—'} "
                                    f"(№{inv_for_advance.get('invoice_number') or '?'})\n"
                                    f"ZP {_fmt_money(plan_total)} ₽ → зачтена в погашение "
                                    f"открытых авансовых items монтажника "
                                    f"(закрыто {res['items_count']} item(s), "
                                    f"offset={_fmt_money(res['offset_total'])} ₽)."
                                    f"{remain_line}\n"
                                    f"ZP-статус → confirmed, BT не пополнен.",
                                )

        # Set actual completion date (Дата Факт)
        today_iso = datetime.now().strftime("%Y-%m-%d")
        await db.conn.execute(
            "UPDATE invoices SET actual_completion_date = ? WHERE id = ? AND actual_completion_date IS NULL",
            (today_iso, invoice_id),
        )
        await db.conn.commit()

        inv = await db.get_invoice(invoice_id)
        if not inv:
            await target_msg.answer("❌ Счёт не найден.")
            await state.clear()
            return None

        # Create task
        task = await db.create_task(
            project_id=None,
            type_=TaskType.INSTALLER_INVOICE_OK,
            status=TaskStatus.DONE,
            created_by=installer_id,
            assigned_to=inv.get("created_by", 0),
            due_at_iso=None,
            payload={
                "invoice_id": invoice_id,
                "invoice_number": inv["invoice_number"],
                "comment": comment,
                "installer_id": installer_id,
            },
        )

        initiator = await get_initiator_label(db, installer_id)
        msg = build_task_done_card(
            task,
            None,
            config.timezone,
            emoji="✅",
            title="Монтажник — Счёт ОК",
            actor_label=initiator,
            actor_field="От",
        )

        # Write Дата Факт back to source ОП spreadsheet
        try:
            if integrations and integrations.sheets:
                await integrations.sheets.write_date_fact_to_op(
                    inv["invoice_number"], today_iso,
                )
        except Exception as e:
            log.warning("Failed to write Дата Факт to ОП: %s", e)

        # Notify manager + RP (deduplicated to avoid double-sending when same person)
        manager_id = inv.get("created_by")
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        seen_targets: set[int] = set()
        for target in [manager_id, rp_id]:
            if target and int(target) not in seen_targets:
                seen_targets.add(int(target))
                await notifier.safe_send(int(target), msg)
                await refresh_recipient_keyboard(notifier, db, config, int(target))

        role, isolated_role = await _current_menu(db, installer_id)
        await state.clear()
        await target_msg.answer(
            f"✅ Подтверждение отправлено по счёту №{inv['invoice_number']}.",
            reply_markup=private_only_reply_markup(
                target_msg,
                main_menu(
                    role,
                    is_admin=installer_id in (config.admin_ids or set()),
                    unread=await db.count_unread_tasks(installer_id),
                    isolated_role=isolated_role,
                ),
            ),
        )
        return int(task["id"])
    finally:
        _INSTOK_FINALIZE_INFLIGHT.discard(invoice_id)


@router.callback_query(F.data.startswith("instok:price_ok:"))
@money_confirm_guard
async def invoice_ok_price_ok(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Монтажник согласен с суммой → НЕМЕДЛЕННАЯ фиксация; комментарий опционален.

    Фикс 16.07 (инцидент КВ 9): раньше здесь только ставился FSM-state comment,
    вся запись в БД ждала комментария — и молча терялась, если монтажник его
    не отправлял. Теперь фиксация происходит прямо тут.
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Зафиксировать сумму если ещё не зафиксирована (итог с +10% для б.н.)
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if not agreed:
        agreed = _calc_est_montazh(inv)
        if agreed:
            await db.conn.execute(
                "UPDATE invoices SET montazh_agreed_amount = ? WHERE id = ?",
                (agreed, invoice_id),
            )
            await db.conn.commit()

    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(f"✅ Стоимость: <b>{agreed:,.0f}₽</b>")  # type: ignore[union-attr]
    task_id = await _finalize_invoice_ok(
        target_msg=cb.message,  # type: ignore[arg-type]
        installer_id=u.id,
        state=state,
        db=db,
        config=config,
        notifier=notifier,
        integrations=integrations,
        invoice_id=invoice_id,
        comment="",
    )
    if task_id is None:
        return
    await state.set_state(InstallerInvoiceOkSG.comment)
    await state.update_data(invoice_id=invoice_id, instok_task_id=task_id)
    await cb.message.answer(  # type: ignore[union-attr]
        "💬 При желании добавьте <b>комментарий</b> к подтверждению "
        "(или «—» — всё уже сохранено):",
    )


@router.callback_query(F.data.startswith("instok:price_edit:"))
async def invoice_ok_price_edit(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Монтажник хочет изменить сумму → ввод новой."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.update_data(invoice_id=invoice_id)
    await state.set_state(InstallerInvoiceOkSG.price_input)
    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer("💰 Введите вашу сумму за монтаж (в рублях):")  # type: ignore[union-attr]


@router.message(InstallerInvoiceOkSG.price_input)
async def invoice_ok_price_input(
    message: Message, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Монтажник ввёл сумму → запись суммы + НЕМЕДЛЕННАЯ фиксация (фикс 16.07)."""
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
    invoice_id = data["invoice_id"]
    inv = await db.get_invoice(invoice_id)
    # Для б.н. — добавить 10% надбавку к ручному вводу (для кредита — как есть).
    agreed = _apply_montazh_bonus(inv or {}, amount) if inv else amount
    # Объединение платежей / «Внести сумму ЗП» РП (owner 15-16.07): монтажник вводит
    # ТОЛЬКО свою сумму — прибавляем выплаченное прошлым группам, иначе безусловная
    # запись затёрла бы объединение (зеркало гарда inst_work:price_confirm).
    _paid_prev = float((inv or {}).get("montazh_paid_prev") or 0)
    if _paid_prev > 0:
        agreed = int(round(_paid_prev + agreed))
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ? WHERE id = ?",
        (agreed, invoice_id),
    )
    await db.conn.commit()

    await _ensure_reply_kb_msg(message, db, config)
    await message.answer(f"✅ Стоимость: <b>{agreed:,}₽</b>")
    # Фикс 16.07 (инцидент КВ 9): фиксация СРАЗУ — не ждём комментария.
    task_id = await _finalize_invoice_ok(
        target_msg=message,
        installer_id=message.from_user.id,
        state=state,
        db=db,
        config=config,
        notifier=notifier,
        integrations=integrations,
        invoice_id=invoice_id,
        comment="",
    )
    if task_id is None:
        return
    await state.set_state(InstallerInvoiceOkSG.comment)
    await state.update_data(invoice_id=invoice_id, instok_task_id=task_id)
    await message.answer(
        "💬 При желании добавьте <b>комментарий</b> к подтверждению "
        "(или «—» — всё уже сохранено):",
    )


@router.message(InstallerInvoiceOkSG.comment)
async def invoice_ok_comment(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Опциональный комментарий к УЖЕ выполненной фиксации «Счёт ОК».

    Фикс 16.07 (инцидент КВ 9): фиксация происходит в момент согласия с
    суммой (_finalize_invoice_ok в price_ok / price_input); здесь только
    дописываем комментарий в задачу и пересылаем его менеджеру/РП.
    Страховка: если state пришёл из старого флоу (до фикса, без
    instok_task_id) и фиксации ещё не было — финализируем сейчас.
    """
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    invoice_id = data.get("invoice_id")
    task_id = data.get("instok_task_id")
    await state.clear()
    if not invoice_id:
        return

    if not task_id:
        # Страховка (state старого флоу, фиксации не было) — выполнить сейчас.
        await _finalize_invoice_ok(
            target_msg=message,
            installer_id=message.from_user.id,
            state=state,
            db=db,
            config=config,
            notifier=notifier,
            integrations=integrations,
            invoice_id=int(invoice_id),
            comment=comment,
        )
        return

    if not comment:
        await message.answer("Ок, без комментария.")
        return

    # Дописать комментарий в задачу + переслать менеджеру/РП.
    try:
        await db.update_task_payload(int(task_id), {"comment": comment})
    except Exception:
        log.exception("invoice_ok_comment: update_task_payload failed (task_id=%s)", task_id)
    inv = await db.get_invoice(int(invoice_id))
    if not inv:
        return
    note = (
        f"💬 Комментарий монтажника к «Счёт ОК» №{inv.get('invoice_number', '?')}:\n"
        f"{html.escape(comment)}"
    )
    manager_id = inv.get("created_by")
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    seen_targets: set[int] = set()
    for target in [manager_id, rp_id]:
        if target and int(target) not in seen_targets:
            seen_targets.add(int(target))
            await notifier.safe_send(int(target), note)
    await message.answer("✅ Комментарий добавлен.")


# =====================================================================
# РАЗМЕРЫ ОК — workflow проверки размеров стекла
# =====================================================================

def _build_mat_init_kb(
    invoices: list[dict[str, Any]], selected: set[int],
) -> InlineKeyboardBuilder:
    """Построить inline-клавиатуру мульти-выбора «материал заказан» (☐/✅)."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = inv["id"]
        prefix = "✅" if inv_id in selected else "☐"
        num = inv.get("invoice_number") or f"#{inv_id}"
        addr = (inv.get("object_address") or "—")[:25]
        b.button(text=f"{prefix} №{num} — {addr}"[:55], callback_data=f"matinit:toggle:{inv_id}")
    b.button(text="✅ Готово", callback_data="matinit:done")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    return b


@router.message(F.text == INST_BTN_RAZMERY_OK)
async def start_razmery_ok(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка «Размеры ОК»: инициализация (первый вход) или стандартный поток."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    installer_id = message.from_user.id  # type: ignore[union-attr]

    # --- Первый заход: инициализация «материал заказан» ---
    if not await db.is_installer_razmery_initialized(installer_id):
        confirmed = await db.list_installer_confirmed_invoices()
        if not confirmed:
            await db.set_installer_razmery_initialized(installer_id)
            # Продолжить к стандартному потоку ниже
        else:
            await state.set_state(InstallerMatInitSG.selecting)
            await state.update_data(
                mat_init_selected=[],
                mat_init_invoices=[inv["id"] for inv in confirmed],
            )
            b = _build_mat_init_kb(confirmed, set())
            await message.answer(
                "📐 <b>Размеры ОК — инициализация</b>\n\n"
                "Выберите счета, по которым <b>материал уже заказан</b>:\n"
                "(они будут исключены из списка «Размеры ОК»)",
                reply_markup=b.as_markup(),
            )
            return

    # --- Стандартный поток ---
    # Счета in_work БЕЗ активного razmery_request → можно отправить бланк
    confirmed = await db.list_installer_confirmed_invoices()
    send_list = []
    check_list = []
    for inv in confirmed:
        stage = inv.get("montazh_stage", "")
        if stage != "in_work":
            continue
        if inv.get("materials_ordered"):
            continue  # Исключить счета с заказанным материалом
        req = await db.get_active_razmery_request(inv["id"])
        if not req:
            send_list.append(inv)
        elif req["status"] == "verification_sent":
            check_list.append((inv, req))

    if not send_list and not check_list:
        await answer_service(message, "📐 Нет счетов для отправки размеров.", delay_seconds=60)
        return

    b = InlineKeyboardBuilder()
    if send_list:
        for inv in send_list:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "")[:20]
            b.button(
                text=f"📤 №{num} — {addr}"[:55],
                callback_data=f"razmok_new:send:{inv['id']}",
            )
    if check_list:
        for inv, req in check_list:
            num = inv.get("invoice_number") or f"#{inv['id']}"
            addr = (inv.get("object_address") or "")[:20]
            b.button(
                text=f"📋 №{num} — проверить"[:55],
                callback_data=f"razmok_new:check:{req['id']}",
            )
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)

    text = "📐 <b>Размеры ОК</b>\n\n"
    if send_list:
        text += f"📤 Отправить бланк ({len(send_list)})\n"
    if check_list:
        text += f"📋 На проверке ({len(check_list)})\n"
    await message.answer(text, reply_markup=b.as_markup())


# --- Mat init: toggle / done ---

@router.callback_query(F.data.startswith("matinit:toggle:"), InstallerMatInitSG.selecting)
async def mat_init_toggle(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Переключить выбор счёта в мульти-выборе «материал заказан»."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    selected = set(data.get("mat_init_selected", []))
    if inv_id in selected:
        selected.discard(inv_id)
    else:
        selected.add(inv_id)
    await state.update_data(mat_init_selected=list(selected))
    # Перестроить клавиатуру
    all_ids = data.get("mat_init_invoices", [])
    invoices = []
    for iid in all_ids:
        inv = await db.get_invoice(iid)
        if inv:
            invoices.append(inv)
    b = _build_mat_init_kb(invoices, selected)
    try:
        await cb.message.edit_reply_markup(reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        pass


@router.callback_query(F.data == "matinit:done", InstallerMatInitSG.selecting)
async def mat_init_done(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Завершить инициализацию: выбранные → materials_ordered=1."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    selected = set(data.get("mat_init_selected", []))
    for inv_id in selected:
        await db.set_invoice_materials_ordered(inv_id, True)
    await db.set_installer_razmery_initialized(u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Инициализация завершена.\n"
        f"Счетов с заказанным материалом: <b>{len(selected)}</b>\n\n"
        "Нажмите «📐 Размеры ОК» ещё раз для работы.",
    )


# --- Шаг 1: отправка бланка размеров РП ---

@router.callback_query(F.data.startswith("razmok_new:send:"))
async def razmery_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.comment)
    await state.update_data(razmery_invoice_id=invoice_id, razmery_attachments=[])
    await cb.message.answer(  # type: ignore[union-attr]
        "📐 <b>Бланк размеров стекла</b>\n\n"
        "Добавьте комментарий к бланку размеров\n"
        "(или «-» для пропуска, «❌ Отмена» для отмены):",
    )


@router.message(InstallerRazmerySG.comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.result_comment, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
@router.message(InstallerRazmerySG.result_attachments, F.text.casefold().in_({"❌ отмена", "отмена", "/cancel"}))
async def razmery_cancel(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    u = message.from_user
    await state.clear()
    role, isolated_role = await _current_menu(db, u.id)  # type: ignore[union-attr]
    await message.answer(
        "❌ Отменено.",
        reply_markup=main_menu(
            role, is_admin=u.id in (config.admin_ids or set()),  # type: ignore[union-attr]
            unread=await db.count_unread_tasks(u.id),  # type: ignore[union-attr]
            isolated_role=isolated_role,
        ),
    )


@router.message(InstallerRazmerySG.comment)
async def razmery_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = None if text == "-" else text
    await state.update_data(razmery_comment=comment)
    await state.set_state(InstallerRazmerySG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить РП", callback_data="razmok_new:create")
    b.button(text="⏭ Без вложений", callback_data="razmok_new:create")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await message.answer(
        "Прикрепите бланк размеров (фото/документ).\n"
        "Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerRazmerySG.attachments)
async def razmery_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(
        message, state, storage, prefix=f"installer/{uid}", key="razmery_attachments"
    )
    if att is None:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "razmok_new:create")
async def razmery_send_to_rp(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Финализация: создать razmery_request + уведомить РП."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    invoice_id = data.get("razmery_invoice_id")
    comment = data.get("razmery_comment")
    attachments = data.get("razmery_attachments", [])

    req_id = await db.create_razmery_request(invoice_id, u.id, comment)

    inv = await db.get_invoice(invoice_id)
    inv_num = inv["invoice_number"] if inv else "?"
    initiator = await get_initiator_label(db, u.id)

    # Уведомить РП
    b = InlineKeyboardBuilder()
    b.button(text="✅ ОК (принял)", callback_data=f"razmok_rp:received:{req_id}")
    b.adjust(1)

    msg = (
        f"📐 <b>Бланк размеров стекла</b>\n"
        f"👤 От: {initiator}\n"
        f"🧾 Счёт: №{inv_num}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        await notifier.safe_send(int(rp_id), msg, reply_markup=b.as_markup())
        for a in attachments:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await state.clear()
    role, isolated_role = await _current_menu(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Бланк размеров отправлен РП по счёту №{inv_num}.",
        reply_markup=main_menu(
            role, is_admin=u.id in (config.admin_ids or set()),
            unread=await db.count_unread_tasks(u.id),
            isolated_role=isolated_role,
        ),
    )


# --- Шаг 3: проверка формы поставщика от РП ---

@router.callback_query(F.data.startswith("razmok_new:check:"))
async def razmery_check_view(cb: CallbackQuery, db: Database) -> None:
    """Просмотр формы поставщика от РП."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    rp_label = await get_initiator_label(db, req["rp_id"]) if req.get("rp_id") else "РП"

    text = (
        f"📐 <b>Проверка размеров</b>\n\n"
        f"🧾 Счёт: №{inv_num}\n"
        f"👤 Форма от: {rp_label}\n"
    )
    if req.get("rp_comment"):
        text += f"💬 {req['rp_comment']}\n"
    text += "\nПроверьте форму и выберите действие:"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Размеры ОК", callback_data=f"razmok_inst:ok:{req_id}")
    b.button(text="❌ Ошибка", callback_data=f"razmok_inst:error:{req_id}")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(2, 1)

    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("razmok_inst:ok:"))
async def razmery_respond_ok(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.result_comment)
    await state.update_data(
        razmery_req_id=req_id, razmery_result="ok", razmery_result_attachments=[],
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ <b>Размеры ОК</b>\n\n"
        "Добавьте комментарий (или «-» для пропуска):",
    )


@router.callback_query(F.data.startswith("razmok_inst:error:"))
async def razmery_respond_error(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerRazmerySG.result_comment)
    await state.update_data(
        razmery_req_id=req_id, razmery_result="error", razmery_result_attachments=[],
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ <b>Ошибка в размерах</b>\n\n"
        "Опишите ошибку (обязательно):",
    )


@router.message(InstallerRazmerySG.result_comment)
async def razmery_result_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    if data.get("razmery_result") == "error" and (not text or text == "-"):
        await message.answer("Опишите ошибку — комментарий обязателен:")
        return
    comment = None if text == "-" else text
    await state.update_data(razmery_result_comment=comment)
    await state.set_state(InstallerRazmerySG.result_attachments)

    b = InlineKeyboardBuilder()
    b.button(text="📤 Отправить", callback_data="razmok_inst:result_send")
    b.button(text="⏭ Без вложений", callback_data="razmok_inst:result_send")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы (опционально). Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerRazmerySG.result_attachments)
async def razmery_result_attach(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(
        message, state, storage, prefix=f"installer/{uid}", key="razmery_result_attachments"
    )
    if att is None:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "razmok_inst:result_send")
async def razmery_result_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Финализация ответа: Размеры ОК или Ошибка."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    req_id = data.get("razmery_req_id")
    result = data.get("razmery_result", "ok")
    comment = data.get("razmery_result_comment")
    attachments = data.get("razmery_result_attachments", [])

    from ..utils import to_iso, utcnow
    now = to_iso(utcnow())

    req = await db.get_razmery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        await state.clear()
        return

    inv = await db.get_invoice(req["invoice_id"])
    inv_num = inv["invoice_number"] if inv else "?"
    initiator = await get_initiator_label(db, u.id)

    if result == "ok":
        await db.update_razmery_request(
            req_id, status="approved", result="ok",
            result_comment=comment, result_at=now,
        )
        await db.update_montazh_stage(req["invoice_id"], MontazhStage.RAZMERY_OK)
        if inv:
            await integrations.sync_invoice_status(
                inv["invoice_number"], inv.get("status", ""), MontazhStage.RAZMERY_OK,
            )

        rp_id = await resolve_default_assignee(db, config, Role.RP)
        if rp_id:
            msg = (
                f"✅ <b>Размеры ОК</b>\n"
                f"👤 От: {initiator}\n"
                f"🧾 Счёт: №{inv_num}\n"
                f"Размеры проверены ✅"
            )
            if comment:
                msg += f"\n💬 {comment}"
            await notifier.safe_send(int(rp_id), msg)
            for a in attachments:
                await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

        await state.clear()
        role, isolated_role = await _current_menu(db, u.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Размеры ОК подтверждены по счёту №{inv_num}.",
            reply_markup=main_menu(
                role, is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        )
    else:
        # Ошибка → возврат к rp_received (РП исправляет)
        await db.update_razmery_request(
            req_id, status="rp_received", result="error",
            result_comment=comment, result_at=now,
        )

        rp_id = await resolve_default_assignee(db, config, Role.RP)
        if rp_id:
            b = InlineKeyboardBuilder()
            b.button(
                text="📐 Отправить исправление",
                callback_data=f"razmok_rp:send_form:{req_id}",
            )
            b.adjust(1)
            msg = (
                f"❌ <b>Ошибка в размерах</b>\n"
                f"👤 От: {initiator}\n"
                f"🧾 Счёт: №{inv_num}\n"
                f"💬 {comment or '-'}"
            )
            await notifier.safe_send(int(rp_id), msg, reply_markup=b.as_markup())
            for a in attachments:
                await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"])
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

        await state.clear()
        role, isolated_role = await _current_menu(db, u.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Ошибка отправлена РП по счёту №{inv_num}.",
            reply_markup=main_menu(
                role, is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        )


# =====================================================================
# МОИ ОБЪЕКТЫ (list invoices)
# =====================================================================

_STAGE_LABEL = {
    "in_work": "🔨 В работе",
    "razmery_ok": "📐 Размеры ОК",
    "invoice_ok": "✅ Счёт ОК",
    "none": "⏳ Ожидает",
}
_STAGE_ORDER = {"in_work": 0, "razmery_ok": 1, "invoice_ok": 2}


@router.message(F.text == INST_BTN_MY_OBJECTS)
async def installer_my_objects(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return

    # Все счета с montazh_stage (назначены на монтаж) — без ограничения по assigned_to (#10)
    invoices = await db.list_invoices(limit=200)
    all_inv = [
        i for i in invoices
        if i.get("montazh_stage") and i["montazh_stage"] != "none"
        and not i.get("parent_invoice_id")
    ]
    # Также включаем ENDED без montazh_stage, если ЗП approved
    ended_with_zp = [
        i for i in invoices
        if i["status"] == InvoiceStatus.ENDED
        and not i.get("parent_invoice_id")
        and (i.get("zp_installer_status") or "not_requested") == "approved"
        and i not in all_inv
    ]
    all_inv.extend(ended_with_zp)

    if not all_inv:
        await answer_service(message, "📌 Нет объектов.", delay_seconds=60)
        return

    work_stages = ("in_work", "razmery_ok")
    _ZP_DONE = ("payment_sent", "confirmed")  # ЗП выплачена/получена → Архив (E, user 18.06)
    in_work = [i for i in all_inv if i.get("montazh_stage") in work_stages]
    archive = [
        i for i in all_inv
        if (i.get("zp_installer_status") or "") in _ZP_DONE
        or (i["status"] == InvoiceStatus.ENDED
            and (i.get("zp_installer_status") or "") in _ZP_DONE)
    ]
    archive_ids = {i["id"] for i in archive}
    work_ids = {i["id"] for i in in_work}
    waiting = [
        i for i in all_inv
        if i["id"] not in archive_ids
        and i["id"] not in work_ids
        and i.get("montazh_stage") in ("invoice_ok", "invoice_end")
    ]

    total = len(in_work) + len(waiting) + len(archive)
    text = f"📌 <b>Мои объекты</b> · {total} шт.\n"

    b = InlineKeyboardBuilder()
    b.button(text=f"🔨 В работе ({len(in_work)})", callback_data="instobj:cat:work")
    b.button(text=f"✅ Ожидает расчёт ({len(waiting)})", callback_data="instobj:cat:waiting")
    b.button(text=f"📦 Архив ({len(archive)})", callback_data="instobj:cat:archive")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)

    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("instobj:cat:"))
async def installer_objects_category(cb: CallbackQuery, db: Database) -> None:
    """Список счетов по категории."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    cat = cb.data.split(":")[-1]  # type: ignore[union-attr]

    invoices = await db.list_invoices(limit=200)
    all_inv = [
        i for i in invoices
        if (i.get("montazh_stage") and i["montazh_stage"] != "none"
            or (i["status"] == InvoiceStatus.ENDED
                and (i.get("zp_installer_status") or "") == "approved"))
        and not i.get("parent_invoice_id")
    ]

    work_stages = ("in_work", "razmery_ok")
    _ZP_DONE = ("payment_sent", "confirmed")  # ЗП выплачена/получена → Архив (E, user 18.06)
    if cat == "work":
        filtered = [i for i in all_inv if i.get("montazh_stage") in work_stages]
        filtered.sort(key=lambda i: _STAGE_ORDER.get(i.get("montazh_stage") or "none", 99))
        title = "🔨 В работе"
    elif cat == "archive":
        filtered = [
            i for i in all_inv
            if (i.get("zp_installer_status") or "") in _ZP_DONE
            or (i["status"] == InvoiceStatus.ENDED
                and (i.get("zp_installer_status") or "") in _ZP_DONE)
        ]
        filtered.sort(key=lambda i: i.get("zp_installer_approved_at") or "", reverse=True)
        title = "📦 Архив"
    else:
        archive_ids = {
            i["id"] for i in all_inv
            if (i.get("zp_installer_status") or "") in _ZP_DONE
        }
        work_ids = {i["id"] for i in all_inv if i.get("montazh_stage") in work_stages}
        filtered = [
            i for i in all_inv
            if i.get("montazh_stage") in ("invoice_ok", "invoice_end")
            and i["id"] not in archive_ids
            and i["id"] not in work_ids
        ]
        filtered.sort(key=lambda i: i.get("created_at") or "", reverse=True)
        title = "✅ Ожидает расчёт"

    if not filtered:
        await cb.message.answer(f"{title}\n\nНет счетов.")  # type: ignore[union-attr]
        return

    if cat == "archive":
        stats = _build_archive_stats(filtered)
        await cb.message.answer(f"{title} ({len(filtered)})\n\n{stats}")  # type: ignore[union-attr]
        card_fn = _build_archive_card
    else:
        await cb.message.answer(f"{title} ({len(filtered)})")  # type: ignore[union-attr]
        card_fn = _build_inst_detail_card

    for inv in filtered[:15]:
        card_text = card_fn(inv)
        b = InlineKeyboardBuilder()
        if cat == "waiting":
            zp_st = inv.get("zp_installer_status") or "not_requested"
            # #18 / Часть 2 (2.3): правка суммы — первичная или повторная на «Счет End».
            if _can_edit_zp_amount(inv):
                _lbl = "✏️ Изменить стоимость" if zp_st == "not_requested" else "✏️ Изменить сумму"
                b.button(text=_lbl, callback_data=f"instzpadj:start:{inv['id']}")
            if zp_st == "not_requested":
                b.button(text="✅ Принять", callback_data=f"instzp_accept:{inv['id']}")
                b.button(text="✅ ЗП получено", callback_data=f"instzp_done:{inv['id']}")
            # #20: Кнопка "Цена ок"
            b.button(text="💲 Цена ок", callback_data=f"instzp_price_ok:{inv['id']}")
        elif cat == "work":
            zp_st = inv.get("zp_installer_status") or "not_requested"
            if _can_edit_zp_amount(inv):
                _lbl = "✏️ Изменить стоимость" if zp_st == "not_requested" else "✏️ Изменить сумму"
                b.button(text=_lbl, callback_data=f"instzpadj:start:{inv['id']}")
            if zp_st == "not_requested":
                b.button(text="✅ Принять", callback_data=f"instzp_accept:{inv['id']}")
        b.button(text="⬅️ Назад", callback_data="instobj:back")
        b.adjust(1)
        await cb.message.answer(card_text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("instzp_done:"))
@money_confirm_guard
async def installer_zp_done(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """#18: Монтажник подтвердил получение ЗП.

    Гард статуса: подтвердить получение можно ТОЛЬКО пока заявка в
    `payment_sent` (ГД действительно отправил выплату). После ручного отката
    выплаты (payment_sent→approved) старая inline-кнопка «✅ ЗП получено» из
    истории чата не должна молча возвращать статус в `confirmed` и помечать
    ошибочную/аннулированную ЗП как полученную. money_confirm_guard страхует от
    гонки одновременных кликов; статус-гард — от старой кнопки.
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.answer("❌ Счёт не найден", show_alert=True)
        return
    zp_st = inv.get("zp_installer_status") or "not_requested"
    if zp_st == "confirmed":
        await cb.answer("✅ Уже подтверждено ранее")
        return
    if zp_st != "payment_sent":
        await cb.answer(
            "⚠️ Заявка изменена или аннулирована — подтверждение недоступно.",
            show_alert=True,
        )
        return
    await cb.answer("✅ ЗП подтверждено")
    await db.set_invoice_zp_installer_status(inv_id, "confirmed")
    await integrations.sync_invoice_row(inv_id)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ ЗП по счёту №{inv.get('invoice_number', '?')} подтверждено.",
    )


@router.callback_query(F.data.startswith("instzp_accept:"))
async def installer_zp_accept(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Монтажник принимает расчётную сумму ЗП без FSM.

    not_requested → requested с amount=_calc_est_montazh(inv). Эквивалент
    instzpadj-флоу, но без диалога — согласие с предложенной суммой.
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.answer("❌ Счёт не найден", show_alert=True)
        return

    zp_st = inv.get("zp_installer_status") or "not_requested"
    if zp_st != "not_requested":
        await cb.answer("⚠️ ЗП уже в обработке", show_alert=True)
        return

    if not _is_work_done_for_zp(inv):
        await cb.answer(_ZP_WORK_NOT_DONE_MSG, show_alert=True)
        return

    # Часть 2 (2.1, 2026-06-08): «✅ Принять» = заявка на ОСТАТОК ЗП
    # (Согласовано − зачтённый аванс ×1.10), а не вся сумма. Без аванса остаток = Итого.
    remainder, agreed, advance_cg = await _zp_remainder_for_invoice(db, inv)
    if remainder <= 0:
        if advance_cg > 0:
            await cb.answer("✅ ЗП уже покрыта авансом — остатка нет", show_alert=True)
        else:
            await cb.answer("⚠️ Сумма не рассчитана", show_alert=True)
        return
    amount = remainder
    # Зафиксировать «Согласовано» если РП не проставил — для консистентности BJ/BS.
    if not float(inv.get("montazh_agreed_amount") or 0) and agreed > 0:
        await db.update_invoice(inv_id, montazh_agreed_amount=agreed)

    u = cb.from_user
    if not u:
        return
    await cb.answer(f"✅ Принято к выплате: {amount:,.0f}₽")

    await db.set_invoice_zp_installer_status(
        inv_id, "requested", amount=amount, requested_by=u.id, is_remainder=True,
    )
    await integrations.sync_invoice_row(inv_id)

    inv_number = inv.get("invoice_number") or "—"
    addr = inv.get("object_address") or "—"

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
                "invoice_id": inv_id,
                "invoice_number": inv_number,
                "amount": amount,
                "source": "installer_zp_accept",
            },
        )
        initiator = await get_initiator_label(db, u.id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{inv_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{inv_id}")
        b.adjust(2)
        await notifier.safe_send(
            int(gd_id),
            _gd_zp_request_card(
                inv, float(amount), initiator=initiator,
                agreed=agreed, advance_cg=advance_cg,
            ),
            reply_markup=b.as_markup(),
        )
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        format_card_section(
            emoji="✅", title="Запрос ЗП отправлен ГД",
            items=[("Счёт", f"№{inv_number}")],
            footer=("Сумма", f"{float(amount):,.0f}₽".replace(",", " ")),
            width=27, compact=True,
        ),
    )


@router.callback_query(F.data.startswith("instzp_price_ok:"))
async def installer_zp_price_ok(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """#20: Монтажник подтвердил цену (Цена ок)."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer("💲 Цена подтверждена")
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
    # Помечаем что монтажник согласен с ценой
    from ..utils import utcnow, to_iso
    await db.update_invoice(inv_id, montazh_stage="invoice_ok", montazh_invoice_ok_at=to_iso(utcnow()))
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💲 Цена по счёту №{inv.get('invoice_number', '?')} подтверждена.",
    )
    # Уведомляем ГД
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        u = cb.from_user
        if not u:
            return
        initiator = await get_initiator_label(db, u.id)
        await notifier.safe_send(
            int(gd_id),
            f"💲 <b>Цена подтверждена монтажником</b>\n"
            f"📄 Счёт №{inv.get('invoice_number', '?')}\n"
            f"👤 {initiator}",
        )


@router.callback_query(F.data == "instobj:back")
async def installer_objects_back(cb: CallbackQuery, db: Database) -> None:
    """Назад к категориям."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoices = await db.list_invoices(limit=200)
    all_inv = [
        i for i in invoices
        if (i.get("montazh_stage") and i["montazh_stage"] != "none"
            or (i["status"] == InvoiceStatus.ENDED
                and (i.get("zp_installer_status") or "") == "approved"))
        and not i.get("parent_invoice_id")
    ]

    work_stages = ("in_work", "razmery_ok")
    _ZP_DONE = ("payment_sent", "confirmed")  # ЗП выплачена/получена → Архив (E, user 18.06)
    in_work = [i for i in all_inv if i.get("montazh_stage") in work_stages]
    archive = [
        i for i in all_inv
        if (i.get("zp_installer_status") or "") in _ZP_DONE
        or (i["status"] == InvoiceStatus.ENDED
            and (i.get("zp_installer_status") or "") in _ZP_DONE)
    ]
    archive_ids = {i["id"] for i in archive}
    work_ids = {i["id"] for i in in_work}
    waiting = [
        i for i in all_inv
        if i.get("montazh_stage") in ("invoice_ok", "invoice_end")
        and i["id"] not in archive_ids
        and i["id"] not in work_ids
    ]

    total = len(in_work) + len(waiting) + len(archive)
    text = f"📌 <b>Мои объекты</b> · {total} шт.\n"

    b = InlineKeyboardBuilder()
    b.button(text=f"🔨 В работе ({len(in_work)})", callback_data="instobj:cat:work")
    b.button(text=f"✅ Ожидает расчёт ({len(waiting)})", callback_data="instobj:cat:waiting")
    b.button(text=f"📦 Архив ({len(archive)})", callback_data="instobj:cat:archive")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


def _inst_card_header(inv: dict) -> tuple[str, str, str, str]:
    """Общие поля для карточек монтажника: mgr_label, lead_name, lead_phone, inv_num."""
    inv_num = inv.get("invoice_number") or f"#{inv.get('id', '?')}"
    if "КИА" in inv_num:
        mgr = "КИА"
        name = inv.get("lead_kia_name") or ""
        phone = inv.get("lead_kia_phone") or ""
    elif "НПН" in inv_num:
        mgr = "НПН"
        name = inv.get("lead_npn_name") or ""
        phone = inv.get("lead_npn_phone") or ""
    else:
        mgr = "КВ"
        name = inv.get("lead_kv_name") or ""
        phone = inv.get("lead_kv_phone") or ""
    if not name:
        name = inv.get("client_name") or ""
    return mgr, name, phone, inv_num


def _build_inst_detail_card(
    inv: dict,
    *,
    agreed: float | None = None,
    advance_cg: float | None = None,
    remainder: float | None = None,
) -> str:
    """Карточка счёта монтажника в стиле стартовой (одиночная ━, ширина 27, итог ЗП в шапке).

    Блок выплаты ЗП (user 2026-06-17): когда переданы agreed/advance_cg/remainder
    (хендлер «Счёт ОК» считает их через _zp_remainder_for_invoice) — добавляются
    строки «Аванс» (зачтённый CG) и «К выплате» (остаток), а нижний «Итого» =
    согласованный монтаж (montazh_agreed), НЕ zp_installer_amount. Без параметров
    (прочие call-site) карточка не меняется (feedback_design_only_indicated_block).

    Содержимое и источники — карта user 22.05, НЕ меняются (только вид,
    feedback_use_only_specified_sources): mgr/lead из _inst_card_header,
    est из _calc_est_montazh(_base), zp_installer_*, deadline_end_date.
    Обёртка как у format_installer_sync_card (utils.format_card_section + схлоп ━→1).
    """
    import re as _re
    from datetime import date as _date

    W = 27

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    stage = inv.get("montazh_stage") or "none"
    stage_lbl = _STAGE_LABEL.get(stage, stage)
    mgr, lead_name, _phone, num = _inst_card_header(inv)
    credit_suffix = ""  # монтажнику кредит-метка в заголовке не показывается

    est_base = _calc_est_montazh_base(inv)  # ×0.67 для б.н., ×0.95 для кредита
    est_total = _calc_est_montazh(inv)  # итог: база + 10% для б.н., база для кредита

    zp_st = inv.get("zp_installer_status") or "not_requested"
    zp_lbl = {"approved": "✅ Одобрено", "requested": "⏳ Запрошено",
              "confirmed": "✅ Подтверждено", "payment_sent": "💳 Отправлено"}.get(zp_st, "—")
    zp_val = float(inv.get("zp_installer_amount") or 0)

    dl_str = ""
    days_str = ""
    deadline = inv.get("deadline_end_date")
    if deadline:
        try:
            d = _date.fromisoformat(str(deadline)[:10])
            delta = (d - _date.today()).days
            dl_str = d.strftime("%d.%m.%Y")
            if delta < 0:
                days_str = f"просрочен {-delta} дн."
            elif delta == 0:
                days_str = "сегодня"
            elif delta <= 7:
                days_str = f"⚠️ {delta} дн."
            else:
                days_str = f"{delta} дн."
        except (ValueError, TypeError):
            dl_str = str(deadline)[:10]

    # Пайаут-вид «Счёт ОК» (user 2026-06-17): когда из хендлера переданы
    # agreed/advance_cg/remainder — показываем Согласовано − зачтённый аванс (CG)
    # = остаток к выплате, а смету («Монтаж») и старую «ЗП сумма» СКРЫВАЕМ (чтобы
    # не путать со согласованной суммой). Без параметров (прочие call-site,
    # счета в работе) карточка прежняя.
    show_zp_payout = agreed is not None and float(agreed) > 0

    # телефон скрыт для монтажника
    items: list[tuple[str, str]] = [
        ("Менеджер", mgr),
        ("Адрес", inv.get("object_address") or "—"),
    ]
    if lead_name:
        items.append(("Клиент", lead_name))
    if est_total and not show_zp_payout:
        if _is_credit(inv):
            items.append(("Монтаж", _f(est_total)))
        else:
            items.append(("Монтаж", _f(est_base)))
            items.append(("Монтаж+10%", _f(est_total)))
    if zp_val and not show_zp_payout:
        items.append(("ЗП сумма", _f(zp_val)))
    items.append(("ЗП статус", zp_lbl))
    if show_zp_payout:
        if advance_cg:
            items.append(("Аванс", _f(advance_cg)))
        if remainder is not None:
            items.append(("К выплате", _f(remainder)))
    if dl_str:
        items.append(("Срок", dl_str))
    # «Осталось» (обратный отсчёт до срока) в пайаут-виде «Счёт ОК» не показываем
    # (user 2026-06-17) — работа завершена, дедлайн не актуален; на счетах в
    # работе строка остаётся.
    if days_str and not show_zp_payout:
        items.append(("Осталось", days_str))

    if show_zp_payout:
        total = _f(agreed)  # Итого = согласованный монтаж (montazh_agreed)
    else:
        total = _f(zp_val) if zp_val else (_f(est_total) if est_total else None)

    return _short(format_card_section(
        emoji="📄",
        title=f"№{num} · {stage_lbl}{credit_suffix}",
        items=items,
        total=total,
        width=W,
        compact=True,
    ))


def _build_archive_stats(invoices: list[dict]) -> str:
    """Статистика архива в стиле В1 (одиночная ━, ширина 27, итог года в шапке).

    Только ВИД (user 01.06): убраны ЗП-сумма и % выполнения; вместо них —
    монтаж база (_calc_est_montazh_base) и итог база+10%/кредит (_calc_est_montazh,
    подпись «итог»). Набор счетов, подсчёт месяца/года/сроков НЕ менялись.
    """
    import re as _re
    from datetime import date as _date

    W = 27

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    today = _date.today()
    cur_month, cur_year = today.month, today.year

    month_inv: list[dict] = []
    year_inv: list[dict] = []
    on_time = 0
    late = 0

    for inv in invoices:
        approved_at = inv.get("zp_installer_approved_at") or ""
        try:
            dt = _date.fromisoformat(str(approved_at)[:10])
        except (ValueError, TypeError):
            dt = None

        if dt and dt.year == cur_year:
            year_inv.append(inv)
            if dt.month == cur_month:
                month_inv.append(inv)

        deadline = inv.get("deadline_end_date")
        completion = inv.get("actual_completion_date") or inv.get("zp_installer_approved_at")
        if deadline and completion:
            try:
                d_dl = _date.fromisoformat(str(deadline)[:10])
                d_co = _date.fromisoformat(str(completion)[:10])
                if d_co <= d_dl:
                    on_time += 1
                else:
                    late += 1
            except (ValueError, TypeError):
                pass

    def _sum_base(invs: list[dict]) -> int:
        return sum(_calc_est_montazh_base(i) for i in invs)

    def _sum_total(invs: list[dict]) -> int:
        return sum(_calc_est_montazh(i) for i in invs)

    items: list[tuple[str, str]] = [
        ("Месяц", f"{_f(_sum_base(month_inv))} · итог {_f(_sum_total(month_inv))}"),
        (f"Год {cur_year}", f"{_f(_sum_base(year_inv))} · итог {_f(_sum_total(year_inv))}"),
        ("Сроки", f"✅ {on_time} · 🔴 {late}"),
    ]

    return _short(format_card_section(
        emoji="📊",
        title="Статистика",
        items=items,
        total=_f(_sum_total(year_inv)),
        width=W,
        compact=True,
    ))


def _build_archive_card(inv: dict) -> str:
    """Карточка архивного счёта для монтажника (табличный формат)."""
    from datetime import date as _date

    mgr, lead_name, lead_phone, num = _inst_card_header(inv)
    text = f"📄 <b>№{num}</b> · 📦 Архив\n\n"

    est_total = _calc_est_montazh(inv)  # итог: ×0.67 + 10% для б.н., ×0.95 для кредита

    zp_st = inv.get("zp_installer_status") or "not_requested"
    zp_lbl = {"approved": "✅ Одобрено", "confirmed": "✅ Подтверждено",
              "requested": "⏳ Запрошено", "payment_sent": "💳 Отправлено"}.get(zp_st, "—")
    zp_val = float(inv.get("zp_installer_amount") or 0)

    # Дельта: ЗП факт - монтаж расч. (итог с +10%)
    delta_str = ""
    if est_total and zp_val:
        delta = zp_val - est_total
        sign = "+" if delta >= 0 else ""
        delta_str = f"{sign}{delta:,.0f}₽"

    # Сроки: (дата факт конец - дата начало - 3 дня комплектация)
    srok_str = ""
    start_str = ""
    end_str = ""
    created = inv.get("receipt_date") or inv.get("created_at")
    completion = inv.get("actual_completion_date") or inv.get("zp_installer_approved_at")
    if created and completion:
        try:
            d_start = _date.fromisoformat(str(created)[:10])
            d_end = _date.fromisoformat(str(completion)[:10])
            fact_days = max((d_end - d_start).days - 3, 0)
            start_str = d_start.strftime("%d.%m.%Y")
            end_str = d_end.strftime("%d.%m.%Y")
            srok_str = f"{fact_days} дн."
        except (ValueError, TypeError):
            pass

    approved = inv.get("zp_installer_approved_at")
    closed_str = str(approved)[:10] if approved else ""

    lines = ["<pre>"]
    lines.append(f"{'Менеджер':16s} {mgr}")
    lines.append(f"{'Адрес':16s} {inv.get('object_address', '—')}")
    if lead_name:
        lines.append(f"{'Клиент':16s} {lead_name}")
    lines.append(f"{'':16s} {'─' * 16}")
    if est_total:
        lines.append(f"{'Монтаж':16s} {est_total:>10,}₽")
    if zp_val:
        lines.append(f"{'ЗП сумма':16s} {zp_val:>10,.0f}₽")
    lines.append(f"{'ЗП статус':16s} {zp_lbl}")
    if delta_str:
        lines.append(f"{'Дельта':16s} {delta_str:>11s}")
    if start_str:
        lines.append(f"{'Начало':16s} {start_str}")
    if end_str:
        lines.append(f"{'Факт конец':16s} {end_str}")
    if srok_str:
        lines.append(f"{'Выполнение':16s} {srok_str}")
    lines.append("</pre>")

    return text + "\n".join(lines)

    return text


@router.callback_query(F.data.startswith("instobj:view:"))
async def installer_object_card(cb: CallbackQuery, db: Database) -> None:
    """Карточка счёта для монтажника."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if _is_work_done_for_zp(inv):
        remainder, agreed, advance_cg = await _zp_remainder_for_invoice(db, inv)
        text = _build_inst_detail_card(
            inv, agreed=agreed, advance_cg=advance_cg, remainder=remainder,
        )
    else:
        text = _build_inst_detail_card(inv)
    stage = inv.get("montazh_stage") or "none"
    cat = "waiting" if stage == "invoice_ok" else "work"
    b = InlineKeyboardBuilder()
    # Кнопка "Запрос ЗП" для карточек в "Ожидает расчёт"
    zp_st = inv.get("zp_installer_status") or "not_requested"
    if cat == "waiting" and zp_st not in ("approved", "requested"):
        b.button(text="💰 Запрос ЗП", callback_data=f"instzpadj:start:{invoice_id}")
    if zp_st == "not_requested":
        b.button(text="✅ Принять", callback_data=f"instzp_accept:{invoice_id}")
    # Чат с менеджером
    b.button(text="💬 Чат с менеджером", callback_data=f"inv_chat:menu:{invoice_id}")
    b.button(text="⬅️ Назад", callback_data=f"instobj:cat:{cat}")
    b.adjust(1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# ЗАПРОС ЗП из «Ожидает расчёт» (InstallerZpAdjustSG)
# =====================================================================


def _is_credit(inv: dict) -> bool:
    """Проверка: кредитный ли счёт.

    is_credit — единственный источник правды (status='credit' ставится автоматически
    через _compute_lifecycle_status). Дополнительно: номер ЗМ-* тоже считается кредитным.
    """
    if inv.get("is_credit"):
        return True
    num = str(inv.get("invoice_number") or "")
    return num.upper().startswith("ЗМ")


def _credit_tag(inv: dict) -> str:
    """Короткая пометка для кредитного счёта."""
    return " · 🏦 <b>КРЕДИТ</b>" if _is_credit(inv) else ""


def _round_montazh(value: float) -> int:
    """Округление суммы монтажа до БЛИЖАЙШЕГО 1000 (остаток ≥500 → вверх).

    Заменяет прежнее округление ВНИЗ (`// 1000 * 1000`): user 03.06 — суммы
    монтажа должны быть кратны 1000 с округлением к ближайшей тысяче, не вниз.
    """
    try:
        return int((float(value) + 500) // 1000) * 1000
    except (ValueError, TypeError):
        return 0


def _calc_est_montazh_base(inv: dict) -> int:
    """База расчётной стоимости монтажа (до 10% надбавки).

    Б.н.: ×0.67 → ближайшие 1000. Кредит: ×0.95 → ближайшие 1000.
    """
    est = inv.get("estimated_installation")
    if not est:
        return 0
    try:
        coef = 0.95 if _is_credit(inv) else 0.67
        return _round_montazh(float(est) * coef)
    except (ValueError, TypeError):
        return 0


def _calc_est_montazh(inv: dict) -> int:
    """Итоговая стоимость монтажа для записи в `montazh_agreed_amount` → Invoices BJ.

    Б.н.: база ×0.67 + 10% надбавка → ближайшие 1000 (итог ≈ ×0.737).
    Кредит: база ×0.95 без надбавки (итог = база).
    """
    base = _calc_est_montazh_base(inv)
    if _is_credit(inv):
        return base
    return _round_montazh(base * 1.10)


def _gd_zp_request_card(
    inv: dict,
    requested: float,
    *,
    initiator: str = "",
    comment: str = "",
    agreed: float | None = None,
    advance_cg: float | None = None,
) -> str:
    """Эталон-v2 карточка «Запрос ЗП монтажника» — входящая задача ГД (✅/❌).

    ТЗ user 07.06: итог в теле <pre> (эталон-v2); показать изначальную
    расчётную стоимость ЗП монтаж рядом с новой (запрошенной) + разницу (Итого);
    добавить имя менеджера и сокращённое название улицы.

    Источники (feedback_use_only_specified_sources):
      • изначальная   = _calc_est_montazh(inv)         (расч. от Invoices R)
      • новая         = requested (zp_installer_amount, что запросил монтажник)
      • разница       = новая − изначальная (показывается, только если менялась)
      • менеджер      = _inst_card_header → КВ/КИА/НПН
      • адрес         = rp_start_card._addr_cell (как в td.py/gd.py): Москва →
                        улица, НЕ Москва → город (owner 07.08, было _street)

    Кредит-пометка и строка «От: монтажник» сохранены как было — поведение не
    меняем (feedback_design_only_indicated_block).
    """
    import re as _re
    from ..rp_start_card import _addr_cell

    W = 27

    def _f(n: float) -> str:
        try:
            return f"{float(n):,.0f}₽".replace(",", " ")
        except (ValueError, TypeError):
            return "—"

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    mgr, _lead, _phone, num = _inst_card_header(inv)
    # Иногородний объект → в карточке ГОРОД, а не улица (owner 07.08): по
    # 2671-1КИА стояло «Поливановская», и из карточки не было видно, что это
    # Подольск, а не Москва. _addr_cell — готовое правило owner'а от 25.07
    # (rp_start_card.py), там же и обоснование; Москва по-прежнему идёт улицей,
    # поэтому вид московских карточек не меняется. Замер на 37 боевых адресах:
    # 32 без изменений, 5 иногородних дают город.
    street = _addr_cell(inv.get("object_address"), 22)
    est_val = _calc_est_montazh(inv)

    items: list[tuple[str, str]] = [
        ("Менеджер", mgr),
        ("Адрес", street),
        ("Счёт", f"№{num}"),
    ]

    footer: tuple[str, str] | None = None
    total: str | None = None
    # Owner 22.08: надбавки +10% к сумме РП больше нет (снята в rp_new.py), поэтому
    # прежняя разбивка «Внёс РП» / «С надбавкой +10%» печатала бы одно и то же число
    # дважды — ГД видит ОДНУ сумму. Надбавка у МОНТАЖНИКА не тронута: она живёт в
    # _calc_est_montazh и на эту карточку приходит через est_val.
    if advance_cg and advance_cg > 0:
        # Часть 2: заявка = ОСТАТОК после зачтённого аванса. Показываем
        # Согласовано + Аванс, итог = к выплате (остаток), без «Разница».
        items.append(("Согласовано", _f(agreed if agreed else est_val)))
        items.append(("Аванс", _f(advance_cg)))
        total = _f(requested)
    else:
        changed = bool(est_val) and abs(float(requested) - float(est_val)) >= 1
        if changed:
            items.append(("Расчётная", _f(est_val)))
            items.append(("Запрошено", _f(requested)))
            diff = float(requested) - float(est_val)
            sign = "+" if diff >= 0 else "−"
            footer = ("Разница", f"{sign}{_f(abs(diff))}")
        else:
            total = _f(requested or est_val)

    card = _short(format_card_section(
        emoji="💰",
        title="Запрос ЗП монтажника",
        items=items,
        total=total,
        footer=footer,
        width=W,
        compact=True,
    ))

    tail = ""
    if _is_credit(inv):
        tail += "\n🏦 <b>⚠️ КРЕДИТНЫЙ СЧЁТ</b>"
    if initiator:
        tail += f"\n👤 От: {initiator} (монтажник)"
    c = (comment or "").strip()
    if c and c != "—":
        tail += f"\n💬 {c}"
    return card + tail


# Оплата каких затрат поднимает ЗП монтаж наёмникам (owner 07.08). Стекло и доп.
# материалы — по ним ГД закрывает «Счёт на оплату», и это самый ранний надёжный
# признак, что работы по объекту пошли. Металл/логистика/грузчики НЕ входят:
# профиль заказывают задолго до монтажа (26225-1КИА оплачен 06.03 и монтаж по нему
# так и не начинался).
NAEM_ZP_TRIGGER_MATERIALS = frozenset({MaterialType.GLASS, MaterialType.EXTRA_MATERIALS})


async def maybe_open_naem_zp_task_after_material_payment(
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    *,
    invoice_id: int,
    material_type: str,
    actor_id: int | None = None,
) -> dict[str, Any]:
    """ГД оплатил стекло/доп.материалы по НАЁМНОМУ счёту → задача ГД на ЗП монтаж.

    Заказ owner'а 06.08. Дыра, которую закрывает: у наёмной группы задача ЗП
    рождается ТОЛЬКО когда РП нажмёт «✅ Монтаж ОК» (rp_montazh_naem_ok). Не нажал —
    деньги не висят нигде: на 07.08 так потерялись 24 000 по 2671-1КИА (Подольск),
    где оплачены стекло 21.07 и доп.материалы 22.07 и 28.07.

    Зеркало rp_montazh_naem_ok: тот же расчёт (Согласовано − выплаченное прошлым
    группам), тот же тип задачи, те же кнопки. Отличия ровно два:
      • карточка собирается ЭТАЛОНОМ (_gd_zp_request_card, тот же состав, что у
        штатного монтажника) — заказ owner'а §1(3);
      • requested_by проставляется РП ЯВНО. Актор здесь ГД, а
        td._finalize_installer_zp_payment шлёт карточку «ЗП выплачена» именно на
        zp_installer_requested_by — оставь пустым, и она не уйдёт никому и молча.

    ✅ installer_ok + стадия «Счёт ОК» ставятся ЗДЕСЬ (owner 13.08) — отмена прежнего
    решения 07.08 «не трогаем». Причина отмены: авто-ЗП ставит zp_installer_status=
    'requested', а гард кнопки РП «✅ Монтаж ОК» (rp_new.py:2210) пропускает только
    'not_requested' — единственный сеттер installer_ok становился недостижим НАВСЕГДА,
    счёт запирался на installer_ok=0, и штатного «Счет End» по нему было уже не
    получить (2671-1КИА висел так с 07.08: ЗП выплачена, стадия assigned).
    Кнопка РП после этого не «пропадает», а отвечает «✅ Монтаж уже подтверждён»
    (rp_new.py:2207) — путь РП схлопывается в авто-путь, а не ломается.
    ⚠️ Побочка на листе, проверенная замером: у наёмных AZ «Этап монтажа» считается
    как edo_task_id==2 AND installer_ok → «Счет End» (sheets.py:830), а AZ с 13.08 —
    триггер автодаты N. Здесь это безвредно: у обоих затронутых счетов цепочка
    AD→AS→AO пуста, писать нечего. Заодно открывается гейт _fact_visible (BG/Y/BL-BO).

    Идемпотентность двойная: zp_installer_status обязан быть 'not_requested' И по
    счёту не должно быть открытой ZP_INSTALLER. Одного статуса мало — по одному
    счёту закрывается НЕСКОЛЬКО оплат материалов (у Подольска три), и без второй
    проверки ГД получил бы три задачи на одну ЗП.

    Возвращает {"created": bool, "reason": str | None, "task_id": int | None,
                "amount": float}. Ошибок наружу не бросает — вызывающий финализирует
    оплату, и падение этой ветки не должно её ломать.
    """
    res: dict[str, Any] = {
        "created": False, "reason": None, "task_id": None, "amount": 0.0,
        "work_confirmed": False,
    }
    if str(material_type or "") not in NAEM_ZP_TRIGGER_MATERIALS:
        res["reason"] = "material_not_trigger"
        return res
    try:
        inv = await db.get_invoice(int(invoice_id))
    except Exception:
        log.warning("naem_zp_trigger: get_invoice failed inv=%s", invoice_id, exc_info=True)
        res["reason"] = "invoice_read_failed"
        return res
    if not inv:
        res["reason"] = "invoice_not_found"
        return res
    # Наёмная группа. edo_task_id=2 и montazh_agreed_amount выставляются ОДНИМ
    # UPDATE в rp_new._finalize_naem → наёмного счёта без Согласованного не бывает,
    # отдельной ветки на пустую сумму не нужно (owner 07.08).
    if inv.get("edo_task_id") != 2:
        res["reason"] = "not_naem"
        return res
    if inv.get("parent_invoice_id") is not None:
        res["reason"] = "not_parent_invoice"
        return res
    if (inv.get("zp_installer_status") or "not_requested") != "not_requested":
        res["reason"] = "zp_already_requested"
        return res
    try:
        if await db.list_open_tasks_by_invoice(int(invoice_id), TaskType.ZP_INSTALLER):
            res["reason"] = "open_task_exists"
            return res
    except Exception:
        log.warning("naem_zp_trigger: dedup check failed inv=%s", invoice_id, exc_info=True)
        res["reason"] = "dedup_check_failed"
        return res

    agreed = float(inv.get("montazh_agreed_amount") or 0)
    paid_prev = float(inv.get("montazh_paid_prev") or 0)
    due = agreed - paid_prev
    if due <= 0:
        res["reason"] = "nothing_due"
        return res
    res["amount"] = due

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        res["reason"] = "no_gd"
        return res
    rp_id = await resolve_default_assignee(db, config, Role.RP)

    # 1) Готовность монтажа — зеркало rp_montazh_naem_ok: installer_ok + «Счёт ОК».
    #    Порядок тот же, что у РП: сначала готовность, затем деньги. Стадию двигаем
    #    ТОЛЬКО вперёд — update_montazh_stage (db.py:3537) пишет что дали, без проверки,
    #    и на invoice_end откатил бы счёт назад. Ошибку глушим: контракт функции —
    #    не ронять финализацию оплаты, ради которой её и вызвали.
    try:
        if not inv.get("installer_ok"):
            await db.set_invoice_installer_ok(int(invoice_id), True)
        if str(inv.get("montazh_stage") or "") not in (
            MontazhStage.INVOICE_OK, MontazhStage.INVOICE_END,
        ):
            await db.update_montazh_stage(int(invoice_id), MontazhStage.INVOICE_OK)
        res["work_confirmed"] = True
    except Exception:
        log.warning(
            "naem_zp_trigger: installer_ok/stage failed inv=%s", invoice_id, exc_info=True,
        )

    # 2) Запрос ЗП монтажника к ГД
    await db.set_invoice_zp_installer_status(
        int(invoice_id), "requested", amount=due,
        requested_by=int(rp_id) if rp_id else None,
    )
    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZP_INSTALLER,
        status=TaskStatus.OPEN,
        created_by=int(rp_id) if rp_id else int(gd_id),
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "invoice_id": int(invoice_id),
            "invoice_number": inv.get("invoice_number") or "—",
            "amount": due,
            "source": "auto_from_material_payment",
            "material_type": str(material_type),
            "paid_by": actor_id,
        },
    )
    res["task_id"] = int(task["id"]) if task else None
    try:
        await integrations.sync_invoice_row(int(invoice_id))
    except Exception:
        log.debug("naem_zp_trigger: sync failed inv=%s", invoice_id, exc_info=True)

    inv_after = await db.get_invoice(int(invoice_id)) or inv
    card = _gd_zp_request_card(inv_after, due, agreed=agreed)
    tail = "\n👤 Наёмная группа 2️⃣ — задача открыта автоматически"
    tail += f"\n📦 Повод: оплачены {MATERIAL_TYPE_LABELS.get(str(material_type), material_type).lower()}"
    if paid_prev > 0:
        # Без этой строки сумма выглядит как ошибка: она МЕНЬШЕ Согласованного.
        tail += (
            f"\n🔗 Согласовано {agreed:,.0f}₽, "
            f"выплачено прошлой группе {paid_prev:,.0f}₽"
        )
    b = InlineKeyboardBuilder()
    b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
    b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
    b.adjust(2)
    await notifier.safe_send(int(gd_id), card + tail, reply_markup=b.as_markup())
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    try:
        await db.audit(
            actor_id=actor_id,
            action="naem_zp_task_auto_opened",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "amount": due, "agreed": agreed, "paid_prev": paid_prev,
                "material_type": str(material_type), "task_id": res["task_id"],
                "requested_by": rp_id, "gd_id": gd_id,
                # Готовность (owner 13.08) — чтобы разбор инцидента видел, из какого
                # состояния счёт был закрыт автоматически.
                "work_confirmed": res["work_confirmed"],
                "installer_ok_before": bool(inv.get("installer_ok")),
                "stage_before": inv.get("montazh_stage"),
            },
        )
    except Exception:
        log.debug("naem_zp_trigger: audit failed inv=%s", invoice_id, exc_info=True)

    res["created"] = True
    return res


async def maybe_ask_naem_montazh_full_payment(
    db: Database,
    config: Config,
    notifier: Notifier,
    *,
    invoice_id: int,
    material_type: str,
    amount: float | None = None,
    actor_id: int | None = None,
) -> bool:
    """ГД внёс затрату на МОНТАЖ по наёмному счёту → «Это вся сумма или будет доплата?»

    Заказ owner'а 06.08 §1(2). Точку спрашивания owner выбрал явно — при вводе
    затрат на монтаж, а не на кнопках задачи ЗП.

    Вопрос задаётся ОТДЕЛЬНЫМ сообщением с инлайн-кнопками, а не шагом FSM. Причина
    практическая: путей ввода затрат на монтаж пять (задача «Счёт на оплату», б/н
    расход ГД, два входа кредит-кошелька и отложенная кредит-заявка), они живут в
    четырёх модулях и имеют РАЗНЫЕ состояния. Шаг FSM пришлось бы вшивать в каждый
    и в каждом же не сломать существующие ветки; одно сообщение после записи
    затраты одинаково работает во всех пяти.

    Возвращает True, если вопрос отправлен.
    """
    if str(material_type or "") != MaterialType.MONTAZH:
        return False
    try:
        inv = await db.get_invoice(int(invoice_id))
    except Exception:
        log.warning("naem_full_ask: get_invoice failed inv=%s", invoice_id, exc_info=True)
        return False
    if not inv or inv.get("edo_task_id") != 2:
        return False
    if (inv.get("montazh_stage") or "") == MontazhStage.INVOICE_END:
        return False  # уже закрыт — спрашивать нечего
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        return False
    b = InlineKeyboardBuilder()
    b.button(text="✅ Это вся сумма", callback_data=f"naemzp:full:{invoice_id}")
    b.button(text="➕ Будет доплата", callback_data=f"naemzp:more:{invoice_id}")
    b.adjust(1)
    _amt = f"\n💵 Внесено: <b>{float(amount or 0):,.0f}₽</b>" if amount else ""
    await notifier.safe_send(
        int(gd_id),
        f"🧾 <b>Затраты на монтаж — наёмная гр. 2️⃣</b>\n"
        f"🔢 Счёт: №{inv.get('invoice_number') or '—'}\n"
        f"📍 {inv.get('object_address') or '—'}{_amt}\n\n"
        f"Это вся сумма или будет доплата?",
        reply_markup=b.as_markup(),
    )
    return True


@router.callback_query(F.data.regexp(r"^naemzp:(full|more):\d+$"))
async def naem_montazh_full_payment_answer(
    cb: CallbackQuery, db: Database, config: Config,
    integrations: IntegrationHub, notifier: Notifier,
) -> None:
    """Ответ ГД на «вся сумма или доплата» по затратам монтажа наёмной группы.

    «Вся» → счёт переводится в «Счёт End (монтаж)». ⛔ Через
    db._auto_invoice_end_after_zp_payment этот переход НЕ идёт: там жёсткий гард
    stage == 'invoice_ok', а наёмный счёт на момент ввода затрат стоит в 'assigned'.
    Гард той функции НЕ трогаем — он защищает три других пути выплаты.

    «Доплата» → ничего не меняем, только фиксируем ответ в audit_log: счёт остаётся
    открытым, ГД довнесёт остаток позже.
    """
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    _, _action, _sid = cb.data.split(":")  # type: ignore[union-attr]
    invoice_id = int(_sid)
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.answer("❌ Счёт не найден.", show_alert=True)
        return
    if inv.get("edo_task_id") != 2:
        await cb.answer("⚠️ Это не наёмный счёт.", show_alert=True)
        return

    if _action == "more":
        await cb.answer("Принято: ожидается доплата.")
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"➕ <b>Затраты на монтаж</b> — счёт №{inv.get('invoice_number') or '—'}\n"
                f"Ожидается доплата, счёт остаётся открытым."
            )
        except Exception:
            pass
    else:
        _stage = inv.get("montazh_stage") or ""
        if _stage == MontazhStage.INVOICE_END:
            await cb.answer("✅ Счёт уже в этапе «Счёт End».", show_alert=True)
            return
        await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_END)
        try:
            inv_after = await db.get_invoice(invoice_id)
            if inv_after:
                await integrations.sync_invoice_status(
                    inv_after["invoice_number"], inv_after.get("status", ""),
                    MontazhStage.INVOICE_END,
                )
            await integrations.sync_invoice_row(invoice_id)
        except Exception:
            log.warning("naem_full: sync failed inv=%s", invoice_id, exc_info=True)
        await cb.answer("✅ Счёт переведён в «Счёт End».")
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"✅ <b>Затраты на монтаж — вся сумма</b>\n"
                f"Счёт №{inv.get('invoice_number') or '—'} переведён в этап "
                f"«Счёт End (монтаж)»."
            )
        except Exception:
            pass

    try:
        await db.audit(
            actor_id=cb.from_user.id,
            action="naem_montazh_full_payment_answer",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "answer": "full" if _action == "full" else "more",
                "stage_before": inv.get("montazh_stage"),
                "agreed": inv.get("montazh_agreed_amount"),
                "cost_montazh": inv.get("cost_montazh"),
            },
        )
    except Exception:
        log.debug("naem_full: audit failed inv=%s", invoice_id, exc_info=True)


async def on_invoice_cost_recorded(
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    *,
    invoice_id: int,
    material_type: str,
    amount: float | None = None,
    actor_id: int | None = None,
) -> dict[str, Any]:
    """Единая точка «по счёту записана затрата» — общая для ВСЕХ путей ввода.

    Разводит две ветки заказа owner'а 06.08:
      • стекло/доп.материалы → поднять ГД задачу на ЗП монтаж наёмникам §1(1);
      • монтаж               → спросить «вся сумма или доплата» §1(2).

    Вызывается из четырёх мест (tasks._invoice_pp_finalize_core,
    chat_proxy._finalize_credit_execution, chat_proxy._credit_spend_finalize,
    gd.op_add_confirm) — они покрывают все пять пользовательских путей.
    """
    out = await maybe_open_naem_zp_task_after_material_payment(
        db, config, notifier, integrations,
        invoice_id=invoice_id, material_type=material_type, actor_id=actor_id,
    )
    out["asked_full_payment"] = await maybe_ask_naem_montazh_full_payment(
        db, config, notifier,
        invoice_id=invoice_id, material_type=material_type,
        amount=amount, actor_id=actor_id,
    )
    return out


def _apply_montazh_bonus(inv: dict, amount: float) -> int:
    """Применить 10% надбавку к ручному вводу суммы — только для б.н.

    Для кредита возвращает amount как есть (ближайшие 1000).
    """
    try:
        amt = float(amount)
    except (ValueError, TypeError):
        return 0
    if not _is_credit(inv):
        amt *= 1.10
    return _round_montazh(amt)


def _advance_cg_amount(advance_offset: float, inv: dict) -> float:
    """Зачтённый аванс монтажника с надбавкой = колонка CG на листе Invoices.

    Б/н: ×1.10 (ЗП монтаж б/н = база+10%); кредит — как есть. Часть 2 (2026-06-08).
    """
    if advance_offset <= 0:
        return 0.0
    return advance_offset if _is_credit(inv) else advance_offset * 1.10


async def _advance_raw_cur(db: Database, inv: dict) -> float:
    """Аванс ТЕКУЩЕЙ монтажной группы по счёту (сырой, без ×1.10).

    installer_advance_items копятся по СЧЁТУ, а не по группе: после объединения
    платежей (owner 15.07) аванс прошлой группы уже учтён внутри montazh_paid_prev,
    поэтому вычитаем базу montazh_adv_prev — иначе он вычитался бы дважды и новая
    группа недополучила бы ровно на его размер. Обычный счёт: adv_prev=0 → всё как было.
    """
    total = await db.get_installer_advance_for_invoice(int(inv["id"]))
    return max(0.0, float(total) - float(inv.get("montazh_adv_prev") or 0))


async def _zp_remainder_for_invoice(db: Database, inv: dict) -> tuple[float, float, float]:
    """Остаток ЗП монтаж к выплате = Согласовано − зачтённый аванс (×1.10 б/н). Часть 2.

    Возвращает (остаток, agreed, advance_cg):
      • agreed       = montazh_agreed_amount (fallback _calc_est_montazh, если 0);
      • advance_cg   = аванс ×1.10 (б/н) / аванс (кредит) = колонка CG на листе;
      • остаток      = max(0, agreed − advance_cg) = колонка BJ (до выплаты ботом).
    «С учётом 10%» — остаток уже на уровне база+10% (agreed и advance_cg оба ×1.10),
    отдельная надбавка не нужна. При выплате остатка ботом счёт закрывается полностью
    (zp_installer_remainder=1 → «Выплачено» = advance_cg + остаток = agreed).

    Объединение платежей (owner 2026-07-15): после смены монтажной группы на выплаченном
    счёте Согласовано = выплаченное прошлым группам + новая сумма (220 000), поэтому из
    остатка вычитается ещё и montazh_paid_prev (90 000) — текущему монтажнику причитается
    только его доплата (130 000). Обычный счёт: montazh_paid_prev=0 → формула прежняя.
    """
    agreed = float(inv.get("montazh_agreed_amount") or 0) or float(_calc_est_montazh(inv))
    advance_offset = await _advance_raw_cur(db, inv)
    advance_cg = _advance_cg_amount(advance_offset, inv)
    paid_prev = float(inv.get("montazh_paid_prev") or 0)
    return max(0.0, agreed - advance_cg - paid_prev), agreed, advance_cg


def _is_work_done_for_zp(inv: dict) -> bool:
    """Готов ли счёт к запросу ЗП.

    Запрос ЗП допустим только если работа подтверждена:
    installer_ok=1 ИЛИ montazh_stage in ('invoice_ok', 'invoice_end') ИЛИ status='ended'.
    Иначе AZ остаётся 'В работе' при ЗП 'Одобрено' — рассинхрон состояний.
    Инцидент 2026-05-18: inv 46/47 ушли в ЗП на стадии in_work.
    """
    return bool(
        inv.get("installer_ok")
        or inv.get("montazh_stage") in ("invoice_ok", "invoice_end")
        or inv.get("status") == "ended"
    )


def _can_edit_zp_amount(inv: dict) -> bool:
    """Можно ли монтажнику изменить/запросить сумму ЗП. Часть 2, 2.3 (user 2026-06-08).

    • not_requested — да (первичный запрос/принятие);
    • requested / approved — да ТОЛЬКО на финальном этапе «Счет End»
      (montazh_stage='invoice_end'), пока ЗП НЕ выплачена — монтажник может
      ещё раз поправить сумму перед повторной отправкой ГД;
    • payment_sent / confirmed / not_applicable — нет (уже выплачено / неактуально).
    """
    zp_st = inv.get("zp_installer_status") or "not_requested"
    if zp_st in ("payment_sent", "confirmed", "not_applicable"):
        return False
    if zp_st == "not_requested":
        return True
    return inv.get("montazh_stage") == "invoice_end"


_ZP_WORK_NOT_DONE_MSG = (
    "⚠️ Сначала отметьте «✅ Счёт ок» в главном меню — "
    "ЗП можно запрашивать только после завершения монтажа."
)


@router.callback_query(F.data.startswith("instzpadj:start:"))
async def zpadj_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 1: старт — показать расч. стоимость, спросить комментарий."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if not _is_work_done_for_zp(inv):
        await cb.message.answer(_ZP_WORK_NOT_DONE_MSG)  # type: ignore[union-attr]
        return

    # Часть 2 (2.1): базовая сумма заявки = ОСТАТОК = Согласовано − зачтённый аванс ×1.10.
    remainder, agreed, advance_cg = await _zp_remainder_for_invoice(db, inv)
    num = inv.get("invoice_number") or f"#{inv['id']}"
    addr = inv.get("object_address") or "—"
    pct_note = "" if _is_credit(inv) else " (с учётом 10%)"

    await state.clear()
    await state.update_data(
        zpadj_invoice_id=invoice_id,
        zpadj_remainder=remainder, zpadj_agreed=agreed, zpadj_advance_cg=advance_cg,
        zpadj_is_credit=_is_credit(inv),
        attachments=[],
    )

    # Эталонная карточка (user 2026-06-17): прежний free-form emoji-вид запрещён
    # (feedback_card_template_standard) → <pre>-блок через format_card_section.
    # Вариант B (выбор user): полный адрес, compact (метка: значение) — контент
    # тот же, меняется ТОЛЬКО дизайн (feedback_design_only_indicated_block).
    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    items: list[tuple[str, str]] = [
        ("Счёт", f"№{num}"),
        ("Адрес", addr),
        ("Согласовано", _f(agreed)),
    ]
    if advance_cg > 0:
        items.append(("Взято авансом", _f(advance_cg)))
        footer = (f"Остаток к выплате{pct_note}", _f(remainder))
    else:
        footer = (f"К выплате{pct_note}", _f(remainder))
    card = format_card_section(
        emoji="💰", title="Запрос ЗП", items=items,
        footer=footer, width=27, compact=True,
    )
    # C/D (user 2026-06-18): комментарий БОЛЬШЕ не обязателен на старте.
    # Два пути: «Запросить остаток» (1 тап → подтверждение, comment="") ИЛИ
    # «Изменить сумму ЗП» (режим → сумма → комментарий ОБЯЗАТЕЛЕН → фото → подтв.).
    b = InlineKeyboardBuilder()
    b.button(text=f"💰 Запросить остаток ({_f(remainder)})", callback_data="instzpadj:reqrem")
    b.button(text="✏️ Изменить сумму ЗП", callback_data="instzpadj:editamt")
    b.button(text="❌ Отмена", callback_data=f"instobj:view:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(card, reply_markup=b.as_markup())  # type: ignore[union-attr]


def _build_zpadj_confirm_text(data: dict[str, Any]) -> str:
    """Карточка подтверждения запроса ЗП (эталон <pre>). Обе ветки C/D:
    «Запросить остаток» (1 тап) и «Изменить сумму» (после комментария+фото)."""
    remainder = float(data.get("zpadj_remainder", 0) or 0)
    total = float(data.get("zpadj_total", 0) or 0)
    new_agreed = float(data.get("zpadj_new_agreed", 0) or 0)
    mode = data.get("zpadj_mode", "replace")
    is_credit = bool(data.get("zpadj_is_credit", False))
    comment = data.get("zpadj_comment", "")
    att_count = len(data.get("attachments", []))

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    items: list[tuple[str, str]] = [("Остаток", _f(remainder))]
    if mode == "add":
        bonus = total - remainder
        note = "" if is_credit else " (+10%)"
        items.append((f"Доплата{note}", _f(bonus)))
        items.append(("Новое «Согласовано»", _f(new_agreed)))
    if comment:
        items.append(("Комментарий", comment))
    items.append(("Вложений", str(att_count)))
    return format_card_section(
        emoji="📋", title="Подтверждение запроса ЗП", items=items,
        footer=("Итого к выплате", _f(total)), width=27, compact=True,
    )


def _zpadj_confirm_kb() -> InlineKeyboardBuilder:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="instzpadj:confirm")
    b.button(text="❌ Отмена", callback_data="instzpadj:cancel")
    b.adjust(2)
    return b


@router.callback_query(F.data == "instzpadj:reqrem")
async def zpadj_req_remainder(cb: CallbackQuery, state: FSMContext) -> None:
    """C/D: «Запросить остаток» в один тап → сразу подтверждение (без комментария).

    total = остаток (BJ), mode='remainder', comment='' — сумма не меняется,
    «Согласовано» не растёт. Экран подтверждения обязателен (денежное действие).
    """
    await cb.answer()
    data = await state.get_data()
    remainder = float(data.get("zpadj_remainder", 0) or 0)
    agreed = float(data.get("zpadj_agreed", 0) or 0)
    if remainder <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Остаток к выплате — 0₽. Запрашивать нечего."
        )
        return
    await state.update_data(
        zpadj_total=remainder, zpadj_input=remainder,
        zpadj_new_agreed=agreed, zpadj_mode="remainder", zpadj_comment="",
    )
    await state.set_state(InstallerZpAdjustSG.confirm)
    await cb.message.answer(  # type: ignore[union-attr]
        _build_zpadj_confirm_text(await state.get_data()),
        reply_markup=_zpadj_confirm_kb().as_markup(),
    )


@router.message(InstallerZpAdjustSG.comment)
async def zpadj_comment(message: Message, state: FSMContext) -> None:
    """Шаг: комментарий (обязателен при изменении суммы) → предложить вложения."""
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("⚠️ Комментарий слишком короткий (мин. 5 символов):")
        return
    await state.update_data(zpadj_comment=text)
    await state.set_state(InstallerZpAdjustSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="⏩ Пропустить", callback_data="instzpadj:to_confirm")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await message.answer(
        "📎 Приложите фото/видео (можно несколько) или нажмите Пропустить:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerZpAdjustSG.attachments)
async def zpadj_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Шаг 3: приём вложений."""
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"installer/{uid}")
    if att is None:
        await message.answer("Пришлите фото/видео/документ или нажмите кнопку.")
        return

    b = InlineKeyboardBuilder()
    b.button(text="⏩ Готово", callback_data="instzpadj:to_confirm")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.", reply_markup=b.as_markup())


@router.callback_query(F.data == "instzpadj:editamt")
async def zpadj_to_mode(cb: CallbackQuery, state: FSMContext) -> None:
    """C/D «Изменить сумму ЗП»: выбор режима — добавить к остатку / своя сумма."""
    await cb.answer()
    data = await state.get_data()
    remainder = float(data.get("zpadj_remainder", 0) or 0)
    await state.set_state(InstallerZpAdjustSG.mode)

    b = InlineKeyboardBuilder()
    b.button(text=f"➕ Добавить к остатку ({remainder:,.0f}₽)", callback_data="instzpadj:mode:add")
    b.button(text="🔄 Указать свою сумму", callback_data="instzpadj:mode:replace")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "Выберите как рассчитать сумму ЗП:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(InstallerZpAdjustSG.mode, F.data.startswith("instzpadj:mode:"))
async def zpadj_mode_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Шаг 4b: выбран режим → запросить сумму."""
    await cb.answer()
    mode = (cb.data or "").split(":")[-1]  # add / replace
    await state.update_data(zpadj_mode=mode)
    await state.set_state(InstallerZpAdjustSG.amount)

    data = await state.get_data()
    remainder = float(data.get("zpadj_remainder", 0) or 0)
    is_credit = bool(data.get("zpadj_is_credit", False))
    if mode == "add":
        note = "" if is_credit else " (к доплате авто +10%)"
        await cb.message.answer(  # type: ignore[union-attr]
            f"Введите доп.сумму к остатку {remainder:,.0f}₽{note} (₽):"
        )
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите итоговую сумму ЗП к выплате (₽):"
        )


@router.message(InstallerZpAdjustSG.amount)
async def zpadj_amount(message: Message, state: FSMContext) -> None:
    """Шаг 5: ввод суммы → подтверждение."""
    raw = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        val = float(raw)
        if val <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число больше 0:")
        return

    data = await state.get_data()
    remainder = float(data.get("zpadj_remainder", 0) or 0)
    agreed = float(data.get("zpadj_agreed", 0) or 0)
    is_credit = bool(data.get("zpadj_is_credit", False))
    mode = data.get("zpadj_mode", "replace")

    if mode == "add":
        # 2.4: доп.сумма +10% (б/н) → растит «Согласовано»; заявка = остаток + доп_bonus.
        bonus = float(_round_montazh(val if is_credit else val * 1.10))
        total = remainder + bonus
        new_agreed = agreed + bonus
    else:
        # «Указать свою» — итоговая сумма к выплате, в пределах остатка
        # (увеличить ЗП можно только через «➕ Добавить к остатку» с +10%).
        total = float(val)
        new_agreed = agreed
        if total > remainder + 0.5:
            await message.answer(
                f"❌ Больше остатка <b>{remainder:,.0f}₽</b> нельзя.\n"
                f"Чтобы увеличить ЗП — выберите «➕ Добавить к остатку». "
                f"Введите сумму не больше остатка:"
            )
            return

    await state.update_data(
        zpadj_total=total, zpadj_input=float(val), zpadj_new_agreed=new_agreed,
    )
    # C/D (user 2026-06-18): сумма изменена → комментарий ОБЯЗАТЕЛЕН,
    # затем фото (опц.) → подтверждение. Карточку строит _build_zpadj_confirm_text.
    await state.set_state(InstallerZpAdjustSG.comment)
    await message.answer(
        "📝 Напишите комментарий — почему меняете сумму ЗП? "
        "(обязательно, мин. 5 символов):"
    )


@router.callback_query(F.data == "instzpadj:to_confirm")
async def zpadj_show_confirm(cb: CallbackQuery, state: FSMContext) -> None:
    """C/D: после комментария+фото (ветка «Изменить сумму») → карточка подтверждения."""
    await cb.answer()
    await state.set_state(InstallerZpAdjustSG.confirm)
    await cb.message.answer(  # type: ignore[union-attr]
        _build_zpadj_confirm_text(await state.get_data()),
        reply_markup=_zpadj_confirm_kb().as_markup(),
    )


@router.callback_query(F.data == "instzpadj:confirm")
async def zpadj_finalize(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Подтверждение: обновить DB + задача ГД."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    invoice_id = data.get("zpadj_invoice_id")
    total = data.get("zpadj_total")
    if not invoice_id or total is None:
        await cb.message.answer("⚠️ Данные сессии утеряны, начните заново.")  # type: ignore[union-attr]
        await state.clear()
        return
    new_agreed = data.get("zpadj_new_agreed")
    agreed_old = float(data.get("zpadj_agreed", 0) or 0)
    comment = data.get("zpadj_comment", "")
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    mode = data.get("zpadj_mode", "replace")

    # 2.4: зафиксировать «Согласовано» — рост при доплате / фиксация fallback при 0.
    inv = await db.get_invoice(invoice_id)
    montazh_agreed_now = float(inv.get("montazh_agreed_amount") or 0) if inv else 0.0
    target_agreed = float(new_agreed) if new_agreed is not None else agreed_old
    # Пишем ДЕЛЬТУ поверх актуального значения, а не снимок из FSM: карточка ЗП живёт в
    # чате долго, и за это время РП мог объединить платежи (owner 15.07) — снимок затёр бы
    # объединённое «Согласовано» (220 000 → 130 000) и урезал доплату до 40 000
    # [[feedback_fsm_old_buttons_trap]]. Дельта > 0 только в режиме «➕ Добавить к остатку».
    _delta = target_agreed - agreed_old
    # База — актуальное значение; если на счёте ещё 0, снимок нёс fallback-смету
    # (_calc_est_montazh) — тогда база она, иначе «Согласовано» схлопнулось бы в одну надбавку.
    _base = montazh_agreed_now if montazh_agreed_now > 0 else agreed_old
    if _delta > 0.5:
        await db.update_invoice(invoice_id, montazh_agreed_amount=_base + _delta)
    elif montazh_agreed_now <= 0 and target_agreed > 0:
        # Фиксация fallback-сметы, когда «Согласовано» на счёте ещё не задано.
        await db.update_invoice(invoice_id, montazh_agreed_amount=target_agreed)

    # Обновить статус + флаг «заявка = остаток» → additive «Выплачено» на листе (2.2).
    await db.set_invoice_zp_installer_status(
        invoice_id, "requested", amount=total, requested_by=u.id, is_remainder=True,
    )
    await integrations.sync_invoice_row(invoice_id)

    inv_number = inv["invoice_number"] if inv else "—"
    addr = inv.get("object_address", "—") if inv else "—"

    # Создать задачу для ГД
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.ZP_INSTALLER,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=int(gd_id),
            due_at_iso=None,
            payload={
                "invoice_id": invoice_id,
                "invoice_number": inv_number,
                "amount": total,
                "comment": comment,
                "source": "installer_zp_adjust",
            },
        )
        # Сохранить вложения к задаче
        for a in attachments:
            await db.add_attachment(
                task_id=int(task["id"]),
                file_id=a["file_id"],
                file_unique_id=a.get("file_unique_id"),
                file_type=a["file_type"],
                caption=a.get("caption"),
                minio_object_key=a.get("minio_object_key"),
            )

        initiator = await get_initiator_label(db, u.id)
        notify_text = _gd_zp_request_card(
            inv, float(total), initiator=initiator, comment=comment,
            agreed=target_agreed, advance_cg=float(data.get("zpadj_advance_cg", 0) or 0),
        )
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
        b.adjust(2)
        await notifier.safe_send(int(gd_id), notify_text, reply_markup=b.as_markup())
        # Переслать вложения
        for a in attachments:
            await notifier.safe_send_media(int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"))
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        format_card_section(
            emoji="✅", title="Запрос ЗП отправлен ГД",
            items=[("Счёт", f"№{inv_number}")],
            footer=("Сумма", f"{float(total):,.0f}₽".replace(",", " ")),
            width=27, compact=True,
        ),
    )


@router.callback_query(F.data == "instzpadj:cancel")
async def zpadj_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Отмена запроса ЗП."""
    await cb.answer()
    await state.clear()
    u = cb.from_user
    if not u:
        await cb.message.answer("❌ Запрос ЗП отменён.")  # type: ignore[union-attr]
        return
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Запрос ЗП отменён.", reply_markup=kb)  # type: ignore[union-attr]


# =====================================================================
# ОТЧЁТ ЗА ДЕНЬ (text to RP via chat-proxy)
# =====================================================================

@router.message(F.text == INST_BTN_DAILY_REPORT)
async def start_daily_report(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    await state.set_state(InstallerDailyReportSG.text)
    await message.answer(
        "📝 <b>Отчёт за день</b>\n\n"
        "Заполните:\n"
        "• Объект\n"
        "• Что сделано\n"
        "• Проблемы\n"
        "• Простой\n\n"
        "Напишите одним сообщением:"
    )


@router.message(InstallerDailyReportSG.text)
async def daily_report_text(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("Напишите подробнее:")
        return
    await state.update_data(text=text, attachments=[])
    await state.set_state(InstallerDailyReportSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить РП", callback_data="inst_report:send")
    b.button(text="⏭ Без вложений", callback_data="inst_report:send")
    b.adjust(1)
    await message.answer(
        "Прикрепите фото/файлы или нажмите «Отправить РП»:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerDailyReportSG.attachments)
async def daily_report_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"installer/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


@router.callback_query(F.data == "inst_report:send")
async def daily_report_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    text = data["text"]
    attachments = data.get("attachments", [])

    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await cb.message.answer("⚠️ РП не найден. Попросите администратора назначить роль РП.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Save as chat message
    await db.save_chat_message(
        channel="montazh",
        sender_id=u.id,
        direction="outgoing",
        text=f"[Отчёт за день]\n{text}",
        receiver_id=int(rp_id),
        has_attachment=bool(attachments),
    )

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📝 <b>Отчёт за день от монтажника</b>\n"
        f"👤 От: {initiator}\n\n"
        f"{text}\n"
    )

    await notifier.safe_send(int(rp_id), msg)
    for a in attachments:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "✅ Отчёт отправлен РП.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# В РАБОТУ (accept tasks from RP)
# =====================================================================

@router.message(lambda m: (m.text or "").strip().startswith(INST_BTN_IN_WORK))
async def installer_in_work(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Список неподтверждённых счетов для принятия в работу."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    # Только счета, назначенные ЭТОМУ монтажнику (user 03.06): не общий пул —
    # неотправленные/наёмные (assigned_to=NULL) не должны попадать к нему.
    invoices = await db.list_installer_unconfirmed_invoices(message.from_user.id)

    if not invoices:
        await answer_service(message, "🔨 Нет новых счетов для принятия в работу ✅", delay_seconds=60)
        return

    # Restore reply keyboard before sending inline content
    await _ensure_reply_kb_msg(message, db, config)

    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        icon = "🏦" if inv.get("is_credit") else "📄"
        b.button(
            text=f"{icon} №{num} — {addr}"[:55],
            callback_data=f"inst_work:view:{inv['id']}",
        )
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)

    await message.answer(
        f"🔨 <b>В Работу</b> ({len(invoices)})\n\n"
        "Счета, назначенные вам. Нажмите для просмотра и подтверждения:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("inst_work:view:"))
async def installer_work_view_card(
    cb: CallbackQuery, db: Database, config: Config,
) -> None:
    """Карточка счёта для подтверждения «В работу»."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    # Монтаж: база + итог (б/н = база ×0.67, итог = база +10%; кредит = база ×0.95, итог = база).
    # Монтажнику показываем СУММЫ (база + итог), без множителей-коэффициентов.
    est_base = _calc_est_montazh_base(inv)
    est_total = _calc_est_montazh(inv)

    addr = inv.get("object_address") or "—"

    # Менеджер — тип из номера счёта
    inv_num = inv.get("invoice_number") or ""
    if "КИА" in inv_num:
        mgr_label = "КИА"
    elif "НПН" in inv_num:
        mgr_label = "НПН"
    else:
        mgr_label = "КВ"

    # Имя и телефон лида (по типу счёта: КВ/КИА/НПН)
    lead_name = ""
    lead_phone = ""
    if "КИА" in inv_num:
        lead_name = inv.get("lead_kia_name") or ""
        lead_phone = inv.get("lead_kia_phone") or ""
    elif "НПН" in inv_num:
        lead_name = inv.get("lead_npn_name") or ""
        lead_phone = inv.get("lead_npn_phone") or ""
    else:
        lead_name = inv.get("lead_kv_name") or ""
        lead_phone = inv.get("lead_kv_phone") or ""
    if not lead_name:
        lead_name = inv.get("client_name") or ""

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
                days_left_str = f"⚠️ просрочен на {-days_left} дн."
            elif days_left == 0:
                days_left_str = "⚠️ сегодня"
            else:
                days_left_str = f"{days_left} дн."
        except (ValueError, TypeError):
            pass

    # Карточка в стиле стартовой (В1): format_card_section(compact) + схлоп ━→1.
    # Монтажнику не показываем сумму счёта / тип оплаты — только данные работы.
    import re as _re

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    items: list[tuple[str, str]] = [
        ("Менеджер", mgr_label),
        ("Адрес", addr),
    ]
    if lead_name:
        items.append(("Клиент", lead_name))
    if dl_str:
        items.append(("Срок", dl_str))
    if days_left_str:
        items.append(("Осталось", days_left_str))
    if est_total:
        # Суммы база+итог (без коэффициентов): б/н — две строки, кредит — одна.
        if _is_credit(inv):
            items.append(("Монтаж расч.", _f(est_total)))
        else:
            items.append(("Монтаж", _f(est_base)))
            items.append(("Монтаж+10%", _f(est_total)))
    comment = (inv.get("description") or "").strip()
    if comment:
        items.append(("Комментарий", comment))

    total = _f(est_total) if est_total else None
    text = _short(format_card_section(
        emoji="🔨",
        title=f"В работу: №{inv_num}",
        items=items,
        total=total,
        width=27,
        compact=True,
    ))

    b = InlineKeyboardBuilder()
    b.button(text=f"🔨 В работу ({est_total:,}₽)", callback_data=f"inst_work:price_ok:{invoice_id}")
    b.button(text="✏️ Изменить сумму", callback_data=f"inst_work:price_edit:{invoice_id}")
    b.adjust(1)

    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]

    # Показать вложения от РП (если есть)
    att_json = inv.get("montazh_assign_attachments_json")
    if att_json:
        import json
        try:
            attachments = json.loads(att_json)
        except (json.JSONDecodeError, TypeError):
            attachments = []
        for a in attachments:
            try:
                ft = a.get("file_type", "")
                fid = a.get("file_id", "")
                cap = a.get("caption", "")
                if ft == "photo":
                    await cb.message.answer_photo(fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "video":
                    await cb.message.answer_video(fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "document":
                    await cb.message.answer_document(fid, caption=cap or None)  # type: ignore[union-attr]
                elif ft == "text" and cap:
                    await cb.message.answer(f"💬 {cap}")  # type: ignore[union-attr]
            except Exception:
                pass


@router.callback_query(F.data.startswith("inst_work:price_ok:"))
async def installer_price_ok(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Монтажник согласен с расчётной ценой → фиксация + В работу."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    # Зафиксировать сумму: СОХРАНИТЬ уже заданную ручную сумму, смету
    # _calc_est_montazh писать ТОЛЬКО если ещё не задана. Раньше кнопка «Согласен
    # с расчётной ценой» безусловно затирала ручной ввод (напр. 32 200) обратно на
    # смету (R×0.95=51 000) — рассинхрон с instok:price_ok, баг user 2026-06-17.
    agreed = float(inv.get("montazh_agreed_amount") or 0) or _calc_est_montazh(inv)
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, assigned_to = ?, updated_at = ? WHERE id = ?",
        (agreed, u.id, datetime.now().isoformat(), invoice_id),
    )
    await db.conn.commit()
    await db.update_montazh_stage(invoice_id, MontazhStage.IN_WORK)
    await integrations.sync_invoice_row(invoice_id)

    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Стоимость монтажа согласована: <b>{agreed:,}₽</b>\n"
        f"🔨 Счёт №{inv['invoice_number']} принят в работу."
    )

    # Уведомление ГД
    if config.default_gd_id:
        try:
            await cb.bot.send_message(
                config.default_gd_id,
                f"✅ ЗП монтаж согласовано: ок\n"
                f"Монтажник <b>@{u.username or u.full_name}</b>: "
                f"№{inv['invoice_number']} — <b>{agreed:,}₽</b>\n"
                f"📍 {inv.get('object_address', '')}",
            )
        except Exception:
            pass


@router.callback_query(F.data.startswith("inst_work:price_edit:"))
async def installer_price_edit(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Монтажник хочет изменить сумму → FSM ввод новой суммы."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(InstallerWorkAcceptSG.price_input)
    await state.update_data(invoice_id=invoice_id)

    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        "💰 Введите вашу сумму за монтаж (в рублях):"
    )


@router.message(InstallerWorkAcceptSG.price_input)
async def installer_price_input(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Монтажник ввёл базу → пересчёт Итого → карточка (база+Итог) + «Согласовать».

    Запись montazh_agreed_amount + «В работу» происходит ТОЛЬКО после нажатия
    «✅ Согласовать» (см. installer_price_confirm), а не на вводе суммы.
    """
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", "")
    try:
        amount = int(float(text))
    except (ValueError, TypeError):
        await message.answer("❌ Введите число (сумма в рублях):")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0:")
        return

    data = await state.get_data()
    invoice_id = data["invoice_id"]
    await state.clear()

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        return

    # Для б.н. — база = ввод (округл. до 1000), итог = база +10%. Для кредита — одна сумма.
    base_disp = _round_montazh(amount)
    agreed = _apply_montazh_bonus(inv, amount)

    def _f(n: float) -> str:
        return f"{float(n):,.0f}₽".replace(",", " ")

    import re as _re

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    items: list[tuple[str, str]] = []
    if _is_credit(inv):
        items.append(("Монтаж расч.", _f(agreed)))
    else:
        items.append(("Монтаж", _f(base_disp)))
        items.append(("Монтаж+10%", _f(agreed)))

    card = _short(format_card_section(
        emoji="🔨",
        title=f"Согласование: №{inv.get('invoice_number') or invoice_id}",
        items=items,
        total=_f(agreed),
        width=27,
        compact=True,
    ))

    b = InlineKeyboardBuilder()
    b.button(
        text=f"✅ Согласовать ({agreed:,}₽)",
        callback_data=f"inst_work:price_confirm:{invoice_id}:{agreed}",
    )
    b.button(text="✏️ Изменить сумму", callback_data=f"inst_work:price_edit:{invoice_id}")
    b.adjust(1)

    await _ensure_reply_kb_msg(message, db, config)
    await message.answer(card, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("inst_work:price_confirm:"))
async def installer_price_confirm(
    cb: CallbackQuery, db: Database, config: Config,
    integrations: IntegrationHub,
) -> None:
    """Монтажник нажал «Согласовать» → фиксация montazh_agreed_amount + В работу + ГД.

    Сумма переносится в callback (не в FSM) — против ловушки старых кнопок при
    смене состояния (feedback_fsm_old_buttons_trap).
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    parts = (cb.data or "").split(":")  # inst_work:price_confirm:<id>:<agreed>
    try:
        invoice_id = int(parts[2])
        agreed = int(parts[3])
    except (IndexError, ValueError):
        await cb.message.answer("⚠️ Данные кнопки устарели, откройте счёт заново.")  # type: ignore[union-attr]
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    if agreed <= 0:
        agreed = _calc_est_montazh(inv)

    # Объединение платежей (owner 15.07): на счёте после смены монтажной группы
    # «Согласовано» = выплаченное прошлым группам + сумма текущей. Монтажник вводит
    # ТОЛЬКО свою сумму, поэтому безусловная запись затёрла бы объединение (220 000 →
    # 130 000), а montazh_paid_prev остался бы → его же доплата ужалась бы до 40 000.
    _paid_prev = float(inv.get("montazh_paid_prev") or 0)
    if _paid_prev > 0:
        agreed = int(round(_paid_prev + agreed))

    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, assigned_to = ?, updated_at = ? WHERE id = ?",
        (agreed, u.id, datetime.now().isoformat(), invoice_id),
    )
    await db.conn.commit()
    await db.update_montazh_stage(invoice_id, MontazhStage.IN_WORK)
    await integrations.sync_invoice_row(invoice_id)

    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Стоимость монтажа согласована: <b>{agreed:,}₽</b>\n"
        f"🔨 Счёт №{inv['invoice_number']} принят в работу."
    )

    # Уведомление ГД
    if config.default_gd_id:
        try:
            await cb.bot.send_message(  # type: ignore[union-attr]
                config.default_gd_id,
                f"✅ ЗП монтаж согласовано: ок\n"
                f"Монтажник <b>@{u.username or u.full_name}</b>: "
                f"№{inv['invoice_number']} — <b>{agreed:,}₽</b>\n"
                f"📍 {inv.get('object_address', '')}",
            )
        except Exception:
            pass


@router.callback_query(F.data.startswith("inst_work:confirm:"))
async def installer_work_confirm(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Монтажник подтверждает «В работу» → montazh_stage=IN_WORK."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_montazh_stage(invoice_id, MontazhStage.IN_WORK)
    # #2: Привязать счёт к монтажнику при «В работу»
    await db.conn.execute(
        "UPDATE invoices SET assigned_to = ?, updated_at = ? WHERE id = ?",
        (u.id, datetime.now().isoformat(), invoice_id),
    )
    await db.conn.commit()

    await integrations.sync_invoice_status(
        inv["invoice_number"], inv.get("status", ""), MontazhStage.IN_WORK,
    )

    # Уведомить РП
    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"🔨 <b>Монтажник — В работу</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Счёт №{inv['invoice_number']} принят в работу ✅"
    )
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    role, isolated_role = await _current_menu(db, u.id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Счёт №{inv['invoice_number']} принят в работу.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# ЗАПРОС ЗП МОНТАЖНИКА (InstallerZpSG)
# =====================================================================

def _build_zp_init_kb(
    invoices: list[dict[str, Any]], selected: set[int],
) -> InlineKeyboardBuilder:
    """Построить inline-клавиатуру мульти-выбора ЗП (☐/✅)."""
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = inv["id"]
        prefix = "✅" if inv_id in selected else "☐"
        num = inv.get("invoice_number") or f"#{inv_id}"
        addr = (inv.get("object_address") or "—")[:25]
        b.button(text=f"{prefix} №{num} — {addr}"[:55], callback_data=f"zpinit:toggle:{inv_id}")
    b.button(text="✅ Готово", callback_data="zpinit:done")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    return b


def _build_inst_zp_card(inv: dict) -> str:
    """Карточка счёта в списке «Запрос ЗП» монтажника — стиль В1 (одиночная ━,
    ширина 27, итог в шапке, компакт `поле: значение`).

    Только вид. Содержимое/источники 22.05 не меняются
    (feedback_use_only_specified_sources): как _build_inst_detail_card + Монтаж
    факт (montazh_agreed_amount) и ЗП (zp_installer_amount при статусе ≥ requested).
    Обёртка как у format_installer_sync_card (format_card_section + схлоп ━→1).
    """
    import re as _re
    from datetime import date as _date, datetime as _dt

    W = 27

    def _f(n: float) -> str:
        try:
            return f"{float(n):,.0f}₽".replace(",", " ")
        except (ValueError, TypeError):
            return "—"

    def _short(block: str) -> str:
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    zp_st = inv.get("zp_installer_status") or "not_requested"
    zp_icon = {"not_requested": "❌", "requested": "⏳", "approved": "✅"}.get(zp_st, "❌")
    zp_label = {"not_requested": "Не запрошена", "requested": "На проверке",
                "approved": "Одобрена", "payment_sent": "Отправлена",
                "confirmed": "Оплачена"}.get(zp_st, "—")
    mgr, lead_name, _phone, num = _inst_card_header(inv)
    credit_suffix = ""  # монтажнику кредит-метка в заголовке не показывается

    est_base = _calc_est_montazh_base(inv)  # ×0.67 для б.н., ×0.95 для кредита
    est_total = _calc_est_montazh(inv)  # итог: база + 10% для б.н., база для кредита
    zp_amount = inv.get("zp_installer_amount")

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

    items: list[tuple[str, str]] = [
        ("Менеджер", mgr),
        ("Адрес", inv.get("object_address") or "—"),
    ]
    if lead_name:
        items.append(("Клиент", lead_name))
    if est_total:
        if _is_credit(inv):
            items.append(("Монтаж расч.", _f(est_total)))
        else:
            items.append(("Монтаж", _f(est_base)))
            items.append(("Монтаж+10%", _f(est_total)))
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if agreed:
        items.append(("Монтаж факт", _f(agreed)))
    zp_val = 0.0
    if zp_amount and zp_st in ("requested", "approved", "payment_sent", "confirmed"):
        try:
            zp_val = float(zp_amount)
            items.append(("ЗП", _f(zp_val)))
        except (ValueError, TypeError):
            zp_val = 0.0
    items.append(("ЗП статус", zp_label))
    if dl_str:
        items.append(("Срок", dl_str))
    if days_left_str:
        items.append(("Осталось", days_left_str))

    total = _f(zp_val) if zp_val else (_f(est_total) if est_total else None)

    return _short(format_card_section(
        emoji=zp_icon,
        title=f"№{num} · {zp_label}{credit_suffix}",
        items=items,
        total=total,
        width=W,
        compact=True,
    ))


def _build_inst_zp_summary(
    invoices: list[dict],
    not_req: list[dict],
    requested: list[dict],
    approved: list[dict],
    sum_approved: float,
) -> str:
    """Сводка раздела «Запрос ЗП» монтажника — эталонный <pre> (user 10.06).

    Заменяет прежний плоский inline-вид с «·»/«|» (запрещён,
    feedback_card_template_standard). Только вид; счётчики/сумма как раньше
    (installer_zp_start / inst_zp_classic).

    Выравнивание (user 10.06): у КАЖДОЙ строки ровно ОДНА single-codepoint emoji
    (📋❌⏳✅💰, len()==1, в Telegram рисуется ~2 ячейки) → одинаковый визуальный
    сдвиг во всех строках → числа справа встают ровно (раньше emoji-строки статусов
    уезжали на ~1 ячейку вправо относительно «Всего»/«Σ» без emoji). Нулевые статусы
    скрываем — как было до переверстки (user 10.06).
    """
    items = [("📋 Всего счетов", str(len(invoices)))]
    if not_req:
        items.append(("❌ Не запрошено", str(len(not_req))))
    if requested:
        items.append(("⏳ На проверке", str(len(requested))))
    if approved:
        items.append(("✅ Оплачено", str(len(approved))))
    footer = ("💰 Сумма", f"{sum_approved:,.0f}".replace(",", " ")) if approved else None
    return format_card_section(
        emoji="💰",
        title="Запрос ЗП",
        items=items,
        footer=footer,
        width=24,
    )


@router.message(lambda m: (m.text or "").strip().startswith(INST_BTN_ZP))
async def installer_zp_start(message: Message, state: FSMContext, db: Database) -> None:
    """Запрос ЗП: инициализация (первый вход) или стандартный поток.

    ТЗ 2026-05-19 блок C: для монтажников в whitelist ADVANCE_ENABLED_INSTALLERS
    показываем подменю с тремя кнопками (ЗП / Аванс / Баланс) вместо прямого списка.
    """
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    await state.clear()

    # --- Whitelist: подменю «Запрос ЗП / Запрос аванса / Мой баланс» ---
    from ..config import ADVANCE_ENABLED_INSTALLERS
    if user_id in ADVANCE_ENABLED_INSTALLERS:
        await _send_zp_submenu(message, db, user_id)
        return

    # --- Первый заход: инициализация ---
    if not await db.is_installer_zp_initialized(user_id):
        invoices = await db.list_installer_confirmed_invoices()
        if not invoices:
            await db.set_installer_zp_initialized(user_id)
            await message.answer("✅ Нет счетов в работе. Инициализация завершена.")
            return
        await state.set_state(InstallerZpInitSG.selecting)
        await state.update_data(
            zp_init_selected=[],
            zp_init_invoices=[inv["id"] for inv in invoices],
        )
        b = _build_zp_init_kb(invoices, set())
        await message.answer(
            "💰 <b>Инициализация ЗП</b>\n\n"
            "Выберите счета, по которым ЗП <b>не оплачена</b>:\n"
            "(нажмите на счёт для выбора/снятия, затем «✅ Готово»)",
            reply_markup=b.as_markup(),
        )
        return

    # --- Стандартный поток: карточки всех счетов со статусом ЗП ---
    # Include invoices in active montazh stages OR already approved ZP (ended invoices)
    cur = await db.conn.execute(
        "SELECT * FROM invoices "
        "WHERE ("
        "  montazh_stage IN ('in_work', 'razmery_ok', 'invoice_ok') "
        "  OR zp_installer_status = 'approved'"
        ") "
        "  AND status IN ('in_progress', 'paid', 'ended', 'credit') "
        "  AND parent_invoice_id IS NULL "
        "  AND (zp_installer_status IS NULL OR zp_installer_status != 'not_applicable') "
        "ORDER BY id DESC LIMIT 30",
    )
    rows = await cur.fetchall()
    invoices = [dict(r) for r in rows]
    if not invoices:
        await message.answer("📭 Нет счетов.")
        return

    # Статистика
    not_req = [i for i in invoices if (i.get("zp_installer_status") or "not_requested") == "not_requested"]
    requested = [i for i in invoices if i.get("zp_installer_status") == "requested"]
    approved = [i for i in invoices if i.get("zp_installer_status") == "approved"]
    sum_approved = sum(float(i.get("zp_installer_amount") or 0) for i in approved)

    await message.answer(
        _build_inst_zp_summary(invoices, not_req, requested, approved, sum_approved)
    )

    # Карточки
    for inv in invoices:
        zp_st = inv.get("zp_installer_status") or "not_requested"
        card = _build_inst_zp_card(inv)

        b = InlineKeyboardBuilder()
        if zp_st == "not_requested":
            b.button(text="✏️ Изменить стоимость", callback_data=f"instzpadj:start:{inv['id']}")
            b.button(text="💰 Запросить ЗП", callback_data=f"instzpadj:start:{inv['id']}")
        elif _can_edit_zp_amount(inv):
            # Часть 2 (2.3): повторная правка суммы на этапе «Счет End».
            b.button(text="✏️ Изменить сумму", callback_data=f"instzpadj:start:{inv['id']}")
        b.button(text="⬅️ Назад", callback_data="inst_nav:home")
        b.adjust(1)
        await message.answer(card, reply_markup=b.as_markup())


# --- ZP init: toggle / done ---

@router.callback_query(F.data.startswith("zpinit:toggle:"), InstallerZpInitSG.selecting)
async def zp_init_toggle(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Переключить выбор счёта в мульти-выборе ЗП."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    selected = set(data.get("zp_init_selected", []))
    if inv_id in selected:
        selected.discard(inv_id)
    else:
        selected.add(inv_id)
    await state.update_data(zp_init_selected=list(selected))
    # Перестроить клавиатуру
    all_ids = data.get("zp_init_invoices", [])
    invoices = []
    for iid in all_ids:
        inv = await db.get_invoice(iid)
        if inv:
            invoices.append(inv)
    b = _build_zp_init_kb(invoices, selected)
    try:
        await cb.message.edit_reply_markup(reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        pass


@router.callback_query(F.data == "zpinit:done", InstallerZpInitSG.selecting)
async def zp_init_done(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Завершить инициализацию ЗП: невыбранные → not_applicable."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    selected = set(data.get("zp_init_selected", []))
    all_ids = data.get("zp_init_invoices", [])
    for inv_id in all_ids:
        if inv_id not in selected:
            await db.set_invoice_zp_installer_status(inv_id, "not_applicable")
    await db.set_installer_zp_initialized(u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Инициализация завершена.\n"
        f"Счетов с неоплаченной ЗП: <b>{len(selected)}</b>\n\n"
        "Нажмите «💰 Запрос ЗП» ещё раз для выбора счёта.",
    )


@router.callback_query(F.data.startswith("instzp:pick:"), InstallerZpSG.select_invoice)
async def installer_zp_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    if not _is_work_done_for_zp(inv):
        await cb.answer(_ZP_WORK_NOT_DONE_MSG, show_alert=True)
        await state.clear()
        return
    await state.update_data(zp_invoice_id=invoice_id)
    await state.set_state(InstallerZpSG.amount)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 Счёт: <b>№{inv['invoice_number']}</b>\n"
        f"📍 Адрес: {inv.get('object_address') or '—'}\n\n"
        "Введите сумму ЗП (число):",
    )


@router.message(InstallerZpSG.amount)
async def installer_zp_amount(message: Message, state: FSMContext, db: Database) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите корректную сумму (положительное число):")
        return
    data = await state.get_data()
    invoice_id = data.get("zp_invoice_id")
    if not invoice_id:
        await message.answer("⚠️ Данные сессии утеряны, начните заново.")
        await state.clear()
        return
    inv = await db.get_invoice(invoice_id)
    await state.update_data(zp_amount=amount)
    await state.set_state(InstallerZpSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="instzp:confirm")
    b.button(text="❌ Отмена", callback_data="instzp:cancel")
    b.adjust(2)
    inv_num = inv["invoice_number"] if inv else "—"
    card = format_card_section(
        emoji="💰", title="Подтверждение запроса ЗП",
        items=[("Счёт", f"№{inv_num}")],
        footer=("ЗП к выплате", f"{float(amount):,.0f}₽".replace(",", " ")),
        width=27, compact=True,
    )
    await message.answer(
        card + "\n\nОтправить запрос ГД?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "instzp:cancel")
async def installer_zp_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("Отменено")
    await state.clear()
    await cb.message.answer("❌ Запрос ЗП отменён.")  # type: ignore[union-attr]


@router.callback_query(F.data == "instzp:confirm", InstallerZpSG.confirm)
@money_confirm_guard
async def installer_zp_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    invoice_id = data.get("zp_invoice_id")
    amount = data.get("zp_amount")
    if not invoice_id or amount is None:
        await cb.message.answer("⚠️ Данные сессии утеряны, начните заново.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Update invoice
    await db.set_invoice_zp_installer_status(invoice_id, "requested", amount=amount, requested_by=u.id)

    inv = await db.get_invoice(invoice_id)
    inv_number = inv["invoice_number"] if inv else "—"

    # Create task for GD
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
                "amount": amount,
                "source": "installer_zp",
            },
        )
        initiator = await get_initiator_label(db, u.id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_inst:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_inst:no:{invoice_id}")
        b.adjust(2)
        if inv:
            zp_card = _gd_zp_request_card(inv, float(amount), initiator=initiator)
        else:
            zp_card = (
                f"💰 <b>Запрос ЗП монтажника</b>\n"
                f"👤 От: {initiator}\n"
                f"🔢 Счёт: №{inv_number}\n"
                f"💵 Сумма: {amount:,.0f}₽"
            )
        await notifier.safe_send(int(gd_id), zp_card, reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        format_card_section(
            emoji="✅", title="Запрос ЗП отправлен ГД",
            items=[("Счёт", f"№{inv_number}")],
            footer=("Сумма", f"{float(amount):,.0f}₽".replace(",", " ")),
            width=27, compact=True,
        ),
    )


# =====================================================================
# ТЗ 2026-05-19 блок C: Авансы монтажника (whitelist Игорь Быканов).
# =====================================================================


def _fmt_money(v: float) -> str:
    return f"{v:,.0f}".replace(",", " ")


def _short_addr(a: Any, n: int = 24) -> str:
    """Короткий адрес для одного <pre>-блока списка аванса: отбрасываем «г. Город,»
    и режем до n символов (…). Полный адрес остаётся на кнопке выбора объекта."""
    s = str(a or "—").strip()
    if s[:2] in ("г.", "г "):
        if "," in s:
            s = s.split(",", 1)[1].strip()
        else:
            parts = s.split(None, 1)
            s = parts[1].strip() if len(parts) > 1 else s
    s = s.strip(" ,")
    return (s[: n - 1] + "…") if len(s) > n else (s or "—")


async def _build_advance_rows(db: Database, installer_id: int) -> list[dict[str, Any]]:
    """Список объектов в работе монтажника с расчётом доступного к авансу.

    User-уточнение 2026-05-19: авансы считаются от БАЗОВОЙ цены установки
    (×0.67 для б.н., ×0.95 для кредита). Итоговая ЗП = база + 10% (для б.н.),
    база (для кредита). В UI показываются обе цифры.
    """
    invoices = await db.list_installer_confirmed_invoices(installer_id)
    rows: list[dict[str, Any]] = []
    for inv in invoices:
        zp_st = (inv.get("zp_installer_status") or "not_requested")
        if zp_st in ("payment_sent", "confirmed"):
            continue
        # ТЗ 2026-06-04: аванс по счёту — только если согласована стоимость монтажа
        # (этап «в работе» уже гарантирован list_installer_confirmed_invoices).
        if float(inv.get("montazh_agreed_amount") or 0) <= 0:
            continue
        plan_base = _calc_est_montazh_base(inv)   # без 10%
        plan_total = _calc_est_montazh(inv)        # с 10% (для б.н.)
        if plan_base <= 0:
            continue
        taken = await db.get_advance_taken_for_invoice(inv["id"])
        available = max(0.0, plan_base - taken)
        if available < 1:
            continue
        rows.append({
            "invoice_id": inv["id"],
            "address": inv.get("object_address") or "—",
            "plan_base": plan_base,
            "plan_total": plan_total,
            "is_credit": _is_credit(inv),
            "taken": taken,
            "available": available,
            "deadline": (inv.get("deadline_end_date") or "")[:10],
        })
    return rows


def _render_advance_list(
    rows: list[dict[str, Any]], cart: dict[int, float],
) -> tuple[str, Any]:
    """Текст карточки + клавиатура для Шага 1 (список + корзина)."""
    # Эталонный <pre> (user 10.06): ВСЕ объекты в ОДНОМ <pre>-блоке (user: «в 1 блок»),
    # прежний плоский inline с «:»/«|» убран, числа справа моноширинно. Адрес в шапке
    # объекта укорочен под ширину блока (город отброшен, …); полный адрес — на кнопке.
    # Кредит-метка «🏦 КРЕДИТ» — ОТДЕЛЬНОЙ строкой под адресом (user 10.06), чтобы
    # адрес не резался жёстко; показ всем кроме бухгалтерии
    # (feedback_credit_filter_accounting_only). Поля/расчёты те же.
    W = 28

    def _mrow(label: str, value: str) -> str:
        used = 3 + len(label) + len(value)
        return f"   {label}{' ' * max(1, W - used)}{value}"

    body: list[str] = []
    total_base = 0.0
    for r in rows:
        cred = bool(r.get("is_credit"))
        addr = _short_addr(r["address"], 24)
        body.append(f"📍 {addr}")
        if cred:
            body.append("   🏦 КРЕДИТ")
        body.append(_mrow("Монтаж база", _fmt_money(r["plan_base"])))
        if not cred:
            body.append(_mrow("+10% итог", _fmt_money(r["plan_total"])))
        if r["deadline"]:
            body.append(_mrow("Срок", r["deadline"]))
        if r["taken"]:
            body.append(_mrow("Взято", _fmt_money(r["taken"])))
        body.append("")
        total_base += r["plan_base"]
    cart_total = sum(cart.values())
    body.append("━" * W)
    body.append("💼 Итого")
    body.append(_mrow("База к авансу", _fmt_money(total_base)))
    body.append(_mrow("Выбрано", _fmt_money(cart_total)))
    body_str = "\n".join(body)
    text = (
        "💸 <b>Запрос аванса</b>\n"
        "<i>Объекты в работе (ЗП ещё не выплачена). Авансы от базы; "
        "+10% бонус доплачивается отдельно.</i>\n"
        f"<pre>{body_str}</pre>"
    )

    b = InlineKeyboardBuilder()
    for r in rows:
        sel = cart.get(r["invoice_id"], 0)
        if sel > 0:
            label = (
                f"✅ {r['address']} — {_fmt_money(sel)} ₽ "
                f"(из {_fmt_money(r['available'])})"
            )
        else:
            label = f"📍 {r['address']} — {_fmt_money(r['available'])} ₽"
        b.button(text=label, callback_data=f"advance_pick:{r['invoice_id']}")
    if cart_total > 0:
        b.button(text=f"📤 Отправить ({_fmt_money(cart_total)} ₽)", callback_data="advance_send")
    b.button(text="❌ Отмена", callback_data="advance_cancel")
    b.adjust(1)
    return text, b.as_markup()


@router.callback_query(F.data == "inst_zp_classic")
async def inst_zp_classic(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Для whitelist-монтажника: повторить классический flow «Запрос ЗП»."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    user_id = cb.from_user.id  # type: ignore[union-attr]
    await state.clear()
    # Инициализация (если нужна) — обычная message-обёртка не подходит, поэтому
    # для whitelist пропускаем init: считаем, что Игорь уже инициализирован.
    if not await db.is_installer_zp_initialized(user_id):
        await db.set_installer_zp_initialized(user_id)
    # Дальше — классический список карточек (тот же SQL, что в installer_zp_start).
    cur = await db.conn.execute(
        "SELECT * FROM invoices "
        "WHERE ("
        "  montazh_stage IN ('in_work', 'razmery_ok', 'invoice_ok') "
        "  OR zp_installer_status = 'approved'"
        ") "
        "  AND status IN ('in_progress', 'paid', 'ended', 'credit') "
        "  AND parent_invoice_id IS NULL "
        "  AND (zp_installer_status IS NULL OR zp_installer_status != 'not_applicable') "
        "ORDER BY id DESC LIMIT 30",
    )
    invoices = [dict(r) for r in await cur.fetchall()]
    if not invoices:
        await cb.message.answer("📭 Нет счетов.")  # type: ignore[union-attr]
        return
    not_req = [i for i in invoices if (i.get("zp_installer_status") or "not_requested") == "not_requested"]
    requested = [i for i in invoices if i.get("zp_installer_status") == "requested"]
    approved = [i for i in invoices if i.get("zp_installer_status") == "approved"]
    sum_approved = sum(float(i.get("zp_installer_amount") or 0) for i in approved)
    await cb.message.answer(  # type: ignore[union-attr]
        _build_inst_zp_summary(invoices, not_req, requested, approved, sum_approved)
    )
    for inv in invoices:
        zp_st = inv.get("zp_installer_status") or "not_requested"
        zp_icon = {"not_requested": "❌", "requested": "⏳", "approved": "✅"}.get(zp_st, "❌")
        zp_label = {
            "not_requested": "Не запрошена", "requested": "На проверке",
            "approved": "Одобрена", "payment_sent": "Отправлена", "confirmed": "Оплачена",
        }.get(zp_st, "—")
        mgr, lead_name, _lead_phone, num = _inst_card_header(inv)
        est_base = _calc_est_montazh_base(inv)   # ×0.67 для б.н., ×0.95 для кредита
        est_total = _calc_est_montazh(inv)        # +10% (б.н.) / база (кредит)
        zp_amount = inv.get("zp_installer_amount")
        kb = InlineKeyboardBuilder()
        if zp_st == "not_requested":
            kb.button(text="✏️ Изменить стоимость", callback_data=f"instzpadj:start:{inv['id']}")
            kb.button(text="💰 Запросить ЗП", callback_data=f"instzpadj:start:{inv['id']}")
        elif _can_edit_zp_amount(inv):
            # Часть 2 (2.3): повторная правка суммы на этапе «Счет End».
            kb.button(text="✏️ Изменить сумму", callback_data=f"instzpadj:start:{inv['id']}")
        kb.button(text="⬅️ Назад", callback_data="inst_nav:home")
        kb.adjust(1)
        text = (
            f"{zp_icon} <b>№{num}</b> · {zp_label}\n"
            f"<pre>\n"
            f"{'Менеджер':16s} {mgr}\n"
            f"{'Адрес':16s} {inv.get('object_address', '—')}\n"
        )
        # User-уточнение 2026-05-19: для Игоря всегда показывать обе цифры
        # (база и +10% итог); для кредита 10% не прибавляется — показываем одну.
        if est_total:
            if _is_credit(inv):
                text += f"{'Монтаж база':16s} {est_base:>10,}₽ (кредит)\n"
            else:
                text += f"{'Монтаж база':16s} {est_base:>10,}₽\n"
                text += f"{'+10% итог':16s} {est_total:>10,}₽\n"
        # «Долг по счету» = лист Invoices AE (sheets.py cells[30] = O−P−Z−AC):
        # Сумма − Сумма 1пл − Сумма допл − Оконч допл = amount − first_payment_amount
        # − surcharge_amount − final_surcharge_amount. Read-only из БД, формула 1:1 с
        # листом (user 10.06). Показываем всегда (0₽ = долга нет).
        _debt = (
            float(inv.get("amount") or 0)
            - float(inv.get("first_payment_amount") or 0)
            - float(inv.get("surcharge_amount") or 0)
            - float(inv.get("final_surcharge_amount") or 0)
        )
        text += f"{'Долг по счету':16s} {_debt:>10,.0f}₽\n"
        if zp_amount and zp_st in ("requested", "approved", "payment_sent", "confirmed"):
            try:
                text += f"{'ЗП':16s} {float(zp_amount):>10,.0f}₽\n"
            except (ValueError, TypeError):
                pass
        text += f"{'ЗП статус':16s} {zp_label}\n</pre>"
        await cb.message.answer(text, reply_markup=kb.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data == "advance_start")
async def advance_start_handler(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Шаг 1: показать список объектов с авансовой корзиной."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    # Витрина «Аванс — история» вверху раздела аванса (user 09.06 веч.: «это ведь аванс»).
    # Read-only; защищена try — её сбой не должен ломать поток запроса аванса.
    try:
        await cb.message.answer(await build_installer_advance_card(db, user_id))  # type: ignore[union-attr]
    except Exception:
        log.exception("advance_start_handler: build_installer_advance_card failed")
    rows = await _build_advance_rows(db, user_id)
    if not rows:
        await cb.message.answer(  # type: ignore[union-attr]
            "📭 Нет объектов в работе для авансирования.\n"
            "(нужен счёт со статусом монтажа in_work/razmery_ok/invoice_ok "
            "и план ЗП > 0)",
        )
        return
    await state.set_state(AdvanceRequestSG.list_invoices)
    await state.update_data(cart={}, rows=rows)
    text, kb = _render_advance_list(rows, {})
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("advance_pick:"), AdvanceRequestSG.list_invoices)
async def advance_pick_handler(cb: CallbackQuery, state: FSMContext) -> None:
    """Шаг 2: выбран счёт → ввод суммы."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    data = await state.get_data()
    rows = data.get("rows") or []
    row = next((r for r in rows if r["invoice_id"] == inv_id), None)
    if not row:
        await cb.message.answer("❌ Счёт не найден в текущей сессии.")  # type: ignore[union-attr]
        return
    available = float(row["available"])
    await state.update_data(picking_inv_id=inv_id)
    await state.set_state(AdvanceRequestSG.enter_amount)
    cred = bool(row.get("is_credit"))
    # Эталонный <pre> (user 10.06): прежний плоский вид с тире-разделителями убран.
    # Доступно — в footer (отделён разделителем). Поля/лимиты те же.
    items: list[tuple[str, str]] = [("Монтаж база", _fmt_money(row["plan_base"]))]
    if not cred:
        items.append(("+10% итог", _fmt_money(row["plan_total"])))
    items.append(("Срок", row["deadline"] or "—"))
    items.append(("Уже взято", _fmt_money(row["taken"])))
    card = format_card_section(
        emoji="📍",
        title=f"{row['address']}{' 🏦 КРЕДИТ' if cred else ''}",
        items=items,
        footer=("Доступно", _fmt_money(available)),
        width=26,
    )
    text = (
        f"{card}\n"
        "<i>(аванс лимит = база, бонус +10% выплачивается отдельно)</i>\n\n"
        f"Введите сумму аванса (число, ≤ {_fmt_money(available)} ₽):"
    )
    b = InlineKeyboardBuilder()
    b.button(text=f"💰 Взять всё ({_fmt_money(available)} ₽)",
             callback_data=f"advance_take_all:{inv_id}")
    b.button(text="◀️ К списку", callback_data="advance_back_list")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("advance_take_all:"), AdvanceRequestSG.enter_amount)
async def advance_take_all_handler(cb: CallbackQuery, state: FSMContext) -> None:
    """Взять весь доступный лимит по выбранному счёту."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    data = await state.get_data()
    rows = data.get("rows") or []
    row = next((r for r in rows if r["invoice_id"] == inv_id), None)
    if not row:
        return
    cart = dict(data.get("cart") or {})
    cart[inv_id] = float(row["available"])
    await state.update_data(cart=cart)
    await state.set_state(AdvanceRequestSG.list_invoices)
    text, kb = _render_advance_list(rows, cart)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.message(AdvanceRequestSG.enter_amount, F.text)
async def advance_amount_input(message: Message, state: FSMContext) -> None:
    """Обработка ручного ввода суммы."""
    raw = (message.text or "").replace(",", ".").replace(" ", "").strip()
    try:
        amount = float(raw)
    except ValueError:
        await message.answer("Введите число (например 15000).")
        return
    data = await state.get_data()
    inv_id = data.get("picking_inv_id")
    rows = data.get("rows") or []
    row = next((r for r in rows if r["invoice_id"] == inv_id), None)
    if not row:
        await message.answer("❌ Сессия выбора потеряна — начните заново.")
        await state.clear()
        return
    if amount <= 0 or amount > float(row["available"]):
        await message.answer(
            f"Сумма должна быть от 1 до {_fmt_money(row['available'])} ₽.",
        )
        return
    cart = dict(data.get("cart") or {})
    cart[inv_id] = amount
    await state.update_data(cart=cart)
    await state.set_state(AdvanceRequestSG.list_invoices)
    text, kb = _render_advance_list(rows, cart)
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "advance_back_list", AdvanceRequestSG.enter_amount)
async def advance_back_list_handler(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    data = await state.get_data()
    rows = data.get("rows") or []
    cart = data.get("cart") or {}
    await state.set_state(AdvanceRequestSG.list_invoices)
    text, kb = _render_advance_list(rows, cart)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data == "advance_cancel")
async def advance_cancel_handler(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.clear()
    await cb.message.answer("❌ Запрос аванса отменён.")  # type: ignore[union-attr]


@router.callback_query(F.data == "advance_send", AdvanceRequestSG.list_invoices)
async def advance_send_handler(
    cb: CallbackQuery, state: FSMContext, db: Database,
    config: Config, notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Финал: создать advance_request, notify ГД."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    user_id = cb.from_user.id  # type: ignore[union-attr]
    data = await state.get_data()
    cart = data.get("cart") or {}
    rows = data.get("rows") or []
    if not cart:
        await cb.answer("Корзина пуста — выберите хотя бы один счёт.", show_alert=True)
        return
    await cb.answer()
    items: list[tuple[int, float, float]] = []
    for inv_id, amount in cart.items():
        row = next((r for r in rows if r["invoice_id"] == inv_id), None)
        if row:
            # plan_zp_snapshot = база (без +10%) — по ней рассчитываются лимиты и offset.
            items.append((int(inv_id), float(amount), float(row["plan_base"])))
    if not items:
        await cb.message.answer("❌ Пустая корзина.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id = await db.create_advance_request(user_id, items, comment=None)
    except ValueError as e:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⛔ Нельзя оформить аванс: {e}.\n\n"
            f"Сначала возьмите счёт в работу и согласуйте стоимость монтажа.",
        )
        await state.clear()
        return
    # ТЗ 2026-05-19 блок C: sync advance metrics в Invoices + общий лист.
    for inv_id, _, _ in items:
        try:
            await integrations.sync_invoice_row(int(inv_id))
        except Exception as e:
            log.warning("sync_invoice_row after advance create failed: %s", e)
    await integrations.sync_advances_journal()
    await state.clear()
    total = sum(a for _, a, _ in items)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос аванса #{req_id} отправлен ГД.\n"
        f"Сумма: {_fmt_money(total)} ₽ ({len(items)} объект(ов))",
    )
    # Notify ГД
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        items_full = await db.get_advance_request_items(req_id)
        initiator = await get_initiator_label(db, user_id)
        lines = [
            f"💸 <b>Запрос аванса монтажника</b>",
            f"👤 От: {initiator}",
            f"💰 Сумма: <b>{_fmt_money(total)} ₽</b>",
            "",
            "<b>По объектам:</b>",
        ]
        for it in items_full:
            lines.append(
                f"  📍 {it.get('object_address') or '—'} (№{it.get('invoice_number') or '?'}): "
                f"{_fmt_money(float(it['amount']))} ₽ "
                f"(план {_fmt_money(float(it.get('plan_zp_snapshot') or 0))} ₽)"
            )
        b = InlineKeyboardBuilder()
        b.button(text="✅ Одобрить", callback_data=f"gd_adv_appr:{req_id}")
        b.button(text="❌ Отклонить", callback_data=f"gd_adv_rej:{req_id}")
        b.adjust(2)
        await notifier.safe_send(int(gd_id), "\n".join(lines), reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))


@router.callback_query(F.data == "advance_balance")
async def advance_balance_handler(cb: CallbackQuery, db: Database) -> None:
    """Карточка «💼 Мой баланс» — эталон-история аванса + опции (этап-2, ТЗ 03.06).

    Полная история движений кошелька аванса (приход от ГД / зачёты по счетам) +
    «Баланс Итого» + блок «Ожидаемая ЗП» (остаток по счетам = agreed − применённое).
    Рендер — utils.build_advance_history_card (read-only витрина, в Invoices/лист
    не пишет; кредит-признак монтажнику не показывается). Кнопка ниже карточки —
    применить аванс к счёту (вход в advance_distribute_start).
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    # «💼 Мой баланс» = ПОЛНАЯ витрина «Аванс — история» (build_installer_advance_card)
    # + кнопки (Наполнить/Распределить) + «◀️ Назад». User 09.06: карточка должна быть
    # здесь, где монтажник смотрит баланс. Read-only; edit_text — не плодит сообщения.
    balance = await db.get_advance_balance(user_id)
    b = InlineKeyboardBuilder()
    # Наполнение аванса из ЗП ОТКЛЮЧЕНО 2026-06-30: кошелёк наполняет только ГД.
    # Монтажнику кошелёк — только расход в счёт ЗП («📋 Распределить аванс»).
    if balance > 0:
        b.button(text=f"📋 Распределить аванс ({_fmt_money(balance)} ₽)",
                 callback_data="advance_distribute_start")
    # «Обновлять, не плодить» (user 09.06): повторное нажатие «💼 Мой баланс»
    # ОБНОВЛЯЕТ это же сообщение (edit_text), а не шлёт новое — иначе при повторных
    # нажатиях карточки баланса задваивались. «◀️ Назад» возвращает в подменю «Запрос ЗП».
    b.button(text="◀️ Назад", callback_data="inst_zp:menu")
    b.adjust(1)
    try:
        text = await build_installer_advance_card(db, user_id)
    except Exception:
        log.exception("advance_balance_handler: build_installer_advance_card failed")
        text = f"💼 <b>Мой баланс аванса:</b> {_fmt_money(balance)} ₽"
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# =====================================================================
# Наполнение аванса монтажника согласованной ЗП по счетам (зеркало РП-потока).
# Монтажник забирает монтажную ЗП (BJ) по выбранным счетам в кошелёк аванса:
# счёт → zp_installer_status='confirmed' (→ «Монтаж Факт»=agreed, ветка-3), Σ → ОДИН
# topup кошелька. Без одобрения ГД (своя ЗП), инфо-уведомление ГД. ТЗ 04.06.
# =====================================================================


def _advfill_addr(addr: Any, width: int = 22) -> str:
    """Короткий адрес объекта для кнопки/строки (обрезка с …)."""
    s = (str(addr).strip() if addr else "") or "—"
    return s if len(s) <= width else s[: width - 1] + "…"


async def _inst_advfill_edit_or_send(cb: CallbackQuery, text: str, markup: Any = None) -> None:
    """edit_text текущего сообщения; при ошибке (не изменено / нет прав) — новое."""
    try:
        await cb.message.edit_text(text, reply_markup=markup)  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=markup)  # type: ignore[union-attr]


def _render_inst_adv_fill(
    invoices: list[dict[str, Any]], selected: set[int],
) -> tuple[str, Any]:
    """Экран наполнения аванса монтажника: счета с невыплаченной ЗП (toggle-чекбоксы)."""
    sel_total = sum(float(i.get("agreed") or 0) for i in invoices if int(i["id"]) in selected)
    avail_total = sum(float(i.get("agreed") or 0) for i in invoices)
    lines = [f"<pre>💵 <b>Наполнить аванс</b>"]
    lines.append(f"   Доступно ЗП          {_fmt_money(avail_total):>10s} ₽")
    lines.append(f"   Выбрано счетов       {len(selected):>10d} / {len(invoices)}")
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_money(sel_total)} ₽</pre>")
    if invoices:
        lines.append("\nВыберите счета, по которым забрать монтажную ЗП в кошелёк аванса.")
        lines.append("По выбранным «Монтаж Факт» заполнится согласованной суммой.")
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = int(inv["id"])
        amount = float(inv.get("agreed") or 0)
        mark = "✅" if inv_id in selected else "▫️"
        b.button(
            text=f"{mark} {_advfill_addr(inv.get('object_address'))} — {_fmt_money(amount)}₽",
            callback_data=f"inst_advfill:pick:{inv_id}",
        )
    if invoices:
        if len(selected) < len(invoices):
            b.button(text="☑️ Выбрать все", callback_data="inst_advfill:all")
        else:
            b.button(text="⬜ Снять все", callback_data="inst_advfill:none")
        if selected:
            b.button(text=f"💵 Зачислить в аванс ({_fmt_money(sel_total)} ₽)",
                     callback_data="inst_advfill:credit")
    b.button(text="❌ Отмена", callback_data="inst_advfill:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


@router.callback_query(F.data == "inst_advfill:start")
async def inst_advfill_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """«💵 Наполнить аванс» → ОТКЛЮЧЕНО 2026-06-30 (кошелёк наполняет только ГД).

    Гард от устаревшей inline-кнопки: наполнение кошелька сотрудником запрещено,
    кошелёк используется только в счёт ЗП.
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "⛔ Наполнение аванса доступно только ГД.\n"
        "Авансовый кошелёк используется только в счёт ЗП.")
    return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    await state.clear()
    invoices = await db.list_installer_advance_fill_invoices(user_id)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "📭 Нет счетов с невыплаченной монтажной ЗП.\n\n"
            "Счёт появляется, когда ЗП-монтаж согласована, а «Монтаж Факт» ещё пуст."
        )
        return
    selected = {int(i["id"]) for i in invoices}  # по умолчанию выбраны все
    await state.set_state(InstallerAdvanceFillSG.list_invoices)
    await state.update_data(advfill_invoices=invoices, advfill_selected=list(selected))
    text, kb = _render_inst_adv_fill(invoices, selected)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("inst_advfill:pick:"), InstallerAdvanceFillSG.list_invoices)
async def inst_advfill_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Toggle одного счёта в выборке наполнения."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    invoices = data.get("advfill_invoices") or []
    selected = set(data.get("advfill_selected") or [])
    if inv_id in selected:
        selected.discard(inv_id)
    else:
        selected.add(inv_id)
    await state.update_data(advfill_selected=list(selected))
    text, kb = _render_inst_adv_fill(invoices, selected)
    await _inst_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "inst_advfill:all", InstallerAdvanceFillSG.list_invoices)
async def inst_advfill_all(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбрать все счета."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("advfill_invoices") or []
    selected = {int(i["id"]) for i in invoices}
    await state.update_data(advfill_selected=list(selected))
    text, kb = _render_inst_adv_fill(invoices, selected)
    await _inst_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "inst_advfill:none", InstallerAdvanceFillSG.list_invoices)
async def inst_advfill_none(cb: CallbackQuery, state: FSMContext) -> None:
    """Снять выбор со всех счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("advfill_invoices") or []
    await state.update_data(advfill_selected=[])
    text, kb = _render_inst_adv_fill(invoices, set())
    await _inst_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "inst_advfill:cancel")
async def inst_advfill_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена наполнения на любом шаге."""
    await cb.answer()
    await state.clear()
    await _inst_advfill_edit_or_send(cb, "❌ Наполнение аванса отменено.")


@router.callback_query(F.data == "inst_advfill:credit", InstallerAdvanceFillSG.list_invoices)
async def inst_advfill_credit_confirm(cb: CallbackQuery, state: FSMContext) -> None:
    """«💵 Зачислить» → подтверждение со списком выбранных счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("advfill_invoices") or []
    selected = set(data.get("advfill_selected") or [])
    if not selected:
        await cb.answer("Выберите хотя бы один счёт.", show_alert=True)
        return
    chosen = [i for i in invoices if int(i["id"]) in selected]
    total = sum(float(i.get("agreed") or 0) for i in chosen)
    lines = [f"<pre>💵 <b>Зачислить в аванс</b>"]
    for inv in chosen:
        lines.append(
            f"   {_advfill_addr(inv.get('object_address'), 18):<18s} "
            f"{_fmt_money(float(inv.get('agreed') or 0)):>10s} ₽"
        )
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_money(total)} ₽")
    lines.append("</pre>")
    lines.append(f"\nЗачислить монтажную ЗП по {len(chosen)} счёт(ам) в кошелёк аванса?")
    lines.append("По счетам «Монтаж Факт» заполнится согласованной суммой (ЗП считается выданной).")
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data="inst_advfill:confirm:yes")
    b.button(text="❌ Нет", callback_data="inst_advfill:confirm:no")
    b.adjust(2)
    await state.set_state(InstallerAdvanceFillSG.confirm)
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("inst_advfill:confirm:"), InstallerAdvanceFillSG.confirm)
@money_confirm_guard
async def inst_advfill_credit_apply(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
    config: Config,
) -> None:
    """Финал: credit_installer_zp_to_advance (атомарно) + sync + инфо ГД.

    ОТКЛЮЧЕНО 2026-06-30: наполнение кошелька — только ГД. Гард от устаревшего
    FSM-состояния/кнопки «✅ Да» (денежная запись не выполняется).
    """
    await state.clear()
    await cb.answer("⛔ Наполнение аванса отключено (только ГД).", show_alert=True)
    return
    answer = cb.data.split(":")[-1] if cb.data else "no"  # type: ignore[union-attr]
    if answer != "yes":
        await state.clear()
        await _inst_advfill_edit_or_send(cb, "❌ Отменено.")
        await cb.answer()
        return
    data = await state.get_data()
    selected = list({int(x) for x in (data.get("advfill_selected") or [])})
    if not selected:
        await cb.answer("Список пуст", show_alert=True)
        await state.clear()
        return
    user_id = cb.from_user.id  # type: ignore[union-attr]
    try:
        req_id, total, credited = await db.credit_installer_zp_to_advance(user_id, selected)
    except Exception:
        log.exception("inst_advfill_credit_apply: credit failed user_id=%s", user_id)
        await state.clear()
        await cb.answer("Ошибка зачисления, попробуйте позже.", show_alert=True)
        await cb.message.answer("❌ Не удалось зачислить в аванс. Попробуйте позже.")  # type: ignore[union-attr]
        return
    for c in credited:
        try:
            await integrations.sync_invoice_row(int(c["invoice_id"]))
        except Exception:
            log.exception("inst_advfill_credit_apply: sync_invoice_row failed inv=%s", c.get("invoice_id"))
    try:
        await integrations.sync_advances_journal()
    except Exception:
        log.warning("inst_advfill_credit_apply: sync_advances_journal failed")
    await state.clear()
    if not credited:
        await _inst_advfill_edit_or_send(cb, "ℹ️ Выбранные счета уже выплачены ранее — изменений нет.")
        await cb.answer()
        return
    try:
        new_balance = await db.get_advance_balance(user_id)
    except Exception:
        new_balance = 0.0
    lines = [f"<pre>✅ <b>Зачислено в аванс</b>"]
    for c in credited:
        lines.append(f"   №{str(c['invoice_number']):<18s} {_fmt_money(float(c['amount'])):>10s} ₽")
    lines.append(f"   Баланс аванса        {_fmt_money(new_balance):>10s} ₽")
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_fmt_money(total)} ₽</pre>")
    await _inst_advfill_edit_or_send(cb, "\n".join(lines))
    await cb.answer("Зачислено")
    # Инфо-уведомление ГД (использование кошелька аванса)
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        inst_user = await db.get_user_optional(user_id)
        inst_label = getattr(inst_user, "full_name", None) or "Монтажник"
        gd_lines = [f"<pre>ℹ️ <b>Монтажник пополнил аванс</b>"]
        gd_lines.append(f"   Кто                  {inst_label}")
        gd_lines.append(f"   Источник             монтажная ЗП · {len(credited)} счёт(ов)")
        gd_lines.append(f"   Баланс аванса        {_fmt_money(new_balance):>10s} ₽")
        gd_lines.append(f"   ━━━━━━━━━━━━━━━━")
        gd_lines.append(f"   Итого  {_fmt_money(total)} ₽</pre>")
        try:
            await notifier.safe_send(int(gd_id), "\n".join(gd_lines))
        except Exception:
            log.exception("inst_advfill_credit_apply: notify ГД failed")


# =====================================================================
# Депозит 04.06: подменю-карточка «💳 Депозит» (история + опции).
# Двухшаговый запрос ГД из депозита (прочтение→исполнение+вложение) — обработка в gd.py.
# =====================================================================


async def _send_zp_submenu(message: Message, db: Database, user_id: int, *, edit: bool = False) -> None:
    """Подменю «💰 Запрос ЗП» (whitelist): ЗП/аванс/баланс + «💳 Депозит» (бейдж 🔴).

    Карточка-витрина «Аванс — история» (build_installer_advance_card) перенесена
    на кнопку «💸 Запрос аванса» (advance_start_handler) — user 09.06 веч.
    """
    advance_balance = await db.get_advance_balance(user_id)
    deposit_balance = await db.get_deposit_balance(user_id)
    b = InlineKeyboardBuilder()
    b.button(text="📋 Запросить ЗП за счёт", callback_data="inst_zp_classic")
    b.button(text="💸 Запрос аванса", callback_data="advance_start")
    b.button(text="💼 Мой баланс", callback_data="advance_balance")
    b.button(text=f"💰 Распределить аванс ({_fmt_money(advance_balance)} ₽)",
             callback_data="advance_distribute_start")
    # Депозит консолидирован под кнопкой «💳 Депозит»: карточка истории + опции
    # (Расход / Депо→Аванс / открытые запросы ГД). Бейдж 🔴 = открытые запросы ГД.
    _depo_badge = await db.count_installer_deposit_tasks(user_id)
    _depo_label = f"💳 Депозит ({_fmt_money(deposit_balance)} ₽)"
    if _depo_badge > 0:
        _depo_label += f" 🔴{_depo_badge}"
    b.button(text=_depo_label, callback_data="inst_depo:card")
    b.adjust(1)
    # Строка «Аванс: N ₽» убрана из шапки (user 09.06) — баланс аванса теперь
    # показывает карточка-витрина выше; дубль-число в меню убирает путаницу.
    # Карточка «ЗП к запросу — Счёт ОК» показывается СРАЗУ при входе в «Запрос ЗП»
    # (ТЗ user 09.06): счета на этапе «Счёт ОК» (AZ=invoice_ok) с остатком BJ. Read-only.
    try:
        card = await build_installer_zp_invoiceok_card(db, user_id)
    except Exception:
        log.exception("_send_zp_submenu: build_installer_zp_invoiceok_card failed")
        card = "💰 <b>Запрос ЗП</b>"
    header = (
        f"{card}"
        f"\n💸 Депозит: <b>{_fmt_money(deposit_balance)} ₽</b>"
    )
    # edit=True (возврат из «Мой баланс» по «◀️ Назад») — обновляем то же сообщение,
    # чтобы не плодить меню; фолбэк на answer, если редактировать нельзя.
    if edit:
        try:
            await message.edit_text(header, reply_markup=b.as_markup())
            return
        except Exception:
            pass
    await message.answer(header, reply_markup=b.as_markup())


@router.callback_query(F.data == "inst_zp:menu")
async def inst_zp_menu_back(cb: CallbackQuery, db: Database) -> None:
    """◀️ Назад из «💼 Мой баланс» → подменю «Запрос ЗП» (edit — не плодит сообщения)."""
    await cb.answer()
    if not cb.from_user:
        return
    await _send_zp_submenu(cb.message, db, cb.from_user.id, edit=True)  # type: ignore[arg-type]


@router.callback_query(F.data == "inst_depo:back")
async def inst_depo_back(cb: CallbackQuery, db: Database) -> None:
    """⬅️ Назад из карточки депозита → подменю «Запрос ЗП»."""
    await cb.answer()
    if not cb.from_user:
        return
    await _send_zp_submenu(cb.message, db, cb.from_user.id)  # type: ignore[arg-type]


@router.callback_query(F.data == "inst_depo:card")
async def inst_depo_card_handler(cb: CallbackQuery, db: Database) -> None:
    """Кнопка «💳 Депозит» → эталон-карточка истории депозита + опции + запросы ГД.

    Read-only витрина (build_deposit_history_card); опции — существующие FSM
    (Расход депозита / Депо→Аванс). Открытые запросы ГД (gd_deposit_request) —
    отдельными кнопками → двухшаговое подтверждение (обработка в gd.py).
    """
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    text = await build_deposit_history_card(db, user_id)
    deposit_balance = await db.get_deposit_balance(user_id)
    b = InlineKeyboardBuilder()
    b.button(text=f"💸 Расход депозита ({_fmt_money(deposit_balance)} ₽)",
             callback_data="inst_withdraw:start")
    b.button(text=f"↔️ Депо → Аванс ({_fmt_money(deposit_balance)} ₽)",
             callback_data="inst_depo_to_adv:start")
    # Открытые запросы ГД из депозита — кнопка на каждый (двухшаговое подтверждение).
    try:
        tasks = await db.list_installer_deposit_tasks(user_id)
    except Exception:
        tasks = []
    for t in tasks:
        pl = try_json_loads(t.get("payload_json"))
        amt = float(pl.get("amount") or 0)
        mark = "🆕 прочитать" if str(t.get("status")) == TaskStatus.OPEN else "⏳ исполнить"
        b.button(text=f"📥 Запрос ГД #{t['id']} · {_fmt_money(amt)} ₽ — {mark}",
                 callback_data=f"inst_depo_req:open:{t['id']}")
    b.button(text="⬅️ Назад", callback_data="inst_depo:back")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# ---------------------------------------------------------------------
# Двухшаговый запрос ГД из депозита: прочтение → исполнение (+вложение).
# Списание (db.create_gd_deposit_withdrawal) — ТОЛЬКО на шаге исполнения.
# ---------------------------------------------------------------------


def _depo_req_text(payload: dict, status: str) -> str:
    """Текст карточки запроса ГД из депозита (по текущему шагу)."""
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    has_file = bool(payload.get("gd_file_id"))
    step = {
        TaskStatus.OPEN: "Шаг 1/2 — подтвердите прочтение.",
        TaskStatus.IN_PROGRESS: "Шаг 2/2 — подтвердите исполнение (можно приложить чек).",
    }.get(status, "")
    file_line = "\n📎 <i>ГД приложил файл к запросу</i>" if has_file else ""
    card = format_card_section(
        "📥", "Запрос ГД из вашего депозита",
        items=[
            ("Сумма", _fmt_money(amount)),
            ("Назначение", html.escape(purpose)),
        ],
    )
    return f"{card}{file_line}\n<i>{step}</i>"


def _depo_req_step_kb(task_id: int, status: str):
    """Кнопки по шагу: OPEN→прочтение, IN_PROGRESS→исполнение."""
    b = InlineKeyboardBuilder()
    if status == TaskStatus.OPEN:
        b.button(text="✅ Подтвердить прочтение", callback_data=f"inst_depo_req:read:{task_id}")
        b.button(text="❌ Отклонить", callback_data=f"inst_depo_req:reject:{task_id}")
    elif status == TaskStatus.IN_PROGRESS:
        b.button(text="✅ Подтвердить исполнение", callback_data=f"inst_depo_req:exec:{task_id}")
        b.button(text="❌ Отклонить", callback_data=f"inst_depo_req:reject:{task_id}")
    b.adjust(1)
    return b.as_markup()


async def _depo_req_load(cb: CallbackQuery, db: Database):
    """Загрузка+валидация задачи запроса (id из callback, проверка адресата)."""
    if not cb.from_user or not cb.data:
        return None
    try:
        task_id = int(cb.data.split(":")[-1])
    except ValueError:
        await cb.answer("Некорректный запрос.", show_alert=True)
        return None
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена.", show_alert=True)
        return None
    if int(task.get("assigned_to") or 0) != cb.from_user.id:
        await cb.answer("Эта задача не для вас.", show_alert=True)
        return None
    return task_id, task


@router.callback_query(F.data.startswith("inst_depo_req:open:"))
async def inst_depo_req_open(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Открыть запрос ГД из карточки «💳 Депозит» → кнопки текущего шага."""
    loaded = await _depo_req_load(cb, db)
    if not loaded:
        return
    task_id, task = loaded
    status = str(task.get("status"))
    if status not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        await cb.answer("Запрос уже закрыт.", show_alert=True)
        return
    await cb.answer()
    payload = try_json_loads(task.get("payload_json"))
    await cb.message.answer(  # type: ignore[union-attr]
        _depo_req_text(payload, status),
        reply_markup=_depo_req_step_kb(task_id, status),
    )
    # Повторно показать файл от ГД (если приложен при создании запроса).
    gd_file_id = payload.get("gd_file_id")
    gd_file_type = payload.get("gd_file_type")
    if gd_file_id and gd_file_type:
        try:
            await notifier.safe_send_media(
                cb.from_user.id, gd_file_type, gd_file_id,  # type: ignore[union-attr]
                caption=f"📎 Файл от ГД к запросу #{task_id}",
            )
        except Exception as e:
            log.warning("show GD depo-request file to employee failed: %s", e)


@router.callback_query(F.data.startswith("inst_depo_req:read:"))
async def inst_depo_req_read(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """Шаг 1: подтверждение ПРОЧТЕНИЯ → OPEN→IN_PROGRESS + notify ГД. Депозит не тронут."""
    loaded = await _depo_req_load(cb, db)
    if not loaded:
        return
    task_id, task = loaded
    if str(task.get("status")) != TaskStatus.OPEN:
        await cb.answer("Прочтение уже подтверждено.", show_alert=True)
        return
    claimed = await db.update_task_status(
        task_id, TaskStatus.IN_PROGRESS, expected_statuses=(TaskStatus.OPEN,),
    )
    if claimed is None:
        await cb.answer("Уже обработано.", show_alert=True)
        return
    try:
        await db.accept_task(task_id)
    except Exception:
        pass
    await cb.answer("✅ Прочтение подтверждено.")
    payload = try_json_loads(task.get("payload_json"))
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    gd_id = int(payload.get("gd_id") or task.get("created_by") or 0)
    name = payload.get("employee_name") or str(cb.from_user.id)  # type: ignore[union-attr]
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            _depo_req_text(payload, TaskStatus.IN_PROGRESS),
            reply_markup=_depo_req_step_kb(task_id, TaskStatus.IN_PROGRESS),
        )
    except Exception:
        pass
    if gd_id:
        _read_card = format_card_section(
            "👀", f"Запрос #{task_id} прочитан",
            items=[
                ("Сотрудник", html.escape(str(name))),
                ("Сумма", _fmt_money(amount)),
                ("Назначение", html.escape(purpose)),
            ],
        )
        await notifier.safe_send(
            gd_id,
            f"{_read_card}\n"
            f"<i>Принято к исполнению (списание ещё не произведено).</i>",
        )


@router.callback_query(F.data.startswith("inst_depo_req:exec:"))
async def inst_depo_req_exec(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Шаг 2 (старт): «Подтвердить исполнение» → опц. вложение (FSM)."""
    loaded = await _depo_req_load(cb, db)
    if not loaded:
        return
    task_id, task = loaded
    if str(task.get("status")) != TaskStatus.IN_PROGRESS:
        await cb.answer("Сначала подтвердите прочтение.", show_alert=True)
        return
    await cb.answer()
    await state.set_state(DepoReqExecuteSG.attach)
    await state.update_data(depo_req_task_id=task_id)
    b = InlineKeyboardBuilder()
    b.button(text="⏭️ Без вложения", callback_data="inst_depo_req:exec_nofile")
    b.button(text="❌ Отмена", callback_data="inst_depo_req:exec_cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        "📎 Приложите чек/фото исполнения (или нажмите «Без вложения»):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(DepoReqExecuteSG.attach, F.data == "inst_depo_req:exec_cancel")
async def inst_depo_req_exec_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ Отменено. Запрос остаётся открытым — подтвердите исполнение позже "
        "из «💰 Запрос ЗП» → «💳 Депозит».",
    )


@router.callback_query(DepoReqExecuteSG.attach, F.data == "inst_depo_req:exec_nofile")
async def inst_depo_req_exec_nofile(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    await cb.answer()
    await _depo_req_finalize(
        cb.message, state, db, config, notifier, integrations, None, None,  # type: ignore[arg-type]
    )


@router.message(DepoReqExecuteSG.attach)
async def inst_depo_req_exec_file(
    message: Message, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    file_id: str | None = None
    file_type: str | None = None
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    else:
        await message.answer(
            "Пришлите фото/документ, либо нажмите «Без вложения» под предыдущим сообщением.",
        )
        return
    await _depo_req_finalize(
        message, state, db, config, notifier, integrations, file_id, file_type,
    )


async def _depo_req_finalize(
    msg: Message, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
    receipt_file_id: str | None, receipt_file_type: str | None,
) -> None:
    """Финал шага 2: атомарно DONE + списание + notify ГД + пересылка файла.

    Анти-двойное-списание: update_task_status(DONE, expected IN_PROGRESS) — победитель
    один. При нехватке средств — откат задачи в IN_PROGRESS, депозит не тронут.
    """
    data = await state.get_data()
    task_id = int(data.get("depo_req_task_id") or 0)
    await state.clear()
    if not task_id:
        await msg.answer("❌ Сессия потеряна.")
        return
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await msg.answer("❌ Задача не найдена.")
        return
    payload = try_json_loads(task.get("payload_json"))
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    wallet_role = payload.get("wallet_role")
    gd_id = int(payload.get("gd_id") or task.get("created_by") or 0)
    emp_id = int(task.get("assigned_to") or 0)
    name = payload.get("employee_name") or str(emp_id)
    if amount <= 0:
        await msg.answer("❌ Некорректная сумма запроса.")
        return
    claimed = await db.update_task_status(
        task_id, TaskStatus.DONE, expected_statuses=(TaskStatus.IN_PROGRESS,),
    )
    if claimed is None:
        await msg.answer("Запрос уже обработан.")
        return
    try:
        await db.create_gd_deposit_withdrawal(
            employee_id=emp_id,
            amount=amount,
            comment=f"Запрос ГД: {purpose}",
            gd_id=gd_id,
            wallet_role=wallet_role,
            receipt_file_id=receipt_file_id,
        )
    except ValueError as e:
        await db.update_task_status(
            task_id, TaskStatus.IN_PROGRESS, expected_statuses=(TaskStatus.DONE,),
        )
        await msg.answer(f"⚠️ {e}")
        if gd_id:
            await notifier.safe_send(
                gd_id,
                f"⚠️ <b>Запрос #{task_id} не исполнен</b>\n"
                f"{html.escape(str(name))} подтвердил, но списание не прошло: {html.escape(str(e))}",
            )
        return
    new_depo = await db.get_deposit_balance(emp_id, wallet_role)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after depo_req execute: %s", e)
    _emp_done = format_card_section(
        "✅", "Исполнено",
        items=[
            ("Списано", _fmt_money(amount)),
            ("Назначение", html.escape(purpose)),
            ("Остаток депозита", _fmt_money(new_depo)),
        ],
    )
    await msg.answer(_emp_done)
    if gd_id:
        _gd_done = format_card_section(
            "✅", f"Запрос #{task_id} исполнен",
            items=[
                ("Сотрудник", html.escape(str(name))),
                ("Списано", _fmt_money(amount)),
                ("Назначение", html.escape(purpose)),
                ("Остаток депозита", _fmt_money(new_depo)),
            ],
        )
        await notifier.safe_send(gd_id, _gd_done)
        if receipt_file_id and receipt_file_type:
            try:
                await notifier.safe_send_media(
                    gd_id, receipt_file_type, receipt_file_id,
                    caption=f"Вложение исполнения запроса #{task_id}",
                )
            except Exception as e:
                log.warning("forward depo_req receipt to GD failed: %s", e)
    try:
        await refresh_recipient_keyboard(notifier, db, config, emp_id)
    except Exception:
        pass


@router.callback_query(F.data.startswith("inst_depo_req:reject:"))
async def inst_depo_req_reject(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Отклонение запроса ГД (с шага OPEN или IN_PROGRESS) → REJECTED, депозит не тронут."""
    loaded = await _depo_req_load(cb, db)
    if not loaded:
        return
    task_id, task = loaded
    if str(task.get("status")) not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    claimed = await db.update_task_status(
        task_id, TaskStatus.REJECTED,
        expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
    )
    if claimed is None:
        await cb.answer("Запрос уже обработан.", show_alert=True)
        return
    payload = try_json_loads(task.get("payload_json"))
    amount = float(payload.get("amount") or 0)
    purpose = str(payload.get("purpose") or "").strip()
    gd_id = int(payload.get("gd_id") or task.get("created_by") or 0)
    name = payload.get("employee_name") or str(cb.from_user.id)  # type: ignore[union-attr]
    await cb.answer("Отклонено.")
    try:
        _rej_card = format_card_section(
            "❌", "Запрос отклонён",
            items=[
                ("Сумма", _fmt_money(amount)),
                ("Назначение", html.escape(purpose)),
            ],
        )
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"{_rej_card}\n<i>Депозит не тронут.</i>",
        )
    except Exception:
        pass
    if gd_id:
        _gd_rej = format_card_section(
            "❌", f"Запрос #{task_id} отклонён сотрудником",
            items=[
                ("Сотрудник", html.escape(str(name))),
                ("Сумма", _fmt_money(amount)),
                ("Назначение", html.escape(purpose)),
            ],
        )
        await notifier.safe_send(
            gd_id,
            f"{_gd_rej}\n<i>Депозит не тронут.</i>",
        )
    try:
        await refresh_recipient_keyboard(notifier, db, config, cb.from_user.id)  # type: ignore[union-attr]
    except Exception:
        pass


# ============================================================================
# ТЗ 2026-05-20: РАСПРЕДЕЛЕНИЕ АВАНСА ПО ОБЪЕКТАМ (AdvanceDistributeSG)
# ============================================================================


async def _build_distribute_candidates(
    db: Database, installer_id: int,
) -> list[dict[str, Any]]:
    """Список счетов-кандидатов на зачёт open аванса монтажника.

    Включает:
      (1) approved-ZP (есть open advance item с этого invoice — спец-кнопка
          «Зачесть сразу»).
      (2) in_work / invoice_ok с активным монтажом — для распределения.
    """
    cur = await db.conn.execute(
        "SELECT id, invoice_number, object_address, is_credit, "
        "       montazh_stage, zp_installer_status, "
        "       COALESCE(zp_installer_amount, 0) AS zp_amt, "
        "       COALESCE(estimated_installation, 0) AS est_inst, "
        # Объединение платежей (owner 15.07): монтажнику причитается ДОПЛАТА —
        # Согласовано за вычетом выплаченного прошлым группам, иначе кнопка предложит
        # закрыть авансом всю объединённую сумму.
        "       COALESCE(montazh_agreed_amount, 0) "
        "         - COALESCE(montazh_paid_prev, 0) AS agreed, "
        "       status "
        "FROM invoices "
        "WHERE assigned_to = ? "
        "  AND status IN ('in_progress', 'paid', 'credit', 'ended') "
        "  AND zp_installer_status IN ('not_requested', 'requested', 'approved') "
        "ORDER BY "
        "  CASE WHEN zp_installer_status = 'approved' THEN 0 ELSE 1 END, "
        "  id DESC",
        (installer_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    open_items = await db.get_open_advance_items_for_installer(installer_id)
    open_by_invoice = {int(i["invoice_id"]): float(i["amount"]) for i in open_items}
    for r in rows:
        r["open_advance"] = open_by_invoice.get(r["id"], 0.0)
    return rows


@router.callback_query(F.data == "advance_distribute_start")
async def advance_distribute_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Карточка распределения аванса — список объектов и кнопок."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    outstanding = await db.get_installer_outstanding(user_id)
    if outstanding <= 0:
        await cb.message.answer("✅ Долгов по авансу нет.")  # type: ignore[union-attr]
        return
    unallocated = await db.get_advance_outstanding_unallocated(user_id)
    open_items = await db.get_open_advance_items_for_installer(user_id)
    candidates = await _build_distribute_candidates(db, user_id)

    lines = [
        "📋 <b>Распределение аванса</b>",
        "",
        f"💼 Текущий долг: <b>{_fmt_money(outstanding)} ₽</b>",
        f"🔒 Уже привязано к объектам: <b>{_fmt_money(outstanding - unallocated)} ₽</b>",
        f"🔓 Нераспределённый остаток: <b>{_fmt_money(unallocated)} ₽</b>",
    ]
    if open_items:
        lines.append("")
        lines.append("<b>🟡 Открытые привязки (ждут approve ZP):</b>")
        for it in open_items:
            num = it.get("invoice_number") or f"#{it['invoice_id']}"
            kr = " 🏦" if it.get("is_credit") else ""
            lines.append(f"  • {num}{kr} — {_fmt_money(float(it['amount']))} ₽")
    if not candidates:
        lines.append("")
        lines.append("⚠️ Нет действующих объектов для зачёта.")
        await cb.message.answer("\n".join(lines))  # type: ignore[union-attr]
        return

    lines.append("")
    lines.append("<b>Выберите объект для зачёта:</b>")
    lines.append("<i>🏁 — Счёт End (закрытый объект)</i>")
    b = InlineKeyboardBuilder()
    for inv in candidates:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        kr = "🏦" if inv["is_credit"] else "bn"
        # 🏁 для ended invoices (TZ tingly-twirling-whistle).
        end_marker = "🏁 " if inv.get("status") == "ended" else ""
        # approved-ZP → спец-кнопка «✅ Зачесть сразу X ₽»
        if inv["zp_installer_status"] == "approved" and inv["open_advance"] > 0:
            zp = float(inv["zp_amt"])
            b.button(
                text=f"✅ {end_marker}{num} {kr} • Зачесть ZP {_fmt_money(zp)} ₽",
                callback_data=f"adv_dist:offset_zp:{inv['id']}",
            )
        else:
            # in_work/invoice_ok → мгновенное применение аванса в счёт ЗП-монтаж.
            # ТЗ 2026-06-04: кандидат только если счёт взят В РАБОТУ И ЗП-монтаж согласована.
            agreed = float(inv.get("agreed") or 0)
            if agreed <= 0:
                continue
            if inv.get("montazh_stage") not in ("in_work", "razmery_ok", "invoice_ok"):
                continue
            b.button(
                text=f"📍 {end_marker}{num} {kr} • ЗП-монтаж {_fmt_money(agreed)} ₽",
                callback_data=f"adv_dist:pick:{inv['id']}",
            )
    b.button(text="❌ Закрыть", callback_data="adv_dist:cancel")
    b.adjust(1)
    await state.set_state(AdvanceDistributeSG.pick_invoice)
    await state.update_data(adv_dist_unallocated=unallocated)
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("adv_dist:offset_zp:"))
async def advance_distribute_offset_zp(
    cb: CallbackQuery, state: FSMContext, db: Database,
    notifier: Notifier, integrations: IntegrationHub, config: Config,
) -> None:
    """Approved-ZP → прямой offset (без ввода суммы)."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    user_id = cb.from_user.id  # type: ignore[union-attr]
    try:
        res = await db.offset_approved_zp_with_advance(invoice_id, actor_id=user_id)
    except Exception as e:
        await cb.message.answer(f"❌ Не удалось зачесть: {e}")  # type: ignore[union-attr]
        return
    await state.clear()
    inv = await db.get_invoice(invoice_id)
    num = inv.get("invoice_number") if inv else f"#{invoice_id}"
    await integrations.sync_invoice_row(invoice_id)
    await integrations.sync_advances_journal()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ <b>Зачтено</b>\n\n"
        f"Счёт №{num}: одобренная ЗП "
        f"<b>{_fmt_money(float(res['zp_amt']))} ₽</b> "
        f"переведена в зачёт аванса.\n"
        f"ZP-статус → confirmed (без физической выплаты)."
    )
    # Notify GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        _inst = await db.get_user_optional(user_id)
        _inst_name = getattr(_inst, "full_name", None) or user_id
        await notifier.safe_send(
            int(gd_id),
            f"💸 <b>Аванс: зачёт ZP</b>\n\n"
            f"Монтажник: {_inst_name}\n"
            f"Счёт: №{num}\n"
            f"Сумма ZP {_fmt_money(float(res['zp_amt']))} ₽ → "
            f"зачтена в погашение аванса (offset_amount={_fmt_money(float(res['offset_amount']))} ₽).\n"
            f"ZP-статус → confirmed, BT не пополнен."
        )


@router.callback_query(F.data.startswith("adv_dist:pick:"), AdvanceDistributeSG.pick_invoice)
async def advance_distribute_pick(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Выбран in_work/invoice_ok счёт → запросить сумму."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    # Объединение платежей (owner 15.07): монтажнику причитается ДОПЛАТА, а не вся
    # объединённая сумма; аванс прошлой группы уже внутри paid_prev — см. _advance_raw_cur.
    agreed = float(inv.get("montazh_agreed_amount") or 0) - float(
        inv.get("montazh_paid_prev") or 0)
    taken = await _advance_raw_cur(db, inv)
    remaining = agreed - taken
    if agreed <= 0 or remaining <= 0.001:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ По этому счёту ЗП-монтаж не согласована или уже закрыта авансом."
        )
        await state.clear()
        return
    data = await state.get_data()
    unallocated = float(data.get("adv_dist_unallocated") or 0)
    limit = min(unallocated, remaining)
    await state.update_data(adv_dist_invoice_id=invoice_id, adv_dist_remaining=remaining)
    await state.set_state(AdvanceDistributeSG.enter_amount)
    num = inv.get("invoice_number") or f"#{invoice_id}"
    await cb.message.answer(  # type: ignore[union-attr]
        f"📍 <b>{num}</b>\n"
        f"Согласованная ЗП-монтаж: <b>{_fmt_money(agreed)} ₽</b>\n"
        f"Остаток к закрытию: <b>{_fmt_money(remaining)} ₽</b>\n"
        f"Доступно аванса: <b>{_fmt_money(limit)} ₽</b>\n\n"
        f"Введите сумму к применению (₽):"
    )


@router.message(AdvanceDistributeSG.enter_amount)
async def advance_distribute_amount(
    message: Message, state: FSMContext, db: Database,
    notifier: Notifier, integrations: IntegrationHub, config: Config,
) -> None:
    """Ввод суммы → создаёт advance_item на выбранный счёт."""
    if not message.from_user:
        return
    raw = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        val = float(raw)
        if val <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число > 0:")
        return
    data = await state.get_data()
    invoice_id = int(data.get("adv_dist_invoice_id") or 0)
    remaining = float(data.get("adv_dist_remaining") or 0)
    unallocated = float(data.get("adv_dist_unallocated") or 0)
    limit = min(unallocated, remaining)
    if val > limit + 0.001:
        await message.answer(
            f"⚠️ Максимум {_fmt_money(limit)} ₽ "
            f"(остаток ЗП {_fmt_money(remaining)} ₽, доступно аванса {_fmt_money(unallocated)} ₽)."
        )
        return
    user_id = message.from_user.id
    try:
        res = await db.apply_advance_to_invoice_now(
            installer_id=user_id, invoice_id=invoice_id, amount=val, actor_id=user_id,
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось применить: {e}")
        return
    await state.clear()
    inv = await db.get_invoice(invoice_id)
    num = inv.get("invoice_number") if inv else f"#{invoice_id}"
    await integrations.sync_invoice_row(invoice_id)
    await integrations.sync_advances_journal()
    _rem = float(res.get("remaining") or 0)
    _tail = (
        "✅ ЗП по счёту закрыта авансом полностью."
        if res.get("full_closed")
        else f"Остаток ЗП по счёту: <b>{_fmt_money(_rem)} ₽</b>."
    )
    await message.answer(
        f"✅ Применено <b>{_fmt_money(val)} ₽</b> аванса к счёту №{num}.\n{_tail}"
    )
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        _closed = " (ЗП закрыта полностью)" if res.get("full_closed") else ""
        _inst = await db.get_user_optional(user_id)
        _inst_name = getattr(_inst, "full_name", None) or user_id
        await notifier.safe_send(
            int(gd_id),
            f"💸 <b>Аванс: применён к счёту</b>{_closed}\n\n"
            f"Монтажник: {_inst_name}\n"
            f"Счёт: №{num}\n"
            f"Применено: {_fmt_money(val)} ₽ в счёт ЗП-монтаж. "
            f"Остаток ЗП: {_fmt_money(_rem)} ₽."
        )


@router.callback_query(F.data == "adv_dist:cancel")
async def advance_distribute_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.clear()
    await cb.message.answer("❌ Распределение отменено.")  # type: ignore[union-attr]


# =====================================================================
# TZ tingly-twirling-whistle 2026-05-25: Withdraw с депозита (личный расход).
# =====================================================================


@router.callback_query(F.data == "inst_withdraw:start")
async def installer_withdraw_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Игорь нажимает «💸 Расход депозита» — старт FSM."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    balance = await db.get_deposit_balance(user_id)
    if balance <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            "❌ На депозите нет средств для расхода.",
        )
        return
    await state.set_state(InstallerWithdrawSG.enter_amount)
    await state.update_data(withdraw_balance=balance)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 <b>Расход с депозита</b>\n"
        f"Доступно: <b>{_fmt_money(balance)} ₽</b>\n\n"
        f"Введите сумму расхода (₽, ≤ {_fmt_money(balance)}):",
    )


@router.message(InstallerWithdrawSG.enter_amount, F.text)
async def installer_withdraw_amount_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 5000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    balance = await db.get_deposit_balance(message.from_user.id)
    if amount > balance + 0.001:
        await message.answer(
            f"❌ Недостаточно средств. Доступно: {_fmt_money(balance)} ₽."
        )
        return
    await state.update_data(withdraw_amount=amount)
    await state.set_state(InstallerWithdrawSG.enter_comment)
    await message.answer(
        f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n\n"
        "📝 На что потратили? (обязательный комментарий, ≥ 3 символов)\n"
        "Например: «такси на объект», «канцелярия», «бензин»",
    )


@router.message(InstallerWithdrawSG.enter_comment, F.text)
async def installer_withdraw_comment_input(
    message: Message, state: FSMContext,
) -> None:
    comment = (message.text or "").strip()
    if len(comment) < 3:
        await message.answer("❌ Комментарий слишком короткий (≥ 3 символов).")
        return
    await state.update_data(withdraw_comment=comment[:500])
    await state.set_state(InstallerWithdrawSG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="⏭️ Пропустить (без чека)", callback_data="inst_withdraw:skip_receipt")
    b.button(text="❌ Отмена", callback_data="inst_withdraw:cancel")
    b.adjust(1)
    await message.answer(
        "📎 Прикрепите ПП/чек/фото (или нажмите «Пропустить»):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(InstallerWithdrawSG.attach_receipt, F.data == "inst_withdraw:skip_receipt")
async def installer_withdraw_skip_receipt(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    await cb.answer()
    await state.update_data(withdraw_receipt_file_id=None)
    await _installer_withdraw_show_confirm(cb.message, state, db)  # type: ignore[arg-type]


@router.message(InstallerWithdrawSG.attach_receipt)
async def installer_withdraw_receipt_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    file_id: str | None = None
    if message.document:
        file_id = message.document.file_id
    elif message.photo:
        file_id = message.photo[-1].file_id
    elif (message.text or "").strip() == "—":
        file_id = None
    else:
        await message.answer(
            "Пришлите фото/документ, либо нажмите «Пропустить» под предыдущим сообщением.",
        )
        return
    await state.update_data(withdraw_receipt_file_id=file_id)
    await _installer_withdraw_show_confirm(message, state, db)


async def _installer_withdraw_show_confirm(
    target: Any, state: FSMContext, db: Database,
) -> None:
    """Показать preview + кнопки confirm/cancel."""
    data = await state.get_data()
    amount = float(data.get("withdraw_amount") or 0)
    comment = str(data.get("withdraw_comment") or "")
    file_id = data.get("withdraw_receipt_file_id")
    user_id = getattr(target.from_user, "id", 0) if hasattr(target, "from_user") else 0
    balance_before = await db.get_deposit_balance(user_id) if user_id else 0
    balance_after = max(0.0, balance_before - amount)
    receipt_str = "📎 прикреплён" if file_id else "— не приложен"
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="inst_withdraw:confirm")
    b.button(text="❌ Отмена", callback_data="inst_withdraw:cancel")
    b.adjust(1)
    await state.set_state(InstallerWithdrawSG.confirm)
    text = (
        f"💸 <b>Подтвердите расход</b>\n\n"
        f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n"
        f"Комментарий: {comment}\n"
        f"Чек: {receipt_str}\n\n"
        f"Баланс депозита: {_fmt_money(balance_before)} ₽ → "
        f"<b>{_fmt_money(balance_after)} ₽</b>"
    )
    if hasattr(target, "answer"):
        await target.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(InstallerWithdrawSG.confirm, F.data == "inst_withdraw:confirm")
@money_confirm_guard
async def installer_withdraw_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    amount = float(data.get("withdraw_amount") or 0)
    comment = str(data.get("withdraw_comment") or "")
    file_id = data.get("withdraw_receipt_file_id")
    user_id = cb.from_user.id
    if amount <= 0 or not comment:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id = await db.create_installer_withdraw(
            installer_id=user_id,
            amount=amount,
            comment=comment,
            receipt_file_id=file_id,
        )
    except ValueError as e:
        await cb.message.answer(f"❌ {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.clear()
    new_balance = await db.get_deposit_balance(user_id)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after withdraw: %s", e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Расход #{req_id} зафиксирован.\n"
        f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n"
        f"Остаток депозита: <b>{_fmt_money(new_balance)} ₽</b>",
    )
    # Notify ГД: текст + пересылка чека (если есть).
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        installer_name = "Монтажник"
        try:
            u = await db.get_user_optional(user_id)
            if u:
                installer_name = getattr(u, "full_name", None) or getattr(u, "username", None) or installer_name
        except Exception:
            pass
        gd_text = (
            f"💸 <b>{installer_name} снял с депозита</b>\n"
            f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n"
            f"Комментарий: {comment}\n"
            f"Чек: {'📎 ниже' if file_id else 'не приложен'}\n\n"
            f"Остаток депозита: <b>{_fmt_money(new_balance)} ₽</b>"
        )
        await notifier.safe_send(int(gd_id), gd_text)
        if file_id:
            try:
                await notifier.safe_send_photo(int(gd_id), file_id)
            except Exception:
                try:
                    await notifier.safe_send_document(int(gd_id), file_id)
                except Exception as e:
                    log.warning("Failed to forward withdraw receipt to GD: %s", e)


@router.callback_query(F.data == "inst_withdraw:cancel")
async def installer_withdraw_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Расход отменён.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]


# ============================================================================
# funds-2balances 25.05: монтажник переводит часть депозита на advance.
# Односторонний (depo→adv only). Использует InstallerDepoToAdvSG.
# ============================================================================


@router.callback_query(F.data == "inst_depo_to_adv:start")
async def installer_depo_to_adv_start(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Монтажник нажимает «↔️ Депо → Аванс» — старт FSM."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    from ..config import ADVANCE_ENABLED_INSTALLERS
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in ADVANCE_ENABLED_INSTALLERS:
        await cb.answer("⛔ Функция недоступна.", show_alert=True)
        return
    await cb.answer()
    depo = await db.get_deposit_balance(user_id)
    if depo <= 0:
        await cb.message.answer("❌ На депозите нет средств.")  # type: ignore[union-attr]
        return
    await state.set_state(InstallerDepoToAdvSG.enter_amount)
    await state.update_data(depo_to_adv_balance=depo)
    await cb.message.answer(  # type: ignore[union-attr]
        f"↔️ <b>Перевод Депозит → Аванс</b>\n"
        f"Депозит: <b>{_fmt_money(depo)} ₽</b>\n\n"
        f"Введите сумму перевода (₽, ≤ {_fmt_money(depo)}):\n"
        f"<i>Переведённые деньги попадут в advance — сможете распределить по счетам.</i>",
    )


@router.message(InstallerDepoToAdvSG.enter_amount, F.text)
async def installer_depo_to_adv_amount(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 5000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    depo = await db.get_deposit_balance(message.from_user.id)
    if amount > depo + 0.001:
        await message.answer(
            f"❌ Недостаточно средств. На депозите: {_fmt_money(depo)} ₽.",
        )
        return
    advance_now = await db.get_advance_balance(message.from_user.id)
    await state.update_data(depo_to_adv_amount=amount)
    await state.set_state(InstallerDepoToAdvSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="inst_depo_to_adv:confirm")
    b.button(text="❌ Отмена", callback_data="inst_depo_to_adv:cancel")
    b.adjust(1)
    await message.answer(
        f"↔️ <b>Подтвердите перевод</b>\n\n"
        f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n\n"
        f"💸 Депозит: {_fmt_money(depo)} → <b>{_fmt_money(max(0, depo - amount))} ₽</b>\n"
        f"💰 Аванс: {_fmt_money(advance_now)} → <b>{_fmt_money(advance_now + amount)} ₽</b>\n\n"
        f"<i>Перевод односторонний — обратно вернуть нельзя.</i>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(InstallerDepoToAdvSG.confirm, F.data == "inst_depo_to_adv:confirm")
@money_confirm_guard
async def installer_depo_to_adv_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    amount = float(data.get("depo_to_adv_amount") or 0)
    user_id = cb.from_user.id
    if amount <= 0:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id = await db.create_employee_depo_to_adv_transfer(
            employee_id=user_id, amount=amount, actor_id=user_id,
        )
    except ValueError as e:
        await cb.message.answer(f"❌ {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.clear()
    new_depo = await db.get_deposit_balance(user_id)
    new_adv = await db.get_advance_balance(user_id)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after depo_to_adv: %s", e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Перевод #{req_id} выполнен.\n"
        f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n\n"
        f"💸 Депозит: <b>{_fmt_money(new_depo)} ₽</b>\n"
        f"💰 Аванс: <b>{_fmt_money(new_adv)} ₽</b>",
    )
    # Notify ГД (informational).
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        name = "Монтажник"
        try:
            u = await db.get_user_optional(user_id)
            if u:
                name = getattr(u, "full_name", None) or getattr(u, "username", None) or name
        except Exception:
            pass
        await notifier.safe_send(
            int(gd_id),
            f"↔️ <b>{name} перевёл депозит → аванс</b>\n"
            f"Сумма: <b>{_fmt_money(amount)} ₽</b>\n\n"
            f"💸 Депозит: <b>{_fmt_money(new_depo)} ₽</b>\n"
            f"💰 Аванс: <b>{_fmt_money(new_adv)} ₽</b>",
        )


@router.callback_query(F.data == "inst_depo_to_adv:cancel")
async def installer_depo_to_adv_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Перевод отменён.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]
