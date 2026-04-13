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

import logging
from datetime import datetime
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, MontazhStage, Role, TaskStatus, TaskType
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
    InstallerDailyReportSG,
    InstallerInvoiceOkSG,
    InstallerMatInitSG,
    InstallerOrderMaterialsSG,
    InstallerRazmerySG,
    InstallerWorkAcceptSG,
    InstallerZpAdjustSG,
    InstallerZpInitSG,
    InstallerZpSG,
)
from ..utils import answer_service, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

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
async def order_mat_attachments(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    elif message.video:
        attachments.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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

    task = await db.create_task(
        project_id=None,
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

    # Текущая согласованная сумма (или расчётная)
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if not agreed:
        est_inst = inv.get("estimated_installation")
        if est_inst:
            try:
                agreed = int(float(est_inst) * 0.71) // 1000 * 1000
            except (ValueError, TypeError):
                pass

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


@router.callback_query(F.data.startswith("instok:price_ok:"))
async def invoice_ok_price_ok(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Монтажник согласен с текущей суммой → переход к комментарию."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Зафиксировать сумму если ещё не зафиксирована
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    if not agreed:
        est_inst = inv.get("estimated_installation")
        if est_inst:
            try:
                agreed = int(float(est_inst) * 0.71) // 1000 * 1000
            except (ValueError, TypeError):
                pass
        if agreed:
            await db.conn.execute(
                "UPDATE invoices SET montazh_agreed_amount = ? WHERE id = ?",
                (agreed, invoice_id),
            )
            await db.conn.commit()

    await state.update_data(invoice_id=invoice_id)
    await state.set_state(InstallerInvoiceOkSG.comment)
    await _ensure_reply_kb(cb, db, config)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Стоимость: <b>{agreed:,.0f}₽</b>\n\n"
        "Добавьте <b>комментарий</b> (или «—»):",
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
) -> None:
    """Монтажник ввёл сумму → фиксация → переход к комментарию."""
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
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ? WHERE id = ?",
        (amount, invoice_id),
    )
    await db.conn.commit()

    await state.set_state(InstallerInvoiceOkSG.comment)
    await _ensure_reply_kb(message, db, config)
    await message.answer(
        f"✅ Стоимость: <b>{amount:,}₽</b>\n\n"
        "Добавьте <b>комментарий</b> (или «—»):",
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
    if not message.from_user:
        return
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""

    data = await state.get_data()
    invoice_id = data["invoice_id"]

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

    # Set actual completion date (Дата Факт)
    today_iso = datetime.now().strftime("%Y-%m-%d")
    await db.conn.execute(
        "UPDATE invoices SET actual_completion_date = ? WHERE id = ? AND actual_completion_date IS NULL",
        (today_iso, invoice_id),
    )
    await db.conn.commit()

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        await state.clear()
        return

    # Create task
    await db.create_task(
        project_id=None,
        type_=TaskType.INSTALLER_INVOICE_OK,
        status=TaskStatus.DONE,
        created_by=message.from_user.id,
        assigned_to=inv.get("created_by", 0),
        due_at_iso=None,
        payload={
            "invoice_id": invoice_id,
            "invoice_number": inv["invoice_number"],
            "comment": comment,
            "installer_id": message.from_user.id,
        },
    )

    initiator = await get_initiator_label(db, message.from_user.id)
    msg = (
        f"✅ <b>Монтажник — Счет ОК</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Счёт №{inv['invoice_number']}\n"
    )
    if comment:
        msg += f"💬 {comment}\n"

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

    role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Подтверждение отправлено по счёту №{inv['invoice_number']}.",
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
async def razmery_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("razmery_attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id})
    elif message.photo:
        attachments.append({"file_type": "photo", "file_id": message.photo[-1].file_id})
    elif message.video:
        attachments.append({"file_type": "video", "file_id": message.video.file_id})
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(razmery_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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
async def razmery_result_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments = data.get("razmery_result_attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id})
    elif message.photo:
        attachments.append({"file_type": "photo", "file_id": message.photo[-1].file_id})
    elif message.video:
        attachments.append({"file_type": "video", "file_id": message.video.file_id})
    else:
        await message.answer("Прикрепите файл/фото или нажмите кнопку.")
        return
    await state.update_data(razmery_result_attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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
    _ZP_DONE = ("confirmed",)  # ЗП получена монтажником
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
    _ZP_DONE = ("confirmed",)
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
            # #18: Две кнопки вместо "Запрос ЗП"
            if zp_st not in ("approved",):
                b.button(text="✏️ Изменить стоимость", callback_data=f"instzpadj:start:{inv['id']}")
            if zp_st == "not_requested":
                b.button(text="✅ ЗП получено", callback_data=f"instzp_done:{inv['id']}")
            # #20: Кнопка "Цена ок"
            b.button(text="💲 Цена ок", callback_data=f"instzp_price_ok:{inv['id']}")
        elif cat == "work":
            zp_st = inv.get("zp_installer_status") or "not_requested"
            if zp_st not in ("approved",):
                b.button(text="✏️ Изменить стоимость", callback_data=f"instzpadj:start:{inv['id']}")
        b.button(text="⬅️ Назад", callback_data="instobj:back")
        b.adjust(1)
        await cb.message.answer(card_text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("instzp_done:"))
async def installer_zp_done(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """#18: Монтажник подтвердил получение ЗП."""
    if not await require_role_callback(cb, db, roles=[Role.INSTALLER]):
        return
    await cb.answer("✅ ЗП подтверждено")
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        return
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
    await db.update_invoice(inv_id, montazh_stage="invoice_ok")
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
    _ZP_DONE = ("confirmed",)
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


def _build_inst_detail_card(inv: dict) -> str:
    """Формирует карточку счёта для монтажника (табличный формат)."""
    from datetime import date as _date

    stage = inv.get("montazh_stage") or "none"
    stage_lbl = _STAGE_LABEL.get(stage, stage)
    mgr, lead_name, lead_phone, num = _inst_card_header(inv)

    text = f"📄 <b>№{num}</b> · {stage_lbl}{_credit_tag(inv)}\n\n"

    est_val = 0
    est_inst = inv.get("estimated_installation")
    if est_inst:
        try:
            est_val = int(float(est_inst) * 0.71) // 1000 * 1000
        except (ValueError, TypeError):
            pass

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

    lines = ["<pre>"]
    lines.append(f"{'Менеджер':16s} {mgr}")
    lines.append(f"{'Адрес':16s} {inv.get('object_address', '—')}")
    if lead_name:
        lines.append(f"{'Клиент':16s} {lead_name}")
    if lead_phone:
        lines.append(f"{'Телефон':16s} {lead_phone}")
    lines.append(f"{'':16s} {'─' * 16}")
    if est_val:
        lines.append(f"{'Монтаж':16s} {est_val:>10,}₽")
        lines.append(f"{'Монтаж +10%':16s} {int(est_val * 1.10):>10,}₽")
    if zp_val:
        lines.append(f"{'ЗП сумма':16s} {zp_val:>10,.0f}₽")
    lines.append(f"{'ЗП статус':16s} {zp_lbl}")
    if dl_str:
        lines.append(f"{'Срок':16s} {dl_str}")
    if days_str:
        lines.append(f"{'Осталось':16s} {days_str}")
    lines.append("</pre>")

    return text + "\n".join(lines)


def _build_archive_stats(invoices: list[dict]) -> str:
    """Статистика архива: месяц, год, сроки."""
    from datetime import date as _date

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

    def _line(label: str, invs: list[dict]) -> str:
        cnt = len(invs)
        zp = sum(float(i.get("zp_installer_amount") or 0) for i in invs)
        est_total = sum(
            int(float(i.get("estimated_installation") or 0) * 0.71) // 1000 * 1000
            for i in invs
        )
        pct = (zp / est_total * 100) if est_total > 0 else 0
        return f"{label}: {cnt} шт. · {zp:,.0f}₽ · {pct:.1f}%"

    lines = [
        "📊 <b>Статистика</b>",
        f"📅 Месяц: {_line('', month_inv).lstrip(': ')}",
        f"📅 Год {cur_year}: {_line('', year_inv).lstrip(': ')}",
        f"⏰ Сроки: ✅ {on_time} в срок · 🔴 {late} просрочено",
    ]
    return "\n".join(lines)


def _build_archive_card(inv: dict) -> str:
    """Карточка архивного счёта для монтажника (табличный формат)."""
    from datetime import date as _date

    mgr, lead_name, lead_phone, num = _inst_card_header(inv)
    text = f"📄 <b>№{num}</b> · 📦 Архив{_credit_tag(inv)}\n\n"

    est_val = 0
    est_inst = inv.get("estimated_installation")
    if est_inst:
        try:
            est_val = int(float(est_inst) * 0.71) // 1000 * 1000
        except (ValueError, TypeError):
            pass

    zp_st = inv.get("zp_installer_status") or "not_requested"
    zp_lbl = {"approved": "✅ Одобрено", "confirmed": "✅ Подтверждено",
              "requested": "⏳ Запрошено", "payment_sent": "💳 Отправлено"}.get(zp_st, "—")
    zp_val = float(inv.get("zp_installer_amount") or 0)

    # Дельта: ЗП факт - монтаж расч.
    delta_str = ""
    if est_val and zp_val:
        delta = zp_val - est_val
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
    if est_val:
        lines.append(f"{'Монтаж':16s} {est_val:>10,}₽")
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

    text = _build_inst_detail_card(inv)
    stage = inv.get("montazh_stage") or "none"
    cat = "waiting" if stage == "invoice_ok" else "work"
    b = InlineKeyboardBuilder()
    # Кнопка "Запрос ЗП" для карточек в "Ожидает расчёт"
    zp_st = inv.get("zp_installer_status") or "not_requested"
    if cat == "waiting" and zp_st not in ("approved", "requested"):
        b.button(text="💰 Запрос ЗП", callback_data=f"instzpadj:start:{invoice_id}")
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


def _calc_est_montazh(inv: dict) -> int:
    """Расчётная стоимость монтажа: ×0.71 ⌊1000."""
    est = inv.get("estimated_installation")
    if not est:
        return 0
    try:
        return int(float(est) * 0.71) // 1000 * 1000
    except (ValueError, TypeError):
        return 0


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

    est_val = _calc_est_montazh(inv)
    num = inv.get("invoice_number") or f"#{inv['id']}"
    addr = inv.get("object_address") or "—"

    await state.clear()
    await state.update_data(zpadj_invoice_id=invoice_id, zpadj_est=est_val, attachments=[])

    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data=f"instobj:view:{invoice_id}")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Запрос ЗП</b>\n\n"
        f"📄 №{num}\n"
        f"📍 {addr}\n"
        f"🔧 Расч. монтаж: <b>{est_val:,}₽</b>\n\n"
        f"📝 Напишите комментарий — почему запрашиваете оплату? (обязательно)",
        reply_markup=b.as_markup(),
    )
    await state.set_state(InstallerZpAdjustSG.comment)


@router.message(InstallerZpAdjustSG.comment)
async def zpadj_comment(message: Message, state: FSMContext) -> None:
    """Шаг 2: комментарий → предложить вложения."""
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("⚠️ Комментарий слишком короткий (мин. 5 символов):")
        return
    await state.update_data(zpadj_comment=text)
    await state.set_state(InstallerZpAdjustSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="⏩ Пропустить", callback_data="instzpadj:skip_attach")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await message.answer(
        "📎 Приложите фото/видео (можно несколько) или нажмите Пропустить:",
        reply_markup=b.as_markup(),
    )


@router.message(InstallerZpAdjustSG.attachments)
async def zpadj_attachments(message: Message, state: FSMContext) -> None:
    """Шаг 3: приём вложений."""
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    elif message.video:
        attachments.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Пришлите фото/видео/документ или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text="⏩ Готово", callback_data="instzpadj:skip_attach")
    b.button(text="⬅️ Назад", callback_data="inst_nav:home")
    b.adjust(1)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.", reply_markup=b.as_markup())


@router.callback_query(F.data == "instzpadj:skip_attach")
async def zpadj_to_mode(cb: CallbackQuery, state: FSMContext) -> None:
    """Шаг 4: выбор режима — добавить / заменить."""
    await cb.answer()
    data = await state.get_data()
    est_val = data.get("zpadj_est", 0)
    await state.set_state(InstallerZpAdjustSG.mode)

    b = InlineKeyboardBuilder()
    b.button(text=f"➕ Добавить к расч. ({est_val:,}₽)", callback_data="instzpadj:mode:add")
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
    est_val = data.get("zpadj_est", 0)
    if mode == "add":
        await cb.message.answer(  # type: ignore[union-attr]
            f"Введите сумму, которую нужно <b>добавить</b> к {est_val:,}₽ (₽):"
        )
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            "Введите итоговую сумму ЗП (₽):"
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
    est_val = data.get("zpadj_est", 0)
    mode = data.get("zpadj_mode", "replace")

    if mode == "add":
        total = est_val + int(val)
    else:
        total = int(val)

    await state.update_data(zpadj_total=total, zpadj_input=int(val))
    await state.set_state(InstallerZpAdjustSG.confirm)

    comment = data.get("zpadj_comment", "")
    att_count = len(data.get("attachments", []))

    text = (
        f"📋 <b>Подтверждение запроса ЗП</b>\n\n"
        f"🔧 Расч. монтаж: {est_val:,}₽\n"
    )
    if mode == "add":
        text += f"➕ Доплата: {int(val):,}₽\n"
    text += (
        f"💵 <b>Итого ЗП: {total:,}₽</b>\n\n"
        f"💬 Комментарий: {comment}\n"
        f"📎 Вложений: {att_count}\n"
    )

    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="instzpadj:confirm")
    b.button(text="❌ Отмена", callback_data="instzpadj:cancel")
    b.adjust(2)
    await message.answer(text, reply_markup=b.as_markup())


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
    est_val = data.get("zpadj_est", 0)
    comment = data.get("zpadj_comment", "")
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    mode = data.get("zpadj_mode", "replace")

    # Обновить invoice
    await db.set_invoice_zp_installer_status(invoice_id, "requested", amount=total, requested_by=u.id)
    await integrations.sync_invoice_row(invoice_id)

    inv = await db.get_invoice(invoice_id)
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
            )

        initiator = await get_initiator_label(db, u.id)
        mode_label = "добавить к расч." if mode == "add" else "своя сумма"

        credit_warn = "\n🏦 <b>⚠️ КРЕДИТНЫЙ СЧЁТ</b>\n" if _is_credit(inv) else ""
        notify_text = (
            f"💰 <b>Запрос ЗП монтажника</b>{credit_warn}\n"
            f"👤 От: {initiator}\n"
            f"🔢 Счёт: №{inv_number}\n"
            f"📍 {addr}\n"
            f"🔧 Расч. монтаж: {est_val:,}₽\n"
            f"💵 Запрошено: <b>{total:,}₽</b> ({mode_label})\n\n"
            f"💬 {comment}"
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
        f"✅ Запрос ЗП отправлен ГД.\n"
        f"Счёт: №{inv_number}, сумма: {total:,}₽",
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
async def daily_report_attachments(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({
            "file_type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "caption": message.caption,
        })
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({
            "file_type": "photo",
            "file_id": ph.file_id,
            "file_unique_id": ph.file_unique_id,
            "caption": message.caption,
        })
    elif message.video:
        attachments.append({
            "file_type": "video",
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "caption": message.caption,
        })
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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

@router.message(F.text == INST_BTN_IN_WORK)
async def installer_in_work(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    """Список неподтверждённых счетов для принятия в работу."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    await state.clear()
    invoices = await db.list_installer_unconfirmed_invoices()

    if not invoices:
        await answer_service(message, "🔨 Нет новых счетов для принятия в работу ✅", delay_seconds=60)
        return

    # Restore reply keyboard before sending inline content
    await _ensure_reply_kb_msg(message, db, config)

    b = InlineKeyboardBuilder()
    for inv in invoices:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:25]
        b.button(
            text=f"📄 №{num} — {addr}"[:55],
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

    # Монтаж расч. (×0.71)
    est_val = 0
    est_inst = inv.get("estimated_installation")
    if est_inst:
        try:
            est_val = int(float(est_inst) * 0.71) // 1000 * 1000
        except (ValueError, TypeError):
            pass

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

    lines = [f"📄 <b>Счёт №{inv_num}</b>\n"]
    lines.append("<pre>")
    lines.append(f"{'Менеджер':16s} {mgr_label}")
    lines.append(f"{'Адрес':16s} {addr}")
    if lead_name:
        lines.append(f"{'Клиент':16s} {lead_name}")
    if lead_phone:
        lines.append(f"{'Телефон':16s} {lead_phone}")
    lines.append(f"{'':16s} {'─' * 16}")
    if est_val:
        lines.append(f"{'Монтаж':16s} {est_val:>10,}₽")
        est_plus10 = int(est_val * 1.10)
        lines.append(f"{'Монтаж +10%':16s} {est_plus10:>10,}₽")
    if dl_str:
        lines.append(f"{'Срок':16s} {dl_str}")
    if days_left_str:
        lines.append(f"{'Осталось':16s} {days_left_str}")
    lines.append("</pre>")
    text = "\n".join(lines)

    b = InlineKeyboardBuilder()
    b.button(text=f"🔨 В работу ({est_val:,}₽)", callback_data=f"inst_work:price_ok:{invoice_id}")
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

    # Рассчитать и зафиксировать сумму
    est_inst = inv.get("estimated_installation")
    agreed = 0
    if est_inst:
        try:
            agreed = int(float(est_inst) * 0.71) // 1000 * 1000
        except (ValueError, TypeError):
            pass
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
                f"📋 Монтажник <b>@{u.username or u.full_name}</b> согласовал стоимость монтажа:\n"
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
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Монтажник вводит свою сумму → фиксация + В работу."""
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

    u = message.from_user
    await db.conn.execute(
        "UPDATE invoices SET montazh_agreed_amount = ?, assigned_to = ?, updated_at = ? WHERE id = ?",
        (amount, u.id, datetime.now().isoformat(), invoice_id),
    )
    await db.conn.commit()
    await db.update_montazh_stage(invoice_id, MontazhStage.IN_WORK)

    await _ensure_reply_kb(message, db, config)
    await message.answer(
        f"✅ Стоимость монтажа согласована: <b>{amount:,}₽</b>\n"
        f"🔨 Счёт №{inv['invoice_number']} принят в работу."
    )

    # Уведомление ГД
    if config.default_gd_id:
        try:
            await message.bot.send_message(
                config.default_gd_id,
                f"📋 Монтажник <b>@{u.username or u.full_name}</b> согласовал стоимость монтажа:\n"
                f"№{inv['invoice_number']} — <b>{amount:,}₽</b>\n"
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


@router.message(F.text == INST_BTN_ZP)
async def installer_zp_start(message: Message, state: FSMContext, db: Database) -> None:
    """Запрос ЗП: инициализация (первый вход) или стандартный поток."""
    if not await require_role_message(message, db, roles=[Role.INSTALLER]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    await state.clear()

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
        "  AND status IN ('in_progress', 'paid', 'ended') "
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

    header = f"💰 <b>Запрос ЗП</b> · {len(invoices)} счетов\n"
    parts = []
    if not_req:
        parts.append(f"❌ {len(not_req)} не запрошено")
    if requested:
        parts.append(f"⏳ {len(requested)} на проверке")
    if approved:
        parts.append(f"✅ {len(approved)} оплачено · {sum_approved:,.0f}₽")
    if parts:
        header += " | ".join(parts)
    await message.answer(header)

    # Карточки
    for inv in invoices:
        zp_st = inv.get("zp_installer_status") or "not_requested"
        zp_icon = {"not_requested": "❌", "requested": "⏳", "approved": "✅"}.get(zp_st, "❌")
        zp_label = {"not_requested": "Не запрошена", "requested": "На проверке", "approved": "Оплачена"}.get(zp_st, "—")

        mgr, lead_name, lead_phone, num = _inst_card_header(inv)
        est_val = _calc_est_montazh(inv)
        zp_amount = inv.get("zp_installer_amount")

        lines = [f"{zp_icon} <b>№{num}</b> · {zp_label}{_credit_tag(inv)}\n"]
        lines.append("<pre>")
        lines.append(f"{'Менеджер':16s} {mgr}")
        lines.append(f"{'Адрес':16s} {inv.get('object_address', '—')}")
        if lead_name:
            lines.append(f"{'Клиент':16s} {lead_name}")
        lines.append(f"{'':16s} {'─' * 16}")
        if est_val:
            lines.append(f"{'Монтаж':16s} {est_val:>10,}₽")
        if zp_amount and zp_st in ("requested", "approved"):
            try:
                lines.append(f"{'ЗП':16s} {float(zp_amount):>10,.0f}₽")
            except (ValueError, TypeError):
                pass
        lines.append(f"{'ЗП статус':16s} {zp_label}")
        lines.append("</pre>")
        card = "\n".join(lines)

        b = InlineKeyboardBuilder()
        if zp_st == "not_requested":
            b.button(text="✏️ Изменить стоимость", callback_data=f"instzpadj:start:{inv['id']}")
            b.button(text="💰 Запросить ЗП", callback_data=f"instzpadj:start:{inv['id']}")
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
    await message.answer(
        f"💰 <b>Подтверждение запроса ЗП</b>\n\n"
        f"🔢 Счёт: №{inv['invoice_number'] if inv else '—'}\n"
        f"💵 ЗП: {amount:,.0f}₽\n\n"
        "Отправить запрос ГД?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "instzp:cancel")
async def installer_zp_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("Отменено")
    await state.clear()
    await cb.message.answer("❌ Запрос ЗП отменён.")  # type: ignore[union-attr]


@router.callback_query(F.data == "instzp:confirm", InstallerZpSG.confirm)
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
        credit_warn = "\n🏦 <b>⚠️ КРЕДИТНЫЙ СЧЁТ</b>\n" if inv and _is_credit(inv) else ""
        await notifier.safe_send(
            int(gd_id),
            f"💰 <b>Запрос ЗП монтажника</b>{credit_warn}\n"
            f"👤 От: {initiator}\n"
            f"🔢 Счёт: №{inv_number}\n"
            f"📍 Адрес: {inv.get('object_address') or '—' if inv else '—'}\n"
            f"💵 Сумма: {amount:,.0f}₽",
            reply_markup=b.as_markup(),
        )
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос ЗП отправлен ГД.\n"
        f"Счёт: №{inv_number}, ЗП: {amount:,.0f}₽",
    )
