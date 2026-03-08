from __future__ import annotations

import logging
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import ProjectCb
from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..keyboards import GD_BTN_INVOICE_END_GD, main_menu, projects_kb, tasks_kb
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..services.notifier import Notifier
from ..states import SupplierPaymentSG
from ..utils import answer_service, fmt_project_card, parse_amount, private_only_reply_markup, refresh_recipient_keyboard
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


# ==================== СЧЁТ END (объединяет подтверждение оплат + Счет End) ====================

@router.message(F.text.startswith(GD_BTN_INVOICE_END_GD))
async def gd_invoice_end_combined(message: Message, db: Database) -> None:
    """Show both PAYMENT_CONFIRM and INVOICE_END_REQUEST tasks for GD."""
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    tasks_pc = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.PAYMENT_CONFIRM)
    tasks_ie = await db.list_tasks_for_user(user_id, limit=30, type_filter=TaskType.INVOICE_END_REQUEST)
    tasks = tasks_pc + tasks_ie
    tasks.sort(key=lambda t: t.get("created_at") or "", reverse=True)
    if not tasks:
        await answer_service(message, "✅ Нет задач «Счёт END» и подтверждений оплат.", delay_seconds=60)
        return
    n_pc = len(tasks_pc)
    n_ie = len(tasks_ie)
    parts = []
    if n_pc:
        parts.append(f"💰 Подтв.оплат: {n_pc}")
    if n_ie:
        parts.append(f"🏁 Счёт End: {n_ie}")
    summary = " | ".join(parts)

    # Build keyboard: tasks + lead stats button
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    rows: list[list[InlineKeyboardButton]] = []
    for t in tasks:
        tid = t.get("id", 0)
        ttype = t.get("type", "")
        prefix = "💰" if ttype == TaskType.PAYMENT_CONFIRM else "🏁"
        payload = t.get("payload") or {}
        label = payload.get("invoice_number") or payload.get("supplier") or f"#{tid}"
        rows.append([InlineKeyboardButton(text=f"{prefix} {label}", callback_data=f"task:{tid}")])
    rows.append([InlineKeyboardButton(text="📊 Статистика по лидам", callback_data="gd_lead_stats")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    await message.answer(
        f"🏁 <b>Счёт END</b> ({len(tasks)})\n{summary}\n\n"
        "Выберите задачу:",
        reply_markup=kb,
    )


@router.callback_query(F.data == "gd_lead_stats")
async def gd_lead_stats_handler(cb: CallbackQuery, db: Database) -> None:
    """Show lead conversion statistics for GD."""
    await cb.answer()
    stats = await db.get_lead_stats()

    by_manager = stats.get("by_manager") or []
    by_source = stats.get("by_source") or []
    total = stats.get("total", 0)
    responded = stats.get("responded", 0)

    manager_labels = {
        "manager_kv": "КВ",
        "manager_kia": "КИА",
        "manager_npn": "НПН",
    }

    lines = [
        "📊 <b>Статистика лидов от РП</b>\n",
        f"Всего лидов: <b>{total}</b>",
        f"Обработано: <b>{responded}</b>\n",
    ]

    if by_manager:
        lines.append("👤 <b>По менеджерам:</b>")
        for m in by_manager:
            role = m.get("assigned_manager_role") or "?"
            lbl = manager_labels.get(role, role)
            cnt = m.get("total", 0)
            avg_min = m.get("avg_time")
            if avg_min and avg_min > 0:
                hours = int(avg_min) // 60
                mins = int(avg_min) % 60
                time_s = f"{hours}ч {mins}мин" if hours else f"{mins}мин"
                lines.append(f"  • {lbl}: {cnt} лидов (ср.время: {time_s})")
            else:
                lines.append(f"  • {lbl}: {cnt} лидов")
        lines.append("")

    if by_source:
        lines.append("📌 <b>По источникам:</b>")
        for s in by_source:
            src = s.get("lead_source") or "Другое"
            cnt = s.get("total", 0)
            lines.append(f"  • {src}: {cnt}")

    if not by_manager and not by_source:
        lines.append("Данных пока нет.")

    await cb.message.answer("\n".join(lines))  # type: ignore


# ==================== ОПЛАТА ПОСТАВЩИКУ (ТД/Сергей -> поставщик) ====================

@router.message(F.text == "💸 Оплата поставщику")
async def start_supplier_payment(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.GD]):
        return
    await state.clear()
    projects = await db.list_recent_projects(limit=20)
    await state.set_state(SupplierPaymentSG.project)
    await message.answer(
        "💸 <b>Оплата поставщику</b>\n"
        "Шаг 1/8: выберите проект.\n"
        "Для отмены: <code>/cancel</code>.",
        reply_markup=projects_kb(projects, ctx="suppl_pay"),
    )


@router.callback_query(ProjectCb.filter(F.ctx == "suppl_pay"))
async def supplier_pay_pick_project(cb: CallbackQuery, callback_data: ProjectCb, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    project = await db.get_project(int(callback_data.project_id))
    await state.update_data(project_id=int(project["id"]))

    # Show parent invoice picker
    from ..keyboards import invoice_select_kb
    invoices = await db.list_invoices_for_selection(limit=15)
    if invoices:
        await state.set_state(SupplierPaymentSG.parent_invoice)
        await cb.message.answer(  # type: ignore
            "Шаг 2/8: привязка к счёту объекта (или пропустите):",
            reply_markup=invoice_select_kb(invoices, prefix="suppl_parent"),
        )
    else:
        await state.update_data(parent_invoice_id=None)
        from ..keyboards import material_type_kb
        await state.set_state(SupplierPaymentSG.material_type)
        await cb.message.answer(  # type: ignore
            "Шаг 3/8: тип материала/услуги:",
            reply_markup=material_type_kb(prefix="suppl_mat"),
        )


@router.callback_query(
    SupplierPaymentSG.parent_invoice,
    lambda cb: cb.data and cb.data.startswith("suppl_parent:"),
)
async def supplier_pay_pick_parent(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick parent invoice for supplier payment."""
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    parent_id = None if val == "skip" else int(val)
    await state.update_data(parent_invoice_id=parent_id)

    from ..keyboards import material_type_kb
    await state.set_state(SupplierPaymentSG.material_type)
    await cb.message.answer(  # type: ignore
        "Шаг 3/8: тип материала/услуги:",
        reply_markup=material_type_kb(prefix="suppl_mat"),
    )


@router.callback_query(
    SupplierPaymentSG.material_type,
    lambda cb: cb.data and cb.data.startswith("suppl_mat:"),
)
async def supplier_pay_pick_material(cb: CallbackQuery, state: FSMContext) -> None:
    """Pick material type for supplier payment."""
    await cb.answer()
    mat_code = (cb.data or "").split(":", 1)[1]
    await state.update_data(material_type=mat_code)

    await state.set_state(SupplierPaymentSG.supplier)
    await cb.message.answer("Шаг 4/8: поставщик (название компании):")  # type: ignore


@router.message(SupplierPaymentSG.supplier)
async def supplier_pay_supplier(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if len(t) < 2:
        await message.answer("Укажите название поставщика:")
        return
    await state.update_data(supplier=t)
    await state.set_state(SupplierPaymentSG.amount)
    await message.answer("Шаг 5/8: сумма оплаты (например 50000 или 50k):")


@router.message(SupplierPaymentSG.amount)
async def supplier_pay_amount(message: Message, state: FSMContext) -> None:
    amount = parse_amount((message.text or "").strip())
    if amount is None:
        await message.answer("Не понял сумму. Пример: 50000 или 50k.")
        return
    await state.update_data(amount=amount)
    await state.set_state(SupplierPaymentSG.invoice_number)
    await message.answer("Шаг 6/8: номер счёта поставщика (или «-»):")


@router.message(SupplierPaymentSG.invoice_number)
async def supplier_pay_invoice(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(invoice_number=t)
    await state.set_state(SupplierPaymentSG.comment)
    await message.answer("Комментарий (или «-»):")


@router.message(SupplierPaymentSG.comment)
async def supplier_pay_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    if t == "-":
        t = ""
    await state.update_data(comment=t, attachments=[])
    await state.set_state(SupplierPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ПП", callback_data="supplpay:create")
    b.button(text="⏭ Без вложений", callback_data="supplpay:create")
    # Шаг 8/8: вложения
    b.adjust(1)
    await message.answer(
        "Приложите платёжное поручение / скрин оплаты (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.message(SupplierPaymentSG.attachments)
async def supplier_pay_attach(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить ПП».")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "supplpay:create")
async def supplier_pay_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    project_id = data.get("project_id")
    if not project_id:
        await cb.message.answer("Не выбран проект. Начните заново.")  # type: ignore
        await state.clear()
        return

    project = await db.get_project(int(project_id))
    supplier = data.get("supplier") or ""
    amount = data.get("amount")
    invoice_number = data.get("invoice_number") or ""
    comment = data.get("comment") or ""
    attachments = data.get("attachments") or []
    parent_invoice_id = data.get("parent_invoice_id")
    material_type = data.get("material_type")

    # Задачу назначаем РП для информирования
    rp_id = await db.get_project_rp_id(int(project_id))
    if not rp_id:
        rp_id = await resolve_default_assignee(db, config, Role.RP)

    source_order_task_id = data.get("source_order_task_id")

    task = await db.create_task(
        project_id=int(project_id),
        type_=TaskType.SUPPLIER_PAYMENT,
        status=TaskStatus.DONE,  # Оплата уже произведена
        created_by=u.id,
        assigned_to=rp_id,
        due_at_iso=None,
        payload={
            "supplier": supplier,
            "amount": amount,
            "invoice_number": invoice_number,
            "comment": comment,
            "td_id": u.id,
            "td_username": u.username,
            "parent_invoice_id": parent_invoice_id,
            "material_type": material_type,
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

    amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
    msg = (
        "💸 <b>Оплата поставщику произведена</b>\n\n"
        f"{fmt_project_card(project, config.timezone)}\n\n"
        f"🏭 Поставщик: <b>{supplier}</b>\n"
        f"💰 Сумма: <b>{amount_s}</b>\n"
    )
    if invoice_number:
        msg += f"🧾 Счёт №: <b>{invoice_number}</b>\n"
    if parent_invoice_id:
        parent_inv = await db.get_invoice(parent_invoice_id)
        if parent_inv:
            msg += f"📋 Объект: Счёт №{parent_inv.get('invoice_number', '?')} — {(parent_inv.get('object_address') or '')[:40]}\n"
    if material_type:
        from ..enums import MATERIAL_TYPE_LABELS
        msg += f"📦 Материал: {MATERIAL_TYPE_LABELS.get(material_type, material_type)}\n"
    if comment:
        msg += f"📝 Комментарий: {comment}\n"
    msg += f"\nОт ГД: <code>{u.id}</code> @{u.username or '-'}"

    # Уведомляем РП и рабочий чат
    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
    await notifier.notify_workchat(msg)

    # Отправляем ПП
    attaches = await db.list_attachments(int(task["id"]))
    for a in attaches:
        if rp_id:
            await notifier.safe_send_media(int(rp_id), a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        await notifier.notify_workchat_media(a["file_type"], a["tg_file_id"], caption=a.get("caption"))
    if rp_id:
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await integrations.sync_task(task, project_code=project.get("code", ""))

    if source_order_task_id:
        try:
            src_task = await db.get_task(int(source_order_task_id))
            if (
                src_task.get("project_id") == int(project_id)
                and src_task.get("type") in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS}
                and src_task.get("status") in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}
            ):
                src_task = await db.update_task_status(int(source_order_task_id), TaskStatus.DONE)
                await integrations.sync_task(src_task, project_code=project.get("code", ""))
        except Exception:
            log.exception("Failed to auto-close source order task id=%s", source_order_task_id)

    user_now = await db.get_user_optional(u.id)
    role_now, isolated_role = resolve_menu_scope(u.id, user_now.role if user_now else Role.GD)
    await cb.message.answer(
        (
            f"✅ Оплата поставщику «{supplier}» зафиксирована. "
            + ("РП уведомлён." if rp_id else "⚠️ РП не назначен (role=rp), уведомление не отправлено.")
        ),
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                role_now,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                unread_channels=await db.count_unread_by_channel(u.id),
                gd_inbox_unread=await db.count_gd_inbox_tasks(u.id),
                gd_invoice_unread=await db.count_gd_invoice_tasks(u.id),
                gd_invoice_end_unread=await db.count_gd_invoice_end_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )  # type: ignore
    await state.clear()
