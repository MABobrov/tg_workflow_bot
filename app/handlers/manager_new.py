"""
Handlers for Manager KV / KIA / NPN roles.

Covers:
- Проверить КП / Счет (CheckKpSG)
- Счет в Работу (InvoiceStartSG)
- Счет End (InvoiceEndSG)
- Замеры (chat-proxy to zamery)
- Бухгалтерия (ЭДО) (EdoRequestSG)
- Менеджер (кред) — chat-proxy mirror
- Мои Счета — list own invoices
- Проблема / Вопрос (IssueSG)
- Поиск Счета
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import (
    InvoiceStatus,
    MANAGER_ROLES,
    Role,
    TaskStatus,
    TaskType,
    ZAMERY_SOURCE_LABELS,
)
from ..keyboards import (
    MGR_BTN_CHECK_KP,
    MGR_BTN_CHAT_RP,
    MGR_BTN_CRED,
    MGR_BTN_EDO,
    MGR_BTN_INVOICE_END,
    MGR_BTN_INVOICE_START,
    MGR_BTN_ISSUE,
    MGR_BTN_MONTAZH,
    MGR_BTN_MY_INVOICES,
    MGR_BTN_SEARCH_INVOICE,
    MGR_BTN_ZAMERY,
    MGR_BTN_ZP,
    edo_invoice_pick_kb,
    edo_type_kb,
    invoice_list_kb,
    main_menu,
    manager_chat_submenu,
    tasks_kb,
    zamery_lead_pick_kb,
    zamery_source_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import (
    CheckKpSG,
    EdoRequestSG,
    InvoiceEndSG,
    InvoiceSearchSG,
    InvoiceStartSG,
    IssueSG,
    ManagerChatProxySG,
    ManagerZpSG,
    ZameryRequestSG,
)
from ..utils import answer_service, format_materials_list, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, try_json_loads
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

ALL_MANAGER_ROLES = [Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN]


# ---------------------------------------------------------------------------
# Auto-refresh middleware — обновляет reply keyboard с бейджами на каждое сообщение
# ---------------------------------------------------------------------------

@router.message.outer_middleware()
async def _manager_auto_refresh(handler, event: Message, data: dict):  # type: ignore[type-arg]
    """При каждом сообщении от менеджера — обновляем reply-клавиатуру с бейджем."""
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
        if menu_role not in MANAGER_ROLES:
            return result
        unread = await db_inst.count_unread_tasks(u.id)
        is_admin = u.id in (cfg.admin_ids or set())
        kb = main_menu(
            menu_role,
            is_admin=is_admin,
            unread=unread,
        )
        await answer_service(event, "🔄", reply_markup=kb, delay_seconds=1)
    except Exception:
        log.debug("manager auto-refresh failed", exc_info=True)
    return result


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return resolve_active_menu_role(user_id, user.role if user else None)


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


def _cred_channel(role: str) -> str:
    """Determine credit channel name by manager role."""
    return {
        Role.MANAGER_KV: "manager_kv",
        Role.MANAGER_KIA: "manager_kia",
        Role.MANAGER_NPN: "manager_npn",
        Role.MANAGER: "manager_kv",  # fallback
    }.get(role, "manager_kv")


# Channel → target role mapping for chat-proxy forwarding
_CHAT_TARGET_MAP: dict[str, str] = {
    "manager_kv": Role.GD,
    "manager_kia": Role.GD,
    "manager_npn": Role.GD,
    "zamery": Role.ZAMERY,
    "rp_to_manager_kv": Role.MANAGER_KV,
    "rp_to_manager_kia": Role.MANAGER_KIA,
    "rp_to_gd": Role.GD,
}

_CHAT_CHANNEL_LABEL: dict[str, str] = {
    "manager_kv": "КВ Кред",
    "manager_kia": "КИА Кред",
    "manager_npn": "НПН Кред",
    "zamery": "Замеры",
    "rp_to_manager_kv": "РП → Менеджер КВ",
    "rp_to_manager_kia": "РП → Менеджер КИА",
    "rp_to_gd": "РП → ГД",
    "montazh": "Монтажная гр.",
}


# =====================================================================
# ПРОВЕРИТЬ КП / СЧЕТ  (CheckKpSG)
# =====================================================================

@router.callback_query(F.data.startswith("create_invoice_from_lead:"))
async def start_check_kp_from_lead(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
) -> None:
    """Start check_kp flow with lead context pre-loaded."""
    if not cb.message or not cb.from_user:
        return
    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    if not task:
        await cb.answer("❌ Задача лида не найдена.", show_alert=True)
        return
    payload = json.loads(task.get("payload_json") or "{}")
    await state.clear()
    await state.update_data(
        lead_task_id=task_id,
        lead_id=payload.get("lead_id"),
        project_id=payload.get("project_id"),
    )
    await state.set_state(CheckKpSG.invoice_number)
    lead_desc = payload.get("description", "")
    await cb.message.answer(
        f"📋 <b>Создание счёта по лиду</b>\n"
        f"📝 {lead_desc}\n\n"
        "Шаг 1/5: Введите <b>номер счёта</b>.\n"
        "Для отмены: <code>/cancel</code>."
    )
    await cb.answer()


@router.message(F.text == MGR_BTN_CHECK_KP)
async def start_check_kp(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await state.set_state(CheckKpSG.invoice_number)
    await message.answer(
        "📋 <b>Проверить КП / Счет</b>\n\n"
        "Шаг 1/5: Введите <b>номер счёта</b>.\n"
        "Для отмены: <code>/cancel</code>."
    )


@router.message(CheckKpSG.invoice_number)
async def check_kp_invoice_number(message: Message, state: FSMContext, db: Database) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите номер счёта:")
        return

    # Check if invoice already exists in DB
    existing = await db.get_invoice_by_number(text)
    if existing:
        # Invoice found → skip to documents (short flow)
        await state.update_data(
            invoice_number=text,
            existing_invoice_id=existing["id"],
            client_name=existing.get("client_name", ""),
            address=existing.get("object_address", ""),
            amount=existing.get("amount", 0),
            payment_type=existing.get("payment_terms", ""),
            deadline_days=existing.get("deadline_days"),
        )
        await state.set_state(CheckKpSG.documents)
        await message.answer(
            f"📄 Счёт <b>№{text}</b> найден в базе.\n"
            f"📍 {existing.get('object_address', '—')}\n"
            f"💰 {existing.get('amount', 0):,.0f}₽\n\n"
            "Прикрепите <b>КП</b> (файл или фото):"
        )
    else:
        # Invoice NOT found → full form
        await state.update_data(invoice_number=text, existing_invoice_id=None)
        await state.set_state(CheckKpSG.client_name)
        await message.answer(
            f"Счёт №{text} <b>не найден</b> в базе.\n"
            "Заполните данные для создания:\n\n"
            "Шаг 2/7: Введите <b>контрагента</b> (название компании/ФИО):"
        )


@router.message(CheckKpSG.client_name)
async def check_kp_client_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите контрагента:")
        return
    await state.update_data(client_name=text)
    await state.set_state(CheckKpSG.address)
    await message.answer("Шаг 3/7: Введите <b>адрес установки</b>:")


@router.message(CheckKpSG.address)
async def check_kp_address(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите адрес:")
        return
    await state.update_data(address=text)
    await state.set_state(CheckKpSG.amount)
    await message.answer("Шаг 4/7: Введите <b>полную сумму счёта</b> (число):")


@router.message(CheckKpSG.amount)
async def check_kp_amount(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("Введите число (сумма счёта):")
        return
    await state.update_data(amount=amount)
    await state.set_state(CheckKpSG.payment_type)

    b = InlineKeyboardBuilder()
    b.button(text="100% предоплата", callback_data="kp_pay:100")
    b.button(text="50/50", callback_data="kp_pay:5050")
    b.button(text="Рассрочка", callback_data="kp_pay:installment")
    b.button(text="Другое", callback_data="kp_pay:other")
    b.adjust(2)
    await message.answer(
        "Шаг 5/7: Выберите <b>тип оплаты</b>:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(CheckKpSG.payment_type, F.data.startswith("kp_pay:"))
async def check_kp_payment_type(cb: CallbackQuery, state: FSMContext) -> None:
    pay_type = (cb.data or "").split(":", 1)[1]
    labels = {"100": "100% предоплата", "5050": "50/50", "installment": "Рассрочка", "other": "Другое"}
    await state.update_data(payment_type=labels.get(pay_type, pay_type))
    await state.set_state(CheckKpSG.deadline_days)
    await cb.message.edit_text(  # type: ignore[union-attr]
        f"✅ Тип оплаты: <b>{labels.get(pay_type, pay_type)}</b>"
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 6/7: Введите <b>срок по договору</b> (кол-во дней):"
    )
    await cb.answer()


@router.message(CheckKpSG.deadline_days)
async def check_kp_deadline(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    try:
        days = int(text)
        if days <= 0:
            raise ValueError
    except (ValueError, TypeError):
        await message.answer("⚠️ Введите положительное число дней (например, 14):")
        return
    await state.update_data(deadline_days=days)
    await state.set_state(CheckKpSG.documents)
    await message.answer(
        "Шаг 7/7: Прикрепите <b>КП</b> (коммерческое предложение).\n"
        "Отправьте файл(ы) или фото."
    )


@router.message(CheckKpSG.documents)
async def check_kp_documents(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("documents", [])

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
    else:
        if not attachments:
            await message.answer("Пришлите файл или фото КП:")
            return
        # Treat text as additional comment
        await state.update_data(documents=attachments)
        await state.set_state(CheckKpSG.comment)
        await message.answer("Шаг 5/5: Добавьте <b>комментарий</b> (или отправьте «—» для пропуска):")
        return

    await state.update_data(documents=attachments)
    await answer_service(
        message,
        f"📎 Принял. Файлов: <b>{len(attachments)}</b>.\n"
        "Отправьте ещё файлы или напишите что-нибудь для перехода к комментарию.",
    )


@router.message(CheckKpSG.comment)
async def check_kp_comment(
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

    invoice_number = data["invoice_number"]
    existing_inv_id = data.get("existing_invoice_id")
    client_name = data.get("client_name", "")
    address = data.get("address", "")
    amount = data.get("amount", 0)
    payment_type = data.get("payment_type", "")
    deadline_days = data.get("deadline_days")
    documents = data.get("documents", [])

    # Create task for RP
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await message.answer("⚠️ РП не найден. Попросите администратора назначить роль РП.")
        await state.clear()
        return

    role = await _current_role(db, message.from_user.id)
    inv_id = existing_inv_id

    # project_id из лида (если из кнопки "Создать счёт") или None
    project_id = data.get("project_id")

    # Если project_id нет (прямое создание) — создаём project автоматически
    if not project_id:
        project = await db.create_project(
            title=f"Счёт: {invoice_number}",
            address=address or None,
            client=client_name or None,
            amount=float(amount) if amount else None,
            deadline_iso=None,
            status="active",
            manager_id=message.from_user.id,
            rp_id=int(rp_id),
        )
        project_id = int(project["id"])

    if not existing_inv_id:
        # New invoice — create in DB
        try:
            inv_id = await db.create_invoice(
                invoice_number=invoice_number,
                project_id=project_id,
                created_by=message.from_user.id,
                creator_role=role or "manager",
                client_name=client_name,
                object_address=address,
                amount=amount,
                description=comment,
                payment_terms=payment_type,
                deadline_days=deadline_days,
            )
        except ValueError:
            await state.clear()
            await message.answer(
                f"⚠️ Счёт №{invoice_number} уже существует в базе.\n"
                "Проверьте номер счёта или используйте существующую карточку."
            )
            return
    else:
        # Existing invoice — update project_id if missing
        await db.update_invoice(inv_id, project_id=project_id)

    # Обновить lead_tracking если создано из лида
    lead_id = data.get("lead_id")
    if lead_id:
        try:
            await db.update_lead_tracking_response(lead_id)
        except Exception:
            log.warning("Failed to update lead_tracking response for lead_id=%s", lead_id)

    role_label = {"manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА", "manager_npn": "Менеджер НПН"}.get(role or "", "Менеджер")

    task = await db.create_task(
        project_id=project_id,
        type_=TaskType.CHECK_KP,
        status=TaskStatus.OPEN,
        created_by=message.from_user.id,
        assigned_to=int(rp_id),
        due_at_iso=None,
        payload={
            "invoice_id": inv_id,
            "invoice_number": invoice_number,
            "client_name": client_name,
            "address": address,
            "amount": amount,
            "payment_type": payment_type,
            "deadline_days": deadline_days,
            "comment": comment,
            "manager_role": role or "manager",
            "manager_id": message.from_user.id,
            "is_new_invoice": not bool(existing_inv_id),
            "project_id": project_id,
        },
    )

    # Save attachments
    for a in documents:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    # Notify RP
    initiator = await get_initiator_label(db, message.from_user.id)
    is_new = "🆕 Новый" if not existing_inv_id else "📄 Существующий"
    msg_text = (
        f"📋 <b>КП от {role_label}</b> ({is_new})\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
    )
    if client_name:
        msg_text += f"🏢 Контрагент: {client_name}\n"
    if address:
        msg_text += f"📍 Адрес: {address}\n"
    if amount:
        msg_text += f"💰 Сумма: {amount:,.0f}₽\n"
    if payment_type:
        msg_text += f"💳 Тип оплаты: {payment_type}\n"
    if deadline_days:
        msg_text += f"⏰ Срок: {deadline_days} дн.\n"
    if comment:
        msg_text += f"💬 Комментарий: {comment}\n"

    # Inline button for RP to respond with documents
    b_kp = InlineKeyboardBuilder()
    b_kp.button(text="📋 Ответить на КП", callback_data=f"kp_review:{task['id']}")
    b_kp.adjust(1)

    await notifier.safe_send(int(rp_id), msg_text, reply_markup=b_kp.as_markup())
    for a in documents:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    status_msg = "создан" if not existing_inv_id else "обновлён"
    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ КП отправлено РП на проверку.\n"
        f"Счёт №{invoice_number} {status_msg}.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# CHECK_KP — ПОДТВЕРЖДЕНИЕ МЕНЕДЖЕРОМ (#26/#27)
# =====================================================================

@router.callback_query(F.data.startswith("mgr_kp_ok:"))
async def mgr_kp_ok_confirm(cb: CallbackQuery, db: Database) -> None:
    """Менеджер подтверждает получение ответа РП по CHECK_KP."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer("✅ Задача подтверждена")
    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]

    # Закрываем кнопку — убираем inline keyboard
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass

    task = await db.get_task(task_id)
    if task:
        # Помечаем как подтверждённую менеджером
        payload = json.loads(task.get("payload_json") or "{}")
        payload["manager_confirmed"] = True
        await db.conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE id = ?",
            (json.dumps(payload, ensure_ascii=False), task_id),
        )
        await db.conn.commit()


# =====================================================================
# СЧЕТ В РАБОТУ (InvoiceStartSG)
# =====================================================================

@router.message(F.text == MGR_BTN_INVOICE_START)
async def start_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await state.set_state(InvoiceStartSG.invoice_number)
    await message.answer(
        "💼 <b>Счет в Работу</b>\n\n"
        "Введите <b>номер счёта</b> для отправки ГД на оплату.\n"
        "Для отмены: <code>/cancel</code>."
    )


@router.message(InvoiceStartSG.invoice_number)
async def invoice_start_number(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user:
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите номер счёта:")
        return

    # Search for invoice
    inv = await db.get_invoice_by_number(text)
    if not inv:
        await message.answer(
            f"❌ Счёт №{text} не найден в базе.\n"
            "Проверьте номер или сначала создайте счёт через «📋 Проверить КП/Счет»."
        )
        return

    if int(inv.get("created_by") or 0) != message.from_user.id:
        await message.answer("⛔️ Можно отправить ГД только свой счёт.")
        return

    if inv["status"] not in (InvoiceStatus.NEW,):
        await message.answer(
            f"⚠️ Счёт №{text} уже в статусе: {inv['status']}.\n"
            "Повторная отправка невозможна."
        )
        await state.clear()
        return

    await state.update_data(invoice_id=inv["id"], invoice_number=text, invoice_data=dict(inv))
    await state.set_state(InvoiceStartSG.client_source)

    b = InlineKeyboardBuilder()
    b.button(text="👤 Мой клиент (50/50)", callback_data="inv_src:own")
    b.button(text="📋 Лид от ГД (75/25)", callback_data="inv_src:gd_lead")
    b.adjust(1)

    await message.answer(
        f"Счёт №{text} найден.\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n\n"
        "❓ <b>Источник клиента</b> (влияет на распределение прибыли):",
        reply_markup=b.as_markup(),
    )


# ---------- Источник клиента ----------

@router.callback_query(F.data.startswith("inv_src:"), InvoiceStartSG.client_source)
async def invoice_start_client_source(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    source = cb.data.split(":")[1]  # type: ignore[union-attr]
    await state.update_data(client_source=source)
    await state.set_state(InvoiceStartSG.deadline_days)
    label = "👤 Мой клиент (50/50)" if source == "own" else "📋 Лид от ГД (75/25)"
    await cb.message.answer(  # type: ignore[union-attr]
        f"Источник: {label}\n\n"
        "📅 Введите <b>срок по договору</b> в днях\n"
        "(количество дней от сегодня до окончания):",
    )


# ---------- Срок по договору ----------

@router.message(InvoiceStartSG.deadline_days)
async def invoice_start_deadline(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    try:
        days = int(text)
        if days <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите целое число дней > 0:")
        return
    from datetime import date, timedelta
    end_date = date.today() + timedelta(days=days)
    await state.update_data(
        deadline_days=days,
        deadline_end_date=end_date.isoformat(),
    )
    await state.set_state(InvoiceStartSG.estimated_glass)
    await message.answer(
        f"📅 Срок по договору: <b>{end_date.strftime('%d.%m.%Y')}</b> ({days} дн.)\n\n"
        "📊 <b>Расчётные данные</b> (шаг 1/5)\n"
        "Введите <b>расчётную стоимость стекла</b> в ₽:\n"
        "<i>Введите 0, если стекла нет.</i>",
    )


# ---------- Расчётные данные (5 шагов) ----------

def _parse_est_value(text: str) -> float | None:
    """Parse estimated value from user input. Returns None if invalid."""
    t = (text or "").strip().replace(",", ".").replace(" ", "").replace("\u00a0", "")
    try:
        val = float(t)
        return val if val >= 0 else None
    except ValueError:
        return None


@router.message(InvoiceStartSG.estimated_glass)
async def invoice_start_est_glass(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_glass=val)
    await state.set_state(InvoiceStartSG.estimated_profile)
    await message.answer(
        "📊 <b>Расчётные данные</b> (шаг 2/5)\n"
        "Введите <b>расчётную стоимость алюминиевого профиля</b> в ₽:\n"
        "<i>Введите 0, если профиля нет.</i>",
    )


@router.message(InvoiceStartSG.estimated_profile)
async def invoice_start_est_profile(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_profile=val)
    await state.set_state(InvoiceStartSG.estimated_installation)
    await message.answer(
        "📊 <b>Расчётные данные</b> (шаг 3/5)\n"
        "Введите <b>расчётную стоимость установки</b> в ₽:\n"
        "<i>Введите 0, если установки нет.</i>",
    )


@router.message(InvoiceStartSG.estimated_installation)
async def invoice_start_est_installation(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_installation=val)
    await state.set_state(InvoiceStartSG.estimated_loaders)
    await message.answer(
        "📊 <b>Расчётные данные</b> (шаг 4/5)\n"
        "Введите <b>расчётную стоимость грузчиков</b> в ₽:\n"
        "<i>Введите 0, если грузчиков нет.</i>",
    )


@router.message(InvoiceStartSG.estimated_loaders)
async def invoice_start_est_loaders(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_loaders=val)
    await state.set_state(InvoiceStartSG.estimated_logistics)
    await message.answer(
        "📊 <b>Расчётные данные</b> (шаг 5/5)\n"
        "Введите <b>расчётную стоимость логистики</b> в ₽:\n"
        "<i>Введите 0, если логистики нет.</i>",
    )


@router.message(InvoiceStartSG.estimated_logistics)
async def invoice_start_est_logistics(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_logistics=val)
    await state.set_state(InvoiceStartSG.attachments)

    # Показать сводку расчётных данных и перейти к вложениям
    data = await state.get_data()
    inv_data = data.get("invoice_data", {})
    amount = float(inv_data.get("amount", 0))
    est_glass = data.get("estimated_glass", 0)
    est_profile = data.get("estimated_profile", 0)
    est_inst = data.get("estimated_installation", 0)
    est_load = data.get("estimated_loaders", 0)
    est_log = val
    est_total = est_glass + est_profile + est_inst + est_load + est_log

    # НДС с учётом возвратного
    output_vat = amount * 22 / 122 if amount > 0 else 0
    input_vat = (est_glass + est_profile) * 22 / 122 if (est_glass + est_profile) > 0 else 0
    net_vat = output_vat - input_vat
    est_profit = amount - est_total - net_vat
    est_pct = (est_profit / amount * 100) if amount > 0 else 0

    # Profit split
    client_source = data.get("client_source", "own")
    rp_zp = est_profit * 0.10 if est_profit > 0 else 0
    remaining = est_profit - rp_zp
    if client_source == "gd_lead":
        mgr_share = remaining * 0.25
        split_label = "Лид ГД (75/25)"
    else:
        mgr_share = remaining * 0.50
        split_label = "Мой клиент (50/50)"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="inv_start:send")
    b.button(text="⏭ Без вложений", callback_data="inv_start:send_no_attach")
    b.adjust(1)

    await message.answer(
        f"📊 <b>Расчётные данные введены:</b>\n"
        f"  Стекло: {est_glass:,.0f}₽\n"
        f"  Ал.профиль: {est_profile:,.0f}₽\n"
        f"  Установка: {est_inst:,.0f}₽\n"
        f"  Грузчики: {est_load:,.0f}₽\n"
        f"  Логистика: {est_log:,.0f}₽\n"
        f"  НДС выходной: {output_vat:,.0f}₽\n"
        f"  Возвр.НДС: -{input_vat:,.0f}₽\n"
        f"  Чистый НДС: {net_vat:,.0f}₽\n"
        f"  ─────────────\n"
        f"  Расч.себестоимость: {est_total:,.0f}₽\n"
        f"  Расч.прибыль: {est_profit:,.0f}₽ ({est_pct:.1f}%)\n\n"
        f"💰 <b>Распределение ({split_label}):</b>\n"
        f"  ЗП РП (10%): {rp_zp:,.0f}₽\n"
        f"  Ваша доля: {mgr_share:,.0f}₽\n\n"
        "📎 Прикрепите документы (необязательно: счёт, договор, приложение)\n"
        "или сразу нажмите «⏭ Без вложений».",
        reply_markup=b.as_markup(),
    )


@router.message(InvoiceStartSG.attachments)
async def invoice_start_attachments(message: Message, state: FSMContext) -> None:
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
    else:
        await message.answer("Пришлите файл/фото или нажмите «✅ Отправить ГД».")
        return

    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data.in_({"inv_start:send", "inv_start:send_no_attach"}))
async def invoice_start_send(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    invoice_id = data["invoice_id"]
    invoice_number = data["invoice_number"]
    inv_data = data.get("invoice_data", {})
    attachments = data.get("attachments", [])

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await cb.message.answer("⚠️ ГД не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    # Save estimated data + client source to DB
    est_glass = data.get("estimated_glass", 0)
    est_profile = data.get("estimated_profile", 0)
    est_inst = data.get("estimated_installation", 0)
    est_load = data.get("estimated_loaders", 0)
    est_log = data.get("estimated_logistics", 0)
    client_source = data.get("client_source", "own")
    deadline_fields = {}
    if data.get("deadline_days"):
        deadline_fields["deadline_days"] = data["deadline_days"]
    if data.get("deadline_end_date"):
        deadline_fields["deadline_end_date"] = data["deadline_end_date"]
    await db.update_invoice(
        invoice_id,
        estimated_glass=est_glass,
        estimated_profile=est_profile,
        estimated_installation=est_inst,
        estimated_loaders=est_load,
        estimated_logistics=est_log,
        client_source=client_source,
        **deadline_fields,
    )

    # Update invoice status
    await db.update_invoice_status(invoice_id, InvoiceStatus.PENDING_PAYMENT)
    await integrations.sync_invoice_status(invoice_number, InvoiceStatus.PENDING_PAYMENT)

    # Create task for GD
    role = await _current_role(db, u.id)
    role_label = {"manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА", "manager_npn": "Менеджер НПН"}.get(role or "", "Менеджер")

    task = await db.create_task(
        project_id=None,
        type_=TaskType.INVOICE_PAYMENT,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "invoice_id": invoice_id,
            "invoice_number": invoice_number,
            "amount": inv_data.get("amount", 0),
            "address": inv_data.get("object_address", ""),
            "supplier": inv_data.get("supplier", ""),
            "manager_role": role or "manager",
            "manager_id": u.id,
            "sender_id": u.id,
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

    # Notify GD
    initiator = await get_initiator_label(db, u.id)
    amount = float(inv_data.get("amount", 0))
    est_total = est_glass + est_profile + est_inst + est_load + est_log

    # НДС с учётом возвратного
    output_vat = amount * 22 / 122 if amount > 0 else 0
    input_vat = (est_glass + est_profile) * 22 / 122 if (est_glass + est_profile) > 0 else 0
    net_vat = output_vat - input_vat
    est_profit = amount - est_total - net_vat
    est_pct = (est_profit / amount * 100) if amount > 0 else 0

    # Profit split
    rp_zp = est_profit * 0.10 if est_profit > 0 else 0
    remaining = est_profit - rp_zp
    if client_source == "gd_lead":
        mgr_share = remaining * 0.25
        gd_share = remaining * 0.75
        src_label = "📋 Лид от ГД (75/25)"
    else:
        mgr_share = remaining * 0.50
        gd_share = remaining * 0.50
        src_label = "👤 Клиент менеджера (50/50)"

    msg_text = (
        f"💼 <b>Запрос подтверждения оплаты от {role_label}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {inv_data.get('object_address', '-')}\n"
        f"💰 Сумма: {amount:,.0f}₽\n"
        f"🔗 Источник: {src_label}\n\n"
        f"📊 <b>Расчётные данные:</b>\n"
        f"  Стекло: {est_glass:,.0f}₽\n"
        f"  Ал.профиль: {est_profile:,.0f}₽\n"
        f"  Установка: {est_inst:,.0f}₽\n"
        f"  Грузчики: {est_load:,.0f}₽\n"
        f"  Логистика: {est_log:,.0f}₽\n"
        f"  НДС выходной: {output_vat:,.0f}₽\n"
        f"  Возвр.НДС: -{input_vat:,.0f}₽\n"
        f"  Чистый НДС: {net_vat:,.0f}₽\n"
        f"  Расч.себест-ть: {est_total:,.0f}₽\n"
        f"  Расч.прибыль: {est_profit:,.0f}₽ ({est_pct:.1f}%)\n\n"
        f"💰 <b>Распределение:</b>\n"
        f"  ЗП РП (10%): {rp_zp:,.0f}₽\n"
        f"  ЗП менеджер: {mgr_share:,.0f}₽\n"
        f"  Доля ГД: {gd_share:,.0f}₽\n"
    )

    from ..keyboards import task_actions_kb
    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(gd_id), msg_text, reply_markup=task_kb)
    for a in attachments:
        await notifier.safe_send_media(int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    # Уведомить РП: счёт взят в работу, ждёт подтверждения ГД
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        rp_text = (
            f"📋 <b>Новый счёт в работе</b>\n"
            f"👤 От: {initiator}\n\n"
            f"📄 Счёт №: <code>{invoice_number}</code>\n"
            f"📍 Адрес: {inv_data.get('object_address', '-')}\n"
            f"💰 Сумма: {amount:,.0f}₽\n\n"
            f"⏳ <b>Статус: ждёт подтверждения ГД</b>"
        )
        await notifier.safe_send(int(rp_id), rp_text)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    # Дополнение 1: спрашиваем ГД — подписаны ли документы в ЭДО
    b_edo = InlineKeyboardBuilder()
    b_edo.button(text="✅ Да, подписаны в ЭДО", callback_data=f"invstart_edo:yes:{invoice_id}")
    b_edo.button(text="❌ Нет, не подписаны", callback_data=f"invstart_edo:no:{invoice_id}")
    b_edo.adjust(1)
    await notifier.safe_send(
        int(gd_id),
        f"❓ <b>Документы (счёт, договор, приложения) подписаны в ЭДО?</b>\n\n"
        f"Счёт №: <code>{invoice_number}</code>",
        reply_markup=b_edo.as_markup(),
    )

    menu_role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Счёт №{invoice_number} отправлен на подтверждение ГД.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# ДОПОЛНЕНИЕ 1: ПРОВЕРКА ЭДО / БУМАЖНЫХ ПОДПИСЕЙ (GD callbacks)
# =====================================================================

@router.callback_query(F.data.startswith("invstart_edo:"))
async def invoice_start_edo_check(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """GD answers: are documents signed in EDO?"""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    answer = parts[1]  # yes or no
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if answer == "yes":
        await db.update_invoice(invoice_id, docs_edo_signed=1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Зафиксировано: документы по счёту №{inv['invoice_number']} подписаны в ЭДО."
        )
        manager_id = inv.get("created_by")
        if manager_id:
            await notifier.safe_send(
                int(manager_id),
                f"✅ ГД подтвердил: документы по счёту №{inv['invoice_number']} подписаны в ЭДО.",
            )
    else:
        # Не подписаны в ЭДО — спрашиваем про бумажные
        b = InlineKeyboardBuilder()
        b.button(text="✅ Да, есть бумажные", callback_data=f"invstart_paper:yes:{invoice_id}")
        b.button(text="❌ Нет бумажных", callback_data=f"invstart_paper:no:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❓ <b>Есть бумажные подписанные версии документов?</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>",
            reply_markup=b.as_markup(),
        )


@router.callback_query(F.data.startswith("invstart_paper:"))
async def invoice_start_paper_check(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """GD answers: are there paper signed versions?"""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    answer = parts[1]
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if answer == "yes":
        await db.update_invoice(invoice_id, docs_paper_signed=1)
        # Уточняем — у кого оригиналы
        b = InlineKeyboardBuilder()
        b.button(text="📁 У ГД", callback_data=f"invstart_orig:gd:{invoice_id}")
        b.button(text="📁 У менеджера", callback_data=f"invstart_orig:manager:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❓ <b>У кого находятся оригиналы?</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>",
            reply_markup=b.as_markup(),
        )
    else:
        await db.update_invoice(invoice_id, docs_paper_signed=0)
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Зафиксировано: по счёту №{inv['invoice_number']} нет подписанных документов "
            f"(ни ЭДО, ни бумажные)."
        )
        manager_id = inv.get("created_by")
        if manager_id:
            await notifier.safe_send(
                int(manager_id),
                f"⚠️ ГД: по счёту №{inv['invoice_number']} нет подписанных документов "
                f"(ни ЭДО, ни бумажные).",
            )


@router.callback_query(F.data.startswith("invstart_orig:"))
async def invoice_start_originals(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """GD answers: who holds the originals?"""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    holder = parts[1]  # gd or manager
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_invoice(invoice_id, docs_originals_holder=holder)

    holder_label = "ГД" if holder == "gd" else "менеджера"
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Зафиксировано: оригиналы по счёту №{inv['invoice_number']} — у {holder_label}."
    )

    manager_id = inv.get("created_by")
    if manager_id:
        await notifier.safe_send(
            int(manager_id),
            f"📁 ГД: оригиналы подписанных документов по счёту №{inv['invoice_number']} "
            f"находятся у {holder_label}.",
        )


# =====================================================================
# СЧЕТ END (InvoiceEndSG)
# =====================================================================

@router.message(F.text == MGR_BTN_INVOICE_END)
async def start_invoice_end(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()

    # Show list of manager's invoices with status IN_PROGRESS or PAID
    invoices = await db.list_invoices(
        created_by=message.from_user.id,  # type: ignore[union-attr]
    )
    active = [i for i in invoices if i["status"] in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID)]

    if not active:
        await answer_service(message, "У вас нет активных счетов для закрытия.", delay_seconds=60)
        return

    await state.set_state(InvoiceEndSG.select_invoice)
    await message.answer(
        "🏁 <b>Счет End</b>\n\n"
        "Выберите счёт для закрытия:",
        reply_markup=invoice_list_kb(active, action_prefix="invend", back_callback="nav:home"),
    )


async def _show_invoice_end_conditions(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    invoice_id: int,
) -> None:
    """Helper: display close-conditions card and ask for comment (condition 4)."""
    conditions = await db.check_close_conditions(invoice_id)
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.set_state(InvoiceEndSG.comment)

    c1 = "✅" if conditions["installer_ok"] else "⏳"
    c2 = "✅" if conditions["edo_signed"] else "⏳"
    c3 = "✅" if conditions["no_debts"] else "⏳"

    text = (
        f"🏁 <b>Счёт №{inv['invoice_number']} — Проверка условий:</b>\n\n"
        f"{c1} 1. Монтажник — Счет ОК\n"
        f"{c2} 2. Бухгалтерия — Закр.ЭДО ок\n"
        f"{c3} 3. Долгов нет — подтверждение ГД\n"
        f"☐ 4. Пояснения (опционально)\n"
    )

    # Показываем информацию о документах: ЭДО или оригиналы
    primary_edo = bool(inv.get("docs_edo_signed"))
    closing_edo = bool(inv.get("edo_signed"))
    primary_h = inv.get("docs_originals_holder")
    closing_h = inv.get("closing_originals_holder")

    if primary_edo:
        text += "\n📄 Первичные: подписаны в ЭДО"
    elif primary_h:
        text += f"\n📁 Оригиналы первичных: у {'ГД' if primary_h == 'gd' else 'менеджера'}"

    if closing_edo:
        text += "\n📄 Закрывающие: подписаны в ЭДО"
    elif closing_h:
        text += f"\n📁 Оригиналы закрывающих: у {'ГД' if closing_h == 'gd' else 'менеджера'}"

    # If conditions 1+2 met -> auto-ask GD about debts
    if (
        inv.get("status") != InvoiceStatus.CLOSING
        and conditions["installer_ok"]
        and conditions["edo_signed"]
        and not conditions["no_debts"]
    ):
        gd_id = await resolve_default_assignee(db, config, Role.GD)
        if gd_id:
            b = InlineKeyboardBuilder()
            b.button(text="✅ Да, оплачен 100%", callback_data=f"invend_gd:yes:{invoice_id}")
            b.button(text="❌ Нет, есть долг", callback_data=f"invend_gd:no:{invoice_id}")
            b.adjust(1)
            await notifier.safe_send(
                int(gd_id),
                f"❓ <b>Счёт №{inv['invoice_number']} — оплачен 100%?</b>\n\n"
                f"Менеджер инициировал «Счет End».",
                reply_markup=b.as_markup(),
            )
            text += "\n\n⏳ Запрос отправлен ГД: «Счёт оплачен 100%?»"

    text += "\n\nНапишите <b>пояснение</b> (или «—» для пропуска):"

    await cb.message.answer(text)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("invend:view:"))
async def invoice_end_select(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier
) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await state.update_data(invoice_id=invoice_id)

    # Сначала проверяем ЭДО — если подписано, оригиналы не нужны
    primary_edo = bool(inv.get("docs_edo_signed"))
    closing_edo = bool(inv.get("edo_signed"))

    primary_missing = not primary_edo and not inv.get("docs_originals_holder")
    closing_missing = not closing_edo and not inv.get("closing_originals_holder")

    if primary_missing:
        # Спрашиваем менеджера: у кого оригиналы первичных документов
        await state.set_state(InvoiceEndSG.closing_originals)
        b = InlineKeyboardBuilder()
        b.button(text="📁 У ГД", callback_data=f"invend_prim_orig:gd:{invoice_id}")
        b.button(text="📁 У менеджера", callback_data=f"invend_prim_orig:manager:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❓ <b>У кого оригиналы первичных подписанных документов?</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>\n\n"
            "⚠️ Информация о местонахождении оригиналов не была указана при запуске счёта.",
            reply_markup=b.as_markup(),
        )
        return

    if closing_missing:
        # Первичные есть, спрашиваем про закрывающие
        await state.set_state(InvoiceEndSG.closing_originals)
        b = InlineKeyboardBuilder()
        b.button(text="📁 У ГД", callback_data=f"invend_clos_orig:gd:{invoice_id}")
        b.button(text="📁 У менеджера", callback_data=f"invend_clos_orig:manager:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❓ <b>У кого оригиналы закрывающих документов?</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>",
            reply_markup=b.as_markup(),
        )
        return

    # Вся информация об оригиналах есть — переходим к условиям
    await _show_invoice_end_conditions(cb, state, db, config, notifier, invoice_id)


# --- Дополнение 2: callbacks для оригиналов при Счет End ---

@router.callback_query(F.data.startswith("invend_prim_orig:"))
async def invoice_end_primary_originals(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Manager answers: who holds primary originals?"""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    holder = parts[1]  # gd or manager
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_invoice(invoice_id, docs_originals_holder=holder)
    holder_label = "ГД" if holder == "gd" else "менеджера"
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Оригиналы первичных — у {holder_label}."
    )

    # Проверяем закрывающие: если ЭДО подписано — оригиналы не нужны
    closing_edo = bool(inv.get("edo_signed"))
    if not closing_edo and not inv.get("closing_originals_holder"):
        b = InlineKeyboardBuilder()
        b.button(text="📁 У ГД", callback_data=f"invend_clos_orig:gd:{invoice_id}")
        b.button(text="📁 У менеджера", callback_data=f"invend_clos_orig:manager:{invoice_id}")
        b.adjust(1)
        await cb.message.answer(  # type: ignore[union-attr]
            f"❓ <b>У кого оригиналы закрывающих документов?</b>\n\n"
            f"Счёт №: <code>{inv['invoice_number']}</code>",
            reply_markup=b.as_markup(),
        )
    else:
        # ЭДО подписано или оригиналы указаны — переходим к условиям
        await _show_invoice_end_conditions(cb, state, db, config, notifier, invoice_id)


@router.callback_query(F.data.startswith("invend_clos_orig:"))
async def invoice_end_closing_originals(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Manager answers: who holds closing originals?"""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    holder = parts[1]  # gd or manager
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    await db.update_invoice(invoice_id, closing_originals_holder=holder)
    holder_label = "ГД" if holder == "gd" else "менеджера"
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Оригиналы закрывающих — у {holder_label}."
    )

    # Переходим к отображению условий
    await _show_invoice_end_conditions(cb, state, db, config, notifier, invoice_id)


@router.message(InvoiceEndSG.comment)
async def invoice_end_comment(
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
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await message.answer("❌ Счёт не найден.")
        await state.clear()
        return

    # Create task for GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    rp_id = await resolve_default_assignee(db, config, Role.RP)

    if not gd_id:
        await message.answer("⚠️ ГД не найден. Попросите администратора назначить роль ГД.")
        await state.clear()
        return

    await db.create_task(
        project_id=None,
        type_=TaskType.INVOICE_END_REQUEST,
        status=TaskStatus.OPEN,
        created_by=message.from_user.id,
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "invoice_id": invoice_id,
            "invoice_number": inv["invoice_number"],
            "comment": comment,
            "manager_id": message.from_user.id,
        },
    )
    await db.update_invoice_status(invoice_id, InvoiceStatus.CLOSING)
    await integrations.sync_invoice_status(inv["invoice_number"], InvoiceStatus.CLOSING)

    initiator = await get_initiator_label(db, message.from_user.id)
    conditions = await db.check_close_conditions(invoice_id)
    cond_text = (
        f"1. {'✅' if conditions['installer_ok'] else '⏳'} Монтажник — Счет ОК\n"
        f"2. {'✅' if conditions['edo_signed'] else '⏳'} Бухгалтерия — Закр.ЭДО ок\n"
        f"3. {'✅' if conditions['no_debts'] else '⏳'} Долгов нет\n"
        f"4. {'✅' if comment else '☐'} Пояснения"
    )

    msg = (
        f"🏁 <b>Счет End: №{inv['invoice_number']}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n\n"
        f"<b>Условия:</b>\n{cond_text}\n"
    )
    if comment:
        msg += f"\n💬 Пояснение: {comment}"

    # Notify GD
    if gd_id:
        b = InlineKeyboardBuilder()
        b.button(text="📌 На проверке", callback_data=f"invend_final:check:{invoice_id}")
        b.button(text="🏁 Счет End", callback_data=f"invend_final:end:{invoice_id}")
        b.adjust(1)
        await notifier.safe_send(int(gd_id), msg, reply_markup=b.as_markup())
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    # Notify RP
    if rp_id:
        await notifier.safe_send(int(rp_id), msg)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Запрос «Счет End» по счёту №{inv['invoice_number']} отправлен.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=message.from_user.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(message.from_user.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# --- GD callbacks for Invoice End ---

@router.callback_query(F.data.startswith("invend_gd:"))
async def invoice_end_gd_debt_response(
    cb: CallbackQuery, db: Database, notifier: Notifier
) -> None:
    """GD responds: is the invoice 100% paid?"""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    answer = parts[1]  # yes or no
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if answer == "yes":
        await db.set_invoice_no_debts(invoice_id, True)
        await cb.message.answer(f"✅ Счёт №{inv['invoice_number']} — подтверждено: долгов нет.")  # type: ignore[union-attr]
        # Notify manager
        manager_id = inv.get("created_by")
        if manager_id:
            await notifier.safe_send(
                int(manager_id),
                f"✅ ГД подтвердил: счёт №{inv['invoice_number']} оплачен 100%. Условие 3 выполнено."
            )
    else:
        await cb.message.answer(f"⚠️ Счёт №{inv['invoice_number']} — есть долг.")  # type: ignore[union-attr]
        manager_id = inv.get("created_by")
        if manager_id:
            await notifier.safe_send(
                int(manager_id),
                f"⚠️ ГД: по счёту №{inv['invoice_number']} есть долг. Условие 3 НЕ выполнено."
            )


@router.callback_query(F.data.startswith("invend_final:"))
async def invoice_end_gd_final(
    cb: CallbackQuery,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """GD final decision: 'На проверке' or 'Счет End'."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = cb.data.split(":")  # type: ignore[union-attr]
    decision = parts[1]  # check or end
    invoice_id = int(parts[2])

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if decision == "end":
        conditions = await db.check_close_conditions(invoice_id)
        missing_conditions = [
            label
            for key, label in (
                ("installer_ok", "1. Монтажник — Счет ОК"),
                ("edo_signed", "2. ЭДО — подписано"),
                ("no_debts", "3. Долгов нет"),
            )
            if not conditions.get(key)
        ]
        if missing_conditions:
            await cb.message.answer(  # type: ignore[union-attr]
                "⛔️ Нельзя закрыть счёт, пока не выполнены обязательные условия:\n"
                + "\n".join(f"• {item}" for item in missing_conditions)
            )
            return
        await db.update_invoice_status(invoice_id, InvoiceStatus.ENDED)
        # Update montazh stage → invoice_end
        from ..enums import MontazhStage
        await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_END)
        await integrations.sync_invoice_status(
            inv["invoice_number"], InvoiceStatus.ENDED, MontazhStage.INVOICE_END,
        )
        linked_tasks = await db.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            type_filter=[TaskType.INVOICE_END_REQUEST],
            limit=20,
        )
        for linked_task in linked_tasks:
            if linked_task.get("status") in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
                updated_task = await db.update_task_status(int(linked_task["id"]), TaskStatus.DONE)
                await integrations.sync_task(updated_task, project_code="")
        msg = f"🏁 <b>Счёт №{inv['invoice_number']} — ЗАКРЫТ (Счет End)</b>"

        await cb.message.answer(msg)  # type: ignore[union-attr]

        # Notify: manager, RP, accounting
        manager_id = inv.get("created_by")
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        acc_id = await resolve_default_assignee(db, config, Role.ACCOUNTING)

        for target_id in [manager_id, rp_id, acc_id]:
            if target_id:
                await notifier.safe_send(int(target_id), msg)

        # Set ZP status to requested
        await db.set_invoice_zp_status(invoice_id, "requested")

        # --- Себестоимость при закрытии (ГД) ---
        from ..utils import format_cost_card
        cost_data = await db.get_full_invoice_cost_card(invoice_id)
        cost_msg = format_cost_card(inv, cost_data)
        await cb.message.answer(cost_msg)  # type: ignore[union-attr]

        # --- Запись маржи в ОП (Рент-ть факт) ---
        if integrations.sheets:
            inv_num = inv.get("invoice_number")
            margin_pct = cost_data.get("margin_pct", 0)
            try:
                await integrations.sheets.write_field_to_op(
                    inv_num, "margin_pct", f"{margin_pct:.1f}",
                )
            except Exception:
                log.warning("Failed to write margin to ОП for %s", inv_num, exc_info=True)

        # --- Список материалов менеджеру (без сумм) ---
        if manager_id:
            children = await db.list_child_invoices(invoice_id)
            sp_list = cost_data.get("supplier_payments_list", [])
            mat_msg = format_materials_list(inv, children, sp_list)
            await notifier.safe_send(int(manager_id), mat_msg)
    else:
        await cb.message.answer(  # type: ignore[union-attr]
            f"📌 Счёт №{inv['invoice_number']} — на проверке."
        )
        manager_id = inv.get("created_by")
        if manager_id:
            await notifier.safe_send(
                int(manager_id),
                f"📌 ГД: счёт №{inv['invoice_number']} — на проверке."
            )


# =====================================================================
# БУХГАЛТЕРИЯ (ЭДО) (EdoRequestSG)
# =====================================================================

@router.message(F.text == MGR_BTN_EDO)
async def start_edo_request(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES + [Role.RP]):
        return
    await state.clear()

    invoices = await db.list_invoices_for_edo(message.from_user.id)  # type: ignore[union-attr]
    if invoices:
        await state.set_state(EdoRequestSG.invoice_pick)
        await message.answer(
            "📄 <b>Бухгалтерия (ЭДО)</b>\n\n"
            "Выберите счёт:",
            reply_markup=edo_invoice_pick_kb(invoices),
        )
    else:
        # Нет счетов — сразу к типу запроса (ручной ввод)
        await state.set_state(EdoRequestSG.request_type)
        await message.answer(
            "📄 <b>Бухгалтерия (ЭДО)</b>\n\n"
            "У вас нет активных счетов.\n"
            "Выберите тип запроса:",
            reply_markup=edo_type_kb(),
        )


@router.callback_query(EdoRequestSG.invoice_pick, F.data.startswith("edo_inv:"))
async def edo_invoice_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    val = cb.data.split(":", 1)[-1]  # type: ignore[union-attr]
    if val == "manual":
        await state.update_data(edo_invoice_id=None)
    else:
        inv_id = int(val)
        inv = await db.get_invoice(inv_id)
        if inv:
            await state.update_data(
                edo_invoice_id=inv_id,
                invoice_number=inv["invoice_number"],
            )
        else:
            await state.update_data(edo_invoice_id=None)

    await state.set_state(EdoRequestSG.request_type)
    await cb.message.answer(  # type: ignore[union-attr]
        "Выберите тип запроса:",
        reply_markup=edo_type_kb(),
    )


@router.callback_query(EdoRequestSG.request_type, F.data.startswith("edo:"))
async def edo_type_selected(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    edo_type = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(request_type=edo_type, attachments=[])

    data = await state.get_data()
    if edo_type == "other":
        await state.set_state(EdoRequestSG.description)
        await cb.message.answer("Опишите суть запроса:")  # type: ignore[union-attr]
    elif data.get("invoice_number"):
        # Номер счёта уже выбран из пикера — пропускаем ввод
        await state.set_state(EdoRequestSG.comment)
        await cb.message.answer(  # type: ignore[union-attr]
            f"Счёт: <code>{data['invoice_number']}</code>\n\n"
            "Добавьте <b>комментарий</b> (или «—» для пропуска):",
        )
    else:
        await state.set_state(EdoRequestSG.invoice_number)
        await cb.message.answer("Введите <b>номер счёта</b>:")  # type: ignore[union-attr]


@router.message(EdoRequestSG.invoice_number)
async def edo_invoice_number(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите номер счёта:")
        return
    await state.update_data(invoice_number=text)
    await state.set_state(EdoRequestSG.comment)
    await message.answer("Добавьте <b>комментарий</b> (или «—» для пропуска):")


@router.message(EdoRequestSG.description)
async def edo_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(EdoRequestSG.comment)
    await message.answer("Добавьте <b>комментарий</b> (или «—» для пропуска):")


@router.message(EdoRequestSG.comment)
async def edo_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "—":
        comment = ""
    await state.update_data(comment=comment)
    await state.set_state(EdoRequestSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить бухгалтеру", callback_data="edo:create")
    b.button(text="⏭ Без вложений", callback_data="edo:create")
    b.adjust(1)
    await message.answer(
        "Прикрепите файл/фото или нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(EdoRequestSG.attachments)
async def edo_attachments(message: Message, state: FSMContext) -> None:
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
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return

    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "edo:create")
async def edo_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES + [Role.RP]):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    request_type = data["request_type"]
    invoice_number = data.get("invoice_number")
    description = data.get("description")
    comment = data.get("comment", "")
    attachments = data.get("attachments", [])

    acc_id = await resolve_default_assignee(db, config, Role.ACCOUNTING)
    if not acc_id:
        await cb.message.answer("⚠️ Бухгалтер не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    requester_role = await _current_role(db, u.id) or "manager"
    edo_invoice_id = data.get("edo_invoice_id")
    edo_id = await db.create_edo_request(
        request_type=request_type,
        requested_by=u.id,
        requested_by_role=requester_role,
        assigned_to=int(acc_id),
        invoice_number=invoice_number,
        description=description,
        comment=comment,
        invoice_id=edo_invoice_id,
    )

    task = await db.create_task(
        project_id=None,
        type_=TaskType.EDO_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(acc_id),
        due_at_iso=None,
        payload={
            "edo_id": edo_id,
            "edo_type": request_type,
            "invoice_number": invoice_number,
            "description": description,
            "comment": comment,
            "requester_id": u.id,
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

    type_label = {
        "sign_invoice": "Подписать по ЭДО (счет)",
        "sign_closing": "Закрывающие по ЭДО (счет)",
        "sign_upd": "Подписать по ЭДО УПД поставщика",
        "other": "Другое",
    }.get(request_type, request_type)

    initiator = await get_initiator_label(db, u.id)
    msg = (
        f"📄 <b>Запрос ЭДО</b>\n"
        f"👤 От: {initiator}\n\n"
        f"Тип: {type_label}\n"
    )
    if invoice_number:
        msg += f"Счёт №: <code>{invoice_number}</code>\n"
    if description:
        msg += f"Описание: {description}\n"
    if comment:
        msg += f"Комментарий: {comment}\n"

    # Inline button for accountant to respond to EDO request
    b_edo_resp = InlineKeyboardBuilder()
    b_edo_resp.button(text="📄 Ответить на ЭДО", callback_data=f"edo_respond:{task['id']}")
    b_edo_resp.adjust(1)

    await notifier.safe_send(int(acc_id), msg, reply_markup=b_edo_resp.as_markup())
    for a in attachments:
        await notifier.safe_send_media(int(acc_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(acc_id))

    menu_role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос ЭДО отправлен бухгалтеру ({type_label}).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# МОИ СЧЕТА
# =====================================================================

_ROLE_MARKER = {"manager_kia": "КИА", "manager_kv": "КВ", "manager_npn": "НПН"}


@router.message(F.text == MGR_BTN_MY_INVOICES)
async def my_invoices(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return

    user = await db.get_user(message.from_user.id)  # type: ignore[union-attr]
    roles = (user.role or "").split(",")

    # Определяем маркер по суб-роли менеджера (manager_kia → КИА, и т.д.)
    marker = None
    for r in roles:
        if r.strip() in _ROLE_MARKER:
            marker = _ROLE_MARKER[r.strip()]
            break

    if marker:
        invoices = await db.list_invoices(marker=marker)
    else:
        invoices = await db.list_invoices(created_by=message.from_user.id)  # type: ignore[union-attr]

    if not invoices:
        await answer_service(message, "📑 У вас пока нет счетов.", delay_seconds=60)
        return

    await message.answer(
        f"📑 <b>Мои Счета</b> ({len(invoices)}):\n\n"
        "Нажмите на счёт для просмотра:",
        reply_markup=invoice_list_kb(invoices, action_prefix="myinv", back_callback="nav:home"),
    )


@router.callback_query(F.data.startswith("myinv:view:"))
async def my_invoice_view(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = {
        "new": "🆕 Новый",
        "pending": "⏳ Ждёт подтверждения ГД",
        "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен",
        "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

    # --- Montazh stage ---
    _mgr_stage_lbl = {
        "in_work": "🔨 В работе", "razmery_ok": "📐 Размеры ОК",
        "invoice_ok": "✅ Счёт ОК", "none": "⏳ Ожидает",
    }
    stage = inv.get("montazh_stage") or "none"

    # Финансы
    amount = float(inv.get("amount") or 0)
    first_pay = float(inv.get("first_payment_amount") or 0)
    debt = inv.get("outstanding_debt")

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {amount:,.0f}₽\n"
        f"💵 Первый платёж: {first_pay:,.0f}₽\n"
    )
    if debt is not None:
        text += f"🔴 Долг: {float(debt):,.0f}₽\n"
    else:
        calc_debt = amount - first_pay
        if calc_debt > 0:
            text += f"🔴 Долг (расч.): {calc_debt:,.0f}₽\n"
        else:
            text += "🟢 Долг: 0₽\n"
    text += (
        f"📊 Статус: {status_label}\n"
        f"🔧 Этап: {_mgr_stage_lbl.get(stage, stage)}\n"
    )

    area = inv.get("area_m2")
    if area:
        try:
            text += f"📐 Площадь: {float(area):,.1f} м²\n"
        except (ValueError, TypeError):
            pass

    est_install = inv.get("estimated_installation")
    if est_install:
        try:
            text += f"🔧 Расч. стоимость монтажа: {float(est_install):,.0f}₽\n"
        except (ValueError, TypeError):
            pass

    # ZP
    zp_mgr = inv.get("zp_manager_status") or "not_requested"
    zp_inst = inv.get("zp_installer_status") or "not_requested"
    def _zp_badge(status: str) -> str:
        if status == "approved":
            return "✅"
        if status == "requested":
            return "⏳"
        return "—"

    text += f"💸 ЗП менеджер: {_zp_badge(zp_mgr)}  монтажник: {_zp_badge(zp_inst)}\n"

    text += f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"

    if inv.get("client_name"):
        text += f"👤 Клиент: {inv['client_name']}\n"
    if inv.get("description"):
        text += f"💬 Комментарий: {inv['description']}\n"

    # Show close conditions if relevant
    if inv["status"] in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID, InvoiceStatus.CLOSING):
        conditions = await db.check_close_conditions(invoice_id)
        c1 = "✅" if conditions["installer_ok"] else "⏳"
        c2 = "✅" if conditions["edo_signed"] else "⏳"
        c3 = "✅" if conditions["no_debts"] else "⏳"
        c4 = "✅" if conditions["zp_approved"] else "⏳"
        text += (
            f"\n<b>Условия закрытия:</b>\n"
            f"{c1} 1. Монтажник — Счет ОК\n"
            f"{c2} 2. ЭДО — подписано\n"
            f"{c3} 3. Долгов нет\n"
            f"{c4} 4. ЗП — утверждено\n"
        )

    # Кнопки на карточке
    b = InlineKeyboardBuilder()
    if inv["status"] == InvoiceStatus.ENDED:
        b.button(text="📦 Материалы", callback_data=f"mgr_mat:{invoice_id}")
    # Чат с монтажником (если назначен)
    if inv.get("assigned_to"):
        b.button(text="💬 Чат с монтажником", callback_data=f"inv_chat:menu:{invoice_id}")
    if b.export():
        b.adjust(1)
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    else:
        await cb.message.answer(text)  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^mgr_mat:\d+$"))
async def manager_invoice_materials(cb: CallbackQuery, db: Database) -> None:
    """Менеджер: список купленных материалов по закрытому счёту."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.message.answer("⚠️ Счёт не найден.")  # type: ignore[union-attr]
        return
    children = await db.list_child_invoices(inv_id)
    sp_list = await db.list_supplier_payments_for_invoice(inv_id)
    await cb.message.answer(format_materials_list(inv, children, sp_list))  # type: ignore[union-attr]


# =====================================================================
# ПРОБЛЕМА / ВОПРОС (existing Issue flow)
# =====================================================================

@router.message(F.text == MGR_BTN_ISSUE)
async def start_manager_issue(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await state.set_state(IssueSG.project)
    projects = await db.list_recent_projects(limit=20)
    from ..keyboards import projects_kb
    if projects:
        await message.answer(
            "🆘 <b>Проблема / Вопрос</b>\n\n"
            "Шаг 1: Выберите проект (или напишите номер/название):",
            reply_markup=projects_kb(projects, ctx="issue"),
        )
    else:
        await message.answer(
            "🆘 <b>Проблема / Вопрос</b>\n\n"
            "Опишите проблему или вопрос:"
        )
        await state.set_state(IssueSG.description)


# =====================================================================
# ПОИСК СЧЕТА
# =====================================================================

@router.message(
    lambda m: (m.text or "").strip() in {MGR_BTN_SEARCH_INVOICE, "🔍 Поиск Счета", "🔍 Найти Счет №", "🔍 Поиск счёта"}
)
async def search_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES + [Role.RP, Role.ACCOUNTING]):
        return
    await state.clear()
    await state.set_state(InvoiceSearchSG.value)
    await message.answer(
        "🔍 <b>Поиск счёта</b>\n\n"
        "Введите номер счёта или часть адреса для поиска:"
    )


@router.message(InvoiceSearchSG.value)
async def search_invoice_query(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Process search query and show results."""
    if not message.from_user:
        return
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES + [Role.RP, Role.ACCOUNTING]):
        return
    query = (message.text or "").strip()
    if len(query) < 2:
        await message.answer("Введите хотя бы 2 символа для поиска:")
        return

    results = await db.search_invoices(query, limit=15)
    if not results:
        await message.answer(
            f"❌ По запросу «{query}» ничего не найдено.\n\n"
            "Введите другой запрос или нажмите /cancel для отмены."
        )
        return

    b = InlineKeyboardBuilder()
    for inv in results:
        status_emoji = {
            "new": "🆕", "pending": "⏳", "in_progress": "🔄",
            "paid": "✅", "on_hold": "⏸", "rejected": "❌",
            "closing": "📌", "ended": "🏁",
        }.get(inv["status"], "❓")
        label = f"{status_emoji} №{inv['invoice_number']} — {inv.get('object_address', '-')[:25]}"
        b.button(text=label, callback_data=f"srch_inv:view:{inv['id']}")
    b.adjust(1)

    await state.clear()
    await message.answer(
        f"🔍 Найдено: <b>{len(results)}</b>\n\n"
        "Нажмите на счёт для подробной информации:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("srch_inv:view:"))
async def search_invoice_view(cb: CallbackQuery, db: Database) -> None:
    """Show detailed invoice card from search results."""
    if not await require_role_callback(
        cb,
        db,
        roles=ALL_MANAGER_ROLES + [Role.RP, Role.ACCOUNTING],
    ):
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ждёт подтверждения ГД",
        "in_progress": "🔄 В работе", "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен", "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие", "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )

    if inv.get("description"):
        text += f"💬 Комментарий: {inv['description']}\n"

    # Originals info
    primary_h = inv.get("docs_originals_holder")
    closing_h = inv.get("closing_originals_holder")
    if primary_h:
        text += f"📁 Оригиналы первичных: у {'ГД' if primary_h == 'gd' else 'менеджера'}\n"
    if closing_h:
        text += f"📁 Оригиналы закрывающих: у {'ГД' if closing_h == 'gd' else 'менеджера'}\n"

    # Close conditions if relevant
    if inv["status"] in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID,
                          InvoiceStatus.CLOSING):
        conditions = await db.check_close_conditions(invoice_id)
        c1 = "✅" if conditions["installer_ok"] else "⏳"
        c2 = "✅" if conditions["edo_signed"] else "⏳"
        c3 = "✅" if conditions["no_debts"] else "⏳"
        c4 = "✅" if conditions["zp_approved"] else "⏳"
        text += (
            f"\n<b>Условия закрытия:</b>\n"
            f"{c1} 1. Монтажник — Счет ОК\n"
            f"{c2} 2. ЭДО — подписано\n"
            f"{c3} 3. Долгов нет\n"
            f"{c4} 4. ЗП — утверждено\n"
        )

    await cb.message.answer(text)  # type: ignore[union-attr]


# =====================================================================
# ЗАМЕРЫ — структурированные заявки на замер
# =====================================================================

@router.message(F.text == MGR_BTN_ZAMERY)
async def mgr_zamery(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка «📐 Замеры» — дашборд заявок на замер."""
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    user_id = message.from_user.id  # type: ignore[union-attr]
    requests = await db.list_zamery_requests(requested_by=user_id, limit=20)

    b = InlineKeyboardBuilder()
    b.button(text="➕ Новая заявка на замер", callback_data="zam_new:start")
    b.button(text="📅 График замерщика", callback_data="mgr_sched:main")
    if requests:
        n_open = sum(1 for r in requests if r["status"] in ("open", "in_progress"))
        n_done = sum(1 for r in requests if r["status"] == "done")
        text = (
            f"📐 <b>Замеры</b> ({len(requests)})\n"
            f"⏳ Активных: {n_open} | ✅ Завершённых: {n_done}\n\n"
        )
        for req in requests[:10]:
            status_emoji = {"open": "⏳", "in_progress": "🔄", "done": "✅", "rejected": "❌"}.get(req["status"], "❓")
            addr = (req.get("address") or "")[:25]
            b.button(text=f"{status_emoji} #{req['id']} — {addr}"[:55], callback_data=f"zam_req:view:{req['id']}")
    else:
        text = "📐 <b>Замеры</b>\n\nНет заявок. Создайте новую:"
    b.button(text="🔄 Обновить", callback_data="zam_dash:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    await message.answer(text, reply_markup=b.as_markup())


@router.callback_query(F.data == "zam_dash:refresh")
async def zamery_dash_refresh(cb: CallbackQuery, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    user_id = cb.from_user.id
    requests = await db.list_zamery_requests(requested_by=user_id, limit=20)
    b = InlineKeyboardBuilder()
    b.button(text="➕ Новая заявка на замер", callback_data="zam_new:start")
    if requests:
        n_open = sum(1 for r in requests if r["status"] in ("open", "in_progress"))
        n_done = sum(1 for r in requests if r["status"] == "done")
        text = (
            f"📐 <b>Замеры</b> ({len(requests)})\n"
            f"⏳ Активных: {n_open} | ✅ Завершённых: {n_done}\n\n"
        )
        for req in requests[:10]:
            status_emoji = {"open": "⏳", "in_progress": "🔄", "done": "✅", "rejected": "❌"}.get(req["status"], "❓")
            addr = (req.get("address") or "")[:25]
            b.button(text=f"{status_emoji} #{req['id']} — {addr}"[:55], callback_data=f"zam_req:view:{req['id']}")
    else:
        text = "📐 <b>Замеры</b>\n\nНет заявок. Создайте новую:"
    b.button(text="🔄 Обновить", callback_data="zam_dash:refresh")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^zam_req:view:\d+$"))
async def zamery_my_view(cb: CallbackQuery, db: Database) -> None:
    """Менеджер: карточка своей заявки на замер."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    req_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    req = await db.get_zamery_request(req_id)
    if not req:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return
    source_label = ZAMERY_SOURCE_LABELS.get(req["source_type"], req["source_type"])
    status_label = {"open": "⏳ Ожидает", "in_progress": "🔄 В работе", "done": "✅ Выполнено", "rejected": "❌ Отклонено"}.get(req["status"], req["status"])
    text = f"📐 <b>Заявка #{req['id']}</b>\n\n"
    text += f"📍 Адрес: {req['address']}\n"
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
    total_cost = req.get("total_cost")
    if total_cost:
        text += f"💰 Стоимость замера: <b>{total_cost}₽</b>\n"
    if req.get("description"):
        text += f"\n📝 Описание: {req['description']}\n"
    text += f"📌 Источник: {source_label}\n"
    text += f"📊 Статус: {status_label}\n"
    if req.get("response_comment"):
        text += f"\n💬 Ответ замерщика: {req['response_comment']}\n"
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Назад к списку", callback_data="zam_dash:refresh")
    b.adjust(1)
    await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


# --- График замерщика (для менеджера) ---

_MGR_RU_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_MGR_RU_MONTHS = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]
_MGR_BOOK_INTERVALS = [
    "08:00–10:00", "10:00–12:00", "12:00–14:00",
    "14:00–16:00", "16:00–18:00", "18:00–20:00",
]


def _mgr_week_range(base: date, offset: int = 0) -> tuple[date, date]:
    monday = base - timedelta(days=base.weekday()) + timedelta(weeks=offset)
    sunday = monday + timedelta(days=6)
    return monday, sunday


@router.callback_query(F.data == "mgr_sched:main")
async def mgr_schedule_main(cb: CallbackQuery, db: Database, config: Config) -> None:
    """Менеджер: главный экран графика замерщика — 3 недели."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_uid:
        await cb.message.answer("⚠️ Замерщик не найден.")  # type: ignore[union-attr]
        return
    await _render_mgr_schedule(cb, db, int(zamery_uid))


async def _render_mgr_schedule(
    target: CallbackQuery,
    db: Database,
    zamery_uid: int,
) -> None:
    today = date.today()
    text = "📅 <b>График замерщика</b>\n\nВыберите неделю:\n"

    b = InlineKeyboardBuilder()
    for w in range(3):
        mon, sun = _mgr_week_range(today, w)
        d_from, d_to = mon.isoformat(), sun.isoformat()
        zamery = await db.list_zamery_for_schedule(zamery_uid, d_from, d_to)
        blackouts = await db.list_zamery_blackout_dates(zamery_uid, d_from, d_to)
        cnt = len(zamery)
        bl = len(blackouts)
        label = f"{mon.day} {_MGR_RU_MONTHS[mon.month]} — {sun.day} {_MGR_RU_MONTHS[sun.month]}"
        if w == 0:
            label = f"📍 {label}"
        badge = ""
        if cnt > 0:
            badge += f" · 📐{cnt}"
        if bl > 0:
            badge += f" · 🚫{bl}"
        if cnt == 0 and bl == 0:
            badge = " · свободна"
        b.button(text=f"{label}{badge}", callback_data=f"mgr_sched:week:{w}")

    b.button(text="⬅️ Назад", callback_data="zam_dash:refresh")
    b.adjust(1)

    try:
        await target.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await target.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("mgr_sched:week:"))
async def mgr_schedule_week(cb: CallbackQuery, db: Database, config: Config) -> None:
    """Менеджер: недельный вид с кнопками записи."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    week_offset = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_uid:
        await cb.message.answer("⚠️ Замерщик не найден.")  # type: ignore[union-attr]
        return
    z_uid = int(zamery_uid)

    today = date.today()
    mon, sun = _mgr_week_range(today, week_offset)
    d_from, d_to = mon.isoformat(), sun.isoformat()

    zamery = await db.list_zamery_for_schedule(z_uid, d_from, d_to)
    blackouts = await db.list_zamery_blackout_dates(z_uid, d_from, d_to)

    zam_by_date: dict[str, list[dict]] = {}
    for z in zamery:
        zam_by_date.setdefault(z["scheduled_date"], []).append(z)
    blackout_set = {bl["blackout_date"] for bl in blackouts}

    text = f"📅 <b>{mon.day} {_MGR_RU_MONTHS[mon.month]} — {sun.day} {_MGR_RU_MONTHS[sun.month]}</b>\n\n"

    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        wd = _MGR_RU_WEEKDAYS[day.weekday()]
        label = f"{day.day} {_MGR_RU_MONTHS[day.month]} ({wd})"

        if ds in blackout_set:
            text += f"🚫 <b>{label}</b> — выходной\n"
        elif ds in zam_by_date:
            intervals = [z.get("scheduled_time_interval") or "—" for z in zam_by_date[ds]]
            text += f"🔴 <b>{label}</b> — {len(zam_by_date[ds])} замер(ов): {', '.join(intervals)}\n"
        else:
            if day < today:
                text += f"▫️ <b>{label}</b>\n"
            else:
                text += f"🟢 <b>{label}</b> — свободен\n"
        text += "\n"

    b = InlineKeyboardBuilder()
    # Кнопки свободных дней
    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        if day >= today and ds not in blackout_set:
            wd = _MGR_RU_WEEKDAYS[day.weekday()]
            if ds not in zam_by_date:
                b.button(
                    text=f"🟢 {day.day} {_MGR_RU_MONTHS[day.month]} ({wd}) — записать",
                    callback_data=f"mgr_sched:book:{ds}:{week_offset}",
                )
            else:
                b.button(
                    text=f"📐 {day.day} {_MGR_RU_MONTHS[day.month]} ({wd}) — доп. замер",
                    callback_data=f"mgr_sched:book:{ds}:{week_offset}",
                )

    if week_offset > 0:
        b.button(text="⬅️ Пред. неделя", callback_data=f"mgr_sched:week:{week_offset - 1}")
    if week_offset < 4:
        b.button(text="След. неделя ➡️", callback_data=f"mgr_sched:week:{week_offset + 1}")
    b.button(text="⬅️ К списку недель", callback_data="mgr_sched:main")
    b.adjust(1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("mgr_sched:book:"))
async def mgr_book_pick_time(cb: CallbackQuery, db: Database, config: Config) -> None:
    """Менеджер: выбор интервала для записи замера."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    parts = (cb.data or "").split(":")
    if len(parts) < 4:
        return
    ds = parts[2]
    week_offset = int(parts[3])

    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    z_uid = int(zamery_uid) if zamery_uid else 0

    d = date.fromisoformat(ds)
    wd = _MGR_RU_WEEKDAYS[d.weekday()]

    summary = await db.get_zamery_schedule_summary(z_uid, ds, ds)
    busy_intervals = summary["busy"].get(ds, [])

    text = (
        f"┌─────────────────────────\n"
        f"│ 📐 <b>Записать замер</b>\n"
        f"├─────────────────────────\n"
        f"│ 📅 {d.day} {_MGR_RU_MONTHS[d.month]} ({wd})\n"
    )
    if busy_intervals:
        text += f"│ ⚠️ Занято: {', '.join(busy_intervals)}\n"
    text += "└─────────────────────────\n\nВыберите интервал:"

    b = InlineKeyboardBuilder()
    for interval in _MGR_BOOK_INTERVALS:
        icon = "🔴" if interval in busy_intervals else "🟢"
        b.button(text=f"{icon} {interval}", callback_data=f"mgr_sched:time:{ds}:{interval}:{week_offset}")
    b.button(text="⬅️ Назад к неделе", callback_data=f"mgr_sched:week:{week_offset}")
    b.adjust(2, 2, 2, 1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("mgr_sched:time:"))
async def mgr_book_start_full_flow(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Менеджер: интервал выбран → запуск полного flow заявки на замер."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    parts = (cb.data or "").split(":")
    if len(parts) < 4:
        return
    ds = parts[2]
    interval = parts[3]

    d = date.fromisoformat(ds)
    wd = _MGR_RU_WEEKDAYS[d.weekday()]

    # Pre-fill date/time into FSM and start normal zamery request flow
    await state.clear()
    await state.set_state(ZameryRequestSG.source_type)
    await state.update_data(
        attachments=[],
        scheduled_date=ds,
        scheduled_time_interval=interval,
    )

    from ..keyboards import zamery_source_kb
    await cb.message.answer(  # type: ignore[union-attr]
        f"📐 <b>Заявка на замер</b>\n"
        f"📅 {d.day} {_MGR_RU_MONTHS[d.month]} ({wd})  ⏰ {interval}\n\n"
        f"Выберите источник:",
        reply_markup=zamery_source_kb(),
    )


# --- Замер: создание новой заявки (FSM) ---

@router.callback_query(F.data == "zam_new:start")
async def zamery_new_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(ZameryRequestSG.source_type)
    await state.update_data(attachments=[])
    await cb.message.answer(  # type: ignore[union-attr]
        "📐 <b>Новая заявка на замер</b>\n\n"
        "Выберите источник:",
        reply_markup=zamery_source_kb(),
    )


@router.callback_query(ZameryRequestSG.source_type, F.data.startswith("zam_src:"))
async def zamery_source_selected(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    source = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(source_type=source)

    if source == "lead":
        user_id = cb.from_user.id
        lead_tasks = await db.list_open_lead_tasks_for_manager(user_id, limit=15)
        if not lead_tasks:
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Нет открытых лидов от РП.\nВыберите другой источник:",
                reply_markup=zamery_source_kb(),
            )
            return
        await state.set_state(ZameryRequestSG.lead_pick)
        await cb.message.answer(  # type: ignore[union-attr]
            "Выберите лид для привязки:",
            reply_markup=zamery_lead_pick_kb(lead_tasks),
        )
    else:
        await state.set_state(ZameryRequestSG.address)
        await cb.message.answer("Введите <b>адрес</b> замера:")  # type: ignore[union-attr]


@router.callback_query(ZameryRequestSG.lead_pick, F.data.startswith("zam_lead:"))
async def zamery_lead_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    task_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await db.get_task(task_id)
    payload = try_json_loads(task.get("payload_json")) if task else {}
    lead_id = payload.get("lead_id")
    await state.update_data(lead_task_id=task_id, lead_id=lead_id)
    await state.set_state(ZameryRequestSG.address)
    await cb.message.answer("Введите <b>адрес</b> замера:")  # type: ignore[union-attr]


@router.message(ZameryRequestSG.address)
async def zamery_address(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Введите адрес (минимум 3 символа):")
        return
    await state.update_data(address=text)
    await state.set_state(ZameryRequestSG.description)
    await message.answer("Введите <b>описание</b> работ:")


@router.message(ZameryRequestSG.description)
async def zamery_description(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите подробнее:")
        return
    await state.update_data(description=text)
    await state.set_state(ZameryRequestSG.client_contact)
    await message.answer("Введите <b>контакт клиента</b> (телефон/имя):")


@router.message(ZameryRequestSG.client_contact)
async def zamery_client_contact(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите контакт клиента:")
        return
    await state.update_data(client_contact=text)
    await state.set_state(ZameryRequestSG.mkad_km)
    await message.answer(
        "📍 Введите <b>расстояние от МКАД</b> в км\n"
        "(0 — если внутри МКАД):",
    )


@router.message(ZameryRequestSG.mkad_km)
async def zamery_mkad_km(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    text = (message.text or "").strip().replace(",", ".")
    try:
        km = float(text)
        if km < 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число ≥ 0 (км от МКАД):")
        return
    await state.update_data(mkad_km=km)

    # Если дата уже выбрана (из графика) — пропустить пикер
    data = await state.get_data()
    if data.get("scheduled_date") and data.get("scheduled_time_interval"):
        await state.set_state(ZameryRequestSG.volume_m2)
        await message.answer("📐 Введите <b>примерный объём</b> (площадь) в м²:")
        return

    # Show zamerschik schedule for date picking
    zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_uid:
        await state.set_state(ZameryRequestSG.volume_m2)
        await message.answer("📐 Введите <b>примерный объём</b> (площадь) в м²:")
        return

    await state.update_data(zamery_uid=int(zamery_uid))
    await state.set_state(ZameryRequestSG.pick_schedule_date)
    await _show_schedule_date_picker(message, db, int(zamery_uid))


# --- Schedule date/time picker for manager ---

_RU_WEEKDAYS_M = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_RU_MONTHS_M = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]

_TIME_INTERVALS = [
    "09:00–12:00",
    "12:00–15:00",
    "15:00–18:00",
    "18:00–21:00",
]


async def _show_schedule_date_picker(
    target: Message,
    db: Database,
    zamery_uid: int,
) -> None:
    """Show 2-week zamerschik schedule with date pick buttons."""
    today = date.today()
    d_from = today.isoformat()
    d_to = (today + timedelta(days=13)).isoformat()

    summary = await db.get_zamery_schedule_summary(zamery_uid, d_from, d_to)
    busy = summary["busy"]  # date_str → [intervals]
    blackout_set = summary["blackout_set"]  # set of date_str

    text = "📅 <b>График замерщика</b> (2 недели)\n\n"
    text += "Выберите дату замера:\n\n"

    b = InlineKeyboardBuilder()
    for i in range(14):
        d = today + timedelta(days=i)
        ds = d.isoformat()
        wd = _RU_WEEKDAYS_M[d.weekday()]
        label = f"{d.day} {_RU_MONTHS_M[d.month]} ({wd})"

        if ds in blackout_set:
            text += f"🚫 {label} — <b>выходной</b>\n"
            # No button for blackout days
        elif ds in busy:
            intervals = busy[ds]
            cnt = len(intervals)
            text += f"🔴 {label} — {cnt} замер(ов): {', '.join(intervals)}\n"
            # Still allow picking busy days (different interval)
            b.button(text=f"🔴 {d.day} {_RU_MONTHS_M[d.month]} ({wd})", callback_data=f"zamsched_mgr:date:{ds}")
        else:
            text += f"🟢 {label} — свободен\n"
            b.button(text=f"🟢 {d.day} {_RU_MONTHS_M[d.month]} ({wd})", callback_data=f"zamsched_mgr:date:{ds}")

    b.button(text="⏭ Пропустить", callback_data="zamsched_mgr:skip")
    b.adjust(2, 2, 2, 2, 2, 2, 2, 1)
    await target.answer(text, reply_markup=b.as_markup())


@router.callback_query(ZameryRequestSG.pick_schedule_date, F.data.startswith("zamsched_mgr:date:"))
async def zamery_pick_date(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Manager picks a date from the schedule."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    ds = cb.data.split(":")[-1]  # type: ignore[union-attr]
    data = await state.get_data()
    zamery_uid = data.get("zamery_uid")
    await state.update_data(scheduled_date=ds)
    await state.set_state(ZameryRequestSG.pick_schedule_time)

    # Show busy intervals for this date
    d = date.fromisoformat(ds)
    d_from = ds
    d_to = ds
    busy_intervals: list[str] = []
    if zamery_uid:
        summary = await db.get_zamery_schedule_summary(zamery_uid, d_from, d_to)
        busy_intervals = summary["busy"].get(ds, [])

    wd = _RU_WEEKDAYS_M[d.weekday()]
    text = f"📅 <b>{d.day} {_RU_MONTHS_M[d.month]} ({wd})</b>\n\n"
    if busy_intervals:
        text += f"⚠️ Занятые интервалы: {', '.join(busy_intervals)}\n\n"
    text += "Выберите временной интервал:"

    b = InlineKeyboardBuilder()
    for interval in _TIME_INTERVALS:
        if interval in busy_intervals:
            b.button(text=f"🔴 {interval}", callback_data=f"zamsched_mgr:time:{interval}")
        else:
            b.button(text=f"🟢 {interval}", callback_data=f"zamsched_mgr:time:{interval}")
    b.button(text="⬅️ Назад к датам", callback_data="zamsched_mgr:back_dates")
    b.adjust(2, 2, 1)

    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(ZameryRequestSG.pick_schedule_time, F.data.startswith("zamsched_mgr:time:"))
async def zamery_pick_time(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Manager picks a time interval."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()

    interval = cb.data.split(":", 2)[-1]  # type: ignore[union-attr]
    await state.update_data(scheduled_time_interval=interval)
    await state.set_state(ZameryRequestSG.volume_m2)

    data = await state.get_data()
    ds = data.get("scheduled_date", "")
    try:
        d = date.fromisoformat(ds)
        wd = _RU_WEEKDAYS_M[d.weekday()]
        date_label = f"{d.day} {_RU_MONTHS_M[d.month]} ({wd})"
    except Exception:
        date_label = ds

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Замер запланирован: <b>{date_label}</b>, {interval}\n\n"
        "📐 Введите <b>примерный объём</b> (площадь) в м²:",
    )


@router.callback_query(ZameryRequestSG.pick_schedule_date, F.data == "zamsched_mgr:skip")
async def zamery_skip_schedule(cb: CallbackQuery, state: FSMContext) -> None:
    """Skip schedule picking."""
    await cb.answer()
    await state.set_state(ZameryRequestSG.volume_m2)
    await cb.message.answer("📐 Введите <b>примерный объём</b> (площадь) в м²:")  # type: ignore[union-attr]


@router.callback_query(ZameryRequestSG.pick_schedule_time, F.data == "zamsched_mgr:back_dates")
async def zamery_back_to_dates(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    """Back to date picker."""
    await cb.answer()
    data = await state.get_data()
    zamery_uid = data.get("zamery_uid")
    if not zamery_uid:
        zamery_uid = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_uid:
        await state.set_state(ZameryRequestSG.volume_m2)
        await cb.message.answer("📐 Введите <b>примерный объём</b> (площадь) в м²:")  # type: ignore[union-attr]
        return
    await state.set_state(ZameryRequestSG.pick_schedule_date)
    await _show_schedule_date_picker(cb.message, db, int(zamery_uid))  # type: ignore[arg-type]


@router.message(ZameryRequestSG.volume_m2)
async def zamery_volume(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".")
    try:
        vol = float(text)
        if vol <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите число > 0 (объём в м²):")
        return
    data = await state.get_data()
    mkad_km = data.get("mkad_km", 0)
    base_cost = 2500
    mkad_surcharge = max(0, int((mkad_km - 5) * 40)) if mkad_km > 5 else 0
    total_cost = base_cost + mkad_surcharge
    await state.update_data(
        volume_m2=vol,
        base_cost=base_cost,
        mkad_surcharge=mkad_surcharge,
        total_cost=total_cost,
    )
    cost_text = f"💰 Стоимость замера: <b>{total_cost}₽</b>"
    if mkad_surcharge:
        cost_text += f" (база 2500₽ + МКАД {mkad_surcharge}₽)"
    await state.set_state(ZameryRequestSG.attachments)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить замерщику", callback_data="zam:create")
    b.button(text="⏭ Без вложений", callback_data="zam:create")
    b.adjust(1)
    await message.answer(
        f"{cost_text}\n\n"
        "Прикрепите файл/фото или нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(ZameryRequestSG.attachments)
async def zamery_attachments(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    attachments: list[dict[str, Any]] = data.get("attachments", [])
    if message.document:
        attachments.append({"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "caption": message.caption})
    elif message.photo:
        ph = message.photo[-1]
        attachments.append({"file_type": "photo", "file_id": ph.file_id, "file_unique_id": ph.file_unique_id, "caption": message.caption})
    else:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    await state.update_data(attachments=attachments)
    await answer_service(message, f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "zam:create")
async def zamery_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    source_type = data["source_type"]
    address = data["address"]
    description = data.get("description")
    client_contact = data.get("client_contact")
    attachments = data.get("attachments", [])
    lead_id = data.get("lead_id")
    lead_task_id = data.get("lead_task_id")

    zamery_id_user = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_id_user:
        await cb.message.answer("⚠️ Замерщик не найден.")  # type: ignore[union-attr]
        await state.clear()
        return

    requester_role = await _current_role(db, u.id) or "manager_kv"
    mkad_km = data.get("mkad_km", 0)
    volume_m2 = data.get("volume_m2")
    base_cost = data.get("base_cost", 2500)
    mkad_surcharge = data.get("mkad_surcharge", 0)
    total_cost = data.get("total_cost", 2500)
    scheduled_date = data.get("scheduled_date")
    scheduled_time_interval = data.get("scheduled_time_interval")

    import json
    zam_req_id = await db.create_zamery_request(
        source_type=source_type,
        address=address,
        description=description,
        client_contact=client_contact,
        requested_by=u.id,
        requester_role=requester_role,
        assigned_to=int(zamery_id_user),
        lead_id=lead_id,
        lead_task_id=lead_task_id,
        attachments_json=json.dumps([{"file_id": a["file_id"], "file_type": a["file_type"]} for a in attachments]) if attachments else None,
        mkad_km=mkad_km,
        volume_m2=volume_m2,
        base_cost=base_cost,
        mkad_surcharge=mkad_surcharge,
        total_cost=total_cost,
    )
    # Save scheduled date/time
    if scheduled_date:
        await db.update_zamery_request(
            zam_req_id,
            scheduled_date=scheduled_date,
            scheduled_time_interval=scheduled_time_interval,
        )

    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZAMERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(zamery_id_user),
        due_at_iso=None,
        payload={
            "zamery_request_id": zam_req_id,
            "source_type": source_type,
            "address": address,
            "description": description,
            "client_contact": client_contact,
            "mkad_km": mkad_km,
            "volume_m2": volume_m2,
            "total_cost": total_cost,
        },
    )
    await db.update_zamery_request(zam_req_id, task_id=int(task["id"]))

    for a in attachments:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
        )

    source_label = ZAMERY_SOURCE_LABELS.get(source_type, source_type)
    role_short = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(requester_role, "")
    initiator = await get_initiator_label(db, u.id)
    from ..keyboards import task_actions_kb
    task_kb = task_actions_kb(task)
    msg = (
        f"📐 <b>Заявка на замер #{zam_req_id}</b>\n\n"
        f"👤 Менеджер: {initiator}"
    )
    if role_short:
        msg += f" ({role_short})"
    msg += (
        f"\n📍 Адрес: {address}\n"
    )
    if client_contact:
        msg += f"📞 Контакт: <code>{client_contact}</code>\n"
    if volume_m2:
        msg += f"📊 Объём: {volume_m2} м²\n"
    if mkad_km and mkad_km > 0:
        if mkad_surcharge:
            msg += f"📍 МКАД: {mkad_km} км (наценка: {mkad_surcharge}₽)\n"
        else:
            msg += f"📍 МКАД: {mkad_km} км\n"
    else:
        msg += "📍 МКАД: внутри МКАД\n"
    msg += f"💰 Стоимость замера: <b>{total_cost}₽</b>\n"
    if scheduled_date:
        try:
            sd = date.fromisoformat(scheduled_date)
            _wd = _RU_WEEKDAYS_M[sd.weekday()]
            msg += f"📅 Дата: <b>{sd.day} {_RU_MONTHS_M[sd.month]} ({_wd})</b>"
            if scheduled_time_interval:
                msg += f" ⏰ {scheduled_time_interval}"
            msg += "\n"
        except Exception:
            pass
    if description:
        msg += f"\n📝 Описание: {description}\n"
    msg += f"📌 Источник: {source_label}\n"

    await notifier.safe_send(int(zamery_id_user), msg, reply_markup=task_kb)
    for a in attachments:
        await notifier.safe_send_media(int(zamery_id_user), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(zamery_id_user))

    # Уведомить РП о новой заявке на замер (все источники)
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        rp_msg = f"📐 <b>Заявка на замер</b> #{zam_req_id}\n"
        rp_msg += f"👤 От: {initiator}\n"
        rp_msg += f"📌 Источник: {source_label}\n"
        if source_type == "lead" and lead_task_id:
            rp_msg += f"🎯 Лид #{lead_task_id}\n"
        rp_msg += f"\n📍 Адрес: {address}\n"
        if description:
            rp_msg += f"📝 {description}\n"
        if client_contact:
            rp_msg += f"📞 Контакт: {client_contact}\n"
        await notifier.safe_send(int(rp_id), rp_msg)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    menu_role, isolated_role = await _current_menu(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Заявка на замер #{zam_req_id} отправлена замерщику.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(
                menu_role,
                is_admin=u.id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(u.id),
                isolated_role=isolated_role,
            ),
        ),
    )


# =====================================================================
# МЕНЕДЖЕР (КРЕД) — chat-proxy mirror
# =====================================================================

@router.message(F.text == MGR_BTN_CRED)
async def mgr_cred_chat(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    role = await _current_role(db, message.from_user.id)  # type: ignore[union-attr]
    channel = _cred_channel(role or "manager_kv")

    cred_label = {
        "manager_kv": "КВ Кред",
        "manager_kia": "КИА Кред",
        "manager_npn": "НПН Кред",
    }.get(channel, "Кред")

    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel=channel)
    await message.answer(
        f"💬 <b>{cred_label}</b>\n\n"
        "Выберите действие:",
        reply_markup=manager_chat_submenu("⬅️ Назад"),
    )


# =====================================================================
# МОНТАЖНАЯ ГР. / ЧАТ С РП — chat-proxy with invoice binding
# =====================================================================

async def _chat_proxy_invoice_pick(
    message: Message, state: FSMContext, db: Database,
    channel: str, title: str, emoji: str,
) -> None:
    """Show invoice picker before entering chat-proxy."""
    uid = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_invoices_for_user(uid, limit=50)
    active = [i for i in invoices if i.get("status") not in ("ended", "cancelled")]

    b = InlineKeyboardBuilder()
    for inv in active:
        num = inv.get("invoice_number", "?")
        addr = inv.get("object_address", "")[:25]
        label = f"📄 {num}"
        if addr:
            label += f" · {addr}"
        b.button(text=label, callback_data=f"mgrchat:{channel}:{inv['id']}")
    b.button(text="💬 Без привязки к счёту", callback_data=f"mgrchat:{channel}:0")
    b.button(text="⬅️ Назад", callback_data="mgrchat:cancel")
    b.adjust(1)

    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel=channel)
    count = len(active)
    await message.answer(
        f"{emoji} <b>{title}</b>\n\n"
        f"Привязать к счёту? ({count} в работе)",
        reply_markup=b.as_markup(),
    )


@router.message(F.text == MGR_BTN_MONTAZH)
async def mgr_montazh_chat(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await _chat_proxy_invoice_pick(message, state, db, "montazh", "Монтажная гр.", "🔧")


@router.message(F.text == MGR_BTN_CHAT_RP)
async def mgr_rp_chat(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await _chat_proxy_invoice_pick(message, state, db, "rp", "Чат с РП", "📋")


@router.callback_query(F.data.startswith("mgrchat:"))
async def mgr_chat_invoice_picked(cb: CallbackQuery, state: FSMContext) -> None:
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    channel = parts[1]
    invoice_id = parts[2]

    if invoice_id == "cancel":
        await state.clear()
        await cb.message.delete()  # type: ignore[union-attr]
        await cb.answer()
        return

    await state.set_state(ManagerChatProxySG.menu)
    inv_ref = ""
    if invoice_id != "0":
        await state.update_data(channel=channel, invoice_id=int(invoice_id))
        inv_ref = f" (счёт #{invoice_id})"
    else:
        await state.update_data(channel=channel, invoice_id=None)

    title = "Монтажная гр." if channel == "montazh" else "Чат с РП"
    await cb.message.edit_text(  # type: ignore[union-attr]
        f"💬 <b>{title}</b>{inv_ref}\n\n"
        "Выберите действие:",
    )
    await cb.message.answer(  # type: ignore[union-attr]
        "Выберите действие:",
        reply_markup=manager_chat_submenu("⬅️ Назад"),
    )
    await cb.answer()


# =====================================================================
# CHAT-PROXY SUBMENU HANDLERS (for manager chat-proxy)
# =====================================================================

@router.message(ManagerChatProxySG.menu, F.text == "📖 Переписка")
async def mgr_chat_history(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    limit = config.chat_history_limit
    messages_list = await db.list_chat_messages(channel, limit=limit)
    if not messages_list:
        await message.answer("Пока нет сообщений в этом чате.")
        return
    lines = []
    for m in messages_list:
        sender = m.get("sender_id", "?")
        text = m.get("text", "")
        ts = m.get("created_at", "")[:16]
        direction = m.get("direction", "")
        arrow = "→" if direction == "outgoing" else "←"
        lines.append(f"<b>{sender}</b> {arrow} ({ts}):\n{text}")
    await message.answer("\n\n".join(lines[-10:]))


@router.message(ManagerChatProxySG.menu, F.text == "✏️ Написать")
async def mgr_chat_write(message: Message, state: FSMContext) -> None:
    await state.set_state(ManagerChatProxySG.writing)
    await message.answer("Напишите сообщение:")


@router.message(ManagerChatProxySG.writing)
async def mgr_chat_writing(
    message: Message, state: FSMContext, db: Database, config: Config, notifier: Notifier
) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    channel = data.get("channel", "")

    text = (message.text or "").strip()
    if not text and not message.document and not message.photo:
        await message.answer("Отправьте текст, файл или фото:")
        return

    # Save chat message (with invoice binding if selected)
    linked_invoice_id = data.get("invoice_id")
    await db.save_chat_message(
        channel=channel,
        sender_id=message.from_user.id,
        direction="outgoing",
        text=text or "[файл/фото]",
        tg_message_id=message.message_id,
        has_attachment=bool(message.document or message.photo),
        invoice_id=linked_invoice_id if linked_invoice_id else None,
    )

    # Determine target by channel
    if channel == "montazh":
        from .chat_proxy import resolve_channel_target

        target_id = await resolve_channel_target(channel, db, config)
    else:
        target_role = _CHAT_TARGET_MAP.get(channel, Role.GD)
        target_id = await resolve_default_assignee(db, config, target_role)

    if target_id:
        channel_label = _CHAT_CHANNEL_LABEL.get(channel, channel)
        role = await _current_role(db, message.from_user.id)
        sender_label = {
            "manager_kv": "Менеджер КВ",
            "manager_kia": "Менеджер КИА",
            "manager_npn": "Менеджер НПН",
            "rp": "РП",
        }.get(role or "", message.from_user.full_name or "Сотрудник")

        fwd_text = (
            f"💬 <b>{channel_label}</b>\n\n"
            f"От: {sender_label} (@{message.from_user.username or '-'})\n\n"
            f"{text}"
        )
        await notifier.safe_send(int(target_id), fwd_text)

        if message.document:
            await notifier.safe_send_media(int(target_id), "document", message.document.file_id, caption=message.caption)
        elif message.photo:
            await notifier.safe_send_media(int(target_id), "photo", message.photo[-1].file_id, caption=message.caption)

    await state.set_state(ManagerChatProxySG.menu)
    await message.answer(
        "✅ Сообщение отправлено.",
        reply_markup=manager_chat_submenu("⬅️ Назад"),
    )


@router.message(ManagerChatProxySG.menu, F.text == "📋 Задачи")
async def mgr_chat_tasks(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    channel = data.get("channel", "")
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=50)
    channel_tasks = [
        task
        for task in tasks
        if try_json_loads(task.get("payload_json")).get("source") == f"chat_proxy:{channel}"
    ]
    if not channel_tasks:
        await message.answer("Задач по этому каналу нет ✅")
        return
    await message.answer(
        f"📋 Задачи канала ({len(channel_tasks)}):",
        reply_markup=tasks_kb(channel_tasks),
    )


@router.message(ManagerChatProxySG.menu, F.text == "📊 Отчёт")
async def mgr_chat_report(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    summary = await db.get_finance_summary(channel)
    total = summary.get("total", 0.0)
    entries = summary.get("entries", [])

    text = f"📊 <b>Финансовый отчёт ({channel})</b>\n\n💰 Итого: {total:,.0f}₽\n"
    if entries:
        text += "\nПоследние операции:\n"
        for e in entries[:5]:
            text += f"• {e.get('amount', 0):,.0f}₽ — {e.get('description', '-')}\n"

    await message.answer(text)


@router.message(ManagerChatProxySG.menu, F.text == "⬅️ Назад")
async def mgr_chat_back(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not message.from_user:
        return
    _uid_back = message.from_user.id
    menu_role, isolated_role = await _current_menu(db, _uid_back)
    is_admin = _uid_back in (config.admin_ids or set())
    rp_t_back = await db.count_rp_role_tasks(_uid_back)
    rp_m_back = await db.count_rp_role_messages(_uid_back)
    await message.answer(
        "Выберите действие:",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                menu_role,
                is_admin=is_admin,
                unread=await db.count_unread_tasks(_uid_back),
                isolated_role=isolated_role,
                rp_tasks=rp_t_back,
                rp_messages=rp_m_back,
            ),
        ),
    )


# =====================================================================
# ЗАПРОС ЗП МЕНЕДЖЕРА (ManagerZpSG)
# =====================================================================

_MGR_ROLES = [Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN]


@router.message(F.text == MGR_BTN_ZP)
async def manager_zp_start(message: Message, state: FSMContext, db: Database) -> None:
    """Show ended invoices eligible for manager ZP request."""
    # If user is in installer menu, delegate to installer handler
    if message.from_user:
        _u = await db.get_user_optional(message.from_user.id)
        if _u and _u.role:
            _menu_role = resolve_active_menu_role(message.from_user.id, _u.role)
            if _menu_role == Role.INSTALLER:
                from .installer_new import installer_zp_start
                return await installer_zp_start(message, state, db)
    if not await require_role_message(message, db, roles=_MGR_ROLES):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    cur = await db.conn.execute(
        "SELECT * FROM invoices "
        "WHERE status = 'ended' "
        "  AND (zp_manager_status IS NULL OR zp_manager_status = 'not_requested') "
        "  AND created_by = ? "
        "ORDER BY id DESC LIMIT 20",
        (user_id,),
    )
    rows = await cur.fetchall()
    invoices = [dict(r) for r in rows]
    if not invoices:
        await message.answer("✅ Нет счетов, по которым можно запросить ЗП.\n"
                             "(Счёт должен иметь статус «Счёт End»)")
        return

    # Check plan/fact for each invoice — filter out those where fact > plan
    eligible: list[dict] = []
    blocked: list[dict] = []
    for inv in invoices:
        pf = await db.get_plan_fact_card(inv["id"])
        if not pf["has_estimated"]:
            # No estimated data — allow (legacy invoices)
            eligible.append(inv)
        elif pf["zp_allowed"]:
            eligible.append(inv)
        else:
            blocked.append(inv)

    if not eligible and not blocked:
        await message.answer("✅ Нет счетов, по которым можно запросить ЗП.\n"
                             "(Счёт должен иметь статус «Счёт End»)")
        return

    b = InlineKeyboardBuilder()
    for inv in eligible:
        label = f"№{inv['invoice_number'] or '—'} / {(inv.get('object_address') or '—')[:30]}"
        b.button(text=label, callback_data=f"mgrzp:pick:{inv['id']}")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    text_parts = ["💰 <b>Запрос ЗП</b>\n"]
    if eligible:
        text_parts.append("Выберите счёт (статус «Счёт End»):")
    else:
        text_parts.append("⚠️ Нет счетов, доступных для запроса ЗП.")

    if blocked:
        text_parts.append(
            f"\n❌ <b>Заблокировано ({len(blocked)}):</b> "
            "фактическая себестоимость превышает расчётную:"
        )
        for inv in blocked:
            pf = await db.get_plan_fact_card(inv["id"])
            text_parts.append(
                f"  • №{inv['invoice_number']} — "
                f"план {pf['estimated_total_cost']:,.0f}₽, "
                f"факт {pf['actual_total_cost']:,.0f}₽"
            )

    if eligible:
        await state.set_state(ManagerZpSG.select_invoice)
    await message.answer(
        "\n".join(text_parts),
        reply_markup=b.as_markup() if eligible else None,
    )


@router.callback_query(F.data.startswith("mgrzp:pick:"), ManagerZpSG.select_invoice)
async def manager_zp_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    # Double-check plan/fact condition
    pf = await db.get_plan_fact_card(invoice_id)
    if pf["has_estimated"] and not pf["zp_allowed"]:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ <b>ЗП заблокирована</b>\n\n"
            f"Счёт №{inv['invoice_number']}\n"
            f"Фактическая себестоимость ({pf['actual_total_cost']:,.0f}₽) "
            f"превышает расчётную ({pf['estimated_total_cost']:,.0f}₽).\n\n"
            "Обратитесь к ГД.",
        )
        await state.clear()
        return

    # Auto-calculate ZP from estimated profit split
    if pf["has_estimated"] and pf["manager_zp"] > 0:
        auto_amount = pf["manager_zp"]
        src = pf.get("client_source", "own")
        src_label = "Лид ГД (75/25)" if src == "gd_lead" else "Мой клиент (50/50)"
        await state.update_data(zp_invoice_id=invoice_id, zp_amount=auto_amount)
        await state.set_state(ManagerZpSG.confirm)
        b = InlineKeyboardBuilder()
        b.button(text="✅ Отправить", callback_data="mgrzp:confirm")
        b.button(text="❌ Отмена", callback_data="mgrzp:cancel")
        b.adjust(2)
        await cb.message.answer(  # type: ignore[union-attr]
            f"💰 <b>ЗП рассчитана автоматически</b>\n\n"
            f"🔢 Счёт: №{inv['invoice_number']}\n"
            f"📍 Адрес: {inv.get('object_address') or '—'}\n"
            f"🔗 Источник: {src_label}\n\n"
            f"📊 Расч.прибыль: {pf['estimated_profit']:,.0f}₽\n"
            f"  ЗП РП (10%): {pf['rp_zp']:,.0f}₽\n"
            f"  <b>Ваша доля: {auto_amount:,.0f}₽</b>\n\n"
            "Отправить запрос ГД?",
            reply_markup=b.as_markup(),
        )
    else:
        # Legacy: no estimated data — manual entry
        await state.update_data(zp_invoice_id=invoice_id)
        await state.set_state(ManagerZpSG.amount)
        await cb.message.answer(  # type: ignore[union-attr]
            f"💰 Счёт: <b>№{inv['invoice_number']}</b>\n"
            f"📍 Адрес: {inv.get('object_address') or '—'}\n\n"
            "Введите сумму ЗП (число):",
        )


@router.message(ManagerZpSG.amount)
async def manager_zp_amount(message: Message, state: FSMContext, db: Database) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите корректную сумму (положительное число):")
        return
    data = await state.get_data()
    invoice_id = data["zp_invoice_id"]
    inv = await db.get_invoice(invoice_id)
    await state.update_data(zp_amount=amount)
    await state.set_state(ManagerZpSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data="mgrzp:confirm")
    b.button(text="❌ Отмена", callback_data="mgrzp:cancel")
    b.adjust(2)
    await message.answer(
        f"💰 <b>Подтверждение запроса ЗП</b>\n\n"
        f"🔢 Счёт: №{inv['invoice_number'] if inv else '—'}\n"
        f"💵 Сумма: {amount:,.0f}₽\n\n"
        "Отправить запрос ГД?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "mgrzp:cancel")
async def manager_zp_cancel(cb: CallbackQuery, state: FSMContext, db: Database, config: Config) -> None:
    await cb.answer("Отменено")
    await state.clear()
    u = cb.from_user
    user = await db.get_user_optional(u.id) if u else None
    role = user.role if user else None
    menu_role, isolated = resolve_menu_scope(u.id, role) if u else (role, False)
    is_admin = bool(u and u.id in (config.admin_ids or set()))
    unread = await db.count_unread_tasks(u.id) if u else 0
    uc = await db.count_unread_by_channel(u.id) if u else {}
    kb = main_menu(menu_role or role, is_admin=is_admin, unread=unread, unread_channels=uc, isolated_role=isolated)
    await cb.message.answer("❌ Запрос ЗП отменён.", reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data == "mgrzp:confirm", ManagerZpSG.confirm)
async def manager_zp_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    invoice_id = data["zp_invoice_id"]
    amount = data["zp_amount"]

    # Update invoice
    await db.set_invoice_zp_manager_status(invoice_id, "requested", amount=amount, requested_by=u.id)

    inv = await db.get_invoice(invoice_id)
    inv_number = inv["invoice_number"] if inv else "—"

    # Create task for GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await db.create_task(
            project_id=None,
            type_=TaskType.ZP_MANAGER,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=int(gd_id),
            payload={
                "invoice_id": invoice_id,
                "invoice_number": inv_number,
                "amount": amount,
                "source": "manager_zp",
            },
        )
        initiator = await get_initiator_label(db, u.id)
        b = InlineKeyboardBuilder()
        b.button(text="✅ ЗП ОК", callback_data=f"gdzp_mgr:ok:{invoice_id}")
        b.button(text="❌ Отклонить", callback_data=f"gdzp_mgr:no:{invoice_id}")
        b.adjust(2)
        await notifier.safe_send(
            int(gd_id),
            f"💰 <b>Запрос ЗП отд.продаж</b>\n\n"
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
        f"Счёт: №{inv_number}, сумма: {amount:,.0f}₽",
    )
