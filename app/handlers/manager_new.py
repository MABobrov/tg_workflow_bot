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
from datetime import timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import (
    MANAGER_ROLES,
    InvoiceStatus,
    Role,
    TaskStatus,
    TaskType,
)
from ..keyboards import (
    MGR_BTN_CHECK_KP,
    MGR_BTN_CRED,
    MGR_BTN_EDO,
    MGR_BTN_INVOICE_END,
    MGR_BTN_INVOICE_START,
    MGR_BTN_ISSUE,
    MGR_BTN_MY_INVOICES,
    MGR_BTN_SEARCH_INVOICE,
    MGR_BTN_ZAMERY,
    edo_type_kb,
    invoice_list_kb,
    main_menu,
    manager_chat_submenu,
    tasks_kb,
)
from ..services.assignment import resolve_default_assignee
from ..services.integration_hub import IntegrationHub
from ..services.notifier import Notifier
from ..states import (
    CheckKpSG,
    EdoRequestSG,
    InvoiceEndSG,
    InvoiceSearchSG,
    InvoiceStartSG,
    IssueSG,
    ManagerChatProxySG,
)
from ..utils import get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, to_iso, utcnow
from .auth import require_role_callback, require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

ALL_MANAGER_ROLES = [Role.MANAGER, Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN]


async def _current_role(db: Database, user_id: int) -> str | None:
    user = await db.get_user_optional(user_id)
    return user.role if user else None


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
    "montazh": Role.INSTALLER,
}

_CHAT_CHANNEL_LABEL: dict[str, str] = {
    "manager_kv": "КВ Кред",
    "manager_kia": "КИА Кред",
    "manager_npn": "НПН Кред",
    "zamery": "Замеры",
    "rp_to_manager_kv": "РП → Менеджер КВ",
    "rp_to_manager_kia": "РП → Менеджер КИА",
    "montazh": "Монтажная гр.",
}


# =====================================================================
# ПРОВЕРИТЬ КП / СЧЕТ  (CheckKpSG)
# =====================================================================

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
async def check_kp_invoice_number(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите номер счёта:")
        return
    await state.update_data(invoice_number=text)
    await state.set_state(CheckKpSG.address)
    await message.answer("Шаг 2/5: Введите <b>адрес установки</b>:")


@router.message(CheckKpSG.address)
async def check_kp_address(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите адрес:")
        return
    await state.update_data(address=text)
    await state.set_state(CheckKpSG.amount)
    await message.answer("Шаг 3/5: Введите <b>полную сумму счёта</b> (число):")


@router.message(CheckKpSG.amount)
async def check_kp_amount(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("Введите число (сумма счёта):")
        return
    await state.update_data(amount=amount)
    await state.set_state(CheckKpSG.documents)
    await message.answer(
        "Шаг 4/5: Прикрепите <b>КП</b> (коммерческое предложение).\n"
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
    await message.answer(
        f"📎 Принял. Файлов: <b>{len(attachments)}</b>.\n"
        "Отправьте ещё файлы или напишите что-нибудь для перехода к комментарию."
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
    address = data["address"]
    amount = data["amount"]
    documents = data.get("documents", [])

    # Create invoice in DB
    role = await _current_role(db, message.from_user.id)
    inv_id = await db.create_invoice(
        invoice_number=invoice_number,
        project_id=None,
        created_by=message.from_user.id,
        creator_role=role or "manager",
        object_address=address,
        amount=amount,
        description=comment,
    )

    # Create task for RP
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if not rp_id:
        await message.answer("⚠️ РП не найден. Назначьте роль RP.")
        await state.clear()
        return

    role = await _current_role(db, message.from_user.id)
    role_label = {"manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА", "manager_npn": "Менеджер НПН"}.get(role or "", "Менеджер")

    task = await db.create_task(
        project_id=None,
        type_=TaskType.CHECK_KP,
        status=TaskStatus.OPEN,
        created_by=message.from_user.id,
        assigned_to=int(rp_id),
        due_at_iso=None,
        payload={
            "invoice_id": inv_id,
            "invoice_number": invoice_number,
            "address": address,
            "amount": amount,
            "comment": comment,
            "manager_role": role or "manager",
            "manager_id": message.from_user.id,
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
    msg_text = (
        f"📋 <b>Новый КП от {role_label}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {address}\n"
        f"💰 Сумма: {amount:,.0f}₽\n"
    )
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

    await state.clear()
    await message.answer(
        f"✅ КП отправлено РП на проверку.\n"
        f"Счёт №{invoice_number} создан в базе (статус: Новый).",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(message.from_user.id)),
        ),
    )


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

    if inv["status"] not in (InvoiceStatus.NEW,):
        await message.answer(
            f"⚠️ Счёт №{text} уже в статусе: {inv['status']}.\n"
            "Повторная отправка невозможна."
        )
        await state.clear()
        return

    await state.update_data(invoice_id=inv["id"], invoice_number=text, invoice_data=dict(inv))
    await state.set_state(InvoiceStartSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="inv_start:send")
    b.button(text="⏭ Без вложений", callback_data="inv_start:send")
    b.adjust(1)

    await message.answer(
        f"Счёт №{text} найден.\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n\n"
        "Прикрепите документы (счёт, договор, приложение) или нажмите «Отправить ГД».",
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
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


@router.callback_query(F.data == "inv_start:send")
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

    # Update invoice status
    await db.update_invoice_status(invoice_id, InvoiceStatus.PENDING_PAYMENT)

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
            "manager_role": role or "manager",
            "manager_id": u.id,
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
    msg_text = (
        f"💼 <b>Новый счёт на оплату от {role_label}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📄 Счёт №: <code>{invoice_number}</code>\n"
        f"📍 Адрес: {inv_data.get('object_address', '-')}\n"
        f"💰 Сумма: {inv_data.get('amount', 0):,.0f}₽\n"
    )

    from ..keyboards import task_actions_kb
    task_kb = task_actions_kb(task)
    await notifier.safe_send(int(gd_id), msg_text, reply_markup=task_kb)
    for a in attachments:
        await notifier.safe_send_media(int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

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

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Счёт №{invoice_number} отправлен ГД на оплату.",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
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
        await message.answer("У вас нет активных счетов для закрытия.")
        return

    await state.set_state(InvoiceEndSG.select_invoice)
    await message.answer(
        "🏁 <b>Счет End</b>\n\n"
        "Выберите счёт для закрытия:",
        reply_markup=invoice_list_kb(active, action_prefix="invend"),
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
    if conditions["installer_ok"] and conditions["edo_signed"] and not conditions["no_debts"]:
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

    # Update invoice status to CLOSING
    await db.update_invoice_status(invoice_id, InvoiceStatus.CLOSING)

    # Create task for GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    rp_id = await resolve_default_assignee(db, config, Role.RP)

    if not gd_id:
        await message.answer("⚠️ ГД не найден. Назначьте роль GD через админ-панель.")
        return

    task = await db.create_task(
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

    role = await _current_role(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Запрос «Счет End» по счёту №{inv['invoice_number']} отправлен.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(role, is_admin=message.from_user.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(message.from_user.id)),
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
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier
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
        await db.update_invoice_status(invoice_id, InvoiceStatus.ENDED)
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
    await state.set_state(EdoRequestSG.request_type)
    await message.answer(
        "📄 <b>Бухгалтерия (ЭДО)</b>\n\n"
        "Выберите тип запроса:",
        reply_markup=edo_type_kb(),
    )


@router.callback_query(EdoRequestSG.request_type, F.data.startswith("edo:"))
async def edo_type_selected(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    edo_type = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(request_type=edo_type, attachments=[])

    if edo_type == "other":
        await state.set_state(EdoRequestSG.description)
        await cb.message.answer("Опишите суть запроса:")  # type: ignore[union-attr]
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
    await message.answer(f"📎 Принял. Файлов: <b>{len(attachments)}</b>.")


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
    edo_id = await db.create_edo_request(
        request_type=request_type,
        requested_by=u.id,
        requested_by_role=requester_role,
        assigned_to=int(acc_id),
        invoice_number=invoice_number,
        description=description,
        comment=comment,
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

    role = await _current_role(db, u.id)
    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос ЭДО отправлен бухгалтеру ({type_label}).",
        reply_markup=private_only_reply_markup(
            cb.message,
            main_menu(role, is_admin=u.id in (config.admin_ids or set()), unread=await db.count_unread_tasks(u.id)),
        ),
    )


# =====================================================================
# МОИ СЧЕТА
# =====================================================================

@router.message(F.text == MGR_BTN_MY_INVOICES)
async def my_invoices(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return

    invoices = await db.list_invoices(created_by=message.from_user.id)  # type: ignore[union-attr]
    if not invoices:
        await message.answer("📑 У вас пока нет счетов.")
        return

    await message.answer(
        f"📑 <b>Мои Счета</b> ({len(invoices)}):\n\n"
        "Нажмите на счёт для просмотра:",
        reply_markup=invoice_list_kb(invoices, action_prefix="myinv"),
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
        "pending": "⏳ Ожидает оплаты",
        "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен",
        "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
    }.get(inv["status"], inv["status"])

    text = (
        f"📄 <b>Счёт №{inv['invoice_number']}</b>\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n"
        f"📊 Статус: {status_label}\n"
        f"📅 Создан: {inv.get('created_at', '-')[:10]}\n"
    )
    if inv.get("comment"):
        text += f"💬 Комментарий: {inv['comment']}\n"

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

    await cb.message.answer(text)  # type: ignore[union-attr]


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

@router.message(F.text == MGR_BTN_SEARCH_INVOICE)
async def search_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES + [Role.RP, Role.ACCOUNTING, Role.GD]):
        return
    await state.clear()
    await state.set_state(InvoiceSearchSG.value)
    await message.answer(
        "🔍 <b>Поиск Счета</b>\n\n"
        "Введите номер счёта или часть адреса для поиска:"
    )


@router.message(InvoiceSearchSG.value)
async def search_invoice_query(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Process search query and show results."""
    if not message.from_user:
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

    role = await _current_role(db, message.from_user.id)
    await state.clear()
    await message.answer(
        f"🔍 Найдено: <b>{len(results)}</b>\n\n"
        "Нажмите на счёт для подробной информации:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("srch_inv:view:"))
async def search_invoice_view(cb: CallbackQuery, db: Database) -> None:
    """Show detailed invoice card from search results."""
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    status_label = {
        "new": "🆕 Новый", "pending": "⏳ Ожидает оплаты",
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

    if inv.get("comment"):
        text += f"💬 Комментарий: {inv['comment']}\n"

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
# ЗАМЕРЫ (chat-proxy to zamery channel)
# =====================================================================

@router.message(F.text == MGR_BTN_ZAMERY)
async def mgr_zamery(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel="zamery")
    await message.answer(
        "📐 <b>Замеры</b>\n\n"
        "Выберите действие:",
        reply_markup=manager_chat_submenu("⬅️ Назад"),
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

    # Save chat message
    await db.save_chat_message(
        channel=channel,
        sender_id=message.from_user.id,
        direction="outgoing",
        text=text or "[файл/фото]",
        tg_message_id=message.message_id,
        has_attachment=bool(message.document or message.photo),
    )

    # Determine target by channel
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
async def mgr_chat_tasks(message: Message, db: Database) -> None:
    if not message.from_user:
        return
    tasks = await db.list_tasks_for_user(message.from_user.id, limit=20)
    if not tasks:
        await message.answer("Задач нет ✅")
        return
    await message.answer(
        f"📋 Ваши задачи ({len(tasks)}):",
        reply_markup=tasks_kb(tasks),
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
            text += f"• {e.get('amount', 0):,.0f}₽ — {e.get('comment', '-')}\n"

    await message.answer(text)


@router.message(ManagerChatProxySG.menu, F.text == "⬅️ Назад")
async def mgr_chat_back(message: Message, state: FSMContext, db: Database, config: Config) -> None:
    await state.clear()
    if not message.from_user:
        return
    role = await _current_role(db, message.from_user.id)
    is_admin = message.from_user.id in (config.admin_ids or set())
    await message.answer(
        "Выберите действие:",
        reply_markup=private_only_reply_markup(message, main_menu(role, is_admin=is_admin, unread=await db.count_unread_tasks(message.from_user.id))),
    )
