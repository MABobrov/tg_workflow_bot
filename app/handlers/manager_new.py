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
from datetime import date, datetime, timedelta
from typing import Any

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
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
from ..integrations.minio_storage import MinioStorage
from ..keyboards import (
    MGR_BTN_ADV_DISTRIBUTE_BASE,
    MGR_BTN_CHECK_KP,
    MGR_BTN_CHAT_RP,
    MGR_BTN_CRED,
    MGR_BTN_CRED_ADD,
    MGR_BTN_CREDIT_MENU,
    MGR_BTN_FUNDS,
    MGR_BTN_BACK_HOME,
    MGR_BTN_CANCEL,
    MGR_BTN_HELP,
    MGR_BTN_GD_CONTACT,
    RP_BTN_CREDIT_BAL,
    RP_BTN_CREDIT_SPEND,
    MGR_BTN_DEPO_TO_ADV_BASE,
    MGR_BTN_DEPOSIT_WITHDRAW,
    MGR_BTN_EDO,
    MGR_BTN_INVOICE_END,
    MGR_BTN_INVOICE_START,
    MGR_BTN_ISSUE,
    MGR_BTN_REMIND,
    MGR_BTN_MONTAZH,
    MGR_BTN_MY_INVOICES,
    MGR_BTN_SEARCH_INVOICE,
    MGR_BTN_ZAMERY,
    MGR_BTN_ZP,
    edo_invoice_pick_kb,
    edo_type_kb,
    invoice_list_kb,
    invoice_start_lead_picker_kb,
    lead_picker_for_kp_kb,
    main_menu,
    manager_chat_submenu,
    mgr_check_kp_card_kb,
    mgr_check_kp_history_kb,
    mgr_check_kp_menu_kb,
    task_actions_kb,
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
    CreditWalletEditSG,
    CreditWalletSpendSG,
    EdoRequestSG,
    FinalPaymentEtaSG,
    InvoiceEndSG,
    InvoiceSearchSG,
    InvoiceStartSG,
    IssueSG,
    ManagerAdvDistributeSG,
    ManagerAdvanceFillSG,
    ManagerChatProxySG,
    ManagerCreditExpenseSG,
    ManagerDepoToAdvSG,
    ManagerWithdrawSG,
    ManagerZpSG,
    SelfReminderSG,
    ZameryRequestSG,
)
from ..utils import answer_service, apply_credit_wallet_spend, build_credit_wallet_card, build_funds_card, build_invoice_section, close_condition_core_rows, compute_plan_profit, credit_wallet_label, fmt_money, format_card, format_card_section, format_invoice_card_standard, format_invoice_end_financials, format_manager_invoices_overview, format_manager_recalc_card, format_materials_list, get_initiator_label, manager_zp_net_payout, private_only_reply_markup, refresh_recipient_keyboard, try_json_loads, credit_zp_montazh_unpaid
from ._mirror import collect_attachment
from .auth import RoleFilter, require_role_callback, require_role_message
from .money_guard import money_confirm_guard

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
    # Менеджерский «Чат с РП» (канал "rp") — адресат РП. Раньше "rp" в карте не было
    # → .get(channel, Role.GD) молча слал сообщения ГД, а заголовок показывал сырое
    # "rp". Намерение (chat_proxy.resolve_channel_target: "rp" → Role.RP) — именно РП.
    "rp": Role.RP,
    "rp_to_manager_kv": Role.MANAGER_KV,
    "rp_to_manager_kia": Role.MANAGER_KIA,
    "rp_to_gd": Role.GD,
}

_CHAT_CHANNEL_LABEL: dict[str, str] = {
    "manager_kv": "КВ Кред",
    "manager_kia": "КИА Кред",
    "manager_npn": "НПН Кред",
    "zamery": "Замеры",
    "rp": "Менеджер → РП",
    "rp_to_manager_kv": "РП → Менеджер КВ",
    "rp_to_manager_kia": "РП → Менеджер КИА",
    "rp_to_gd": "РП → ГД",
    "montazh": "Монтажная гр.",
}


# =====================================================================
# ПРОВЕРИТЬ КП / СЧЕТ  (CheckKpSG)
# =====================================================================

async def _kp_start_new_flow(target: Message, state: FSMContext, db: Database, user_id: int) -> None:
    """Запустить мастер нового КП на проверку РП (общая часть для reply и inline входов)."""
    await state.clear()
    leads = await db.list_open_lead_tasks_for_manager(user_id)
    await state.set_state(CheckKpSG.lead_pick)
    if leads:
        await target.answer(
            "🎯 <b>Лид на расчет</b>\n\n"
            "Выберите лид или создайте нового клиента:",
            reply_markup=lead_picker_for_kp_kb(leads),
        )
    else:
        await target.answer(
            "🎯 <b>Лид на расчет</b>\n\n"
            "У вас пока нет назначенных лидов.\n"
            "Создайте нового клиента:",
            reply_markup=lead_picker_for_kp_kb([]),
        )


def _kp_history_card_text(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Сформировать текст карточки CHECK_KP-задачи менеджера + распарсенный payload."""
    payload = try_json_loads(task.get("payload_json"))
    status = (task.get("status") or "").lower()
    confirmed = bool(payload.get("manager_confirmed"))
    rejected = bool(payload.get("response_rejected"))
    if rejected or status == "rejected":
        header = "❌ <b>КП отклонён РП</b>"
    elif status == "done":
        header = ("✅ <b>Счёт выставлен — подтверждено</b>" if confirmed
                  else "📥 <b>Счёт выставлен РП</b>")
    else:
        header = "⌛ <b>Ждёт ответа РП</b>"

    inv_num = payload.get("invoice_number") or "—"
    client = payload.get("client_name") or "—"
    address = payload.get("address") or "—"
    amount = payload.get("amount") or 0
    is_credit = bool(payload.get("response_is_credit") or payload.get("is_credit"))
    pay_label = "🏦 Кред" if is_credit else "💳 б/н"
    sent_at = (task.get("created_at") or "")[:16].replace("T", " ")
    finalized_at = (payload.get("response_finalized_at") or "")[:16].replace("T", " ")
    response_comment = payload.get("response_comment") or ""

    text = (
        f"{header}\n\n"
        f"📋 Счёт: <b>№{inv_num}</b>\n"
        f"🏢 Клиент: {client}\n"
        f"📍 Адрес: {address}\n"
    )
    if amount:
        try:
            text += f"💰 Сумма: {float(amount):,.0f}₽\n"
        except (ValueError, TypeError):
            pass
    text += f"💳 Тип: {pay_label}\n"
    if sent_at:
        text += f"📤 Отправлено: {sent_at}\n"
    if finalized_at:
        text += f"📨 Ответ РП: {finalized_at}\n"
    if response_comment:
        text += f"\n💬 Комментарий РП: {response_comment}\n"
    return text, payload


@router.message(F.text == MGR_BTN_CHECK_KP)
async def start_check_kp(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    if not message.from_user:
        return
    await state.clear()
    history = await db.list_check_kp_history_for_manager(message.from_user.id, limit=30)
    if not history:
        # Истории нет — сразу мастер нового КП
        await _kp_start_new_flow(message, state, db, message.from_user.id)
        return
    unconfirmed = await db.count_check_kp_unconfirmed_for_manager(message.from_user.id)
    total = len(history)
    txt = (
        "📋 <b>Проверить КП / Счет</b>\n\n"
        f"Всего отправок: <b>{total}</b>"
    )
    if unconfirmed:
        txt += f"\nНеподтверждённых ответов РП: <b>{unconfirmed}</b>"
    txt += "\n\nВыберите действие:"
    await message.answer(txt, reply_markup=mgr_check_kp_menu_kb(unconfirmed))


@router.callback_query(F.data == "kp_menu:new")
async def kp_menu_new(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Из меню «Проверить КП» — запустить новый КП на проверку РП."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _kp_start_new_flow(cb.message, state, db, cb.from_user.id)  # type: ignore[arg-type]


@router.callback_query(F.data == "kp_menu:history")
async def kp_menu_history(cb: CallbackQuery, db: Database) -> None:
    """Подсписок «Ответы РП» — все CHECK_KP-задачи менеджера."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    items = await db.list_check_kp_history_for_manager(cb.from_user.id, limit=30)
    if not items:
        await cb.message.answer(  # type: ignore[union-attr]
            "📥 <b>Ответы РП</b>\n\nПока нет отправленных КП."
        )
        return
    unconfirmed = sum(
        1
        for t in items
        if (t.get("status") or "").lower() in ("done", "rejected")
        and not bool(try_json_loads(t.get("payload_json")).get("manager_confirmed"))
    )
    header = (
        "📥 <b>КП отправленные на проверку РП</b>\n\n"
        f"Всего: {len(items)}"
    )
    if unconfirmed:
        header += f" | неподтверждённых: {unconfirmed}"
    header += "\n\nНажмите запись для просмотра:"
    await cb.message.answer(  # type: ignore[union-attr]
        header, reply_markup=mgr_check_kp_history_kb(items)
    )


@router.callback_query(F.data == "kp_menu:back")
async def kp_menu_back(cb: CallbackQuery) -> None:
    """Закрыть inline-меню «Проверить КП / Счет»."""
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^kp_hist:open:\d+$"))
async def kp_hist_open(cb: CallbackQuery, db: Database) -> None:
    """Карточка одного ответа РП."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    try:
        task_id = int((cb.data or "").split(":")[-1])
    except (ValueError, IndexError):
        return
    task = await db.get_task(task_id)
    if not task or int(task.get("created_by") or 0) != cb.from_user.id:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return
    text, payload = _kp_history_card_text(task)
    status = (task.get("status") or "").lower()
    rejected = bool(payload.get("response_rejected")) or status == "rejected"
    confirmed = bool(payload.get("manager_confirmed"))
    finished = status in ("done", "rejected")
    has_docs = bool(payload.get("response_documents"))
    await cb.message.answer(  # type: ignore[union-attr]
        text,
        reply_markup=mgr_check_kp_card_kb(
            task_id=task_id,
            has_docs=has_docs,
            confirmed=confirmed,
            rejected=rejected,
            finished=finished,
        ),
    )


@router.callback_query(F.data.regexp(r"^kp_hist:docs:\d+$"))
async def kp_hist_docs(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    """Переслать менеджеру документы ответа РП (счёт/договор/приложения)."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    try:
        task_id = int((cb.data or "").split(":")[-1])
    except (ValueError, IndexError):
        return
    task = await db.get_task(task_id)
    if not task or int(task.get("created_by") or 0) != cb.from_user.id:
        await cb.message.answer("❌ Заявка не найдена.")  # type: ignore[union-attr]
        return
    payload = try_json_loads(task.get("payload_json"))
    documents: list[dict[str, Any]] = payload.get("response_documents") or []
    if not documents:
        await cb.message.answer(  # type: ignore[union-attr]
            "📄 Документы по этой заявке не сохранены."
        )
        return
    inv_num = payload.get("invoice_number") or "—"
    await cb.message.answer(  # type: ignore[union-attr]
        f"📄 Документы по счёту <b>№{inv_num}</b> ({len(documents)} шт.):"
    )
    for doc in documents:
        try:
            await notifier.safe_send_media(
                cb.from_user.id,
                doc.get("file_type", "document"),
                doc["file_id"],
                caption=doc.get("caption"),
            )
        except Exception:
            log.exception("kp_hist_docs: failed to resend doc for task=%s", task_id)


@router.callback_query(CheckKpSG.lead_pick, F.data == "kp_lead:new")
async def check_kp_new_client(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(flow_type="new")
    await state.set_state(CheckKpSG.client_name)
    await cb.message.answer(  # type: ignore[union-attr]
        "Шаг 1/7: Введите <b>контрагента</b> (название компании/ФИО):"
    )


@router.callback_query(CheckKpSG.lead_pick, F.data.regexp(r"^kp_lead:pick:\d+$"))
async def check_kp_pick_lead(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    lead_task_id = int((cb.data or "").split(":")[-1])
    lead_task = await db.get_task(lead_task_id)
    if not lead_task:
        await cb.message.answer("❌ Лид не найден.")  # type: ignore[union-attr]
        return
    payload = try_json_loads(lead_task.get("payload_json"))
    # Ключ заявки — "name" ("description" оставлен фолбэком для легаси-задач,
    # как в lead_picker_for_kp_kb). Адрес берём из заявки, а не пустую строку:
    # в ветке лида шаги client_name/address пропускаются, и раньше оба поля
    # доезжали до счёта пустыми (счёт 26721-1НПН, 22.07).
    client_name = payload.get("name") or payload.get("description") or ""
    lead_source = payload.get("source", "")
    address = payload.get("address") or ""
    await state.update_data(
        flow_type="lead",
        lead_task_id=lead_task_id,
        client_name=client_name,
        lead_source=lead_source,
        address=address,
    )
    await state.set_state(CheckKpSG.documents)
    # Витрина данных заявки перед отправкой КП (owner 27.07): имя и адрес
    # уезжают в счёт молча, ошибку в заявке поймать иначе негде. Телефон —
    # справочно, в счёт он НЕ переносится (owner: только показать).
    # ⛔ read-only, ничего не пишем: feedback_card_display_only_no_data_writes.
    import html as _html

    def _cell(value: Any) -> str:
        text = str(value or "").strip()
        return _html.escape(text) if text else "—"

    card = format_card_section(
        "📌", "Лид — данные клиента",
        [
            ("Имя", _cell(client_name)),
            ("Телефон", _cell(payload.get("phone"))),
            ("Адрес", _cell(address)),
            ("Источник", _cell(lead_source)),
        ],
        width=38, compact=True,
    )
    await cb.message.answer(  # type: ignore[union-attr]
        f"{card}\n\nПрикрепите <b>КП</b> (файл или фото расчёта):"
    )


@router.message(CheckKpSG.client_name)
async def check_kp_client_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите контрагента:")
        return
    await state.update_data(client_name=text)
    await state.set_state(CheckKpSG.address)
    await message.answer("Шаг 2/7: Введите <b>адрес установки</b>:")


@router.message(CheckKpSG.address)
async def check_kp_address(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите адрес:")
        return
    await state.update_data(address=text)
    await state.set_state(CheckKpSG.amount)
    await message.answer("Шаг 3/7: Введите <b>полную сумму</b> (число):")


@router.message(CheckKpSG.amount)
async def check_kp_amount(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().replace(",", ".").replace(" ", "")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("Введите число (сумма):")
        return
    await state.update_data(amount=amount)
    data = await state.get_data()

    # Оба ветки → сначала выбор типа клиента (кредит/безнал)
    _b = InlineKeyboardBuilder()
    _b.button(text="🏢 Безналичный (юрлицо)", callback_data="kp_credit:0")
    _b.button(text="💰 Кредитный (физлицо)", callback_data="kp_credit:1")
    _b.adjust(1)
    await state.set_state(CheckKpSG.credit_type)
    step = "4" if data.get("flow_type") != "lead" else "3"
    await message.answer(
        f"Шаг {step}/7: Тип клиента:",
        reply_markup=_b.as_markup(),
    )




@router.callback_query(CheckKpSG.credit_type, F.data.startswith("kp_credit:"))
async def check_kp_credit_type(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    is_credit = int((cb.data or "").split(":", 1)[1])
    await state.update_data(is_credit=is_credit)
    label = "💰 Кредитный" if is_credit else "🏢 Безналичный"
    await cb.message.edit_text(f"✅ Тип клиента: <b>{label}</b>")  # type: ignore[union-attr]
    data = await state.get_data()
    if data.get("flow_type") == "lead":
        # Короткий путь: → комментарий
        await state.set_state(CheckKpSG.comment)
        await cb.message.answer(  # type: ignore[union-attr]
            "Добавьте <b>комментарий</b> (или отправьте «—» для пропуска):"
        )
    else:
        # Полная форма: → тип оплаты
        b = InlineKeyboardBuilder()
        b.button(text="100% предоплата", callback_data="kp_pay:100")
        b.button(text="50/50", callback_data="kp_pay:5050")
        b.button(text="Рассрочка", callback_data="kp_pay:installment")
        b.button(text="Другое", callback_data="kp_pay:other")
        b.adjust(2)
        await state.set_state(CheckKpSG.payment_type)
        await cb.message.answer(  # type: ignore[union-attr]
            "Выберите <b>тип оплаты</b>:",
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
        "Шаг 5/7: Введите <b>срок по договору</b> (кол-во дней):"
    )
    await cb.answer()


@router.message(CheckKpSG.deadline_days)
async def check_kp_deadline(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    try:
        days = int(text)
    except (ValueError, TypeError):
        await message.answer("Введите число (кол-во дней):")
        return
    await state.update_data(deadline_days=days)
    await state.set_state(CheckKpSG.documents)
    await message.answer(
        "Шаг 6/7: Прикрепите <b>КП</b> (коммерческое предложение).\n"
        "Отправьте файл(ы) или фото."
    )


@router.message(CheckKpSG.documents)
async def check_kp_documents(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    data = await state.get_data()

    uid = message.from_user.id if message.from_user else "anon"
    # ⚠️ Ключ состояния здесь `documents`, а не `attachments` — имя переменной обманчиво.
    att, doc_count = await collect_attachment(
        message, state, storage, prefix=f"manager/{uid}", key="documents"
    )
    if att is None:
        if not doc_count:
            await message.answer("Пришлите файл или фото КП:")
            return
        if data.get("flow_type") == "lead":
            await state.set_state(CheckKpSG.amount)
            await message.answer("Введите <b>сумму</b> из расчёта (число):")
        else:
            await state.set_state(CheckKpSG.comment)
            await message.answer("Шаг 7/7: Добавьте <b>комментарий</b> (или отправьте «—» для пропуска):")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(
        message,
        f"📎 Принял. Файлов: <b>{doc_count}</b>.{suffix}\n"
        "Отправьте ещё файлы или напишите что-нибудь для перехода к следующему шагу.",
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

    flow_type = data.get("flow_type", "new")
    lead_task_id = data.get("lead_task_id")
    client_name = data.get("client_name", "")
    address = data.get("address", "")
    amount = data.get("amount", 0)
    payment_type = data.get("payment_type", "")
    deadline_days = data.get("deadline_days")
    lead_source = data.get("lead_source", "")
    documents = data.get("documents", [])

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
            "flow_type": flow_type,
            "lead_task_id": lead_task_id,
            "client_name": client_name,
            "address": address,
            "amount": amount,
            "payment_type": payment_type,
            "deadline_days": deadline_days,
            "lead_source": lead_source,
            "comment": comment,
            "manager_role": role or "manager",
            "manager_id": message.from_user.id,
            "is_credit": data.get("is_credit", 0),
        },
    )

    for a in documents:
        await db.add_attachment(
            task_id=int(task["id"]),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    initiator = await get_initiator_label(db, message.from_user.id)
    flow_label = "Лид" if flow_type == "lead" else "Новый клиент"
    # Карточка по стандартному образцу (docs/rules/feedback_card_template_standard.md):
    # заголовок «{emoji} <b>{Type}: №{X}</b>» → «👤 От:» → мета-блок → «💬 Пояснение:».
    msg_text = (
        f"📋 <b>КП: №{task['id']}</b>\n"
        f"👤 От: {initiator}\n\n"
    )
    if address:
        msg_text += f"📍 Адрес: {address}\n"
    if amount:
        msg_text += f"💰 Сумма: {amount:,.0f}₽\n"
    if payment_type:
        msg_text += f"💳 Тип: {payment_type}\n"
    if client_name:
        msg_text += f"🏢 Клиент: {client_name}\n"
    msg_text += f"🆕 Поток: {flow_label}\n"
    if lead_source:
        msg_text += f"🔗 Источник: {lead_source}\n"
    if deadline_days:
        msg_text += f"⏰ Срок: {deadline_days} дн.\n"
    if comment:
        msg_text += f"\n💬 Пояснение: {comment}"

    b_kp = InlineKeyboardBuilder()
    b_kp.button(text="📋 Ответить на КП", callback_data=f"kp_review:{task['id']}")
    b_kp.adjust(1)

    await notifier.safe_send(int(rp_id), msg_text, reply_markup=b_kp.as_markup())
    for a in documents:
        await notifier.safe_send_media(int(rp_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    menu_role, isolated_role = await _current_menu(db, message.from_user.id)
    await state.clear()
    await message.answer(
        "✅ КП отправлено РП на проверку.",
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
    try:
        task_id = int((cb.data or "").split(":")[-1])
    except (ValueError, IndexError):
        return

    # Закрываем кнопку — убираем inline keyboard
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass

    task = await db.get_task(task_id)
    if task:
        # Помечаем как подтверждённую менеджером
        try:
            payload = json.loads(task.get("payload_json") or "{}")
            payload["manager_confirmed"] = True
            await db.conn.execute(
                "UPDATE tasks SET payload_json = ? WHERE id = ?",
                (json.dumps(payload, ensure_ascii=False), task_id),
            )
            await db.conn.commit()
        except Exception:
            log.exception("Failed to update task payload_json for task_id=%s", task_id)


# =====================================================================
# СЧЕТ В РАБОТУ (InvoiceStartSG)
# =====================================================================

@router.message(F.text == MGR_BTN_INVOICE_START)
async def start_invoice_start(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    if not message.from_user:
        return
    await state.clear()

    invoices = await db.list_manager_leads_with_invoice(message.from_user.id)
    if invoices:
        await message.answer(
            "💼 <b>Счёт в Работу</b>\n\n"
            "Выберите счёт из ваших лидов или введите номер вручную.\n"
            "Запрос подтверждения оплаты пойдёт ГД.\n\n"
            "<i>Для отмены: /cancel</i>",
            reply_markup=invoice_start_lead_picker_kb(invoices),
        )
    else:
        # Нет готовых счетов — сразу ручной ввод номера
        await state.set_state(InvoiceStartSG.invoice_number)
        await message.answer(
            "💼 <b>Счёт в Работу</b>\n\n"
            "У вас пока нет лидов с выставленным счётом.\n"
            "Введите <b>номер счёта</b> вручную:\n"
            "<i>Для отмены: /cancel</i>"
        )


@router.callback_query(F.data == "invstart_inv:manual")
async def invoice_start_manual(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """«Без лида»: переход к ручному вводу номера счёта."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    await state.set_state(InvoiceStartSG.invoice_number)
    await cb.message.answer(  # type: ignore[union-attr]
        "Введите <b>номер счёта</b> вручную:"
    )


@router.callback_query(F.data.startswith("invstart_inv:pick:"))
async def invoice_start_pick_lead(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Менеджер выбрал лид → берём связанный счёт, переходим к client_source."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    try:
        invoice_id = int((cb.data or "").split(":")[-1])
    except (ValueError, IndexError):
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Доп. защита: счёт должен принадлежать этому менеджеру и быть готовым к работе
    if int(inv.get("created_by") or 0) != cb.from_user.id:
        await cb.message.answer("⛔️ Можно отправить ГД только свой счёт.")  # type: ignore[union-attr]
        return
    if inv["status"] not in (InvoiceStatus.PENDING_PAYMENT, InvoiceStatus.CREDIT):
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Счёт №{inv.get('invoice_number')} в статусе «{inv['status']}» — "
            "нельзя запустить в работу.\n"
            "В работу запускаются только счета, выставленные РП (безналичные или кредитные)."
        )
        return

    invoice_number = inv.get("invoice_number") or ""
    await state.update_data(
        invoice_id=inv["id"],
        invoice_number=invoice_number,
        invoice_data=dict(inv),
    )
    await state.set_state(InvoiceStartSG.client_source)

    b = InlineKeyboardBuilder()
    b.button(text="👤 Мой клиент (50/50)", callback_data="inv_src:own")
    b.button(text="📋 Лид от ГД (75/25)", callback_data="inv_src:gd_lead")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    await cb.message.answer(  # type: ignore[union-attr]
        f"Счёт №{invoice_number} выбран.\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {inv.get('amount', 0):,.0f}₽\n\n"
        "❓ <b>Источник клиента</b> (влияет на распределение прибыли):",
        reply_markup=b.as_markup(),
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

    if inv["status"] not in (InvoiceStatus.PENDING_PAYMENT, InvoiceStatus.CREDIT):
        await message.answer(
            f"⚠️ Счёт №{text} в статусе «{inv['status']}» — нельзя запустить в работу.\n"
            "В работу запускаются только счета, выставленные РП (безналичные или кредитные)."
        )
        await state.clear()
        return

    await state.update_data(invoice_id=inv["id"], invoice_number=text, invoice_data=dict(inv))
    await state.set_state(InvoiceStartSG.client_source)

    b = InlineKeyboardBuilder()
    b.button(text="👤 Мой клиент (50/50)", callback_data="inv_src:own")
    b.button(text="📋 Лид от ГД (75/25)", callback_data="inv_src:gd_lead")
    b.button(text="⬅️ Назад", callback_data="nav:home")
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
    await state.set_state(InvoiceStartSG.receipt_date)
    label = "👤 Мой клиент (50/50)" if source == "own" else "📋 Лид от ГД (75/25)"
    await cb.message.answer(  # type: ignore[union-attr]
        f"Источник: {label}\n\n"
        "📅 Введите <b>дату счёта</b> в формате ДД.ММ.ГГГГ\n"
        "<i>Например: 22.07.2026</i>",
    )


# ---------- Дата счёта + сумма первого платежа (owner 2026-07-25) ----------

_RECEIPT_DATE_FORMATS = ("%d.%m.%Y", "%d.%m.%y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d")


def _parse_receipt_date(text: str) -> str | None:
    """Дата счёта из ввода менеджера → ISO YYYY-MM-DD; None, если не распознана.

    Год обязателен: дата счёта бывает и задним числом, поэтому догадка про год
    (utils.parse_date переносит прошедшую дату на следующий год) здесь вредна.
    """
    t = (text or "").strip().replace(" ", " ")
    for fmt in _RECEIPT_DATE_FORMATS:
        try:
            d = datetime.strptime(t, fmt).date()
        except ValueError:
            continue
        # Год вне разумного диапазона — не отказ сразу: «22.07.26» сначала
        # ловится форматом %Y (год 26), правильный разбор даёт следующий %y.
        if not (2020 <= d.year <= date.today().year + 1):
            continue
        return d.isoformat()
    return None


@router.message(InvoiceStartSG.receipt_date)
async def invoice_start_receipt_date(message: Message, state: FSMContext) -> None:
    iso = _parse_receipt_date(message.text or "")
    if iso is None:
        await message.answer(
            "⚠️ Не понял дату. Введите в формате ДД.ММ.ГГГГ (например: 22.07.2026):"
        )
        return
    await state.update_data(receipt_date=iso)
    data = await state.get_data()
    amount = float((data.get("invoice_data") or {}).get("amount") or 0)
    await state.set_state(InvoiceStartSG.first_payment)
    await message.answer(
        f"📅 Дата счёта: <b>{datetime.strptime(iso, '%Y-%m-%d').strftime('%d.%m.%Y')}</b>\n\n"
        "💵 Введите <b>сумму первого платежа</b> в ₽:\n"
        f"<i>Сумма счёта: {amount:,.0f}₽</i>",
    )


@router.message(InvoiceStartSG.first_payment)
async def invoice_start_first_payment(message: Message, state: FSMContext) -> None:
    val = _parse_est_value(message.text or "")
    if val is None or val <= 0:
        await message.answer("⚠️ Введите сумму первого платежа — число больше 0:")
        return
    await state.update_data(first_payment_amount=val)
    data = await state.get_data()
    amount = float((data.get("invoice_data") or {}).get("amount") or 0)
    warn = f"\n⚠️ Это больше суммы счёта ({amount:,.0f}₽) — проверьте." if amount and val > amount else ""
    await state.set_state(InvoiceStartSG.deadline_days)
    await message.answer(
        f"💵 Первый платёж: <b>{val:,.0f}₽</b>{warn}\n\n"
        "📅 Введите <b>срок по договору</b> в днях\n"
        "(количество дней от даты счёта до окончания):",
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
    # Срок считаем от даты счёта — так же, как импорт из «Импорт ОП»
    # (db._compute_deadline_end_date), чтобы бот и лист не расходились.
    data = await state.get_data()
    try:
        base_date = datetime.strptime(str(data.get("receipt_date")), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        base_date = date.today()
    end_date = base_date + timedelta(days=days)
    await state.update_data(
        deadline_days=days,
        deadline_end_date=end_date.isoformat(),
    )
    await state.set_state(InvoiceStartSG.estimated_glass)
    await message.answer(
        f"📅 Срок по договору: <b>{end_date.strftime('%d.%m.%Y')}</b> ({days} дн.)\n\n"
        "📊 <b>Расчётные данные</b> (шаг 1/4)\n"
        "Введите <b>расчётную стоимость материалов</b> в ₽:\n"
        "<i>Введите 0, если материалов нет.</i>",
    )


# ---------- Расчётные данные (4 шага: материалы / установка / грузчики / логистика) ----------

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
    """Шаг 1/4: стоимость материалов (стекло + металл/профиль — объединено, user 2026-06-24).

    Вся сумма пишется в estimated_glass (поле с возвратным НДС), estimated_profile=0.
    Финансово эквивалентно прежним двум полям: refundable_base = glass + profile.
    """
    val = _parse_est_value(message.text or "")
    if val is None:
        await message.answer("⚠️ Введите корректное число ≥ 0:")
        return
    await state.update_data(estimated_glass=val, estimated_profile=0)
    await state.set_state(InvoiceStartSG.estimated_installation)
    await message.answer(
        "📊 <b>Расчётные данные</b> (шаг 2/4)\n"
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
        "📊 <b>Расчётные данные</b> (шаг 3/4)\n"
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
        "📊 <b>Расчётные данные</b> (шаг 4/4)\n"
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
    # ЕДИНЫЙ helper compute_plan_profit (credit-aware НДС + гард распределения, user 2026-06-19).
    client_source = data.get("client_source", "own")
    _pp = compute_plan_profit(
        amount=amount, est_glass=est_glass, est_profile=est_profile,
        est_inst=est_inst, est_load=est_load, est_log=est_log,
        is_credit=bool(inv_data.get("is_credit")), client_source=client_source,
    )
    est_total = _pp["est_total"]
    output_vat = _pp["output_vat"]
    input_vat = _pp["input_vat"]
    net_vat = _pp["net_vat"]
    est_profit = _pp["est_profit"]
    est_pct = _pp["est_pct"]
    rp_zp = _pp["rp_zp"]
    mgr_share = _pp["manager_zp"]
    split_label = "Лид ГД (75/25)" if client_source == "gd_lead" else "Мой клиент (50/50)"

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить ГД", callback_data="inv_start:send")
    b.button(text="⏭ Без вложений", callback_data="inv_start:send_no_attach")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    await message.answer(
        f"📊 <b>Расчётные данные введены:</b>\n"
        f"  Материалы: {est_glass:,.0f}₽\n"
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
async def invoice_start_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"manager/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото/видео или нажмите «✅ Отправить ГД».")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


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
    # Дата счёта + сумма первого платежа (owner 2026-07-25): receipt_date задаёт
    # порядок строк листа, first_payment_amount → колонка P (от неё AE «Долг»).
    invoice_date_fields: dict[str, Any] = {}
    if data.get("receipt_date"):
        invoice_date_fields["receipt_date"] = data["receipt_date"]
    if data.get("first_payment_amount") is not None:
        invoice_date_fields["first_payment_amount"] = float(data["first_payment_amount"])
    await db.update_invoice(
        invoice_id,
        estimated_glass=est_glass,
        estimated_profile=est_profile,
        estimated_installation=est_inst,
        estimated_loaders=est_load,
        estimated_logistics=est_log,
        client_source=client_source,
        **deadline_fields,
        **invoice_date_fields,
    )

    # Статус счёта не меняем: счёт уже PENDING_PAYMENT (безнал) или CREDIT —
    # выставил РП на этапе ответа на КП. Кнопка «Счёт в работу» = запрос подтверждения
    # оплаты у ГД, а не смена жизненного цикла счёта.

    # Create task for GD
    role = await _current_role(db, u.id)
    role_label = {"manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА", "manager_npn": "Менеджер НПН"}.get(role or "", "Менеджер")

    try:
        await db.audit(
            actor_id=u.id,
            action="invoice_sent_to_gd",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "invoice_number": invoice_number,
                "gd_id": gd_id,
                "manager_role": role,
                "role_label": role_label,
                "client_source": client_source,
                "has_attachments": bool(attachments),
                "attachments_count": len(attachments) if attachments else 0,
                "est_glass": est_glass,
                "est_profile": est_profile,
                "est_inst": est_inst,
                "est_load": est_load,
                "est_log": est_log,
                "deadline_days": data.get("deadline_days"),
                "receipt_date": data.get("receipt_date"),
                "first_payment_amount": data.get("first_payment_amount"),
            },
        )
    except Exception:
        log.exception("invoice_start_send: audit() failed for invoice=%s", invoice_id)

    # «Счёт в работу»: задача идёт в общий inbox ГД («📥 Входящие для ГД»),
    # а НЕ в «Счета на Оплату» (там только supplier-payment-запросы РП→ГД).
    task = await db.create_task(
        project_id=None,
        type_=TaskType.GD_TASK,
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
            "source": "manager_invoice_start",
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

    # Notify GD
    initiator = await get_initiator_label(db, u.id)
    amount = float(inv_data.get("amount", 0))
    # ЕДИНЫЙ helper compute_plan_profit (credit-aware НДС + гард распределения, user 2026-06-19).
    _pp = compute_plan_profit(
        amount=amount, est_glass=est_glass, est_profile=est_profile,
        est_inst=est_inst, est_load=est_load, est_log=est_log,
        is_credit=bool(inv_data.get("is_credit")), client_source=client_source,
    )
    est_total = _pp["est_total"]
    output_vat = _pp["output_vat"]
    input_vat = _pp["input_vat"]
    net_vat = _pp["net_vat"]
    est_profit = _pp["est_profit"]
    est_pct = _pp["est_pct"]
    rp_zp = _pp["rp_zp"]
    mgr_share = _pp["manager_zp"]
    gd_share = _pp["gd_profit"]
    src_label = "📋 Лид от ГД (75/25)" if client_source == "gd_lead" else "👤 Клиент менеджера (50/50)"

    client = inv_data.get("client_name") or "—"
    is_credit = bool(inv_data.get("is_credit"))
    pay_label = "🏦 Кред" if is_credit else "💳 б/н"

    # Карточка по стандартному образцу (см. docs/rules/feedback_card_template_standard.md):
    # заголовок «{emoji} <b>{Type}: №{X}</b>» → «👤 От:» → мета-блок → <b>Section:</b>-блоки.
    msg_text = (
        f"💼 <b>Счёт в работу: №{invoice_number}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📍 Адрес: {inv_data.get('object_address', '-')}\n"
        f"💰 Сумма: {amount:,.0f}₽\n"
        f"💳 Тип: {pay_label}\n"
        f"🏢 Клиент: {client}\n"
        f"🔗 Источник: {src_label}\n\n"
        f"<b>Расчёт:</b>\n"
        f"  Материалы: {est_glass:,.0f}₽\n"
        f"  Установка: {est_inst:,.0f}₽\n"
        f"  Грузчики: {est_load:,.0f}₽\n"
        f"  Логистика: {est_log:,.0f}₽\n"
        f"  НДС выходной: {output_vat:,.0f}₽\n"
        f"  Возвр.НДС: -{input_vat:,.0f}₽\n"
        f"  Чистый НДС: {net_vat:,.0f}₽\n"
        f"  Себестоимость: {est_total:,.0f}₽\n"
        f"  Прибыль: {est_profit:,.0f}₽ ({est_pct:.1f}%)\n\n"
        f"<b>Распределение:</b>\n"
        f"  ЗП РП (10%): {rp_zp:,.0f}₽\n"
        f"  ЗП менеджер: {mgr_share:,.0f}₽\n"
        f"  Доля ГД: {gd_share:,.0f}₽"
    )

    # Одна кнопка: «Подтвердить оплату» (б/н) / «Подтвердить (кред)».
    # После нажатия: счёт → IN_PROGRESS, sync row, close task, notify менеджер+РП.
    confirm_text = "✅ Подтвердить (кред)" if is_credit else "✅ Подтвердить оплату"
    b_merged = InlineKeyboardBuilder()
    b_merged.row(InlineKeyboardButton(
        text=confirm_text,
        callback_data=f"invstart_confirm:{invoice_id}",
    ))
    await notifier.safe_send(int(gd_id), msg_text, reply_markup=b_merged.as_markup())
    for a in attachments:
        await notifier.safe_send_media(int(gd_id), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    # Уведомить РП: счёт взят в работу, ждёт подтверждения ГД (по тому же образцу).
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        rp_text = (
            f"💼 <b>Счёт в работу: №{invoice_number}</b>\n"
            f"👤 От: {initiator}\n\n"
            f"📍 Адрес: {inv_data.get('object_address', '-')}\n"
            f"💰 Сумма: {amount:,.0f}₽\n"
            f"💳 Тип: {pay_label}\n"
            f"🏢 Клиент: {client}\n\n"
            f"⏳ <b>Статус: ждёт подтверждения ГД</b>"
        )
        await notifier.safe_send(int(rp_id), rp_text)
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

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
# ПОДТВЕРЖДЕНИЕ ОПЛАТЫ ГД (Счёт в работу)
# =====================================================================

@router.callback_query(F.data.startswith("invstart_confirm:"))
async def invoice_start_confirm(
    cb: CallbackQuery,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """ГД нажал «Подтвердить оплату»: счёт → IN_PROGRESS, sync, close task, notify."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer("✅ Принято")
    try:
        invoice_id = int((cb.data or "").split(":")[-1])
    except (ValueError, IndexError):
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    # Status → IN_PROGRESS + sync row (cost_card подтягивается внутри integration push).
    await db.update_invoice_status(invoice_id, InvoiceStatus.IN_PROGRESS)
    await integrations.sync_invoice_status(inv["invoice_number"], InvoiceStatus.IN_PROGRESS)
    await integrations.sync_invoice_row(invoice_id)

    # Закрыть GD_TASK от менеджера (source=manager_invoice_start).
    linked = await db.search_tasks_by_payload(
        field="invoice_id",
        value=str(invoice_id),
        type_filter=[TaskType.GD_TASK],
        limit=20,
    )
    for t in linked:
        if t.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
            continue
        try:
            t_payload = json.loads(t.get("payload_json") or "{}")
        except Exception:
            t_payload = {}
        if t_payload.get("source") != "manager_invoice_start":
            continue
        updated = await db.update_task_status(int(t["id"]), TaskStatus.DONE)
        try:
            await integrations.sync_task(updated, project_code="")
        except Exception:
            log.exception("invoice_start_confirm: sync_task failed for task=%s", t.get("id"))

    try:
        await db.audit(
            actor_id=cb.from_user.id,
            action="invoice_payment_confirmed_by_gd",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={
                "invoice_number": inv["invoice_number"],
                "is_credit": bool(inv.get("is_credit")),
            },
        )
    except Exception:
        log.exception("invoice_start_confirm: audit() failed for invoice=%s", invoice_id)

    # У ГД убираем кнопку и показываем подтверждение.
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ <b>Оплата по счёту №{inv['invoice_number']} подтверждена.</b>\n"
        f"Счёт переведён в работу."
    )

    # Карточка менеджеру и РП — по стандартному образцу.
    initiator = await get_initiator_label(db, cb.from_user.id)
    is_credit = bool(inv.get("is_credit"))
    pay_label = "🏦 Кред" if is_credit else "💳 б/н"
    client = inv.get("client_name") or "—"
    mgr_msg = (
        f"💼 <b>Счёт в работу: №{inv['invoice_number']} — подтверждён</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📍 Адрес: {inv.get('object_address', '-')}\n"
        f"💰 Сумма: {float(inv.get('amount', 0)):,.0f}₽\n"
        f"💳 Тип: {pay_label}\n"
        f"🏢 Клиент: {client}\n\n"
        f"<b>Статус:</b>\n"
        f"  ✅ Оплата подтверждена ГД\n"
        f"  ✅ Счёт переведён в работу"
    )
    manager_id = inv.get("created_by")
    if manager_id:
        await notifier.safe_send(int(manager_id), mgr_msg)
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        await notifier.safe_send(int(rp_id), mgr_msg)


# =====================================================================
# СЧЕТ END (InvoiceEndSG)
# =====================================================================

# startswith (не ==): кнопка получает бейдж «🔴N» когда есть счета к закрытию
# (keyboards.main_menu, mgr_invoice_end_ready) → текст становится «Счет End 🔴2».
# Точное сравнение тогда не срабатывало и кнопка «ничего не делала» именно когда
# счета ЕСТЬ. Зеркало стороны ГД (td.py: F.text.startswith(GD_BTN_INVOICE_END_GD)).
@router.message(F.text.startswith(MGR_BTN_INVOICE_END))
async def start_invoice_end(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()

    # Show list of manager's invoices with status IN_PROGRESS / PAID / CREDIT
    # (CREDIT — активные кред-счета закрываются тем же флоу, ТЗ 18.06: чтобы
    # бейдж 🔴 на кнопке совпадал со списком при нажатии).
    invoices = await db.list_invoices(
        created_by=message.from_user.id,  # type: ignore[union-attr]
    )
    active = [
        i for i in invoices
        if i["status"] in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID, InvoiceStatus.CREDIT)
    ]

    if not active:
        await answer_service(message, "У вас нет активных счетов для закрытия.", delay_seconds=60)
        return

    await state.set_state(InvoiceEndSG.select_invoice)
    await message.answer(
        "🏁 <b>Счет End</b>\n\n"
        "Выберите счёт для закрытия:",
        reply_markup=invoice_list_kb(active, action_prefix="invend", back_callback="nav:home"),
    )


def _invoice_docs_lines(inv: dict[str, Any]) -> list[str]:
    """Строки статуса документов (ЭДО / у кого оригиналы) — эталон _show_invoice_end_conditions."""
    lines: list[str] = []
    if bool(inv.get("docs_edo_signed")):
        lines.append("📄 Первичные: подписаны в ЭДО")
    elif inv.get("docs_originals_holder"):
        holder = "ГД" if inv.get("docs_originals_holder") == "gd" else "менеджера"
        lines.append(f"📁 Оригиналы первичных: у {holder}")
    if bool(inv.get("edo_signed")):
        lines.append("📄 Закрывающие: подписаны в ЭДО")
    elif inv.get("closing_originals_holder"):
        holder = "ГД" if inv.get("closing_originals_holder") == "gd" else "менеджера"
        lines.append(f"📁 Оригиналы закрывающих: у {holder}")
    return lines


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

    cond_rows = close_condition_core_rows(
        inv, conditions, debts_label="Долгов нет — подтверждение ГД"
    )
    cond_rows.append(("☐", "Пояснения (опционально)"))
    cond_lines = "\n".join(
        f"{mark} {i}. {label}" for i, (mark, label) in enumerate(cond_rows, 1)
    )

    text = (
        f"🏁 <b>Счёт №{inv['invoice_number']} — Проверка условий:</b>\n\n"
        f"{cond_lines}\n"
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
            initiator = await get_initiator_label(db, cb.from_user.id)
            is_credit = bool(inv.get("is_credit"))
            pay_label = "🏦 Кред" if is_credit else "💳 б/н"
            client = inv.get("client_name") or "—"
            gd_card = (
                f"❓ <b>Счёт End: №{inv['invoice_number']} — оплачен 100%?</b>\n"
                f"👤 От: {initiator}\n\n"
                f"📍 Адрес: {inv.get('object_address', '-')}\n"
                f"💰 Сумма: {float(inv.get('amount', 0)):,.0f}₽\n"
                f"💳 Тип: {pay_label}\n"
                f"🏢 Клиент: {client}\n\n"
                f"<b>Статус:</b>\n"
                f"  ⏳ Менеджер инициировал «Счет End»\n"
                f"  ⏳ Ожидается подтверждение оплаты"
            )
            await notifier.safe_send(int(gd_id), gd_card, reply_markup=b.as_markup())
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
        b.button(text="📄 Подписан по ЭДО", callback_data=f"invend_prim_orig:edo:{invoice_id}")
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
        b.button(text="📄 Подписан по ЭДО", callback_data=f"invend_clos_orig:edo:{invoice_id}")
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
    """Manager answers: who holds primary originals? (или подписано по ЭДО)"""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        return
    holder = parts[1]  # gd | manager | edo
    try:
        invoice_id = int(parts[2])
    except (ValueError, IndexError):
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if holder == "edo":
        await db.set_invoice_docs_edo_signed(invoice_id, True, actor_id=cb.from_user.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Первичные документы по счёту №{inv['invoice_number']} подписаны в ЭДО."
        )
    else:
        await db.set_docs_originals_holder(invoice_id, holder, actor_id=cb.from_user.id)
        holder_label = "ГД" if holder == "gd" else "менеджера"
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Оригиналы первичных — у {holder_label}."
        )

    # Проверяем закрывающие: если ЭДО подписано — оригиналы не нужны
    closing_edo = bool(inv.get("edo_signed"))
    if not closing_edo and not inv.get("closing_originals_holder"):
        b = InlineKeyboardBuilder()
        b.button(text="📄 Подписан по ЭДО", callback_data=f"invend_clos_orig:edo:{invoice_id}")
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
    """Manager answers: who holds closing originals? (или подписано по ЭДО)"""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        return
    holder = parts[1]  # gd | manager | edo
    try:
        invoice_id = int(parts[2])
    except (ValueError, IndexError):
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if holder == "edo":
        await db.set_invoice_edo_signed(invoice_id, signed=True, actor_id=cb.from_user.id)
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Закрывающие документы по счёту №{inv['invoice_number']} подписаны в ЭДО."
        )
    else:
        await db.set_closing_originals_holder(invoice_id, holder, actor_id=cb.from_user.id)
        holder_label = "ГД" if holder == "gd" else "менеджера"
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Оригиналы закрывающих — у {holder_label}."
        )

    # Переходим к отображению условий
    await _show_invoice_end_conditions(cb, state, db, config, notifier, invoice_id)


# ── «Нет документов по счёту» (INVOICE_DOCS_MISSING, ТЗ 10.07) ──────────────
# Менеджер выставляет статус первичных документов по задаче. «Оформлены»
# доспрашивает КАК (ЭДО / оригиналы у ГД / у менеджера) → пишет в счёт — сеттеры
# сами вызывают resolve_invoice_docs_missing, задача закрывается и daily_sync её
# не пересоздаёт. «В работе»/«Запрошены» — пометка docs_status в payload, задача
# остаётся открытой (R3: сменить статус позже = переоткрыть из «Все задачи»).
# Old-buttons-safe ([[feedback-fsm-old-buttons-trap]]): без FSM-фильтра,
# обновление сообщения в try/except.
_DOCS_STATUS_MSG = {
    "in_work": "⏳ Отмечено: документы в работе.",
    "requested": "📤 Отмечено: документы запрошены у клиента.",
}


async def _docs_task_owned(cb: CallbackQuery, db: Database, tid: int) -> dict[str, Any] | None:
    """Загрузить задачу INVOICE_DOCS_MISSING + проверить тип/владельца (иначе alert)."""
    try:
        task = await db.get_task(tid)
    except KeyError:
        await cb.answer("Задача не найдена или была удалена.", show_alert=True)
        return None
    if str(task.get("type") or "") != TaskType.INVOICE_DOCS_MISSING:
        await cb.answer("Неверный тип задачи.", show_alert=True)
        return None
    if int(task.get("assigned_to") or 0) != int(cb.from_user.id):
        await cb.answer("Эта задача назначена другому человеку.", show_alert=True)
        return None
    return task


@router.callback_query(F.data.startswith("docstat:"))
async def invoice_docs_status(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    code = parts[1]  # formalized | in_work | requested | back
    try:
        tid = int(parts[2])
    except (ValueError, IndexError):
        await cb.answer()
        return
    task = await _docs_task_owned(cb, db, tid)
    if not task:
        return
    if task.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        await cb.answer("Задача уже закрыта.", show_alert=True)
        return

    if code == "formalized":
        # Доспрашиваем, КАК оформлены → запишем в счёт → задача закроется.
        b = InlineKeyboardBuilder()
        b.button(text="📄 Подписан по ЭДО", callback_data=f"docsfin:edo:{tid}")
        b.button(text="📁 Оригиналы у ГД", callback_data=f"docsfin:gd:{tid}")
        b.button(text="📁 Оригиналы у меня", callback_data=f"docsfin:manager:{tid}")
        b.button(text="↩️ Назад", callback_data=f"docstat:back:{tid}")
        b.adjust(1)
        await cb.answer()
        try:
            await cb.message.edit_reply_markup(reply_markup=b.as_markup())  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(  # type: ignore[union-attr]
                "Как оформлены первичные документы?", reply_markup=b.as_markup()
            )
        return

    if code == "back":
        await cb.answer()
        try:
            await cb.message.edit_reply_markup(reply_markup=task_actions_kb(task))  # type: ignore[union-attr]
        except Exception:
            pass
        return

    if code == "in_work":
        # → in_progress (глушит 15-мин эскалацию) + пометка docs_status.
        await db.update_task_status(
            tid, TaskStatus.IN_PROGRESS, expected_statuses=("open", "in_progress")
        )
        await db.update_task_payload(tid, {"docs_status": "in_work"})
    elif code == "requested":
        # Пометка «запрошены у клиента», задача остаётся открытой; глушим 15-мин
        # напоминание — менеджер уже занялся задачей.
        if not task.get("accepted_at"):
            try:
                await db.accept_task(tid)
            except Exception:
                log.debug("docstat: accept_task failed tid=%s", tid, exc_info=True)
        await db.update_task_payload(tid, {"docs_status": "requested"})
    else:
        await cb.answer()
        return

    await cb.answer(_DOCS_STATUS_MSG.get(code, "Отмечено."))
    try:
        task = await db.get_task(tid)
        await cb.message.edit_reply_markup(reply_markup=task_actions_kb(task))  # type: ignore[union-attr]
    except Exception:
        pass


@router.callback_query(F.data.startswith("docsfin:"))
async def invoice_docs_finalize(cb: CallbackQuery, db: Database, notifier: Notifier) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    how = parts[1]  # edo | gd | manager
    try:
        tid = int(parts[2])
    except (ValueError, IndexError):
        await cb.answer()
        return
    task = await _docs_task_owned(cb, db, tid)
    if not task:
        return
    if task.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        await cb.answer("Задача уже закрыта.", show_alert=True)
        return
    payload = try_json_loads(task.get("payload_json")) or {}
    invoice_id = int(payload.get("invoice_id") or 0)
    if not invoice_id:
        await cb.answer("В задаче нет привязки к счёту.", show_alert=True)
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.answer("Счёт не найден.", show_alert=True)
        return

    # Записать первичку в счёт. Сеттеры сами закрывают задачу
    # (resolve_invoice_docs_missing) → daily_sync её не пересоздаст.
    await db.update_task_payload(tid, {"docs_status": "formalized"})
    if how == "edo":
        await db.set_invoice_docs_edo_signed(invoice_id, True, actor_id=cb.from_user.id)
        how_label = "подписаны в ЭДО"
    elif how in ("gd", "manager"):
        await db.set_docs_originals_holder(invoice_id, how, actor_id=cb.from_user.id)
        how_label = "оригиналы у " + ("ГД" if how == "gd" else "менеджера")
    else:
        await cb.answer()
        return

    num = inv.get("invoice_number") or invoice_id
    await cb.answer("✅ Документы оформлены, задача закрыта.")

    # Уведомить бухгалтерию (FYI) — она ждала документы по этому счёту.
    try:
        acc_users = await db.find_users_by_role(Role.ACCOUNTING)
        acc_text = (
            f"✅ <b>Документы оформлены — счёт №{num}</b>\n\n"
            f"Первичные документы по счёту оформлены ({how_label})."
        )
        for au in acc_users:
            if int(au.telegram_id) == int(cb.from_user.id):
                continue
            await notifier.safe_send(int(au.telegram_id), acc_text)
    except Exception:
        log.debug("docsfin: accounting notify failed inv=%s", invoice_id, exc_info=True)

    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Первичные документы по счёту №{num} оформлены ({how_label}). Задача закрыта."
    )


async def _build_invoice_end_cards(
    db: Database,
    inv: dict[str, Any],
    invoice_id: int,
    initiator: str,
    comment: str,
) -> tuple[str, str]:
    """Карточки «Счёт End» → (для РП, для ГД). Единый рендер (owner 14.08).

    Им пользуются оба входа: запрос менеджера (invoice_end_comment) и
    самостоятельное закрытие ГД (invend_pick). Две копии разъехались бы и
    нарушили эталон карточек ([[feedback_card_template_standard]]) — вынесено
    без изменения содержимого, вывод для менеджерского пути обязан остаться
    байт в байт прежним (проверяется функтестом).

    Разница между карточками ровно одна: у ГД есть финблок (себестоимость /
    прибыль расч+факт / ЗП менеджера), РП его НЕ видит.
    """
    import html as _html

    conditions = await db.check_close_conditions(invoice_id)

    # Условия (эталон <pre>): у кредитных строка ЭДО опущена, нумерация сквозная.
    cond_rows = close_condition_core_rows(inv, conditions)
    cond_rows.append(("✅" if comment else "☐", "Пояснения"))
    cond_lines = [f"{i}. {mark} {label}" for i, (mark, label) in enumerate(cond_rows, 1)]
    cond_section = "<b>✅  Условия</b>\n<pre>" + "\n".join(f"   {ln}" for ln in cond_lines) + "</pre>"

    # Шапка карточки (От/Адрес/Сумма — контент сохранён, без добавления полей).
    head_section = format_card_section(
        "🏁", f"Счёт End: №{inv['invoice_number']}",
        [
            ("От", initiator),
            ("Адрес", _html.escape(str(inv.get("object_address") or "—"))),
            ("Сумма", fmt_money(inv.get("amount") or 0)),
        ],
        width=44, compact=True,
    )

    # PART B (ТЗ 19.06): справочный финблок (Себест/Прибыль расч+факт, ЗП менеджера
    # + ставка) — display-only из get_plan_fact_card, эталон-секция. Только ГД; РП его
    # НЕ видит (прибыль/себестоимость скрыты от РП, как в format_plan_fact_card role='rp').
    pf = await db.get_plan_fact_card(invoice_id)
    fin_section = format_invoice_end_financials(inv, pf)  # эталон-секция <pre> или ""

    docs = _invoice_docs_lines(inv)
    docs_section = (
        "<b>📄  Документы</b>\n<pre>" + "\n".join(f"   {ln}" for ln in docs) + "</pre>"
        if docs else ""
    )
    comment_tail = f"\n\n💬 Пояснение: {_html.escape(comment)}" if comment else ""

    msg = format_card([head_section, cond_section, docs_section]) + comment_tail  # → РП (без финблока)
    gd_msg = format_card([head_section, fin_section, cond_section, docs_section]) + comment_tail  # → ГД
    return msg, gd_msg


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

    # owner 2026-07-03: кредитный счёт нельзя закрывать в «Счет End», пока ЗП
    # монтажнику не выплачена — счёт остаётся «Счет ОК» до выплаты.
    if credit_zp_montazh_unpaid(inv):
        await message.answer(
            f"⛔️ Счёт №{inv['invoice_number']} — кредитный, ЗП монтажнику ещё НЕ выплачена.\n"
            "Закрыть в «Счет End» можно только после выплаты ЗП монтаж — сейчас счёт "
            "остаётся «Счет ОК». Повторите после отправки платёжки по ЗП монтажника."
        )
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
    msg, gd_msg = await _build_invoice_end_cards(db, inv, invoice_id, initiator, comment)

    # Notify GD
    if gd_id:
        b = InlineKeyboardBuilder()
        b.button(text="📌 На проверке", callback_data=f"invend_final:check:{invoice_id}")
        b.button(text="🏁 Счет End", callback_data=f"invend_final:end:{invoice_id}")
        b.button(text="⚠️ Закрыть с задачами", callback_data=f"invend_final:force:{invoice_id}")
        b.adjust(1)
        await notifier.safe_send(int(gd_id), gd_msg, reply_markup=b.as_markup())
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

# ГД закрывает счёт сам (owner 14.08).
#
# До этой правки у ГД не было НИ ОДНОГО самостоятельного входа: кнопки решения
# (invend_final:*) приходили только на пуш-карточке запроса менеджера, а
# «✅ ОК (Счёт End)» — только на карточке задачи INVOICE_END_REQUEST. Не отправил
# менеджер запрос (или погасил своё напоминание generic-кнопкой «✅ Завершить» —
# см. ветку INVOICE_END_READY в keyboards.py) → счёт зависал молча, и подтвердить
# его статус ГД было нечем. Боевые случаи: 2671-1КИА и 26623-1КВ.
#
# 🔑 Новой бизнес-логики здесь НЕТ. Пикер отдаёт ту же карточку (_build_invoice_end_cards)
# и те же три кнопки invend_final:check/end/force — значит все гейты остаются на месте:
# кредит с невыплаченной ЗП монтаж, идемпотентность по status='ended', запрет штатного
# «Счет End» при невыполненных условиях и fixup-задачи менеджеру при форс-закрытии.

@router.callback_query(F.data == "invend_pick:list")
async def invoice_end_gd_pick_list(cb: CallbackQuery, db: Database) -> None:
    """Список счетов, ожидающих закрытия, — вход ГД в «Счет End»."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    invoices = await db.list_invoices_pending_end()
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "✅ Нет счетов, ожидающих закрытия.\n\n"
            "Сюда попадают материнские счета, у которых монтаж дошёл до «Счет ОК» "
            "или «Счет End», а статус ещё не закрыт."
        )
        return
    b = InlineKeyboardBuilder()
    for inv in invoices:
        conds = await db.check_close_conditions(int(inv["id"]))
        # ⚠️ — есть невыполненные условия: штатное «Счет End» по такому счёту
        # откажет, закрыть можно только «⚠️ Закрыть с задачами». zp_approved в
        # набор НЕ входит намеренно: условие витринное и закрытие не блокирует
        # (approved не стоит ни у одного счёта за всю историю).
        _ok = all(conds.get(k) for k in ("installer_ok", "edo_signed", "no_debts"))
        b.button(
            text=f"{'✅' if _ok else '⚠️'} №{inv.get('invoice_number')} — {fmt_money(inv.get('amount') or 0)}",
            callback_data=f"invend_pick:inv:{int(inv['id'])}",
        )
    b.button(text="◀️ Назад", callback_data="gd_end:menu")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"🏁 <b>Закрыть счёт</b>\n\nОжидают закрытия: <b>{len(invoices)}</b>\n"
        "⚠️ — есть невыполненные условия (только «Закрыть с задачами»).",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("invend_pick:inv:"))
async def invoice_end_gd_pick_invoice(cb: CallbackQuery, db: Database) -> None:
    """Карточка выбранного счёта + те же кнопки решения, что в запросе менеджера."""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    try:
        invoice_id = int((cb.data or "").rsplit(":", 1)[1])
    except (ValueError, IndexError):
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    # Пояснения от менеджера в этом входе нет — карточка строится с пустым
    # комментарием, строка «Пояснения» в условиях остаётся «☐», как и должна.
    initiator = await get_initiator_label(db, cb.from_user.id)
    _msg, gd_msg = await _build_invoice_end_cards(db, inv, invoice_id, initiator, "")
    b = InlineKeyboardBuilder()
    b.button(text="📌 На проверке", callback_data=f"invend_final:check:{invoice_id}")
    b.button(text="🏁 Счет End", callback_data=f"invend_final:end:{invoice_id}")
    b.button(text="⚠️ Закрыть с задачами", callback_data=f"invend_final:force:{invoice_id}")
    b.button(text="◀️ Назад", callback_data="invend_pick:list")
    b.adjust(1)
    await cb.message.answer(gd_msg, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("invend_gd:"))
async def invoice_end_gd_debt_response(
    cb: CallbackQuery, db: Database, notifier: Notifier
) -> None:
    """GD responds: is the invoice 100% paid?"""
    if not await require_role_callback(cb, db, roles=[Role.GD]):
        return
    await cb.answer()
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        return
    answer = parts[1]  # yes or no
    try:
        invoice_id = int(parts[2])
    except (ValueError, IndexError):
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    gd_label = await get_initiator_label(db, cb.from_user.id)

    if answer == "yes":
        await db.set_invoice_no_debts(invoice_id, True)
        await cb.message.answer(f"✅ Счёт №{inv['invoice_number']} — подтверждено: долгов нет.")  # type: ignore[union-attr]
        manager_id = inv.get("created_by")
        if manager_id:
            mgr_msg = format_invoice_card_standard(
                inv=inv,
                creator_label=gd_label,
                section=("Статус", ["✅ Долгов нет (условие 3 выполнено)"]),
                title_override=("✅", f"Счёт End: №{inv['invoice_number']} — оплачен 100%"),
            )
            await notifier.safe_send(int(manager_id), mgr_msg)
    else:
        await cb.message.answer(f"⚠️ Счёт №{inv['invoice_number']} — есть долг.")  # type: ignore[union-attr]
        manager_id = inv.get("created_by")
        if manager_id:
            mgr_msg = format_invoice_card_standard(
                inv=inv,
                creator_label=gd_label,
                section=("Статус", ["❌ Условие 3 не выполнено (есть долг)"]),
                title_override=("⚠️", f"Счёт End: №{inv['invoice_number']} — есть долг"),
            )
            await notifier.safe_send(int(manager_id), mgr_msg)


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
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        return
    decision = parts[1]  # check or end
    try:
        invoice_id = int(parts[2])
    except (ValueError, IndexError):
        return

    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return

    if decision in ("end", "force"):
        # Идемпотентность: повторный клик по уже закрытому счёту — no-op
        # (защита от дублей карточек/задач при двойном нажатии ГД).
        if (inv.get("status") or "").lower() == InvoiceStatus.ENDED:
            await cb.message.answer(  # type: ignore[union-attr]
                f"ℹ️ Счёт №{inv['invoice_number']} уже закрыт."
            )
            return

        # owner 2026-07-03: кредит нельзя закрывать в «Счет End», пока ЗП монтаж
        # не выплачена (блокирует и «⚠️ Закрыть с задачами»).
        if credit_zp_montazh_unpaid(inv):
            await cb.message.answer(  # type: ignore[union-attr]
                f"⛔️ Счёт №{inv['invoice_number']} — кредитный, ЗП монтажнику не выплачена.\n"
                "«Счет End» доступен только после выплаты ЗП монтаж. Пока — «Счет ОК»."
            )
            return

        conditions = await db.check_close_conditions(invoice_id)
        # (ключ условия, метка для карточки, короткая метка для задачи менеджеру)
        cond_specs = (
            ("installer_ok", "1. Монтажник — Счет ОК", "Монтажник — Счет ОК"),
            ("edo_signed", "2. ЭДО — подписано", "Закрыть ЭДО (закрывающие документы)"),
            ("no_debts", "3. Долгов нет", "Погасить долг по счёту"),
        )
        missing = [(k, short) for k, label, short in cond_specs if not conditions.get(k)]

        # Штатное «Счет End» при незакрытых условиях — нельзя (нужна форс-кнопка).
        if missing and decision == "end":
            await cb.message.answer(  # type: ignore[union-attr]
                "⛔️ Нельзя закрыть счёт, пока не выполнены обязательные условия:\n"
                + "\n".join(
                    f"• {label}" for k, label, short in cond_specs if not conditions.get(k)
                )
                + "\n\nЛибо нажмите «⚠️ Закрыть с задачами» — счёт закроется, "
                "а менеджеру придут задачи на устранение."
            )
            return

        forced = decision == "force" and bool(missing)

        await db.update_invoice_status(invoice_id, InvoiceStatus.ENDED)
        # Update montazh stage → invoice_end
        from ..enums import MontazhStage
        await db.update_montazh_stage(invoice_id, MontazhStage.INVOICE_END)
        await integrations.sync_invoice_status(
            inv["invoice_number"], InvoiceStatus.ENDED, MontazhStage.INVOICE_END,
        )
        await integrations.sync_invoice_row(invoice_id)

        # ТЗ 14.06: «Счёт End» с непогашенным долгом → задача менеджеру на ввод
        # ориент. даты фин. платежа (дедуп: 1 задача даже если уже была на «Счёт ОК»).
        from ..utils import request_final_payment_eta
        await request_final_payment_eta(db, notifier, config, invoice_id, cb.from_user.id)

        linked_tasks = await db.search_tasks_by_payload(
            field="invoice_id",
            value=str(invoice_id),
            # + INVOICE_END_READY (ТЗ 18.06): счёт закрыт → гасим напоминание/бейдж
            type_filter=[TaskType.INVOICE_END_REQUEST, TaskType.INVOICE_END_READY],
            limit=20,
        )
        for linked_task in linked_tasks:
            _pl = try_json_loads(linked_task.get("payload_json")) or {}
            if int(_pl.get("invoice_id") or 0) != invoice_id:
                continue  # LIKE-поиск: отсекаем substring-совпадения по id
            if linked_task.get("status") in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
                updated_task = await db.update_task_status(int(linked_task["id"]), TaskStatus.DONE)
                await integrations.sync_task(updated_task, project_code="")

        # --- ч.3.2: форс-закрытие → задачи менеджеру на устранение пунктов ---
        new_fix: list[tuple[str, str]] = []
        if forced:
            manager_fix_id = inv.get("created_by")
            existing_fix = await db.search_tasks_by_payload(
                field="invoice_id", value=str(invoice_id),
                type_filter=[TaskType.INVOICE_END_FIXUP], limit=20,
            )
            existing_keys: set[str] = set()
            for t in existing_fix:
                if t.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
                    continue
                p = try_json_loads(t.get("payload_json")) or {}
                if int(p.get("invoice_id") or 0) == invoice_id and p.get("condition_key"):
                    existing_keys.add(str(p.get("condition_key")))
            for key, short in missing:
                if key in existing_keys:  # дубль open-задачи по этому пункту не плодим
                    continue
                try:
                    await db.create_task(
                        project_id=None,
                        type_=TaskType.INVOICE_END_FIXUP,
                        status=TaskStatus.OPEN,
                        created_by=cb.from_user.id,
                        assigned_to=int(manager_fix_id) if manager_fix_id else None,
                        due_at_iso=None,
                        payload={
                            "invoice_id": invoice_id,
                            "invoice_number": inv["invoice_number"],
                            "condition_key": key,
                            "condition_label": short,
                        },
                    )
                    new_fix.append((key, short))
                except Exception:
                    log.exception(
                        "invoice_end force: create fixup failed inv=%s key=%s",
                        invoice_id, key,
                    )
            if manager_fix_id and new_fix:
                fix_lines = "\n".join(f"  • {short}" for _, short in new_fix)
                from ..utils import build_manager_task_card, to_iso, utcnow
                synth_task = {
                    "id": None,
                    "type": TaskType.INVOICE_END_FIXUP,
                    "status": TaskStatus.OPEN,
                    "created_at": to_iso(utcnow()),
                    "due_at": None,
                    "payload_json": json.dumps(
                        {
                            "invoice_id": invoice_id,
                            "invoice_number": inv["invoice_number"],
                            "details": (
                                "Необходимо устранить:\n" + fix_lines
                                + "\n\n⛔ Выдача ЗП по этому счёту заблокирована "
                                "до устранения всех пунктов."
                            ),
                        },
                        ensure_ascii=False,
                    ),
                }
                try:
                    fixup_msg = await build_manager_task_card(
                        db, synth_task, config.timezone,
                        header_emoji="⚠️",
                        header_title="Счёт закрыт с пунктами — устранить",
                    )
                except Exception:
                    log.exception("invoice_end fixup: card render failed inv=%s", invoice_id)
                    fixup_msg = (
                        f"⚠️ <b>Счёт №{inv['invoice_number']} закрыт ГД с незакрытыми пунктами</b>\n\n"
                        f"Необходимо устранить:\n{fix_lines}\n\n"
                        f"⛔ Выдача ЗП по этому счёту заблокирована до устранения всех пунктов."
                    )
                await notifier.safe_send(int(manager_fix_id), fixup_msg)

        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Счёт №{inv['invoice_number']} — закрыт"
            + (" (принудительно, с задачами)." if forced else ".")
        )

        # Карточка по стандартному образцу — менеджеру, РП (и бухгалтерии,
        # если счёт безналичный — feedback_credit_no_accounting_notify.md).
        gd_label = await get_initiator_label(db, cb.from_user.id)
        is_credit = bool(inv.get("is_credit"))
        if forced:
            status_lines = [
                "⚠️ Закрыт принудительно (ГД)",
                f"❗ Незакрытые пункты: {', '.join(short for _, short in missing)}",
                "✅ Счёт переведён в ENDED",
            ]
        else:
            status_lines = [
                "✅ Все условия выполнены",
                "✅ Счёт переведён в ENDED",
            ]
        msg = format_invoice_card_standard(
            inv=inv,
            creator_label=gd_label,
            section=("Статус", status_lines),
            title_override=("🏁", f"Счёт End: №{inv['invoice_number']} — ЗАКРЫТ"),
        )
        docs = _invoice_docs_lines(inv)
        if docs:
            msg += "\n\n<b>📄  Документы</b>\n<pre>" + "\n".join(f"   {ln}" for ln in docs) + "</pre>"

        manager_id = inv.get("created_by")
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        targets = [manager_id, rp_id]
        if not is_credit:
            acc_id = await resolve_default_assignee(db, config, Role.ACCOUNTING)
            targets.append(acc_id)

        for target_id in targets:
            if target_id:
                await notifier.safe_send(int(target_id), msg)

        # ЗП замерщика НЕ запрашиваем автоматически при закрытии счёта —
        # её инициирует сам замерщик (feedback_zp_request_initiated_by_role).

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
            gd_label = await get_initiator_label(db, cb.from_user.id)
            mgr_msg = format_invoice_card_standard(
                inv=inv,
                creator_label=gd_label,
                section=("Статус", ["⏳ ГД пока не принял решение"]),
                title_override=("📌", f"Счёт End: №{inv['invoice_number']} — на проверке"),
            )
            await notifier.safe_send(int(manager_id), mgr_msg)


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
        reply_markup=edo_type_kb(with_back=True),
    )


# Req 23: Навигация — «🏠 В главное меню» из любого состояния EdoRequestSG.
@router.callback_query(F.data == "edo:home")
async def edo_nav_home(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.delete()  # type: ignore[union-attr]
    except Exception:
        pass


# Req 23: Навигация — «⬅️ К списку счетов» из стейта request_type.
@router.callback_query(EdoRequestSG.request_type, F.data == "edo:back_pick")
async def edo_nav_back_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not cb.from_user:
        return
    invoices = await db.list_invoices_for_edo(cb.from_user.id)
    if not invoices:
        await state.set_state(EdoRequestSG.request_type)
        await cb.message.answer(  # type: ignore[union-attr]
            "📄 <b>Бухгалтерия (ЭДО)</b>\n\n"
            "У вас нет активных счетов.\n"
            "Выберите тип запроса:",
            reply_markup=edo_type_kb(),
        )
        return
    await state.set_state(EdoRequestSG.invoice_pick)
    await cb.message.answer(  # type: ignore[union-attr]
        "📄 <b>Бухгалтерия (ЭДО)</b>\n\n"
        "Выберите счёт:",
        reply_markup=edo_invoice_pick_kb(invoices),
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
async def edo_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"manager/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


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
            minio_object_key=a.get("minio_object_key"),
        )

    type_label = {
        "sign_invoice": "Подписать по ЭДО (счет)",
        "sign_closing": "Закрывающие по ЭДО (счет)",
        "sign_upd": "Подписать по ЭДО УПД поставщика",
        "other": "Другое",
    }.get(request_type, request_type)

    initiator = await get_initiator_label(db, u.id)
    if invoice_number:
        header = f"📄 <b>Запрос ЭДО: №{invoice_number}</b>"
    else:
        header = f"📄 <b>Запрос ЭДО: {type_label}</b>"
    msg = f"{header}\n👤 От: {initiator}\n\n"
    msg += f"📂 Тип: {type_label}"
    if description:
        msg += f"\n📝 Описание: {description}"
    if comment:
        msg += f"\n\n💬 Пояснение: {comment}"

    # Inline buttons: Принято + Вопрос + Ответить на ЭДО
    from ..callbacks import TaskCb
    b_edo_resp = InlineKeyboardBuilder()
    tid = int(task["id"])
    b_edo_resp.button(text="✅ Принято", callback_data=TaskCb(task_id=tid, action="accept").pack())
    b_edo_resp.button(text="❓ Вопрос", callback_data=f"acc_q:{tid}")
    b_edo_resp.button(text="📄 Ответить на ЭДО", callback_data=f"edo_respond:{tid}")
    b_edo_resp.adjust(2, 1)

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


def _sort_invoices_desc(invoices: list[dict]) -> list[dict]:
    """Свежие сверху: по receipt_date → created_at → id."""
    def _key(i: dict) -> str:
        return str(i.get("receipt_date") or i.get("created_at") or "")
    return sorted(invoices, key=lambda i: (_key(i), int(i.get("id") or 0)), reverse=True)


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

    invoices = _sort_invoices_desc(invoices)
    initiator = await get_initiator_label(db, message.from_user.id)  # type: ignore[union-attr]
    overview = format_manager_invoices_overview(initiator, marker or "", invoices)

    await message.answer(
        overview,
        reply_markup=invoice_list_kb(
            invoices,
            action_prefix="myinv",
            back_callback="nav:home",
            show_address=True,
        ),
    )

    # owner 2026-06-23: карточки «Перерасчёт прибыли» (точно как у ГД — себест-ть,
    # Прибыль Итого, ЗП менеджера) по СВОИМ счетам, подходящим под механизм (CN≠0 И
    # долг=0). Скоуп тот же, что у списка выше: marker (КВ/КИА/НПН) либо created_by.
    # Display-only, ничего не пишет. Нет подходящих счетов → ничего не присылается.
    if marker:
        recalc_ids = await db.list_invoices_under_recalc(marker=marker)
    else:
        recalc_ids = await db.list_invoices_under_recalc(created_by=message.from_user.id)  # type: ignore[union-attr]
    for inv_id in recalc_ids:
        rinv = await db.get_invoice(inv_id)
        if not rinv:
            continue
        await message.answer(format_manager_recalc_card(rinv))


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

    status = inv["status"]

    # Карточка по эталону card-template-standard через helper'ы (как в ГД).
    creator_label = "—"
    creator_id = inv.get("created_by")
    if creator_id:
        try:
            creator_label = await get_initiator_label(db, int(creator_id))
        except (TypeError, ValueError):
            creator_label = "—"

    section = await build_invoice_section(db, inv, invoice_id)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
    )

    # Inline-кнопки actions — переиспользуем существующие callback handlers.
    b = InlineKeyboardBuilder()
    if status in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID):
        b.button(text="🏁 Счёт End", callback_data=f"invend:view:{invoice_id}")
        b.button(text="🛒 Материалы", callback_data=f"mgr_mat:{invoice_id}")
        b.button(text="💬 Чат с РП", callback_data=f"inv_chat:menu:{invoice_id}")
    elif status == InvoiceStatus.ENDED:
        b.button(text="📦 Материалы", callback_data=f"mgr_mat:{invoice_id}")
        if inv.get("assigned_to"):
            b.button(text="💬 Чат с монтажником", callback_data=f"inv_chat:menu:{invoice_id}")
    elif status == InvoiceStatus.CREDIT:
        b.button(text="💬 Чат с РП", callback_data=f"inv_chat:menu:{invoice_id}")
    # closing — пассивный (ждём ГД), без кнопок.

    if b.export():
        b.adjust(1)
        await cb.message.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]
    else:
        await cb.message.answer(text)  # type: ignore[union-attr]


@router.callback_query(F.data.regexp(r"^mgr_mat:\d+$"))
async def manager_invoice_materials(cb: CallbackQuery, db: Database) -> None:
    """Менеджер: список купленных материалов по закрытому счёту."""
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
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

@router.message(F.text == MGR_BTN_ISSUE, RoleFilter(ALL_MANAGER_ROLES))
async def start_manager_issue(message: Message, state: FSMContext, db: Database) -> None:
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
# ⏰ НАПОМИНАЛКА — самозадача менеджера / РП (SELF_REMINDER)
# =====================================================================

_SELFREM_CANCEL_WORDS = {"отмена", "❌ отмена", "/cancel", "cancel"}

# Самонапоминалку могут ставить менеджеры (КВ/КИА/НПН) и РП — одинаковая
# reply-кнопка «⏰ Напомнить» у двух ролей → общий хендлер с RoleFilter
# (правило feedback_chatrp_shared_button_rolefilter).
_SELF_REMINDER_ROLES = ALL_MANAGER_ROLES + [Role.RP, Role.GD, Role.TD]


def _self_reminder_when_kb() -> Any:
    """Inline-пресеты выбора времени напоминания + «своя дата» + отмена."""
    b = InlineKeyboardBuilder()
    b.button(text="⏱ Через 1 час", callback_data="selfrem:1h")
    b.button(text="⏱ Через 3 часа", callback_data="selfrem:3h")
    b.button(text="🌆 Сегодня 18:00", callback_data="selfrem:today18")
    b.button(text="🌅 Завтра 10:00", callback_data="selfrem:tmrw10")
    b.button(text="📅 Своя дата/время", callback_data="selfrem:custom")
    b.button(text="❌ Отмена", callback_data="selfrem:cancel")
    b.adjust(2, 2, 1, 1)
    return b.as_markup()


async def _finalize_self_reminder(
    target: Message, state: FSMContext, db: Database, config: Config, user_id: int, due,
) -> None:
    """Создать SELF_REMINDER (assigned_to=сам) с due_at и подтвердить пользователю.

    due — tz-aware datetime; to_iso нормализует в UTC. Задача авто-принимается
    (accept_task), чтобы не висела как «непринятая»; из acceptance-loops и бейджа
    она исключена по типу (db.py), а срабатывает ТОЧНО в срок в reminders_loop
    (services/reminders.py) с последующим закрытием.
    """
    import html as _html
    from ..utils import to_iso, from_iso, tzinfo
    data = await state.get_data()
    text = (data.get("reminder_text") or "").strip()
    await state.clear()
    if not text:
        await target.answer("❌ Текст напоминания потерян. Начните заново кнопкой «⏰ Напомнить».")
        return
    due_iso = to_iso(due)
    task = await db.create_task(
        project_id=None,
        type_=TaskType.SELF_REMINDER,
        status=TaskStatus.OPEN,
        created_by=user_id,
        assigned_to=user_id,
        due_at_iso=due_iso,
        payload={"comment": text, "source": "self_reminder"},
    )
    try:
        await db.accept_task(int(task["id"]))
    except Exception:
        log.exception("self_reminder: accept_task failed for #%s", task.get("id"))
    when_str = from_iso(due_iso).astimezone(tzinfo(config.timezone)).strftime("%d.%m.%Y %H:%M")
    await target.answer(
        "✅ <b>Напоминание поставлено</b>\n"
        f"🕒 {when_str}\n"
        f"📝 {_html.escape(text)}"
    )


@router.message(F.text == MGR_BTN_REMIND, RoleFilter(_SELF_REMINDER_ROLES))
async def start_self_reminder(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=_SELF_REMINDER_ROLES):
        return
    await state.clear()
    await state.set_state(SelfReminderSG.text)
    await message.answer(
        "⏰ <b>Напоминание</b>\n\n"
        "О чём напомнить? Напишите текст:"
    )


@router.callback_query(F.data == "selfrem_new")
async def start_self_reminder_cb(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """Вход в самонапоминалку из инлайн-пункта экрана «📋 Все задачи» (для ГД).

    У менеджера/РП точка входа — reply-кнопка «⏰ Напомнить» в меню; у ГД её нет,
    поэтому для ГД вход — отдельным пунктом внутри «Все задачи». Дальше — общий
    флоу (SelfReminderSG.text → пресеты «Когда?» → _finalize_self_reminder).
    Callback `selfrem_new` не пересекается с `selfrem:` (пресеты) и
    `selfrem_cancel:` (отмена) — startswith-обработчики их не ловят.
    """
    if not await require_role_callback(cb, db, roles=_SELF_REMINDER_ROLES):
        return
    await cb.answer()
    await state.clear()
    await state.set_state(SelfReminderSG.text)
    await cb.message.answer(  # type: ignore[union-attr]
        "⏰ <b>Напоминание</b>\n\n"
        "О чём напомнить? Напишите текст:"
    )


@router.message(SelfReminderSG.text)
async def self_reminder_text(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text.lower() in _SELFREM_CANCEL_WORDS:
        await state.clear()
        await message.answer("❌ Отменено.")
        return
    if len(text) < 2:
        await message.answer("Напишите текст напоминания (минимум 2 символа):")
        return
    await state.update_data(reminder_text=text)
    await state.set_state(SelfReminderSG.when)
    await message.answer("🕒 Когда напомнить?", reply_markup=_self_reminder_when_kb())


@router.callback_query(F.data.startswith("selfrem:"))
async def self_reminder_when(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    from datetime import timedelta
    from ..utils import utcnow, tzinfo
    await cb.answer()
    if not cb.from_user:
        return
    # F6: снять inline-клавиатуру пресетов с исходного сообщения, чтобы старые
    # кнопки «Когда?» не висели активными после выбора/финала. В ветках, где
    # клавиатура показывается заново (прошедшее «сегодня 18:00» / неизвестный
    # вариант), новое сообщение получит свежую клавиатуру — дублей не будет.
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    choice = (cb.data or "").split(":", 1)[-1]

    if choice == "cancel":
        await state.clear()
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]
        return

    if choice == "custom":
        await state.set_state(SelfReminderSG.custom_date)
        await cb.message.answer(  # type: ignore[union-attr]
            "📅 Введите дату — например <b>07 марта</b> или <b>15.03.2026</b>:"
        )
        return

    tz = tzinfo(config.timezone)
    now_utc = utcnow()
    now_local = now_utc.astimezone(tz)
    if choice == "1h":
        due = now_utc + timedelta(hours=1)
    elif choice == "3h":
        due = now_utc + timedelta(hours=3)
    elif choice == "today18":
        due = now_local.replace(hour=18, minute=0, second=0, microsecond=0)
        if due <= now_local:
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Сегодня 18:00 уже прошло. Выберите другой вариант:",
                reply_markup=_self_reminder_when_kb(),
            )
            return
    elif choice == "tmrw10":
        due = (now_local + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)
    else:
        await cb.message.answer("Неизвестный вариант. Выберите на клавиатуре:")  # type: ignore[union-attr]
        return

    await _finalize_self_reminder(cb.message, state, db, config, cb.from_user.id, due)  # type: ignore[arg-type]


@router.message(SelfReminderSG.custom_date)
async def self_reminder_custom_date(message: Message, state: FSMContext, config: Config) -> None:
    from ..utils import parse_date, to_iso
    text = (message.text or "").strip()
    if text.lower() in _SELFREM_CANCEL_WORDS:
        await state.clear()
        await message.answer("❌ Отменено.")
        return
    parsed = parse_date(text, config.timezone)
    if not parsed:
        await message.answer(
            "Не удалось распознать дату.\n"
            "Укажите в формате <b>07 марта</b> или <b>дд.мм.гггг</b>:"
        )
        return
    await state.update_data(reminder_date=to_iso(parsed))
    await state.set_state(SelfReminderSG.custom_time)
    await message.answer("🕒 Укажите время — например <b>14:00</b> (или просто <b>14</b>):")


@router.message(SelfReminderSG.custom_time)
async def self_reminder_custom_time(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    import re as _re
    from ..utils import from_iso, tzinfo, utcnow
    if not message.from_user:
        return
    text = (message.text or "").strip()
    if text.lower() in _SELFREM_CANCEL_WORDS:
        await state.clear()
        await message.answer("❌ Отменено.")
        return
    m = _re.fullmatch(r"(\d{1,2})[:.\s](\d{2})", text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
    else:
        m2 = _re.fullmatch(r"(\d{1,2})", text)
        if not m2:
            await message.answer("Не удалось распознать время. Формат <b>14:00</b> или <b>14</b>:")
            return
        hour, minute = int(m2.group(1)), 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        await message.answer("Некорректное время. Укажите от 00:00 до 23:59:")
        return
    data = await state.get_data()
    date_iso = data.get("reminder_date")
    if not date_iso:
        await state.clear()
        await message.answer("❌ Дата потеряна. Начните заново кнопкой «⏰ Напомнить».")
        return
    tz = tzinfo(config.timezone)
    due = from_iso(date_iso).astimezone(tz).replace(hour=hour, minute=minute, second=0, microsecond=0)
    if due <= utcnow().astimezone(tz):
        await message.answer("⚠️ Это время уже прошло. Укажите время в будущем (или напишите «Отмена»):")
        return
    await _finalize_self_reminder(message, state, db, config, message.from_user.id, due)


@router.callback_query(F.data.startswith("selfrem_cancel:"))
async def self_reminder_cancel(cb: CallbackQuery, db: Database) -> None:
    """Отмена личной напоминалки из «Все задачи» (кнопка «🗑 Отменить напоминание»).

    CAS-закрываем задачу (DONE): reminders_loop берёт только open/in_progress,
    поэтому закрытая уже не сработает. Отменить может только сам постановщик
    (assigned_to). Callback-префикс `selfrem_cancel:` не пересекается с `selfrem:`
    (FSM-пресеты) — разные обработчики.
    """
    if not cb.from_user:
        await cb.answer()
        return
    raw = (cb.data or "").split(":")[-1]
    if not raw.isdigit():
        await cb.answer()
        return
    tid = int(raw)
    try:
        task = await db.get_task(tid)
    except Exception:
        await cb.answer("Напоминание не найдено.", show_alert=True)
        return
    if (
        int(task.get("assigned_to") or 0) != cb.from_user.id
        or task.get("type") != TaskType.SELF_REMINDER
    ):
        await cb.answer("Это не ваше напоминание.", show_alert=True)
        return
    updated = await db.update_task_status(
        tid, TaskStatus.DONE, expected_statuses=("open", "in_progress"),
    )
    await cb.answer("🗑 Отменено" if updated else "Уже неактивно")
    text = (
        "🗑 <b>Напоминание отменено.</b>"
        if updated
        else "ℹ️ Напоминание уже сработало или было отменено."
    )
    try:
        await cb.message.edit_text(text)  # type: ignore[union-attr]
    except Exception:
        pass


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
    """Show detailed invoice card from search results — эталон card-template-standard."""
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

    creator_label = "—"
    if inv.get("created_by"):
        try:
            creator_label = await get_initiator_label(db, int(inv["created_by"]))
        except (TypeError, ValueError):
            pass

    extra_meta: list[str] = []
    created_at = inv.get("created_at")
    if created_at:
        extra_meta.append(f"📅 Создан: {str(created_at)[:10]}")

    primary_h = inv.get("docs_originals_holder")
    closing_h = inv.get("closing_originals_holder")
    if primary_h:
        holder = "ГД" if primary_h == "gd" else "менеджера"
        extra_meta.append(f"📁 Оригиналы первички: у {holder}")
    if closing_h:
        holder = "ГД" if closing_h == "gd" else "менеджера"
        extra_meta.append(f"📁 Оригиналы закрывающих: у {holder}")

    section = await build_invoice_section(db, inv, invoice_id)
    text = format_invoice_card_standard(
        inv=inv,
        creator_label=creator_label,
        section=section,
        comment=inv.get("description") or None,
        extra_meta=extra_meta or None,
    )

    await cb.message.answer(text)  # type: ignore[union-attr]


# =====================================================================
# ЗАМЕРЫ — структурированные заявки на замер
# =====================================================================

@router.message(F.text == MGR_BTN_ZAMERY, RoleFilter(ALL_MANAGER_ROLES))
async def mgr_zamery(message: Message, state: FSMContext, db: Database) -> None:
    """Кнопка «📐 Замеры» — дашборд заявок на замер."""
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
    # blackout_set = выходные + занятые (оба блокируют запись); busy_set — только «занят».
    blackout_set = {bl["blackout_date"] for bl in blackouts}
    busy_set = {bl["blackout_date"] for bl in blackouts if (bl.get("kind") or "off") == "busy"}
    off_set = blackout_set - busy_set

    text = f"📅 <b>{mon.day} {_MGR_RU_MONTHS[mon.month]} — {sun.day} {_MGR_RU_MONTHS[sun.month]}</b>\n\n"

    for i in range(7):
        day = mon + timedelta(days=i)
        ds = day.isoformat()
        wd = _MGR_RU_WEEKDAYS[day.weekday()]
        label = f"{day.day} {_MGR_RU_MONTHS[day.month]} ({wd})"

        if ds in off_set:
            text += f"🚫 <b>{label}</b> — выходной\n"
        elif ds in busy_set:
            if ds in zam_by_date:
                intervals = [z.get("scheduled_time_interval") or "—" for z in zam_by_date[ds]]
                text += f"🔒 <b>{label}</b> — занят: {', '.join(intervals)}\n"
            else:
                text += f"🔒 <b>{label}</b> — занят\n"
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
    """Show next-10-days zamerschik schedule as compact clickable day buttons.

    Без отдельного текстового списка — нажатие на день прямо на графике = выбор даты.
    Статус прямо на кнопке: 🟢 свободно · 🔴 есть замеры. Выходные скрыты (выбрать нельзя).
    """
    today = date.today()
    d_from = today.isoformat()
    d_to = (today + timedelta(days=10)).isoformat()

    summary = await db.get_zamery_schedule_summary(zamery_uid, d_from, d_to)
    busy = summary["busy"]  # date_str → [intervals]
    blackout_set = summary["blackout_dates"]  # set of date_str

    text = (
        "📅 <b>График замерщика</b> (на 10 дней вперёд)\n\n"
        "Нажмите на дату замера — 🟢 свободно · 🔴 есть замеры:"
    )

    b = InlineKeyboardBuilder()
    for i in range(11):
        d = today + timedelta(days=i)
        ds = d.isoformat()
        if ds in blackout_set:
            continue  # выходной — кнопки нет
        wd = _RU_WEEKDAYS_M[d.weekday()]
        icon = "🔴" if ds in busy else "🟢"
        b.button(text=f"{icon} {d.day} {_RU_MONTHS_M[d.month]} ({wd})", callback_data=f"zamsched_mgr:date:{ds}")

    b.button(text="⏭ Пропустить", callback_data="zamsched_mgr:skip")
    b.adjust(2, 2, 2, 2, 2, 1, 1)
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
    from ..config import compute_zamery_cost
    base_cost, mkad_surcharge, total_cost = compute_zamery_cost(mkad_km)
    await state.update_data(
        volume_m2=vol,
        base_cost=base_cost,
        mkad_surcharge=mkad_surcharge,
        total_cost=total_cost,
    )
    cost_text = f"💰 Стоимость замера: <b>{total_cost}₽</b>"
    if mkad_surcharge:
        cost_text += f" (база {base_cost}₽ + МКАД {mkad_surcharge}₽)"
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
async def zamery_attachments(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"manager/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку.")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Файлов: <b>{count}</b>.{suffix}")


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
    from ..config import ZAMERY_BASE_COST
    mkad_km = data.get("mkad_km", 0)
    volume_m2 = data.get("volume_m2")
    base_cost = data.get("base_cost", ZAMERY_BASE_COST)
    mkad_surcharge = data.get("mkad_surcharge", 0)
    total_cost = data.get("total_cost", ZAMERY_BASE_COST)
    scheduled_date = data.get("scheduled_date")
    scheduled_time_interval = data.get("scheduled_time_interval")

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
            minio_object_key=a.get("minio_object_key"),
        )

    source_label = ZAMERY_SOURCE_LABELS.get(source_type, source_type)
    role_short = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(requester_role, "")
    initiator = await get_initiator_label(db, u.id)
    from ..keyboards import task_actions_kb
    task_kb = task_actions_kb(task)
    initiator_full = f"{initiator} ({role_short})" if role_short else initiator
    # Замерщику показываем ТОЛЬКО дивизион менеджера (НПН/КВ/КИА), без имени (user 29.06).
    # initiator_full (с именем) остаётся для уведомления РП ниже.
    msg = (
        f"📐 <b>Заявка на замер: #{zam_req_id}</b>\n"
        f"🏢 Менеджер: {role_short or '—'}\n\n"
        f"📍 Адрес: {address}\n"
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
    msg += f"💰 Стоимость замера: {total_cost}₽\n"
    if scheduled_date:
        try:
            sd = date.fromisoformat(scheduled_date)
            _wd = _RU_WEEKDAYS_M[sd.weekday()]
            msg += f"📅 Дата: {sd.day} {_RU_MONTHS_M[sd.month]} ({_wd})"
            if scheduled_time_interval:
                msg += f" ⏰ {scheduled_time_interval}"
            msg += "\n"
        except Exception:
            pass
    msg += f"🔗 Источник: {source_label}"
    if description:
        msg += f"\n\n💬 Пояснение: {description}"

    await notifier.safe_send(int(zamery_id_user), msg, reply_markup=task_kb)
    for a in attachments:
        await notifier.safe_send_media(int(zamery_id_user), a["file_type"], a["file_id"], caption=a.get("caption"))
    await refresh_recipient_keyboard(notifier, db, config, int(zamery_id_user))

    # Уведомить РП о новой заявке на замер (все источники)
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    if rp_id:
        rp_msg = (
            f"📐 <b>Заявка на замер: #{zam_req_id}</b>\n"
            f"👤 От: {initiator_full}\n\n"
            f"📍 Адрес: {address}\n"
        )
        if client_contact:
            rp_msg += f"📞 Контакт: {client_contact}\n"
        rp_msg += f"🔗 Источник: {source_label}"
        if source_type == "lead" and lead_task_id:
            rp_msg += f"\n🎯 Лид: #{lead_task_id}"
        if description:
            rp_msg += f"\n\n💬 Пояснение: {description}"
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
# ТРАТА КРЕДИТНОГО КОШЕЛЬКА (CreditWalletSpendSG) — TZ кошелёк 02.06
# Единая форма: менеджер (свой кошелёк), РП (выбор кошелька inline),
# ГД (вход из finance-канала — entry в chat_proxy, кошелёк=канал).
# Привязка → create_supplier_payment (cost_*/DP–DV); без привязки →
# add_op_company_entry («Баланс компании» I/J). Реестр трат — credit_spends.
# manager_new.router включён раньше rp/td/gd → его state-хендлеры (cwspend:)
# ловят все три роли. Старый ManagerCreditExpenseSG (credit_expenses) удалён.
# =====================================================================

CREDIT_WALLET_ROLES = ("manager_kv", "manager_kia", "manager_npn")
_CREDIT_COST_CHOICES = [
    ("metal", "Металл"), ("glass", "Стекло"), ("montazh", "Монтаж"),
    ("loaders", "Грузчики"), ("logistics", "Логистика"),
    ("extra_mat", "Доп. материалы"), ("extra_svc", "Доп. услуги"),
]
_CREDIT_COST_LABELS = dict(_CREDIT_COST_CHOICES)


async def _cw_show_mode(target: Message, state: FSMContext, db: Database) -> None:
    """Показать баланс кошелька + выбор режима (привязка / без)."""
    data = await state.get_data()
    role = data.get("wallet_role") or ""
    try:
        card = await build_credit_wallet_card(db, role)
    except Exception:
        card = ""
    await state.set_state(CreditWalletSpendSG.pick_mode)
    b = InlineKeyboardBuilder()
    b.button(text="🔗 С привязкой к счёту", callback_data="cwspend:mode:bound")
    b.button(text="📄 Без привязки", callback_data="cwspend:mode:free")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(1)
    head = card + "\n\n" if card else ""
    await target.answer(
        head
        + "Выберите режим траты:\n"
        "🔗 <b>С привязкой</b> — к счёту в работе (ляжет в его расходы DP–DV)\n"
        "📄 <b>Без привязки</b> — в «Баланс компании»",
        reply_markup=b.as_markup(),
    )


# --- Вход: менеджер (кошелёк = своя роль) ---
@router.message(F.text == MGR_BTN_CRED_ADD)
async def cw_start_manager(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    user = await db.get_user_optional(message.from_user.id)  # type: ignore[union-attr]
    role_raw = (getattr(user, "role", "") or "")
    wallet = next((r for r in CREDIT_WALLET_ROLES if r in role_raw), None)
    if not wallet:
        await message.answer("❌ Кредитный кошелёк есть только у менеджеров КВ/КИА/НПН.")
        return
    await state.clear()
    await state.update_data(wallet_role=wallet, spender_role="manager")
    await _cw_show_mode(message, state, db)


# --- Вход: РП (выбор кошелька inline) ---
@router.message(F.text == RP_BTN_CREDIT_SPEND)
async def cw_start_rp(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP, Role.TD]):
        return
    await state.clear()
    await state.update_data(spender_role="rp")
    await state.set_state(CreditWalletSpendSG.pick_wallet)
    b = InlineKeyboardBuilder()
    for r in CREDIT_WALLET_ROLES:
        b.button(text=f"🏦 {credit_wallet_label(r)}", callback_data=f"cwspend:wallet:{r}")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(1)
    await message.answer("🏦 <b>Расход кредита</b>\n\nЧей кошелёк тратим?", reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("cwspend:wallet:"), CreditWalletSpendSG.pick_wallet)
async def cw_pick_wallet(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    role = cb.data.split(":")[-1]  # type: ignore[union-attr]
    if role not in CREDIT_WALLET_ROLES:
        await cb.answer("Некорректный кошелёк", show_alert=True)
        return
    await state.update_data(wallet_role=role)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _cw_show_mode(cb.message, state, db)  # type: ignore[arg-type]


# --- Режим ---
@router.callback_query(F.data == "cwspend:mode:free", CreditWalletSpendSG.pick_mode)
async def cw_mode_free(cb: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(mode="free", invoice_id=None, invoice_number="", cost_type=None)
    await state.set_state(CreditWalletSpendSG.amount)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Введите сумму расхода ₽ (например: <code>15000</code>):")  # type: ignore[union-attr]


@router.callback_query(F.data == "cwspend:mode:withdraw", CreditWalletSpendSG.pick_mode)
async def cw_mode_withdraw(cb: CallbackQuery, state: FSMContext) -> None:
    # «Вывод ДС» (TZ 09.06) — как «Без привязки», но запись ТОЛЬКО в кредит-баланс
    # (не на «Баланс компании»). Привязки/категории нет; дальше — сумма → назначение.
    await state.update_data(mode="withdraw", invoice_id=None, invoice_number="", cost_type=None)
    await state.set_state(CreditWalletSpendSG.amount)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Введите сумму вывода ₽ (например: <code>15000</code>):")  # type: ignore[union-attr]


@router.callback_query(F.data == "cwspend:mode:bound", CreditWalletSpendSG.pick_mode)
async def cw_mode_bound(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    wallet = data.get("wallet_role") or ""
    spender = data.get("spender_role") or ""
    # §B (TZ 04.06): scope списка по роли инициатора. ГД/РП видят ВСЕ материнские
    # счета «в работе» (parent_invoice_id IS NULL); менеджер — только свои
    # (creator_role = кошелёк). Кредит виден РП/ГД штатно (не бухгалтерия).
    # ТЗ 19.06: + последние 10 счетов «Счёт End» (закрытые) тем же scope — чтобы
    # можно было привязать поздний расход к недавно закрытому счёту (user 29.06: 5→10).
    if spender in ("rp", "gd"):
        invoices = await db.list_invoices_in_work(limit=60, include_credit=True)
        invoices = [i for i in invoices if not i.get("parent_invoice_id")]
        ended = await db.list_ended_invoices(limit=25, include_credit=True)
        ended = [i for i in ended if not i.get("parent_invoice_id")]
        empty_msg = (
            "❌ Нет счетов для привязки (ни «в работе», ни закрытых).\n"
            "Выберите «📄 Без привязки» или /cancel."
        )
    else:
        invoices = await db.list_invoices_in_work(
            limit=40, include_credit=True, creator_role=wallet,
        )
        ended = await db.list_ended_invoices(
            limit=25, include_credit=True, creator_role=wallet,
        )
        empty_msg = (
            f"❌ У {credit_wallet_label(wallet)} нет счетов для привязки "
            "(ни «в работе», ни закрытых).\n"
            "Выберите «📄 Без привязки» или /cancel."
        )
    # Дедуп: credit+invoice_end попадает И в «в работе» (как открытый credit), И в
    # ended — оставляем один раз (в «в работе»). Новые в списке = именно status='ended'.
    inwork_ids = {i["id"] for i in invoices}
    ended = [i for i in ended if i["id"] not in inwork_ids][:10]
    if not invoices and not ended:
        await cb.answer()
        await cb.message.answer(empty_msg)  # type: ignore[union-attr]
        return
    await state.update_data(mode="bound")
    await state.set_state(CreditWalletSpendSG.pick_invoice)
    b = InlineKeyboardBuilder()
    for inv in invoices[:30]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        lbl = f"📄 {num}" + (f" · {addr}" if addr else "")
        b.button(text=lbl, callback_data=f"cwspend:inv:{inv['id']}")
    # Закрытые («Счёт End») — после «в работе», помечены 🏁 (привязка позднего расхода).
    for inv in ended:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        lbl = f"🏁 {num}" + (f" · {addr}" if addr else "")
        b.button(text=lbl, callback_data=f"cwspend:inv:{inv['id']}")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(1)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    cnt = f"{len(invoices)} в работе"
    if ended:
        cnt += f" + {len(ended)} закрытых 🏁"
    await cb.message.answer(  # type: ignore[union-attr]
        f"Выберите счёт ({cnt}):", reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwspend:inv:"), CreditWalletSpendSG.pick_invoice)
async def cw_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.answer("Счёт не найден", show_alert=True)
        return
    await state.update_data(
        invoice_id=inv_id, invoice_number=inv.get("invoice_number") or f"#{inv_id}",
    )
    await state.set_state(CreditWalletSpendSG.pick_category)
    b = InlineKeyboardBuilder()
    for ct, lbl in _CREDIT_COST_CHOICES:
        b.button(text=lbl, callback_data=f"cwspend:cat:{ct}")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(2)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Категория затрат:", reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("cwspend:cat:"), CreditWalletSpendSG.pick_category)
async def cw_pick_category(cb: CallbackQuery, state: FSMContext) -> None:
    ct = cb.data.split(":")[-1]  # type: ignore[union-attr]
    if ct not in _CREDIT_COST_LABELS:
        await cb.answer("Некорректная категория", show_alert=True)
        return
    await state.update_data(cost_type=ct)
    await state.set_state(CreditWalletSpendSG.amount)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Введите сумму расхода ₽ (например: <code>15000</code>):")  # type: ignore[union-attr]


async def _cw_gd_back(message: Message, state: FSMContext) -> bool:
    """ГД-выход «⬅️ Назад» с шагов суммы/назначения кредит-расхода → меню канала.

    cwspend amount/purpose ловятся manager_new (router раньше chat_proxy), поэтому
    reply-«⬅️ Назад» меню finance-канала ГД нужно перехватить здесь отдельно
    (на picker-шагах это делает chat_proxy.cw_back_from_picker). GD-only. user 03.06.
    """
    if (message.text or "").strip() != "⬅️ Назад":
        return False
    data = await state.get_data()
    if data.get("spender_role") != "gd":
        return False
    from .chat_proxy import _cw_exit_to_channel
    await _cw_exit_to_channel(message, state)
    return True


async def _cw_gd_restore_menu(
    message: Message, state: FSMContext, spender_role: str, channel: str,
) -> None:
    """После confirm/cancel расхода кредита ГД — вернуть меню finance-канала.

    cw_confirm/cw_cancel делают state.clear(); у ГД (вход из канала) reply-«⬅️ Назад»
    привязана к ChatProxySG.menu и после clear висит unhandled (user live 04.06).
    GD-only: менеджер (spender_role='manager') и РП ('rp') возвращаются в своё
    главное меню, где «Назад» не state-bound — их не трогаем.
    """
    if spender_role != "gd" or not channel:
        return
    from .chat_proxy import enter_chat_menu
    await enter_chat_menu(message, state, channel)


@router.message(CreditWalletSpendSG.amount)
async def cw_amount(message: Message, state: FSMContext, db: Database) -> None:
    if await _mgr_amount_escape(message, state, db):
        return
    if await _cw_gd_back(message, state):
        return
    txt = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(txt)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Сумма должна быть положительным числом. Попробуйте ещё раз:")
        return
    await state.update_data(amount=amount)
    await state.set_state(CreditWalletSpendSG.purpose)
    await message.answer("Введите назначение (например: <code>Оплата стекла Зенит</code>):")


@router.message(CreditWalletSpendSG.purpose)
async def cw_purpose(message: Message, state: FSMContext) -> None:
    if await _cw_gd_back(message, state):
        return
    import html as _html
    desc = (message.text or "").strip()
    if not (3 <= len(desc) <= 200):
        await message.answer("⚠️ Назначение должно быть 3–200 символов. Попробуйте ещё раз:")
        return
    await state.update_data(purpose=desc)
    # §B (TZ 04.06): опц. шаг вложения (фото/документ) перед подтверждением.
    await state.set_state(CreditWalletSpendSG.attach)
    b = InlineKeyboardBuilder()
    b.button(text="⏭️ Пропустить (без файла)", callback_data="cwspend:attach:skip")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(1)
    await message.answer(
        f"Назначение: {_html.escape(desc)}\n\n"
        "📎 По желанию приложите файл (фото/документ) — счёт, реквизиты, фото.\n"
        "Или нажмите «Пропустить»:",
        reply_markup=b.as_markup(),
    )


async def _cw_show_confirm(target: Message, state: FSMContext) -> None:
    """Экран подтверждения расхода кредита (§B: + пометка вложения)."""
    import html as _html
    await state.set_state(CreditWalletSpendSG.confirm)
    data = await state.get_data()
    wallet = credit_wallet_label(data.get("wallet_role") or "")
    amount = float(data.get("amount") or 0)
    desc = (data.get("purpose") or "").strip()
    if data.get("mode") == "bound":
        cat = _CREDIT_COST_LABELS.get(data.get("cost_type") or "", "—")
        bind_s = f"Счёт №{data.get('invoice_number')} · {cat}"
    elif data.get("mode") == "withdraw":
        bind_s = "Вывод ДС → только кредитный баланс"
    else:
        bind_s = "Без привязки → «Баланс компании»"
    file_line = "\n  📎 Вложение приложено" if data.get("attach_file_id") else ""
    b = InlineKeyboardBuilder()
    b.button(text="✅ Записать", callback_data="cwspend:confirm")
    b.button(text="❌ Отмена", callback_data="cwspend:cancel")
    b.adjust(1)
    await target.answer(
        "<b>Подтвердите расход:</b>\n\n"
        f"  Кошелёк: <b>{wallet}</b>\n"
        f"  {bind_s}\n"
        f"  Сумма: <b>{amount:,.0f}</b> ₽\n"
        f"  Назначение: {_html.escape(desc)}"
        f"{file_line}",
        reply_markup=b.as_markup(),
    )


async def rp_start_credit_to_manager(
    cb: CallbackQuery, state: FSMContext, db: Database, *, wallet_role: str,
) -> None:
    """§E мост: РП «Счёт на оплату» (кредит) → расход из кредит-кошелька менеджера.

    Переносит уже собранные в InvoiceCreateSG данные (привязка к материнскому
    счёту, сумма, тип затрат, поставщик, вложение) в состояние CreditWalletSpendSG
    и показывает штатный экран подтверждения (_cw_show_confirm). Дальше — обычный
    cw_confirm: гард двойного клика + (для чужого кошелька) отложенная задача
    владельцу с записью расхода на исполнении; своя трата (Павел-РП→НПН) спишется
    сразу. Путь записи денег ТОТ ЖЕ, что у §C — дублирования нет.
    """
    data = await state.get_data()
    parent_id = data.get("parent_invoice_id")
    amount = float(data.get("amount") or 0)
    cost_type = data.get("material_type")
    supplier = (data.get("supplier") or "").strip()
    supplier_inv_no = (data.get("invoice_number") or "").strip()
    comment = (data.get("comment") or "").strip()
    attachments = data.get("attachments") or []

    # Назначение расхода: поставщик + № счёта поставщика (+ коммент), ≤200 симв.
    parts = []
    if supplier:
        parts.append(supplier)
    if supplier_inv_no:
        parts.append(f"счёт {supplier_inv_no}")
    purpose = ", ".join(parts) or "Оплата поставщику"
    if comment:
        purpose = f"{purpose} — {comment}"
    purpose = purpose[:200]

    # Привязка к материнскому счёту: номер — для отображения (запись идёт по invoice_id).
    bound_inv_no = ""
    if parent_id:
        try:
            parent_inv = await db.get_invoice(int(parent_id))
            bound_inv_no = (parent_inv or {}).get("invoice_number") or ""
        except Exception:
            bound_inv_no = ""

    # Первое вложение (опц.) → в payload задачи владельцу.
    attach_file_id = attach_file_type = None
    if attachments:
        a0 = attachments[0]
        attach_file_id = a0.get("file_id")
        attach_file_type = a0.get("file_type")

    await state.update_data(
        wallet_role=wallet_role,
        spender_role="rp",
        channel=wallet_role,
        amount=amount,
        purpose=purpose,
        mode="bound",
        invoice_id=int(parent_id) if parent_id else None,
        cost_type=cost_type,
        invoice_number=bound_inv_no,
        attach_file_id=attach_file_id,
        attach_file_type=attach_file_type,
    )
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"✅ Маршрут: <b>На оплату к {credit_wallet_label(wallet_role)}</b>"
        )
    except Exception:
        pass
    await _cw_show_confirm(cb.message, state)  # type: ignore[arg-type]


@router.callback_query(F.data == "cwspend:attach:skip", CreditWalletSpendSG.attach)
async def cw_attach_skip(cb: CallbackQuery, state: FSMContext) -> None:
    """§B: «Пропустить» вложение → экран подтверждения."""
    await state.update_data(attach_file_id=None, attach_file_type=None)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await _cw_show_confirm(cb.message, state)  # type: ignore[arg-type]


@router.message(CreditWalletSpendSG.attach)
async def cw_attach_file(message: Message, state: FSMContext, db: Database) -> None:
    """§B: инициатор прислал фото/документ → сохранить → экран подтверждения.

    Гарды как на шаге суммы: manager-кнопки меню (escape) и ГД «⬅️ Назад».
    """
    if await _mgr_amount_escape(message, state, db):
        return
    if await _cw_gd_back(message, state):
        return
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
            "Пришлите фото/документ, либо нажмите «Пропустить» под предыдущим сообщением."
        )
        return
    await state.update_data(attach_file_id=file_id, attach_file_type=file_type)
    await _cw_show_confirm(message, state)


@router.callback_query(F.data == "cwspend:cancel")
async def cw_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена FSM на любом шаге."""
    data = await state.get_data()
    spender_role = data.get("spender_role") or ""
    channel = data.get("wallet_role") or data.get("channel") or ""
    await state.clear()
    try:
        await cb.message.edit_text("❌ Отменено.")  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()
    if cb.message:
        await _cw_gd_restore_menu(cb.message, state, spender_role, channel)  # type: ignore[arg-type]


# Анти-двойное-списание confirm-экрана расхода кредита (инцидент 05.06: один
# платёж 18 000 ₽ записался 3 раза от повторных кликов «✅ Записать», пока шла
# медленная запись с синком Google-таблиц, а сброс состояния — только в конце).
# Бот — один процесс (single polling), поэтому module-level set достаточно.
_CW_CONFIRM_INFLIGHT: set[tuple[int, int]] = set()


@router.callback_query(F.data == "cwspend:confirm", CreditWalletSpendSG.confirm)
async def cw_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Gate против двойного списания → _cw_confirm_impl.

    aiogram обрабатывает колбэки конкурентно, а _cw_confirm_impl
    (apply_credit_wallet_spend + синки таблиц) длится ~секунды. Без gate
    повторные клики «✅ Записать» успевали пройти фильтр состояния (оно
    сбрасывается лишь в конце impl) и провести дубль расхода. Claim синхронный:
    между `key in set` и `set.add` нет await, поэтому два конкурентных колбэка
    не могут оба пройти. finally освобождает claim даже при исключении.
    """
    u = cb.from_user
    if not u:
        return
    key = (u.id, cb.message.message_id if cb.message else 0)
    if key in _CW_CONFIRM_INFLIGHT:
        await cb.answer("Уже обрабатываю, секунду…")
        return
    _CW_CONFIRM_INFLIGHT.add(key)
    # Убрать кнопки сразу — не приглашать к повторному нажатию (defense-in-depth).
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    try:
        await _cw_confirm_impl(cb, state, db, config, notifier, integrations)
    finally:
        _CW_CONFIRM_INFLIGHT.discard(key)


async def _cw_confirm_impl(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Подтверждение траты кошелька (TZ 04.06 §C).

    Своя трата (spender = владелец кошелька, вкл. Павел-РП→НПН) → запись СРАЗУ
    через apply_credit_wallet_spend (DP–DV/«Баланс компании» + credit_spend +
    carry-DA + синки + audit) + инфо ГД/РП.
    Чужой кошелёк → запись ОТКЛАДЫВАЕТСЯ: создаётся задача-платёжка владельцу
    с полным payload; расход спишется при подтверждении «исполнения» менеджером
    (credit_recv → credit_exec в chat_proxy).
    """
    import html as _html

    u = cb.from_user
    if not u:
        return
    data = await state.get_data()
    wallet_role = data.get("wallet_role") or ""
    spender_role = data.get("spender_role") or ""
    gd_channel = wallet_role or data.get("channel") or ""
    amount = float(data.get("amount") or 0)
    purpose = (data.get("purpose") or "").strip()
    mode = data.get("mode")
    inv_id = data.get("invoice_id")
    cost_type = data.get("cost_type")
    inv_num = data.get("invoice_number") or ""
    attach_file_id = data.get("attach_file_id")
    attach_file_type = data.get("attach_file_type")
    if wallet_role not in CREDIT_WALLET_ROLES or amount <= 0 or not purpose:
        await cb.answer("⚠️ Данные сессии утеряны", show_alert=True)
        await state.clear()
        return

    wlabel = credit_wallet_label(wallet_role)

    # Владелец кошелька — резолвим ДО записи (TZ 04.06 §C): при трате с ЧУЖОГО
    # кошелька запись расхода ОТКЛАДЫВАЕТСЯ до подтверждения «исполнения»
    # менеджером-владельцем. Своя трата (вкл. Павел-РП на кошельке НПН, т.к.
    # resolve(MANAGER_NPN)=Павел) — пишется сразу.
    owner_enum = {
        "manager_kv": Role.MANAGER_KV, "manager_kia": Role.MANAGER_KIA,
        "manager_npn": Role.MANAGER_NPN,
    }.get(wallet_role)
    owner_id = None
    if owner_enum:
        try:
            owner_id = await resolve_default_assignee(db, config, owner_enum)
        except Exception:
            owner_id = None
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    rp_id = await resolve_default_assignee(db, config, Role.RP)
    is_own = (owner_id is None) or (int(owner_id) == u.id)

    initiator = await get_initiator_label(db, u.id)
    if mode == "bound":
        bind_line = f"Счёт №{inv_num} · {_CREDIT_COST_LABELS.get(cost_type or '', '—')}"
    elif mode == "withdraw":
        bind_line = "Вывод ДС → только кредитный баланс"
    else:
        bind_line = "Без привязки → «Баланс компании»"
    info = format_card_section(
        "🏦", f"Расход кредита — {wlabel}",
        [
            ("Внёс", initiator),
            ("Привязка", bind_line),
            ("Назначение", _html.escape(purpose)),
        ],
        total=fmt_money(amount), width=38, compact=True,
    )

    async def _notify_gd_rp() -> None:
        # Инфо-карточка ГД (если не сам) + РП (если не он сам) + опц. вложение (§B).
        async def _send(uid: int) -> None:
            await notifier.safe_send(uid, info)
            if attach_file_id and attach_file_type:
                try:
                    await notifier.safe_send_media(
                        uid, attach_file_type, attach_file_id,
                        caption="📎 Вложение к расходу кредита",
                    )
                except Exception:
                    log.debug("cw_confirm: forward attach to %s failed", uid, exc_info=True)
        seen: set[int] = set()
        if gd_id and int(gd_id) != u.id:
            await _send(int(gd_id))
            seen.add(int(gd_id))
        if rp_id and int(rp_id) != u.id and int(rp_id) not in seen:
            await _send(int(rp_id))

    if is_own:
        # --- Своя трата хозяина: запись СРАЗУ, без гейта подтверждения ГД. ---
        # Гейт (п.5, 12.06) снят owner'ом 10.08: списание кредит-кошелька легитимно
        # ровно тогда, когда хозяин сам привязал трату к счёту и выбрал статью —
        # спрашивать на это разрешение у ГД незачем, и это расходилось с правилом
        # «запрос инициирует сама роль». ГД и РП получают инфо-карточку постфактум.
        # Money-хендлер: любой сбой записи обязан быть виден человеку, иначе
        # обёртка cw_confirm (try/finally, без except) оставит вечный спиннер.
        try:
            res = await apply_credit_wallet_spend(
                db, integrations,
                wallet_role=wallet_role, amount=amount, mode=mode or "",
                purpose=purpose, entered_by=u.id,
                invoice_id=int(inv_id) if inv_id else None,
                cost_type=cost_type, invoice_number=inv_num,
            )
        except Exception:
            log.exception("cw_confirm: apply_credit_wallet_spend failed (own spend)")
            await state.clear()
            await cb.answer("⚠️ Не удалось записать расход — повторите", show_alert=True)
            if cb.message:
                await _cw_gd_restore_menu(cb.message, state, spender_role, gd_channel)  # type: ignore[arg-type]
            return
        spend_id = res.get("spend_id")

        # Триггер наёмной ЗП — раньше срабатывал ПОСЛЕ ✅ГД, в chat_proxy
        # (_credit_spend_finalize). Со снятием гейта этот путь сюда больше не
        # доходит, поэтому хук переносится вместе с ним: без него оплата стекла/
        # допматов по наёмному счёту перестала бы поднимать ГД-задачу на ЗП монтаж,
        # то есть один из пяти путей ввода затрат замолчал бы молча.
        if cost_type and inv_id:
            try:
                from .installer_new import on_invoice_cost_recorded
                _naem = await on_invoice_cost_recorded(
                    db, config, notifier, integrations,
                    invoice_id=int(inv_id),
                    material_type=str(cost_type),
                    amount=amount,
                    actor_id=u.id,
                )
                if _naem.get("created"):
                    log.info(
                        "naem_zp: задача ГД открыта по расходу кошелька %s, счёт=%s",
                        cost_type, inv_num,
                    )
            except Exception:
                log.warning(
                    "naem_zp: авто-задача ЗП не создана (cw own spend=%s)",
                    spend_id, exc_info=True,
                )

        await _notify_gd_rp()
        await state.clear()
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                format_card_section(
                    "✅", f"Расход записан — {wlabel}",
                    [
                        ("Привязка", bind_line),
                        ("Назначение", _html.escape(purpose)),
                        ("№ расхода", f"#{spend_id}"),
                    ],
                    total=fmt_money(amount), width=38, compact=True,
                ),
                reply_markup=_cw_edit_actions_kb(int(spend_id)),
            )
        except Exception:
            pass
        await cb.answer("Записано")
        if cb.message:
            await _cw_gd_restore_menu(cb.message, state, spender_role, gd_channel)  # type: ignore[arg-type]
        return

    # --- Чужой кошелёк: НЕ писать в БД. Задача-платёжка владельцу (§C, 2 этапа). ---
    # Запись расхода произойдёт при подтверждении «исполнения» менеджером
    # (обработчик credit_exec в chat_proxy) — payload несёт все данные записи.
    task_ok = False
    tid = 0  # id созданной задачи (для кнопок правки/отмены инициатором, Фаза 2)
    try:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.INVOICE_PAYMENT,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=int(owner_id),
            due_at_iso=None,
            payload={
                "kind": "credit_payment_request",
                "wallet_role": wallet_role,
                "amount": amount,
                "purpose": purpose,
                "invoice_number": inv_num,
                "mode": mode,
                "invoice_id": int(inv_id) if inv_id else None,
                "cost_type": cost_type,
                "initiator_id": u.id,
                "initiator_file_id": attach_file_id,
                "initiator_file_type": attach_file_type,
                "applied": False,
            },
        )
        tid = int(task["id"])
        b2 = InlineKeyboardBuilder()
        b2.button(text="✅ Получил", callback_data=f"credit_recv:{tid}")
        b2.adjust(1)
        attach_line = "📎 <i>Вложение прикреплено</i>\n" if attach_file_id else ""
        card_owner = format_card_section(
            "💳", f"Расход с твоего баланса — {wlabel}",
            [
                ("Просит", initiator),
                ("Привязка", bind_line),
                ("Назначение", _html.escape(purpose)),
            ],
            total=fmt_money(amount), width=38, compact=True,
        )
        await notifier.safe_send(
            int(owner_id),
            f"{card_owner}\n\n{attach_line}"
            "Подтвердите получение задачи кнопкой ниже.",
            reply_markup=b2.as_markup(),
        )
        if attach_file_id and attach_file_type:
            try:
                await notifier.safe_send_media(
                    int(owner_id), attach_file_type, attach_file_id,
                    caption="📎 Вложение к расходу кредита",
                )
            except Exception:
                log.debug("cw_confirm: forward initiator file to owner failed", exc_info=True)
        task_ok = True
    except Exception:
        log.warning("cw_confirm: credit_payment_request task failed", exc_info=True)

    await _notify_gd_rp()
    await state.clear()
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            format_card_section(
                "📨", f"Запрос отправлен — {wlabel}",
                [
                    ("Привязка", bind_line),
                    ("Назначение", _html.escape(purpose)),
                ],
                total=fmt_money(amount),
                footer=("Статус", "спишется после исполнения"),
                width=38, compact=True,
            )
            if task_ok else
            "⚠️ Не удалось создать задачу менеджеру — повторите позже.",
            reply_markup=_cw_task_edit_actions_kb(tid) if task_ok else None,
        )
    except Exception:
        pass
    await cb.answer("Отправлено" if task_ok else "Ошибка")
    if cb.message:
        await _cw_gd_restore_menu(cb.message, state, spender_role, gd_channel)  # type: ignore[arg-type]


# =====================================================================
# ПРАВКА / ОТМЕНА расхода кредита инициатором (TZ #3, Фаза 1: СВОЯ трата)
# =====================================================================
# Только инициатор. Своя трата (записана сразу) → реверс старого
# (cancel_credit_spend) + перезапись нового (apply_credit_wallet_spend) либо
# просто реверс (отмена). Анти-двойной-клик гард на confirm (как
# _CW_CONFIRM_INFLIGHT). Чужой кошелёк (задача) — Фазы 2-3.
_CW_EDIT_INFLIGHT: set[tuple[int, int]] = set()


def _cw_edit_actions_kb(spend_id: int):
    """Кнопки под карточкой «Расход записан» (своя трата) — правка/отмена."""
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Изменить сумму и назначение", callback_data=f"cwedit:a:{spend_id}")
    b.button(text="❌ Отменить расход", callback_data=f"cwcanc:a:{spend_id}")
    b.adjust(1)
    return b.as_markup()


def _cw_bind_line(mode: str | None, invoice_number: str, cost_type: str | None) -> str:
    if mode == "bound":
        cat = _CREDIT_COST_LABELS.get(cost_type or "", "—")
        return f"Счёт №{invoice_number} · {cat}"
    if mode == "withdraw":
        return "Вывод ДС → только кредитный баланс"
    return "Без привязки → «Баланс компании»"


def _msk_now_str() -> str:
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    return _dt.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%y %H:%M")


async def _cw_initiator_guard(
    cb: CallbackQuery, db: Database, spend_id: int,
) -> dict | None:
    """Вернуть credit_spend, если нажал ИНИЦИАТОР; иначе alert + None."""
    spend = await db.get_credit_spend(spend_id)
    if not spend:
        await cb.answer("Расход уже отменён или не найден", show_alert=True)
        return None
    if int(spend.get("entered_by") or 0) != int(cb.from_user.id):
        await cb.answer("Изменить может только инициатор расхода", show_alert=True)
        return None
    return spend


async def _cw_resync_after_change(integrations, db: Database, snap: dict) -> None:
    """Ресинк листов/строк после реверса траты (snap из cancel_credit_spend)."""
    try:
        if snap.get("mode") == "bound" and snap.get("bound_invoice_id"):
            await integrations.sync_invoice_row(int(snap["bound_invoice_id"]))
        elif getattr(integrations, "sheets", None):
            await integrations.sheets.sync_balance_company_sheet(db)
    except Exception:
        log.warning("_cw_resync_after_change: bind resync failed", exc_info=True)
    try:
        if snap.get("active_credit_invoice_id"):
            await integrations.sync_invoice_row(int(snap["active_credit_invoice_id"]))
    except Exception:
        log.warning("_cw_resync_after_change: active resync failed", exc_info=True)
    try:
        if getattr(integrations, "sheets", None):
            await integrations.sheets.sync_advances_journal_sheet(db)
    except Exception:
        log.warning("_cw_resync_after_change: advances resync failed", exc_info=True)


async def _cw_notify_credit_change(
    db: Database, config: Config, notifier: Notifier, actor_id: int, text: str,
) -> None:
    """Инфо об изменении/отмене расхода → ГД + РП (кроме actor). Бухгалтерии НЕ слать."""
    seen: set[int] = set()
    for role in (Role.GD, Role.RP):
        try:
            uid = await resolve_default_assignee(db, config, role)
        except Exception:
            uid = None
        if uid and int(uid) != int(actor_id) and int(uid) not in seen:
            seen.add(int(uid))
            try:
                await notifier.safe_send(int(uid), text)
            except Exception:
                log.debug("_cw_notify_credit_change: send to %s failed", uid, exc_info=True)


async def _cw_edit_show_mode(target: Message, state: FSMContext) -> None:
    await state.set_state(CreditWalletEditSG.pick_mode)
    b = InlineKeyboardBuilder()
    b.button(text="🔗 С привязкой к счёту", callback_data="cwedit:mode:bound")
    b.button(text="📄 Без привязки", callback_data="cwedit:mode:free")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await target.answer("Изменение назначения — выберите привязку:", reply_markup=b.as_markup())


async def _cw_edit_show_confirm(target: Message, state: FSMContext) -> None:
    import html as _html
    await state.set_state(CreditWalletEditSG.confirm)
    d = await state.get_data()
    wlabel = credit_wallet_label(d.get("wallet_role") or "")
    new_amount = float(d.get("new_amount") or 0)
    new_purpose = (d.get("new_purpose") or "").strip()
    bind = _cw_bind_line(d.get("new_mode"), d.get("new_invoice_number") or "", d.get("new_cost_type"))
    old_amount = float(d.get("old_amount") or 0)
    old_purpose = d.get("old_purpose") or ""
    changes: list[str] = []
    if abs(new_amount - old_amount) > 0.001:
        changes.append(f"сумма {fmt_money(old_amount)} → {fmt_money(new_amount)}")
    if new_purpose != old_purpose:
        changes.append("назначение изменено")
    diff = "; ".join(changes) or "без изменений"
    b = InlineKeyboardBuilder()
    b.button(text="✅ Сохранить", callback_data="cwedit:save")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await target.answer(
        f"✏️ <b>Подтвердите правку — {wlabel}</b>\n\n"
        f"  {bind}\n"
        f"  Сумма: <b>{new_amount:,.0f}</b> ₽\n"
        f"  Назначение: {_html.escape(new_purpose)}\n"
        f"  🔁 {diff}",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwedit:a:"))
async def cw_edit_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    import html as _html
    spend_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    spend = await _cw_initiator_guard(cb, db, spend_id)
    if not spend:
        return
    old_mode = "bound" if spend.get("bound_invoice_id") else "free"
    old_inv_num = ""
    if old_mode == "bound" and spend.get("bound_invoice_id"):
        try:
            inv = await db.get_invoice(int(spend["bound_invoice_id"]))
            old_inv_num = (inv or {}).get("invoice_number") or ""
        except Exception:
            old_inv_num = ""
    await state.clear()
    await state.set_state(CreditWalletEditSG.pick_field)
    await state.update_data(
        edit_spend_id=spend_id,
        wallet_role=spend.get("wallet_role") or "",
        old_amount=float(spend.get("amount") or 0),
        old_purpose=spend.get("description") or "",
        old_mode=old_mode,
        old_cost_type=spend.get("cost_type"),
        old_invoice_id=spend.get("bound_invoice_id"),
        old_invoice_number=old_inv_num,
        # prefill new_* = old_* (правится только выбранное)
        new_amount=float(spend.get("amount") or 0),
        new_purpose=spend.get("description") or "",
        new_mode=old_mode,
        new_cost_type=spend.get("cost_type"),
        new_invoice_id=spend.get("bound_invoice_id"),
        new_invoice_number=old_inv_num,
    )
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Сумму", callback_data="cwedit:field:amount")
    b.button(text="✏️ Назначение", callback_data="cwedit:field:purpose")
    b.button(text="✏️ И сумму, и назначение", callback_data="cwedit:field:both")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✏️ <b>Правка расхода #{spend_id}</b>\n"
        f"Сейчас: {fmt_money(float(spend.get('amount') or 0))} · "
        f"{_html.escape(spend.get('description') or '')}\n\nЧто изменить?",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwedit:field:"), CreditWalletEditSG.pick_field)
async def cw_edit_pick_field(cb: CallbackQuery, state: FSMContext) -> None:
    which = cb.data.split(":")[-1]  # type: ignore[union-attr]
    await state.update_data(edit_fields=which)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    if which in ("amount", "both"):
        await state.set_state(CreditWalletEditSG.amount)
        await cb.message.answer("Введите НОВУЮ сумму расхода ₽ (например: <code>15000</code>):")  # type: ignore[union-attr]
    else:
        await _cw_edit_show_mode(cb.message, state)  # type: ignore[arg-type]


@router.message(CreditWalletEditSG.amount)
async def cw_edit_amount(message: Message, state: FSMContext, db: Database) -> None:
    if await _mgr_amount_escape(message, state, db):
        return
    t = (message.text or "").strip()
    if t.casefold() in ("/cancel", "отмена", "❌ отмена"):
        await state.clear()
        await message.answer("✏️ Правка отменена.")
        return
    txt = t.replace(" ", "").replace(",", ".")
    try:
        amount = float(txt)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Сумма должна быть положительным числом. Ещё раз:")
        return
    await state.update_data(new_amount=amount)
    d = await state.get_data()
    if d.get("edit_fields") == "both":
        await _cw_edit_show_mode(message, state)
    else:
        await _cw_edit_show_confirm(message, state)


@router.callback_query(F.data == "cwedit:mode:free", CreditWalletEditSG.pick_mode)
async def cw_edit_mode_free(cb: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(new_mode="free", new_invoice_id=None, new_invoice_number="", new_cost_type=None)
    await state.set_state(CreditWalletEditSG.purpose)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Введите НОВОЕ назначение (3–200 символов):")  # type: ignore[union-attr]


@router.callback_query(F.data == "cwedit:mode:bound", CreditWalletEditSG.pick_mode)
async def cw_edit_mode_bound(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    d = await state.get_data()
    wallet = d.get("wallet_role") or ""
    invoices = await db.list_invoices_in_work(limit=40, include_credit=True, creator_role=wallet)
    if not invoices:
        await cb.answer()
        await cb.message.answer(  # type: ignore[union-attr]
            "❌ Нет счетов «в работе» для привязки. Выберите «📄 Без привязки» или ❌ Отмена."
        )
        return
    await state.update_data(new_mode="bound")
    await state.set_state(CreditWalletEditSG.pick_invoice)
    b = InlineKeyboardBuilder()
    for inv in invoices[:30]:
        num = inv.get("invoice_number") or f"#{inv['id']}"
        addr = (inv.get("object_address") or "")[:20]
        b.button(text=f"📄 {num}" + (f" · {addr}" if addr else ""), callback_data=f"cwedit:inv:{inv['id']}")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"Выберите счёт ({len(invoices)} в работе):", reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwedit:inv:"), CreditWalletEditSG.pick_invoice)
async def cw_edit_pick_invoice(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(inv_id)
    if not inv:
        await cb.answer("Счёт не найден", show_alert=True)
        return
    await state.update_data(
        new_invoice_id=inv_id, new_invoice_number=inv.get("invoice_number") or f"#{inv_id}",
    )
    await state.set_state(CreditWalletEditSG.pick_category)
    b = InlineKeyboardBuilder()
    for ct, lbl in _CREDIT_COST_CHOICES:
        b.button(text=lbl, callback_data=f"cwedit:cat:{ct}")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(2)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Категория затрат:", reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("cwedit:cat:"), CreditWalletEditSG.pick_category)
async def cw_edit_pick_category(cb: CallbackQuery, state: FSMContext) -> None:
    ct = cb.data.split(":")[-1]  # type: ignore[union-attr]
    if ct not in _CREDIT_COST_LABELS:
        await cb.answer("Некорректная категория", show_alert=True)
        return
    await state.update_data(new_cost_type=ct)
    await state.set_state(CreditWalletEditSG.purpose)
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer("Введите НОВОЕ назначение (3–200 символов):")  # type: ignore[union-attr]


@router.message(CreditWalletEditSG.purpose)
async def cw_edit_purpose(message: Message, state: FSMContext, db: Database) -> None:
    if await _mgr_amount_escape(message, state, db):
        return
    t = (message.text or "").strip()
    if t.casefold() in ("/cancel", "отмена", "❌ отмена"):
        await state.clear()
        await message.answer("✏️ Правка отменена.")
        return
    if not (3 <= len(t) <= 200):
        await message.answer("⚠️ Назначение должно быть 3–200 символов. Ещё раз:")
        return
    await state.update_data(new_purpose=t)
    d = await state.get_data()
    if d.get("edit_case") == "c":
        await _cw_reattr_show_confirm(message, state)
    else:
        await _cw_edit_show_confirm(message, state)


@router.callback_query(F.data == "cwedit:abort")
async def cw_edit_abort(cb: CallbackQuery, state: FSMContext) -> None:
    d = await state.get_data()
    edit_case = d.get("edit_case")
    spend_id = int(d.get("edit_spend_id") or 0)
    task_id = int(d.get("edit_task_id") or 0)
    await state.clear()
    await cb.answer("Правка отменена")
    try:
        if edit_case == "c" and task_id:
            await cb.message.edit_text(  # type: ignore[union-attr]
                "🔁 Перенос назначения отменён. Расход не изменён.",
                reply_markup=_cw_reattr_actions_kb(task_id),
            )
        elif spend_id:
            await cb.message.edit_text(  # type: ignore[union-attr]
                "✏️ Правка отменена. Расход не изменён.",
                reply_markup=_cw_edit_actions_kb(spend_id),
            )
        elif task_id:
            await cb.message.edit_text(  # type: ignore[union-attr]
                "✏️ Правка отменена. Запрос не изменён.",
                reply_markup=_cw_task_edit_actions_kb(task_id),
            )
        else:
            await cb.message.edit_text("✏️ Правка отменена.")  # type: ignore[union-attr]
    except Exception:
        pass


@router.callback_query(F.data == "cwedit:save", CreditWalletEditSG.confirm)
async def cw_edit_save(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Сохранить правку своей траты: реверс старого + перезапись нового. Гард дубля."""
    import html as _html
    u = cb.from_user
    key = (u.id, cb.message.message_id if cb.message else 0)
    if key in _CW_EDIT_INFLIGHT:
        await cb.answer("Уже обрабатываю…")
        return
    _CW_EDIT_INFLIGHT.add(key)
    try:
        try:
            await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
        except Exception:
            pass
        d = await state.get_data()
        # Фаза 2 (случай b): чужой кошелёк, НЕ исполнен → правка payload задачи,
        # баланс/cost_* НЕ трогаются (расхода в БД ещё нет). Ветвимся ДО реверса.
        if d.get("edit_case") == "b":
            await _cw_edit_task_save(cb, state, db, config, notifier, d)
            return
        # Фаза 3 (случай c, режим D): чужой ИСПОЛНЕН → перенос привязки, сумма
        # ФИКС, total_da инвариант. credit_expenses не трогаем — реверса НЕТ.
        if d.get("edit_case") == "c":
            await _cw_reattribute_save(cb, state, db, config, notifier, integrations, d)
            return
        spend_id = int(d.get("edit_spend_id") or 0)
        spend = await db.get_credit_spend(spend_id)
        if not spend or int(spend.get("entered_by") or 0) != int(u.id):
            await cb.answer("Расход недоступен (отменён/чужой)", show_alert=True)
            await state.clear()
            return
        new_amount = float(d.get("new_amount") or 0)
        new_mode = d.get("new_mode") or "free"
        new_purpose = (d.get("new_purpose") or "").strip()
        new_invoice_id = d.get("new_invoice_id")
        new_cost_type = d.get("new_cost_type")
        new_invoice_number = d.get("new_invoice_number") or ""
        wallet_role = d.get("wallet_role") or spend.get("wallet_role") or ""
        old_amount = float(d.get("old_amount") or 0)
        old_purpose = d.get("old_purpose") or ""

        # (1) реверс старого
        try:
            old_snap = await db.cancel_credit_spend(
                spend_id, u.id, f"edit→{new_amount:.0f}", action="credit_spend_edit",
            )
        except KeyError:
            await cb.answer("Расход уже отменён", show_alert=True)
            await state.clear()
            return
        await _cw_resync_after_change(integrations, db, old_snap)
        if not old_snap.get("credit_expense_reversed"):
            # CX не реверснулся однозначно → НЕ писать новый (иначе задвоение баланса)
            await db.audit(
                actor_id=u.id, action="credit_spend_amend_failed",
                entity="credit_spends", entity_id=str(spend_id),
                payload={"stage": "reverse_ce", "old": old_snap},
            )
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Не удалось однозначно отменить баланс старого расхода.\n"
                "Старая запись удалена, НОВАЯ не записана — обратитесь к ГД."
            )
            await state.clear()
            return

        # (2) перезапись нового
        try:
            res = await apply_credit_wallet_spend(
                db, integrations,
                wallet_role=wallet_role, amount=new_amount, mode=new_mode,
                purpose=new_purpose, entered_by=u.id,
                invoice_id=int(new_invoice_id) if new_invoice_id else None,
                cost_type=new_cost_type, invoice_number=new_invoice_number,
            )
        except Exception:
            log.warning("cw_edit_save: apply failed after reverse spend=%s", spend_id, exc_info=True)
            await db.audit(
                actor_id=u.id, action="credit_spend_amend_failed",
                entity="credit_spends", entity_id=str(spend_id),
                payload={"stage": "rewrite", "old": old_snap, "new_amount": new_amount},
            )
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Старый расход отменён, но НОВЫЙ не записан (ошибка). "
                "ГД — запишите вручную или повторите."
            )
            await state.clear()
            return
        new_spend_id = int(res.get("spend_id"))

        await db.audit(
            actor_id=u.id, action="credit_spend_amended",
            entity="credit_spends", entity_id=str(new_spend_id),
            payload={
                "old_spend_id": spend_id,
                "old": {"amount": old_amount, "purpose": old_purpose},
                "new": {"amount": new_amount, "purpose": new_purpose},
            },
        )

        wlabel = credit_wallet_label(wallet_role)
        bind = _cw_bind_line(new_mode, new_invoice_number, new_cost_type)
        msk = _msk_now_str()
        diff_parts: list[str] = []
        if abs(new_amount - old_amount) > 0.001:
            diff_parts.append(f"было {fmt_money(old_amount)} → стало {fmt_money(new_amount)}")
        if new_purpose != old_purpose:
            diff_parts.append("назначение изменено")
        diff = "; ".join(diff_parts) or "без изменений"
        card = format_card_section(
            "✏️", f"Расход изменён — {wlabel}",
            [
                ("Привязка", bind),
                ("Назначение", _html.escape(new_purpose)),
                ("№ расхода", f"#{new_spend_id}"),
                ("Было→стало", diff),
                ("Время (МСК)", msk),
            ],
            total=fmt_money(new_amount), width=38, compact=True,
        )
        try:
            await cb.message.edit_text(card, reply_markup=_cw_edit_actions_kb(new_spend_id))  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(card, reply_markup=_cw_edit_actions_kb(new_spend_id))  # type: ignore[union-attr]
        info = format_card_section(
            "✏️", f"Расход кредита изменён — {wlabel}",
            [
                ("Изменил", await get_initiator_label(db, u.id)),
                ("Привязка", bind),
                ("Назначение", _html.escape(new_purpose)),
                ("Было→стало", diff),
                ("Время (МСК)", msk),
            ],
            total=fmt_money(new_amount), width=38, compact=True,
        )
        await _cw_notify_credit_change(db, config, notifier, u.id, info)
        await cb.answer("Изменено")
        await state.clear()
    finally:
        _CW_EDIT_INFLIGHT.discard(key)


@router.callback_query(F.data.startswith("cwcanc:a:"))
async def cw_cancel_start(cb: CallbackQuery, db: Database) -> None:
    spend_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    spend = await _cw_initiator_guard(cb, db, spend_id)
    if not spend:
        return
    await cb.answer()
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да, отменить", callback_data=f"cwcanc:yes:{spend_id}")
    b.button(text="↩️ Нет", callback_data=f"cwcanc:no:{spend_id}")
    b.adjust(1)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"❌ Отменить расход #{spend_id} на {fmt_money(float(spend.get('amount') or 0))}?\n"
        "Баланс кошелька будет восстановлен.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwcanc:no:"))
async def cw_cancel_no(cb: CallbackQuery) -> None:
    spend_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await cb.answer("Отмена прервана")
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "Отмена расхода прервана.", reply_markup=_cw_edit_actions_kb(spend_id),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("cwcanc:yes:"))
async def cw_cancel_yes(
    cb: CallbackQuery, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Отмена своей траты: полный реверс + восстановление баланса. Гард дубля."""
    u = cb.from_user
    spend_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    key = (u.id, spend_id)
    if key in _CW_EDIT_INFLIGHT:
        await cb.answer("Уже обрабатываю…")
        return
    _CW_EDIT_INFLIGHT.add(key)
    try:
        try:
            await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
        except Exception:
            pass
        spend = await db.get_credit_spend(spend_id)
        if not spend or int(spend.get("entered_by") or 0) != int(u.id):
            await cb.answer("Расход недоступен", show_alert=True)
            return
        amount = float(spend.get("amount") or 0)
        try:
            snap = await db.cancel_credit_spend(
                spend_id, u.id, "initiator_cancel", action="credit_spend_cancel",
            )
        except KeyError:
            await cb.answer("Расход уже отменён", show_alert=True)
            return
        await _cw_resync_after_change(integrations, db, snap)
        wlabel = credit_wallet_label(snap.get("wallet_role") or "")
        msk = _msk_now_str()
        reversed_ok = bool(snap.get("credit_expense_reversed"))
        card = format_card_section(
            "❌", f"Расход отменён — {wlabel}",
            [("Сумма", fmt_money(amount)), ("Время (МСК)", msk)],
            footer=("Итог", "баланс восстановлен" if reversed_ok else "проверьте баланс у ГД"),
            width=38, compact=True,
        )
        try:
            await cb.message.edit_text(card)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(card)  # type: ignore[union-attr]
        info = format_card_section(
            "❌", f"Расход кредита отменён — {wlabel}",
            [
                ("Отменил", await get_initiator_label(db, u.id)),
                ("Сумма", fmt_money(amount)),
                ("Время (МСК)", msk),
            ],
            footer=("Итог", "баланс восстановлен" if reversed_ok else "проверьте баланс"),
            width=38, compact=True,
        )
        await _cw_notify_credit_change(db, config, notifier, u.id, info)
        await cb.answer("Отменено")
    finally:
        _CW_EDIT_INFLIGHT.discard(key)


# =====================================================================
# ПРАВКА / ОТМЕНА запроса кредита инициатором (TZ #3, Фаза 2: ЧУЖОЙ
# кошелёк, НЕ исполнен) — случай (b).
# =====================================================================
# Инициатор (РП/ГД/менеджер) потратил ЧУЖОЙ кошелёк → создана задача-платёжка
# владельцу (credit_payment_request, payload applied=False). Расход в БД ещё НЕ
# записан — пишется владельцем при «исполнении» (chat_proxy._finalize_credit_
# execution читает СВЕЖИЙ payload). Поэтому правка = merge-patch payload задачи
# (баланс/cost_* не трогаются), отмена = REJECT задачи (CAS). Только инициатор
# (по created_by, НЕ assigned_to — то владелец). FSM CreditWalletEditSG общая с
# Фазой 1, ветвление на confirm по edit_case=="b".


def _cw_task_edit_actions_kb(task_id: int):
    """Кнопки под карточкой «Запрос отправлен» (чужой кошелёк) — правка/отмена."""
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Изменить сумму и назначение", callback_data=f"cwedit:t:{task_id}")
    b.button(text="❌ Отменить запрос", callback_data=f"cwcanc:t:{task_id}")
    b.adjust(1)
    return b.as_markup()


def _cw_task_payload(task: dict) -> dict:
    """Распарсить payload_json задачи в dict (как chat_proxy._parse_task_payload)."""
    import json as _json
    raw = task.get("payload_json") or {}
    if isinstance(raw, str):
        try:
            raw = _json.loads(raw)
        except Exception:
            raw = {}
    return dict(raw) if isinstance(raw, dict) else {}


def _classify_credit_task(task: dict, p: dict) -> str:
    """Классифицировать §C-задачу-платёжку для правки инициатором.

    'b'         — чужой кошелёк, НЕ исполнен (OPEN/IN_PROGRESS, applied=False) → правка/отмена.
    'c'         — исполнен (DONE / applied / credit_spend_id) → только назначение (режим D, Фаза 3).
    'cancelled' — отменён инициатором (REJECTED + маркер).
    'rejected'  — отклонён владельцем (REJECTED).
    'other'     — не §C-отложенный запрос (старый CreditTaskSG и пр.) → не редактируем.
    """
    if p.get("kind") != "credit_payment_request" or "applied" not in p:
        return "other"
    st = task.get("status")
    if st == TaskStatus.REJECTED:
        return "cancelled" if p.get("cancelled_by_initiator") else "rejected"
    if st == TaskStatus.DONE or p.get("applied") is True or p.get("credit_spend_id"):
        return "c"
    if st in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        return "b"
    return "other"


async def _cw_task_initiator_guard(cb: CallbackQuery, db: Database, tid: int) -> dict | None:
    """Вернуть задачу, если нажал ИНИЦИАТОР (created_by); иначе alert + None."""
    try:
        task = await db.get_task(tid)
    except KeyError:
        await cb.answer("Запрос не найден или удалён", show_alert=True)
        return None
    if int(task.get("created_by") or 0) != int(cb.from_user.id):
        await cb.answer("Изменить может только инициатор запроса", show_alert=True)
        return None
    return task


async def _cw_notify_request_change(
    db: Database, config: Config, notifier: Notifier,
    actor_id: int, owner_id: int, text: str,
) -> None:
    """Инфо об изменении/отмене запроса → владелец + ГД + РП (кроме actor, без дублей).

    Бухгалтерии кредит-инфо НЕ слать ([[feedback_credit_filter_accounting_only]]).
    """
    targets: list[int] = []
    seen: set[int] = set()
    if owner_id and int(owner_id) != int(actor_id):
        seen.add(int(owner_id))
        targets.append(int(owner_id))
    for role in (Role.GD, Role.RP):
        try:
            uid = await resolve_default_assignee(db, config, role)
        except Exception:
            uid = None
        if uid and int(uid) != int(actor_id) and int(uid) not in seen:
            seen.add(int(uid))
            targets.append(int(uid))
    for uid in targets:
        try:
            await notifier.safe_send(uid, text)
        except Exception:
            log.debug("_cw_notify_request_change: send to %s failed", uid, exc_info=True)


@router.callback_query(F.data.startswith("cwedit:t:"))
async def cw_task_edit_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """Фаза 2: вход правки запроса (чужой кошелёк, НЕ исполнен). Переиспользует FSM."""
    import html as _html
    tid = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await _cw_task_initiator_guard(cb, db, tid)
    if not task:
        return
    p = _cw_task_payload(task)
    cls = _classify_credit_task(task, p)
    if cls == "c":
        # Режим D (Фаза 3): владелец исполнил → сумма ФИКС, переносим только
        # назначение (привязку/счёт/категорию/текст). total_da не сдвигается.
        await _cw_reattr_start(cb, state, db, tid, p)
        return
    if cls in ("rejected", "cancelled"):
        await cb.answer("Запрос уже отменён или отклонён.", show_alert=True)
        return
    if cls != "b":
        await cb.answer("Этот запрос недоступен для правки.", show_alert=True)
        return

    old_amount = float(p.get("amount") or 0)
    old_purpose = p.get("purpose") or ""
    old_mode = p.get("mode") or ("bound" if p.get("invoice_id") else "free")
    old_ct = p.get("cost_type")
    old_inv_id = p.get("invoice_id")
    old_inv_num = p.get("invoice_number") or ""
    await state.clear()
    await state.set_state(CreditWalletEditSG.pick_field)
    await state.update_data(
        edit_case="b", edit_task_id=tid,
        wallet_role=p.get("wallet_role") or "",
        old_amount=old_amount, old_purpose=old_purpose, old_mode=old_mode,
        old_cost_type=old_ct, old_invoice_id=old_inv_id, old_invoice_number=old_inv_num,
        # prefill new_* = old_* (правится только выбранное)
        new_amount=old_amount, new_purpose=old_purpose, new_mode=old_mode,
        new_cost_type=old_ct, new_invoice_id=old_inv_id, new_invoice_number=old_inv_num,
    )
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Сумму", callback_data="cwedit:field:amount")
    b.button(text="✏️ Назначение", callback_data="cwedit:field:purpose")
    b.button(text="✏️ И сумму, и назначение", callback_data="cwedit:field:both")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✏️ <b>Правка запроса #{tid}</b> (ещё не исполнен)\n"
        f"Сейчас: {fmt_money(old_amount)} · {_html.escape(old_purpose)}\n\nЧто изменить?",
        reply_markup=b.as_markup(),
    )


async def _cw_edit_task_save(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, d: dict,
) -> None:
    """Confirm правки запроса (b): merge-patch payload задачи. Баланс НЕ трогаем.

    Вызывается из cw_edit_save под гардом _CW_EDIT_INFLIGHT. Перед записью повторно
    проверяем статус задачи (race с исполнением владельцем): если уже исполнена/
    отклонена — правку не применяем.
    """
    import html as _html
    u = cb.from_user
    tid = int(d.get("edit_task_id") or 0)
    if not tid:
        await cb.answer("Запрос потерян", show_alert=True)
        await state.clear()
        return
    try:
        task = await db.get_task(tid)
    except KeyError:
        await cb.answer("Запрос не найден", show_alert=True)
        await state.clear()
        return
    if int(task.get("created_by") or 0) != int(u.id):
        await cb.answer("Изменить может только инициатор", show_alert=True)
        await state.clear()
        return
    p = _cw_task_payload(task)
    cls = _classify_credit_task(task, p)
    if cls != "b":
        # Владелец успел исполнить/отклонить между входом и confirm.
        msg = (
            "⚠️ Владелец уже исполнил запрос — правка суммы не применена."
            if cls == "c" else
            "⚠️ Запрос уже отменён/отклонён — правка не применена."
        )
        await cb.message.answer(msg)  # type: ignore[union-attr]
        await cb.answer()
        await state.clear()
        return

    new_amount = float(d.get("new_amount") or 0)
    new_mode = d.get("new_mode") or "free"
    new_purpose = (d.get("new_purpose") or "").strip()
    bound = new_mode == "bound"
    new_inv_id = d.get("new_invoice_id") if bound else None
    new_ct = d.get("new_cost_type") if bound else None
    new_inv_num = (d.get("new_invoice_number") or "") if bound else ""
    old_amount = float(d.get("old_amount") or 0)
    old_purpose = d.get("old_purpose") or ""
    wallet_role = d.get("wallet_role") or p.get("wallet_role") or ""

    # merge-patch payload (явно перезаписываем bound-поля, чтобы не осталось stale)
    await db.update_task_payload(tid, {
        "amount": new_amount,
        "purpose": new_purpose,
        "mode": new_mode,
        "invoice_id": int(new_inv_id) if new_inv_id else None,
        "cost_type": new_ct,
        "invoice_number": new_inv_num,
        "edited_by": u.id,
        "edited_at": _msk_now_str(),
    })
    try:
        await db.audit(
            actor_id=u.id, action="credit_request_amended",
            entity="tasks", entity_id=str(tid),
            payload={
                "old": {"amount": old_amount, "purpose": old_purpose},
                "new": {"amount": new_amount, "purpose": new_purpose},
            },
        )
    except Exception:
        log.debug("_cw_edit_task_save: audit failed tid=%s", tid, exc_info=True)

    wlabel = credit_wallet_label(wallet_role)
    bind = _cw_bind_line(new_mode, new_inv_num, new_ct)
    msk = _msk_now_str()
    diff_parts: list[str] = []
    if abs(new_amount - old_amount) > 0.001:
        diff_parts.append(f"было {fmt_money(old_amount)} → стало {fmt_money(new_amount)}")
    if new_purpose != old_purpose:
        diff_parts.append("назначение изменено")
    diff = "; ".join(diff_parts) or "без изменений"
    card = format_card_section(
        "✏️", f"Запрос изменён — {wlabel}",
        [
            ("Привязка", bind),
            ("Назначение", _html.escape(new_purpose)),
            ("Запрос", f"#{tid}"),
            ("Было→стало", diff),
            ("Время (МСК)", msk),
        ],
        total=fmt_money(new_amount),
        footer=("Статус", "спишется после исполнения"),
        width=38, compact=True,
    )
    try:
        await cb.message.edit_text(card, reply_markup=_cw_task_edit_actions_kb(tid))  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(card, reply_markup=_cw_task_edit_actions_kb(tid))  # type: ignore[union-attr]

    owner_id = int(task.get("assigned_to") or 0)
    info = format_card_section(
        "✏️", f"Запрос кредита изменён — {wlabel}",
        [
            ("Изменил", await get_initiator_label(db, u.id)),
            ("Привязка", bind),
            ("Назначение", _html.escape(new_purpose)),
            ("Было→стало", diff),
            ("Время (МСК)", msk),
        ],
        total=fmt_money(new_amount),
        footer=("Статус", "исполните по новым данным"),
        width=38, compact=True,
    )
    await _cw_notify_request_change(db, config, notifier, u.id, owner_id, info)
    await cb.answer("Изменено")
    await state.clear()


@router.callback_query(F.data.startswith("cwcanc:t:"))
async def cw_task_cancel_start(cb: CallbackQuery, db: Database) -> None:
    """Фаза 2: подтверждение отмены запроса (чужой кошелёк, НЕ исполнен)."""
    tid = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    task = await _cw_task_initiator_guard(cb, db, tid)
    if not task:
        return
    p = _cw_task_payload(task)
    cls = _classify_credit_task(task, p)
    if cls != "b":
        if cls == "c":
            await cb.answer("Уже исполнено владельцем — отменить нельзя.", show_alert=True)
        else:
            await cb.answer("Запрос уже отменён или отклонён.", show_alert=True)
        return
    amount = float(p.get("amount") or 0)
    await cb.answer()
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да, отменить запрос", callback_data=f"cwcanc:tyes:{tid}")
    b.button(text="↩️ Нет", callback_data=f"cwcanc:tno:{tid}")
    b.adjust(1)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"❌ Отменить запрос #{tid} на {fmt_money(amount)}?\n"
        "Владелец кошелька не сможет его исполнить.",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwcanc:tno:"))
async def cw_task_cancel_no(cb: CallbackQuery) -> None:
    tid = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    await cb.answer("Отмена прервана")
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "Отмена запроса прервана.", reply_markup=_cw_task_edit_actions_kb(tid),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("cwcanc:tyes:"))
async def cw_task_cancel_yes(
    cb: CallbackQuery, db: Database, config: Config, notifier: Notifier,
) -> None:
    """Отмена запроса (b): REJECT задачи (CAS) + маркер. Расхода в БД нет. Гард дубля."""
    import html as _html
    u = cb.from_user
    tid = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    key = (u.id, tid)
    if key in _CW_EDIT_INFLIGHT:
        await cb.answer("Уже обрабатываю…")
        return
    _CW_EDIT_INFLIGHT.add(key)
    try:
        try:
            await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
        except Exception:
            pass
        try:
            task = await db.get_task(tid)
        except KeyError:
            await cb.answer("Запрос не найден", show_alert=True)
            return
        if int(task.get("created_by") or 0) != int(u.id):
            await cb.answer("Отменить может только инициатор", show_alert=True)
            return
        p = _cw_task_payload(task)
        # CAS: отменяем только пока OPEN/IN_PROGRESS (владелец не исполнил).
        won = await db.update_task_status(
            tid, TaskStatus.REJECTED,
            expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
        )
        if won is None:
            await cb.answer("Уже исполнено — отменить нельзя", show_alert=True)
            try:
                await cb.message.answer(  # type: ignore[union-attr]
                    "⚠️ Владелец уже исполнил/обработал запрос — отмена невозможна."
                )
            except Exception:
                pass
            return
        try:
            await db.update_task_payload(
                tid, {"cancelled_by_initiator": True, "cancel_reason": "initiator_cancel"}
            )
        except Exception:
            log.debug("cw_task_cancel_yes: update_task_payload failed tid=%s", tid, exc_info=True)
        try:
            await db.audit(
                actor_id=u.id, action="credit_request_cancelled",
                entity="tasks", entity_id=str(tid),
                payload={"amount": float(p.get("amount") or 0), "purpose": p.get("purpose")},
            )
        except Exception:
            log.debug("cw_task_cancel_yes: audit failed tid=%s", tid, exc_info=True)

        wlabel = credit_wallet_label(p.get("wallet_role") or "")
        amount = float(p.get("amount") or 0)
        msk = _msk_now_str()
        card = format_card_section(
            "❌", f"Запрос отменён — {wlabel}",
            [("Запрос", f"#{tid}"), ("Время (МСК)", msk)],
            total=fmt_money(amount),
            footer=("Итог", "владелец не спишет"),
            width=38, compact=True,
        )
        try:
            await cb.message.edit_text(card)  # type: ignore[union-attr]
        except Exception:
            await cb.message.answer(card)  # type: ignore[union-attr]
        owner_id = int(task.get("assigned_to") or 0)
        info = format_card_section(
            "❌", f"Запрос кредита отменён инициатором — {wlabel}",
            [
                ("Отменил", await get_initiator_label(db, u.id)),
                ("Назначение", _html.escape(p.get("purpose") or "—")),
                ("Время (МСК)", msk),
            ],
            total=fmt_money(amount),
            footer=("Итог", "исполнять не нужно"),
            width=38, compact=True,
        )
        await _cw_notify_request_change(db, config, notifier, u.id, owner_id, info)
        await cb.answer("Отменено")
    finally:
        _CW_EDIT_INFLIGHT.discard(key)


# =====================================================================
# РЕЖИМ D (TZ #3, Фаза 3): чужой кошелёк, УЖЕ ИСПОЛНЕН владельцем —
# перенос ТОЛЬКО назначения (сумма ФИКС). Вход — та же cwedit:t:{tid} на
# карточке инициатора (cls=="c"). Переиспользует FSM CreditWalletEditSG
# (pick_mode→[bound:pick_invoice→pick_category]→purpose→confirm). На confirm
# ветвится по edit_case=="c" → db.reattribute_credit_spend: credit_expenses
# НЕ трогается → total_da инвариант, реверса/сдвига баланса НЕТ.
# =====================================================================


def _cw_reattr_actions_kb(task_id: int):
    """Кнопка под карточкой «Назначение перенесено» (режим D) — повторный перенос."""
    b = InlineKeyboardBuilder()
    b.button(text="🔁 Изменить назначение", callback_data=f"cwedit:t:{task_id}")
    b.adjust(1)
    return b.as_markup()


async def _cw_reattr_start(
    cb: CallbackQuery, state: FSMContext, db: Database, tid: int, p: dict,
) -> None:
    """Вход режима D: исполненный чужой расход → перенос назначения (сумма ФИКС)."""
    sid = p.get("credit_spend_id")
    spend = await db.get_credit_spend(int(sid)) if sid else None
    if not spend:
        await cb.answer("Расход не найден — обновите карточку.", show_alert=True)
        return
    old_mode = "bound" if spend.get("bound_invoice_id") else "free"
    old_inv_num = ""
    if old_mode == "bound" and spend.get("bound_invoice_id"):
        try:
            inv = await db.get_invoice(int(spend["bound_invoice_id"]))
            old_inv_num = (inv or {}).get("invoice_number") or ""
        except Exception:
            old_inv_num = ""
    amount = float(spend.get("amount") or 0)
    await state.clear()
    await state.update_data(
        edit_case="c", edit_task_id=tid, edit_spend_id=int(sid),
        wallet_role=spend.get("wallet_role") or p.get("wallet_role") or "",
        old_amount=amount, old_purpose=spend.get("description") or "",
        old_mode=old_mode, old_cost_type=spend.get("cost_type"),
        old_invoice_id=spend.get("bound_invoice_id"), old_invoice_number=old_inv_num,
        # prefill new_* = old_*; сумма ФИКС (не редактируется в режиме D)
        new_amount=amount, new_purpose=spend.get("description") or "",
        new_mode=old_mode, new_cost_type=spend.get("cost_type"),
        new_invoice_id=spend.get("bound_invoice_id"), new_invoice_number=old_inv_num,
    )
    await cb.answer()
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
        f"🔁 <b>Перенос назначения — расход #{int(sid)}</b>\n"
        f"Сумма: <b>{fmt_money(amount)}</b> (фиксирована — переносим только назначение)\n"
        f"Сейчас: {_cw_bind_line(old_mode, old_inv_num, spend.get('cost_type'))}"
    )
    await _cw_edit_show_mode(cb.message, state)  # type: ignore[arg-type]


async def _cw_reattr_show_confirm(target: Message, state: FSMContext) -> None:
    """Confirm режима D: сумма ФИКС, показываем перенос было→стало."""
    import html as _html
    await state.set_state(CreditWalletEditSG.confirm)
    d = await state.get_data()
    wlabel = credit_wallet_label(d.get("wallet_role") or "")
    amount = float(d.get("new_amount") or d.get("old_amount") or 0)
    new_purpose = (d.get("new_purpose") or "").strip()
    old_bind = _cw_bind_line(d.get("old_mode"), d.get("old_invoice_number") or "", d.get("old_cost_type"))
    new_bind = _cw_bind_line(d.get("new_mode"), d.get("new_invoice_number") or "", d.get("new_cost_type"))
    moved = "без изменений" if old_bind == new_bind else f"{old_bind} → {new_bind}"
    b = InlineKeyboardBuilder()
    b.button(text="✅ Перенести", callback_data="cwedit:save")
    b.button(text="❌ Отмена", callback_data="cwedit:abort")
    b.adjust(1)
    await target.answer(
        f"🔁 <b>Подтвердите перенос назначения — {wlabel}</b>\n\n"
        f"  Сумма: <b>{amount:,.0f}</b> ₽ (фиксирована)\n"
        f"  Назначение: {_html.escape(new_purpose)}\n"
        f"  🔁 {moved}",
        reply_markup=b.as_markup(),
    )


async def _cw_resync_after_reattribute(integrations, db: Database, snap: dict) -> None:
    """Ресинк листов после переноса привязки (режим D): старый+новый счёт +
    «Баланс компании» (если затронут free) + активный кредит-счёт + журнал."""
    old = snap.get("old") or {}
    new = snap.get("new") or {}
    for parent in (old.get("parent_invoice_id"), new.get("parent_invoice_id")):
        if parent:
            try:
                await integrations.sync_invoice_row(int(parent))
            except Exception:
                log.warning("_cw_resync_after_reattribute: invoice row %s failed", parent, exc_info=True)
    if (old.get("mode") == "free" or new.get("mode") == "free") and getattr(integrations, "sheets", None):
        try:
            await integrations.sheets.sync_balance_company_sheet(db)
        except Exception:
            log.warning("_cw_resync_after_reattribute: balance sheet failed", exc_info=True)
    try:
        if snap.get("active_credit_invoice_id"):
            await integrations.sync_invoice_row(int(snap["active_credit_invoice_id"]))
    except Exception:
        log.warning("_cw_resync_after_reattribute: active row failed", exc_info=True)
    if getattr(integrations, "sheets", None):
        try:
            await integrations.sheets.sync_advances_journal_sheet(db)
        except Exception:
            log.warning("_cw_resync_after_reattribute: advances journal failed", exc_info=True)


async def _cw_reattribute_save(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub, d: dict,
) -> None:
    """Confirm режима D: перенос привязки исполненной траты. Сумма ФИКС, total_da
    инвариант. Вызывается из cw_edit_save под гардом _CW_EDIT_INFLIGHT."""
    import html as _html
    u = cb.from_user
    spend_id = int(d.get("edit_spend_id") or 0)
    spend = await db.get_credit_spend(spend_id)
    if not spend:
        await cb.answer("Расход не найден (отменён?)", show_alert=True)
        await state.clear()
        return
    if int(spend.get("entered_by") or 0) != int(u.id):
        await cb.answer("Переносить может только инициатор расхода", show_alert=True)
        await state.clear()
        return
    new_mode = d.get("new_mode") or "free"
    new_purpose = (d.get("new_purpose") or "").strip()
    bound = new_mode == "bound"
    new_inv_id = d.get("new_invoice_id") if bound else None
    new_ct = d.get("new_cost_type") if bound else None
    new_inv_num = (d.get("new_invoice_number") or "") if bound else ""
    wallet_role = d.get("wallet_role") or spend.get("wallet_role") or ""
    amount = float(spend.get("amount") or 0)
    old_mode = d.get("old_mode") or ("bound" if spend.get("bound_invoice_id") else "free")
    old_inv_num = d.get("old_invoice_number") or ""
    old_ct = d.get("old_cost_type")
    tid = int(d.get("edit_task_id") or 0)

    try:
        snap = await db.reattribute_credit_spend(
            spend_id, u.id, new_mode=new_mode,
            new_invoice_id=int(new_inv_id) if new_inv_id else None,
            new_cost_type=new_ct, new_invoice_number=new_inv_num, new_purpose=new_purpose,
        )
    except KeyError:
        await cb.answer("Расход не найден", show_alert=True)
        await state.clear()
        return
    except Exception:
        log.warning("_cw_reattribute_save: reattribute failed spend=%s", spend_id, exc_info=True)
        try:
            await db.audit(
                actor_id=u.id, action="credit_spend_reattr_failed",
                entity="credit_spends", entity_id=str(spend_id),
                payload={"new_mode": new_mode, "new_invoice_id": new_inv_id},
            )
        except Exception:
            pass
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Не удалось перенести назначение. Баланс не изменён — обратитесь к ГД."
        )
        await state.clear()
        return

    await _cw_resync_after_reattribute(integrations, db, snap)

    wlabel = credit_wallet_label(wallet_role)
    old_bind = _cw_bind_line(old_mode, old_inv_num, old_ct)
    new_bind = _cw_bind_line(new_mode, new_inv_num, new_ct)
    msk = _msk_now_str()
    moved = "без изменений" if old_bind == new_bind else f"{old_bind} → {new_bind}"
    card = format_card_section(
        "🔁", f"Назначение перенесено — {wlabel}",
        [
            ("Привязка", new_bind),
            ("Назначение", _html.escape(new_purpose)),
            ("№ расхода", f"#{spend_id}"),
            ("Было→стало", moved),
            ("Время (МСК)", msk),
        ],
        total=fmt_money(amount),
        footer=("Сумма", "не изменилась — перенос назначения"),
        width=38, compact=True,
    )
    try:
        await cb.message.edit_text(card, reply_markup=_cw_reattr_actions_kb(tid))  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(card, reply_markup=_cw_reattr_actions_kb(tid))  # type: ignore[union-attr]

    owner_id = 0
    try:
        _t = await db.get_task(tid)
        owner_id = int(_t.get("assigned_to") or 0)
    except Exception:
        owner_id = 0
    info = format_card_section(
        "🔁", f"Назначение кредит-расхода перенесено — {wlabel}",
        [
            ("Перенёс", await get_initiator_label(db, u.id)),
            ("Было→стало", moved),
            ("Назначение", _html.escape(new_purpose)),
            ("Время (МСК)", msk),
        ],
        total=fmt_money(amount),
        footer=("Сумма", "не изменилась"),
        width=38, compact=True,
    )
    await _cw_notify_request_change(db, config, notifier, u.id, owner_id, info)
    await cb.answer("Перенесено")
    await state.clear()


# --- Просмотр баланса кошелька (РП: выбор кошелька → карточка) ---
@router.message(F.text == RP_BTN_CREDIT_BAL)
async def cw_balance_rp(message: Message, state: FSMContext, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.RP, Role.TD]):
        return
    b = InlineKeyboardBuilder()
    for r in CREDIT_WALLET_ROLES:
        b.button(text=f"🏦 {credit_wallet_label(r)}", callback_data=f"cwbal:{r}")
    b.adjust(1)
    await message.answer(
        "🏦 <b>Кредитный баланс</b>\n\nЧей кошелёк показать?", reply_markup=b.as_markup(),
    )


@router.callback_query(F.data.startswith("cwbal:"))
async def cw_balance_show(cb: CallbackQuery, db: Database) -> None:
    role = cb.data.split(":")[-1]  # type: ignore[union-attr]
    if role not in CREDIT_WALLET_ROLES:
        await cb.answer("Некорректный кошелёк", show_alert=True)
        return
    await cb.answer()
    try:
        card = await build_credit_wallet_card(db, role)
    except Exception:
        card = "⚠️ Не удалось построить карточку баланса."
    await cb.message.answer(card)  # type: ignore[union-attr]


# =====================================================================
# МОНТАЖНАЯ ГР. / ЧАТ С РП — chat-proxy with invoice binding
# =====================================================================

async def _chat_proxy_invoice_pick(
    message: Message, state: FSMContext, db: Database,
    channel: str, title: str, emoji: str,
) -> None:
    """Show invoice picker before entering chat-proxy."""
    uid = message.from_user.id  # type: ignore[union-attr]
    invoices = await db.list_invoices(created_by=uid, limit=50)
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


@router.message(F.text == MGR_BTN_MONTAZH, RoleFilter(ALL_MANAGER_ROLES))
async def mgr_montazh_chat(message: Message, state: FSMContext, db: Database) -> None:
    await _chat_proxy_invoice_pick(message, state, db, "montazh", "Монтажная гр.", "🔧")


@router.message(F.text == MGR_BTN_CHAT_RP, RoleFilter(ALL_MANAGER_ROLES))
async def mgr_rp_chat(message: Message, state: FSMContext, db: Database) -> None:
    # RoleFilter в декораторе → для ГД aiogram ПРОПУСКАЕТ хендлер (а не шлёт
    # «Нет доступа»), и «Чат с РП» доходит до gd_chat_rp (gd.py). Текст кнопки
    # общий у менеджера и ГД, поэтому фильтр роли обязателен здесь.
    await _chat_proxy_invoice_pick(message, state, db, "rp", "Чат с РП", "📋")


@router.callback_query(F.data.startswith("mgrchat:"))
async def mgr_chat_invoice_picked(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not await require_role_callback(cb, db, roles=ALL_MANAGER_ROLES):
        return
    await cb.answer()
    parts = (cb.data or "").split(":")
    if len(parts) < 3:
        return
    channel = parts[1]
    invoice_id = parts[2]

    if invoice_id == "cancel":
        await state.clear()
        try:
            await cb.message.delete()  # type: ignore[union-attr]
        except Exception:
            pass
        return

    await state.set_state(ManagerChatProxySG.menu)
    inv_ref = ""
    if invoice_id != "0":
        try:
            await state.update_data(channel=channel, invoice_id=int(invoice_id))
            inv_ref = f" (счёт #{invoice_id})"
        except (ValueError, TypeError):
            await state.update_data(channel=channel, invoice_id=None)
    else:
        await state.update_data(channel=channel, invoice_id=None)

    title = "Монтажная гр." if channel == "montazh" else "Чат с РП"
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            f"💬 <b>{title}</b>{inv_ref}\n\n"
            "Выберите действие:",
        )
    except Exception:
        pass
    await cb.message.answer(  # type: ignore[union-attr]
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


async def deliver_manager_chat_message(
    *,
    sender_id: int,
    sender_name: str,
    sender_username: str | None,
    channel: str,
    text: str,
    db: Database,
    config: Config,
    notifier: Notifier,
    invoice_id: int | None = None,
    tg_message_id: int | None = None,
    has_attachment: bool = False,
) -> int | None:
    """Сохранить сообщение менеджера/РП в канал и переслать адресату.

    Ядро вынесено из `mgr_chat_writing` (owner 26.07), чтобы тем же путём уходил
    текст, набранный сразу в меню канала, — без повторного ввода. Зеркалит приём
    25.07, когда для ГД из `handle_writing` вынесли `chat_proxy._deliver_chat_message`.
    Обслуживает и РП: его «✉️ Сообщение» переводит в то же состояние
    `ManagerChatProxySG.writing`, а хендлер этого состояния живёт только здесь.

    Возвращает telegram_id адресата или None, если адресат канала не настроен
    (сообщение при этом всё равно сохранено в переписке канала).
    """
    await db.save_chat_message(
        channel=channel,
        sender_id=sender_id,
        direction="outgoing",
        text=text or "[файл/фото]",
        tg_message_id=tg_message_id,
        has_attachment=has_attachment,
        invoice_id=invoice_id if invoice_id else None,
    )

    # Determine target by channel
    if channel == "montazh":
        from .chat_proxy import resolve_channel_target

        target_id = await resolve_channel_target(channel, db, config)
    else:
        target_role = _CHAT_TARGET_MAP.get(channel, Role.GD)
        target_id = await resolve_default_assignee(db, config, target_role)

    if not target_id:
        return None

    channel_label = _CHAT_CHANNEL_LABEL.get(channel, channel)
    role = await _current_role(db, sender_id)
    sender_label = {
        "manager_kv": "Менеджер КВ",
        "manager_kia": "Менеджер КИА",
        "manager_npn": "Менеджер НПН",
        "rp": "РП",
    }.get(role or "", sender_name or "Сотрудник")

    fwd_text = (
        f"💬 <b>{channel_label}</b>\n\n"
        f"От: {sender_label} (@{sender_username or '-'})\n\n"
        f"{text}"
    )
    await notifier.safe_send(int(target_id), fwd_text)
    return int(target_id)


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

    target_id = await deliver_manager_chat_message(
        sender_id=message.from_user.id,
        sender_name=message.from_user.full_name or "",
        sender_username=message.from_user.username,
        channel=channel,
        text=text,
        db=db,
        config=config,
        notifier=notifier,
        invoice_id=data.get("invoice_id"),
        tg_message_id=message.message_id,
        has_attachment=bool(message.document or message.photo),
    )

    if target_id:
        if message.document:
            await notifier.safe_send_media(target_id, "document", message.document.file_id, caption=message.caption)
        elif message.photo:
            await notifier.safe_send_media(target_id, "photo", message.photo[-1].file_id, caption=message.caption)
        elif message.video:
            await notifier.safe_send_media(target_id, "video", message.video.file_id, caption=message.caption)

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
            if _menu_role == Role.RP:
                # Reply-кнопка «💰 Запрос ЗП» имеет ОДИНАКОВЫЙ текст у менеджера
                # (MGR_BTN_ZP) и у РП-хаба (RP_BTN_SALARY_HUB), а manager_new.router
                # зарегистрирован раньше rp.router → без этой ветки нажатие РП
                # перехватывается менеджерским флоу (показывает AJ вместо РП-10%).
                # Делегируем в РП-хаб по активной роли, как сделано для INSTALLER.
                from .rp import rp_salary_hub_start
                return await rp_salary_hub_start(message, state, db)
    if not await require_role_message(message, db, roles=_MGR_ROLES):
        return
    await _manager_zp_show(message, state, db, message.from_user.id)  # type: ignore[union-attr]


async def _manager_zp_show(
    target: Message, state: FSMContext, db: Database, user_id: int,
) -> None:
    """Эталон-карточка «Запрос ЗП» + список счетов «End» для запроса.

    Финансы-рефактор 02.06: вызывается из reply-кнопки MGR_BTN_ZP и из inline
    «Запрос ЗП» под карточкой «Финансы». target — Message для ответа
    (message либо cb.message); user_id — реальный автор (cb.from_user.id).
    Карточка добавлена по запросу user 02.06: построчно №счёта → расчётная доля
    ЗП + Итого к запросу.
    """
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
        await target.answer("✅ Нет счетов, по которым можно запросить ЗП.\n"
                            "(Счёт должен иметь статус «Счёт End»)")
        return

    # plan/fact по каждому: флаг перерасхода + сбор расчётной ЗП для карточки
    eligible: list[dict] = []
    flagged_ids: set[int] = set()
    blocked_ids: set[int] = set()  # ч.3.3 (Q4): счета с открытыми задачами Счёт-END
    card_rows: list[tuple[str, str]] = []
    total_zp = 0.0
    for inv in invoices:
        pf = await db.get_plan_fact_card(inv["id"])
        eligible.append(inv)
        if pf["has_estimated"] and not pf["zp_allowed"]:
            flagged_ids.add(inv["id"])
        if await db.manager_zp_block_reason(inv["id"], inv):
            blocked_ids.add(inv["id"])
        # «Ваша доля» к запросу = бланк (AJ) с учётом удержания CN — единый
        # источник manager_zp_net_payout, как в пикере mgrzp:pick (owner 27.06):
        # список и «Итого к запросу» совпадают с реально запрашиваемой суммой.
        zp = manager_zp_net_payout(inv)
        # заблокированные не идут в «Итого к запросу» (запросить нельзя)
        if inv["id"] not in blocked_ids:
            total_zp += zp
        mark = "🔒 " if inv["id"] in blocked_ids else (
            "⚠️ " if inv["id"] in flagged_ids else ""
        )
        num = inv.get("invoice_number") or f"id={inv['id']}"
        card_rows.append((f"{mark}№{num}", fmt_money(zp) if zp > 0 else "—"))

    b = InlineKeyboardBuilder()
    for inv in eligible:
        if inv["id"] in blocked_ids:
            continue  # заблокированный счёт нельзя выбрать для ЗП (ч.3.3)
        prefix = "⚠️ " if inv["id"] in flagged_ids else ""
        label = f"{prefix}№{inv['invoice_number'] or '—'} / {(inv.get('object_address') or '—')[:30]}"
        b.button(text=label, callback_data=f"mgrzp:pick:{inv['id']}")
    b.button(text="⬅️ Назад", callback_data="nav:home")
    b.adjust(1)

    card = format_card_section(
        emoji="💰",
        title="Запрос ЗП",
        items=card_rows,
        footer=("Итого к запросу", fmt_money(total_zp)),
        width=29,
    )
    tail = "\n\nВыберите счёт (статус «Счёт End»):"
    if flagged_ids:
        tail += (
            f"\n⚠️ <b>Перерасчет прибыли ({len(flagged_ids)}):</b> "
            "факт. себестоимость превышает расчётную — ГД будет уведомлён."
        )
    if blocked_ids:
        tail += (
            f"\n🔒 <b>Заблокировано ({len(blocked_ids)}):</b> по счёту есть "
            "непогашенный долг или незакрытые задачи «Счёт-END» — ЗП недоступна."
        )

    await state.set_state(ManagerZpSG.select_invoice)
    await target.answer(card + tail, reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("fpeta:"))
async def fpeta_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """ТЗ 14.06: менеджер нажал «Указать дату» → запрос ввода даты ДД.ММ.ГГГГ."""
    await cb.answer()
    try:
        invoice_id = int((cb.data or "").split(":")[1])
    except (ValueError, IndexError):
        return
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        return
    if float(inv.get("outstanding_debt") or 0) <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ По счёту №{inv['invoice_number']} долг уже погашен — дата не нужна."
        )
        return
    await state.set_state(FinalPaymentEtaSG.date_input)
    await state.update_data(fpeta_invoice_id=invoice_id)
    await cb.message.answer(  # type: ignore[union-attr]
        f"📅 Счёт №{inv['invoice_number']}: введите <b>ориентировочную дату</b> "
        "финального платежа в формате <b>ДД.ММ.ГГГГ</b> (например 25.06.2026):"
    )


@router.message(FinalPaymentEtaSG.date_input)
async def fpeta_date_input(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """ТЗ 14.06: менеджер ввёл дату → запись БД+лист, ГД уведомить 1× (первая установка)."""
    from datetime import datetime as _dt
    text = (message.text or "").strip()
    try:
        d = _dt.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer(
            "⚠️ Неверный формат. Введите дату как ДД.ММ.ГГГГ (например 25.06.2026):"
        )
        return
    data = await state.get_data()
    invoice_id = data.get("fpeta_invoice_id")
    if not invoice_id:
        await state.clear()
        return
    inv = await db.get_invoice(int(invoice_id))
    if not inv:
        await state.clear()
        await message.answer("❌ Счёт не найден.")
        return
    prev_state = inv.get("final_payment_track_state") or ""
    await db.set_final_payment_eta(int(invoice_id), d.isoformat())
    try:
        await integrations.sync_invoice_row(int(invoice_id))
    except Exception:
        log.exception("fpeta: sync_invoice_row failed inv=%s", invoice_id)
    await state.clear()

    # Закрыть открытую задачу FINAL_PAYMENT_ETA по этому счёту
    try:
        from ..enums import TaskType, TaskStatus
        _tasks = await db.search_tasks_by_payload(
            field="invoice_id", value=str(invoice_id),
            type_filter=[TaskType.FINAL_PAYMENT_ETA], limit=20,
        )
        for _t in _tasks:
            if _t.get("status") in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
                _p = try_json_loads(_t.get("payload_json")) or {}
                if int(_p.get("invoice_id") or 0) == int(invoice_id):
                    await db.update_task_status(int(_t["id"]), TaskStatus.DONE)
    except Exception:
        log.exception("fpeta: close task failed inv=%s", invoice_id)

    d_human = d.strftime("%d.%m.%Y")
    await message.answer(
        f"✅ Дата финального платежа по счёту №{inv['invoice_number']} принята: "
        f"<b>{d_human}</b>.\nГД уведомлён. Бот проконтролирует поступление."
    )

    # ГД уведомить 1× — только при ПЕРВОЙ установке даты (prev_state ''/none),
    # чтобы не дублировать при переназначении после просрочки (ТЗ 14.06).
    if prev_state in ("", "none"):
        try:
            gd_id = await resolve_default_assignee(db, config, Role.GD)
            if gd_id:
                debt = float(inv.get("outstanding_debt") or 0)
                debt_str = f"{int(round(debt)):,}".replace(",", " ")
                await notifier.safe_send(
                    int(gd_id),
                    f"📅 <b>Намечена дата фин. платежа</b>\n\n"
                    f"Счёт №{inv['invoice_number']} — {inv.get('object_address') or '—'}\n"
                    f"Долг: {debt_str} ₽\n"
                    f"Менеджер назначил платёж на <b>{d_human}</b>.\n"
                    "Зафиксировано в карточке «Долги».",
                )
        except Exception:
            log.exception("fpeta: GD notify failed inv=%s", invoice_id)


@router.callback_query(F.data.startswith("mgrzp:pick:"), ManagerZpSG.select_invoice)
async def manager_zp_pick(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv:
        await cb.message.answer("❌ Счёт не найден.")  # type: ignore[union-attr]
        await state.clear()
        return
    # ч.3.3 (Q4) + ТЗ 14.06: блок ЗП при долге ИЛИ открытых задачах-устранения
    _zp_block = await db.manager_zp_block_reason(invoice_id, inv)
    if _zp_block:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⛔ ЗП по счёту №{inv['invoice_number']} заблокирована: {_zp_block}."
        )
        await state.clear()
        return
    # Check plan/fact — flag overbudget for GD notification (no block)
    pf = await db.get_plan_fact_card(invoice_id)
    is_overbudget = pf["has_estimated"] and not pf["zp_allowed"]
    await state.update_data(
        is_overbudget=is_overbudget,
        overbudget_est=pf.get("estimated_total_cost", 0),
        overbudget_fact=pf.get("actual_total_cost", 0),
    )

    # Auto-calculate ZP from estimated profit split
    if pf["has_estimated"] and pf["manager_zp"] > 0:
        # «Ваша доля» = ЗП к выплате из бланка (AJ manager_zp_blank) с учётом
        # удержания CN (zp_manager_hold, хранится со знаком): при CN ≥ 0 → бланк,
        # при CN < 0 → бланк − удержание, floor 0 (AJ пусто/0 → 0). Единый источник
        # истины manager_zp_net_payout (owner 2026-06-27) — и отображение, и сумма
        # запроса (zp_amount уходит в mgrzp:confirm без пересчёта).
        auto_amount = manager_zp_net_payout(inv)
        src = pf.get("client_source", "own")
        src_label = "Лид ГД (75/25)" if src == "gd_lead" else "Мой клиент (50/50)"
        await state.update_data(zp_invoice_id=invoice_id, zp_amount=auto_amount)
        await state.set_state(ManagerZpSG.confirm)
        b = InlineKeyboardBuilder()
        b.button(text="✅ Отправить", callback_data="mgrzp:confirm")
        b.button(text="❌ Отмена", callback_data="mgrzp:cancel")
        b.adjust(2)
        import html as _html
        head_section = format_card_section(
            "🧾", f"Счёт: №{inv['invoice_number']}",
            [
                ("Адрес", _html.escape(str(inv.get("object_address") or "—"))),
                ("Источник", src_label),
            ],
            width=38, compact=True,
        )
        calc_section = format_card_section(
            "📊", "Расчёт ЗП",
            [
                ("Расч.прибыль", fmt_money(pf["estimated_profit"])),
                ("ЗП РП (10%)", fmt_money(pf["rp_zp"])),
            ],
            footer=("Ваша доля", fmt_money(auto_amount)),
            width=30,
        )
        await cb.message.answer(  # type: ignore[union-attr]
            "💰 <b>ЗП рассчитана автоматически</b>\n\n"
            + format_card([head_section, calc_section])
            + "\n\nОтправить запрос ГД?",
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
@money_confirm_guard
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

    # ч.3.3 (Q4) + ТЗ 14.06: блок ЗП при долге ИЛИ открытых задачах-устранения
    inv_b = await db.get_invoice(invoice_id)
    _zp_block = await db.manager_zp_block_reason(invoice_id, inv_b)
    if _zp_block:
        await state.clear()
        await cb.message.answer(  # type: ignore[union-attr]
            f"⛔ ЗП по счёту №{(inv_b or {}).get('invoice_number', invoice_id)} "
            f"заблокирована: {_zp_block}."
        )
        return

    inv = inv_b
    inv_number = inv["invoice_number"] if inv else "—"

    # Сначала резолвим ГД: без получателя запрос отправить некуда. НЕ меняем
    # статус и НЕ шлём ложное «✅ отправлено» — иначе счёт завис бы в requested
    # без задачи у ГД и пропал из списка «Запрос ЗП».
    # См. project_manager_zp_create_task_dueat_fix_20260629 (hardening).
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await state.clear()
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Не удалось отправить запрос ЗП по счёту №{inv_number}: "
            "ГД не настроен в системе. Обратитесь к администратору — "
            "счёт остаётся доступным для повторного запроса."
        )
        return

    # Задача ГД создаётся ДО смены статуса: если create_task упадёт, статус
    # останется not_requested и счёт не выпадет из списка (можно повторить).
    await db.create_task(
        project_id=None,
        type_=TaskType.ZP_MANAGER,
        status=TaskStatus.OPEN,
        created_by=u.id,
        assigned_to=int(gd_id),
        due_at_iso=None,
        payload={
            "invoice_id": invoice_id,
            "invoice_number": inv_number,
            "amount": amount,
            "source": "manager_zp",
        },
    )

    # Задача создана успешно — теперь фиксируем статус заявки.
    await db.set_invoice_zp_manager_status(invoice_id, "requested", amount=amount, requested_by=u.id)

    initiator = await get_initiator_label(db, u.id)

    # Блок «🔴 ПЕРЕРАСЧЕТ ПРИБЫЛИ» убран из карточки ГД (owner 27.06).
    # Флаг is_overbudget остаётся в payload, просто не рендерится.

    b = InlineKeyboardBuilder()
    b.button(text="✅ ЗП ОК", callback_data=f"gdzp_mgr:ok:{invoice_id}")
    b.button(text="❌ Отклонить", callback_data=f"gdzp_mgr:no:{invoice_id}")
    b.adjust(2)
    is_credit = bool(inv.get("is_credit")) if inv else False
    pay_label = "🏦 Кред" if is_credit else "💳 б/н"
    client = (inv.get("client_name") if inv else None) or "—"
    address = (inv.get("object_address") if inv else None) or "-"
    gd_card = (
        f"💰 <b>ЗП отд.продаж: №{inv_number}</b>\n"
        f"👤 От: {initiator}\n\n"
        f"📍 Адрес: {address}\n"
        f"💵 Сумма: {amount:,.0f}₽\n"
        f"💳 Тип: {pay_label}\n"
        f"🏢 Клиент: {client}"
    )
    await notifier.safe_send(int(gd_id), gd_card, reply_markup=b.as_markup())
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Запрос ЗП отправлен ГД.\n"
        f"Счёт: №{inv_number}, сумма: {amount:,.0f}₽",
    )


# =====================================================================
# MANAGER WITHDRAW (TZ synthetic-hopping-ocean 2026-05-25)
# Менеджер (КВ/КИА/НПН) снимает с депозита. WHITELIST в gd.py.
# БД-метод create_installer_withdraw параметрический — работает для любого employee tg_id.
# =====================================================================


async def _mgr_wallet_role(db: Database, user_id: int) -> str | None:
    """Дискриминатор кошелька для менеджерского меню (TZ 2026-05-29 wallet-sep).

    Нужен только для двуролевого РП+Менеджер (Павел): его менеджерские записи
    тегируются 'manager_npn', чтобы баланс/журнал не смешивались с кошельком РП.
    Для single-role менеджеров (КВ/КИА/…) → None: их записи остаются NULL,
    балансы считаются как раньше (без фильтра).
    """
    try:
        u = await db.get_user_optional(user_id)
        # get_user_optional → UserRow (объект, не dict): берём .role через getattr.
        # Баг-фикс 02.06: было (u or {}).get("role") → AttributeError → всегда None
        # → кошельки Павла (РП+Менеджер НПН) смешивались. См. feedback_rp_npn_separate_wallets.
        roles = [r.strip().lower() for r in str(getattr(u, "role", "") or "").split(",")]
    except Exception:
        roles = []
    return "manager_npn" if "rp" in roles else None


async def _mgr_role_key(db: Database, user_id: int) -> str | None:
    """Бренд-роль менеджера (manager_kv/kia/npn) из строки роли — для creator_role
    фильтра наполнения аванса из ЗП (TZ 06.06). None, если у пользователя нет
    менеджерского бренда (тогда кнопка наполнения не показывается)."""
    try:
        u = await db.get_user_optional(user_id)
        role_raw = (getattr(u, "role", "") or "")
    except Exception:
        return None
    return next((r for r in CREDIT_WALLET_ROLES if r in role_raw), None)


# =====================================================================
# «💰 ФИНАНСЫ» и «🏦 КРЕДИТ» — экраны-карточки + INLINE-действия.
# Финансы-рефактор 02.06 (по запросу user): денежные механизмы
# (аванс / депозит / расход депозита / запрос ЗП) сведены в одну кнопку
# «💰 Финансы» → сводная эталон-карточка с inline-действиями; кредит-механизм
# (баланс + расход + кредит-чат) — «🏦 Кредит». Действия идут INLINE под
# карточкой, поэтому reply-меню «Ещё»/главное НЕ подменяется submenu и всегда
# показывается (фикс «стирается меню»). Старый chat-proxy вход «Менеджер (кред)»
# доступен пунктом «💬 Кредит-чат» внутри «Кредит».
# Inline-обёртки берут реального автора из cb.from_user.id, дублируя только
# старт существующих FSM-потоков; ввод суммы/выбор счёта дальше обрабатывают
# уже существующие state-хендлеры (message.from_user.id корректен).
# =====================================================================


async def _mgr_cb_ok(cb: CallbackQuery, db: Database) -> bool:
    """Гард inline-действий Финансы/Кредит: автор — менеджер (вкл. двуролевого
    РП+Менеджер). cb.from_user.id — реальный пользователь, не бот."""
    if not cb.from_user:
        return False
    u = await db.get_user_optional(cb.from_user.id)
    return "manager" in (getattr(u, "role", "") or "")


def _matches_funds_menu_button(text: str | None) -> bool:
    """Совпадение с «💰 Финансы» c опц. бейджем «🔴N» (открытые задачи перерасчёта).

    Раньше хендлер стоял на exact-match F.text == MGR_BTN_FUNDS → кнопка с бейджем
    «💰 Финансы 🔴N» под него НЕ попадала ([[feedback_badge_button_handlers]]).
    Пробел после базы исключает коллизию с «💰 Запрос ЗП»/«💰 Наполнить ЗП из аванса».
    """
    t = (text or "").strip()
    return bool(t) and (t == MGR_BTN_FUNDS or t.startswith(f"{MGR_BTN_FUNDS} "))


@router.message(lambda m: _matches_funds_menu_button(m.text))
async def manager_funds_menu(message: Message, state: FSMContext, db: Database) -> None:
    """«💰 Финансы» — сводная эталон-карточка (аванс+депозит) + inline-действия."""
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await _funds_show(message, db, message.from_user.id)  # type: ignore[union-attr]


async def _funds_show(target: Message, db: Database, user_id: int) -> None:
    """Карточка «Финансы» + inline-действия (видимость зависит от whitelist/балансов)."""
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    wallet = await _mgr_wallet_role(db, user_id)
    try:
        card = await build_funds_card(db, user_id, wallet)
    except Exception:
        log.warning("build_funds_card failed user=%s", user_id, exc_info=True)
        card = "💰 <b>Финансы</b>"
    in_whitelist = user_id in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    depo = 0.0
    unalloc = 0.0
    try:
        depo = await db.get_deposit_balance(user_id, wallet)
    except Exception:
        log.debug("funds: get_deposit_balance failed", exc_info=True)
    try:
        unalloc = await db.get_advance_outstanding_unallocated(user_id, wallet)
    except Exception:
        log.debug("funds: get_advance_outstanding_unallocated failed", exc_info=True)
    # Наполнение аванса из ЗП ОТКЛЮЧЕНО 2026-06-30: авансовый/депозитный кошелёк
    # наполняет ТОЛЬКО ГД. Для менеджера кошелёк — только расход в счёт ЗП
    # («💰 Наполнить ЗП из аванса» = funds:advdist).
    b = InlineKeyboardBuilder()
    b.button(text="💰 Запрос ЗП", callback_data="funds:zp")
    if in_whitelist and depo > 0:
        b.button(text="💸 Расход депозита", callback_data="funds:withdraw")
    if in_whitelist and unalloc > 0:
        b.button(text="💰 Наполнить ЗП из аванса", callback_data="funds:advdist")
    if in_whitelist and depo > 0:
        b.button(text="↔️ Депо → Аванс", callback_data="funds:depo2adv")
    b.adjust(1)
    await target.answer(card, reply_markup=b.as_markup())

    # ТЗ 02.07: под карточкой «Финансы» — открытые задачи «Перерасчёт → согласие»
    # (ГД отправил счёт под перерасчётом). Менеджер может согласиться отсюда, если
    # пропустил пуш-карточку. Согласие → сумма |CN| в авансовый кошелёк.
    try:
        recalc_tasks = await db.list_recalc_confirm_tasks(user_id)
    except Exception:
        recalc_tasks = []
    for t in recalc_tasks:
        try:
            p = json.loads(t.get("payload_json") or "{}")
        except (ValueError, TypeError):
            continue
        rid = int(p.get("invoice_id") or 0)
        rinv = await db.get_invoice(rid) if rid else None
        if not rinv:
            continue
        rb = InlineKeyboardBuilder()
        rb.button(text="✅ С перерасчётом согласен", callback_data=f"recalc_agree:{rid}")
        rb.adjust(1)
        await target.answer(
            format_manager_recalc_card(rinv), reply_markup=rb.as_markup(),
        )


@router.callback_query(F.data.regexp(r"^recalc_agree:\d+$"))
@money_confirm_guard
async def recalc_agree(
    cb: CallbackQuery, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    """ТЗ 02.07: менеджер жмёт «✅ С перерасчётом согласен».

    Остаток перерасчёта (|CN| − zp_hold_advanced) зачисляется в авансовый кошелёк
    менеджера как выданный аванс (create_recalc_advance_topup — bot-owned, durable,
    пишет аванс и отметку на счёте одной транзакцией). Задача recalc_confirm
    закрывается атомарно (open→done, идемпотентно), ГД уведомляется.

    Фикс 30.07: раньше начислялся полный |CN| через create_gd_advance_topup, а
    отметка на счёте не ставилась — авто-свип на следующем синке ГД переносил ту
    же переплату второй раз. Теперь оба канала пишут общий трекер, и счёт сам
    уходит из list_invoices_under_recalc; ручное обнуление CF не обязательно."""
    if not cb.from_user:
        return
    await cb.answer()
    manager_id = cb.from_user.id
    u = await db.get_user_optional(manager_id)
    if "manager" not in (getattr(u, "role", "") or ""):
        await cb.message.answer("⛔ Доступно только менеджеру.")  # type: ignore[union-attr]
        return
    inv_id = int(cb.data.split(":")[1])  # type: ignore[union-attr]
    # Найти открытую задачу по счёту, назначенную этому менеджеру.
    task = None
    payload: dict[str, Any] = {}
    for t in await db.list_recalc_confirm_tasks(manager_id):
        try:
            p = json.loads(t.get("payload_json") or "{}")
        except (ValueError, TypeError):
            continue
        if int(p.get("invoice_id") or 0) == inv_id:
            task, payload = t, p
            break
    if not task:
        await cb.message.answer("✅ Эта задача уже обработана.")  # type: ignore[union-attr]
        return
    # Атомарный claim: только первый клик закрывает задачу (open→done).
    updated = await db.update_task_status(
        int(task["id"]), TaskStatus.DONE, expected_statuses=("open",),
    )
    if not updated:
        await cb.message.answer("✅ Эта задача уже обработана.")  # type: ignore[union-attr]
        return
    inv = await db.get_invoice(inv_id)
    num = (inv.get("invoice_number") if inv else None) or payload.get("invoice_number") or "—"
    # Начисляем ОСТАТОК |CN| − zp_hold_advanced, а не полный |CN| (фикс 30.07):
    # авто-свип на синке ГД мог уже перенести переплату, и полная сумма здесь
    # означала бы второй аванс на те же деньги.
    cn_abs = abs(float(
        (inv.get("zp_manager_hold") if inv else None) or payload.get("amount") or 0
    ))
    advanced = float((inv.get("zp_hold_advanced") if inv else None) or 0)
    amount = round(cn_abs - advanced, 2)
    gd_id = int(payload.get("gd_id") or 0)
    if not gd_id:
        gd_id = int(await resolve_default_assignee(db, config, Role.GD) or 0)
    if amount <= 0:
        # Всё уже перенесено свипом. Задачу оставляем закрытой (claim выше) и
        # НЕ начисляем второй раз — деньги менеджер уже получил в кошелёк.
        adv_s = f"{advanced:,.0f}".replace(",", " ")
        await cb.message.answer(  # type: ignore[union-attr]
            f"✅ Перерасчёт по счёту №{num} уже учтён.\n"
            f"Сумма <b>{adv_s} ₽</b> ранее зачислена в ваш авансовый кошелёк — "
            f"повторно не начисляем."
        )
        return
    # Зачисление ДОЛЖНО пройти успешно, иначе задачу возвращаем в open (claim
    # откатывается) — чтобы менеджер мог повторить, а не потерял задачу без денег.
    # Claim-first защищает от двойного клика; revert-on-failure — от «done без аванса».
    # [[feedback_money_confirm_idempotent_gate]]
    try:
        wallet_role = await _mgr_wallet_role(db, manager_id)
        # create_recalc_advance_topup (а не create_gd_advance_topup): пишет аванс
        # и отметку zp_hold_advanced ОДНОЙ транзакцией — общий трекер со свипом,
        # иначе следующий синк ГД перенёс бы ту же переплату второй раз.
        req_id = await db.create_recalc_advance_topup(
            invoice_id=inv_id,
            invoice_number=str(num),
            employee_id=manager_id,
            amount=amount,
            gd_id=gd_id or manager_id,
            wallet_role=wallet_role,
        )
    except Exception as e:
        await db.update_task_status(
            int(task["id"]), TaskStatus.OPEN, expected_statuses=("done",),
        )
        log.warning(
            "recalc_agree: credit failed inv=%s task=%s: %s", inv_id, task["id"], e,
        )
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Не удалось зачислить аванс: {e}. Задача возвращена — попробуйте ещё раз."
        )
        return
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("recalc_agree: sync_advances_journal failed: %s", e)
    new_adv = await db.get_advance_balance(manager_id, wallet_role)
    amt_s = f"{amount:,.0f}".replace(",", " ")
    bal_s = f"{new_adv:,.0f}".replace(",", " ")
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Перерасчёт по счёту №{num} принят.\n"
        f"Зачислено в авансовый кошелёк: <b>{amt_s} ₽</b> (аванс #{req_id}).\n"
        f"💰 Баланс аванса: <b>{bal_s} ₽</b>"
    )
    if gd_id:
        mname = "Менеджер"
        try:
            mu = await db.get_user_optional(manager_id)
            if mu:
                mname = getattr(mu, "full_name", None) or getattr(mu, "username", None) or mname
        except Exception:
            pass
        await notifier.safe_send(
            gd_id,
            f"✅ <b>{mname}</b> согласился с перерасчётом по счёту №{num}.\n"
            f"Сумма <b>{amt_s} ₽</b> зачислена ему в авансовый кошелёк.\n"
            f"<i>Счёт ушёл из списка перерасчёта, повторного начисления не будет. "
            f"CN в «Импорт ОП» (CF) — по желанию, для чистоты столбца BZ.</i>"
        )
    try:
        await refresh_recipient_keyboard(notifier, db, config, manager_id)
    except Exception:
        log.warning("recalc_agree: refresh keyboard failed for %s", manager_id)


@router.callback_query(F.data == "funds:zp")
async def funds_zp(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    await _manager_zp_show(cb.message, state, db, cb.from_user.id)  # type: ignore[arg-type,union-attr]


@router.callback_query(F.data == "funds:withdraw")
async def funds_withdraw(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}:
        await cb.message.answer(  # type: ignore[union-attr]
            "⛔ Функция недоступна.\nЗапросите ГД добавить вас в whitelist депозитов.")
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    balance = await db.get_deposit_balance(user_id, wallet_role)
    if balance <= 0:
        await cb.message.answer("❌ На депозите нет средств для расхода.")  # type: ignore[union-attr]
        return
    await state.set_state(ManagerWithdrawSG.enter_amount)
    await state.update_data(mgr_withdraw_balance=balance, mgr_withdraw_wallet_role=wallet_role)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💸 <b>Расход с депозита</b>\n"
        f"Доступно: <b>{fmt_money(balance)}</b>\n\n"
        f"Введите сумму расхода (₽, ≤ {fmt_money(balance)}):",
    )


@router.callback_query(F.data == "funds:advdist")
async def funds_advdist(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}:
        await cb.message.answer("⛔ Функция недоступна.")  # type: ignore[union-attr]
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    unallocated = await db.get_advance_outstanding_unallocated(user_id, wallet_role)
    if unallocated <= 0:
        await cb.message.answer("✅ Нечего распределять — свободного advance нет.")  # type: ignore[union-attr]
        return
    invoices = await db.list_invoices_for_manager(user_id)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "❌ Нет активных счетов без выплаченной ЗП.\n"
            "<i>Аванс можно распределять только на счета где Invoices.AN (ЗП-менеджер) = 0.</i>",
        )
        return
    await state.set_state(ManagerAdvDistributeSG.select_invoice)
    await state.update_data(mgr_adv_dist_unallocated=unallocated)
    b = InlineKeyboardBuilder()
    for inv in invoices[:15]:
        num = inv.get("invoice_number") or f"id={inv['id']}"
        addr = (inv.get("object_address") or "—")[:25]
        # net = ЗП бланк − удержание (CN) при погашенном долге (механизм перерасчёта,
        # owner 23.06); cap распределения аванса по net-ЗП к выплате.
        plan = manager_zp_net_payout(inv)
        label = f"№{num} {addr}"
        if plan > 0:
            label += f" • план {fmt_money(plan)}"
        b.button(text=label[:60], callback_data=f"mgr_adv_dist:inv:{inv['id']}")
    b.button(text="❌ Отмена", callback_data="mgr_adv_dist:cancel")
    b.adjust(1)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💰 <b>Наполнить ЗП из аванса</b>\n"
        f"🔓 Свободно: <b>{fmt_money(unallocated)}</b>\n\n"
        f"Выберите счёт, под который зачислить часть аванса:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "funds:depo2adv")
async def funds_depo2adv(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    user_id = cb.from_user.id  # type: ignore[union-attr]
    if user_id not in {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}:
        await cb.message.answer("⛔ Функция недоступна.")  # type: ignore[union-attr]
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    depo = await db.get_deposit_balance(user_id, wallet_role)
    if depo <= 0:
        await cb.message.answer("❌ На депозите нет средств.")  # type: ignore[union-attr]
        return
    await state.set_state(ManagerDepoToAdvSG.enter_amount)
    await state.update_data(mgr_depo_to_adv_balance=depo, mgr_depo_to_adv_wallet_role=wallet_role)
    await cb.message.answer(  # type: ignore[union-attr]
        f"↔️ <b>Перевод Депозит → Аванс</b>\n"
        f"Депозит: <b>{fmt_money(depo)}</b>\n\n"
        f"Введите сумму перевода (₽, ≤ {fmt_money(depo)}):\n"
        f"<i>Переведённые деньги попадут в advance — сможете распределить по счетам.</i>",
    )


# =====================================================================
# Наполнение аванса менеджера незабранной ЗП по счетам (TZ 06.06).
# Зеркало installer-флоу (inst_advfill), импорт-безопасный вариант: метит
# zp_manager_status='confirmed' (не AN/zp_manager_payout, которое реимпорт ОП
# затирает), Σ → ОДИН topup кошелька менеджера (wallet_role НПН='manager_npn',
# КВ/КИА=NULL). Своя ЗП — без одобрения ГД, инфо-уведомление ГД.
# =====================================================================


def _mgr_advfill_addr(addr: Any, width: int = 22) -> str:
    """Короткий адрес объекта для строки/кнопки (обрезка с …)."""
    s = (str(addr).strip() if addr else "") or "—"
    return s if len(s) <= width else s[: width - 1] + "…"


async def _mgr_advfill_edit_or_send(cb: CallbackQuery, text: str, markup: Any = None) -> None:
    """edit_text текущего сообщения; при ошибке (не изменено / нет прав) — новое."""
    try:
        await cb.message.edit_text(text, reply_markup=markup)  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer(text, reply_markup=markup)  # type: ignore[union-attr]


def _render_mgr_adv_fill(
    invoices: list[dict[str, Any]], selected: set[int],
) -> tuple[str, Any]:
    """Экран наполнения аванса менеджера: счета с незабранной ЗП (toggle-чекбоксы)."""
    sel_total = sum(float(i.get("amount") or 0) for i in invoices if int(i["id"]) in selected)
    avail_total = sum(float(i.get("amount") or 0) for i in invoices)
    lines = [f"<pre>💵 <b>Наполнить аванс</b>"]
    lines.append(f"   Доступно ЗП          {fmt_money(avail_total):>11s}")
    lines.append(f"   Выбрано счетов       {len(selected):>11d} / {len(invoices)}")
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {fmt_money(sel_total)}</pre>")
    if invoices:
        lines.append("\nВыберите счета, по которым забрать ЗП менеджера в кошелёк аванса.")
        lines.append("ЗП по выбранным счетам пометится забранной (целиком по счёту).")
    b = InlineKeyboardBuilder()
    for inv in invoices:
        inv_id = int(inv["id"])
        amount = float(inv.get("amount") or 0)
        mark = "✅" if inv_id in selected else "▫️"
        b.button(
            text=f"{mark} {_mgr_advfill_addr(inv.get('object_address'))} — {fmt_money(amount)}",
            callback_data=f"mgr_advfill:pick:{inv_id}",
        )
    if invoices:
        if len(selected) < len(invoices):
            b.button(text="☑️ Выбрать все", callback_data="mgr_advfill:all")
        else:
            b.button(text="⬜ Снять все", callback_data="mgr_advfill:none")
        if selected:
            b.button(text=f"💵 Зачислить в аванс ({fmt_money(sel_total)})",
                     callback_data="mgr_advfill:credit")
    b.button(text="❌ Отмена", callback_data="mgr_advfill:cancel")
    b.adjust(1)
    return "\n".join(lines), b.as_markup()


@router.callback_query(F.data == "mgr_advfill:start")
async def mgr_advfill_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """«💵 Наполнить аванс из ЗП» → ОТКЛЮЧЕНО 2026-06-30 (кошелёк наполняет только ГД).

    Гард от устаревшей inline-кнопки: наполнение кошелька сотрудником запрещено,
    кошелёк используется только в счёт ЗП.
    """
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "⛔ Наполнение аванса доступно только ГД.\n"
        "Авансовый кошелёк используется только в счёт ЗП.")
    return
    if not await _mgr_cb_ok(cb, db):
        return
    user_id = cb.from_user.id  # type: ignore[union-attr]
    role_key = await _mgr_role_key(db, user_id)
    if not role_key:
        await cb.answer("Доступно только менеджерам КВ/КИА/НПН.", show_alert=True)
        return
    await state.clear()
    invoices = await db.list_manager_advance_fill_invoices(role_key, user_id)
    if not invoices:
        await cb.message.answer(  # type: ignore[union-attr]
            "📭 Нет счетов с незабранной ЗП менеджера.\n\n"
            "Счёт появляется, когда по нему есть ЗП менеджера (по бланку), ещё не "
            "забранная и не в выплате."
        )
        return
    selected = {int(i["id"]) for i in invoices}  # по умолчанию выбраны все
    await state.set_state(ManagerAdvanceFillSG.list_invoices)
    await state.update_data(
        mgr_advfill_invoices=invoices, mgr_advfill_selected=list(selected),
        mgr_advfill_role_key=role_key,
    )
    text, kb = _render_mgr_adv_fill(invoices, selected)
    await cb.message.answer(text, reply_markup=kb)  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("mgr_advfill:pick:"), ManagerAdvanceFillSG.list_invoices)
async def mgr_advfill_pick(cb: CallbackQuery, state: FSMContext) -> None:
    """Toggle одного счёта в выборке наполнения."""
    await cb.answer()
    inv_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    data = await state.get_data()
    invoices = data.get("mgr_advfill_invoices") or []
    selected = set(data.get("mgr_advfill_selected") or [])
    if inv_id in selected:
        selected.discard(inv_id)
    else:
        selected.add(inv_id)
    await state.update_data(mgr_advfill_selected=list(selected))
    text, kb = _render_mgr_adv_fill(invoices, selected)
    await _mgr_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "mgr_advfill:all", ManagerAdvanceFillSG.list_invoices)
async def mgr_advfill_all(cb: CallbackQuery, state: FSMContext) -> None:
    """Выбрать все счета."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("mgr_advfill_invoices") or []
    selected = {int(i["id"]) for i in invoices}
    await state.update_data(mgr_advfill_selected=list(selected))
    text, kb = _render_mgr_adv_fill(invoices, selected)
    await _mgr_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "mgr_advfill:none", ManagerAdvanceFillSG.list_invoices)
async def mgr_advfill_none(cb: CallbackQuery, state: FSMContext) -> None:
    """Снять выбор со всех счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("mgr_advfill_invoices") or []
    await state.update_data(mgr_advfill_selected=[])
    text, kb = _render_mgr_adv_fill(invoices, set())
    await _mgr_advfill_edit_or_send(cb, text, kb)


@router.callback_query(F.data == "mgr_advfill:cancel")
async def mgr_advfill_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Отмена наполнения на любом шаге."""
    await cb.answer()
    await state.clear()
    await _mgr_advfill_edit_or_send(cb, "❌ Наполнение аванса отменено.")


@router.callback_query(F.data == "mgr_advfill:credit", ManagerAdvanceFillSG.list_invoices)
async def mgr_advfill_credit_confirm(cb: CallbackQuery, state: FSMContext) -> None:
    """«💵 Зачислить» → подтверждение со списком выбранных счетов."""
    await cb.answer()
    data = await state.get_data()
    invoices = data.get("mgr_advfill_invoices") or []
    selected = set(data.get("mgr_advfill_selected") or [])
    if not selected:
        await cb.answer("Выберите хотя бы один счёт.", show_alert=True)
        return
    chosen = [i for i in invoices if int(i["id"]) in selected]
    total = sum(float(i.get("amount") or 0) for i in chosen)
    lines = [f"<pre>💵 <b>Зачислить в аванс</b>"]
    for inv in chosen:
        lines.append(
            f"   {_mgr_advfill_addr(inv.get('object_address'), 18):<18s} "
            f"{fmt_money(float(inv.get('amount') or 0)):>11s}"
        )
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {fmt_money(total)}")
    lines.append("</pre>")
    lines.append(f"\nЗачислить ЗП менеджера по {len(chosen)} счёт(ам) в кошелёк аванса?")
    lines.append("ЗП по этим счетам пометится забранной (целиком по счёту).")
    b = InlineKeyboardBuilder()
    b.button(text="✅ Да", callback_data="mgr_advfill:confirm:yes")
    b.button(text="❌ Нет", callback_data="mgr_advfill:confirm:no")
    b.adjust(2)
    await state.set_state(ManagerAdvanceFillSG.confirm)
    await cb.message.answer("\n".join(lines), reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(F.data.startswith("mgr_advfill:confirm:"), ManagerAdvanceFillSG.confirm)
@money_confirm_guard
async def mgr_advfill_credit_apply(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
    config: Config,
) -> None:
    """Финал: credit_manager_zp_to_advance (атомарно) + sync + инфо ГД.

    ОТКЛЮЧЕНО 2026-06-30: наполнение кошелька — только ГД. Гард от устаревшего
    FSM-состояния/кнопки «✅ Да» (денежная запись не выполняется).
    """
    await state.clear()
    await cb.answer("⛔ Наполнение аванса отключено (только ГД).", show_alert=True)
    return
    answer = cb.data.split(":")[-1] if cb.data else "no"  # type: ignore[union-attr]
    if answer != "yes":
        await state.clear()
        await _mgr_advfill_edit_or_send(cb, "❌ Отменено.")
        await cb.answer()
        return
    data = await state.get_data()
    selected = list({int(x) for x in (data.get("mgr_advfill_selected") or [])})
    role_key = data.get("mgr_advfill_role_key") or ""
    if not selected or not role_key:
        await cb.answer("Список пуст", show_alert=True)
        await state.clear()
        return
    user_id = cb.from_user.id  # type: ignore[union-attr]
    wallet_role = await _mgr_wallet_role(db, user_id)
    try:
        req_id, total, credited = await db.credit_manager_zp_to_advance(
            user_id, role_key, wallet_role, selected,
        )
    except Exception:
        log.exception("mgr_advfill_credit_apply: credit failed user_id=%s", user_id)
        await state.clear()
        await cb.answer("Ошибка зачисления, попробуйте позже.", show_alert=True)
        await cb.message.answer("❌ Не удалось зачислить в аванс. Попробуйте позже.")  # type: ignore[union-attr]
        return
    for c in credited:
        try:
            await integrations.sync_invoice_row(int(c["invoice_id"]))
        except Exception:
            log.exception("mgr_advfill_credit_apply: sync_invoice_row failed inv=%s", c.get("invoice_id"))
    try:
        await integrations.sync_advances_journal()
    except Exception:
        log.warning("mgr_advfill_credit_apply: sync_advances_journal failed")
    await state.clear()
    if not credited:
        await _mgr_advfill_edit_or_send(cb, "ℹ️ Выбранные счета уже забраны ранее — изменений нет.")
        await cb.answer()
        return
    try:
        new_balance = await db.get_advance_balance(user_id, wallet_role)
    except Exception:
        new_balance = 0.0

    # п.6 (TZ 12.06): забор ЗП по КРЕДИТ-счёту → дополнительно списать ту же сумму с
    # кредит-кошелька менеджера (движение «ЗП менеджера»), через гейт подтверждения
    # ГД (как п.5). ОДНА GD-задача (kind=credit_spend_gd_confirm, как cw_confirm-
    # хозяин) на КАЖДЫЙ кредит-счёт; запись расхода откладывается до ✅ГД
    # (cw_gd_ok в chat_proxy → apply_credit_wallet_spend mode='withdraw'). AN НЕ
    # пишем — durable-маркер zp_manager_status='confirmed' уже проставлен забором.
    # Кредит-кошелёк = role_key (manager_kv/kia/npn). б/н счета кошелёк НЕ трогают
    # (только аванс). user 12.06: списывать при любом заборе; гранулярность — на счёт.
    credit_zp_sent = 0
    try:
        gd_id_cw = await resolve_default_assignee(db, config, Role.GD)
        mgr_lbl = await get_initiator_label(db, user_id)
        for c in credited:
            inv_full = await db.get_invoice(int(c["invoice_id"]))
            if not inv_full or not inv_full.get("is_credit"):
                continue  # б/н счёт — кредит-кошелёк не трогаем (только аванс)
            amt = float(c.get("amount") or 0)
            if amt <= 0:
                continue
            inv_num = c.get("invoice_number") or f"#{c['invoice_id']}"
            purpose = f"ЗП менеджера №{inv_num}"
            if gd_id_cw and int(gd_id_cw) != user_id:
                task = await db.create_task(
                    project_id=None,
                    type_=TaskType.INVOICE_PAYMENT,
                    status=TaskStatus.OPEN,
                    created_by=user_id,
                    assigned_to=int(gd_id_cw),
                    due_at_iso=None,
                    payload={
                        "kind": "credit_spend_gd_confirm",
                        "wallet_role": role_key,
                        "amount": amt,
                        "purpose": purpose,
                        "invoice_number": str(inv_num),
                        "mode": "withdraw",
                        "invoice_id": int(c["invoice_id"]),
                        "cost_type": None,
                        "initiator_id": user_id,
                        "owner_spend": True,
                        "applied": False,
                    },
                )
                tid = int(task["id"])
                b_gd = InlineKeyboardBuilder()
                b_gd.button(text="✅ Подтвердить", callback_data=f"cw_gd_ok:{tid}")
                b_gd.button(text="❌ Отклонить", callback_data=f"cw_gd_no:{tid}")
                b_gd.adjust(1)
                gd_card = format_card_section(
                    "🏦", "Списание кредита — ЗП менеджера",
                    [
                        ("Менеджер", mgr_lbl),
                        ("Счёт", f"№{inv_num}"),
                        ("Кошелёк", credit_wallet_label(role_key)),
                    ],
                    total=fmt_money(amt), width=38, compact=True,
                )
                await notifier.safe_send(
                    int(gd_id_cw),
                    f"{gd_card}\n\nПодтвердите списание кредит-кошелька или отклоните.",
                    reply_markup=b_gd.as_markup(),
                )
                credit_zp_sent += 1
            else:
                # Fallback (ГД не резолвится / менеджер сам ГД — у КВ/КИА/НПН не бывает):
                # списываем сразу, чтобы кошелёк не рассинхронился с забором.
                try:
                    await apply_credit_wallet_spend(
                        db, integrations,
                        wallet_role=role_key, amount=amt, mode="withdraw",
                        purpose=purpose, entered_by=user_id,
                        invoice_id=int(c["invoice_id"]), invoice_number=str(inv_num),
                    )
                    credit_zp_sent += 1
                except Exception:
                    log.warning(
                        "mgr_advfill: direct credit ЗП spend failed inv=%s",
                        c.get("invoice_id"), exc_info=True,
                    )
    except Exception:
        log.warning("mgr_advfill: credit ЗП GD-gate block failed", exc_info=True)

    lines = [f"<pre>✅ <b>Зачислено в аванс</b>"]
    for c in credited:
        lines.append(f"   №{str(c['invoice_number']):<18s} {fmt_money(float(c['amount'])):>11s}")
    lines.append(f"   Баланс аванса        {fmt_money(new_balance):>11s}")
    lines.append(f"   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {fmt_money(total)}</pre>")
    _msg = "\n".join(lines)
    if credit_zp_sent:
        _msg += (
            f"\n\n🏦 ЗП по {credit_zp_sent} кред-счёт(ам) отправлена на подтверждение "
            "ГД — спишется с кредит-кошелька после ✅."
        )
    await _mgr_advfill_edit_or_send(cb, _msg)
    await cb.answer("Зачислено")
    # Инфо-уведомление ГД (использование кошелька аванса; финансы — только ГД/ТД)
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        mgr_user = await db.get_user_optional(user_id)
        mgr_label = getattr(mgr_user, "full_name", None) or "Менеджер"
        gd_lines = [f"<pre>ℹ️ <b>Менеджер пополнил аванс</b>"]
        gd_lines.append(f"   Кто                  {mgr_label}")
        gd_lines.append(f"   Источник             ЗП менеджер · {len(credited)} счёт(ов)")
        gd_lines.append(f"   Баланс аванса        {fmt_money(new_balance):>11s}")
        gd_lines.append(f"   ━━━━━━━━━━━━━━━━")
        gd_lines.append(f"   Итого  {fmt_money(total)}</pre>")
        try:
            await notifier.safe_send(int(gd_id), "\n".join(gd_lines))
        except Exception:
            log.exception("mgr_advfill_credit_apply: notify ГД failed")


def _matches_credit_menu_button(text: str | None) -> bool:
    """Совпадение с «🏦 Кредит» c опц. бейджем «💳N».

    Пробел после базы исключает коллизию с РП-кнопкой «🏦 Кредитный баланс»
    (RP_BTN_CREDIT_BAL): у неё после «🏦 Кредит» идёт «ный», а не пробел.
    """
    t = (text or "").strip()
    return bool(t) and (t == MGR_BTN_CREDIT_MENU or t.startswith(f"{MGR_BTN_CREDIT_MENU} "))


@router.message(lambda m: _matches_credit_menu_button(m.text))
async def manager_credit_menu(message: Message, state: FSMContext, db: Database) -> None:
    """«🏦 Кредит» — эталон-карточка баланса + inline (расход / кредит-чат)."""
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    await state.clear()
    await _credit_show(message, db, message.from_user.id)  # type: ignore[union-attr]


async def _credit_show(target: Message, db: Database, user_id: int) -> None:
    user = await db.get_user_optional(user_id)
    role_raw = (getattr(user, "role", "") or "")
    wallet = next((r for r in CREDIT_WALLET_ROLES if r in role_raw), None)
    if not wallet:
        await target.answer("❌ Кредитный кошелёк есть только у менеджеров КВ/КИА/НПН.")
        return
    try:
        card = await build_credit_wallet_card(db, wallet)
    except Exception:
        log.warning("build_credit_wallet_card failed role=%s", wallet, exc_info=True)
        card = "🏦 <b>Кредитный баланс</b>"
    b = InlineKeyboardBuilder()
    b.button(text="🏦 Расход кредита", callback_data="cred:spend")
    b.button(text="💬 Кредит-чат", callback_data="cred:chat")
    b.adjust(1)
    await target.answer(card, reply_markup=b.as_markup())


# Reply-кнопки меню «Ещё», остающиеся на экране, пока менеджер вводит сумму.
# Нажатие любой из них во время amount-шага FSM должно ПРЕРВАТЬ ввод, а не
# трактоваться как сумма («введите число»-trap, [[feedback_fsm_old_buttons_trap]]).
_MGR_AMOUNT_ESCAPE_BTNS = frozenset({
    MGR_BTN_FUNDS, MGR_BTN_CREDIT_MENU, MGR_BTN_GD_CONTACT,
    MGR_BTN_SEARCH_INVOICE, "📋 Все задачи", MGR_BTN_HELP,
    MGR_BTN_CANCEL, MGR_BTN_BACK_HOME,
})


async def _mgr_amount_escape(
    message: Message, state: FSMContext, db: Database,
) -> bool:
    """Прервать ввод суммы, если нажата reply-кнопка меню «Ещё».

    amount-хендлеры ниже ловят любой текст в своём состоянии и ответили бы
    «введите число» на нажатие кнопки, заперев пользователя. Возвращает True,
    если текст — кнопка меню (вызывающий обязан сразу выйти): «Финансы»/«Кредит»
    открываются сразу, прочие кнопки прерывают ввод (повторное нажатие сработает
    уже в дефолтном состоянии — клавиатуру не подменяем, роль-нейтрально).
    """
    text = (message.text or "").strip()
    is_credit = _matches_credit_menu_button(text)
    is_funds = _matches_funds_menu_button(text)
    if text not in _MGR_AMOUNT_ESCAPE_BTNS and not is_credit and not is_funds:
        return False
    await state.clear()
    if is_funds and message.from_user:
        await _funds_show(message, db, message.from_user.id)
    elif is_credit and message.from_user:
        await _credit_show(message, db, message.from_user.id)
    else:
        await message.answer("↩️ Ввод суммы прерван — нажмите нужную кнопку ещё раз.")
    return True


@router.callback_query(F.data == "cred:spend")
async def cred_spend(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    user = await db.get_user_optional(cb.from_user.id)  # type: ignore[union-attr]
    role_raw = (getattr(user, "role", "") or "")
    wallet = next((r for r in CREDIT_WALLET_ROLES if r in role_raw), None)
    if not wallet:
        await cb.message.answer(  # type: ignore[union-attr]
            "❌ Кредитный кошелёк есть только у менеджеров КВ/КИА/НПН.")
        return
    await state.clear()
    await state.update_data(wallet_role=wallet, spender_role="manager")
    await _cw_show_mode(cb.message, state, db)  # type: ignore[arg-type]


@router.callback_query(F.data == "cred:chat")
async def cred_chat(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    await cb.answer()
    if not await _mgr_cb_ok(cb, db):
        return
    role = await _current_role(db, cb.from_user.id)  # type: ignore[union-attr]
    channel = _cred_channel(role or "manager_kv")
    cred_label = {
        "manager_kv": "КВ Кред",
        "manager_kia": "КИА Кред",
        "manager_npn": "НПН Кред",
    }.get(channel, "Кред")
    await state.set_state(ManagerChatProxySG.menu)
    await state.update_data(channel=channel)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💬 <b>{cred_label}</b>\n\nВыберите действие:",
        reply_markup=manager_chat_submenu("⬅️ Назад"),
    )


@router.message(F.text == MGR_BTN_DEPOSIT_WITHDRAW)
async def manager_withdraw_start(
    message: Message, state: FSMContext, db: Database,
) -> None:
    """Менеджер нажимает «💸 Расход депозита» — старт FSM."""
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    whitelist_ids = {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    if user_id not in whitelist_ids:
        await message.answer(
            "⛔ Функция недоступна.\n"
            "Запросите ГД добавить вас в whitelist депозитов.",
        )
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    balance = await db.get_deposit_balance(user_id, wallet_role)
    # Павел = РП+НПН, кошельки РАЗНЫЕ (rp-npn-separate-wallets): менеджерская кнопка
    # смотрит первичный кошелёк, но депозит ГД мог положить в РП-кошелёк — инцидент
    # 16.07 «баланс 0 при живых 80 000». Первичный пуст, РП-кошелёк не пуст → расход с него.
    if balance <= 0:
        try:
            _u = await db.get_user_optional(user_id)
            _is_rp = bool(_u) and "rp" in str(getattr(_u, "role", "") or "").split(",")
        except Exception:
            _is_rp = False
        if _is_rp:
            _rp_bal = await db.get_deposit_balance(user_id, "rp")
            if _rp_bal > 0:
                wallet_role, balance = "rp", _rp_bal
    if balance <= 0:
        await message.answer("❌ На депозите нет средств для расхода.")
        return
    await state.set_state(ManagerWithdrawSG.enter_amount)
    await state.update_data(mgr_withdraw_balance=balance, mgr_withdraw_wallet_role=wallet_role)
    await message.answer(
        f"💸 <b>Расход с депозита</b>\n"
        f"Доступно: <b>{fmt_money(balance)}</b>\n\n"
        f"Введите сумму расхода (₽, ≤ {fmt_money(balance)}):",
    )


@router.message(ManagerWithdrawSG.enter_amount, F.text)
async def manager_withdraw_amount_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
    if await _mgr_amount_escape(message, state, db):
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
    data = await state.get_data()
    wallet_role = data.get("mgr_withdraw_wallet_role")
    balance = await db.get_deposit_balance(message.from_user.id, wallet_role)
    if amount > balance + 0.001:
        await message.answer(
            f"❌ Недостаточно средств. Доступно: {fmt_money(balance)}.",
        )
        return
    await state.update_data(mgr_withdraw_amount=amount)
    await state.set_state(ManagerWithdrawSG.enter_comment)
    await message.answer(
        f"Сумма: <b>{fmt_money(amount)}</b>\n\n"
        "📝 На что потратили? (обязательный комментарий, ≥ 3 символов)\n"
        "Например: «бензин», «канцелярия», «такси»",
    )


@router.message(ManagerWithdrawSG.enter_comment, F.text)
async def manager_withdraw_comment_input(
    message: Message, state: FSMContext,
) -> None:
    comment = (message.text or "").strip()
    if len(comment) < 3:
        await message.answer("❌ Комментарий слишком короткий (≥ 3 символов).")
        return
    await state.update_data(mgr_withdraw_comment=comment[:500])
    await state.set_state(ManagerWithdrawSG.attach_receipt)
    b = InlineKeyboardBuilder()
    b.button(text="⏭️ Пропустить (без чека)", callback_data="mgr_withdraw:skip_receipt")
    b.button(text="❌ Отмена", callback_data="mgr_withdraw:cancel")
    b.adjust(1)
    await message.answer(
        "📎 Прикрепите ПП/чек/фото (или нажмите «Пропустить»):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ManagerWithdrawSG.attach_receipt, F.data == "mgr_withdraw:skip_receipt")
async def manager_withdraw_skip_receipt(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    await cb.answer()
    await state.update_data(mgr_withdraw_receipt_file_id=None)
    await _manager_withdraw_show_confirm(
        cb.message, state, db, cb.from_user.id,  # type: ignore[union-attr]
    )


@router.message(ManagerWithdrawSG.attach_receipt)
async def manager_withdraw_receipt_input(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
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
    await state.update_data(mgr_withdraw_receipt_file_id=file_id)
    await _manager_withdraw_show_confirm(message, state, db, message.from_user.id)


async def _manager_withdraw_show_confirm(
    target: Any, state: FSMContext, db: Database, user_id: int,
) -> None:
    """Show preview + confirm/cancel buttons."""
    data = await state.get_data()
    amount = float(data.get("mgr_withdraw_amount") or 0)
    comment = str(data.get("mgr_withdraw_comment") or "")
    file_id = data.get("mgr_withdraw_receipt_file_id")
    wallet_role = data.get("mgr_withdraw_wallet_role")
    balance_before = await db.get_deposit_balance(user_id, wallet_role) if user_id else 0
    balance_after = max(0.0, balance_before - amount)
    receipt_str = "📎 прикреплён" if file_id else "— не приложен"
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="mgr_withdraw:confirm")
    b.button(text="❌ Отмена", callback_data="mgr_withdraw:cancel")
    b.adjust(1)
    await state.set_state(ManagerWithdrawSG.confirm)
    text = (
        f"💸 <b>Подтвердите расход</b>\n\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n"
        f"Комментарий: {comment}\n"
        f"Чек: {receipt_str}\n\n"
        f"Баланс депозита: {fmt_money(balance_before)} → "
        f"<b>{fmt_money(balance_after)}</b>"
    )
    if hasattr(target, "answer"):
        await target.answer(text, reply_markup=b.as_markup())  # type: ignore[union-attr]


@router.callback_query(ManagerWithdrawSG.confirm, F.data == "mgr_withdraw:confirm")
async def manager_withdraw_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    amount = float(data.get("mgr_withdraw_amount") or 0)
    comment = str(data.get("mgr_withdraw_comment") or "")
    file_id = data.get("mgr_withdraw_receipt_file_id")
    wallet_role = data.get("mgr_withdraw_wallet_role")
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
            wallet_role=wallet_role,
        )
    except ValueError as e:
        await cb.message.answer(f"❌ {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.clear()
    new_balance = await db.get_deposit_balance(user_id, wallet_role)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after manager withdraw: %s", e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Расход #{req_id} зафиксирован.\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n"
        f"Остаток депозита: <b>{fmt_money(new_balance)}</b>",
    )
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        manager_name = "Менеджер"
        try:
            u = await db.get_user_optional(user_id)
            if u:
                manager_name = getattr(u, "full_name", None) or getattr(u, "username", None) or manager_name
        except Exception:
            pass
        gd_text = (
            f"💸 <b>{manager_name} снял с депозита</b>\n"
            f"Сумма: <b>{fmt_money(amount)}</b>\n"
            f"Комментарий: {comment}\n"
            f"Чек: {'📎 ниже' if file_id else 'не приложен'}\n\n"
            f"Остаток депозита: <b>{fmt_money(new_balance)}</b>"
        )
        await notifier.safe_send(int(gd_id), gd_text)
        if file_id:
            try:
                await notifier.safe_send_photo(int(gd_id), file_id)
            except Exception:
                try:
                    await notifier.safe_send_document(int(gd_id), file_id)
                except Exception as e:
                    log.warning(
                        "Failed to forward manager withdraw receipt to GD: %s", e,
                    )


@router.callback_query(F.data == "mgr_withdraw:cancel")
async def manager_withdraw_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Расход отменён.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]


# =====================================================================
# funds-2balances 25.05: менеджер сам распределяет advance под счёт.
# =====================================================================


@router.message(F.text.startswith(MGR_BTN_ADV_DISTRIBUTE_BASE))
async def manager_adv_distribute_start(
    message: Message, state: FSMContext, db: Database,
) -> None:
    """Менеджер нажимает «💰 Наполнить ЗП из аванса (X ₽)» — старт FSM."""
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    whitelist_ids = {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    if user_id not in whitelist_ids:
        await message.answer("⛔ Функция недоступна.")
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    unallocated = await db.get_advance_outstanding_unallocated(user_id, wallet_role)
    if unallocated <= 0:
        await message.answer("✅ Нечего распределять — свободного advance нет.")
        return
    invoices = await db.list_invoices_for_manager(user_id)
    if not invoices:
        await message.answer(
            "❌ Нет активных счетов без выплаченной ЗП.\n"
            "<i>Аванс можно распределять только на счета где Invoices.AN (ЗП-менеджер) = 0.</i>",
        )
        return
    await state.set_state(ManagerAdvDistributeSG.select_invoice)
    await state.update_data(mgr_adv_dist_unallocated=unallocated)
    b = InlineKeyboardBuilder()
    for inv in invoices[:15]:
        num = inv.get("invoice_number") or f"id={inv['id']}"
        addr = (inv.get("object_address") or "—")[:25]
        # net = ЗП бланк − удержание (CN) при погашенном долге (механизм перерасчёта,
        # owner 23.06); cap распределения аванса по net-ЗП к выплате.
        plan = manager_zp_net_payout(inv)
        label = f"№{num} {addr}"
        if plan > 0:
            label += f" • план {fmt_money(plan)}"
        b.button(text=label[:60], callback_data=f"mgr_adv_dist:inv:{inv['id']}")
    b.button(text="❌ Отмена", callback_data="mgr_adv_dist:cancel")
    b.adjust(1)
    await message.answer(
        f"💰 <b>Наполнить ЗП из аванса</b>\n"
        f"🔓 Свободно: <b>{fmt_money(unallocated)}</b>\n\n"
        f"Выберите счёт, под который зачислить часть аванса:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(
    ManagerAdvDistributeSG.select_invoice, F.data.startswith("mgr_adv_dist:inv:"),
)
async def manager_adv_distribute_pick_invoice(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    invoice_id = int(cb.data.split(":")[-1])  # type: ignore[union-attr]
    inv = await db.get_invoice(invoice_id)
    if not inv or inv.get("created_by") != cb.from_user.id:
        await cb.message.answer("❌ Счёт не принадлежит вам.")  # type: ignore[union-attr]
        await state.clear()
        return
    plan = manager_zp_net_payout(inv)  # net = бланк − удержание (CN), долг=0 (owner 23.06)
    taken = await db.get_advance_taken_for_invoice(invoice_id)
    data = await state.get_data()
    unalloc = float(data.get("mgr_adv_dist_unallocated") or 0)
    if plan <= 0:
        # Если plan_zp_blank=0 — cap только по unallocated (без плана).
        limit = unalloc
        plan_hint = "<i>План ЗП не задан — лимит только по свободному авансу.</i>"
    else:
        limit = min(unalloc, max(0.0, plan - taken))
        plan_hint = f"План ЗП: <b>{fmt_money(plan)}</b> (взято {fmt_money(taken)})"
    if limit <= 0:
        await cb.message.answer(  # type: ignore[union-attr]
            f"❌ Под этот счёт уже распределено столько ЗП сколько планировалось.",
        )
        await state.clear()
        return
    await state.update_data(
        mgr_adv_dist_invoice_id=invoice_id,
        mgr_adv_dist_plan=plan,
        mgr_adv_dist_limit=limit,
    )
    await state.set_state(ManagerAdvDistributeSG.enter_amount)
    num = inv.get("invoice_number") or f"#{invoice_id}"
    addr = inv.get("object_address") or "—"
    await cb.message.answer(  # type: ignore[union-attr]
        f"📋 Счёт: <b>№{num}</b>\n"
        f"📍 {addr}\n"
        f"{plan_hint}\n"
        f"Доступно к зачёту: <b>{fmt_money(limit)}</b>\n\n"
        f"Введите сумму к зачёту (₽):",
    )


@router.message(ManagerAdvDistributeSG.enter_amount, F.text)
async def manager_adv_distribute_amount(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
    if await _mgr_amount_escape(message, state, db):
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число. Например: 30000")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    data = await state.get_data()
    limit = float(data.get("mgr_adv_dist_limit") or 0)
    if amount > limit + 0.001:
        await message.answer(
            f"❌ Превышает лимит. Доступно: {fmt_money(limit)}.",
        )
        return
    invoice_id = int(data.get("mgr_adv_dist_invoice_id") or 0)
    inv = await db.get_invoice(invoice_id)
    num = inv.get("invoice_number") if inv else f"#{invoice_id}"
    # Счёт END (BQ, sheets.py): status='ended' OR (кредит AND montazh_stage='invoice_end').
    # На закрытом счёте зачёт применится СРАЗУ (AN/AO); на «в работе» — резерв до утв. ЗП.
    is_end = bool(inv) and (
        inv.get("status") == "ended"
        or (bool(inv.get("is_credit")) and inv.get("montazh_stage") == "invoice_end")
    )
    await state.update_data(mgr_adv_dist_amount=amount, mgr_adv_dist_is_end=is_end)
    await state.set_state(ManagerAdvDistributeSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="mgr_adv_dist:confirm")
    b.button(text="❌ Отмена", callback_data="mgr_adv_dist:cancel")
    b.adjust(1)
    hint = (
        "<i>Счёт закрыт — зачёт применится сразу: ЗП по счёту будет "
        "отмечена выплаченной.</i>"
        if is_end else
        "<i>Зачёт сработает автоматически при утверждении ЗП по этому счёту.</i>"
    )
    await message.answer(
        f"💰 <b>Подтвердите распределение</b>\n\n"
        f"Счёт: <b>№{num}</b>\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n\n"
        f"{hint}",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ManagerAdvDistributeSG.confirm, F.data == "mgr_adv_dist:confirm")
async def manager_adv_distribute_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    amount = float(data.get("mgr_adv_dist_amount") or 0)
    invoice_id = int(data.get("mgr_adv_dist_invoice_id") or 0)
    plan = float(data.get("mgr_adv_dist_plan") or 0)
    user_id = cb.from_user.id
    if amount <= 0 or not invoice_id:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    # Счёт END (authoritative на момент подтверждения) → немедленная запись AN/AO
    # без ГД-approval (owner 04.07); иначе — резерв до утверждения ЗП. End = BQ
    # (sheets.py): status='ended' OR (кредит AND montazh_stage='invoice_end').
    inv0 = await db.get_invoice(invoice_id)
    is_end = bool(inv0) and (
        inv0.get("status") == "ended"
        or (bool(inv0.get("is_credit")) and inv0.get("montazh_stage") == "invoice_end")
    )
    try:
        if is_end:
            res = await db.apply_manager_advance_immediate(
                manager_id=user_id, invoice_id=invoice_id, amount=amount,
                actor_id=user_id, wallet_role=wallet_role,
            )
            if res.get("full_closed"):
                result_line = "✅ ЗП по счёту закрыта авансом (отмечена выплаченной)."
            else:
                result_line = (
                    f"✅ Зачтено {fmt_money(res.get('an') or amount)} в ЗП "
                    f"(частично, остаётся невыплаченный остаток)."
                )
        else:
            # Если plan=0 — передадим amount как plan_zp_snapshot чтобы пройти guard.
            snapshot_for_guard = plan if plan > 0 else amount
            item_id = await db.add_advance_item_for_distribution(
                installer_id=user_id, invoice_id=invoice_id, amount=amount,
                plan_zp_snapshot=snapshot_for_guard, actor_id=user_id, role="manager",
                wallet_role=wallet_role,
            )
            result_line = f"✅ Item #{item_id} создан (зачёт при утверждении ЗП)."
    except (ValueError, RuntimeError) as e:
        await cb.message.answer(f"❌ {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.clear()
    try:
        await integrations.sync_invoice_row(invoice_id)
    except Exception as e:
        log.warning("sync_invoice_row failed after mgr_adv_dist: %s", e)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after mgr_adv_dist: %s", e)
    inv = await db.get_invoice(invoice_id)
    num = inv.get("invoice_number") if inv else f"#{invoice_id}"
    await cb.message.answer(  # type: ignore[union-attr]
        f"{result_line}\n"
        f"Счёт: <b>№{num}</b>\n"
        f"Сумма: <b>{fmt_money(amount)}</b>",
    )
    # Notify ГД (informational).
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        name = "Менеджер"
        try:
            u = await db.get_user_optional(user_id)
            if u:
                name = getattr(u, "full_name", None) or getattr(u, "username", None) or name
        except Exception:
            pass
        await notifier.safe_send(
            int(gd_id),
            f"💰 <b>{name} распределил аванс</b>\n"
            f"Счёт: <b>№{num}</b>\n"
            f"Сумма: <b>{fmt_money(amount)}</b>",
        )


@router.callback_query(F.data == "mgr_adv_dist:cancel")
async def manager_adv_distribute_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Распределение отменено.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]


# =====================================================================
# funds-2balances 25.05: менеджер переводит депозит → advance.
# =====================================================================


@router.message(F.text.startswith(MGR_BTN_DEPO_TO_ADV_BASE))
async def manager_depo_to_adv_start(
    message: Message, state: FSMContext, db: Database,
) -> None:
    from .gd import EMPLOYEE_DEPOSIT_WHITELIST
    if not await require_role_message(message, db, roles=ALL_MANAGER_ROLES):
        return
    user_id = message.from_user.id  # type: ignore[union-attr]
    whitelist_ids = {tid for tid in EMPLOYEE_DEPOSIT_WHITELIST.values() if tid}
    if user_id not in whitelist_ids:
        await message.answer("⛔ Функция недоступна.")
        return
    wallet_role = await _mgr_wallet_role(db, user_id)
    depo = await db.get_deposit_balance(user_id, wallet_role)
    if depo <= 0:
        await message.answer("❌ На депозите нет средств.")
        return
    await state.set_state(ManagerDepoToAdvSG.enter_amount)
    await state.update_data(mgr_depo_to_adv_balance=depo, mgr_depo_to_adv_wallet_role=wallet_role)
    await message.answer(
        f"↔️ <b>Перевод Депозит → Аванс</b>\n"
        f"Депозит: <b>{fmt_money(depo)}</b>\n\n"
        f"Введите сумму перевода (₽, ≤ {fmt_money(depo)}):\n"
        f"<i>Переведённые деньги попадут в advance — сможете распределить по счетам.</i>",
    )


@router.message(ManagerDepoToAdvSG.enter_amount, F.text)
async def manager_depo_to_adv_amount(
    message: Message, state: FSMContext, db: Database,
) -> None:
    if not message.from_user:
        return
    if await _mgr_amount_escape(message, state, db):
        return
    text = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(text)
    except (ValueError, TypeError):
        await message.answer("❌ Введите число.")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть > 0.")
        return
    data = await state.get_data()
    wallet_role = data.get("mgr_depo_to_adv_wallet_role")
    depo = await db.get_deposit_balance(message.from_user.id, wallet_role)
    if amount > depo + 0.001:
        await message.answer(f"❌ Недостаточно средств. Депозит: {fmt_money(depo)}.")
        return
    advance_now = await db.get_advance_balance(message.from_user.id, wallet_role)
    await state.update_data(mgr_depo_to_adv_amount=amount)
    await state.set_state(ManagerDepoToAdvSG.confirm)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data="mgr_depo_to_adv:confirm")
    b.button(text="❌ Отмена", callback_data="mgr_depo_to_adv:cancel")
    b.adjust(1)
    await message.answer(
        f"↔️ <b>Подтвердите перевод</b>\n\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n\n"
        f"💸 Депозит: {fmt_money(depo)} → <b>{fmt_money(max(0, depo - amount))} ₽</b>\n"
        f"💰 Аванс: {fmt_money(advance_now)} → <b>{fmt_money(advance_now + amount)}</b>\n\n"
        f"<i>Перевод односторонний — обратно вернуть нельзя.</i>",
        reply_markup=b.as_markup(),
    )


@router.callback_query(ManagerDepoToAdvSG.confirm, F.data == "mgr_depo_to_adv:confirm")
@money_confirm_guard
async def manager_depo_to_adv_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    if not cb.from_user:
        return
    await cb.answer()
    data = await state.get_data()
    amount = float(data.get("mgr_depo_to_adv_amount") or 0)
    wallet_role = data.get("mgr_depo_to_adv_wallet_role")
    user_id = cb.from_user.id
    if amount <= 0:
        await cb.message.answer("❌ Сессия потеряна.")  # type: ignore[union-attr]
        await state.clear()
        return
    try:
        req_id = await db.create_employee_depo_to_adv_transfer(
            employee_id=user_id, amount=amount, actor_id=user_id,
            wallet_role=wallet_role,
        )
    except ValueError as e:
        await cb.message.answer(f"❌ {e}")  # type: ignore[union-attr]
        await state.clear()
        return
    await state.clear()
    new_depo = await db.get_deposit_balance(user_id, wallet_role)
    new_adv = await db.get_advance_balance(user_id, wallet_role)
    try:
        await integrations.sync_advances_journal()
    except Exception as e:
        log.warning("sync_advances_journal failed after mgr_depo_to_adv: %s", e)
    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Перевод #{req_id} выполнен.\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n\n"
        f"💸 Депозит: <b>{fmt_money(new_depo)}</b>\n"
        f"💰 Аванс: <b>{fmt_money(new_adv)}</b>",
    )
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        name = "Менеджер"
        try:
            u = await db.get_user_optional(user_id)
            if u:
                name = getattr(u, "full_name", None) or getattr(u, "username", None) or name
        except Exception:
            pass
        await notifier.safe_send(
            int(gd_id),
            f"↔️ <b>{name} перевёл депозит → аванс</b>\n"
            f"Сумма: <b>{fmt_money(amount)}</b>\n\n"
            f"💸 Депозит: <b>{fmt_money(new_depo)}</b>\n"
            f"💰 Аванс: <b>{fmt_money(new_adv)}</b>",
        )


@router.callback_query(F.data == "mgr_depo_to_adv:cancel")
async def manager_depo_to_adv_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    await cb.answer()
    await state.clear()
    try:
        await cb.message.edit_text("❌ Перевод отменён.")  # type: ignore[union-attr]
    except Exception:
        await cb.message.answer("❌ Отменено.")  # type: ignore[union-attr]
