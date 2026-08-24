"""Universal chat-proxy engine for GD ↔ employee/group communication.

Handles:
- Entering/exiting chat mode per channel
- Showing message history
- Sending messages (text + attachments) and forwarding them to the recipient
- Processing incoming replies from recipients back to GD
- Showing tasks related to a channel

Channels: rp, zamery, accounting, montazh, otd_prodazh, manager_kv, manager_kia, manager_npn
"""

from __future__ import annotations

import html as _html
import logging
import re
from typing import Any

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..config import Config
from ..db import Database
from ..enums import Role, TaskStatus, TaskType
from ..integrations.minio_storage import MinioStorage
from ..keyboards import (
    GD_BTN_CRED_BAL,
    GD_BTN_CRED_SPEND,
    gd_chat_submenu,
    gd_chat_submenu_finance,
    gd_sales_submenu,
    invoice_select_kb,
    main_menu,
    task_actions_kb,
    tasks_kb,
)
from ..services.notifier import Notifier
from ..services.integration_hub import IntegrationHub
from ..services.menu_scope import resolve_menu_scope
from ..states import ChatProxySG, CreditPaymentExecuteSG, CreditPaymentReceiptSG, CreditTaskRejectSG, CreditTaskSG, CreditWalletSpendSG, GdTaskCreateSG, InvoicePaymentSG, ReplyToGDSG
from ..utils import answer_service, apply_credit_wallet_spend, build_credit_wallet_card, credit_wallet_label, fmt_money, format_card_section, get_initiator_label, private_only_reply_markup, refresh_recipient_keyboard, resolve_installer_zp_by_wallet_payment, utcnow, to_iso
from ._mirror import collect_attachment, mirror_attachment

log = logging.getLogger(__name__)

router = Router()
router.message.filter(F.chat.type == "private")


# ---------------------------------------------------------------------------
# Channel resolution: map channel name → target user/chat id
# ---------------------------------------------------------------------------

async def resolve_channel_target(
    channel: str, db: Database, config: Config
) -> int | None:
    """Return the telegram_id (or chat_id for groups) for a given channel."""
    from ..services.assignment import get_work_chat_id, resolve_default_assignee

    role_by_channel = {
        "rp": Role.RP,
        "zamery": Role.ZAMERY,
        "accounting": Role.ACCOUNTING,
        "manager_kv": Role.MANAGER_KV,
        "manager_kia": Role.MANAGER_KIA,
        "manager_npn": Role.MANAGER_NPN,
    }
    if channel == "montazh":
        return await get_work_chat_id(db, config)
    target_role = role_by_channel.get(channel)
    if target_role:
        return await resolve_default_assignee(db, config, target_role)

    return None


def channel_label(channel: str) -> str:
    """Human-readable label for a channel."""
    labels = {
        "rp": "РП",
        "zamery": "Замеры",
        "accounting": "Бухгалтерия",
        "montazh": "Монтажная гр.",
        "otd_prodazh": "Отд.Продаж",
        "manager_kv": "КВ Кред",
        "manager_kia": "КИА Кред",
        "manager_npn": "НПН Кред",
    }
    return labels.get(channel, channel)


def is_group_channel(channel: str) -> bool:
    """Whether this channel targets a group chat (not a user)."""
    return channel == "montazh"


def parse_amount_from_text(text: str) -> float | None:
    """Try to extract a monetary amount from text.

    Recognizes patterns like:
      - 150000
      - 150 000
      - 150000.50
      - 150 000,50
      - сумма: 150000
      - оплата 150000 руб
    Returns the first found amount or None.
    """
    if not text:
        return None
    # Pattern: optional label, then digits with optional spaces/dots as thousands sep, optional decimal
    pattern = r"(?:^|\s)(\d[\d\s.]*\d(?:[,.]\d{1,2})?)(?:\s|$|\s*(?:руб|р\b|₽))"
    matches = re.findall(pattern, text)
    if not matches:
        # Try standalone number
        pattern2 = r"(?:^|\s)(\d{4,}(?:[,.]\d{1,2})?)(?:\s|$)"
        matches = re.findall(pattern2, text)
    if not matches:
        return None
    raw = matches[0].replace(" ", "")
    # Detect whether the last separator (. or ,) is a decimal marker
    # (followed by exactly 1-2 digits at the end), or a thousands separator.
    decimal_match = re.search(r"[,.](\d{1,2})$", raw)
    if decimal_match:
        decimal_part = decimal_match.group(1)
        integer_part = raw[: decimal_match.start()]
        integer_clean = integer_part.replace(".", "").replace(",", "")
        raw_clean = integer_clean + "." + decimal_part
    else:
        # No decimal part — strip all separators
        raw_clean = raw.replace(".", "").replace(",", "")
    if not raw_clean or raw_clean == ".":
        return None
    try:
        return float(raw_clean)
    except ValueError:
        return None


FINANCE_CHANNELS = {"manager_kv", "manager_kia", "manager_npn"}

# Маппинг канала → роль менеджера (для поиска кредитных счетов)
_CHANNEL_TO_ROLE = {
    "manager_kv": "manager_kv",
    "manager_kia": "manager_kia",
    "manager_npn": "manager_npn",
}


# Регекс для invoice_number в тексте: '26323-1КВ', '2642-1НПН', 'КВ 4', 'КИА 11', 'НПН 2'
_INVOICE_NUMBER_RE = re.compile(
    r"\b(\d+-\d+(?:КВ|КИА|НПН))\b|\b((?:КВ|КИА|НПН)\s*\d+)\b",
    re.IGNORECASE,
)


def _extract_invoice_number(text: str) -> str | None:
    """Extract first invoice_number-like token from text. None if not found."""
    if not text:
        return None
    m = _INVOICE_NUMBER_RE.search(text)
    if not m:
        return None
    found = m.group(1) or m.group(2) or ""
    # Нормализация: 'КВ  4' -> 'КВ 4', upper-case суффикс
    found = re.sub(r"\s+", " ", found.strip())
    return found


async def _auto_credit_expense(
    db: "Database",
    channel: str,
    amount: float,
    description: str,
    *,
    entered_by: int,
    chat_message_id: int | None = None,
) -> None:
    """Auto-record credit expense to the credit invoice referenced in message text.

    GAP 3.1 fix: ищем invoice_number в тексте сообщения (regex). Привязываем
    расход к этому конкретному счёту (если он кредитный для роли канала).
    Если номер не найден или не подходит — НЕ пишем (избегаем рандомной
    привязки к 'последнему кредитному').
    """
    role = _CHANNEL_TO_ROLE.get(channel)
    if not role:
        return

    inv_num = _extract_invoice_number(description)
    if not inv_num:
        import logging
        logging.getLogger(__name__).info(
            "_auto_credit_expense: no invoice_number in text, skip (channel=%s)", channel,
        )
        return

    try:
        cur = await db.conn.execute(
            "SELECT id, invoice_number, creator_role, is_credit FROM invoices "
            "WHERE invoice_number = ? COLLATE NOCASE",
            (inv_num,),
        )
        row = await cur.fetchone()
        if not row:
            import logging
            logging.getLogger(__name__).info(
                "_auto_credit_expense: invoice_number=%r not found, skip", inv_num,
            )
            return
        if not row["is_credit"]:
            import logging
            logging.getLogger(__name__).info(
                "_auto_credit_expense: invoice %r is not credit, skip", inv_num,
            )
            return
        if row["creator_role"] != role:
            import logging
            logging.getLogger(__name__).info(
                "_auto_credit_expense: invoice %r role=%s but channel role=%s, skip",
                inv_num, row["creator_role"], role,
            )
            return

        invoice_id = int(row["id"])
        # carry-DA (02.06 вечер-2): авто-расход из чат-канала двигает баланс —
        # (1) credit_expense на привязанный кредит-счёт (уменьшает carry-DA);
        # (2) реестр credit_spends (журнал; без cost_type → не в DP–DV).
        # description НЕ должен начинаться с «остаток» (иначе маркер абс. остатка).
        _ce_desc = description if not (description or "").lower().startswith("остаток") else f"Расход: {description}"
        await db.add_credit_expense(
            invoice_id, amount, _ce_desc, entered_by,
            chat_message_id=chat_message_id,
        )
        await db.add_credit_spend(
            role, amount, entered_by,
            description=description,
            bound_invoice_id=invoice_id,
            chat_message_id=chat_message_id,
        )
    except Exception:
        import logging
        logging.getLogger(__name__).warning(
            "Failed to auto-record credit expense for channel=%s", channel, exc_info=True,
        )

# Composite channels: one button -> multiple underlying channels
COMPOSITE_CHANNELS = {
    "otd_prodazh": ["rp", "manager_kv", "manager_kia", "manager_npn"],
}

# Write targets: who can be written to in each GD channel
# Format: list of (channel_key, button_label)
CHANNEL_WRITE_TARGETS: dict[str, list[tuple[str, str]]] = {
    "rp": [("rp", "➡️ РП")],
    "zamery": [("zamery", "➡️ Замерщик")],
    "accounting": [("accounting", "➡️ Бухгалтерия")],
    "montazh": [("montazh", "➡️ Монтажная гр.")],
    "otd_prodazh": [
        ("rp", "➡️ РП"),
        ("manager_kv", "➡️ Менеджер КВ"),
        ("manager_kia", "➡️ Менеджер КИА"),
        ("manager_npn", "➡️ Менеджер НПН"),
    ],
    "manager_kv": [("manager_kv", "➡️ Менеджер КВ")],
    "manager_kia": [("manager_kia", "➡️ Менеджер КИА")],
    "manager_npn": [("manager_npn", "➡️ Менеджер НПН")],
}


def gd_channel_menu(channel: str):
    """Return the correct GD submenu keyboard for a channel."""
    if channel == "otd_prodazh":
        return gd_sales_submenu()
    if channel in FINANCE_CHANNELS:
        return gd_chat_submenu_finance()
    return gd_chat_submenu()



# ---------------------------------------------------------------------------
# Enter chat submenu
# ---------------------------------------------------------------------------

async def enter_chat_menu(
    message: Message,
    state: FSMContext,
    channel: str,
    db: Database | None = None,
) -> None:
    """Show chat-proxy submenu for a given channel.

    Для finance-каналов ГД (КВ/КИА/НПН) при передаче db карточка кредитного
    баланса показывается СРАЗУ при входе (owner 21.07): раньше она появлялась
    только после нажатия «🏦 Баланс кошелька». Прочие каналы (db=None) —
    поведение без изменений.
    """
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    if db is not None and channel in FINANCE_CHANNELS:
        try:
            card = await build_credit_wallet_card(db, channel, show_header_total=False)
            await message.answer(card)
        except Exception:
            log.warning("enter_chat_menu: credit card failed channel=%s", channel, exc_info=True)

    label = channel_label(channel)
    await message.answer(
        f"💬 <b>{label}</b>\n\nВыберите действие:",
        reply_markup=gd_channel_menu(channel),
    )


# ---------------------------------------------------------------------------
# Show message history
# ---------------------------------------------------------------------------

async def show_history(
    message: Message,
    db: Database,
    config: Config,
    channel: str,
) -> None:
    """Display last N messages for a channel."""
    limit = config.chat_history_limit
    viewer_id = message.from_user.id if message.from_user else None

    if channel in COMPOSITE_CHANNELS:
        # Aggregate messages from all sub-channels
        all_msgs: list[dict] = []
        for sub_ch in COMPOSITE_CHANNELS[channel]:
            sub_msgs = await db.list_chat_messages(sub_ch, limit=limit)
            for m in sub_msgs:
                m["_channel"] = sub_ch
            all_msgs.extend(sub_msgs)
            # Mark incoming messages as read for viewer
            if viewer_id:
                await db.mark_messages_read(viewer_id, sub_ch)
        all_msgs.sort(key=lambda m: m.get("created_at", ""), reverse=True)
        messages = all_msgs[:limit]
    else:
        messages = await db.list_chat_messages(channel, limit=limit)
        if viewer_id:
            await db.mark_messages_read(viewer_id, channel)

    label = channel_label(channel)

    if not messages:
        await message.answer(
            f"📖 <b>{label} — Переписка</b>\n\n"
            "Сообщений пока нет.",
            reply_markup=gd_channel_menu(channel),
        )
        return

    lines = [f"📖 <b>{label} — Переписка</b> (последние {len(messages)}):\n"]
    for m in messages:
        direction = "➡️" if m["direction"] == "outgoing" else "⬅️"
        if channel in COMPOSITE_CHANNELS and m.get("_channel"):
            direction += f" [{channel_label(m['_channel'])}]"
        ts = m["created_at"][:16].replace("T", " ") if m.get("created_at") else ""
        text_preview = (m.get("text") or "📎 вложение")[:100]
        lines.append(f"{direction} <i>{ts}</i>  {text_preview}")

    text = "\n".join(lines)
    # Truncate if too long for Telegram
    if len(text) > 3800:
        text = text[:3800] + "\n\n... (обрезано)"

    await message.answer(text, reply_markup=gd_channel_menu(channel))


# ---------------------------------------------------------------------------
# Enter writing mode
# ---------------------------------------------------------------------------

async def enter_writing(
    message: Message,
    state: FSMContext,
    channel: str,
) -> None:
    """Switch to message input mode."""
    await state.set_state(ChatProxySG.writing)
    await state.update_data(channel=channel, pending_attachments=[])

    label = channel_label(channel)
    await message.answer(
        f"✏️ <b>Написать → {label}</b>\n\n"
        "Введите текст сообщения.\n"
        "Можно прикрепить файлы/фото.\n"
        "Для отмены: /cancel",
    )


# ---------------------------------------------------------------------------
# Handle outgoing message (GD writes text/attachment in writing state)
# ---------------------------------------------------------------------------

async def _deliver_chat_message(
    *,
    reply_to: Message,
    sender_id: int,
    channel: str,
    text: str,
    db: Database,
    config: Config,
    notifier: Notifier,
    file_info: dict[str, Any] | None = None,
    tg_message_id: int | None = None,
    caption: str | None = None,
) -> bool:
    """Отправить сообщение ГД в канал: сохранить в БД + переслать адресату.

    Ядро вынесено из handle_writing (owner 25.07), чтобы тем же путём уходил
    текст, набранный сразу в меню канала (chat_menu_freetext) — без повторного
    ввода. `reply_to` — сообщение, в ответ на которое отчитаться ГД.
    Возвращает False, если адресат канала не настроен.
    """
    target_id = await resolve_channel_target(channel, db, config)
    if not target_id:
        await reply_to.answer(
            f"⚠️ Адресат для канала «{channel_label(channel)}» не настроен.\n"
            "Попросите администратора настроить конфигурацию.",
        )
        return False

    label = channel_label(channel)
    has_attach = file_info is not None

    # Save to DB
    chat_msg = await db.save_chat_message(
        channel=channel,
        sender_id=sender_id,
        direction="outgoing",
        text=text or None,
        receiver_id=target_id if not is_group_channel(channel) else None,
        receiver_chat_id=target_id if is_group_channel(channel) else None,
        tg_message_id=tg_message_id,
        has_attachment=has_attach,
    )

    # Auto-detect sum for finance channels
    if channel in FINANCE_CHANNELS and text:
        amount = parse_amount_from_text(text)
        if amount is not None:
            await db.save_finance_entry(
                channel=channel,
                amount=amount,
                entered_by=sender_id,
                chat_message_id=int(chat_msg["id"]),
                description=text[:200],
            )
            # Автозапись расхода в credit_expenses для кредитных счетов
            await _auto_credit_expense(
                db, channel, amount, text[:200],
                entered_by=sender_id, chat_message_id=int(chat_msg["id"]),
            )

    if file_info:
        await db.save_chat_attachment(
            chat_message_id=int(chat_msg["id"]),
            tg_file_id=file_info["file_id"],
            file_type=file_info["file_type"],
            tg_file_unique_id=file_info.get("file_unique_id"),
            caption=caption,
            minio_object_key=file_info.get("minio_object_key"),
        )

    # Forward to recipient with reply button (для всех каналов, включая группу)
    header = f"📩 <b>От ГД</b> ({label}):\n\n"
    reply_b = InlineKeyboardBuilder()
    reply_b.button(text="💬 Ответить ГД", callback_data=f"reply_to_gd:{channel}")
    reply_b.adjust(1)
    reply_markup = reply_b.as_markup()
    if text:
        await notifier.safe_send(target_id, header + text, reply_markup=reply_markup)
    if file_info:
        await notifier.safe_send_media(
            target_id,
            file_info["file_type"],
            file_info["file_id"],
            caption=caption,
        )
    if not is_group_channel(channel):
        await refresh_recipient_keyboard(notifier, db, config, int(target_id))

    await reply_to.answer(
        f"✅ Сообщение отправлено → {label}",
        reply_markup=gd_channel_menu(channel),
    )
    return True


@router.message(ChatProxySG.writing)
async def handle_writing(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    storage: MinioStorage | None = None,
) -> None:
    """Process GD's outgoing message in chat-proxy."""
    data = await state.get_data()
    channel = data.get("channel", "")
    u = message.from_user
    if not u:
        return

    text = (message.text or message.caption or "").strip()

    # Handle file/photo attachments via shared helper (uploads to MinIO if enabled)
    file_info = await mirror_attachment(message, storage, prefix=f"chat/{channel}/{u.id}")

    if not text and not file_info:
        await message.answer("Введите текст или прикрепите файл.")
        return

    await _deliver_chat_message(
        reply_to=message,
        sender_id=u.id,
        channel=channel,
        text=text,
        db=db,
        config=config,
        notifier=notifier,
        file_info=file_info,
        tg_message_id=message.message_id,
        caption=message.caption,
    )
    await state.set_state(ChatProxySG.menu)


# ---------------------------------------------------------------------------
# Show tasks for channel
# ---------------------------------------------------------------------------

async def show_channel_tasks(
    message: Message,
    db: Database,
    config: Config,
    channel: str,
    gd_user_id: int,
) -> None:
    """Show tasks related to a channel (incoming + outgoing)."""
    target_id = await resolve_channel_target(channel, db, config)
    label = channel_label(channel)

    all_tasks: list[dict[str, Any]] = []

    if is_group_channel(channel):
        # Для группового канала — ищем задачи по source в payload
        all_tasks = await db.list_tasks_by_source(
            source=f"chat_proxy:{channel}",
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            created_by=gd_user_id,
            limit=20,
        )
    elif target_id:
        # Outgoing: GD created, assigned to target
        outgoing = await db.list_tasks_for_user(
            assigned_to=target_id,
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            limit=20,
        )
        # Filter to tasks created by GD
        outgoing = [t for t in outgoing if t.get("created_by") == gd_user_id]
        all_tasks.extend(outgoing)

        # Incoming: target created, assigned to GD
        incoming = await db.list_tasks_for_user(
            assigned_to=gd_user_id,
            statuses=[TaskStatus.OPEN, TaskStatus.IN_PROGRESS],
            limit=20,
        )
        incoming = [t for t in incoming if t.get("created_by") == target_id]
        all_tasks.extend(incoming)

    # 💳 Кредит-заявки канала (credit_payment_request) — платит владелец-менеджер.
    # Показываем в «Задачи», чтобы дрилл-даун совпадал с 💳-бейджем на кнопке
    # канала (user 04.07: «бейдж 1, а внутри пусто»). Источник ключа канала —
    # тот же COALESCE(wallet_role, channel), что и у count-функции бейджа.
    try:
        all_tasks.extend(await db.list_open_credit_payment_requests_by_channel(channel))
    except Exception:
        log.exception("show_channel_tasks: credit requests fetch failed (channel=%s)", channel)

    # Deduplicate and sort
    seen_ids: set[int] = set()
    unique_tasks: list[dict[str, Any]] = []
    for t in all_tasks:
        tid = int(t["id"])
        if tid not in seen_ids:
            seen_ids.add(tid)
            unique_tasks.append(t)
    unique_tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)

    if not unique_tasks:
        b = InlineKeyboardBuilder()
        b.button(text="➕ Создать задачу", callback_data=f"gd_task_create:{channel}")
        await message.answer(
            f"📋 <b>{label} — Задачи</b>\n\nОткрытых задач нет.",
            reply_markup=b.as_markup(),
        )
        return

    b = InlineKeyboardBuilder()
    b.button(text="➕ Создать задачу", callback_data=f"gd_task_create:{channel}")
    await message.answer(
        f"📋 <b>{label} — Задачи</b> ({len(unique_tasks)}):",
        reply_markup=tasks_kb(unique_tasks),
    )
    await message.answer("Или создайте новую:", reply_markup=b.as_markup())


# ---------------------------------------------------------------------------
# Chat submenu navigation (Переписка / Написать / Задачи / Назад)
# ---------------------------------------------------------------------------

@router.message(ChatProxySG.menu, F.text == "📖 Переписка")
async def chat_menu_history(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    await show_history(message, db, config, channel)


@router.message(ChatProxySG.menu, F.text == "✏️ Написать")
async def chat_menu_write(
    message: Message, state: FSMContext
) -> None:
    """Fallback: gd.py:gd_write_pick_target перехватывает раньше (gd.router первый).
    Этот хэндлер сработает только если gd.router не обработал сообщение."""
    data = await state.get_data()
    channel = data.get("channel", "")
    await enter_writing(message, state, channel)


@router.message(ChatProxySG.menu, F.text == "📋 Задачи")
async def chat_menu_tasks(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    u = message.from_user

    # «📋 Задачи» показывает список задач канала. Трата кредитного кошелька
    # перенесена на отдельную кнопку «➕ Расход кредита» (TZ кошелёк 02.06);
    # старый вход CreditTaskSG отсюда убран.
    if u:
        await show_channel_tasks(message, db, config, channel, u.id)


# ---------------------------------------------------------------------------
# Кредитный кошелёк ГД: вход из finance-канала (кошелёк = канал). TZ 02.06.
# «🏦 Баланс кошелька» → карточка; «➕ Расход кредита» → форма CreditWalletSpendSG
# (shared-хендлеры в manager_new). Выбор кошелька не нужен — он = канал.
# ---------------------------------------------------------------------------

@router.message(ChatProxySG.menu, F.text == GD_BTN_CRED_BAL)
async def gd_cred_balance(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    if channel not in FINANCE_CHANNELS:
        return
    try:
        # ГД-вид: сумму баланса в шапке не показываем (она остаётся в footer
        # «Остаток»). По запросу user 03.06 — только для ГД (канал=кошелёк).
        card = await build_credit_wallet_card(db, channel, show_header_total=False)
    except Exception:
        log.warning("gd_cred_balance: card failed channel=%s", channel, exc_info=True)
        card = "⚠️ Не удалось построить карточку баланса."
    await message.answer(card)


@router.message(ChatProxySG.menu, F.text == GD_BTN_CRED_SPEND)
async def gd_cred_spend(message: Message, state: FSMContext, db: Database) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    if channel not in FINANCE_CHANNELS:
        return
    from .manager_new import _cw_show_mode
    await state.update_data(wallet_role=channel, spender_role="gd")
    await _cw_show_mode(message, state, db)


async def _cw_exit_to_channel(message: Message, state: FSMContext) -> None:
    """Выйти из FSM траты кредита обратно в меню finance-канала ГД.

    Reply-кнопка «⬅️ Назад» меню канала остаётся на экране во время cwspend-формы,
    но штатный chat_menu_back фильтрован по ChatProxySG.menu и в состояниях
    CreditWalletSpendSG не срабатывал (баг: «Назад» на списке счетов мёртвая,
    user 03.06). Сбрасываем cwspend-стейт и возвращаемся в меню канала.
    """
    data = await state.get_data()
    channel = data.get("wallet_role") or data.get("channel") or ""
    await enter_chat_menu(message, state, channel)


@router.message(
    StateFilter(
        CreditWalletSpendSG.pick_mode,
        CreditWalletSpendSG.pick_invoice,
        CreditWalletSpendSG.pick_category,
        CreditWalletSpendSG.confirm,
    ),
    F.text == "⬅️ Назад",
)
async def cw_back_from_picker(message: Message, state: FSMContext) -> None:
    """«⬅️ Назад» на шагах режим/счёт/категория/подтверждение кредит-расхода → меню канала."""
    await _cw_exit_to_channel(message, state)


# ---------------------------------------------------------------------------
# CreditTaskSG: ГД → finance-канал → «📋 Задачи» → сумма + назначение
# ---------------------------------------------------------------------------

async def _credit_task_start(
    message: Message, state: FSMContext, db: Database, channel: str
) -> None:
    """Войти в FSM CreditTaskSG: показать активный счёт и попросить сумму."""
    label = channel_label(channel)
    active = await db.get_active_credit_invoice_for_channel(channel)
    if not active:
        await message.answer(
            f"⚠️ Нет активного кредитного счёта для канала <b>{label}</b>.\n"
            "Все open credit-счета закрыты или DA ≤ 0. "
            "Дождитесь создания нового кредитного счёта.",
        )
        return

    da = float(active.get("_da") or 0)
    await state.set_state(CreditTaskSG.amount)
    await state.update_data(
        channel=channel,
        active_invoice_id=int(active["id"]),
        active_invoice_number=active.get("invoice_number") or f"#{active['id']}",
        active_invoice_da=da,
    )
    await message.answer(
        f"💳 <b>Новый расход кред. средств — {label}</b>\n\n"
        f"Активный счёт: <b>{active.get('invoice_number')}</b>\n"
        f"Остаток (DA): <b>{da:,.0f} ₽</b>\n\n"
        "Введите <b>сумму расхода ₽</b> (например: <code>25000</code>):\n"
        "Для отмены: <code>/cancel</code>",
    )


@router.message(F.text.casefold().in_({"/cancel", "❌ отмена", "отмена", "cancel"}), CreditTaskSG.amount)
@router.message(F.text.casefold().in_({"/cancel", "❌ отмена", "отмена", "cancel"}), CreditTaskSG.purpose)
async def credit_task_cancel_msg(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)
    await message.answer("❌ Расход отменён.", reply_markup=gd_channel_menu(channel))


@router.message(CreditTaskSG.amount)
async def credit_task_amount(message: Message, state: FSMContext) -> None:
    """Шаг 1: парс суммы."""
    txt = (message.text or "").strip().replace(" ", "").replace(",", ".")
    try:
        amount = float(txt)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Сумма должна быть положительным числом. Попробуйте ещё раз:")
        return
    await state.update_data(amount=amount)
    await state.set_state(CreditTaskSG.purpose)
    await message.answer(
        f"Сумма: <b>{amount:,.0f} ₽</b>\n\n"
        "Введите <b>назначение платежа</b> (3–200 символов):\n"
        "Например: <code>Оплата стекла Зенит</code>\n"
        "Для отмены: <code>/cancel</code>",
    )


@router.message(CreditTaskSG.purpose)
async def credit_task_purpose(message: Message, state: FSMContext) -> None:
    """Шаг 2: парс назначения → карточка подтверждения."""
    purpose = (message.text or "").strip()
    if not (3 <= len(purpose) <= 200):
        await message.answer("⚠️ Назначение должно быть 3–200 символов. Попробуйте ещё раз:")
        return
    await state.update_data(purpose=purpose)
    await state.set_state(CreditTaskSG.confirm)
    data = await state.get_data()
    amount = float(data["amount"])
    inv_num = data["active_invoice_number"]
    da = float(data.get("active_invoice_da") or 0)
    new_da = da - amount

    b = InlineKeyboardBuilder()
    b.button(text="✅ Записать", callback_data="credit_task:confirm")
    b.button(text="❌ Отмена", callback_data="credit_task:cancel")
    b.adjust(1)
    warning = ""
    if new_da < 0:
        warning = (
            f"\n\n⚠️ После записи DA станет <b>{new_da:,.0f} ₽</b> (отрицательный). "
            "Активный счёт переключится при следующем расходе."
        )
    await message.answer(
        "<b>Подтвердите расход:</b>\n\n"
        f"  Счёт: <b>{inv_num}</b>\n"
        f"  Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"  Назначение: {purpose}\n\n"
        f"  Остаток DA: {da:,.0f} → <b>{new_da:,.0f}</b> ₽"
        + warning,
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "credit_task:cancel")
async def credit_task_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    channel = data.get("channel", "")
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)
    try:
        await cb.message.edit_text("❌ Расход отменён.")  # type: ignore[union-attr]
    except Exception:
        pass
    await cb.answer()
    try:
        await cb.message.answer(  # type: ignore[union-attr]
            "Возврат в меню:", reply_markup=gd_channel_menu(channel),
        )
    except Exception:
        pass


@router.callback_query(F.data == "credit_task:confirm", CreditTaskSG.confirm)
async def credit_task_confirm(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    integrations: IntegrationHub,
    notifier: Notifier,
    config: Config,
) -> None:
    """Шаг 3: INSERT credit_expense + sync лист + создать task менеджеру + уведомить."""
    from ..services.assignment import resolve_default_assignee

    data = await state.get_data()
    channel = str(data.get("channel") or "")
    inv_id = int(data["active_invoice_id"])
    inv_num = str(data["active_invoice_number"])
    amount = float(data["amount"])
    purpose = str(data["purpose"])
    gd_id = cb.from_user.id

    # 1. INSERT credit_expense
    ce_id = await db.add_credit_expense(
        invoice_id=inv_id,
        amount=amount,
        description=purpose,
        entered_by=gd_id,
    )

    # 2. Resolve manager by channel role
    role_map = {
        "manager_kv": Role.MANAGER_KV,
        "manager_kia": Role.MANAGER_KIA,
        "manager_npn": Role.MANAGER_NPN,
    }
    manager_role = role_map.get(channel)
    manager_id: int | None = None
    if manager_role:
        try:
            manager_id = await resolve_default_assignee(db, config, manager_role)
        except Exception:
            log.warning("credit_task: cannot resolve manager for %s", channel, exc_info=True)
            manager_id = None

    # 3. Create task (assigned_to=manager) — payload включает credit_expense_id для close-flow
    task_id: int | None = None
    if manager_id:
        try:
            task = await db.create_task(
                project_id=None,
                type_=TaskType.INVOICE_PAYMENT,
                status=TaskStatus.OPEN,
                created_by=gd_id,
                assigned_to=manager_id,
                due_at_iso=None,
                payload={
                    "kind": "credit_payment_request",
                    "invoice_id": inv_id,
                    "invoice_number": inv_num,
                    "amount": amount,
                    "purpose": purpose,
                    "credit_expense_id": ce_id,
                    "channel": channel,
                },
            )
            task_id = int(task["id"])
        except Exception:
            log.warning("credit_task: create_task failed", exc_info=True)

    # 4. Force-sync invoice → лист (CZ + DA пересчитаются)
    try:
        await integrations.sync_invoice_row(inv_id)
    except Exception:
        log.warning("credit_task: sync_invoice_row failed for %s", inv_id, exc_info=True)

    # 5. Audit
    try:
        await db.audit(
            actor_id=gd_id,
            action="credit_expense_added",
            entity="credit_expenses",
            entity_id=str(ce_id),
            payload={
                "invoice_id": inv_id, "invoice_number": inv_num,
                "amount": amount, "purpose": purpose,
                "channel": channel, "task_id": task_id, "manager_id": manager_id,
            },
        )
    except Exception:
        log.debug("credit_task: audit failed", exc_info=True)

    # 6. Notify manager
    if manager_id and task_id:
        b2 = InlineKeyboardBuilder()
        b2.button(text="📎 Прикрепить платёжку", callback_data=f"credit_pay_attach:{task_id}")
        b2.adjust(1)
        await notifier.safe_send(
            chat_id=manager_id,
            text=(
                "💳 <b>Новая задача: нужна платёжка</b>\n\n"
                f"  Счёт: <b>{inv_num}</b>\n"
                f"  Сумма: <b>{amount:,.0f} ₽</b>\n"
                f"  Назначение: {purpose}\n\n"
                "Прикрепите платёжку (фото/PDF) кнопкой ниже."
            ),
            reply_markup=b2.as_markup(),
        )

    # 7. Confirm to GD + return to menu
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)
    sync_note = ""
    if manager_id is None:
        sync_note = "\n\n⚠️ Менеджер для канала не найден — задача не создана."
    elif task_id is None:
        sync_note = "\n\n⚠️ Не удалось создать задачу (см. логи)."

    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "✅ <b>Расход записан</b>\n\n"
            f"  Счёт: <b>{inv_num}</b>\n"
            f"  Сумма: <b>{amount:,.0f} ₽</b>\n"
            f"  Назначение: {purpose}\n"
            + (f"  Задача отправлена менеджеру (id={task_id})." if task_id else "")
            + sync_note,
        )
    except Exception:
        pass
    await cb.answer("Записано")


# ---------------------------------------------------------------------------
# CreditPaymentReceiptSG: менеджер прикрепляет платёжку → task close
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("credit_pay_attach:"))
async def credit_payment_attach_start(
    cb: CallbackQuery, state: FSMContext, db: Database
) -> None:
    """Менеджер нажимает «📎 Прикрепить платёжку» → переход в state ожидания файла."""
    try:
        task_id = int(cb.data.split(":", 1)[1])  # type: ignore[union-attr]
    except (ValueError, IndexError, AttributeError):
        await cb.answer("Некорректный task_id", show_alert=True)
        return

    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена", show_alert=True)
        return

    if int(task.get("assigned_to") or 0) != cb.from_user.id:
        await cb.answer("Эта задача не для вас", show_alert=True)
        return

    if task.get("status") not in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
        await cb.answer("Задача уже закрыта", show_alert=True)
        return

    await state.clear()
    await state.set_state(CreditPaymentReceiptSG.waiting)
    await state.update_data(credit_task_id=task_id)
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "📎 Пришлите <b>платёжку</b> (фото или PDF) в ответ на это сообщение.\n"
        "Для отмены: <code>/cancel</code>",
    )


@router.message(F.text.casefold().in_({"/cancel", "❌ отмена", "отмена", "cancel"}), CreditPaymentReceiptSG.waiting)
async def credit_payment_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Отменено. Задача осталась открытой.")


@router.message(CreditPaymentReceiptSG.waiting)
async def credit_payment_receipt(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    storage: MinioStorage,
) -> None:
    """Менеджер прислал платёжку — сохраняем, закрываем задачу, шлём инициатору + ГД."""
    data = await state.get_data()
    task_id = int(data.get("credit_task_id") or 0)
    if not task_id:
        await state.clear()
        await message.answer("⚠️ task_id потерян. Попробуйте ещё раз через кнопку «📎 Прикрепить платёжку».")
        return

    try:
        task = await db.get_task(task_id)
    except KeyError:
        await state.clear()
        await message.answer("⚠️ Задача не найдена.")
        return

    file_info = await mirror_attachment(message, storage, prefix=f"credit_payments/task_{task_id}")
    if not file_info:
        await message.answer(
            "⚠️ Это не похоже на фото или документ. "
            "Пришлите платёжку (фото или PDF), либо /cancel для отмены."
        )
        return

    # Закрываем задачу (payload-обогащение пропускаем — file уже отправляем ГД ниже)
    await db.update_task_status(task_id, TaskStatus.DONE)

    # Парсим оригинальный payload (для invoice_number / amount / purpose в сообщении ГД)
    raw_payload = task.get("payload_json") or {}
    if isinstance(raw_payload, str):
        import json as _json
        try:
            raw_payload = _json.loads(raw_payload)
        except Exception:
            raw_payload = {}
    payload = dict(raw_payload or {})

    # Платёжка → инициатору (created_by: РП или ГД) + ГД (TZ кошелёк 02.06).
    # Если инициатор = ГД, dedup оставит одного адресата.
    inv_num = (payload.get("invoice_number") or "—")
    amount = float(payload.get("amount") or 0)
    purpose = payload.get("purpose") or ""
    caption = (
        "✅ <b>Платёжка получена</b>\n\n"
        f"  Счёт: <b>{inv_num}</b>\n"
        f"  Сумма: <b>{amount:,.0f} ₽</b>\n"
        f"  Назначение: {purpose}\n"
        f"  От: {message.from_user.full_name if message.from_user else '—'}"
    )
    recipients: list[int] = []
    cb_id = task.get("created_by")
    if cb_id:
        recipients.append(int(cb_id))
    try:
        from ..services.assignment import resolve_default_assignee
        gd_default = await resolve_default_assignee(db, config, Role.GD)
        if gd_default and int(gd_default) not in recipients:
            recipients.append(int(gd_default))
    except Exception:
        log.debug("credit_payment_receipt: resolve GD failed", exc_info=True)
    ft = file_info.get("file_type")
    fid = file_info.get("file_id")
    for rcpt in recipients:
        if ft == "document" and fid:
            await notifier.safe_send_document(rcpt, fid, caption=caption)
        elif ft == "photo" and fid:
            await notifier.safe_send_photo(rcpt, fid, caption=caption)
        else:
            await notifier.safe_send(rcpt, caption)

    await db.audit(
        actor_id=message.from_user.id if message.from_user else None,
        action="credit_payment_receipt_received",
        entity="tasks",
        entity_id=str(task_id),
        payload={"file_type": file_info.get("file_type")},
    )

    await state.clear()
    await message.answer("✅ Платёжка отправлена ГД. Задача закрыта.")


# ---------------------------------------------------------------------------
# §C (TZ 04.06): 2-этапное подтверждение кредит-задачи менеджером-владельцем.
#   credit_recv       — «✅ Получил» (open→in_progress, read-ack)
#   credit_exec       — «✅ Исполнено» → платёжка опц. → запись расхода + close
#   credit_exec_skip  — исполнить без платёжки
#   credit_rej        — «❌ Отклонить» → причина → reject (расход НЕ пишется)
# Запись расхода ОТЛОЖЕНА и происходит ТОЛЬКО в _finalize_credit_execution.
# ---------------------------------------------------------------------------

def _parse_task_payload(task: dict[str, Any]) -> dict[str, Any]:
    raw = task.get("payload_json") or {}
    if isinstance(raw, str):
        import json as _json
        try:
            raw = _json.loads(raw)
        except Exception:
            raw = {}
    return dict(raw or {}) if isinstance(raw, dict) else {}


async def _credit_task_guard(
    cb: CallbackQuery, db: Database, *, allow: tuple[str, ...]
) -> dict[str, Any] | None:
    """Общая проверка: задача есть, назначена этому юзеру, статус ∈ allow."""
    try:
        task_id = int(cb.data.split(":", 1)[1])  # type: ignore[union-attr]
    except (ValueError, IndexError, AttributeError):
        await cb.answer("Некорректный task_id", show_alert=True)
        return None
    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена", show_alert=True)
        return None
    if int(task.get("assigned_to") or 0) != cb.from_user.id:
        await cb.answer("Эта задача не для вас", show_alert=True)
        return None
    if task.get("status") not in allow:
        await cb.answer("Задача уже обработана", show_alert=True)
        return None
    return task


@router.callback_query(F.data.startswith("credit_recv:"))
async def credit_task_received(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """§C шаг 1: read-ack получения задачи (open→in_progress), показать действия."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        return
    tid = int(task["id"])
    if task.get("status") == TaskStatus.OPEN:
        updated = await db.update_task_status(
            tid, TaskStatus.IN_PROGRESS, expected_statuses=(TaskStatus.OPEN,)
        )
        if updated is None:
            await cb.answer("Уже в работе", show_alert=True)
    p = _parse_task_payload(task)
    amount = float(p.get("amount") or 0)
    wlabel = credit_wallet_label(p.get("wallet_role") or "")
    b = InlineKeyboardBuilder()
    b.button(text="✅ Исполнено", callback_data=f"credit_exec:{tid}")
    b.button(text="❌ Отклонить", callback_data=f"credit_rej:{tid}")
    b.adjust(1)
    await cb.answer("Получено")
    try:
        await cb.message.edit_text(  # type: ignore[union-attr]
            "📥 <b>Задача принята в работу</b>\n\n"
            f"  Кошелёк: <b>{wlabel}</b>\n"
            f"  Сумма: <b>{amount:,.0f} ₽</b>\n"
            f"  Назначение: {p.get('purpose') or '—'}\n\n"
            "Когда проведёте оплату — «✅ Исполнено» (можно приложить платёжку) "
            "или «❌ Отклонить».",
            reply_markup=b.as_markup(),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("credit_exec:"))
async def credit_task_execute_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """§C шаг 2: старт исполнения → запрос платёжки (опц.)."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        return
    tid = int(task["id"])
    await state.clear()
    await state.set_state(CreditPaymentExecuteSG.waiting)
    await state.update_data(credit_task_id=tid)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Исполнить без платёжки", callback_data=f"credit_exec_skip:{tid}")
    b.adjust(1)
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "📎 Пришлите <b>платёжку</b> (фото/PDF) в ответ — задача закроется и расход спишется.\n"
        "Либо кнопкой ниже — исполнить без платёжки. Отмена: <code>/cancel</code>",
        reply_markup=b.as_markup(),
    )


@router.message(
    F.text.casefold().in_({"/cancel", "❌ отмена", "отмена", "cancel"}),
    CreditPaymentExecuteSG.waiting,
)
async def credit_task_execute_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Отменено. Задача осталась в работе — исполните позже.")


@router.callback_query(F.data.startswith("credit_exec_skip:"), CreditPaymentExecuteSG.waiting)
async def credit_task_execute_skip(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    data = await state.get_data()
    tid = int(data.get("credit_task_id") or 0)
    await state.clear()
    await cb.answer()
    if not tid:
        await cb.message.answer("⚠️ task_id потерян. Откройте задачу заново.")  # type: ignore[union-attr]
        return
    await _finalize_credit_execution(
        tid, None, cb.from_user.id, cb.message, db, config, notifier, integrations
    )


@router.callback_query(F.data.startswith("credit_exec_send:"), CreditPaymentExecuteSG.waiting)
async def credit_task_execute_send(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Орфан-кейс: менеджер прислал платёжку ДО клика «✅ Исполнено» — файл сохранён
    в state orphan-catcher'ом (tasks.invoice_pp_orphan_catch). Здесь по кнопке
    финализируем исполнение с этим файлом (запись расхода + close + рассылка)."""
    data = await state.get_data()
    tid = int(data.get("credit_task_id") or 0)
    file_info = data.get("credit_exec_file")
    await state.clear()
    await cb.answer()
    if not tid:
        await cb.message.answer("⚠️ task_id потерян. Откройте задачу заново.")  # type: ignore[union-attr]
        return
    await _finalize_credit_execution(
        tid, file_info, cb.from_user.id, cb.message, db, config, notifier, integrations
    )


@router.callback_query(F.data.startswith("credit_exec_acancel:"), CreditPaymentExecuteSG.waiting)
async def credit_task_execute_attach_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    """Орфан-кейс: «❌ Отмена» на экране принятой платёжки — расход НЕ пишется,
    задача остаётся в работе, исполнить можно позже из «Счета на оплату»."""
    await state.clear()
    await cb.answer("Отменено")
    _msg = ("Отменено. Платёжка не проведена — откройте задачу позже и "
            "нажмите «✅ Исполнено».")
    try:
        await cb.message.edit_text(_msg)  # type: ignore[union-attr]
    except Exception:
        try:
            await cb.message.answer(_msg)  # type: ignore[union-attr]
        except Exception:
            pass


@router.message(CreditPaymentExecuteSG.waiting)
async def credit_task_execute_receipt(
    message: Message, state: FSMContext, db: Database, config: Config,
    notifier: Notifier, integrations: IntegrationHub, storage: MinioStorage,
) -> None:
    """Менеджер прислал платёжку → исполнение (запись расхода + close + файл)."""
    data = await state.get_data()
    tid = int(data.get("credit_task_id") or 0)
    if not tid:
        await state.clear()
        await message.answer("⚠️ task_id потерян. Откройте задачу заново.")
        return
    file_info = await mirror_attachment(message, storage, prefix=f"credit_exec/task_{tid}")
    if not file_info:
        await message.answer(
            "⚠️ Это не похоже на фото/документ. Пришлите платёжку, "
            "нажмите «Исполнить без платёжки» или /cancel."
        )
        return
    await state.clear()
    await _finalize_credit_execution(
        tid, file_info, message.from_user.id, message, db, config, notifier, integrations
    )


async def _finalize_credit_execution(
    tid: int, file_info: dict[str, Any] | None, actor_id: int, msg: Message,
    db: Database, config: Config, notifier: Notifier, integrations: IntegrationHub,
) -> None:
    """Запись отложенного расхода + закрытие задачи + рассылка платёжки. Идемпотентно.

    Idempotency: атомарный CAS статуса в DONE «забирает» задачу — только один
    обработчик пишет расход. На ошибке записи статус откатывается в in_progress.
    """
    try:
        task = await db.get_task(tid)
    except KeyError:
        await msg.answer("⚠️ Задача не найдена.")
        return
    p = _parse_task_payload(task)

    # Фаза 2: инициатор отменил запрос до исполнения (REJECT уже блокирует CAS
    # ниже, но даём владельцу понятное сообщение вместо «уже обработана»).
    if p.get("cancelled_by_initiator"):
        await msg.answer("ℹ️ Запрос отменён инициатором — исполнение не требуется.")
        return

    won = await db.update_task_status(
        tid, TaskStatus.DONE,
        expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
    )
    if won is None:
        await msg.answer("ℹ️ Задача уже обработана ранее — расход не задвоен.")
        return

    spend_id: int | None = None
    if p.get("applied") is False:
        try:
            res = await apply_credit_wallet_spend(
                db, integrations,
                wallet_role=p.get("wallet_role") or "",
                amount=float(p.get("amount") or 0),
                mode=p.get("mode") or "",
                purpose=p.get("purpose") or "",
                entered_by=int(p.get("initiator_id") or actor_id),
                invoice_id=p.get("invoice_id"),
                cost_type=p.get("cost_type"),
                invoice_number=p.get("invoice_number") or "",
            )
            spend_id = res.get("spend_id")
        except Exception:
            log.warning("credit_exec: apply_credit_wallet_spend failed tid=%s", tid, exc_info=True)
            await db.update_task_status(tid, TaskStatus.IN_PROGRESS)
            await msg.answer(
                "⚠️ Ошибка записи расхода. Статус возвращён «в работе» — повторите «Исполнено»."
            )
            return
        try:
            await db.update_task_payload(
                tid, {"applied": True, "credit_spend_id": spend_id, "executed_by": actor_id}
            )
        except Exception:
            log.debug("credit_exec: update_task_payload failed tid=%s", tid, exc_info=True)

    # Анти-задвоение ЗП монтажника: кредит-кошелёк выплатил ЗП монтажа по счёту →
    # закрыть парную открытую zp_installer + пометить ЗП выплаченной, чтобы ГД не
    # провёл вторую платёжку (cost_type=montazh = ЗП монтажника). Идемпотентно.
    # Фикс owner 25.07: ЗП закрывается только если траты ХВАТАЕТ на причитающееся;
    # меньшая сумма — аванс/частичная выплата (зачёт ВНУТРЬ Согласованного), задача ЗП
    # остаётся открытой на остаток (см. resolve_installer_zp_by_wallet_payment).
    if (p.get("cost_type") == "montazh") and p.get("invoice_id"):
        try:
            _inv_id = int(p["invoice_id"])
            _zp_open = await db.list_open_tasks_by_invoice(_inv_id, "zp_installer")
            if _zp_open:
                _spent = float(p.get("amount") or 0)
                res_zp = await resolve_installer_zp_by_wallet_payment(
                    db, _inv_id, spend_amount=_spent, actor_id=actor_id,
                    spend_note=(
                        f"Частичная выплата ЗП монтаж из кредит-кошелька "
                        f"{credit_wallet_label(p.get('wallet_role') or '')}: "
                        f"{p.get('purpose') or '—'}"
                    ),
                )
                try:
                    await integrations.sync_invoice_row(_inv_id)
                except Exception:
                    log.debug("credit_exec: sync after zp-resolve failed", exc_info=True)
                _inv_num = p.get("invoice_number") or _inv_id
                if not res_zp.get("partial"):
                    _zp_msg = (
                        f"ℹ️ ЗП монтажника по счёту №{_inv_num} выплачена через "
                        "кредит-кошелёк — задача ЗП закрыта автоматически. "
                        "Повторно платёжку отправлять не нужно."
                    )
                elif res_zp.get("reason") is None:
                    _zp_msg = (
                        f"ℹ️ <b>Частичная выплата ЗП монтажа</b> — счёт №{_inv_num}\n"
                        f"Из кредит-кошелька: {fmt_money(_spent)}\n"
                        f"Зачтено в счёт ЗП (аванс монтажника). Задача ЗП "
                        f"<b>остаётся открытой</b>.\n"
                        f"💰 Остаток к выплате: "
                        f"<b>{fmt_money(res_zp.get('remainder') or 0)}</b>"
                    )
                else:
                    _zp_msg = (
                        f"⚠️ <b>Частичная выплата ЗП монтажа</b> — счёт №{_inv_num}\n"
                        f"Из кредит-кошелька: {fmt_money(_spent)}, причитается "
                        f"{fmt_money(res_zp.get('due') or 0)}.\n"
                        f"Задача ЗП <b>НЕ закрыта</b>; зачёт автоматически не проведён "
                        f"(наёмная группа или ошибка записи) — проведите зачёт вручную.\n"
                        f"💰 Остаток к выплате: "
                        f"<b>{fmt_money(res_zp.get('remainder') or 0)}</b>"
                    )
                for _zt in _zp_open:
                    _aid = _zt.get("assigned_to")
                    if _aid:
                        try:
                            await notifier.safe_send(int(_aid), _zp_msg)
                        except Exception:
                            log.debug("credit_exec: notify zp assignee failed", exc_info=True)
                try:
                    await db.audit(
                        actor_id=actor_id, action="installer_zp_resolved_by_wallet",
                        entity="invoice", entity_id=str(_inv_id),
                        payload={
                            "task_ids": res_zp.get("task_ids"),
                            "marked_paid": res_zp.get("marked_paid"),
                            "credit_task_id": tid,
                            "spend_amount": _spent,
                            "partial": bool(res_zp.get("partial")),
                            "offset_applied": res_zp.get("offset_applied"),
                            "due": res_zp.get("due"),
                            "remainder": res_zp.get("remainder"),
                            "reason": res_zp.get("reason"),
                        },
                    )
                except Exception:
                    log.debug("credit_exec: audit zp-resolve failed", exc_info=True)
        except Exception:
            log.warning(
                "credit_exec: installer-zp anti-double hook failed tid=%s", tid, exc_info=True
            )

    # Кредитная ветка того же триггера, что в tasks._invoice_pp_finalize_core:
    # оплачены стекло/доп.материалы по НАЁМНОМУ счёту → задача ГД на ЗП монтаж
    # (owner 06.08). Ставится РЯДОМ с montazh-хуком выше, а не внутри него: тот
    # ловит cost_type='montazh' (выплата самой ЗП), этот — материалы.
    if p.get("cost_type") and p.get("invoice_id"):
        try:
            from .installer_new import on_invoice_cost_recorded
            _naem = await on_invoice_cost_recorded(
                db, config, notifier, integrations,
                invoice_id=int(p["invoice_id"]),
                material_type=str(p.get("cost_type") or ""),
                amount=float(p.get("amount") or 0),
                actor_id=actor_id,
            )
            if _naem.get("created"):
                log.info(
                    "naem_zp: задача ГД открыта по кредит-оплате %s, счёт=%s сумма=%s",
                    p.get("cost_type"), p.get("invoice_number"), _naem.get("amount"),
                )
        except Exception:
            log.warning(
                "naem_zp: авто-задача ЗП не создана (credit tid=%s)", tid, exc_info=True
            )

    try:
        await db.audit(
            actor_id=actor_id, action="credit_payment_executed",
            entity="tasks", entity_id=str(tid),
            payload={"credit_spend_id": spend_id, "had_receipt": bool(file_info)},
        )
    except Exception:
        log.debug("credit_exec: audit failed", exc_info=True)

    wlabel = credit_wallet_label(p.get("wallet_role") or "")
    amount = float(p.get("amount") or 0)

    # Имена инициатора/исполнителя берём из БД (а не live-имя Telegram),
    # номер материнского счёта — из payload задачи.
    async def _full_name(uid: Any, fallback: str = "—") -> str:
        try:
            u = await db.get_user_optional(int(uid)) if uid else None
        except Exception:
            u = None
        return u.full_name if (u and u.full_name) else fallback

    initiator_name = await _full_name(p.get("initiator_id") or task.get("created_by"))
    _exec_fallback = msg.from_user.full_name if msg.from_user else "—"
    executor_name = await _full_name(actor_id, _exec_fallback)
    inv_no = str(p.get("invoice_number") or "").strip()
    inv_line = f"№{inv_no}" if inv_no else "—"

    caption = format_card_section(
        "✅", f"Кредит-расход исполнен — {wlabel}",
        [
            ("Счёт", _html.escape(inv_line)),
            ("Назначение", _html.escape(p.get("purpose") or "—")),
            ("Инициатор", _html.escape(initiator_name)),
            ("Исполнил", _html.escape(executor_name)),
        ],
        total=fmt_money(amount), width=38, compact=True,
    )
    recipients: list[int] = []
    cb_id = task.get("created_by")
    if cb_id:
        recipients.append(int(cb_id))
    # Исполнитель получает ту же карточку, что инициатор. ГД сюда БОЛЬШЕ НЕ
    # добавляется принудительно (owner 24.08): он уже видел эту трату карточкой
    # "Расход кредита" при СОЗДАНИИ запроса (manager_new._notify_gd_rp), и вторая
    # карточка с тем же Назначением/Суммой была для него дублем. Если ГД сам
    # инициатор или исполнитель — он остаётся в списке как created_by/actor_id.
    if actor_id and int(actor_id) not in recipients:
        recipients.append(int(actor_id))
    ft = (file_info or {}).get("file_type")
    fid = (file_info or {}).get("file_id")
    for rcpt in recipients:
        if file_info and ft == "document" and fid:
            await notifier.safe_send_document(rcpt, fid, caption=caption)
        elif file_info and ft == "photo" and fid:
            await notifier.safe_send_photo(rcpt, fid, caption=caption)
        else:
            await notifier.safe_send(rcpt, caption)

    await msg.answer("✅ Исполнено. Расход записан, задача закрыта.")


@router.callback_query(F.data.startswith("credit_rej:"))
async def credit_task_reject_start(cb: CallbackQuery, state: FSMContext, db: Database) -> None:
    """§C: отклонение задачи → запрос причины."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        return
    tid = int(task["id"])
    await state.clear()
    await state.set_state(CreditTaskRejectSG.reason)
    await state.update_data(credit_task_id=tid)
    await cb.answer()
    await cb.message.answer(  # type: ignore[union-attr]
        "✏️ Укажите <b>причину отклонения</b> (текст). Отмена: <code>/cancel</code>"
    )


@router.message(
    F.text.casefold().in_({"/cancel", "❌ отмена", "отмена", "cancel"}),
    CreditTaskRejectSG.reason,
)
async def credit_task_reject_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("❌ Отмена. Задача осталась в работе.")


@router.message(CreditTaskRejectSG.reason)
async def credit_task_reject_reason(
    message: Message, state: FSMContext, db: Database, config: Config, notifier: Notifier,
) -> None:
    from ..services.assignment import resolve_default_assignee

    data = await state.get_data()
    tid = int(data.get("credit_task_id") or 0)
    reason = (message.text or "").strip()
    if not tid:
        await state.clear()
        await message.answer("⚠️ task_id потерян.")
        return
    if len(reason) < 3:
        await message.answer("⚠️ Причина слишком короткая. Опишите подробнее или /cancel.")
        return
    try:
        task = await db.get_task(tid)
    except KeyError:
        await state.clear()
        await message.answer("⚠️ Задача не найдена.")
        return
    won = await db.update_task_status(
        tid, TaskStatus.REJECTED,
        expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
    )
    await state.clear()
    if won is None:
        await message.answer("ℹ️ Задача уже обработана — отклонение не применено.")
        return
    p = _parse_task_payload(task)
    try:
        await db.audit(
            actor_id=message.from_user.id if message.from_user else None,
            action="credit_payment_rejected", entity="tasks", entity_id=str(tid),
            payload={"reason": reason},
        )
    except Exception:
        log.debug("credit_rej: audit failed", exc_info=True)
    wlabel = credit_wallet_label(p.get("wallet_role") or "")
    amount = float(p.get("amount") or 0)
    note = format_card_section(
        "❌", f"Кредит-расход отклонён — {wlabel}",
        [
            ("Назначение", _html.escape(p.get("purpose") or "—")),
            ("Причина", _html.escape(reason)),
        ],
        total=fmt_money(amount), width=38, compact=True,
    )
    recipients: list[int] = []
    cb_id = task.get("created_by")
    if cb_id:
        recipients.append(int(cb_id))
    try:
        gd_default = await resolve_default_assignee(db, config, Role.GD)
        if gd_default and int(gd_default) not in recipients:
            recipients.append(int(gd_default))
    except Exception:
        log.debug("credit_rej: resolve GD failed", exc_info=True)
    for rcpt in recipients:
        await notifier.safe_send(rcpt, note)
    await message.answer("❌ Задача отклонена. Инициатор и ГД уведомлены.")


# ---------------------------------------------------------------------------
# п.5 (TZ 12.06): гейт подтверждения ГД на СВОЮ трату кошелька хозяином.
#   Менеджер-владелец инициирует трату → задача kind=credit_spend_gd_confirm
#   (assigned_to=ГД, applied=False) → ГД жмёт ✅ cw_gd_ok (запись расхода) или
#   ❌ cw_gd_no (отмена). Запись ОТЛОЖЕНА до ✅ГД — тот же механизм, что §C, но
#   подтверждает ГД. Идемпотентность: атомарный CAS статуса
#   OPEN/IN_PROGRESS→DONE/REJECTED «забирает» задачу; повторный клик — no-op.
#   _CW_GD_INFLIGHT — анти-двойной-клик ГД до завершения апплая.
# ---------------------------------------------------------------------------
_CW_GD_INFLIGHT: set[tuple[int, int]] = set()


@router.callback_query(F.data.startswith("cw_gd_ok:"))
async def credit_spend_gd_confirm(
    cb: CallbackQuery, state: FSMContext, db: Database,
) -> None:
    """ГД жмёт «✅ Подтвердить» → экран вложения (документ опционален), затем
    cw_gd_send / cw_gd_skip фиксируют расход. Запись расхода НЕ здесь — она в
    _credit_spend_finalize после экрана (owner 27.06: ГД может приложить документ
    к подтверждению своей траты кредит-кошелька). Прежде запись шла сразу по ✅."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        return
    tid = int(task["id"])
    await cb.answer()
    p = _parse_task_payload(task)
    wlabel = credit_wallet_label(p.get("wallet_role") or "")
    amount = float(p.get("amount") or 0)
    # Переиспользуем generic-сборщик файлов InvoicePaymentSG.attaching_pp
    # (tasks.invoice_pp_collect): он лишь копит pp_files/pp_comment в state, без
    # supplier-логики. Финализируем своими cw_gd_send/cw_gd_skip (кредит-логика).
    await state.clear()
    await state.set_state(InvoicePaymentSG.attaching_pp)
    await state.update_data(cw_tid=tid)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    b = InlineKeyboardBuilder()
    b.button(text="✅ Подтвердить", callback_data=f"cw_gd_send:{tid}")
    b.button(text="✅ Без вложения", callback_data=f"cw_gd_skip:{tid}")
    b.button(text="❌ Отмена", callback_data=f"cw_gd_acancel:{tid}")
    b.adjust(1)
    _txt = format_card_section(
        "💳", f"Подтверждение расхода — {wlabel}",
        [("Назначение", _html.escape(p.get("purpose") or "—"))],
        total=fmt_money(amount), width=38, compact=True,
    )
    _txt += (
        "\n\nПрикрепите документ (PDF/фото) и/или напишите комментарий, "
        "затем «✅ Подтвердить».\nЕсли документа нет — «✅ Без вложения»."
    )
    await cb.message.answer(_txt, reply_markup=b.as_markup())  # type: ignore[union-attr]


async def _credit_spend_finalize(
    cb: CallbackQuery, db: Database, notifier: Notifier, integrations: IntegrationHub,
    task: dict[str, Any], pp_files: list[dict[str, Any]], pp_comment: str,
    config: Config | None = None,
) -> None:
    """Зафиксировать трату кредит-кошелька (apply_credit_wallet_spend) + сохранить
    вложения ГД + закрыть задачу. Идемпотентность: _CW_GD_INFLIGHT + CAS статуса
    OPEN/IN_PROGRESS→DONE. Логика записи 1:1 с прежним cw_gd_ok."""
    tid = int(task["id"])
    key = (cb.from_user.id, tid)
    if key in _CW_GD_INFLIGHT:
        await cb.answer("Уже обрабатываю, секунду…")
        return
    _CW_GD_INFLIGHT.add(key)
    try:
        won = await db.update_task_status(
            tid, TaskStatus.DONE,
            expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
        )
        if won is None:
            await cb.answer("Задача уже обработана", show_alert=True)
            return
        p = _parse_task_payload(task)
        spend_id: int | None = None
        if p.get("applied") is False:
            try:
                res = await apply_credit_wallet_spend(
                    db, integrations,
                    wallet_role=p.get("wallet_role") or "",
                    amount=float(p.get("amount") or 0),
                    mode=p.get("mode") or "",
                    purpose=p.get("purpose") or "",
                    entered_by=int(p.get("initiator_id") or cb.from_user.id),
                    invoice_id=p.get("invoice_id"),
                    cost_type=p.get("cost_type"),
                    invoice_number=p.get("invoice_number") or "",
                )
                spend_id = res.get("spend_id")
            except Exception:
                log.warning("cw_gd finalize: apply_credit_wallet_spend failed tid=%s", tid, exc_info=True)
                await db.update_task_status(tid, TaskStatus.IN_PROGRESS)
                await cb.answer(
                    "⚠️ Ошибка записи. Статус «в работе» — повторите подтверждение.",
                    show_alert=True,
                )
                return
            try:
                _upd: dict[str, Any] = {"applied": True, "credit_spend_id": spend_id,
                                        "confirmed_by": cb.from_user.id}
                if pp_comment:
                    _upd["gd_pp_comment"] = pp_comment
                await db.update_task_payload(tid, _upd)
            except Exception:
                log.debug("cw_gd finalize: update_task_payload failed tid=%s", tid, exc_info=True)
        # Сохранить вложения ГД в задачу
        for a in (pp_files or []):
            try:
                await db.add_attachment(
                    task_id=tid,
                    file_id=a["file_id"],
                    file_unique_id=a.get("file_unique_id"),
                    file_type=a["file_type"],
                    caption=a.get("caption"),
                    minio_object_key=a.get("minio_object_key"),
                )
            except Exception:
                log.debug("cw_gd finalize: add_attachment failed tid=%s", tid, exc_info=True)
        try:
            await db.audit(
                actor_id=cb.from_user.id, action="credit_spend_gd_confirmed",
                entity="tasks", entity_id=str(tid),
                payload={"credit_spend_id": spend_id,
                         "has_attach": bool(pp_files),
                         "has_comment": bool(pp_comment)},
            )
        except Exception:
            log.debug("cw_gd finalize: audit failed", exc_info=True)
        wlabel = credit_wallet_label(p.get("wallet_role") or "")
        amount = float(p.get("amount") or 0)
        _items = [
            ("Назначение", _html.escape(p.get("purpose") or "—")),
            ("№ расхода", f"#{spend_id}" if spend_id else "—"),
        ]
        if pp_files:
            _items.append(("Вложение", f"{len(pp_files)} файл(ов)"))
        _done = format_card_section(
            "✅", f"Расход подтверждён — {wlabel}", _items,
            total=fmt_money(amount), width=38, compact=True,
        )
        try:
            await cb.message.edit_text(_done)  # type: ignore[union-attr]
        except Exception:
            try:
                await cb.message.answer(_done)  # type: ignore[union-attr]
            except Exception:
                pass
        await cb.answer("Подтверждено")
        init_id = p.get("initiator_id")
        if init_id and int(init_id) != cb.from_user.id:
            note = format_card_section(
                "✅", f"ГД подтвердил расход — {wlabel}",
                [
                    ("Назначение", _html.escape(p.get("purpose") or "—")),
                    ("Статус", "записано"),
                ],
                total=fmt_money(amount), width=38, compact=True,
            )
            try:
                await notifier.safe_send(int(init_id), note)
            except Exception:
                log.debug("cw_gd finalize: notify initiator failed", exc_info=True)

        # Тот же триггер наёмной ЗП, что в _finalize_credit_execution: это ВТОРОЙ
        # финализатор кредит-расхода (сюда приходят «💳 Расход кошелька» менеджера/
        # РП после ✅ГД), и без хука здесь два пути ввода затрат из пяти остались бы
        # без задачи. config приходит из DI вызывающих хендлеров; None — только у
        # гипотетического старого вызова, тогда просто пропускаем.
        if config is not None and p.get("cost_type") and p.get("invoice_id"):
            try:
                from .installer_new import on_invoice_cost_recorded
                _naem = await on_invoice_cost_recorded(
                    db, config, notifier, integrations,
                    invoice_id=int(p["invoice_id"]),
                    material_type=str(p.get("cost_type") or ""),
                    amount=amount,
                    actor_id=cb.from_user.id,
                )
                if _naem.get("created"):
                    log.info(
                        "naem_zp: задача ГД открыта по расходу кошелька %s, счёт=%s",
                        p.get("cost_type"), p.get("invoice_number"),
                    )
            except Exception:
                log.warning(
                    "naem_zp: авто-задача ЗП не создана (cw_gd tid=%s)", tid, exc_info=True
                )
    finally:
        _CW_GD_INFLIGHT.discard(key)


@router.callback_query(F.data.startswith("cw_gd_send:"))
async def credit_spend_gd_send(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    """ГД: «✅ Подтвердить» — зафиксировать расход с приложенными документами."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        await state.clear()
        return
    data = await state.get_data()
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "")
    await state.clear()
    await _credit_spend_finalize(
        cb, db, notifier, integrations, task, pp_files, pp_comment, config,
    )


@router.callback_query(F.data.startswith("cw_gd_skip:"))
async def credit_spend_gd_skip(
    cb: CallbackQuery, state: FSMContext, db: Database, notifier: Notifier,
    integrations: IntegrationHub, config: Config,
) -> None:
    """ГД: «✅ Без вложения» — зафиксировать расход (документ опционален; уже
    приложенные файлы, если есть, всё равно сохраняем — не теряем)."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        await state.clear()
        return
    data = await state.get_data()
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "")
    await state.clear()
    await _credit_spend_finalize(
        cb, db, notifier, integrations, task, pp_files, pp_comment, config,
    )


@router.callback_query(F.data.startswith("cw_gd_acancel:"))
async def credit_spend_gd_attach_cancel(
    cb: CallbackQuery, state: FSMContext,
) -> None:
    """ГД: «❌ Отмена» на экране вложения — расход НЕ фиксируется, задача жива."""
    await state.clear()
    await cb.answer("Отменено")
    _msg = ("Подтверждение отменено. Откройте задачу в «Счета на оплату», "
            "чтобы подтвердить расход.")
    try:
        await cb.message.edit_text(_msg)  # type: ignore[union-attr]
    except Exception:
        try:
            await cb.message.answer(_msg)  # type: ignore[union-attr]
        except Exception:
            pass


@router.callback_query(F.data.startswith("cw_gd_no:"))
async def credit_spend_gd_reject(
    cb: CallbackQuery, db: Database, notifier: Notifier,
) -> None:
    """ГД отклонил свою трату хозяина — расход НЕ пишется, инициатор уведомлён."""
    task = await _credit_task_guard(cb, db, allow=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS))
    if not task:
        return
    tid = int(task["id"])
    key = (cb.from_user.id, tid)
    if key in _CW_GD_INFLIGHT:
        await cb.answer("Уже обрабатываю…")
        return
    _CW_GD_INFLIGHT.add(key)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    try:
        won = await db.update_task_status(
            tid, TaskStatus.REJECTED,
            expected_statuses=(TaskStatus.OPEN, TaskStatus.IN_PROGRESS),
        )
        if won is None:
            await cb.answer("Задача уже обработана", show_alert=True)
            return
        p = _parse_task_payload(task)
        try:
            await db.audit(
                actor_id=cb.from_user.id, action="credit_spend_gd_rejected",
                entity="tasks", entity_id=str(tid), payload={},
            )
        except Exception:
            log.debug("cw_gd_no: audit failed", exc_info=True)
        wlabel = credit_wallet_label(p.get("wallet_role") or "")
        amount = float(p.get("amount") or 0)
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                format_card_section(
                    "❌", f"Расход отклонён — {wlabel}",
                    [("Назначение", _html.escape(p.get("purpose") or "—"))],
                    total=fmt_money(amount), width=38, compact=True,
                ),
            )
        except Exception:
            pass
        await cb.answer("Отклонено")
        init_id = p.get("initiator_id")
        if init_id and int(init_id) != cb.from_user.id:
            note = format_card_section(
                "❌", f"ГД отклонил расход — {wlabel}",
                [
                    ("Назначение", _html.escape(p.get("purpose") or "—")),
                    ("Статус", "не записано"),
                ],
                total=fmt_money(amount), width=38, compact=True,
            )
            try:
                await notifier.safe_send(int(init_id), note)
            except Exception:
                log.debug("cw_gd_no: notify initiator failed", exc_info=True)
    finally:
        _CW_GD_INFLIGHT.discard(key)


@router.message(ChatProxySG.menu, F.text == "📊 Отчёт")
async def chat_menu_report(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    """Баланс кредитных счетов менеджера (КВ/КИА/НПН).

    Channel manager_kv/kia/npn → суммирует CV/CX/DA по всем is_credit=1
    счетам данного менеджера (зеркалит лист Invoices CV/CX/DA).
    """
    data = await state.get_data()
    channel = data.get("channel", "")

    if channel not in FINANCE_CHANNELS:
        await message.answer("Отчётность доступна только для КВ/КИА каналов.")
        return

    label = channel_label(channel)
    summary = await db.get_credit_balance_summary(channel)
    invoices = summary["invoices"]
    total_da = summary["total_da"]

    def _rub(v: float) -> str:
        s = f"{abs(float(v or 0)):,.0f}".replace(",", " ")
        sign = "−" if float(v or 0) < 0 else ""
        return f"{sign}{s}₽"

    if not invoices:
        card = format_card_section(
            emoji="📊", title=f"Баланс кредитных счетов — {label}",
            items=[("Кредитных счетов пока нет", "")], width=36,
        )
        await message.answer(card, reply_markup=gd_channel_menu(channel))
        return

    # Эталон-карточка: открытый счёт — подзаголовок 🟢 + Оплачено/Израсходовано/
    # Остаток (отступ); закрытый — строка ✅ … закрыт; footer = Текущий баланс (DA).
    items: list[tuple[str, str]] = []
    for inv in invoices:
        num = str(inv["invoice_number"])
        if inv["is_closed"]:
            items.append((f"✅ {num}", "закрыт"))
        else:
            items.append((f"🟢 {num}", ""))
            items.append(("   Оплачено", _rub(inv["cv"])))
            items.append(("   Израсходовано", _rub(inv["cx"])))
            items.append(("   Остаток", _rub(inv["da"])))
    card = format_card_section(
        emoji="📊", title=f"Баланс кредитных счетов — {label}",
        items=items, footer=("Текущий баланс (DA)", _rub(total_da)),
        width=36,
    )
    await message.answer(card, reply_markup=gd_channel_menu(channel))


@router.message(ChatProxySG.menu, F.text == "⬅️ Назад")
async def chat_menu_back(
    message: Message, state: FSMContext, db: Database, config: Config
) -> None:
    """Return from chat submenu to main menu."""
    await state.clear()
    u = message.from_user
    if not u:
        return
    user = await db.get_user_optional(u.id)
    role, isolated_role = resolve_menu_scope(u.id, user.role if user else None)
    is_admin = u.id in (config.admin_ids or set())
    unread = await db.count_unread_tasks(u.id)
    uc = await db.count_unread_by_channel(u.id)
    from ..enums import Role as _Role
    from ..utils import parse_roles as _parse_roles
    _parsed_cp = _parse_roles(role) if role else []
    gd_ur = await db.count_gd_inbox_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    gd_inv = await db.count_gd_invoice_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    gd_ie = await db.count_gd_invoice_end_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    gd_tot = await db.count_gd_more_total_open_tasks(u.id) if role and _Role.GD in _parsed_cp else None
    _is_rp_cp = _Role.RP in _parsed_cp or _Role.MANAGER_NPN in _parsed_cp
    rp_t_cp = await db.count_rp_role_tasks(u.id) if _is_rp_cp else 0
    rp_m_cp = await db.count_rp_role_messages(u.id) if _is_rp_cp else 0
    await message.answer(
        "Главное меню.",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role,
                is_admin=is_admin,
                unread=unread,
                unread_channels=uc,
                gd_inbox_unread=gd_ur,
                gd_invoice_unread=gd_inv,
                gd_invoice_end_unread=gd_ie,
                gd_total_open_tasks=gd_tot,
                isolated_role=isolated_role,
                rp_tasks=rp_t_cp,
                rp_messages=rp_m_cp,
            ),
        ),
    )




# ---------------------------------------------------------------------------
# GD Task creation from chat-proxy
# ---------------------------------------------------------------------------

_GDTASK_INV_PREFIX = "gdtask_inv"


_DEFAULT_DUE_DAYS = 7
_DEFAULT_DUE_HOUR = 18


def _default_task_due(config: Config):
    """Срок по умолчанию: +7 дней, 18:00 по TZ бота (конец рабочего дня)."""
    from datetime import timedelta
    from ..utils import tzinfo as _tzinfo
    d = (utcnow() + timedelta(days=_DEFAULT_DUE_DAYS)).astimezone(_tzinfo(config.timezone))
    return d.replace(hour=_DEFAULT_DUE_HOUR, minute=0, second=0, microsecond=0)


async def _show_task_confirm(
    target: Message,
    state: FSMContext,
    config: Config,
    label: str,
    inv_label: str = "",
    note: str = "",
) -> None:
    """Единственный экран подтверждения задачи (owner 30.07).

    Раньше после описания шли ТРИ последовательных вопроса: дата → время →
    экран вложений, и только потом задача создавалась. Теперь всё видно сразу
    одним экраном: кому, по какому счёту, текст, срок (по умолчанию — неделя).
    «✅ Создать» — один тап. Срок меняется кнопкой, файл просто присылается
    сообщением: состояние GdTaskCreateSG.attachments его и принимает.
    """
    from ..utils import format_dt_iso

    data = await state.get_data()
    desc = (data.get("task_description") or "").strip()
    preview = desc if len(desc) <= 300 else desc[:300] + "…"

    due_iso = data.get("task_due")
    if not due_iso:
        due_iso = to_iso(_default_task_due(config))
        await state.update_data(task_due=due_iso)
        due_hint = " <i>(по умолчанию — неделя)</i>"
    else:
        due_hint = ""

    n_att = len(data.get("task_attachments") or [])
    att_line = f"\n📎 Файлов: <b>{n_att}</b>" if n_att else ""

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать задачу", callback_data="gd_task_finalize")
    b.button(text="📅 Указать срок", callback_data="gd_task_setdue")
    b.button(text="❌ Отмена", callback_data="gd_task_cancel")
    b.adjust(1)

    await state.set_state(GdTaskCreateSG.attachments)
    await target.answer(
        f"{note}"
        f"📝 <b>Новая задача → {label}</b>{inv_label}\n\n"
        f"{_html.escape(preview)}\n\n"
        f"⏰ Срок: <b>{format_dt_iso(due_iso, config.timezone)}</b>{due_hint}"
        f"{att_line}\n\n"
        "Можно прислать файл — приложу к задаче. "
        "Время (<b>20:00</b>) — поставлю его в срок.",
        reply_markup=b.as_markup(),
    )


async def _ask_task_description_or_deadline(
    target: Message,
    state: FSMContext,
    label: str,
    inv_label: str = "",
    config: Config | None = None,
) -> None:
    """Следующий шаг после выбора контекста задачи (адресат / счёт / площадь).

    Если описание уже есть — оно пришло текстом прямо в меню канала
    (chat_menu_freetext, owner 25.07) — второй раз его не спрашиваем и сразу
    показываем экран подтверждения. Иначе — единственный вопрос «опишите задачу».
    """
    data = await state.get_data()
    desc = (data.get("task_description") or "").strip()
    if desc and config is not None:
        await _show_task_confirm(target, state, config, label, inv_label)
        return

    await state.set_state(GdTaskCreateSG.description)
    await target.answer(
        f"📝 <b>Новая задача → {label}</b>{inv_label}\n\n"
        "Опишите задачу\n"
        "(«❌ Отмена» — отменить):",
    )


async def _show_task_invoice_picker_or_desc(
    source: CallbackQuery,
    state: FSMContext,
    db: Database,
    label: str,
    config: Config | None = None,
) -> None:
    """Показать invoice picker перед описанием задачи, или пропустить."""
    # ГД видит кредит ([[feedback_credit_filter_accounting_only]]): обычные + кредитные.
    # limit=30 + свежие первыми (db.py) — чтобы активный кредит влезал в выборку.
    invoices = await db.list_invoices_for_selection(limit=30, only_regular=True, include_credit=True)
    msg_target = source.message
    if invoices:
        await state.set_state(GdTaskCreateSG.invoice_pick)
        await msg_target.answer(  # type: ignore[union-attr]
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "По какому счёту задача?\n"
            "Для отмены: «❌ Отмена».",
            reply_markup=invoice_select_kb(invoices, prefix=_GDTASK_INV_PREFIX, back_callback="nav:home"),
        )
    else:
        await state.update_data(linked_invoice_id=None)
        await _ask_task_description_or_deadline(msg_target, state, label, config=config)  # type: ignore[arg-type]


@router.callback_query(F.data.startswith(f"{_GDTASK_INV_PREFIX}:"))
async def gd_task_pick_invoice(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """GD выбрал счёт для привязки к задаче."""
    await cb.answer()
    val = (cb.data or "").split(":", 1)[1]
    linked = None if val == "skip" else int(val)
    await state.update_data(linked_invoice_id=linked)

    data = await state.get_data()
    label = channel_label(data.get("task_channel", ""))
    channel = data.get("task_channel", "")

    inv_label = ""
    if linked:
        inv = await db.get_invoice(linked)
        if inv:
            inv_label = f"\n📋 Счёт: №{inv.get('invoice_number', '?')}"

    # Для montazh + выбран счёт → запросить площадь (м²)
    if channel == "montazh" and linked:
        await state.set_state(GdTaskCreateSG.area_m2)
        await cb.message.answer(  # type: ignore[union-attr]
            f"📝 <b>Новая задача → {label}</b>{inv_label}\n\n"
            "📐 Укажите площадь (м²):\n"
            "(«❌ Отмена» — отменить, «-» — без площади)",
        )
        return

    await _ask_task_description_or_deadline(cb.message, state, label, inv_label, config)  # type: ignore[arg-type]


async def _show_task_target_picker(
    target: Message,
    state: FSMContext,
    db: Database,
    channel: str,
    label: str,
) -> bool:
    """Выбор конкретного адресата там, где канал его не задаёт однозначно.

    montazh → монтажник, otd_prodazh → менеджер КВ/КИА/НПН или «всем».
    True — picker показан (дальше ждём callback), False — адресат однозначен.
    """
    # Для montazh — сначала выбрать конкретного монтажника
    if channel == "montazh":
        installers = await db.find_users_by_role("installer")
        if not installers:
            await target.answer("⚠️ Нет активных монтажников.")
            return True
        b = InlineKeyboardBuilder()
        for inst in installers:
            name = inst.full_name or inst.username or str(inst.telegram_id)
            b.button(text=name, callback_data=f"pick_installer:{inst.telegram_id}")
        b.adjust(1)
        await state.set_state(GdTaskCreateSG.pick_installer)
        await target.answer(
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "👷 Выберите монтажника:",
            reply_markup=b.as_markup(),
        )
        return True

    # Для Отд.Продаж — выбрать конкретного менеджера (КВ/КИА/НПН) или всем.
    # РП в задачном потоке не участвует (user 2026-06-14: «только КВ/КИА/НПН + всем»).
    if channel == "otd_prodazh":
        b = InlineKeyboardBuilder()
        b.button(text="Менеджер КВ", callback_data="gd_task_mgr:manager_kv")
        b.button(text="Менеджер КИА", callback_data="gd_task_mgr:manager_kia")
        b.button(text="Менеджер НПН", callback_data="gd_task_mgr:manager_npn")
        b.button(text="Всем (КВ+КИА+НПН)", callback_data="gd_task_mgr:all")
        b.button(text="❌ Отмена", callback_data="gd_task_cancel")
        b.adjust(1)
        await state.set_state(GdTaskCreateSG.pick_manager)
        await target.answer(
            f"📝 <b>Новая задача → {label}</b>\n\n"
            "👤 Кому поставить задачу?",
            reply_markup=b.as_markup(),
        )
        return True

    return False


@router.callback_query(F.data.startswith("gd_task_create:"))
async def gd_task_create_start(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """GD starts creating a task for channel target.

    Контекст чата НАСЛЕДУЕТСЯ (owner 30.07): счёт ГД уже выбрал кнопкой при входе
    в чат («📄 №…» либо «📝 Без привязки»), поэтому второй раз тот же список
    счетов не показываем — раньше здесь стоял state.clear() и вопрос повторялся.
    Исключение — montazh «без привязки»: там счёт нужен, чтобы привязать
    монтажника и площадь, поэтому спросим.
    """
    await cb.answer()
    channel = cb.data.split(":", 1)[1]  # type: ignore[union-attr]

    # Backstop: пустой канал в callback_data = кнопка отрисована без контекста
    # чата. Раньше он молча становился task_channel="" — пустая строка ЛОЖНА,
    # поэтому дальше поток разваливался в двух местах сразу: «Черновик задачи
    # потерян» на кнопке срока и «Адресат для  не настроен» на создании (двойной
    # пробел — это и есть пустой channel_label). Лучше сказать прямо и не пускать.
    if not channel:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Кнопка потеряла канал — откройте чат нужного канала заново "
            "и нажмите «➕ Создать задачу» там.",
        )
        return

    prev = await state.get_data()
    inv_ctx = bool(prev.get("invoice_ctx_set"))
    inherited_inv = prev.get("linked_invoice_id")
    await state.set_data({
        "task_channel": channel,
        "task_attachments": [],
        "linked_invoice_id": inherited_inv,
        "invoice_ctx_set": inv_ctx,
    })

    label = channel_label(channel)

    if await _show_task_target_picker(cb.message, state, db, channel, label):  # type: ignore[arg-type]
        return

    if _can_skip_invoice_picker(inv_ctx, inherited_inv, channel):
        inv_label = await _invoice_label(db, inherited_inv)
        await _ask_task_description_or_deadline(cb.message, state, label, inv_label, config)  # type: ignore[arg-type]
        return

    # Контекста счёта нет (в чат вошли не через выбор счёта) — спросить.
    await _show_task_invoice_picker_or_desc(cb, state, db, label, config)


def _can_skip_invoice_picker(inv_ctx: bool, inherited_inv: Any, channel: str) -> bool:
    """Спрашивать ли счёт повторно после входа в чат.

    Контекст задан и счёт выбран → не спрашиваем никогда. Контекст задан, но
    «без привязки» → для montazh всё же спросим (счёт нужен для привязки
    монтажника и площади), для остальных каналов уважаем выбор ГД.
    """
    if not inv_ctx:
        return False
    if inherited_inv is not None:
        return True
    return channel != "montazh"


async def _invoice_label(db: Database, invoice_id: Any) -> str:
    """Строка «📋 Счёт: №…» для шапки шагов задачи ('' если счёта нет)."""
    if not invoice_id:
        return ""
    inv = await db.get_invoice(int(invoice_id))
    if not inv:
        return ""
    return f"\n📋 Счёт: №{inv.get('invoice_number', '?')}"


@router.callback_query(F.data.startswith("pick_installer:"), GdTaskCreateSG.pick_installer)
async def gd_task_pick_installer(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """GD picks a specific installer for montazh task."""
    await cb.answer()
    installer_id = int(cb.data.split(":", 1)[1])  # type: ignore[union-attr]
    await state.update_data(montazh_target_id=installer_id)

    data = await state.get_data()
    label = channel_label(data.get("task_channel", "montazh"))
    await _show_task_invoice_picker_or_desc(cb, state, db, label, config)


@router.callback_query(F.data.startswith("gd_task_mgr:"), GdTaskCreateSG.pick_manager)
async def gd_task_pick_manager(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """ГД выбрал конкретного менеджера (КВ/КИА/НПН) или «всем» для задачи Отд.Продаж."""
    await cb.answer()
    sel = (cb.data or "").split(":", 1)[1]  # manager_kv | manager_kia | manager_npn | all
    await state.update_data(sales_task_target=sel)

    data = await state.get_data()
    channel = data.get("task_channel", "otd_prodazh")
    label = channel_label(channel)
    # Счёт, выбранный в чате, наследуется и здесь — второй раз не спрашиваем.
    if _can_skip_invoice_picker(
        bool(data.get("invoice_ctx_set")), data.get("linked_invoice_id"), channel,
    ):
        inv_label = await _invoice_label(db, data.get("linked_invoice_id"))
        await _ask_task_description_or_deadline(cb.message, state, label, inv_label, config)  # type: ignore[arg-type]
        return
    await _show_task_invoice_picker_or_desc(cb, state, db, label, config)


@router.message(GdTaskCreateSG.area_m2)
async def gd_task_area_m2(message: Message, state: FSMContext, config: Config) -> None:
    """Ввод площади м² для монтажной задачи."""
    text = (message.text or "").strip()
    if text == "-":
        area = None
    else:
        text = text.replace(",", ".").replace("м2", "").replace("m2", "").strip()
        try:
            area = float(text)
            if area <= 0:
                raise ValueError
        except (ValueError, TypeError):
            await message.answer(
                "Введите число (площадь в м²), например <b>45.5</b>\n"
                "или «-» без площади, «❌ Отмена» для отмены:"
            )
            return
    await state.update_data(task_area_m2=area)

    data = await state.get_data()
    label = channel_label(data.get("task_channel", ""))
    await _ask_task_description_or_deadline(message, state, label, config=config)


# --- Cancel task creation at any step ---
_CANCEL_TEXTS = {"❌ отмена", "отмена", "cancel", "/cancel", "❌отмена"}


@router.message(GdTaskCreateSG.pick_installer, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.pick_manager, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.invoice_pick, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.area_m2, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.description, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.deadline, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.deadline_time, F.text.casefold().in_(_CANCEL_TEXTS))
@router.message(GdTaskCreateSG.attachments, F.text.casefold().in_(_CANCEL_TEXTS))
async def gd_task_create_cancel(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Cancel task creation and return to chat submenu."""
    data = await state.get_data()
    channel = data.get("task_channel", "")
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await message.answer("❌ Создание задачи отменено.", reply_markup=gd_channel_menu(channel))


@router.message(GdTaskCreateSG.description)
async def gd_task_create_desc(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите задачу подробнее (минимум 3 символа):")
        return
    await state.update_data(task_description=text)

    # Дальше — сразу экран подтверждения (owner 30.07). Раньше здесь начинались
    # три отдельных вопроса: дата → время → вложения.
    data = await state.get_data()
    label = channel_label(data.get("task_channel", ""))
    inv_label = await _invoice_label(db, data.get("linked_invoice_id"))
    await _show_task_confirm(message, state, config, label, inv_label)


@router.callback_query(F.data == "gd_task_setdue")
async def gd_task_ask_due(cb: CallbackQuery, state: FSMContext) -> None:
    """«📅 Указать срок» с экрана подтверждения — дата и время ОДНОЙ строкой.

    Без StateFilter намеренно: кнопка живёт в сообщении, и фильтр состояния
    молча съел бы нажатие после любого сбоя контекста ([[feedback_fsm_old_buttons_trap]]).
    """
    await cb.answer()
    data = await state.get_data()
    if not data.get("task_channel"):
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Черновик задачи потерян — начните заново кнопкой «➕ Создать задачу».",
        )
        return
    await state.set_state(GdTaskCreateSG.deadline)
    await cb.message.answer(  # type: ignore[union-attr]
        "📅 Укажите срок одной строкой:\n"
        "<b>07 марта 14:00</b> · <b>15.03.2026</b> · <b>07 марта</b>\n\n"
        "Без времени — 18:00. «-» — срок по умолчанию (неделя).",
    )


_DUE_TIME_RE = re.compile(r"(?:^|\s)(\d{1,2})[:.](\d{2})\s*$")
_DUE_HOUR_ONLY_RE = re.compile(r"\s(\d{1,2})\s*$")
_BARE_TIME_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?")


def _parse_bare_time(text: str) -> tuple[int, int] | None:
    """«20:00» / «20» → (час, минута); иначе None.

    Нужно там, где дата УЖЕ выбрана и человек досылает одно время отдельным
    сообщением (owner 03.08). Голое число трактуем как час: на экране срока и
    подтверждения другого смысла у одинокого числа нет, а результат сразу видно
    в перерисованной карточке.

    ⚠️ Точка как разделитель НЕ принимается намеренно: «15.03» — это 15 марта,
    и на шаге срока эта ветка стоит ПЕРЕД парсером даты. Разреши точку — дата
    молча стала бы временем 15:03.
    """
    m = _BARE_TIME_RE.fullmatch((text or "").strip())
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2) or 0)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _apply_time_to_due(due_iso: str, hour: int, minute: int, tz_name: str) -> str:
    """Подставить час/минуту в уже выбранный срок, дату не трогая."""
    from ..utils import from_iso, tzinfo as _tz

    due_dt = from_iso(due_iso).astimezone(_tz(tz_name))
    return to_iso(due_dt.replace(hour=hour, minute=minute, second=0, microsecond=0))


@router.message(GdTaskCreateSG.deadline)
async def gd_task_create_deadline(
    message: Message, state: FSMContext, db: Database, config: Config,
) -> None:
    """Срок задачи одной строкой: дата + (необязательно) время.

    Было двумя вопросами подряд — дата, потом время (owner 30.07: лишний шаг).
    Время отделяем с конца строки, остаток парсим как дату; нет времени — 18:00.
    """
    from ..utils import parse_date, tzinfo as _tzinfo

    text = (message.text or "").strip()
    data = await state.get_data()
    label = channel_label(data.get("task_channel", ""))
    inv_label = await _invoice_label(db, data.get("linked_invoice_id"))

    if text == "-":
        await state.update_data(task_due=to_iso(_default_task_due(config)))
        await _show_task_confirm(message, state, config, label, inv_label)
        return

    # Одно только время («20:00») — не отказ, а смена часа у уже выбранной даты
    # (owner 03.08). Раньше регулярка срезала время с конца, остаток-дата
    # оказывался ПУСТЫМ, и шаг отвечал «Не удалось распознать срок» — ввести
    # время отдельным сообщением было нельзя по построению.
    hm = _parse_bare_time(text)
    if hm and data.get("task_due"):
        await state.update_data(
            task_due=_apply_time_to_due(data["task_due"], hm[0], hm[1], config.timezone),
        )
        await _show_task_confirm(message, state, config, label, inv_label)
        return

    hour, minute = _DEFAULT_DUE_HOUR, 0
    m = _DUE_TIME_RE.search(text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        text = text[: m.start()].strip()
    else:
        m = _DUE_HOUR_ONLY_RE.search(text)   # «07 марта 14» — час без минут
        if m:
            hour, minute = int(m.group(1)), 0
            text = text[: m.start()].strip()

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        await message.answer("Некорректное время. Укажите от 00:00 до 23:59:")
        return

    parsed = parse_date(text, config.timezone) if text else None
    if not parsed:
        await message.answer(
            "Не удалось распознать срок.\n"
            "Например: <b>07 марта 14:00</b>, <b>15.03.2026</b> или «-» (неделя):"
        )
        return

    due = parsed.astimezone(_tzinfo(config.timezone)).replace(
        hour=hour, minute=minute, second=0, microsecond=0,
    )
    await state.update_data(task_due=to_iso(due))
    await _show_task_confirm(message, state, config, label, inv_label)


@router.message(GdTaskCreateSG.deadline_time)
async def gd_task_create_time(message: Message, state: FSMContext, config: Config) -> None:
    """LEGACY-шаг «время дедлайна» — из потока убран (owner 30.07: срок вводится
    одной строкой в gd_task_create_deadline). Оставлен, чтобы сессии, зависшие
    в этом состоянии до деплоя, доехали до конца, а не встали молча
    ([[feedback_fsm_old_buttons_trap]]). Новые задачи сюда не попадают.
    """
    import re as _re
    from ..utils import from_iso, tzinfo as _tzinfo

    text = (message.text or "").strip()

    if text == "-":
        hour, minute = 18, 0
    else:
        m = _re.fullmatch(r"(\d{1,2})[:\.](\d{2})", text)
        if not m:
            m = _re.fullmatch(r"(\d{1,2})", text)
            if m:
                hour, minute = int(m.group(1)), 0
            else:
                await message.answer(
                    "Не удалось распознать время.\n"
                    "Укажите в формате <b>14:00</b> или просто <b>14</b>:"
                )
                return
        else:
            hour, minute = int(m.group(1)), int(m.group(2))

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        await message.answer("Некорректное время. Укажите от 00:00 до 23:59:")
        return

    data = await state.get_data()
    due_iso = data.get("task_due", "")
    if due_iso:
        due_dt = from_iso(due_iso).astimezone(_tzinfo(config.timezone))
        due_dt = due_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        await state.update_data(task_due=to_iso(due_dt))

    await state.set_state(GdTaskCreateSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Создать задачу", callback_data="gd_task_finalize")
    b.button(text="⏭ Без вложений", callback_data="gd_task_finalize")
    b.button(text="❌ Отмена", callback_data="gd_task_cancel")
    b.adjust(1)
    await message.answer(
        "Прикрепите файлы (по желанию). Когда готовы — нажмите кнопку:",
        reply_markup=b.as_markup(),
    )


@router.message(GdTaskCreateSG.attachments)
async def gd_task_create_attach(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    storage: MinioStorage | None = None,
) -> None:
    data = await state.get_data()

    uid = message.from_user.id if message.from_user else "anon"
    att, _att_count = await collect_attachment(
        message, state, storage, prefix=f"gd_task/{uid}", key="task_attachments"
    )
    if att is None:
        # 🔴 Сюда прилетало «20:00» и получало «Это не файл» (инцидент owner'а
        # 03.08, 15:09:37). Причина: после ввода даты шаг срока возвращает экран
        # подтверждения и ставит состояние attachments — отдельного шага времени
        # в потоке нет с 30.07. Со стороны это читалось как «после даты всё
        # виснет, время ввести нельзя». Теперь голое время меняет час у срока.
        hm = _parse_bare_time(message.text or "")
        if hm and data.get("task_due"):
            await state.update_data(
                task_due=_apply_time_to_due(
                    data["task_due"], hm[0], hm[1], config.timezone,
                ),
            )
            label = channel_label(data.get("task_channel", ""))
            inv_label = await _invoice_label(db, data.get("linked_invoice_id"))
            await _show_task_confirm(
                message, state, config, label, inv_label,
                note="⏰ Время срока обновил.\n\n",
            )
            return
        await message.answer(
            "Это не файл. Пришлите файл/фото/видео, либо укажите время "
            "(<b>20:00</b>) — поставлю его в срок, либо нажмите «✅ Создать задачу».",
        )
        return
    # Список вложений уже записан collect_attachment под блокировкой.
    # Экран подтверждения пересобираем — кнопка «✅ Создать» должна быть под
    # последним сообщением, а не уехать вверх за принятыми файлами (owner 30.07).
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    label = channel_label(data.get("task_channel", ""))
    inv_label = await _invoice_label(db, data.get("linked_invoice_id"))
    await _show_task_confirm(
        message, state, config, label, inv_label,
        note=f"📎 Принял файл.{suffix}\n\n",
    )


@router.callback_query(F.data == "gd_task_cancel")
async def gd_task_cancel_cb(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Cancel task creation via inline button."""
    await cb.answer("Отменено")
    data = await state.get_data()
    # `task_channel` пуст, если черновик уже завершён/сброшен (finalize чистит
    # состояние, а кнопка «❌ Отмена» остаётся в старом сообщении). Тогда берём
    # канал самого чата: иначе в меню уезжал ПУСТОЙ channel, «📋 Задачи» рисовали
    # кнопку `gd_task_create:` без канала, и следующая задача создавалась вслепую
    # — ровно так 03.08 в 15:10 родилось «⚠️ Адресат для  не настроен».
    channel = data.get("task_channel") or data.get("channel") or ""
    await state.clear()
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await cb.message.answer(  # type: ignore[union-attr]
        "❌ Создание задачи отменено.",
        reply_markup=gd_channel_menu(channel),
    )


@router.callback_query(F.data == "gd_task_finalize")
async def gd_task_create_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Create GD_TASK and notify the target."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    channel = data.get("task_channel", "")
    description = data.get("task_description", "")
    due_iso = data.get("task_due", to_iso(utcnow()))
    attachments = data.get("task_attachments", [])

    # Resolve target(s): composite channels → multiple recipients
    # Для montazh — использовать выбранного монтажника
    montazh_target = data.get("montazh_target_id")
    sales_target = data.get("sales_task_target")  # КВ/КИА/НПН/all (Отд.Продаж)
    if channel == "montazh" and montazh_target:
        targets: list[tuple[str, int]] = [(channel, int(montazh_target))]
    elif channel == "otd_prodazh" and sales_target:
        # Конкретный менеджер либо «всем 3 менеджерам»; РП исключён (user 2026-06-14).
        mgr_channels = (
            ["manager_kv", "manager_kia", "manager_npn"]
            if sales_target == "all" else [sales_target]
        )
        targets = []
        for sc in mgr_channels:
            tid = await resolve_channel_target(sc, db, config)
            if tid:
                targets.append((sc, int(tid)))
    else:
        sub_channels = COMPOSITE_CHANNELS.get(channel)
        if sub_channels:
            targets = []
            for sc in sub_channels:
                tid = await resolve_channel_target(sc, db, config)
                if tid:
                    targets.append((sc, int(tid)))
        else:
            tid = await resolve_channel_target(channel, db, config)
            targets = [(channel, int(tid))] if tid else []

    if not targets:
        await cb.message.answer(  # type: ignore[union-attr]
            f"⚠️ Адресат для {channel_label(channel)} не настроен.",
        )
        await state.clear()
        return

    label = channel_label(channel)
    # Для Отд.Продаж — показать в подтверждении конкретного адресата.
    if channel == "otd_prodazh" and sales_target:
        _mgr_lbl = {"manager_kv": "Менеджер КВ", "manager_kia": "Менеджер КИА", "manager_npn": "Менеджер НПН"}
        label = "Всем (КВ+КИА+НПН)" if sales_target == "all" else _mgr_lbl.get(sales_target, channel_label(sales_target))
    initiator = await get_initiator_label(db, u.id)

    # Invoice label for notification
    linked_inv_id = data.get("linked_invoice_id")
    inv_label = ""
    if linked_inv_id:
        inv_row = await db.get_invoice(int(linked_inv_id))
        if inv_row:
            inv_label = f"\n🧾 Счёт: {inv_row.get('invoice_number') or '—'} / {inv_row.get('object_address') or '—'}"

    for sc, target_id in targets:
        task = await db.create_task(
            project_id=None,
            type_=TaskType.GD_TASK,
            status=TaskStatus.OPEN,
            created_by=u.id,
            assigned_to=target_id,
            due_at_iso=due_iso,
            payload={
                "comment": description,
                "source": f"chat_proxy:{channel}",
                "sender_id": u.id,
                "sender_username": u.username,
                "linked_invoice_id": data.get("linked_invoice_id"),
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

        from ..utils import build_manager_task_card
        try:
            msg = await build_manager_task_card(
                db, task, config.timezone,
                header_emoji="📝", header_title="Новая задача от ГД",
                actor_label=initiator,
            )
        except Exception:
            log.exception("chat_proxy GD_TASK: card render failed, fallback")
            msg = (
                f"📝 <b>Новая задача от ГД</b>\n"
                f"👤 От: {initiator}{inv_label}\n\n"
                f"📋 {description}"
            )
        await notifier.safe_send(target_id, msg, reply_markup=task_actions_kb(task))

        for a in attachments:
            await notifier.safe_send_media(target_id, a["file_type"], a["file_id"], caption=a.get("caption"))
        await refresh_recipient_keyboard(notifier, db, config, target_id)

        await integrations.sync_task(task, project_code="")

        # При назначении монтажника на счёт — привязать к счёту + сохранить площадь
        if channel == "montazh" and linked_inv_id:
            await db.assign_installer_to_invoice(int(linked_inv_id), target_id)
            task_area = data.get("task_area_m2")
            if task_area is not None:
                await db.conn.execute(
                    "UPDATE invoices SET area_m2 = ? WHERE id = ?",
                    (task_area, int(linked_inv_id)),
                )
                await db.conn.commit()

    await state.clear()

    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)

    await cb.message.answer(  # type: ignore[union-attr]
        f"✅ Задача создана и отправлена → {label}.",
        reply_markup=gd_channel_menu(channel),
    )

# ---------------------------------------------------------------------------
# Reply from employee to GD (incoming replies)
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("reply_to_gd:"))
async def reply_to_gd_start(cb: CallbackQuery, state: FSMContext) -> None:
    """Employee clicks 'Ответить ГД' button."""
    await cb.answer()
    channel = cb.data.split(":", 1)[1]  # type: ignore[union-attr]
    await state.clear()
    await state.set_state(ReplyToGDSG.text)
    await state.update_data(reply_channel=channel)

    label = channel_label(channel)
    await cb.message.answer(  # type: ignore[union-attr]
        f"💬 <b>Ответ ГД</b> (канал: {label})\n\n"
        "Введите текст ответа.\n"
        "Можно прикрепить файл.\n"
        "Для отмены: /cancel",
    )


@router.message(ReplyToGDSG.text)
async def reply_to_gd_send(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    storage: MinioStorage | None = None,
) -> None:
    """Forward employee reply to GD."""
    data = await state.get_data()
    channel = data.get("reply_channel", "")
    u = message.from_user
    if not u:
        return

    text = (message.text or message.caption or "").strip()
    file_info = await mirror_attachment(message, storage, prefix=f"chat_reply/{channel}/{u.id}")

    if not text and not file_info:
        await message.answer("Введите текст или прикрепите файл.")
        return

    # Find GD user
    from ..services.assignment import resolve_default_assignee
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        await message.answer("Не удалось найти ГД.")
        await state.clear()
        return

    # Save to DB as incoming message
    chat_msg = await db.save_chat_message(
        channel=channel,
        sender_id=u.id,
        direction="incoming",
        text=text or None,
        receiver_id=int(gd_id),
        tg_message_id=message.message_id,
        has_attachment=bool(file_info),
    )

    if file_info:
        await db.save_chat_attachment(
            chat_message_id=int(chat_msg["id"]),
            tg_file_id=file_info["file_id"],
            file_type=file_info["file_type"],
            tg_file_unique_id=file_info.get("file_unique_id"),
            caption=message.caption,
            minio_object_key=file_info.get("minio_object_key"),
        )

    # Auto-detect credit expense from manager reply
    if channel in FINANCE_CHANNELS and text:
        amount = parse_amount_from_text(text)
        if amount is not None:
            await db.save_finance_entry(
                channel=channel,
                amount=amount,
                entered_by=u.id,
                chat_message_id=int(chat_msg["id"]),
                description=text[:200],
            )
            await _auto_credit_expense(
                db, channel, amount, text[:200],
                entered_by=u.id, chat_message_id=int(chat_msg["id"]),
            )

    # Forward to GD
    label = channel_label(channel)
    header = f"💬 <b>Ответ от {label}</b> (@{u.username or u.id}):\n\n"
    if text:
        await notifier.safe_send(int(gd_id), header + text)
    if file_info:
        await notifier.safe_send_media(
            int(gd_id), file_info["file_type"], file_info["file_id"], caption=message.caption,
        )
    await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await answer_service(message, "✅ Ответ отправлен ГД.")


# ---------------------------------------------------------------------------
# Свободный текст прямо в меню канала (owner 25.07)
#
# Баг: ГД заходил в «Чат с РП», выбирал счёт и СРАЗУ печатал текст задачи, не
# нажимая кнопок. В состоянии ChatProxySG.menu ловятся только точные тексты
# кнопок, произвольный текст не обрабатывал НИКТО → бот молчал (в логах
# update_logger: status=unhandled). fallback.py тут не помогает — он
# StateFilter(None), а состояние активное.
#
# Хендлер РЕГИСТРИРУЕТСЯ ПОСЛЕДНИМ в роутере (файл читается сверху вниз),
# поэтому все кнопочные хендлеры состояния срабатывают раньше него.
# ---------------------------------------------------------------------------

_FREE_TEXT_KEY = "menu_free_text"

# Кнопки самого подменю канала: их ловят точные хендлеры выше (в chat_proxy и
# в gd.py — тот роутер подключён раньше). Перечислены защитно, чтобы кнопка
# никогда не ушла в «свободный текст», даже если её хендлер отвалится.
_MENU_OWN_BUTTONS: frozenset[str] = frozenset({
    "📖 Переписка",
    "✏️ Написать",
    "📋 Задачи",
    "📨 Входящие",
    "📊 Отчёт",
    GD_BTN_CRED_BAL,      # 🏦 Баланс кошелька
    GD_BTN_CRED_SPEND,    # ➕ Расход кредита
    "⬅️ Назад",
    "◀️ Назад",
})

# Кнопки, которые обслуживают роутеры, подключённые ПОСЛЕ chat_proxy
# (urgent.router, main.py:291). Перехватывать их нельзя — иначе они «умрут»
# внутри меню канала.
_MENU_PASSTHROUGH: frozenset[str] = frozenset({
    "🚨 Срочно ГД",
    "📞 Связь с ГД",
    "📩 Не срочно ГД",
    "Не срочно ГД",
})


def _is_menu_free_text(message: Message) -> bool:
    """Фильтр: это произвольный текст, а не кнопка и не команда."""
    text = (message.text or "").strip()
    if len(text) < 2 or text.startswith("/"):
        return False
    if text in _MENU_OWN_BUTTONS or text in _MENU_PASSTHROUGH:
        return False
    if text.startswith("➡️"):  # подменю «Кому писать?» (SalesWriteSG)
        return False
    return True


@router.message(ChatProxySG.menu, F.text, _is_menu_free_text)
async def chat_menu_freetext(message: Message, state: FSMContext) -> None:
    """ГД набрал произвольный текст в меню канала → спросить, что с ним сделать."""
    text = (message.text or "").strip()
    data = await state.get_data()
    channel = data.get("channel", "")
    await state.update_data(**{_FREE_TEXT_KEY: text})

    b = InlineKeyboardBuilder()
    b.button(text="📨 Отправить сообщением", callback_data="gdfree:send")
    b.button(text="📋 Создать задачу", callback_data="gdfree:task")
    b.button(text="❌ Отмена", callback_data="gdfree:cancel")
    b.adjust(1)

    preview = text if len(text) <= 200 else text[:200] + "…"
    await message.answer(
        f"💬 <b>{channel_label(channel)}</b>\n\n"
        f"Ваш текст: <i>{_html.escape(preview)}</i>\n\n"
        "Что с ним сделать?",
        reply_markup=b.as_markup(),
    )


async def _take_pending_free_text(cb: CallbackQuery, state: FSMContext) -> tuple[str, str, dict[str, Any]]:
    """Забрать набранный текст из state и СРАЗУ его погасить.

    Гашение до выполнения действия — синхронный гард от двойного клика
    ([[feedback_money_confirm_idempotent_gate]]): в finance-каналах отправка
    сообщения с суммой автоматически пишет расход кредита, дублировать нельзя.
    Возвращает (текст, канал, прежние данные state); текст пуст — действие уже
    выполнено или контекст потерян.
    """
    data = await state.get_data()
    text = (data.get(_FREE_TEXT_KEY) or "").strip()
    channel = data.get("channel", "")
    if text:
        await state.update_data(**{_FREE_TEXT_KEY: None})
    try:
        await cb.message.edit_reply_markup(reply_markup=None)  # type: ignore[union-attr]
    except Exception:
        pass
    return text, channel, data


@router.callback_query(F.data == "gdfree:cancel")
async def chat_menu_freetext_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("Отменено")
    _, channel, _ = await _take_pending_free_text(cb, state)
    await cb.message.answer(  # type: ignore[union-attr]
        "❌ Текст отброшен.",
        reply_markup=gd_channel_menu(channel),
    )


@router.callback_query(F.data == "gdfree:send")
async def chat_menu_freetext_send(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
) -> None:
    """Отправить набранный текст в канал — тем же путём, что «✏️ Написать»."""
    await cb.answer()
    u = cb.from_user
    if not u:
        return
    text, channel, _ = await _take_pending_free_text(cb, state)
    if not text:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Текст уже обработан — наберите заново.",
            reply_markup=gd_channel_menu(channel),
        )
        return

    await _deliver_chat_message(
        reply_to=cb.message,  # type: ignore[arg-type]
        sender_id=u.id,
        channel=channel,
        text=text,
        db=db,
        config=config,
        notifier=notifier,
    )
    await state.set_state(ChatProxySG.menu)
    await state.update_data(channel=channel)


@router.callback_query(F.data == "gdfree:task")
async def chat_menu_freetext_task(
    cb: CallbackQuery, state: FSMContext, db: Database, config: Config,
) -> None:
    """Сделать из набранного текста задачу: описание уже есть → сразу подтверждение."""
    await cb.answer()
    text, channel, data = await _take_pending_free_text(cb, state)
    if not text:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Текст уже обработан — наберите заново.",
            reply_markup=gd_channel_menu(channel),
        )
        return

    # Контекст задачи наследуется из чата: канал + уже выбранный там счёт.
    linked = data.get("linked_invoice_id")
    await state.set_data({
        "task_channel": channel,
        "task_attachments": [],
        "task_description": text,
        "linked_invoice_id": linked,
        "invoice_ctx_set": bool(data.get("invoice_ctx_set")),
    })

    label = channel_label(channel)
    # montazh / otd_prodazh — адресата всё равно надо выбрать; дальше поток
    # сам увидит готовое описание и не спросит его повторно.
    if await _show_task_target_picker(cb.message, state, db, channel, label):  # type: ignore[arg-type]
        return

    inv_label = await _invoice_label(db, linked)
    await _ask_task_description_or_deadline(cb.message, state, label, inv_label, config)  # type: ignore[arg-type]
