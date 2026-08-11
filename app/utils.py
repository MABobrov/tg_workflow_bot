from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional
from zoneinfo import ZoneInfo

from aiogram import html
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

if TYPE_CHECKING:
    from .db import Database
    from .config import Config


log = logging.getLogger(__name__)
SERVICE_MESSAGE_TTL_SECONDS = 120

# Strong references to pending cleanup tasks so the GC doesn't collect them
# before the sleep completes. Entries are discarded automatically on completion.
_pending_cleanup_tasks: set[asyncio.Task] = set()


ROLE_LABELS: dict[str, str] = {
    "manager": "Менеджер",
    "manager_kv": "Менеджер КВ",
    "manager_kia": "Менеджер КИА",
    "manager_npn": "Менеджер НПН",
    "rp": "РП",
    "td": "ТД",
    "accounting": "Бухгалтерия",
    "installer": "Монтажник",
    "zamery": "Замерщик",
    "gd": "ГД",
    "driver": "Водитель",
    "loader": "Грузчик",
    "tinter": "Тонировщик",
}
ROLE_ORDER: list[str] = [
    "manager", "manager_kv", "manager_kia", "manager_npn",
    "rp", "td", "accounting", "installer", "zamery",
    "driver", "loader", "tinter", "gd",
]

PROJECT_STATUS_LABELS: dict[str, str] = {
    "docs_request": "Запрос документов",
    "quote_request": "Запрос КП",
    "invoice_sent": "Счёт/документы отправлены",
    "waiting_payment": "Ожидает оплату",
    "payment_reported": "Оплата поступила",
    "in_work": "В работе",
    "ordering": "Заказ материалов",
    "delivery": "Доставка",
    "installation": "Монтаж",
    "tinting": "Тонировка",
    "closing_docs": "Закрывающие / ЭДО",
    "archive": "Архив",
}

TASK_STATUS_LABELS: dict[str, str] = {
    "open": "Новая",
    "in_progress": "В работе",
    "done": "Завершена",
    "rejected": "Отклонена",
}

TASK_TYPE_LABELS: dict[str, str] = {
    "docs_request": "Запрос документов/счёта",
    "quote_request": "Запрос КП",
    "payment_confirm": "Подтверждение оплаты",
    "closing_docs": "Документы / ЭДО",
    "manager_info_request": "Запрос информации менеджеру",
    "urgent_gd": "Срочно ГД",
    "issue": "Проблема / вопрос",
    "daily_report": "Ежедневный отчёт",
    "installation_done": "Счёт ОК / монтаж завершён",
    "project_end": "Счёт End",
    # --- новые типы ---
    "order_profile": "Заказ профиля",
    "order_glass": "Заказ стекла",
    "order_materials": "Заказ материалов",
    "supplier_payment": "Оплата поставщику",
    "delivery_request": "Оплата доставки",
    "delivery_done": "Доставка выполнена",
    "tinting_request": "Заявка на тонировку",
    "tinting_done": "Тонировка выполнена",
    "assign_lead": "Распределение лида",
    "invoice_payment": "Счёт на оплату",
    "gd_task": "Задача от ГД",
    "self_reminder": "Напоминание",
    "not_urgent_gd": "Не срочно ГД",
    # --- новые типы (фаза расширения) ---
    "edo_request": "Запрос ЭДО",
    "installer_ok": "Монтажник — Счет ОК",
    "zp_calculation": "Расчёт ЗП",
    "lead_to_project": "Лид в проект",
    "invoice_end": "Счет End",
    "invoice_end_fixup": "Счёт End — устранить пункт",
    "invoice_end_ready": "Счёт готов к закрытию",
    "check_kp": "Проверить КП / Счет",
    "acc_question": "Вопрос от бухгалтерии",
    # --- ЗП / финансы / прочее (добавлено 18.06 — без них карточки показывали сырой тип) ---
    "zp_manager": "ЗП менеджера",
    "zp_installer": "ЗП монтажника",
    "zp_rp": "ЗП РП",
    "zp_zamery_batch": "ЗП замерщика",
    "zamery_request": "Заявка на замер",
    "razmery_verification": "Проверка размеров",
    "supplier_invoice": "Счёт от поставщика",
    "rp_salary": "Оклад РП",
    "gd_deposit_request": "Запрос из депозита",
    "final_payment_eta": "Ориентировочная дата финального платежа по счёту",
    "invoice_docs_missing": "Нет документов по счёту",
    # --- Перерасчёт прибыли → аванс менеджеру (ТЗ 02.07, деплой 03.07) ---
    "recalc_confirm": "Перерасчёт прибыли",
}


def fmt_money(value: float | int | None) -> str:
    """Сумма с пробелом-разделителем разрядов и символом ₽.

    По правилу feedback_card_template_standard.md (п.7): `{value:,.0f}` + replace.
    """
    return f"{float(value or 0):,.0f}".replace(",", " ") + "₽"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(s: str) -> datetime:
    # Python parses ISO with timezone
    return datetime.fromisoformat(s)


def tzinfo(tz_name: str) -> ZoneInfo:
    return ZoneInfo(tz_name)


def format_dt_iso(iso_s: str | None, tz_name: str) -> str:
    if not iso_s:
        return "—"
    dt = from_iso(iso_s).astimezone(tzinfo(tz_name))
    return dt.strftime("%d.%m.%Y %H:%M")


_MONTHS_RU_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def format_date_iso(iso_s: str | None, tz_name: str) -> str:
    if not iso_s:
        return "—"
    dt = from_iso(iso_s).astimezone(tzinfo(tz_name))
    return f"{dt.day:02d} {_MONTHS_RU_GEN[dt.month]}"


def parse_amount(text: str) -> Optional[float]:
    if not text:
        return None
    t = text.strip().replace(" ", "").replace(",", ".")
    # allow "100k" "100000"
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([kк]?)", t, flags=re.IGNORECASE)
    if not m:
        return None
    num = float(m.group(1))
    if m.group(2):
        num *= 1000
    return num


def parse_date(text: str, tz_name: str) -> Optional[datetime]:
    if not text:
        return None
    t = text.strip()
    now_local = utcnow().astimezone(tzinfo(tz_name))

    # YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime(y, mo, d, 12, 0, tzinfo=tzinfo(tz_name))
        except ValueError:
            return None

    # DD.MM.YYYY or DD.MM
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", t)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y_raw = m.group(3)
        if y_raw:
            y = int(y_raw)
            if y < 100:
                y += 2000
        else:
            y = now_local.year
        try:
            result = datetime(y, mo, d, 12, 0, tzinfo=tzinfo(tz_name))
            # Год не указан и дата уже прошла → это про следующий год. Сравниваем
            # по календарному дню (не datetime): «сегодня» (12:00-якорь может быть
            # < now при вводе после полудня) НЕ откатывается вперёд. Явный год не
            # трогаем — пользователь указал его намеренно.
            if not y_raw and result.date() < now_local.date():
                result = datetime(y + 1, mo, d, 12, 0, tzinfo=tzinfo(tz_name))
        except ValueError:
            return None
        return result

    # "DD месяц" — e.g. "07 марта", "15 апреля"
    MONTHS_RU = {
        "января": 1, "янв": 1,
        "февраля": 2, "фев": 2,
        "марта": 3, "мар": 3,
        "апреля": 4, "апр": 4,
        "мая": 5,
        "июня": 6, "июн": 6,
        "июля": 7, "июл": 7,
        "августа": 8, "авг": 8,
        "сентября": 9, "сен": 9,
        "октября": 10, "окт": 10,
        "ноября": 11, "ноя": 11,
        "декабря": 12, "дек": 12,
    }
    m_ru = re.fullmatch(r"(\d{1,2})\s+([а-яё]+)", t.lower())
    if m_ru:
        d_val = int(m_ru.group(1))
        mo_val = MONTHS_RU.get(m_ru.group(2))
        if mo_val:
            y_val = now_local.year
            try:
                result = datetime(y_val, mo_val, d_val, 12, 0, tzinfo=tzinfo(tz_name))
                # If the date is already past (by calendar day), assume next year.
                # Сравнение по дню (не datetime): «сегодня» не откатывается вперёд,
                # даже если 12:00-якорь оказался раньше текущего времени.
                if result.date() < now_local.date():
                    result = datetime(y_val + 1, mo_val, d_val, 12, 0, tzinfo=tzinfo(tz_name))
                return result
            except ValueError:
                return None

    # "today", "tomorrow" in ru
    if t.lower() in {"сегодня", "today"}:
        return now_local.replace(hour=12, minute=0, second=0, microsecond=0)
    if t.lower() in {"завтра", "tomorrow"}:
        dt = now_local + timedelta(days=1)
        return dt.replace(hour=12, minute=0, second=0, microsecond=0)

    return None


def parse_callback_int(cb_data: str | None, sep: str = ":", index: int = -1) -> int | None:
    """Safely parse an integer from callback data split by separator.

    Returns None if data is missing, index is out of bounds, or value is not a valid int.
    """
    if not cb_data:
        return None
    parts = cb_data.split(sep)
    try:
        return int(parts[index])
    except (IndexError, ValueError):
        return None


def try_json_loads(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}


def role_label(value: str | None) -> str:
    if not value:
        return "—"
    roles = parse_roles(value)
    if len(roles) > 1:
        return ", ".join(ROLE_LABELS.get(r, r) for r in roles)
    return ROLE_LABELS.get(value, value)


def parse_roles(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [p.strip().lower() for p in value.replace(";", ",").split(",")]
    seen: set[str] = set()
    out: list[str] = []
    for role in parts:
        if not role or role not in ROLE_LABELS:
            continue
        if role in seen:
            continue
        seen.add(role)
        out.append(role)
    # stable business order for predictable menus/labels
    order_index = {r: i for i, r in enumerate(ROLE_ORDER)}
    out.sort(key=lambda r: order_index.get(r, 10_000))
    return out


def roles_to_storage(roles: list[str] | tuple[str, ...] | set[str]) -> str | None:
    normalized = parse_roles(",".join(str(r) for r in roles))
    if not normalized:
        return None
    return ",".join(normalized)


def has_role(value: str | None, role: str) -> bool:
    return role in set(parse_roles(value))


def has_any_role(value: str | None, roles: set[str]) -> bool:
    current = set(parse_roles(value))
    return bool(current & roles)


def project_status_label(value: str | None) -> str:
    if not value:
        return "—"
    return PROJECT_STATUS_LABELS.get(value, value)


def task_status_label(value: str | None) -> str:
    if not value:
        return "—"
    return TASK_STATUS_LABELS.get(value, value)


def task_type_label(value: str | None) -> str:
    if not value:
        return "—"
    return TASK_TYPE_LABELS.get(value, value)


def private_only_reply_markup(event_message: Any, markup: Any | None) -> Any | None:
    """Return reply markup only for private chats.

    Prevents bot reply keyboards from appearing in groups/supergroups.
    """
    if markup is None:
        return None
    chat = getattr(event_message, "chat", None)
    chat_type = getattr(chat, "type", None)
    if chat_type == "private":
        return markup
    return None


def schedule_message_cleanup(sent_message: Any, delay_seconds: int = SERVICE_MESSAGE_TTL_SECONDS) -> None:
    """Delete a bot service message later to keep private chats clean."""
    if not sent_message or delay_seconds <= 0:
        return
    chat = getattr(sent_message, "chat", None)
    chat_id = getattr(chat, "id", None)
    chat_type = getattr(chat, "type", None)
    message_id = getattr(sent_message, "message_id", None)
    bot = getattr(sent_message, "bot", None)
    if not chat_id or not message_id or not bot or chat_type != "private":
        return

    async def _cleanup() -> None:
        try:
            await asyncio.sleep(delay_seconds)
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except (TelegramBadRequest, TelegramForbiddenError):
            return
        except Exception:
            log.exception("Failed to auto-delete service message chat_id=%s message_id=%s", chat_id, message_id)

    task = asyncio.create_task(_cleanup())
    _pending_cleanup_tasks.add(task)
    task.add_done_callback(_pending_cleanup_tasks.discard)


async def answer_service(
    target_message: Any,
    text: str,
    *,
    delay_seconds: int = SERVICE_MESSAGE_TTL_SECONDS,
    **kwargs: Any,
) -> Any:
    """Send a transient bot-only service message and schedule its deletion."""
    sent_message = await target_message.answer(text, **kwargs)
    schedule_message_cleanup(sent_message, delay_seconds=delay_seconds)
    return sent_message


def encode_sa_json(value: str) -> dict[str, Any]:
    """Accept raw JSON or base64 JSON and return dict."""
    raw = value.strip()
    if not raw:
        raise ValueError("Empty service account json")
    if raw.startswith("{"):
        return json.loads(raw)
    # assume base64
    decoded = base64.b64decode(raw).decode("utf-8")
    return json.loads(decoded)


@dataclass
class TgUserView:
    telegram_id: int
    username: str | None
    full_name: str | None
    role: str | None

    def mention(self) -> str:
        # prefer @username
        if self.username:
            return f"@{html.quote(self.username)}"
        # fallback to tg://user?id=
        name = html.quote(self.full_name or str(self.telegram_id))
        return f"<a href=\"tg://user?id={self.telegram_id}\">{name}</a>"


async def get_initiator_label(db: Database, user_id: int) -> str:
    """Return formatted initiator string: 'Full Name (@username)' with fallback."""
    user = await db.get_user_optional(user_id)
    if not user:
        return f"User#{user_id}"
    parts: list[str] = []
    if user.full_name:
        parts.append(html.quote(user.full_name))
    if user.username:
        parts.append(f"(@{html.quote(user.username)})")
    return " ".join(parts) if parts else f"User#{user_id}"


async def refresh_recipient_keyboard(
    notifier: Any,
    db: Database,
    config: Any,
    user_id: int,
) -> None:
    """Send updated main_menu with unread counter to the recipient."""
    from .services.menu_context import build_main_menu_for_user  # lazy import to avoid circular
    from .services.menu_scope import resolve_menu_scope

    user = await db.get_user_optional(user_id)
    if not user:
        return
    menu_role, isolated_role = resolve_menu_scope(user_id, user.role)
    unread = await db.count_unread_tasks(user_id)
    kb = await build_main_menu_for_user(
        db, config, user_id, menu_role, isolated_role=isolated_role,
    )
    text = f"📥 У вас {unread} активных задач." if unread else "📥 Нет активных задач."
    await notifier.safe_send(user_id, text, reply_markup=kb)


_INV_STATUS_TITLE: dict[str, tuple[str, str]] = {
    "new": ("🆕", "Новый счёт"),
    "pending": ("⏳", "Ждёт подтверждения ГД"),
    "in_progress": ("🔄", "Счёт в работе"),
    "paid": ("✅", "Счёт оплачен"),
    "on_hold": ("⏸", "Счёт отложен"),
    "rejected": ("❌", "Счёт отклонён"),
    "closing": ("📌", "Счёт на проверке"),
    "ended": ("🏁", "Счёт End"),
    "credit": ("🏦", "Кредитный счёт"),
}


_INV_STATUS_TOTAL: dict[str, str] = {
    "new": "🆕 Новый",
    "pending": "⏳ Ожидает",
    "in_progress": "🔄 В работе",
    "paid": "✅ Оплачен",
    "on_hold": "⏸ Отложен",
    "rejected": "❌ Отклонён",
    "closing": "📌 Проверка",
    "ended": "🏁 End",
    "credit": "🏦 Кредит",
}

_SECTION_EMOJI: dict[str, str] = {
    "Условия": "✅",
    "Статус": "📌",
    "Документы": "📂",
    "Закупки": "📦",
}


def format_invoice_card_standard(
    *,
    inv: dict[str, Any],
    creator_label: str,
    section: tuple[str, list[str]] | None = None,
    comment: str | None = None,
    title_override: tuple[str, str] | None = None,
    extra_meta: list[str] | None = None,
) -> str:
    """Карточка счёта по эталону card-template-standard (assets/card_etalon.png).

    Каждая секция — отдельный <pre>-блок: заголовок plain bold ВНЕ <pre>
    (теги форматирования внутри <pre> Telegram не рендерит) + моноширинное
    тело. Anti-pattern B (emoji-мета plain text) запрещён.

    - inv: запись из db.get_invoice().
    - creator_label: "Менеджер КВ Семён" (через get_initiator_label).
    - section: (label, body_lines) — вторая секция (Условия/Статус/Документы).
      Тело — готовые строки (например "1. ✅ Монтажник — Счет ОК"); рендерится
      в <pre> без выравнивания (это уже законченные строки, не label/value).
    - comment: содержимое блока «💬 Комментарий: ...» (plain text под секциями).
    - title_override: (emoji, title) — нестандартный заголовок (без статуса справа).
    - extra_meta: список строк формата "📅 Создан: 2026-05-22" — парсится в
      label/value по " : " и добавляется к мета-полям.
    """
    status = inv.get("status") or ""
    is_credit = bool(inv.get("is_credit"))
    num = html.quote(str(inv.get("invoice_number") or "?"))
    addr = html.quote(str(inv.get("object_address") or "—"))
    client = html.quote(str(inv.get("client_name") or "—"))
    amount = float(inv.get("amount") or 0)
    creator = html.quote(str(creator_label or "—"))

    if title_override:
        emoji, title = title_override
        status_total: str | None = None
    else:
        emoji = "📋"
        title = f"Счёт №{num}"
        if is_credit and status not in ("ended", "rejected"):
            status_total = "🏦 Кредит"
        else:
            status_total = _INV_STATUS_TOTAL.get(status)

    pay_label = "🏦 Кред" if is_credit else "💳 б/н"

    meta_items: list[tuple[str, str]] = [
        ("Адрес", addr),
        ("Клиент", client),
        ("Тип", pay_label),
        ("Сумма", fmt_money(amount)),
        ("От", creator),
    ]
    if extra_meta:
        for em in extra_meta:
            if ": " in em:
                label_part, _, val_part = em.partition(": ")
                meta_items.append((label_part, val_part))
            else:
                meta_items.append((em, ""))

    sections_out: list[str] = [
        format_card_section(
            emoji=emoji,
            title=title,
            items=meta_items,
            total=status_total,
            width=44,
            compact=True,  # «Адрес: ...» в одной строке, без столбика-переноса
        )
    ]

    if section is not None:
        section_label, section_body = section
        sec_emoji = _SECTION_EMOJI.get(section_label, "📋")
        indent = "   "
        header = f"<b>{sec_emoji}  {section_label}</b>"
        body = "\n".join(f"{indent}{line}" for line in section_body)
        sections_out.append(f"{header}\n<pre>{body}</pre>")

    text = format_card(sections_out)

    if comment:
        text += f"\n\n💬 Комментарий: {html.quote(str(comment))}"

    return text


def invoice_status_label(status: str | None) -> str:
    return {
        "new": "🆕 Новый",
        "pending": "⏳ Ждёт подтверждения ГД",
        "in_progress": "🔄 В работе",
        "paid": "✅ Оплачен",
        "on_hold": "⏸ Отложен",
        "rejected": "❌ Отклонён",
        "closing": "📌 Закрытие",
        "ended": "🏁 Счет End",
        "credit": "🏦 Кредит",
    }.get(status or "", status or "—")


def invoice_status_emoji(status: str | None) -> str:
    return {
        "new": "🆕",
        "pending": "⏳",
        "in_progress": "🔄",
        "paid": "✅",
        "on_hold": "⏸",
        "rejected": "❌",
        "closing": "📌",
        "ended": "🏁",
        "credit": "🏦",
    }.get(status or "", "❓")


def close_condition_core_rows(
    inv: dict[str, Any],
    conditions: dict[str, Any],
    *,
    debts_label: str = "Долгов нет",
) -> list[tuple[str, str]]:
    """Базовые условия закрытия счёта как (галка, текст), с учётом кредитности.

    У КРЕДИТНЫХ счетов строка ЭДО («Бухгалтерия — Закр.ЭДО ок») ОПУСКАЕТСЯ:
    бухгалтерия в credit-flow не участвует, edo_signed у них всегда «выполнено»
    (см. db.check_close_conditions). owner 24.06 «убрать строку ЭДО у кредитных».
    Вызывающий нумерует строки (enumerate) и дописывает свой 4-й пункт
    (Пояснения / ЗП — утверждено) — нумерация выходит сквозной автоматически.
    """
    is_credit = bool(inv.get("is_credit")) or str(inv.get("invoice_number") or "").startswith("ЗМ")
    rows: list[tuple[str, str]] = [
        ("✅" if conditions.get("installer_ok") else "⏳", "Монтажник — Счет ОК"),
    ]
    if not is_credit:
        rows.append(("✅" if conditions.get("edo_signed") else "⏳", "Бухгалтерия — Закр.ЭДО ок"))
    rows.append(("✅" if conditions.get("no_debts") else "⏳", debts_label))
    return rows


async def build_invoice_section(
    db: "Database",
    inv: dict[str, Any],
    invoice_id: int,
    *,
    include_zp: bool = True,
) -> tuple[str, list[str]]:
    """Готовит секцию <b>Условия:</b> / <b>Статус:</b> для карточки счёта.

    Возвращает (label, body_lines) для format_invoice_card_standard.
    include_zp=False — для карточек где ЗП-условие не отображается (например kp_issued_view).
    """
    from .enums import InvoiceStatus

    status = inv.get("status") or ""
    is_credit = bool(inv.get("is_credit"))

    if status in (InvoiceStatus.IN_PROGRESS, InvoiceStatus.PAID, InvoiceStatus.CLOSING):
        conditions = await db.check_close_conditions(invoice_id)
        rows = close_condition_core_rows(inv, conditions)
        if include_zp:
            rows.append(("✅" if conditions.get("zp_approved") else "⏳", "ЗП — утверждено"))
        body = [f"{i}. {mark} {label}" for i, (mark, label) in enumerate(rows, 1)]
        return ("Условия", body)
    if status in (InvoiceStatus.NEW, InvoiceStatus.PENDING_PAYMENT):
        return ("Статус", ["⏳ Ждёт подтверждения ГД"])
    if status == InvoiceStatus.ENDED:
        return ("Статус", ["✅ Все условия выполнены, счёт ЗАКРЫТ"])
    if status == InvoiceStatus.CREDIT or is_credit:
        return ("Статус", ["🏦 Кредитный счёт активен"])
    if status == InvoiceStatus.REJECTED:
        return ("Статус", ["❌ Счёт отклонён"])
    if status == InvoiceStatus.ON_HOLD:
        return ("Статус", ["⏸ Счёт отложен"])
    return ("Статус", [invoice_status_label(status)])


def _accounting_blocked(viewer_role: Any, what: str) -> str | None:
    """Стоячий гард: роль «Бухгалтерия» не видит себестоимость/прибыль.

    [[feedback_accounting_no_cost_profit_visibility]]. Возвращает текст-отказ, если
    роль == Role.ACCOUNTING, иначе None. Дефолтные вызовы (viewer_role=None или
    role='gd'/'rp') → None: поведение ГД/менеджера/РП/Sheets без изменений. Гард
    срабатывает только если cost/profit-рендер когда-нибудь подключат к потоку
    бухгалтера и передадут его роль.
    """
    if viewer_role is None:
        return None
    from .enums import Role

    if viewer_role == Role.ACCOUNTING:
        return f"⛔ {what} недоступно для роли «Бухгалтерия»."
    return None


def format_materials_list(
    inv: dict[str, Any],
    children: list[dict[str, Any]],
    supplier_payments: list[dict[str, Any]],
    viewer_role: Any = None,
) -> str:
    """Список купленных материалов для менеджера (без сумм).

    Карточка по стандартному образцу (docs/rules/feedback_card_template_standard.md):
    заголовок «📦 Материалы: №X» → мета (адрес/клиент/тип) → секции с подпунктами.
    """
    from .enums import MATERIAL_TYPE_LABELS

    blocked = _accounting_blocked(viewer_role, "Список материалов")
    if blocked:
        return blocked

    num = html.quote(str(inv.get("invoice_number") or f"#{inv.get('id')}"))
    addr = html.quote(str(inv.get("object_address") or inv.get("address") or "-"))
    client = html.quote(str(inv.get("client_name") or "—"))
    is_credit = bool(inv.get("is_credit"))
    pay_label = "🏦 Кред" if is_credit else "💳 б/н"

    lines: list[str] = [
        f"📦 <b>Материалы: №{num}</b>",
        "",
        f"📍 Адрес: {addr}",
        f"💳 Тип: {pay_label}",
        f"🏢 Клиент: {client}",
    ]

    if not children and not supplier_payments:
        lines.append("")
        lines.append("Нет записей о закупках.")
        return "\n".join(lines)

    # Дочерние счета (материалы от РП)
    if children:
        lines.append("")
        lines.append("<b>Закупки (дочерние счета):</b>")
        for ch in children:
            mat = ch.get("material_type") or "other"
            label = MATERIAL_TYPE_LABELS.get(mat, mat)
            supplier = html.quote(ch.get("supplier") or "—")
            desc = html.quote((ch.get("description") or "")[:40])
            line = f"  • {label}"
            if supplier and supplier != "—":
                line += f" — {supplier}"
            if desc:
                line += f" ({desc})"
            lines.append(line)

    # Оплаты поставщикам (от ГД)
    if supplier_payments:
        lines.append("")
        lines.append("<b>Оплаты поставщикам:</b>")
        for sp in supplier_payments:
            supplier = html.quote(sp.get("supplier", "—") or "—")
            mat = sp.get("material_type", "")
            mat_label = MATERIAL_TYPE_LABELS.get(mat, mat) if mat else ""
            line = f"  • {supplier}"
            if mat_label:
                line += f" ({mat_label})"
            lines.append(line)

    lines.append("")
    lines.append(f"Всего позиций: {len(children) + len(supplier_payments)}")
    return "\n".join(lines)


def format_rp_expenses(
    inv: dict[str, Any],
    children: list[dict[str, Any]],
    supplier_payments: list[dict[str, Any]],
    viewer_role: Any = None,
) -> str:
    """Расходы по счёту для РП (расширенный доступ — С суммами, БЕЗ маржи)."""
    from .enums import MATERIAL_TYPE_LABELS

    blocked = _accounting_blocked(viewer_role, "Расходы по счёту (себестоимость)")
    if blocked:
        return blocked

    num = html.quote(str(inv.get("invoice_number") or f"#{inv.get('id')}"))
    addr = html.quote(str(inv.get("object_address") or inv.get("address") or "—"))

    lines: list[str] = [
        f"📦 <b>Расходы — Счёт №{num}</b>",
        f"📍 {addr}",
    ]

    if not children and not supplier_payments:
        lines.append("\nНет записей о закупках.")
        return "\n".join(lines)

    materials_total = 0.0

    # Дочерние счета (материалы)
    if children:
        lines.append("")
        lines.append("<b>Материалы (дочерние счета):</b>")
        for ch in children:
            mat = ch.get("material_type") or "other"
            label = MATERIAL_TYPE_LABELS.get(mat, mat)
            supplier = html.quote(ch.get("supplier") or "—")
            try:
                amt = float(ch.get("amount") or 0)
            except (ValueError, TypeError):
                amt = 0.0
            materials_total += amt
            amt_s = f"{amt:,.0f}".replace(",", " ")
            line = f"  • {label}"
            if supplier and supplier != "—":
                line += f" — {supplier}"
            line += f": <b>{amt_s}</b> руб."
            lines.append(line)
        mt_s = f"{materials_total:,.0f}".replace(",", " ")
        lines.append(f"Итого материалов: <b>{mt_s}</b> руб.")

    # Оплаты поставщикам
    sp_total = 0.0
    if supplier_payments:
        lines.append("")
        lines.append("<b>Оплаты поставщикам:</b>")
        for sp in supplier_payments:
            supplier = html.quote(sp.get("supplier", "—") or "—")
            try:
                amt = float(sp.get("amount") or 0)
            except (ValueError, TypeError):
                amt = 0.0
            sp_total += amt
            mat = sp.get("material_type", "")
            mat_label = MATERIAL_TYPE_LABELS.get(mat, mat) if mat else ""
            amt_s = f"{amt:,.0f}".replace(",", " ")
            line = f"  • {supplier}"
            if mat_label:
                line += f" ({mat_label})"
            line += f": <b>{amt_s}</b> руб."
            lines.append(line)
        spt_s = f"{sp_total:,.0f}".replace(",", " ")
        lines.append(f"Итого оплат: <b>{spt_s}</b> руб.")

    grand = materials_total + sp_total
    grand_s = f"{grand:,.0f}".replace(",", " ")
    lines.append("")
    lines.append(f"Всего расходов: <b>{grand_s}</b> руб.")
    lines.append(f"Позиций: {len(children) + len(supplier_payments)}")
    return "\n".join(lines)


def _compute_remaining_to_buy(inv: dict[str, Any]) -> dict[str, Any] | None:
    """«🛒 Осталось закупить» — план Q-T минус факт по 4 категориям.

    Зеркалит логику CA-CE из sheets.py:_invoice_cells (синхронизировано
    2026-06-16 с правилами «предстоящих затрат»). Возвращает None если блок
    показывать не надо: status='ended' или credit полностью закрыт
    (is_credit=1 AND montazh_stage='invoice_end'), либо нет планов вовсе.

    Триггеры обнуления остатка (как в листе CA-CE):
      • Материалы (CA): «Счёт OK» монтажника (montazh_invoice_ok_at) ЛИБО факт
        затрат CF (=материалы + тонировка) > 65% расчётной суммы (user 16.06).
      • Установка (CB): база = сумма СОГЛАСОВАНИЯ (montazh_agreed, fallback расчёт
        _calc_est_montazh — что показывается монтажнику), НЕ валовой estimated_installation
        (user 16.06). Обнуляется при запросе ЗП (requested/approved/confirmed/paid или
        zp_installer_requested_at).
      • Грузчики (CC) / Логистика (CD): внесён факт из «Импорт ОП»
        (loaders_fact_op / logistics_fact_op > 0) — user 16.06.
      • Установка (CB): кроме запроса ЗП — обнуляется при заполненном BS «Монтаж
        Факт» (установка фактически выплачена/закрыта): согласовано И выплачено
        ≥ согласовано (через AN/бот), либо legacy-AN без согласования (mfo>0).
        Зеркалит пост-обнуление листа (sheets.py 1318-1321) — user 16.06 (ч.8).
    ⚠️ Аванс монтажника (CG, installer_advance_offset) в карточке недоступен (он в
       cost-card), поэтому полная выплата ТОЛЬКО авансом без запроса ЗП здесь не
       ловится — edge-case; через AN/бот/запрос ЗП установка обнуляется корректно.
    """
    status = (inv.get("status") or "").lower()
    is_credit = bool(inv.get("is_credit"))
    credit_fully_closed = is_credit and inv.get("montazh_stage") == "invoice_end"
    if status not in ("in_progress", "credit"):
        return None
    if status == "ended" or credit_fully_closed:
        return None

    est_mat = float(inv.get("estimated_materials") or 0)
    est_inst = float(inv.get("estimated_installation") or 0)
    est_load = float(inv.get("estimated_loaders") or 0)
    est_log = float(inv.get("estimated_logistics") or 0)

    fact_mat = (
        float(inv.get("cost_metal") or 0)
        + float(inv.get("cost_glass") or 0)
        + float(inv.get("cost_extra_mat") or 0)
    )
    # fact_inst — как в листе: AN (montazh_fact_op) приоритетнее, затем confirmed ЗП.
    mfo = float(inv.get("montazh_fact_op") or 0)
    zia = float(inv.get("zp_installer_amount") or 0)
    zis = inv.get("zp_installer_status") or ""
    if mfo:
        fact_inst = mfo
    elif zis == "confirmed" and zia > 0:
        fact_inst = zia
    else:
        fact_inst = 0.0
    fact_load = float(inv.get("cost_loaders") or 0) + float(inv.get("loaders_fact_op") or 0)
    fact_log = float(inv.get("cost_logistics") or 0) + float(inv.get("logistics_fact_op") or 0)

    # Материалы закуплены: «Счёт OK» ИЛИ потрачено > 65% расчётной суммы (CF, user 16.06).
    mat_locked = bool(inv.get("montazh_invoice_ok_at"))
    cf_spend = fact_mat + float(inv.get("cost_extra_svc") or 0)
    mat_bought_by_spend = est_mat > 0 and cf_spend > 0.65 * est_mat
    mat_done = mat_locked or mat_bought_by_spend
    inst_zp_requested = (
        zis in ("requested", "approved", "confirmed", "paid")
        or bool(inv.get("zp_installer_requested_at"))
    )

    # «Монтаж»/CB: остаток от суммы СОГЛАСОВАНИЯ (montazh_agreed — что показывается монтажнику
    # и платится за установку), НЕ от валового estimated_installation (user 16.06). Fallback
    # (ещё не согласовано) — расчёт как у монтажника (installer_new._calc_est_montazh).
    inst_credit = is_credit or str(inv.get("invoice_number") or "").upper().startswith("ЗМ")
    inst_agreed = float(inv.get("montazh_agreed_amount") or 0)
    if inst_agreed <= 0 and est_inst > 0:
        coef = 0.95 if inst_credit else 0.67
        base = int((est_inst * coef + 500) // 1000) * 1000
        inst_agreed = base if inst_credit else int((base * 1.10 + 500) // 1000) * 1000

    rem_mat = (0.0 if est_mat > 0 else None) if mat_done else ((est_mat - fact_mat) if est_mat > 0 else None)
    rem_inst = (0.0 if inst_agreed > 0 else None) if inst_zp_requested else ((inst_agreed - fact_inst) if inst_agreed > 0 else None)
    rem_load = (est_load - fact_load) if est_load > 0 else None
    rem_log = (est_log - fact_log) if est_log > 0 else None

    # Грузчики/Логистика: внесён факт «Импорт ОП» → остаток 0 (лист: BR/BU непусто).
    if float(inv.get("loaders_fact_op") or 0) > 0 and est_load > 0:
        rem_load = 0.0
    if float(inv.get("logistics_fact_op") or 0) > 0 and est_log > 0:
        rem_log = 0.0

    # Установка: BS «Монтаж Факт» заполнен → остаток 0 (лист: пост-обнуление CB при
    # непустом BS, sheets.py 1318-1321). BS заполняется, когда установка выплачена/
    # закрыта: согласовано И выплачено ≥ согласовано (канал AN montazh_fact_op или
    # бот при payment_sent/confirmed), ЛИБО legacy-AN без согласования (agreed<=0, mfo>0).
    # Аванс (CG) в карточке недоступен → полная выплата лишь авансом без запроса ЗП
    # здесь не ловится (edge-case; через AN/бот/запрос ЗП — ловится корректно).
    inst_locked = inst_zp_requested
    _m_agreed = float(inv.get("montazh_agreed_amount") or 0)
    _bot_paid = zia if zis in ("payment_sent", "confirmed") else 0.0
    _paid_known = max(mfo, _bot_paid)
    _bs_filled = (_m_agreed > 0 and _paid_known >= _m_agreed - 0.001) or (_m_agreed <= 0 and mfo > 0)
    if _bs_filled and inst_agreed > 0:
        rem_inst = 0.0
        inst_locked = True

    if all(x is None for x in (rem_mat, rem_inst, rem_load, rem_log)):
        return None

    rem_total = sum(x for x in (rem_mat, rem_inst, rem_load, rem_log) if x is not None)

    return {
        "rem_mat": rem_mat,
        "rem_inst": rem_inst,
        "rem_load": rem_load,
        "rem_log": rem_log,
        "total": rem_total,
        "mat_locked": mat_done,
        "inst_locked": inst_locked,
    }


def _render_remaining_to_buy(remaining: dict[str, Any]) -> list[str]:
    """Эталон-блок «🛒 Осталось закупить» (один <pre>). Возвращает list[str]
    для вклейки в большую карточку — контракт сохранён."""
    def _fmt(v: float) -> str:
        sign = "+" if v >= 0 else "−"
        amount = f"{abs(v):,.0f}".replace(",", " ")
        return f"{sign}{amount} ₽"

    cats = [
        ("Материалы", remaining["rem_mat"], remaining["mat_locked"], "✅ закуплено"),
        ("Монтаж", remaining["rem_inst"], remaining["inst_locked"], "✅ согласовано"),
        ("Грузчики", remaining["rem_load"], False, ""),
        ("Логистика", remaining["rem_log"], False, ""),
    ]
    items: list[tuple[str, str]] = []
    for label, val, locked, locked_label in cats:
        if val is None:
            continue
        items.append((label, locked_label if locked else _fmt(val)))
    if not items:
        return []

    block = format_card_section(
        emoji="🛒",
        title="Осталось закупить",
        total=_fmt(remaining["total"]),
        items=items,
        width=32,
    )
    return ["", *block.split("\n")]


def build_purchases_in_work_block(invoices: list[dict[str, Any]]) -> list[str]:
    """Блок «🛒 Закупки по счетам в работе» для «Сводки дня» ГД.

    Для каждого in_progress/credit-открытого счёта показывает таблицу
    план / факт / остаток по 4 категориям + ИТОГО. Внизу — агрегат по портфелю.

    Возвращает пустой список если нет счетов с расчётом или нет планов вовсе.
    """
    def _fmt(v: float) -> str:
        return f"{v:,.0f}".replace(",", " ")

    def _fmt_signed(v: float | int | None) -> str:
        if v is None or not isinstance(v, (int, float)):
            return "—"
        if v == 0:
            return "0"
        sign = "+" if v > 0 else "−"
        return f"{sign}{abs(v):,.0f}".replace(",", " ")

    rows: list[dict[str, Any]] = []
    g_mat_p = g_mat_f = 0.0
    g_inst_p = g_inst_f = 0.0
    g_load_p = g_load_f = 0.0
    g_log_p = g_log_f = 0.0
    g_rem_mat = g_rem_inst = g_rem_load = g_rem_log = 0.0

    for inv in invoices:
        rem = _compute_remaining_to_buy(inv)
        if not rem:
            continue
        p_mat = float(inv.get("estimated_materials") or 0)
        p_inst = float(inv.get("estimated_installation") or 0)
        p_load = float(inv.get("estimated_loaders") or 0)
        p_log = float(inv.get("estimated_logistics") or 0)
        rm = rem.get("rem_mat") if isinstance(rem.get("rem_mat"), (int, float)) else 0
        ri = rem.get("rem_inst") if isinstance(rem.get("rem_inst"), (int, float)) else 0
        rl = rem.get("rem_load") if isinstance(rem.get("rem_load"), (int, float)) else 0
        rg = rem.get("rem_log") if isinstance(rem.get("rem_log"), (int, float)) else 0
        f_mat = p_mat - rm
        f_inst = p_inst - ri
        f_load = p_load - rl
        f_log = p_log - rg

        rows.append({
            "num": inv.get("invoice_number") or f"#{inv.get('id')}",
            "addr": (inv.get("object_address") or "")[:24],
            "status": inv.get("status"),
            "p_mat": p_mat, "f_mat": f_mat, "r_mat": rm,
            "p_inst": p_inst, "f_inst": f_inst, "r_inst": ri,
            "p_load": p_load, "f_load": f_load, "r_load": rl,
            "p_log": p_log, "f_log": f_log, "r_log": rg,
            "total": rem.get("total", 0),
        })
        g_mat_p += p_mat; g_mat_f += f_mat; g_rem_mat += rm
        g_inst_p += p_inst; g_inst_f += f_inst; g_rem_inst += ri
        g_load_p += p_load; g_load_f += f_load; g_rem_load += rl
        g_log_p += p_log; g_log_f += f_log; g_rem_log += rg

    if not rows:
        return []

    out: list[str] = []
    INDENT = "   "
    HEAD = f"{INDENT}{'':<6}{'план':>8}{'факт':>9}{'ост.':>9}"

    def _row(lbl: str, p: float, f: float, r: float | int | None) -> str:
        return f"{INDENT}{lbl:<6}{_fmt(p):>8}{_fmt(f):>9}{_fmt_signed(r):>9}"

    def _table_lines(quad: list[tuple[str, float, float, float]]) -> list[str]:
        rows_out = [HEAD]
        tp = tf = tr = 0.0
        for lbl, p, f, rv in quad:
            rows_out.append(_row(lbl, p, f, rv))
            tp += p
            tf += f
            tr += rv if isinstance(rv, (int, float)) else 0
        rows_out.append(f"{INDENT}{'━' * 32}")
        rows_out.append(_row("Итого", tp, tf, tr))
        return rows_out

    out.append("")
    out.append("<b>🛒  Закупки по счетам в работе</b>")
    out.append(f"<pre>{INDENT}Итого: {len(rows)}</pre>")

    _status_short = {"in_progress": "🟡 в работе", "credit": "🏦 кредит"}
    for r in rows:
        st = _status_short.get(r["status"], r["status"] or "")
        header = f"<b>🧾  {r['num']}"
        if st:
            header += f" · {st}"
        header += "</b>"
        out.append("")
        out.append(header)
        out.append("<pre>")
        if r["addr"]:
            out.append(f"{INDENT}{r['addr']}")
        out.extend(_table_lines([
            ("Мат", r["p_mat"], r["f_mat"], r["r_mat"]),
            ("Мон", r["p_inst"], r["f_inst"], r["r_inst"]),
            ("Груз", r["p_load"], r["f_load"], r["r_load"]),
            ("Лог", r["p_log"], r["f_log"], r["r_log"]),
        ]))
        out.append("</pre>")

    out.append("")
    out.append(f"<b>📊  Итого по {len(rows)} счетам</b>")
    out.append("<pre>")
    out.extend(_table_lines([
        ("Мат", g_mat_p, g_mat_f, g_rem_mat),
        ("Мон", g_inst_p, g_inst_f, g_rem_inst),
        ("Груз", g_load_p, g_load_f, g_rem_load),
        ("Лог", g_log_p, g_log_f, g_rem_log),
    ]))
    out.append("</pre>")
    return out


def build_purchases_summary_line(invoices: list[dict[str, Any]]) -> list[str]:
    """Короткая сводка «🛒 Закупки» — ИТОГО по портфелю одним блоком.

    Используется в «Сводке дня» ГД. Подробности по каждому счёту — отдельным
    сообщением через callback кнопки «🛒 Подробнее по закупкам».
    Возвращает пустой список если нет счетов с расчётом.
    """
    rows_count = 0
    g_plan = 0.0
    g_rem = 0.0
    for inv in invoices:
        rem = _compute_remaining_to_buy(inv)
        if not rem:
            continue
        rows_count += 1
        p_mat = float(inv.get("estimated_materials") or 0)
        p_inst = float(inv.get("estimated_installation") or 0)
        p_load = float(inv.get("estimated_loaders") or 0)
        p_log = float(inv.get("estimated_logistics") or 0)
        g_plan += p_mat + p_inst + p_load + p_log
        g_rem += float(rem.get("total", 0) or 0)
    if rows_count == 0:
        return []
    g_fact = g_plan - g_rem

    def _fmt(v: float) -> str:
        return f"{v:,.0f}".replace(",", " ")

    sign = "+" if g_rem > 0 else ("-" if g_rem < 0 else "")
    return [
        "",
        f"🛒 <b>Закупки по {rows_count} счетам в работе</b>",
        f"  План: {_fmt(g_plan)} ₽ · Факт: {_fmt(g_fact)} ₽ · Остаток: <b>{sign}{_fmt(abs(g_rem))} ₽</b>",
    ]


def format_cost_card(inv: dict[str, Any], cost: dict[str, Any], viewer_role: Any = None) -> str:
    """HTML-карточка себестоимости для Telegram.

    ⛔ Стоячий гард viewer_role: роль «Бухгалтерия» не видит себестоимость и
    прибыль/маржу ([[feedback_accounting_no_cost_profit_visibility]]). Дефолт None
    = ГД/менеджер/Sheets без изменений.
    """
    from .enums import MATERIAL_TYPE_LABELS

    blocked = _accounting_blocked(viewer_role, "Себестоимость и прибыль")
    if blocked:
        return blocked

    num = html.quote(str(inv.get("invoice_number") or f"#{inv.get('id')}"))
    addr = html.quote(str(inv.get("object_address") or inv.get("address") or "—"))

    inv_amount = cost.get("invoice_amount", 0)
    inv_amount_s = f"{inv_amount:,.0f}".replace(",", " ")

    debt = float(inv.get("outstanding_debt") or 0)
    first_pay = float(inv.get("first_payment_amount") or 0)

    lines: list[str] = [
        f"📊 <b>Себестоимость — Счёт №{num}</b>",
        f"📍 {addr}",
        "",
        f"💰 Сумма счёта: <b>{inv_amount_s}</b> руб.",
    ]
    if first_pay > 0:
        lines.append(f"💵 Оплачено: {first_pay:,.0f} руб.")
    if debt > 0:
        lines.append(f"🔴 Долг клиента: <b>{debt:,.0f}</b> руб.")

    # --- Материалы ---
    materials_by_type: dict[str, float] = cost.get("materials_by_type", {})
    materials_total = cost.get("materials_total", 0)
    materials_fact_op = cost.get("materials_fact_op", 0)
    materials_combined = cost.get("materials_combined", 0)

    if materials_fact_op or materials_by_type:
        lines.append("")
        lines.append("📦 <b>Материалы:</b>")
        if materials_fact_op:
            lines.append(f"  ├ Закуплено (ОП): {materials_fact_op:,.0f} руб.")
        if materials_by_type:
            items = sorted(materials_by_type.items(), key=lambda x: -x[1])
            for mat, amt in items:
                label = MATERIAL_TYPE_LABELS.get(mat, mat)
                lines.append(f"  ├ {label}: {amt:,.0f} руб.")
        lines.append(f"  └ <b>Итого материалов: {materials_combined:,.0f} руб.</b>")

    # --- Монтаж ---
    montazh_fact_op = cost.get("montazh_fact_op", 0)
    montazh_combined = cost.get("montazh_combined", 0)
    zp_inst_for_display = cost.get("zp_installer", 0)
    if montazh_fact_op or zp_inst_for_display:
        lines.append("")
        lines.append("🔨 <b>Монтаж:</b>")
        if montazh_fact_op:
            lines.append(f"  ├ Оплачено (ОП): {montazh_fact_op:,.0f} руб.")
        if zp_inst_for_display:
            lines.append(f"  ├ ЗП монтажник: {zp_inst_for_display:,.0f} руб.")
        lines.append(f"  └ <b>Итого монтаж: {montazh_combined:,.0f} руб.</b>")

    # --- Оплаты поставщикам ---
    sp_list: list[dict[str, Any]] = cost.get("supplier_payments_list", [])
    sp_total = cost.get("supplier_payments_total", 0)
    if sp_list:
        lines.append("")
        lines.append("💸 <b>Оплаты поставщикам:</b>")
        for idx, sp in enumerate(sp_list):
            supplier = html.quote(sp.get("supplier", "—") or "—")
            prefix = "  └" if idx == len(sp_list) - 1 else "  ├"
            lines.append(f"{prefix} {supplier}: {sp['amount']:,.0f} руб.")
        lines.append(f"Итого оплат: <b>{sp_total:,.0f}</b> руб.")

    # --- Зарплаты ---
    zp_zamery = cost.get("zp_zamery", 0)
    zp_manager = cost.get("zp_manager", 0)
    zp_installer = cost.get("zp_installer", 0)
    zp_total = cost.get("zp_total", 0)
    if zp_total > 0:
        lines.append("")
        lines.append("💰 <b>Зарплаты:</b>")
        zp_items = [
            ("Замерщик", zp_zamery),
            ("Монтажник", zp_installer),
            ("Отд.Продаж", zp_manager),
        ]
        zp_items = [(n, v) for n, v in zp_items if v > 0]
        for idx, (name, val) in enumerate(zp_items):
            prefix = "  └" if idx == len(zp_items) - 1 else "  ├"
            lines.append(f"{prefix} {name}: {val:,.0f} руб.")
        lines.append(f"Итого ЗП: <b>{zp_total:,.0f}</b> руб.")

    # --- Итого: сверка маржи (user 2026-06-17, хвост #3) ---
    # Расходы (BG = BR+BS+BT+BU+АМ) + явные вычеты НДС/налог/НПН (+ долг), чтобы
    # «Сумма − всё = МАРЖА» сходилось визуально. Display-only: значения из
    # cost-dict (db-first, считаются в get_full_invoice_cost_card). Прежде
    # печатался total_cost (старая база НДС/налога) — он не сходился с маржой.
    bg_cost = cost.get("bg_cost", 0)
    nds_fact = cost.get("nds_fact", 0)
    profit_tax_fact = cost.get("profit_tax_fact", 0)
    npn_10pct = cost.get("npn_10pct", float(inv.get("npn_amount") or 0))
    margin = cost.get("margin", 0)
    margin_pct = cost.get("margin_pct", 0)
    lines.append("")
    lines.append("═══════════════════")
    lines.append(f"📊 <b>Расходы (BG):</b> {bg_cost:,.0f} руб.".replace(",", " "))
    if nds_fact:
        lines.append(f"🧾 НДС: −{nds_fact:,.0f} руб.".replace(",", " "))
    if profit_tax_fact:
        lines.append(f"🏛 Налог на прибыль: −{profit_tax_fact:,.0f} руб.".replace(",", " "))
    if npn_10pct:
        lines.append(f"💼 НПН 10%: −{npn_10pct:,.0f} руб.".replace(",", " "))
    if debt > 0:
        lines.append(f"🔴 минус долг клиента: −{debt:,.0f} руб.".replace(",", " "))
    if inv_amount > 0:
        lines.append(f"📈 <b>МАРЖА:</b> {margin:,.0f} руб. ({margin_pct:.1f}%)".replace(",", " "))

    remaining = _compute_remaining_to_buy(inv)
    if remaining is not None:
        lines.extend(_render_remaining_to_buy(remaining))

    return "\n".join(lines)


def format_plan_fact_card(
    inv: dict[str, Any],
    pf: dict[str, Any],
    role: str = "gd",
) -> str:
    """HTML-карточка «План / Факт». role='rp' — упрощённая (без прибыли/себестоимости).

    ⛔ role=Role.ACCOUNTING → отказ: бухгалтер не видит план/факт-прибыль
    ([[feedback_accounting_no_cost_profit_visibility]]). gd/rp — без изменений.
    """
    blocked = _accounting_blocked(role, "Карточка «План/Факт»")
    if blocked:
        return blocked

    inv_number = inv.get("invoice_number") or "—"
    amount = float(inv.get("amount") or 0)

    est_glass = pf.get("estimated_glass", 0)
    est_profile = pf.get("estimated_profile", 0)
    est_mat_legacy = pf.get("estimated_materials_legacy", 0)
    materials_total = pf.get("materials_total", est_glass + est_profile + est_mat_legacy)
    est_inst = pf.get("estimated_installation", 0)
    est_load = pf.get("estimated_loaders", 0)
    est_log = pf.get("estimated_logistics", 0)
    est_total = pf.get("estimated_total_cost", 0)
    output_vat = pf.get("output_vat", 0)
    input_vat = pf.get("input_vat", 0)
    net_vat = pf.get("net_vat", 0)
    est_profit = pf.get("estimated_profit", 0)
    est_pct = pf.get("estimated_profitability", 0)

    cost = pf.get("cost_card", {})
    # Группировка supplier_payments по правильным категориям (Hunk 2).
    # Раньше всё кроме 'service' падало в материалы (default "mat"),
    # из-за чего logistics/loaders/montazh supplier_payments искажали материалы.
    _sp_svc = 0.0   # → установка
    _sp_load = 0.0  # → грузчики
    _sp_log = 0.0   # → логистика
    _SP_CAT = {"profile": "mat", "glass": "mat", "ldsp": "mat",
               "gkl": "mat", "sandwich": "mat", "other": "mat",
               "metal": "mat", "extra_mat": "mat",
               "service": "svc", "montazh": "svc", "extra_svc": "svc",
               "loaders": "loaders",
               "logistics": "logistics"}
    for _sp in cost.get("supplier_payments_list", []):
        cat = _SP_CAT.get(_sp.get("material_type", "other"), "mat")
        amt = _sp.get("amount", 0)
        if cat == "svc":
            _sp_svc += amt
        elif cat == "loaders":
            _sp_load += amt
        elif cat == "logistics":
            _sp_log += amt
        # mat — уже учтён внутри mat_and_suppliers (db.py max-защита от дубля),
        # отдельно не суммируем (Hunk 1).
    # Hunk 1: использовать mat_and_suppliers (защита от дубля в db.py)
    fact_mat = cost.get("mat_and_suppliers", cost.get("materials_combined", 0))
    # Hotfix 2026-05-28: max() против дубля для install/loaders/logistics.
    # cost_* fallback в db.py уже агрегирует supplier_payments по supplier-mapping,
    # _sp_*  — те же платежи через material_type. Без max() сумма → 2×.
    fact_inst = max(
        cost.get("montazh_combined", float(cost.get("zp_installer", 0))),
        _sp_svc,
    )
    fact_load = max(cost.get("loaders_fact", 0), _sp_load)
    fact_log = max(cost.get("logistics_fact", 0), _sp_log)
    fact_total = pf.get("actual_total_cost", 0)
    fact_profit = pf.get("actual_profit", 0)
    fact_pct = pf.get("actual_profitability", 0)

    # --- РП: упрощённая карточка (только План + Факт, без Δ) ---
    if role == "rp":
        fact_glass = pf.get("fact_glass", 0)
        fact_metal = pf.get("fact_metal", 0)

        def _fv(v: float) -> str:
            return f"{v:>10,.0f}" if v else f"{'—':>10s}"

        lines = [
            f"📊 <b>План / Факт</b> — Счёт №{inv_number}",
            f"💰 Сумма: {amount:,.0f}₽\n",
            "<pre>",
            f"{'':14s} {'План':>10s} {'Факт':>10s}",
            f"{'Материалы':14s} {materials_total:>10,.0f} {_fv(fact_mat)}",
            f"{'Установка':14s} {est_inst:>10,.0f} {_fv(fact_inst)}",
            f"{'Грузчики':14s} {est_load:>10,.0f} {_fv(fact_load)}",
            f"{'Логистика':14s} {est_log:>10,.0f} {_fv(fact_log)}",
            "</pre>",
        ]
        return "\n".join(lines)

    # --- ГД: полная карточка (План + Факт + Δ + прибыль) ---
    def _delta(plan: float, fact: float, invert: bool = False) -> str:
        # owner 27.06: для строк затрат/налогов показываем (план − факт):
        # перерасход (факт>план) → отрицательное, экономия (факт<план) → положительное.
        # Для прибыли (invert=True) — натуральный знак (факт−план): больше прибыли = «+».
        # Иконка ✅/⚠️ считается по сырому отклонению (raw), знак числа — отдельно.
        raw = fact - plan
        if abs(raw) < 0.5:
            return "     0 ✅"
        d = raw if invert else -raw
        sign = "+" if d > 0 else ""
        ok = (raw <= 0) if not invert else (raw >= 0)
        icon = "✅" if ok else "⚠️"
        return f"{sign}{d:,.0f} {icon}"

    def _row(
        label: str, plan: float, fact: float, invert: bool = False,
        hide_plan: bool = False,
    ) -> str:
        """Строка план/факт. hide_plan=True — для строк, где план не имеет смысла
        для visual consistency (Налог приб., НПН 10%): est_profit от них не зависит,
        потому показывать «План» вводит в заблуждение (см. ТЗ 2026-05-19 A.6, вариант B).
        """
        if hide_plan:
            if not fact:
                return f"{label:14s} {'—':>10s} {'—':>10s} {'':>12s}"
            return f"{label:14s} {'—':>10s} {fact:>10,.0f} {'':>12s}"
        if not fact:
            return f"{label:14s} {plan:>10,.0f} {'—':>10s} {'':>12s}"
        return f"{label:14s} {plan:>10,.0f} {fact:>10,.0f} {_delta(plan, fact, invert):>12s}"

    lines = [
        f"📊 <b>План / Факт</b> — Счёт №{inv_number}",
        f"💰 Сумма: {amount:,.0f}₽\n",
        "<pre>",
        f"{'':14s} {'План':>10s} {'Факт':>10s} {'Δ':>12s}",
    ]
    # Прибыль факт считаем только если есть факт материалов И установки
    _has_key_facts = bool(fact_mat) and bool(fact_inst)

    # ТЗ 2026-05-19 A.6: показываем НДС/Налог приб./НПН строками между
    # Себест-ть и Прибыль — чтобы визуально Сумма − Себест-ть − НДС − Налог − НПН = Прибыль.
    # 2026-05-28: для кредитных счетов налоги не считаются (см. db.py:get_full_invoice_cost_card)
    # и не показываются в карточке (запрос user'а).
    is_credit_inv = bool(inv.get("is_credit"))
    # План НДС/Налог приб. — зеркало листа Invoices (V/W): НДС = выходной минус
    # входной по материалам ((Сумма − Материалы)·22/122, как sheets.py _nds),
    # налог = 20% от (Сумма − Себест − НДС). owner 27.06.
    nds_p = 0.0 if is_credit_inv else (((amount - materials_total) * 22 / 122) if amount else 0.0)
    nds_f = 0.0 if is_credit_inv else (cost.get("nds_fact", 0) or 0)
    tax_p = 0.0 if is_credit_inv else (max(0.0, (amount - est_total - nds_p) * 0.20) if amount else 0.0)
    tax_f = 0.0 if is_credit_inv else (cost.get("profit_tax_fact", 0) or 0)
    npn_p = 0.0 if is_credit_inv else float(inv.get("npn_amount") or 0)
    npn_f = npn_p

    lines += [
        _row("Материалы", materials_total, fact_mat),
        _row("Установка", est_inst, fact_inst),
        _row("Грузчики", est_load, fact_load),
        _row("Логистика", est_log, fact_log),
        f"{'─' * 50}",
        _row("Себест-ть", est_total, fact_total if _has_key_facts else 0),
    ]
    if not is_credit_inv:
        lines.append(_row("НДС", nds_p, nds_f if _has_key_facts else 0))
        # owner 27.06: План Налог/НПН показываем (зеркало листа Invoices W/AP).
        # Ранее были скрыты (A.6 вариант B, hide_plan) — отменено по запросу owner.
        lines.append(_row("Налог приб.", tax_p, tax_f if _has_key_facts else 0))
        if npn_p > 0 or npn_f > 0:
            lines.append(_row("НПН 10%", npn_p, npn_f if _has_key_facts else 0))
    lines += [
        f"{'─' * 50}",
        _row("Прибыль", est_profit, fact_profit if _has_key_facts else 0, invert=True),
        f"{'Рент-ть':14s} {est_pct:>9.1f}% " + (f"{fact_pct:>9.1f}%" if _has_key_facts else ""),
    ]
    # BM — Перерасчёт прибыли. Если owner проставил «Разница себест. расч-факт»
    # (CO/cost_diff_calc_fact) и долг погашен — берём ЕГО значение (механизм
    # перерасчёта, owner 23.06); иначе авто (факт−план) при полных факт-данных.
    _co = float(inv.get("cost_diff_calc_fact") or 0)
    _no_debt = abs(float(inv.get("outstanding_debt") or 0)) < 1
    if _co and _no_debt:
        lines.append(f"{'Перерасчёт':14s} {_co:>+10,.0f}")
    elif _has_key_facts:
        recalc = fact_profit - est_profit
        if abs(recalc) > 2000:
            lines.append(f"{'Перерасчёт':14s} {recalc:>+10,.0f}")
    # Profit split (inside <pre>)
    client_source = pf.get("client_source", "own")
    rp_zp = pf.get("rp_zp", 0)
    gd_pr = pf.get("gd_profit", 0)
    src_label = "📋 Лид ГД (75/25)" if client_source == "gd_lead" else "👤 Клиент менеджера (50/50)"

    if pf.get("has_estimated") and est_profit > 0:
        lines.append(f"{'─' * 50}")
        lines.append(src_label)
        lines.append(f"{'Распределение прибыли:'}")
        lines.append(f"{'  ЗП РП (10%)':14s} {rp_zp:>10,.0f}₽")
        # ЗП менеджера: источник = AJ (manager_zp_blank) через единый net-payout
        # с CN-гейтом (owner 27.06: «Правильно: aj», НЕ расчётный сплит pf.manager_zp).
        mgr_payout = manager_zp_net_payout(inv)
        lines.append(f"{'  ЗП менеджер':14s} {mgr_payout:>10,.0f}₽")
        _cn = float(inv.get("zp_manager_hold") or 0)
        if _cn and abs(float(inv.get("outstanding_debt") or 0)) < 1:
            lines.append(f"{'  (удержано CN)':14s} {_cn:>+10,.0f}₽")
            # Сколько удержания уже ушло в авансовый кошелёк менеджера (30.07):
            # раньше это состояние жило только в БД. Display-only.
            _adv = float(inv.get("zp_hold_advanced") or 0)
            if _adv > 0:
                lines.append(f"{'  (в аванс)':14s} {_adv:>10,.0f}₽")
        lines.append(f"{'  Доля ГД':14s} {gd_pr:>10,.0f}₽")

    lines.append("</pre>")

    remaining = _compute_remaining_to_buy(inv)
    if remaining is not None:
        lines.extend(_render_remaining_to_buy(remaining))

    # ZP status — разрешается при status='ended' AND перерасход ≤ 10 000 ₽.
    # Для всех остальных (pending/in_progress/paid/credit) — блокировка.
    inv_status = inv.get("status", "")
    if pf.get("has_estimated"):
        if inv_status != "ended":
            lines.append(
                "\n🔒 ЗП менеджера: <b>Заблокирована</b> "
                "(счёт ещё не «Счёт End»)"
            )
        elif pf.get("zp_allowed"):
            delta = pf.get("cost_delta", 0)
            if delta > 0:
                lines.append(
                    f"\n✅ ЗП менеджера: <b>Разрешена</b> "
                    f"(перерасход {delta:+,.0f}₽ ≤ допуск 10 000₽)"
                )
            else:
                lines.append("\n✅ ЗП менеджера: <b>Разрешена</b> (факт ≤ план)")
        else:
            delta = pf.get("cost_delta", 0)
            lines.append(
                f"\n❌ ЗП менеджера: <b>Заблокирована</b>\n"
                f"    Перерасход: {delta:+,.0f}₽ (> допуска 10 000₽)"
            )
    else:
        lines.append("\n⚠️ Расчётные данные не заполнены")

    return "\n".join(lines)


def format_invoice_end_financials(inv: dict[str, Any], pf: dict[str, Any]) -> str:
    """Display-only финансовый блок для карточки «Счёт End» у ГД (PART B, ТЗ 19.06).

    Справочный блок: Себест-ть расч/факт, Прибыль расч/факт, ЗП менеджера (расч)
    + ставка (👤 свой 50% / 📋 лид ГД 25%). Источник — db.get_plan_fact_card();
    деньги/кредит/лист НЕ трогает (только чтение готовых значений).

    «Факт себест.» = total_cost — единообразно с format_plan_fact_card
    (выбор owner 19.06: одно и то же число на всех ГД-карточках важнее, чем
    арифметическая сводимость с «Прибыль факт», которая считается от bg_cost).

    ⚠️ Только для ГД/ТД — содержит прибыль/себестоимость/ЗП-распределение, скрытые
    от РП (см. format_plan_fact_card role='rp'). Вызывающий код обязан гейтить роль.

    Возвращает "" если расчётные данные не заполнены (нечего показывать).
    """
    if not pf.get("has_estimated"):
        return ""

    def _f(v: Any) -> str:
        try:
            return f"{float(v or 0):,.0f}".replace(",", " ")
        except (ValueError, TypeError):
            return "—"

    est_total = pf.get("estimated_total_cost", 0) or 0
    fact_total = pf.get("actual_total_cost", 0) or 0
    est_profit = pf.get("estimated_profit", 0) or 0
    fact_profit = pf.get("actual_profit", 0) or 0
    manager_zp = pf.get("manager_zp", 0) or 0
    client_source = pf.get("client_source", "own")

    # Гейт «факт готов» — зеркало _has_key_facts из format_plan_fact_card: факт
    # показываем только при наличии факт-материалов И факт-установки, иначе «—»
    # (частичные данные на этапе закрытия не вводят в заблуждение).
    cost = pf.get("cost_card", {}) or {}
    _sp_svc = 0.0
    for _sp in cost.get("supplier_payments_list", []) or []:
        if _sp.get("material_type") in ("service", "montazh", "extra_svc"):
            _sp_svc += _sp.get("amount", 0) or 0
    fact_mat = cost.get("mat_and_suppliers", cost.get("materials_combined", 0)) or 0
    fact_inst = max(
        cost.get("montazh_combined", float(cost.get("zp_installer", 0) or 0)) or 0,
        _sp_svc,
    )
    has_fact = bool(fact_mat) and bool(fact_inst)

    fact_total_s = f"{_f(fact_total)}₽" if has_fact else "—"
    fact_profit_s = f"{_f(fact_profit)}₽" if has_fact else "—"
    src = "📋 лид ГД 25%" if client_source == "gd_lead" else "👤 свой 50%"

    # Эталон-секция <pre> (ТЗ 24.06 разд.12: финблок «Счёт End» → card-template-standard).
    # Контент сохранён: Себест-ть/Прибыль «расч N / факт N», ЗП менеджера (расч) + ставка.
    fin_items: list[tuple[str, str]] = [
        ("Себест-ть", f"расч {_f(est_total)} / факт {fact_total_s}"),
        ("Прибыль", f"расч {_f(est_profit)} / факт {fact_profit_s}"),
    ]
    # При расч. прибыли ≤ 0 распределять нечего (зеркало format_plan_fact_card,
    # который скрывает блок распределения при est_profit ≤ 0) — не показываем
    # отрицательную «зарплату».
    if est_profit and float(est_profit) > 0:
        fin_items.append(("ЗП менеджера (расч)", f"{_f(manager_zp)}₽ ({src})"))
    else:
        fin_items.append(("ЗП менеджера (расч)", "— (нет расч. прибыли)"))

    return format_card_section("📊", "Финансы (справочно)", fin_items, width=44, compact=True)


def credit_zp_montazh_unpaid(inv: dict[str, Any]) -> bool:
    """owner 2026-07-03: КРЕДИТНЫЙ счёт нельзя закрывать в «Счет End», пока ЗП
    монтажнику НЕ выплачена — до выплаты счёт остаётся «Счет ОК».

    True  → закрытие нужно ЗАБЛОКИРОВАТЬ (кредит + ЗП монтаж заявлена/согласована,
            но платёж НЕ отправлен).
    False → не блокируем: не кредит, ЗП уже выплачена (payment_sent/confirmed),
            неактуальна (not_applicable) либо обязательства по ЗП монтаж нет.
    Скоуп: только кредит (у б/н закрытие идёт через свои условия no_debts/ЭДО).
    """
    if not inv:
        return False
    is_credit = bool(inv.get("is_credit")) or str(
        inv.get("invoice_number") or ""
    ).upper().startswith("ЗМ")
    if not is_credit:
        return False
    st = str(inv.get("zp_installer_status") or "")
    if st in ("payment_sent", "confirmed", "not_applicable"):
        return False
    # Обязательство по ЗП монтаж есть, если сумма согласована ИЛИ заявка в процессе.
    owed = (float(inv.get("montazh_agreed_amount") or 0) > 0) or st in ("requested", "approved")
    return owed


def format_manager_recalc_card(inv: dict[str, Any], pf: dict[str, Any] | None = None) -> str:
    """Компактная карточка «Перерасчёт прибыли» по счёту с переплатой ЗП менеджера.

    owner 2026-07-03: свести к ОДНОМУ блоку из 3 подписанных полей —
    «№ счёта / Объект / Сумма», убрать блоки себестоимости / прибыли / ЗП
    («объединить в одну карточку + убрать лишнее»). Сумма = abs(zp_manager_hold)
    (CN, «переплата ЗП менеджера») — именно она уходит менеджеру в авансовый
    кошелёк при согласии (recalc_agree). Display-only, деньги/лист НЕ трогает.

    Ранее (owner 23.06) карточка показывала полный расчёт себестоимость/прибыль/ЗП;
    03.07 owner попросил ужать. Параметр pf сохранён для совместимости вызовов
    (get_plan_fact_card), в компактной карточке не используется.
    """
    num = inv.get("invoice_number") or "—"
    addr = inv.get("object_address") or "—"
    cn = float(inv.get("zp_manager_hold") or 0)   # CN «переплата ЗП мен.» (знак −)
    amount = abs(cn)

    def _m(v: float) -> str:
        return f"{v:,.0f}".replace(",", " ") + " ₽"

    body_lines = [
        "📊 Перерасчёт прибыли",
        f"№ счёта: {num}",
        f"Объект: {addr}",
        f"Сумма: {_m(amount)}",
    ]
    # Статус переноса (30.07): до этого zp_hold_advanced не было видно нигде,
    # кроме БД — нельзя было понять, отработал ли механизм по счёту. Строки
    # появляются ТОЛЬКО когда перенос уже был, иначе карточка остаётся ровно
    # трёхполевой, как просил owner 03.07.
    advanced = float(inv.get("zp_hold_advanced") or 0)
    if advanced > 0:
        body_lines.append(f"Перенесено: {_m(min(advanced, amount))}")
        body_lines.append(f"Остаток: {_m(max(0.0, amount - advanced))}")
    return "<pre>" + "\n".join(body_lines) + "</pre>"


# ТЗ 2026-05-19 блок A.1/A.2: единый источник «Установка факт» + расширенные ZP-статусы.
# Все три статуса (approved/payment_sent/confirmed) считаются «факт оплачено».
ZP_FACT_STATUSES = ("approved", "payment_sent", "confirmed")
ZP_INSTALLER_PAID_STATUSES = ZP_FACT_STATUSES  # alias по ТЗ


def fact_installation(inv: dict[str, Any]) -> float:
    """BS=AN правило (2026-05-18): montazh_fact_op (AN из «Импорт ОП») приоритет.
    Fallback на zp_installer_amount если статус в ZP_INSTALLER_PAID_STATUSES.
    Единственный источник правды для «Установка факт» во всех меню/карточках."""
    mfo = float(inv.get("montazh_fact_op") or 0)
    if mfo > 0:
        return mfo
    if inv.get("zp_installer_status") in ZP_INSTALLER_PAID_STATUSES:
        return float(inv.get("zp_installer_amount") or 0)
    return 0.0


def format_inwork_summary(invoices: list[dict[str, Any]]) -> str:
    """Сводная карточка счетов в работе — агрегация план/факт."""
    cnt = len(invoices)
    total_amount = sum(float(inv.get("amount") or 0) for inv in invoices)
    total_debt = sum(float(inv.get("outstanding_debt") or 0) for inv in invoices)

    est_mat = sum(float(inv.get("estimated_glass") or 0)
                  + float(inv.get("estimated_profile") or 0)
                  + float(inv.get("estimated_materials") or 0) for inv in invoices)
    est_inst = sum(float(inv.get("estimated_installation") or 0) for inv in invoices)
    est_load = sum(float(inv.get("estimated_loaders") or 0) for inv in invoices)
    est_log = sum(float(inv.get("estimated_logistics") or 0) for inv in invoices)
    est_total = est_mat + est_inst + est_load + est_log

    fact_mat = sum(
        float(inv.get("materials_fact_op") or 0)
        or sum(float(inv.get(f) or 0) for f in ("cost_metal", "cost_glass", "cost_extra_mat"))
        for inv in invoices
    )
    fact_inst = sum(fact_installation(inv) for inv in invoices)
    fact_load = sum(float(inv.get("loaders_fact_op") or 0) for inv in invoices)
    fact_log = sum(float(inv.get("logistics_fact_op") or 0) for inv in invoices)
    # ТЗ 2026-05-19 A.4: agent_payout_op включён в итого, чтобы «Итого затрат»
    # в сводке «В работе» совпадал с детальной карточкой счёта.
    fact_agent = sum(
        float(inv.get("agent_payout_op") or inv.get("agent_fee") or 0)
        for inv in invoices
    )
    fact_total = fact_mat + fact_inst + fact_load + fact_log + fact_agent

    def _f(v: float) -> str:
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.1f}м"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.0f}к"
        return f"{v:.0f}"

    def _fv(v: float) -> str:
        return _f(v) if v else "—"

    lines = [
        f"📊 <b>В работе — сводка</b> ({cnt} счетов)",
        f"💰 Сумма: {total_amount:,.0f}₽",
        f"🔴 Долг: {total_debt:,.0f}₽\n",
        "<pre>",
        f"{'':14s} {'План':>8s} {'Факт':>8s}",
        f"{'Материалы':14s} {_f(est_mat):>8s} {_fv(fact_mat):>8s}",
        f"{'Установка':14s} {_f(est_inst):>8s} {_fv(fact_inst):>8s}",
        f"{'Грузчики':14s} {_f(est_load):>8s} {_fv(fact_load):>8s}",
        f"{'Логистика':14s} {_f(est_log):>8s} {_fv(fact_log):>8s}",
        f"{'─' * 24}",
        f"{'Итого затрат':14s} {_f(est_total):>8s} {_fv(fact_total):>8s}",
        "</pre>",
    ]
    return "\n".join(lines)


def format_inwork_remaining(invoices: list[dict[str, Any]]) -> str:
    """Карточка «Осталось закупить» — сумма остатков по портфелю (3 категории).
    Используется в главном меню ГД → «📊 Счета в работе».

    Агрегирует _compute_remaining_to_buy по каждому счёту (с обнулениями
    «предстоящих затрат» и базой установки = сумма согласования), а не сырое
    план−факт — синхронизировано с листом/карточками 2026-06-16.

    Разбивка задана owner'ом 11.08 через колонки листа, где CE — итог, а CA-CD —
    четыре категории остатка (sheets.py:1211-1218):
        Материал  = CE − CB − CC − CD = CA           (_rem_mat)
        Установка = CE − CA − CC − CD = CB           (_rem_inst)
        Услуги    = CE − CA − CB      = CC + CD      (_rem_load + _rem_log)
    То есть грузчики и логистика сведены в одну строку «Услуги»; сами величины
    и итог прежние, меняется только группировка показа. Счета берём ВСЕ, что
    пришли в списке (включая кредитные) — «показывать все счета из CE».
    """
    cnt = len(invoices)
    rem_mat = rem_inst = rem_load = rem_log = 0.0
    for inv in invoices:
        r = _compute_remaining_to_buy(inv)
        if not r:
            continue
        rem_mat += r["rem_mat"] or 0
        rem_inst += r["rem_inst"] or 0
        rem_load += r["rem_load"] or 0
        rem_log += r["rem_log"] or 0
    rem_svc = rem_load + rem_log
    rem_total = rem_mat + rem_inst + rem_svc

    def _fmt(v: float) -> str:
        sign = "+" if v >= 0 else "−"
        amount = f"{abs(v):,.0f}".replace(",", " ")
        return f"{sign}{amount} ₽"

    return format_card_section(
        emoji="🛒",
        title="Осталось закупить",
        total=_fmt(rem_total),
        items=[
            ("Материал", _fmt(rem_mat)),
            ("Установка", _fmt(rem_inst)),
            ("Услуги", _fmt(rem_svc)),
        ],
        footer=("Счетов в работе", str(cnt)),
        width=32,
    )


_MONTH_NAMES = {
    "01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
    "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
    "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь",
}


def format_monthly_ended_summary(months: list[dict[str, Any]]) -> str:
    """Сводная карточка ended-счетов с разбивкой по месяцам (табличный формат)."""
    if not months:
        return "✅ Закрытых счетов нет."

    def _f(v: float) -> str:
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.1f}м"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.0f}к"
        return f"{v:.0f}"

    total_cnt = sum(m["cnt"] for m in months)
    total_amount = sum(m["total_amount"] for m in months)

    lines = [
        f"📊 <b>Счета end — сводка</b> (всего: {total_cnt})",
        f"💰 Общая сумма: {total_amount:,.0f}₽\n",
    ]

    for m in months:
        month_str = m["month"]  # "2026-03"
        mm = month_str[5:7] if len(month_str) >= 7 else "?"
        month_name = _MONTH_NAMES.get(mm, month_str)

        est_cost = m["est_materials"] + m["est_installation"] + m["est_loaders"] + m["est_logistics"]
        fact_cost = m["fact_materials"] + m["fact_montazh"] + m["fact_loaders"] + m["fact_logistics"]
        agent = m.get("agent_payout") or 0

        # Налоги: НДС = (сумма − материалы) × 22/122, налог на прибыль = (сумма − расходы − НДС) × 20%
        # Прибыль = сумма − расходы − ЗП мен. − НДС − налог (как BL, после ЗП мен.)
        # ТЗ 2026-05-19 A.5: zp_manager включён в total_expenses (иначе прибыль завышена).
        amt = m["total_amount"]
        total_expenses = fact_cost + agent + float(m.get("zp_manager") or 0)
        nds = (amt * 22 / 122) - (m["fact_materials"] * 22 / 122) if amt else 0
        profit_tax = max(0, (amt - total_expenses - nds) * 0.20) if amt else 0
        taxes_total = nds + profit_tax
        profit = amt - total_expenses - taxes_total

        # Расчётная прибыль = сумма − плановые расходы − агент (зп/налогов нет в смете)
        est_profit = amt - est_cost - agent if amt else 0
        debt = float(m.get("total_debt") or 0)

        lines.append(f"<b>{month_name} {month_str[:4]}</b> — {m['cnt']} счетов")
        lines.append("<pre>")
        lines.append(f"{'Сумма счетов':14s} {_f(m['total_amount']):>8s}")
        lines.append(f"{'Долги':14s} {_f(debt):>8s}")
        lines.append(f"{'─' * 32}")
        lines.append(f"{'':14s} {'План':>8s} {'Факт':>8s}")
        lines.append(f"{'Материалы':14s} {_f(m['est_materials']):>8s} {_f(m['fact_materials']):>8s}")
        lines.append(f"{'Установка':14s} {_f(m['est_installation']):>8s} {_f(m['fact_montazh']):>8s}")
        lines.append(f"{'Грузчики':14s} {_f(m['est_loaders']):>8s} {_f(m['fact_loaders']):>8s}")
        lines.append(f"{'Логистика':14s} {_f(m['est_logistics']):>8s} {_f(m['fact_logistics']):>8s}")
        lines.append(f"{'─' * 32}")
        lines.append(f"{'Затраты':14s} {_f(est_cost):>8s} {_f(fact_cost):>8s}")
        # ТЗ 2026-05-19 блок B: «ЗП монтажник» убрана (уже включена в «Установка»
        # через fact_montazh); «ЗП менеджер» и «Налоги» — теперь с колонкой План.
        est_mgr_zp = float(m.get("est_manager_zp") or 0)
        est_taxes = float(m.get("est_taxes") or 0)
        lines.append(f"{'ЗП менеджер':14s} {_f(est_mgr_zp):>8s} {_f(m['zp_manager']):>8s}")
        lines.append(f"{'Налоги':14s} {_f(est_taxes):>8s} {_f(taxes_total):>8s}")
        lines.append(f"{'─' * 32}")
        lines.append(f"{'Прибыль':14s} {_f(est_profit):>8s} {_f(profit):>8s}")
        lines.append("</pre>")

    return "\n".join(lines)


def format_manager_sync_card(metrics: dict[str, Any]) -> str:
    """Карточка-сводка менеджера при синхронизации, в эталонном дизайне.

    Эталон: docs/rules/feedback_card_template_standard.md (assets/card_etalon.png).
    Структура — секции в <pre>-блоках: 📋 Счета / 💰 Финансы / 💼 ЗП-менеджер.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    _now = _dt.now(_ZI("Europe/Moscow"))

    def _f(n: float) -> str:
        return f"{n:,.0f}".replace(",", " ")

    role_label = metrics.get("role_label") or ""
    role_suffix = f" {role_label}" if role_label else ""

    cnt_inv = int(metrics.get("count_invoices") or 0)
    cnt_tasks = int(metrics.get("count_tasks") or 0)
    cnt_unread_tasks = int(metrics.get("count_unread_tasks") or 0)
    cnt_unread_msgs = int(metrics.get("count_unread_msgs") or 0)
    sum_debt = float(metrics.get("sum_debt") or 0)
    sum_invoices_year = float(metrics.get("sum_invoices_year") or 0)
    sum_zp_unpaid = float(metrics.get("sum_zp_unpaid") or 0)
    zp_monthly = metrics.get("zp_monthly") or {}
    zp_total_year = float(metrics.get("zp_total_year") or 0)
    year = int(metrics.get("year") or _now.year)

    sections: list[str] = []

    tasks_label = (
        f"{cnt_tasks}  🔴 {cnt_unread_tasks}" if cnt_unread_tasks else str(cnt_tasks)
    )
    sections.append(
        format_card_section(
            emoji="📋",
            title=f"Счета менеджера{role_suffix}",
            items=[
                ("Счетов в работе", str(cnt_inv)),
                ("Задач", tasks_label),
                ("Входящих", str(cnt_unread_msgs)),
            ],
        )
    )

    sections.append(
        format_card_section(
            emoji="💰",
            title="Финансы",
            items=[
                ("Долги", f"{_f(sum_debt)}₽"),
                ("Счетов за год", f"{_f(sum_invoices_year)}₽"),
                ("ЗП не выплачено", f"{_f(sum_zp_unpaid)}₽"),
            ],
        )
    )

    month_names_short = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн",
                         "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    zp_items: list[tuple[str, str]] = []
    for m in range(1, _now.month + 1):
        v = float(zp_monthly.get(m) or 0)
        if v > 0:
            zp_items.append((month_names_short[m - 1], f"{_f(v)}₽"))
    if not zp_items:
        zp_items.append(("—", "0₽"))
    sections.append(
        format_card_section(
            emoji="💼",
            title=f"ЗП-менеджер {year}",
            items=zp_items,
            footer=("Итого", f"{_f(zp_total_year)}₽"),
        )
    )

    return format_card(sections)


def format_rp_sync_card(metrics: dict[str, Any]) -> str:
    """Карточка-сводка РП при синхронизации, в эталонном дизайне.

    Эталон: docs/rules/feedback_card_template_standard.md (assets/card_etalon.png).
    Отличия от менеджерской: метрики по всей компании, помесячно — «10% прибыли РП»
    (Invoices.AP) вместо ЗП-менеджера, без «ЗП не выплачено».
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    _now = _dt.now(_ZI("Europe/Moscow"))

    def _f(n: float) -> str:
        return f"{n:,.0f}".replace(",", " ")

    cnt_inv = int(metrics.get("count_invoices") or 0)
    cnt_tasks = int(metrics.get("count_tasks") or 0)
    cnt_unread_tasks = int(metrics.get("count_unread_tasks") or 0)
    cnt_unread_msgs = int(metrics.get("count_unread_msgs") or 0)
    sum_debt = float(metrics.get("sum_debt") or 0)
    sum_invoices_year = float(metrics.get("sum_invoices_year") or 0)
    rp_monthly = metrics.get("rp_monthly") or {}
    rp_total_year = float(metrics.get("rp_total_year") or 0)
    year = int(metrics.get("year") or _now.year)

    sections: list[str] = []

    tasks_label = (
        f"{cnt_tasks}  🔴 {cnt_unread_tasks}" if cnt_unread_tasks else str(cnt_tasks)
    )
    sections.append(
        format_card_section(
            emoji="📋",
            title="Счета компании",
            items=[
                ("Счетов в работе", str(cnt_inv)),
                ("Задач", tasks_label),
                ("Входящих", str(cnt_unread_msgs)),
            ],
        )
    )

    sections.append(
        format_card_section(
            emoji="💰",
            title="Финансы",
            items=[
                ("Долги", f"{_f(sum_debt)}₽"),
                ("Счетов за год", f"{_f(sum_invoices_year)}₽"),
            ],
        )
    )

    month_names_short = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн",
                         "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    rp_items: list[tuple[str, str]] = []
    for m in range(1, _now.month + 1):
        v = float(rp_monthly.get(m) or 0)
        if v > 0:
            rp_items.append((month_names_short[m - 1], f"{_f(v)}₽"))
    if not rp_items:
        rp_items.append(("—", "0₽"))
    sections.append(
        format_card_section(
            emoji="📈",
            title=f"10% прибыли РП {year}",
            items=rp_items,
            footer=("Итого", f"{_f(rp_total_year)}₽"),
        )
    )

    return format_card(sections)


def format_rp_invoices_in_work_card(breakdown: dict[str, int]) -> str:
    """Минимальная карточка РП «Счета в работе» (для /start и Синхронизации).

    Одна секция в эталонном дизайне (format_card_section). Все счета компании,
    без фильтра по менеджеру. Итог = pending + in_progress + credit (paid/ended
    не входят). Эталон: docs/rules/feedback_card_template_standard.md.
    """
    n_pending = int(breakdown.get("pending") or 0)
    n_progress = int(breakdown.get("in_progress") or 0)
    n_credit = int(breakdown.get("credit") or 0)
    total = n_pending + n_progress + n_credit
    return format_card_section(
        emoji="📋",
        title="Счета в работе",
        total=str(total),
        items=[
            ("Ждёт подтверждения", str(n_pending)),
            ("В работе", str(n_progress)),
            ("Кредитные", str(n_credit)),
        ],
    )


def format_installer_sync_card(metrics: dict[str, Any]) -> str:
    """Стартовая карточка монтажника в стиле РП/ГД (титул + одиночная ━, ширина 27).

    Показывается на /start, «🔄 Обновить меню», «🔄 Синхронизация данных» и в
    рассылке 09:00 (services/daily_sync) — одна функция на все 4 точки. Содержимое
    и источники — карта user 22.05, НЕ меняются ([[feedback_use_only_specified_sources]]):
      - 📋 Счёт End за {year} ━ {count_ended_year}  (AZ=montazh_stage='invoice_end')
        + В работе / Не взято / Задач 🔴 / Входящих
      - 💼 ЗП-монтажник {year} помесячно (BS=montazh_fact_op), Итого года — в шапке
      - 💰 Финансы: Баланс аванса (Лист «Авансы».H), ЗП в работе (BJ=zp_installer_amount)

    Ширина 27 колонок — подгон под телефон, данные без переноса (как РП-карточка,
    project_rp_card_widthfit). Эталон: docs/rules/feedback_card_template_standard.md.
    """
    import re as _re
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    _now = _dt.now(_ZI("Europe/Moscow"))
    _W = 27  # ширина под экран телефона монтажника (данные без переноса, как РП)

    def _f(n: float) -> str:
        return f"{n:,.0f}".replace(",", " ")

    def _short(block: str) -> str:
        """Хвост ━ в шапке секции (часть до первого <pre>) → 1 штука, как у РП."""
        parts = block.split("<pre>", 1)
        if len(parts) == 2:
            return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
        return block

    count_ended_year = int(metrics.get("count_ended_year") or 0)
    count_in_work = int(metrics.get("count_in_work") or 0)
    count_not_taken = int(metrics.get("count_not_taken") or 0)
    cnt_tasks = int(metrics.get("count_tasks") or 0)
    cnt_unread_tasks = int(metrics.get("count_unread_tasks") or 0)
    cnt_unread_msgs = int(metrics.get("count_unread_msgs") or 0)
    zp_monthly = metrics.get("zp_monthly") or {}
    zp_total_year = float(metrics.get("zp_total_year") or 0)
    balance_advance = float(metrics.get("balance_advance") or 0)
    year = int(metrics.get("year") or _now.year)

    sections: list[str] = []

    tasks_label = (
        f"{cnt_tasks}  🔴 {cnt_unread_tasks}" if cnt_unread_tasks else str(cnt_tasks)
    )
    sections.append(
        format_card_section(
            emoji="📋",
            title=f"Счёт End за {year}",
            total=str(count_ended_year),
            items=[
                ("В работе", str(count_in_work)),
                ("Не взято в работу", str(count_not_taken)),
                ("Задач", tasks_label),
                ("Входящих", str(cnt_unread_msgs)),
            ],
            width=_W,
        )
    )

    month_names_short = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн",
                         "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    zp_items: list[tuple[str, str]] = [
        (month_names_short[m - 1], f"{_f(float(zp_monthly.get(m) or 0))}₽")
        for m in range(1, _now.month + 1)
    ]
    sections.append(
        format_card_section(
            emoji="💼",
            title=f"ЗП-монтажник {year}",
            total=_f(zp_total_year),
            items=zp_items,
            width=_W,
        )
    )

    zp_in_work_unpaid = float(metrics.get("zp_in_work_unpaid") or 0)
    sections.append(
        format_card_section(
            emoji="💰",
            title="Финансы",
            items=[
                ("Баланс аванса", f"{_f(balance_advance)}₽"),
                ("ЗП в работе", f"{_f(zp_in_work_unpaid)}₽"),
            ],
            width=_W,
        )
    )

    return (
        "<b><i>Т в о я   А т м о с ф е р а</i></b>\n\n"
        + format_card([_short(s) for s in sections])
    )


def _format_gd_inwork_section(
    *,
    inv_pay: int,
    paid_today_inv: int,
    suppl_pay: int,
    paid_today_sup: int,
    in_progress: int,
    inv_year: int,
    credit_count: int,
    credit_year: int,
    total: int,
    unread_msgs: int,
) -> str:
    """Кастомная секция «Счета в работе» ГД-карточки с 2-м столбцом статистики.

    Столбец 1 = текущее число; столбец 2 = «оплачено за день» (строки 1-2,
    подпись /день) и «счетов с начала года» (строки 3-4, подпись /год). Под
    разделителем — строка «Входящие сообщения» (непрочитанные incoming у ГД).
    format_card_section 2-колоночный, поэтому рендер ручной (как
    _build_gd_balance_section). User-спека 28.05.
    """
    HEAVY = "━"
    INDENT = "   "
    W = 42
    W_LABEL = 22
    # Правый край столбца 2 выровнен по самому широкому значению (мин. 2), чтобы
    # единицы и подписи /день//год стояли в колонку (user 28.05).
    _c2w = max(2, len(str(paid_today_inv)), len(str(paid_today_sup)),
               len(str(inv_year)), len(str(credit_year)))

    def _row(label: str, c1: int, c2: int, suffix: str) -> str:
        return f"{INDENT}{label:<{W_LABEL}}{c1:>2}  · {c2:>{_c2w}} {suffix}"

    rows = [
        _row("Счета на оплату", inv_pay, paid_today_inv, "/день"),
        _row("Оплата поставщику", suppl_pay, paid_today_sup, "/день"),
        _row("В работе", in_progress, inv_year, "/год"),
        _row("Кредит", credit_count, credit_year, "/год"),
    ]
    title = "Счета в работе"
    header = f"<b>📋  {title}</b>"
    sep = INDENT + HEAVY * max(3, W - len(INDENT))
    itogo_row = f"{INDENT}{'Итого':<{W_LABEL}}{total:>13}"
    msg_row = f"{INDENT}{'Входящие сообщения':<{W_LABEL}}{unread_msgs:>13}"
    body = "\n".join([*rows, itogo_row, sep, msg_row])
    return f"{header}\n<pre>{body}</pre>"


async def resolve_invoice_manager_id(
    db: "Database", config: "Config", inv: dict
) -> "int | None":
    """Telegram_id менеджера-владельца счёта.

    Резолвит по `creator_role` (manager_kv/kia/npn) через тот же
    `resolve_default_assignee`, которым фича определяет ГД: settings override →
    env default → первый активный пользователь с этой ролью в БД. Нужно потому,
    что у счетов из «Импорт ОП» / созданных синком ГД `created_by` = ГД (или
    актор-синк), а НЕ реальный менеджер счёта — иначе задача ушла бы ГД.
    Fallback — `created_by` (счета, созданные самим менеджером в боте, где
    created_by и есть менеджер).
    """
    role = (inv.get("creator_role") or "").strip()
    if role:
        from .services.assignment import resolve_default_assignee
        resolved = await resolve_default_assignee(db, config, role)
        if resolved:
            return int(resolved)
    return inv.get("created_by")


def build_fpeta_debt_card(
    inv: dict[str, Any],
    *,
    header_emoji: str = "💰",
    header_title: str = "Долг по счёту — нужна ориентировочная дата финального платежа",
) -> str:
    """ТЗ 17.07: эталонная карточка задачи FINAL_PAYMENT_ETA менеджеру.

    Строго поля по ТЗ: № счёта (заголовок секции), полный адрес (под шапкой,
    вне <pre> — переносится, не скроллит), сумма счёта, дата запуска в работу
    (receipt_date «Дата пост.» — счёт из «Импорт ОП» стартует сразу «в работе»,
    отдельной колонки запуска в БД нет), 1-й и промежуточный платёж (только
    при наличии), долг, дата окончания сроков по договору; если срок по
    договору уже прошёл — 🔴-строка-индикатор под секцией.
    Read-only витрина ([[feedback_card_display_only_no_data_writes]]),
    дизайн — format_card_section ([[feedback_card_template_standard]]).
    """
    def _f(v: Any) -> float:
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    def _rub(v: Any) -> str:
        return f"{int(round(_f(v))):,}".replace(",", " ")

    def _d(iso: Any) -> str:
        s = str(iso or "")
        return f"{s[8:10]}.{s[5:7]}.{s[0:4]}" if len(s) >= 10 else "—"

    num = html.quote(str(inv.get("invoice_number") or inv.get("id") or "—"))
    addr = html.quote(str(inv.get("object_address") or "—"))

    items: list[tuple[str, str]] = [
        ("Сумма счёта", _rub(inv.get("amount"))),
        ("Запуск в работу", _d(inv.get("receipt_date"))),
    ]
    _fp = _f(inv.get("first_payment_amount"))
    if _fp > 0:
        items.append(("1-й платёж", _rub(_fp)))
    _sp = _f(inv.get("surcharge_amount"))
    if _sp > 0:
        items.append(("Промежут. платёж", _rub(_sp)))
    items.append(("Долг", _rub(inv.get("outstanding_debt"))))
    items.append(("Срок по договору", _d(inv.get("deadline_end_date"))))

    parts = [
        f"{header_emoji} <b>{html.quote(header_title)}</b>\n📍 {addr}",
        format_card_section("📋", f"Счёт №{num}", items),
    ]
    # 🔴 тревожный индикатор: дата окончания сроков по договору уже прошла.
    # Отдельной строкой ВНЕ <pre> — эмодзи внутри <pre> ломает выравнивание.
    _dl = str(inv.get("deadline_end_date") or "")[:10]
    if _dl:
        try:
            _today = datetime.now(ZoneInfo("Europe/Moscow")).date()
            if datetime.strptime(_dl, "%Y-%m-%d").date() < _today:
                parts.append(f"🔴 <b>Срок по договору просрочен — {_d(_dl)}</b>")
        except ValueError:
            pass
    return "\n\n".join(parts)


async def request_final_payment_eta(
    db: "Database",
    notifier: "Notifier",
    config: "Config",
    invoice_id: int,
    actor_id: int,
) -> bool:
    """ТЗ 14.06: при «Счёт ОК»/«Счёт End» с долгом по материнскому счёту —
    создать менеджеру 1 задачу «укажи ориент. дату фин. платежа» + пинг с кнопкой.

    Идемпотентно (дедуп): не плодит задачу, если уже идёт трекинг (state
    planned/overdue/paid) или открыта задача FINAL_PAYMENT_ETA. Вся логика
    обёрнута в try/except — сбой follow-up НЕ должен ломать денежный хендлер.
    Возвращает True, если задача создана.
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        inv = await db.get_invoice(invoice_id)
        if not inv:
            return False
        # только материнский счёт (доплаты-дети пропускаем)
        if inv.get("parent_invoice_id") is not None:
            return False
        # есть непогашенный долг?
        debt = float(inv.get("outstanding_debt") or 0)
        if debt <= 0:
            return False
        # владелец-менеджер счёта: резолвим по creator_role (created_by у счетов
        # из «Импорт ОП»/синка ГД = ГД, а не реальный менеджер — иначе задача
        # ушла бы ГД). Fallback — created_by.
        manager_id = await resolve_invoice_manager_id(db, config, inv)
        if not manager_id:
            return False
        # дедуп: уже в трекинге или есть открытая задача
        if (inv.get("final_payment_track_state") or "") in ("planned", "overdue", "paid"):
            return False
        if await db.has_open_final_payment_eta_task(invoice_id):
            return False

        from .enums import TaskType, TaskStatus
        try:
            created_task = await db.create_task(
                project_id=None,
                type_=TaskType.FINAL_PAYMENT_ETA,
                status=TaskStatus.OPEN,
                created_by=actor_id,
                assigned_to=int(manager_id),
                due_at_iso=None,
                payload={
                    "invoice_id": invoice_id,
                    "invoice_number": inv.get("invoice_number"),
                },
            )
        except Exception:
            _log.exception("request_final_payment_eta: create_task failed inv=%s", invoice_id)
            return False

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        b = InlineKeyboardBuilder()
        b.button(text="📅 Указать ориентировочную дату", callback_data=f"fpeta:{invoice_id}")
        b.adjust(1)
        # Эталонная карточка строго по ТЗ 17.07: № счёта / полный адрес /
        # сумма / запуск в работу / 1-й+промежуточный платёж / долг / срок
        # по договору (+🔴 при просрочке срока).
        try:
            card = build_fpeta_debt_card(inv)
        except Exception:
            _log.exception("request_final_payment_eta: card build failed inv=%s", invoice_id)
            debt_str = f"{int(round(debt)):,}".replace(",", " ")
            card = (
                f"💰 <b>Долг по счёту №{inv.get('invoice_number') or invoice_id}</b>\n\n"
                f"📍 {inv.get('object_address') or '—'}\n"
                f"Остаток долга: <b>{debt_str} ₽</b>\n\n"
                "Когда планируется <b>финальный платёж</b>? Укажите ориентировочную дату."
            )
        await notifier.safe_send(int(manager_id), card, reply_markup=b.as_markup())
        return True
    except Exception:
        _log.exception("request_final_payment_eta: unexpected inv=%s", invoice_id)
        return False


async def prompt_invoice_end_ready(
    db: "Database",
    notifier: "Notifier",
    invoice_id: int,
    actor_id: int | None = None,
    config: Any = None,
) -> bool:
    """ТЗ 18.06: монтажник закончил работы («Счёт ОК») И по материнскому счёту
    НЕТ долга → создать менеджеру 1 задачу-напоминание «счёт готов к закрытию»
    + push эталонной карточкой с инлайн-кнопкой перехода в флоу «Счет End».

    Условия (материнский счёт): montazh_stage == 'invoice_ok', outstanding_debt
    <= 0, статус активный (in_progress/paid). Идемпотентно: не плодит задачу,
    если уже есть открытое напоминание ИЛИ менеджер уже начал закрытие
    (открытая INVOICE_END_REQUEST) — db.invoice_end_prompt_blocked. Кнопка
    «Счет End» в меню менеджера получает динамический бейдж 🔴N отдельно
    (count_invoices_ready_for_end). config (опц.) → refresh меню сразу.
    try/except — сбой follow-up НЕ ломает денежный хендлер. True, если создана.
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        inv = await db.get_invoice(invoice_id)
        if not inv:
            return False
        if inv.get("parent_invoice_id") is not None:
            return False  # только материнский счёт
        if (inv.get("status") or "") not in ("in_progress", "paid", "credit"):
            return False  # уже закрыт/отклонён/на проверке (credit = активный кред-счёт)
        if (inv.get("montazh_stage") or "") != "invoice_ok":
            return False  # монтажник ещё не нажал «Счёт ОК»
        if float(inv.get("outstanding_debt") or 0) > 0:
            return False  # есть долг — рано закрывать
        manager_id = inv.get("created_by")
        if not manager_id:
            return False
        # дедуп: открытое напоминание ИЛИ менеджер уже инициировал закрытие
        if await db.invoice_end_prompt_blocked(invoice_id):
            return False

        from .enums import TaskType, TaskStatus
        try:
            created_task = await db.create_task(
                project_id=None,
                type_=TaskType.INVOICE_END_READY,
                status=TaskStatus.OPEN,
                created_by=actor_id or int(manager_id),
                assigned_to=int(manager_id),
                due_at_iso=None,
                payload={
                    "invoice_id": invoice_id,
                    "invoice_number": inv.get("invoice_number"),
                },
            )
        except Exception:
            _log.exception("prompt_invoice_end_ready: create_task failed inv=%s", invoice_id)
            return False

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        b = InlineKeyboardBuilder()
        b.button(text="🏁 Закрыть счёт", callback_data=f"invend:view:{invoice_id}")
        b.adjust(1)
        try:
            card = await build_manager_task_card(
                db, created_task,
                header_emoji="🏁",
                header_title="Счёт готов к закрытию",
            )
        except Exception:
            _log.exception("prompt_invoice_end_ready: card build failed inv=%s", invoice_id)
            card = (
                f"🏁 <b>Счёт №{inv.get('invoice_number') or invoice_id} готов к закрытию</b>\n\n"
                f"📍 {inv.get('object_address') or '—'}\n"
                "Монтаж завершён, долга по счёту нет. Можно закрыть счёт (Счет End)."
            )
        await notifier.safe_send(int(manager_id), card, reply_markup=b.as_markup())
        # обновить reply-меню менеджера → бейдж 🔴 на кнопке «Счет End» сразу
        if config is not None:
            try:
                await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
            except Exception:
                _log.debug("prompt_invoice_end_ready: kb refresh failed mgr=%s", manager_id, exc_info=True)
        return True
    except Exception:
        _log.exception("prompt_invoice_end_ready: unexpected inv=%s", invoice_id)
        return False


async def prompt_invoice_docs_missing(
    db: "Database",
    notifier: "Notifier",
    invoice_id: int,
    actor_id: int | None = None,
    config: Any = None,
) -> bool:
    """ТЗ 18.06 (B): активный материнский б/н счёт без первичных документов
    (нет ЭДО первички И нет оригиналов первички) → создать менеджеру (created_by)
    задачу INVOICE_DOCS_MISSING + push эталонной карточкой; бухгалтерию уведомить
    коротким FYI (без отдельной задачи, без сумм/прибыли).

    Идемпотентно: db.invoice_docs_missing_blocked. Подпризнак 🔴 «совсем нет
    документов» (payload.fully_empty) — если нет и закрывающих, и договора.
    try/except — сбой follow-up не ломает вызывающий проход. True, если создана.
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    try:
        inv = await db.get_invoice(invoice_id)
        if not inv:
            return False
        if inv.get("parent_invoice_id") is not None:
            return False  # только материнский счёт
        if int(inv.get("is_credit") or 0) != 0:
            return False  # только б/н
        if (inv.get("status") or "") != "in_progress":
            return False  # контроль только активных счетов
        # первичка не оформлена (нет ЭДО И нет оригиналов)?
        if int(inv.get("docs_edo_signed") or 0) != 0:
            return False
        if (inv.get("docs_originals_holder") or ""):
            return False
        manager_id = inv.get("created_by")
        if not manager_id:
            return False
        if await db.invoice_docs_missing_blocked(invoice_id):
            return False  # уже есть открытая задача — не плодим

        # severity: «совсем нет документов» (нет также закрывающих и договора)
        fully_empty = (
            int(inv.get("edo_signed") or 0) == 0
            and not (inv.get("closing_originals_holder") or "")
            and not (inv.get("contract_signed") or "")
        )

        from .enums import TaskType, TaskStatus, Role
        try:
            created_task = await db.create_task(
                project_id=None,
                type_=TaskType.INVOICE_DOCS_MISSING,
                status=TaskStatus.OPEN,
                created_by=actor_id or int(manager_id),
                assigned_to=int(manager_id),
                due_at_iso=None,
                payload={
                    "invoice_id": invoice_id,
                    "invoice_number": inv.get("invoice_number"),
                    "fully_empty": bool(fully_empty),
                },
            )
        except Exception:
            _log.exception("prompt_invoice_docs_missing: create_task failed inv=%s", invoice_id)
            return False

        title = "Счёт без документов" + (" 🔴" if fully_empty else "")
        try:
            card = await build_manager_task_card(
                db, created_task,
                header_emoji="📄",
                header_title=title,
            )
        except Exception:
            _log.exception("prompt_invoice_docs_missing: card build failed inv=%s", invoice_id)
            card = (
                f"📄 <b>Счёт №{inv.get('invoice_number') or invoice_id} — нет документов</b>\n\n"
                f"📍 {inv.get('object_address') or '—'}\n"
                "Первичные документы (ЭДО/оригиналы) не оформлены. Нужно оформить."
            )
        # Первичный push — с доменной клавиатурой (Принято + статус документов).
        # Локальный импорт: keyboards импортирует utils на уровне модуля (цикл).
        try:
            from .keyboards import task_actions_kb
            _docs_kb = task_actions_kb(created_task)
        except Exception:
            _log.debug("prompt_invoice_docs_missing: kb build failed inv=%s", invoice_id, exc_info=True)
            _docs_kb = None
        await notifier.safe_send(int(manager_id), card, reply_markup=_docs_kb)

        # уведомить бухгалтерию (без отдельной задачи) — короткий FYI без сумм/прибыли
        try:
            acc_users = await db.find_users_by_role(Role.ACCOUNTING)
            num = inv.get("invoice_number") or invoice_id
            addr = inv.get("object_address") or "—"
            client = inv.get("client_name") or "—"
            acc_text = (
                f"📄 <b>Счёт без документов — №{num}</b>\n\n"
                f"📍 {addr}\n"
                f"👤 {client}\n\n"
                "Первичные документы по счёту ещё не оформлены "
                "(задача поставлена менеджеру)."
            )
            for au in acc_users:
                if int(au.telegram_id) == int(manager_id):
                    continue
                await notifier.safe_send(int(au.telegram_id), acc_text)
        except Exception:
            _log.debug("prompt_invoice_docs_missing: accounting notify failed inv=%s", invoice_id, exc_info=True)
        return True
    except Exception:
        _log.exception("prompt_invoice_docs_missing: unexpected inv=%s", invoice_id)
        return False


async def _build_gd_debts_section(db: "Database") -> str:
    """Блок «Долги» для стартовой карточки ГД (display-only, read-only).

    Все материнские счета с outstanding_debt>0, сортировка по сумме долга ↓.
    Колонки (словесные заголовки): Объект (адрес≈10 симв, добит точками +
    маркер КВ/КИА/НПН) | Этап (иконка монтажа) | Тип (💳 б/н / 🏦 кред) |
    Дата (ориент. дата фин. платежа = planned_final_payment_date, кол. EQ,
    вводит менеджер по задаче FINAL_PAYMENT_ETA при долге) | Долг («к»).
    Итог НЕ в шапке (как карточки «Баланс»): в теле — итоги по менеджерам
    (КВ/КИА/НПН) + общий. Ширина INDENT 3 / BODY_WIDTH 40 = как «Баланс».
    В БД/Sheets НЕ пишет.
    """
    from .rp_start_card import _addr_cell, vw

    _STAGE_ICON = {
        "none": "⬛", "assigned": "📋", "in_work": "🔨",
        "razmery_ok": "📐", "invoice_ok": "✅", "invoice_end": "🏁",
    }

    def _k(n: Any) -> str:
        return f"{int(round(float(n or 0) / 1000))}к"

    def _marker(num: str, role: str) -> str:
        u = (num or "").upper()
        if "КИА" in u:
            return "КИА"
        if "НПН" in u:
            return "НПН"
        if "КВ" in u:
            return "КВ"
        return {"manager_kv": "КВ", "manager_kia": "КИА",
                "manager_npn": "НПН"}.get(role or "", "?")

    def _street_dot(addr: Any, w: int) -> str:
        """Адрес ровно w симв: длинный обрезать, короткий добить точками.

        Правило адреса единое для всех карточек ГД (owner 30.07): Москва →
        улица, НЕ Москва → название города. Хелпер тот же, что в «Этапах
        работы» — rp_start_card._addr_cell (был _street, город терялся).
        """
        s = _addr_cell(addr, w)
        if len(s) < w:
            s = s + "." * (w - len(s))
        return s

    def _eta(v: Any) -> str:
        """Ориент. дата фин. платежа → DD.MM ('—' если пусто)."""
        s = str(v or "").strip()
        if not s:
            return "—"
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":   # YYYY-MM-DD
            return f"{s[8:10]}.{s[5:7]}"
        parts = s.split(".")
        if len(parts) >= 2 and parts[0].isdigit():          # DD.MM.YYYY
            return f"{parts[0].zfill(2)}.{parts[1].zfill(2)}"
        return s[:5]

    def _pl(s: str, w: int) -> str:  # left-align к визуальной ширине w
        return s + " " * max(0, w - vw(s))

    def _pr(s: str, w: int) -> str:  # right-align к визуальной ширине w
        return " " * max(0, w - vw(s)) + s

    cur = await db.conn.execute(
        "SELECT invoice_number, object_address, creator_role, "
        "       COALESCE(montazh_stage,'none') AS stage, "
        "       COALESCE(is_credit,0) AS is_credit, "
        "       COALESCE(outstanding_debt,0) AS debt, "
        "       planned_final_payment_date "
        "FROM invoices "
        "WHERE parent_invoice_id IS NULL AND COALESCE(outstanding_debt,0) > 0 "
        "ORDER BY debt DESC"
    )
    rows = [dict(r) for r in await cur.fetchall()]
    if not rows:
        return ""

    INDENT = "   "
    BODY_WIDTH = 40
    W_STREET = 10
    W_OBJ = W_STREET + 1 + 3 + 1   # улица + пробел + маркер + зазор = 15
    W_STAGE, W_TYPE, W_DATE = 6, 5, 8
    W_DEBT = BODY_WIDTH - W_OBJ - W_STAGE - W_TYPE - W_DATE   # 6 (справа)

    def _row(obj: str, stage: str, typ: str, date: str, debt: str) -> str:
        return (f"{INDENT}{_pl(obj, W_OBJ)}{_pl(stage, W_STAGE)}"
                f"{_pl(typ, W_TYPE)}{_pl(date, W_DATE)}{_pr(debt, W_DEBT)}")

    def _foot(label: str, val: str) -> str:
        pad = max(1, BODY_WIDTH - len(label) - len(val))
        return f"{INDENT}{label}{' ' * pad}{val}"

    head = _row("Объект", "Этап", "Тип", "Дата", "Долг")
    sep = f"{INDENT}{'━' * BODY_WIDTH}"

    lines = [head, sep]
    per_mgr: dict[str, float] = {}
    total = 0.0
    for r in rows:
        street = _street_dot(r.get("object_address"), W_STREET)
        mk = _marker(r.get("invoice_number") or "", r.get("creator_role") or "")
        obj = _pl(street, W_STREET) + " " + mk
        stage = _STAGE_ICON.get(r["stage"], "⬛")
        typ = "🏦" if int(r["is_credit"]) else "💳"
        date = _eta(r.get("planned_final_payment_date"))
        debt = float(r["debt"] or 0)
        per_mgr[mk] = per_mgr.get(mk, 0.0) + debt
        total += debt
        lines.append(_row(obj, stage, typ, date, _k(debt)))

    lines.append(sep)
    for mk in ("КВ", "КИА", "НПН"):
        if mk in per_mgr:
            lines.append(_foot(f"Итого {mk}", _k(per_mgr[mk])))
    lines.append(_foot("Итого", _k(total)))

    body = "\n".join(lines)
    return f"<b>💰  Долги</b>\n<pre>{body}</pre>"


async def build_gd_sync_card_text(
    db: "Database",
    config: Any,
    user_id: int,
) -> str:
    """Текст карточки «Синхронизация данных» для ГД.

    Идентичная карточка используется:
    - при ручном нажатии GD_BTN_SYNC (handlers/gd.py::gd_sync_data),
    - в daily_sync.py для admin_ids после 09:00 МСК cron.

    Возвращает только text (markup собирается в caller'е, т.к. требует
    актуальных счётчиков бейджей).
    """
    from datetime import datetime as _datetime
    from zoneinfo import ZoneInfo as _ZoneInfo
    import logging as _logging
    _log = _logging.getLogger(__name__)

    s = await db.get_daily_summary()
    in_progress = s["invoices_by_status"].get("in_progress", 0)
    tasks_open = s["tasks_open"]
    inv_pay = tasks_open.get("invoice_payment", 0)
    suppl_pay = await db.count_gd_supplier_pay_tasks(user_id)
    total_debt_bn = float(s["total_debt"] or 0)

    try:
        _xstats = await db.get_gd_inwork_extra_stats(user_id)
    except Exception:
        _xstats = {
            "paid_today_inv": 0, "paid_today_sup": 0,
            "inv_year": 0, "credit_year": 0, "unread_msgs": 0,
        }

    # Кредит — открытые is_credit=1 счета с DA>0.
    _credit_total_da = 0.0
    _credit_visible_count = 0
    _credit_per_role: dict[str, float] = {}
    for _crole in ("manager_kv", "manager_kia", "manager_npn"):
        try:
            _cs = await db.get_credit_balance_summary(_crole)
        except Exception:
            _log.debug("gd_sync_card: credit block — %s load failed", _crole, exc_info=True)
            continue
        _open = [r for r in (_cs.get("invoices") or []) if not r.get("is_closed")]
        if not _open:
            continue
        _credit_visible_count += sum(1 for r in _open if float(r.get("da") or 0) > 0)
        _role_da = float(_cs.get("total_da") or 0)
        _credit_total_da += _role_da
        _credit_per_role[_crole] = _role_da

    # Сумма долга кредит (AE = outstanding_debt для is_credit=1).
    total_debt_credit = 0.0
    try:
        _cur = await db.conn.execute(
            "SELECT COALESCE(SUM(outstanding_debt), 0) AS d "
            "FROM invoices "
            "WHERE parent_invoice_id IS NULL AND is_credit = 1 "
            "AND status IN ('pending', 'in_progress', 'paid', 'credit')"
        )
        _row = await _cur.fetchone()
        total_debt_credit = float((_row[0] if _row else 0) or 0)
    except Exception:
        _log.debug("gd_sync_card: total_debt_credit load failed", exc_info=True)

    # Лиды: сегодня + за месяц, с разбивкой за месяц по менеджеру (колонка E
    # листа «Лиды» = responsible_user_id → config.amocrm_user_map) и по источнику
    # (категоризир. by_source: source→тег→имя). Масс-импорт исключён в get_lead_stats_v2.
    leads_today = 0
    leads_month = 0
    _lead_mgr_rows: list[tuple[str, str]] = []
    _lead_src_rows: list[tuple[str, str]] = []
    try:
        _lstats = await db.get_lead_stats_v2()
        _ltot = (_lstats or {}).get("totals") or {}
        leads_today = int(_ltot.get("today") or 0)
        leads_month = int(_ltot.get("month") or 0)

        # Менеджер (E): responsible_user_id → метка из карты AMO; агрегируем за месяц.
        _user_map = getattr(config, "amocrm_user_map", {}) or {}
        _mgr_agg: dict[str, int] = {}
        for _rid, _d in ((_lstats or {}).get("by_responsible") or {}).items():
            _n = int((_d or {}).get("month") or 0)
            if _n <= 0:
                continue
            _lbl = (_user_map.get(int(_rid)) if _rid else None) or "Без менеджера"
            _mgr_agg[_lbl] = _mgr_agg.get(_lbl, 0) + _n
        _lead_mgr_rows = [
            (lbl, str(n)) for lbl, n in sorted(_mgr_agg.items(), key=lambda kv: -kv[1])
        ]

        # Источник (F, категоризир.): by_source за месяц, непустые, топ-6.
        for _s in (_lstats or {}).get("by_source") or []:
            _n = int((_s or {}).get("month") or 0)
            if _n > 0:
                _lead_src_rows.append((str(_s.get("src") or "—")[:16], str(_n)))
        _lead_src_rows = _lead_src_rows[:6]
    except Exception:
        _log.debug("gd_sync_card: lead_stats_v2 failed", exc_info=True)

    _now_msk = _datetime.now(_ZoneInfo("Europe/Moscow"))

    def _k(n: float) -> str:
        """Compact: 1234k (без разделителя тысяч, латинская k). Минус «−» (U+2212)."""
        v = int(n / 1000) if n else 0
        if v == 0:
            return "0"
        sign = "−" if v < 0 else ""
        return f"{sign}{abs(v)}k"

    in_progress_total = in_progress + _credit_visible_count
    sections: list[str] = []

    sections.append(
        _format_gd_inwork_section(
            inv_pay=inv_pay,
            paid_today_inv=_xstats["paid_today_inv"],
            suppl_pay=suppl_pay,
            paid_today_sup=_xstats["paid_today_sup"],
            in_progress=in_progress,
            inv_year=_xstats["inv_year"],
            credit_count=_credit_visible_count,
            credit_year=_xstats["credit_year"],
            total=in_progress_total,
            unread_msgs=_xstats["unread_msgs"],
        )
    )

    # Этапы работы — матрица активных счетов (тот же блок что в РП-карточке),
    # БЕЗ легенды, пустые клетки чёрные ⬛ (user 02.06: у ГД легенду убрать;
    # цвет незанятых клеток сделать чёрным — как у РП).
    try:
        from .rp_start_card import _matrix as _rp_matrix
        sections.append(await _rp_matrix(db, show_legend=False, empty="⬛"))
    except Exception:
        _log.warning("gd_sync_card: matrix block failed", exc_info=True)

    # Лиды в ОДНОМ <pre>-блоке. Подгруппы «Менеджеры»/«Источники» — маркер ◎ +
    # данные с увеличенным отступом (жирный/подчёркнутый внутри <pre> Telegram
    # не рисует). Тот же вид, что в РП-карточке (общий format_leads_section).
    # User 31.05 — переверстка для читаемости подгрупп.
    sections.append(
        format_leads_section(
            emoji="⏰",
            title="Лиды",
            today=leads_today,
            month=leads_month,
            mgr_rows=_lead_mgr_rows,
            src_rows=_lead_src_rows,
        )
    )

    # Замеры по менеджерам (сетка прошлый/текущий/следующий месяц, КВ/КИА/НПН + Всего) —
    # ВНУТРИ карты между «Лиды» и «Кредитный баланс» (owner 14.07; прежде уходила
    # ОТДЕЛЬНЫМ сообщением после карты). None если замерщиков нет.
    try:
        from .zamery_start_card import build_zamery_manager_stats_card
        _zstats_sec = await build_zamery_manager_stats_card(db)
        if _zstats_sec:
            sections.append(_zstats_sec)
    except Exception:
        _log.warning("gd_sync_card: zamery manager-stats section failed", exc_info=True)

    # Кредитный баланс — Баланс (total DA) + разбивка по ролям из Invoices.DA с привязкой к B (Роль).
    # (owner 14.07: блок «Финансы» переименован в «Кредитный баланс».)
    _role_short = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}
    _fin_items = [("Баланс", _k(_credit_total_da))]
    for _crole, _short in _role_short.items():
        _fin_items.append((_short, _k(_credit_per_role.get(_crole, 0.0))))
    sections.append(
        format_card_section(
            emoji="💰",
            title="Кредитный баланс",
            items=_fin_items,
        )
    )

    # СЕКЦИЯ 5 «Баланс б/н» + СЕКЦИЯ 6 «Баланс (кред)» — таблица 12×4 + 3 итога
    # (Баланс / Долг / Прибыль прогноз). Spec: docs/specs/gd_sync_card_spec.md.
    try:
        sections.append(
            await _build_gd_balance_section(
                db, is_credit=False, year=_now_msk.year, current_month=_now_msk.month,
                total_debt=total_debt_bn,
            )
        )
    except Exception:
        _log.warning("gd_sync_card: balance b/n section failed", exc_info=True)
    try:
        sections.append(
            await _build_gd_balance_section(
                db, is_credit=True, year=_now_msk.year, current_month=_now_msk.month,
                total_debt=total_debt_credit,
            )
        )
    except Exception:
        _log.warning("gd_sync_card: balance credit section failed", exc_info=True)

    # --- Долги: обзор счетов с непогашенным долгом (ТЗ 16.06) ---
    try:
        _debts_sec = await _build_gd_debts_section(db)
        if _debts_sec:
            sections.append(_debts_sec)
    except Exception:
        _log.warning("gd_sync_card: debts overview section failed", exc_info=True)

    # --- Долги: трекинг финального платежа (ТЗ 14.06) ---
    # Фиксирует намеченную дату (📅) и просрочку (🔴) по счетам с НЕпогашенным долгом.
    # owner 23.06: «долгов по материнскому счёту нет — задача закрывается» → счета с
    # outstanding_debt ≤ 0 в секции НЕ показываем. Сверяемся с ФАКТИЧЕСКИМ долгом, а
    # не только со stored-state: переход overdue/planned → 'paid' делает daily_sync
    # (1×/сутки), поэтому иначе ГД до следующего синка видит погашенный долг как
    # «просроч.» (баг 23.06 на КВ6: debt=0, но state ещё 'overdue').
    try:
        _track = await db.list_invoices_tracking_final_payment()
        _track_active = [
            _r for _r in (_track or [])
            if float(_r.get("outstanding_debt") or 0) > 0
        ]
        if _track_active:
            # Адрес — единое правило карточек ГД (owner 30.07): Москва → улица,
            # НЕ Москва → город. Тот же хелпер, что в «Этапах работы» и «Долгах».
            from .rp_start_card import _addr_cell as _addr_fp
            _debt_items: list[tuple[str, str]] = []
            for _r in _track_active[:8]:
                _st = _r.get("final_payment_track_state") or ""
                _num = _r.get("invoice_number") or f"id{_r.get('id')}"
                _addr = _addr_fp(_r.get("object_address") or "", 14)
                _pd = (_r.get("planned_final_payment_date") or "")[:10]
                _dshort = f"{_pd[8:10]}.{_pd[5:7]}" if (len(_pd) == 10 and _pd[4] == "-") else ""
                if _st == "overdue":
                    _mark, _val = "🔴", (f"{_dshort} просроч." if _dshort else "просроч.")
                else:  # planned (paid сюда не попадёт — у него долг = 0, отфильтрован)
                    _mark, _val = "📅", (_dshort or "дата?")
                _debt_items.append((f"{_mark} №{_num} {_addr}", _val))
            sections.append(
                format_card_section(
                    emoji="💸",
                    title="Долги — фин.платёж",
                    items=_debt_items,
                )
            )
    except Exception:
        _log.warning("gd_sync_card: final-payment debts section failed", exc_info=True)

    # --- Задачи ролям: задачи (gd_task), которые ГД поставил другим ролям,
    # + статус выполнения каждой (ТЗ 17.06, вариант A — все недавние). ---
    try:
        _tasks_sec = await _build_gd_tasks_section(db, user_id, limit=6)
        if _tasks_sec:
            sections.append(_tasks_sec)
    except Exception:
        _log.warning("gd_sync_card: tasks section failed", exc_info=True)

    # --- График замеров (календарь-окно) УБРАН из стартовой карты (owner 14.07):
    # теперь показывается при нажатии кнопки «Замеры» (handlers/gd.py::gd_chat_zamery),
    # сразу карточкой, ниже — пункты меню. build_zamery_calendar_section сохранён. ---

    # Заголовок-название карточки (user 28.05; разрядка+курсив 31.05).
    return f"<b><i>Т в о я   А т м о с ф е р а</i></b>\n\n{format_card(sections)}"


async def _build_gd_tasks_section(db: "Database", user_id: int, limit: int = 9, for_user: int | None = None) -> str | None:
    """Блок «Задачи ролям» — история задач (ТЗ 25.06, owner).

    for_user=None → ПОЛНАЯ история всех ролей (ГД/РП-карты). for_user=<tg_id> →
    только задачи, касающиеся этого пользователя (создал ИЛИ назначен) — карта
    менеджера, «касаемо именно его роли».

    Многострочный аудит на задачу:
        {дата+время создания}  {роль+имя создателя} → {роль+имя исполнителя}
           №{счёт} · {описание / тип задачи}
           {иконка} {статус} {дата+время выполнения}

    Включает ВСЕ типы (в т.ч. расход кред-кошелька = INVOICE_PAYMENT с
    payload.kind='credit_payment_request') и любых создателей (в т.ч. задачи,
    которые РП ставит другим ролям). Последние LIMIT задач, новые сверху, все
    статусы. Read-only витрина (feedback_card_display_only_no_data_writes).
    user_id не фильтрует (ГД видит историю всех) — параметр сохранён для
    совместимости вызова build_gd_sync_card_text.
    """
    LIMIT = limit   # бюджет под лимит Telegram 4096 задаёт вызывающий (ГД-карта=6 — место под 2-мес. календарь; РП=9)
    _MSK = "Europe/Moscow"
    _ST = {
        "open": ("🔴", "Открыта"),
        "in_progress": ("⏳", "В работе"),
        "done": ("✅", "Выполнено"),
        "rejected": ("❌", "Отклонено"),
        "cancelled": ("🚫", "Отменена"),
        "pending": ("⌛", "Ожидает"),
    }
    _ROLE_SHORT = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
        "manager": "Менеджер", "rp": "РП", "td": "ТД",
        "accounting": "Бухгалтер", "installer": "Монтажник",
        "zamery": "Замерщик", "driver": "Водитель",
        "tinter": "Тонировщик", "loader": "Грузчик", "gd": "ГД",
    }
    _EMPTY = {"-", "—", ".", ""}

    try:
        if for_user is not None:
            rows = await db.list_recent_tasks_for_user(for_user, limit=LIMIT)
        else:
            rows = await db.list_recent_tasks_all(limit=LIMIT)
    except Exception:
        log.debug("gd_tasks_section: recent-tasks query failed", exc_info=True)
        rows = []
    if not rows:
        return None

    def _dt_short(iso: Any) -> str:
        if not iso:
            return ""
        try:
            return from_iso(str(iso)).astimezone(tzinfo(_MSK)).strftime("%d.%m %H:%M")
        except Exception:
            return str(iso)[:10]

    def _ulabel(role: Any, name: Any, fid: Any) -> str:
        role_s = (str(role).split(",")[0] if role else "")
        if not role_s and not name:
            return "Система" if not fid else str(fid)
        rs = _ROLE_SHORT.get(role_s, role_s)
        # ГД — БЕЗ имени: подпись «ГД инфо-перегородки» избыточна, оставляем «ГД»
        # (owner 25.07). Остальные роли по-прежнему с именем («РП Павел»).
        if role_s == "gd":
            return rs
        nm = (str(name or "").strip().split(" ") or [""])[0]
        out = f"{rs} {nm}".strip()
        return html.quote(out) if out else (str(fid) if fid else "—")

    async def _inv_num(pl: dict) -> str | None:
        if not isinstance(pl, dict):
            return None
        n = pl.get("invoice_number")
        if n:
            return str(n)
        lid = pl.get("linked_invoice_id") or pl.get("invoice_id")
        if lid:
            try:
                iv = await db.get_invoice(int(lid))
                return iv.get("invoice_number") if iv else None
            except Exception:
                return None
        return None

    def _describe(t: dict, pl: dict) -> str:
        typ = t.get("type")
        kind = pl.get("kind") if isinstance(pl, dict) else None
        if kind == "credit_payment_request":
            c = str(pl.get("comment") or pl.get("purpose") or "").replace("\n", " ").strip()
            if c in _EMPTY:
                c = ""
            base = "Расход кредита"
            amt = pl.get("amount")
            if amt:
                try:
                    base += f" {int(float(amt))}₽"
                except Exception:
                    pass
            return base + (f" — {c}" if c else "")
        comment = ""
        if isinstance(pl, dict):
            comment = str(pl.get("comment") or pl.get("details") or "").replace("\n", " ").strip()
        if comment in _EMPTY:
            comment = ""
        if typ in ("gd_task", "not_urgent_gd", "urgent_gd", "issue") and comment:
            return comment
        # В карточке «Задачи ролям» тип invoice_payment называется «Назначение»
        # (owner 25.07). Локально — глобальный TASK_TYPE_LABELS не трогаем, чтобы
        # не менять подпись в других местах (feedback_design_only_indicated_block).
        lab = "Назначение" if typ == "invoice_payment" else task_type_label(typ)
        return lab + (f": {comment}" if comment else "")

    IND = ""         # без отступа — строка задачи от начала строки (owner 26.06)
    SUB = ""         # без отступа — детали (счёт/описание, результат) тоже flush-left
    blocks: list[str] = []
    for t in rows:
        try:
            pl = json.loads(t.get("payload_json") or "{}") or {}
        except Exception:
            pl = {}
        cl = _ulabel(t.get("creator_role"), t.get("creator_name"), t.get("created_by"))
        al = _ulabel(t.get("assignee_role"), t.get("assignee_name"), t.get("assigned_to"))
        num = await _inv_num(pl)
        desc = _describe(t, pl)
        if len(desc) > 52:
            desc = desc[:51] + "…"
        desc = html.quote(desc)
        status = t.get("status") or ""
        ic, word = _ST.get(status, ("•", status))
        suffix = ""
        if status in ("done", "rejected", "cancelled"):
            suffix = " " + _dt_short(t.get("updated_at"))
        elif status == "in_progress" and t.get("accepted_at"):
            suffix = " (принята " + _dt_short(t.get("accepted_at")) + ")"
        head = f"{IND}{_dt_short(t.get('created_at'))}  {cl} → {al}"
        line_task = f"{SUB}{('№' + html.quote(str(num)) + ' · ') if num else ''}{desc}"
        line_res = f"{SUB}{ic} {word}{suffix}"
        blocks.append("\n".join((head, line_task, line_res)))

    body = "\n\n".join(blocks)
    return f"<b>📋  Задачи ролям (последние {len(blocks)})</b>\n<pre>{body}</pre>"


async def with_manager_tasks_section(db: "Database", user_id: int, card_text: str) -> str:
    """Дописывает менеджеру под его sync-карту блок «Задачи ролям» (ТОЛЬКО его
    задачи — создал ИЛИ назначен; ТЗ 25.06 «касаемо именно его роли») + блок
    «📅 График замеров» (2-месячный агрегат-календарь всех замерщиков, как у
    ГД/РП; owner 26.06). Display/read-only; при ошибке/пусто секция пропускается.
    """
    try:
        sec = await _build_gd_tasks_section(db, user_id, limit=9, for_user=user_id)
    except Exception:
        log.exception("manager card: tasks section failed")
        sec = None
    if sec:
        card_text = f"{card_text}\n\n{sec}"
    # «📅 График замеров» — агрегат-календарь всех замерщиков внизу карты
    # (owner 26.06: «как у ГД/РП»). Тот же build_zamery_calendar_section(db).
    try:
        from .zamery_start_card import build_zamery_calendar_section
        cal = await build_zamery_calendar_section(db)
        if cal:
            card_text = f"{card_text}\n\n{cal}"
    except Exception:
        log.exception("manager card: zamery calendar section failed")
    return card_text


async def _build_gd_balance_section(
    db: "Database",
    *,
    is_credit: bool,
    year: int,
    current_month: int,
    total_debt: float = 0.0,
) -> str:
    """Секция «Баланс б/н» (is_credit=False) или «Баланс (кред)» (True).

    Spec: docs/specs/gd_sync_card_spec.md (секции 4/5).
    Возвращает: `<b>📈  {title} ━━━ {итог}</b>\\n<pre>таблица + 3 итога</pre>`.

    Таблица: 5 колонок (Месяц | Доход (Р) | Доход | Расход | Баланс), от
    Января до current_month (будущие скрыты), формат «к» (truncate тысяч,
    типографский минус). Бейдж 🔴N после месяца если open_count > 0 (учитывает
    что 🔴 ≈ 2 visual char в моноширинном Telegram-рендере). Итог секции в
    заголовке через `━━━` + 3 строки под разделителем («Баланс б/н» / «Долг
    б.н.» (или «Долг кредит») / «Прибыль прогноз» — все 3 в полном формате
    XX XXXр).
    """
    data = await db.get_gd_balance_section_data(
        is_credit=is_credit, year=year, current_month=current_month,
    )
    income_p = data["income_p"]
    income = data["income"]
    expense = data["expense"]
    an = data["an"]
    open_count = data["open_count"]
    forecast = float(data["forecast"] or 0)

    months_ru = ["Янв.", "Фев.", "Мар.", "Апр.", "Май", "Июн.",
                 "Июл.", "Авг.", "Сен.", "Окт.", "Ноя.", "Дек."]

    def _k(n: float) -> str:
        v = int(n / 1000) if n else 0
        if v == 0:
            return "0"
        sign = "−" if v < 0 else ""
        return f"{sign}{abs(v)}k"

    def _signed_full(n: float) -> str:
        """XX XXXр, типографский минус для отрицательных, без знака для положительных."""
        rounded = int(round(n))
        if rounded == 0:
            return "0р"
        sign = "−" if rounded < 0 else ""
        return f"{sign}{abs(rounded):,}р".replace(",", " ")

    INDENT = "   "
    # Колонки (Python char widths; 🔴 в Telegram ≈ 2 visual, компенсируем -1 у label).
    # Месяц = сокращённое название (Янв...Дек). W_LABEL=9 — таблица сдвинута вправо
    # +4 (запрос user 2026-05-28; ширину тюним по живому). Бейдж 🔴N сохранён.
    W_LABEL = 9
    W_INC_P = 7
    W_INC = 7
    W_EXP = 8
    W_BAL = 9
    BODY_WIDTH = W_LABEL + W_INC_P + W_INC + W_EXP + W_BAL  # 36

    lines: list[str] = []
    lines.append(
        f"{INDENT}{'Месяц':<{W_LABEL}s}"
        f"{'Доход₽':>{W_INC_P}s}"
        f"{'Доход':>{W_INC}s}"
        f"{'Расход':>{W_EXP}s}"
        f"{'Баланс':>{W_BAL}s}"
    )

    balance_real_sum = 0.0
    last_month = max(1, min(current_month, 12))
    for m in range(1, last_month + 1):
        ip = float(income_p.get(m, 0.0) or 0)
        i = float(income.get(m, 0.0) or 0)
        e = float(expense.get(m, 0.0) or 0)
        a = float(an.get(m, 0.0) or 0)
        b = i - e - a
        balance_real_sum += b

        cnt = int(open_count.get(m, 0) or 0)
        if cnt > 0:
            label = f"{months_ru[m - 1]}🔴{cnt}"
            label_width = W_LABEL - 1  # 🔴 ≈ 2 visual chars, len() даёт 1
        else:
            label = months_ru[m - 1]
            label_width = W_LABEL

        lines.append(
            f"{INDENT}{label:<{label_width}s}"
            f"{_k(ip):>{W_INC_P}s}"
            f"{_k(i):>{W_INC}s}"
            f"{_k(e):>{W_EXP}s}"
            f"{_k(b):>{W_BAL}s}"
        )

    # Разделитель + 3 итоговые строки (Баланс / Долг / Прибыль прогноз).
    title_ru = "Баланс (кред)" if is_credit else "Баланс б/н"
    debt_label = "Долг кредит" if is_credit else "Долг б.н."
    forecast_label = "Прибыль (кред) прогноз" if is_credit else "Прибыль прогноз"
    bal_str = _signed_full(balance_real_sum)

    def _unsigned_full(n: float) -> str:
        """XX XXXр без знака, для положительных долгов."""
        rounded = int(round(abs(n)))
        if rounded == 0:
            return "0р"
        return f"{rounded:,}р".replace(",", " ")

    debt_str = _unsigned_full(total_debt)
    fcast_str = _signed_full(forecast)

    sep = "━" * (BODY_WIDTH - 3)
    lines.append(f"{INDENT}{sep}")

    pad_b = max(1, BODY_WIDTH - len(title_ru) - len(bal_str))
    pad_d = max(1, BODY_WIDTH - len(debt_label) - len(debt_str))
    pad_f = max(1, BODY_WIDTH - len(forecast_label) - len(fcast_str))
    lines.append(f"{INDENT}{title_ru}{' ' * pad_b}{bal_str}")
    lines.append(f"{INDENT}{debt_label}{' ' * pad_d}{debt_str}")
    lines.append(f"{INDENT}{forecast_label}{' ' * pad_f}{fcast_str}")

    # Эталон-v2: итог (баланс) не в шапке — он уже в теле строкой «{title_ru} {bal_str}».
    header = f"<b>📈  {title_ru}</b>"

    body = "\n".join(lines)
    return f"{header}\n<pre>{body}</pre>"


def format_manager_invoices_overview(
    initiator: str,
    role_label: str,
    invoices: list[dict[str, Any]],
) -> str:
    """Общая сводная карточка «Мои Счета» для менеджера.

    По образцу docs/rules/feedback_card_template_standard.md:
    заголовок + От + мета (всего/в работе/закрытых/сумма/долг) +
    bold-секция «По месяцам:» (счёт/сумма/в работе) за весь год.
    """
    from collections import defaultdict
    from datetime import datetime as _dt

    _IN_WORK = {"new", "pending", "in_progress", "paid", "closing", "credit", "on_hold"}
    _ENDED = {"ended"}

    def _f(n: float) -> str:
        return f"{n:,.0f}".replace(",", " ")

    total = len(invoices)
    in_work = sum(1 for i in invoices if (i.get("status") or "") in _IN_WORK)
    ended = sum(1 for i in invoices if (i.get("status") or "") in _ENDED)
    sum_amount = sum(float(i.get("amount") or 0) for i in invoices)
    sum_debt = 0.0
    for i in invoices:
        d = i.get("outstanding_debt")
        if d is not None:
            sum_debt += float(d)
        else:
            calc = float(i.get("amount") or 0) - float(i.get("first_payment_amount") or 0)
            if calc > 0:
                sum_debt += calc

    # Группировка по месяцу (receipt_date → fallback created_at)
    months_ru = [
        "Янв", "Фев", "Мар", "Апр", "Май", "Июн",
        "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек",
    ]
    by_month: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"cnt": 0, "amount": 0.0, "in_work": 0, "ended": 0}
    )
    for inv in invoices:
        raw = inv.get("receipt_date") or inv.get("created_at") or ""
        ym: str | None = None
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                ym = _dt.strptime(str(raw)[: len(fmt) - 2 if "%S" in fmt else 10], fmt).strftime("%Y-%m")
                break
            except (ValueError, TypeError):
                continue
        if not ym and isinstance(raw, str) and len(raw) >= 7:
            ym = raw[:7]
        if not ym:
            continue
        bucket = by_month[ym]
        bucket["cnt"] += 1
        bucket["amount"] += float(inv.get("amount") or 0)
        st = (inv.get("status") or "")
        if st in _IN_WORK:
            bucket["in_work"] += 1
        elif st in _ENDED:
            bucket["ended"] += 1

    role_suffix = f" {role_label}" if role_label else ""

    sections: list[str] = []

    sections.append(
        format_card_section(
            emoji="📋",
            title=f"Мои Счета{role_suffix}",
            items=[
                ("В работе", str(in_work)),
                ("Закрытых", str(ended)),
            ],
            total=str(total),
        )
    )

    sections.append(
        format_card_section(
            emoji="💰",
            title="Финансы",
            items=[
                ("Общая сумма", f"{_f(sum_amount)}₽"),
                ("Долг", f"{_f(sum_debt)}₽" if sum_debt > 0 else "0₽"),
            ],
        )
    )

    month_items: list[tuple[str, str]] = []
    if by_month:
        for ym in sorted(by_month.keys()):
            try:
                y, m = ym.split("-")
                label = f"{months_ru[int(m) - 1]} {y}"
            except (ValueError, IndexError):
                label = ym
            b = by_month[ym]
            month_items.append((label, f"{b['cnt']} · {_f(b['amount'])}₽"))
    else:
        month_items.append(("—", "нет данных"))

    sections.append(
        format_card_section(
            emoji="📅",
            title="По месяцам",
            items=month_items,
        )
    )

    return format_card(sections)


def format_lead_stats_card(stats: dict[str, Any]) -> str:
    """Карточка лид-статистики для ГД: периоды × менеджер + воронка РП.

    RP-приоритет (user: «РП — главный источник»): менеджер и источник берутся из
    таблицы РП «Импорт ОП», где есть (by_manager_eff / by_source_eff — считаются
    в get_lead_stats_v2: rp_manager BV / rp_source BW; КИА — по суффиксу имени),
    иначе amoCRM-fallback. Воронка РП — срез по rp_status (BX). Старые ключи
    by_manager/by_source оставлены как fallback для совместимости.

    stats: {
      by_manager_eff: {label: {today, week, month, prev_month, total}},
      funnel_rp: [{status, count}], rp_matched: int, rp_inv_linked: int,
      by_source_eff: [{src, ...periods}],
      unclaimed: {...}, totals: {...}, claimed_total: int,
    }
    """
    by_mgr_eff = stats.get("by_manager_eff") or {}
    by_mgr = stats.get("by_manager") or {}
    unclaimed = stats.get("unclaimed") or {}
    totals = stats.get("totals") or {}
    claimed_total = stats.get("claimed_total") or 0
    grand_total = totals.get("total") or 0
    rp_matched = int(stats.get("rp_matched") or 0)
    rp_inv_linked = int(stats.get("rp_inv_linked") or 0)

    _PKEYS = ("today", "week", "month", "prev_month", "total")

    def _row(label: str, d: dict[str, Any]) -> str:
        t = int(d.get("today") or 0)
        w = int(d.get("week") or 0)
        m = int(d.get("month") or 0)
        pm = int(d.get("prev_month") or 0)
        return f"{label:<10s} {t:>6d} {w:>6d} {m:>6d} {pm:>7d}"

    # Менеджеры — эффективная атрибуция (RP-приоритет, by_manager_eff); fallback
    # на старую by_manager-эвристику, если эфф-ключа нет.
    mgr_rows: list[tuple[str, dict[str, Any]]] = []
    if by_mgr_eff:
        named = sorted(
            ((l, d) for l, d in by_mgr_eff.items() if l != "Без мен."),
            key=lambda x: (-x[1]["month"], -x[1]["total"]),
        )
        no_mgr = [
            (l, d) for l, d in by_mgr_eff.items()
            if l == "Без мен." and int(d.get("total") or 0) > 0
        ]
        mgr_rows = list(named) + no_mgr
    else:
        _labels = {"kv": "КВ", "kia": "КИА", "npn": "НПН", "other": "Без суфф."}
        for code in ("kv", "kia", "npn"):
            mgr_rows.append((_labels[code], by_mgr.get(code) or {k: 0 for k in _PKEYS}))
        other = by_mgr.get("other")
        if other and (int(other.get("total") or 0) > 0):
            mgr_rows.append((_labels["other"], other))

    excluded_n = int(stats.get("excluded_import_count") or 0)
    head_extra = f" | импорт: {excluded_n}" if excluded_n else ""
    rp_extra = f" · РП-сделок: {rp_matched}" if rp_matched else ""
    lines = [
        "📊 <b>Лиды — статистика</b>",
        f"👥 Всего: <b>{grand_total}</b>{rp_extra}  "
        f"(взято: {claimed_total} | не взято: {grand_total - claimed_total}{head_extra})",
        "",
        "<pre>",
        f"{'':<10s} {'Сегодн':>6s} {'Недел':>6s} {'Месяц':>6s} {'Прошл.':>7s}",
    ]
    for label, d in mgr_rows:
        lines.append(_row(label, d))
    lines.append("─" * 40)
    lines.append(_row("Не взято", unclaimed))
    lines.append("─" * 40)
    lines.append(_row("Итого", totals))
    lines.append("</pre>")

    # Воронка РП (rp_status) — текущий срез сделок РП (не за период).
    funnel_rp = stats.get("funnel_rp") or []
    if funnel_rp or rp_inv_linked:
        funnel_total = sum(int(f.get("count") or 0) for f in funnel_rp)
        lines.append("")
        lines.append(f"🔻 <b>Воронка РП</b> — {funnel_total}")
        lines.append("<pre>")
        for f in funnel_rp:
            st = str(f.get("status") or "—")[:16]
            c = int(f.get("count") or 0)
            lines.append(f"   {st:<16s} {c:>4d}")
        if rp_inv_linked:
            lines.append(f"   {'→ счёт':<16s} {rp_inv_linked:>4d}")
        lines.append("</pre>")

    # Источники — RP-приоритет (by_source_eff); fallback by_source.
    by_source = stats.get("by_source_eff") or stats.get("by_source") or []
    if by_source:
        lines.append("")
        lines.append("📌 <b>Источники · РП-приоритет</b>")
        lines.append("<pre>")
        lines.append(
            f"{'Источник':<14s} {'Сег':>4s} {'Нед':>4s} {'Мес':>5s} {'Прошл.':>6s}"
        )
        for s in by_source:
            label = str(s.get("src") or "—")[:14]
            t = int(s.get("today") or 0)
            w = int(s.get("week") or 0)
            m = int(s.get("month") or 0)
            pm = int(s.get("prev_month") or 0)
            lines.append(f"{label:<14s} {t:>4d} {w:>4d} {m:>5d} {pm:>6d}")
        lines.append("</pre>")
    return "\n".join(lines)


def format_discrepancy_card(disc: dict[str, Any]) -> str:
    """Карточка отчёта расхождений «РП ↔ счета» для ГД/ТД (стиль В1).

    disc: {role_mismatch: [...], unlinked: [...], checked: int}
      • role_mismatch — лид сматчен со счётом, но менеджер по номеру счёта ≠ по РП.
      • unlinked — РП проставил №счёта, но счёта с таким номером в БД нет.
    Списки рендерятся compact=True (горизонтальный скролл, без переноса столбиком).
    """
    mism = disc.get("role_mismatch") or []
    unlinked = disc.get("unlinked") or []
    checked = int(disc.get("checked") or 0)

    def _k(v: float) -> str:
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.1f}м"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.0f}к"
        return f"{v:.0f}"

    parts: list[str] = ["🔀 <b>Расхождения РП ↔ счета</b>"]

    if mism:
        items: list[tuple[str, str]] = []
        for d in mism:
            num = d.get("invoice_number") or "—"
            val = f"счёт {d.get('invoice_manager') or '?'} · РП {d.get('rp_manager') or '?'}"
            amt = float(d.get("amount") or 0)
            if amt:
                val += f" · {_k(amt)}₽"
            items.append((f"№{num}", val))
        parts.append(format_card_section(
            "🧮", "Менеджер не совпал", items,
            total=str(len(mism)), compact=True,
        ))

    if unlinked:
        u_items: list[tuple[str, str]] = []
        for d in unlinked:
            num = d.get("invoice_number") or "—"
            who = d.get("rp_manager") or d.get("name") or ""
            u_items.append((f"№{num}", who))
        parts.append(format_card_section(
            "🔗", "№счёта без счёта в БД", u_items,
            total=str(len(unlinked)), compact=True,
        ))

    if not mism and not unlinked:
        parts.append(format_card_section(
            "✅", "Расхождений нет",
            [("Проверено лидов с №счёта", str(checked))],
            compact=True,
        ))
    else:
        parts.append(format_card_section(
            "📊", "Итог",
            [
                ("Проверено", str(checked)),
                ("Менеджер не совпал", str(len(mism))),
                ("№счёта без счёта", str(len(unlinked))),
            ],
            compact=True,
        ))

    return "\n".join(parts)


def format_ended_invoice_compact(inv: dict[str, Any], pf: dict[str, Any]) -> str:
    """Компактная карточка ended-счёта для списка ГД."""
    num = inv.get("invoice_number") or f"#{inv.get('id', '?')}"
    addr = (inv.get("object_address") or "—")[:25]
    role_label = {
        "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
    }.get(inv.get("creator_role", ""), "Менеджер")

    def _k(v: float) -> str:
        """Format as compact thousands: 179000 → 179к, 1200000 → 1.2м."""
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.1f}м"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.0f}к"
        return f"{v:.0f}"

    mat_p = pf.get("materials_total", 0)
    inst_p = pf.get("estimated_installation", 0)
    log_p = pf.get("estimated_logistics", 0)
    load_p = pf.get("estimated_loaders", 0)
    cost_p = pf.get("estimated_total_cost", 0)
    profit_p = pf.get("estimated_profit", 0)
    mgr_zp_p = pf.get("manager_zp", 0)
    gd_profit_p = pf.get("gd_profit", 0)

    cost_card = pf.get("cost_card", {})
    # Факт материалы (как в format_plan_fact_card)
    _sp_mat = 0.0
    _sp_svc = 0.0
    _SP_CAT = {"profile": "mat", "glass": "mat", "ldsp": "mat",
               "gkl": "mat", "sandwich": "mat", "other": "mat",
               "service": "svc"}
    for _sp in cost_card.get("supplier_payments_list", []):
        if _SP_CAT.get(_sp.get("material_type", "other"), "mat") == "svc":
            _sp_svc += _sp.get("amount", 0)
        else:
            _sp_mat += _sp.get("amount", 0)
    mat_f = cost_card.get("materials_combined", 0) + _sp_mat
    inst_f = cost_card.get("montazh_combined", float(cost_card.get("zp_installer", 0))) + _sp_svc
    log_f = cost_card.get("logistics_fact", 0)
    load_f = cost_card.get("loaders_fact", 0)
    nds_p = pf.get("net_vat", 0)
    nds_f = cost_card.get("nds_fact", 0) + cost_card.get("profit_tax_fact", 0)
    cost_f = pf.get("actual_total_cost", 0)
    profit_f = pf.get("actual_profit", 0)
    mgr_zp_f = cost_card.get("zp_manager", 0)

    # Сроки
    dl = (inv.get("deadline_end_date") or "")[:10]
    compl = (inv.get("completion_date") or inv.get("updated_at") or "")[:10]
    srok = f"{dl}→{compl}" if dl else (compl or "—")

    # Прибыль компании: profit - RP ZP - manager ZP
    rp_zp_p = pf.get("rp_zp", 0)
    company_p = profit_p - rp_zp_p - mgr_zp_p if profit_p > 0 else profit_p
    company_f = profit_f - cost_card.get("zp_zamery", 0) - mgr_zp_f - float(cost_card.get("zp_installer", 0))

    return (
        f"<b>№{num}</b> | {role_label} | {addr}\n"
        f"  Мат: {_k(mat_p)}/{_k(mat_f)}  Уст: {_k(inst_p)}/{_k(inst_f)}\n"
        f"  Лог: {_k(log_p)}/{_k(log_f)}  Груз: {_k(load_p)}/{_k(load_f)}\n"
        f"  Налог: {_k(nds_p)}/{_k(nds_f)}  Срок: {srok}\n"
        f"  Итого: {_k(cost_p)}/{_k(cost_f)}  Приб: {_k(profit_p)}/{_k(profit_f)}\n"
        f"  ЗП мен: {_k(mgr_zp_p)}/{_k(mgr_zp_f)}  Комп: {_k(company_p)}/{_k(company_f)}"
    )


def compute_plan_profit(
    *,
    amount: float,
    est_glass: float = 0.0,
    est_profile: float = 0.0,
    est_mat_legacy: float = 0.0,
    est_inst: float = 0.0,
    est_load: float = 0.0,
    est_log: float = 0.0,
    is_credit: bool = False,
    client_source: str = "own",
) -> dict[str, float]:
    """ЕДИНЫЙ расчёт ПЛАНОВОЙ прибыли + распределения (единственный источник истины).

    Зеркалит налоговую логику факт-стороны db.get_full_invoice_cost_card:
      • Кредитные счета → НДС = 0 (для кредита налоги не начисляются). Иначе плановая
        прибыль кредитного счёта занижалась на полный output_vat.
      • Иначе net_vat = выходной(Сумма) − возвратный(стекло+профиль), ставка 22/122.
    Распределение прибыли: РП 10%; остаток лид-ГД 25(мен)/75(ГД), свой клиент 50/50.
    При прибыли ≤ 0 ВСЕ доли = 0 (раньше гард стоял только у rp_zp → manager_zp/gd_profit
    уходили в минус). Введён 2026-06-19 (user), чтобы 5 копий этого расчёта
    (db.get_plan_fact_card, format_estimated_summary, manager_new ×2, sheet_commands)
    не расходились в будущем.
    """
    amount = float(amount or 0)
    est_glass = float(est_glass or 0)
    est_profile = float(est_profile or 0)
    est_mat_legacy = float(est_mat_legacy or 0)
    est_inst = float(est_inst or 0)
    est_load = float(est_load or 0)
    est_log = float(est_log or 0)

    materials_total = est_glass + est_profile + est_mat_legacy
    est_total = materials_total + est_inst + est_load + est_log

    if is_credit:
        output_vat = 0.0
        input_vat = 0.0
        net_vat = 0.0
    else:
        refundable_base = est_glass + est_profile  # возвратный НДС: стекло + профиль
        output_vat = amount * 22 / 122 if amount > 0 else 0.0
        input_vat = refundable_base * 22 / 122 if refundable_base > 0 else 0.0
        net_vat = output_vat - input_vat

    est_profit = amount - est_total - net_vat
    est_pct = (est_profit / amount * 100) if amount > 0 else 0.0

    if est_profit > 0:
        rp_zp = est_profit * 0.10
        remaining = est_profit - rp_zp
        if client_source == "gd_lead":
            manager_zp = remaining * 0.25
            gd_profit = remaining * 0.75
        else:
            manager_zp = remaining * 0.50
            gd_profit = remaining * 0.50
    else:
        rp_zp = 0.0
        manager_zp = 0.0
        gd_profit = 0.0

    return {
        "materials_total": materials_total,
        "est_total": est_total,
        "output_vat": output_vat,
        "input_vat": input_vat,
        "net_vat": net_vat,
        "est_profit": est_profit,
        "est_pct": est_pct,
        "rp_zp": rp_zp,
        "manager_zp": manager_zp,
        "gd_profit": gd_profit,
    }


def manager_zp_net_payout(inv: dict[str, Any]) -> float:
    """ЗП менеджера К ВЫПЛАТЕ с учётом удержания (переплаты CN/zp_manager_hold).

    Механизм перерасчёта (owner 2026-06-23): удержание из ЗП менеджера (CN,
    хранится со знаком — отрицательное = удержать) применяется ТОЛЬКО когда у
    счёта погашен долг (outstanding_debt == 0, т.е. сделан финальный платёж).
    Пока по материнскому счёту есть долг — счёт НЕ входит в механизм, ЗП = бланк
    без удержания. Флор 0: нельзя выплатить отрицательную ЗП (в карточке истинное
    значение показывается отдельно). РП 10% и доля ГД не трогаются (scope = только
    ЗП менеджера). Единый источник net-выплаты для хендлеров и карточек.

    Перенос переплаты в аванс (owner 2026-06-23): когда удержание (или его часть)
    уже перенесено на баланс аванса менеджера (zp_hold_advanced), эта часть БОЛЬШЕ
    НЕ вычитается пер-счётно — бланк платится полностью, а переплата гасится
    распределением аванса по объектам. min(advanced, |hold|) — кап: даже если CN
    вручную уменьшили после переноса, net не превысит бланк (защита от переплаты).
    """
    blank = float(inv.get("manager_zp_blank") or 0)
    if abs(float(inv.get("outstanding_debt") or 0)) >= 1:
        return blank  # есть долг → удержание пока не применяется
    hold = float(inv.get("zp_manager_hold") or 0)
    advanced = float(inv.get("zp_hold_advanced") or 0)
    # hold ≤ 0; advanced ≥ 0 — возвращает перенесённую в аванс часть удержания.
    return max(0.0, blank + hold + min(advanced, abs(hold)))


def format_estimated_summary(inv: dict[str, Any]) -> str:
    """Краткая сводка расчётных данных для менеджера."""
    amount = float(inv.get("amount") or 0)
    est_glass = float(inv.get("estimated_glass") or 0)
    est_profile = float(inv.get("estimated_profile") or 0)
    est_mat_legacy = float(inv.get("estimated_materials") or 0)
    est_inst = float(inv.get("estimated_installation") or 0)
    est_load = float(inv.get("estimated_loaders") or 0)
    est_log = float(inv.get("estimated_logistics") or 0)
    # ЕДИНЫЙ helper (credit-aware НДС + гард распределения).
    _pp = compute_plan_profit(
        amount=amount, est_glass=est_glass, est_profile=est_profile,
        est_mat_legacy=est_mat_legacy, est_inst=est_inst, est_load=est_load,
        est_log=est_log, is_credit=bool(inv.get("is_credit")),
        client_source=inv.get("client_source") or "own",
    )
    materials_total = _pp["materials_total"]
    est_total = _pp["est_total"]
    output_vat = _pp["output_vat"]
    input_vat = _pp["input_vat"]
    net_vat = _pp["net_vat"]
    est_profit = _pp["est_profit"]
    est_pct = _pp["est_pct"]

    if not any([est_glass, est_profile, est_mat_legacy, est_inst, est_load, est_log]):
        return "📊 Расчётные данные: <i>не заполнены</i>"

    lines = [
        "📊 <b>Расчётные данные:</b>",
        f"  Стекло: {est_glass:,.0f}₽",
        f"  Ал.профиль: {est_profile:,.0f}₽",
    ]
    if est_mat_legacy > 0:
        lines.append(f"  Мат.(стар.): {est_mat_legacy:,.0f}₽")
    lines += [
        f"  Установка: {est_inst:,.0f}₽",
        f"  Грузчики: {est_load:,.0f}₽",
        f"  Логистика: {est_log:,.0f}₽",
        f"  Чистый НДС: {net_vat:,.0f}₽ (возвр. -{input_vat:,.0f}₽)",
        f"  Расч.себест-ть: {est_total:,.0f}₽",
        f"  Расч.прибыль: {est_profit:,.0f}₽ ({est_pct:.1f}%)",
    ]
    return "\n".join(lines)


def format_card_section(
    emoji: str,
    title: str,
    items: list[tuple[str, str]],
    total: str | None = None,
    footer: tuple[str, str] | None = None,
    width: int = 32,
    compact: bool = False,
    sep_ratio: float = 1.0,
) -> str:
    """Рендер одной секции карточки в эталонном дизайне.

    Эталон: docs/rules/feedback_card_template_standard.md (assets/card_etalon.png).

    Структура (compact=False, default):
        <b>{emoji}  {title} ━━━━━━━━ {total}</b>            ← опц. total
        <pre>   {label_1}{padding}{value_1}
           {label_2}{padding}{value_2}
           ━━━━━━━━━━━━━━━━━━━━━━━━━━━━                     ← опц. separator
           {footer_label}{padding}{footer_value}</pre>      ← опц. footer

    Структура (compact=True) — без right-align padding, label/value на одной
    логической строке, длинные значения не переносятся столбиком:
        <pre>   {label_1}: {value_1}
           {label_2}: {value_2}</pre>

    Заголовок — bold plain text ВНЕ <pre> (теги форматирования внутри <pre>
    в Telegram не работают). Тело — моноширинный <pre>-блок (серый «конверт»).
    """
    HEAVY = "━"
    INDENT = "   "

    # Эталон-v2 (07.06): итог НИКОГДА не в шапке — только название.
    # total/footer рендерятся строками в теле (см. ниже).
    header = f"<b>{emoji}  {title}</b>"

    body_lines: list[str] = []
    for label, value in items:
        if compact:
            # "Label: Value" в одну логическую строку, без right-align
            # padding. Telegram внутри <pre> делает горизонтальный скролл
            # для длинных строк (а не word-wrap столбиком).
            if value:
                body_lines.append(f"{INDENT}{label}: {value}")
            else:
                body_lines.append(f"{INDENT}{label}")
        else:
            used = len(INDENT) + len(label) + len(value)
            pad_n = max(1, width - used)
            body_lines.append(f"{INDENT}{label}{' ' * pad_n}{value}")

    foot_rows: list[tuple[str, str]] = []
    if footer is not None:
        foot_rows.append(footer)
    if total is not None:
        foot_rows.append(("Итого", total))
    if foot_rows:
        # sep_ratio<1.0 укорачивает разделитель (━ ≈ 2 симв., user 2026-06-10);
        # default 1.0 сохраняет прежнюю длину для всех прочих карточек.
        sep_len = max(3, int((width - len(INDENT)) * sep_ratio))
        body_lines.append(INDENT + HEAVY * sep_len)
        for flabel, fvalue in foot_rows:
            if compact:
                body_lines.append(f"{INDENT}{flabel}: {fvalue}" if fvalue else f"{INDENT}{flabel}")
            else:
                used = len(INDENT) + len(flabel) + len(fvalue)
                pad_n = max(1, width - used)
                body_lines.append(f"{INDENT}{flabel}{' ' * pad_n}{fvalue}")

    body = "\n".join(body_lines)
    return f"{header}\n<pre>{body}</pre>"


def format_zamery_settlement_card(
    summary: dict[str, Any], surveyor_name: str,
) -> str:
    """Карточка взаиморасчётов с замерщиком (эталон <pre>, числа справа, без ₽).

    summary — из Database.get_zamery_settlement_summary(). Блок 1 — сводка
    (начислено/нач.долг/оплачено → текущий долг). Блок 2 — движения (платежи/
    правки) по датам. Начисления берутся из выполненных замеров, не из леджера.
    """
    def _n(x: float | int | None) -> str:
        return f"{int(round(x or 0)):,}".replace(",", " ")

    items: list[tuple[str, str]] = [
        ("Замеров (вып.)", str(summary.get("n_measurements", 0))),
        ("Начислено", _n(summary.get("charges"))),
    ]
    if summary.get("opening"):
        items.append(("Нач. долг", _n(summary.get("opening"))))
    if summary.get("adjustments"):
        items.append(("Корректировки", _n(summary.get("adjustments"))))
    items.append(("Оплачено", _n(summary.get("paid"))))

    card = format_card_section(
        "💰", f"Взаиморасчёты — {html.quote(surveyor_name)}",
        items=items, footer=("Текущий долг", _n(summary.get("debt"))),
    )

    moves = [
        e for e in (summary.get("entries") or [])
        if e.get("kind") in ("payment", "adjustment")
    ]
    if moves:
        rows: list[tuple[str, str]] = []
        for e in moves[:20]:
            d = str(e.get("entry_date") or "")[:10]
            try:
                d = datetime.fromisoformat(d).strftime("%d.%m.%Y")
            except (ValueError, TypeError):
                pass
            sign = "−" if e.get("kind") == "payment" else "±"
            label = d + (f" · {e['comment']}" if e.get("comment") else "")
            rows.append((label, f"{sign}{_n(e.get('amount'))}"))
        card += "\n" + format_card_section("💸", "Платежи", items=rows, compact=True)
    return card


def format_zamery_settlement_detail_cards(rows: list[dict[str, Any]]) -> str:
    """Помесячная детализация взаиморасчётов с замерщиком (Вариант A, эталон, ТЗ 14.07).

    Второй блок под сводкой format_zamery_settlement_card на экране ГД «Взаиморасчёты
    с замерщиком». По каждому выполненному замеру: дата · стоимость · дата оплаты ·
    менеджер+улица; группировка по месяцу замера, месячный «Итого» (начислено /
    оплачено). Числа держим СЛЕВА (моноширинно от фикс. старта строки), менеджер+улица —
    в КОНЦЕ, где переменная пиксель-ширина кириллицы уже ни на что не влияет
    (feedback_card_telegram_pre_alignment, mobile-safe). Read-only витрина
    (feedback_card_display_only_no_data_writes). rows — Database.list_zamery_settlement_detail().
    """
    if not rows:
        return ""
    INDENT = "   "
    _MONTHS = ("", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль",
               "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь")
    _MGR = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}

    def _n(x: float | int | None) -> str:
        return f"{int(round(x or 0)):,}".replace(",", " ")

    def _dd(iso: Any) -> str:
        s = str(iso or "")
        return f"{s[8:10]}.{s[5:7]}" if len(s) >= 10 else "—"

    def _zam_street(raw: Any) -> str:
        """Название улицы ПОЛНОСТЬЮ (owner 14.07: показывать у каждого замера, не терять).

        Срезаем префикс «г. Город,» и хвостовой номер дома («13-42», «89с2», «31-39»),
        но НЕ трогаем внутренние цифры (иначе «2-я Звенигородская» → «?», как ломал
        прежний rp_start_card._street). Без обрезки длины — улица в конце строки,
        Telegram <pre> длинную строку скроллит, колонки слева не съезжают.
        """
        a = str(raw or "").strip()
        a = re.sub(r'^г\.?\s*[А-Яа-яЁё-]+\s*,\s*', '', a)              # «г. Город,»
        cut = re.sub(r'[\s,]+\d[\w\-/.]*\s*$', '', a, flags=re.UNICODE).strip(' ,.')
        cut = re.sub(r'\s+', ' ', cut)
        return cut or a or "?"

    # Группировка по (год, месяц) замера. rows уже сорт по scheduled_date → порядок
    # групп естественный (по возрастанию даты).
    groups: list[tuple[int, list[dict[str, Any]]]] = []
    index: dict[tuple[int, int], int] = {}
    for r in rows:
        s = str(r.get("scheduled_date") or "")
        key = (int(s[0:4]), int(s[5:7])) if len(s) >= 7 else (0, 0)
        if key not in index:
            index[key] = len(groups)
            groups.append((key[1], []))
        groups[index[key]][1].append(r)

    sections: list[str] = []
    for month, items in groups:
        title = _MONTHS[month] if 1 <= month <= 12 else "Без даты"
        body: list[str] = []
        tot = paid_tot = 0.0
        for r in items:
            cost = float(r.get("total_cost") or 0)
            tot += cost
            paid = r.get("paid_amount") is not None
            pd = r.get("paid_date")
            if paid:
                paid_tot += cost
            opl = (_dd(pd) if pd else "опл") if paid else "—"
            mgr = _MGR.get(str(r.get("requester_role") or ""), "—")
            addr = html.quote(_zam_street(r.get("address")))
            body.append(
                f"{INDENT}{_dd(r.get('scheduled_date'))}  {_n(cost):>6}  {opl:>5}  {mgr} {addr}"
            )
        body.append(INDENT + "━" * 26)
        body.append(f"{INDENT}Итого {_n(tot):>6}  опл {_n(paid_tot)}")
        sections.append(f"<b>📅  {title}</b>\n<pre>" + "\n".join(body) + "</pre>")
    return "\n\n".join(sections)


def format_leads_section(
    *,
    emoji: str,
    title: str,
    today: int,
    month: int,
    mgr_rows: list[tuple[str, str]],
    src_rows: list[tuple[str, str]],
    funnel_rows: list[tuple[str, str]] | None = None,
    width: int = 32,
    show_header_total: bool = False,
) -> str:
    """Секция «Лиды» одним <pre>-блоком (общая для РП- и ГД-карточек).

    Сегодня/За месяц — основной уровень (отступ 3). Подгруппы «Менеджеры»/
    «Источники» — строка-подзаголовок с маркером ◎, их данные — с увеличенным
    отступом (6). Жирный/подчёркнутый ВНУТРИ <pre> Telegram не рисует, поэтому
    иерархия подгрупп задаётся маркером + отступом. Итог в шапке = за месяц.
    User 31.05 (переверстка для читаемости подгрупп; раньше были отдельные
    под-блоки/строки-подзаголовки без выделения).
    """
    HEAVY = "━"
    INDENT = "   "        # основной уровень (3 пробела)
    SUB = "      "        # данные подгруппы — глубже (6 пробелов)
    MARK = "◎ "

    def _line(indent: str, label: str, value: str) -> str:
        used = len(indent) + len(label) + len(value)
        return f"{indent}{label}{' ' * max(1, width - used)}{value}"

    body_lines = [
        _line(INDENT, "Сегодня", str(today)),
        _line(INDENT, "За месяц", str(month)),
    ]
    if mgr_rows:
        body_lines.append(f"{INDENT}{MARK}Менеджеры")
        body_lines.extend(_line(SUB, lbl, val) for lbl, val in mgr_rows)
    if funnel_rows:
        body_lines.append(f"{INDENT}{MARK}Воронка РП")
        body_lines.extend(_line(SUB, lbl, val) for lbl, val in funnel_rows)
    if src_rows:
        body_lines.append(f"{INDENT}{MARK}Источники")
        body_lines.extend(_line(SUB, lbl, val) for lbl, val in src_rows)

    if show_header_total:
        total_s = str(month)
        prefix_visible = 2 + 2 + len(title) + 1
        fill = max(3, width - prefix_visible - len(total_s) - 1)
        header = f"<b>{emoji}  {title} {HEAVY * fill} {total_s}</b>"
    else:
        header = f"<b>{emoji}  {title}</b>"
    return f"{header}\n<pre>" + "\n".join(body_lines) + "</pre>"


def format_card(sections: list[str]) -> str:
    """Склейка нескольких <pre>-секций карточки через пустую строку.

    Каждая секция — результат format_card_section(). Используется для карточек
    с несколькими блоками (sync, dashboard, task с деталями).
    """
    return "\n\n".join(s for s in sections if s)


def credit_wallet_label(role: str) -> str:
    """Короткая метка кошелька менеджера для кредит-карточек/журнала."""
    return {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}.get(role, role)


async def build_credit_wallet_card(db: "Database", role: str, *, recent: int = 10, show_header_total: bool = True) -> str:
    """Карточка «Кредитный баланс» кошелька менеджера (эталон, TZ 02.06; relayout 2026-06-10).

    Источник остатка — db.get_credit_balance_summary(role) (carry-DA): остатки
    переносятся на последний открытый кредит-счёт роли, total_da = его DA.

    Вёрстка (user 2026-06-10, feedback_card_telegram_pre_alignment): числа справа,
    ₽ из ячеек убран ВЕЗДЕ (шапка + движения; рубли подразумеваются — ₽ не
    моноширинный в Telegram <pre> и ломал бы выравнивание). Низ — ОДНА строка
    «Итого» (= остаток); прежний дубль «Остаток»+«Итого» убран, показывается
    всегда (в т.ч. finance-канал ГД, show_header_total=False — параметр оставлен
    для совместимости вызовов, на вёрстку больше НЕ влияет). Блок «Последние
    движения» — модель активного счёта (user 2026-06-10): СХОДИТСЯ со сводкой.
    Первая строка «баланс» = перенос-остаток с предыдущего счёта (carry_in
    активного; для КВ это реальный остаток КВ-8), далее приход активного счёта и
    траты кошелька, хронологически. Σ(баланс+приход) − Σтрат = «Итого». Сырые
    приходы прочих открытых счетов НЕ показываем — они свёрнуты в перенос (раньше
    показывались и Σприходов ≠ «Вход», что путало; TZ переноса 02–04.06 цел).
    """
    label = credit_wallet_label(role)

    def _num(n: Any) -> str:
        # Без ₽ в ячейках: ₽ не моноширинный в Telegram <pre> и ломает выравнивание
        # чисел (feedback_card_telegram_pre_alignment). Рубли подразумеваются. Минус → U+2212.
        return f"{float(n or 0):,.0f}".replace(",", " ").replace("-", "−")

    def _k(n: Any) -> str:
        # Компактный формат тысяч ТОЛЬКО для «Последних движений» (user 2026-06-10,
        # вариант «к»): «558 000»→«558к», «24 813»→«24.8к» — без пробела внутри числа,
        # поэтому строка не переносится на узком экране (баг «36\n000» уходит). Знак
        # минус → U+2212. «Итого»/баланс в шапке остаются полными (_num) — там точность.
        v = float(n or 0)
        if not v:
            return "0"
        sign = "−" if v < 0 else ""
        t = abs(v) / 1000.0
        s = f"{t:.0f}к" if abs(t - round(t)) < 0.05 else f"{t:.1f}к"
        return sign + s

    cs = await db.get_credit_balance_summary(role)
    invs = cs.get("invoices") or []
    total_da = float(cs.get("total_da") or 0)
    last_open = None
    for r in invs:
        if not r.get("is_closed"):
            last_open = r
    # «Итого» (остаток). При наличии авторитетного якоря-сверки (durable-модель
    # 19.06) показываем ТОЧНОЕ значение — сверка это чистое число owner, без мусорных
    # копеек, и должно совпасть с «баланс» в движениях. Иначе — legacy округление
    # ВНИЗ до 1000 (carry-DA имел дробь; user 2026-06-10: 314 187,41 → «314 000»).
    _wallet_anchor = await db.get_latest_credit_wallet_anchor(role)
    if _wallet_anchor:
        bal_s = _num(total_da)
    else:
        bal_s = _num((total_da // 1000) * 1000)
    # Строки «Вход (с переносом)» и «Расход» убраны из сводки (user 2026-06-10):
    # в шапке остаётся только «Активный счёт» + «Итого» (= остаток). Разбивка
    # вход/расход видна в блоке «Последние движения». cv/cx больше не показываем.
    items: list[tuple[str, str]] = []
    if last_open:
        items.append(("Активный счёт", str(last_open.get("invoice_number") or "—")))
    # Низ: единая строка «Итого» (= остаток баланса). Дубль «Остаток» убран
    # (user 2026-06-10); показываем всегда — иначе в finance-канале ГД
    # (show_header_total=False) пропала бы строка баланса.
    main = format_card_section(
        emoji="🏦",
        title=f"Кредитный баланс — {label}",
        items=items,
        footer=("Итого", bal_s),
        width=36,
        sep_ratio=0.5,  # разделитель вдвое короче (user 2026-06-10)
    )

    # ── Блок «Последние движения» — вариант A (user 2026-06-11). Числа выровнены
    # столбцом СЛЕВА, улица в КОНЦЕ строки (правило feedback_card_telegram_pre_alignment:
    # кириллица-в-середине ломает выравнивание чисел на Android). Формат строки:
    #   {дата} {стрелка}{иконка} {сумма-к:справа} {инициатор} {улица}
    # и СЛЕДУЮЩЕЙ строкой running-баланс кошелька («баланс {остаток:справа}»). Грузчики
    # (loaders ИЛИ desc~грузчик) свёрнуты в 1 строку за день («Грузчики ×N»); прочие
    # расходы — каждый отдельной строкой (merge «↩️ ещё N трат» убран). running
    # считается по ОТОБРАЖАЕМЫМ строкам (перенос+приход − траты) и сходится с «Итого».
    # Шапку «Кредитный баланс» НЕ трогаем (feedback_design_only_indicated_block).
    from .rp_start_card import _street as _street_fn, vw as _vw

    _ICON = {"metal": "🔩", "glass": "🔷", "loaders": "💪", "logist": "🚚",
             "logistics": "🚚", "extra_mat": "🧱", "extra_svc": "🧾", "montazh": "👷"}
    _CASH = "💸"   # снятие/аванс ГД (без категории и улицы)
    _ABBR = {"gd": "ГД", "rp": "РП", "td": "ТД", "accounting": "Бух",
             "manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН",
             "installer": "Монт", "manager": "Мен"}
    _PREFIX_W, _SUM_W, _ABBR_W, _ST_W = 10, 6, 3, 10

    def _pad(s: str, width: int, right: bool = False) -> str:
        gap = max(0, width - _vw(s))
        return (" " * gap + s) if right else (s + " " * gap)

    def _abbr(role_str: Any) -> str:
        if not role_str:
            return label
        first = str(role_str).split(",")[0].strip()
        return _ABBR.get(first, first[:3].upper())

    moves_body = ""
    try:
        bundle = await db.list_all_credit_events(limit=500)
        mgr = next(
            (m for m in (bundle.get("managers") or []) if m.get("role") == role),
            None,
        )
        events = (mgr or {}).get("events") or []
        active_id = (last_open or {}).get("id")

        # карта entered_by -> роль (для аббревиатуры инициатора)
        _ids = {int(e["entered_by"]) for e in events if e.get("entered_by")}
        _umap: dict[int, str] = {}
        if _ids:
            _q = ",".join("?" * len(_ids))
            _cur = await db.conn.execute(
                f"SELECT telegram_id, role FROM users WHERE telegram_id IN ({_q})",
                tuple(_ids),
            )
            for _r in await _cur.fetchall():
                _umap[int(_r["telegram_id"])] = _r["role"]

        def _d(ts: Any) -> str:
            ts = str(ts or "")
            return f"{ts[8:10]}.{ts[5:7]}" if len(ts) >= 10 and ts[4] == "-" else ts[:5]

        # Модель «активного счёта» (user 2026-06-10): движения сходятся со сводкой
        # (перенос + приход − траты = Итого). Опорные строки: «перенос»-остаток с
        # предыдущего счёта (carry_in активного) + приход активного счёта.
        carry_in = 0.0
        prev_ts = ""
        if active_id is not None:
            try:
                carry_in = float(await db.get_credit_carry_in(int(active_id)))
            except Exception:
                carry_in = 0.0
            idx = next((i for i, r in enumerate(invs) if r.get("id") == active_id), None)
            if idx is not None and idx > 0:
                prev_id = invs[idx - 1].get("id")
                prev_in = next(
                    (e for e in events
                     if e.get("kind") == "in" and e.get("invoice_id") == prev_id),
                    None,
                )
                prev_ts = str((prev_in or {}).get("ts") or "")

        # Строки движений: (ts, kind, icon, amount, signed, abbr, street).
        rows: list[tuple[str, str, str, float, float, str, str]] = []
        # Доплаты ПРОЧИХ (не активного) кредит-счетов роли — гашение долга
        # (income_kind='debt_payment', invoice_id≠active). Эти приходы «зашиты» в
        # перенос-остаток активного (carry_in = маркер «Остаток …», включающий
        # доплаты — сверка 2026-06-13). Показываем их ОТДЕЛЬНЫМИ строками с №
        # материнского счёта (КВ5/КВ6), а перенос УМЕНЬШАЕМ ровно на их сумму:
        # Σ строк не меняется → running по-прежнему сходится с «Итого» (user 2026-06-13).
        # Откат к прежнему показу (доплаты свёрнуты в перенос), если вычет увёл бы
        # перенос в минус (доплаты не входят в маркер — иная роль/состояние).
        _all_doplaty = [
            e for e in events
            if e.get("kind") == "in"
            and e.get("income_kind") == "debt_payment"
            and e.get("invoice_id") != active_id
        ]
        # Доплаты ПОСЛЕ сверки-якоря показываем ВСЕГДА отдельными строками (user
        # 26.06): иначе при сворачивании в «перенос» (когда carry_in < Σдоплат)
        # сверка обнулила бы их и running не сошёлся бы с «Итого». Доплаты ДО якоря
        # (или вовсе без якоря) — прежняя логика (фолд в перенос при отрицат. carry).
        _anchor_ts_rows = str(_wallet_anchor["created_at"]) if _wallet_anchor else ""
        if _anchor_ts_rows:
            post_doplaty = [e for e in _all_doplaty if str(e.get("ts") or "") > _anchor_ts_rows]
            other_doplaty = [e for e in _all_doplaty if str(e.get("ts") or "") <= _anchor_ts_rows]
        else:
            post_doplaty = []
            other_doplaty = _all_doplaty
        _sum_other = sum(float(e.get("amount") or 0) for e in other_doplaty)
        _carry_base = carry_in - _sum_other
        _split = bool(other_doplaty) and _carry_base > -0.005
        if not _split:
            _carry_base = carry_in
        if abs(_carry_base) > 0.005:
            rows.append((prev_ts, "bal", "⚖️", _carry_base, _carry_base, label, "перенос"))

        def _doplata_row(e: dict) -> tuple:
            # Доплата = приход ДС по материнскому счёту: ⬆️💵 + № счёта вместо улицы.
            _amt = float(e.get("amount") or 0)
            return (
                str(e.get("ts") or ""), "in", "💵",
                _amt, _amt, label,
                (str(e.get("invoice_number") or "").strip() or "доплата")[:_ST_W],
            )

        if _split:
            for e in other_doplaty:
                rows.append(_doplata_row(e))
        for e in post_doplaty:
            rows.append(_doplata_row(e))
        # Приход активного счёта: base (оплачено при создании) + DISTINCT строки
        # гашения долга (income_kind='debt_payment' — оконч.доплата AC, п.3 2026-06-12).
        # Σ(base+доплаты)=amount−долг → running сходится с «Итого». Доплата помечена
        # 💵 + меткой «доплата» вместо улицы (это приход ДС, не объект).
        for e in events:
            if e.get("kind") != "in" or e.get("invoice_id") != active_id:
                continue
            _is_pay = (e.get("income_kind") == "debt_payment")
            _amt = float(e.get("amount") or 0)
            rows.append((
                str(e.get("ts") or ""), "in", ("💵" if _is_pay else ""),
                _amt, _amt, label,
                ("доплата" if _is_pay else _street_fn(e.get("object_address") or "", _ST_W)),
            ))

        # Расходы; грузчики (loaders ИЛИ desc~грузчик) сворачиваем в 1 строку за день.
        def _is_load(e: dict) -> bool:
            return e.get("cost_type") == "loaders" or "грузчик" in (e.get("description") or "").lower()

        _load_day: dict[str, dict] = {}
        for e in events:
            if e.get("kind") != "out":
                continue
            ets = str(e.get("ts") or "")
            amt = float(e.get("amount") or 0)
            ab = _abbr(_umap.get(int(e["entered_by"]))) if e.get("entered_by") else label
            if _is_load(e):
                day = ets[:10]
                d = _load_day.setdefault(day, {"ts": ets, "sum": 0.0, "n": 0, "ab": ab})
                d["sum"] += amt
                d["n"] += 1
                if ets < d["ts"]:
                    d["ts"] = ets
                continue
            ct = e.get("cost_type")
            _desc = e.get("description") or ""
            addr = e.get("object_address") or ""
            if _desc.startswith("ЗП менеджера"):
                # п.6 (2026-06-12): ЗП менеджера из кредит-кошелька — distinct строка
                # 👔 «ЗП №X» (а не 💸 + обрезка описания). Списание делает забор ЗП по
                # кредит-счёту через гейт ГД (mode='withdraw', desc='ЗП менеджера №X').
                icon = "👔"
                _zpn = _desc.split("№", 1)[1].strip() if "№" in _desc else ""
                street = (f"ЗП №{_zpn}" if _zpn else "ЗП мен")[:_ST_W]
            else:
                icon = _ICON.get(ct or "", _CASH)
                # user 12.06: расход в истории кошелька показывает привязку к материнскому
                # счёту (его №) — а если привязки нет, назначение. Прежде показывали улицу.
                _inv_no = (e.get("invoice_number") or "").strip()
                if _inv_no:
                    street = _inv_no[:_ST_W]
                elif addr:
                    street = _street_fn(addr, _ST_W)
                else:
                    street = (_desc or "расход")[:_ST_W]
            rows.append((ets, "out", icon, amt, -amt, ab, street))
        for _day, d in _load_day.items():
            rows.append((d["ts"], "out", "💪", d["sum"], -d["sum"], d["ab"], f"Грузчики ×{d['n']}"))

        # 2026-06-20 (откат обрезки истории от 19.06 — по запросу owner): durable-
        # якорь по-прежнему задаёт «Итого» в шапке (total_da от якоря, см.
        # get_credit_balance_summary), НО блок «Последние движения» снова показывает
        # ПОЛНУЮ историю кошелька (перенос + все приходы/траты), как до 19.06. Прежний
        # якорь-ребейз («сверка» + ТОЛЬКО события после якоря) обрезал видимую историю
        # до момента сверки — owner этого не запрашивал. running внизу идёт по legacy-
        # модели и может не совпасть с «Итого» (= якорь-сверка) — это ожидаемо, данные целы.

        # Прямая хронология (старые сверху, свежие снизу); «перенос» открывает блок.
        rows.sort(key=lambda x: x[0])

        # Рендер + строка running-баланса после каждого движения (со словом «баланс»).
        # Якорь-сверка (credit_wallet_anchors, durable 19.06): на его дату running
        # ПЕРЕУСТАНАВЛИВАЕТСЯ на сверенный остаток отдельной строкой «⚖️ Сверка»
        # (user 26.06). История ДО сверки остаётся реальной, после неё running идёт
        # от сверенного остатка → итоговый «баланс» сходится с «Итого» (= якорь +
        # движения после, get_credit_balance_summary). Без якоря — чистый legacy-run.
        _anchor_amt = float(_wallet_anchor["amount"]) if _wallet_anchor else None
        _anchor_ts = str(_wallet_anchor["created_at"]) if _wallet_anchor else ""
        _sverka_done = _anchor_amt is None

        _lines: list[str] = []
        _run = 0.0

        def _bal_line() -> None:
            _lines.append("   " + _pad("баланс", _PREFIX_W) + _pad(_k(_run), _SUM_W, True))

        def _sverka_line() -> None:
            _lines.append(
                "   " + _pad(f"{_d(_anchor_ts)} ⚖️", _PREFIX_W) + _pad(_k(_anchor_amt), _SUM_W, True)
                + " " + _pad(label, _ABBR_W) + " " + "Сверка"
            )

        for (ts, kind, icon, amt, signed, ab, street) in rows:
            if not _sverka_done and str(ts) > _anchor_ts:
                _run = _anchor_amt  # type: ignore[assignment]
                _sverka_line()
                _bal_line()
                _sverka_done = True
            _run += signed
            arrow = "" if kind == "bal" else ("⬆️" if kind == "in" else "⬇️")
            prefix = f"{_d(ts)} {arrow}{icon}".rstrip()
            _lines.append(
                "   " + _pad(prefix, _PREFIX_W) + _pad(_k(amt), _SUM_W, True)
                + " " + _pad(ab, _ABBR_W) + " " + street
            )
            _bal_line()
        if not _sverka_done:
            # Все движения раньше якоря (или их нет) — сверка закрывает блок.
            _run = _anchor_amt  # type: ignore[assignment]
            _sverka_line()
            _bal_line()
        moves_body = "\n".join(_lines)
    except Exception:
        moves_body = ""

    if moves_body:
        # Блоки местами (user 2026-06-10): «Последние движения» сверху, баланс снизу.
        moves = f"<b>🧾  Последние движения</b>\n<pre>{moves_body}</pre>"
        return f"{moves}\n\n{main}"
    return main


async def apply_credit_wallet_spend(
    db: "Database",
    integrations: Any,
    *,
    wallet_role: str,
    amount: float,
    mode: str,
    purpose: str,
    entered_by: int,
    invoice_id: int | None = None,
    cost_type: str | None = None,
    invoice_number: str = "",
    existing_supplier_payment_id: int | None = None,
) -> dict[str, Any]:
    """Записать трату кредит-кошелька: вся БД-запись + синки (TZ 04.06 §C).

    Вынесено из manager_new.cw_confirm, чтобы запись можно было ОТЛОЖИТЬ до
    подтверждения «исполнения» менеджером-владельцем (чужой кошелёк). При своей
    трате вызывается сразу; при чужой — из обработчика исполнения по payload задачи.
    entered_by = инициатор траты (РП/ГД/менеджер) — атрибуция сумм не меняется
    относительно прежней синхронной записи; факт исполнения менеджером логируется
    отдельным audit в обработчике исполнения.

    Эффекты:
      mode=='bound'    → create_supplier_payment (DP–DV) + add_credit_spend +
                         sync_invoice_row; если existing_supplier_payment_id задан —
                         create_supplier_payment ПРОПУСКАЕТСЯ, переиспользуется
                         готовый sp_id (invoice_pp_finalize уже создал оплату; п.2 10.06);
      mode=='withdraw' → ТОЛЬКО add_credit_spend (вывод ДС, TZ 09.06): ни «Баланс
                         компании», ни привязки к счёту — фиксация лишь в кредит-
                         балансе (общий блок carry-DA ниже);
      иначе (free)     → add_op_company_entry («Баланс компании» I/J) +
                         add_credit_spend + sync_balance_company_sheet;
      + add_credit_expense на активный открытый кредит-счёт роли (carry-DA↓) +
        sync_invoice_row(active) + sync_advances_journal_sheet + audit.
    Возвращает {spend_id, supplier_payment_id, op_entry_id, credit_expense_id,
                active_credit_invoice_id}.
    """
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    sp_id: int | None = None
    op_id: int | None = None

    if mode == "bound" and invoice_id:
        if existing_supplier_payment_id is not None:
            # Оплата поставщику уже создана вызывающим (напр. invoice_pp_finalize) —
            # НЕ создаём дубль supplier_payment, только списываем кошелёк (п.2 10.06).
            sp_id = int(existing_supplier_payment_id)
        else:
            sp_id = await db.create_supplier_payment(
                parent_invoice_id=int(invoice_id), amount=amount,
                material_type=cost_type or "extra_mat",
                invoice_number=invoice_number, created_by=entered_by,
            )
        spend_id = await db.add_credit_spend(
            wallet_role, amount, entered_by,
            cost_type=cost_type, description=purpose,
            bound_invoice_id=int(invoice_id), supplier_payment_id=sp_id,
        )
        try:
            await integrations.sync_invoice_row(int(invoice_id))
        except Exception:
            log.warning(
                "apply_credit_wallet_spend: sync_invoice_row failed inv=%s",
                invoice_id, exc_info=True,
            )
    elif mode == "withdraw":
        # «Вывод ДС» (TZ 09.06): фиксируется ТОЛЬКО в кредитном балансе кошелька —
        # без записи на «Баланс компании» (add_op_company_entry) и без привязки к
        # материнскому счёту (create_supplier_payment). Реестр трат (add_credit_spend)
        # + общий блок ниже (add_credit_expense на активный кредит-счёт → carry-DA↓)
        # дают всю фиксацию; листы «Баланс компании» I/J и DP–DV не трогаем.
        spend_id = await db.add_credit_spend(
            wallet_role, amount, entered_by,
            description=purpose,
        )
    else:
        op_id = await db.add_op_company_entry(
            year=now.year, month=now.month,
            date_iso=now.strftime("%Y-%m-%d"),
            date_other_display=now.strftime("%d.%m.%Y"),
            other_amount=amount, description_credit=purpose,
            source="credit_wallet_spend",
        )
        spend_id = await db.add_credit_spend(
            wallet_role, amount, entered_by,
            description=purpose, op_entry_id=op_id,
        )
        if getattr(integrations, "sheets", None):
            try:
                await integrations.sheets.sync_balance_company_sheet(db)
            except Exception:
                log.warning(
                    "apply_credit_wallet_spend: sync_balance_company_sheet failed",
                    exc_info=True,
                )

    # (1) Уменьшить кредит-остаток: расход на активный открытый счёт роли.
    # Двигает carry-DA (get_credit_balance_summary) — это и есть баланс кошелька.
    ce_id: int | None = None
    active_inv: dict[str, Any] | None = None
    try:
        active_inv = await db.get_active_credit_invoice_for_channel(wallet_role)
    except Exception:
        log.warning(
            "apply_credit_wallet_spend: get_active_credit_invoice_for_channel failed",
            exc_info=True,
        )
    if active_inv:
        # description НЕ должен начинаться с «остаток» — иначе трактуется как
        # маркер абсолютного остатка в get_credit_balance_summary/carry_in.
        ce_desc = purpose if not purpose.lower().startswith("остаток") else f"Расход: {purpose}"
        try:
            ce_id = await db.add_credit_expense(
                int(active_inv["id"]), amount, ce_desc, entered_by, cost_type=cost_type,
            )
            await integrations.sync_invoice_row(int(active_inv["id"]))
        except Exception:
            log.warning(
                "apply_credit_wallet_spend: add_credit_expense failed inv=%s",
                active_inv.get("id"), exc_info=True,
            )
    else:
        log.warning(
            "apply_credit_wallet_spend: нет активного открытого кредит-счёта %s — остаток не уменьшен",
            wallet_role,
        )

    # Журнал «Авансирование сотрудников» (кредит-блок).
    if getattr(integrations, "sheets", None):
        try:
            await integrations.sheets.sync_advances_journal_sheet(db)
        except Exception:
            log.warning(
                "apply_credit_wallet_spend: sync_advances_journal_sheet failed",
                exc_info=True,
            )

    try:
        await db.audit(
            actor_id=entered_by, action="credit_wallet_spend",
            entity="credit_spends", entity_id=str(spend_id),
            payload={
                "wallet_role": wallet_role, "amount": amount, "mode": mode,
                "invoice_id": invoice_id, "cost_type": cost_type,
                "op_entry_id": op_id, "supplier_payment_id": sp_id, "purpose": purpose,
                "credit_expense_id": ce_id,
                "active_credit_invoice_id": (active_inv or {}).get("id"),
            },
        )
    except Exception:
        log.debug("apply_credit_wallet_spend: audit failed", exc_info=True)

    return {
        "spend_id": spend_id,
        "supplier_payment_id": sp_id,
        "op_entry_id": op_id,
        "credit_expense_id": ce_id,
        "active_credit_invoice_id": (active_inv or {}).get("id"),
    }


async def resolve_installer_zp_by_wallet_payment(
    db: "Database", invoice_id: int, *,
    spend_amount: float | None = None, actor_id: int | None = None,
    spend_note: str = "",
) -> dict[str, Any]:
    """Кредит-кошелёк заплатил ЗП монтажа по счёту → ПОЛНАЯ выплата закрывает ЗП,
    ЧАСТИЧНАЯ засчитывается авансом внутрь согласованной суммы.

    Анти-задвоение (исходная задача): выплата всей ЗП из кошелька закрывает парную
    ОТКРЫТУЮ zp_installer-задачу + метит ЗП payment_sent, чтобы ГД не провёл вторую
    платёжку по той же ЗП.

    ⛔ Фикс owner 25.07 (правило «Выплачено по ЗП монтаж ВСЕГДА ≥ Согласовано; аванс/
    частичная выплата — ВНУТРЬ согласованной»): раньше ЛЮБАЯ трата кошелька с
    cost_type='montazh' закрывала ЗП ЦЕЛИКОМ, сколько бы ни была её сумма — трата
    50 000 помечала выплаченной ЗП 120 465 (инцидент 23.07, сч. КВ 9 → ложная сумма
    ушла в BS и вернулась петлёй через «Импорт ОП»). Теперь сумма траты сравнивается
    с ПРИЧИТАЮЩИМСЯ текущей монтажной группе:
      причитается = Согласовано − выплаченное прошлым группам (montazh_paid_prev)
                    − уже зачтённый аванс текущей группы (CG);
      трата ≥ причитающегося → как раньше (payment_sent + закрыть задачу);
      трата < причитающегося → ЗАЧЁТ: аванс монтажника на сумму траты
        (db.record_installer_advance_offset_from_wallet — та же пара записей, что
        делалась руками), открытая заявка ЗП пересчитывается в ОСТАТОК
        (zp_installer_amount = остаток, zp_installer_remainder=1 → лист считает
        «Выплачено» ADDITIVE: аванс + выплата бота = ровно Согласовано), статус и
        задача НЕ трогаются — ГД доплачивает остаток штатно.
    Сумма зачёта сравнивается в масштабе листа (CG): для б/н трата грос-апится ×1.10
    (ЗП б/н = база+10%, [[feedback_installer_advance_spend_scope]]), для кредита 1:1.

    Наёмная группа (assigned_to=NULL / владелец не монтажник) — зачесть аванс НЕКОМУ
    (гард get_installer_advance_for_invoice отбросил бы запись): ЗП НЕ закрываем и НЕ
    метим, возвращаем reason='no_installer' — вызывающий предупреждает ГД, что остаток
    нужно провести руками. Молчаливое закрытие всей ЗП здесь и было баг-поведением.

    Идемпотентно: нет открытой zp_installer по счёту → no-op. В полной ветке сумму/флаг
    остатка НЕ меняем (set_invoice_zp_installer_status без amount/is_remainder сохраняет
    их); статус трогаем только из «в процессе» (requested/approved), чтобы не перетереть
    уже выплаченные/закрытые.

    Возвращает {"closed": int, "marked_paid": bool, "task_ids": [...], "partial": bool,
                "offset_applied": float, "remainder": float, "due": float,
                "reason": str | None}.
    """
    open_zp = await db.list_open_tasks_by_invoice(invoice_id, "zp_installer")
    if not open_zp:
        return {
            "closed": 0, "marked_paid": False, "task_ids": [], "partial": False,
            "offset_applied": 0.0, "remainder": 0.0, "due": 0.0, "reason": "no_open_task",
        }
    inv = await db.get_invoice(invoice_id) or {}
    task_ids = [int(t["id"]) for t in open_zp]

    # Причитается ТЕКУЩЕЙ монтажной группе — 1-в-1 с листом (BJ, sheets.py::_invoice_cells):
    # Согласовано включает ногу прошлых групп (montazh_paid_prev) и уже зачтённый аванс.
    agreed = float(inv.get("montazh_agreed_amount") or 0)
    paid_prev = float(inv.get("montazh_paid_prev") or 0)
    try:
        adv_raw = max(
            0.0,
            await db.get_installer_advance_for_invoice(invoice_id)
            - float(inv.get("montazh_adv_prev") or 0),
        )
    except Exception:
        log.warning(
            "resolve_installer_zp: advance lookup failed inv=%s", invoice_id, exc_info=True
        )
        adv_raw = 0.0
    _is_credit_inv = bool(inv.get("is_credit")) or str(
        inv.get("invoice_number") or ""
    ).upper().startswith("ЗМ")
    adv_cg = adv_raw if (_is_credit_inv or adv_raw <= 0) else adv_raw * 1.10
    due = agreed - paid_prev - adv_cg
    # Масштаб листа: наличная/кредитная трата по б/н счёту закрывает ЗП с надбавкой +10%.
    spend_cg = (
        None if spend_amount is None
        else (float(spend_amount) if _is_credit_inv else float(spend_amount) * 1.10)
    )

    # spend_amount не передан (старые вызовы) или Согласовано не выставлено — судить
    # о частичности нечем, поведение ровно прежнее (полное закрытие).
    if spend_cg is not None and agreed > 0 and spend_cg < due - 0.001:
        # round(2): грос-ап ×1.10 даёт float-хвост (65464.99999999999) — он ушёл бы в
        # zp_installer_amount и в карточку ГД.
        remainder = round(max(0.0, due - spend_cg), 2)
        installer_id = inv.get("assigned_to")
        role_ok = False
        if installer_id:
            try:
                _u = await db.get_user_optional(int(installer_id))
                role_ok = bool(_u) and "installer" in [
                    r.strip() for r in str(getattr(_u, "role", "") or "").split(",")
                ]
            except Exception:
                log.warning(
                    "resolve_installer_zp: user lookup failed inv=%s", invoice_id, exc_info=True
                )
        base = {
            "closed": 0, "marked_paid": False, "task_ids": task_ids, "partial": True,
            "due": due, "remainder": remainder,
        }
        if not role_ok:
            log.warning(
                "resolve_installer_zp: частичная выплата %s по счёту %s без монтажника "
                "(assigned_to=%s) — зачёт не создан, ЗП оставлена открытой",
                spend_amount, invoice_id, installer_id,
            )
            return {**base, "offset_applied": 0.0, "reason": "no_installer"}
        try:
            await db.record_installer_advance_offset_from_wallet(
                int(installer_id), invoice_id, float(spend_amount), actor_id,
                comment=(
                    spend_note
                    or f"Частичная выплата ЗП монтаж из кредит-кошелька (счёт id={invoice_id})"
                ),
                plan_zp_snapshot=agreed,
            )
        except Exception:
            log.warning(
                "resolve_installer_zp: зачёт аванса не создан inv=%s amount=%s",
                invoice_id, spend_amount, exc_info=True,
            )
            return {**base, "offset_applied": 0.0, "reason": "offset_failed"}
        # Открытая заявка ЗП → ОСТАТОК. Прямой update_invoice, а НЕ
        # set_invoice_zp_installer_status: статус остаётся прежним (requested/approved),
        # а повторная его установка перезаписала бы zp_installer_requested_at/_approved_at.
        try:
            await db.update_invoice(
                invoice_id, zp_installer_amount=remainder, zp_installer_remainder=1,
            )
        except Exception:
            log.warning(
                "resolve_installer_zp: пересчёт заявки в остаток не удался inv=%s",
                invoice_id, exc_info=True,
            )
            return {**base, "offset_applied": float(spend_amount), "reason": "remainder_failed"}
        return {**base, "offset_applied": float(spend_amount), "reason": None}

    marked = False
    st = inv.get("zp_installer_status")
    if st in ("requested", "approved"):
        try:
            await db.set_invoice_zp_installer_status(invoice_id, "payment_sent")
            marked = True
        except Exception:
            log.warning(
                "resolve_installer_zp: set zp status failed inv=%s", invoice_id, exc_info=True
            )
    closed = await db.close_tasks_by_invoice(invoice_id, "zp_installer")
    return {
        "closed": int(closed or 0),
        "marked_paid": marked,
        "task_ids": task_ids,
        "partial": False,
        "offset_applied": 0.0,
        "remainder": 0.0,
        "due": due,
        "reason": None,
    }


async def build_funds_card(
    db: "Database", user_id: int, wallet_role: str | None = None, *, recent: int = 6,
) -> str:
    """Сводная карточка «💰 Финансы» менеджера (эталон, Финансы-рефактор 02.06).

    Один блок: Аванс + Депозит + Итого; опц. блок последних движений.
    Источники: db.get_advance_balance / db.get_deposit_balance (per-user,
    wallet_role-фильтр как в балансах); движения — db.list_all_advance_events
    (paid-строки installer_advance_requests: request/deposit/withdraw/transfer).
    """

    def _rub(n: Any) -> str:
        return f"{float(n or 0):,.0f}".replace(",", " ").replace("-", "−") + "₽"

    adv = await db.get_advance_balance(user_id, wallet_role)
    depo = await db.get_deposit_balance(user_id, wallet_role)
    total = float(adv) + float(depo)
    total_s = _rub(total)
    main = format_card_section(
        emoji="💰",
        title="Финансы",
        items=[("Аванс", _rub(adv)), ("Депозит", _rub(depo))],
        total=total_s,
        footer=("Итого", total_s),
        width=36,
    )

    # wallet-фильтр строк (зеркало db._wallet_clause): None→все; 'rp'→только rp;
    # иначе (manager_npn/primary)→NULL или !='rp'.
    def _wallet_ok(wr: Any) -> bool:
        if wallet_role is None:
            return True
        if wallet_role == "rp":
            return wr == "rp"
        return wr is None or wr != "rp"

    _type_label = {
        "request": ("⬆️", "аванс"),
        "transfer_depo_to_adv": ("↔️", "депо→аванс"),
        "deposit": ("⬆️", "депозит"),
        "withdraw": ("⬇️", "расход депо"),
    }
    move_items: list[tuple[str, str]] = []
    try:
        bundle = await db.list_all_advance_events(limit=500)
        reqs = [
            r for r in (bundle.get("requests") or [])
            if r.get("installer_id") == user_id
            and r.get("status") == "paid"
            and _wallet_ok(r.get("wallet_role"))
        ]
        for r in reqs[:recent]:
            rt = r.get("request_type") or "request"
            arrow, tag = _type_label.get(rt, ("•", str(rt)))
            ts = str(r.get("paid_at") or r.get("requested_at") or "")
            d = f"{ts[8:10]}.{ts[5:7]}" if len(ts) >= 10 and ts[4:5] == "-" else ts[:5]
            cmt = (r.get("comment") or "").strip()
            val = f"{_rub(r.get('total_amount'))} {tag}"
            if cmt:
                val += f" · {cmt[:16]}"
            move_items.append((f"{d} {arrow}", val))
    except Exception:
        move_items = []

    if move_items:
        moves = format_card_section(
            emoji="🧾",
            title="Последние движения",
            items=move_items,
            width=36,
            compact=True,
        )
        return f"{main}\n\n{moves}"
    return main


async def build_advance_history_card(
    db: "Database", installer_id: int, wallet_role: str | None = None,
) -> str:
    """Карточка «Аванс — история» кошелька монтажника (эталон, ТЗ аванс 03.06, этап-2).

    ПОЛНАЯ история движений аванса роли + «Баланс Итого» внизу:
      • приход  — пополнение от ГД (paid request) / перевод депо→аванс;
      • расход  — зачёт применённого аванса в счёт ЗП-монтаж по счёту
                  (closed offset-item, db.apply_advance_to_invoice_now).
    Footer-остаток = авторитетный db.get_advance_balance (Σ прихода − Σ зачётов).
    Доп. блок «Ожидаемая ЗП» — остаток ЗП-монтаж по счетам (agreed − применённое).

    Read-only витрина: в Invoices/лист ничего не пишет; кредит-признак монтажнику
    НЕ показывается (для монтажника кредит == безнал). Источники: db.list_all_advance_events
    / db.get_advance_balance / db.get_installer_pending_zp / db.get_installer_advance_for_invoice.
    """

    def _rub(n: Any) -> str:
        return f"{float(n or 0):,.0f}".replace(",", " ").replace("-", "−") + "₽"

    def _dd(ts: Any) -> str:
        s = str(ts or "")
        return f"{s[8:10]}.{s[5:7]}" if len(s) >= 10 and s[4:5] == "-" else (s[:5] or "—")

    # wallet-фильтр строк (зеркало db._wallet_clause / build_funds_card).
    def _wallet_ok(wr: Any) -> bool:
        if wallet_role is None:
            return True
        if wallet_role == "rp":
            return wr == "rp"
        return wr is None or wr != "rp"

    bal = await db.get_advance_balance(installer_id, wallet_role)
    bal_s = _rub(bal)
    bundle = await db.list_all_advance_events(limit=500)

    # Приходы аванса: оплаченные пополнения (request) + перевод депо→аванс.
    reqs = [
        r for r in (bundle.get("requests") or [])
        if r.get("installer_id") == installer_id
        and r.get("status") == "paid"
        and (r.get("request_type") or "request") in ("request", "transfer_depo_to_adv")
        and _wallet_ok(r.get("wallet_role"))
    ]
    # Зачёты, реально уменьшившие баланс аванса (offset_zp_id положительный,
    # родитель — request; так же считает get_advance_balance → Σ совпадёт с footer).
    req_parent_ids = {
        r.get("req_id") for r in reqs
        if (r.get("request_type") or "request") == "request"
    }
    offs = [
        it for it in (bundle.get("items") or [])
        if it.get("installer_id") == installer_id
        and it.get("request_id") in req_parent_ids
        and (it.get("offset_zp_id") or 0) and int(it["offset_zp_id"]) > 0
    ]

    events: list[tuple[str, str, float, str]] = []  # (ts, sign, amount, label)
    for r in reqs:
        ts = str(r.get("paid_at") or r.get("requested_at") or "")
        if (r.get("request_type") or "request") == "transfer_depo_to_adv":
            label = "Перевод депо→аванс"
        else:
            label = "Пополнение" + (" (ГД)" if r.get("initiator") == "gd" else "")
        cmt = (r.get("comment") or "").strip()
        if cmt:
            label += f" · {cmt[:18]}"
        events.append((ts, "+", float(r.get("total_amount") or 0), label))
    for it in offs:
        ts = str(it.get("offset_at") or "")
        num = it.get("invoice_number") or f"#{it.get('invoice_id')}"
        events.append((ts, "−", float(it.get("offset_amount") or 0), f"Зачёт {num}"))

    events.sort(key=lambda e: (e[0] or ""), reverse=True)  # новые сверху

    move_items: list[tuple[str, str]] = []
    for ts, sign, amt, label in events:
        arrow = "⬆️" if sign == "+" else "⬇️"
        move_items.append((f"{_dd(ts)} {arrow}", f"{sign}{_rub(amt)} {label}".strip()))
    if not move_items:
        move_items = [("—", "пока нет операций")]

    card = format_card_section(
        emoji="💼",
        title="Аванс — история",
        items=move_items,
        total=bal_s,
        footer=("Баланс Итого", bal_s),
        width=36,
        compact=True,
    )

    # Блок «Ожидаемая ЗП» — остаток ЗП-монтаж по счетам (agreed − применённый аванс).
    pending = await db.get_installer_pending_zp(installer_id)
    prows: list[tuple[str, str]] = []
    ptotal = 0.0
    for p in pending:
        agreed = float(p.get("agreed") or 0)
        applied = await db.get_installer_advance_for_invoice(int(p["id"]))
        rem = max(0.0, agreed - applied)
        if rem <= 0:
            continue
        ptotal += rem
        num = p.get("invoice_number") or f"#{p['id']}"
        prows.append((str(num), _rub(rem)))
    if prows:
        zp = format_card_section(
            emoji="🔨",
            title="Ожидаемая ЗП",
            items=prows,
            total=_rub(ptotal),
            footer=("Итого к выплате", _rub(ptotal)),
            width=36,
        )
        return f"{card}\n\n{zp}"
    return card


async def build_installer_advance_card(
    db: "Database", installer_id: int, wallet_role: str | None = None,
) -> str:
    """Карточка-витрина «Аванс — история» монтажника (ТЗ user 2026-06-09).

    Read-only: НИЧЕГО не пишет в БД / Google Sheets / задачи. Показывается сразу
    при входе монтажника в «Аванс» (handlers/installer_new._send_zp_submenu).

    Макет (user 2026-06-09, перекомпонован вечером):
      • строка «Аванс Вход» = ПОСЛЕДНЯЯ заявка-приход аванса (total_amount + дата);
      • верхний разделитель — полная ширина;
      • таблица ТОЛЬКО по объектам с авансом (CG > 0). Аванс по счёту = ровно как лист
        Invoices (CG, sheets.py cells[84]) — полный зачёт по счёту БЕЗ фильтров. Числа «к».
        Улица с начала строки (6-7 букв) + метка «б/н» (безнал) / «кр» (кредит) через 1
        пробел — кредит-признак счёта (user 09.06 веч.). Ширина 39 (шире 34, метка норм. 3;
        столбцы равной ширины 7; без переноса на узком экране). Колонки ЗП/Аванс/Остаток
        равномерно, Дата — у правого края:
          Объект  = _street(object_address)[:7] + б/н|кр
          ЗП      = BJ + CG   (Остаток + Аванс)
          Аванс   = CG  «Аванс монтажника» (зачтённый аванс по счёту; б/н ×1.10)
          Остаток = BJ  «ЗП Монтажник» (Согласовано − Выплачено)
          Дата    = CH  «Дата аванса» (дата зачёта; прижата к правому краю)
      • разделитель ПЕРЕД итогами — 50% длины (user 10.06);
      • «Авансовый баланс» = Аванс Вход − Σ базы аванса (без +10%) → 0;
      • «Остаток в ЗП» = Σ BJ.

    Формулы CG/BJ зеркалят integrations/sheets.py `_invoice_cells` (ячейки 84/85/61)
    и карточку ГД «Монтаж ЗП» (handlers/td._montazh_zp_list_card) → цифры совпадают
    с листом Invoices. Кредит-признак счёта монтажнику ПОКАЗЫВАЕТСЯ меткой «б/н»/«кр»
    у строки (user 09.06 веч.; ранее скрывался). Источники (только read-only методы db): list_all_advance_events
    / get_installer_pending_zp / get_invoice / get_installer_advance_date_for_invoice
    (аванс по счёту — из items бандла list_all_advance_events, только оплаченные заявки).
    """
    from .rp_start_card import _street

    def _rub(n: Any) -> str:
        return f"{float(n or 0):,.0f}".replace(",", " ").replace("-", "−") + "₽"

    def _k(n: Any) -> str:
        # Компактный формат тысяч: 55000→«55к», 5000→«5к» (user 09.06, формат «к»).
        v = float(n or 0)
        if not v:
            return "—"
        t = v / 1000.0
        return f"{t:.0f}к" if abs(t - round(t)) < 0.05 else f"{t:.1f}к"

    def _dd(ts: Any) -> str:
        s = str(ts or "")
        return f"{s[8:10]}.{s[5:7]}" if len(s) >= 10 and s[4:5] == "-" else "—"

    # «Аванс Вход» — ПОСЛЕДНЯЯ оплаченная заявка-приход аванса (user 09.06: не сумма,
    # а последняя). request_type='request' (не transfer депо→аванс), status='paid'.
    bundle = await db.list_all_advance_events(limit=500)
    topups = [
        r for r in (bundle.get("requests") or [])
        if r.get("installer_id") == installer_id
        and r.get("status") == "paid"
        and (r.get("request_type") or "request") == "request"
    ]
    topups.sort(
        key=lambda r: str(r.get("paid_at") or r.get("requested_at") or ""), reverse=True
    )
    if topups:
        vhod_amount = float(topups[0].get("total_amount") or 0)
        vhod_date = _dd(topups[0].get("paid_at") or topups[0].get("requested_at"))
    else:
        vhod_amount, vhod_date = 0.0, "—"

    # Таблица по объектам монтажника (montazh_agreed>0, ещё не confirmed).
    rows = await db.get_installer_pending_zp(installer_id)
    table: list[tuple[str, str, float, float, str, float]] = []  # street, marker, zp, adv_cg, date, bj
    sum_adv = 0.0
    sum_base = 0.0   # Σ базы аванса (без +10%) — для «Авансовый баланс итого» (user 09.06)
    sum_bj = 0.0
    for r in rows:
        inv_id = int(r["id"])
        inv = await db.get_invoice(inv_id) or {}
        num = str(r.get("invoice_number") or inv.get("invoice_number") or f"#{inv_id}")
        is_credit = bool(r.get("is_credit")) or num.upper().startswith("ЗМ")
        # Аванс по счёту = ровно как лист Invoices (CG, sheets.py cells[84]):
        # полный зачёт аванса по счёту, БЕЗ фильтров (×1.10 для б/н — ниже).
        # Аванс ТЕКУЩЕЙ монтажной группы: аванс прошлой уже внутри montazh_paid_prev
        # (объединение платежей, owner 15.07) — иначе вычелся бы дважды.
        adv_offset = max(
            0.0,
            await db.get_installer_advance_for_invoice(inv_id)
            - float(inv.get("montazh_adv_prev") or 0),
        )
        # CG «Аванс монтажника»: б/н ×1.10, кредит как есть (sheets.py cells[84]).
        adv_cg = adv_offset * 1.10 if (adv_offset > 0 and not is_credit) else adv_offset
        agreed = float(r.get("agreed") or inv.get("montazh_agreed_amount") or 0)
        # BJ «ЗП Монтажник» = Согласовано − Выплачено (sheets.py cells[61]).
        # Выплачено = max(AN, аванс, бот); ADDITIVE (аванс+бот) если заявка = ОСТАТОК
        # (zp_installer_remainder, Часть 2 от 08.06). Совпадает с листом для любого статуса.
        # Объединение платежей: + нога прошлых групп (montazh_paid_prev). AN — накопитель
        # ВСЕХ ног, поэтому конкурирует по максимуму, а не суммируется. paid_prev=0 →
        # формула ровно прежняя (1-в-1 с листом), ср. _montazh_money_state (rp_new.py).
        an = float(inv.get("montazh_fact_op") or 0)
        paid_prev = float(inv.get("montazh_paid_prev") or 0)
        bot_paid = (
            float(inv.get("zp_installer_amount") or 0)
            if inv.get("zp_installer_status") in ("payment_sent", "confirmed")
            else 0.0
        )
        # Канал DR «Затр. Монтаж» — канон sheets.py:1274 (owner 01.08). Транши ОДНОЙ
        # группе копятся в cost_montazh; zp_installer_amount — только последний платёж.
        # Через max, НЕ сумму [[feedback_montazh_zp_multi_payment_sum]].
        dr = float(inv.get("cost_montazh") or 0)
        if inv.get("zp_installer_remainder") and bot_paid > 0:
            paid = max(an, paid_prev + adv_cg + bot_paid, dr)
        else:
            paid = max(an, paid_prev + max(adv_cg, bot_paid), dr)
        if adv_offset <= 0:
            continue  # только счета С авансом (user 09.06: «Только с авансом — 3»)
        bj = max(0.0, agreed - paid) if agreed > 0 else 0.0
        zp = bj + adv_cg
        adv_date = _dd(await db.get_installer_advance_date_for_invoice(inv_id))
        street = _street(r.get("object_address") or inv.get("object_address"), 14) or num
        marker = "кр." if is_credit else "б/н"  # кредит-признак счёта (user 09.06 веч.)
        table.append((street, marker, zp, adv_cg, adv_date, bj))
        sum_adv += adv_cg
        sum_base += adv_offset
        sum_bj += bj

    bal_itogo = vhod_amount - sum_base     # user 09.06: Вход − Σ базы аванса (без +10%) → 0
    ost_zp_itogo = sum_bj

    # --- Рендер (<pre>; ширина 39 — шире прежних 34, user 10.06) ---
    # Выравнивание устойчивое к кириллице Telegram (user 09.06): объект + метка —
    # единым блоком СЛЕВА; числовые колонки (ЗП/Аванс/Остаток/Дата) — СПРАВА, только
    # цифры (моноширинные). БЕЗ точек-разделителей; ₽ только в итоговых строках
    # (не колонки), в таблице — «к» без ₽. Метка нормализована до 3 симв. (`mk:<3s`),
    # столбцы — равной ширины 7 с увеличенным зазором → визуально ровнее (user 10.06).
    FULL = "━" * 20          # верхний разделитель ≈ полная ширина 39 (━ ≈ 2 симв.)
    HALF = "━" * 10          # разделитель ПЕРЕД итогами — 50% длины (user 10.06)

    def _row(left: str, zp: str, adv: str, ost: str, dt: str) -> str:
        return f"{left[:11]:<11s}{zp:>7s}{adv:>7s}{ost:>7s}{dt:>7s}"

    def _label(street: str, mk: str) -> str:
        return f"{street[:7]:<7s} {mk:<3s}"   # ровно 11: улица 7 + пробел + метка 3

    lines: list[str] = []
    lines.append(f"Аванс Вход: {_rub(vhod_amount)} ({vhod_date})")
    lines.append(FULL)
    lines.append(_row("Объект", "ЗП", "Аванс", "Остат", "Дата"))
    if table:
        for street, mk, zp, adv_cg, adv_date, bj in table:
            lines.append(_row(_label(street, mk), _k(zp), _k(adv_cg), _k(bj), adv_date))
    else:
        lines.append("нет счетов с авансом")
    lines.append(HALF)
    lines.append(f"Авансовый баланс: {_rub(bal_itogo)}")
    lines.append(f"Остаток в ЗП: {_rub(ost_zp_itogo)}")

    body = "\n".join(lines)
    return f"<b>💰  Аванс — история</b>\n<pre>{body}</pre>"


async def build_installer_zp_invoiceok_card(
    db: "Database", installer_id: int,
) -> str:
    """Карточка-витрина «ЗП к запросу — Счёт ОК» монтажника (ТЗ user 2026-06-09).

    Read-only: НИЧЕГО не пишет в БД / Google Sheets / задачи. Показывается СРАЗУ
    при входе монтажника в «💰 Запрос ЗП» (handlers/installer_new._send_zp_submenu).

    Содержимое (решения user 2026-06-09):
      • только счета монтажника на этапе «Счёт ОК» (montazh_stage='invoice_ok' —
        значение колонки AZ листа Invoices); БЕЗ «и далее» (invoice_end/закрытые
        НЕ входят — буквально «статус счёт ОК»);
      • только счета с остатком BJ>0 (есть что запросить);
      • строка: Объект (улица 7, как «Этапы работы») + метка «б/н»(безнал)/«кр»(кредит),
        числа в формате «к» (22000→«22к»): Аванс = CG, Остаток = BJ;
      • «Итого к запросу» = Σ BJ.

    Сумма = BJ «ЗП Монтажник» (Согласовано − Выплачено) — ровно как лист Invoices
    (sheets.py cells[61]) и build_installer_advance_card: Выплачено = max(AN,
    аванс-зачёт CG [б/н ×1.10], выплата ботом), ADDITIVE при флаге
    zp_installer_remainder (Часть 2 от 08.06) → цифры совпадают с листом.
    Источники (read-only db): raw SQL по invoices + get_installer_advance_for_invoice.
    """
    from .rp_start_card import _street

    def _k(n: Any) -> str:
        # Компактный формат тысяч: 22000→«22к», 42000→«42к», 5000→«5к» (user 10.06,
        # формат «к» — как build_installer_advance_card; короче → строка не переносится).
        # 0 → «—». Без ₽ в ячейках (₽ не всегда моноширинный в Telegram <pre>).
        v = float(n or 0)
        if not v:
            return "—"
        t = v / 1000.0
        return f"{t:.0f}к" if abs(t - round(t)) < 0.05 else f"{t:.1f}к"

    cur = await db.conn.execute(
        "SELECT id, invoice_number, object_address, COALESCE(is_credit, 0) AS is_credit, "
        "       COALESCE(montazh_agreed_amount, 0) AS agreed, "
        "       COALESCE(montazh_fact_op, 0) AS an, "
        "       COALESCE(zp_installer_amount, 0) AS zia, "
        "       COALESCE(zp_installer_status, '') AS zis, "
        "       COALESCE(zp_installer_remainder, 0) AS rem, "
        # DR «Затр. Монтаж» — накопитель траншей ЗП монтажа (owner 01.08). Без него
        # правка ниже стала бы молчаливым no-op: этот SELECT собирает строки САМ,
        # а не через db.get_invoice(), и лишних полей в нём не было.
        # pprev/aprev — ноги объединения платежей (owner 01.08, выравнивание с каноном).
        "       COALESCE(cost_montazh, 0) AS dr, "
        "       COALESCE(montazh_paid_prev, 0) AS pprev, "
        "       COALESCE(montazh_adv_prev, 0) AS aprev "
        "FROM invoices "
        "WHERE assigned_to = ? AND montazh_stage = 'invoice_ok' "
        "  AND parent_invoice_id IS NULL "
        "ORDER BY id DESC",
        (installer_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]

    table: list[tuple[str, str, float, float]] = []  # street, marker, adv_cg, bj
    sum_bj = 0.0
    for r in rows:
        inv_id = int(r["id"])
        is_credit = bool(r["is_credit"])
        # Аванс-зачёт = CG «Аванс монтажника» (б/н ×1.10, кредит как есть), но только за
        # ТЕКУЩУЮ монтажную группу: аванс прошлой уже сидит внутри montazh_paid_prev
        # (нога добавлена 01.08 ниже), и без вычета montazh_adv_prev он посчитался бы
        # ДВАЖДЫ — ровно тот дефект, что закрыли 31.07 в sheets.py/db.py. Канон —
        # installer_new.py::_advance_raw_cur. adv_prev = 0 → величина ровно прежняя.
        adv_offset = max(0.0, await db.get_installer_advance_for_invoice(inv_id)
                         - float(r["aprev"] or 0))
        adv_cg = adv_offset * 1.10 if (adv_offset > 0 and not is_credit) else adv_offset
        agreed = float(r["agreed"] or 0)
        an = float(r["an"] or 0)
        bot_paid = (
            float(r["zia"] or 0)
            if r["zis"] in ("payment_sent", "confirmed") else 0.0
        )
        # Выплачено: ADDITIVE (аванс+бот) если заявка = ОСТАТОК (zp_installer_remainder),
        # иначе max — ровно как sheets.py cells[61] / build_installer_advance_card.
        # Нога прошлых групп (montazh_paid_prev) добавлена 01.08 — до этого карточка
        # расходилась с каноном и показывала завышенный «Остаток ЗП» после переброски
        # монтажников. AN — накопитель ВСЕХ ног, поэтому конкурирует по максимуму, а не
        # суммируется. paid_prev = 0 (обычный счёт) → формула ровно прежняя.
        dr = float(r["dr"] or 0)
        paid_prev = float(r["pprev"] or 0)
        if r["rem"] and bot_paid > 0:
            paid = max(an, paid_prev + adv_cg + bot_paid, dr)
        else:
            paid = max(an, paid_prev + max(adv_cg, bot_paid), dr)
        bj = max(0.0, agreed - paid) if agreed > 0 else 0.0
        if bj <= 0.001:
            continue  # только счета с остатком (user 09.06)
        # Улица — 7 символов, как в карточке «Этапы работы» (rp_start_card._matrix,
        # name_w=7: «ряды не переносятся» на узком экране телефона). User 10.06.
        street = _street(r.get("object_address"), 7) or str(
            r.get("invoice_number") or f"#{inv_id}"
        )
        marker = "кр." if is_credit else "б/н"   # кредит-признак счёта (как в карточке аванса)
        table.append((street, marker, adv_cg, bj))
        sum_bj += bj

    # --- Рендер (<pre>) ---
    # A1 (user 10.06, вариант «текст слева / числа справа», доужато): объект+метка —
    # единым блоком СЛЕВА (улица 7 + метка 3 в ФИКС. char-колонках → марк-колонка ровная),
    # числа (Аванс CG / Остаток BJ) — СПРАВА в узких моноширинных полях. ⚠️ кириллица
    # улиц в Telegram <pre> НЕ моноширинна → числа выровнены по char-сетке, но возможен
    # лёгкий пиксель-съезд (feedback_card_telegram_pre_alignment); пиксель-идеал даёт
    # только раскладка «числа слева / объект справа» (user пока выбрал этот вариант).
    W_STREET = 7            # улица (как «Этапы работы», _street name_w=7)
    W_MK = 3                # метка «б/н»/«кр.» — фикс. char-колонка
    W_LEFT = W_STREET + 1 + W_MK   # 11: улица + пробел + метка
    W_ADV = 6               # колонка «Аванс» (вмещает заголовок «Аванс»=5 + зазор)
    W_OST = 8               # колонка «Остаток» (заголовок «Остаток»=7 + ведущий пробел-зазор)

    def _row(left: str, adv: str, ost: str) -> str:
        return f"{left[:W_LEFT]:<{W_LEFT}s}{adv:>{W_ADV}s}{ost:>{W_OST}s}"

    def _left(street: str, mk: str) -> str:
        # улица left-align в 7, метка left-align в 3 → метка всегда в одной char-колонке.
        return f"{street:<{W_STREET}s} {mk:<{W_MK}s}"

    SEP = "━" * 13          # heavy-разделитель (ширина карточки 25 char)

    lines: list[str] = []
    lines.append(_row("Объект", "Аванс", "Остаток"))
    lines.append(SEP)
    if table:
        for street, mk, adv_cg, bj in table:
            lines.append(_row(_left(street, mk), _k(adv_cg), _k(bj)))
    else:
        lines.append("нет счетов «Счёт ОК»")
    lines.append(SEP)
    # A2 (user 10.06): в строке «Итого» сумма аванса убрана — только Остаток (Σ BJ).
    lines.append(_row("Итого", "", _k(sum_bj)))

    body = "\n".join(lines)
    return f"<b>💰  ЗП к запросу — Счёт ОК</b>\n<pre>{body}</pre>"


async def build_deposit_history_card(
    db: "Database", installer_id: int, wallet_role: str | None = None,
) -> str:
    """Карточка «Депозит — история» кошелька сотрудника (эталон, ТЗ депозит 04.06).

    ПОЛНАЯ история движений депозита роли + «Баланс Итого» внизу. Зеркалит формулу
    db.get_deposit_balance (Σ движений == footer до рубля):
      • приход  — пополнение от ГД (paid deposit);
      • расход  — withdraw (личный расход сотрудника ИЛИ списание по запросу ГД),
                  показываем назначение + 📎 если есть вложение;
      • перевод — депозит → аванс (paid transfer_depo_to_adv, −депозит);
      • зачёт   — авто-зачёт депозита в счёт ЗП (deposit-parent item, offset_zp_id>0).

    Read-only витрина: в Invoices/лист ничего не пишет; кредит-признак НЕ показывается
    (для монтажника кредит == безнал). Депозит — отдельный личный кошелёк, к BS/BJ не
    привязан. Источники: db.list_all_advance_events / db.get_deposit_balance.
    """

    def _rub(n: Any) -> str:
        return f"{float(n or 0):,.0f}".replace(",", " ").replace("-", "−") + "₽"

    def _dd(ts: Any) -> str:
        s = str(ts or "")
        return f"{s[8:10]}.{s[5:7]}" if len(s) >= 10 and s[4:5] == "-" else (s[:5] or "—")

    # wallet-фильтр строк (зеркало db._wallet_clause / build_advance_history_card).
    def _wallet_ok(wr: Any) -> bool:
        if wallet_role is None:
            return True
        if wallet_role == "rp":
            return wr == "rp"
        return wr is None or wr != "rp"

    bal = await db.get_deposit_balance(installer_id, wallet_role)
    bal_s = _rub(bal)
    bundle = await db.list_all_advance_events(limit=500)

    # Приходы депозита (deposit) + расходы (withdraw) + перевод депо→аванс.
    reqs = [
        r for r in (bundle.get("requests") or [])
        if r.get("installer_id") == installer_id
        and r.get("status") == "paid"
        and (r.get("request_type") or "") in ("deposit", "withdraw", "transfer_depo_to_adv")
        and _wallet_ok(r.get("wallet_role"))
    ]
    # Зачёты депозита в ЗП: items с родителем-deposit и положительным offset_zp_id
    # (так же считает get_deposit_balance → Σ совпадёт с footer).
    depo_parent_ids = {
        r.get("req_id") for r in reqs
        if (r.get("request_type") or "") == "deposit"
    }
    offs = [
        it for it in (bundle.get("items") or [])
        if it.get("installer_id") == installer_id
        and it.get("request_id") in depo_parent_ids
        and (it.get("offset_zp_id") or 0) and int(it["offset_zp_id"]) > 0
    ]

    events: list[tuple[str, str, float, str]] = []  # (ts, sign, amount, label)
    for r in reqs:
        rt = r.get("request_type") or ""
        ts = str(r.get("paid_at") or r.get("requested_at") or "")
        cmt = (r.get("comment") or "").strip()
        if rt == "deposit":
            label = "Пополнение" + (" (ГД)" if r.get("initiator") == "gd" else "")
            if cmt:
                label += f" · {cmt[:18]}"
            events.append((ts, "+", float(r.get("total_amount") or 0), label))
        elif rt == "transfer_depo_to_adv":
            events.append((ts, "−", float(r.get("total_amount") or 0), "Перевод → Аванс"))
        else:  # withdraw
            label = "Расход"
            if cmt:
                label += f" · {cmt[:18]}"
            if r.get("payment_file_id"):
                label += " 📎"
            events.append((ts, "−", float(r.get("total_amount") or 0), label))
    for it in offs:
        ts = str(it.get("offset_at") or "")
        num = it.get("invoice_number") or f"#{it.get('invoice_id')}"
        events.append((ts, "−", float(it.get("offset_amount") or 0), f"Зачёт {num}"))

    events.sort(key=lambda e: (e[0] or ""), reverse=True)  # новые сверху

    move_items: list[tuple[str, str]] = []
    for ts, sign, amt, label in events:
        arrow = "⬆️" if sign == "+" else "⬇️"
        move_items.append((f"{_dd(ts)} {arrow}", f"{sign}{_rub(amt)} {label}".strip()))
    if not move_items:
        move_items = [("—", "пока нет операций")]

    return format_card_section(
        emoji="💳",
        title="Депозит — история",
        items=move_items,
        total=bal_s,
        footer=("Баланс Итого", bal_s),
        width=36,
        compact=True,
    )


def fmt_project_card(project: dict[str, Any], tz_name: str) -> str:
    """Карточка проекта в эталонном дизайне (assets/card_etalon.png).

    Одна <pre>-секция с моноширинным выравниванием.
    """
    title = html.quote(project.get("title") or "—")
    address = html.quote(project.get("address") or "—")
    client = html.quote(project.get("client") or "—")
    status = html.quote(project_status_label(str(project.get("status") or "")))
    amount = project.get("amount")
    amount_s = (
        f"{amount:,.0f}₽".replace(",", " ")
        if isinstance(amount, (int, float))
        else "—"
    )
    deadline = format_date_iso(project.get("deadline"), tz_name)
    created = format_dt_iso(project.get("created_at"), tz_name)
    updated = format_dt_iso(project.get("updated_at"), tz_name)

    return format_card_section(
        emoji="📋",
        title="Проект",
        items=[
            ("Название", title),
            ("Адрес", address),
            ("Клиент", client),
            ("Сумма", amount_s),
            ("Дедлайн", deadline),
            ("Статус", status),
            ("Создан", created),
            ("Обновлён", updated),
        ],
        width=44,
        compact=True,
    )


async def enrich_task_invoice_label(db: "Database", task: dict[str, Any]) -> dict[str, Any]:
    """Дополнить payload задачи номером счёта + адресом объекта по
    linked_invoice_id / invoice_id, чтобы карточки задач показывали
    человекочитаемую привязку (напр. «КВ 5» + адрес) вместо сырого #id
    (ТЗ 17.06: «#48 ни о чём не говорит»). Мутирует task["payload_json"]
    (сериализует обратно в JSON-строку). Идемпотентно; ошибки — no-op.
    """
    try:
        payload = try_json_loads(task.get("payload_json"))
        if not isinstance(payload, dict):
            return task
        if payload.get("invoice_number") and payload.get("object_address"):
            return task
        inv_id = payload.get("linked_invoice_id") or payload.get("invoice_id")
        if not inv_id:
            return task
        inv = await db.get_invoice(int(inv_id))
        if inv:
            payload.setdefault("invoice_number", inv.get("invoice_number"))
            payload.setdefault("object_address", inv.get("object_address"))
            task["payload_json"] = json.dumps(payload, ensure_ascii=False)
    except Exception:
        log.debug("enrich_task_invoice_label failed for task %s", task.get("id"), exc_info=True)
    return task


def _task_payload_items(task: dict[str, Any]) -> list[tuple[str, str]]:
    """Поля из payload задачи для секции «Детали».

    Общая логика для fmt_task_card (все роли) и build_manager_task_open_card
    (роль менеджер — ТЗ 23.06). Возвращает список (label, value), уже HTML-safe.
    """
    payload = try_json_loads(task.get("payload_json"))
    payload_items: list[tuple[str, str]] = []
    # Поля лида (LEAD_TO_PROJECT / ASSIGN_LEAD) — иначе теряются в карточке.
    # Гейт по типу: ключи name/phone/address генеричны и в других задачах
    # значат «объект/клиент», поэтому печатаем их ТОЛЬКО для лид-задач
    # (feedback_design_only_indicated_block — не трогаем чужие карточки).
    if str(task.get("type") or "") in ("lead_to_project", "assign_lead"):
        _ln = payload.get("name") or payload.get("lead_name")
        if _ln:
            payload_items.append(("Имя", html.quote(str(_ln))))
        _lp = payload.get("phone") or payload.get("lead_phone")
        if _lp:
            payload_items.append(("Телефон", html.quote(str(_lp))))
        _la = payload.get("address") or payload.get("lead_address")
        if _la:
            payload_items.append(("Адрес", html.quote(str(_la))))
        _ls = payload.get("source") or payload.get("lead_source")
        if _ls:
            payload_items.append(("Источник", html.quote(str(_ls))))
    if payload.get("invoice_number"):
        payload_items.append(("№ счёта", html.quote(str(payload["invoice_number"]))))
    if payload.get("object_address"):
        payload_items.append(("Объект", html.quote(str(payload["object_address"]))))
    if payload.get("material_type"):
        from .enums import MATERIAL_TYPE_LABELS
        _mt = str(payload["material_type"])
        payload_items.append(("Материал", html.quote(MATERIAL_TYPE_LABELS.get(_mt, _mt))))
    if payload.get("supplier"):
        payload_items.append(("Поставщик", html.quote(str(payload["supplier"]))))
    if payload.get("payment_method"):
        payload_items.append(("Тип оплаты", html.quote(str(payload["payment_method"]))))
    if payload.get("payment_type"):
        payload_items.append(("Этап оплаты", html.quote(str(payload["payment_type"]))))
    if payload.get("payment_amount"):
        try:
            _pa = float(payload["payment_amount"])
            payload_items.append(("Сумма оплаты", f"{_pa:,.0f}₽".replace(",", " ")))
        except (TypeError, ValueError):
            payload_items.append(("Сумма оплаты", html.quote(str(payload["payment_amount"]))))
    if payload.get("amount") and not payload.get("payment_amount"):
        try:
            _a = float(payload["amount"])
            payload_items.append(("Сумма", f"{_a:,.0f}₽".replace(",", " ")))
        except (TypeError, ValueError):
            payload_items.append(("Сумма", html.quote(str(payload["amount"]))))
    if payload.get("doc_type"):
        payload_items.append(("Документы", html.quote(str(payload["doc_type"]))))
    if payload.get("sign_type"):
        payload_items.append(("Подписание", html.quote(str(payload["sign_type"]))))
    if payload.get("issue_type"):
        payload_items.append(("Тип проблемы", html.quote(str(payload["issue_type"]))))
    if payload.get("address_from"):
        payload_items.append(("Откуда", html.quote(str(payload["address_from"]))))
    if payload.get("address_to"):
        payload_items.append(("Куда", html.quote(str(payload["address_to"]))))
    if payload.get("cargo"):
        payload_items.append(("Груз", html.quote(str(payload["cargo"]))))
    if payload.get("measurements"):
        payload_items.append(("Размеры/ТЗ", html.quote(str(payload["measurements"]))))
    if payload.get("description"):
        payload_items.append(("Описание", html.quote(str(payload["description"]))))
    if payload.get("details"):
        payload_items.append(("Уточнение", html.quote(str(payload["details"]))))
    if payload.get("comment"):
        payload_items.append(("Комментарий", html.quote(str(payload["comment"]))))
    # Назначение траты кредит-кошелька: у таких задач invoice_number пуст, и без
    # этой строки карточка из списка показывала «Счёт на оплату / Сумма 20 000 ₽»
    # и больше ничего — по задаче #426 полгода нельзя было понять, за что деньги.
    # Пуш при создании подпись имел, терялась только карточка. Метка та же, что в
    # соседних сборщиках (build_gd_task_open_card, build_manager_task_open_card).
    if payload.get("purpose"):
        payload_items.append(("Назначение", html.quote(str(payload["purpose"]))))
    # Привязка к счёту: человекочитаемо — «№ счёта» + «Объект» (адрес) выше,
    # обогащаются enrich_task_invoice_label. Сырой #id — только fallback, если
    # счёт не разрешился (ТЗ 17.06: «#48 ни о чём не говорит»).
    if payload.get("linked_invoice_id") and not payload.get("invoice_number"):
        payload_items.append(("Привязка", f"#{payload['linked_invoice_id']}"))
    if payload.get("parent_invoice_id"):
        payload_items.append(("Объект (счёт)", f"#{payload['parent_invoice_id']}"))
    return payload_items


def fmt_task_card(task: dict[str, Any], project: dict[str, Any] | None, tz_name: str) -> str:
    """Карточка задачи в эталонном дизайне (assets/card_etalon.png).

    1-2 <pre>-секции: главная (статус/срок/тип) + опц. детали из payload.
    Если задан project — добавляется секция fmt_project_card.
    """
    tid = task["id"]
    ttype = html.quote(task_type_label(task.get("type")))
    status = html.quote(task_status_label(task.get("status")))
    due = format_dt_iso(task.get("due_at"), tz_name)
    created = format_dt_iso(task.get("created_at"), tz_name)

    sections: list[str] = []
    sections.append(
        format_card_section(
            emoji="📋",
            title=f"Задача #{tid}",
            items=[
                ("Тип", ttype),
                ("Статус", status),
                ("Срок", due),
                ("Создана", created),
            ],
            compact=True,
        )
    )

    payload_items = _task_payload_items(task)
    if payload_items:
        sections.append(
            format_card_section(
                emoji="📦", title="Детали", items=payload_items, compact=True
            )
        )

    if project:
        sections.append(fmt_project_card(project, tz_name))

    return format_card(sections)


RP_OKLAD_CARD_SEP = "   ━━━━━━━━━━━━━━━━"


def format_rp_oklad_lines(calc: dict[str, float] | None, oklad: float) -> list[str]:
    """Строки суммы для карточек оклада РП — с зачётом выданного аванса или без него.

    ТЗ owner 31.07: выданный РП аванс вычитается из оклада, показывается остаток.
    Аванса нет (deduct=0) → возвращаем РОВНО прежнюю однострочную «Итого», чтобы там,
    где менять нечего, карточка не менялась [[feedback_card_compact_means_height]].

    Аванс лежит в кошельке телом (30 000), а оклад идёт б/н самозанятому и уже содержит
    +10%, поэтому вычитается приведённая величина (33 000) — расчёт в
    db.get_rp_oklad_advance_offset, здесь только показ [[feedback_card_display_only]].
    Хвост (carry) печатаем отдельной строкой: он остаётся в кошельке на следующий месяц.
    """
    def _r(v: Any) -> str:
        return f"{float(v or 0):,.0f}".replace(",", " ")

    calc = calc or {}
    deduct = float(calc.get("deduct") or 0)
    if deduct <= 0:
        return [f"   Итого  {_r(oklad)} ₽"]
    lines = [
        f"   {'Оклад':<20}{_r(oklad):>7} ₽",
        f"   {'Аванс зачтён':<20}{'−' + _r(deduct):>7} ₽",
        RP_OKLAD_CARD_SEP,
        f"   К выплате  {_r(calc.get('payout'))} ₽",
    ]
    carry = float(calc.get("carry") or 0)
    if carry > 0:
        lines.append(f"   {'Аванс остаток':<20}{_r(carry):>7} ₽")
    return lines


def build_rp_zp_family_open_card(
    task: dict[str, Any], advance: dict[str, float] | None = None,
) -> str:
    """Эталонная карточка ЗП РП 10% / Оклад РП при открытии задачи из инбокса ГД.

    Тот же вид, что уведомление РП→ГД (rp.py) и экран «Прочие ЗП»
    (td.gd_pay_rpzp_open): От / список счетов (или месяц) / Итого / Статус —
    развёрнутая инфа вместо generic-стаба fmt_task_card (у которого нет разбивки
    счетов и суммы). Display-only. [[feedback_card_template_standard]]

    advance — результат db.get_rp_oklad_advance_offset, считается ЖИВЬЁМ вызывающим
    (tasks.send_task_open_card). Не из payload: задачи, созданные до 31.07, ключей про
    аванс не имеют, а пересоздавать их ради показа нельзя (задача #420 уже висит у ГД).
    """
    from .enums import TaskType

    payload = try_json_loads(task.get("payload_json")) or {}
    ttype = str(task.get("type") or "")
    status_lbl = html.quote(task_status_label(task.get("status")))
    rp_name = html.quote(str(payload.get("rp_name") or "РП"))

    def _rub(v: Any) -> str:
        return f"{float(v or 0):,.0f}".replace(",", " ")

    # Оклад РП (RP_SALARY): месяц + фикс. сумма (минус зачтённый аванс, если он есть).
    if ttype == TaskType.RP_SALARY.value:
        amount = float(payload.get("amount") or 0)
        month = html.quote(str(payload.get("month") or "—"))
        return "\n".join([
            "<pre>💼 <b>Запрос оклада</b>",
            f"   От                   {rp_name}",
            f"   Месяц                {month}",
            RP_OKLAD_CARD_SEP,
            *format_rp_oklad_lines(advance, amount),
            f"   Статус  {status_lbl}",
            "</pre>",
        ])

    # ЗП РП 10% (мульти-счёт): список счетов + итог + тип оплаты (кредит/б/н).
    total = float(payload.get("total") or 0)
    invoices_info = payload.get("invoices") or []
    pt = str(payload.get("payment_type") or "")
    pt_suffix = {"credit": " · 🏦 Кредитные", "beznal": " · 💳 Б/н"}.get(pt, "")
    lines = [f"<pre>💰 <b>Запрос ЗП РП</b>{pt_suffix}", f"   От                    {rp_name}"]
    for it in invoices_info:
        num = str(it.get("invoice_number") or "?")
        lines.append(f"   №{num:<18s} {_rub(it.get('amount')):>10s} ₽")
    lines.append("   ━━━━━━━━━━━━━━━━")
    lines.append(f"   Итого  {_rub(total)} ₽")
    lines.append(f"   Статус  {status_lbl}")
    lines.append("</pre>")
    return "\n".join(lines)


# Замер: дивизион менеджера для карточки замерщика (без личного имени, user 29.06).
_ZAMERY_ROLE_SHORT = {"manager_kv": "КВ", "manager_kia": "КИА", "manager_npn": "НПН"}
_ZAMERY_DOW = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


async def build_task_reminder_card(
    db: "Database",
    task: dict[str, Any],
    project: dict[str, Any] | None,
    tz_name: str,
) -> str:
    """ЕДИНАЯ карточка-напоминание (ТЗ owner 24.06).

    Отличия от fmt_task_card (её НЕ трогаем — держит открытие задач/done-карточки):
      • заголовок = СУТЬ задачи (тип, напр. «ЗП монтажника»), а НЕ «Задача #N»;
      • ОДИН <pre>-блок — прежние секции «Задача»+«Детали» объединены;
      • + Адрес объекта (из привязанного счёта);
      • для zp_installer (ЗП монтажника): + Расчётная (предложенная монтажнику на
        согласование = _calc_est_montazh) + Фактическая (что монтажник подал).

    Прочие типы (тонировка/поставщик/…) расчётной/фактической НЕ имеют — у них
    показывается обычная «Сумма». owner 24.06 «все напоминания generic».
    Скоуп — ТОЛЬКО напоминания (services/reminders.py).
    """
    payload = try_json_loads(task.get("payload_json")) or {}
    ttype = str(task.get("type") or "")
    title = task_type_label(ttype)

    # Привязка к счёту — источник адреса и расчётной стоимости монтажа.
    inv_id = (
        payload.get("invoice_id")
        or payload.get("linked_invoice_id")
        or payload.get("parent_invoice_id")
    )
    inv = None
    if inv_id:
        try:
            inv = await db.get_invoice(int(inv_id))
        except Exception:
            inv = None

    items: list[tuple[str, str]] = []

    # Заявка на замер (для замерщика, user 29.06): вместо «Инициатор: <имя>» —
    # дивизион менеджера «Менеджер: НПН/КВ/КИА» (без личного имени/username) +
    # дата/время замера ниже. requester_role/scheduled_* берём из zamery_requests.
    zam_req: dict[str, Any] | None = None
    if ttype == "zamery_request":
        zr_id = payload.get("zamery_request_id")
        if zr_id:
            try:
                zam_req = await db.get_zamery_request(int(zr_id))
            except Exception:
                zam_req = None
        division = _ZAMERY_ROLE_SHORT.get(
            str((zam_req or {}).get("requester_role") or ""), ""
        )
        items.append(("Менеджер", division or "—"))
    else:
        # Инициатор — «кто подал» (created_by; для кредит-задач — payload.initiator_id).
        initiator_id = task.get("created_by") or payload.get("initiator_id")
        if initiator_id:
            try:
                items.append(("Инициатор", await get_initiator_label(db, int(initiator_id))))
            except Exception:
                pass

    # Адрес объекта (сокращённая улица, как в карточках монтажника).
    addr = (
        (inv.get("object_address") if inv else None)
        or payload.get("object_address")
        or payload.get("address")
    )
    if addr:
        from .rp_start_card import _street
        items.append(("Адрес", html.quote(_street(str(addr), 24))))

    # Дата/время замера (указывает менеджер при создании заявки) — для zamery_request.
    if zam_req:
        sd = zam_req.get("scheduled_date")
        if sd:
            try:
                from datetime import date as _date
                _d = _date.fromisoformat(str(sd))
                items.append((
                    "Дата",
                    f"{_ZAMERY_DOW[_d.weekday()]} {_d.strftime('%d.%m.%Y')}",
                ))
            except (ValueError, TypeError):
                items.append(("Дата", str(sd)))
        if zam_req.get("scheduled_time_interval"):
            items.append(("Время", str(zam_req["scheduled_time_interval"])))

    # № счёта
    num = payload.get("invoice_number") or (inv.get("invoice_number") if inv else None)
    if num:
        items.append(("Счёт", html.quote(f"№{num}")))

    # Назначение (кредит-задачи: purpose)
    if payload.get("purpose"):
        items.append(("Назначение", html.quote(str(payload["purpose"]))))

    if ttype == "zp_installer":
        # Расчётная (предложенная монтажнику) + Фактическая (что монтажник подал).
        if inv:
            from .handlers.installer_new import _calc_est_montazh
            est = _calc_est_montazh(inv)
            if est:
                items.append(("Расчётная", fmt_money(est)))
        if payload.get("amount") is not None:
            items.append(("Фактическая", fmt_money(payload.get("amount"))))
    else:
        amt = payload.get("amount") or payload.get("payment_amount")
        if amt is not None:
            items.append(("Сумма", fmt_money(amt)))

    items.append(("Статус", html.quote(task_status_label(task.get("status")))))
    items.append(("Создана", format_dt_iso(task.get("created_at"), tz_name)))
    if task.get("due_at"):
        items.append(("Срок", format_dt_iso(task.get("due_at"), tz_name)))

    sections = [format_card_section(emoji="📋", title=title, items=items, compact=True)]
    if project:
        sections.append(fmt_project_card(project, tz_name))
    return format_card(sections)


def build_task_done_card(
    task: dict[str, Any],
    project: dict[str, Any] | None,
    tz_name: str,
    *,
    title: str = "Задача выполнена",
    emoji: str = "✅",
    actor_label: str | None = None,
    actor_field: str = "Исполнитель",
) -> str:
    """Эталонный отчёт-карточка «задача выполнена» постановщику/инициатору.

    Шапка (эмодзи + название + кто выполнил) ВНЕ <pre> + эталонная карточка
    задачи (fmt_task_card): секция 📋 Задача (Тип = какую конкретно задачу /
    Статус / Срок / Создана) + 📦 Детали из payload (счёт/поставщик/сумма/
    комментарий/…) [+ секция проекта, если передан]. Единый вид для всех ролей.

    actor_label — уже HTML-safe строка из get_initiator_label (повторно НЕ
    экранировать: может содержать <a href> mention).
    Эталон: feedback_card_template_standard.md.
    """
    head = f"{emoji} <b>{title}</b>"
    if actor_label:
        head += f"\n👤 {actor_field}: {actor_label}"
    return f"{head}\n\n{fmt_task_card(task, project, tz_name)}"


# «Что сделать» — человекочитаемое действие по типу задачи (для шапки карточки).
_TASK_ACTION_HINT: dict[str, str] = {
    "final_payment_eta": "Указать ориентировочную дату финального платежа",
    "invoice_end_ready": "Закрыть счёт (Счет End)",
    "invoice_end_fixup": "Устранить пункты по счёту",
    "invoice_payment": "Оплатить поставщику",
    "supplier_payment": "Оплатить поставщику",
    "assign_lead": "Принять лида в работу",
    "lead_to_project": "Обработать лида",
    "gd_task": "Выполнить задачу",
}


async def build_manager_task_card(
    db: "Database",
    task: dict[str, Any],
    tz_name: str = "Europe/Moscow",
    *,
    header_emoji: str = "📋",
    header_title: str | None = None,
    actor_label: str | None = None,
    actor_field: str = "Поставил",
) -> str:
    """Полная информативная карточка задачи для менеджера (эталон).

    Шапка (emoji + «что сделать» + кто поставил) ВНЕ <pre> + секции:
    📋 Задача (что сделать/статус/срок/создана). Если задача привязана к счёту —
    + 📋 Счёт (адрес/клиент/тип/сумма/долг) + 💰 Что оплачивается (сумма/
    поставщик/материал/этап из payload) + 📦 Услуги/материалы (дочерние счета +
    оплаты поставщикам — список без сумм по позициям). Если счёта нет (лиды и
    пр.) — генеричная эталонная карточка fmt_task_card со всеми полями payload.

    Read-only витрина ([[feedback_card_display_only_no_data_writes]]): суммы
    оплаты/общая/долг — показываем, себестоимость по позициям и прибыль — НЕТ
    (user 18.06: «услуги/материалы + суммы оплаты»). Эталон — feedback_card_template_standard.
    """
    from .enums import MATERIAL_TYPE_LABELS

    await enrich_task_invoice_label(db, task)
    payload = try_json_loads(task.get("payload_json")) or {}
    ttype = str(task.get("type") or "")

    title = header_title or _TASK_ACTION_HINT.get(ttype) or task_type_label(ttype)
    head = f"{header_emoji} <b>{html.quote(title)}</b>"
    if actor_label:
        head += f"\n👤 {actor_field}: {actor_label}"

    # «Что сделать» — реальная инструкция (comment/description) или дефолт по типу.
    action_text = (
        str(payload.get("comment") or payload.get("description") or "").strip()
        or _TASK_ACTION_HINT.get(ttype)
        or task_type_label(ttype)
    )

    inv_id = (
        payload.get("invoice_id")
        or payload.get("linked_invoice_id")
        or payload.get("parent_invoice_id")
    )
    inv = None
    if inv_id:
        try:
            inv = await db.get_invoice(int(inv_id))
        except Exception:
            inv = None

    # Нет привязки к счёту (лиды/прочее) → генеричная эталонная карточка.
    if not inv:
        return f"{head}\n\n{fmt_task_card(task, None, tz_name)}"

    sections: list[str] = []

    # 📋 Задача
    task_items: list[tuple[str, str]] = [
        ("Что сделать", html.quote(action_text)),
        ("Статус", html.quote(task_status_label(task.get("status")))),
    ]
    if task.get("due_at"):
        task_items.append(("Срок", format_dt_iso(task.get("due_at"), tz_name)))
    task_items.append(("Создана", format_dt_iso(task.get("created_at"), tz_name)))
    _tid = task.get("id")
    _task_title = f"Задача #{_tid}" if _tid else "Задача"
    sections.append(
        format_card_section(emoji="📋", title=_task_title, items=task_items, compact=True)
    )

    # 📋 Счёт
    num = html.quote(str(inv.get("invoice_number") or f"#{inv_id}"))
    is_credit = bool(inv.get("is_credit"))
    status = inv.get("status") or ""
    inv_items: list[tuple[str, str]] = [
        ("Адрес", html.quote(str(inv.get("object_address") or "—"))),
        ("Клиент", html.quote(str(inv.get("client_name") or "—"))),
        ("Тип", "🏦 Кред" if is_credit else "💳 б/н"),
        ("Сумма", fmt_money(float(inv.get("amount") or 0))),
    ]
    debt = float(inv.get("outstanding_debt") or 0)
    if debt > 0:
        inv_items.append(("Долг", fmt_money(debt)))
    inv_total = "🏦 Кредит" if (is_credit and status not in ("ended", "rejected")) else None
    sections.append(
        format_card_section(emoji="📋", title=f"Счёт №{num}", items=inv_items, total=inv_total, compact=True)
    )

    # 💰 Что оплачивается (специфика задачи из payload)
    pay_items: list[tuple[str, str]] = []
    _pa = payload.get("payment_amount") or payload.get("amount")
    if _pa:
        try:
            pay_items.append(("Сумма оплаты", fmt_money(float(_pa))))
        except (TypeError, ValueError):
            pay_items.append(("Сумма оплаты", html.quote(str(_pa))))
    if payload.get("purpose"):
        pay_items.append(("Назначение", html.quote(str(payload["purpose"]))))
    if payload.get("supplier"):
        pay_items.append(("Поставщик", html.quote(str(payload["supplier"]))))
    if payload.get("material_type"):
        _mt = str(payload["material_type"])
        pay_items.append(("Материал", html.quote(MATERIAL_TYPE_LABELS.get(_mt, _mt))))
    if payload.get("payment_type"):
        pay_items.append(("Этап", html.quote(str(payload["payment_type"]))))
    if payload.get("payment_method"):
        pay_items.append(("Способ", html.quote(str(payload["payment_method"]))))
    if pay_items:
        sections.append(
            format_card_section(emoji="💰", title="Что оплачивается", items=pay_items, compact=True)
        )

    # 📦 Услуги/материалы — список категорий (дедуп) + поставщики, без сумм по позициям.
    try:
        children = await db.list_child_invoices(int(inv_id))
    except Exception:
        children = []
    try:
        sps = await db.list_supplier_payments_for_invoice(int(inv_id))
    except Exception:
        sps = []
    cat_suppliers: dict[str, list[str]] = {}
    for rec in list(children or []) + list(sps or []):
        _m = rec.get("material_type") or ""
        lbl = MATERIAL_TYPE_LABELS.get(_m, _m) if _m else "Прочее"
        sup = str(rec.get("supplier") or "").strip()
        cat_suppliers.setdefault(lbl, [])
        if sup and sup not in cat_suppliers[lbl]:
            cat_suppliers[lbl].append(sup)
    if cat_suppliers:
        mat_lines: list[str] = []
        for lbl, sups in cat_suppliers.items():
            line = f"   • {html.quote(lbl)}"
            if sups:
                line += f" — {html.quote(', '.join(sups))}"
            mat_lines.append(line)
        sections.append("<b>📦  Услуги / материалы</b>\n<pre>" + "\n".join(mat_lines) + "</pre>")

    body = format_card(sections)

    extra_note = payload.get("details")
    tail = f"\n\n💬 {html.quote(str(extra_note))}" if extra_note else ""
    return f"{head}\n\n{body}{tail}"


def _docs_status_compact(inv: dict[str, Any], payload: dict[str, Any]) -> str:
    """Компактный статус первичных документов счёта: ЭДО + оригиналы (ТЗ 23.06).

    Источник: docs_edo_signed / docs_originals_holder. 🔴 при payload.fully_empty
    («совсем нет документов»). Выбор owner 23.06: «ЭДО + оригиналы (компактно)».
    """
    edo = "✅" if int(inv.get("docs_edo_signed") or 0) else "⏳"
    holder = inv.get("docs_originals_holder") or ""
    if holder == "gd":
        orig = "✅ГД"
    elif holder == "manager":
        orig = "✅Мнж"
    else:
        orig = "⏳"
    line = f"ЭДО {edo} · ориг {orig}"
    if payload.get("fully_empty"):
        line = "🔴 " + line
    return line


# Отметка менеджера по задаче «Нет документов по счёту» (payload.docs_status,
# ТЗ 10.07). Отдельно от _docs_status_compact (тот — ЭДО/оригиналы самого счёта).
_DOCS_STATUS_MANAGER_LABELS: dict[str, str] = {
    "formalized": "✅ Оформлены",
    "in_work": "⏳ В работе",
    "requested": "📤 Запрошены у клиента",
}


async def build_manager_task_open_card(
    db: "Database",
    task: dict[str, Any],
    tz_name: str = "Europe/Moscow",
) -> str:
    """Карточка задачи при открытии из списка — роль МЕНЕДЖЕР (ТЗ 23.06).

    ОДИН <pre>-блок (объединяет прежние «📋 Задача» + «📦 Детали»). Заголовок —
    текст задачи («{тип} {№ счёта}») вместо «#id». Если задача привязана к счёту —
    + поля: инициатор / адрес / сумма счёта / долг / статус документов.
    Owner 23.06: применять ко ВСЕМ карточкам задач менеджера (поля счёта —
    только где есть привязанный счёт). Read-only витрина
    ([[feedback_card_display_only_no_data_writes]]).
    """
    await enrich_task_invoice_label(db, task)
    payload = try_json_loads(task.get("payload_json")) or {}
    ttype = str(task.get("type") or "")

    inv_id = (
        payload.get("invoice_id")
        or payload.get("linked_invoice_id")
        or payload.get("parent_invoice_id")
    )
    inv = None
    if inv_id:
        try:
            inv = await db.get_invoice(int(inv_id))
        except Exception:
            inv = None

    # Заголовок: «Задача «{тип}{ № счёта}»» вместо «Задача #id» (требование owner #2).
    label = task_type_label(ttype)
    num = (inv.get("invoice_number") if inv else None) or payload.get("invoice_number")
    if num:
        title = f"Задача «{html.quote(label)} {html.quote(str(num))}»"
    else:
        title = f"Задача «{html.quote(label)}»"

    items: list[tuple[str, str]] = [
        ("Статус", html.quote(task_status_label(task.get("status")))),
    ]
    if task.get("due_at"):
        items.append(("Срок", format_dt_iso(task.get("due_at"), tz_name)))
    items.append(("Создана", format_dt_iso(task.get("created_at"), tz_name)))

    created_by = task.get("created_by")
    if created_by:
        # get_initiator_label — plain "Имя (@username)", HTML-safe, без <a> → ок в <pre>.
        items.append(("Инициатор", await get_initiator_label(db, int(created_by))))

    if inv:
        items.append(("Адрес", html.quote(str(inv.get("object_address") or "—"))))
        items.append(("Сумма счёта", fmt_money(float(inv.get("amount") or 0))))
        items.append(("Долг", fmt_money(float(inv.get("outstanding_debt") or 0))))
        items.append(("Статус документов", _docs_status_compact(inv, payload)))
        # Отметка менеджера по задаче «Нет документов» (docs_status) — видно перед сменой.
        _mgr_ds = payload.get("docs_status")
        if ttype == "invoice_docs_missing" and _mgr_ds:
            items.append(("Отметка", _DOCS_STATUS_MANAGER_LABELS.get(_mgr_ds, str(_mgr_ds))))

    # «Детали» из payload — в тот же блок. № счёта/Объект/Адрес уже показаны из
    # счёта (заголовок + поля), поэтому при наличии счёта их не дублируем.
    _skip = {"№ счёта", "Объект", "Адрес"} if inv else set()
    for k, v in _task_payload_items(task):
        if k in _skip:
            continue
        items.append((k, v))

    section = format_card_section(emoji="📋", title=title, items=items, compact=True)
    return format_card([section])


def fmt_payment_details_card(payload: dict[str, Any], object_address: str = "") -> str:
    """«📦 Детали» как отдельная карточка — для дублирования в шаг «Подтверждение
    оплаты» (роль ГД).

    Раскладка как у секции «Детали» в fmt_task_card, но строка «Объект» показывает
    адрес объекта (object_address) вместо #parent_invoice_id (запрос ГД 01.06).
    Возвращает "" если деталей нет.
    """
    items: list[tuple[str, str]] = []
    if payload.get("invoice_number"):
        items.append(("№ счёта", html.quote(str(payload["invoice_number"]))))
    if payload.get("material_type"):
        from .enums import MATERIAL_TYPE_LABELS
        _mt = str(payload["material_type"])
        items.append(("Материал", html.quote(MATERIAL_TYPE_LABELS.get(_mt, _mt))))
    if payload.get("supplier"):
        items.append(("Поставщик", html.quote(str(payload["supplier"]))))
    if payload.get("payment_method"):
        items.append(("Тип оплаты", html.quote(str(payload["payment_method"]))))
    if payload.get("payment_type"):
        items.append(("Этап оплаты", html.quote(str(payload["payment_type"]))))
    if payload.get("payment_amount"):
        try:
            _pa = float(payload["payment_amount"])
            items.append(("Сумма оплаты", f"{_pa:,.0f}₽".replace(",", " ")))
        except (TypeError, ValueError):
            items.append(("Сумма оплаты", html.quote(str(payload["payment_amount"]))))
    if payload.get("amount") and not payload.get("payment_amount"):
        try:
            _a = float(payload["amount"])
            items.append(("Сумма", f"{_a:,.0f}₽".replace(",", " ")))
        except (TypeError, ValueError):
            items.append(("Сумма", html.quote(str(payload["amount"]))))
    if payload.get("comment"):
        items.append(("Комментарий", html.quote(str(payload["comment"]))))
    # «Объект»: адрес объекта вместо #parent_invoice_id (запрос ГД)
    if object_address:
        items.append(("Объект", html.quote(str(object_address))))
    elif payload.get("parent_invoice_id"):
        items.append(("Объект (счёт)", f"#{payload['parent_invoice_id']}"))

    if not items:
        return ""
    return format_card_section(emoji="📦", title="Детали", items=items, compact=True)
