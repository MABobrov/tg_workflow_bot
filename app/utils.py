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
    "not_urgent_gd": "Не срочно ГД",
    # --- новые типы (фаза расширения) ---
    "edo_request": "Запрос ЭДО",
    "installer_ok": "Монтажник — Счет ОК",
    "zp_calculation": "Расчёт ЗП",
    "lead_to_project": "Лид в проект",
    "invoice_end": "Счет End",
    "check_kp": "Проверить КП / Счет",
}


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
            return datetime(y, mo, d, 12, 0, tzinfo=tzinfo(tz_name))
        except ValueError:
            return None

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
                # If the date is in the past, assume next year
                if result < now_local:
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


def format_materials_list(
    inv: dict[str, Any],
    children: list[dict[str, Any]],
    supplier_payments: list[dict[str, Any]],
) -> str:
    """Список купленных материалов для менеджера (без сумм)."""
    from .enums import MATERIAL_TYPE_LABELS

    num = html.quote(str(inv.get("invoice_number") or f"#{inv.get('id')}"))
    addr = html.quote(str(inv.get("object_address") or inv.get("address") or "—"))

    lines: list[str] = [
        f"📦 <b>Материалы — Счёт №{num}</b>",
        f"📍 {addr}",
    ]

    if not children and not supplier_payments:
        lines.append("\nНет записей о закупках.")
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

    lines.append(f"\nВсего позиций: {len(children) + len(supplier_payments)}")
    return "\n".join(lines)


def format_rp_expenses(
    inv: dict[str, Any],
    children: list[dict[str, Any]],
    supplier_payments: list[dict[str, Any]],
) -> str:
    """Расходы по счёту для РП (расширенный доступ — С суммами, БЕЗ маржи)."""
    from .enums import MATERIAL_TYPE_LABELS

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


def format_cost_card(inv: dict[str, Any], cost: dict[str, Any]) -> str:
    """HTML-карточка себестоимости для Telegram."""
    from .enums import MATERIAL_TYPE_LABELS

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

    # --- Итого ---
    total_cost = cost.get("total_cost", 0)
    margin = cost.get("margin", 0)
    margin_pct = cost.get("margin_pct", 0)
    lines.append("")
    lines.append("═══════════════════")
    lines.append(f"📊 <b>ИТОГО РАСХОДЫ:</b> {total_cost:,.0f} руб.")
    if inv_amount > 0:
        lines.append(f"📈 <b>МАРЖА:</b> {margin:,.0f} руб. ({margin_pct:.1f}%)")

    return "\n".join(lines)


def format_plan_fact_card(inv: dict[str, Any], pf: dict[str, Any], role: str = "gd") -> str:
    """HTML-карточка «План / Факт». role='rp' — упрощённая (без прибыли/себестоимости)."""
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
    # Группировка supplier payments: материалы vs услуги (→ монтаж)
    _sp_mat = 0.0
    _sp_svc = 0.0
    _SP_CAT = {"profile": "mat", "glass": "mat", "ldsp": "mat",
               "gkl": "mat", "sandwich": "mat", "other": "mat",
               "service": "svc"}
    for _sp in cost.get("supplier_payments_list", []):
        if _SP_CAT.get(_sp.get("material_type", "other"), "mat") == "svc":
            _sp_svc += _sp.get("amount", 0)
        else:
            _sp_mat += _sp.get("amount", 0)
    fact_mat = cost.get("materials_combined", cost.get("materials_total", 0)) + _sp_mat
    fact_inst = cost.get("montazh_combined", float(cost.get("zp_installer", 0))) + _sp_svc
    fact_load = cost.get("loaders_fact", 0)
    fact_log = cost.get("logistics_fact", 0)
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
        d = fact - plan
        if abs(d) < 0.5:
            return "     0 ✅"
        sign = "+" if d > 0 else ""
        ok = (d <= 0) if not invert else (d >= 0)
        icon = "✅" if ok else "⚠️"
        return f"{sign}{d:,.0f} {icon}"

    def _row(label: str, plan: float, fact: float, invert: bool = False) -> str:
        """Строка план/факт: если факт=0 (нет данных) — дельту не считать."""
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

    lines += [
        _row("Материалы", materials_total, fact_mat),
        _row("Установка", est_inst, fact_inst),
        _row("Грузчики", est_load, fact_load),
        _row("Логистика", est_log, fact_log),
        f"{'─' * 50}",
        _row("Себест-ть", est_total, fact_total if _has_key_facts else 0),
        f"{'─' * 50}",
        _row("Прибыль", est_profit, fact_profit if _has_key_facts else 0, invert=True),
        f"{'Рент-ть':14s} {est_pct:>9.1f}% " + (f"{fact_pct:>9.1f}%" if _has_key_facts else ""),
    ]
    # BM — Перерасчёт прибыли (показать если есть полные факт-данные и |разница| > 2000)
    if _has_key_facts:
        recalc = fact_profit - est_profit
        if abs(recalc) > 2000:
            lines.append(f"{'Перерасчёт':14s} {recalc:>+10,.0f}")
    # Profit split (inside <pre>)
    client_source = pf.get("client_source", "own")
    rp_zp = pf.get("rp_zp", 0)
    mgr_zp = pf.get("manager_zp", 0)
    gd_pr = pf.get("gd_profit", 0)
    src_label = "📋 Лид ГД (75/25)" if client_source == "gd_lead" else "👤 Клиент менеджера (50/50)"

    if pf.get("has_estimated") and est_profit > 0:
        lines.append(f"{'─' * 50}")
        lines.append(src_label)
        lines.append(f"{'Распределение прибыли:'}")
        lines.append(f"{'  ЗП РП (10%)':14s} {rp_zp:>10,.0f}₽")
        lines.append(f"{'  ЗП менеджер':14s} {mgr_zp:>10,.0f}₽")
        lines.append(f"{'  Доля ГД':14s} {gd_pr:>10,.0f}₽")

    lines.append("</pre>")

    # ZP status
    inv_status = inv.get("status", "")
    if pf.get("has_estimated"):
        if inv_status in ("pending", "in_progress", "paid"):
            lines.append("\n🔒 ЗП менеджера: <b>Заблокирована</b> (счёт в работе)")
        elif pf.get("zp_allowed"):
            lines.append("\n✅ ЗП менеджера: <b>Разрешена</b> (факт ≤ план)")
        else:
            delta = pf.get("cost_delta", 0)
            lines.append(
                f"\n❌ ЗП менеджера: <b>Заблокирована</b>\n"
                f"    Перерасход: {delta:+,.0f}₽"
            )
    else:
        lines.append("\n⚠️ Расчётные данные не заполнены")

    return "\n".join(lines)


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
    fact_inst = sum(
        float(inv.get("montazh_fact_op") or 0)
        or (float(inv.get("montazh_agreed_amount") or 0) or float(inv.get("zp_installer_amount") or 0)
            if inv.get("zp_installer_status") in ("approved", "confirmed") else 0)
        for inv in invoices
    )
    fact_load = sum(float(inv.get("loaders_fact_op") or 0) for inv in invoices)
    fact_log = sum(float(inv.get("logistics_fact_op") or 0) for inv in invoices)
    fact_total = fact_mat + fact_inst + fact_load + fact_log

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

        lines.append(f"<b>{month_name} {month_str[:4]}</b> — {m['cnt']} счетов")
        lines.append("<pre>")
        lines.append(f"{'':14s} {'План':>8s} {'Факт':>8s}")
        lines.append(f"{'Материалы':14s} {_f(m['est_materials']):>8s} {_f(m['fact_materials']):>8s}")
        lines.append(f"{'Установка':14s} {_f(m['est_installation']):>8s} {_f(m['fact_montazh']):>8s}")
        lines.append(f"{'Грузчики':14s} {_f(m['est_loaders']):>8s} {_f(m['fact_loaders']):>8s}")
        lines.append(f"{'Логистика':14s} {_f(m['est_logistics']):>8s} {_f(m['fact_logistics']):>8s}")
        lines.append(f"{'─' * 32}")
        lines.append(f"{'Затраты':14s} {_f(est_cost):>8s} {_f(fact_cost):>8s}")
        lines.append(f"{'ЗП менеджер':14s} {'':>8s} {_f(m['zp_manager']):>8s}")
        lines.append(f"{'ЗП монтажник':14s} {'':>8s} {_f(m['zp_installer']):>8s}")
        lines.append(f"{'Сумма счетов':14s} {_f(m['total_amount']):>8s}")
        lines.append("</pre>")

    return "\n".join(lines)


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


def format_estimated_summary(inv: dict[str, Any]) -> str:
    """Краткая сводка расчётных данных для менеджера."""
    amount = float(inv.get("amount") or 0)
    est_glass = float(inv.get("estimated_glass") or 0)
    est_profile = float(inv.get("estimated_profile") or 0)
    est_mat_legacy = float(inv.get("estimated_materials") or 0)
    est_inst = float(inv.get("estimated_installation") or 0)
    est_load = float(inv.get("estimated_loaders") or 0)
    est_log = float(inv.get("estimated_logistics") or 0)
    materials_total = est_glass + est_profile + est_mat_legacy
    est_total = materials_total + est_inst + est_load + est_log

    # НДС с возвратным
    refundable_base = est_glass + est_profile
    output_vat = amount * 22 / 122 if amount > 0 else 0
    input_vat = refundable_base * 22 / 122 if refundable_base > 0 else 0
    net_vat = output_vat - input_vat
    est_profit = amount - est_total - net_vat
    est_pct = (est_profit / amount * 100) if amount > 0 else 0

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


def fmt_project_card(project: dict[str, Any], tz_name: str) -> str:
    """Pretty HTML card for a project dict."""
    # PRJ code removed from display
    title = html.quote(project.get("title") or "—")
    address = html.quote(project.get("address") or "—")
    client = html.quote(project.get("client") or "—")
    status = html.quote(project_status_label(str(project.get("status") or "")))
    amount = project.get("amount")
    amount_s = f"{amount:,.0f}".replace(",", " ") if isinstance(amount, (int, float)) else "—"
    deadline = format_date_iso(project.get("deadline"), tz_name)

    created = format_dt_iso(project.get("created_at"), tz_name)
    updated = format_dt_iso(project.get("updated_at"), tz_name)

    lines = [
        f"<b>Проект</b> — {title}",
        f"📍 Адрес: <b>{address}</b>",
        f"👤 Клиент: <b>{client}</b>",
        f"💰 Сумма: <b>{amount_s}</b>",
        f"🗓 Дедлайн: <b>{deadline}</b>",
        f"📌 Статус: <b>{status}</b>",
        f"🕒 Создан: {created}",
        f"🔄 Обновлён: {updated}",
    ]
    return "\n".join(lines)


def fmt_task_card(task: dict[str, Any], project: dict[str, Any] | None, tz_name: str) -> str:
    tid = task["id"]
    ttype = html.quote(task_type_label(task.get("type")))
    status = html.quote(task_status_label(task.get("status")))
    due = format_dt_iso(task.get("due_at"), tz_name)
    created = format_dt_iso(task.get("created_at"), tz_name)

    header = f"<b>Задача #{tid}</b> — <b>{ttype}</b>"
    if project:
        header += f"\n{fmt_project_card(project, tz_name)}"

    payload = try_json_loads(task.get("payload_json"))
    extra_lines = []
    if payload.get("comment"):
        extra_lines.append(f"📝 Комментарий: {html.quote(str(payload['comment']))}")
    if payload.get("measurements"):
        extra_lines.append(f"📐 Размеры/ТЗ: {html.quote(str(payload['measurements']))}")
    if payload.get("payment_method"):
        extra_lines.append(f"💳 Тип оплаты: <b>{html.quote(str(payload['payment_method']))}</b>")
    if payload.get("payment_type"):
        extra_lines.append(f"🧾 Этап оплаты: <b>{html.quote(str(payload['payment_type']))}</b>")
    if payload.get("payment_amount"):
        extra_lines.append(f"💰 Сумма оплаты: <b>{html.quote(str(payload['payment_amount']))}</b>")
    if payload.get("issue_type"):
        extra_lines.append(f"⚠️ Тип проблемы: <b>{html.quote(str(payload['issue_type']))}</b>")
    if payload.get("doc_type"):
        extra_lines.append(f"📄 Документы: <b>{html.quote(str(payload['doc_type']))}</b>")
    if payload.get("details"):
        extra_lines.append(f"ℹ️ Уточнение: {html.quote(str(payload['details']))}")
    if payload.get("invoice_number"):
        extra_lines.append(f"🧾 № счёта: <b>{html.quote(str(payload['invoice_number']))}</b>")
    if payload.get("sign_type"):
        extra_lines.append(f"✍️ Подписание: <b>{html.quote(str(payload['sign_type']))}</b>")
    if payload.get("material_type"):
        from .enums import MATERIAL_TYPE_LABELS
        _mt = str(payload["material_type"])
        _mt_label = MATERIAL_TYPE_LABELS.get(_mt, _mt)
        extra_lines.append(f"📦 Материал: <b>{html.quote(_mt_label)}</b>")
    if payload.get("supplier"):
        extra_lines.append(f"🏭 Поставщик: <b>{html.quote(str(payload['supplier']))}</b>")
    if payload.get("description"):
        extra_lines.append(f"📋 Описание: {html.quote(str(payload['description']))}")
    if payload.get("address_from"):
        extra_lines.append(f"📍 Откуда: <b>{html.quote(str(payload['address_from']))}</b>")
    if payload.get("address_to"):
        extra_lines.append(f"📍 Куда: <b>{html.quote(str(payload['address_to']))}</b>")
    if payload.get("cargo"):
        extra_lines.append(f"📦 Груз: <b>{html.quote(str(payload['cargo']))}</b>")
    if payload.get("amount") and not payload.get("payment_amount"):
        extra_lines.append(f"💰 Сумма: <b>{html.quote(str(payload['amount']))}</b>")
    if payload.get("linked_invoice_id"):
        extra_lines.append(f"📋 Привязка к счёту: <b>#{payload['linked_invoice_id']}</b>")
    if payload.get("parent_invoice_id"):
        extra_lines.append(f"📋 Объект (счёт): <b>#{payload['parent_invoice_id']}</b>")

    lines = [
        header,
        "",
        f"📌 Статус задачи: <b>{status}</b>",
        f"⏳ Срок: <b>{due}</b>",
        f"🕒 Создана: {created}",
    ]
    if extra_lines:
        lines.append("")
        lines.extend(extra_lines)
    return "\n".join(lines)
