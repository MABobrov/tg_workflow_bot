"""Process commands coming from Google Sheets via webhook.

Supported commands (set in the «Команда боту» column):
  - Напомнить менеджеру     → send reminder to manager
  - Запрос документов       → create DOCS_REQUEST task
  - Запрос КП               → create QUOTE_REQUEST task
  - Подтвердить оплату      → move invoice to IN_WORK
  - В монтаж                → assign installer, create INSTALLATION task
  - Запрос замера            → create ZAMERY_REQUEST task
  - Оплата доставки       → create DELIVERY_REQUEST task
  - Закрыть счёт            → initiate invoice closing

Triggered fields (no command column, just field changes):
  - менеджер changed        → reassign invoice
  - приоритет changed to 🔴  → urgent notification
  - комментарий changed     → forward comment to manager
  - сумма/адрес/сроки       → update DB + notify participants
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from ..db import Database
from ..enums import (
    Role,
    TaskStatus,
    TaskType,
)
from ..services.assignment import resolve_default_assignee
from ..utils import refresh_recipient_keyboard, utcnow, to_iso

log = logging.getLogger(__name__)

# --- Command registry ---

# Map of command label (lowercased, stripped of emoji) → handler function name
_COMMAND_MAP: dict[str, str] = {
    "напомнить менеджеру": "_cmd_remind_manager",
    "запрос документов": "_cmd_docs_request",
    "запрос кп": "_cmd_quote_request",
    "подтвердить оплату": "_cmd_confirm_payment",
    "в монтаж": "_cmd_to_installation",
    "запрос замера": "_cmd_zamery_request",
    "оплата доставки": "_cmd_delivery_request",
    "закрыть счёт": "_cmd_close_invoice",
    # Статистика (только для ГД из листа Общая)
    "сводка": "_cmd_stats_summary",
    "по менеджерам": "_cmd_stats_by_manager",
    "выставленные счета": "_cmd_stats_issued_invoices",
    "зп задолженность": "_cmd_stats_zp_pending",
    "задачи в работе": "_cmd_stats_active_tasks",
    "документооборот": "_cmd_stats_docs",
}


_STATS_COMMANDS = {"сводка", "по менеджерам", "выставленные счета", "зп задолженность", "задачи в работе", "документооборот"}


def _effective_logistics_cost(estimated: object, actual: object) -> float:
    """Use actual logistics once known; otherwise fall back to the estimate."""
    if actual is not None:
        try:
            return float(actual)
        except (TypeError, ValueError):
            pass
    try:
        return float(estimated or 0)
    except (TypeError, ValueError):
        return 0.0


def _clean_command(raw: str) -> str:
    """Strip emoji and whitespace from command string."""
    import re
    # Remove emoji characters
    cleaned = re.sub(
        r'[\U0001F300-\U0001F9FF\U00002600-\U000027BF\U0000FE00-\U0000FE0F'
        r'\U0000200D\U00002702-\U000027B0\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF]+',
        '', raw,
    )
    return cleaned.strip().lower()


async def process_sheet_webhook(
    data: dict[str, Any],
    db: Database,
    config: Any,
    notifier: Any,
    sheets_service: Any | None = None,
) -> dict[str, Any]:
    """Main entry point for processing webhook from Google Sheets.

    data payload structure:
    {
        "type": "command" | "field_change" | "data_sync",
        "invoice_number": "...",
        "command": "Напомнить менеджеру",  # if type=command
        "source": "op" | "general",         # which sheet triggered
        "changed_fields": {...},             # if type=field_change
        "row": [...],                        # if type=data_sync (full row values)
    }

    Returns: {"status": "ok", "action": "...", "details": "..."}
    """
    event_type = data.get("type", "data_sync")
    inv_num = str(data.get("invoice_number", "")).strip()

    if event_type == "command":
        # Statistics commands don't require invoice_number
        raw_cmd = str(data.get("command", ""))
        cmd_key = _clean_command(raw_cmd)
        if cmd_key in _STATS_COMMANDS:
            return await _handle_stats_command(cmd_key, data, db, config, notifier)
        if not inv_num:
            return {"status": "error", "detail": "invoice_number is required"}
        return await _handle_command(data, inv_num, db, config, notifier)
    elif event_type == "field_change":
        return await _handle_field_change(data, inv_num, db, config, notifier)
    elif event_type == "data_sync":
        return await _handle_data_sync(data, inv_num, db, sheets_service)
    elif event_type == "search":
        return await _handle_search(data, db, sheets_service)
    else:
        return {"status": "error", "detail": f"unknown type: {event_type}"}


# --- Command handler ---

async def _handle_command(
    data: dict[str, Any],
    inv_num: str,
    db: Database,
    config: Any,
    notifier: Any,
) -> dict[str, Any]:
    raw_cmd = str(data.get("command", ""))
    cmd_key = _clean_command(raw_cmd)

    handler_name = _COMMAND_MAP.get(cmd_key)
    if not handler_name:
        return {"status": "error", "detail": f"unknown command: {raw_cmd}"}

    invoice = await db.get_invoice_by_number(inv_num)
    if not invoice:
        return {"status": "error", "detail": f"invoice {inv_num} not found"}

    handler_fn = globals()[handler_name]
    source = data.get("source", "op")
    return await handler_fn(invoice, db, config, notifier, source, data)


# --- Individual command handlers ---

async def _cmd_remind_manager(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Send reminder notification to the manager assigned to the invoice."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return {"status": "error", "detail": "no manager assigned"}

    inv_num = invoice["invoice_number"]
    address = invoice.get("object_address") or "—"
    amount = invoice.get("amount") or "—"

    text = (
        f"📩 <b>Напоминание по счёту {inv_num}</b>\n"
        f"📍 {address}\n"
        f"💰 Сумма: {amount}\n\n"
        f"⚡ Отправлено из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    ok = await notifier.safe_send(int(manager_id), text)
    if ok:
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return {"status": "ok", "action": "remind_manager", "sent": ok}


async def _cmd_docs_request(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Create DOCS_REQUEST task for the manager."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return {"status": "error", "detail": "no manager assigned"}

    # Determine who created the command (RP for OP sheet, GD for General)
    creator_id = None
    if source == "op":
        creator_id = await resolve_default_assignee(db, config, Role.RP)
    else:
        creator_id = await resolve_default_assignee(db, config, Role.GD)

    task = await db.create_task(
        project_id=None,
        type_=TaskType.DOCS_REQUEST,
        status=TaskStatus.OPEN,
        created_by=creator_id,
        assigned_to=int(manager_id),
        due_at_iso=to_iso(utcnow() + timedelta(hours=24)),
        payload={
            "invoice_number": invoice["invoice_number"],
            "invoice_id": invoice["id"],
            "source": f"sheets_{source}",
            "assigned_role": invoice.get("creator_role"),
        },
    )
    text = (
        f"📋 <b>Запрос документов</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"📍 {invoice.get('object_address') or '—'}\n\n"
        f"⚡ Создано из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return {"status": "ok", "action": "docs_request", "task_id": task["id"]}


async def _cmd_quote_request(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Create QUOTE_REQUEST task for the manager."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return {"status": "error", "detail": "no manager assigned"}

    creator_id = None
    if source == "op":
        creator_id = await resolve_default_assignee(db, config, Role.RP)
    else:
        creator_id = await resolve_default_assignee(db, config, Role.GD)

    task = await db.create_task(
        project_id=None,
        type_=TaskType.QUOTE_REQUEST,
        status=TaskStatus.OPEN,
        created_by=creator_id,
        assigned_to=int(manager_id),
        due_at_iso=to_iso(utcnow() + timedelta(hours=24)),
        payload={
            "invoice_number": invoice["invoice_number"],
            "invoice_id": invoice["id"],
            "source": f"sheets_{source}",
            "assigned_role": invoice.get("creator_role"),
        },
    )
    text = (
        f"📊 <b>Запрос КП</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"📍 {invoice.get('object_address') or '—'}\n\n"
        f"⚡ Создано из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return {"status": "ok", "action": "quote_request", "task_id": task["id"]}


async def _cmd_confirm_payment(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Move invoice to IN_WORK status, notify manager."""
    from ..enums import InvoiceStatus

    inv_id = int(invoice["id"])
    await db.update_invoice_status(inv_id, InvoiceStatus.IN_WORK)

    manager_id = invoice.get("created_by")
    if manager_id:
        text = (
            f"💰 <b>Оплата подтверждена</b>\n"
            f"Счёт: {invoice['invoice_number']}\n"
            f"Статус: ✅ В работе\n\n"
            f"⚡ Подтверждено из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(int(manager_id), text)
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return {"status": "ok", "action": "confirm_payment", "invoice_id": inv_id}


async def _cmd_to_installation(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Assign installer and create installation task."""
    from ..enums import MontazhStage

    inv_id = int(invoice["id"])
    installer_id = await resolve_default_assignee(db, config, Role.INSTALLER)

    if installer_id:
        await db.update_invoice(inv_id, montazh_stage=MontazhStage.IN_WORK)

        creator_id = None
        if source == "op":
            creator_id = await resolve_default_assignee(db, config, Role.RP)
        else:
            creator_id = await resolve_default_assignee(db, config, Role.GD)

        task = await db.create_task(
            project_id=None,
            type_=TaskType.INSTALLATION_DONE,
            status=TaskStatus.OPEN,
            created_by=creator_id,
            assigned_to=installer_id,
            due_at_iso=to_iso(utcnow() + timedelta(hours=48)),
            payload={
                "invoice_number": invoice["invoice_number"],
                "invoice_id": inv_id,
                "object_address": invoice.get("object_address") or "",
                "source": f"sheets_{source}",
            },
        )

        text = (
            f"🔨 <b>Назначен монтаж</b>\n"
            f"Счёт: {invoice['invoice_number']}\n"
            f"📍 {invoice.get('object_address') or '—'}\n\n"
            f"⚡ Назначено из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(installer_id, text)
        await refresh_recipient_keyboard(notifier, db, config, installer_id)
        return {"status": "ok", "action": "to_installation", "task_id": task["id"]}

    return {"status": "error", "detail": "no installer configured"}


async def _cmd_zamery_request(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Create zamery (measurement) request task."""
    zamery_id = await resolve_default_assignee(db, config, Role.ZAMERY)
    if not zamery_id:
        return {"status": "error", "detail": "no zamery user configured"}

    creator_id = None
    if source == "op":
        creator_id = await resolve_default_assignee(db, config, Role.RP)
    else:
        creator_id = await resolve_default_assignee(db, config, Role.GD)

    task = await db.create_task(
        project_id=None,
        type_=TaskType.ZAMERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=creator_id,
        assigned_to=zamery_id,
        due_at_iso=to_iso(utcnow() + timedelta(hours=24)),
        payload={
            "invoice_number": invoice["invoice_number"],
            "invoice_id": invoice["id"],
            "object_address": invoice.get("object_address") or "",
            "client_name": invoice.get("client_name") or "",
            "source": f"sheets_{source}",
        },
    )

    text = (
        f"📐 <b>Запрос замера</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"📍 {invoice.get('object_address') or '—'}\n"
        f"👤 {invoice.get('client_name') or '—'}\n\n"
        f"⚡ Создано из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(zamery_id, text)
    await refresh_recipient_keyboard(notifier, db, config, zamery_id)
    return {"status": "ok", "action": "zamery_request", "task_id": task["id"]}


async def _cmd_delivery_request(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Create delivery payment task for GD."""
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if not gd_id:
        return {"status": "error", "detail": "no GD configured"}

    creator_id = None
    if source == "op":
        creator_id = await resolve_default_assignee(db, config, Role.RP)
    else:
        creator_id = gd_id

    task = await db.create_task(
        project_id=None,
        type_=TaskType.DELIVERY_REQUEST,
        status=TaskStatus.OPEN,
        created_by=creator_id,
        assigned_to=gd_id,
        due_at_iso=to_iso(utcnow() + timedelta(hours=24)),
        payload={
            "invoice_number": invoice["invoice_number"],
            "invoice_id": invoice["id"],
            "object_address": invoice.get("object_address") or "",
            "estimated_logistics": invoice.get("estimated_logistics"),
            "source": f"sheets_{source}",
        },
    )

    est_log = invoice.get("estimated_logistics") or "—"
    text = (
        f"🚚 <b>Оплата доставки</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"📍 {invoice.get('object_address') or '—'}\n"
        f"🚚 Расч. логистика: {est_log}\n\n"
        f"⚡ Создано из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(gd_id, text)
    await refresh_recipient_keyboard(notifier, db, config, gd_id)
    return {"status": "ok", "action": "delivery_request", "task_id": task["id"]}


async def _cmd_close_invoice(
    invoice: dict, db: Database, config: Any, notifier: Any,
    source: str, data: dict,
) -> dict[str, Any]:
    """Initiate invoice closing (set status to CLOSING)."""
    from ..enums import InvoiceStatus

    inv_id = int(invoice["id"])
    await db.update_invoice_status(inv_id, InvoiceStatus.CLOSING)

    # Notify manager
    manager_id = invoice.get("created_by")
    if manager_id:
        text = (
            f"🏁 <b>Закрытие счёта инициировано</b>\n"
            f"Счёт: {invoice['invoice_number']}\n"
            f"Статус: 🏁 Закрытие\n\n"
            f"⚡ Инициировано из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(int(manager_id), text)
        await refresh_recipient_keyboard(notifier, db, config, int(manager_id))

    # Notify GD
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id and gd_id != (manager_id or 0):
        text_gd = (
            f"🏁 <b>Счёт {invoice['invoice_number']} → закрытие</b>\n"
            f"📍 {invoice.get('object_address') or '—'}\n"
            f"💰 Сумма: {invoice.get('amount') or '—'}\n\n"
            f"⚡ Инициировано из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(gd_id, text_gd)

    return {"status": "ok", "action": "close_invoice", "invoice_id": inv_id}


# --- Field change handlers ---

async def _handle_field_change(
    data: dict[str, Any],
    inv_num: str,
    db: Database,
    config: Any,
    notifier: Any,
) -> dict[str, Any]:
    """Process field changes from the sheet."""
    invoice = await db.get_invoice_by_number(inv_num)
    if not invoice:
        return {"status": "error", "detail": f"invoice {inv_num} not found"}

    changed = data.get("changed_fields", {})
    source = data.get("source", "op")
    actions: list[str] = []

    # --- Manager reassignment ---
    new_manager = changed.get("manager")
    if new_manager:
        result = await _field_reassign_manager(invoice, new_manager, db, config, notifier, source)
        if result:
            actions.append(result)

    # --- Priority change ---
    new_priority = changed.get("priority")
    if new_priority and "🔴" in str(new_priority):
        result = await _field_urgent_priority(invoice, db, config, notifier, source)
        if result:
            actions.append(result)

    # --- Comment from RP/GD ---
    new_comment = changed.get("comment")
    if new_comment:
        result = await _field_comment(invoice, str(new_comment), db, config, notifier, source)
        if result:
            actions.append(result)

    # --- Amount changed ---
    new_amount = changed.get("amount")
    if new_amount is not None:
        old_amount = invoice.get("amount") or 0
        await db.update_invoice(int(invoice["id"]), amount=new_amount)
        result = await _field_amount_changed(
            invoice, old_amount, new_amount, db, config, notifier, source,
        )
        if result:
            actions.append(result)

    # --- Address changed ---
    new_address = changed.get("object_address")
    if new_address:
        await db.update_invoice(int(invoice["id"]), object_address=new_address)
        result = await _field_address_changed(
            invoice, new_address, db, config, notifier, source,
        )
        if result:
            actions.append(result)

    # --- Outstanding debt changed ---
    new_debt = changed.get("outstanding_debt")
    if new_debt is not None:
        old_debt = invoice.get("outstanding_debt") or 0
        try:
            new_debt_val = float(str(new_debt).replace(",", ".").replace("\xa0", "").replace(" ", "") or 0)
        except (TypeError, ValueError):
            new_debt_val = 0
        await db.update_invoice(int(invoice["id"]), outstanding_debt=new_debt_val)
        result = await _field_debt_changed(
            invoice, old_debt, new_debt_val, db, config, notifier, source,
        )
        if result:
            actions.append(result)

    # --- Materials fact (OP) changed ---
    new_mat_fact = changed.get("materials_fact_op")
    if new_mat_fact is not None:
        try:
            new_mat_val = float(str(new_mat_fact).replace(",", ".").replace("\xa0", "").replace(" ", "") or 0)
        except (TypeError, ValueError):
            new_mat_val = 0
        await db.update_invoice(int(invoice["id"]), materials_fact_op=new_mat_val)
        actions.append("materials_fact_op_updated")

    # --- Montazh fact (OP) changed ---
    new_mont_fact = changed.get("montazh_fact_op")
    if new_mont_fact is not None:
        try:
            new_mont_val = float(str(new_mont_fact).replace(",", ".").replace("\xa0", "").replace(" ", "") or 0)
        except (TypeError, ValueError):
            new_mont_val = 0
        await db.update_invoice(int(invoice["id"]), montazh_fact_op=new_mont_val)
        actions.append("montazh_fact_op_updated")

    # --- Deadline changed ---
    new_deadline = changed.get("deadline_days")
    if new_deadline is not None:
        await db.import_invoice_from_sheet({
            "invoice_number": inv_num,
            "deadline_days": int(new_deadline),
        })
        result = await _field_deadline_changed(
            invoice, int(new_deadline), db, config, notifier, source,
        )
        if result:
            actions.append(result)

    if not actions:
        return {"status": "ok", "action": "no_changes"}
    return {"status": "ok", "actions": actions}


async def _field_reassign_manager(
    invoice: dict, new_manager_marker: str, db: Database, config: Any,
    notifier: Any, source: str,
) -> str | None:
    """Reassign invoice to a different manager by marker (КВ/КИА/НПН)."""
    marker = str(new_manager_marker).strip().upper()
    role_map = {"КВ": Role.MANAGER_KV, "КИА": Role.MANAGER_KIA, "НПН": Role.MANAGER_NPN}
    role = role_map.get(marker)
    if not role:
        return None

    new_id = await resolve_default_assignee(db, config, role)
    if not new_id:
        return None

    old_manager_id = invoice.get("created_by")
    inv_id = int(invoice["id"])
    await db.update_invoice(inv_id, created_by=new_id, creator_role=str(role))

    # Notify new manager
    text = (
        f"📋 <b>Вам назначен счёт {invoice['invoice_number']}</b>\n"
        f"📍 {invoice.get('object_address') or '—'}\n"
        f"💰 {invoice.get('amount') or '—'}\n\n"
        f"⚡ Назначено из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(new_id, text)
    await refresh_recipient_keyboard(notifier, db, config, new_id)

    # Notify old manager
    if old_manager_id and int(old_manager_id) != new_id:
        text_old = (
            f"📋 Счёт {invoice['invoice_number']} переназначен менеджеру {marker}.\n"
            f"⚡ Изменение из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(int(old_manager_id), text_old)

    return f"reassigned_to_{marker}"


async def _field_urgent_priority(
    invoice: dict, db: Database, config: Any, notifier: Any, source: str,
) -> str | None:
    """Send urgent notification to manager when priority set to red."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return None

    text = (
        f"🔴 <b>СРОЧНО: счёт {invoice['invoice_number']}</b>\n"
        f"📍 {invoice.get('object_address') or '—'}\n"
        f"💰 {invoice.get('amount') or '—'}\n\n"
        f"⚡ Приоритет установлен из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return "urgent_priority"


async def _field_comment(
    invoice: dict, comment: str, db: Database, config: Any,
    notifier: Any, source: str,
) -> str | None:
    """Forward comment to manager."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return None

    sender = "РП" if source == "op" else "ГД"
    text = (
        f"💬 <b>Комментарий от {sender}</b>\n"
        f"Счёт: {invoice['invoice_number']}\n\n"
        f"{comment}\n\n"
        f"⚡ Из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return "comment_sent"


async def _field_amount_changed(
    invoice: dict, old_amount: Any, new_amount: Any, db: Database,
    config: Any, notifier: Any, source: str,
) -> str | None:
    """Notify manager about amount change."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return None

    text = (
        f"💰 <b>Сумма изменена</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"Было: {old_amount} → Стало: {new_amount}\n\n"
        f"⚡ Изменено из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    return "amount_changed"


async def _field_address_changed(
    invoice: dict, new_address: str, db: Database, config: Any,
    notifier: Any, source: str,
) -> str | None:
    """Notify participants about address change."""
    inv_num = invoice["invoice_number"]
    notified: list[int] = []

    # Notify manager
    manager_id = invoice.get("created_by")
    if manager_id:
        text = (
            f"📍 <b>Адрес изменён</b>\n"
            f"Счёт: {inv_num}\n"
            f"Новый адрес: {new_address}\n\n"
            f"⚡ Изменено из таблицы {'ОП' if source == 'op' else 'Общая'}"
        )
        await notifier.safe_send(int(manager_id), text)
        notified.append(int(manager_id))

    # Notify installer if assigned
    installer_id = invoice.get("assigned_to")
    if installer_id and int(installer_id) not in notified:
        text_inst = (
            f"📍 <b>Адрес изменён</b>\n"
            f"Счёт: {inv_num}\n"
            f"Новый адрес: {new_address}\n\n"
            f"⚡ Изменено из таблицы"
        )
        await notifier.safe_send(int(installer_id), text_inst)

    return "address_changed"


async def _field_deadline_changed(
    invoice: dict, new_days: int, db: Database, config: Any,
    notifier: Any, source: str,
) -> str | None:
    """Notify manager about deadline change."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return None

    text = (
        f"⏰ <b>Сроки изменены</b>\n"
        f"Счёт: {invoice['invoice_number']}\n"
        f"Новый срок: {new_days} дней\n\n"
        f"⚡ Изменено из таблицы {'ОП' if source == 'op' else 'Общая'}"
    )
    await notifier.safe_send(int(manager_id), text)
    return "deadline_changed"


async def _field_debt_changed(
    invoice: dict, old_debt: Any, new_debt: float, db: Database, config: Any,
    notifier: Any, source: str,
) -> str | None:
    """Notify manager when outstanding debt changes."""
    manager_id = invoice.get("created_by")
    if not manager_id:
        return None
    try:
        old_val = float(old_debt or 0)
    except (TypeError, ValueError):
        old_val = 0
    if abs(old_val - new_debt) < 1:
        return None  # insignificant change

    inv_num = invoice.get("invoice_number", "?")
    address = invoice.get("object_address") or "—"

    if new_debt == 0 and old_val > 0:
        text = (
            f"✅ <b>Долг погашен!</b>\n"
            f"Счёт: <b>{inv_num}</b>\n"
            f"📍 {address}\n"
            f"Было: {old_val:,.0f}₽ → Долг: <b>0₽</b>"
        )
    elif new_debt > old_val:
        text = (
            f"📈 <b>Долг увеличился</b>\n"
            f"Счёт: <b>{inv_num}</b>\n"
            f"📍 {address}\n"
            f"Было: {old_val:,.0f}₽ → Стало: <b>{new_debt:,.0f}₽</b>"
        )
    else:
        text = (
            f"💰 <b>Изменение долга</b>\n"
            f"Счёт: <b>{inv_num}</b>\n"
            f"📍 {address}\n"
            f"Было: {old_val:,.0f}₽ → Стало: <b>{new_debt:,.0f}₽</b>"
        )

    await notifier.safe_send(int(manager_id), text)
    await refresh_recipient_keyboard(notifier, db, config, int(manager_id))
    return "debt_changed"


# --- Search from Органайзер ---

async def _handle_search(
    data: dict[str, Any],
    db: Database,
    sheets_service: Any | None,
) -> dict[str, Any]:
    """Search invoice by query and write result back to the sheet."""
    query = str(data.get("query", "")).strip()
    if not query:
        return {"status": "error", "detail": "empty query"}

    sheet_name = data.get("sheet", "Органайзер")
    result_row = data.get("result_row")
    result_col = data.get("result_col")

    # Search by exact invoice number first
    invoice = await db.get_invoice_by_number(query)
    if invoice:
        result = _format_search_result(invoice)
    else:
        # Fuzzy search by number or address
        invoices = await db.search_invoices(query, limit=3)
        if invoices:
            result = " | ".join(_format_search_result(inv) for inv in invoices)
        else:
            result = f"Не найдено: {query}"

    # Write result back to the sheet
    if sheets_service and result_row and result_col:
        try:
            await sheets_service.write_cell_to_sheet(
                sheet_name, int(result_row), int(result_col), result,
            )
        except Exception:
            log.warning("Failed to write search result to %s R%sC%s", sheet_name, result_row, result_col)

    return {"status": "ok", "action": "search", "result": result[:200]}


def _format_search_result(inv: dict[str, Any]) -> str:
    """Format invoice for search result (compact, for sheet cell)."""
    num = inv.get("invoice_number", "?")
    addr = inv.get("object_address") or "—"
    amount = inv.get("amount")
    status = inv.get("status") or "—"
    debt = inv.get("outstanding_debt")

    parts = [f"№{num}", addr]
    if amount:
        parts.append(f"{float(amount):,.0f}р")
    if debt and float(debt) > 0:
        parts.append(f"долг:{float(debt):,.0f}р")
    parts.append(f"[{status}]")
    return " · ".join(parts)


# --- Data sync (full row import) ---

async def _handle_data_sync(
    data: dict[str, Any],
    inv_num: str,
    db: Database,
    sheets_service: Any | None,
) -> dict[str, Any]:
    """Import a single row of data from the sheet into the bot DB."""
    row_values = data.get("row")
    if row_values and sheets_service:
        parsed = sheets_service.parse_op_row_from_webhook(row_values)
        if parsed:
            inv_id = await db.import_invoice_from_sheet(parsed)
            return {"status": "ok", "action": "data_sync", "invoice_id": inv_id}

    # Fallback: import from changed_fields directly
    fields = data.get("fields", {})
    if fields:
        fields["invoice_number"] = inv_num
        inv_id = await db.import_invoice_from_sheet(fields)
        return {"status": "ok", "action": "data_sync", "invoice_id": inv_id}

    return {"status": "ok", "action": "data_sync", "detail": "no data to import"}


# --- Statistics commands (GD from "Общая" sheet) ---

async def _handle_stats_command(
    cmd_key: str,
    data: dict[str, Any],
    db: Database,
    config: Any,
    notifier: Any,
) -> dict[str, Any]:
    """Dispatch statistics command."""
    handler_name = _COMMAND_MAP.get(cmd_key)
    if not handler_name:
        return {"status": "error", "detail": f"unknown stats command: {cmd_key}"}
    handler_fn = globals()[handler_name]
    return await handler_fn(db, config, notifier, data)


async def _cmd_stats_summary(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """General summary: invoices by status, revenue, debts, costs."""
    # Счета по статусам
    all_inv = await db.conn.execute(
        "SELECT status, COUNT(*) as cnt, "
        "  COALESCE(SUM(amount), 0) as total, "
        "  COALESCE(SUM(outstanding_debt), 0) as debt "
        "FROM invoices WHERE parent_invoice_id IS NULL GROUP BY status"
    )
    rows = await all_inv.fetchall()
    status_lines = []
    total_count = 0
    total_revenue = 0.0
    total_debt = 0.0
    credit_debt = 0.0
    regular_debt = 0.0
    for r in rows:
        st = r["status"] or "—"
        cnt = r["cnt"]
        total = r["total"]
        debt = r["debt"]
        total_count += cnt
        total_revenue += total
        total_debt += debt
        if st == "credit":
            credit_debt += debt
        else:
            regular_debt += debt
        label = {"new": "Новые", "in_work": "В работе", "closing": "Закрытие",
                 "ended": "Закрыты", "credit": "Кредит"}.get(st, st)
        debt_tag = f" | долг: {debt:,.0f}₽" if debt > 0 else ""
        status_lines.append(f"  {label}: {cnt} ({total:,.0f}₽{debt_tag})")

    # Необходимые затраты: расч. стоимость, заменяя фактической по мере оплаты
    cost_cur = await db.conn.execute(
        "SELECT i.id, i.amount, "
        "  COALESCE(i.estimated_materials, 0) + COALESCE(i.estimated_glass, 0) "
        "    + COALESCE(i.estimated_profile, 0) as est_mat, "
        "  COALESCE(i.estimated_installation, 0) as est_inst, "
        "  COALESCE(i.estimated_loaders, 0) as est_load, "
        "  i.estimated_logistics, i.actual_logistics "
        "FROM invoices i "
        "WHERE i.parent_invoice_id IS NULL AND i.status IN ('new', 'in_work', 'closing')"
    )
    cost_rows = await cost_cur.fetchall()

    total_est_cost = 0.0
    total_fact_paid = 0.0
    for cr in cost_rows:
        inv_id = cr["id"]
        est_log = _effective_logistics_cost(cr["estimated_logistics"], cr["actual_logistics"])
        est = cr["est_mat"] + cr["est_inst"] + cr["est_load"] + est_log
        total_est_cost += est
        # Фактически оплачено (дочерние счета + supplier payments)
        child_cur = await db.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) as paid FROM invoices "
            "WHERE parent_invoice_id = ?", (inv_id,)
        )
        child_row = await child_cur.fetchone()
        total_fact_paid += child_row["paid"] if child_row else 0

    remaining_cost = max(0, total_est_cost - total_fact_paid)

    # Активные задачи
    tasks_cur = await db.conn.execute(
        "SELECT COUNT(*) as cnt FROM tasks WHERE status IN ('open', 'in_progress')"
    )
    tasks_row = await tasks_cur.fetchone()
    active_tasks = tasks_row["cnt"] if tasks_row else 0

    text = (
        f"📊 <b>Сводка</b>\n\n"
        f"📄 Счетов: {total_count} (сумма: {total_revenue:,.0f}₽)\n"
        + "\n".join(status_lines) + "\n\n"
        f"💳 <b>Долги:</b>\n"
        f"  Обычные: {regular_debt:,.0f}₽\n"
        f"  Кредитные: {credit_debt:,.0f}₽\n"
        f"  Итого: {total_debt:,.0f}₽\n\n"
        f"🏭 <b>Затраты (активные счета):</b>\n"
        f"  Расчётные: {total_est_cost:,.0f}₽\n"
        f"  Оплачено: {total_fact_paid:,.0f}₽\n"
        f"  Осталось оплатить: {remaining_cost:,.0f}₽\n\n"
        f"📋 Активных задач: {active_tasks}"
    )
    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_summary"}


async def _cmd_stats_by_manager(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """Per-manager breakdown: invoices, tasks, debts, doc statuses."""
    cur = await db.conn.execute(
        "SELECT u.telegram_id, u.full_name, u.role, "
        "  COUNT(i.id) as inv_count, "
        "  COALESCE(SUM(i.amount), 0) as total, "
        "  SUM(CASE WHEN i.status = 'in_work' THEN 1 ELSE 0 END) as in_work, "
        "  SUM(CASE WHEN i.status = 'ended' THEN 1 ELSE 0 END) as ended, "
        "  COALESCE(SUM(i.outstanding_debt), 0) as debt, "
        "  SUM(CASE WHEN i.docs_edo_signed = 1 THEN 1 ELSE 0 END) as prim_edo, "
        "  SUM(CASE WHEN i.edo_signed = 1 THEN 1 ELSE 0 END) as clos_edo, "
        "  SUM(CASE WHEN i.status IN ('in_work','closing') "
        "       AND (i.docs_edo_signed = 0 OR i.edo_signed = 0 "
        "            OR COALESCE(i.docs_originals_holder,'') = '' "
        "            OR COALESCE(i.closing_originals_holder,'') = '') "
        "       THEN 1 ELSE 0 END) as docs_missing "
        "FROM invoices i "
        "JOIN users u ON i.created_by = u.telegram_id "
        "WHERE i.parent_invoice_id IS NULL "
        "GROUP BY i.created_by "
        "ORDER BY total DESC"
    )
    rows = await cur.fetchall()
    if not rows:
        text = "📊 <b>По менеджерам</b>\n\nНет данных."
    else:
        lines = []
        for r in rows:
            name = r["full_name"] or "—"
            role_short = {"manager_kv": "КВ", "manager_kia": "КИА",
                          "manager_npn": "НПН"}.get(r["role"] or "", "")
            mgr_id = r["telegram_id"]
            # Open tasks for this manager
            tasks_cur = await db.conn.execute(
                "SELECT COUNT(*) as cnt FROM tasks "
                "WHERE assigned_to = ? AND status IN ('open', 'in_progress')",
                (mgr_id,),
            )
            tasks_row = await tasks_cur.fetchone()
            open_tasks = tasks_row["cnt"] if tasks_row else 0

            debt_tag = f"\n  💳 Долг: {r['debt']:,.0f}₽" if r["debt"] > 0 else ""
            docs_tag = ""
            if r["docs_missing"] > 0:
                docs_tag = f"\n  📋 Док: ✅П-эдо:{r['prim_edo']} ✅З-эдо:{r['clos_edo']} | ❌неполных: {r['docs_missing']}"

            lines.append(
                f"👤 <b>{name}</b> ({role_short})\n"
                f"  Счетов: {r['inv_count']} | В работе: {r['in_work']} | Закрыто: {r['ended']}\n"
                f"  Сумма: {r['total']:,.0f}₽{debt_tag}\n"
                f"  📋 Задач в работе: {open_tasks}{docs_tag}"
            )
        text = "📊 <b>По менеджерам</b>\n\n" + "\n\n".join(lines)

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_by_manager"}


async def _cmd_stats_issued_invoices(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """Issued invoices: RP sends primary docs to managers, fixation stats."""
    # Invoices where docs have been issued (docs_edo_signed or docs_originals_holder set)
    cur = await db.conn.execute(
        "SELECT i.invoice_number, i.object_address, i.amount, i.status, "
        "  i.docs_edo_signed, i.docs_originals_holder, "
        "  i.edo_signed, i.closing_originals_holder, "
        "  u.full_name as mgr_name, u.role as mgr_role "
        "FROM invoices i "
        "LEFT JOIN users u ON i.created_by = u.telegram_id "
        "WHERE i.parent_invoice_id IS NULL "
        "  AND i.status IN ('in_work', 'closing') "
        "ORDER BY i.invoice_number "
        "LIMIT 50"
    )
    rows = await cur.fetchall()

    # Per-manager stats
    mgr_stats: dict[str, dict[str, int]] = {}
    total = len(rows)
    total_prim_edo = 0
    total_clos_edo = 0
    total_prim_orig = 0
    total_clos_orig = 0

    for r in rows:
        mgr = r["mgr_name"] or "—"
        role_short = {"manager_kv": "КВ", "manager_kia": "КИА",
                      "manager_npn": "НПН"}.get(r["mgr_role"] or "", "")
        key = f"{mgr} ({role_short})" if role_short else mgr
        if key not in mgr_stats:
            mgr_stats[key] = {"total": 0, "prim_edo": 0, "clos_edo": 0,
                              "prim_orig": 0, "clos_orig": 0}
        mgr_stats[key]["total"] += 1
        if r["docs_edo_signed"]:
            mgr_stats[key]["prim_edo"] += 1
            total_prim_edo += 1
        if r["edo_signed"]:
            mgr_stats[key]["clos_edo"] += 1
            total_clos_edo += 1
        if r["docs_originals_holder"]:
            mgr_stats[key]["prim_orig"] += 1
            total_prim_orig += 1
        if r["closing_originals_holder"]:
            mgr_stats[key]["clos_orig"] += 1
            total_clos_orig += 1

    text = (
        f"📊 <b>Выставленные счета</b> ({total})\n\n"
        f"<b>Общая статистика:</b>\n"
        f"  П-ЭДО подписано: {total_prim_edo}/{total}\n"
        f"  З-ЭДО подписано: {total_clos_edo}/{total}\n"
        f"  П-оригиналы зафиксированы: {total_prim_orig}/{total}\n"
        f"  З-оригиналы зафиксированы: {total_clos_orig}/{total}\n"
    )

    if mgr_stats:
        text += "\n<b>По менеджерам:</b>\n"
        for mgr_key, st in mgr_stats.items():
            text += (
                f"\n👤 <b>{mgr_key}</b> — {st['total']} счетов\n"
                f"  П: ✅эдо {st['prim_edo']} ✅ориг {st['prim_orig']} "
                f"| З: ✅эдо {st['clos_edo']} ✅ориг {st['clos_orig']}\n"
            )

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_issued_invoices"}


async def _cmd_stats_zp_pending(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """Pending salary (ZP): installer, manager, zamerschik, RP."""
    # Монтажник + Менеджер + Замерщик (из таблицы invoices)
    cur = await db.conn.execute(
        "SELECT i.invoice_number, i.object_address, i.amount, "
        "  i.zp_installer_status, i.zp_installer_amount, "
        "  i.zp_manager_status, i.zp_manager_amount, "
        "  i.zp_status, i.zp_zamery_total, "
        "  i.estimated_materials, i.estimated_glass, i.estimated_profile, "
        "  i.estimated_installation, i.estimated_loaders, "
        "  i.estimated_logistics, i.actual_logistics, "
        "  i.client_source, i.profit_tax, i.nds_amount "
        "FROM invoices i "
        "WHERE i.parent_invoice_id IS NULL "
        "  AND (i.zp_installer_status = 'requested' "
        "       OR i.zp_manager_status = 'requested' "
        "       OR i.zp_status = 'requested') "
        "ORDER BY i.invoice_number"
    )
    rows = await cur.fetchall()

    lines = []
    total_inst = 0.0
    total_mgr = 0.0
    total_zam = 0.0
    total_rp = 0.0

    for r in rows:
        parts = []
        if r["zp_installer_status"] == "requested":
            amt = r["zp_installer_amount"] or 0
            parts.append(f"Монтажник: {amt:,.0f}₽")
            total_inst += amt
        if r["zp_manager_status"] == "requested":
            amt = r["zp_manager_amount"] or 0
            parts.append(f"Менеджер: {amt:,.0f}₽")
            total_mgr += amt
        if r["zp_status"] == "requested":
            amt = r["zp_zamery_total"] or 0
            parts.append(f"Замерщик: {amt:,.0f}₽")
            total_zam += amt

        # РП: 10% от прибыли с вычетом налогов
        amount = r["amount"] or 0
        est_mat = (r["estimated_materials"] or 0) + (r["estimated_glass"] or 0) + (r["estimated_profile"] or 0)
        est_inst = r["estimated_installation"] or 0
        est_load = r["estimated_loaders"] or 0
        est_log = _effective_logistics_cost(r["estimated_logistics"], r["actual_logistics"])
        est_total = est_mat + est_inst + est_load + est_log
        # НДС: 22/122 (выход - вход)
        refundable = est_mat + est_log
        output_vat = amount * 22 / 122 if amount > 0 else 0
        input_vat = refundable * 22 / 122 if refundable > 0 else 0
        net_vat = output_vat - input_vat
        est_profit = amount - est_total - net_vat
        rp_zp = est_profit * 0.10 if est_profit > 0 else 0
        if rp_zp > 0:
            parts.append(f"РП(10%): {rp_zp:,.0f}₽")
            total_rp += rp_zp

        if parts:
            lines.append(
                f"📄 <b>{r['invoice_number']}</b> — {r['object_address'] or '—'}\n"
                f"  {' | '.join(parts)}"
            )

    total_all = total_inst + total_mgr + total_zam + total_rp

    if not lines:
        text = "📊 <b>ЗП задолженность</b>\n\n✅ Нет ожидающих запросов ЗП."
    else:
        summary = (
            f"📊 <b>ЗП задолженность</b> ({len(lines)} счетов)\n"
            f"💰 Итого к выплате: {total_all:,.0f}₽\n"
            f"  🔧 Монтажник: {total_inst:,.0f}₽\n"
            f"  👤 Менеджер: {total_mgr:,.0f}₽\n"
            f"  📐 Замерщик: {total_zam:,.0f}₽\n"
            f"  📋 РП (10%): {total_rp:,.0f}₽\n\n"
        )
        text = summary + "\n\n".join(lines)

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_zp_pending"}


async def _cmd_stats_active_tasks(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """Active tasks breakdown by type and assignee."""
    cur = await db.conn.execute(
        "SELECT t.type, t.status, u.full_name as assignee_name, COUNT(*) as cnt "
        "FROM tasks t "
        "LEFT JOIN users u ON t.assigned_to = u.telegram_id "
        "WHERE t.status IN ('open', 'in_progress') "
        "GROUP BY t.type, t.status, t.assigned_to "
        "ORDER BY cnt DESC"
    )
    rows = await cur.fetchall()
    if not rows:
        text = "📊 <b>Задачи в работе</b>\n\n✅ Активных задач нет."
    else:
        from ..utils import task_type_label
        # Group by type
        by_type: dict[str, list[str]] = {}
        type_counts: dict[str, int] = {}
        for r in rows:
            ttype = r["type"] or "—"
            label = task_type_label(ttype)
            if label not in by_type:
                by_type[label] = []
                type_counts[label] = 0
            type_counts[label] += r["cnt"]
            status_icon = "🟡" if r["status"] == "open" else "🔵"
            by_type[label].append(
                f"  {status_icon} {r['assignee_name'] or '—'}: {r['cnt']}"
            )
        total = sum(type_counts.values())
        lines = []
        for label, entries in by_type.items():
            lines.append(f"📋 <b>{label}</b> ({type_counts[label]})")
            lines.extend(entries)
        text = f"📊 <b>Задачи в работе</b> (всего: {total})\n\n" + "\n".join(lines)

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_active_tasks"}


async def _cmd_stats_docs(
    db: Database, config: Any, notifier: Any, data: dict,
) -> dict[str, Any]:
    """Document workflow status: EDO, originals, pending requests."""
    # EDO status for active invoices
    cur = await db.conn.execute(
        "SELECT "
        "  COUNT(*) as total, "
        "  SUM(CASE WHEN docs_edo_signed = 1 THEN 1 ELSE 0 END) as prim_edo_ok, "
        "  SUM(CASE WHEN edo_signed = 1 THEN 1 ELSE 0 END) as clos_edo_ok, "
        "  SUM(CASE WHEN docs_originals_holder = 'gd' THEN 1 ELSE 0 END) as prim_orig_gd, "
        "  SUM(CASE WHEN docs_originals_holder = 'manager' THEN 1 ELSE 0 END) as prim_orig_mgr, "
        "  SUM(CASE WHEN closing_originals_holder = 'gd' THEN 1 ELSE 0 END) as clos_orig_gd, "
        "  SUM(CASE WHEN closing_originals_holder = 'manager' THEN 1 ELSE 0 END) as clos_orig_mgr "
        "FROM invoices "
        "WHERE parent_invoice_id IS NULL AND status IN ('in_work', 'closing')"
    )
    doc_row = await cur.fetchone()

    total = doc_row["total"] if doc_row else 0
    prim_edo = doc_row["prim_edo_ok"] if doc_row else 0
    clos_edo = doc_row["clos_edo_ok"] if doc_row else 0
    prim_orig_gd = doc_row["prim_orig_gd"] if doc_row else 0
    prim_orig_mgr = doc_row["prim_orig_mgr"] if doc_row else 0
    clos_orig_gd = doc_row["clos_orig_gd"] if doc_row else 0
    clos_orig_mgr = doc_row["clos_orig_mgr"] if doc_row else 0
    prim_orig_none = total - prim_orig_gd - prim_orig_mgr
    clos_orig_none = total - clos_orig_gd - clos_orig_mgr

    # Pending EDO_REQUEST tasks
    edo_cur = await db.conn.execute(
        "SELECT COUNT(*) as cnt FROM tasks "
        "WHERE type = 'edo_request' AND status IN ('open', 'in_progress')"
    )
    edo_row = await edo_cur.fetchone()
    pending_edo_tasks = edo_row["cnt"] if edo_row else 0

    # Invoices missing documents
    missing_cur = await db.conn.execute(
        "SELECT invoice_number, object_address, "
        "  docs_edo_signed, edo_signed, "
        "  docs_originals_holder, closing_originals_holder "
        "FROM invoices "
        "WHERE parent_invoice_id IS NULL "
        "  AND status IN ('in_work', 'closing') "
        "  AND (docs_edo_signed = 0 OR edo_signed = 0 "
        "       OR docs_originals_holder IS NULL OR docs_originals_holder = '' "
        "       OR closing_originals_holder IS NULL OR closing_originals_holder = '') "
        "ORDER BY invoice_number "
        "LIMIT 20"
    )
    missing_rows = await missing_cur.fetchall()

    text = (
        f"📊 <b>Документооборот</b>\n"
        f"Активных счетов: {total}\n\n"
        f"<b>ЭДО (электронный):</b>\n"
        f"  Первичные подписаны: {prim_edo}/{total}\n"
        f"  Закрывающие подписаны: {clos_edo}/{total}\n\n"
        f"<b>Оригиналы первичных:</b>\n"
        f"  У ГД: {prim_orig_gd} | У менеджера: {prim_orig_mgr} | Нет: {prim_orig_none}\n\n"
        f"<b>Оригиналы закрывающих:</b>\n"
        f"  У ГД: {clos_orig_gd} | У менеджера: {clos_orig_mgr} | Нет: {clos_orig_none}\n\n"
        f"📨 Открытых запросов ЭДО: {pending_edo_tasks}\n"
    )

    if missing_rows:
        text += f"\n<b>Неполные документы ({len(missing_rows)}):</b>\n"
        for r in missing_rows:
            prim = "✅" if r["docs_edo_signed"] else "❌"
            clos = "✅" if r["edo_signed"] else "❌"
            po = {"gd": "ГД", "manager": "Мен"}.get(r["docs_originals_holder"] or "", "—")
            co = {"gd": "ГД", "manager": "Мен"}.get(r["closing_originals_holder"] or "", "—")
            text += f"  📄 {r['invoice_number']} — П:{prim}эдо {po}ориг | З:{clos}эдо {co}ориг\n"

    gd_id = await resolve_default_assignee(db, config, Role.GD)
    if gd_id:
        await notifier.safe_send(gd_id, text)
    return {"status": "ok", "action": "stats_docs"}
