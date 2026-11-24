"""Owner 24.11: подтверждение получения убрано по всему боту — везде осталась
одна кнопка «⏳ Взять в работу», которая сама ставит «получена + в работе».
Счёт на оплату (ГД) и оплата доставки (ГД): надпись «Взять в работу» вместо
«✅ Принять»/«✅ Принял», действие то же (одним тапом)."""

import json

from app.enums import Role, TaskStatus, TaskType
from app.keyboards import task_actions_kb


def _task(ttype: TaskType, status: TaskStatus) -> dict:
    return {
        "id": 9,
        "type": ttype,
        "status": status,
        "accepted_at": None,
        "payload_json": json.dumps({"invoice_id": 1}),
    }


def _texts(markup) -> list[str]:
    return [b.text for row in markup.inline_keyboard for b in row]


def test_invoice_payment_open_single_take_button():
    kb = task_actions_kb(_task(TaskType.INVOICE_PAYMENT, TaskStatus.OPEN), viewer_role=Role.GD)
    texts = _texts(kb)
    # У ГД спец-надпись «📥 Беру, оплачу позже» — это и есть «взять в работу»
    # (одним тапом: получена + в работе), не отдельное подтверждение
    assert any("Беру" in t for t in texts)
    assert "✅ Принять" not in texts
    assert "✅ Принято" not in texts


def test_invoice_payment_gd_take_txt_preserved():
    # Спец-надпись ГД «Беру, оплачу позже» не затёрта и осталась единственной «взять»-кнопкой
    kb = task_actions_kb(_task(TaskType.INVOICE_PAYMENT, TaskStatus.OPEN), viewer_role=Role.GD)
    texts = _texts(kb)
    assert sum(1 for t in texts if "Беру" in t) == 1


def test_delivery_request_open_single_take_button():
    kb = task_actions_kb(_task(TaskType.DELIVERY_REQUEST, TaskStatus.OPEN), viewer_role=Role.GD)
    texts = _texts(kb)
    assert "⏳ Взять в работу" in texts
    assert "✅ Принял" not in texts
    assert "✅ Принято" not in texts


def test_no_receipt_confirm_buttons_anywhere():
    # Ни один тип задачи не показывает кнопку подтверждения получения
    for ttype in (TaskType.GD_TASK, TaskType.INVOICE_PAYMENT, TaskType.DELIVERY_REQUEST):
        for status in (TaskStatus.OPEN, TaskStatus.IN_PROGRESS):
            for role in (Role.GD, Role.RP, Role.INSTALLER, Role.ACCOUNTING):
                texts = _texts(task_actions_kb(_task(ttype, status), viewer_role=role))
                for t in texts:
                    assert "Принято" not in t, (ttype, status, role, t)
                    assert "Подтвердить получение" not in t, (ttype, status, role, t)
