"""Owner 24.11: у чистого монтажника в «Входящих задачах» только
«⏳ Взять в работу» + «🚫 Отмена» — «✅ Завершить»/«❌ Отклонить» убраны
(инцидент Игорь 16.11: жал «Завершить» как отчёт о получении)."""

import json

from app.enums import Role, TaskStatus, TaskType
from app.keyboards import task_actions_kb


def _task(status: TaskStatus, ttype: TaskType = TaskType.GD_TASK) -> dict:
    return {
        "id": 7,
        "type": ttype,
        "status": status,
        "accepted_at": None,
        "payload_json": json.dumps({"invoice_id": 1}),
    }


def _texts(markup) -> list[str]:
    return [b.text for row in markup.inline_keyboard for b in row]


def test_installer_open_only_take_and_cancel():
    kb = task_actions_kb(_task(TaskStatus.OPEN), viewer_role=Role.INSTALLER)
    texts = _texts(kb)
    assert "⏳ Взять в работу" in texts
    assert "🚫 Отмена" in texts
    assert "✅ Завершить" not in texts
    assert "❌ Отклонить" not in texts


def test_installer_in_progress_only_cancel():
    kb = task_actions_kb(_task(TaskStatus.IN_PROGRESS), viewer_role=Role.INSTALLER)
    texts = _texts(kb)
    # Только «Отмена» (+ «Написать» для GD_TASK ниже); дубля «Снять задачу» нет
    assert "🚫 Отмена" in texts
    assert "✅ Завершить" not in texts
    assert "❌ Отклонить" not in texts
    assert "🚫 Снять задачу" not in texts


def test_installer_gd_task_keeps_write_button():
    kb = task_actions_kb(_task(TaskStatus.IN_PROGRESS), viewer_role=Role.INSTALLER)
    assert "✏️ Написать" in _texts(kb)


def test_other_roles_unchanged():
    kb = task_actions_kb(_task(TaskStatus.OPEN), viewer_role=Role.RP)
    texts = _texts(kb)
    assert "✅ Завершить" in texts
    assert "⏳ Взять в работу" in texts
    assert "❌ Отклонить" in texts
    assert "🚫 Отмена" not in texts


def test_gd_with_installer_role_not_restricted():
    # ГД с совмещённой ролью монтажника видит полное меню
    kb = task_actions_kb(_task(TaskStatus.OPEN), viewer_role=f"{Role.GD},{Role.INSTALLER}")
    texts = _texts(kb)
    assert "✅ Завершить" in texts
    assert "🚫 Отмена" not in texts
