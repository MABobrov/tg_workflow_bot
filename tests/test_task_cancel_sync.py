from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.enums import Role, TaskStatus
from app.handlers import tasks as tasks_module


class _FakeMessage:
    def __init__(self, text: str, user_id: int = 7) -> None:
        self.text = text
        self.from_user = SimpleNamespace(id=user_id)
        self.answers: list[str] = []

    async def answer(self, text: str, reply_markup=None) -> None:
        self.answers.append(text)


class _FakeState:
    def __init__(self, data: dict) -> None:
        self._data = data
        self.cleared = False

    async def get_data(self) -> dict:
        return dict(self._data)

    async def clear(self) -> None:
        self.cleared = True


class _FakeDb:
    def __init__(self, task: dict, project: dict | None = None) -> None:
        self.task = dict(task)
        self.project = project
        self.status_updates: list[tuple] = []

    async def get_task(self, task_id: int) -> dict:
        return dict(self.task)

    async def update_task_status(self, task_id: int, status, expected_statuses=None) -> dict:
        self.status_updates.append((task_id, status, expected_statuses))
        self.task["status"] = status
        return dict(self.task)

    async def get_project(self, project_id: int) -> dict | None:
        return self.project

    async def count_unread_tasks(self, user_id: int) -> int:
        return 0


class _FakeNotifier:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str]] = []

    async def safe_send(self, chat_id: int, text: str, reply_markup=None) -> None:
        self.calls.append((chat_id, text))


class _FakeIntegrations:
    def __init__(self) -> None:
        self.tasks: list[tuple[dict, str]] = []

    async def sync_task(self, task: dict, project_code: str = "") -> None:
        self.tasks.append((task, project_code))


def _task(**overrides) -> dict:
    task = {
        "id": 42,
        "type": "invoice_payment",
        "status": "open",
        "assigned_to": None,
        "created_by": None,
        "project_id": 7,
        "payload_json": None,
    }
    task.update(overrides)
    return task


@pytest.fixture
def handler_env(monkeypatch):
    async def _fake_current_menu(db, user_id):
        return Role.MANAGER_KV, False

    async def _fake_initiator(db, user_id):
        return "Менеджер КВ"

    monkeypatch.setattr(tasks_module, "_current_menu", _fake_current_menu)
    monkeypatch.setattr(tasks_module, "get_initiator_label", _fake_initiator)
    monkeypatch.setattr(tasks_module, "private_only_reply_markup", lambda message, markup: None)
    monkeypatch.setattr(tasks_module, "main_menu", lambda *args, **kwargs: None)


def _run_cancel(task: dict, project: dict | None) -> tuple[_FakeDb, _FakeIntegrations, _FakeState]:
    async def scenario():
        db = _FakeDb(task, project=project)
        integrations = _FakeIntegrations()
        state = _FakeState({"cancel_task_id": task["id"]})
        await tasks_module.task_cancel_with_reason(
            message=_FakeMessage("клиент отказался"),
            state=state,
            db=db,
            config=SimpleNamespace(admin_ids=set()),
            notifier=_FakeNotifier(),
            integrations=integrations,
        )
        return db, integrations, state

    return asyncio.run(scenario())


def test_cancel_with_reason_syncs_task_with_project_code(handler_env) -> None:
    db, integrations, state = _run_cancel(_task(), project={"code": "KV-11"})

    assert db.status_updates == [(42, TaskStatus.REJECTED, ("open", "in_progress"))]
    assert state.cleared is True
    assert len(integrations.tasks) == 1
    synced_task, project_code = integrations.tasks[0]
    assert synced_task["id"] == 42
    assert synced_task["status"] == TaskStatus.REJECTED
    assert project_code == "KV-11"


def test_cancel_with_reason_syncs_task_without_project(handler_env) -> None:
    db, integrations, state = _run_cancel(_task(project_id=None), project=None)

    assert state.cleared is True
    assert len(integrations.tasks) == 1
    assert integrations.tasks[0][1] == ""
