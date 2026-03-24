from __future__ import annotations

from aiogram.filters.callback_data import CallbackData


class ProjectCb(CallbackData, prefix="proj"):
    project_id: int
    ctx: str  # context: payment|closing|issue|report|...
    action: str = "select"


class TaskCb(CallbackData, prefix="task"):
    task_id: int
    action: str  # open|take|done|reject|pay_ok|pay_need|...


class ManagerProjectCb(CallbackData, prefix="mgrproj"):
    project_id: int
    action: str  # open|payment|closing|issue|end|tasks|refresh


class AdminUserCb(CallbackData, prefix="admusr"):
    user_id: int
    action: str  # view|roles_add|roles_remove|block|unblock|tasks_active|tasks_done|tasks_rejected


class AdminRoleCb(CallbackData, prefix="admrole"):
    user_id: int
    action: str  # set|add|remove
    role: str


class LeadCb(CallbackData, prefix="lead"):
    lead_id: int
    action: str  # claim|assign


class LeadAssignCb(CallbackData, prefix="leadassign"):
    lead_id: int
    manager_id: int


class AdminUsersListCb(CallbackData, prefix="admlist"):
    offset: int = 0


class SummaryCb(CallbackData, prefix="smry"):
    section: str  # inv_pending|inv_inprog|inv_paid|inv_closing|
    #               task_urgent|task_invpay|task_supplpay|
    #               zp_pending|dl_overdue|dl_today|dl_soon
    action: str   # list|back
