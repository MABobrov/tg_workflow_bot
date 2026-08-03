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


class LeadSourceCb(CallbackData, prefix="leadsrc"):
    lead_id: int
    source: str  # key from SOURCE_OPTIONS


class AdminUsersListCb(CallbackData, prefix="admlist"):
    offset: int = 0


class SummaryCb(CallbackData, prefix="smry"):
    section: str  # inv_pending|inv_inprog|inv_paid|inv_closing|
    #               task_urgent|task_invpay|task_supplpay|
    #               zp_pending|dl_overdue|dl_today|dl_soon
    action: str   # list|back


class RpZpPayCb(CallbackData, prefix="rpzp_pay"):
    """B2 TZ v8 (cart-rework): ГД выплачивает группу счетов одного запроса.

    task_id — идентификатор группы (1 task per ГД), payload содержит invoice_ids.
    """
    task_id: int


class RpZpRejectCb(CallbackData, prefix="rpzp_rej"):
    """B2 TZ v8 (cart-rework): ГД отклоняет группу счетов одного запроса."""
    task_id: int


class RpZpPaySelCb(CallbackData, prefix="rpzp_sel"):
    """ГД: тумблер-выбор счетов к выплате в задаче ЗП РП 10% (частичная выплата).

    action: toggle (вкл/выкл счёт inv_id) | all (выбрать все) | none (снять все)
          | go (перейти к подтверждению выбранных). inv_id=0 для all/none/go.
    """
    task_id: int
    inv_id: int
    action: str  # toggle | all | none | go


class RpZpPayActCb(CallbackData, prefix="rpzp_payact"):
    """ГД: действия на экране подтверждения выплаты ЗП РП 10% (опц. платёжка).

    task_id — задача группы (для race-guard на submit). action: attach (приложить
    платёжку) | submit (выплатить) | cancel.
    """
    task_id: int
    action: str  # attach | submit | cancel


class ZamZpPayCb(CallbackData, prefix="zamzp_pay"):
    """ГД выплачивает пакет замеров одного запроса (ЗП замерщика → леджер).

    Объединение «Оплата замеров» + взаиморасчёты (ТЗ 06.07): выплата ГД работает
    как «ЗП РП» — тумблер-выбор замеров, платёж = Σ выбранных в леджер +
    mark_zamery_paid. task_id — задача ZP_ZAMERY_BATCH (payload содержит zam_ids).
    """
    task_id: int


class ZamZpRejectCb(CallbackData, prefix="zamzp_rej"):
    """ГД отклоняет пакет замеров: замеры возвращаются в «К оплате», task REJECTED."""
    task_id: int


class ZamZpPaySelCb(CallbackData, prefix="zamzp_sel"):
    """ГД: тумблер-выбор замеров к оплате (частичная выплата ЗП замерщика).

    action: toggle (вкл/выкл замер zam_id) | all (выбрать все) | none (снять все)
          | go (перейти к подтверждению). zam_id=0 для all/none/go.
    """
    task_id: int
    zam_id: int
    action: str  # toggle | all | none | go


class ZamZpPayActCb(CallbackData, prefix="zamzp_payact"):
    """ГД: действия на экране подтверждения выплаты ЗП замерщика (опц. платёжка).

    action: attach (приложить платёжку) | submit (выплатить) | cancel.
    """
    task_id: int
    action: str  # attach | submit | cancel


class ZamAttrCb(CallbackData, prefix="zam_attr"):
    """Замерщик отвечает: кто из менеджеров направлял на замер (атрибуция).

    Бот шлёт замерщику карточку-вопрос по каждому замеру без менеджера (UNK),
    он жмёт кнопку. role: kv|npn|kia (проставить менеджера) | unknown («не помню»,
    оставить UNK) | change (вернуть кнопки-выбор). Аналитика — на долг НЕ влияет.
    """
    zam_id: int
    role: str  # kv | npn | kia | unknown | change


class RpSalaryCb(CallbackData, prefix="rp_sal"):
    """B5 TZ v8: операции с окладом РП (66К/мес).

    action: start (выбор РП → экран подтверждения) | attach (приложить опц. платёжку)
          | confirm (выплатить + запись в БК; платёжка опциональна)
    """
    rp_id: int
    action: str  # start | attach | confirm


class RpSalaryRequestCb(CallbackData, prefix="rp_sal_req"):
    """B5 v2 request-based TZ 27.05: РП инициирует запрос оклада через бота.

    action: submit (создать task) | cancel
    """
    action: str  # submit | cancel


class RpSalaryTaskCb(CallbackData, prefix="rp_sal_t"):
    """B5 v2 request-based TZ 27.05: ГД работает с task'ом запроса оклада.

    action: open (открыть для выплаты — переход в RpSalaryPaySG)
          | reject_start (начать отклонение — переход в RpSalaryRejectSG)
    """
    task_id: int
    action: str  # open | reject_start


class RpOkladAdvCb(CallbackData, prefix="rp_okl_adv"):
    """A2: РП переводит оклад (66К/часть) за следующий месяц в кошелёк аванса.

    action: start (открыть выбор) | whole (весь остаток) | part (ввести часть)
          | submit (записать перевод) | cancel
    """
    action: str  # start | whole | part | submit | cancel


class RpOkladRecvCb(CallbackData, prefix="rp_okl_recv"):
    """РП фиксирует ФАКТ получения оклада за ТЕКУЩИЙ месяц (user 2026-06-14).

    Эффект = как ГД-выплата: маркер «Оклад РП …» в «Баланс компании» → месяц
    закрыт (блокирует b5-запрос и перевод-в-аванс). Сумма = остаток (66К − ушедшее
    в аванс). Платёжка опциональна.

    action: attach (приложить платёжку) | submit (зафиксировать) | cancel
    """
    action: str  # attach | submit | cancel
