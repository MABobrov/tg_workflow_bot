from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    MANAGER = "manager"            # устаревшая (для обратной совместимости)
    RP = "rp"                      # руководитель проектов
    TD = "td"                      # технический директор
    ACCOUNTING = "accounting"
    INSTALLER = "installer"
    GD = "gd"                      # генеральный директор
    DRIVER = "driver"              # водитель
    LOADER = "loader"              # грузчик
    TINTER = "tinter"              # тонировщик
    # --- новые роли ---
    MANAGER_KV = "manager_kv"     # менеджер 1 (КВ — конструкции ПВХ)
    MANAGER_KIA = "manager_kia"   # менеджер 2 (КИА — комплектующие)
    MANAGER_NPN = "manager_npn"   # менеджер 3 (НПН — непрофильная номенклатура)
    ZAMERY = "zamery"             # замерщик


# Группы ролей (для общих обработчиков)
MANAGER_ROLES = {Role.MANAGER_KV, Role.MANAGER_KIA, Role.MANAGER_NPN}
SALES_DEPARTMENT = {Role.RP} | MANAGER_ROLES


class ProjectStatus(StrEnum):
    DOCS_REQUEST = "docs_request"          # Запрос документов
    QUOTE_REQUEST = "quote_request"        # Запрос КП
    INVOICE_SENT = "invoice_sent"          # Счет/документы отправлены
    WAITING_PAYMENT = "waiting_payment"    # Ждем оплату
    PAYMENT_REPORTED = "payment_reported"  # Менеджер сообщил об оплате
    IN_WORK = "in_work"                    # Оплата подтверждена -> в работе
    ORDERING = "ordering"                  # Заказ материалов (профиль/стекло/прочее)
    DELIVERY = "delivery"                  # Логистика/доставка
    INSTALLATION = "installation"          # Монтаж профиля/стекла
    TINTING = "tinting"                    # Тонировка
    CLOSING_DOCS = "closing_docs"          # Закрывающие
    ARCHIVE = "archive"                    # Архив


class TaskType(StrEnum):
    DOCS_REQUEST = "docs_request"
    QUOTE_REQUEST = "quote_request"
    PAYMENT_CONFIRM = "payment_confirm"
    CLOSING_DOCS = "closing_docs"
    MANAGER_INFO_REQUEST = "manager_info_request"
    URGENT_GD = "urgent_gd"
    ISSUE = "issue"
    DAILY_REPORT = "daily_report"
    INSTALLATION_DONE = "installation_done"
    PROJECT_END = "project_end"
    # --- новые типы задач ---
    ORDER_PROFILE = "order_profile"          # заказ профиля
    ORDER_GLASS = "order_glass"              # заказ стекла
    ORDER_MATERIALS = "order_materials"      # заказ прочих материалов (ЛДСП, ГКЛ и т.д.)
    SUPPLIER_PAYMENT = "supplier_payment"    # оплата поставщику (ТД -> поставщик)
    DELIVERY_REQUEST = "delivery_request"    # заявка на доставку
    DELIVERY_DONE = "delivery_done"          # доставка выполнена
    TINTING_REQUEST = "tinting_request"      # заявка на тонировку
    TINTING_DONE = "tinting_done"            # тонировка выполнена
    ASSIGN_LEAD = "assign_lead"              # распределение лида менеджеру (РП)
    INVOICE_PAYMENT = "invoice_payment"    # счёт на оплату поставщику (от менеджера к ГД)
    GD_TASK = "gd_task"                    # универсальная задача от ГД
    NOT_URGENT_GD = "not_urgent_gd"        # "Не срочно ГД" (пониженный приоритет)
    # --- новые типы (фаза расширения) ---
    EDO_REQUEST = "edo_request"            # запрос ЭДО (менеджер/РП → бухгалтерия)
    INSTALLER_INVOICE_OK = "installer_ok"  # монтажник — "Счет ОК"
    ZP_CALCULATION = "zp_calculation"      # расчёт ЗП
    LEAD_TO_PROJECT = "lead_to_project"    # лид в проект (РП → менеджер)
    INVOICE_END_REQUEST = "invoice_end"    # запрос "Счет End"
    CHECK_KP = "check_kp"                 # проверить КП / Счет (менеджер → РП)


class TaskStatus(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    REJECTED = "rejected"


class InvoiceStatus(StrEnum):
    """Статусы жизненного цикла счёта."""
    NEW = "new"                     # создан менеджером (этап «Проверить КП»)
    PENDING_PAYMENT = "pending"     # отправлен ГД, ожидает оплаты
    IN_PROGRESS = "in_progress"     # ГД подтвердил, в работе
    PAID = "paid"                   # ГД оплатил (прикрепил платёжку)
    ON_HOLD = "on_hold"             # отложен ГД
    REJECTED = "rejected"           # отклонён ГД
    CLOSING = "closing"             # менеджер инициировал «Счет End», проверка условий
    ENDED = "ended"                 # «Счет End» — финально закрыт


class ZpStatus(StrEnum):
    """Статусы расчёта ЗП по счёту."""
    NOT_REQUESTED = "not_requested"
    REQUESTED = "requested"         # расчёт ЗП отправлен ГД
    APPROVED = "approved"           # ГД подтвердил: «ЗП ок»


class EdoRequestType(StrEnum):
    """Типы запросов ЭДО к бухгалтерии."""
    SIGN_INVOICE = "sign_invoice"           # 1. Подписать по ЭДО (счет №_)
    SIGN_CLOSING = "sign_closing"           # 2. Закрывающие по ЭДО (счет №_)
    SIGN_UPD = "sign_upd"                  # 3. Подписать по ЭДО УПД поставщика
    OTHER = "other"                         # 4. Другое: пояснить суть
