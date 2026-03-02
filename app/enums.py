from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    MANAGER = "manager"
    RP = "rp"  # руководитель проектов
    TD = "td"  # технический директор
    ACCOUNTING = "accounting"
    INSTALLER = "installer"
    GD = "gd"  # генеральный директор
    DRIVER = "driver"  # водитель
    LOADER = "loader"  # грузчик
    TINTER = "tinter"  # тонировщик


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


class TaskStatus(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    REJECTED = "rejected"
