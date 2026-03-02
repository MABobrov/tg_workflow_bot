from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class DocsRequestSG(StatesGroup):
    title = State()
    address = State()
    client = State()
    amount = State()
    deadline = State()
    measurements = State()
    comment = State()
    attachments = State()


class QuoteRequestSG(StatesGroup):
    title = State()
    address = State()
    client = State()
    deadline = State()
    measurements = State()
    comment = State()
    attachments = State()


class PaymentReportSG(StatesGroup):
    project = State()
    amount = State()
    payment_method = State()
    payment_type = State()
    payment_date = State()
    comment = State()
    attachments = State()


class ClosingDocsSG(StatesGroup):
    project = State()
    doc_type = State()
    details = State()
    due_date = State()
    comment = State()
    attachments = State()


class IssueSG(StatesGroup):
    project = State()
    issue_type = State()
    description = State()
    attachments = State()


class DailyReportSG(StatesGroup):
    project = State()
    done = State()
    hours = State()
    issues = State()
    attachments = State()


class TaskCompleteSG(StatesGroup):
    attachments = State()


class InstallationDoneSG(StatesGroup):
    project = State()
    end_date = State()
    comment = State()


class ProjectEndSG(StatesGroup):
    project = State()
    invoice_number = State()
    sign_type = State()
    comment = State()


class ManagerInfoRequestSG(StatesGroup):
    manager = State()
    description = State()
    attachments = State()


class UrgentGDSG(StatesGroup):
    description = State()
    attachments = State()


class SearchProjectSG(StatesGroup):
    query = State()


# --- новые FSM-группы ---

class OrderMaterialSG(StatesGroup):
    """Заказ материалов: профиль / стекло / прочее (РП)"""
    project = State()
    material_type = State()   # профиль / стекло / ЛДСП / ГКЛ / сэндвич / нестандарт
    supplier = State()
    description = State()     # спецификация / размеры
    comment = State()
    attachments = State()


class SupplierPaymentSG(StatesGroup):
    """Оплата поставщику (ТД/Сергей)"""
    project = State()
    supplier = State()
    amount = State()
    invoice_number = State()
    comment = State()
    attachments = State()


class DeliveryRequestSG(StatesGroup):
    """Заявка на доставку (РП -> Водитель)"""
    project = State()
    address_from = State()
    address_to = State()
    delivery_date = State()
    cargo_description = State()
    comment = State()


class DeliveryDoneSG(StatesGroup):
    """Доставка выполнена (Водитель -> РП)"""
    project = State()
    comment = State()
    attachments = State()


class TintingRequestSG(StatesGroup):
    """Заявка на тонировку (РП -> Тонировщик)"""
    project = State()
    description = State()
    deadline = State()
    comment = State()
    attachments = State()


class TintingDoneSG(StatesGroup):
    """Тонировка выполнена (Тонировщик -> РП)"""
    project = State()
    comment = State()
    attachments = State()


class AssignLeadSG(StatesGroup):
    """Распределение лида менеджеру (РП)"""
    manager = State()
    description = State()
    comment = State()
