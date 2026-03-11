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
    parent_invoice = State()   # привязка к родительскому счёту «в работе»
    material_type = State()    # тип материала/услуги
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


class ChatProxySG(StatesGroup):
    """Чат-прокси: ГД ↔ сотрудник/группа."""
    menu = State()              # Подменю чата
    writing = State()           # Ввод текста сообщения
    writing_attachments = State()  # Прикрепление файлов


class BroadcastSG(StatesGroup):
    """Рассылка 'Сообщение Всем'."""
    text = State()              # Ввод текста
    attachments = State()       # Прикрепление файлов
    confirm = State()           # Подтверждение


class GdTaskCreateSG(StatesGroup):
    """Создание задачи от ГД из чат-прокси."""
    pick_installer = State()  # Выбор монтажника (для montazh)
    invoice_pick = State()    # Выбор счёта для привязки
    area_m2 = State()         # Площадь м² (только для montazh)
    description = State()
    deadline = State()
    deadline_time = State()
    attachments = State()


class MontazhCommentSG(StatesGroup):
    """Комментарий к задаче монтажной группы."""
    text = State()


class InvoicePaymentSG(StatesGroup):
    """Счёт на оплату — реакция ГД."""
    viewing = State()           # Просмотр карточки счёта
    attaching_pp = State()      # Прикрепление платёжки


class InvoiceSearchSG(StatesGroup):
    """Поиск счёта по критериям."""
    criteria = State()          # Выбор критерия
    value = State()             # Ввод значения


class InvoiceCreateSG(StatesGroup):
    """Создание счёта на оплату (РП -> ГД)."""
    project = State()
    parent_invoice = State()   # привязка к родительскому счёту «в работе»
    material_type = State()    # тип материала/услуги
    supplier = State()
    amount = State()
    invoice_number = State()
    comment = State()
    urgency = State()          # срочность: 1h / 7h / 24h
    attachments = State()


class NotUrgentGDSG(StatesGroup):
    """Не срочно ГД — задача с пониженным приоритетом."""
    description = State()
    attachments = State()


class SalesWriteSG(StatesGroup):
    """Отд.Продаж — выбор адресата и написание сообщения."""
    pick_target = State()
    invoice_pick = State()    # Выбор счёта для привязки
    writing = State()


class ReplyToGDSG(StatesGroup):
    """Reply from employee to GD via chat-proxy."""
    text = State()


# ======================================================================
# Новые FSM-группы (расширение на все роли)
# ======================================================================

class CheckKpSG(StatesGroup):
    """Менеджер: Проверить КП / Счет — создание счёта в БД."""
    invoice_number = State()     # номер счёта (вводит менеджер)
    address = State()            # адрес установки
    amount = State()             # полная сумма
    documents = State()          # вложения (КП)
    comment = State()            # комментарий


class KpReviewResponseSG(StatesGroup):
    """РП: ответ на запрос «Проверить КП» — формирует пакет документов (legacy)."""
    documents = State()          # вложения (счёт, договор, приложение)
    comment = State()            # комментарий к проверке


class KpReviewSG(StatesGroup):
    """РП: полный flow ответа на CHECK_KP (Этап 5).

    Flow:
    - Да → payment_type → (б/н: documents → comment) / (Кред: comment)
    - Нет → reject_comment
    """
    payment_type = State()       # выбор: б/н или Кред
    documents = State()          # вложения (Счёт, Договор, Приложение) — только б/н
    comment = State()            # комментарий (для «Да»)
    reject_comment = State()     # комментарий (для «Нет»)


class InvoiceStartSG(StatesGroup):
    """Менеджер: Счет в Работу — отправка счёта ГД на оплату."""
    invoice_number = State()     # номер счёта (поиск в БД)
    # Источник клиента (для распределения прибыли)
    client_source = State()      # own (50/50) | gd_lead (75/25 в пользу ГД)
    # Срок по договору
    deadline_days = State()      # кол-во дней → deadline_end_date
    # Расчётные данные (План/Факт)
    estimated_glass = State()        # стекло (с возвратным НДС)
    estimated_profile = State()      # ал. профиль (с возвратным НДС)
    estimated_installation = State()  # расч. стоимость установки
    estimated_loaders = State()       # расч. стоимость грузчиков
    estimated_logistics = State()     # расч. стоимость логистики
    attachments = State()        # счёт, договор, приложение
    # Дополнение 1: проверка ЭДО / бумажных подписей
    edo_check = State()          # ГД: документы подписаны в ЭДО? (да/нет)
    paper_check = State()        # ГД: есть бумажные подписанные? (да/нет)
    originals_holder = State()   # ГД: у кого оригиналы? (gd/manager)


class InvoiceEndSG(StatesGroup):
    """Менеджер/РП: Счет End — инициация закрытия счёта."""
    select_invoice = State()     # выбор счёта из списка
    comment = State()            # пояснение (условие 4, опционально)
    # Дополнение 2: проверка оригиналов закрывающих
    closing_originals = State()  # у кого оригиналы закрывающих? (gd/manager)
    closing_originals_comment = State()  # доп. пояснение


class GdInvoiceEndSG(StatesGroup):
    """ГД: Счет End — финальное решение по закрытию счёта."""
    viewing = State()            # просмотр карточки с условиями


class EdoRequestSG(StatesGroup):
    """Менеджер/РП: запрос ЭДО к бухгалтерии."""
    invoice_pick = State()       # выбор счёта из списка
    request_type = State()       # тип запроса (1-4 inline-кнопки)
    invoice_number = State()     # номер счёта (для типов 1-3)
    description = State()        # пояснение (для типа «Другое»)
    comment = State()            # комментарий
    attachments = State()        # вложения


class EdoResponseSG(StatesGroup):
    """Бухгалтерия: ответ на запрос ЭДО."""
    response_type = State()      # Подписано / Ожидание / Запрос документов
    comment = State()            # комментарий
    attachments = State()        # вложения


class MyInvoicesSG(StatesGroup):
    """Менеджер: Мои Счета — просмотр списка счетов."""
    viewing = State()


class LeadToProjectSG(StatesGroup):
    """РП: Лид в проект — назначение лида менеджеру."""
    pick_manager = State()       # выбор менеджера (КВ / КИА / НПН)
    description = State()        # описание + источник лида
    source = State()             # источник лида
    attachments = State()        # вложения


class RoleSwitchSG(StatesGroup):
    """РП: Смена роли — переключение РП ↔ Менеджер НПН."""
    confirm = State()            # подтверждение переключения


class InstallerInvoiceOkSG(StatesGroup):
    """Монтажник: Счет ОК — подтверждение выполнения работ."""
    select_invoice = State()     # выбор счёта из списка
    comment = State()            # комментарий


class InstallerWorkAcceptSG(StatesGroup):
    """Монтажник: В Работу — принятие задачи."""
    viewing = State()


class InstallerRazmerySG(StatesGroup):
    """Монтажник: бланк размеров стекла + ответ на проверку."""
    select_invoice = State()       # выбор счёта
    comment = State()              # комментарий к бланку
    attachments = State()          # вложения (бланк размеров)
    result_comment = State()       # комментарий к ОК/Ошибке
    result_attachments = State()   # вложения к ОК/Ошибке


class RpRazmerySG(StatesGroup):
    """РП: форма поставщика для монтажника на проверку."""
    comment = State()              # комментарий к форме
    attachments = State()          # вложения (бланк поставщика)


class InstallerOrderMaterialsSG(StatesGroup):
    """Монтажник: Заказ материалов / Заказ доп.материалов → РП."""
    invoice_pick = State()       # выбор счёта для привязки
    description = State()        # описание: что нужно
    comment = State()            # комментарий
    attachments = State()        # фото/документы с размерами


class InstallerDailyReportSG(StatesGroup):
    """Монтажник: Отчёт за день — текстовое сообщение РП."""
    text = State()               # объект, что сделано, проблемы, простой
    attachments = State()        # вложения


class ZameryWorkSG(StatesGroup):
    """Замерщик: работа с замерами."""
    viewing = State()            # просмотр входящих запросов
    responding = State()         # ответ: «ок» + бланк замера
    attachments = State()        # фото, видео, комментарии


class ManagerChatProxySG(StatesGroup):
    """Менеджер/РП: чат-прокси с другими сотрудниками (зеркало для ГД)."""
    menu = State()               # подменю чата
    writing = State()            # ввод сообщения
    writing_attachments = State()  # прикрепление файлов


class ZameryRequestSG(StatesGroup):
    """Менеджер: заявка на замер."""
    source_type = State()        # выбор источника: lead / own_client / repeat
    lead_pick = State()          # выбор лида из списка (только для source=lead)
    address = State()            # адрес замера
    description = State()        # описание работ
    client_contact = State()     # контакт клиента (телефон/имя)
    mkad_km = State()            # расстояние от МКАД в км
    volume_m2 = State()          # примерный объём в м²
    attachments = State()        # вложения (фото, документы)


class ZameryAcceptSG(StatesGroup):
    """Замерщик: принятие заявки с комментарием/календарём."""
    choose_action = State()    # 📅 Дата | 💬 Комментарий | ⏭ Без комментария
    pick_date = State()        # выбор дня (inline-кнопки 7 дней)
    pick_time = State()        # выбор интервала (08-10, 10-12, ...)
    comment = State()          # ввод комментария


class ZameryCompleteSG(StatesGroup):
    """Замерщик: завершение замера — отправка результата менеджеру."""
    attachments = State()      # вложения (фото, видео, документы)
    comment = State()          # комментарий к результату


class ZameryCostEditSG(StatesGroup):
    """Замерщик: редактирование стоимости замера."""
    enter_cost = State()       # ввод новой стоимости


class ZameryZpSG(StatesGroup):
    """Замерщик: Расчёт ЗП — запрос выплаты с указанием стоимости замеров."""
    select_invoice = State()     # выбор счёта / объекта
    cost_per_zamery = State()    # стоимость каждого замера
    all_same_price = State()     # все замеры по одной цене? (да/нет)
    custom_prices = State()      # ввод разных цен
    confirm = State()            # подтверждение + отправка ГД


class InstallerZpSG(StatesGroup):
    """Монтажник: Расчёт ЗП — запрос выплаты после Счет ОК."""
    select_invoice = State()     # выбор счёта с installer_ok=True
    amount = State()             # ввод суммы ЗП
    confirm = State()            # подтверждение


class InstallerZpInitSG(StatesGroup):
    """Монтажник: инициализация ЗП — мульти-выбор счетов с неоплаченной ЗП."""
    selecting = State()


class InstallerMatInitSG(StatesGroup):
    """Монтажник: инициализация «Размеры ОК» — по каким счетам материал заказан."""
    selecting = State()


class ManagerZpSG(StatesGroup):
    """Менеджер: Расчёт ЗП — запрос выплаты после Счет End."""
    select_invoice = State()     # выбор счёта со статусом 'ended'
    amount = State()             # ввод суммы ЗП
    confirm = State()            # подтверждение


class RpSupplierInvoiceSG(StatesGroup):
    """РП: отправить счёт от поставщика ГД (из карточки «Счета в работе»)."""
    attachments = State()        # прикрепить файл(ы) счёта
    comment = State()            # комментарий


class AccRequestToManagerSG(StatesGroup):
    """Бухгалтерия: запрос/задача менеджеру счёта."""
    text = State()               # текст запроса
    attachments = State()        # вложения
