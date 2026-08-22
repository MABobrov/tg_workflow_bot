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
    """Оплата доставки (РП -> ГД)"""
    invoice = State()        # выбор счёта
    comment = State()        # комментарий
    attachments = State()    # вложения (фото/pdf/excel)


class DeliveryPaymentSG(StatesGroup):
    """Оплата доставки — ответ ГД (платёжка + сумма)."""
    task_id = State()       # placeholder
    amount = State()        # фактическая стоимость доставки
    comment = State()       # комментарий ГД
    attachments = State()   # платёжка (PDF)


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


class GdTaskCreateSG(StatesGroup):
    """Создание задачи от ГД из чат-прокси."""
    pick_installer = State()  # Выбор монтажника (для montazh)
    pick_manager = State()    # Выбор конкретного менеджера КВ/КИА/НПН (для otd_prodazh)
    invoice_pick = State()    # Выбор счёта для привязки
    area_m2 = State()         # Площадь м² (только для montazh)
    description = State()
    deadline = State()
    deadline_time = State()
    attachments = State()


class SelfReminderSG(StatesGroup):
    """Самонапоминалка: роль (менеджер) ставит себе задачу-напоминание.

    text → when (inline-пресеты / «своя дата») → custom_date → custom_time.
    """
    text = State()
    when = State()
    custom_date = State()
    custom_time = State()


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
    credit_type = State()      # тип оплаты: б/н (0) или кредит (1)
    credit_route = State()     # §E: кредит → «на оплату к ГД / к КВ·КИА·НПН»
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
    """Менеджер: Проверить КП / Счет — отправка КП на проверку РП."""
    lead_pick = State()          # шаг 1: выбор лида или «Новый клиент»
    # --- Только для нового клиента ---
    client_name = State()        # контрагент
    address = State()            # адрес установки
    # --- Обе ветки ---
    amount = State()             # полная сумма
    credit_type = State()        # тип клиента: б/н (0) или кредит (1)
    # --- Только для нового клиента ---
    payment_type = State()       # тип оплаты (100%, рассрочка и т.д.)
    deadline_days = State()      # срок по договору (дни)
    # --- Обе ветки ---
    documents = State()          # вложения (КП файлы)
    comment = State()            # комментарий


class KpReviewSG(StatesGroup):
    """РП: полный flow ответа на CHECK_KP.

    Flow:
    - Да → payment_type → (б/н: documents → invoice_number → comment) / (Кред: invoice_number → comment)
    - Нет → reject_comment
    """
    payment_type = State()       # выбор: б/н или Кред
    documents = State()          # вложения (Счёт, Договор, Приложение) — только б/н
    invoice_number = State()     # РП вводит номер счёта (после документов)
    comment = State()            # комментарий (для «Да»)
    reject_comment = State()     # комментарий (для «Нет»)


class InvoiceStartSG(StatesGroup):
    """Менеджер: Счет в Работу — отправка счёта ГД на оплату."""
    invoice_number = State()     # номер счёта (поиск в БД)
    # Источник клиента (для распределения прибыли)
    client_source = State()      # own (50/50) | gd_lead (75/25 в пользу ГД)
    # Данные счёта (owner 2026-07-25)
    receipt_date = State()       # дата счёта → invoices.receipt_date (порядок строк листа)
    first_payment = State()      # сумма первого платежа → invoices.first_payment_amount (P)
    # Срок по договору
    deadline_days = State()      # кол-во дней → deadline_end_date (от даты счёта)
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


class LeadToProjectSG(StatesGroup):
    """РП: Лид в проект — назначение лида менеджеру."""
    pick_manager = State()       # выбор менеджера (КВ / КИА / НПН)
    name = State()               # имя клиента
    phone = State()              # телефон клиента
    address = State()            # адрес объекта
    source = State()             # источник лида
    attachments = State()        # вложения


class InstallerInvoiceOkSG(StatesGroup):
    """Монтажник: Счет ОК — подтверждение выполнения работ + согласование цены."""
    select_invoice = State()     # выбор счёта из списка
    price_input = State()        # ввод новой суммы (если изменить)
    comment = State()            # комментарий


class InstallerWorkAcceptSG(StatesGroup):
    """Монтажник: В Работу — принятие задачи + согласование цены."""
    viewing = State()
    price_input = State()   # ввод новой суммы (если монтажник не согласен)


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


class ManagerChatProxySG(StatesGroup):
    """Менеджер/РП: чат-прокси с другими сотрудниками (зеркало для ГД)."""
    menu = State()               # подменю чата
    writing = State()            # ввод сообщения
    writing_attachments = State()  # прикрепление файлов


class InvoiceChatSG(StatesGroup):
    """Чат менеджер↔монтажник привязанный к счёту."""
    writing = State()            # ввод сообщения


class ZameryRequestSG(StatesGroup):
    """Менеджер: заявка на замер."""
    source_type = State()        # выбор источника: lead / own_client / repeat
    lead_pick = State()          # выбор лида из списка (только для source=lead)
    address = State()            # адрес замера
    description = State()        # описание работ
    client_contact = State()     # контакт клиента (телефон/имя)
    mkad_km = State()            # расстояние от МКАД в км
    pick_schedule_date = State() # менеджер выбирает дату из графика замерщика
    pick_schedule_time = State() # менеджер выбирает интервал
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


class ZameryBlackoutSG(StatesGroup):
    """Замерщик: добавление дней 'Не ставить замер'."""
    pick_dates = State()       # выбор дат для blackout


class ZameryQuickBookSG(StatesGroup):
    """Запись замера из графика (замерщик) — полный сбор данных."""
    enter_address = State()
    enter_description = State()
    enter_client_contact = State()
    enter_mkad_km = State()
    enter_volume = State()
    attachments = State()


class ZameryZpSG(StatesGroup):
    """Замерщик: Расчёт ЗП — запрос выплаты с указанием стоимости замеров."""
    select_invoice = State()     # выбор счёта / объекта
    cost_per_zamery = State()    # стоимость каждого замера
    all_same_price = State()     # все замеры по одной цене? (да/нет)
    custom_prices = State()      # ввод разных цен
    confirm = State()            # подтверждение + отправка ГД


class ZamerySettlementPaySG(StatesGroup):
    """ГД: добавление платежа в леджер взаиморасчётов с замерщиком."""
    enter_amount = State()       # сумма платежа
    enter_date = State()         # дата платежа (ДД.ММ.ГГГГ или 'сегодня')
    enter_comment = State()      # комментарий (опц.)


class InstallerZpSG(StatesGroup):
    """Монтажник: Расчёт ЗП — запрос выплаты после Счет ОК."""
    select_invoice = State()     # выбор счёта с installer_ok=True
    amount = State()             # ввод суммы ЗП
    confirm = State()            # подтверждение


class InstallerZpInitSG(StatesGroup):
    """Монтажник: инициализация ЗП — мульти-выбор счетов с неоплаченной ЗП."""
    selecting = State()


class GdZpPaymentSG(StatesGroup):
    """ГД: отправка платёжки по ЗП монтажника."""
    waiting_pdf = State()      # ожидание PDF/фото платёжки


class GdZpInstAdjustSG(StatesGroup):
    """ГД: изменение суммы ЗП монтажника (аналог InstallerZpAdjustSG)."""
    amount = State()        # ввод новой суммы (₽)
    confirm = State()       # подтверждение


class InstallerZpAdjustSG(StatesGroup):
    """Монтажник: корректировка стоимости монтажа из 'Ожидает расчёт'."""
    comment = State()        # почему? (обязательно)
    attachments = State()    # фото/видео (можно пропустить)
    mode = State()           # ➕ добавить к расч. / 🔄 заменить сумму
    amount = State()         # сумма ₽
    confirm = State()        # подтверждение


class InstallerMatInitSG(StatesGroup):
    """Монтажник: инициализация «Размеры ОК» — по каким счетам материал заказан."""
    selecting = State()


class ManagerZpSG(StatesGroup):
    """Менеджер: Расчёт ЗП — запрос выплаты после Счет End."""
    select_invoice = State()     # выбор счёта со статусом 'ended'
    amount = State()             # ввод суммы ЗП
    confirm = State()            # подтверждение


class FinalPaymentEtaSG(StatesGroup):
    """Менеджер: ввод ориентировочной даты финального платежа по счёту с долгом.

    ТЗ 14.06: при «Счёт ОК»/«Счёт End» с непогашенным долгом по материнскому
    счёту менеджеру приходит задача → он указывает дату → бот пишет её в
    invoices.planned_final_payment_date (+ колонка листа) и уведомляет ГД 1×.
    """
    date_input = State()         # ввод даты ДД.ММ.ГГГГ


class RpZpRequestSG(StatesGroup):
    """РП: Запрос ЗП РП (10% от прибыли счёта) — cart-style мульти-выбор.

    Flow: «📂 Еще» → «💰 Запрос ЗП РП» → list_invoices (cart со списком ended-счетов
    где ЗП не выплачена/не запрошена/не забрана в аванс: rp_payout_op=0, rp_request_op=0,
    rp_payout_advance_at пусто) → enter_amount (для текущего счёта) → list_invoices
    (выбранные подсвечены ✓ + сумма) → «📤 Отправить» → confirm (✅ Да / ❌ Нет) →
    массовый UPDATE invoices.rp_request_op + sync + ONE task per ГД с
    payload {invoice_ids: [...], total: ...} → notify ГД с pre-card списка +
    общая сумма + кнопки Выплатить / Отклонить.
    """
    list_invoices = State()      # cart: список ended-счетов + итог
    enter_amount = State()       # ввод суммы для выбранного счёта
    confirm = State()            # ✅ Да / ❌ Нет (финальное подтверждение)


class RpAdvanceFillSG(StatesGroup):
    """РП: наполнение кошелька аванса незабранной ЗП (10% НПН) по счетам.

    Flow: хаб «Запрос ЗП» → «💵 Аванс» → list_invoices (toggle-чекбоксы счетов
    где npn_amount(AP)>0 И ЗП не забрана/не выплачена/не запрошена: rp_payout_advance_at
    пусто, rp_payout_op/AR=0, rp_request_op/AQ=0; сумма по счёту фиксирована = AP,
    ввода суммы НЕТ) → confirm (✅ Да / ❌ Нет) → db.credit_rp_zp_to_advance: по счетам
    durable-метка rp_payout_advance_at:=now (НЕ rp_payout_op/AR — импорт-безопасно,
    07.06; одна ЗП один раз) + ОДИН topup кошелька (wallet_role='rp') →
    sync_invoice_row + инфо-уведомление ГД (ТЗ advance-wallet-fill 30.05).
    """
    list_invoices = State()      # toggle-список счетов с незабранной ЗП
    confirm = State()            # ✅ Да / ❌ Нет (финальное подтверждение)


class RpOkladToAdvanceSG(StatesGroup):
    """РП: перевод оклада (66К/мес или часть) за СЛЕДУЮЩИЙ месяц в кошелёк аванса (A2).

    Flow: экран наполнения аванса → «💼 Оклад {след.месяц} → аванс» → choose
    (весь остаток / ввести часть) → [enter_amount если часть] → confirm (✅/❌) →
    db.credit_rp_oklad_to_advance: op_company_entries('ЗП РП Нижельченко', сумма) +
    ОДИН topup кошелька (wallet_role='rp') + sync «Баланс компании» + инфо ГД.
    Взаимоисключение «один оклад в месяц» с ГД-выплатой (td.py rp_salary_confirm).
    """
    choose = State()         # весь остаток / ввести часть
    enter_amount = State()   # ввод суммы (ветка «часть»)
    confirm = State()        # ✅ Да / ❌ Нет


class RpOkladReceivedSG(StatesGroup):
    """РП: фиксация ФАКТА получения оклада за ТЕКУЩИЙ месяц (user 2026-06-14).

    Flow: хаб «Запрос ЗП» → «✅ Оклад получен» → confirm (показ месяц+остаток,
    кнопки «✅ Зафиксировать» / «📎 С платёжкой» / «✖️ Отмена») →
    [attach_receipt если выбрана платёжка: приём фото/документа → назад в confirm] →
    submit (под @money_confirm_guard) → db.record_rp_oklad_received:
    op_company_entries('Оклад РП {name} {YYYY-MM}', остаток) — как ГД-выплата, БЕЗ
    topup кошелька → месяц закрыт + sync «Баланс компании» + инфо ГД (+опц. платёжка).
    Сумма = остаток (66К − ушедшее в аванс): итог по месяцу = 66К без двойного учёта.
    Взаимоисключение «один оклад в месяц» с ГД-выплатой и переводом-в-аванс.
    """
    confirm = State()         # ✅ Зафиксировать / 📎 С платёжкой / ✖️ Отмена
    attach_receipt = State()  # приём фото/документа платёжки (опционально)


class RpAdvDistributeSG(StatesGroup):
    """РП: распределить (зачесть) кошелёк аванса в ЗП-10% (npn) счёта (user 2026-06-13).

    Аналог менеджерского «Распределить аванс», но с НЕМЕДЛЕННЫМ зачётом — у РП нет
    шага «ЗП по счёту одобрена». Flow: хаб «Запрос ЗП» → «💰 Распределить аванс»
    (whitelist + свободный аванс>0) → select_invoice (счета с непокрытой 10%-ЗП:
    npn>0, не выплачено/запрошено ГД, не забрано в аванс, нет rp-offset; остаток
    10% = npn − уже зачтённое) → enter_amount (≤ свободный аванс и ≤ остаток 10%) →
    confirm → db.apply_rp_advance_to_invoice_now: сразу ЗАКРЫТЫЙ offset-item
    (баланс аванса −= сумма) + sync счёта/журнала + инфо ГД. Наличие rp-offset
    исключает счёт из налива и ГД-выплаты (одна 10%-ЗП учитывается один раз).
    """
    select_invoice = State()
    enter_amount = State()
    confirm = State()


class RpSalaryPaySG(StatesGroup):
    """ГД: Выплата оклада РП (66К/мес) — с ОПЦИОНАЛЬНОЙ платёжкой.

    Flow (confirm-first, как RpOkladReceivedSG): «💼 Оклад {имя_РП} 66К» (или task) →
    confirm (✅ Выплатить без платёжки / 📎 С платёжкой / ❌ Отмена) → [attach_receipt
    если выбрана платёжка: PDF/фото/документ → назад в confirm] → confirm (@money_confirm_guard) →
    INSERT op_company_entries(cashless_amount=66000, description='Оклад РП {name} {YYYY-MM}')
    → sync «Баланс компании» + audit + notify РП (pre-card + платёжка вложением, если была).
    """
    attach_receipt = State()     # фото/документ платёжки (опционально)
    confirm = State()            # ✅ Выплатить без платёжки / 📎 С платёжкой / ❌ Отмена


class RpZpPaySG(StatesGroup):
    """ГД: Выплата ЗП РП 10% «ЗП от завершённых счетов» — с ОПЦИОНАЛЬНОЙ платёжкой.

    Flow: ГД-задача (RpZpPayCb «✅ Выплатить») → confirm (✅ Выплатить без платёжки /
    📎 С платёжкой / ❌ Отмена) → [attach_receipt если выбрана платёжка: фото/документ
    → назад в confirm] → submit (@money_confirm_guard): per-invoice rp_payout_op/AR +
    rp_payout_date_op/AS + sync_invoice_row + close group tasks + notify РП (+опц.
    платёжка вложением). Расход счёта учитывается в invoices AR/AS (НЕ в op_company_entries).

    select: тумблер-выбор счетов к выплате (частичная выплата — ГД платит подмножество,
    снятые счета остаются открытым запросом; задача закрывается лишь когда все выплачены).
    """
    select = State()             # ✓/▫️ выбор счетов к выплате (частичная выплата)
    confirm = State()            # ✅ Выплатить без платёжки / 📎 С платёжкой / ❌ Отмена
    attach_receipt = State()     # фото/документ платёжки (опционально)


class ZamZpPaySG(StatesGroup):
    """ГД: Выплата ЗП замерщика (объединение «Оплата замеров» + леджер, ТЗ 06.07).

    Копия RpZpPaySG. Flow: ГД-задача (ZamZpPayCb «✅ Выплатить») → select (тумблер-
    выбор замеров, по умолчанию все, уже оплаченные отфильтрованы) → confirm (✅
    Выплатить без платёжки / 📎 С платёжкой / ❌ Отмена) → [attach_receipt если
    выбрана платёжка] → submit (@money_confirm_guard): mark_zamery_paid(sel) +
    add_zamery_settlement_entry('payment', Σsel) → долг −Σ; невыбранные остаются
    открытым запросом; task DONE когда всё оплачено. Платежи в леджере (НЕ invoices).
    """
    select = State()             # ✓/▫️ выбор замеров к оплате (частичная выплата)
    confirm = State()            # ✅ Выплатить без платёжки / 📎 С платёжкой / ❌ Отмена
    attach_receipt = State()     # фото/документ платёжки (опционально)


class RpSupplierInvoiceSG(StatesGroup):
    """РП: отправить счёт от поставщика ГД (из карточки «Счета в работе»)."""
    amount = State()             # сумма счёта
    material_type = State()      # тип материала/услуги
    attachments = State()        # прикрепить файл(ы) счёта
    comment = State()            # комментарий


class RpInvCancelSG(StatesGroup):
    """РП: снять свой счёт на оплату с ГД — ввод причины (owner 20.08).

    ⚠️ СВОЙ state, а НЕ общий `TaskCancelReasonSG` — причина техническая, не
    вкусовая: `tasks.py:699` это `@router.message(TaskCancelReasonSG.reason)`,
    и переиспользуй мы тот же state, чужой хендлер перехватил бы ввод и завершил
    отмену по-своему — без `sync_task` (его там нет вовсе) и с главным меню
    вместо дашборда «Счета на оплату».
    """
    reason = State()             # текст причины (уходит ГД)


class RpMontazhAssignSG(StatesGroup):
    """РП: назначить счёт монтажнику с вложениями."""
    attachments = State()        # сбор файлов (photo/document/video/text)


class RpMontazhNaemSG(StatesGroup):
    """РП: наёмная монт. группа (2️⃣) — ввод согласованной суммы ЗП монтажа."""
    amount = State()             # ввод согласованной суммы (₽)


class RpMontazhRegroupSG(StatesGroup):
    """РП: смена монт. группы на счёте в работе — ввод новой согласованной суммы."""
    amount = State()             # ввод согласованной суммы (₽)


class RpMontazhZpSG(StatesGroup):
    """РП: «✏️ Внести сумму ЗП монтаж» после назначения группы (ТЗ owner 16.07)."""
    amount = State()             # ввод суммы ЗП (₽)


class AccRequestToManagerSG(StatesGroup):
    """Бухгалтерия: запрос/задача менеджеру счёта."""
    text = State()               # текст запроса
    attachments = State()        # вложения


class AccDocCommentSG(StatesGroup):
    """Бухгалтерия: ввод комментария/статуса для документов."""
    waiting_text = State()       # ожидание текста


class AccQuestionSG(StatesGroup):
    """Бухгалтерия: вопрос инициатору задачи."""
    text = State()               # текст вопроса
    attachments = State()        # вложения (фото/документ)


class TaskCancelReasonSG(StatesGroup):
    """#33: Отмена задачи с причиной (после подтверждения получателем)."""
    reason = State()             # текст причины


class OpAddSG(StatesGroup):
    """ГД: добавление операционного расхода компании (op_company_entries).

    Flow: тип → сумма → (НДС или направление займа) → описание → confirm.
    Запись идёт в БД op_company_entries (НЕ в «Импорт ОП» — read-only лист).
    После confirm: sync «Баланс компании» + audit_log.

    ТЗ owner 16.07: для «Б/н расход» (cashless) после выбора типа — развилка
    «с привязкой к счёту / без» (канон — CreditWalletSpendSG):
      • без привязки: сумма → НДС → описание → confirm → op_company_entries
        + «Баланс компании» (как раньше, без изменений);
      • с привязкой: bind_invoice (материнский счёт) → bind_category (7 категорий)
        → сумма (БЕЗ НДС-вопроса: у supplier_payments поля НДС нет) → описание →
        confirm → create_supplier_payment → invoices.cost_*/DP–DV + sync_invoice_row
        (НЕ в op_company_entries — иначе задвоение в «Балансе компании»).
    """
    type = State()              # 4 inline-кнопки: cashless / taxes / loan / cash
    bind_mode = State()         # cashless: 🔗 С привязкой к счёту / 📄 Без привязки
    bind_invoice = State()      # привязка: выбор материнского счёта (в работе + 🏁)
    bind_category = State()     # привязка: категория затрат → cost_type
    amount = State()            # сумма (text input, float > 0)
    nds = State()               # НДС (text input, float ≥ 0; только для cashless без привязки)
    loan_direction = State()    # направление займа (in/refund; только для loan)
    description = State()       # описание (text input, 3-200 символов)
    confirm = State()           # подтверждение + ✅ Записать / ❌ Отмена


class CreditTaskSG(StatesGroup):
    """ГД → «Менеджер КВ/КИА/НПН (кредит)» → «📋 Задачи»: новый расход кредитных средств.

    Flow: сумма → назначение → confirm.
    На confirm: INSERT credit_expense в активный credit-счёт (самый новый open
    с DA>0 для канала), force-sync лист Invoices (CZ + DA), создание задачи
    менеджеру с просьбой прислать платёжку (фото/PDF).
    """
    amount = State()            # сумма ₽ (text input, float > 0)
    purpose = State()           # назначение (text input, 3-200 символов)
    confirm = State()           # ✅ Записать / ❌ Отмена


class CreditPaymentReceiptSG(StatesGroup):
    """Менеджер: приём платёжки (фото/PDF) → закрытие задачи credit_payment_request.

    Legacy 1-этапный приём (ГД chat-flow credit_task_confirm: расход уже записан).
    Новый отложенный flow §C использует CreditPaymentExecuteSG (см. ниже).
    """
    waiting = State()           # ждём вложение для конкретного task_id (в state-data)


class CreditPaymentExecuteSG(StatesGroup):
    """Менеджер: ИСПОЛНЕНИЕ кредит-задачи (TZ 04.06 §C, отложенная запись).

    Flow: «✅ Получил» (open→in_progress) → «✅ Исполнено» → этот state ждёт
    платёжку (опц., или кнопка «без платёжки») → apply_credit_wallet_spend
    (запись расхода в БД) + закрытие задачи (CAS done) + файл инициатору/ГД.
    """
    waiting = State()           # ждём вложение/«пропустить» для credit_task_id


class CreditTaskRejectSG(StatesGroup):
    """Менеджер: ОТКЛОНЕНИЕ кредит-задачи (TZ 04.06 §C) — ввод причины.

    На вводе причины: CAS status→rejected, расход НЕ пишется (он отложен),
    уведомление инициатору+ГД с причиной.
    """
    reason = State()            # текст причины для credit_task_id


class ManagerCreditExpenseSG(StatesGroup):
    """Менеджер (KV/KIA/NPN): добавление расхода кредитных средств напрямую.

    Flow: выбор кред-счёта → сумма → назначение → confirm.
    Запись идёт в credit_expenses (по invoice_id), затем sync_invoice_row →
    обновляет Invoices CX («Кредит расход») и CZ («Кредит назначение»).
    После confirm: audit_log + DM ГД.

    Отличается от CreditTaskSG: там ГД инициирует и поручает менеджеру; здесь
    менеджер сам фиксирует расход без участия ГД (notify only).
    """
    invoice = State()       # inline-список кредитных счетов менеджера
    amount = State()        # сумма расхода (float > 0)
    description = State()   # назначение (3-200 символов)
    confirm = State()       # ✅ Записать / ❌ Отмена


class CreditWalletSpendSG(StatesGroup):
    """TZ кошелёк 02.06: единая форма траты кредитного кошелька (КВ/КИА/НПН).

    Доступ: менеджер (свой кошелёк), РП (выбор кошелька inline), ГД (кошелёк=канал).
    Flow: [РП: pick_wallet] → pick_mode →
      • привязка:  pick_invoice (счета «в работе» владельца кошелька) → pick_category
                   (Металл/Стекло/Монтаж/Грузчики/Логистика/Доп.мат/Доп.услуги) → amount → purpose
      • без привязки: amount → purpose
      → confirm.
    На confirm: add_credit_spend(wallet) +
      привязка → create_supplier_payment → cost_*/DP–DV + sync_invoice_row;
      без привязки → add_op_company_entry → «Баланс компании» I/J + sync_balance_company_sheet;
      + sync_advances_journal + инфо-карточка ГД/РП. Если spender≠владелец-менеджер —
      задача-платёжка менеджеру (kind=credit_payment_request), платёжка → инициатор+ГД.
    Shared-хендлеры в manager_new.router (callback-префикс cwspend:).
    """
    pick_wallet = State()    # РП: выбор кошелька КВ/КИА/НПН (ГД/менеджер пропускают)
    pick_mode = State()      # 🔗 С привязкой / 📄 Без привязки
    pick_invoice = State()   # привязка: счёт «в работе» владельца кошелька
    pick_category = State()  # привязка: категория затрат → cost_type
    amount = State()         # сумма ₽ (float > 0)
    purpose = State()        # назначение (3-200 символов)
    attach = State()         # §B: опц. вложение (фото/документ) или «Пропустить»
    confirm = State()        # ✅ Записать / ❌ Отмена


class CreditWalletEditSG(StatesGroup):
    """TZ #3 (Фаза 1): правка/отмена СВОЕЙ траты кредит-кошелька инициатором.

    Отдельная от CreditWalletSpendSG — чтобы edit-confirm не провалился в боевой
    cw_confirm (двойное списание). Вход: кнопка «✏️ Изменить» под карточкой «Расход
    записан» (callback cwedit:a:{spend_id}). Случай (a) — своя трата уже в БД.
    Flow: pick_field (что менять) →
      • сумма:      amount → confirm
      • назначение: pick_mode → [привязка: pick_invoice → pick_category] → purpose → confirm
      • и то, и то: amount → pick_mode → … → purpose → confirm
    На confirm: cancel_credit_spend(old) + apply_credit_wallet_spend(new) + «было→стало».
    Shared-хендлеры в manager_new.router (callback-префикс cwedit:).
    """
    pick_field = State()     # ✏️ Сумму / ✏️ Назначение / ✏️ И сумму, и назначение
    amount = State()         # новая сумма ₽ (float > 0)
    pick_mode = State()      # 🔗 С привязкой / 📄 Без привязки (при смене назначения)
    pick_invoice = State()   # привязка: счёт «в работе» владельца кошелька
    pick_category = State()  # привязка: категория затрат → cost_type
    purpose = State()        # новое назначение (3-200 символов)
    confirm = State()        # ✅ Сохранить / ❌ Отмена


class AdvanceRequestSG(StatesGroup):
    """ТЗ 2026-05-19 блок C: запрос аванса монтажником (whitelist Игорь).

    Flow: список объектов в работе → выбор объекта → ввод суммы (или «Взять всё»)
    → возврат к списку (cart-style) → «📤 Отправить» → notify ГД.
    Допуск: tg_id ∈ ADVANCE_ENABLED_INSTALLERS. Лимит по счёту: ≤ план ЗП.
    """
    list_invoices = State()    # карточка со списком + корзина
    enter_amount = State()     # ввод суммы для выбранного счёта


class GdAdvanceRejectSG(StatesGroup):
    """ГД отклоняет запрос аванса — ввод причины."""
    reason = State()


class GdAdvancePaySG(StatesGroup):
    """ГД оплачивает запрос аванса — загрузка чека/п/п."""
    receipt = State()


class AdvanceDistributeSG(StatesGroup):
    """ТЗ 2026-05-20: монтажник распределяет outstanding аванс по своим счетам.

    Flow: «💼 Мой баланс» → «📋 Распределить долг» → список candidates (in_work
    + invoice_ok без confirmed + approved-ZP) → выбор счёта → ввод суммы offset
    → подтверждение → создание/закрытие advance_item.
    """
    pick_invoice = State()
    enter_amount = State()
    confirm = State()


class InstallerAdvanceFillSG(StatesGroup):
    """Монтажник: наполнение кошелька аванса согласованной монтажной ЗП по счетам.

    Зеркало RpAdvanceFillSG. Flow: «💼 Мой баланс» → «💵 Наполнить аванс» →
    list_invoices (toggle-чекбоксы счетов где montazh_agreed_amount(BJ)>0 И
    zp_installer_status ∈ not_requested/requested/approved = «Монтаж Факт»(BS) пуст;
    сумма по счёту фиксирована = BJ, целиком, ввода НЕТ) → confirm (✅/❌) →
    db.credit_installer_zp_to_advance: по счетам zp_installer_status:='confirmed' +
    zp_installer_amount:=agreed (→ BS=agreed ветка-3) + ОДИН topup кошелька
    (wallet_role=NULL) → sync_invoice_row + инфо-уведомление ГД.
    """
    list_invoices = State()      # toggle-список счетов с невыплаченной ЗП
    confirm = State()            # ✅ Да / ❌ Нет (финальное подтверждение)


class GdDepositSG(StatesGroup):
    """TZ tingly-twirling-whistle + synthetic-hopping-ocean 2026-05-25:
    ГД-инициированное движение средств сотруднику. Hub-FSM для 2 веток.

    Flow: «💸 Оплата поставщику» → «💸 Финансы» → select_installer →
    select_type (Аванс / Депозит). Дальше ветвление:
    • DEPOSIT (для всех 4 в whitelist): enter_amount → attach_receipt (обязательно)
      → enter_comment → confirm → db.create_gd_deposit.
    • ADVANCE (только Игорь = installer; auto-offset на zp_installer_*):
      adv_select_invoice (активные счета Игоря) → adv_enter_amount → adv_attach_receipt
      (обязательно) → adv_enter_comment → adv_confirm → db.create_gd_advance.
    """
    select_installer = State()    # выбор сотрудника из whitelist
    select_type = State()         # NEW: Аванс / Депозит (skip→deposit для не-installer'а)
    # --- DEPOSIT branch ---
    enter_amount = State()        # сумма депозита ₽
    attach_receipt = State()      # фото / документ чека (обязательно)
    enter_comment = State()       # комментарий (или «—»)
    confirm = State()             # ✅ Подтвердить / ❌ Отмена
    # --- ADVANCE branch ---
    adv_select_invoice = State()  # NEW: выбор счёта Игоря (assigned_to=installer)
    adv_enter_amount = State()    # NEW: сумма аванса ₽
    adv_attach_receipt = State()  # NEW: чек (обязательно)
    adv_enter_comment = State()   # NEW: комментарий
    adv_confirm = State()         # NEW: ✅
    # --- REQUEST-FROM-DEPOSIT branch (ТЗ C 30.05: ГД запрашивает сумму из депозита) ---
    req_enter_amount = State()    # сумма запроса из депозита ₽
    req_enter_purpose = State()   # назначение (обязательно)
    req_attach = State()          # NEW 04.06: опц. вложение от ГД (фото/док; можно «Пропустить»)
    req_confirm = State()         # ✅ Отправить запрос сотруднику


class InstallerWithdrawSG(StatesGroup):
    """TZ tingly-twirling-whistle 2026-05-25: Игорь снимает с депозита на личные расходы.

    Flow: подменю «💰 Запрос ЗП» → «💸 Расход с депозита» → ввод суммы (≤ balance) →
    ввод комментария (обязательно: на что потрачено) → опц. прикрепить ПП/фото →
    confirm → db.create_installer_withdraw + notify ГД.
    """
    enter_amount = State()        # сумма withdraw ₽ (≤ deposit_balance)
    enter_comment = State()       # на что потрачено (обязательно)
    attach_receipt = State()      # ПП/фото (опционально, можно «Пропустить»)
    confirm = State()             # ✅ Подтвердить / ❌ Отмена


class ManagerWithdrawSG(StatesGroup):
    """TZ synthetic-hopping-ocean 2026-05-25: менеджер (КВ/КИА/НПН) снимает с депозита.

    Flow: «Еще» → «💸 Расход с депозита» → guard role∈MANAGER_* + tg_id ∈ WHITELIST + balance>0 →
    ввод суммы → комментарий → опц. чек → confirm → db.create_installer_withdraw + notify ГД.
    БД-метод параметрический (installer_id принимает любой employee tg_id).
    """
    enter_amount = State()
    enter_comment = State()
    attach_receipt = State()
    confirm = State()


class InstallerAdvDistributeSG(StatesGroup):
    """TZ funds-2balances 25.05: монтажник сам распределяет advance под счёт.

    Flow: подменю «💰 Запрос ЗП» → «💰 Распределить аванс (X ₽)» →
    select_invoice (активные счета Игоря, zp_installer не payment_sent/confirmed) →
    enter_amount (≤ advance_balance, ≤ plan_zp − taken) → confirm →
    db.add_advance_item_for_distribution(role='installer').
    """
    select_invoice = State()
    enter_amount = State()
    confirm = State()


class ManagerAdvDistributeSG(StatesGroup):
    """TZ funds-2balances 25.05: менеджер сам распределяет advance под счёт.

    Flow: «Еще» → «💰 Распределить аванс (X ₽)» → select_invoice (created_by менеджера,
    zp_manager_payout=0) → enter_amount → confirm → db.add_advance_item_for_distribution(role='manager').
    """
    select_invoice = State()
    enter_amount = State()
    confirm = State()


class ManagerAdvanceFillSG(StatesGroup):
    """Менеджер: наполнение кошелька аванса незабранной ЗП по счетам (TZ 06.06).

    Зеркало InstallerAdvanceFillSG, импорт-безопасный вариант. Flow: «💰 Финансы» →
    «💵 Наполнить аванс из ЗП (X ₽)» → list_invoices (toggle-чекбоксы счетов где
    creator_role=бренд менеджера, manager_zp_blank(AJ)>0, ЗП не в выплате/не забрана;
    сумма по счёту фиксирована = manager_zp_blank, целиком, ввода НЕТ) → confirm
    (✅/❌) → db.credit_manager_zp_to_advance: по счетам zp_manager_status:='confirmed'
    + zp_manager_amount:=blank (бот-поле, реимпорт ОП не трогает) + ОДИН topup
    кошелька (wallet_role: НПН='manager_npn', КВ/КИА=NULL) → sync_invoice_row +
    sync_advances_journal + инфо-уведомление ГД.
    """
    list_invoices = State()
    confirm = State()


class DepoReqExecuteSG(StatesGroup):
    """Депозит 04.06: сотрудник подтверждает ИСПОЛНЕНИЕ запроса ГД из депозита.

    Двухшаговый flow: «✅ Подтвердить прочтение» (read-ack) → «✅ Подтвердить
    исполнение» → этот FSM: опц. вложение (чек/фото) → db.create_gd_deposit_withdrawal
    (списание) + уведомление + пересылка файла ГД. Обработка — handlers/gd.py.
    """
    attach = State()  # опц. вложение исполнения (можно «Без вложения»)


class InstallerDepoToAdvSG(StatesGroup):
    """TZ funds-2balances 25.05: монтажник переводит часть депозита на advance.

    Flow: подменю «💰 Запрос ЗП» → «↔️ Депозит → Аванс (X ₽)» →
    enter_amount (≤ deposit_balance) → confirm → db.create_employee_depo_to_adv_transfer.
    Односторонний: реверса adv→depo нет.
    """
    enter_amount = State()
    confirm = State()


class ManagerDepoToAdvSG(StatesGroup):
    """TZ funds-2balances 25.05: менеджер переводит часть депозита на advance.

    Flow: «Еще» → «↔️ Депозит → Аванс (X ₽)» → enter_amount (≤ deposit_balance) →
    confirm → db.create_employee_depo_to_adv_transfer.
    """
    enter_amount = State()
    confirm = State()


class RpSalaryRequestSG(StatesGroup):
    """B5 v2 request-based TZ 27.05: РП инициирует запрос оклада 60К.

    Flow: «📂 Ещё» → «💼 Запрос оклада» → confirm («Запросить оклад за {YYYY-MM}? — 60 000 ₽»)
    → ✅ Да → INSERT task RP_SALARY, assigned_to=ГД, payload {month, amount, rp_id, rp_name}
    → notify ГД pre-card.
    Идемпотентность: повтор разрешён только если предыдущий task закрыт/отклонён.
    """
    confirm = State()


class RpSalaryRejectSG(StatesGroup):
    """B5 v2 request-based TZ 27.05: ГД отклоняет запрос оклада РП с указанием причины."""
    reason = State()
