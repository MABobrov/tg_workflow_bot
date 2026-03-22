/**
 * Google Apps Script for sheet "Отдел продаж"
 *
 * Sends webhook to the bot when:
 * 1. Command is written in "Команда боту" column (col AN = index 39)
 * 2. Data fields are changed (amount, address, deadline, manager, priority, comment)
 * 3. Any cell in a row with invoice number is edited (data sync)
 *
 * INSTALLATION:
 * 1. Open the Google Sheet "Отдел продаж"
 * 2. Extensions → Apps Script
 * 3. Paste this code, replace CONFIG values
 * 4. Save, then: Triggers → Add trigger → onEditOP → From spreadsheet → On edit
 *
 * NOTE: For webhook to work, the bot server must be accessible at BOT_WEBHOOK_URL.
 *
 * SETUP FUNCTIONS (run ONCE from Apps Script editor):
 * - setupCommandDropdown()      → creates dropdowns for command/manager/priority
 * - setupOPColumnComments()     → adds comments to all column headers
 */

// ============ CONFIG — CHANGE THESE ============
var CONFIG = {
  BOT_WEBHOOK_URL: "http://46.23.98.118:8443/webhooks/sheets",
  WEBHOOK_SECRET: "77",  // same as SHEETS_WEBHOOK_SECRET in .env
  SHEET_NAME: "Отдел продаж",

  // Column indexes (0-based) in "Отдел продаж" sheet
  COL_INVOICE_NUMBER: 4,   // E — Номер счёта
  COL_AMOUNT: 10,          // K — Сумма
  COL_ADDRESS: 5,          // F — Адрес
  COL_DEADLINE: 7,         // H — Сроки (дни)
  COL_DEBT: 25,            // Z — Долг (задолженность)
  COL_COMMAND: 39,         // AN — Команда боту (после AM «Комментарии»)
  COL_MANAGER: 40,         // AO — Менеджер (КВ/КИА/НПН)
  COL_PRIORITY: 41,        // AP — Приоритет
  COL_COMMENT: 42,         // AQ — Комментарий РП
  COL_MATERIALS_FACT: 37,  // AL — Материалы Факт

  // Total data columns to send for full row sync
  TOTAL_DATA_COLS: 52,     // A through AZ (all OP columns)

  // Bot-managed columns (skip to prevent circular updates)
  COL_BOT_STATUS: 45,      // AT — Статус бота (written by bot)
  COL_BOT_MONTAZH: 46,     // AU — Стадия монтажа (written by bot)

  HEADER_ROW: 1,           // skip header row
};
// ================================================

/**
 * Dropdown values for "Команда боту" column.
 * Add Data Validation in Google Sheets: col AN, list from range or manual:
 *
 * 📩 Напомнить менеджеру
 * 📋 Запрос документов
 * 📊 Запрос КП
 * 💰 Подтвердить оплату
 * 🔨 В монтаж
 * 📐 Запрос замера
 * 🚚 Оплата доставки
 * 🏁 Закрыть счёт
 */

// Fields that trigger field_change events
var TRACKED_FIELDS = {};
TRACKED_FIELDS[CONFIG.COL_AMOUNT] = "amount";
TRACKED_FIELDS[CONFIG.COL_ADDRESS] = "object_address";
TRACKED_FIELDS[CONFIG.COL_DEADLINE] = "deadline_days";
TRACKED_FIELDS[CONFIG.COL_MANAGER] = "manager";
TRACKED_FIELDS[CONFIG.COL_PRIORITY] = "priority";
TRACKED_FIELDS[CONFIG.COL_COMMENT] = "comment";
TRACKED_FIELDS[CONFIG.COL_DEBT] = "outstanding_debt";
TRACKED_FIELDS[CONFIG.COL_MATERIALS_FACT] = "materials_fact_op";


function onEditOP(e) {
  if (!e || !e.range) return;

  var sheet = e.source.getActiveSheet();
  if (sheet.getName() !== CONFIG.SHEET_NAME) return;

  var row = e.range.getRow();
  if (row <= CONFIG.HEADER_ROW) return;  // skip header

  var col = e.range.getColumn() - 1;  // 0-based

  // Skip bot-managed columns to prevent circular updates
  if (col === CONFIG.COL_BOT_STATUS || col === CONFIG.COL_BOT_MONTAZH) return;

  // Get invoice number from the row
  var invoiceNumber = sheet.getRange(row, CONFIG.COL_INVOICE_NUMBER + 1).getValue();
  if (!invoiceNumber) return;  // no invoice number — skip
  invoiceNumber = String(invoiceNumber).trim();
  if (!invoiceNumber) return;

  // --- 1. Command column ---
  if (col === CONFIG.COL_COMMAND) {
    var command = String(e.range.getValue()).trim();
    if (!command) return;

    sendWebhook({
      type: "command",
      invoice_number: invoiceNumber,
      command: command,
      source: "op"
    });

    // Clear the command cell after sending
    SpreadsheetApp.flush();
    Utilities.sleep(500);
    e.range.setValue("");
    return;
  }

  // --- 2. Tracked field changes ---
  var fieldName = TRACKED_FIELDS[col];
  if (fieldName) {
    var newValue = e.range.getValue();
    var changed_fields = {};
    changed_fields[fieldName] = newValue;

    sendWebhook({
      type: "field_change",
      invoice_number: invoiceNumber,
      changed_fields: changed_fields,
      source: "op"
    });
    return;
  }

  // --- 3. Data sync for other data columns ---
  if (col < CONFIG.TOTAL_DATA_COLS) {
    var rowValues = sheet.getRange(row, 1, 1, CONFIG.TOTAL_DATA_COLS).getValues()[0];
    var rowStrings = rowValues.map(function(v) { return String(v); });

    sendWebhook({
      type: "data_sync",
      invoice_number: invoiceNumber,
      row: rowStrings,
      source: "op"
    });
  }
}


function sendWebhook(payload) {
  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "X-Webhook-Secret": CONFIG.WEBHOOK_SECRET
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    connectTimeout: 10000,
    timeout: 15000
  };

  try {
    var response = UrlFetchApp.fetch(CONFIG.BOT_WEBHOOK_URL, options);
    var code = response.getResponseCode();
    if (code !== 200) {
      Logger.log("Webhook error: " + code + " — " + response.getContentText());
    }
  } catch (err) {
    Logger.log("Webhook failed: " + err.message);
  }
}


/**
 * Utility: Set up data validation (dropdown) for the Command column.
 * Run this function ONCE from Apps Script editor to create the dropdown.
 */
function setupCommandDropdown() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  if (!sheet) {
    Logger.log("Sheet not found: " + CONFIG.SHEET_NAME);
    return;
  }

  var commands = [
    "📩 Напомнить менеджеру",
    "📋 Запрос документов",
    "📊 Запрос КП",
    "💰 Подтвердить оплату",
    "🔨 В монтаж",
    "📐 Запрос замера",
    "🚚 Оплата доставки",
    "🏁 Закрыть счёт"
  ];

  var lastRow = sheet.getLastRow();
  var range = sheet.getRange(
    CONFIG.HEADER_ROW + 1, CONFIG.COL_COMMAND + 1,
    Math.max(lastRow - CONFIG.HEADER_ROW, 100), 1
  );

  var rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(commands, true)
    .setAllowInvalid(false)
    .build();

  range.setDataValidation(rule);

  // Set header
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_COMMAND + 1).setValue("Команда боту");
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_MANAGER + 1).setValue("Менеджер");
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_PRIORITY + 1).setValue("Приоритет");
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_COMMENT + 1).setValue("Комментарий РП");
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_BOT_STATUS + 1).setValue("Статус бота");
  sheet.getRange(CONFIG.HEADER_ROW, CONFIG.COL_BOT_MONTAZH + 1).setValue("Стадия монтажа");

  // Priority dropdown
  var priorityRange = sheet.getRange(
    CONFIG.HEADER_ROW + 1, CONFIG.COL_PRIORITY + 1,
    Math.max(lastRow - CONFIG.HEADER_ROW, 100), 1
  );
  var priorityRule = SpreadsheetApp.newDataValidation()
    .requireValueInList(["🟢", "🟡", "🔴"], true)
    .setAllowInvalid(false)
    .build();
  priorityRange.setDataValidation(priorityRule);

  // Manager dropdown
  var managerRange = sheet.getRange(
    CONFIG.HEADER_ROW + 1, CONFIG.COL_MANAGER + 1,
    Math.max(lastRow - CONFIG.HEADER_ROW, 100), 1
  );
  var managerRule = SpreadsheetApp.newDataValidation()
    .requireValueInList(["КВ", "КИА", "НПН"], true)
    .setAllowInvalid(false)
    .build();
  managerRange.setDataValidation(managerRule);

  Logger.log("Dropdowns created successfully!");
}


/**
 * Set up comments (notes) on all column headers for "Отдел продаж".
 * Run this function ONCE from Apps Script editor.
 *
 * Adds detailed description, purpose, and notes to each column header
 * WITHOUT changing column names.
 */
function setupOPColumnComments() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  if (!sheet) {
    Logger.log("Sheet not found: " + CONFIG.SHEET_NAME);
    return;
  }

  var comments = {
    1:  "A — №\nПорядковый номер строки.\nАвтоматическая нумерация для удобства навигации.",

    2:  "B — В работу\nДата перевода счёта в статус «В работе».\n" +
        "Заполняется при подтверждении оплаты (через бот или вручную).\n" +
        "Формат: ДД.ММ.ГГГГ",

    3:  "C — Менеджер\nОтветственный менеджер по данному счёту.\n" +
        "Синхронизируется с ботом. При изменении в столбце AL → переназначение в боте.",

    4:  "D — Бухг.ЭДО\nСтатус электронного документооборота.\n" +
        "Заполняется автоматически бухгалтером через бот.\n" +
        "Значения: подписано / ожидание / не подписано\n\n" +
        "═══ ДОКУМЕНТООБОРОТ ═══\n" +
        "Бот отслеживает 4 типа документов:\n" +
        "• Первичные ЭДО (docs_edo_signed) — электронная подпись счёта\n" +
        "• Закрывающие ЭДО (edo_signed) — электронная подпись закрывающих\n" +
        "• Первичные оригиналы (docs_originals_holder) — у ГД / у менеджера / нет\n" +
        "• Закрывающие оригиналы (closing_originals_holder) — у ГД / у менеджера / нет\n\n" +
        "Статус виден в карточках бухгалтерии:\n" +
        "📋 П: ✅эдо ✅ГДориг | З: ⏳эдо ⏳ориг",

    5:  "E — Номер счёта / Контрагент\n⚠️ КЛЮЧЕВОЕ ПОЛЕ — уникальный идентификатор счёта.\n\n" +
        "По этому номеру бот:\n" +
        "• Находит счёт во всех операциях\n" +
        "• Связывает задачи, платежи, материалы\n" +
        "• Синхронизирует данные между листами\n\n" +
        "Форматы: 1234-КВ, 1234-КИА, ЗМ-456 (кредитный)\n" +
        "❌ НЕ МЕНЯТЬ формат без согласования!",

    6:  "F — Адрес объекта\nАдрес монтажа/доставки/замера.\n\n" +
        "При изменении:\n" +
        "• Автообновление в БД бота\n" +
        "• Уведомление менеджеру и монтажнику (если назначен)\n" +
        "• Зеркалирование с листа Общая (если изменено там)",

    7:  "G — Ист. трафика\nИсточник трафика / откуда пришёл клиент.\n" +
        "Заполняется вручную для маркетинговой аналитики.\n" +
        "Примеры: сайт, Авито, рекомендация, холодный звонок",

    8:  "H — Сроки (дни)\nСрок выполнения заказа в рабочих днях.\n\n" +
        "При изменении:\n" +
        "• Бот пересчитывает deadline_end_date\n" +
        "• Менеджер получает уведомление\n" +
        "• Индикация в боте: ✅ >7дн / ⚠️ ≤7дн / 🔴 просрочен",

    9:  "I — Дата оконч. (формула)\nРасчётная дата окончания = дата пост. + сроки.\n" +
        "⚠️ ФОРМУЛА — не редактировать вручную!",

    10: "J — Дата факт\nФактическая дата завершения работ.\n" +
        "Заполняется при закрытии счёта (через бот или вручную).",

    11: "K — Сумма\n💰 Общая сумма счёта (₽).\n\n" +
        "При изменении:\n" +
        "• Автообновление в БД бота\n" +
        "• Уведомление менеджеру: «Сумма изменена: было X → стало Y»\n" +
        "• Пересчёт рентабельности и маржи",

    12: "L — Сумма 1пл\nСумма первого платежа / аванса (₽).\n" +
        "Для контроля частичной оплаты.",

    13: "M — Расч. мат.\nРасчётная стоимость материалов (₽).\n" +
        "Используется для План/Факт анализа и расчёта себестоимости.\n" +
        "Факт берётся из дочерних счетов и оплат поставщикам.",

    14: "N — Установка\nРасчётная стоимость монтажных работ (₽).\n" +
        "Для монтажника бот показывает: int(установка × 0.77) // 1000 × 1000",

    15: "O — Грузчики\nСтоимость услуг грузчиков (₽).",

    16: "P — Логистика\nРасчётная стоимость доставки (₽).\n\n" +
        "Фактическая стоимость записывается ботом при\n" +
        "оплате доставки через функцию «🚚 Оплата доставки».\n" +
        "Бот сохраняет actual_logistics в БД и обновляет эту ячейку.",

    17: "Q — Прибыль\nРасчётная прибыль по счёту (₽).",

    18: "R — НДС\nСумма НДС. Ручное заполнение.",

    19: "S — Нал.приб.\nНалогооблагаемая прибыль. Ручное заполнение.",

    20: "T — Рент-ть расч\nРасчётная рентабельность (%).\n" +
        "Формула: (прибыль / сумма) × 100",

    21: "U — Рент-ть факт\nФактическая рентабельность (%).\n" +
        "Рассчитывается после закрытия с учётом всех расходов.",

    22: "V — Сумма допл\nСумма доплаты по счёту (₽).",
    23: "W — Допл подтв\nПодтверждение получения доплаты.",
    24: "X — Дата допл\nДата поступления доплаты.",
    25: "Y — Оконч допл\nСрок окончания по доплате.",
    26: "Z — Дата оконч\nДата окончания работ.",

    27: "AA — Долг\nТекущая задолженность клиента (₽).\n" +
        "Формула: сумма - оплаты.",

    28: "AB — Договор\nНомер или статус договора.\n" +
        "Значения: подписан / не подписан / №договора",

    29: "AC — Закр.док\nСтатус закрывающих документов.\n\n" +
        "═══ ДОКУМЕНТООБОРОТ ═══\n" +
        "В боте бухгалтерия отслеживает документы через кнопку «✏️ Документы»:\n" +
        "• ЭДО первичные — электронная подпись на счёт\n" +
        "• ЭДО закрывающие — электронная подпись на акты/УПД\n" +
        "• Оригиналы первичные — бумажные документы (у ГД / у менеджера / нет)\n" +
        "• Оригиналы закрывающие — бумажные закрывающие (у ГД / у менеджера / нет)\n\n" +
        "Бухгалтер может запросить документы у менеджера через бот (📨 Запрос менеджеру).\n" +
        "Менеджер получает задачу EDO_REQUEST.\n" +
        "При обновлении статуса бухгалтером → задачи EDO_REQUEST закрываются автоматически.",

    30: "AD — Пояснения\nПримечания и комментарии по счёту.\n" +
        "Свободное текстовое поле для любых заметок.",

    31: "AE — Агентское\nАгентское вознаграждение (₽).\n" +
        "Учитывается в себестоимости.",

    32: "AF — Мен.ЗП\nЗарплата менеджера по данному счёту (₽).\n\n" +
        "Менеджер запрашивает ЗП через бот (кнопка «💰 Запрос ЗП»).\n" +
        "ГД одобряет/отклоняет. Статусы: не запрошена / запрошена / одобрена.",

    33: "AG — Запрос\nСтатус текущего запроса по счёту.",

    34: "AH — тех\nТехнические заметки. Ручное заполнение.",

    // Bot-control columns (AK-AN)
    37: "AK — Команда боту 🤖\n" +
        "Выберите команду из выпадающего списка.\n" +
        "После выбора команда отправляется боту и ячейка очищается автоматически.\n\n" +
        "═══ КОМАНДЫ (требуют номер счёта в строке) ═══\n\n" +
        "📩 Напомнить менеджеру\n" +
        "   → Отправляет напоминание менеджеру по этому счёту в Telegram.\n" +
        "   → Менеджер видит: номер счёта, адрес, сумму.\n\n" +
        "📋 Запрос документов\n" +
        "   → Создаёт задачу DOCS_REQUEST для менеджера.\n" +
        "   → Задача приходит в «📥 Входящие задачи» менеджера.\n" +
        "   → Менеджер должен предоставить недостающие документы.\n\n" +
        "📊 Запрос КП\n" +
        "   → Создаёт задачу QUOTE_REQUEST для менеджера.\n" +
        "   → Запрос коммерческого предложения по счёту.\n\n" +
        "💰 Подтвердить оплату\n" +
        "   → Переводит счёт в статус «В работе» (IN_WORK).\n" +
        "   → Менеджер получает уведомление: «Оплата подтверждена».\n\n" +
        "🔨 В монтаж\n" +
        "   → Назначает монтажника на счёт.\n" +
        "   → Создаёт задачу INSTALLATION в «📥 Входящие» монтажника.\n" +
        "   → Устанавливает montazh_stage = IN_WORK.\n\n" +
        "📐 Запрос замера\n" +
        "   → Создаёт заявку на замер (ZAMERY_REQUEST).\n" +
        "   → Замерщик получает задачу в «📋 Заявка на замер».\n\n" +
        "🚚 Оплата доставки\n" +
        "   → Создаёт задачу DELIVERY_REQUEST для ГД.\n" +
        "   → ГД принимает, оплачивает, указывает фактическую стоимость.\n" +
        "   → Фактическая стоимость записывается в БД и таблицу (кол. P).\n\n" +
        "🏁 Закрыть счёт\n" +
        "   → Переводит счёт в статус «Закрытие» (CLOSING).\n" +
        "   → Уведомление менеджеру и ГД.\n" +
        "   → Далее ГД завершает через бот (карточка себестоимости + маржа).\n\n" +
        "Результат всех команд приходит в Telegram-бот получателю.",

    38: "AL — Менеджер 👤\n" +
        "Назначенный менеджер для этого счёта.\n\n" +
        "Выберите из списка: КВ / КИА / НПН\n\n" +
        "При смене менеджера бот:\n" +
        "• Переназначает счёт в БД (created_by + creator_role)\n" +
        "• Отправляет новому менеджеру: «Вам назначен счёт»\n" +
        "• Отправляет старому: «Счёт переназначен»\n" +
        "• Все будущие задачи по счёту идут новому менеджеру",

    39: "AM — Приоритет ⚡\n" +
        "Приоритет обработки счёта.\n\n" +
        "🟢 — Обычный приоритет (без уведомлений)\n" +
        "🟡 — Повышенный (без уведомлений, визуальный маркер)\n" +
        "🔴 — СРОЧНО! → Менеджер немедленно получает\n" +
        "     уведомление «🔴 СРОЧНО» в Telegram-бот\n" +
        "     с номером счёта, адресом и суммой.",

    40: "AN — Комментарий РП 💬\n" +
        "Текстовый комментарий от РП.\n\n" +
        "При вводе текста:\n" +
        "• Комментарий мгновенно пересылается менеджеру в Telegram-бот\n" +
        "• В сообщении указано: «💬 Комментарий от РП, Счёт: №...»\n" +
        "• Используйте для оперативной связи по конкретному счёту\n" +
        "• Ячейка НЕ очищается — комментарий остаётся для истории"
  };

  for (var col in comments) {
    sheet.getRange(CONFIG.HEADER_ROW, parseInt(col)).setNote(comments[col]);
  }

  Logger.log("Column comments added to «Отдел продаж» successfully!");
}
