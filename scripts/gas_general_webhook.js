/**
 * Google Apps Script for sheet "Общая" (General / GD sheet)
 *
 * When GD edits this sheet:
 * 1. Command column → sends webhook to bot (source: "general")
 * 2. Field changes → mirrors to "Отдел продаж" sheet + sends webhook to bot
 * 3. Data changes → mirrors to "Отдел продаж" sheet + sends webhook to bot
 *
 * This allows GD to manage invoices from the "Общая" sheet,
 * with all changes flowing: Общая → Отдел продаж → Bot DB
 *
 * INSTALLATION:
 * 1. Open the Google Sheet that contains both "Общая" and "Отдел продаж"
 *    (or the separate GD spreadsheet)
 * 2. Extensions → Apps Script
 * 3. Paste this code, replace CONFIG values
 * 4. Save, then: Triggers → Add trigger → onEditGeneral → From spreadsheet → On edit
 *
 * NOTE: If "Общая" and "Отдел продаж" are in DIFFERENT spreadsheets,
 *       set OP_SPREADSHEET_ID to the ID of the ОП spreadsheet.
 *       If they're in the SAME spreadsheet, leave OP_SPREADSHEET_ID empty ("").
 *
 * SETUP FUNCTIONS (run ONCE from Apps Script editor):
 * - setupGeneralDropdowns()      → creates dropdowns for command/manager/priority
 * - setupGeneralColumnComments() → adds comments to all column headers + bot columns
 */

// ============ CONFIG — CHANGE THESE ============
var GEN_CONFIG = {
  BOT_WEBHOOK_URL: "http://46.23.98.118:8443/webhooks/sheets",
  WEBHOOK_SECRET: "77",  // same as SHEETS_WEBHOOK_SECRET in .env

  // Sheet names
  GENERAL_SHEET_NAME: "Общая",
  OP_SHEET_NAME: "Отдел продаж",

  // If "Отдел продаж" is in a DIFFERENT spreadsheet, set its ID here.
  // Leave empty ("") if both sheets are in the same spreadsheet.
  OP_SPREADSHEET_ID: "",

  // Column mapping in "Общая" sheet (0-based).
  // Adjust these to match YOUR "Общая" layout.
  // The GD sheet may have a different column order than ОП.
  GEN_COL_INVOICE_NUMBER: 4,   // E — Номер счёта
  GEN_COL_AMOUNT: 10,          // K — Сумма
  GEN_COL_ADDRESS: 5,          // F — Адрес
  GEN_COL_DEADLINE: 7,         // H — Сроки (дни)
  GEN_COL_COMMAND: 39,         // AN — Команда боту
  GEN_COL_MANAGER: 40,         // AO — Менеджер (КВ/КИА/НПН)
  GEN_COL_PRIORITY: 41,        // AP — Приоритет
  GEN_COL_COMMENT: 42,         // AQ — Комментарий ГД

  // Column mapping in "Отдел продаж" (target for mirroring)
  OP_COL_INVOICE_NUMBER: 4,    // E — same as ОП
  OP_COL_AMOUNT: 10,           // K
  OP_COL_ADDRESS: 5,           // F
  OP_COL_DEADLINE: 7,          // H

  TOTAL_DATA_COLS: 34,
  HEADER_ROW: 1,
};

// Mapping: "Общая" column index → ОП column index (for mirroring)
var GEN_TO_OP_MAP = {};
GEN_TO_OP_MAP[GEN_CONFIG.GEN_COL_AMOUNT] = GEN_CONFIG.OP_COL_AMOUNT;
GEN_TO_OP_MAP[GEN_CONFIG.GEN_COL_ADDRESS] = GEN_CONFIG.OP_COL_ADDRESS;
GEN_TO_OP_MAP[GEN_CONFIG.GEN_COL_DEADLINE] = GEN_CONFIG.OP_COL_DEADLINE;

// Fields that trigger field_change events
var GEN_TRACKED_FIELDS = {};
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_AMOUNT] = "amount";
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_ADDRESS] = "object_address";
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_DEADLINE] = "deadline_days";
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_MANAGER] = "manager";
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_PRIORITY] = "priority";
GEN_TRACKED_FIELDS[GEN_CONFIG.GEN_COL_COMMENT] = "comment";

// Statistics commands — don't require invoice number
var GEN_STATS_COMMANDS = [
  "📊 Сводка",
  "📊 По менеджерам",
  "📊 Выставленные счета",
  "📊 ЗП задолженность",
  "📊 Задачи в работе",
  "📊 Документооборот"
];
// ================================================


function onEditGeneral(e) {
  if (!e || !e.range) return;

  var sheet = e.source.getActiveSheet();
  if (sheet.getName() !== GEN_CONFIG.GENERAL_SHEET_NAME) return;

  var row = e.range.getRow();
  if (row <= GEN_CONFIG.HEADER_ROW) return;

  var col = e.range.getColumn() - 1;  // 0-based

  // --- 1. Command column ---
  if (col === GEN_CONFIG.GEN_COL_COMMAND) {
    var command = String(e.range.getValue()).trim();
    if (!command) return;

    // Stats commands don't need invoice_number
    var isStats = GEN_STATS_COMMANDS.some(function(c) {
      return command === c;
    });

    var invoiceNumber = "";
    if (!isStats) {
      invoiceNumber = String(
        sheet.getRange(row, GEN_CONFIG.GEN_COL_INVOICE_NUMBER + 1).getValue()
      ).trim();
      if (!invoiceNumber) return;
    }

    sendWebhookGeneral({
      type: "command",
      invoice_number: invoiceNumber,
      command: command,
      source: "general"
    });

    // Clear command cell
    SpreadsheetApp.flush();
    Utilities.sleep(500);
    e.range.setValue("");
    return;
  }

  // Get invoice number (required for non-command edits)
  var invoiceNumber = sheet.getRange(row, GEN_CONFIG.GEN_COL_INVOICE_NUMBER + 1).getValue();
  if (!invoiceNumber) return;
  invoiceNumber = String(invoiceNumber).trim();
  if (!invoiceNumber) return;

  // --- 2. Tracked field changes ---
  var fieldName = GEN_TRACKED_FIELDS[col];
  if (fieldName) {
    var newValue = e.range.getValue();

    // Mirror to ОП sheet (if it's a data field, not just priority/comment/manager)
    var opCol = GEN_TO_OP_MAP[col];
    if (opCol !== undefined) {
      mirrorToOP(invoiceNumber, opCol, newValue);
    }

    var changed_fields = {};
    changed_fields[fieldName] = newValue;

    sendWebhookGeneral({
      type: "field_change",
      invoice_number: invoiceNumber,
      changed_fields: changed_fields,
      source: "general"
    });
    return;
  }

  // --- 3. Data sync for other columns ---
  if (col < GEN_CONFIG.TOTAL_DATA_COLS) {
    // Mirror the changed cell to ОП
    var newVal = e.range.getValue();
    mirrorToOP(invoiceNumber, col, newVal);

    // Send full row to bot
    var rowValues = sheet.getRange(row, 1, 1, GEN_CONFIG.TOTAL_DATA_COLS).getValues()[0];
    var rowStrings = rowValues.map(function(v) { return String(v); });

    sendWebhookGeneral({
      type: "data_sync",
      invoice_number: invoiceNumber,
      row: rowStrings,
      source: "general"
    });
  }
}


/**
 * Mirror a cell value from "Общая" to "Отдел продаж" by invoice number.
 */
function mirrorToOP(invoiceNumber, opColIndex, value) {
  try {
    var opSheet;
    if (GEN_CONFIG.OP_SPREADSHEET_ID) {
      // Different spreadsheet
      var opSS = SpreadsheetApp.openById(GEN_CONFIG.OP_SPREADSHEET_ID);
      opSheet = opSS.getSheetByName(GEN_CONFIG.OP_SHEET_NAME);
    } else {
      // Same spreadsheet
      opSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(GEN_CONFIG.OP_SHEET_NAME);
    }

    if (!opSheet) {
      Logger.log("ОП sheet not found: " + GEN_CONFIG.OP_SHEET_NAME);
      return;
    }

    // Find the row with this invoice number in ОП
    var opInvoiceCol = GEN_CONFIG.OP_COL_INVOICE_NUMBER + 1;  // 1-based
    var lastRow = opSheet.getLastRow();
    if (lastRow <= GEN_CONFIG.HEADER_ROW) return;

    var invoiceValues = opSheet.getRange(
      GEN_CONFIG.HEADER_ROW + 1, opInvoiceCol,
      lastRow - GEN_CONFIG.HEADER_ROW, 1
    ).getValues();

    for (var i = 0; i < invoiceValues.length; i++) {
      if (String(invoiceValues[i][0]).trim() === invoiceNumber) {
        var targetRow = GEN_CONFIG.HEADER_ROW + 1 + i;
        opSheet.getRange(targetRow, opColIndex + 1).setValue(value);
        Logger.log("Mirrored to ОП: row " + targetRow + ", col " + (opColIndex + 1) + " = " + value);
        return;
      }
    }

    Logger.log("Invoice " + invoiceNumber + " not found in ОП sheet");
  } catch (err) {
    Logger.log("Mirror error: " + err.message);
  }
}


function sendWebhookGeneral(payload) {
  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "X-Webhook-Secret": GEN_CONFIG.WEBHOOK_SECRET
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    connectTimeout: 10000,
    timeout: 15000
  };

  try {
    var response = UrlFetchApp.fetch(GEN_CONFIG.BOT_WEBHOOK_URL, options);
    var code = response.getResponseCode();
    if (code !== 200) {
      Logger.log("Webhook error: " + code + " — " + response.getContentText());
    }
  } catch (err) {
    Logger.log("Webhook failed: " + err.message);
  }
}


/**
 * Utility: Set up dropdowns for the "Общая" sheet.
 * Run this function ONCE from Apps Script editor.
 */
function setupGeneralDropdowns() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(GEN_CONFIG.GENERAL_SHEET_NAME);
  if (!sheet) {
    Logger.log("Sheet not found: " + GEN_CONFIG.GENERAL_SHEET_NAME);
    return;
  }

  // All commands: action commands + statistics commands
  var commands = [
    "📩 Напомнить менеджеру",
    "📋 Запрос документов",
    "📊 Запрос КП",
    "💰 Подтвердить оплату",
    "🔨 В монтаж",
    "📐 Запрос замера",
    "🚚 Оплата доставки",
    "🏁 Закрыть счёт",
    "───── Статистика ─────",
    "📊 Сводка",
    "📊 По менеджерам",
    "📊 Выставленные счета",
    "📊 ЗП задолженность",
    "📊 Задачи в работе",
    "📊 Документооборот"
  ];

  var lastRow = sheet.getLastRow();
  var dataRows = Math.max(lastRow - GEN_CONFIG.HEADER_ROW, 100);

  // Command dropdown
  var cmdRange = sheet.getRange(
    GEN_CONFIG.HEADER_ROW + 1, GEN_CONFIG.GEN_COL_COMMAND + 1,
    dataRows, 1
  );
  var cmdRule = SpreadsheetApp.newDataValidation()
    .requireValueInList(commands, true)
    .setAllowInvalid(false)
    .build();
  cmdRange.setDataValidation(cmdRule);

  // Priority dropdown
  var priorityRange = sheet.getRange(
    GEN_CONFIG.HEADER_ROW + 1, GEN_CONFIG.GEN_COL_PRIORITY + 1,
    dataRows, 1
  );
  var priorityRule = SpreadsheetApp.newDataValidation()
    .requireValueInList(["🟢", "🟡", "🔴"], true)
    .setAllowInvalid(false)
    .build();
  priorityRange.setDataValidation(priorityRule);

  // Manager dropdown
  var managerRange = sheet.getRange(
    GEN_CONFIG.HEADER_ROW + 1, GEN_CONFIG.GEN_COL_MANAGER + 1,
    dataRows, 1
  );
  var managerRule = SpreadsheetApp.newDataValidation()
    .requireValueInList(["КВ", "КИА", "НПН"], true)
    .setAllowInvalid(false)
    .build();
  managerRange.setDataValidation(managerRule);

  // Set headers
  sheet.getRange(GEN_CONFIG.HEADER_ROW, GEN_CONFIG.GEN_COL_COMMAND + 1).setValue("Команда боту");
  sheet.getRange(GEN_CONFIG.HEADER_ROW, GEN_CONFIG.GEN_COL_MANAGER + 1).setValue("Менеджер");
  sheet.getRange(GEN_CONFIG.HEADER_ROW, GEN_CONFIG.GEN_COL_PRIORITY + 1).setValue("Приоритет");
  sheet.getRange(GEN_CONFIG.HEADER_ROW, GEN_CONFIG.GEN_COL_COMMENT + 1).setValue("Комментарий ГД");

  Logger.log("General sheet dropdowns created successfully!");
}


/**
 * Set up comments (notes) on all column headers and bot-control columns.
 * Run this function ONCE from Apps Script editor.
 *
 * Adds:
 * 1. Descriptions to EVERY column header on "Общая"
 * 2. Instructions for bot-command columns (AK-AN)
 * 3. Descriptions to columns on "Отдел продаж"
 */
function setupGeneralColumnComments() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // ========== "Общая" sheet ==========
  var genSheet = ss.getSheetByName(GEN_CONFIG.GENERAL_SHEET_NAME);
  if (genSheet) {
    // Comments for standard data columns (A-AH, 0-33)
    // Adjust these to match the actual layout of your "Общая" sheet
    var genColComments = {
      1:  "A — №\nПорядковый номер строки. Авто-нумерация.",
      2:  "B — В работу\nДата перевода счёта в статус «В работе». Заполняется при подтверждении оплаты.",
      3:  "C — Менеджер\nИмя/ФИО ответственного менеджера (КВ, КИА или НПН). Заполняется автоматически из бота или вручную.",
      4:  "D — Бухг.ЭДО\nСтатус электронного документооборота с бухгалтерией. Заполняется бухгалтером через бот.",
      5:  "E — Контрагент / Номер счёта\n⚠️ КЛЮЧЕВОЕ ПОЛЕ — по нему бот находит счёт.\nУникальный номер счёта (например: 1234-КВ). Не изменять формат!",
      6:  "F — Адрес объекта\nАдрес монтажа/замера. Изменение → автообновление в боте + уведомление менеджеру.",
      7:  "G — Ист. трафика\nИсточник трафика (откуда пришёл клиент). Ручное заполнение.",
      8:  "H — Сроки (дни)\nКоличество рабочих дней на выполнение. Изменение → пересчёт дедлайна в боте.",
      9:  "I — Б.Н./Кред\nФорма оплаты: безналичный расчёт или кредит.",
      10: "J — Свой/Атм\nИсточник клиента: собственный или атмосферный.",
      11: "K — Сумма\n💰 Общая сумма счёта (₽). Изменение → автообновление в БД бота + уведомление.",
      12: "L — Сумма 1пл\nСумма первого платежа. Для контроля частичной оплаты.",
      13: "M — Дата пост.\nДата поступления заказа/счёта.",
      14: "N — Расч. мат.\nРасчётная стоимость материалов (₽). Используется для План/Факт анализа.",
      15: "O — Установка\nРасчётная стоимость монтажных работ (₽).",
      16: "P — Логистика\nРасчётная стоимость доставки (₽). Фактическая фиксируется ботом при оплате.",
      17: "Q — Грузчики\nСтоимость услуг грузчиков (₽). Ручное заполнение.",
      18: "R — Прибыль\nРасчётная прибыль по счёту (₽).",
      19: "S — НДС\nСумма НДС. Ручное заполнение.",
      20: "T — Нал.приб.\nНалогооблагаемая прибыль. Ручное заполнение.",
      21: "U — Рент-ть расч\nРасчётная рентабельность (%).",
      22: "V — Рент-ть факт\nФактическая рентабельность (%). Ручное заполнение.",
      23: "W — Сумма допл\nСумма доплаты по счёту (₽).",
      24: "X — Допл подтв\nПодтверждение доплаты.",
      25: "Y — Дата допл\nДата доплаты.",
      26: "Z — Дата оконч.\nДата окончания работ по договору (формула или ручное).",
      27: "AA — Долг\nТекущая задолженность по счёту (₽).",
      28: "AB — Договор\nНомер или статус договора.",
      29: "AC — Закр.док\nСтатус закрывающих документов.",
      30: "AD — Пояснения\nПримечания, комментарии по счёту. Ручное заполнение.",
      31: "AE — Агентское\nАгентское вознаграждение.",
      32: "AF — Мен.ЗП\nЗарплата менеджера по данному счёту.",
      33: "AG — Запрос\nСтатус запроса.",
      34: "AH — тех\nТехнические заметки.",
    };

    for (var col in genColComments) {
      genSheet.getRange(GEN_CONFIG.HEADER_ROW, parseInt(col)).setNote(genColComments[col]);
    }

    // Bot-control columns (AK-AN) with detailed instructions
    var botColComments = {};
    botColComments[GEN_CONFIG.GEN_COL_COMMAND + 1] =
      "AK — Команда боту 🤖\n" +
      "Выберите команду из выпадающего списка.\n" +
      "После выбора команда отправляется боту и ячейка очищается.\n\n" +
      "═══ КОМАНДЫ (требуют номер счёта в строке) ═══\n" +
      "📩 Напомнить менеджеру — отправить напоминание менеджеру по этому счёту\n" +
      "📋 Запрос документов — создать задачу менеджеру на предоставление документов\n" +
      "📊 Запрос КП — запросить коммерческое предложение у менеджера\n" +
      "💰 Подтвердить оплату — подтвердить оплату, перевести счёт в «В работе»\n" +
      "🔨 В монтаж — назначить монтажника, создать задачу на монтаж\n" +
      "📐 Запрос замера — создать заявку на замер для замерщика\n" +
      "🚚 Оплата доставки — создать задачу на оплату доставки\n" +
      "🏁 Закрыть счёт — инициировать закрытие счёта\n\n" +
      "═══ СТАТИСТИКА (не требуют номер счёта) ═══\n" +
      "📊 Сводка — общая сводка: счета по статусам, суммы, задачи\n" +
      "📊 По менеджерам — разбивка по менеджерам: счета, суммы\n" +
      "📊 Выставленные счета — первичные документы, фиксация по менеджерам\n" +
      "📊 ЗП задолженность — невыплаченные запросы ЗП\n" +
      "📊 Задачи в работе — активные задачи по типам и исполнителям\n" +
      "📊 Документооборот — статус ЭДО, оригиналы, незакрытые запросы\n\n" +
      "Результат приходит ГД в Telegram-бот.";

    botColComments[GEN_CONFIG.GEN_COL_MANAGER + 1] =
      "AL — Менеджер 👤\n" +
      "Назначенный менеджер для этого счёта.\n\n" +
      "Выберите из списка: КВ / КИА / НПН\n\n" +
      "При смене менеджера:\n" +
      "• Счёт переназначается в БД бота\n" +
      "• Новый менеджер получает уведомление\n" +
      "• Старый менеджер получает уведомление о передаче";

    botColComments[GEN_CONFIG.GEN_COL_PRIORITY + 1] =
      "AM — Приоритет ⚡\n" +
      "Приоритет обработки счёта.\n\n" +
      "🟢 — Обычный приоритет\n" +
      "🟡 — Повышенный приоритет\n" +
      "🔴 — СРОЧНО! Менеджер получит уведомление «СРОЧНО» в бот";

    botColComments[GEN_CONFIG.GEN_COL_COMMENT + 1] =
      "AN — Комментарий ГД 💬\n" +
      "Текстовый комментарий от ГД.\n\n" +
      "При вводе текста:\n" +
      "• Комментарий пересылается менеджеру в Telegram-бот\n" +
      "• Указывается номер счёта и отправитель (ГД)\n\n" +
      "Используйте для оперативной связи по конкретному счёту.";

    for (var col in botColComments) {
      genSheet.getRange(GEN_CONFIG.HEADER_ROW, parseInt(col)).setNote(botColComments[col]);
    }

    Logger.log("Comments added to «Общая» sheet headers.");
  }

  // ========== "Отдел продаж" sheet ==========
  _setupOPColumnComments(ss);
}


/**
 * Add column comments to "Отдел продаж" sheet.
 * Called from setupGeneralColumnComments() or can be run standalone.
 */
function _setupOPColumnComments(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  var opSheet = ss.getSheetByName(GEN_CONFIG.OP_SHEET_NAME);
  if (!opSheet) {
    Logger.log("Sheet not found: " + GEN_CONFIG.OP_SHEET_NAME);
    return;
  }

  // ОП column descriptions (1-based column numbers)
  var opColComments = {
    1:  "A — №\nПорядковый номер. Авто-нумерация строк в таблице.",
    2:  "B — В работу\nДата перевода счёта в работу. Заполняется при подтверждении оплаты (через бот или вручную).",
    3:  "C — Менеджер\nОтветственный менеджер (ФИО или код). Синхронизируется с ботом.",
    4:  "D — Бухг.ЭДО\nСтатус ЭДО (электронного документооборота). Заполняется бухгалтером через бот.",
    5:  "E — Номер счёта / Контрагент\n⚠️ КЛЮЧЕВОЕ ПОЛЕ — уникальный идентификатор счёта.\nПо нему бот находит и связывает данные.\nФормат: 1234-КВ, ЗМ-456 (кредитный) и т.д.\nНЕ МЕНЯТЬ формат без согласования!",
    6:  "F — Адрес объекта\nАдрес монтажа/доставки. При изменении → обновление в боте + уведомление участникам.",
    7:  "G — Ист. трафика\nОткуда пришёл клиент. Заполняется вручную для аналитики.",
    8:  "H — Сроки (дни)\nСрок выполнения в рабочих днях от даты поступления.\nПри изменении → пересчёт дедлайна + уведомление менеджеру.",
    9:  "I — Дата оконч.\nРасчётная дата окончания работ (формула: дата пост. + сроки). Не редактировать вручную.",
    10: "J — Дата факт\nФактическая дата завершения работ. Заполняется при закрытии.",
    11: "K — Сумма\n💰 Общая сумма счёта (₽).\nПри изменении → автоматическое обновление в БД бота + уведомление менеджеру.",
    12: "L — Сумма 1пл\nСумма первого платежа/аванса (₽).",
    13: "M — Б.Н./Кред\nТип оплаты: безналичный расчёт или кредит.",
    14: "N — Расч. мат.\nРасчётная стоимость материалов (₽). Для План/Факт анализа.",
    15: "O — Установка\nРасчётная стоимость монтажных работ (₽).",
    16: "P — Грузчики\nСтоимость услуг грузчиков (₽).",
    17: "Q — Логистика (расч.)\nРасчётная стоимость доставки (₽). Фактическая записывается ботом при оплате.",
    18: "R — Прибыль\nРасчётная прибыль (₽).",
    19: "S — НДС\nСумма НДС.",
    20: "T — Нал.приб.\nНалогооблагаемая прибыль.",
    21: "U — Рент-ть расч\nРасчётная рентабельность (%).",
    22: "V — Рент-ть факт\nФактическая рентабельность (%).",
    23: "W — Сумма допл\nСумма доплаты (₽).",
    24: "X — Допл подтв\nПодтверждение доплаты.",
    25: "Y — Дата допл\nДата доплаты.",
    26: "Z — Оконч допл\nОкончание по доплате.",
    27: "AA — Дата оконч\nДата окончания.",
    28: "AB — Долг\nТекущая задолженность (₽).",
    29: "AC — Договор\nНомер/статус договора.",
    30: "AD — Закр.док\nЗакрывающие документы.",
    31: "AE — Пояснения\nПримечания по счёту.",
    32: "AF — Агентское\nАгентское вознаграждение.",
    33: "AG — Мен.ЗП\nЗП менеджера по счёту.",
    34: "AH — Запрос\nСтатус запроса.",
    // Bot-specific columns (AK-AN)
    37: "AK — Команда боту 🤖\n" +
        "Выберите команду из выпадающего списка.\n" +
        "После выбора команда отправляется боту и ячейка очищается.\n\n" +
        "📩 Напомнить менеджеру — напоминание менеджеру\n" +
        "📋 Запрос документов — задача на документы\n" +
        "📊 Запрос КП — запрос коммерческого предложения\n" +
        "💰 Подтвердить оплату — перевод в «В работе»\n" +
        "🔨 В монтаж — назначение монтажника\n" +
        "📐 Запрос замера — заявка на замер\n" +
        "🚚 Оплата доставки — задача на доставку\n" +
        "🏁 Закрыть счёт — инициировать закрытие\n\n" +
        "Результат приходит в Telegram-бот.",
    38: "AL — Менеджер 👤\n" +
        "Назначенный менеджер: КВ / КИА / НПН.\n" +
        "При смене → переназначение в боте + уведомления.",
    39: "AM — Приоритет ⚡\n" +
        "🟢 Обычный | 🟡 Повышенный | 🔴 СРОЧНО\n" +
        "При 🔴 → уведомление «СРОЧНО» менеджеру.",
    40: "AN — Комментарий РП 💬\n" +
        "Текстовый комментарий от РП.\n" +
        "При вводе → пересылка менеджеру в Telegram."
  };

  for (var col in opColComments) {
    opSheet.getRange(GEN_CONFIG.HEADER_ROW, parseInt(col)).setNote(opColComments[col]);
  }

  Logger.log("Comments added to «Отдел продаж» sheet headers.");
}
