#!/usr/bin/env python3
"""
Add column header comments (notes) to "Отдел продаж" sheet
and create bot-control columns (AK-AN) with dropdowns.

Uses Google Sheets API via service account.
Run once: python scripts/setup_op_comments.py
"""
import json
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ---------- CONFIG ----------
SA_FILE_PATHS = [
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
    "./secrets/google-sa.json",
    str(Path.home() / "Desktop" / "Меню бота" / "секреты" / "secrets" / "google-sa.json"),
]

# Hardcoded fallback, can be overridden via env
SPREADSHEET_ID = os.environ.get(
    "GSHEET_SALES_SPREADSHEET_ID",
    "1i6fZi8TLC8ghtuRLZYkHt-3UsfoJ50Ng4EJuMMQXjN4",
)
SHEET_NAME = os.environ.get("GSHEET_SALES_TAB", "Отдел продаж")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ---------- COLUMN COMMENTS (0-based col index → note text) ----------
COLUMN_COMMENTS: dict[int, str] = {
    0: (
        "A — №\n"
        "Порядковый номер строки.\n"
        "Автоматическая нумерация для удобства навигации."
    ),
    1: (
        "B — В работу\n"
        "Дата перевода счёта в статус «В работе».\n"
        "Заполняется при подтверждении оплаты (через бот или вручную).\n"
        "Формат: ДД.ММ.ГГГГ"
    ),
    2: (
        "C — Менеджер\n"
        "Ответственный менеджер по данному счёту.\n"
        "Синхронизируется с ботом. При изменении в столбце AL → переназначение в боте."
    ),
    3: (
        "D — Бухг.ЭДО\n"
        "Статус электронного документооборота.\n"
        "Заполняется автоматически бухгалтером через бот.\n"
        "Значения: подписано / ожидание / не подписано\n\n"
        "═══ ДОКУМЕНТООБОРОТ ═══\n"
        "Бот отслеживает 4 типа документов:\n"
        "• Первичные ЭДО (docs_edo_signed) — электронная подпись счёта\n"
        "• Закрывающие ЭДО (edo_signed) — электронная подпись закрывающих\n"
        "• Первичные оригиналы (docs_originals_holder) — у ГД / у менеджера / нет\n"
        "• Закрывающие оригиналы (closing_originals_holder) — у ГД / у менеджера / нет\n\n"
        "Статус виден в карточках бухгалтерии:\n"
        "📋 П: ✅эдо ✅ГДориг | З: ⏳эдо ⏳ориг"
    ),
    4: (
        "E — Номер счёта / Контрагент\n"
        "⚠️ КЛЮЧЕВОЕ ПОЛЕ — уникальный идентификатор счёта.\n\n"
        "По этому номеру бот:\n"
        "• Находит счёт во всех операциях\n"
        "• Связывает задачи, платежи, материалы\n"
        "• Синхронизирует данные между листами\n\n"
        "Форматы: 1234-КВ, 1234-КИА, ЗМ-456 (кредитный)\n"
        "❌ НЕ МЕНЯТЬ формат без согласования!"
    ),
    5: (
        "F — Адрес объекта\n"
        "Адрес монтажа/доставки/замера.\n\n"
        "При изменении:\n"
        "• Автообновление в БД бота\n"
        "• Уведомление менеджеру и монтажнику (если назначен)"
    ),
    6: (
        "G — Ист. трафика\n"
        "Источник трафика / откуда пришёл клиент.\n"
        "Примеры: сайт, Авито, рекомендация, холодный звонок"
    ),
    7: (
        "H — Сроки (дни)\n"
        "Срок выполнения заказа в рабочих днях.\n\n"
        "При изменении:\n"
        "• Бот пересчитывает deadline_end_date\n"
        "• Менеджер получает уведомление\n"
        "• Индикация в боте: ✅ >7дн / ⚠️ ≤7дн / 🔴 просрочен"
    ),
    8: (
        "I — Дата оконч. (формула)\n"
        "Расчётная дата окончания = дата пост. + сроки.\n"
        "⚠️ ФОРМУЛА — не редактировать вручную!"
    ),
    9: (
        "J — Дата факт\n"
        "Фактическая дата завершения работ.\n"
        "Заполняется при закрытии счёта (через бот или вручную)."
    ),
    10: (
        "K — Сумма\n"
        "💰 Общая сумма счёта (₽).\n\n"
        "При изменении:\n"
        "• Автообновление в БД бота\n"
        "• Уведомление менеджеру: «Сумма изменена»\n"
        "• Пересчёт рентабельности и маржи"
    ),
    11: (
        "L — Сумма 1пл\n"
        "Сумма первого платежа / аванса (₽).\n"
        "Для контроля частичной оплаты."
    ),
    12: (
        "M — Расч. мат.\n"
        "Расчётная стоимость материалов (₽).\n"
        "Используется для План/Факт анализа и расчёта себестоимости.\n"
        "Факт берётся из дочерних счетов и оплат поставщикам."
    ),
    13: (
        "N — Установка\n"
        "Расчётная стоимость монтажных работ (₽).\n"
        "Для монтажника бот показывает: int(установка × 0.77) // 1000 × 1000"
    ),
    14: (
        "O — Грузчики\n"
        "Стоимость услуг грузчиков (₽)."
    ),
    15: (
        "P — Логистика\n"
        "Расчётная стоимость доставки (₽).\n\n"
        "Фактическая стоимость записывается ботом при\n"
        "оплате доставки через функцию «🚚 Оплата доставки»."
    ),
    16: (
        "Q — Прибыль\n"
        "Расчётная прибыль по счёту (₽)."
    ),
    17: (
        "R — НДС\n"
        "Сумма НДС."
    ),
    18: (
        "S — Нал.приб.\n"
        "Налогооблагаемая прибыль."
    ),
    19: (
        "T — Рент-ть расч\n"
        "Расчётная рентабельность (%).\n"
        "Формула: (прибыль / сумма) × 100"
    ),
    20: (
        "U — Рент-ть факт\n"
        "Фактическая рентабельность (%).\n"
        "Рассчитывается после закрытия с учётом всех расходов."
    ),
    21: (
        "V — Сумма допл\n"
        "Сумма доплаты по счёту (₽)."
    ),
    22: (
        "W — Допл подтв\n"
        "Подтверждение получения доплаты."
    ),
    23: (
        "X — Дата допл\n"
        "Дата поступления доплаты."
    ),
    24: (
        "Y — Оконч допл\n"
        "Срок окончания по доплате."
    ),
    25: (
        "Z — Дата оконч\n"
        "Дата окончания работ."
    ),
    26: (
        "AA — Долг\n"
        "Текущая задолженность клиента (₽).\n"
        "Формула: сумма - оплаты."
    ),
    27: (
        "AB — Договор\n"
        "Номер или статус договора.\n"
        "Значения: подписан / не подписан / №договора"
    ),
    28: (
        "AC — Закр.док\n"
        "Статус закрывающих документов.\n\n"
        "В боте бухгалтерия отслеживает документы через кнопку «✏️ Документы»:\n"
        "• ЭДО первичные — электронная подпись на счёт\n"
        "• ЭДО закрывающие — электронная подпись на акты/УПД\n"
        "• Оригиналы — бумажные документы (у ГД / у менеджера / нет)\n\n"
        "Менеджер получает задачу EDO_REQUEST при запросе документов."
    ),
    29: (
        "AD — Пояснения\n"
        "Примечания и комментарии по счёту.\n"
        "Свободное текстовое поле."
    ),
    30: (
        "AE — Агентское\n"
        "Агентское вознаграждение (₽).\n"
        "Учитывается в себестоимости."
    ),
    31: (
        "AF — Мен.ЗП\n"
        "Зарплата менеджера по данному счёту (₽).\n\n"
        "Менеджер запрашивает ЗП через бот (кнопка «💰 Запрос ЗП»).\n"
        "ГД одобряет/отклоняет. Статусы: не запрошена / запрошена / одобрена."
    ),
    32: (
        "AG — Запрос\n"
        "Статус текущего запроса по счёту."
    ),
    33: (
        "AH — тех\n"
        "Технические заметки."
    ),
    # --- Bot-control columns (AN=39, AO=40, AP=41, AQ=42) ---
    39: (
        "AN — Команда боту 🤖\n"
        "Выберите команду из выпадающего списка.\n"
        "После выбора команда отправляется боту и ячейка очищается автоматически.\n\n"
        "═══ КОМАНДЫ (требуют номер счёта в строке) ═══\n\n"
        "📩 Напомнить менеджеру\n"
        "   → Отправляет напоминание менеджеру в Telegram.\n\n"
        "📋 Запрос документов\n"
        "   → Создаёт задачу DOCS_REQUEST для менеджера.\n\n"
        "📊 Запрос КП\n"
        "   → Создаёт задачу QUOTE_REQUEST для менеджера.\n\n"
        "💰 Подтвердить оплату\n"
        "   → Переводит счёт в статус «В работе».\n\n"
        "🔨 В монтаж\n"
        "   → Назначает монтажника на счёт.\n\n"
        "📐 Запрос замера\n"
        "   → Создаёт заявку на замер.\n\n"
        "🚚 Оплата доставки\n"
        "   → Создаёт задачу на оплату доставки для ГД.\n\n"
        "🏁 Закрыть счёт\n"
        "   → Переводит счёт в статус «Закрытие».\n\n"
        "Результат всех команд приходит в Telegram-бот получателю."
    ),
    40: (
        "AO — Менеджер 👤\n"
        "Назначенный менеджер для этого счёта.\n\n"
        "Выберите из списка: КВ / КИА / НПН\n\n"
        "При смене менеджера бот:\n"
        "• Переназначает счёт в БД\n"
        "• Отправляет новому менеджеру: «Вам назначен счёт»\n"
        "• Отправляет старому: «Счёт переназначен»"
    ),
    41: (
        "AP — Приоритет ⚡\n"
        "Приоритет обработки счёта.\n\n"
        "🟢 — Обычный приоритет\n"
        "🟡 — Повышенный\n"
        "🔴 — СРОЧНО! → Менеджер получает уведомление «🔴 СРОЧНО» в бот"
    ),
    42: (
        "AQ — Комментарий РП 💬\n"
        "Текстовый комментарий от РП.\n\n"
        "При вводе текста:\n"
        "• Комментарий пересылается менеджеру в Telegram-бот\n"
        "• В сообщении: «💬 Комментарий от РП, Счёт: №...»\n"
        "• Ячейка НЕ очищается — комментарий остаётся для истории"
    ),
}

# Bot command dropdown values
BOT_COMMANDS = [
    "📩 Напомнить менеджеру",
    "📋 Запрос документов",
    "📊 Запрос КП",
    "💰 Подтвердить оплату",
    "🔨 В монтаж",
    "📐 Запрос замера",
    "🚚 Оплата доставки",
    "🏁 Закрыть счёт",
]

MANAGERS = ["КВ", "КИА", "НПН"]
PRIORITIES = ["🟢", "🟡", "🔴"]

# Column headers for new bot-control columns
BOT_HEADERS = {
    39: "Команда боту",
    40: "Менеджер",
    41: "Приоритет",
    42: "Комментарий РП",
}


def get_credentials():
    """Find and load service account credentials."""
    for path in SA_FILE_PATHS:
        if path and os.path.isfile(path):
            print(f"Using SA file: {path}")
            return Credentials.from_service_account_file(path, scopes=SCOPES)
    raise FileNotFoundError(
        "Service account JSON not found. Tried:\n" +
        "\n".join(f"  - {p}" for p in SA_FILE_PATHS if p)
    )


def col_letter(col_index: int) -> str:
    """Convert 0-based column index to letter (0→A, 25→Z, 26→AA)."""
    result = ""
    i = col_index
    while True:
        result = chr(ord("A") + i % 26) + result
        i = i // 26 - 1
        if i < 0:
            break
    return result


def main():
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheets = service.spreadsheets()

    # 1. Get spreadsheet info to find sheet ID
    print(f"\nSpreadsheet: {SPREADSHEET_ID}")
    print(f"Sheet: {SHEET_NAME}")

    sp_info = sheets.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_id = None
    for s in sp_info.get("sheets", []):
        if s["properties"]["title"] == SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
            break

    if sheet_id is None:
        print(f"ERROR: Sheet '{SHEET_NAME}' not found!")
        print("Available sheets:", [s["properties"]["title"] for s in sp_info.get("sheets", [])])
        sys.exit(1)

    print(f"Sheet ID: {sheet_id}")

    # 2. Read current header row to verify structure
    header_range = f"'{SHEET_NAME}'!A1:AN1"
    result = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=header_range,
    ).execute()
    headers = result.get("values", [[]])[0]
    print(f"\nCurrent headers ({len(headers)} columns):")
    for i, h in enumerate(headers):
        if h:
            print(f"  {col_letter(i)} ({i}): {h}")

    # 3. Add notes (comments) to header cells
    print("\n--- Adding column comments (notes) ---")
    requests = []

    for col_idx, note_text in COLUMN_COMMENTS.items():
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "rows": [{
                    "values": [{
                        "note": note_text,
                    }]
                }],
                "fields": "note",
            }
        })

    # 4. Set headers for bot-control columns (AK-AN) if not already set
    for col_idx, header_text in BOT_HEADERS.items():
        if col_idx >= len(headers) or not headers[col_idx]:
            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rows": [{
                        "values": [{
                            "userEnteredValue": {"stringValue": header_text},
                        }]
                    }],
                    "fields": "userEnteredValue",
                }
            })
            print(f"  Setting header {col_letter(col_idx)}: {header_text}")

    # 5. Add data validation (dropdowns) for bot-control columns
    # Get last row count
    row_count_result = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A:A",
    ).execute()
    last_row = len(row_count_result.get("values", []))
    data_rows = max(last_row, 200)  # At least 200 rows of dropdowns

    # Command dropdown (col AN = 39)
    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": data_rows,
                "startColumnIndex": 39,
                "endColumnIndex": 40,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": cmd} for cmd in BOT_COMMANDS],
                },
                "showCustomUi": True,
                "strict": True,
            },
        }
    })
    print(f"  Command dropdown: AN2:AN{data_rows} ({len(BOT_COMMANDS)} options)")

    # Manager dropdown (col AO = 40)
    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": data_rows,
                "startColumnIndex": 40,
                "endColumnIndex": 41,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": m} for m in MANAGERS],
                },
                "showCustomUi": True,
                "strict": True,
            },
        }
    })
    print(f"  Manager dropdown: AO2:AO{data_rows}")

    # Priority dropdown (col AP = 41)
    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": data_rows,
                "startColumnIndex": 41,
                "endColumnIndex": 42,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": p} for p in PRIORITIES],
                },
                "showCustomUi": True,
                "strict": True,
            },
        }
    })
    print(f"  Priority dropdown: AP2:AP{data_rows}")

    # 6. Execute all requests
    if requests:
        print(f"\nExecuting {len(requests)} API requests...")
        response = sheets.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests},
        ).execute()
        print(f"Done! {response.get('totalUpdatedCells', 'N/A')} cells updated.")
    else:
        print("\nNo changes needed.")

    # 7. Verify — re-read notes
    print("\n--- Verification ---")
    # Read a few notes back to confirm
    verify_range = f"'{SHEET_NAME}'!A1:AN1"
    verify_result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        ranges=[verify_range],
        includeGridData=True,
    ).execute()

    grid_data = verify_result["sheets"][0].get("data", [{}])[0]
    row_data = grid_data.get("rowData", [{}])[0]
    cells = row_data.get("values", [])

    notes_found = 0
    for i, cell in enumerate(cells):
        note = cell.get("note", "")
        if note:
            notes_found += 1
            first_line = note.split("\n")[0]
            # Safe print for Windows console
            safe_line = first_line.encode("ascii", "replace").decode("ascii")
            print(f"  OK {col_letter(i)}: {safe_line}")

    print(f"\nTotal notes set: {notes_found}")
    print(f"Expected: {len(COLUMN_COMMENTS)}")

    if notes_found >= len(COLUMN_COMMENTS) - 2:  # Allow small margin
        print("\nSUCCESS: Column comments added!")
    else:
        print("\nWARNING: Some notes may not have been set correctly.")


if __name__ == "__main__":
    main()
