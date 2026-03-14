#!/usr/bin/env python3
"""
Add cell comments (notes) to rows 1-8 of the "TG Workflow Bot Data" spreadsheet
(Projects, Tasks, Invoices sheets).

Uses Google Sheets API via service account.
Run once: python scripts/setup_organizer_comments.py
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SA_FILE_PATHS = [
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
    "./secrets/google-sa.json",
    str(Path.home() / "Desktop" / "Меню бота" / "секреты" / "secrets" / "google-sa.json"),
]

SPREADSHEET_ID = "14hrBVQSrme8t-b01nOoomrh43n0AsB1xhFeSLy6VNaU"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_credentials():
    for path in SA_FILE_PATHS:
        if path and os.path.isfile(path):
            return Credentials.from_service_account_file(path, scopes=SCOPES)
    raise FileNotFoundError("Service account JSON not found")


def make_note_request(sheet_id: int, row: int, col: int, note: str) -> dict:
    return {
        "updateCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row,
                "endRowIndex": row + 1,
                "startColumnIndex": col,
                "endColumnIndex": col + 1,
            },
            "rows": [{"values": [{"note": note}]}],
            "fields": "note",
        }
    }


# --- Projects (12 cols) ---
PROJECTS_COMMENTS: dict[int, str] = {
    0: (
        "A — Код\n"
        "Уникальный код проекта.\n"
        "Формат: PRJ-YYYY-NNNNNN\n"
        "Генерируется ботом автоматически."
    ),
    1: (
        "B — Проект\n"
        "Название проекта / объекта.\n"
        "Обычно совпадает с адресом."
    ),
    2: "C — Адрес\nАдрес объекта монтажа.",
    3: "D — Клиент\nНаименование клиента / организации.",
    4: (
        "E — Сумма\n"
        "Общая сумма проекта (₽).\n"
        "Синхронизируется из данных счёта."
    ),
    5: (
        "F — Дедлайн\n"
        "Срок выполнения.\n"
        "Формат: ДД месяц (напр. '02 марта')."
    ),
    6: (
        "G — Статус\n"
        "Текущий статус проекта в боте.\n\n"
        "Возможные значения:\n"
        "• Новый\n"
        "• Ожидание оплаты\n"
        "• В работе\n"
        "• Счёт/документы отправлены\n"
        "• Закрытие\n"
        "• Архив"
    ),
    7: (
        "H — Менеджер (ID)\n"
        "Telegram ID менеджера.\n"
        "Числовой идентификатор пользователя."
    ),
    8: (
        "I — Менеджер\n"
        "Username менеджера в Telegram.\n"
        "Формат: @username"
    ),
    9: "J — Создан\nДата и время создания проекта.",
    10: "K — Обновлён\nДата и время последнего обновления.",
    11: "L — amo_lead_id\nID сделки в amoCRM.\nДля связи с CRM-системой.",
}

# --- Tasks (22 cols) ---
TASKS_COMMENTS: dict[int, str] = {
    0: "A — ID задачи\nУникальный номер задачи в БД бота.",
    1: (
        "B — Код проекта\n"
        "Привязка к проекту (PRJ-YYYY-NNNNNN).\n"
        "Пустое — задача без привязки к проекту\n"
        "(напр. «Срочно ГД»)."
    ),
    2: (
        "C — Тип задачи\n"
        "Категория задачи.\n\n"
        "Основные типы:\n"
        "• Запрос документов/счёта\n"
        "• Оплата подтверждение\n"
        "• Заказ материалов\n"
        "• Монтаж\n"
        "• Замер\n"
        "• Срочно ГД\n"
        "• Не срочно ГД\n"
        "• ЗП (зарплата)\n"
        "• ЭДО запрос\n"
        "• Отчёт за день"
    ),
    3: (
        "D — Статус\n"
        "Текущий статус задачи.\n\n"
        "• Открыта — ожидает принятия\n"
        "• В работе — принята исполнителем\n"
        "• Завершена — выполнена\n"
        "• Отклонена — отменена/отклонена"
    ),
    4: (
        "E — Назначена (ID)\n"
        "Telegram ID исполнителя задачи."
    ),
    5: "F — Создал (ID)\nTelegram ID создателя задачи.",
    6: "G — Срок\nДедлайн задачи (дата и время).",
    7: "H — Создана\nДата и время создания.",
    8: "I — Обновлена\nДата и время последнего обновления.",
    9: (
        "J — Комментарий\n"
        "Текст комментария / описание задачи.\n"
        "Заполняется при создании задачи."
    ),
    10: "K — Размеры/ТЗ\nТехническое задание или размеры.",
    11: (
        "L — Тип проблемы\n"
        "Категория проблемы (для задач типа\n"
        "«Проблема / Рекламация»)."
    ),
    12: "M — Документы\nПрикреплённые документы.",
    13: "N — Уточнение\nДополнительные уточнения.",
    14: "O — Сумма оплаты\nСумма платежа (₽), если задача связана с оплатой.",
    15: "P — Тип оплаты\nТип платежа: наличные, безнал, карта.",
    16: "Q — Этап оплаты\nЭтап оплаты: аванс, доплата, полная.",
    17: "R — Дата оплаты\nДата совершения платежа.",
    18: "S — № счёта\nНомер счёта, связанного с задачей.",
    19: (
        "T — Тип подписания\n"
        "Тип ЭДО подписания:\n"
        "• Подпись счёта\n"
        "• Закрывающие\n"
        "• УПД поставщика\n"
        "• Другое"
    ),
    20: "U — Источник\nОткуда создана задача (бот, webhook, таблица).",
    21: "V — Отправитель\nКто инициировал создание задачи.",
}

# --- Invoices (65 cols) ---
INVOICES_COMMENTS: dict[int, str] = {
    0: "A — Контрагент\nНаименование клиента.",
    1: "B — Ист.трафика\nИсточник трафика.",
    2: "C — Б.Н./Кред\n0 = кредитный, 1 = безналичный.",
    3: "D — Свой/Атм\n1 = Свой клиент, 2 = Атмосфера.",
    4: (
        "E — Номер счета\n"
        "⚠️ КЛЮЧЕВОЕ ПОЛЕ.\n"
        "Уникальный идентификатор счёта.\n"
        "Связывает данные между Отделом продаж и ботом."
    ),
    5: "F — Адрес\nАдрес объекта.",
    6: "G — Дата пост.\nДата поступления счёта.",
    7: "H — Сроки\nСрок выполнения (дни).",
    8: "I — Дата оконч.\nРасчётная дата окончания.",
    9: "J — Дата Факт\nФактическая дата завершения.",
    10: "K — Сумма\nОбщая сумма счёта (₽).",
    11: "L — Сумма 1пл\nСумма первого платежа.",
    12: "M — Расч.мат.\nРасчётная стоимость материалов.",
    13: "N — Установка\nРасчётная стоимость монтажа.",
    14: "O — Грузчики\nСтоимость грузчиков.",
    15: "P — Логистика\nСтоимость логистики.",
    16: "Q — Прибыль\nРасчётная прибыль.",
    17: "R — НДС\nСумма НДС.",
    18: "S — Нал.приб.\nНалогооблагаемая прибыль.",
    19: "T — Рент-ть расч\nРасчётная рентабельность (%).",
    20: "U — Рент-ть факт\nФактическая рентабельность (%).",
    21: "V — Сумма допл\nСумма доплаты.",
    22: "W — Дата допл\nДата доплаты.",
    23: "X — Оконч допл\nОкончание по доплате.",
    24: "Y — Дата оконч\nДата окончания.",
    25: "Z — Долг\nТекущая задолженность.",
    26: "AA — Пояснения\nПримечания по счёту.",
    27: "AB — Агентское\nАгентское вознаграждение.",
    28: "AC — Мен.ЗП\nЗП менеджера по счёту.",
    29: "AD — Запрос\nСтатус запроса.",
    30: "AE — Выпл.Агент\nВыплаченное агентское.",
    31: "AF — Выпл.МенЗП\nВыплаченная ЗП менеджеру.",
    32: "AG — Дата выпл\nДата последней выплаты.",
    33: "AH — НПН 10%\nНПН 10% комиссия.",
    34: "AI — Логистика НПН\nЛогистика по НПН.",
    35: "AJ — Дата лог.\nДата логистики.",
    36: "AK — Грузчики НПН\nГрузчики по НПН.",
    37: "AL — Дата груз.\nДата грузчиков.",
    38: "AM — Комментарии\nКомментарии по счёту.",
    39: (
        "AN — Бух.ЭДО\n"
        "Статус ЭДО документов.\n"
        "Обновляется бухгалтером через бот."
    ),
    40: "AO — В работу\nДата перевода в статус «В работе».",
    41: "AP — Менеджер\nОтветственный менеджер.",
    42: (
        "AQ — Статус\n"
        "Текущий статус счёта в боте.\n\n"
        "• new — новый\n"
        "• active — в работе\n"
        "• ending — закрытие\n"
        "• ended — закрыт\n"
        "• credit — кредитный"
    ),
    43: "AR — Роль менеджера\nРоль: КВ / КИА / НПН.",
    44: "AS — РП\nОтветственный руководитель проекта.",
    45: "AT — Поставщик\nПоставщик материалов (для дочерних счетов).",
    46: "AU — Тип материала\nТип: профиль / стекло / ЛДСП / жалюзи и т.д.",
    47: "AV — Родит. счёт\nID родительского счёта (для дочерних).",
    48: (
        "AW — Этап монтажа\n"
        "Текущий этап:\n"
        "• not_started\n"
        "• in_work\n"
        "• razmery_ok\n"
        "• invoice_ok"
    ),
    49: "AX — Монтажник ОК\nПодтверждение монтажника.",
    50: "AY — Монтажник (кто)\nID назначенного монтажника.",
    51: "AZ — Монтажник дата\nДата назначения монтажника.",
    52: "BA — ЭДО подписано\nСтатус подписания ЭДО.",
    53: "BB — ЭДО дата\nДата подписания ЭДО.",
    54: "BC — Долгов нет\nФлаг: все долги погашены.",
    55: "BD — Оригиналы\nСтатус бумажных документов.",
    56: "BE — Коммент. закрытия\nКомментарий при закрытии счёта.",
    57: "BF — ЗП Замерщик статус\nСтатус ЗП замерщика: not_requested / requested / approved.",
    58: "BG — ЗП Замерщик сумма\nСумма ЗП замерщика (₽).",
    59: "BH — ЗП Монтажник статус\nСтатус ЗП монтажника.",
    60: "BI — ЗП Монтажник сумма\nСумма ЗП монтажника (₽).",
    61: "BJ — ЗП Менеджер статус\nСтатус ЗП менеджера.",
    62: "BK — ЗП Менеджер сумма\nСумма ЗП менеджера (₽).",
    63: "BL — Создан\nДата создания записи в боте.",
    64: "BM — Обновлён\nДата последнего обновления.",
}

SHEET_COMMENTS = {
    "Projects": PROJECTS_COMMENTS,
    "Tasks": TASKS_COMMENTS,
    "Invoices": INVOICES_COMMENTS,
}


def safe_print(text: str):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))


def main():
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheets_api = service.spreadsheets()

    sp_info = sheets_api.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_map: dict[str, int] = {}
    for s in sp_info.get("sheets", []):
        title = s["properties"]["title"]
        sheet_map[title] = s["properties"]["sheetId"]

    safe_print(f"Spreadsheet: {sp_info['properties']['title']}")
    safe_print(f"Sheets: {list(sheet_map.keys())}")

    requests = []
    stats = {}

    for sheet_title, comments in SHEET_COMMENTS.items():
        if sheet_title not in sheet_map:
            safe_print(f"  SKIP: '{sheet_title}' not found")
            continue
        sid = sheet_map[sheet_title]
        count = 0
        for col_idx, note_text in comments.items():
            requests.append(make_note_request(sid, 0, col_idx, note_text))
            count += 1
        stats[sheet_title] = count
        safe_print(f"  {sheet_title}: {count} header comments")

    safe_print(f"\nTotal requests: {len(requests)}")
    if not requests:
        safe_print("Nothing to do.")
        return

    for i in range(0, len(requests), 100):
        chunk = requests[i:i+100]
        sheets_api.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": chunk},
        ).execute()
        safe_print(f"  Batch {i//100 + 1}: {len(chunk)} requests done")

    safe_print("\n=== SUMMARY ===")
    total = 0
    for sheet_title, count in stats.items():
        safe_print(f"  {sheet_title}: {count} comments")
        total += count
    safe_print(f"\nTotal: {total} comments added")
    safe_print("SUCCESS!")


if __name__ == "__main__":
    main()
