#!/usr/bin/env python3
"""
Add column header comments (notes) to ALL sheets in "Отдел продаж" spreadsheet
and cell comments to rows 1-8 of "Органайзер" sheet.

Uses Google Sheets API via service account.
Run once: python scripts/setup_all_comments.py
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ---------- CONFIG ----------
SA_FILE_PATHS = [
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
    "./secrets/google-sa.json",
    str(Path.home() / "Desktop" / "Меню бота" / "секреты" / "secrets" / "google-sa.json"),
]

SPREADSHEET_ID = os.environ.get(
    "GSHEET_SALES_SPREADSHEET_ID",
    "1i6fZi8TLC8ghtuRLZYkHt-3UsfoJ50Ng4EJuMMQXjN4",
)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_credentials():
    for path in SA_FILE_PATHS:
        if path and os.path.isfile(path):
            return Credentials.from_service_account_file(path, scopes=SCOPES)
    raise FileNotFoundError("Service account JSON not found")


def col_letter(col_index: int) -> str:
    result = ""
    i = col_index
    while True:
        result = chr(ord("A") + i % 26) + result
        i = i // 26 - 1
        if i < 0:
            break
    return result


def make_note_request(sheet_id: int, row: int, col: int, note: str) -> dict:
    """Create an updateCells request to set a note on a single cell."""
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


# ===================================================================
# COMMENTS FOR EACH SHEET
# ===================================================================

# --- КВ / КИА (same structure, 32 cols) ---
KV_KIA_COMMENTS: dict[int, str] = {
    0: "A — Контрагент\nНаименование клиента / организации.",
    1: "B — Ист.трафика\nИсточник, откуда пришёл клиент.\nПримеры: сайт, Авито, рекомендация.",
    2: "C — Б.Н./Кред\n0 = кредитный, 1 = безналичный.\nИспользуется ботом для фильтрации.",
    3: "D — Свой/Атм\n1 = Свой клиент, 2 = Атмосфера.\nИсточник привлечения клиента.",
    4: "E — Номер счета\n⚠️ КЛЮЧЕВОЕ ПОЛЕ — уникальный идентификатор.\nФорматы: 1234-КВ, 1234-КИА, ЗМ-456 (кредитный).",
    5: "F — Адрес\nАдрес объекта монтажа/доставки.",
    6: "G — Дата пост.\nДата поступления счёта.\nФормат: ДД.ММ.ГГГГ",
    7: "H — Сроки\nСрок выполнения заказа в рабочих днях.",
    8: "I — Дата оконч.\nРасчётная дата окончания работ.\n⚠️ Формула — не редактировать.",
    9: "J — Сумма\nОбщая сумма счёта (₽).",
    10: "K — Сумма 1пл\nСумма первого платежа / аванса (₽).",
    11: "L — Расч.мат.\nРасчётная стоимость материалов (₽).",
    12: "M — Установка\nРасчётная стоимость монтажа (₽).",
    13: "N — Грузчики\nСтоимость услуг грузчиков (₽).",
    14: "O — Логистика\nРасчётная стоимость доставки (₽).",
    15: "P — Прибыль\nРасчётная прибыль по счёту (₽).",
    16: "Q — НДС\nСумма НДС.",
    17: "R — Нал.приб.\nНалогооблагаемая прибыль.",
    18: "S — Рент-ть расч\nРасчётная рентабельность (%).",
    19: "T — Рент-ть факт\nФактическая рентабельность (%).",
    20: "U — Сумма допл\nСумма доплаты по счёту (₽).",
    21: "V — Дата допл\nДата поступления доплаты.",
    22: "W — Оконч допл\nСрок окончания по доплате.",
    23: "X — Дата оконч\nДата окончания работ.",
    24: "Y — Долг\nТекущая задолженность (₽).\nФормула: сумма − оплаты.",
    25: "Z — Пояснения\nПримечания по счёту. Свободное текстовое поле.",
    26: "AA — Агентское\nАгентское вознаграждение (₽).",
    27: "AB — Мен.ЗП\nЗарплата менеджера по данному счёту (₽).",
    28: "AC — Запрос\nСтатус текущего запроса по счёту.",
    29: "AD — Выпл.Агент\nВыплаченное агентское вознаграждение.",
    30: "AE — Выпл.МенЗП\nВыплаченная зарплата менеджеру.",
    31: "AF — Дата выпл\nДата последней выплаты.",
}

# --- Общая (43 cols) ---
OBSHAYA_COMMENTS: dict[int, str] = {
    0: "A — №\nПорядковый номер строки.",
    1: "B — Контрагент\nНаименование клиента / организации.",
    2: "C — Б.Н./Кред\n0 = кредитный, 1 = безналичный.",
    3: "D — Номер счета\n⚠️ КЛЮЧЕВОЕ ПОЛЕ — уникальный идентификатор счёта.\nПо нему бот связывает все операции.",
    4: "E — Адрес\nАдрес объекта монтажа/доставки.",
    5: "F — Дата пост.\nДата поступления счёта.",
    6: "G — Сроки\nСрок выполнения заказа в рабочих днях.",
    7: "H — Дней ост.\nДней осталось до окончания срока.\nОтрицательное = просрочка.",
    8: "I — Дата оконч.\nРасчётная дата окончания.",
    9: "J — Дата Факт\nФактическая дата завершения работ.",
    10: "K — Сумма б/н\nСумма безналичного платежа.",
    11: "L — Сумма\nОбщая сумма счёта (₽).",
    12: "M — Сумма 1пл\nСумма первого платежа / аванса.",
    13: "N — Расч.мат.\nРасчётная стоимость материалов.",
    14: "O — Расч.мат.(ост)\nОстаток расчётных материалов.",
    15: "P — Установка\nРасчётная стоимость монтажа.",
    16: "Q — Грузч.расч.\nРасчётная стоимость грузчиков.",
    17: "R — Логист.расч.\nРасчётная стоимость логистики.",
    18: "S — Грузчики\nФактические затраты на грузчиков.",
    19: "T — Логистика\nФактические затраты на логистику.",
    20: "U — Агентское\nАгентское вознаграждение (₽).",
    21: "V — Мен.прибыль\nПрибыль менеджера.",
    22: "W — Запрос выпл.\nЗапрос на выплату ЗП менеджеру.",
    23: "X — Выпл.Агент\nВыплаченное агентское.",
    24: "Y — Выпл.МенЗП\nВыплаченная ЗП менеджеру.",
    25: "Z — Остаток ЗП\nОстаток невыплаченной ЗП.",
    26: "AA — Дата выпл.ЗП\nДата выплаты зарплаты.",
    27: "AB — Сумма допл.\nСумма доплаты по счёту.",
    28: "AC — Дата допл.\nДата поступления доплаты.",
    29: "AD — Оконч.допл.\nСрок окончания по доплате.",
    30: "AE — Дата оконч.допл.\nДата окончания по доплате.",
    31: "AF — Задолж.Б/Н\nЗадолженность по безналичному.",
    32: "AG — Задолж.Кред\nЗадолженность по кредиту.",
    33: "AH — Сумма долга\nОбщая сумма долга клиента.",
    34: "AI — Док-ты ЭДО\nСтатус электронного документооборота.\nОтслеживается ботом через бухгалтерию.",
    35: "AJ — НДС\nСумма НДС.",
    36: "AK — Нал.приб.Факт\nФактическая налогооблагаемая прибыль.",
    37: "AL — Остаток ЗП мон.\nОстаток ЗП монтажнику.",
    38: "AM — Устан.Факт\nФактическая стоимость монтажа.",
    39: "AN — Затр.матер.Факт\nФактические затраты на материалы.",
    40: "AO — Выручка расч.\nРасчётная выручка.",
    41: "AP — Выручка б/нал\nВыручка по безналичному.",
    42: "AQ — Выручка Кред\nВыручка по кредитным счетам.",
}

# --- Органайзер — COMPREHENSIVE cell comments for rows 1-8, ALL columns ---
# Structure: 7 object blocks + structural columns A, B, AK
# Object blocks: C-G (obj1, 5 cols), H-K (obj2), L-O (obj3), P-S (obj4),
#                T-W (obj5), X-AA (obj6), AB-AE (obj7) — each 4 cols
# Each block within rows: addr, invoice, notes, days, dates, deadlines, materials


def _generate_organizer_comments() -> list[tuple[int, int, str]]:
    """Generate cell comments for ALL columns (A-AK) × rows 1-8 of Органайзер."""
    comments: list[tuple[int, int, str]] = []

    # ---------- Column A (structural labels) ----------
    comments.append((0, 0, (
        "Адрес Объекта\n"
        "Заголовок строки 1.\n"
        "Объекты расположены горизонтально:\n"
        "каждый блок столбцов = один объект.\n"
        "В строке 1 объектных столбцов — срок (дни)."
    )))
    comments.append((1, 0, (
        "Строка 2 — Адреса\n"
        "Адреса объектов монтажа.\n"
        "Каждый объект в своём блоке столбцов."
    )))
    comments.append((2, 0, (
        "№ Счета / Вр.раб.\n"
        "Номер счёта и время работы.\n"
        "Напротив каждого объекта — номер счёта\n"
        "(напр. 26112-1КВ).\n"
        "По этому номеру бот идентифицирует счёт."
    )))
    comments.append((3, 0, (
        "Примечания\n"
        "Свободное текстовое поле для заметок\n"
        "по каждому объекту."
    )))
    comments.append((4, 0, (
        "График Работ\n"
        "Рабочие дни по каждому объекту.\n"
        "«День» = запланирован рабочий день.\n"
        "Пустая ячейка = выходной / нет работ."
    )))
    comments.append((5, 0, (
        "Текущая дата\n"
        "Обновляется автоматически.\n"
        "Используется для расчёта дней остатка."
    )))
    comments.append((6, 0, (
        "Строка 7 — Сроки\n"
        "Дней по договору для каждого объекта.\n"
        "Отрицательное число = просрочка."
    )))
    comments.append((7, 0, (
        "Мат. счета\n"
        "Счета на материалы для каждого объекта.\n"
        "Разбивка: Мет.1, Мет.2 (металл),\n"
        "Стек.1, Стек.2 (стекло), ЛДСП и др."
    )))

    # ---------- Column B (structural) ----------
    comments.append((7, 1, "Дн.нед\nДень недели.\nПривязан к дате в строке 6 (A6)."))

    # ---------- Object blocks ----------
    # Block definitions: (start_col, n_cols, block_name)
    blocks = [
        (2, 5, "Объект 1"),   # C-G
        (7, 4, "Объект 2"),   # H-K
        (11, 4, "Объект 3"),  # L-O
        (15, 4, "Объект 4"),  # P-S
        (19, 4, "Объект 5"),  # T-W
        (23, 4, "Объект 6"),  # X-AA
        (27, 4, "Объект 7"),  # AB-AE
    ]

    for start, n_cols, block_name in blocks:
        first = col_letter(start)
        last = col_letter(start + n_cols - 1)
        block_range = f"{first}-{last}"

        # Row 1 (index 0): Days/deadline count per object
        comments.append((0, start, (
            f"{col_letter(start)}1 — Срок (дни)\n"
            f"{block_name} ({block_range})\n"
            "Количество дней по договору."
        )))

        # Row 2 (index 1): Address
        comments.append((1, start, (
            f"{col_letter(start)}2 — Адрес объекта\n"
            f"{block_name} ({block_range})\n"
            "Адрес монтажа / доставки."
        )))

        # Row 3 (index 2): Invoice number
        comments.append((2, start, (
            f"{col_letter(start)}3 — № счёта\n"
            f"{block_name} ({block_range})\n"
            "Номер счёта (напр. 26112-1КВ).\n"
            "По этому номеру бот привязывает данные."
        )))

        # Row 4 (index 3): Notes
        comments.append((3, start, (
            f"{col_letter(start)}4 — Примечания\n"
            f"{block_name}\n"
            "Свободный текст — заметки по объекту."
        )))

        # Row 5 (index 4): Work days for ALL columns in block
        for offset in range(n_cols):
            c = start + offset
            comments.append((4, c, (
                f"{col_letter(c)}5 — Рабочий день\n"
                f"{block_name}, день {offset + 1}\n"
                "«День» = запланированы работы.\n"
                "Пустая ячейка = нет работ."
            )))

        # Row 6 (index 5): Dates — first col of block = label, rest = dates
        if n_cols >= 2:
            # First date column
            comments.append((5, start, (
                f"{col_letter(start)}6 — Дата оплаты\n"
                f"{block_name}\n"
                "Дата поступления первого платежа."
            )))
            # Label column (usually start+1 for 5-col or start itself for 4-col)
            label_col = start + 1 if n_cols == 5 else start
            if n_cols == 5:
                comments.append((5, start + 1, (
                    f"{col_letter(start + 1)}6 — Дата оплаты/Окончания\n"
                    f"{block_name}\n"
                    "Метка: дата оплаты и расч. дата окончания."
                )))
            # Remaining date columns
            for offset in range(2 if n_cols == 5 else 1, n_cols):
                c = start + offset
                comments.append((5, c, (
                    f"{col_letter(c)}6 — Дата\n"
                    f"{block_name}\n"
                    "Дата оплаты или окончания работ."
                )))

        # Row 7 (index 6): Days by contract / Ready
        if n_cols >= 2:
            # First date column in row 7
            comments.append((6, start, (
                f"{col_letter(start)}7 — Дата факт\n"
                f"{block_name}\n"
                "Фактическая дата завершения."
            )))
            label_col7 = start + 1 if n_cols == 5 else start
            if n_cols == 5:
                comments.append((6, start + 1, (
                    f"{col_letter(start + 1)}7 — Дней по договору / Готов!\n"
                    f"{block_name}\n"
                    "Метка: срок в днях.\n"
                    "Отрицательное число = просрочка."
                )))
            for offset in range(2 if n_cols == 5 else 1, n_cols):
                c = start + offset
                comments.append((6, c, (
                    f"{col_letter(c)}7 — Дней осталось\n"
                    f"{block_name}\n"
                    "Дни до дедлайна.\n"
                    "Отрицательное = просрочка."
                )))

        # Row 8 (index 7): Material types
        if n_cols == 5:
            mat_labels = ["ЛДСП", "Мет.1", "Мет.2", "Стек.1", "Стек.2"]
        else:
            mat_labels = ["Мет.1", "Мет.2", "Стек.1", "Стек.2"]
        for offset, mat in enumerate(mat_labels[:n_cols]):
            c = start + offset
            comments.append((7, c, (
                f"{col_letter(c)}8 — {mat}\n"
                f"{block_name}\n"
                f"Счёт на материал: {mat}.\n"
                "Номер дочернего счёта поставщику."
            )))

    # ---------- Column AK (days of week, rows 2-8) ----------
    days = [
        (1, "Пн — Понедельник"),
        (2, "Вт — Вторник"),
        (3, "Ср — Среда"),
        (4, "Чт — Четверг"),
        (5, "Пт — Пятница"),
        (6, "Сб — Суббота"),
        (7, "Вс — Воскресенье"),
    ]
    comments.append((0, 36, (
        "AK — Дни недели\n"
        "Вспомогательная колонка.\n"
        "Пн–Вс для графика работ."
    )))
    for row_idx, day_label in days:
        comments.append((row_idx, 36, (
            f"AK{row_idx + 1} — {day_label}\n"
            "Используется для графика работ."
        )))

    return comments


ORGANIZER_ALL_COMMENTS = _generate_organizer_comments()

# --- _Органайзер (бот) sheet (65 cols) ---
BOT_ORGANIZER_COMMENTS: dict[int, str] = {
    0: "A — №\nПорядковый номер.",
    1: "B — Номер счета\n⚠️ КЛЮЧЕВОЕ ПОЛЕ.\nУникальный идентификатор счёта в боте.",
    2: "C — Контрагент\nНаименование клиента.",
    3: "D — Адрес\nАдрес объекта.",
    4: "E — Тип\nТип менеджера: КВ / КИА / НПН.",
    5: "F — Дата пост.\nДата поступления счёта.",
    6: "G — Сроки\nСрок выполнения (дни).",
    7: "H — Дата оконч.\nРасчётная дата окончания.",
    8: "I — Дата Факт\nФактическая дата завершения.",
    9: "J — Сумма\nОбщая сумма счёта (₽).",
    10: "K — Долг\nТекущая задолженность.",
    11: "L — Этап\nТекущий этап работ.\nСинхронизируется с ботом.",
    12: "M — Бригада\nНазначенная бригада монтажников.",
    13: "N — Водитель\nНазначенный водитель.",
    14: "O — Дата дост.\nДата доставки материалов.",
    15: "P — Приоритет\nПриоритет обработки.\n🟢 обычный / 🟡 повышенный / 🔴 срочно",
    16: "Q — Комментарий\nКомментарии по объекту.",
    17: "R — Мет.№1\nЗаказ металла №1 (номер счёта поставщику).",
    18: "S — Мет.ст.1\nСтатус заказа металла №1.",
    19: "T — Мет.№2\nЗаказ металла №2.",
    20: "U — Мет.ст.2\nСтатус заказа металла №2.",
    21: "V — Стек.№1\nЗаказ стекла №1.",
    22: "W — Стек.ст.1\nСтатус заказа стекла №1.",
    23: "X — Стек.№2\nЗаказ стекла №2.",
    24: "Y — Стек.ст.2\nСтатус заказа стекла №2.",
    25: "Z — Жал.№\nЗаказ жалюзи (номер).",
    26: "AA — Жал.ст.\nСтатус заказа жалюзи.",
    27: "AB — ЛДСП №\nЗаказ ЛДСП (номер).",
    28: "AC — ЛДСП ст.\nСтатус заказа ЛДСП.",
    29: "AD — ГКЛ №\nЗаказ ГКЛ (номер).",
    30: "AE — ГКЛ ст.\nСтатус заказа ГКЛ.",
    31: "AF — Доп.№\nДополнительный заказ (номер).",
    32: "AG — Доп.ст.\nСтатус дополнительного заказа.",
    33: "AH — Январь\nДанные календаря (помесячно).\nКалендарные столбцы продолжаются далее.",
}

# --- Бланк заказа ---
BLANK_COMMENTS: dict[int, str] = {
    0: "A — САБ/КИА/НБА/КВ\nТип системы: САБ, КИА, НБА, КВ.\nВыбор влияет на расчёт.",
    1: "B — #\nНомер строки / позиции.",
    4: "E — Расчет каркасной перегородки\nЗаголовок раздела расчёта.\nСекция для расчёта каркасных перегородок.",
    12: "M — Min S=5m2\nМинимальная площадь заказа — 5 м².\nПри меньшей площади применяется мин.тариф.",
    15: "P — Менеджер\nМенеджер, создавший заказ.",
}

# --- Расчет (pricing calculations) ---
RASCHET_COMMENTS: dict[int, str] = {
    0: "A — Доп. К стоимости\nДополнительные наценки к стоимости.",
    1: "B — Доп. Дни к срокам\nДополнительные дни к срокам изготовления.",
    3: "D — Комплектующие Status Lite\nСписок комплектующих для системы Status Lite.\nЦены и количество рассчитываются автоматически.",
    10: "K — Ед.Измерения\nЕдиница измерения (м, м², шт).",
    11: "L — Кол-во I витраж\nКоличество для первого витража.",
    12: "M — Кол-во II витраж\nКоличество для второго витража.",
    13: "N — Цена за Ед.\nЦена за единицу измерения.",
    14: "O — Стоимость I витраж\nСтоимость первого витража.",
    15: "P — Стоимость II витраж\nСтоимость второго витража.",
    16: "Q — В Работе\nФлаг: позиция в работе.",
    22: "W — Комплектующие Forum\nСписок комплектующих для системы Forum.",
}

# --- Ценообразование ---
CENO_COMMENTS: dict[int, str] = {
    0: "A — Параметр\nНазвание параметра ценообразования.",
    1: "B — Значение\nЧисловое значение параметра.",
    2: "C — Ед.\nЕдиница измерения (м, ₽, %).",
    3: "D — Источник\nСсылка на ячейку-источник данных.\nПример: Расчет!AO2",
}

# --- Монтаж ---
MONTAZH_COMMENTS: dict[int, str] = {
    4: "E — Сегодня\nТекущая дата. Обновляется автоматически.",
    15: "P — Да/Нет\nФлаг завершения монтажа.",
}

# --- _Данные_КВ ---
DANNYE_KV_COMMENTS: dict[int, str] = {
    0: (
        "Данные импортированы из КВ\n"
        "⚠️ Системный лист — НЕ РЕДАКТИРОВАТЬ.\n"
        "Данные синхронизируются автоматически\n"
        "из листа «КВ» для использования в формулах."
    ),
}

# --- _Данные_КИА ---
DANNYE_KIA_COMMENTS: dict[int, str] = {
    0: (
        "Данные импортированы из КИА\n"
        "⚠️ Системный лист — НЕ РЕДАКТИРОВАТЬ.\n"
        "Данные синхронизируются автоматически\n"
        "из листа «КИА» для использования в формулах."
    ),
}

# --- _Праздники ---
PRAZDNIKI_COMMENTS: dict[int, str] = {
    0: "A — Дата\nДата праздника/выходного.\nИспользуется для расчёта рабочих дней в сроках.",
    1: "B — Описание\nНазвание праздника.\nСегодняшняя дата отображается автоматически.",
}

# --- _Органайзер (бот) first row header ---
# Uses _ORGANIZER_BOT already defined above as BOT_ORGANIZER_COMMENTS

# ===================================================================
# MAPPING: sheet_title → (sheet_comments_dict, is_header_only)
# For header-only: comments go to row=0 for each col_index
# ===================================================================
SHEET_COMMENTS_MAP: dict[str, dict[int, str]] = {
    "КВ": KV_KIA_COMMENTS,
    "КИА": KV_KIA_COMMENTS,
    "Общая": OBSHAYA_COMMENTS,
    "_Органайзер (бот)": BOT_ORGANIZER_COMMENTS,
    "Бланк заказа": BLANK_COMMENTS,
    "Расчет": RASCHET_COMMENTS,
    "Ценообразование": CENO_COMMENTS,
    "Монтаж": MONTAZH_COMMENTS,
    "_Данные_КВ": DANNYE_KV_COMMENTS,
    "_Данные_КИА": DANNYE_KIA_COMMENTS,
    "_Праздники": PRAZDNIKI_COMMENTS,
}


def main():
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheets_api = service.spreadsheets()

    # Get all sheets
    sp_info = sheets_api.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheet_map: dict[str, int] = {}
    for s in sp_info.get("sheets", []):
        title = s["properties"]["title"]
        sheet_map[title] = s["properties"]["sheetId"]

    safe_print(f"Spreadsheet: {sp_info['properties']['title']}")
    safe_print(f"Sheets found: {list(sheet_map.keys())}")

    requests = []
    stats = {}

    # 1. Header comments for each sheet in SHEET_COMMENTS_MAP
    for sheet_title, comments in SHEET_COMMENTS_MAP.items():
        if sheet_title not in sheet_map:
            safe_print(f"  SKIP: '{sheet_title}' not found")
            continue
        sid = sheet_map[sheet_title]
        count = 0
        for col_idx, note_text in comments.items():
            requests.append(make_note_request(sid, 0, col_idx, note_text))
            count += 1
        # For _Данные_КВ and _Данные_КИА, the header is in row 2 (index 1)
        if sheet_title in ("_Данные_КВ", "_Данные_КИА"):
            pass  # comment goes on row 0 cell A (already done above)
        stats[sheet_title] = count
        safe_print(f"  {sheet_title}: {count} header comments")

    # 2. Органайзер — comprehensive cell comments for ALL columns, rows 1-8
    if "Органайзер" in sheet_map:
        org_id = sheet_map["Органайзер"]
        count = 0
        for row, col, note in ORGANIZER_ALL_COMMENTS:
            requests.append(make_note_request(org_id, row, col, note))
            count += 1
        stats["Органайзер"] = count
        safe_print(f"  Органайзер: {count} cell comments (rows 1-8, all columns)")
    else:
        safe_print("  SKIP: 'Органайзер' not found")

    # 3. Execute
    safe_print(f"\nTotal requests: {len(requests)}")
    if not requests:
        safe_print("Nothing to do.")
        return

    # Batch in chunks of 100 (API limit safety)
    total_done = 0
    for i in range(0, len(requests), 100):
        chunk = requests[i:i+100]
        sheets_api.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": chunk},
        ).execute()
        total_done += len(chunk)
        safe_print(f"  Batch {i//100 + 1}: {len(chunk)} requests done ({total_done}/{len(requests)})")

    # 4. Summary
    safe_print("\n=== SUMMARY ===")
    total_notes = 0
    for sheet_title, count in stats.items():
        safe_print(f"  {sheet_title}: {count} comments")
        total_notes += count
    safe_print(f"\nTotal comments added: {total_notes}")
    safe_print("SUCCESS!")


def safe_print(text: str):
    """Print with safe encoding for Windows console."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))


if __name__ == "__main__":
    main()
