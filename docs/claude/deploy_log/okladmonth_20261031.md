# Оклад РП — выбор месяца (owner 31.10) — деплой 31.10.2026

ТЗ: `D:\Бот+Гидра\Меню бота\HANDOFF_ТЗ_оклад_РП_выбор_месяца_20261031.md`.

## Модель (решения owner 31.10)
- Оклад месяца = тело 60 000 (`RP_OKLAD_MONTHLY`), б/н-форма ×1,1.
- Янв–июнь 2026 закрыты константой (`RP_OKLAD_PREBOT_CLOSED_THROUGH`) — платили вне бота.
- Июль+ closed = Σ БК «Оклад РП%» (cashless+other, обе колонки описаний) + Σ переводов-в-аванс
  (`RP_OKLAD_ADVANCE_DESC`). remaining = 60 000 − closed. state: paid/partial/open.
- РП выбирает любой месяц года: запрос остатка у ГД или списание из кошелька аванса.
- Списание из кошелька: зачёт 1:1 телом, кред-строка БК «Оклад РП …» + гашение
  `oklad_offset` (wallet_role='rp').
- ГД-выплата: remaining-based (частичный месяц НЕ блокирует доплату), зачёт-аванса
  при deduct пишет отдельную кред-строку «(аванс)» → Σ БК по месяцу = полный остаток.

## Изменённые файлы
- `app/db.py` — константы, `get_rp_oklad_month_state`, `oklad_override` в
  `get_rp_oklad_advance_offset`, remaining-based `record_rp_salary_payment`
  (+кред-строка «(аванс)»), новый `record_rp_oklad_from_wallet` (атомарно).
- `app/callbacks.py` — новый `RpOkladMonthCb(prefix="rp_okl_m", action, year, month)`.
- `app/handlers/rp.py` — пикеры месяцев (req/recv, ⬛/◐ по 2 в ряд), хендлеры выбора,
  recv-флоу = списание из кошелька (старый attach → stale-гард), remaining-based
  запрос ГД (payload amount = остаток тела, amount_bn = ×1,1).
- `app/handlers/td.py` — remaining-based экран подтверждения ГД, гарды
  (remaining ≤ 0 → «месяц уже закрыт»; частичное НЕ блокирует).
- `app/rp_start_card.py` — блок «Оклад» из `get_rp_oklad_month_state` (closed),
  журналы офиса в этом месте больше не участвуют.

## Функтест (на копии БД, /tmp/okladmonth_ft.sqlite3)
PASS=27 FAIL=0: янв–июнь константа; июль 134 000 / авг 107 000 / сен 60 000 = Σ БК;
окт open 60 000; топап 21 000 → списание 21 000 → partial (ост. 39 000); ГД-доплата
б/н base=39 000, oklad=42 900 → paid; все гарды (ValueError при пустом кошельке,
OkladAlreadyPaidError × 2).

## Деплой
- Бэкапы: `bot.sqlite3(.wal/.shm).bak-pre-okladmonth-20261031-071859` (в /app/data).
- Стенд: `/root/_okladmonth_stand/`. Грабля CRLF: sed через cmd-обёртку НЕ сработал —
  конвертация `tr -d '\r'`, повторный функтест PASS=27, рестарт.
- health=200, Traceback=0.

## Известное
- Июль/август closed > 60 000 (реальные доплаты) — remaining=0, paid. Ожидаемо.
- `record_rp_oklad_received` (старая фиксация факта) и `_oklad_paid_months`/
  `_oklad_bot_months` — больше не вызываются из хендлеров, оставлены в коде.
- GitHub-пуш: ждёт доступа owner (в очереди fbbaba8, aafbb98, a001929 + этот коммит).
