"""
One-time migration: fill invoice data from the manager spreadsheet.
Run inside the Docker container after deploy.
"""
import sqlite3
from datetime import datetime

DB_PATH = "/app/data/bot.sqlite3"

# ── Existing invoices to UPDATE (matched by id) ─────────────────────
UPDATES = [
    {   # Row 1 → DB id=1
        "id": 1,
        "client_name": 'ООО "ПРАЙМ СТРОЙ"',
        "traffic_source": "Др.",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 1,
        "object_address": "г. Москва, Научный проезд 19",
        "receipt_date": "2026-01-13",
        "deadline_days": 13,
        "first_payment_amount": 179900,
        "estimated_materials": 112068,
        "estimated_installation": 44881,
        "estimated_loaders": 9000,
        "estimated_logistics": 24295,
        "nds_amount": 26135,
        "profit_tax": 8124,
        "rentability_calc": 7,
        "surcharge_amount": 0,
        "final_surcharge_amount": 77100,
        "final_surcharge_date": "2026-02-10",
        "outstanding_debt": 0,
        "contract_signed": "Эдо",
        "payment_terms": "Оплата 100%",
        "agent_fee": 0,
        "manager_zp_blank": 11220,
        "zp_manager_amount": 11220,
        "actual_completion_date": "2026-01-27",
        "npn_amount": 3520,
    },
    {   # Row 2 → DB id=2
        "id": 2,
        "client_name": "Александр",
        "traffic_source": "Др.",
        "contract_type": "credit",
        "is_credit": 1,
        "client_type": 1,
        "object_address": "г. Москва, Соколово-Мещеренская, 25",
        "receipt_date": "2026-01-16",
        "deadline_days": 18,
        "deadline_end_date": "2026-02-11",
        "first_payment_amount": 121000,
        "estimated_materials": 67742,
        "estimated_installation": 18526,
        "estimated_loaders": 4000,
        "estimated_logistics": 15000,
        "nds_amount": 0,
        "profit_tax": 0,
        "rentability_calc": 13,
        "final_surcharge_amount": 21000,
        "final_surcharge_date": "2026-02-09",
        "outstanding_debt": 0,
        "contract_signed": "Нет",
        "payment_terms": "Оплата 100%",
        "agent_fee": 0,
        "manager_zp_blank": 15000,
        "zp_manager_amount": 15000,
        "actual_completion_date": "2026-02-05",
        "npn_amount": 3850,
    },
    {   # Row 3 → DB id=3
        "id": 3,
        "client_name": 'АО "ПРОМТЕХ-Дубна"',
        "traffic_source": "Ав",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 1,
        "object_address": "г. Дубна, Проспект Науки, дом 14, к. 5",
        "receipt_date": "2026-01-20",
        "deadline_days": 12,
        "deadline_end_date": "2026-02-05",
        "first_payment_amount": 484000,
        "estimated_materials": 241640,
        "estimated_installation": 125447,
        "estimated_loaders": 34000,
        "estimated_logistics": 37000,
        "nds_amount": 65524,
        "profit_tax": 20278,
        "rentability_calc": 10,
        "final_surcharge_amount": 121000,
        "final_surcharge_date": "2026-02-13",
        "outstanding_debt": 0,
        "contract_signed": "Оригинал",
        "payment_terms": "Оплата 100%",
        "manager_zp_blank": 12320,
        "zp_manager_amount": 12320,
        "actual_completion_date": "2026-01-29",
        "npn_amount": 7810,
    },
    {   # Row 4 → DB id=4
        "id": 4,
        "client_name": "ИП Лисина",
        "traffic_source": "ТГ. Сайт",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 2,
        "object_address": "Московская обл., д. Захарово, ул. Заречная, 45А, стр. 11",
        "receipt_date": "2026-01-21",
        "deadline_days": 17,
        "deadline_end_date": "2026-02-13",
        "first_payment_amount": 205000,
        "estimated_materials": 58197,
        "estimated_installation": 37705,
        "estimated_loaders": 6000,
        "estimated_logistics": 18000,
        "nds_amount": 26473,
        "profit_tax": 5785,
        "rentability_calc": 9,
        "outstanding_debt": 0,
        "contract_signed": "ЭДО",
        "payment_terms": "Оплата 100%",
        "agent_fee": 29700,
        "manager_zp_blank": 2850,
        "zp_manager_amount": 2850,
        "actual_completion_date": "2026-02-06",
        "npn_amount": 1760,
    },
    {   # Row 7 → DB id=5 (2624-1КВ)
        "id": 5,
        "client_name": 'ООО АСГ ГРУПП',
        "traffic_source": "Др.",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 1,
        "object_address": "г. Москва, Долгоруковская 7",
        "receipt_date": "2026-02-06",
        "deadline_days": 17,
        "deadline_end_date": "2026-03-03",
        "first_payment_amount": 184000,
        "estimated_materials": 124647,
        "estimated_installation": 77917,
        "estimated_loaders": 6000,
        "estimated_logistics": 15000,
        "nds_amount": 28736,
        "profit_tax": 6340,
        "rentability_calc": 6,
        "outstanding_debt": 100000,
        "payment_terms": "Оплата 100%",
        "manager_zp_blank": 7370,
        "actual_completion_date": "2026-02-19",
        "npn_amount": 2310,
    },
    {   # Row 8 → DB id=6 (2625-1КВ)
        "id": 6,
        "client_name": 'ООО "ПРАЙМ СТРОЙ"',
        "traffic_source": "Др.",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 1,
        "object_address": "г. Москва, Научный проезд 19",
        "receipt_date": "2026-02-10",
        "deadline_days": 19,
        "deadline_end_date": "2026-03-09",
        "first_payment_amount": 111500,
        "estimated_materials": 82361,
        "estimated_installation": 29597,
        "estimated_loaders": 9000,
        "estimated_logistics": 24295,
        "nds_amount": 25361,
        "profit_tax": 10477,
        "rentability_calc": 10,
        "outstanding_debt": 111500,
        "payment_terms": "50/50",
        "manager_zp_blank": 14520,
        "actual_completion_date": "2026-03-02",
        "npn_amount": 4510,
    },
    {   # Row 9 → DB id=7 (2625-2КВ)
        "id": 7,
        "client_name": 'ООО СК "Спектр"',
        "traffic_source": "Сайт",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 2,
        "object_address": "г. Москва, ул. Усачева д. 33 стр.1",
        "receipt_date": "2026-02-13",
        "deadline_days": 17,
        "deadline_end_date": "2026-03-10",
        "first_payment_amount": 245600,
        "estimated_materials": 106699,
        "estimated_installation": 43531,
        "estimated_loaders": 6000,
        "estimated_logistics": 15000,
        "nds_amount": 36120,
        "profit_tax": 19930,
        "rentability_calc": 21,
        "outstanding_debt": 61400,
        "payment_terms": "80/20",
        "manager_zp_blank": 9570,
        "actual_completion_date": "2026-03-02",
        "npn_amount": 6050,
    },
    {   # Row 10 → DB id=8 (26226-1КВ)
        "id": 8,
        "client_name": 'ООО СК "Спектр"',
        "traffic_source": "Сайт",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 2,
        "object_address": "г. Москва, ул. Усачева д. 33 стр.1",
        "receipt_date": "2026-03-04",
        "deadline_days": 15,
        "deadline_end_date": "2026-03-25",
        "first_payment_amount": 187000,
        "estimated_materials": 75821,
        "estimated_installation": 25903,
        "estimated_loaders": 7000,
        "estimated_logistics": 22500,
        "nds_amount": 20049,
        "profit_tax": 7145,
        "rentability_calc": 11,
        "outstanding_debt": 0,
        "payment_terms": "Оплата 100%",
        "manager_zp_blank": 5000,
        "npn_amount": 3080,
    },
    {   # Row 11 → DB id=9 (26225-1КИА)
        "id": 9,
        "client_name": "ООО ГК СТРОЙКООПЕРАТИВ",
        "traffic_source": "Др.",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 1,
        "object_address": "г.Москва ул. Рабочая д.91 стр.3",
        "receipt_date": "2026-03-05",
        "deadline_days": 23,
        "deadline_end_date": "2026-04-07",
        "first_payment_amount": 806400,
        "estimated_materials": 365928,
        "estimated_installation": 129894,
        "estimated_loaders": 12000,
        "estimated_logistics": 30000,
        "nds_amount": 115783,
        "profit_tax": 60879,
        "rentability_calc": 14,
        "outstanding_debt": 0,
        "payment_terms": "80/20",
        "agent_fee": 50000,
        "manager_zp_blank": 80080,
        "npn_amount": 25410,
    },
]

# ── New invoices to INSERT (rows 5 and 6 from spreadsheet) ──────────
NEW_INVOICES = [
    {   # Row 5: КВ (Анна, 147,000) — ended
        "invoice_number": "КВ",
        "status": "ended",
        "client_name": "Анна",
        "traffic_source": "Ав",
        "contract_type": "bn",
        "is_credit": 0,
        "client_type": 2,
        "object_address": "г. Москва, Большая Якиманка д. 24",
        "receipt_date": "2026-01-23",
        "deadline_days": 11,
        "deadline_end_date": "2026-02-09",
        "amount": 147000,
        "first_payment_amount": 113000,
        "estimated_materials": 74957,
        "estimated_installation": 18558,
        "estimated_loaders": 2000,
        "estimated_logistics": 11000,
        "nds_amount": 0,
        "profit_tax": 0,
        "rentability_calc": 19,
        "final_surcharge_amount": 34000,
        "final_surcharge_date": "2026-02-18",
        "outstanding_debt": 0,
        "contract_signed": "Нет",
        "payment_terms": "Оплата 100%",
        "manager_zp_blank": 8000,
        "zp_manager_amount": 8000,
        "zp_manager_status": "approved",
        "zp_installer_status": "approved",
        "actual_completion_date": "2026-02-13",
        "npn_amount": 3960,
        "montazh_stage": "none",
    },
    {   # Row 6: КВ (Ирина, 465,000) — paid
        "invoice_number": "КВ",
        "status": "paid",
        "client_name": "Ирина",
        "traffic_source": "Тон",
        "client_type": 2,
        "object_address": "г. Дзержинский, Лесная 11",
        "receipt_date": "2026-02-06",
        "deadline_days": 15,
        "deadline_end_date": "2026-02-27",
        "amount": 465000,
        "first_payment_amount": 355000,
        "estimated_materials": 292031,
        "estimated_installation": 70867,
        "estimated_loaders": 8000,
        "estimated_logistics": 11000,
        "nds_amount": 0,
        "profit_tax": 0,
        "rentability_calc": 12,
        "final_surcharge_amount": 110000,
        "final_surcharge_date": "2026-03-04",
        "outstanding_debt": 110000,
        "payment_terms": "Оплата 100%",
        "manager_zp_blank": 19000,
        "zp_manager_amount": 19000,
        "actual_completion_date": "2026-02-27",
        "npn_amount": 9130,
        "montazh_stage": "in_work",
    },
]


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    now = datetime.utcnow().isoformat()

    # ── 1. Update existing invoices ──
    for inv in UPDATES:
        inv_id = inv.pop("id")
        fields = {k: v for k, v in inv.items() if v is not None}
        fields["updated_at"] = now
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [inv_id]
        conn.execute(f"UPDATE invoices SET {set_clause} WHERE id = ?", vals)
        print(f"  Updated id={inv_id}")

    # ── 2. Insert new invoices ──
    # Find KV manager for created_by
    cur = conn.execute(
        "SELECT telegram_id FROM users WHERE is_active = 1 "
        "AND (',' || lower(role) || ',') LIKE '%,manager_kv,%' "
        "AND (',' || lower(role) || ',') LIKE '%,manager,%' LIMIT 1"
    )
    row = cur.fetchone()
    kv_manager_id = row[0] if row else 0

    for inv in NEW_INVOICES:
        inv["created_by"] = kv_manager_id
        inv["creator_role"] = "manager_kv"
        inv["created_at"] = now
        inv["updated_at"] = now
        cols = list(inv.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        vals = [inv[c] for c in cols]
        conn.execute(f"INSERT INTO invoices ({col_names}) VALUES ({placeholders})", vals)
        print(f"  Inserted invoice_number={inv['invoice_number']}, amount={inv.get('amount')}")

    conn.commit()

    # ── 3. Verify ──
    cur = conn.execute(
        "SELECT id, invoice_number, client_name, amount, outstanding_debt, "
        "estimated_materials, estimated_installation, manager_zp_blank, npn_amount "
        "FROM invoices WHERE parent_invoice_id IS NULL ORDER BY id"
    )
    print("\n=== RESULT ===")
    for r in cur:
        print(dict(r))

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    run()
