"""
One-time import: Google Sheet "Отдел продаж" → bot SQLite DB.

Usage:
    cd /root/tg_workflow_bot   (or local clone)
    python -m scripts.import_sales_sheet

Reads from GSHEET_SALES_SPREADSHEET_ID / GSHEET_SALES_TAB,
parses rows, determines invoice status, inserts into invoices table.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

from app.db import Database
from app.enums import InvoiceStatus

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _parse_number(text: str) -> float:
    """Parse Russian-formatted number: '257,000' or '257 000' → 257000.0."""
    if not text or not text.strip():
        return 0.0
    t = text.strip().replace("\u00a0", "").replace(" ", "").replace(",", "")
    t = t.replace("%", "").replace("₽", "").replace("\xa0", "")
    try:
        return float(t)
    except ValueError:
        return 0.0


def _parse_int_safe(text: str) -> int | None:
    if not text or not text.strip():
        return None
    try:
        return int(text.strip())
    except ValueError:
        return None


def _parse_date_cell(text: str) -> str | None:
    """Parse DD.MM.YYYY or serial date → ISO date string."""
    if not text or not text.strip():
        return None
    t = text.strip()
    # DD.MM.YYYY
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            return None
    # Google Sheets serial date (number of days since 1899-12-30)
    try:
        serial = float(t)
        if 40000 < serial < 60000:
            from datetime import timedelta
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=serial)
            return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def _determine_status(bn_kred: str, data_fakt: str, dolg: float) -> tuple[str, bool]:
    """Determine InvoiceStatus and is_credit from sheet data."""
    is_credit = bn_kred.strip() == "0"
    if is_credit:
        return InvoiceStatus.CREDIT, True
    if data_fakt:
        if dolg > 0:
            return InvoiceStatus.PAID, False
        return InvoiceStatus.ENDED, False
    return InvoiceStatus.IN_PROGRESS, False


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main() -> None:
    # Load .env
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        alt = Path.home() / "Desktop" / "Меню бота" / "секреты" / "env.dat"
        if alt.exists():
            env_path = alt
    load_dotenv(env_path)

    # Config
    sales_sheet_id = os.getenv(
        "GSHEET_SALES_SPREADSHEET_ID",
        "1i6fZi8TLC8ghtuRLZYkHt-3UsfoJ50Ng4EJuMMQXjN4",
    )
    sales_tab = os.getenv("GSHEET_SALES_TAB", "Отдел продаж")
    db_path = os.getenv("DB_PATH", "data/bot.sqlite3")

    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not sa_file:
        sa_file = str(
            Path.home() / "Desktop" / "Меню бота" / "секреты" / "secrets" / "google-sa.json"
        )

    default_gd_id = int(os.getenv("DEFAULT_GD_ID", "0") or "0")

    print(f"Sheet: {sales_sheet_id} / tab: {sales_tab}")
    print(f"DB: {db_path}")
    print(f"SA: {sa_file}")
    print()

    # Google Sheets
    creds = Credentials.from_service_account_file(
        sa_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sales_sheet_id)
    ws = sh.worksheet(sales_tab)
    all_rows = ws.get_all_values()

    print(f"Total rows in sheet: {len(all_rows)}")

    # Parse header (row index 5)
    if len(all_rows) < 7:
        print("ERROR: sheet has too few rows")
        return

    headers = all_rows[5]
    print(f"Header cols: {len(headers)}")

    # Open DB
    db = Database(db_path)
    await db.connect()
    await db.init_schema()

    imported = 0
    updated = 0
    skipped = 0

    for row_idx in range(6, len(all_rows)):
        row = all_rows[row_idx]

        # Skip separators and empty rows
        first_cell = (row[0] if row else "").strip()
        if not first_cell or first_cell.startswith("───") or first_cell.startswith("ИТОГО"):
            continue

        # Require Контрагент (col 4)
        kontragent = (row[4] if len(row) > 4 else "").strip()
        if not kontragent:
            continue

        # Require Номер счета (col 8)
        nomer = (row[8] if len(row) > 8 else "").strip()
        if not nomer:
            continue

        # Parse fields
        address = (row[9] if len(row) > 9 else "").strip()
        amount = _parse_number(row[14] if len(row) > 14 else "")
        first_pay = _parse_number(row[15] if len(row) > 15 else "") or None
        receipt_dt = _parse_date_cell(row[10] if len(row) > 10 else "")
        deadline_d = _parse_int_safe(row[11] if len(row) > 11 else "")
        data_fakt = _parse_date_cell(row[13] if len(row) > 13 else "")
        bn_kred = (row[6] if len(row) > 6 else "").strip()
        dolg = _parse_number(row[30] if len(row) > 30 else "")
        contract = (row[31] if len(row) > 31 else "").strip()
        closing_docs = (row[32] if len(row) > 32 else "").strip()
        poyasneniya = (row[33] if len(row) > 33 else "").strip()
        traffic = (row[5] if len(row) > 5 else "").strip()

        status, is_credit = _determine_status(bn_kred, data_fakt or "", dolg)
        existing_before_import = await db.get_invoice_by_number(nomer)

        # Creator: use GD as default creator for imported invoices
        creator_id = default_gd_id if default_gd_id else 0
        # Determine role from invoice number
        if "КИА" in nomer.upper():
            creator_role = "manager_kia"
        elif "КВ" in nomer.upper():
            creator_role = "manager_kv"
        else:
            creator_role = "manager_npn"

        try:
            inv_id = await db.import_invoice_from_sheet(
                invoice_number=nomer,
                created_by=creator_id,
                creator_role=creator_role,
                status=status,
                object_address=address,
                amount=amount,
                is_credit=is_credit,
                client_name=kontragent,
                traffic_source=traffic,
                receipt_date=receipt_dt,
                deadline_days=deadline_d,
                actual_completion_date=data_fakt,
                first_payment_amount=first_pay,
                outstanding_debt=dolg if dolg > 0 else None,
                contract_type=contract or None,
                closing_docs_status=closing_docs or None,
                payment_terms=poyasneniya or None,
                description=kontragent,
            )
            action = "UPD" if existing_before_import else "NEW"

            if action == "NEW":
                imported += 1
            else:
                updated += 1

            print(
                f"  [{action}] #{inv_id} {nomer:20s} | {kontragent:30s} | "
                f"{amount:>12,.0f}₽ | {status:15s} | {receipt_dt or '-'}"
            )
        except Exception as e:
            print(f"  [ERR] {nomer}: {e}")
            skipped += 1

    await db.close()

    print(f"\n{'='*60}")
    print(f"Imported: {imported} | Updated: {updated} | Skipped: {skipped}")
    print(f"Total: {imported + updated + skipped}")


if __name__ == "__main__":
    asyncio.run(main())
