# -*- coding: utf-8 -*-
"""Trigger sheets sync manually."""
import asyncio
import sys
sys.path.insert(0, "/app")

from app.config import load_config
from app.db import Database
from app.integrations.sheets import GoogleSheetsService, SheetsConfig
from app.integrations.amocrm import AmoCRMService, AmoConfig
from app.services.sheets_sync import export_to_sheets


async def main():
    cfg = load_config()
    db = Database(cfg.db_path)
    await db.connect()

    sheets = GoogleSheetsService(
        SheetsConfig(
            enabled=True,
            spreadsheet_id=cfg.gsheet_spreadsheet_id,
            projects_tab=cfg.gsheet_projects_tab,
            tasks_tab=cfg.gsheet_tasks_tab,
            invoices_tab=cfg.gsheet_invoices_tab,
            timezone_name=cfg.timezone,
            service_account_json=cfg.google_sa_json,
            service_account_file=cfg.google_sa_file,
            source_spreadsheet_id=cfg.gsheet_sales_spreadsheet_id,
            source_sheet_name=cfg.gsheet_sales_tab,
        )
    )

    amo_cfg = AmoConfig(
        enabled=cfg.amocrm_enabled, base_url=cfg.amocrm_base_url,
        client_id=cfg.amocrm_client_id, client_secret=cfg.amocrm_client_secret,
        redirect_uri=cfg.amocrm_redirect_uri, access_token=cfg.amocrm_access_token,
        refresh_token=cfg.amocrm_refresh_token,
    )
    amocrm = AmoCRMService(amo_cfg, db)
    await amocrm.start()

    result = await export_to_sheets(
        db, sheets,
        include_invoice_cost=True,
        sync_invoices=False,
        amocrm_user_map=cfg.amocrm_user_map,
        amocrm=amocrm,
    )
    print("Sync result: %s" % result)

    await amocrm.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
