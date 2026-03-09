from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from .config import load_config
from .db import Database
from .integrations.amocrm import AmoCRMService, AmoConfig
from .integrations.sheets import GoogleSheetsService, SheetsConfig
from .middlewares.update_logger import UpdateLoggingMiddleware
from .middlewares.usage_audit import UsageAuditMiddleware
from .services.assignment import get_work_chat_id
from .services.integration_hub import IntegrationHub
from .services.notifier import Notifier
from .services.lead_poller import lead_poller_loop
from .services.reminders import acceptance_reminders_loop, reminders_loop

from .handlers import (
    accounting_new,
    admin,
    chat_proxy,
    common,
    driver,
    gd,
    group_guard,
    installer_new,
    leads,
    manager,
    manager_new,
    projects,
    rp_new,
    search,
    tasks,
    td,
    tinter,
    urgent,
    zamery,
)


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # We log update lifecycle ourselves (with context), so hide noisy duplicate lines.
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)


async def main() -> None:
    load_dotenv()
    setup_logging()

    config = load_config()

    # ensure db dir exists
    db_path = Path(config.db_path)
    if db_path.parent and str(db_path.parent) not in {".", ""}:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    db = Database(str(db_path))
    await db.connect()
    await db.init_schema()

    # Привязка счетов к менеджерам по маркировке в номере
    marker_map = {}
    if config.default_manager_kia_id:
        marker_map["КИА"] = config.default_manager_kia_id
    if config.default_manager_kv_id:
        marker_map["КВ"] = config.default_manager_kv_id
    if config.default_manager_npn_id:
        marker_map["НПН"] = config.default_manager_npn_id
    if marker_map:
        await db.assign_invoices_by_marker(marker_map)

    work_chat_id = await get_work_chat_id(db, config)

    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    notifier = Notifier(
        bot,
        work_chat_id=work_chat_id,
        workchat_events_enabled=bool(work_chat_id),
    )

    # Integrations
    sheets_service = None
    if config.sheets_enabled:
        if not config.gsheet_spreadsheet_id:
            raise RuntimeError("SHEETS_ENABLED=true but GSHEET_SPREADSHEET_ID not set")
        sheets_service = GoogleSheetsService(
            SheetsConfig(
                enabled=True,
                spreadsheet_id=config.gsheet_spreadsheet_id,
                projects_tab=config.gsheet_projects_tab,
                tasks_tab=config.gsheet_tasks_tab,
                invoices_tab=config.gsheet_invoices_tab,
                timezone_name=config.timezone,
                service_account_json=config.google_sa_json,
                service_account_file=config.google_sa_file,
            )
        )

    amocrm_service = None
    if config.amocrm_enabled:
        if not config.amocrm_base_url:
            raise RuntimeError("AMOCRM_ENABLED=true but AMOCRM_BASE_URL not set")

        has_access = bool(config.amocrm_access_token)
        has_refresh = bool(config.amocrm_refresh_token)
        has_oauth = all([config.amocrm_client_id, config.amocrm_client_secret, config.amocrm_redirect_uri])

        if not has_access and not (has_refresh and has_oauth):
            raise RuntimeError(
                "AMOCRM_ENABLED=true but token config is incomplete: "
                "set AMOCRM_ACCESS_TOKEN, or set AMOCRM_REFRESH_TOKEN + "
                "AMOCRM_CLIENT_ID/AMOCRM_CLIENT_SECRET/AMOCRM_REDIRECT_URI"
            )

        if has_refresh and not has_oauth:
            raise RuntimeError(
                "AMOCRM_REFRESH_TOKEN is set but OAuth credentials are missing: "
                "AMOCRM_CLIENT_ID/AMOCRM_CLIENT_SECRET/AMOCRM_REDIRECT_URI"
            )

        if has_access and not (has_refresh and has_oauth):
            logging.getLogger(__name__).warning(
                "amoCRM runs in access-token-only mode. It will work until token expires; "
                "set AMOCRM_REFRESH_TOKEN + OAuth credentials for auto-refresh."
            )

        amocrm_service = AmoCRMService(
            AmoConfig(
                enabled=True,
                base_url=config.amocrm_base_url,
                client_id=config.amocrm_client_id,
                client_secret=config.amocrm_client_secret,
                redirect_uri=config.amocrm_redirect_uri,
                access_token=config.amocrm_access_token,
                refresh_token=config.amocrm_refresh_token,
            ),
            db=db,
        )

    integrations = IntegrationHub(db=db, sheets=sheets_service, amocrm=amocrm_service)
    await integrations.start()

    dp = Dispatcher()
    dp.update.outer_middleware(UpdateLoggingMiddleware())
    dp.update.outer_middleware(UsageAuditMiddleware())
    dp.include_router(group_guard.router)
    dp.include_router(common.router)
    dp.include_router(admin.router)
    dp.include_router(search.router)
    dp.include_router(projects.router)

    # New role-specific routers
    dp.include_router(manager_new.router)
    dp.include_router(rp_new.router)
    dp.include_router(accounting_new.router)
    dp.include_router(installer_new.router)
    dp.include_router(zamery.router)

    # Legacy routers (kept: unique active handlers)
    dp.include_router(manager.router)     # ManagerProjectCb, DocsRequest, etc.
    dp.include_router(td.router)          # SupplierPayment, Подтверждение оплат
    dp.include_router(driver.router)      # Доставка выполнена
    dp.include_router(tinter.router)      # Тонировка выполнена
    dp.include_router(gd.router)
    dp.include_router(chat_proxy.router)
    dp.include_router(urgent.router)
    dp.include_router(leads.router)

    dp.include_router(tasks.router)

    reminder_task: asyncio.Task | None = None
    if config.reminders_enabled:
        reminder_task = asyncio.create_task(
            reminders_loop(
                db=db,
                notifier=notifier,
                timezone_name=config.timezone,
                remind_soon_minutes=config.remind_soon_minutes,
                remind_overdue_minutes=config.remind_overdue_minutes,
                interval_seconds=60,
            )
        )

    # 15-min acceptance reminders + 2h post-accept reminder
    acceptance_task: asyncio.Task | None = asyncio.create_task(
        acceptance_reminders_loop(db=db, notifier=notifier, interval_seconds=60)
    )

    lead_poller_task: asyncio.Task | None = None
    if config.amocrm_enabled and amocrm_service is not None:
        lead_poller_task = asyncio.create_task(
            lead_poller_loop(
                db=db,
                amocrm=amocrm_service,
                notifier=notifier,
                interval_seconds=30,
            )
        )

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            db=db,
            config=config,
            notifier=notifier,
            integrations=integrations,
        )
    finally:
        if lead_poller_task:
            lead_poller_task.cancel()
            try:
                await lead_poller_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logging.exception("Lead poller task terminated with error")
        if reminder_task:
            reminder_task.cancel()
            try:
                await reminder_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logging.exception("Reminder task terminated with error")
        if acceptance_task:
            acceptance_task.cancel()
            try:
                await acceptance_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logging.exception("Acceptance reminder task terminated with error")
        await integrations.stop()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
