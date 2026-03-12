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
    rp,
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
    marker_map: dict[str, int] = {}
    # Сначала из env, потом fallback на роли из БД
    for marker, env_id, role_name in [
        ("КИА", config.default_manager_kia_id, "manager_kia"),
        ("КВ", config.default_manager_kv_id, "manager_kv"),
        ("НПН", config.default_manager_npn_id, "manager_npn"),
    ]:
        if env_id:
            marker_map[marker] = env_id
        else:
            users = await db.find_users_by_role(role_name)
            # Предпочесть пользователя с базовой ролью «manager»
            users.sort(key=lambda u: (0 if "manager" in (u.role or "").split(",") else 1))
            if users:
                marker_map[marker] = users[0].telegram_id
    if marker_map:
        await db.assign_invoices_by_marker(marker_map)

    # One-time import: 8 zamery records as unpaid invoices
    zamery_uid = config.default_zamery_id
    if not zamery_uid:
        _zam_users = await db.find_users_by_role("zamery")
        # Предпочесть пользователя с ролью gd (ГД), иначе первого
        _zam_users.sort(key=lambda u: (0 if "gd" in (u.role or "").split(",") else 1))
        if _zam_users:
            zamery_uid = _zam_users[0].telegram_id
    if zamery_uid:
        _zamery_records = [
            {"invoice_number": "ЗМ-КВ-1", "object_address": "г. Москва, Енисейская 2 стр.2", "client_contact": "Константин 89857948959"},
            {"invoice_number": "ЗМ-КИА-1", "object_address": "г. Москва, Краснопресненская наб. 12 ЦМТ", "client_contact": "Юрий 89296880543"},
            {"invoice_number": "ЗМ-КВ-2", "object_address": "г. Москва, ул. Рабочая д.91 стр.3", "client_contact": "Илья 89255702601"},
            {"invoice_number": "ЗМ-НПН-1", "object_address": "г. Москва, ул. 2-я Звенигородская, д. 13, стр. 42", "client_contact": "Екатерина 8 926 588 19 84"},
            {"invoice_number": "ЗМ-КВ-3", "object_address": "г. Москва, наб. Туполева 17", "client_contact": "Валерий Анатольевич 8 953 742 02 31"},
            {"invoice_number": "ЗМ-КВ-4", "object_address": "г. Москва, пр-т Вернадского, д. 11/19", "client_contact": "Сюзанна 89152144332"},
            {"invoice_number": "ЗМ-КВ-5", "object_address": "г. Москва, ул. Лобачевского д.28А", "client_contact": "Евгений 8 903 324 42 87"},
            {"invoice_number": "ЗМ-КИА-2", "object_address": "г. Москва, ул. Мосфильмовская 88 корп 4 стр 1 кв. 742", "client_contact": "Петр +7 991 305-86-04"},
        ]
        imported = await db.import_zamery_invoices(_zamery_records, zamery_uid)
        if imported:
            logging.getLogger(__name__).info("Imported %d zamery records", imported)

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
                source_spreadsheet_id=config.gsheet_sales_spreadsheet_id,
                source_sheet_name=config.gsheet_sales_tab,
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
    dp.include_router(rp.router)          # InvoiceCreateSG, OrderMaterial, Delivery, etc.
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

    # --- Auto-sync from ОП sheet at startup + periodically ---
    async def _op_sync_loop(interval: int = 900) -> None:
        """Sync from source ОП sheet → SQLite every `interval` seconds."""
        while True:
            try:
                if integrations.sheets:
                    op_rows = await asyncio.to_thread(integrations.sheets.read_op_sheet_sync)
                    ok = 0
                    for row_data in op_rows:
                        try:
                            await db.import_invoice_from_sheet(row_data)
                            ok += 1
                        except Exception:
                            pass
                    if ok:
                        log.info("ОП auto-sync: imported/updated %d invoices", ok)
            except Exception as e:
                log.error("ОП auto-sync error: %s", e)
            await asyncio.sleep(interval)

    op_sync_task = asyncio.create_task(_op_sync_loop(interval=900))

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
        op_sync_task.cancel()
        try:
            await op_sync_task
        except (asyncio.CancelledError, Exception):
            pass
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
