from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from .config import load_config
from .db import Database
from .integrations.amocrm import AmoCRMService, AmoConfig
from .integrations.sheets import GoogleSheetsService, SheetsConfig
from .middlewares.keep_menu import KeepMenuMiddleware
from .middlewares.update_logger import UpdateLoggingMiddleware
from .middlewares.usage_audit import UsageAuditMiddleware
from .services.assignment import get_work_chat_id
from .services.integration_hub import IntegrationHub
from .services.notifier import Notifier
from .services.daily_sync import daily_sync_loop
from .services.lead_poller import lead_poller_loop
from .services.reminders import acceptance_reminders_loop, reminders_loop
from .services.sheet_commands import process_sheet_webhook
from .services.sheets_sync import import_from_source_sheet

from .handlers import (
    accounting_new,
    admin,
    chat_proxy,
    common,
    driver,
    gd,
    group_guard,
    installer_new,
    invoice_chat,
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
    dp.message.outer_middleware(KeepMenuMiddleware())
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
    dp.include_router(invoice_chat.router)
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

    # NOTE: Startup sheets sync removed — sync only via "Синхронизация данных" button
    log = logging.getLogger(__name__)

    # Daily auto-sync at 09:00 MSK (keyboards, deadlines, debts — NO Sheets import/export)
    daily_sync_task: asyncio.Task | None = asyncio.create_task(
        daily_sync_loop(
            db=db,
            notifier=notifier,
            config=config,
            integrations=integrations,
            target_hour=9,
            target_minute=0,
        )
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

    # --- Sheets webhook server (aiohttp) ---
    webhook_runner: web.AppRunner | None = None
    if config.sheets_webhook_secret:
        async def _handle_sheets_webhook(request: web.Request) -> web.Response:
            secret = request.headers.get("X-Webhook-Secret", "")
            if secret != config.sheets_webhook_secret:
                return web.Response(status=403, text="Forbidden")
            try:
                payload = await request.json()
            except Exception:
                return web.Response(status=400, text="Bad JSON")

            if not isinstance(payload, dict):
                return web.Response(status=400, text="Payload must be a JSON object")
            if not payload.get("sheet") and not payload.get("command") and not payload.get("invoice_number"):
                return web.Response(status=400, text="Missing required field: sheet, command, or invoice_number")

            # Process in background — return 200 immediately to avoid GAS timeout
            async def _process_in_bg(data: dict) -> None:
                try:
                    result = await process_sheet_webhook(
                        data=data,
                        db=db,
                        config=config,
                        notifier=notifier,
                        sheets_service=sheets_service,
                    )
                    log.info("Sheets webhook processed: %s", result)
                except Exception:
                    log.exception("Sheets webhook background processing error")

            asyncio.create_task(_process_in_bg(payload))
            return web.json_response({"status": "accepted"})

        import time as _time
        _start_ts = _time.monotonic()

        async def _health(request: web.Request) -> web.Response:
            """Health dashboard: DB, tasks queue, uptime."""
            try:
                uptime_sec = int(_time.monotonic() - _start_ts)
                hours, remainder = divmod(uptime_sec, 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h {minutes}m {seconds}s"

                # DB check
                cur = await db.conn.execute("SELECT COUNT(*) FROM invoices")
                inv_count = (await cur.fetchone())[0]
                cur2 = await db.conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE status IN ('open','in_progress')"
                )
                active_tasks = (await cur2.fetchone())[0]
                cur3 = await db.conn.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
                user_count = (await cur3.fetchone())[0]

                return web.json_response({
                    "status": "ok",
                    "uptime": uptime_str,
                    "db": "connected",
                    "invoices": inv_count,
                    "active_tasks": active_tasks,
                    "active_users": user_count,
                    "sheets_enabled": config.sheets_enabled,
                })
            except Exception as exc:
                return web.json_response(
                    {"status": "error", "detail": str(exc)}, status=500
                )

        webapp = web.Application()
        webapp.router.add_post("/webhooks/sheets", _handle_sheets_webhook)
        webapp.router.add_get("/health", _health)

        webhook_runner = web.AppRunner(webapp)
        await webhook_runner.setup()
        site = web.TCPSite(webhook_runner, "0.0.0.0", config.sheets_webhook_port)
        await site.start()
        log.info("Sheets webhook server started on port %d", config.sheets_webhook_port)

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
        if daily_sync_task:
            daily_sync_task.cancel()
            try:
                await daily_sync_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logging.exception("Daily sync task terminated with error")
        if webhook_runner:
            await webhook_runner.cleanup()
        await integrations.stop()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
