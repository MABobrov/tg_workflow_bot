from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.types import ReplyKeyboardRemove
from dotenv import load_dotenv

from .config import load_config
from .db import Database
from .integrations.amocrm import AmoCRMService, AmoConfig
from .integrations.sheets import GoogleSheetsService, SheetsConfig
from .integrations.minio_storage import MinioConfig, MinioStorage
from .middlewares.keep_menu import KeepMenuMiddleware
from .middlewares.quiet_hours import QuietHoursMiddleware
from .middlewares.update_logger import UpdateLoggingMiddleware
from .middlewares.usage_audit import UsageAuditMiddleware
from .services.assignment import get_work_chat_id
from .services.integration_hub import IntegrationHub
from .services.notifier import Notifier
from .services.daily_sync import daily_sync_loop
from .services.lead_poller import lead_poller_loop
from .services.reminders import (
    acceptance_reminders_loop,
    installer_acceptance_reminders_loop,
    reminders_loop,
)
from .services.sheet_commands import process_sheet_webhook
from .services.sheets_sync import import_from_source_sheet

from .handlers import (
    accounting_new,
    admin,
    chat_proxy,
    common,
    driver,
    fallback,
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


def _task_exception_cb(task: asyncio.Task) -> None:
    """Log unhandled exceptions from background tasks instead of silently dropping them."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logging.getLogger(__name__).error(
            "Background task %s failed: %s", task.get_name(), exc, exc_info=exc,
        )


async def main() -> None:
    load_dotenv()
    setup_logging()
    log = logging.getLogger(__name__)

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

    # zamery one-time import removed — records were test data

    work_chat_id = await get_work_chat_id(db, config)

    _proxy_url = os.environ.get("TG_PROXY_URL", "socks5://172.19.0.1:40001")
    _session = AiohttpSession(proxy=_proxy_url) if _proxy_url else None
    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=_session,
    )
    # «Тихие часы»: пуши без звука 22:00–09:00 МСК. Session-middleware ловит
    # ЛЮБУЮ отправку (через Notifier и прямые bot.send_*), см.
    # middlewares/quiet_hours.py.
    bot.session.middleware(QuietHoursMiddleware())
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

    # Virtual Manager (Claude Haiku 4.5 first-line agent for amoCRM Chat).
    # Requires amoCRM enabled (uses its OAuth client to update leads / add notes)
    # plus an Anthropic API key. Chat-channel credentials are checked at send time.
    # NOTE: anthropic SDK is imported lazily so the bot still boots if VM is disabled
    # and the SDK isn't installed locally.
    virtual_manager = None  # type: ignore[var-annotated]
    if config.virtual_manager_enabled:
        if not config.anthropic_api_key:
            log.warning(
                "VIRTUAL_MANAGER_ENABLED=true but ANTHROPIC_API_KEY is missing — disabled."
            )
        elif amocrm_service is None:
            log.warning(
                "VIRTUAL_MANAGER_ENABLED=true but amoCRM is not enabled — disabled. "
                "Virtual manager needs the amoCRM OAuth client to update leads."
            )
        else:
            try:
                from .integrations.anthropic_client import AnthropicClient
                from .services.conversation_store import ConversationStore
                from .services.virtual_manager import VirtualManager
            except ImportError as exc:
                log.error(
                    "VIRTUAL_MANAGER_ENABLED=true but anthropic SDK not installed: %s. "
                    "Run `pip install -r requirements.txt` to enable.", exc,
                )
            else:
                anthropic_client = AnthropicClient(
                    api_key=config.anthropic_api_key,
                    model=config.claude_model,
                    max_tokens=config.vm_max_tokens,
                )
                virtual_manager = VirtualManager(
                    cfg=config,
                    amocrm=amocrm_service,
                    anthropic_client=anthropic_client,
                    store=ConversationStore(db),
                    notifier=notifier,
                )
                log.info(
                    "Virtual manager enabled (model=%s, debounce=%ds, history=%d)",
                    config.claude_model, config.vm_debounce_seconds, config.vm_history_limit,
                )

    storage = MinioStorage(MinioConfig(
        enabled=config.minio_enabled,
        endpoint=config.minio_endpoint or "",
        access_key=config.minio_access_key or "",
        secret_key=config.minio_secret_key or "",
        bucket=config.minio_bucket,
        secure=config.minio_secure,
    ))
    if storage.enabled:
        ok = await storage.ensure_bucket()
        log.info("MinIO storage enabled (bucket=%s, endpoint=%s, ready=%s)",
                 config.minio_bucket, config.minio_endpoint, ok)
    else:
        log.info("MinIO storage DISABLED (set MINIO_ENABLED=true to activate)")

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

    # Self-healing fallback — ПОСЛЕДНИМ: ловит осиротевшие меню-кнопки без
    # состояния (после рестарта бота in-memory FSM обнуляется) и возвращает в
    # корневое меню вместо тишины. См. handlers/fallback.py.
    dp.include_router(fallback.router)

    # -- Background tasks & webhook server --
    # Keep references to all background tasks for proper cleanup.
    background_tasks: list[asyncio.Task] = []
    # Hold strong refs to fire-and-forget webhook processing tasks so they aren't GC'd.
    _webhook_bg_tasks: set[asyncio.Task] = set()

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
            ),
            name="reminders_loop",
        )
        reminder_task.add_done_callback(_task_exception_cb)
        background_tasks.append(reminder_task)

    # 15-min acceptance reminders + 2h post-accept reminder (все роли кроме installer)
    acceptance_task: asyncio.Task | None = asyncio.create_task(
        acceptance_reminders_loop(db=db, notifier=notifier, timezone_name=config.timezone, interval_seconds=60),
        name="acceptance_reminders_loop",
    )
    acceptance_task.add_done_callback(_task_exception_cb)
    background_tasks.append(acceptance_task)

    # Installer-only: aggressive cadence (10 min / 60 min) + MSK quiet hours
    installer_acceptance_task: asyncio.Task | None = asyncio.create_task(
        installer_acceptance_reminders_loop(
            db=db,
            notifier=notifier,
            timezone_name=config.timezone,
            not_accepted_interval_min=config.installer_remind_interval_min,
            post_accept_interval_min=config.installer_post_accept_interval_min,
            quiet_start_hour=config.installer_quiet_start_hour,
            quiet_end_hour=config.installer_quiet_end_hour,
            interval_seconds=60,
        ),
        name="installer_acceptance_reminders_loop",
    )
    installer_acceptance_task.add_done_callback(_task_exception_cb)
    background_tasks.append(installer_acceptance_task)

    # Daily auto-sync at 09:00 MSK: ОП import (with credit auto-close), Sheets export, keyboards, deadlines, debts
    daily_sync_task: asyncio.Task | None = asyncio.create_task(
        daily_sync_loop(
            db=db,
            notifier=notifier,
            config=config,
            integrations=integrations,
            target_hour=9,
            target_minute=0,
        ),
        name="daily_sync_loop",
    )
    daily_sync_task.add_done_callback(_task_exception_cb)
    background_tasks.append(daily_sync_task)

    lead_poller_task: asyncio.Task | None = None
    if config.amocrm_enabled and amocrm_service is not None:
        # Since webhook deploy 2026-05-25 poller is a fallback (interval relaxed 30→300s).
        lead_poller_task = asyncio.create_task(
            lead_poller_loop(
                db=db,
                amocrm=amocrm_service,
                notifier=notifier,
                interval_seconds=300,
            ),
            name="lead_poller_loop",
        )
        lead_poller_task.add_done_callback(_task_exception_cb)
        background_tasks.append(lead_poller_task)

    # Sheet «Leads» throttle: every 60s upserts changed leads if dirty flag is set
    # by webhook handler or polling fallback. Decouples webhook latency from Sheets API.
    sheet_throttle_task: asyncio.Task | None = None
    if (
        config.sheets_enabled
        and sheets_service is not None
        and config.amocrm_enabled
    ):
        from .services.sheet_throttle import sheet_throttle_loop
        sheet_throttle_task = asyncio.create_task(
            sheet_throttle_loop(
                db=db,
                sheets=sheets_service,
                amocrm=amocrm_service,
                amocrm_user_map=config.amocrm_user_map,
            ),
            name="sheet_throttle_loop",
        )
        sheet_throttle_task.add_done_callback(_task_exception_cb)
        background_tasks.append(sheet_throttle_task)

    # P4 watchdog heartbeat: пингует bot.get_me() каждые 30 сек (со своим
    # таймаутом) и обновляет _polling_health. /health отдаёт 503 только при
    # ДЛИТЕЛЬНОМ затыке (last_ok_ts старше _HEARTBEAT_STALE_SEC) И серии из
    # _HEARTBEAT_FAIL_STREAK подряд неудачных get_me — иначе брифовые блипы WARP
    # (DNS-флип / SOCKS upstream-timeout к Telegram) вызывали ложный restart
    # контейнера внешним chain-watchdog. Hard-cap ловит смерть самого heartbeat-таска.
    import time as _time_p4
    _polling_health: dict[str, float] = {"last_ok_ts": _time_p4.monotonic(), "consec_fail": 0}
    _HEARTBEAT_STALE_SEC = 300        # было 120: терпим несколько минут блипов WARP
    _HEARTBEAT_FAIL_STREAK = 4        # + требуем 4 подряд неудачных get_me (30с×4 ≈ 2 мин)
    _HEARTBEAT_HARD_STALE_SEC = 600   # hard-cap: 503 при 10-мин тишине даже если heartbeat-таск умер
    _HEARTBEAT_GET_ME_TIMEOUT = 15    # свой таймаут get_me, чтобы зависший вызов не тянул окно

    async def _heartbeat_loop() -> None:
        while True:
            try:
                await asyncio.wait_for(bot.get_me(), timeout=_HEARTBEAT_GET_ME_TIMEOUT)
                _polling_health["last_ok_ts"] = _time_p4.monotonic()
                _polling_health["consec_fail"] = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                _polling_health["consec_fail"] = _polling_health.get("consec_fail", 0) + 1
                log.warning(
                    "Polling heartbeat get_me failed (streak=%s)",
                    _polling_health["consec_fail"],
                )
            await asyncio.sleep(30)

    heartbeat_task: asyncio.Task = asyncio.create_task(
        _heartbeat_loop(), name="polling_heartbeat_loop"
    )
    heartbeat_task.add_done_callback(_task_exception_cb)
    background_tasks.append(heartbeat_task)

    # --- HTTP webhook server (aiohttp) ---
    # Hosts both the Sheets webhook and the amoCRM Chat webhook (if their
    # respective credentials are configured). Started if at least one is enabled.
    webhook_runner: web.AppRunner | None = None
    _webapp_enabled = (
        bool(config.sheets_webhook_secret)
        or virtual_manager is not None
        or bool(config.amocrm_leads_webhook_secret)
    )
    if _webapp_enabled:
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
                        integrations=integrations,
                    )
                    log.info("Sheets webhook processed: %s", result)
                except Exception:
                    log.exception("Sheets webhook background processing error")

            task = asyncio.create_task(_process_in_bg(payload), name="webhook_bg")
            # Prevent GC of fire-and-forget tasks
            _webhook_bg_tasks.add(task)
            task.add_done_callback(_webhook_bg_tasks.discard)
            return web.json_response({"status": "accepted"})

        import time as _time
        _start_ts = _time.monotonic()

        async def _health(request: web.Request) -> web.Response:
            """Health dashboard: DB, tasks queue, uptime, polling heartbeat."""
            try:
                uptime_sec = int(_time.monotonic() - _start_ts)
                hours, remainder = divmod(uptime_sec, 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h {minutes}m {seconds}s"

                heartbeat_age = int(_time.monotonic() - _polling_health["last_ok_ts"])

                # DB check
                cur = await db.conn.execute("SELECT COUNT(*) FROM invoices")
                inv_count = (await cur.fetchone())[0]
                cur2 = await db.conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE status IN ('open','in_progress')"
                )
                active_tasks = (await cur2.fetchone())[0]
                cur3 = await db.conn.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
                user_count = (await cur3.fetchone())[0]

                payload = {
                    "status": "ok",
                    "uptime": uptime_str,
                    "db": "connected",
                    "invoices": inv_count,
                    "active_tasks": active_tasks,
                    "active_users": user_count,
                    "sheets_enabled": config.sheets_enabled,
                    "polling_heartbeat_age_sec": heartbeat_age,
                }
                consec_fail = int(_polling_health.get("consec_fail", 0))
                payload["heartbeat_consec_fail"] = consec_fail
                # 503 при ДЛИТЕЛЬНОМ затыке И серии подряд неудач (брифовые блипы WARP
                # не роняют бота), ЛИБО при очень долгой тишине (hard-cap — на случай
                # если сам heartbeat-таск умер и consec_fail перестал расти).
                if (
                    heartbeat_age > _HEARTBEAT_STALE_SEC
                    and consec_fail >= _HEARTBEAT_FAIL_STREAK
                ) or heartbeat_age > _HEARTBEAT_HARD_STALE_SEC:
                    payload["status"] = "polling_stuck"
                    return web.json_response(payload, status=503)
                return web.json_response(payload)
            except Exception as exc:
                return web.json_response(
                    {"status": "error", "detail": str(exc)}, status=500
                )

        webapp = web.Application()
        if config.sheets_webhook_secret:
            webapp.router.add_post("/webhooks/sheets", _handle_sheets_webhook)
        webapp.router.add_get("/health", _health)

        if virtual_manager is not None and config.amocrm_chat_channel_secret:
            from .handlers.amocrm_webhook import handle_amocrm_chat_webhook
            _vm = virtual_manager
            _vm_secret = config.amocrm_chat_channel_secret
            async def _handle_vm_webhook(request: web.Request) -> web.Response:
                return await handle_amocrm_chat_webhook(
                    request,
                    vm=_vm,
                    channel_secret=_vm_secret,
                    bg_tasks=_webhook_bg_tasks,
                )
            webapp.router.add_post("/webhooks/amocrm/chat", _handle_vm_webhook)
            log.info("Virtual manager webhook registered at /webhooks/amocrm/chat")
        elif virtual_manager is not None:
            log.warning(
                "Virtual manager active but AMOCRM_CHAT_CHANNEL_SECRET not set — "
                "webhook endpoint NOT registered. Add the secret to enable inbound traffic."
            )

        if config.amocrm_leads_webhook_secret and amocrm_service is not None:
            from .handlers.amocrm_leads_webhook import handle_amocrm_leads_webhook
            _leads_secret = config.amocrm_leads_webhook_secret
            _amocrm_for_leads = amocrm_service
            async def _handle_leads_webhook(request: web.Request) -> web.Response:
                return await handle_amocrm_leads_webhook(
                    request,
                    db=db,
                    amocrm=_amocrm_for_leads,
                    notifier=notifier,
                    secret=_leads_secret,
                    bg_tasks=_webhook_bg_tasks,
                )
            webapp.router.add_post("/webhooks/amocrm/leads", _handle_leads_webhook)
            log.info("amoCRM leads webhook registered at /webhooks/amocrm/leads")
        elif config.amocrm_leads_webhook_secret:
            log.warning(
                "AMOCRM_LEADS_WEBHOOK_SECRET set but amoCRM service inactive — "
                "leads webhook endpoint NOT registered."
            )

        webhook_runner = web.AppRunner(webapp)
        await webhook_runner.setup()
        site = web.TCPSite(webhook_runner, "0.0.0.0", config.sheets_webhook_port)
        await site.start()
        log.info("HTTP webhook server started on port %d", config.sheets_webhook_port)

    # NOTE: startup message in work chat removed — not needed in group chats

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            db=db,
            config=config,
            notifier=notifier,
            integrations=integrations,
            storage=storage,
        )
    finally:
        # Cancel all background tasks and await them
        for task in background_tasks:
            task.cancel()
        results = await asyncio.gather(*background_tasks, return_exceptions=True)
        for task, result in zip(background_tasks, results):
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                log.error("Task %s terminated with error: %s", task.get_name(), result)
        if webhook_runner:
            await webhook_runner.cleanup()
        if virtual_manager is not None:
            await virtual_manager.shutdown()
        await integrations.stop()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
