from __future__ import annotations

import logging
from typing import Any

from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from ..callbacks import TaskCb
from ..config import Config
from ..db import Database
from ..enums import InvoiceStatus, MANAGER_ROLES, ProjectStatus, Role, TaskStatus, TaskType
from ..integrations.minio_storage import MinioStorage
from ..keyboards import main_menu, manager_project_actions_kb, task_actions_kb
from ..services.integration_hub import IntegrationHub
from ..services.assignment import resolve_default_assignee
from ..services.menu_context import build_main_menu_for_user
from ..services.menu_scope import resolve_active_menu_role, resolve_menu_scope
from ..services.notifier import Notifier
from ..states import CreditPaymentExecuteSG, DeliveryPaymentSG, InvoicePaymentSG, MontazhCommentSG, SupplierPaymentSG, TaskCancelReasonSG, TaskCompleteSG
from ..utils import answer_service, build_manager_task_open_card, build_rp_zp_family_open_card, build_task_done_card, enrich_task_invoice_label, fmt_task_card, format_invoice_end_financials, get_initiator_label, parse_roles, private_only_reply_markup, refresh_recipient_keyboard, task_type_label, try_json_loads
from ._mirror import collect_attachment, mirror_attachment
from .money_guard import money_confirm_guard

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")


async def _can_manage_task(cb: CallbackQuery, db: Database, config: Config, task: dict[str, Any]) -> bool:
    """Allow assigned user or admin."""
    if not cb.from_user:
        return False
    user_id = cb.from_user.id
    user = await db.get_user_optional(user_id)
    if user and not user.is_active:
        return False
    if user_id in (config.admin_ids or set()):
        return True
    assigned_to = task.get("assigned_to")
    if assigned_to and int(assigned_to) == user_id:
        return True
    return False


async def _current_menu(db: Database, user_id: int) -> tuple[str | None, bool]:
    user = await db.get_user_optional(user_id)
    return resolve_menu_scope(user_id, user.role if user else None)


def _ignorable_markup_error(exc: TelegramBadRequest) -> bool:
    text = str(exc).lower()
    return any(
        needle in text
        for needle in (
            "message is not modified",
            "message can't be edited",
            "message to edit not found",
            "there is no reply markup in the message",
        )
    )


async def _safe_edit_task_markup(
    message: Message | None,
    *,
    reply_markup: Any | None,
) -> None:
    if not message:
        return
    try:
        await message.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if _ignorable_markup_error(exc):
            return
        log.debug("Failed to refresh task callback markup", exc_info=True)
    except Exception:
        log.debug("Failed to refresh task callback markup", exc_info=True)


async def _answer_with_menu(
    message: Message | None,
    db: Database,
    config: Config,
    user_id: int,
    text: str,
    *,
    role: str | None,
    isolated_role: bool = False,
) -> None:
    if not message:
        return
    await message.answer(
        text,
        reply_markup=private_only_reply_markup(
            message,
            await build_main_menu_for_user(
                db,
                config,
                user_id,
                role,
                isolated_role=isolated_role,
            ),
        ),
    )


async def _maybe_mark_lead_tracking_response(db: Database, task: dict[str, Any] | None) -> None:
    if not task or task.get("type") != TaskType.LEAD_TO_PROJECT:
        return
    payload = try_json_loads(task.get("payload_json"))
    lead_id = payload.get("lead_id")
    try:
        lead_tracking_id = int(lead_id)
    except (TypeError, ValueError):
        return
    await db.update_lead_tracking_response(lead_tracking_id)


async def _notify_task_creator_done(
    db: Database,
    notifier: Notifier,
    config: Config,
    actor_id: int | None,
    task: dict[str, Any] | None,
    project: dict[str, Any] | None = None,
) -> None:
    if not task:
        return
    created_by = task.get("created_by")
    if not created_by:
        return
    try:
        created_by_int = int(created_by)
    except (TypeError, ValueError):
        return
    if actor_id and created_by_int == actor_id:
        return
    initiator = await get_initiator_label(db, actor_id) if actor_id else "Исполнитель"
    # ТЗ 17.06: человекочитаемая привязка к счёту (№ + адрес) вместо сырого #id.
    await enrich_task_invoice_label(db, task)
    await notifier.safe_send(
        created_by_int,
        build_task_done_card(
            task,
            project,
            config.timezone,
            title="Задача выполнена",
            actor_label=initiator,
        ),
    )


async def _apply_done_side_effects(
    db: Database,
    integrations: IntegrationHub,
    task: dict[str, Any],
    project: dict[str, Any] | None,
) -> dict[str, Any] | None:
    await _maybe_mark_lead_tracking_response(db, task)
    if project and task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.INVOICE_SENT)
        await integrations.sync_project(project)
    if project and task.get("type") in {TaskType.CLOSING_DOCS, TaskType.PROJECT_END}:
        project = await db.update_project_status(int(project["id"]), ProjectStatus.ARCHIVE)
        await integrations.sync_project(project)
    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
    return project


def _invoice_task_sender_id(payload: dict[str, Any]) -> int | None:
    sender_id = payload.get("sender_id") or payload.get("manager_id")
    if sender_id is None:
        return None
    try:
        return int(sender_id)
    except (TypeError, ValueError):
        return None


def _invoice_task_details(payload: dict[str, Any]) -> tuple[int | None, str, str, str]:
    invoice_id_raw = payload.get("invoice_id")
    try:
        invoice_id = int(invoice_id_raw) if invoice_id_raw is not None else None
    except (TypeError, ValueError):
        invoice_id = None

    invoice_number = str(payload.get("invoice_number") or "")
    supplier = str(payload.get("supplier") or "")
    amount = str(payload.get("amount") or "")
    return invoice_id, invoice_number, supplier, amount


def _task_take_text(task: dict[str, Any], project: dict[str, Any] | None) -> str:
    """Build a short human-readable confirmation for 'take in work'."""
    task_id = task.get("id")
    task_type = task.get("type")
    payload = try_json_loads(task.get("payload_json"))

    lines = [f"⏳ Взял в работу: #{task_id} — {task_type_label(task_type)}"]

    if project:
        title = str(project.get("title") or "").strip()
        if title:
            lines.append(f"📁 Проект: {title}")
        return "\n".join(lines)

    invoice_number = str(payload.get("invoice_number") or "").strip()
    if invoice_number:
        lines.append(f"📄 Счёт: {invoice_number}")

    address = str(payload.get("address") or payload.get("object_address") or "").strip()
    if address:
        lines.append(f"📍 Адрес: {address}")

    supplier = str(payload.get("supplier") or "").strip()
    if supplier:
        lines.append(f"🏢 Поставщик: {supplier}")

    comment = str(payload.get("comment") or payload.get("description") or "").strip()
    if comment:
        preview = comment if len(comment) <= 120 else f"{comment[:117]}..."
        lines.append(f"📝 {preview}")

    return "\n".join(lines)


_ACTIONS_BASIC = {"delete", "accept", "accept_take", "open", "take", "reject", "cancel"}

_ACTIONS_EXTENDED = {
    "pay_ok", "pay_need",
    "pay_supplier",
    "inv_received", "inv_pay", "inv_hold", "inv_reject",
    "del_accept", "del_pay",
    "montazh_yes", "montazh_no", "montazh_comment",
    "done",
    "invend_ok", "invend_review",
}

# Денежные типы, у которых generic-«✅ Завершить» закрыл бы задачу МИМО денег: расход в БК
# пишет и уведомляет РП ТОЛЬКО платёжный флоу «✅ Выплатить» (rp_salary_confirm →
# db.record_rp_salary_payment; rp_zp_pay_submit → выплата по счетам), а generic-done просто
# ставит статус. keyboards.py:786-811 таким задачам generic-кнопки не рисует (early-return),
# но кнопка из СТАРОГО сообщения досюда доходит — ровно так 30.06 в 15:33 «съелась» задача
# 320 «Оклад РП 2026-07» на 66 000 ₽: ГД нажал три generic-кнопки подряд (accept → take →
# done), задача ушла в done, а записи «Оклад РП% 2026-07» в op_company_entries нет и никогда
# не было. Хуже того, повторно запросить оклад РП уже не может: rp_salary_request_start
# (rp.py:1856) читает done как «уже выплачен». Тот же класс защиты, что гард RECALC_CONFIRM
# ниже (случай 336/337/338 от 03.07). Ключ — TaskType (StrEnum): хэшируется как str, поэтому
# строка из БД находится словарём. Сузить/расширить объём = убрать/добавить ключ.
_DONE_BLOCKED_PAYOUT_TYPES: dict[str, str] = {
    TaskType.RP_SALARY: (
        "Закройте кнопкой «✅ Выплатить» — иначе оклад не попадёт в «Баланс компании». "
        "Откройте задачу заново: «💰 Прочие ЗП» или «📥 Входящие для ГД»."
    ),
    TaskType.ZP_RP: (
        "Закройте кнопкой «✅ Выплатить» — иначе ЗП РП 10% не встанет в счета и РП её не "
        "увидит. Откройте задачу заново: «💰 Прочие ЗП» или «📥 Входящие для ГД»."
    ),
    # Запрос ГД из депозита (добавлен 03.08). Отличие от двух типов выше: у этого
    # generic-кнопка рисовалась ЖИВОЙ до 03.08 (ветки в keyboards.py не было вовсе),
    # поэтому дыра сработала на боевых данных 3 раза из 6 — #346/#347/#353. Списание
    # пишет только шаг «исполнение» (installer_new.py:4842); закрытая мимо него задача
    # неисполнима навсегда — и обе точки, и _depo_req_finalize требуют IN_PROGRESS.
    TaskType.GD_DEPOSIT_REQUEST: (
        "Закройте кнопками запроса: «✅ Подтвердить прочтение» → «✅ Подтвердить "
        "исполнение» — иначе списание с депозита не пройдёт, а запрос станет "
        "неисполнимым. Если запрос больше не нужен — «❌ Отклонить». Откройте "
        "задачу заново из списка задач."
    ),
}

# Типы, для которых ветка `done` легитимно уводит в сбор вложений (TaskCompleteSG,
# см. ниже по файлу). Список ЗАКРЫТЫЙ и нужен как гард в taskcomplete_finalize:
# 🔴 тот хендлер зарегистрирован БЕЗ StateFilter и берёт task_id из данных FSM.
# Пока кнопки taskcomplete:* рождались только у DOCS_REQUEST / QUOTE_REQUEST (РП) и
# CLOSING_DOCS (бухгалтерия), чужой task_id попасть туда не мог: эти роли не ведут
# денежных флоу, кладущих id в тот же ключ. С добавлением URGENT_GD (03.08) кнопки
# впервые получает ГД — а он под тем же ключом `task_id` кладёт ДЕНЕЖНУЮ задачу при
# отклонении оклада РП (td.py:2621, RpSalaryRejectSG). Без этой проверки устаревшая
# кнопка «⏭ Закрыть без отправки» закрыла бы оклад мимо выплаты — ровно тот класс,
# что съел задачу 320 на 66 000 ₽. Ключ — TaskType (StrEnum), строка из БД находится.
_TASKCOMPLETE_ALLOWED_TYPES: frozenset[str] = frozenset({
    TaskType.DOCS_REQUEST,
    TaskType.QUOTE_REQUEST,
    TaskType.CLOSING_DOCS,
    TaskType.URGENT_GD,
})


async def send_task_open_card(
    target, db: Database, config: Config, task: dict, viewer_role, project=None,
) -> None:
    """Отправить карточку открытой задачи (текст + кнопки действий).

    Общий рендер: используется веткой open в task_actions (клик по задаче) и
    одиночным показом во «Входящие для ГД» (gd.py, user 04.07 — одна задача →
    сразу карточка). Вложения НЕ шлёт — их досылает вызывающий при необходимости.
    [[feedback_card_template_standard]]
    """
    await enrich_task_invoice_label(db, task)
    # ЗП РП 10% / Оклад РП: эталонная карточка запроса (От/счета/Итого/статус) +
    # кнопки Выплатить/Отклонить — тот же вид, что уведомление РП→ГД и «Прочие ЗП».
    if task.get("type") in (TaskType.ZP_RP, TaskType.RP_SALARY):
        # Оклад РП: зачёт выданного аванса считаем ЖИВЬЁМ (ТЗ owner 31.07). Не из payload —
        # задачи, созданные до правки, ключей про аванс не имеют, а пересоздавать открытую
        # задачу ради показа нельзя. Ошибка расчёта не должна ронять карточку.
        advance = None
        if task.get("type") == TaskType.RP_SALARY:
            try:
                _pl = try_json_loads(task.get("payload_json")) or {}
                _rp_id = int(_pl.get("rp_id") or 0)
                if _rp_id:
                    advance = await db.get_rp_oklad_advance_offset(_rp_id)
            except Exception:
                log.exception(
                    "send_task_open_card: расчёт аванса РП не удался task=%s", task.get("id"),
                )
        await target.answer(
            build_rp_zp_family_open_card(task, advance),
            reply_markup=task_actions_kb(task, viewer_role=viewer_role),
        )
        return
    if project is None and task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None
    if viewer_role in MANAGER_ROLES or viewer_role == Role.MANAGER:
        text = await build_manager_task_open_card(db, task, config.timezone)
    else:
        text = fmt_task_card(task, project, config.timezone)
        # PART B (ТЗ 19.06): справочный финблок в задаче «Счёт End» — display-only.
        # Только ГД/ТД (прибыль/себестоимость/ЗП скрыты от прочих ролей).
        if task.get("type") == TaskType.INVOICE_END_REQUEST and ({Role.GD, Role.TD} & set(parse_roles(viewer_role))):
            try:
                _pl = try_json_loads(task.get("payload_json")) or {}
                _inv_id = int(_pl.get("invoice_id") or 0)
                if _inv_id:
                    _inv = await db.get_invoice(_inv_id)
                    if _inv:
                        _pf = await db.get_plan_fact_card(_inv_id)
                        _fin = format_invoice_end_financials(_inv, _pf)
                        if _fin:
                            text += "\n\n" + _fin
            except Exception:
                log.exception("invend open: financials block failed for task #%s", task.get("id"))
    await target.answer(text, reply_markup=task_actions_kb(task, viewer_role=viewer_role))


@router.callback_query(TaskCb.filter(F.action.in_(_ACTIONS_BASIC)))
async def task_actions(
    cb: CallbackQuery,
    callback_data: TaskCb,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    state: FSMContext,
) -> None:
    task_id = int(callback_data.task_id)
    action = callback_data.action

    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена или была удалена.", show_alert=True)
        return
    active_statuses = {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}

    # Роль зрителя — нужна task_actions_kb для роль-зависимых кнопок (#4: «Закрыть» для НПН).
    _viewer_user = await db.get_user_optional(cb.from_user.id) if cb.from_user else None
    viewer_role = resolve_active_menu_role(cb.from_user.id, _viewer_user.role) if (_viewer_user and cb.from_user) else None

    # DELETE — GD (admin) and RP
    if action == "delete":
        u = await db.get_user_optional(cb.from_user.id)
        user_roles = set(parse_roles(u.role if u else None))
        is_authorized = cb.from_user.id in (config.admin_ids or set()) or Role.RP in user_roles
        if not is_authorized:
            await cb.answer("⛔️ Удаление доступно только ГД и РП", show_alert=True)
            return
        await db.delete_task(task_id)
        await cb.answer(f"🗑 Задача #{task_id} удалена", show_alert=True)
        try:
            await cb.message.edit_text(  # type: ignore[union-attr]
                f"🗑 <s>Задача #{task_id}</s> — удалена",
            )
        except Exception:
            pass
        return

    if not await _can_manage_task(cb, db, config, task):
        await cb.answer("Эта задача назначена другому человеку", show_alert=True)
        return

    if action == "accept":
        if task.get("status") != TaskStatus.OPEN or task.get("accepted_at"):
            await cb.answer("Эта задача уже подтверждена или закрыта.", show_alert=True)
            return
        await db.accept_task(task_id)
        await cb.answer("✅ Принято")
        # Update the inline keyboard to remove the "Принято" button
        task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)
        if task:
            await _safe_edit_task_markup(cb.message, reply_markup=task_actions_kb(task, viewer_role=viewer_role))
        # Notify task creator
        created_by = task.get("created_by") if task else None
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            # ТЗ 17.06: эталонная карточка вместо free-form + ВИДНО какую задачу
            # приняли (тип / комментарий / счёт). enrich → привязка «КВ N + адрес».
            await enrich_task_invoice_label(db, task)
            await notifier.safe_send(
                int(created_by),
                build_task_done_card(
                    task, None, config.timezone,
                    title="Задача принята", actor_label=initiator,
                ),
            )
        return

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None

    # OPEN: show card + actions
    if action == "open":
        await cb.answer()
        await send_task_open_card(cb.message, db, config, task, viewer_role, project=project)

        # send attachments, if any
        attaches = await db.list_attachments(task_id)
        if attaches:
            await cb.message.answer(f"📎 Вложения: {len(attaches)}")  # type: ignore
            for a in attaches[:10]:
                await notifier.safe_send_media(cb.from_user.id, a["file_type"], a["tg_file_id"], caption=a.get("caption"))
        return

    # TAKE
    if action == "take":
        status = task.get("status")
        if status == TaskStatus.IN_PROGRESS:
            await cb.answer("Эта задача уже взята в работу.", show_alert=True)
            return
        if status not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(
            task_id, TaskStatus.IN_PROGRESS,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        if not task.get("accepted_at"):
            await db.accept_task(task_id)
            task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)

        # Update montazh_stage for installer tasks
        try:
            user_row = await db.get_user_optional(cb.from_user.id)
            if user_row and Role.INSTALLER in (user_row.role or ""):
                payload = task.get("payload_json") or {}
                if isinstance(payload, str):
                    payload = try_json_loads(payload)
                inv_id = payload.get("invoice_id")
                if inv_id:
                    from ..enums import MontazhStage
                    await db.update_montazh_stage(int(inv_id), MontazhStage.IN_WORK)
                    inv_row = await db.get_invoice(int(inv_id))
                    if inv_row:
                        await integrations.sync_invoice_status(
                            inv_row["invoice_number"], inv_row.get("status", ""), MontazhStage.IN_WORK,
                        )
        except Exception:
            log.exception("Failed to update montazh_stage on take")
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=task_actions_kb(task, viewer_role=viewer_role))
        await answer_service(cb.message, _task_take_text(task, project))  # type: ignore[arg-type]
        return

    # ACCEPT+TAKE — менеджерская единая «Принято» (ТЗ 23.06): принять + взять в
    # работу (→ IN_PROGRESS) + уведомить постановщика за один тап. Объединяет
    # семантику accept (нотификация) и take (статус). Только для роли менеджер
    # (кнопка показывается лишь менеджеру в task_actions_kb).
    if action == "accept_take":
        status = task.get("status")
        if status == TaskStatus.IN_PROGRESS:
            await cb.answer("Эта задача уже в работе.", show_alert=True)
            return
        if status not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(
            task_id, TaskStatus.IN_PROGRESS,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        if not task.get("accepted_at"):
            await db.accept_task(task_id)
            task = await db.get_task(task_id)
        await _maybe_mark_lead_tracking_response(db, task)
        await cb.answer("✅ Принято, взято в работу")
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=task_actions_kb(task, viewer_role=viewer_role))
        # Уведомить постановщика (как в accept) — эталонная карточка «Задача принята».
        created_by = task.get("created_by") if task else None
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await enrich_task_invoice_label(db, task)
            await notifier.safe_send(
                int(created_by),
                build_task_done_card(
                    task, None, config.timezone,
                    title="Задача принята", actor_label=initiator,
                ),
            )
        return

    # REJECT
    if action == "reject":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        task = await db.update_task_status(
            task_id, TaskStatus.REJECTED,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        await _maybe_mark_lead_tracking_response(db, task)
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "❌ Задача отклонена.",
                role=role_now,
                isolated_role=isolated_role,
            )

        # notify creator
        created_by = task.get("created_by")
        if created_by:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(created_by),
                f"❌ Ваша задача #{task_id} отклонена\n"
                f"👤 Исполнитель: {initiator}",
            )
        return

    # CANCEL (снять задачу) — available to assigned user, creator, and admin
    if action == "cancel":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        # Allow creator to cancel too
        user_id = cb.from_user.id
        created_by = task.get("created_by")
        try:
            is_creator = created_by is not None and int(created_by) == user_id
        except (ValueError, TypeError):
            is_creator = False
        try:
            is_assigned = task.get("assigned_to") is not None and int(task["assigned_to"]) == user_id
        except (ValueError, TypeError):
            is_assigned = False
        is_admin = user_id in (config.admin_ids or set())
        if not (is_creator or is_assigned or is_admin):
            await cb.answer("Снять задачу может только автор, исполнитель или администратор.", show_alert=True)
            return
        # #33/#48: Если задача уже подтверждена — запросить причину отмены
        if task.get("accepted_at") and not is_admin:
            from ..states import TaskCancelReasonSG
            await state.clear()
            await state.set_state(TaskCancelReasonSG.reason)
            await state.update_data(cancel_task_id=task_id)
            await cb.message.answer(  # type: ignore[union-attr]
                f"⚠️ Задача #{task_id} уже была подтверждена получателем.\n\n"
                "Для отмены укажите <b>причину</b>:",
            )
            return
        # Atomic update — prevent race condition
        task = await db.update_task_status(
            task_id, TaskStatus.REJECTED,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "🚫 Задача снята.",
                role=role_now,
                isolated_role=isolated_role,
            )
        # Notify the other party (creator or assigned)
        initiator = await get_initiator_label(db, cb.from_user.id)
        task_label = task_type_label(task.get("type") or "")
        inv_num = ""
        payload = try_json_loads(task.get("payload_json"))
        if payload:
            inv_num = payload.get("invoice_number", "")
        cancel_detail = f"📋 {task_label}"
        if inv_num:
            cancel_detail += f" | Счёт: {inv_num}"

        notified_ids: set[int] = {user_id}
        if is_creator and task.get("assigned_to"):
            try:
                tid_assigned = int(task["assigned_to"])
                await notifier.safe_send(
                    tid_assigned,
                    f"🚫 Задача #{task_id} снята автором\n{cancel_detail}\n👤 {initiator}",
                )
                notified_ids.add(tid_assigned)
            except (ValueError, TypeError):
                pass
        elif is_assigned and created_by:
            try:
                tid_creator = int(created_by)
                await notifier.safe_send(
                    tid_creator,
                    f"🚫 Ваша задача #{task_id} снята исполнителем\n{cancel_detail}\n👤 {initiator}",
                )
                notified_ids.add(tid_creator)
            except (ValueError, TypeError):
                pass
        elif is_admin:
            for notify_id in filter(None, [created_by, task.get("assigned_to")]):
                try:
                    nid = int(notify_id)
                except (ValueError, TypeError):
                    continue
                if nid != user_id:
                    await notifier.safe_send(
                        nid,
                        f"🚫 Задача #{task_id} снята администратором\n{cancel_detail}\n👤 {initiator}",
                    )
                    notified_ids.add(nid)

        # Always notify RP and GD about cancellation
        rp_id = await resolve_default_assignee(db, config, Role.RP)
        gd_id = await resolve_default_assignee(db, config, Role.GD)
        cancel_msg_rp_gd = (
            f"🚫 Задача #{task_id} снята\n{cancel_detail}\n👤 Инициатор: {initiator}"
        )
        for mgmt_id in filter(None, [rp_id, gd_id]):
            if mgmt_id not in notified_ids:
                await notifier.safe_send(mgmt_id, cancel_msg_rp_gd)
                notified_ids.add(mgmt_id)
        return

# #33/#48: Обработка причины отмены задачи (после подтверждения)
@router.message(TaskCancelReasonSG.reason)
async def task_cancel_with_reason(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Принять причину отмены и отменить задачу."""
    reason = (message.text or "").strip()
    if len(reason) < 3:
        await message.answer("Укажите причину отмены (минимум 3 символа):")
        return

    data = await state.get_data()
    task_id = data.get("cancel_task_id")
    if not task_id or not isinstance(task_id, int) or task_id <= 0:
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача не найдена.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    task = await db.get_task(task_id)
    if not task or task.get("status") not in ("open", "in_progress"):
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача уже закрыта или обработана.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    task = await db.update_task_status(
        task_id, TaskStatus.REJECTED,
        expected_statuses=("open", "in_progress"),
    )
    if task is None:
        await state.clear()
        role_now, isolated_role = await _current_menu(db, message.from_user.id)
        await message.answer(
            "❌ Задача уже была обработана другим пользователем.",
            reply_markup=private_only_reply_markup(
                message, main_menu(role_now, is_admin=message.from_user.id in (config.admin_ids or set()),
                                   unread=await db.count_unread_tasks(message.from_user.id), isolated_role=isolated_role)),
        )
        return

    await state.clear()

    initiator = await get_initiator_label(db, message.from_user.id)
    task_label = task_type_label(task.get("type") or "")
    payload = try_json_loads(task.get("payload_json"))
    inv_num = payload.get("invoice_number", "") if payload else ""
    cancel_detail = f"📋 {task_label}"
    if inv_num:
        cancel_detail += f" | Счёт: {inv_num}"

    cancel_msg = (
        f"🚫 Задача #{task_id} снята\n"
        f"{cancel_detail}\n"
        f"👤 {initiator}\n"
        f"📝 Причина: {reason}"
    )

    # Уведомить все стороны
    user_id = message.from_user.id
    notified: set[int] = {user_id}
    for nid_raw in [task.get("assigned_to"), task.get("created_by")]:
        if nid_raw:
            try:
                nid = int(nid_raw)
                if nid not in notified:
                    await notifier.safe_send(nid, cancel_msg)
                    notified.add(nid)
            except (ValueError, TypeError):
                pass

    role_now, isolated_role = await _current_menu(db, user_id)
    await message.answer(
        f"🚫 Задача #{task_id} снята.\n📝 Причина: {reason}",
        reply_markup=private_only_reply_markup(
            message,
            main_menu(
                role_now,
                is_admin=user_id in (config.admin_ids or set()),
                unread=await db.count_unread_tasks(user_id),
                isolated_role=isolated_role,
            ),
        ),
    )


@router.callback_query(TaskCb.filter(F.action.in_(_ACTIONS_EXTENDED)))
async def task_actions_part2(
    cb: CallbackQuery,
    callback_data: TaskCb,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    state: FSMContext,
) -> None:
    """Continuation of task_actions — PAYMENT_CONFIRM, ORDER, INVOICE_PAYMENT actions."""
    task_id = int(callback_data.task_id)
    action = callback_data.action

    try:
        task = await db.get_task(task_id)
    except KeyError:
        await cb.answer("Задача не найдена.", show_alert=True)
        return
    active_statuses = {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}

    if not await _can_manage_task(cb, db, config, task):
        await cb.answer("Эта задача назначена другому человеку", show_alert=True)
        return

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            project = None
    # Fallback: resolve project from invoice_id in payload
    if not project:
        _payload = try_json_loads(task.get("payload_json"))
        _inv_id = _payload.get("invoice_id") if _payload else None
        if _inv_id:
            try:
                _inv = await db.get_invoice(int(_inv_id))
                if _inv and _inv.get("project_id"):
                    project = await db.get_project(int(_inv["project_id"]))
            except Exception:
                pass

    # PAYMENT CONFIRM actions (TD)
    if action in {"pay_ok", "pay_need"} and task.get("type") == TaskType.PAYMENT_CONFIRM:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        if not project:
            await cb.message.answer("Проект не найден для этой задачи.")  # type: ignore
            return

        payload = try_json_loads(task.get("payload_json"))
        manager_id = payload.get("manager_id") or project.get("manager_id")
        rp_id = project.get("rp_id") or (await db.get_project_rp_id(int(project["id"])))

        if action == "pay_ok":
            task = await db.update_task_status(
                task_id, TaskStatus.DONE,
                expected_statuses=tuple(active_statuses),
            )
            if task is None:
                await cb.answer("Задача уже была обработана.", show_alert=True)
                return
            project = await db.update_project_status(int(project["id"]), ProjectStatus.IN_WORK)
            # Обновить статус подтверждения оплаты на счетах проекта
            from ..utils import utcnow, to_iso
            _now = to_iso(utcnow())
            await db.conn.execute(
                "UPDATE invoices SET payment_confirm_status = 'Подтверждена', "
                "payment_confirmed_by = ?, payment_confirmed_at = ? WHERE project_id = ?",
                (cb.from_user.id, _now, int(project["id"])),
            )
            await db.conn.commit()

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "✅ <b>Оплата подтверждена</b> — можно запускать закупки и монтаж.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('title','')}"
            )
            if manager_id:
                await notifier.safe_send(
                    int(manager_id),
                    text,
                    reply_markup=manager_project_actions_kb(int(project["id"])),
                )
            if rp_id:
                await notifier.safe_send(int(rp_id), text)
            await notifier.notify_workchat(text)

        else:
            task = await db.update_task_status(
                task_id, TaskStatus.REJECTED,
                expected_statuses=tuple(active_statuses),
            )
            if task is None:
                await cb.answer("Задача уже была обработана.", show_alert=True)
                return
            project = await db.update_project_status(int(project["id"]), ProjectStatus.WAITING_PAYMENT)
            # Обновить статус подтверждения оплаты на счетах проекта
            await db.conn.execute(
                "UPDATE invoices SET payment_confirm_status = 'Нужна доплата' WHERE project_id = ?",
                (int(project["id"]),),
            )
            await db.conn.commit()

            initiator = await get_initiator_label(db, cb.from_user.id)
            text = (
                "⚠️ <b>Оплата не подтверждена</b>: нужна доплата/уточнение.\n"
                f"👤 От: {initiator}\n\n"
                f"{project.get('title','')}"
            )
            if manager_id:
                await notifier.safe_send(
                    int(manager_id),
                    text,
                    reply_markup=manager_project_actions_kb(int(project["id"])),
                )
            if rp_id:
                await notifier.safe_send(int(rp_id), text)
            await notifier.notify_workchat(text)

        await integrations.sync_project(project, manager_label="")
        await integrations.sync_task(task, project_code=project.get("code", ""))
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "Готово.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    # INVOICE_END_REQUEST → ✅ ОК — ГД подтверждает закрытие счёта (status = ended).
    # Логика по образцу auto_close_credit_invoice (services/sheet_commands.py:600).
    if action == "invend_ok" and task.get("type") == TaskType.INVOICE_END_REQUEST:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        payload = try_json_loads(task.get("payload_json"))
        invoice_id = payload.get("invoice_id") if payload else None
        if not invoice_id:
            await cb.answer("В задаче нет invoice_id.", show_alert=True)
            return
        try:
            invoice_id = int(invoice_id)
        except (TypeError, ValueError):
            await cb.answer("Некорректный invoice_id.", show_alert=True)
            return
        invoice = await db.get_invoice(invoice_id)
        if not invoice:
            await cb.answer("Счёт не найден.", show_alert=True)
            return
        invoice_number = str(invoice.get("invoice_number", ""))

        from ..enums import MontazhStage as _MontazhStage
        await db.update_invoice_status(invoice_id, InvoiceStatus.ENDED)
        await db.update_montazh_stage(invoice_id, _MontazhStage.INVOICE_END)
        if not invoice.get("installer_ok"):
            try:
                await db.set_invoice_installer_ok(invoice_id, ok=True)
            except Exception:
                log.exception("invend_ok: set_installer_ok failed for %s", invoice_number)
        if not invoice.get("no_debts"):
            try:
                await db.set_invoice_no_debts(invoice_id, no_debts=True)
            except Exception:
                log.exception("invend_ok: set_no_debts failed for %s", invoice_number)

        try:
            await integrations.sync_invoice_status(invoice_number, InvoiceStatus.ENDED, _MontazhStage.INVOICE_END)
            await integrations.sync_invoice_row(invoice_id)
        except Exception:
            log.exception("invend_ok: sheets sync failed for %s", invoice_number)

        updated_task = await db.update_task_status(
            task_id, TaskStatus.DONE,
            expected_statuses=tuple(active_statuses),
        )
        if updated_task is not None:
            try:
                await integrations.sync_task(updated_task, project_code="")
            except Exception:
                log.exception("invend_ok: sync_task failed for #%s", task_id)

        await db.audit(
            actor_id=cb.from_user.id if cb.from_user else None,
            action="invoice_end_confirmed_by_gd",
            entity="invoice",
            entity_id=str(invoice_id),
            payload={"task_id": task_id, "invoice_number": invoice_number},
        )

        await cb.answer("✅ Счёт переведён в END")
        try:
            await _safe_edit_task_markup(cb.message, reply_markup=None)
        except Exception:
            pass

        # Уведомить менеджера (creator задачи)
        creator_id = (updated_task or task).get("created_by")
        if creator_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            try:
                await notifier.safe_send(
                    int(creator_id),
                    f"🏁 Счёт №{invoice_number} подтверждён ГД как Счёт END\n"
                    f"👤 От: {initiator}",
                )
            except Exception:
                log.exception("invend_ok: notify creator failed (uid=%s)", creator_id)
        return

    # INVOICE_END_REQUEST → 🔍 На проверку — ГД просит менеджера перепроверить данные.
    # Задача остаётся открытой (IN_PROGRESS), менеджер получает уведомление.
    # payload_json.review_requested=True — маркер для аудита/UI.
    if action == "invend_review" and task.get("type") == TaskType.INVOICE_END_REQUEST:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        payload = try_json_loads(task.get("payload_json")) or {}
        invoice_id = payload.get("invoice_id")
        invoice_number = payload.get("invoice_number") or "?"

        # Перевести в IN_PROGRESS, если ещё OPEN.
        if task.get("status") == TaskStatus.OPEN:
            try:
                await db.update_task_status(
                    task_id, TaskStatus.IN_PROGRESS,
                    expected_statuses=(TaskStatus.OPEN,),
                )
            except Exception:
                log.exception("invend_review: update_task_status failed for #%s", task_id)

        # Записать маркер «отправлено на проверку» в payload.
        payload["review_requested"] = True
        payload["review_requested_by"] = cb.from_user.id if cb.from_user else None
        import json as _json
        try:
            await db.conn.execute(
                "UPDATE tasks SET payload_json = ? WHERE id = ?",
                (_json.dumps(payload, ensure_ascii=False), int(task_id)),
            )
            await db.conn.commit()
        except Exception:
            log.exception("invend_review: payload update failed for #%s", task_id)

        await db.audit(
            actor_id=cb.from_user.id if cb.from_user else None,
            action="invoice_end_sent_for_review",
            entity="task",
            entity_id=str(task_id),
            payload={"invoice_id": invoice_id, "invoice_number": invoice_number},
        )

        # Уведомить менеджера (creator задачи).
        creator_id = task.get("created_by")
        if creator_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            try:
                await notifier.safe_send(
                    int(creator_id),
                    f"🔍 ГД отправил Счёт №{invoice_number} на проверку.\n"
                    f"👤 От: {initiator}\n\n"
                    f"Проверьте данные счёта и при необходимости отправьте «Счёт End» снова.",
                )
            except Exception:
                log.exception("invend_review: notify creator failed (uid=%s)", creator_id)

        await cb.answer("🔍 Отправлено менеджеру на проверку.")
        try:
            await _safe_edit_task_markup(cb.message, reply_markup=None)
        except Exception:
            pass
        return

    # ORDER actions (TD) -> accept order or open supplier payment flow
    if action == "pay_supplier" and task.get("type") in {TaskType.ORDER_PROFILE, TaskType.ORDER_GLASS, TaskType.ORDER_MATERIALS}:
        _order_payload = try_json_loads(task.get("payload_json"))
        _order_inv_id = _order_payload.get("invoice_id") if _order_payload else None

        # Installer order without project — just accept task + update invoice status
        if not project:
            await cb.answer()
            try:
                task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
            except Exception as e:
                log.error("pay_supplier: update_task_status failed: %s", e, exc_info=True)
                return
            # Update glass_order_status on linked invoice
            if _order_inv_id:
                await db.conn.execute(
                    "UPDATE invoices SET glass_order_status = 'заказано' WHERE id = ?",
                    (int(_order_inv_id),),
                )
                await db.conn.commit()
            # Notify installer
            _sender_id = _order_payload.get("sender_id") if _order_payload else None
            if _sender_id:
                initiator = await get_initiator_label(db, cb.from_user.id)
                _desc = (_order_payload.get("description") or "")[:100] if _order_payload else ""
                await notifier.safe_send(
                    int(_sender_id),
                    f"✅ <b>Заявка на материалы принята</b>\n"
                    f"👤 {initiator}\n"
                    f"📦 {_desc}",
                )
            kb = task_actions_kb(task)
            await _safe_edit_task_markup(cb.message, reply_markup=kb)
            return

        # With project — open full supplier payment flow
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        await state.update_data(project_id=int(project["id"]), source_order_task_id=int(task_id))
        await state.set_state(SupplierPaymentSG.supplier)
        await cb.message.answer(
            "💸 <b>Оплата поставщику</b>\n"
            f"Проект: <b>{project.get('title','')}</b>\n\n"
            "Укажите поставщика (название компании):"
        )  # type: ignore
        return

    # INVOICE_PAYMENT — шаг 1: Принять (OPEN → IN_PROGRESS)
    if action == "inv_received" and task.get("type") == TaskType.INVOICE_PAYMENT:
        # Кредит-заявку нельзя проводить обычной «оплатой поставщику» (читает
        # material_type → теряет cost_type=montazh, перетирает назначение, не
        # трогает zp_installer → задвоение ЗП). Перенаправляем на кредит-флоу.
        _cpr0 = try_json_loads(task.get("payload_json"))
        if isinstance(_cpr0, dict) and _cpr0.get("kind") == "credit_payment_request":
            await cb.answer()
            _b = InlineKeyboardBuilder()
            _b.button(text="✅ Исполнить", callback_data=f"credit_exec:{task_id}")
            _b.button(text="❌ Отклонить", callback_data=f"credit_rej:{task_id}")
            _b.adjust(1)
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Это кредит-заявка (расход кошелька). Исполните её кнопкой "
                "«✅ Исполнить» ниже — обычная «оплата поставщику» здесь не применяется.",
                reply_markup=_b.as_markup(),
            )
            return
        if task.get("status") != TaskStatus.OPEN:
            await cb.answer("Этот счёт уже принят.", show_alert=True)
            return
        await cb.answer()
        try:
            task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        except Exception as e:
            log.error("inv_received: update_task_status failed: %s", e, exc_info=True)
            return
        payload = try_json_loads(task.get("payload_json"))
        _inv_num = payload.get("invoice_number") or ""
        _amount = payload.get("amount") or ""
        _mat_type = payload.get("material_type") or ""
        # Уведомить РП о принятии
        sender_id = _invoice_task_sender_id(payload)
        _inv_id_for_audit = payload.get("invoice_id")
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="invoice_received_by_gd",
                entity="invoice",
                entity_id=str(_inv_id_for_audit) if _inv_id_for_audit else None,
                payload={
                    "invoice_number": _inv_num,
                    "supplier": payload.get("supplier"),
                    "amount": _amount,
                    "material_type": _mat_type,
                    "task_id": task_id,
                    "sender_id": sender_id,
                },
            )
        except Exception:
            log.exception("inv_received: audit() failed for invoice=%s", _inv_id_for_audit)
        # DS «Затр. Грузчики» (owner 25.07): пишем ПРИ ПРИНЯТИИ задачи в работу,
        # а не после оплаты. Сумма оплаты совпадает с заявленной (owner), поэтому
        # на шаге оплаты cost_loaders повторно НЕ прибавляем — метка
        # ds_cost_applied в payload это гарантирует. Остальные столбцы затрат
        # (DP/DQ/DR/DT/DU/DV) по-прежнему заполняются при оплате.
        if _mat_type == "loaders" and _inv_id_for_audit and not payload.get("ds_cost_applied"):
            try:
                _amt_ds = float(_amount or 0)
            except (TypeError, ValueError):
                _amt_ds = 0.0
            if _amt_ds > 0:
                try:
                    await db.bump_invoice_cost(int(_inv_id_for_audit), "loaders", _amt_ds)
                    await db.update_task_payload(task_id, {"ds_cost_applied": True})
                    payload["ds_cost_applied"] = True
                    await integrations.sync_invoice_row(int(_inv_id_for_audit))
                except Exception:
                    log.exception(
                        "inv_received: DS (cost_loaders) при принятии не записан, inv=%s",
                        _inv_id_for_audit,
                    )
        if sender_id:
            from ..enums import MATERIAL_TYPE_LABELS
            _mat_label = MATERIAL_TYPE_LABELS.get(_mat_type, _mat_type)
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                f"✅ <b>Счёт принят ГД</b>\n"
                f"👤 {initiator}\n"
                f"🔢 № счёта: {_inv_num}\n"
                f"💰 Сумма: {_amount}\n"
                f"📦 Тип: {_mat_label}",
            )
        # Показать ГД кнопку "Подтвердить оплату"
        kb = task_actions_kb(task)
        try:
            await notifier.bot.send_message(
                cb.from_user.id,
                "✅ Счёт принят. Нажмите «💳 Подтвердить оплату» для завершения.",
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            log.error("inv_received: send_message failed: %s", e, exc_info=True)
        try:
            await _safe_edit_task_markup(cb.message, reply_markup=None)
        except Exception:
            pass
        return

    # INVOICE_PAYMENT — шаг 2: Подтвердить оплату (вложение + комментарий → закрыть)
    if action == "inv_pay" and task.get("type") == TaskType.INVOICE_PAYMENT:
        _cpr1 = try_json_loads(task.get("payload_json"))
        if isinstance(_cpr1, dict) and _cpr1.get("kind") == "credit_payment_request":
            await cb.answer()
            _b = InlineKeyboardBuilder()
            _b.button(text="✅ Исполнить", callback_data=f"credit_exec:{task_id}")
            _b.button(text="❌ Отклонить", callback_data=f"credit_rej:{task_id}")
            _b.adjust(1)
            await cb.message.answer(  # type: ignore[union-attr]
                "⚠️ Это кредит-заявка (расход кошелька). Исполните её кнопкой "
                "«✅ Исполнить» ниже — обычная оплата поставщику здесь не применяется.",
                reply_markup=_b.as_markup(),
            )
            return
        if task.get("status") != TaskStatus.IN_PROGRESS:
            await cb.answer("Сначала примите счёт.", show_alert=True)
            return
        await cb.answer()
        try:
            await _safe_edit_task_markup(cb.message, reply_markup=None)
        except Exception:
            pass
        await state.clear()
        await state.set_state(InvoicePaymentSG.attaching_pp)
        await state.update_data(invoice_task_id=task_id)
        # Дублируем карточку «Детали» оплаты для ГД («Объект» = адрес объекта,
        # резолвим из parent_invoice_id; запрос ГД 01.06). Только отображение.
        # Вся сборка в try/except Exception: сбой построения Детали НЕ должен
        # ронять подтверждение оплаты (паттерн РП-карточки 01.06).
        _details = ""
        try:
            from ..utils import fmt_payment_details_card
            _pay_payload = try_json_loads(task.get("payload_json"))
            _obj_addr = ""
            _pid = _pay_payload.get("parent_invoice_id") or _pay_payload.get("invoice_id")
            if _pid:
                try:
                    _pinv = await db.get_invoice(int(_pid))
                    if _pinv:
                        _obj_addr = _pinv.get("object_address") or ""
                except (TypeError, ValueError):
                    pass
            _details = fmt_payment_details_card(_pay_payload, object_address=_obj_addr)
        except Exception:
            _details = ""
        b = InlineKeyboardBuilder()
        b.button(text="✅ Отправить", callback_data=f"inv_pp_done:{task_id}")
        b.button(text="✅ Оплачено (без платёжки)", callback_data=f"inv_pp_paid_no_pdf:{task_id}")
        b.button(text="❌ Отмена", callback_data=f"inv_pp_cancel:{task_id}")
        b.adjust(1)
        _confirm_text = (
            "💳 <b>Подтверждение оплаты</b>\n\n"
            "Прикрепите документ (PDF/фото) и/или напишите комментарий, "
            "затем нажмите «✅ Отправить».\n\n"
            "Если платёжки нет — можно просто нажать «✅ Оплачено (без платёжки)» "
            "(по желанию добавив комментарий)."
        )
        if _details:
            _confirm_text = _details + "\n\n" + _confirm_text
        await notifier.bot.send_message(
            cb.from_user.id,
            _confirm_text,
            reply_markup=b.as_markup(),
            parse_mode="HTML",
        )
        return

    if action == "inv_hold" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        # Mark as in_progress (on hold)
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        payload = try_json_loads(task.get("payload_json"))
        invoice_id, invoice_number, supplier, amount = _invoice_task_details(payload)
        if invoice_id is not None:
            await db.update_invoice_status(invoice_id, InvoiceStatus.ON_HOLD)
            if invoice_number:
                await integrations.sync_invoice_status(invoice_number, InvoiceStatus.ON_HOLD)
        sender_id = _invoice_task_sender_id(payload)
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="invoice_held_by_gd",
                entity="invoice",
                entity_id=str(invoice_id) if invoice_id is not None else None,
                payload={
                    "invoice_number": invoice_number,
                    "supplier": supplier,
                    "amount": amount,
                    "task_id": task_id,
                    "sender_id": sender_id,
                    "new_status": InvoiceStatus.ON_HOLD,
                },
            )
        except Exception:
            log.exception("inv_hold: audit() failed for invoice=%s", invoice_id)
        if sender_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                "⏸ <b>Счёт отложен</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "⏸ Счёт отложен.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    if action == "inv_reject" and task.get("type") == TaskType.INVOICE_PAYMENT:
        if task.get("status") not in active_statuses:
            await cb.answer("Этот счёт уже обработан.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        payload = try_json_loads(task.get("payload_json"))
        invoice_id, invoice_number, supplier, amount = _invoice_task_details(payload)
        # Отклоняем ТОЛЬКО заявку на оплату — сам счёт-проект не трогаем.
        # Прежде здесь стоял update_invoice_status(invoice_id, REJECTED):
        # отклонение дубль-заявки роняло весь счёт в rejected и прятало его
        # расходы в таблице (инцидент 2649-1КВ, 2026-05-27).
        sender_id = _invoice_task_sender_id(payload)
        try:
            await db.audit(
                actor_id=cb.from_user.id,
                action="invoice_payment_rejected_by_gd",
                entity="invoice",
                entity_id=str(invoice_id) if invoice_id is not None else None,
                payload={
                    "invoice_number": invoice_number,
                    "supplier": supplier,
                    "amount": amount,
                    "task_id": task_id,
                    "sender_id": sender_id,
                },
            )
        except Exception:
            log.exception("inv_reject: audit() failed for invoice=%s", invoice_id)
        if sender_id:
            initiator = await get_initiator_label(db, cb.from_user.id)
            await notifier.safe_send(
                int(sender_id),
                "❌ <b>Заявка на оплату отклонена</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {invoice_number or '—'}\n"
                f"🏢 Поставщик: {supplier or '—'}\n"
                f"💰 Сумма: {amount or '—'}",
            )
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "❌ Заявка на оплату отклонена. РП уведомлён.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return

    # DELIVERY_REQUEST — ГД принял заявку (в работу)
    if action == "del_accept" and task.get("type") == TaskType.DELIVERY_REQUEST:
        if task.get("status") != TaskStatus.OPEN:
            await cb.answer("Заявка уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.IN_PROGRESS)
        await db.accept_task(task_id)
        # Notify RP
        rp_id = task.get("created_by")
        if rp_id:
            await notifier.safe_send(
                int(rp_id),
                f"✅ <b>Оплата доставки — принято ГД</b>\n"
                f"Задача #{task_id} в работе.",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(rp_id))
        # Show updated card
        task_kb = task_actions_kb(task)
        try:
            await cb.message.edit_reply_markup(reply_markup=task_kb)  # type: ignore
        except TelegramBadRequest:
            pass
        await cb.answer("Принято, статус: в работе")
        return

    # DELIVERY_REQUEST — ГД оплачивает доставку (FSM: сумма → комментарий → платёжка)
    if action == "del_pay" and task.get("type") == TaskType.DELIVERY_REQUEST:
        if task.get("status") != TaskStatus.IN_PROGRESS:
            await cb.answer("Задача не в работе.", show_alert=True)
            return
        await state.clear()
        await state.update_data(delivery_task_id=task_id)
        await state.set_state(DeliveryPaymentSG.amount)
        payload = try_json_loads(task.get("payload_json"))
        est = payload.get("estimated_logistics") or "—"
        await cb.message.answer(  # type: ignore
            f"💳 <b>Оплата доставки</b>\n"
            f"Задача #{task_id}\n"
            f"🚚 Расч. логистика: {est}\n\n"
            "Введите фактическую стоимость доставки (число):",
        )
        return

    # MONTAZH — подтверждение задачи (Да/Нет/Комментарий)
    if action == "montazh_yes" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.DONE)
        payload = try_json_loads(task.get("payload_json"))
        comment_text = payload.get("comment", "")
        gd_id = task.get("created_by")
        user_label = await get_initiator_label(db, cb.from_user.id)
        if gd_id:
            await notifier.safe_send(
                int(gd_id),
                f"✅ <b>Задача подтверждена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}\n\n"
                f"📋 {comment_text}" if comment_text else
                f"✅ <b>Задача подтверждена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(gd_id))
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        await cb.message.edit_text(  # type: ignore[union-attr]
            "✅ Задача подтверждена.",
            reply_markup=None,
        )
        return

    if action == "montazh_no" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        task = await db.update_task_status(task_id, TaskStatus.REJECTED)
        payload = try_json_loads(task.get("payload_json"))
        comment_text = payload.get("comment", "")
        gd_id = task.get("created_by")
        user_label = await get_initiator_label(db, cb.from_user.id)
        if gd_id:
            await notifier.safe_send(
                int(gd_id),
                f"❌ <b>Задача отклонена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}\n\n"
                f"📋 {comment_text}" if comment_text else
                f"❌ <b>Задача отклонена (Монтажная гр.)</b>\n"
                f"👤 От: {user_label}",
            )
            await refresh_recipient_keyboard(notifier, db, config, int(gd_id))
        await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
        await state.clear()
        await cb.message.edit_text(  # type: ignore[union-attr]
            "❌ Задача отклонена.",
            reply_markup=None,
        )
        return

    if action == "montazh_comment" and task.get("type") == TaskType.GD_TASK:
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже обработана.", show_alert=True)
            return
        await state.clear()
        await state.set_state(MontazhCommentSG.text)
        await state.update_data(montazh_task_id=task_id)
        await cb.message.answer(  # type: ignore[union-attr]
            "💬 Введите комментарий к задаче:",
        )
        return

    # DONE (generic)
    if action == "done":
        if task.get("status") not in active_statuses:
            await cb.answer("Эта задача уже закрыта.", show_alert=True)
            return
        # Перерасчёт прибыли закрывается ТОЛЬКО кнопкой согласия (recalc_agree →
        # зачисление аванса). keyboards.py убирает generic-кнопки у таких задач,
        # но устаревшая кнопка из старого сообщения досюда доходит и закрыла бы
        # задачу МИМО денег — ровно так 03.07 «съелись» три задачи (336/337/338).
        if task.get("type") == TaskType.RECALC_CONFIRM:
            await cb.answer(
                "Закройте кнопкой «✅ С перерасчётом согласен» — иначе аванс "
                "не начислится.",
                show_alert=True,
            )
            return
        # Оклад РП / ЗП РП 10% — тот же капкан, но кнопка «✅ Выплатить»; прецедент —
        # задача 320 (оклад 2026-07, 66 000 ₽), 30.06 15:33. Состав типов и тексты —
        # в _DONE_BLOCKED_PAYOUT_TYPES (см. комментарий у константы). Гард стоит ПОСЛЕ
        # проверки статуса (уже закрытая задача отвечает как раньше) и ДО любой записи.
        _payout_only_alert = _DONE_BLOCKED_PAYOUT_TYPES.get(task.get("type"))
        if _payout_only_alert:
            await cb.answer(_payout_only_alert, show_alert=True)
            return
        # For request/closing tasks we can optionally collect and send attachments to manager
        _docs_reply = (
            task.get("type") in {TaskType.DOCS_REQUEST, TaskType.QUOTE_REQUEST, TaskType.CLOSING_DOCS}
            and project
        )
        # «🚨 Срочно ГД» (owner 03.08): бухгалтерия просит у ГД выписки — ответ обязан
        # вернуться постановщику ФАЙЛАМИ, а не только карточкой «Задача выполнена».
        # Адресат — created_by, а не manager_id: у urgent_gd project_id пуст, поэтому
        # прежнее условие `and project` не пускало сюда этот тип в принципе.
        _urgent_reply = task.get("type") == TaskType.URGENT_GD
        if _docs_reply or _urgent_reply:
            if _docs_reply:
                target_user_id = project.get("manager_id")
                if task.get("type") == TaskType.CLOSING_DOCS:
                    target_user_id = project.get("manager_id")
                _prompt = (
                    "Пришлите готовые документы (файлы/фото) несколькими сообщениями.\n"
                    "Когда закончите — нажмите «✅ Отправить и закрыть».\n"
                    "Или можно «⏭ Закрыть без отправки»."
                )
            else:
                target_user_id = task.get("created_by")
                _prompt = (
                    "📎 Приложите файлы для ответа постановщику — можно несколькими "
                    "сообщениями.\n"
                    "Когда закончите — нажмите «✅ Отправить и закрыть».\n"
                    "Если файлы не нужны — «⏭ Закрыть без отправки»."
                )
            await _safe_edit_task_markup(cb.message, reply_markup=None)
            await state.clear()
            await state.set_state(TaskCompleteSG.attachments)
            await state.update_data(task_id=task_id, target_user_id=target_user_id)

            b = InlineKeyboardBuilder()
            b.button(text="✅ Отправить и закрыть", callback_data="taskcomplete:send")
            b.button(text="⏭ Закрыть без отправки", callback_data="taskcomplete:skip")
            b.adjust(1)

            await cb.message.answer(
                _prompt,
                reply_markup=b.as_markup(),
            )  # type: ignore
            return

        # simple close
        task = await db.update_task_status(
            task_id, TaskStatus.DONE,
            expected_statuses=tuple(active_statuses),
        )
        if task is None:
            await cb.answer("Задача уже была обработана.", show_alert=True)
            return
        project = await _apply_done_side_effects(db, integrations, task, project)
        await _notify_task_creator_done(
            db,
            notifier,
            config,
            cb.from_user.id if cb.from_user else None,
            task,
            project,
        )
        await _safe_edit_task_markup(cb.message, reply_markup=None)
        await state.clear()
        if cb.from_user:
            role_now, isolated_role = await _current_menu(db, cb.from_user.id)
            await _answer_with_menu(
                cb.message,
                db,
                config,
                cb.from_user.id,
                "✅ Закрыл задачу.",
                role=role_now,
                isolated_role=isolated_role,
            )
        return


@router.message(TaskCompleteSG.attachments)
async def taskcomplete_collect(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    uid = message.from_user.id if message.from_user else "anon"
    att, count = await collect_attachment(message, state, storage, prefix=f"tasks/{uid}")
    if att is None:
        await message.answer("Пришлите файл/фото или нажмите кнопку «✅ Отправить и закрыть».")
        return
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    await answer_service(message, f"📎 Принял. Сейчас файлов: <b>{count}</b>.{suffix}")

@router.callback_query(F.data.in_({"taskcomplete:send", "taskcomplete:skip"}))
async def taskcomplete_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    data = await state.get_data()
    task_id = data.get("task_id")
    if not task_id:
        await cb.message.answer("Не вижу задачу. /cancel")  # type: ignore
        await state.clear()
        return

    try:
        task = await db.get_task(int(task_id))
    except KeyError:
        await cb.message.answer("Задача не найдена.")  # type: ignore
        await state.clear()
        return
    if task.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        await cb.message.answer("Эта задача уже закрыта.")  # type: ignore[union-attr]
        await state.clear()
        return
    # 🔴 Гард типа — см. _TASKCOMPLETE_ALLOWED_TYPES. Сюда попадает task_id из FSM, а не
    # из callback_data, поэтому устаревшая кнопка из старого сообщения может указывать на
    # ЧУЖУЮ задачу — например на оклад РП, id которого положил в тот же ключ флоу
    # отклонения (td.py:2621). Закрывать такую задачу этим путём нельзя: денежная запись
    # идёт только через свой флоу. Состояние НАМЕРЕННО не чистим — человек может быть в
    # середине другого потока, и state.clear() сломал бы его.
    if task.get("type") not in _TASKCOMPLETE_ALLOWED_TYPES:
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Эта кнопка — от другой задачи (устаревшее сообщение). "
            f"Задача #{task_id} закрывается своими кнопками, откройте её из списка задач."
        )
        return
    project = await db.get_project(int(task["project_id"])) if task.get("project_id") else None
    target_user_id = data.get("target_user_id")

    # Save attachments to DB (for history)
    # ⚠️ Сохраняем ВСЕГДА, независимо от нажатой кнопки. taskcomplete_collect (см. ниже)
    # заливает каждый файл в MinIO СРАЗУ при получении (mirror_attachment), то есть объект
    # в хранилище уже создан; без строки в attachments указателя на него не остаётся
    # нигде — файл теряется молча. «⏭ Закрыть без отправки» означает «не слать адресату»,
    # а НЕ «удалить присланное». Отправка ниже закрыта своим условием.
    attachments = data.get("attachments", [])
    for a in attachments:
        await db.add_attachment(
            task_id=int(task_id),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    # Send attachments to target (manager)
    if cb.data == "taskcomplete:send" and target_user_id:
        manager_markup = (
            manager_project_actions_kb(int(project["id"]))
            if project and task.get("project_id")
            else None
        )
        initiator = await get_initiator_label(db, cb.from_user.id)
        # «Срочно ГД» — это ответ ГД постановщику, а не «документы по проекту»: шапка
        # своя, чтобы бухгалтерия видела, на какой свой запрос пришёл файл.
        if task.get("type") == TaskType.URGENT_GD:
            _head = (
                f"📎 <b>Ответ на задачу #{task_id}</b>\n"
                f"👤 От: {initiator}\n\n"
                f"См. вложения."
            )
        else:
            _head = (
                f"📄 <b>Документы по задаче #{task_id} готовы</b>\n"
                f"👤 От: {initiator}\n\n"
                f"См. вложения."
            )
        await notifier.safe_send(
            int(target_user_id),
            _head,
            reply_markup=manager_markup,
        )
        # send actual files
        for a in attachments:
            await notifier.safe_send_media(int(target_user_id), a["file_type"], a["file_id"], caption=a.get("caption"))

    # Close task and update project status
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)
    project = await _apply_done_side_effects(db, integrations, task, project)
    await _notify_task_creator_done(
        db,
        notifier,
        config,
        cb.from_user.id if cb.from_user else None,
        task,
        project,
    )

    await _safe_edit_task_markup(cb.message, reply_markup=None)
    if cb.from_user:
        role_now, isolated_role = await _current_menu(db, cb.from_user.id)
        await _answer_with_menu(
            cb.message,
            db,
            config,
            cb.from_user.id,
            "✅ Готово.",
            role=role_now,
            isolated_role=isolated_role,
        )
    await state.clear()



# ---------------------------------------------------------------------------
# Invoice payment: attach payment order (PP) and send to RP
# ---------------------------------------------------------------------------

@router.message(InvoicePaymentSG.attaching_pp)
async def invoice_pp_collect(
    message: Message,
    state: FSMContext,
    storage: MinioStorage | None = None,
) -> None:
    """Collect payment order attachments from GD."""
    data = await state.get_data()

    uid = message.from_user.id if message.from_user else "anon"
    att, pp_count = await collect_attachment(
        message, state, storage, prefix=f"tasks/{uid}", key="pp_files"
    )
    if att is not None:
        suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
        await answer_service(message, f"📎 Принял. Файлов: <b>{pp_count}</b>.{suffix}")
    elif message.text:
        # Текстовый комментарий от ГД
        pp_comment = data.get("pp_comment", "")
        pp_comment = (pp_comment + "\n" + message.text).strip() if pp_comment else message.text.strip()
        await state.update_data(pp_comment=pp_comment)
        await answer_service(message, "💬 Комментарий сохранён.")
    else:
        await message.answer("Прикрепите файл/фото или напишите комментарий.")
        return


async def _invoice_pp_finalize_core(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
    u: Any,
    task_id: int,
    pp_files: list[dict[str, Any]],
    pp_comment: str,
    no_pdf_mode: bool = False,
) -> None:
    """Core: close invoice task and notify RP. Used by inv_pp_done and inv_pp_paid_no_pdf."""
    task = await db.get_task(int(task_id))
    if task.get("status") not in {TaskStatus.OPEN, TaskStatus.IN_PROGRESS}:
        await state.clear()
        await cb.message.answer("Этот счёт уже обработан.")  # type: ignore[union-attr]
        return
    payload = try_json_loads(task.get("payload_json"))
    # КРИТИЧНО: кредит-заявку нельзя финализировать обычной оплатой поставщику —
    # она читает material_type (у кредит-заявки его нет → extra_mat), перетирает
    # назначение и НЕ закрывает парную zp_installer → потеря типа расхода и
    # задвоение ЗП монтажника. Исполняется только через _finalize_credit_execution.
    if isinstance(payload, dict) and payload.get("kind") == "credit_payment_request":
        await state.clear()
        await cb.message.answer(  # type: ignore[union-attr]
            "⚠️ Это кредит-заявка — исполните её через кредит-расход (кнопка "
            "«✅ Исполнить»), а не обычной оплатой поставщику."
        )
        return
    sender_id = _invoice_task_sender_id(payload)
    invoice_id, inv_num, supplier, amount = _invoice_task_details(payload)

    # Save GD comment into task payload for RP visibility
    if pp_comment:
        payload["pp_comment"] = pp_comment
        import json as _json
        await db.conn.execute(
            "UPDATE tasks SET payload_json = ? WHERE id = ?",
            (_json.dumps(payload, ensure_ascii=False), int(task_id)),
        )
        await db.conn.commit()

    # Mark task as done (parent invoice status is NOT changed)
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)

    # Auto-create SUPPLIER_PAYMENT for cost tracking
    _parent_inv_id = payload.get("parent_invoice_id") or payload.get("invoice_id")
    try:
        _audit_inv_id = invoice_id if invoice_id is not None else _parent_inv_id
        await db.audit(
            actor_id=u.id,
            action="invoice_paid_by_gd",
            entity="invoice",
            entity_id=str(_audit_inv_id) if _audit_inv_id is not None else None,
            payload={
                "invoice_number": inv_num,
                "supplier": supplier,
                "amount": amount,
                "task_id": task_id,
                "parent_invoice_id": _parent_inv_id,
                "has_pp_files": bool(pp_files),
                "pp_files_count": len(pp_files) if pp_files else 0,
                "has_pp_comment": bool(pp_comment),
                "no_pdf_mode": bool(no_pdf_mode),
                "sender_id": sender_id,
            },
        )
    except Exception:
        log.exception("invoice_pp_finalize: audit() failed for invoice=%s", invoice_id)
    _sp_amount = payload.get("amount")
    _sp_id: int | None = None
    if _parent_inv_id is not None and _sp_amount:
        _sp_mat_type = payload.get("material_type") or "extra_mat"
        _sp_supplier = payload.get("supplier") or ""
        _sp_inv_num = payload.get("invoice_number") or ""
        try:
            sp_task = await db.create_task(
                project_id=int(task.get("project_id") or 0) or None,
                type_=TaskType.SUPPLIER_PAYMENT,
                status=TaskStatus.DONE,
                created_by=u.id,
                assigned_to=int(sender_id) if sender_id else u.id,
                due_at_iso=None,
                payload={
                    "supplier": _sp_supplier,
                    "amount": float(_sp_amount),
                    "invoice_number": _sp_inv_num,
                    "material_type": _sp_mat_type,
                    "parent_invoice_id": int(_parent_inv_id),
                    "td_id": u.id,
                    "td_username": getattr(u, "username", "") or "",
                    "auto_from_invoice_payment": int(task_id),
                },
            )
            # Also write to supplier_payments table
            _sp_id = await db.create_supplier_payment(
                parent_invoice_id=int(_parent_inv_id),
                amount=float(_sp_amount),
                material_type=_sp_mat_type,
                invoice_number=_sp_inv_num,
                supplier=_sp_supplier,
                task_id=sp_task["id"] if sp_task else None,
                created_by=u.id,
                # DS «Затр. Грузчики» уже записан при принятии задачи в работу
                # (owner 25.07) — второй раз к cost_loaders не прибавляем.
                update_cost=not bool(payload.get("ds_cost_applied")),
            )
        except Exception:
            log.warning("Failed to auto-create SUPPLIER_PAYMENT from task %s", task_id, exc_info=True)

        # п.2 (10.06): авто-списание кредит-кошелька при оплате поставщику по
        # КРЕДИТ-счёту. ГД уже подтвердил оплату на этом шаге → списываем сразу,
        # без доп. гейта (решение user 10.06). Закрывает дыру, из-за которой КВ 8
        # «потерял» 24 812,59 (оплаты наполняли cost_*/DU, но не трогали кошелёк).
        # Отдельный try: сбой списания НЕ должен ломать финализацию/уведомление РП.
        if _sp_id is not None:
            try:
                _parent_inv = await db.get_invoice(int(_parent_inv_id))
                _pc_role = (_parent_inv or {}).get("creator_role") or ""
                if (
                    _parent_inv
                    and int(_parent_inv.get("is_credit") or 0) == 1
                    and _pc_role in ("manager_kv", "manager_kia", "manager_npn")
                ):
                    from ..utils import apply_credit_wallet_spend
                    _cw_purpose = (
                        f"Оплата поставщику (счёт {_sp_inv_num})"
                        if _sp_inv_num else "Оплата поставщику"
                    )
                    if _sp_supplier:
                        _cw_purpose += f" — {_sp_supplier}"
                    await apply_credit_wallet_spend(
                        db, integrations,
                        wallet_role=_pc_role,
                        amount=float(_sp_amount),
                        mode="bound",
                        purpose=_cw_purpose,
                        entered_by=u.id,
                        invoice_id=int(_parent_inv_id),
                        cost_type=_sp_mat_type,
                        invoice_number=_sp_inv_num,
                        existing_supplier_payment_id=int(_sp_id),
                    )
                    log.info(
                        "п.2 авто-списание кошелька %s: счёт=%s sp=%s сумма=%s",
                        _pc_role, _sp_inv_num, _sp_id, _sp_amount,
                    )
            except Exception:
                log.warning(
                    "п.2 авто-списание кошелька не удалось (task=%s, inv=%s)",
                    task_id, _parent_inv_id, exc_info=True,
                )

    project = None
    if task.get("project_id"):
        try:
            project = await db.get_project(int(task["project_id"]))
        except Exception:
            log.warning("Failed to get project %s for task %s", task.get("project_id"), task_id, exc_info=True)

    # Save PP attachments to task
    for a in pp_files:
        await db.add_attachment(
            task_id=int(task_id),
            file_id=a["file_id"],
            file_unique_id=a.get("file_unique_id"),
            file_type=a["file_type"],
            caption=a.get("caption"),
            minio_object_key=a.get("minio_object_key"),
        )

    # Notify RP
    if sender_id:
        title_txt = "Счёт оплачен" if pp_files else "Счёт оплачен (без платёжки)"
        # Карточка В1 (стиль стартовой РП): «Поставщик» был пуст → РП угадывал
        # платёж. Money-хендлер: ЛЮБОЙ сбой сборки НЕ должен ломать финализацию /
        # отправку РП → fallback на старый текст.
        try:
            import re as _re
            from ..utils import format_card_section
            from ..rp_start_card import CATS, _money, _mt_to_cat, _street

            def _short(block: str) -> str:
                parts = block.split("<pre>", 1)
                if len(parts) == 2:
                    return _re.sub(r"━{2,}", "━", parts[0]) + "<pre>" + parts[1]
                return block

            # Адрес объекта: первый счёт (дочерний/родитель) с непустым адресом
            object_address = ""
            for _aid in (invoice_id, _parent_inv_id):
                if _aid is not None:
                    _inv_a = await db.get_invoice(int(_aid))
                    if _inv_a and (_inv_a.get("object_address") or "").strip():
                        object_address = _inv_a["object_address"]
                        break

            _cat = _mt_to_cat(payload.get("material_type"))
            _crow = next((c for c in CATS if c[0] == _cat), None)
            _ptype = f"{_crow[1]} {_crow[3]}" if _crow else "🧱 Доп.мат"

            items = [
                ("От", "Ген.Дир"),
                ("Объект", _street(object_address, 22) if object_address else "—"),
                ("№ счёта", inv_num),
                ("Тип", _ptype),
            ]
            msg = _short(format_card_section(
                emoji="✅",
                title=title_txt,
                items=items,
                total=f"{_money(amount)} ₽",
                width=30,
                compact=True,
            ))
        except Exception:
            log.exception("invoice_pp_finalize: build RP card failed, fallback to plain text")
            initiator = await get_initiator_label(db, u.id)
            msg = (
                f"✅ <b>{title_txt}</b>\n"
                f"👤 От: {initiator}\n\n"
                f"🔢 № счёта: {inv_num}\n"
                f"🏢 Поставщик: {supplier}\n"
                f"💰 Сумма: {amount}"
            )
        if pp_comment:
            msg += f"\n\n💬 Комментарий ГД: {pp_comment}"
        if pp_files:
            msg += "\n\n📎 Документ прикреплён ниже."
        await notifier.safe_send(int(sender_id), msg)
        for a in pp_files:
            await notifier.safe_send_media(
                int(sender_id), a["file_type"], a["file_id"], caption=a.get("caption"),
            )
        # Бейдж 🔴N на «Счета в Работе»: пуш РП проходит мимо счётчиков, поэтому
        # пишем непрочитанное в канал 'rp_invoice_paid' (гаснет при открытии раздела).
        # Money-хендлер → в try/except, чтобы НЕ сорвать финализацию.
        try:
            await db.save_chat_message(
                channel="rp_invoice_paid",
                sender_id=u.id,
                direction="incoming",
                text=f"Счёт оплачен: {inv_num}",
                receiver_id=int(sender_id),
                has_attachment=bool(pp_files),
                invoice_id=invoice_id if invoice_id is not None else _parent_inv_id,
            )
        except Exception:
            log.exception("invoice_pp_finalize: save badge chat_message failed")

    # Уведомить монтажника о поступлении оплаты (если счёт привязан)
    if invoice_id is not None:
        inv = await db.get_invoice(invoice_id)
        if inv and inv.get("assigned_to"):
            installer_id = inv["assigned_to"]
            inst_msg = (
                f"💰 <b>Оплата поступила</b>\n"
                f"📄 Счёт №{inv.get('invoice_number', inv_num)}\n"
                f"📍 {inv.get('object_address', '—')}\n"
            )
            await notifier.safe_send(int(installer_id), inst_msg)
            await refresh_recipient_keyboard(notifier, db, config, int(installer_id))

    await integrations.sync_task(task, project_code=project.get("code", "") if project else "")
    await _safe_edit_task_markup(cb.message, reply_markup=None)
    await state.clear()

    role_now, isolated_role = await _current_menu(db, u.id)
    ack_text = (
        "✅ Счёт оплачен. Платёжка отправлена РП."
        if pp_files
        else "✅ Счёт закрыт без платёжки. РП уведомлён."
    )
    await _answer_with_menu(
        cb.message,
        db,
        config,
        u.id,
        ack_text,
        role=role_now,
        isolated_role=isolated_role,
    )


# п.2 (10.06): in-flight claim от двойного клика по «Оплачено». Финализация
# теперь СПИСЫВАЕТ кредит-кошелёк (денежный confirm) → повторный вход недопустим,
# иначе задвоение списания ([[feedback_money_confirm_idempotent_gate]]). Claim
# берётся СИНХРОННО (до первого await), снимается в finally; после успеха задача
# уже DONE и статус-гард в _core ловит любые поздние клики.
_PP_FINALIZE_INFLIGHT: set[int] = set()


@router.callback_query(F.data.startswith("inv_pp_done:"))
async def invoice_pp_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Send payment order to RP and close invoice task (requires file or comment)."""
    u = cb.from_user
    if not u:
        await cb.answer()
        return
    data = await state.get_data()
    task_id = data.get("invoice_task_id")
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "")
    if not task_id:
        await cb.answer()
        await state.clear()
        return
    if not pp_files and not pp_comment:
        await cb.answer(
            "Прикрепите документ или напишите комментарий, потом нажмите «✅ Отправить».",
            show_alert=True,
        )
        return
    tid = int(task_id)
    if tid in _PP_FINALIZE_INFLIGHT:
        await cb.answer("Счёт уже обрабатывается, подождите…", show_alert=True)
        return
    _PP_FINALIZE_INFLIGHT.add(tid)
    await cb.answer()
    try:
        await _invoice_pp_finalize_core(
            cb, state, db, config, notifier, integrations,
            u, tid, pp_files, pp_comment, no_pdf_mode=False,
        )
    finally:
        _PP_FINALIZE_INFLIGHT.discard(tid)


@router.callback_query(F.data.startswith("inv_pp_paid_no_pdf:"))
async def invoice_pp_paid_no_pdf(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Close invoice task WITHOUT payment order PDF. Default comment 'Оплачено' if user typed nothing."""
    u = cb.from_user
    if not u:
        await cb.answer()
        return
    data = await state.get_data()
    task_id = data.get("invoice_task_id")
    pp_files = data.get("pp_files", [])
    pp_comment = data.get("pp_comment", "") or "Оплачено"
    if not task_id:
        await cb.answer()
        await state.clear()
        return
    tid = int(task_id)
    if tid in _PP_FINALIZE_INFLIGHT:
        await cb.answer("Счёт уже обрабатывается, подождите…", show_alert=True)
        return
    _PP_FINALIZE_INFLIGHT.add(tid)
    await cb.answer()
    try:
        await _invoice_pp_finalize_core(
            cb, state, db, config, notifier, integrations,
            u, tid, pp_files, pp_comment, no_pdf_mode=True,
        )
    finally:
        _PP_FINALIZE_INFLIGHT.discard(tid)


@router.callback_query(F.data.startswith("inv_pp_cancel:"))
async def invoice_pp_cancel(cb: CallbackQuery, state: FSMContext, config: Config, db: Database) -> None:
    """Cancel payment order attachment."""
    await cb.answer()
    await _safe_edit_task_markup(cb.message, reply_markup=None)
    await state.clear()
    u = cb.from_user
    if u:
        role, isolated_role = await _current_menu(db, u.id)
        await _answer_with_menu(
            cb.message,
            db,
            config,
            u.id,
            "Отменено.",
            role=role,
            isolated_role=isolated_role,
        )


# ---------------------------------------------------------------------------
# Montazh — ввод комментария к задаче
# ---------------------------------------------------------------------------

@router.message(MontazhCommentSG.text, F.text)
async def montazh_comment_text(
    message: Message,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    """Получен комментарий к задаче монтажной группы."""
    u = message.from_user
    if not u:
        return
    data = await state.get_data()
    task_id = data.get("montazh_task_id")
    if not task_id:
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите текст комментария:")
        return

    task = await db.get_task(task_id)
    if not task:
        await message.answer("Задача не найдена.")
        await state.clear()
        return

    # Уведомить ГД о комментарии
    gd_id = task.get("created_by")
    user_label = await get_initiator_label(db, u.id)
    payload = try_json_loads(task.get("payload_json"))
    task_comment = payload.get("comment", "")

    if gd_id:
        await notifier.safe_send(
            int(gd_id),
            f"💬 <b>Комментарий к задаче (Монтажная гр.)</b>\n"
            f"👤 От: {user_label}\n\n"
            f"📋 Задача: {task_comment}\n\n"
            f"💬 Комментарий: {text}",
        )
        await refresh_recipient_keyboard(notifier, db, config, int(gd_id))

    await state.clear()
    await message.answer(
        "✅ Комментарий отправлен ГД.",
        reply_markup=task_actions_kb(task),
    )


# ==================== ОПЛАТА ДОСТАВКИ — FSM ГД ====================


@router.message(DeliveryPaymentSG.amount)
async def delivery_payment_amount(message: Message, state: FSMContext) -> None:
    """GD enters actual delivery cost."""
    t = (message.text or "").strip().replace(" ", "").replace("\u00a0", "")
    # Parse number (supports 50000, 50k, 50К)
    raw = t.lower().replace("к", "000").replace("k", "000")
    try:
        amount = float(raw)
    except ValueError:
        await message.answer("Введите число (пример: 15000 или 15к):")
        return
    await state.update_data(delivery_amount=amount)
    await state.set_state(DeliveryPaymentSG.comment)

    b = InlineKeyboardBuilder()
    b.button(text="⏭ Без комментария", callback_data="delpay_gd:nocomment")
    b.adjust(1)
    await message.answer(
        f"Сумма: <b>{amount:.0f}₽</b>\n\n"
        "Комментарий (или нажмите кнопку):",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "delpay_gd:nocomment", DeliveryPaymentSG.comment)
async def delivery_payment_no_comment(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    await state.update_data(delivery_comment="", delivery_attachments=[])
    await state.set_state(DeliveryPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Завершить без файла", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await cb.message.answer(  # type: ignore
        "Прикрепите платёжку (PDF/фото) или завершите без файла:",
        reply_markup=b.as_markup(),
    )


@router.message(DeliveryPaymentSG.comment)
async def delivery_payment_comment(message: Message, state: FSMContext) -> None:
    t = (message.text or "").strip()
    await state.update_data(delivery_comment=t, delivery_attachments=[])
    await state.set_state(DeliveryPaymentSG.attachments)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Завершить без файла", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await message.answer(
        "Прикрепите платёжку (PDF/фото) или завершите без файла:",
        reply_markup=b.as_markup(),
    )


@router.message(DeliveryPaymentSG.attachments)
async def delivery_payment_attachment(message: Message, state: FSMContext) -> None:
    file_id = None
    file_type = None
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"

    if not file_id:
        b = InlineKeyboardBuilder()
        b.button(text="✅ Завершить", callback_data="delpay_gd:finalize")
        b.adjust(1)
        await message.answer("Прикрепите файл или завершите:", reply_markup=b.as_markup())
        return

    data = await state.get_data()
    attachments = data.get("delivery_attachments", [])
    attachments.append({"file_id": file_id, "type": file_type})
    await state.update_data(delivery_attachments=attachments)

    b = InlineKeyboardBuilder()
    b.button(text=f"✅ Завершить (файлов: {len(attachments)})", callback_data="delpay_gd:finalize")
    b.adjust(1)
    await message.answer(
        f"📎 Файл добавлен ({len(attachments)}). Ещё или завершить:",
        reply_markup=b.as_markup(),
    )


@router.callback_query(F.data == "delpay_gd:finalize")
@money_confirm_guard
async def delivery_payment_finalize(
    cb: CallbackQuery,
    state: FSMContext,
    db: Database,
    config: Config,
    notifier: Notifier,
    integrations: IntegrationHub,
) -> None:
    await cb.answer()
    u = cb.from_user
    if not u:
        return

    data = await state.get_data()
    task_id = data.get("delivery_task_id")
    amount = data.get("delivery_amount", 0)
    comment = data.get("delivery_comment", "")
    attachments = data.get("delivery_attachments", [])

    if not task_id:
        await cb.message.answer("Ошибка: задача не найдена.")  # type: ignore
        await state.clear()
        return

    task = await db.get_task(int(task_id))
    # Денежный confirm: финализация создаёт supplier_payment + (кредит) списывает
    # кошелёк → поздний/повторный клик после закрытия задачи НЕ должен провести
    # оплату повторно ([[feedback_money_confirm_idempotent_gate]]).
    if not task:
        await cb.message.answer("Ошибка: задача не найдена.")  # type: ignore
        await state.clear()
        return
    if str(task.get("status")) == "done":
        await cb.message.answer("Эта доставка уже оплачена.")  # type: ignore
        await state.clear()
        return
    payload = try_json_loads(task.get("payload_json"))
    inv_id = payload.get("invoice_id")
    inv_num = payload.get("invoice_number", "")

    # Save actual delivery cost to invoice
    if inv_id:
        await db.update_invoice(
            int(inv_id),
            actual_logistics=amount,
        )
        # Write to Google Sheets if available
        if integrations.sheets:
            try:
                await integrations.sheets.write_field_to_op(
                    inv_num, "estimated_logistics", amount,
                )
            except Exception:
                log.warning("Failed to write delivery cost to ОП sheet")

        # Root-fix (delpay_gd): помимо actual_logistics (план/факт) записываем
        # логистику как supplier_payment → cost_logistics ("Затр. Логистика"/DT),
        # как через пикер оплаты. Раньше этот канал был невидим в затратах/прибыли.
        # sp-строку линкуем к самой delivery-задаче (task_id), отдельную
        # SUPPLIER_PAYMENT-задачу не плодим. Money-хендлер → весь блок в try/except,
        # сбой НЕ должен срывать финализацию и уведомление РП.
        _sp_id: int | None = None
        if float(amount or 0) > 0:
            try:
                _sp_id = await db.create_supplier_payment(
                    parent_invoice_id=int(inv_id),
                    amount=float(amount),
                    material_type="logistics",
                    invoice_number=inv_num,
                    supplier="",
                    task_id=int(task_id),
                    created_by=u.id,
                )
            except Exception:
                log.warning(
                    "delivery_payment_finalize: create logistics supplier_payment "
                    "failed (task=%s inv=%s)", task_id, inv_id, exc_info=True,
                )
            # КРЕДИТ-счёт → списываем кредит-кошелёк (зеркало пикера, п.2 10.06),
            # переиспользуя готовый sp_id (без дубля оплаты); иначе — просто
            # пересинкаем строку счёта, чтобы cost_logistics дошёл до листа (DT).
            if _sp_id is not None:
                try:
                    _inv = await db.get_invoice(int(inv_id))
                    _role = (_inv or {}).get("creator_role") or ""
                    if (
                        _inv
                        and int(_inv.get("is_credit") or 0) == 1
                        and _role in ("manager_kv", "manager_kia", "manager_npn")
                    ):
                        from ..utils import apply_credit_wallet_spend
                        await apply_credit_wallet_spend(
                            db, integrations,
                            wallet_role=_role,
                            amount=float(amount),
                            mode="bound",
                            purpose=(
                                f"Оплата доставки (счёт {inv_num})"
                                if inv_num else "Оплата доставки"
                            ),
                            entered_by=u.id,
                            invoice_id=int(inv_id),
                            cost_type="logistics",
                            invoice_number=inv_num,
                            existing_supplier_payment_id=int(_sp_id),
                        )
                    else:
                        await integrations.sync_invoice_row(int(inv_id))
                except Exception:
                    log.warning(
                        "delivery_payment_finalize: credit debit / row sync "
                        "failed (task=%s inv=%s)", task_id, inv_id, exc_info=True,
                    )

    # Update task payload with payment info
    payload["gd_amount"] = amount
    payload["gd_comment"] = comment
    payload["gd_attachments"] = attachments
    import json as _json
    from ..utils import to_iso, utcnow
    await db.conn.execute(
        "UPDATE tasks SET payload_json = ?, updated_at = ? WHERE id = ?",
        (_json.dumps(payload, ensure_ascii=False), to_iso(utcnow()), int(task_id)),
    )
    await db.conn.commit()

    # Close task
    task = await db.update_task_status(int(task_id), TaskStatus.DONE)

    # Notify RP
    rp_id = task.get("created_by")
    if rp_id:
        msg = (
            f"💳 <b>Доставка оплачена</b>\n"
            f"Счёт: {inv_num}\n"
            f"💰 Сумма: <b>{amount:.0f}₽</b>\n"
        )
        if comment:
            msg += f"📝 Комментарий: {comment}\n"
        await notifier.safe_send(int(rp_id), msg)
        # Send payment attachments to RP
        for att in attachments:
            await notifier.safe_send_media(
                int(rp_id), att.get("type", "document"), att["file_id"],
            )
        await refresh_recipient_keyboard(notifier, db, config, int(rp_id))

    await state.clear()
    await cb.message.answer(  # type: ignore
        f"✅ Доставка оплачена: {amount:.0f}₽. Задача закрыта.",
    )


# ---------------------------------------------------------------------------
# Orphan-PDF auto-link: catch PDF/photo sent BEFORE 'inv_pay' was clicked
# ---------------------------------------------------------------------------

@router.message(F.document | F.photo)
async def invoice_pp_orphan_catch(
    message: Message,
    state: FSMContext,
    db: Database,
    storage: MinioStorage | None = None,
) -> None:
    """
    Catch document/photo sent outside InvoicePaymentSG.attaching_pp.
    If user has exactly ONE INVOICE_PAYMENT task in IN_PROGRESS, auto-link
    the file to it (sets FSM state and pp_files), so the PDF is not lost.
    Reason: GD sometimes sends payment PDF BEFORE clicking "💸 Оплатить?",
    which used to drop the file silently (unhandled message).
    """
    if await state.get_state() is not None:
        return
    u = message.from_user
    if not u:
        return
    try:
        open_tasks = await db.list_tasks_for_user(
            assigned_to=u.id,
            statuses=("in_progress",),
            type_filter=TaskType.INVOICE_PAYMENT,
            limit=5,
        )
    except Exception:
        log.exception("invoice_pp_orphan_catch: list_tasks_for_user failed for uid=%s", u.id)
        return
    if not open_tasks:
        return
    if len(open_tasks) > 1:
        # Несколько открытых задач-оплат — бот не может угадать, к какой привязать
        # файл. Раньше тихо терялся (return). Теперь подсказываем открыть нужную
        # задачу и нажать её кнопку, затем прислать платёжку. owner 27.06.
        await message.answer(
            "📎 Файл получен, но у вас несколько открытых задач на оплату — "
            "бот не может определить, к какой его привязать.\n"
            "Откройте нужную задачу, нажмите её кнопку (например «💸 Оплатить» / "
            "«✅ Исполнено»), затем пришлите платёжку.",
        )
        return
    task = open_tasks[0]
    task_id = int(task["id"])
    att = await mirror_attachment(message, storage, prefix=f"tasks/{u.id}")
    if att is None:
        return
    payload = try_json_loads(task.get("payload_json"))
    suffix = " (☁️ зеркало)" if att.get("minio_object_key") else ""
    # Кредит-трата хозяина кошелька (kind=credit_spend_gd_confirm): если ГД шлёт
    # платёжку ДО клика «✅ Подтвердить», файл должен попасть в КРЕДИТ-флоу
    # подтверждения (cw_gd_send/skip → запись расхода кошелька + вложение), а НЕ в
    # supplier-оплату (inv_pp_done → _invoice_pp_finalize_core создаёт
    # SUPPLIER_PAYMENT, расход кошелька не пишется — мис-роутинг). owner 27.06.
    if isinstance(payload, dict) and payload.get("kind") == "credit_spend_gd_confirm":
        await state.set_state(InvoicePaymentSG.attaching_pp)
        await state.update_data(cw_tid=task_id, pp_files=[att])
        b = InlineKeyboardBuilder()
        b.button(text="✅ Подтвердить", callback_data=f"cw_gd_send:{task_id}")
        b.button(text="✅ Без вложения", callback_data=f"cw_gd_skip:{task_id}")
        b.button(text="❌ Отмена", callback_data=f"cw_gd_acancel:{task_id}")
        b.adjust(1)
        await message.answer(
            f"📎 Документ принят.{suffix}\n"
            "Нажмите «✅ Подтвердить» — расход кошелька запишется с вложением.",
            reply_markup=b.as_markup(),
        )
        return
    # Кредит-оплата менеджером (kind=credit_payment_request, §C): задача тоже
    # type=INVOICE_PAYMENT, поэтому попадает сюда. Если менеджер шлёт платёжку ДО
    # клика «✅ Исполнено» (state ещё None), файл должен идти в КРЕДИТ-исполнение
    # (credit_exec_send → _finalize_credit_execution: запись расхода + close), а НЕ
    # в supplier-оплату (inv_pp_done создаёт SUPPLIER_PAYMENT — мис-роутинг). owner 27.06.
    if isinstance(payload, dict) and payload.get("kind") == "credit_payment_request":
        await state.set_state(CreditPaymentExecuteSG.waiting)
        await state.update_data(credit_task_id=task_id, credit_exec_file=att)
        b = InlineKeyboardBuilder()
        b.button(text="✅ Исполнено", callback_data=f"credit_exec_send:{task_id}")
        b.button(text="❌ Отмена", callback_data=f"credit_exec_acancel:{task_id}")
        b.adjust(1)
        await message.answer(
            f"📎 Платёжка принята.{suffix}\n"
            "Нажмите «✅ Исполнено» — расход спишется и задача закроется.",
            reply_markup=b.as_markup(),
        )
        return
    await state.set_state(InvoicePaymentSG.attaching_pp)
    await state.update_data(invoice_task_id=task_id, pp_files=[att])
    _, inv_num, _, _ = _invoice_task_details(payload)
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить", callback_data=f"inv_pp_done:{task_id}")
    b.button(text="❌ Отмена", callback_data=f"inv_pp_cancel:{task_id}")
    b.adjust(1)
    inv_label = f"счёта №{inv_num}" if inv_num else f"задачи #{task_id}"
    await message.answer(
        f"📎 Файл принят для оплаты {inv_label}.{suffix}\n"
        f"Нажмите «✅ Отправить» для подтверждения.",
        reply_markup=b.as_markup(),
    )
