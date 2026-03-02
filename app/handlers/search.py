from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from ..config import Config
from ..db import Database
from ..enums import Role
from ..keyboards import projects_kb
from ..states import SearchProjectSG
from .auth import require_role_message

log = logging.getLogger(__name__)
router = Router()
router.message.filter(F.chat.type == "private")


@router.message(Command("search"))
async def cmd_search(message: Message, db: Database) -> None:
    if not await require_role_message(message, db, roles=[Role.MANAGER, Role.RP, Role.TD, Role.ACCOUNTING, Role.INSTALLER, Role.GD, Role.DRIVER, Role.TINTER]):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /search <текст>. Например: /search Ленина 45")
        return
    q = parts[1].strip()
    projects = await db.search_projects(q, limit=20)
    if not projects:
        await message.answer("Ничего не нашёл.")
        return
    await message.answer("Нашёл проекты. Выберите:", reply_markup=projects_kb(projects, ctx="view"))


@router.message(F.text == "📌 Поиск проекта")
async def start_search(message: Message, state: FSMContext, db: Database) -> None:
    # доступно ролям, которые вообще работают с проектами
    if not await require_role_message(message, db, roles=[Role.MANAGER, Role.RP, Role.TD, Role.ACCOUNTING, Role.INSTALLER, Role.GD, Role.DRIVER, Role.TINTER]):
        return
    await state.clear()
    await state.set_state(SearchProjectSG.query)
    await message.answer(
        "🔎 Введите часть кода, адреса или названия проекта.\n"
        "Минимум 2 символа. Для отмены: <code>/cancel</code>."
    )


@router.message(SearchProjectSG.query)
async def run_search(message: Message, state: FSMContext, db: Database) -> None:
    q = (message.text or "").strip()
    if len(q) < 2:
        await message.answer("Слишком коротко. Введите минимум 2 символа:")
        return
    projects = await db.search_projects(q, limit=20)
    if not projects:
        await message.answer("Ничего не нашёл. Попробуйте другой запрос или /cancel.")
        return
    await message.answer("Нашёл проекты. Выберите:", reply_markup=projects_kb(projects, ctx="view"))
    await state.clear()
