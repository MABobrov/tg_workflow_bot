"""
Migration script: MANAGER -> MANAGER_KV + deactivate old MANAGER users.

Run after deploying the new code:
    python -m app.migrations.migrate_roles

Steps:
1. Find all users with role='manager'
2. Change their role to 'manager_kv'
3. Migrate their projects to use the new role
4. Log all changes
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from ..db import Database

log = logging.getLogger(__name__)


async def migrate():
    load_dotenv()
    db_path = os.getenv("DB_PATH", "./data/bot.sqlite3")

    db = Database(db_path)
    await db.connect()
    await db.init_schema()

    log.info("Starting role migration: MANAGER -> MANAGER_KV")

    # Find all users with role='manager'
    cur = await db.conn.execute(
        "SELECT telegram_id, username, full_name, role FROM users WHERE role = 'manager'"
    )
    rows = await cur.fetchall()
    users = [dict(r) for r in rows]

    if not users:
        log.info("No users with role='manager' found. Nothing to migrate.")
        await db.close()
        return

    log.info("Found %d users with role='manager':", len(users))
    for u in users:
        log.info("  - ID: %s, username: %s, name: %s", u["telegram_id"], u["username"], u["full_name"])

    # Migrate to manager_kv
    for u in users:
        await db.conn.execute(
            "UPDATE users SET role = 'manager_kv' WHERE telegram_id = ?",
            (u["telegram_id"],),
        )
        log.info("  Migrated user %s -> manager_kv", u["telegram_id"])

    await db.conn.commit()
    log.info("Migration complete. %d users migrated to manager_kv.", len(users))

    # --- Phase 2: TD -> GD migration ---
    log.info("Starting role migration: TD -> GD")

    cur = await db.conn.execute(
        "SELECT telegram_id, username, full_name, role FROM users WHERE role = 'td'"
    )
    td_rows = await cur.fetchall()
    td_users = [dict(r) for r in td_rows]

    if not td_users:
        log.info("No users with role='td' found. Nothing to migrate.")
    else:
        log.info("Found %d users with role='td':", len(td_users))
        for u in td_users:
            log.info("  - ID: %s, username: %s, name: %s", u["telegram_id"], u["username"], u["full_name"])

        for u in td_users:
            await db.conn.execute(
                "UPDATE users SET role = 'gd' WHERE telegram_id = ?",
                (u["telegram_id"],),
            )
            log.info("  Migrated user %s -> gd", u["telegram_id"])

        await db.conn.commit()
        log.info("TD->GD migration complete. %d users migrated.", len(td_users))

    # Also handle combined roles containing 'td' (e.g. 'td,gd' -> 'gd')
    cur = await db.conn.execute(
        "SELECT telegram_id, role FROM users WHERE role LIKE '%td%' AND role != 'td'"
    )
    combo_rows = await cur.fetchall()
    for row in combo_rows:
        old_role = row["role"]
        parts = [p.strip() for p in old_role.replace(";", ",").split(",")]
        new_parts = [p for p in parts if p != "td"]
        if "gd" not in new_parts:
            new_parts.append("gd")
        new_role = ",".join(new_parts)
        if new_role != old_role:
            await db.conn.execute(
                "UPDATE users SET role = ? WHERE telegram_id = ?",
                (new_role, row["telegram_id"]),
            )
            log.info("  Migrated combined role user %s: '%s' -> '%s'", row["telegram_id"], old_role, new_role)
    await db.conn.commit()

    await db.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
