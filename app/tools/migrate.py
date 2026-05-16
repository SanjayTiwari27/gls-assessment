"""Apply SQL migrations from app/storage/migrations.

Files are applied in lexicographic order. Each file is a self-contained
transaction; we record applied versions in `schema_migrations`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

from app.config import get_settings
from app.logging import configure_logging, get_logger

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "storage" / "migrations"


async def _ensure_schema_table(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


async def _applied(conn: asyncpg.Connection) -> set[str]:
    rows = await conn.fetch("SELECT name FROM schema_migrations")
    return {r["name"] for r in rows}


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("migrate")

    conn = await asyncpg.connect(dsn=settings.asyncpg_dsn)
    try:
        await _ensure_schema_table(conn)
        already = await _applied(conn)

        files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not files:
            log.warning("no_migrations_found", dir=str(MIGRATIONS_DIR))
            return

        for f in files:
            if f.name in already:
                log.info("migration_skipped", name=f.name)
                continue
            sql = f.read_text(encoding="utf-8")
            log.info("migration_applying", name=f.name)
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING",
                    f.name,
                )
            log.info("migration_applied", name=f.name)
    finally:
        await conn.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
