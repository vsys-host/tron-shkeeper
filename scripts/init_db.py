"""
One-time / idempotent database bootstrap for tron-shkeeper.

It:

1. Waits for the configured MySQL/MariaDB server (config.DB_URI) to become
   reachable.
2. Creates the schema from app/schema.sql, including the tron_balances table.
3. If this is an upgrade from a previous SQLite-based installation, copies the
   legacy data (data/database.db, data/tron.db) into MySQL exactly once.

Safe to re-run: a `sqlite_data_migrated` marker is stored in the `settings` table
once migration completes, and every insert uses `INSERT IGNORE` keyed on the
original row IDs, so a crashed/retried run never duplicates rows. The original
SQLite files are never modified or deleted.

Fresh installs (no legacy `data/*.db` files present) just get an empty schema and
the marker is set immediately - there is nothing to migrate.
"""

import pathlib
import sqlite3
import sys
import time
from urllib.parse import urlsplit

import pymysql.cursors
from sqlalchemy import text

# Ensure the repo root (parent of this scripts/ directory) is importable
# regardless of the current working directory the script is invoked from.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from app.db import engine  # noqa: E402
from app.config import config  # noqa: E402

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA_SQL = REPO_ROOT / "app" / "schema.sql"

# Historical SQLite file locations (same relative paths the app used to read via
# config.DATABASE / config.DB_URI before the MySQL migration).
LEGACY_DATABASE_DB = REPO_ROOT / "data" / "database.db"
LEGACY_TRON_DB = REPO_ROOT / "data" / "tron.db"

MIGRATION_MARKER_NAME = "sqlite_data_migrated"

WAIT_FOR_MYSQL_ATTEMPTS = 30
WAIT_FOR_MYSQL_DELAY_SECONDS = 2


def log(msg: str) -> None:
    print(f"[init_db] {msg}", flush=True)


def wait_for_mysql() -> None:
    last_error = None
    for attempt in range(1, WAIT_FOR_MYSQL_ATTEMPTS + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log(f"MySQL is reachable (attempt {attempt}).")
            return
        except Exception as e:
            last_error = e
            log(
                f"MySQL not ready yet (attempt {attempt}/{WAIT_FOR_MYSQL_ATTEMPTS}): {e!r}"
            )
            time.sleep(WAIT_FOR_MYSQL_DELAY_SECONDS)
    raise SystemExit(f"MySQL never became reachable: {last_error!r}")


def create_schema() -> None:
    statements = [s.strip() for s in SCHEMA_SQL.read_text().split(";") if s.strip()]
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
        cur.close()
        conn.commit()
    finally:
        conn.close()

    log("Schema created/verified.")


def _is_migrated() -> bool:
    conn = engine.raw_connection()
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT value FROM settings WHERE name = %s", (MIGRATION_MARKER_NAME,)
        )
        row = cur.fetchone()
        cur.close()
        return bool(row and row["value"] == "1")
    finally:
        conn.close()


def _set_migrated() -> None:
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settings (name, value) VALUES (%s, '1') "
            "ON DUPLICATE KEY UPDATE value = '1'",
            (MIGRATION_MARKER_NAME,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _sqlite_table_exists(sqlite_conn: sqlite3.Connection, table_name: str) -> bool:
    cur = sqlite_conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    return cur.fetchone() is not None


def _copy_rows(mysql_conn, insert_sql: str, rows: list[tuple], table_name: str | None = None) -> int:
    if not rows:
        log(f"No rows to insert for {table_name or 'migration batch'}.")
        return 0
    cur = mysql_conn.cursor()
    try:
        log(
            f"Inserting {len(rows)} rows into {table_name or 'target table'} "
            f"with batch size {len(rows)}."
        )
        cur.executemany(insert_sql, rows)
        return len(rows)
    finally:
        cur.close()


def _count_mysql_rows(mysql_conn, table_name: str) -> int:
    cur = mysql_conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = int(cur.fetchone()[0])
        log(f"MySQL table {table_name} count: {count}")
        return count
    finally:
        cur.close()


def _validate_phase_counts(
    mysql_conn, expected_counts: dict[str, int], before_counts: dict[str, int], phase: str
) -> None:
    mismatches: list[str] = []
    log(f"Validating migration phase '{phase}' against expected counts: {expected_counts}")
    for table_name, expected_rows in expected_counts.items():
        before = before_counts.get(table_name, 0)
        after = _count_mysql_rows(mysql_conn, table_name)
        delta = after - before
        log(
            f"Phase '{phase}' table '{table_name}': before={before}, "
            f"after={after}, delta={delta}, expected_delta={expected_rows}"
        )
        if delta != expected_rows:
            mismatches.append(
                f"{table_name}: expected +{expected_rows}, got +{delta} "
                f"(before={before}, after={after})"
            )

    if mismatches:
        msg = f"Migration validation failed during {phase}: " + "; ".join(mismatches)
        log(msg)
        raise RuntimeError(msg)

    log(f"Migration phase '{phase}' validation passed.")


def migrate_legacy_sqlite() -> None:
    if _is_migrated():
        log("Legacy SQLite data already migrated, skipping.")
        return

    if not LEGACY_DATABASE_DB.exists() and not LEGACY_TRON_DB.exists():
        log("No legacy SQLite files found - fresh install, nothing to migrate.")
        _set_migrated()
        return

    counts: dict[str, int] = {}
    mysql_conn = engine.raw_connection()
    try:
        if LEGACY_DATABASE_DB.exists():
            log(f"Reading legacy {LEGACY_DATABASE_DB} ...")
            before_counts = {
                "keys": _count_mysql_rows(mysql_conn, "keys"),
                "settings": _count_mysql_rows(mysql_conn, "settings"),
            }
            log(f"Before database.db migration baseline: {before_counts}")
            mysql_conn.autocommit(False)
            try:
                src = sqlite3.connect(str(LEGACY_DATABASE_DB))
                src.row_factory = sqlite3.Row
                try:
                    log(f"Loading keys from database.db")
                    keys_rows = [
                        (r["id"], r["public"], r["private"], r["symbol"], r["type"])
                        for r in src.execute(
                            "SELECT id, public, private, symbol, type FROM keys"
                        )
                    ]
                    log(f"database.db keys source row count: {len(keys_rows)}")
                    counts["keys"] = _copy_rows(
                        mysql_conn,
                        "INSERT IGNORE INTO `keys` (id, public, private, symbol, type) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        keys_rows,
                        "keys",
                    )

                    log(f"Loading settings from database.db")
                    settings_rows = [
                        (r["name"], r["value"])
                        for r in src.execute("SELECT name, value FROM settings")
                    ]
                    log(f"database.db settings source row count: {len(settings_rows)}")
                    counts["settings"] = _copy_rows(
                        mysql_conn,
                        "INSERT IGNORE INTO settings (name, value) VALUES (%s, %s)",
                        settings_rows,
                        "settings",
                    )
                finally:
                    src.close()

                _validate_phase_counts(
                    mysql_conn,
                    {"keys": len(keys_rows), "settings": len(settings_rows)},
                    before_counts,
                    "database.db",
                )
                mysql_conn.commit()
                log(f"Committed database.db migration results: {counts}")
            except Exception as exc:
                log(f"Rolling back database.db migration due to error: {exc!r}")
                mysql_conn.rollback()
                raise
            finally:
                mysql_conn.autocommit(True)

        if LEGACY_TRON_DB.exists():
            log(f"Reading legacy {LEGACY_TRON_DB} ...")
            before_counts = {
                "tron_balances": _count_mysql_rows(mysql_conn, "tron_balances"),
            }
            log(f"Before tron.db migration baseline: {before_counts}")
            mysql_conn.autocommit(False)
            try:
                src = sqlite3.connect(str(LEGACY_TRON_DB))
                src.row_factory = sqlite3.Row
                try:
                    if _sqlite_table_exists(src, "tron_balances"):
                        log("Loading tron_balances from tron.db")
                        balance_rows = [
                            (
                                r["id"],
                                r["account"],
                                r["symbol"],
                                str(r["balance"]),
                                r["created_at"],
                                r["updated_at"],
                            )
                            for r in src.execute(
                                "SELECT id, account, symbol, balance, created_at, "
                                "updated_at FROM tron_balances"
                            )
                        ]
                        log(f"tron.db tron_balances source row count: {len(balance_rows)}")
                        counts["tron_balances"] = _copy_rows(
                            mysql_conn,
                            "INSERT IGNORE INTO tron_balances "
                            "(id, account, symbol, balance, created_at, updated_at) "
                            "VALUES (%s, %s, %s, %s, %s, %s)",
                            balance_rows,
                            "tron_balances",
                        )
                    else:
                        balance_rows = []
                        log("tron.db tron_balances table not found; skipping migration for it.")

                finally:
                    src.close()

                _validate_phase_counts(
                    mysql_conn,
                    {
                        "tron_balances": len(balance_rows),
                    },
                    before_counts,
                    "tron.db",
                )
                mysql_conn.commit()
                log(f"Committed tron.db migration results: {counts}")
            except Exception as exc:
                log(f"Rolling back tron.db migration due to error: {exc!r}")
                mysql_conn.rollback()
                raise
            finally:
                mysql_conn.autocommit(True)

        mysql_conn.commit()
    finally:
        mysql_conn.close()

    _set_migrated()
    log(f"Legacy SQLite data migrated: {counts}")


if __name__ == "__main__":
    log(f"Target DB_URI: {config.DB_URI}")
    log(f"Legacy SQLite sources: {LEGACY_DATABASE_DB}, {LEGACY_TRON_DB}")
    wait_for_mysql()
    log("MySQL connection established; creating schema...")
    create_schema()
    log("Starting legacy SQLite migration...")
    migrate_legacy_sqlite()
    log("Done.")
