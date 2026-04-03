#!/usr/bin/env python3
"""One-time data migration from SQLite to PostgreSQL."""

import argparse
import os
import sqlite3
import sys


TABLES = [
    "services",
    "service_button_overrides",
    "service_emoji_overrides",
    "numbers",
    "banned_users",
    "users",
    "otp_log",
    "channels",
    "bot_config",
    "user_cooldowns",
]

ID_TABLES = ["services", "numbers", "otp_log", "channels"]


def get_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def copy_table(src_conn, dst_conn, table_name):
    columns = get_columns(src_conn, table_name)
    if not columns:
        return 0

    col_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    rows = src_conn.execute(f"SELECT {col_sql} FROM {table_name}").fetchall()
    if not rows:
        return 0

    sql = f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    with dst_conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def reset_sequences(dst_conn):
    with dst_conn.cursor() as cur:
        for table_name in ID_TABLES:
            cur.execute(
                "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM "
                + table_name
                + "), 1), true)",
                (table_name,),
            )


def main():
    parser = argparse.ArgumentParser(description="Migrate Number Bot data from SQLite to PostgreSQL.")
    parser.add_argument("--sqlite-path", default="number_panel.db", help="Path to the source SQLite database.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Destination PostgreSQL DATABASE_URL.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.sqlite_path):
        print(f"SQLite database not found: {args.sqlite_path}", file=sys.stderr)
        return 1

    if not args.database_url:
        print("DATABASE_URL is required for PostgreSQL migration.", file=sys.stderr)
        return 1

    os.environ["DB_ENGINE"] = "postgresql"
    os.environ["DATABASE_URL"] = args.database_url

    import psycopg
    import core

    core.init_db()

    src_conn = sqlite3.connect(args.sqlite_path)
    dst_conn = psycopg.connect(args.database_url)

    try:
        migrated = {}
        for table_name in TABLES:
            migrated[table_name] = copy_table(src_conn, dst_conn, table_name)

        reset_sequences(dst_conn)
        dst_conn.commit()

        print("Migration complete.")
        for table_name in TABLES:
            print(f"{table_name}: {migrated[table_name]} row(s)")
        return 0
    except Exception as exc:
        dst_conn.rollback()
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    finally:
        src_conn.close()
        dst_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
