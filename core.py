"""Core configuration and database setup."""

# NOTE: Your Python 3.14 install has a broken stdlib module at
# `...Lib/importlib/resources/_common.py` (it contains unrelated code and imports `requests`),
# which causes a circular import when `requests -> certifi -> importlib.resources` happens.
# This stub avoids importing that broken stdlib module and provides the small subset of the
# API needed by `certifi` (used by `requests`, used by `telebot` and our webhook sender).
import contextlib
import datetime
import importlib.util
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import threading
import time
import types

from dotenv import load_dotenv
from flag import extract_display_flag, strip_display_flag, resolve_country, normalize_country_code
from custom_emoji import render_custom_emoji_text, normalize_custom_emoji_text, cleanup_broken_custom_emoji_text

try:
    import psycopg
except Exception:
    psycopg = None

try:
    from psycopg_pool import ConnectionPool
except Exception:
    ConnectionPool = None


def _install_importlib_resources_stub():
    sys.modules.pop("importlib.resources", None)

    m = types.ModuleType("importlib.resources")

    def files(package):
        name = package if isinstance(package, str) else getattr(package, "__name__", None)
        if not name:
            raise TypeError("files() expects a package name or module")
        spec = importlib.util.find_spec(name)
        if not spec or not spec.submodule_search_locations:
            raise ModuleNotFoundError(name)
        return Path(list(spec.submodule_search_locations)[0])

    @contextlib.contextmanager
    def as_file(traversable):
        yield Path(traversable)

    @contextlib.contextmanager
    def path(package, resource):
        yield files(package).joinpath(resource)

    def read_text(package, resource, encoding="utf-8"):
        return files(package).joinpath(resource).read_text(encoding=encoding)

    m.files = files
    m.as_file = as_file
    m.path = path
    m.read_text = read_text

    sys.modules["importlib.resources"] = m


try:
    import importlib.resources as _ilr  # noqa: F401
except Exception:
    _install_importlib_resources_stub()


env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

BOT_TOKEN = os.getenv("BOT_TOKEN") or ""
FORWARDER_BOT_TOKEN = os.getenv("FORWARDER_BOT_TOKEN", BOT_TOKEN) or BOT_TOKEN
_admin_val = os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID"))
ADMIN_IDS = []
if _admin_val:
    try:
        ADMIN_IDS = [int(x.strip()) for x in _admin_val.split(",") if x.strip()]
    except ValueError:
        ADMIN_IDS = []

BOT_NAME = os.getenv("BOT_NAME") or "NUMBER BOT"
OTP_GROUP_URL = os.getenv("OTP_GROUP_URL") or ""
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "127.0.0.1")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_WORKERS = int(os.getenv("WEBHOOK_WORKERS", "2"))
SEND_WORKERS = int(os.getenv("SEND_WORKERS", "3"))
SEND_DELAY = float(os.getenv("SEND_DELAY", "0.2"))
GROUP_SEND_WORKERS = int(os.getenv("GROUP_SEND_WORKERS", "4"))
GROUP_SEND_DELAY = float(os.getenv("GROUP_SEND_DELAY", "0.02"))
TELEGRAM_DEBUG_JSON = os.getenv("TELEGRAM_DEBUG_JSON", "0").strip() == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_NAME = os.getenv("DB_NAME", "number_panel.db")
DB_ENGINE = (os.getenv("DB_ENGINE") or ("postgresql" if DATABASE_URL else "sqlite")).strip().lower()
if DB_ENGINE in {"postgres", "postgresql", "psql"}:
    DB_ENGINE = "postgresql"
else:
    DB_ENGINE = "sqlite"
POSTGRES_USE_POOL = os.getenv("POSTGRES_USE_POOL", "1").strip() == "1"
POSTGRES_POOL_MIN_SIZE = max(1, int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1")))
POSTGRES_POOL_MAX_SIZE = max(POSTGRES_POOL_MIN_SIZE, int(os.getenv("POSTGRES_POOL_MAX_SIZE", "8")))
POSTGRES_CONNECT_TIMEOUT = max(1, int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")))

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "backups"))

LOG_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"bot_{datetime.datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

logging.getLogger("telebot").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def normalize_service_name_key(service_name):
    return " ".join(str(service_name or "").strip().casefold().split())


def _service_emoji_markup(emoji_text="", custom_emoji_id="", html=False):
    emoji_text = normalize_custom_emoji_text(emoji_text)
    emoji_text = str(emoji_text or "").strip()
    custom_emoji_id = str(custom_emoji_id or "").strip()
    if html and custom_emoji_id:
        return f'<tg-emoji emoji-id="{custom_emoji_id}"></tg-emoji>'
    rendered = render_custom_emoji_text(emoji_text, html=html)
    return rendered or emoji_text


def _compose_service_display_name(service_name, emoji_text="", custom_emoji_id="", html=False):
    normalized_name = normalize_custom_emoji_text(service_name)
    normalized_name = str(normalized_name or "").strip()
    emoji_text = normalize_custom_emoji_text(emoji_text)
    emoji_text = str(emoji_text or "").strip()
    custom_emoji_id = str(custom_emoji_id or "").strip()

    if emoji_text and normalized_name.startswith(emoji_text):
        normalized_name = normalized_name[len(emoji_text):].strip() or normalized_name

    rendered_name = render_custom_emoji_text(normalized_name, html=html)
    rendered_name = rendered_name or str(service_name or "").strip() or "Unknown"
    rendered_emoji = _service_emoji_markup(emoji_text, custom_emoji_id, html=html)
    if rendered_emoji:
        return f"{rendered_emoji} {rendered_name}".strip()
    return rendered_name


def get_service_emoji_override_data(service_name, conn=None):
    service_name = str(service_name or "").strip()
    if not service_name:
        return "", ""
    target_key = normalize_service_name_key(service_name)
    if not target_key:
        return "", ""
    try:
        def _lookup(active_conn):
            rows = active_conn.execute(
                "SELECT service_name, service_emoji, COALESCE(custom_emoji_id, '') FROM service_emoji_overrides"
            ).fetchall()
            for stored_name, stored_emoji, custom_emoji_id in rows:
                if normalize_service_name_key(stored_name) == target_key:
                    return str(stored_emoji or "").strip(), str(custom_emoji_id or "").strip()
            return "", ""

        if conn is not None:
            return _lookup(conn)
        with get_db_connection() as temp_conn:
            return _lookup(temp_conn)
    except Exception:
        return "", ""


def get_service_emoji_override(service_name, conn=None):
    emoji_text, _custom_emoji_id = get_service_emoji_override_data(service_name, conn=conn)
    return emoji_text


def get_service_button_icon_data(service_name, conn=None):
    service_name = str(service_name or "").strip()
    if not service_name:
        return "", ""
    target_key = normalize_service_name_key(service_name)
    if not target_key:
        return "", ""
    try:
        def _lookup(active_conn):
            rows = active_conn.execute(
                "SELECT service_name, button_emoji, COALESCE(custom_emoji_id, '') FROM service_button_overrides"
            ).fetchall()
            for stored_name, button_emoji, custom_emoji_id in rows:
                if normalize_service_name_key(stored_name) == target_key:
                    return str(button_emoji or "").strip(), str(custom_emoji_id or "").strip()

            rows = active_conn.execute(
                "SELECT name, MAX(COALESCE(button_emoji, '')) FROM services GROUP BY name"
            ).fetchall()
            for stored_name, button_emoji in rows:
                if normalize_service_name_key(stored_name) == target_key:
                    return str(button_emoji or "").strip(), ""
            return "", ""

        if conn is not None:
            return _lookup(conn)
        with get_db_connection() as temp_conn:
            return _lookup(temp_conn)
    except Exception:
        return "", ""


def get_service_button_emoji(service_name, conn=None):
    emoji_text, _custom_emoji_id = get_service_button_icon_data(service_name, conn=conn)
    return emoji_text


def save_service_button_emoji(service_name, button_emoji, custom_emoji_id="", conn=None):
    service_name = str(service_name or "").strip()
    button_emoji = str(button_emoji or "").strip()
    custom_emoji_id = str(custom_emoji_id or "").strip()
    if not service_name:
        return

    def _save(active_conn):
        active_conn.execute(
            "INSERT INTO service_button_overrides (service_name, button_emoji, custom_emoji_id, updated_at) "
            "VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(service_name) DO UPDATE SET button_emoji=excluded.button_emoji, custom_emoji_id=excluded.custom_emoji_id, updated_at=CURRENT_TIMESTAMP",
            (service_name, button_emoji, custom_emoji_id),
        )
        active_conn.execute(
            "UPDATE services SET button_emoji = ? WHERE name = ?",
            (button_emoji, service_name),
        )

    if conn is not None:
        _save(conn)
        return
    with get_db_connection() as temp_conn:
        _save(temp_conn)
        temp_conn.commit()


def format_service_display(
    service_name,
    service_emoji=None,
    service_custom_emoji_id=None,
    webhook_override=False,
    conn=None,
    html=False,
):
    emoji_text = str(service_emoji or "").strip()
    custom_emoji_id = str(service_custom_emoji_id or "").strip()
    if webhook_override:
        override_text, override_custom_id = get_service_emoji_override_data(service_name, conn=conn)
        if override_text or override_custom_id:
            return _compose_service_display_name(
                service_name,
                emoji_text=override_text,
                custom_emoji_id=override_custom_id,
                html=html,
            )
    return _compose_service_display_name(
        service_name,
        emoji_text=emoji_text,
        custom_emoji_id=custom_emoji_id,
        html=html,
    )


def format_service_visible(service_name, service_emoji=None, webhook_override=False, conn=None):
    emoji_text = str(service_emoji or "").strip()
    if webhook_override:
        override_text, _override_custom_id = get_service_emoji_override_data(service_name, conn=conn)
        if override_text:
            emoji_text = override_text
    return _compose_service_display_name(
        service_name,
        emoji_text=emoji_text,
        custom_emoji_id="",
        html=False,
    )


def format_service_icon_only(
    service_name,
    service_emoji=None,
    service_custom_emoji_id=None,
    webhook_override=False,
    conn=None,
    html=False,
):
    emoji_text = str(service_emoji or "").strip()
    custom_emoji_id = str(service_custom_emoji_id or "").strip()
    if webhook_override:
        override_text, override_custom_id = get_service_emoji_override_data(service_name, conn=conn)
        if override_text or override_custom_id:
            emoji_text = override_text
            custom_emoji_id = override_custom_id
    rendered = _service_emoji_markup(emoji_text, custom_emoji_id, html=html)
    if rendered:
        return rendered
    normalized_name = normalize_custom_emoji_text(service_name)
    normalized_name = str(normalized_name or "").strip()
    if normalized_name:
        first = normalized_name.split(maxsplit=1)[0].strip()
        if first and not any(ch.isalnum() for ch in first):
            return first
    return ""


def cleanup_old_logs(days_to_keep=7):
    """Delete log files older than specified days."""
    try:
        current_time = datetime.datetime.now()
        cutoff_time = current_time - datetime.timedelta(days=days_to_keep)

        deleted_count = 0
        for log_file in LOG_DIR.glob("*.log"):
            file_time = datetime.datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_time:
                log_file.unlink()
                deleted_count += 1
                logger.info(f"Deleted old log: {log_file.name}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old log files")
    except Exception as e:
        logger.error(f"Failed to cleanup logs: {e}")


def _rewrite_qmark_placeholders(sql: str) -> str:
    return sql.replace("?", "%s")


def _rewrite_insert_or_ignore(sql: str) -> str:
    match = re.match(
        r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+(.+?)\s+VALUES\s+(.+)$",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return sql
    return f"INSERT INTO {match.group(1)} VALUES {match.group(2)} ON CONFLICT DO NOTHING"


def _rewrite_sql_for_postgres(sql: str) -> str:
    sql = _rewrite_insert_or_ignore(sql)
    sql = _rewrite_qmark_placeholders(sql)
    return sql


def _derive_country_metadata(country_value):
    raw = str(country_value or "").strip()
    flag_text = extract_display_flag(raw)
    display_name = strip_display_flag(raw) or raw or "Unknown"
    resolved = resolve_country(display_name)
    country_code = normalize_country_code((resolved or {}).get("iso2") or "")
    return flag_text, country_code, display_name


def _backfill_service_country_metadata(conn):
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, country, COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), "
        "COALESCE(country_code, ''), COALESCE(country_display_name, '') FROM services"
    ).fetchall()
    for service_id, country_value, stored_flag, stored_custom_id, stored_code, stored_display in rows:
        flag_text = str(stored_flag or "").strip()
        custom_emoji_id = str(stored_custom_id or "").strip()
        country_code = normalize_country_code(stored_code)
        display_name = str(stored_display or "").strip()
        derived_flag, derived_code, derived_display = _derive_country_metadata(country_value)
        if not flag_text or flag_text in {"•", "◦", "▪", "▫"}:
            flag_text = derived_flag
        if not country_code:
            country_code = derived_code
        if not display_name:
            display_name = derived_display
        plain_country = display_name or strip_display_flag(country_value) or str(country_value or "").strip() or "Unknown"
        if custom_emoji_id and flag_text.startswith("[[ce:"):
            flag_text = ""
        cur.execute(
            "UPDATE services SET country = ?, country_flag = ?, country_code = ?, country_display_name = ? WHERE id = ?",
            (plain_country, flag_text, country_code, display_name, service_id),
        )


def _dedupe_button_emoji_in_service_names(conn):
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, name, COALESCE(button_emoji, '') FROM services"
    ).fetchall()
    for service_id, name, button_emoji in rows:
        name = str(name or "").strip()
        button_emoji = str(button_emoji or "").strip()
        if not name or not button_emoji:
            continue
        if name.startswith(button_emoji):
            stripped = name[len(button_emoji):].strip()
            if stripped:
                try:
                    cur.execute("UPDATE services SET name = ? WHERE id = ?", (stripped, service_id))
                except Exception as e:
                    logger.warning(f"Skipped button emoji name cleanup for service {service_id}: {e}")


def _cleanup_broken_custom_emoji_tokens(conn):
    cur = conn.cursor()
    service_rows = cur.execute(
        "SELECT id, name, country, country_flag, COALESCE(country_custom_emoji_id, ''), country_display_name, COALESCE(button_emoji, ''), service_emoji FROM services"
    ).fetchall()
    for service_id, name, country, country_flag, country_custom_emoji_id, country_display_name, button_emoji, service_emoji in service_rows:
        new_name = cleanup_broken_custom_emoji_text(name)
        new_country = cleanup_broken_custom_emoji_text(country)
        new_country_flag = cleanup_broken_custom_emoji_text(country_flag)
        new_country_custom_emoji_id = str(country_custom_emoji_id or "").strip()
        new_country_display_name = cleanup_broken_custom_emoji_text(country_display_name)
        new_button_emoji = cleanup_broken_custom_emoji_text(button_emoji)
        new_service_emoji = cleanup_broken_custom_emoji_text(service_emoji)
        if (
            new_name != str(name or "")
            or new_country != str(country or "")
            or new_country_flag != str(country_flag or "")
            or new_country_custom_emoji_id != str(country_custom_emoji_id or "")
            or new_country_display_name != str(country_display_name or "")
            or new_button_emoji != str(button_emoji or "")
            or new_service_emoji != str(service_emoji or "")
        ):
            try:
                cur.execute(
                    "UPDATE services SET name = ?, country = ?, country_flag = ?, country_custom_emoji_id = ?, country_display_name = ?, button_emoji = ?, service_emoji = ? WHERE id = ?",
                    (new_name, new_country, new_country_flag, new_country_custom_emoji_id, new_country_display_name, new_button_emoji, new_service_emoji, service_id),
                )
            except Exception as e:
                logger.warning(f"Skipped custom emoji cleanup for service {service_id}: {e}")

    override_rows = cur.execute(
        "SELECT service_name, service_emoji FROM service_emoji_overrides"
    ).fetchall()
    for service_name, service_emoji in override_rows:
        new_service_name = cleanup_broken_custom_emoji_text(service_name)
        new_service_emoji = cleanup_broken_custom_emoji_text(service_emoji)
        if new_service_name != str(service_name or "") or new_service_emoji != str(service_emoji or ""):
            try:
                cur.execute(
                    "UPDATE service_emoji_overrides SET service_name = ?, service_emoji = ? WHERE service_name = ?",
                    (new_service_name, new_service_emoji, service_name),
                )
            except Exception as e:
                logger.warning(f"Skipped custom emoji cleanup for override {service_name}: {e}")


def _backfill_service_button_overrides(conn):
    cur = conn.cursor()
    try:
        rows = cur.execute(
            "SELECT name, MAX(COALESCE(button_emoji, '')), '' FROM services GROUP BY name"
        ).fetchall()
    except Exception:
        return
    for service_name, button_emoji, custom_emoji_id in rows:
        service_name = str(service_name or "").strip()
        button_emoji = str(button_emoji or "").strip()
        custom_emoji_id = str(custom_emoji_id or "").strip()
        if not service_name:
            continue
        try:
            cur.execute(
                "INSERT OR IGNORE INTO service_button_overrides (service_name, button_emoji, custom_emoji_id) VALUES (?, ?, ?)",
                (service_name, button_emoji, custom_emoji_id),
            )
        except Exception as e:
            logger.warning(f"Skipped button emoji backfill for {service_name}: {e}")


def _service_cleanup_sort_key(row):
    service_id, name, country, status, button_emoji, service_emoji, service_custom_emoji_id = row
    name = str(name or "")
    country = str(country or "")
    button_emoji = str(button_emoji or "")
    service_emoji = str(service_emoji or "")
    service_custom_emoji_id = str(service_custom_emoji_id or "")
    return (
        0 if status == "active" else 1,
        0 if service_custom_emoji_id else 1,
        0 if "[[ce:" in name and "[[ce:[[ce:" not in name else 1,
        0 if "[[ce:[[ce:" not in name else 1,
        0 if button_emoji else 1,
        0 if service_emoji else 1,
        len(name),
        int(service_id),
    )


def _merge_duplicate_services(conn):
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, name, country, status, COALESCE(button_emoji, ''), COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, '') "
        "FROM services ORDER BY id"
    ).fetchall()
    groups = {}
    for row in rows:
        service_id, name, country, status, button_emoji, service_emoji, service_custom_emoji_id = row
        cleaned_name = cleanup_broken_custom_emoji_text(name)
        cleaned_country = cleanup_broken_custom_emoji_text(country)
        key = (normalize_service_name_key(cleaned_name), cleaned_country.strip().casefold())
        groups.setdefault(key, []).append(
            (service_id, cleaned_name, cleaned_country, status, button_emoji, service_emoji, service_custom_emoji_id)
        )

    for _key, items in groups.items():
        if len(items) <= 1:
            continue
        items = sorted(items, key=_service_cleanup_sort_key)
        keeper = items[0]
        keeper_id, keeper_name, keeper_country, keeper_status, keeper_button_emoji, keeper_emoji, keeper_custom_id = keeper
        merged_name = keeper_name
        merged_status = keeper_status
        merged_button_emoji = keeper_button_emoji
        merged_emoji = keeper_emoji
        merged_custom_id = keeper_custom_id
        for duplicate in items[1:]:
            dup_id, dup_name, _dup_country, dup_status, dup_button_emoji, dup_emoji, dup_custom_id = duplicate
            try:
                cur.execute("UPDATE numbers SET service_id = ? WHERE service_id = ?", (keeper_id, dup_id))
                merged_status = merged_status if merged_status == "active" else dup_status
                merged_button_emoji = merged_button_emoji or dup_button_emoji
                merged_emoji = merged_emoji or dup_emoji
                merged_custom_id = merged_custom_id or dup_custom_id
                if len(str(dup_name or "")) < len(str(merged_name or "")):
                    merged_name = dup_name
                cur.execute("DELETE FROM services WHERE id = ?", (dup_id,))
                logger.info("Merged duplicate service %s into %s", dup_id, keeper_id)
            except Exception as e:
                logger.warning(f"Failed to merge duplicate service {dup_id} into {keeper_id}: {e}")
        try:
            cur.execute(
                "UPDATE services SET name = ?, country = ?, status = ?, button_emoji = ?, service_emoji = ?, service_custom_emoji_id = ? WHERE id = ?",
                (merged_name, keeper_country, merged_status, merged_button_emoji, merged_emoji, merged_custom_id, keeper_id),
            )
        except Exception as e:
            logger.warning(f"Failed to normalize merged keeper service {keeper_id}: {e}")


class PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, params=None):
        sql = _rewrite_sql_for_postgres(sql)
        if params is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, params)
        return self

    def executemany(self, sql, param_list):
        sql = _rewrite_sql_for_postgres(sql)
        self._cursor.executemany(sql, param_list)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        self._cursor.close()

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        return None

    def __iter__(self):
        return iter(self._cursor)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class PostgresConnection:
    def __init__(self, conn):
        object.__setattr__(self, "_conn", conn)

    def cursor(self):
        return PostgresCursor(self._conn.cursor())

    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def executemany(self, sql, param_list):
        cur = self.cursor()
        return cur.executemany(sql, param_list)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __setattr__(self, name, value):
        if name == "_conn":
            object.__setattr__(self, name, value)
            return
        setattr(self._conn, name, value)

    def __getattr__(self, name):
        return getattr(self._conn, name)


_pg_pool = None
_pg_pool_lock = threading.Lock()


def _close_postgres_pool():
    global _pg_pool
    with _pg_pool_lock:
        if _pg_pool is not None:
            try:
                _pg_pool.close()
            except Exception:
                pass
            _pg_pool = None


def _get_postgres_pool():
    global _pg_pool
    if not POSTGRES_USE_POOL:
        return None
    if ConnectionPool is None:
        logger.warning("psycopg_pool is not installed; falling back to direct PostgreSQL connections")
        return None
    with _pg_pool_lock:
        if _pg_pool is None:
            conninfo = DATABASE_URL
            if "connect_timeout=" not in conninfo:
                sep = "&" if "?" in conninfo else "?"
                conninfo = f"{conninfo}{sep}connect_timeout={POSTGRES_CONNECT_TIMEOUT}"
            _pg_pool = ConnectionPool(
                conninfo=conninfo,
                min_size=POSTGRES_POOL_MIN_SIZE,
                max_size=POSTGRES_POOL_MAX_SIZE,
                open=True,
                kwargs={"autocommit": False},
            )
            logger.info(
                "PostgreSQL pool ready min=%s max=%s",
                POSTGRES_POOL_MIN_SIZE,
                POSTGRES_POOL_MAX_SIZE,
            )
        return _pg_pool


@contextlib.contextmanager
def get_db_connection():
    if DB_ENGINE == "postgresql":
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is required when DB_ENGINE=postgresql")
        if psycopg is None:
            raise RuntimeError("psycopg is not installed. Run: python3 -m pip install psycopg[binary]")
        pool = _get_postgres_pool()
        if pool is not None:
            with pool.connection() as raw_conn:
                conn = PostgresConnection(raw_conn)
                yield conn
            return
        raw_conn = psycopg.connect(DATABASE_URL, connect_timeout=POSTGRES_CONNECT_TIMEOUT)
        conn = PostgresConnection(raw_conn)
    else:
        conn = sqlite3.connect(DB_NAME, timeout=10)
        conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
    finally:
        conn.close()


def _init_sqlite_db(conn):
    c = conn.cursor()
    old_schema = c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='services'").fetchone()
    needs_migration = old_schema and "UNIQUE" in old_schema[0] and "UNIQUE(country, name)" not in old_schema[0]
    needs_status_migration = old_schema and "status" not in old_schema[0]
    needs_service_emoji_migration = old_schema and "service_emoji" not in old_schema[0]

    if needs_migration:
        logger.info("Migrating services table to new schema...")
        try:
            c.execute("DROP TABLE IF EXISTS services_old")
            c.execute("ALTER TABLE services RENAME TO services_old")
            c.execute(
                """CREATE TABLE services (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        country TEXT DEFAULT 'Other',
                        country_flag TEXT DEFAULT '',
                        country_custom_emoji_id TEXT DEFAULT '',
                        country_code TEXT DEFAULT '',
                        country_display_name TEXT DEFAULT '',
                        status TEXT DEFAULT 'active',
                        button_emoji TEXT DEFAULT '',
                        service_emoji TEXT DEFAULT '',
                        service_custom_emoji_id TEXT DEFAULT '',
                        UNIQUE(country, name)
                   )"""
            )
            c.execute(
                "INSERT INTO services (id, name, country) "
                "SELECT id, name, country FROM services_old"
            )
            c.execute("DROP TABLE services_old")
            conn.commit()
            logger.info("Services table migrated successfully")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            conn.rollback()
            raise
    elif needs_status_migration:
        logger.info("Adding status column to services table...")
        try:
            c.execute("ALTER TABLE services ADD COLUMN status TEXT DEFAULT 'active'")
            conn.commit()
            logger.info("Status column added successfully")
        except Exception as e:
            logger.error(f"Failed to add status column: {e}")
            conn.rollback()
    elif needs_service_emoji_migration:
        logger.info("Adding service_emoji column to services table...")
        try:
            c.execute("ALTER TABLE services ADD COLUMN service_emoji TEXT DEFAULT ''")
            conn.commit()
            logger.info("service_emoji column added successfully")
        except Exception as e:
            logger.error(f"Failed to add service_emoji column: {e}")
            conn.rollback()
    else:
        c.execute(
            """CREATE TABLE IF NOT EXISTS services (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    country TEXT DEFAULT 'Other',
                    country_flag TEXT DEFAULT '',
                    country_custom_emoji_id TEXT DEFAULT '',
                    country_code TEXT DEFAULT '',
                    country_display_name TEXT DEFAULT '',
                    status TEXT DEFAULT 'active',
                    button_emoji TEXT DEFAULT '',
                    service_emoji TEXT DEFAULT '',
                    service_custom_emoji_id TEXT DEFAULT '',
                    UNIQUE(country, name)
               )"""
        )
    existing_service_cols = {row[1] for row in c.execute("PRAGMA table_info(services)").fetchall()}
    if "service_emoji" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN service_emoji TEXT DEFAULT ''")
    if "service_custom_emoji_id" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN service_custom_emoji_id TEXT DEFAULT ''")
    if "button_emoji" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN button_emoji TEXT DEFAULT ''")
    if "country_custom_emoji_id" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN country_custom_emoji_id TEXT DEFAULT ''")
    if "country_flag" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN country_flag TEXT DEFAULT ''")
    if "country_code" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN country_code TEXT DEFAULT ''")
    if "country_display_name" not in existing_service_cols:
        c.execute("ALTER TABLE services ADD COLUMN country_display_name TEXT DEFAULT ''")

    c.execute(
        """CREATE TABLE IF NOT EXISTS numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_id INTEGER,
                number TEXT,
                status TEXT DEFAULT 'active',
                user_id INTEGER,
                last_used TIMESTAMP,
                received_otp INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE
           )"""
    )
    existing_schema = c.execute("PRAGMA table_info(numbers)").fetchall()
    existing_cols = {row[1] for row in existing_schema}
    if "received_otp" not in existing_cols:
        c.execute("ALTER TABLE numbers ADD COLUMN received_otp INTEGER DEFAULT 0")
    if "queue_pos" not in existing_cols:
        c.execute("ALTER TABLE numbers ADD COLUMN queue_pos INTEGER")
    if "reserved_at" not in existing_cols:
        c.execute("ALTER TABLE numbers ADD COLUMN reserved_at TIMESTAMP")
    if "expires_at" not in existing_cols:
        c.execute("ALTER TABLE numbers ADD COLUMN expires_at TIMESTAMP")

    c.execute(
        """CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reason TEXT
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS otp_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                number_id INTEGER,
                otp_code TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                channel_identifier TEXT NOT NULL UNIQUE,
                invite_link TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS user_cooldowns (
                user_id INTEGER PRIMARY KEY,
                last_change_time INTEGER,
                warning_count INTEGER DEFAULT 0
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS service_emoji_overrides (
                service_name TEXT PRIMARY KEY,
                service_emoji TEXT NOT NULL DEFAULT '',
                custom_emoji_id TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS service_button_overrides (
                service_name TEXT PRIMARY KEY,
                button_emoji TEXT NOT NULL DEFAULT '',
                custom_emoji_id TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    existing_override_cols = {row[1] for row in c.execute("PRAGMA table_info(service_emoji_overrides)").fetchall()}
    if "custom_emoji_id" not in existing_override_cols:
        c.execute("ALTER TABLE service_emoji_overrides ADD COLUMN custom_emoji_id TEXT DEFAULT ''")
    existing_button_cols = {row[1] for row in c.execute("PRAGMA table_info(service_button_overrides)").fetchall()}
    if "button_emoji" not in existing_button_cols:
        c.execute("ALTER TABLE service_button_overrides ADD COLUMN button_emoji TEXT DEFAULT ''")
    if "custom_emoji_id" not in existing_button_cols:
        c.execute("ALTER TABLE service_button_overrides ADD COLUMN custom_emoji_id TEXT DEFAULT ''")
    _backfill_service_country_metadata(conn)
    _dedupe_button_emoji_in_service_names(conn)
    _cleanup_broken_custom_emoji_tokens(conn)
    _merge_duplicate_services(conn)
    c.execute("CREATE INDEX IF NOT EXISTS idx_service_id ON numbers(service_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_status_queue ON numbers(status, service_id, queue_pos)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_expires ON numbers(status, expires_at)")


def _init_postgres_db(conn):
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS services (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                country TEXT DEFAULT 'Other',
                country_flag TEXT DEFAULT '',
                country_custom_emoji_id TEXT DEFAULT '',
                country_code TEXT DEFAULT '',
                country_display_name TEXT DEFAULT '',
                status TEXT DEFAULT 'active',
                button_emoji TEXT DEFAULT '',
                service_emoji TEXT DEFAULT '',
                service_custom_emoji_id TEXT DEFAULT '',
                UNIQUE(country, name)
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS numbers (
                id BIGSERIAL PRIMARY KEY,
                service_id BIGINT REFERENCES services(id) ON DELETE CASCADE,
                number TEXT,
                status TEXT DEFAULT 'active',
                user_id BIGINT,
                last_used TIMESTAMP,
                received_otp INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                queue_pos INTEGER,
                reserved_at TIMESTAMP,
                expires_at TIMESTAMP
           )"""
    )
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS button_emoji TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS service_emoji TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS service_custom_emoji_id TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS country_flag TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS country_custom_emoji_id TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS country_code TEXT DEFAULT ''")
    c.execute("ALTER TABLE services ADD COLUMN IF NOT EXISTS country_display_name TEXT DEFAULT ''")
    c.execute("ALTER TABLE numbers ADD COLUMN IF NOT EXISTS received_otp INTEGER DEFAULT 0")
    c.execute("ALTER TABLE numbers ADD COLUMN IF NOT EXISTS queue_pos INTEGER")
    c.execute("ALTER TABLE numbers ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMP")
    c.execute("ALTER TABLE numbers ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP")

    c.execute(
        """CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reason TEXT
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS otp_log (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                number_id BIGINT,
                otp_code TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS channels (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                channel_identifier TEXT NOT NULL UNIQUE,
                invite_link TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS user_cooldowns (
                user_id BIGINT PRIMARY KEY,
                last_change_time BIGINT,
                warning_count INTEGER DEFAULT 0
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS service_emoji_overrides (
                service_name TEXT PRIMARY KEY,
                service_emoji TEXT NOT NULL DEFAULT '',
                custom_emoji_id TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS service_button_overrides (
                service_name TEXT PRIMARY KEY,
                button_emoji TEXT NOT NULL DEFAULT '',
                custom_emoji_id TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    _backfill_service_country_metadata(conn)
    _dedupe_button_emoji_in_service_names(conn)
    _cleanup_broken_custom_emoji_tokens(conn)
    _merge_duplicate_services(conn)
    c.execute("CREATE INDEX IF NOT EXISTS idx_service_id ON numbers(service_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_status_queue ON numbers(status, service_id, queue_pos)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_expires ON numbers(status, expires_at)")


def init_db():
    with get_db_connection() as conn:
        if DB_ENGINE == "postgresql":
            _init_postgres_db(conn)
        else:
            _init_sqlite_db(conn)

        c = conn.cursor()
        _backfill_service_button_overrides(conn)
        default_config = [
            ("max_numbers_per_assign", "5"),
            ("channels_version", "1"),
            ("change_number_cooldown_seconds", "7"),
            ("sms_limit", "1"),
            ("subscription_recheck_hours", "0"),
            ("assignment_mode", "serial"),
            ("auto_release_enabled", "1"),
            ("reservation_minutes", "60"),
            ("auto_release_interval_sec", "15"),
        ]
        for key, value in default_config:
            c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES (?, ?)", (key, value))

        service_rows = c.execute(
            "SELECT DISTINCT COALESCE(service_id, -1) AS sid FROM numbers ORDER BY sid"
        ).fetchall()
        for (sid,) in service_rows:
            if sid == -1:
                rows = c.execute("SELECT id FROM numbers WHERE service_id IS NULL ORDER BY id").fetchall()
                where_sql = "service_id IS NULL"
                where_args = ()
            else:
                rows = c.execute(
                    "SELECT id FROM numbers WHERE service_id = ? ORDER BY id",
                    (sid,),
                ).fetchall()
                where_sql = "service_id = ?"
                where_args = (sid,)
            if not rows:
                continue
            null_count = c.execute(
                f"SELECT COUNT(*) FROM numbers WHERE {where_sql} AND queue_pos IS NULL",
                where_args,
            ).fetchone()[0]
            if null_count <= 0:
                continue
            for pos, (num_id,) in enumerate(rows, start=1):
                c.execute("UPDATE numbers SET queue_pos = ? WHERE id = ?", (pos, num_id))

        conn.commit()
        logger.info("Database initialized successfully")


def get_bot_config_value(key, default=""):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM bot_config WHERE key = ?", (key,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return str(row[0])
    except Exception as e:
        logger.error(f"Failed reading bot_config {key}: {e}")
    return str(default)


def set_bot_config_value(key, value):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO bot_config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (key, str(value)),
            )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Failed writing bot_config {key}: {e}")
        return False


def get_assignment_mode():
    mode = (get_bot_config_value("assignment_mode", "serial") or "serial").strip().lower()
    return "serial" if mode != "random" else "random"


def _reservation_config(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_config WHERE key='auto_release_enabled'")
        enabled_row = cur.fetchone()
        enabled = str(enabled_row[0]).strip() == "1" if enabled_row and enabled_row[0] is not None else True
    except Exception:
        enabled = True
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_config WHERE key='reservation_minutes'")
        mins_row = cur.fetchone()
        minutes = int(mins_row[0]) if mins_row and mins_row[0] is not None else 60
    except Exception:
        minutes = 60
    return enabled, max(1, minutes)


def mark_numbers_reserved(conn, user_id, number_ids):
    if not number_ids:
        return
    enabled, minutes = _reservation_config(conn)
    now_utc = datetime.datetime.utcnow()
    reserved_at = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    expires_at = None
    if enabled and minutes > 0:
        expires_at = (now_utc + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    params = [(user_id, reserved_at, expires_at, int(num_id)) for num_id in number_ids]
    conn.executemany(
        "UPDATE numbers "
        "SET status='reserved', user_id=?, reserved_at=?, expires_at=?, last_used=CURRENT_TIMESTAMP "
        "WHERE id=?",
        params,
    )


def _max_queue_pos_for_service(conn, service_id):
    cur = conn.cursor()
    if service_id is None:
        cur.execute(
            "SELECT COALESCE(MAX(COALESCE(queue_pos, id)), 0) FROM numbers WHERE service_id IS NULL"
        )
    else:
        cur.execute(
            "SELECT COALESCE(MAX(COALESCE(queue_pos, id)), 0) FROM numbers WHERE service_id = ?",
            (service_id,),
        )
    row = cur.fetchone()
    return int(row[0] or 0)


def _move_rows_to_service_tail(conn, rows):
    if not rows:
        return 0
    service_to_ids = {}
    for num_id, service_id in rows:
        service_to_ids.setdefault(service_id, []).append(int(num_id))
    moved = 0
    for service_id, ids in service_to_ids.items():
        tail = _max_queue_pos_for_service(conn, service_id)
        cur = conn.cursor()
        for num_id in ids:
            tail += 1
            cur.execute("UPDATE numbers SET queue_pos = ? WHERE id = ?", (tail, num_id))
            moved += 1
    return moved


def release_numbers_for_user(conn, user_id, sms_limit=1, service_id=None):
    if service_id is None:
        where = "user_id = ?"
        args = [user_id]
    else:
        where = "user_id = ? AND service_id = ?"
        args = [user_id, service_id]

    cur = conn.cursor()
    if int(sms_limit) == 0:
        cur.execute(f"DELETE FROM numbers WHERE {where}", tuple(args))
        return {"deleted": cur.rowcount, "released": 0}

    delete_sql = f"DELETE FROM numbers WHERE {where} AND received_otp >= ?"
    cur.execute(delete_sql, tuple(args + [int(sms_limit)]))

    select_sql = f"SELECT id, service_id FROM numbers WHERE {where} AND received_otp < ?"
    cur.execute(select_sql, tuple(args + [int(sms_limit)]))
    release_rows = cur.fetchall()
    if release_rows:
        update_sql = (
            f"UPDATE numbers "
            f"SET status='active', user_id=NULL, reserved_at=NULL, expires_at=NULL "
            f"WHERE {where} AND received_otp < ?"
        )
        cur.execute(update_sql, tuple(args + [int(sms_limit)]))
        _move_rows_to_service_tail(conn, release_rows)

    return {"deleted": 0, "released": len(release_rows)}


def rebuild_number_queue():
    with get_db_connection() as conn:
        c = conn.cursor()
        service_rows = c.execute(
            "SELECT DISTINCT COALESCE(service_id, -1) AS sid FROM numbers ORDER BY sid"
        ).fetchall()
        total = 0
        for (sid,) in service_rows:
            if sid == -1:
                rows = c.execute(
                    "SELECT id FROM numbers WHERE service_id IS NULL ORDER BY COALESCE(queue_pos, id), id"
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT id FROM numbers WHERE service_id = ? ORDER BY COALESCE(queue_pos, id), id",
                    (sid,),
                ).fetchall()
            for pos, (num_id,) in enumerate(rows, start=1):
                c.execute("UPDATE numbers SET queue_pos = ? WHERE id = ?", (pos, num_id))
                total += 1
        conn.commit()
    return total


def get_queue_stats():
    with get_db_connection() as conn:
        c = conn.cursor()
        total = c.execute("SELECT COUNT(*) FROM numbers").fetchone()[0]
        active = c.execute("SELECT COUNT(*) FROM numbers WHERE status='active'").fetchone()[0]
        reserved = c.execute("SELECT COUNT(*) FROM numbers WHERE status='reserved'").fetchone()[0]
        expired = c.execute(
            "SELECT COUNT(*) FROM numbers "
            "WHERE status='reserved' AND expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP"
        ).fetchone()[0]
    return {
        "total": int(total or 0),
        "active": int(active or 0),
        "reserved": int(reserved or 0),
        "expired_ready": int(expired or 0),
    }


def release_expired_numbers_once(batch_size=500):
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT id, service_id FROM numbers "
            "WHERE status='reserved' AND expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP "
            "ORDER BY expires_at ASC LIMIT ?",
            (int(batch_size),),
        ).fetchall()
        if not rows:
            return 0
        ids = [int(r[0]) for r in rows]
        placeholders = ",".join(["?"] * len(ids))
        conn.execute(
            f"UPDATE numbers SET status='active', user_id=NULL, reserved_at=NULL, expires_at=NULL "
            f"WHERE id IN ({placeholders})",
            tuple(ids),
        )
        _move_rows_to_service_tail(conn, rows)
        conn.commit()
        return len(ids)


_auto_release_worker_started = False
_auto_release_lock = threading.Lock()


def start_auto_release_worker():
    global _auto_release_worker_started
    with _auto_release_lock:
        if _auto_release_worker_started:
            return
        _auto_release_worker_started = True

    def worker():
        while True:
            try:
                enabled = get_bot_config_value("auto_release_enabled", "1").strip() == "1"
                interval_raw = get_bot_config_value("auto_release_interval_sec", "15").strip()
                try:
                    interval = max(5, int(interval_raw))
                except Exception:
                    interval = 15
                if enabled:
                    released = release_expired_numbers_once()
                    if released:
                        logger.info("Auto-release moved %s expired reserved numbers back to tail", released)
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Auto-release worker error: {e}")
                time.sleep(10)

    threading.Thread(target=worker, daemon=True).start()


def reload_config():
    """Reload configuration from .env file."""
    global BOT_TOKEN, FORWARDER_BOT_TOKEN, ADMIN_IDS, BOT_NAME, OTP_GROUP_URL, WEBHOOK_SECRET
    global WEBHOOK_HOST, WEBHOOK_PORT, WEBHOOK_PATH, WEBHOOK_WORKERS, SEND_WORKERS, SEND_DELAY
    global GROUP_SEND_WORKERS, GROUP_SEND_DELAY, TELEGRAM_DEBUG_JSON, DATABASE_URL, DB_NAME, DB_ENGINE
    global POSTGRES_USE_POOL, POSTGRES_POOL_MIN_SIZE, POSTGRES_POOL_MAX_SIZE, POSTGRES_CONNECT_TIMEOUT

    load_dotenv(dotenv_path=env_path, override=True)

    BOT_TOKEN = os.getenv("BOT_TOKEN") or ""
    FORWARDER_BOT_TOKEN = os.getenv("FORWARDER_BOT_TOKEN", BOT_TOKEN) or BOT_TOKEN
    _admin_val = os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID"))
    ADMIN_IDS = []
    if _admin_val:
        try:
            ADMIN_IDS = [int(x.strip()) for x in _admin_val.split(",") if x.strip()]
        except ValueError:
            ADMIN_IDS = []

    BOT_NAME = os.getenv("BOT_NAME") or "NUMBER BOT"
    OTP_GROUP_URL = os.getenv("OTP_GROUP_URL") or ""
    WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
    WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "127.0.0.1")
    WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8080"))
    WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
    WEBHOOK_WORKERS = int(os.getenv("WEBHOOK_WORKERS", "2"))
    SEND_WORKERS = int(os.getenv("SEND_WORKERS", "3"))
    SEND_DELAY = float(os.getenv("SEND_DELAY", "0.2"))
    GROUP_SEND_WORKERS = int(os.getenv("GROUP_SEND_WORKERS", "4"))
    GROUP_SEND_DELAY = float(os.getenv("GROUP_SEND_DELAY", "0.02"))
    TELEGRAM_DEBUG_JSON = os.getenv("TELEGRAM_DEBUG_JSON", "0").strip() == "1"
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    DB_NAME = os.getenv("DB_NAME", "number_panel.db")
    DB_ENGINE = (os.getenv("DB_ENGINE") or ("postgresql" if DATABASE_URL else "sqlite")).strip().lower()
    if DB_ENGINE in {"postgres", "postgresql", "psql"}:
        DB_ENGINE = "postgresql"
    else:
        DB_ENGINE = "sqlite"
    POSTGRES_USE_POOL = os.getenv("POSTGRES_USE_POOL", "1").strip() == "1"
    POSTGRES_POOL_MIN_SIZE = max(1, int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1")))
    POSTGRES_POOL_MAX_SIZE = max(POSTGRES_POOL_MIN_SIZE, int(os.getenv("POSTGRES_POOL_MAX_SIZE", "8")))
    POSTGRES_CONNECT_TIMEOUT = max(1, int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")))
    _close_postgres_pool()

    logger.info("Configuration reloaded from .env file")


import telebot
from telebot import apihelper

apihelper.CONNECT_TIMEOUT = 15
apihelper.READ_TIMEOUT = 30

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=os.getenv("TELEGRAM_PARSE_MODE", "HTML"))
