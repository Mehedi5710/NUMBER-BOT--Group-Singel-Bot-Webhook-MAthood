"""Core configuration and database setup"""

# NOTE: Your Python 3.14 install has a broken stdlib module at
# `...Lib/importlib/resources/_common.py` (it contains unrelated code and imports `requests`),
# which causes a circular import when `requests -> certifi -> importlib.resources` happens.
# This stub avoids importing that broken stdlib module and provides the small subset of the
# API needed by `certifi` (used by `requests`, used by `telebot` and our webhook sender).
import sys
import types
import contextlib
import importlib.util
from pathlib import Path


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
        # certifi passes a Path-like object (from files(...).joinpath(...))
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
    # This will currently fail on your machine due to the corrupted stdlib module.
    import importlib.resources as _ilr  # noqa: F401
except Exception:
    _install_importlib_resources_stub()

from dotenv import load_dotenv
import os
import logging
import datetime
import sqlite3
import contextlib
import threading
import time

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

BOT_TOKEN = os.getenv('BOT_TOKEN') or ''
FORWARDER_BOT_TOKEN = os.getenv('FORWARDER_BOT_TOKEN', BOT_TOKEN) or BOT_TOKEN
_admin_val = os.getenv('ADMIN_IDS', os.getenv('ADMIN_ID'))
ADMIN_IDS = []
if _admin_val:
    try:
        ADMIN_IDS = [int(x.strip()) for x in _admin_val.split(',') if x.strip()]
    except ValueError:
        ADMIN_IDS = []

BOT_NAME = os.getenv('BOT_NAME') or "NUMBER BOT"
OTP_GROUP_URL = os.getenv('OTP_GROUP_URL') or ""
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '').strip()
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '127.0.0.1')
WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '8080'))
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/webhook')
WEBHOOK_WORKERS = int(os.getenv('WEBHOOK_WORKERS', '2'))
SEND_WORKERS = int(os.getenv('SEND_WORKERS', '3'))
SEND_DELAY = float(os.getenv('SEND_DELAY', '0.2'))
GROUP_SEND_WORKERS = int(os.getenv('GROUP_SEND_WORKERS', '4'))
GROUP_SEND_DELAY = float(os.getenv('GROUP_SEND_DELAY', '0.02'))
TELEGRAM_DEBUG_JSON = os.getenv('TELEGRAM_DEBUG_JSON', '0').strip() == '1'
DB_NAME = os.getenv('DB_NAME', 'number_panel.db')
LOG_DIR = Path(os.getenv('LOG_DIR', 'logs'))
BACKUP_DIR = Path(os.getenv('BACKUP_DIR', 'backups'))

# Create directories
LOG_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f'bot_{datetime.datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress excessive telebot debug logging
logging.getLogger('telebot').setLevel(logging.CRITICAL)  # Only show critical errors
logging.getLogger('urllib3').setLevel(logging.WARNING)

def cleanup_old_logs(days_to_keep=7):
    """Delete log files older than specified days"""
    try:
        current_time = datetime.datetime.now()
        cutoff_time = current_time - datetime.timedelta(days=days_to_keep)
        
        deleted_count = 0
        for log_file in LOG_DIR.glob('*.log'):
            file_time = datetime.datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_time:
                log_file.unlink()
                deleted_count += 1
                logger.info(f"Deleted old log: {log_file.name}")
        
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old log files")
    except Exception as e:
        logger.error(f"Failed to cleanup logs: {e}")

@contextlib.contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db_connection() as conn:
        c = conn.cursor()
        # Check if services table exists with old schema
        old_schema = c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='services'").fetchone()
        needs_migration = old_schema and 'UNIQUE' in old_schema[0] and 'UNIQUE(country, name)' not in old_schema[0]
        needs_status_migration = old_schema and 'status' not in old_schema[0]
        
        if needs_migration:
            # Need to recreate table without UNIQUE constraint on name alone
            logger.info("Migrating services table to new schema...")
            try:
                # Check if old table exists from previous failed migration
                c.execute("DROP TABLE IF EXISTS services_old")
                
                # Rename current table
                c.execute("ALTER TABLE services RENAME TO services_old")
                
                # Create new table with correct schema
                c.execute('''CREATE TABLE services (
                                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                name TEXT NOT NULL, 
                                country TEXT DEFAULT 'Other',
                                status TEXT DEFAULT 'active',
                                UNIQUE(country, name)
                             )''')
                
                # Copy data from old table
                c.execute("INSERT INTO services (id, name, country) SELECT id, name, country FROM services_old")
                
                # Drop old table
                c.execute("DROP TABLE services_old")
                conn.commit()
                logger.info("Services table migrated successfully")
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                conn.rollback()
                raise
        elif needs_status_migration:
            # Add status column to existing table
            logger.info("Adding status column to services table...")
            try:
                c.execute("ALTER TABLE services ADD COLUMN status TEXT DEFAULT 'active'")
                conn.commit()
                logger.info("Status column added successfully")
            except Exception as e:
                logger.error(f"Failed to add status column: {e}")
                conn.rollback()
        else:
            c.execute('''CREATE TABLE IF NOT EXISTS services (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            name TEXT NOT NULL, 
                            country TEXT DEFAULT 'Other',
                            status TEXT DEFAULT 'active',
                            UNIQUE(country, name)
                         )''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS numbers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service_id INTEGER,
                        number TEXT,
                        status TEXT DEFAULT 'active',
                        user_id INTEGER,
                        last_used TIMESTAMP,
                        received_otp INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE
                     )''')
        # Add received_otp column for older databases
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
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users (
                        user_id INTEGER PRIMARY KEY,
                        banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        reason TEXT
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS otp_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        number_id INTEGER,
                        otp_code TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS channels (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        channel_identifier TEXT NOT NULL UNIQUE,
                        invite_link TEXT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS bot_config (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_cooldowns (
                        user_id INTEGER PRIMARY KEY,
                        last_change_time INTEGER,
                        warning_count INTEGER DEFAULT 0
                     )''')
        c.execute("CREATE INDEX IF NOT EXISTS idx_service_id ON numbers(service_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_status_queue ON numbers(status, service_id, queue_pos)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_numbers_expires ON numbers(status, expires_at)")
        
        # Set default max numbers if not exists
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('max_numbers_per_assign', '5')")
        # Track channel config changes for subscription caching
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('channels_version', '1')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('change_number_cooldown_seconds', '7')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('sms_limit', '1')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('subscription_recheck_hours', '0')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('assignment_mode', 'serial')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('auto_release_enabled', '1')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('reservation_minutes', '60')")
        c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('auto_release_interval_sec', '15')")

        # Initialize queue positions once for rows that don't have it yet.
        service_rows = c.execute(
            "SELECT DISTINCT COALESCE(service_id, -1) AS sid FROM numbers ORDER BY sid"
        ).fetchall()
        for (sid,) in service_rows:
            if sid == -1:
                rows = c.execute(
                    "SELECT id FROM numbers WHERE service_id IS NULL ORDER BY id"
                ).fetchall()
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
            row = conn.execute("SELECT value FROM bot_config WHERE key = ?", (key,)).fetchone()
            if row and row[0] is not None:
                return str(row[0])
    except Exception as e:
        logger.error(f"Failed reading bot_config {key}: {e}")
    return str(default)


def set_bot_config_value(key, value):
    try:
        with get_db_connection() as conn:
            conn.execute(
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
        enabled_row = conn.execute(
            "SELECT value FROM bot_config WHERE key='auto_release_enabled'"
        ).fetchone()
        enabled = str(enabled_row[0]).strip() == "1" if enabled_row and enabled_row[0] is not None else True
    except Exception:
        enabled = True
    try:
        mins_row = conn.execute(
            "SELECT value FROM bot_config WHERE key='reservation_minutes'"
        ).fetchone()
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
    if service_id is None:
        row = conn.execute(
            "SELECT COALESCE(MAX(COALESCE(queue_pos, id)), 0) FROM numbers WHERE service_id IS NULL"
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COALESCE(MAX(COALESCE(queue_pos, id)), 0) FROM numbers WHERE service_id = ?",
            (service_id,),
        ).fetchone()
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
        for num_id in ids:
            tail += 1
            conn.execute("UPDATE numbers SET queue_pos = ? WHERE id = ?", (tail, num_id))
            moved += 1
    return moved


def release_numbers_for_user(conn, user_id, sms_limit=1, service_id=None):
    if service_id is None:
        where = "user_id = ?"
        args = [user_id]
    else:
        where = "user_id = ? AND service_id = ?"
        args = [user_id, service_id]

    if int(sms_limit) == 0:
        conn.execute(f"DELETE FROM numbers WHERE {where}", tuple(args))
        return {"deleted": conn.total_changes, "released": 0}

    delete_sql = f"DELETE FROM numbers WHERE {where} AND received_otp >= ?"
    conn.execute(delete_sql, tuple(args + [int(sms_limit)]))

    release_rows = conn.execute(
        f"SELECT id, service_id FROM numbers WHERE {where} AND received_otp < ?",
        tuple(args + [int(sms_limit)]),
    ).fetchall()
    if release_rows:
        conn.execute(
            f"UPDATE numbers "
            f"SET status='active', user_id=NULL, reserved_at=NULL, expires_at=NULL "
            f"WHERE {where} AND received_otp < ?",
            tuple(args + [int(sms_limit)]),
        )
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
            "SELECT COUNT(*) FROM numbers WHERE status='reserved' AND expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP"
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
    """Reload configuration from .env file"""
    global BOT_TOKEN, FORWARDER_BOT_TOKEN, ADMIN_IDS, BOT_NAME, OTP_GROUP_URL, WEBHOOK_SECRET
    global WEBHOOK_HOST, WEBHOOK_PORT, WEBHOOK_PATH, WEBHOOK_WORKERS, SEND_WORKERS, SEND_DELAY
    global GROUP_SEND_WORKERS, GROUP_SEND_DELAY, TELEGRAM_DEBUG_JSON
    
    load_dotenv(dotenv_path=env_path, override=True)
    
    BOT_TOKEN = os.getenv('BOT_TOKEN') or ''
    FORWARDER_BOT_TOKEN = os.getenv('FORWARDER_BOT_TOKEN', BOT_TOKEN) or BOT_TOKEN
    _admin_val = os.getenv('ADMIN_IDS', os.getenv('ADMIN_ID'))
    ADMIN_IDS = []
    if _admin_val:
        try:
            ADMIN_IDS = [int(x.strip()) for x in _admin_val.split(',') if x.strip()]
        except ValueError:
            ADMIN_IDS = []
    
    BOT_NAME = os.getenv('BOT_NAME') or "NUMBER BOT"
    OTP_GROUP_URL = os.getenv('OTP_GROUP_URL') or ""
    WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '').strip()
    WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '127.0.0.1')
    WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '8080'))
    WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/webhook')
    WEBHOOK_WORKERS = int(os.getenv('WEBHOOK_WORKERS', '2'))
    SEND_WORKERS = int(os.getenv('SEND_WORKERS', '3'))
    SEND_DELAY = float(os.getenv('SEND_DELAY', '0.2'))
    GROUP_SEND_WORKERS = int(os.getenv('GROUP_SEND_WORKERS', '4'))
    GROUP_SEND_DELAY = float(os.getenv('GROUP_SEND_DELAY', '0.02'))
    TELEGRAM_DEBUG_JSON = os.getenv('TELEGRAM_DEBUG_JSON', '0').strip() == '1'
    
    logger.info("Configuration reloaded from .env file")

import telebot
from telebot import apihelper

# Configure API timeouts for better stability
apihelper.CONNECT_TIMEOUT = 15
apihelper.READ_TIMEOUT = 30

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=os.getenv('TELEGRAM_PARSE_MODE', 'HTML'))
