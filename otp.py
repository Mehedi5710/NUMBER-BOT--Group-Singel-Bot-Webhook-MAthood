"""OTP receiving and forwarding module"""
import re
import datetime
import threading
import queue
import time
import requests
import socket
import json
from core import (
    get_db_connection,
    logger,
    SEND_WORKERS,
    SEND_DELAY,
    GROUP_SEND_WORKERS,
    GROUP_SEND_DELAY,
    TELEGRAM_DEBUG_JSON,
)
import core
from group_message_format import build_group_message
from flag import canonical_country_name

_send_queue = queue.Queue()
_send_workers_started = False
_group_send_queue = queue.Queue()
_group_send_workers_started = False


def _sanitize_reply_markup(reply_markup):
    """Normalize reply markup structure while preserving supported button fields."""
    if not isinstance(reply_markup, dict):
        return reply_markup
    cleaned = {"inline_keyboard": []}
    for row in reply_markup.get("inline_keyboard", []):
        if not isinstance(row, list):
            continue
        new_row = []
        for btn in row:
            if not isinstance(btn, dict):
                continue
            b = dict(btn)
            new_row.append(b)
        if new_row:
            cleaned["inline_keyboard"].append(new_row)
    return cleaned


def _start_send_workers(workers=2, min_interval=0.05):
    """Start background workers for Telegram sends."""
    global _send_workers_started
    if _send_workers_started:
        return
    _send_workers_started = True

    def worker():
        session = requests.Session()
        while True:
            item = _send_queue.get()
            if item is None:
                break
            url, payload, done_event, result = item
            try:
                response = session.post(url, json=payload, timeout=10)
                ok = response.status_code == 200
                if not ok and response.status_code == 429:
                    try:
                        retry_after = response.json().get("parameters", {}).get("retry_after", 1)
                    except Exception:
                        retry_after = 1
                    time.sleep(max(1, int(retry_after)))
                    response = session.post(url, json=payload, timeout=10)
                    ok = response.status_code == 200
                result["ok"] = ok
            except Exception as e:
                logger.error(f"Error sending Telegram message: {e}")
                result["ok"] = False
            finally:
                done_event.set()
                _send_queue.task_done()
                time.sleep(min_interval)

    for _ in range(max(1, workers)):
        threading.Thread(target=worker, daemon=True).start()


def _start_group_send_workers(workers=4, min_interval=0.02):
    """Start background workers for forwarder group sends (non-blocking)."""
    global _group_send_workers_started
    if _group_send_workers_started:
        return
    _group_send_workers_started = True

    def worker():
        session = requests.Session()
        while True:
            item = _group_send_queue.get()
            if item is None:
                break
            url, payload = item
            try:
                if TELEGRAM_DEBUG_JSON:
                    logger.info(
                        "TG_GROUP_REQUEST chat_id=%s payload=%s",
                        payload.get("chat_id"),
                        json.dumps(payload, ensure_ascii=False),
                    )
                response = session.post(url, json=payload, timeout=10)
                if TELEGRAM_DEBUG_JSON:
                    logger.info(
                        "TG_GROUP_RESPONSE chat_id=%s status=%s body=%s",
                        payload.get("chat_id"),
                        response.status_code,
                        response.text[:1200],
                    )
                if response.status_code == 429:
                    try:
                        retry_after = response.json().get("parameters", {}).get("retry_after", 1)
                    except Exception:
                        retry_after = 1
                    time.sleep(max(1, int(retry_after)))
                    response = session.post(url, json=payload, timeout=10)
                if response.status_code != 200:
                    logger.warning(
                        "Group send failed chat_id=%s status=%s body=%s",
                        payload.get("chat_id"),
                        response.status_code,
                        response.text[:300],
                    )
            except Exception as e:
                logger.error(f"Error sending group message: {e}")
            finally:
                _group_send_queue.task_done()
                time.sleep(min_interval)

    for _ in range(max(1, workers)):
        threading.Thread(target=worker, daemon=True).start()


def _load_forwarder_group_ids():
    try:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT value FROM bot_config WHERE key='forwarder_group_ids'"
            ).fetchone()
        raw = str(row[0]).strip() if row and row[0] is not None else ""
    except Exception as e:
        logger.error(f"Failed to load forwarder group ids: {e}")
        return []

    items = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except Exception:
            continue
    # ordered unique
    seen = set()
    result = []
    for gid in items:
        if gid in seen:
            continue
        seen.add(gid)
        result.append(gid)
    return result


def _build_otp_text(phone_number: str, service_name: str, country: str):
    from flag import get_flag
    flag = get_flag(country) if country else "dYO?"
    display_phone = phone_number.replace('+', '').replace(' ', '')
    return f"{flag} <b>{service_name}</b>  {display_phone}"


def _enqueue_otp_to_groups(phone_number: str, otp_code: str, service_name: str, country: str):
    from core import BOT_TOKEN, FORWARDER_BOT_TOKEN
    group_ids = _load_forwarder_group_ids()
    if not group_ids:
        logger.info("Group forward skipped: no forwarder_group_ids configured")
        return

    def _get_cfg(key):
        try:
            with get_db_connection() as conn:
                row = conn.execute("SELECT value FROM bot_config WHERE key=?", (key,)).fetchone()
            return str(row[0]).strip() if row and row[0] is not None else ""
        except Exception:
            return ""

    send_token = FORWARDER_BOT_TOKEN or BOT_TOKEN
    url = f"https://api.telegram.org/bot{send_token}/sendMessage"
    number_bot_link = _get_cfg("forwarder_number_bot_link")
    support_group_link = _get_cfg("forwarder_support_group_link")
    text, reply_markup = build_group_message(
        phone_number,
        otp_code,
        service_name,
        country,
        number_bot_link=number_bot_link,
        support_group_link=support_group_link,
    )
    reply_markup = _sanitize_reply_markup(reply_markup)

    _start_group_send_workers(workers=GROUP_SEND_WORKERS, min_interval=GROUP_SEND_DELAY)
    for gid in group_ids:
        payload = {
            "chat_id": gid,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": json.dumps(reply_markup),
        }
        _group_send_queue.put((url, payload))
    logger.info(
        "Group forward queued: %s otp(s) for %s group(s), number=%s",
        1,
        len(group_ids),
        phone_number,
    )


def extract_multiple_otps(message_text: str):
    """Extract OTP codes from messages"""
    try:
        otps = []
        message_lower = message_text.lower()

        # OTP patterns with keywords
        patterns = [
            r'otp[:\s\-]+([0-9]{4,8})', r'code[:\s\-]+([0-9]{4,8})',
            r'verification[:\s\-]+([0-9]{4,8})', r'confirm[:\s\-]+([0-9]{4,8})',
            r'pin[:\s\-]+([0-9]{4,8})', r'password[:\s\-]+([0-9]{4,8})',
            r'security[:\s\-]+([0-9]{4,8})', r'auth[:\s\-]+([0-9]{4,8})',
            r'login[:\s\-]+([0-9]{4,8})', r'FB\-([0-9]{4,8})',
            r'WA\-([0-9]{4,8})', r'TG\-([0-9]{4,8})'
        ]

        for pattern in patterns:
            matches = re.findall(pattern, message_lower, re.IGNORECASE)
            otps.extend(matches)

        # Numbers with dashes (397-834)
        dash_matches = re.findall(r'([0-9]{3})\-([0-9]{3})', message_text)
        for match in dash_matches:
            otp = match[0] + match[1]
            if 4 <= len(otp) <= 8:
                otps.append(otp)

        # Standalone numbers (fallback)
        if not otps:
            all_numbers = re.findall(r'\d+', message_text)
            for num in all_numbers:
                if 4 <= len(num) <= 8 and len(num) < 10:
                    otps.append(num)

        # Remove duplicates
        seen = set()
        unique_otps = [otp for otp in otps if not (otp in seen or seen.add(otp))]

        return unique_otps

    except Exception as e:
        logger.error(f"Error extracting OTPs: {str(e)}")
        return []


def find_user_by_number(phone_number: str):
    """Find user assigned to phone number"""
    try:
        clean_phone = re.sub(r'[^\d]', '', phone_number)

        with get_db_connection() as conn:
            # Try without + prefix
            result = conn.execute(
                "SELECT id, user_id, service_id FROM numbers WHERE number = ? AND status = 'reserved'",
                (clean_phone,)
            ).fetchone()

            if result:
                return {'number_id': result[0], 'user_id': result[1], 'service_id': result[2]}

            # Try with + prefix
            result = conn.execute(
                "SELECT id, user_id, service_id FROM numbers WHERE number = ? AND status = 'reserved'",
                ('+' + clean_phone,)
            ).fetchone()

            if result:
                return {'number_id': result[0], 'user_id': result[1], 'service_id': result[2]}

        return None

    except Exception as e:
        logger.error(f"Error finding user: {str(e)}")
        return None


def get_service_name(service_id: int):
    """Get service name by ID"""
    try:
        with get_db_connection() as conn:
            result = conn.execute("SELECT name, country FROM services WHERE id = ?", (service_id,)).fetchone()
            return (result[0], result[1]) if result else (None, None)
    except Exception as e:
        logger.error(f"Error getting service: {str(e)}")
        return None, None


def process_incoming_message(
    phone_number: str,
    otp_message: str,
    service_name_override: str = "",
    country_override: str = "",
):
    """Process a single OTP message payload"""
    if not phone_number or not otp_message:
        return False

    otps = extract_multiple_otps(otp_message)
    if not otps:
        return False

    user_info = find_user_by_number(phone_number)
    user_id = None
    number_id = None

    # Resolve service/country for formatting. Overrides always win.
    service_name = service_name_override or ""
    country = country_override or ""
    if user_info:
        user_id = user_info['user_id']
        service_id = user_info['service_id']
        number_id = user_info['number_id']
        db_service_name, db_country = get_service_name(service_id)
        if not service_name:
            service_name = db_service_name or ""
        if not country:
            country = db_country or ""
    if not service_name:
        service_name = "Unknown"
    if not country:
        country = "Unknown"

    # Fallback: when webhook does not provide country and DB also has no reliable country,
    # detect country from phone number prefix so flag rendering still works.
    if str(country).strip().lower() in {"", "unknown", "xx"}:
        detected_country = canonical_country_name("", numbers=[phone_number])
        if detected_country:
            country = detected_country

    if not user_id:
        logger.info("No assigned user for %s; group forward only", phone_number)

    for otp in otps:
        # Group forwards always run for valid webhook OTPs, independent from user assignment.
        _enqueue_otp_to_groups(phone_number, otp, service_name, country)

        # User forward runs only when this number is currently assigned to a user.
        if user_id:
            success = forward_otp_to_user(user_id, phone_number, otp, service_name, country, number_id)
            if success and number_id:
                try:
                    with get_db_connection() as conn:
                        conn.execute("UPDATE numbers SET received_otp = 1 WHERE id = ?", (number_id,))
                        conn.commit()
                except Exception as e:
                    logger.error(f"Error updating received_otp for {number_id}: {e}")
    return True


def start_webhook_server(host="127.0.0.1", port=8080, path="/webhook", workers=2):
    """Start FastAPI webhook server in a background thread"""
    try:
        from fastapi import FastAPI, Request, Header, HTTPException
        import uvicorn
    except Exception as e:
        logger.error(f"FastAPI/uvicorn not installed: {e}")
        return None

    # Avoid noisy uvicorn bind errors when another instance is already running.
    # Don't "bind to test" on Windows (can raise WinError 10013); instead probe by connecting.
    probe_host = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    try:
        with socket.create_connection((probe_host, int(port)), timeout=0.5):
            logger.warning(f"Webhook server not started: {host}:{port} already has a listener")
            return None
    except OSError:
        # No listener (or not reachable) -> proceed and let uvicorn attempt the bind.
        pass

    app = FastAPI()
    msg_queue = queue.Queue()

    def _pick_phone(item: dict) -> str:
        return str(
            item.get("phone_number")
            or item.get("number")
            or item.get("phone")
            or ""
        ).strip()

    def _pick_message(item: dict) -> str:
        return str(
            item.get("message")
            or item.get("message_text")
            or item.get("sms")
            or item.get("text")
            or ""
        ).strip()

    def _pick_service(item: dict) -> str:
        return str(item.get("service") or item.get("service_name") or "").strip()

    def _pick_country(item: dict) -> str:
        return str(item.get("country") or "").strip()

    def worker_loop():
        while True:
            item = msg_queue.get()
            if item is None:
                break
            try:
                phone_number = _pick_phone(item)
                otp_message = _pick_message(item)
                service_name = _pick_service(item)
                country = _pick_country(item)
                process_incoming_message(
                    phone_number,
                    otp_message,
                    service_name_override=service_name,
                    country_override=country,
                )
            except Exception as e:
                logger.error(f"Webhook worker error: {e}")
            finally:
                msg_queue.task_done()

    for _ in range(max(1, workers)):
        threading.Thread(target=worker_loop, daemon=True).start()

    @app.post(path)
    async def receive_webhook(request: Request, x_webhook_secret: str = Header(default="")):
        payload = await request.json()

        # Optional webhook secret verification. If WEBHOOK_SECRET is set,
        # caller must provide matching secret either via header or payload field.
        secret_expected = (getattr(core, "WEBHOOK_SECRET", "") or "").strip()
        if secret_expected:
            payload_secret = ""
            if isinstance(payload, dict):
                payload_secret = str(payload.get("secret") or "").strip()
            provided = (x_webhook_secret or "").strip() or payload_secret
            if provided != secret_expected:
                raise HTTPException(status_code=403, detail="Invalid webhook secret")

        items = []
        if isinstance(payload, dict) and "messages" in payload and isinstance(payload["messages"], list):
            items = payload["messages"]
        elif isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = [payload]

        for item in items:
            if not isinstance(item, dict):
                continue
            msg_queue.put(item)

        return {"ok": True, "queued": len(items)}

    def run_server():
        uvicorn.run(app, host=host, port=port, log_level="warning")

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    logger.info(f"Webhook server started at http://{host}:{port}{path}")
    return thread


def forward_otp_to_user(user_id: int, phone_number: str, otp_code: str, service_name: str, country: str, number_id: int = None):
    """Forward OTP to user"""
    try:
        from core import BOT_TOKEN

        message = _build_otp_text(phone_number, service_name, country)

        reply_markup = {
            "inline_keyboard": [
                [{"text": otp_code, "copy_text": {"text": otp_code}}]
            ]
        }
        reply_markup = _sanitize_reply_markup(reply_markup)

        # User OTP must go through main bot token so users receive in main bot chat.
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": user_id,
            "text": message,
            "parse_mode": "HTML",
            "reply_markup": json.dumps(reply_markup)
        }

        done_event = threading.Event()
        result = {"ok": False}

        _start_send_workers(workers=SEND_WORKERS, min_interval=SEND_DELAY)
        if TELEGRAM_DEBUG_JSON:
            logger.info(
                "TG_USER_REQUEST chat_id=%s payload=%s",
                user_id,
                json.dumps(payload, ensure_ascii=False),
            )
        _send_queue.put((url, payload, done_event, result))
        done_event.wait(timeout=15)

        if result["ok"]:
            logger.info(f"OTP sent to user {user_id}")
            return True
        logger.error("Failed to send OTP")
        return False

    except Exception as e:
        logger.error(f"Error forwarding OTP: {str(e)}")
        return False


