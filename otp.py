"""OTP receiving and forwarding module"""
import json
import re
import datetime
import threading
import queue
import time
import requests
import socket
import json
from entity_text import EntityTextBuilder
from core import (
    get_db_connection,
    logger,
    SEND_WORKERS,
    SEND_DELAY,
    GROUP_SEND_WORKERS,
    GROUP_SEND_DELAY,
    TELEGRAM_DEBUG_JSON,
    format_service_display,
    format_service_visible,
    format_service_icon_only,
    get_service_emoji_override_data,
)
import core
from group_message_format import build_group_message
from flag import canonical_country_name, strip_display_flag, format_country_icon, format_display_country

_send_queue = queue.Queue()
_send_workers_started = False
_group_send_queue = queue.Queue()
_group_send_workers_started = False


def _normalize_entities(entities):
    if not entities:
        return []
    normalized = []
    for entity in entities:
        if hasattr(entity, "to_dict"):
            normalized.append(entity.to_dict())
        else:
            normalized.append(entity)
    return normalized


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

    def _telegram_request_ok(response):
        if response.status_code != 200:
            return False
        try:
            data = response.json()
        except Exception:
            return False
        return bool(data.get("ok"))

    def worker():
        session = requests.Session()
        while True:
            item = _send_queue.get()
            if item is None:
                break
            url, payload, done_event, result = item
            try:
                response = session.post(url, json=payload, timeout=10)
                ok = _telegram_request_ok(response)
                if not ok and response.status_code == 429:
                    try:
                        retry_after = response.json().get("parameters", {}).get("retry_after", 1)
                    except Exception:
                        retry_after = 1
                    time.sleep(max(1, int(retry_after)))
                    response = session.post(url, json=payload, timeout=10)
                    ok = _telegram_request_ok(response)
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

    def _telegram_request_ok(response):
        if response.status_code != 200:
            return False
        try:
            data = response.json()
        except Exception:
            return False
        return bool(data.get("ok"))

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
                if not _telegram_request_ok(response):
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


def _build_otp_text(
    phone_number: str,
    service_name: str,
    service_emoji: str = "",
    service_custom_emoji_id: str = "",
    country_flag: str = "",
    country_custom_emoji_id: str = "",
):
    display_phone = phone_number.replace('+', '').replace(' ', '')
    builder = EntityTextBuilder()
    country_flag = str(country_flag or "").strip()
    country_custom_emoji_id = str(country_custom_emoji_id or "").strip()
    service_emoji = str(service_emoji or "").strip()
    service_custom_emoji_id = str(service_custom_emoji_id or "").strip()
    country_icon_added = False
    if country_custom_emoji_id and country_flag:
        country_icon_added = builder.append_custom_emoji(country_flag, country_custom_emoji_id)
    elif country_flag:
        builder.append(country_flag)
        country_icon_added = True
    if country_icon_added:
        builder.append(" ")
    service_icon_added = False
    if service_custom_emoji_id and service_emoji:
        service_icon_added = builder.append_custom_emoji(service_emoji, service_custom_emoji_id)
    elif service_emoji:
        builder.append(service_emoji)
        service_icon_added = True
    if service_icon_added:
        builder.append(" ")
    builder.append_bold(service_name or "Unknown")
    builder.append("  ")
    builder.append_code(display_phone)
    return builder.text, builder.entities


def _enqueue_otp_to_groups(
    phone_number: str,
    otp_code: str,
    service_name: str,
    country: str,
    country_code: str = "",
    country_flag: str = "",
    country_custom_emoji_id: str = "",
    service_icon: str = "",
    service_custom_emoji_id: str = "",
):
    from core import BOT_TOKEN, FORWARDER_BOT_TOKEN
    group_ids = _load_forwarder_group_ids()
    if not group_ids:
        logger.info("Group forward skipped: no forwarder_group_ids configured")
        return

    def _get_cfg(key):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT value FROM bot_config WHERE key=?", (key,))
                row = cur.fetchone()
            return str(row[0]).strip() if row and row[0] is not None else ""
        except Exception:
            return ""

    send_token = FORWARDER_BOT_TOKEN or BOT_TOKEN
    url = f"https://api.telegram.org/bot{send_token}/sendMessage"
    number_bot_link = _get_cfg("forwarder_number_bot_link")
    support_group_link = _get_cfg("forwarder_support_group_link")
    text, entities, reply_markup = build_group_message(
        phone_number,
        otp_code,
        service_name,
        country,
        country_code=country_code,
        country_flag=country_flag,
        country_custom_emoji_id=country_custom_emoji_id,
        service_icon=service_icon,
        service_custom_emoji_id=service_custom_emoji_id,
        number_bot_link=number_bot_link,
        support_group_link=support_group_link,
    )
    reply_markup = _sanitize_reply_markup(reply_markup)
    entities = _normalize_entities(entities)

    _start_group_send_workers(workers=GROUP_SEND_WORKERS, min_interval=GROUP_SEND_DELAY)
    for gid in group_ids:
        payload = {
            "chat_id": gid,
            "text": text,
            "disable_web_page_preview": True,
            "entities": entities,
            "reply_markup": reply_markup,
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
            cur = conn.cursor()
            # Try without + prefix
            cur.execute(
                "SELECT id, user_id, service_id FROM numbers WHERE number = ? AND status = 'reserved'",
                (clean_phone,)
            )
            result = cur.fetchone()
            if result:
                return {'number_id': result[0], 'user_id': result[1], 'service_id': result[2]}
            # Try with + prefix
            cur.execute(
                "SELECT id, user_id, service_id FROM numbers WHERE number = ? AND status = 'reserved'",
                ('+' + clean_phone,)
            )
            result = cur.fetchone()
            if result:
                return {'number_id': result[0], 'user_id': result[1], 'service_id': result[2]}

        return None

    except Exception as e:
        logger.error(f"Error finding user: {str(e)}")
        return None


def get_service_meta(service_id: int):
    """Get service metadata by ID"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT name, country, COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, ''), "
                "COALESCE(country_code, ''), COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), "
                "COALESCE(country_display_name, '') "
                "FROM services WHERE id = ?",
                (service_id,),
            )
            result = cur.fetchone()
            if not result:
                return None
            name, country, service_emoji, service_custom_emoji_id, country_code, country_flag, country_custom_emoji_id, country_display_name = result
            country_label = country_display_name or country
            override_text, override_custom_id = get_service_emoji_override_data(name, conn=conn)
            effective_service_emoji = override_text if (override_text or override_custom_id) else service_emoji
            effective_service_custom_emoji_id = override_custom_id if (override_text or override_custom_id) else service_custom_emoji_id
            return {
                "service_name_html": format_service_display(
                    name,
                    service_emoji,
                    service_custom_emoji_id=service_custom_emoji_id,
                    webhook_override=True,
                    conn=conn,
                    html=True,
                ),
                "service_name_visible": format_service_visible(
                    name,
                    effective_service_emoji,
                    webhook_override=True,
                    conn=conn,
                ),
                "service_icon_html": format_service_icon_only(
                    name,
                    effective_service_emoji,
                    service_custom_emoji_id=effective_service_custom_emoji_id,
                    webhook_override=True,
                    conn=conn,
                    html=True,
                ),
                "service_name_text": name,
                "service_emoji": effective_service_emoji,
                "service_custom_emoji_id": effective_service_custom_emoji_id,
                "country": country_label,
                "country_flag": country_flag,
                "country_custom_emoji_id": country_custom_emoji_id,
                "country_html": format_display_country(
                    country_label,
                    html=True,
                    custom_emoji_id=country_custom_emoji_id,
                    flag_text=country_flag,
                ),
                "country_visible": format_display_country(
                    country_label,
                    html=False,
                    custom_emoji_id=country_custom_emoji_id,
                    flag_text=country_flag,
                ),
                "country_icon_html": format_country_icon(
                    country_flag,
                    country_custom_emoji_id,
                    html=True,
                ),
                "country_code": country_code,
            }
    except Exception as e:
        logger.error(f"Error getting service: {str(e)}")
        return None


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
    service_name_text = service_name_override or ""
    service_emoji = ""
    service_custom_emoji_id = ""
    group_service_icon = ""
    group_service_custom_emoji_id = ""
    country = country_override or ""
    country_icon_html = ""
    country_flag = ""
    country_custom_emoji_id = ""
    if user_info:
        user_id = user_info['user_id']
        service_id = user_info['service_id']
        number_id = user_info['number_id']
        meta = get_service_meta(service_id)
        db_country_code = (meta or {}).get("country_code", "")
        if not service_name:
            service_name = (meta or {}).get("service_name_html", "") or ""
        if not service_name_text:
            service_name_text = (meta or {}).get("service_name_text", "") or ""
        if not service_emoji:
            service_emoji = (meta or {}).get("service_emoji", "") or ""
        if not service_custom_emoji_id:
            service_custom_emoji_id = (meta or {}).get("service_custom_emoji_id", "") or ""
        if not group_service_icon:
            group_service_icon = (meta or {}).get("service_emoji", "") or ""
        if not group_service_custom_emoji_id:
            group_service_custom_emoji_id = (meta or {}).get("service_custom_emoji_id", "") or ""
        if not country:
            country = (meta or {}).get("country", "") or ""
        if not country_flag:
            country_flag = (meta or {}).get("country_flag", "") or ""
        if not country_custom_emoji_id:
            country_custom_emoji_id = (meta or {}).get("country_custom_emoji_id", "") or ""
        if not country_icon_html:
            country_icon_html = (meta or {}).get("country_icon_html", "") or ""
    else:
        db_country_code = ""
    if service_name_override:
        override_text, override_custom_id = get_service_emoji_override_data(service_name_override)
        service_name_text = service_name_override
        service_emoji = override_text
        service_custom_emoji_id = override_custom_id
        service_name = format_service_display(service_name_override, webhook_override=True, html=True)
        group_service_icon = override_text
        group_service_custom_emoji_id = override_custom_id
    if country_override:
        country_icon_html = format_display_country(country_override, html=True)
    if not service_name:
        service_name = "Unknown"
    if not service_name_text:
        service_name_text = strip_display_flag(service_name) or "Unknown"
    if not country:
        country = ""
    if not country_icon_html:
        country_icon_html = format_display_country(
            country,
            html=True,
            custom_emoji_id=country_custom_emoji_id,
            flag_text=country_flag,
        )

    if not user_id:
        logger.info("No assigned user for %s; group forward only", phone_number)

    for otp in otps:
        # Group forwards always run for valid webhook OTPs, independent from user assignment.
        _enqueue_otp_to_groups(
            phone_number,
            otp,
            service_name,
            country,
            db_country_code,
            country_flag=country_flag,
            country_custom_emoji_id=country_custom_emoji_id,
            service_icon=group_service_icon,
            service_custom_emoji_id=group_service_custom_emoji_id,
        )

        # User forward runs only when this number is currently assigned to a user.
        if user_id:
            success = forward_otp_to_user(
                user_id,
                phone_number,
                otp,
                service_name_text,
                service_emoji,
                service_custom_emoji_id,
                country_flag,
                country_custom_emoji_id,
                number_id,
            )
            if success and number_id:
                try:
                    with get_db_connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE numbers SET received_otp = COALESCE(received_otp, 0) + 1 WHERE id = ?",
                            (number_id,),
                        )
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
        try:
            payload = await request.json()
        except json.JSONDecodeError as e:
            raw_body = await request.body()
            body_preview = raw_body[:200].decode("utf-8", errors="replace")
            logger.warning(
                "Invalid webhook JSON at line %s col %s: %s | body=%r",
                getattr(e, "lineno", "?"),
                getattr(e, "colno", "?"),
                e.msg,
                body_preview,
            )
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

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


def forward_otp_to_user(
    user_id: int,
    phone_number: str,
    otp_code: str,
    service_name_text: str,
    service_emoji: str = "",
    service_custom_emoji_id: str = "",
    country_flag: str = "",
    country_custom_emoji_id: str = "",
    number_id: int = None,
):
    """Forward OTP to user"""
    try:
        from core import BOT_TOKEN

        message, entities = _build_otp_text(
            phone_number,
            service_name_text,
            service_emoji=service_emoji,
            service_custom_emoji_id=service_custom_emoji_id,
            country_flag=country_flag,
            country_custom_emoji_id=country_custom_emoji_id,
        )

        reply_markup = {
            "inline_keyboard": [
                [{"text": otp_code, "copy_text": {"text": otp_code}}]
            ]
        }
        reply_markup = _sanitize_reply_markup(reply_markup)
        entities = _normalize_entities(entities)

        # User OTP must go through main bot token so users receive in main bot chat.
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": user_id,
            "text": message,
            "entities": entities,
            "reply_markup": reply_markup
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
