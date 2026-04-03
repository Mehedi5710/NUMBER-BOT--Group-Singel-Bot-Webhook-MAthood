"""Styled assignment message sender for Telegram Bot API."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import requests


def _api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def _normalize_entities(entities):
    if entities is None:
        return None
    normalized = []
    for entity in entities:
        if hasattr(entity, "to_dict"):
            normalized.append(entity.to_dict())
        else:
            normalized.append(entity)
    return normalized


def _post(token: str, method: str, payload: dict) -> Tuple[bool, str]:
    try:
        resp = requests.post(_api_url(token, method), json=payload, timeout=12)
        if resp.status_code != 200:
            return False, f"http {resp.status_code}: {resp.text[:300]}"
        data = resp.json()
        if not data.get("ok"):
            return False, str(data)
        return True, ""
    except Exception as e:
        return False, str(e)


def _normalize_reply_markup(reply_markup):
    if reply_markup is None:
        return None
    if hasattr(reply_markup, "to_dict"):
        return reply_markup.to_dict()
    return reply_markup


def build_number_rows(flag: str, numbers: List[str], statuses: Dict[str, str], custom_emoji_id: str = "") -> List[List[dict]]:
    rows: List[List[dict]] = []
    custom_emoji_id = str(custom_emoji_id or "").strip()
    for display_num in numbers:
        button = {
            "text": display_num if custom_emoji_id else f"{flag} {display_num}",
            "copy_text": {"text": display_num},
        }
        if custom_emoji_id:
            button["icon_custom_emoji_id"] = custom_emoji_id
        rows.append([button])
    return rows


def build_action_rows(
    change_cb: str,
    country_cb: str,
    otp_group_url: str = "",
) -> List[List[dict]]:
    rows: List[List[dict]] = [
        [{"text": "🔄 Change Numbers", "callback_data": change_cb}],
        [{"text": "🌍 Change Country", "callback_data": country_cb}],
    ]
    if otp_group_url:
        rows.append([{"text": "📢 OTP Group", "url": otp_group_url}])
    return rows


def edit_message(
    token: str,
    chat_id: int,
    message_id: int,
    text: str,
    inline_keyboard: List[List[dict]],
    parse_mode: str = "HTML",
    entities: Optional[List[dict]] = None,
) -> Tuple[bool, str]:
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "reply_markup": {"inline_keyboard": inline_keyboard},
    }
    if entities is not None:
        payload["entities"] = _normalize_entities(entities)
    else:
        payload["parse_mode"] = parse_mode
    return _post(token, "editMessageText", payload)


def send_message(
    token: str,
    chat_id: int,
    text: str,
    inline_keyboard: List[List[dict]],
    parse_mode: str = "HTML",
    entities: Optional[List[dict]] = None,
) -> Tuple[bool, str, Optional[int]]:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {"inline_keyboard": inline_keyboard},
    }
    if entities is not None:
        payload["entities"] = _normalize_entities(entities)
    else:
        payload["parse_mode"] = parse_mode
    try:
        resp = requests.post(_api_url(token, "sendMessage"), json=payload, timeout=12)
        if resp.status_code != 200:
            return False, f"http {resp.status_code}: {resp.text[:300]}", None
        data = resp.json()
        if not data.get("ok"):
            return False, str(data), None
        result = data.get("result") or {}
        return True, "", result.get("message_id")
    except Exception as e:
        return False, str(e), None


def edit_message_with_markup(
    token: str,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup,
    parse_mode: str = "HTML",
    entities: Optional[List[dict]] = None,
) -> Tuple[bool, str]:
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    normalized_markup = _normalize_reply_markup(reply_markup)
    if normalized_markup is not None:
        payload["reply_markup"] = normalized_markup
    if entities is not None:
        payload["entities"] = _normalize_entities(entities)
    else:
        payload["parse_mode"] = parse_mode
    return _post(token, "editMessageText", payload)


def send_message_with_markup(
    token: str,
    chat_id: int,
    text: str,
    reply_markup,
    parse_mode: str = "HTML",
    entities: Optional[List[dict]] = None,
) -> Tuple[bool, str, Optional[int]]:
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    normalized_markup = _normalize_reply_markup(reply_markup)
    if normalized_markup is not None:
        payload["reply_markup"] = normalized_markup
    if entities is not None:
        payload["entities"] = _normalize_entities(entities)
    else:
        payload["parse_mode"] = parse_mode
    try:
        resp = requests.post(_api_url(token, "sendMessage"), json=payload, timeout=12)
        if resp.status_code != 200:
            return False, f"http {resp.status_code}: {resp.text[:300]}", None
        data = resp.json()
        if not data.get("ok"):
            return False, str(data), None
        result = data.get("result") or {}
        return True, "", result.get("message_id")
    except Exception as e:
        return False, str(e), None
