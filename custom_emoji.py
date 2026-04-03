"""Helpers for storing and rendering Telegram custom emoji IDs in text fields."""

from __future__ import annotations

import re


TOKEN_RE = re.compile(r"\[\[ce:(\d{10,40})\]\]")
RAW_ID_RE = re.compile(r"(?<!\d)(\d{10,40})(?!\d)")
BROKEN_NESTED_TOKEN_RE = re.compile(r"\[\[ce:\[\[ce:(\d{10,40})\]\]\]\]")


def encode_custom_emoji_id(custom_emoji_id: str) -> str:
    custom_emoji_id = str(custom_emoji_id or "").strip()
    return f"[[ce:{custom_emoji_id}]]" if custom_emoji_id else ""


def normalize_custom_emoji_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    # Do not re-encode IDs that are already wrapped as custom emoji tokens.
    protected = {}

    def _protect(match):
        key = f"__CE_TOKEN_{len(protected)}__"
        protected[key] = match.group(0)
        return key

    raw = TOKEN_RE.sub(_protect, raw)
    raw = RAW_ID_RE.sub(lambda m: encode_custom_emoji_id(m.group(1)), raw)
    for key, value in protected.items():
        raw = raw.replace(key, value)
    return raw


def contains_custom_emoji_token(text: str) -> bool:
    return bool(TOKEN_RE.search(str(text or "")))


def render_custom_emoji_text(text: str, html: bool = False, plain_fallback: str = "") -> str:
    raw = str(text or "")
    if not raw:
        return ""

    def _replace(match):
        custom_emoji_id = match.group(1)
        if html:
            return f'<tg-emoji emoji-id="{custom_emoji_id}"></tg-emoji>'
        return plain_fallback

    rendered = TOKEN_RE.sub(_replace, raw)
    rendered = re.sub(r"\s+", " ", rendered).strip()
    return rendered


def cleanup_broken_custom_emoji_text(text: str) -> str:
    raw = str(text or "")
    if not raw:
        return ""
    prev = None
    while prev != raw:
        prev = raw
        raw = BROKEN_NESTED_TOKEN_RE.sub(lambda m: encode_custom_emoji_id(m.group(1)), raw)
    return raw


def render_button_custom_emoji_text(text: str, fallback: str = "•") -> str:
    raw = normalize_custom_emoji_text(cleanup_broken_custom_emoji_text(text))
    if not raw:
        return ""

    def _replace(match):
        custom_emoji_id = match.group(1)
        return f"<emoji id=\"{custom_emoji_id}\">{fallback}</emoji>"

    rendered = TOKEN_RE.sub(_replace, raw)
    rendered = re.sub(r"\s+", " ", rendered).strip()
    return rendered
