"""Group OTP message formatting helpers.

Edit this file to change only the group-delivery message format/layout.
"""
import re
import os
from flag import format_country_icon
from entity_text import EntityTextBuilder

BRANDING_NAME = os.getenv("BRANDING_NAME", "Number Bot")
BRANDING_URL = os.getenv("BRANDING_URL", "https://t.me/")
DEV_PREFIX = "永"
STYLE = {
    "otp": "primary",
    "number_bot": "danger",
    "support_group": "success",
}
OTP_COPY_BUTTON_EMOJI = "🔑"
OTP_COPY_BUTTON_CUSTOM_EMOJI_ID = "5330115548900501467"
NUMBER_BOT_BUTTON_EMOJI = "🤖"
NUMBER_BOT_BUTTON_CUSTOM_EMOJI_ID = "6222008745350668274"
SUPPORT_GROUP_BUTTON_EMOJI = "📢"
SUPPORT_GROUP_BUTTON_CUSTOM_EMOJI_ID = "6221877169027554461"
MASK_TOKEN = "MAX"


def _mask_number(raw_number: str) -> str:
    digits = re.sub(r"\D", "", str(raw_number or ""))
    if not digits:
        return ""
    # Group format mask: keep first 7 and last 3 digits with a configurable token.
    if len(digits) > 10:
        return f"{digits[:7]}{MASK_TOKEN}{digits[-3:]}"
    if len(digits) > 5:
        return f"{digits[:-5]}{MASK_TOKEN}{digits[-3:]}"
    return digits


def _safe_tag(text: str) -> str:
    tag = re.sub(r"[^A-Za-z0-9_]+", "", str(text or "").strip())
    return tag or "Service"


def build_group_message(
    phone_number: str,
    otp_code: str,
    service_name: str,
    country: str,
    country_code: str = "",
    country_flag: str = "",
    country_custom_emoji_id: str = "",
    service_icon: str = "",
    service_custom_emoji_id: str = "",
    number_bot_link: str = "",
    support_group_link: str = "",
):
    """Return (text, entities, reply_markup_dict) for group OTP delivery."""
    flag_text = str(country_flag or "").strip()
    masked = _mask_number(phone_number)
    service_icon = str(service_icon or "").strip()
    service_custom_emoji_id = str(service_custom_emoji_id or "").strip()
    short_code = str(country_code or "").strip().upper()

    builder = EntityTextBuilder()
    country_icon_added = False
    if country_custom_emoji_id and flag_text:
        country_icon_added = builder.append_custom_emoji(flag_text, country_custom_emoji_id)
    elif flag_text:
        builder.append(flag_text)
        country_icon_added = True
    if short_code:
        if country_icon_added:
            builder.append(" ")
        builder.append_bold(f"#{short_code}")
    if service_icon or service_custom_emoji_id:
        builder.append(" • ")
        if service_custom_emoji_id and service_icon:
            builder.append_custom_emoji(service_icon, service_custom_emoji_id)
        elif service_icon:
            builder.append(service_icon)
        else:
            # Strict mode: no fallback when only a custom emoji ID exists.
            builder.text = builder.text[:-3] if builder.text.endswith(" • ") else builder.text
    else:
        service_tag = _safe_tag(service_name)
        if service_tag:
            builder.append(" ")
            builder.append_bold(f"#{service_tag}")
    builder.append(" • ")
    builder.append_code(masked)
    keyboard = [[{
        "text": otp_code,
        "style": "primary",
        "copy_text": {"text": otp_code},
        "icon_custom_emoji_id": OTP_COPY_BUTTON_CUSTOM_EMOJI_ID,
    }]]
    bottom = []
    if number_bot_link:
        bottom.append({
            "text": "Nmbr Bot",
            "style": "danger",
            "icon_custom_emoji_id": NUMBER_BOT_BUTTON_CUSTOM_EMOJI_ID,
            "url": number_bot_link,
        })
    if support_group_link:
        bottom.append({
            "text": "Sprt Grup",
            "style": "success",
            "icon_custom_emoji_id": SUPPORT_GROUP_BUTTON_CUSTOM_EMOJI_ID,
            "url": support_group_link,
        })
    if bottom:
        keyboard.append(bottom)

    return builder.text, builder.entities, {"inline_keyboard": keyboard}
