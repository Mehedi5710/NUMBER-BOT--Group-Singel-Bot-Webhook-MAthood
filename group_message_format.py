"""Group OTP message formatting helpers.

Edit this file to change only the group-delivery message format/layout.
"""
import re
import os
from flag import get_flag, resolve_country

BRANDING_NAME = os.getenv("BRANDING_NAME", "Number Bot")
BRANDING_URL = os.getenv("BRANDING_URL", "https://t.me/")
DEV_PREFIX = "永"
STYLE = {
    "otp": "primary",
    "number_bot": "danger",
    "support_group": "success",
}


def _mask_number(raw_number: str) -> str:
    digits = re.sub(r"\D", "", str(raw_number or ""))
    if not digits:
        return ""
    # Old-style mask: keep last 5 digits, split with dot/bullet marker.
    if len(digits) > 5:
        return f"{digits[:-5]}•{digits[-5:]}"
    return digits


def _safe_tag(text: str) -> str:
    tag = re.sub(r"[^A-Za-z0-9_]+", "", str(text or "").strip())
    return tag or "Service"


def build_group_message(
    phone_number: str,
    otp_code: str,
    service_name: str,
    country: str,
    number_bot_link: str = "",
    support_group_link: str = "",
):
    """Return (text, reply_markup_dict) for group OTP delivery."""
    flag = get_flag(country) if country else "🌍?"
    masked = _mask_number(phone_number)
    country_obj = resolve_country(country) if country else None
    country_code = (country_obj or {}).get("iso2", "XX")

    hashtags = f"#{country_code}"
    service_tag = _safe_tag(service_name)
    if service_tag:
        hashtags += f" #{service_tag}"

    text = (
        f"{flag} {hashtags}  <code>{masked}</code>\n\n"
        f"{DEV_PREFIX} Developed BY "
        f"<a href='{BRANDING_URL}'><b>{BRANDING_NAME}</b></a> 🎯"
    )

    keyboard = [[{
        "text": f"🔑  {otp_code}",
        "style": "primary",
        "copy_text": {"text": otp_code},
    }]]
    bottom = []
    if number_bot_link:
        bottom.append({
            "text": "🤖 Number Bot",
            "style": "danger",
            "url": number_bot_link,
        })
    if support_group_link:
        bottom.append({
            "text": "👥 Support Group",
            "style": "success",
            "url": support_group_link,
        })
    if bottom:
        keyboard.append(bottom)

    return text, {"inline_keyboard": keyboard}
