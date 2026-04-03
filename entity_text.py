"""Helpers for building Telegram message text with UTF-16-based entities."""

from __future__ import annotations

from telebot import types


def utf16_len(text: str) -> int:
    return len((text or "").encode("utf-16-le")) // 2


class EntityTextBuilder:
    def __init__(self) -> None:
        self.text = ""
        self.entities = []

    def append(self, value: str) -> None:
        self.text += str(value or "")

    def _offset(self) -> int:
        return utf16_len(self.text)

    def append_bold(self, value: str) -> None:
        value = str(value or "")
        if not value:
            return
        offset = self._offset()
        self.text += value
        self.entities.append(
            types.MessageEntity(
                type="bold",
                offset=offset,
                length=utf16_len(value),
            )
        )

    def append_code(self, value: str) -> None:
        value = str(value or "")
        if not value:
            return
        offset = self._offset()
        self.text += value
        self.entities.append(
            types.MessageEntity(
                type="code",
                offset=offset,
                length=utf16_len(value),
            )
        )

    def append_italic(self, value: str) -> None:
        value = str(value or "")
        if not value:
            return
        offset = self._offset()
        self.text += value
        self.entities.append(
            types.MessageEntity(
                type="italic",
                offset=offset,
                length=utf16_len(value),
            )
        )

    def append_text_link(self, value: str, url: str) -> None:
        value = str(value or "")
        url = str(url or "").strip()
        if not value:
            return
        offset = self._offset()
        self.text += value
        self.entities.append(
            types.MessageEntity(
                type="text_link",
                offset=offset,
                length=utf16_len(value),
                url=url or None,
            )
        )

    def append_custom_emoji(self, fallback_text: str, custom_emoji_id: str) -> bool:
        fallback_text = str(fallback_text or "").strip()
        custom_emoji_id = str(custom_emoji_id or "").strip()
        if not fallback_text or not custom_emoji_id:
            return False
        offset = self._offset()
        self.text += fallback_text
        self.entities.append(
            types.MessageEntity(
                type="custom_emoji",
                offset=offset,
                length=utf16_len(fallback_text),
                custom_emoji_id=custom_emoji_id,
            )
        )
        return True
