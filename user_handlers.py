"""User command and message handlers"""
import time
from telebot import types
from entity_text import EntityTextBuilder
from core import get_assignment_mode, mark_numbers_reserved, release_numbers_for_user
from core import format_service_display, format_service_visible, get_service_button_emoji, get_service_button_icon_data
from assignment_styled_ui import (
    build_action_rows as styled_build_action_rows,
    build_number_rows as styled_build_number_rows,
    edit_message as styled_edit_message,
    edit_message_with_markup as styled_edit_message_with_markup,
    send_message as styled_send_message,
    send_message_with_markup as styled_send_message_with_markup,
)


def register_handlers(bot, get_db_connection, logger):
    subscribed_cache = {}
    broken_targets_alerted = set()
    active_assignment_messages = {}
    BTN_GET_NUMBER = "📱 Get Number"
    BTN_SEARCH_NUMBER = "🔎 Search Number"
    BTN_BACK = "⬅️ Back"
    BTN_CANCEL = "❌ Cancel"

    def user_text_is(text, base_label):
        raw = (text or "").strip().lower()
        base = base_label.strip().lower()
        if raw == base:
            return True
        # Accept emoji-prefixed labels without breaking old plain labels.
        cleaned = __import__('re').sub(r'[^a-z ]', '', raw).strip()
        return cleaned == base.lower()

    def build_main_keyboard():
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
        return keyboard

    def build_prompt_keyboard():
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        keyboard.add(BTN_BACK, BTN_CANCEL)
        return keyboard

    def forget_assignment_message(user_id, message_id=None):
        current = active_assignment_messages.get(user_id)
        if current is None:
            return
        if message_id is None or current == message_id:
            active_assignment_messages.pop(user_id, None)

    def remember_assignment_message(user_id, message_id):
        if message_id:
            active_assignment_messages[user_id] = int(message_id)

    def delete_previous_assignment_message(chat_id, user_id, keep_message_id=None):
        prev_id = active_assignment_messages.get(user_id)
        if not prev_id or (keep_message_id and int(prev_id) == int(keep_message_id)):
            return
        try:
            bot.delete_message(chat_id, prev_id)
        except Exception:
            pass
        finally:
            active_assignment_messages.pop(user_id, None)

    def format_user_identity(user_id, username=None, first_name=None, last_name=None):
        u = username
        fn = first_name
        ln = last_name
        if not (u or fn or ln):
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT username, first_name, last_name FROM users WHERE user_id = ?",
                        (user_id,)
                    )
                    row = cur.fetchone()
                if row:
                    u, fn, ln = row
            except Exception:
                pass
        uname = f"@{u}" if u else "no_username"
        full_name = " ".join([x for x in [fn, ln] if x]).strip() or "no_name"
        return uname, full_name

    def notify_broken_target(channel_id, name, err_text):
        key = f"{channel_id}:{err_text}"
        if key in broken_targets_alerted:
            return
        broken_targets_alerted.add(key)
        logger.warning(f"Subscription target issue {channel_id} ({name}): {err_text}")
        try:
            from core import ADMIN_IDS
            for admin_id in ADMIN_IDS:
                try:
                    bot.send_message(
                        admin_id,
                        f"Subscription target issue detected:\n"
                        f"Name: {name}\n"
                        f"Target: {channel_id}\n"
                        f"Error: {err_text}"
                    )
                except Exception:
                    pass
        except Exception:
            pass

    def build_join_url(invite_link, channel_id):
        link = (invite_link or "").strip()
        if not link:
            return None
        if link.startswith("http://") or link.startswith("https://"):
            return link.replace("http://", "https://", 1)
        if link.startswith("t.me/"):
            return f"https://{link}"
        if link.startswith("@"):
            return f"https://t.me/{link.replace('@', '').strip()}"
        return None

    def get_channels_version():
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT value FROM bot_config WHERE key='channels_version'"
                )
                row = cur.fetchone()
            return int(row[0]) if row and row[0].isdigit() else 0
        except Exception as e:
            logger.error(f"Error reading channels version: {e}")
            return 0

    def get_subscription_recheck_seconds():
        """How often to re-check subscription for already-subscribed users.
        0 means check every time.
        """
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT value FROM bot_config WHERE key='subscription_recheck_hours'"
                )
                row = cur.fetchone()
            hours = float(row[0]) if row and row[0] is not None else 0.0
            if hours <= 0:
                return 0
            return int(hours * 3600)
        except Exception:
            return 0

    def is_recently_verified(user_id, current_version):
        cached = subscribed_cache.get(user_id)
        if not cached:
            return False
        if not isinstance(cached, tuple) or len(cached) < 3:
            return False
        cached_version, is_ok, checked_at = cached
        if not is_ok or cached_version != current_version:
            return False
        recheck_seconds = get_subscription_recheck_seconds()
        if recheck_seconds <= 0:
            return False
        return (time.time() - float(checked_at)) < recheck_seconds

    def get_unjoined_channels(user_id):
        """Returns list of (name, channel_identifier, invite_link) for channels user hasn't joined"""
        current_version = get_channels_version()
        if is_recently_verified(user_id, current_version):
            return []
        unjoined = []
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT name, channel_identifier, invite_link FROM channels ORDER BY id"
                )
                channels = cur.fetchall()
                
                for name, channel_id, invite_link in channels:
                    try:
                        member = bot.get_chat_member(channel_id, user_id)
                        if member.status not in ['creator', 'administrator', 'member']:
                            link_to_show = invite_link if invite_link else channel_id
                            unjoined.append((name, channel_id, link_to_show))
                    except Exception as e:
                        notify_broken_target(channel_id, name, str(e))
                        # Skip invalid targets so one broken entry does not block all users.
                        continue
        except Exception as e:
            logger.error(f"Error getting channels: {e}")
        
        return unjoined

    def is_subscribed(user_id):
        """Check if user is subscribed to ALL configured channels"""
        current_version = get_channels_version()
        if is_recently_verified(user_id, current_version):
            return True
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name, channel_identifier FROM channels")
                channels = cur.fetchall()
                
                if not channels:
                    return True
                
                checked_targets = 0
                for (name, channel) in channels:
                    try:
                        member = bot.get_chat_member(channel, user_id)
                        checked_targets += 1
                        if member.status not in ['creator', 'administrator', 'member']:
                            return False
                    except Exception as e:
                        notify_broken_target(channel, name, str(e))
                        # Skip invalid targets and continue checking valid ones.
                        continue

                # If all configured targets are currently invalid/unreachable, do not block users.
                if checked_targets == 0:
                    return True
                
                subscribed_cache[user_id] = (current_version, True, time.time())
                return True
        except Exception as e:
            logger.error(f"Error checking subscription for user {user_id}: {e}")
            return False

    def is_banned(user_id):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT reason FROM banned_users WHERE user_id=?", (user_id,))
            banned = cur.fetchone()
            return banned is not None

    def get_country_flag(country_name, sample_numbers=None):
        from flag import get_flag
        return get_flag(country_name)

    def format_country_display(country_name, html=False, custom_emoji_id="", flag_text=""):
        from flag import format_display_country
        return format_display_country(country_name, html=html, custom_emoji_id=custom_emoji_id, flag_text=flag_text)

    def format_country_visible_label(country_name, flag_text="", custom_emoji_id=""):
        from flag import format_display_country_visible
        return format_display_country_visible(country_name, flag_text=flag_text, custom_emoji_id=custom_emoji_id)

    def format_service_label(service_name, service_emoji=None, service_custom_emoji_id=None, html=False):
        return str(service_name or "").strip() or "Unknown"

    def format_service_visible_label(service_name, service_emoji=None):
        service_name = str(service_name or "").strip() or "Unknown"
        button_emoji = str(service_emoji or "").strip() or get_service_button_emoji(service_name)
        return f"{button_emoji} {service_name}".strip() if button_emoji else service_name

    def build_assignment_header(country_display="", service_display=""):
        parts = [str(country_display or "").strip(), str(service_display or "").strip()]
        parts = [part for part in parts if part]
        return " ".join(parts).strip()

    def build_assignment_message_entities(
        country_label="",
        country_flag="",
        country_custom_emoji_id="",
        service_name="",
        service_emoji="",
        service_custom_emoji_id="",
        display_num="",
        include_title=False,
        waiting=False,
    ):
        builder = EntityTextBuilder()
        if include_title:
            builder.append_bold("Number Assigned!")
            builder.append("\n\n")
        country_label = str(country_label or "").strip()
        country_flag = str(country_flag or "").strip()
        country_custom_emoji_id = str(country_custom_emoji_id or "").strip()
        service_name = str(service_name or "").strip()
        if country_custom_emoji_id and country_flag:
            builder.append_custom_emoji(country_flag, country_custom_emoji_id)
        elif country_flag:
            builder.append(country_flag)
        if country_label:
            if builder.text and not builder.text.endswith((" ", "\n")):
                builder.append(" ")
            builder.append(country_label)
        service_emoji = str(service_emoji or "").strip()
        service_custom_emoji_id = str(service_custom_emoji_id or "").strip()
        if service_name and (not service_emoji and not service_custom_emoji_id):
            service_emoji, service_custom_emoji_id = get_service_button_icon_data(service_name)
        if service_custom_emoji_id and service_emoji:
            if builder.text and not builder.text.endswith((" ", "\n")):
                builder.append(" ")
            builder.append_custom_emoji(service_emoji, service_custom_emoji_id)
        elif service_emoji:
            if builder.text and not builder.text.endswith((" ", "\n")):
                builder.append(" ")
            builder.append(service_emoji)
        if service_name:
            if builder.text and not builder.text.endswith((" ", "\n")):
                builder.append(" ")
            builder.append(service_name)
        if display_num:
            builder.append("\n")
            builder.append_code(display_num)
        if waiting:
            builder.append("\n\n")
            builder.append_italic("Waiting for OTP...")
        return builder.text, builder.entities

    def build_select_country_title(service_name="", service_emoji="", service_custom_emoji_id=""):
        builder = EntityTextBuilder()
        builder.append("🌍 Select Country for ")
        service_name = str(service_name or "").strip() or "Unknown"
        service_emoji = str(service_emoji or "").strip()
        service_custom_emoji_id = str(service_custom_emoji_id or "").strip()
        if service_name and (not service_emoji and not service_custom_emoji_id):
            service_emoji, service_custom_emoji_id = get_service_button_icon_data(service_name)
        if service_custom_emoji_id and service_emoji:
            builder.append_custom_emoji(service_emoji, service_custom_emoji_id)
            builder.append(" ")
        elif service_emoji:
            builder.append(service_emoji)
            builder.append(" ")
        builder.append(service_name)
        builder.append(":")
        return builder.text, builder.entities

    def build_service_button_label(service_name, button_emoji=""):
        service_name = str(service_name or "").strip() or "Unknown"
        button_emoji = str(button_emoji or "").strip()
        if button_emoji and service_name.startswith(button_emoji):
            service_name = service_name[len(button_emoji):].strip() or service_name
        return f"{button_emoji} {service_name}".strip() if button_emoji else service_name

    def build_service_inline_button(service_name, callback_data, button_emoji="", custom_emoji_id="", suffix="", style=None):
        label_name = str(service_name or "").strip() or "Unknown"
        button_emoji = str(button_emoji or "").strip()
        custom_emoji_id = str(custom_emoji_id or "").strip()
        if custom_emoji_id:
            text = f"{label_name}{suffix}"
            kwargs = {"icon_custom_emoji_id": custom_emoji_id}
            if style:
                kwargs["style"] = style
            return types.InlineKeyboardButton(text, callback_data=callback_data, **kwargs)
        text = f"{build_service_button_label(label_name, button_emoji)}{suffix}"
        kwargs = {}
        if style:
            kwargs["style"] = style
        return types.InlineKeyboardButton(text, callback_data=callback_data, **kwargs)

    def build_country_inline_button(country_label, callback_data, flag_text="", custom_emoji_id="", suffix="", style=None):
        label_name = str(country_label or "").strip() or "Unknown"
        flag_text = str(flag_text or "").strip()
        custom_emoji_id = str(custom_emoji_id or "").strip()
        if flag_text and label_name.startswith(flag_text):
            label_name = label_name[len(flag_text):].strip() or label_name
        if custom_emoji_id:
            text = f"{label_name}{suffix}"
            kwargs = {"icon_custom_emoji_id": custom_emoji_id}
            if style:
                kwargs["style"] = style
            return types.InlineKeyboardButton(text, callback_data=callback_data, **kwargs)
        text = f"{(flag_text + ' ' + label_name).strip() if flag_text else label_name}{suffix}"
        kwargs = {}
        if style:
            kwargs["style"] = style
        return types.InlineKeyboardButton(text, callback_data=callback_data, **kwargs)

    def release_user_numbers(conn, user_id, sms_limit, service_id=None):
        try:
            return release_numbers_for_user(conn, user_id, sms_limit=sms_limit, service_id=service_id)
        except Exception as e:
            logger.error(f"Failed to release numbers for user {user_id}: {e}")
            return {"deleted": 0, "released": 0}

    def assignment_order_sql():
        return "COALESCE(queue_pos, id), id" if get_assignment_mode() == "serial" else "RANDOM()"
    
    def create_number_markup(service_id, service_name):
        """Create standard inline keyboard for number assignment"""
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔄 Change Numbers", callback_data=f"srv_{service_id}"))
        markup.add(types.InlineKeyboardButton("🌍 Change Country", callback_data=f"change_country_{service_name}"))
        from core import OTP_GROUP_URL
        if OTP_GROUP_URL:
            markup.add(types.InlineKeyboardButton("📢 OTP Group", url=OTP_GROUP_URL))
        try:
            with get_db_connection() as conn:
                nb = conn.execute(
                    "SELECT value FROM bot_config WHERE key='forwarder_number_bot_link'"
                ).fetchone()
                sg = conn.execute(
                    "SELECT value FROM bot_config WHERE key='forwarder_support_group_link'"
                ).fetchone()
            number_bot_link = (nb[0] if nb and nb[0] else "").strip()
            support_group_link = (sg[0] if sg and sg[0] else "").strip()
            bottom = []
            if number_bot_link:
                bottom.append(types.InlineKeyboardButton("🤖 Number Bot", url=number_bot_link))
            if support_group_link:
                bottom.append(types.InlineKeyboardButton("💬 Support Group", url=support_group_link))
            if bottom:
                markup.row(*bottom)
        except Exception:
            pass
        return markup

    def show_service_list(chat_id, edit_msg_id=None):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT s.name,
                       MAX(COALESCE(s.button_emoji, '')) as button_emoji,
                       SUM(CASE WHEN n.status='active' AND n.user_id IS NULL THEN 1 ELSE 0 END) as total_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.status = 'active'
                GROUP BY s.name
                HAVING SUM(CASE WHEN n.status='active' AND n.user_id IS NULL THEN 1 ELSE 0 END) > 0
                ORDER BY s.name
            """)
            services = cur.fetchall()

        if not services:
            text = "❌ No services available."
            keyboard = build_main_keyboard()
            if edit_msg_id:
                try:
                    bot.delete_message(chat_id, edit_msg_id)
                except:
                    pass
            bot.send_message(chat_id, text, reply_markup=keyboard)
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for service_name, button_emoji, total_count in services:
            button_emoji, button_custom_emoji_id = get_service_button_icon_data(service_name)
            markup.add(
                build_service_inline_button(
                    service_name,
                    callback_data=f"service_select_{service_name}",
                    button_emoji=button_emoji,
                    custom_emoji_id=button_custom_emoji_id,
                    suffix=f" ({total_count})",
                )
            )

        text = " <b>Select a Service:</b>"
        
        if edit_msg_id:
            try:
                bot.edit_message_text(text, chat_id, edit_msg_id, reply_markup=markup, parse_mode='HTML')
            except Exception:
                try:
                    bot.delete_message(chat_id, edit_msg_id)
                except:
                    pass
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')

    def show_countries_for_service(chat_id, service_name, edit_msg_id=None):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT s.country, COALESCE(s.country_flag, ''), COALESCE(s.country_custom_emoji_id, ''), COALESCE(s.country_code, ''),
                       COALESCE(s.country_display_name, ''), s.id, COUNT(n.id) as count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id AND n.status='active' AND n.user_id IS NULL
                WHERE s.name = ? AND s.status = 'active'
                GROUP BY s.country, s.country_flag, s.country_custom_emoji_id, s.country_code, s.country_display_name, s.id
                HAVING COUNT(n.id) > 0
                ORDER BY s.country
            """, (service_name,))
            countries = cur.fetchall()
        service_display = format_service_label(service_name, html=True)

        if not countries:
            text = f" No countries available for {service_display}."
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Back to Services", callback_data="get_number"))
            if edit_msg_id:
                try:
                    bot.edit_message_text(text, chat_id, edit_msg_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            else:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for country, country_flag, country_custom_emoji_id, country_code, country_display_name, service_id, count in countries:
            country_label = country_display_name or country
            if country_code:
                country_label = f"{country_label} [{country_code}]"
            markup.add(
                build_country_inline_button(
                    country_label,
                    callback_data=f"srv_{service_id}",
                    flag_text=country_flag,
                    custom_emoji_id=country_custom_emoji_id,
                    suffix=f" ({count})",
                )
            )

        markup.add(types.InlineKeyboardButton("⬅️ Back to Services", callback_data="get_number"))
        text, title_entities = build_select_country_title(service_name, "", "")
        from core import BOT_TOKEN
        
        if edit_msg_id:
            ok, _err = styled_edit_message_with_markup(
                BOT_TOKEN,
                chat_id,
                edit_msg_id,
                text,
                markup,
                entities=title_entities,
            )
            if not ok:
                try:
                    bot.delete_message(chat_id, edit_msg_id)
                except:
                    pass
                styled_send_message_with_markup(
                    BOT_TOKEN,
                    chat_id,
                    text,
                    markup,
                    entities=title_entities,
                )
        else:
            styled_send_message_with_markup(
                BOT_TOKEN,
                chat_id,
                text,
                markup,
                entities=title_entities,
            )

    @bot.message_handler(commands=['start'])
    def send_welcome(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        if message.chat.type != 'private':
            return

        with get_db_connection() as conn:
            existing_user = conn.execute(
                "SELECT 1 FROM users WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, last_active)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    last_active = CURRENT_TIMESTAMP
                """,
                (user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name),
            )
            conn.commit()
        
        if is_banned(user_id):
            bot.send_message(chat_id, "⛔ <b>Access Denied!</b>\n\nYou have been banned from using this bot.")
            return
        
        with get_db_connection() as conn:
            sms_limit = 1
            try:
                row = conn.execute("SELECT value FROM bot_config WHERE key='sms_limit'").fetchone()
                if row and row[0] is not None:
                    sms_limit = int(row[0])
            except Exception:
                sms_limit = 1

            release_user_numbers(conn, user_id, sms_limit=sms_limit, service_id=None)
            conn.commit()
        
        if not is_subscribed(user_id):
            unjoined = get_unjoined_channels(user_id)
            
            if unjoined:
                markup = types.InlineKeyboardMarkup()
                for (name, channel_id, invite_link) in unjoined:
                    button_url = build_join_url(invite_link, channel_id)
                    if button_url:
                        markup.add(types.InlineKeyboardButton(f" {name}", url=button_url))
                markup.add(types.InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
                
                with get_db_connection() as conn:
                    joined_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] - len(unjoined)
                    total_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
                
                bot.send_message(chat_id, 
                    f"<b>Access Denied!</b>\n\nYou are in {joined_count}/{total_count} channels.\n\nPlease join the remaining channel(s):",
                    reply_markup=markup, parse_mode='HTML')
            else:
                bot.send_message(chat_id, 
                    " <b>Bot Permission Issue!</b>\n\nMake sure the bot is added as admin in all channels.",
                    parse_mode='HTML')
        else:
            if not existing_user:
                keyboard = build_main_keyboard()
                bot.send_message(chat_id, "👋 Welcome!", reply_markup=keyboard)
            else:
                keyboard = build_main_keyboard()
                bot.send_message(chat_id, "•", reply_markup=keyboard)
            handle_get_number_button(message)

    @bot.callback_query_handler(func=lambda call: True)
    def handle_query(call):
        data = call.data
        chat_id = call.message.chat.id
        if call.message.chat.type != 'private':
            return
        msg_id = call.message.message_id
        user_id = call.from_user.id

        if is_banned(user_id):
            bot.answer_callback_query(call.id, " You have been banned from using this bot!", show_alert=True)
            return

        if data == "verify_join":
            if is_subscribed(user_id):
                bot.delete_message(chat_id, msg_id)
                keyboard = build_main_keyboard()
                text = "🎉 <b>Welcome! You're all set!</b>\n\nChoose an option:"
                bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode='HTML')
                show_service_list(chat_id)
            else:
                unjoined = get_unjoined_channels(user_id)
                
                if unjoined:
                    markup = types.InlineKeyboardMarkup()
                    for (name, channel_id, invite_link) in unjoined:
                        button_url = build_join_url(invite_link, channel_id)
                        if button_url:
                            markup.add(types.InlineKeyboardButton(f" {name}", url=button_url))
                    markup.add(types.InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
                    
                    with get_db_connection() as conn:
                        joined_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] - len(unjoined)
                        total_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
                    
                    bot.edit_message_text(
                        f"<b>Almost there!</b> \n\nYou joined {joined_count}/{total_count} channels.\n\nPlease join the remaining:",
                        chat_id, msg_id, reply_markup=markup, parse_mode='HTML')
                    bot.answer_callback_query(call.id, f" {joined_count}/{total_count} channels joined!", show_alert=False)
                else:
                    bot.answer_callback_query(call.id, "⚠️ Bot needs admin permissions in channels!", show_alert=True)
            return

        if not is_subscribed(user_id):
            bot.answer_callback_query(call.id, "🔒 Please join channel(s) first!", show_alert=True)
            return

        if data == "back_to_main":
            keyboard = build_main_keyboard()
            bot.send_message(chat_id, "🏠 <b>Welcome! Choose an option:</b>", reply_markup=keyboard, parse_mode='HTML')
            show_service_list(chat_id)
            return

        if data.startswith("search_select_"):
            num_id = int(data.split("_")[2])
            
            with get_db_connection() as conn:
                c = conn.cursor()
                num_data = c.execute("SELECT number, service_id FROM numbers WHERE id = ?", (num_id,)).fetchone()
                
                if num_data:
                    number, service_id = num_data
                    display_num = f"+{number}" if not number.startswith('+') else number
                    
                    mark_numbers_reserved(conn, user_id, [num_id])
                    service_info = c.execute(
                        "SELECT name, country, COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), "
                        "COALESCE(country_display_name, ''), COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, '') "
                        "FROM services WHERE id = ?",
                        (service_id,),
                    ).fetchone()
                    conn.commit()
                    
                    if service_info:
                        service_name, country, country_flag, country_custom_emoji_id, country_display_name, service_emoji, service_custom_emoji_id = service_info
                        country_label = country_display_name or country
                        success_text, success_entities = build_assignment_message_entities(
                            country_label=country_label,
                            country_flag=country_flag,
                            country_custom_emoji_id=country_custom_emoji_id,
                            service_name=service_name,
                            service_emoji=service_emoji,
                            service_custom_emoji_id=service_custom_emoji_id,
                            display_num=display_num,
                            include_title=True,
                            waiting=True,
                        )
                        
                        markup = create_number_markup(service_id, service_name)
                        
                        try:
                            bot.edit_message_text(
                                success_text,
                                chat_id,
                                msg_id,
                                reply_markup=markup,
                                entities=success_entities,
                                parse_mode=None,
                            )
                            delete_previous_assignment_message(chat_id, user_id, keep_message_id=msg_id)
                            remember_assignment_message(user_id, msg_id)
                        except:
                            delete_previous_assignment_message(chat_id, user_id)
                            sent = bot.send_message(
                                chat_id,
                                success_text,
                                reply_markup=markup,
                                entities=success_entities,
                                parse_mode=None,
                            )
                            remember_assignment_message(user_id, sent.message_id)
                        
                        bot.answer_callback_query(call.id, " Number assigned!", show_alert=False)
            return

        with get_db_connection() as conn:
            c = conn.cursor()
            if data == "get_number":
                show_service_list(chat_id, edit_msg_id=msg_id)
            
            elif data.startswith("service_select_"):
                service_name = data.split("service_select_", 1)[1]
                show_countries_for_service(chat_id, service_name, edit_msg_id=msg_id)
                return
            
            elif data.startswith("change_country_"):
                service_name = data.split("change_country_", 1)[1]
                show_countries_for_service(chat_id, service_name, edit_msg_id=msg_id)
                return
            
            elif data == "search_start":
                msg = types.Message.__new__(types.Message)
                msg.from_user = call.from_user
                msg.chat = call.message.chat
                search_number_start(msg)

            elif data.startswith("search_change_"):
                prefix = data.split("search_change_", 1)[1]
                user_id = call.from_user.id
                cooldown_seconds = 7
                try:
                    with get_db_connection() as conn_cd:
                        row = conn_cd.execute("SELECT value FROM bot_config WHERE key='change_number_cooldown_seconds'").fetchone()
                        if row and row[0]:
                            cooldown_seconds = int(row[0])
                except Exception:
                    pass

                now_ts = int(time.time())
                try:
                    with get_db_connection() as conn_cd:
                        c_cd = conn_cd.cursor()
                        row = c_cd.execute(
                            "SELECT last_change_time, warning_count FROM user_cooldowns WHERE user_id = ?",
                            (user_id,)
                        ).fetchone()
                        last_change = row[0] if row and row[0] is not None else None
                        warning_count = row[1] if row and row[1] is not None else 0

                        if last_change and (now_ts - int(last_change)) < cooldown_seconds:
                            remaining = max(1, cooldown_seconds - (now_ts - int(last_change)))
                            if warning_count == 0:
                                c_cd.execute(
                                    "INSERT OR IGNORE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0)",
                                    (user_id, last_change)
                                )
                                c_cd.execute(
                                    "UPDATE user_cooldowns SET warning_count = 1 WHERE user_id = ?",
                                    (user_id,)
                                )
                                conn_cd.commit()
                                bot.answer_callback_query(call.id, f"⏳ Warning: Please wait {remaining} seconds before changing numbers again. Next click will ban you.", show_alert=True)
                                return
                            else:
                                from core import ADMIN_IDS
                                if user_id in ADMIN_IDS:
                                    bot.answer_callback_query(call.id, f"⏳ Please wait {remaining} seconds before changing numbers again.", show_alert=True)
                                    return
                                c_cd.execute(
                                    "INSERT OR IGNORE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 1)",
                                    (user_id, last_change)
                                )
                                c_cd.execute(
                                    "UPDATE user_cooldowns SET warning_count = 2 WHERE user_id = ?",
                                    (user_id,)
                                )
                                c_cd.execute(
                                    "INSERT INTO banned_users (user_id, reason, banned_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
                                    "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, banned_at=CURRENT_TIMESTAMP",
                                    (user_id, "Cooldown abuse")
                                )
                                conn_cd.commit()
                                bot.send_message(chat_id, "⛔ You have been banned for cooldown abuse.")
                                for admin_id in ADMIN_IDS:
                                    try:
                                        uname, full_name = format_user_identity(
                                            user_id,
                                            username=getattr(call.from_user, "username", None),
                                            first_name=getattr(call.from_user, "first_name", None),
                                            last_name=getattr(call.from_user, "last_name", None),
                                        )
                                        markup = types.InlineKeyboardMarkup()
                                        markup.add(types.InlineKeyboardButton("✅ Unban User", callback_data=f"quick_unban_{user_id}"))
                                        bot.send_message(
                                            admin_id,
                                            f"⛔ User banned for cooldown abuse: {user_id} - {uname} - {full_name}",
                                            reply_markup=markup
                                        )
                                    except Exception:
                                        pass
                                return
                except Exception as e:
                    logger.error(f"Cooldown check error: {e}")

                try:
                    country_flag = ""
                    try:
                        with get_db_connection() as conn:
                            row = conn.execute(
                                """
                                SELECT s.country, COALESCE(s.country_flag, ''), COALESCE(s.country_custom_emoji_id, ''), COALESCE(s.country_display_name, '')
                                FROM numbers n
                                JOIN services s ON s.id = n.service_id
                                WHERE n.number LIKE ? AND n.status='active'
                                LIMIT 1
                                """,
                                (f"{prefix}%",)
                            ).fetchone()
                        if row:
                            country_name, country_flag_text, country_custom_emoji_id, country_display_name = row
                            country_label = country_display_name or country_name
                            country_flag = format_country_display(
                                country_label,
                                html=True,
                                custom_emoji_id=country_custom_emoji_id,
                                flag_text=country_flag_text,
                            )
                    except Exception:
                        pass
                    bot.edit_message_text(f"{country_flag} <b>Getting numbers...</b>", chat_id, msg_id, parse_mode='HTML')
                except Exception:
                    pass
                search_and_show_results(chat_id, user_id, prefix, edit_msg_id=msg_id)
                return

            elif data.startswith("srv_"):
                service_id = int(data.split("_")[1])
                user_id = call.from_user.id

                cooldown_seconds = 7
                try:
                    with get_db_connection() as conn_cd:
                        row = conn_cd.execute("SELECT value FROM bot_config WHERE key='change_number_cooldown_seconds'").fetchone()
                        if row and row[0]:
                            cooldown_seconds = int(row[0])
                except Exception:
                    pass

                now_ts = int(time.time())
                try:
                    with get_db_connection() as conn_cd:
                        c_cd = conn_cd.cursor()
                        row = c_cd.execute(
                            "SELECT last_change_time, warning_count FROM user_cooldowns WHERE user_id = ?",
                            (user_id,)
                        ).fetchone()
                        last_change = row[0] if row and row[0] is not None else None
                        warning_count = row[1] if row and row[1] is not None else 0

                        if last_change and (now_ts - int(last_change)) < cooldown_seconds:
                            remaining = max(1, cooldown_seconds - (now_ts - int(last_change)))
                            if warning_count == 0:
                                c_cd.execute(
                                    "INSERT OR IGNORE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0)",
                                    (user_id, last_change)
                                )
                                c_cd.execute(
                                    "UPDATE user_cooldowns SET warning_count = 1 WHERE user_id = ?",
                                    (user_id,)
                                )
                                conn_cd.commit()
                                bot.answer_callback_query(call.id, f"⏳ Warning: Please wait {remaining} seconds before changing numbers again. Next click will ban you.", show_alert=True)
                                return
                            else:
                                from core import ADMIN_IDS
                                if user_id in ADMIN_IDS:
                                    bot.answer_callback_query(call.id, f"⏳ Please wait {remaining} seconds before changing numbers again.", show_alert=True)
                                    return
                                c_cd.execute(
                                    "INSERT OR IGNORE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 1)",
                                    (user_id, last_change)
                                )
                                c_cd.execute(
                                    "UPDATE user_cooldowns SET warning_count = 2 WHERE user_id = ?",
                                    (user_id,)
                                )
                                c_cd.execute(
                                    "INSERT INTO banned_users (user_id, reason, banned_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
                                    "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, banned_at=CURRENT_TIMESTAMP",
                                    (user_id, "Cooldown abuse")
                                )
                                conn_cd.commit()
                                bot.send_message(chat_id, "⛔ You have been banned for cooldown abuse.")
                                for admin_id in ADMIN_IDS:
                                    try:
                                        uname, full_name = format_user_identity(
                                            user_id,
                                            username=getattr(call.from_user, "username", None),
                                            first_name=getattr(call.from_user, "first_name", None),
                                            last_name=getattr(call.from_user, "last_name", None),
                                        )
                                        markup = types.InlineKeyboardMarkup()
                                        markup.add(types.InlineKeyboardButton("✅ Unban User", callback_data=f"quick_unban_{user_id}"))
                                        bot.send_message(
                                            admin_id,
                                            f"⛔ User banned for cooldown abuse: {user_id} - {uname} - {full_name}",
                                            reply_markup=markup
                                        )
                                    except Exception:
                                        pass
                                return
                except Exception as e:
                    logger.error(f"Cooldown check error: {e}")
                
                sms_limit = 1
                try:
                    row = c.execute("SELECT value FROM bot_config WHERE key='sms_limit'").fetchone()
                    if row and row[0] is not None:
                        sms_limit = int(row[0])
                except Exception:
                    sms_limit = 1

                release_user_numbers(conn, user_id, sms_limit=sms_limit, service_id=None)
                conn.commit()
                
                country_flag = ""
                try:
                    svc_row = c.execute(
                        "SELECT country, COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), COALESCE(country_display_name, '') "
                        "FROM services WHERE id=?",
                        (service_id,),
                    ).fetchone()
                    if svc_row:
                        country_name, country_flag_text, country_custom_emoji_id, country_display_name = svc_row
                        country_label = country_display_name or country_name
                        country_flag = format_country_display(
                            country_label,
                            html=True,
                            custom_emoji_id=country_custom_emoji_id,
                            flag_text=country_flag_text,
                        )
                except Exception:
                    pass
                loading_text = f"{country_flag} <b>Getting numbers...</b>"
                try:
                    bot.edit_message_text(loading_text, chat_id, msg_id, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        raise
                
                max_numbers = 5
                try:
                    limit_result = c.execute("SELECT value FROM bot_config WHERE key='max_numbers_per_assign'").fetchone()
                    if limit_result:
                        max_numbers = int(limit_result[0])
                except:
                    pass
                
                order_sql = assignment_order_sql()
                number_data = c.execute(
                    f"SELECT id, number FROM numbers WHERE service_id=? AND status='active' "
                    f"ORDER BY {order_sql} LIMIT ?",
                    (service_id, max_numbers)
                ).fetchall()
                service_data = c.execute(
                    "SELECT name, COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, '') FROM services WHERE id=?",
                    (service_id,),
                ).fetchone()
                service_name = service_data[0] if service_data else "Unknown"

                markup = types.InlineKeyboardMarkup(row_width=1)
                if number_data:
                    service_info = c.execute(
                        "SELECT name, country, COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), "
                        "COALESCE(country_display_name, ''), COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, '') "
                        "FROM services WHERE id=?",
                        (service_id,),
                    ).fetchone()
                    country_name = service_info[1] if service_info else "Unknown"
                    country_flag = service_info[2] if service_info else ""
                    country_custom_emoji_id = service_info[3] if service_info else ""
                    country_display_name = service_info[4] if service_info else ""
                    country_label = country_display_name or country_name
                    country_display = format_country_visible_label(
                        country_label,
                        flag_text=country_flag,
                        custom_emoji_id=country_custom_emoji_id,
                    )
                    service_full_name = service_info[0] if service_info else "Unknown"
                    service_emoji = service_info[5] if service_info else ""
                    service_custom_emoji_id = service_info[6] if service_info else ""
                    
                    reserved_ids = [num_id for num_id, _ in number_data]
                    mark_numbers_reserved(conn, user_id, reserved_ids)

                    c.execute(
                        "INSERT INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0) "
                        "ON CONFLICT(user_id) DO UPDATE SET last_change_time=excluded.last_change_time, warning_count=excluded.warning_count",
                        (user_id, now_ts)
                    )
                    
                    conn.commit()
                    success_text, success_entities = build_assignment_message_entities(
                        country_label=country_label,
                        country_flag=country_flag,
                        country_custom_emoji_id=country_custom_emoji_id,
                        service_name=service_full_name,
                        service_emoji=service_emoji,
                        service_custom_emoji_id=service_custom_emoji_id,
                    )
                    number_buttons = []
                    display_numbers = []
                    button_flag = country_flag
                    for _, raw_num in number_data:
                        display_num = f"+{raw_num}" if not raw_num.startswith('+') else raw_num
                        display_numbers.append(display_num)
                        copy_text_obj = types.CopyTextButton(text=display_num)
                        if country_custom_emoji_id:
                            number_buttons.append([types.InlineKeyboardButton(display_num, copy_text=copy_text_obj, icon_custom_emoji_id=country_custom_emoji_id)])
                        else:
                            number_buttons.append([types.InlineKeyboardButton(f"{button_flag} {display_num}", copy_text=copy_text_obj)])
                    from core import OTP_GROUP_URL
                    from core import BOT_TOKEN
                    styled_rows = styled_build_number_rows(button_flag, display_numbers, {}, custom_emoji_id=country_custom_emoji_id)
                    styled_rows.extend(
                        styled_build_action_rows(
                            change_cb=f"srv_{service_id}",
                            country_cb=f"change_country_{service_full_name}",
                            otp_group_url=OTP_GROUP_URL or "",
                        )
                    )
                    styled_ok, _ = styled_edit_message(
                        BOT_TOKEN,
                        chat_id,
                        msg_id,
                        success_text,
                        styled_rows,
                        entities=success_entities,
                    )
                    if not styled_ok:
                        markup = types.InlineKeyboardMarkup(number_buttons)
                        markup.add(types.InlineKeyboardButton("🔄 Change Numbers", callback_data=f"srv_{service_id}"))
                        markup.add(types.InlineKeyboardButton("🌍 Change Country", callback_data=f"change_country_{service_full_name}"))
                        if OTP_GROUP_URL:
                            markup.add(types.InlineKeyboardButton("📢 OTP Group", url=OTP_GROUP_URL))
                        bot.edit_message_text(
                            success_text,
                            chat_id,
                            msg_id,
                            reply_markup=markup,
                            entities=success_entities,
                            parse_mode=None,
                        )
                    delete_previous_assignment_message(chat_id, user_id, keep_message_id=msg_id)
                    remember_assignment_message(user_id, msg_id)
                else:
                    markup.add(types.InlineKeyboardButton("⬅️ Back to Services", callback_data="get_number"))
                    text = "📭 <b>Out of Stock!</b>\n\nNo numbers available for this service.\nPlease try another service."
                    bot.edit_message_text(text, chat_id, msg_id, reply_markup=markup)
                    forget_assignment_message(user_id, msg_id)

    @bot.message_handler(commands=['getnumber', 'get_number', 'get'])
    def cmd_get_number(message):
        handle_get_number_button(message)

    @bot.message_handler(func=lambda message: message.chat.type == 'private' and user_text_is(message.text, "Get Number"))
    def handle_get_number_button(message):
        user_id = message.from_user.id
        
        if not is_subscribed(user_id):
            unjoined = get_unjoined_channels(user_id)
            
            if unjoined:
                markup = types.InlineKeyboardMarkup()
                for (name, channel_id, invite_link) in unjoined:
                    button_url = build_join_url(invite_link, channel_id)
                    if button_url:
                        markup.add(types.InlineKeyboardButton(f" {name}", url=button_url))
                markup.add(types.InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
                
                with get_db_connection() as conn:
                    joined_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] - len(unjoined)
                    total_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
                
                bot.send_message(message.chat.id, 
                    f"<b>Access Denied!</b>\n\nYou are in {joined_count}/{total_count} channels.\n\nPlease join the remaining:",
                    reply_markup=markup, parse_mode='HTML')
            return
        
        show_service_list(message.chat.id)

    user_search_cache = {}

    def search_and_show_results(chat_id, user_id, prefix, edit_msg_id=None):
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                sms_limit = 1
                try:
                    row = c.execute("SELECT value FROM bot_config WHERE key='sms_limit'").fetchone()
                    if row and row[0] is not None:
                        sms_limit = int(row[0])
                except Exception:
                    sms_limit = 1

                release_user_numbers(conn, user_id, sms_limit=sms_limit, service_id=None)
                conn.commit()
                
                max_numbers = 5
                try:
                    limit_result = c.execute("SELECT value FROM bot_config WHERE key='max_numbers_per_assign'").fetchone()
                    if limit_result:
                        max_numbers = int(limit_result[0])
                except:
                    pass
                
                order_sql = assignment_order_sql()
                all_numbers = c.execute(
                    f"SELECT id, number, service_id FROM numbers WHERE number LIKE ? AND status='active' "
                    f"ORDER BY {order_sql} LIMIT 10",
                    (f"{prefix}%",)
                ).fetchall()
            
            if not all_numbers:
                bot.send_message(chat_id, f"📭 No numbers found matching <code>{prefix}</code>", parse_mode='HTML')
                return
            
            anchor_service_id = all_numbers[0][2]
            unique_numbers = []
            seen_numbers = set()

            for num_id, number, service_id in all_numbers:
                if service_id != anchor_service_id:
                    continue
                if number not in seen_numbers:
                    seen_numbers.add(number)
                    unique_numbers.append((num_id, number, service_id))
                    if len(unique_numbers) >= max_numbers:
                        break
            
            if len(unique_numbers) == 0:
                bot.send_message(chat_id, f"📭 No active unique numbers found matching <code>{prefix}</code>", parse_mode='HTML')
                return
            
            with get_db_connection() as conn:
                c = conn.cursor()
                service_id = unique_numbers[0][2]
                
                service_info = c.execute(
                    "SELECT name, country, COALESCE(country_flag, ''), COALESCE(country_custom_emoji_id, ''), "
                    "COALESCE(country_display_name, ''), COALESCE(service_emoji, ''), COALESCE(service_custom_emoji_id, '') "
                    "FROM services WHERE id = ?",
                    (service_id,),
                ).fetchone()
                if service_info:
                    service_name, country, country_flag, country_custom_emoji_id, country_display_name, service_emoji, service_custom_emoji_id = service_info
                else:
                    service_name, country, country_flag, country_custom_emoji_id, country_display_name, service_emoji, service_custom_emoji_id = ("Unknown", "Unknown", "", "", "", "", "")
                country_label = country_display_name or country
                country_display = format_country_visible_label(
                    country_label,
                    flag_text=country_flag,
                    custom_emoji_id=country_custom_emoji_id,
                )
                
                reserved_ids = [num_id for num_id, _, _ in unique_numbers]
                mark_numbers_reserved(conn, user_id, reserved_ids)
                
                now_ts = int(time.time())
                c.execute(
                    "INSERT INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0) "
                    "ON CONFLICT(user_id) DO UPDATE SET last_change_time=excluded.last_change_time, warning_count=excluded.warning_count",
                    (user_id, now_ts)
                )
                
                conn.commit()
                
                if service_info:
                    success_text, success_entities = build_assignment_message_entities(
                        country_label=country_label,
                        country_flag=country_flag,
                        country_custom_emoji_id=country_custom_emoji_id,
                        service_name=service_name,
                        service_emoji=service_emoji,
                        service_custom_emoji_id=service_custom_emoji_id,
                    )
                    number_buttons = []
                    display_numbers = []
                    button_flag = country_flag
                    for _, number, _ in unique_numbers:
                        display_num = f"+{number}" if not number.startswith('+') else number
                        display_numbers.append(display_num)
                        copy_text_obj = types.CopyTextButton(text=display_num)
                        if country_custom_emoji_id:
                            number_buttons.append([types.InlineKeyboardButton(display_num, copy_text=copy_text_obj, icon_custom_emoji_id=country_custom_emoji_id)])
                        else:
                            number_buttons.append([types.InlineKeyboardButton(f"{button_flag} {display_num}", copy_text=copy_text_obj)])
                    
                    search_markup = types.InlineKeyboardMarkup(number_buttons)
                    search_markup.add(types.InlineKeyboardButton("🔄 Change Numbers", callback_data=f"search_change_{prefix}"))
                    search_markup.add(types.InlineKeyboardButton("🌍 Change Country", callback_data=f"change_country_{service_name}"))
                    from core import OTP_GROUP_URL
                    from core import BOT_TOKEN
                    if OTP_GROUP_URL:
                        search_markup.add(types.InlineKeyboardButton("📢 OTP Group", url=OTP_GROUP_URL))
                    
                    keyboard = build_main_keyboard()
                    user_search_cache[user_id] = prefix

                    styled_rows = styled_build_number_rows(button_flag, display_numbers, {}, custom_emoji_id=country_custom_emoji_id)
                    styled_rows.extend(
                        styled_build_action_rows(
                            change_cb=f"search_change_{prefix}",
                            country_cb=f"change_country_{service_name}",
                            otp_group_url=OTP_GROUP_URL or "",
                        )
                    )
                    
                    if edit_msg_id:
                        styled_ok, styled_err = styled_edit_message(
                            BOT_TOKEN,
                            chat_id,
                            edit_msg_id,
                            success_text,
                            styled_rows,
                            entities=success_entities,
                        )
                        if not styled_ok:
                            logger.warning(f"Styled edit failed, fallback used: {styled_err}")
                            try:
                                bot.edit_message_text(
                                    success_text,
                                    chat_id,
                                    edit_msg_id,
                                    reply_markup=search_markup,
                                    entities=success_entities,
                                    parse_mode=None,
                                )
                                delete_previous_assignment_message(chat_id, user_id, keep_message_id=edit_msg_id)
                                remember_assignment_message(user_id, edit_msg_id)
                            except Exception as e:
                                logger.error(f"Failed to edit message: {e}")
                                delete_previous_assignment_message(chat_id, user_id)
                                sent = bot.send_message(
                                    chat_id,
                                    success_text,
                                    reply_markup=search_markup,
                                    entities=success_entities,
                                    parse_mode=None,
                                )
                                remember_assignment_message(user_id, sent.message_id)
                                bot.send_message(chat_id, "", reply_markup=keyboard)
                        else:
                            delete_previous_assignment_message(chat_id, user_id, keep_message_id=edit_msg_id)
                            remember_assignment_message(user_id, edit_msg_id)
                    else:
                        styled_ok, styled_err, styled_message_id = styled_send_message(
                            BOT_TOKEN,
                            chat_id,
                            success_text,
                            styled_rows,
                            entities=success_entities,
                        )
                        if not styled_ok:
                            logger.warning(f"Styled send failed, fallback used: {styled_err}")
                            delete_previous_assignment_message(chat_id, user_id)
                            sent = bot.send_message(
                                chat_id,
                                success_text,
                                reply_markup=search_markup,
                                entities=success_entities,
                                parse_mode=None,
                            )
                            remember_assignment_message(user_id, sent.message_id)
                        else:
                            delete_previous_assignment_message(chat_id, user_id, keep_message_id=styled_message_id)
                            remember_assignment_message(user_id, styled_message_id)
                        bot.send_message(chat_id, "", reply_markup=keyboard)
        
        except Exception as e:
            logger.error(f"Error searching numbers: {e}")
            bot.send_message(chat_id, f"❌ Error during search: {str(e)}")

    @bot.message_handler(commands=['services'])
    def cmd_services(message):
        if message.chat.type != 'private':
            return
        show_service_list(message.chat.id)

    @bot.message_handler(commands=['buy'])
    def cmd_buy(message):
        if message.chat.type != 'private':
            return

        user_id = message.from_user.id
        chat_id = message.chat.id

        if is_banned(user_id):
            bot.send_message(chat_id, "⛔ You are banned from using this bot.")
            return

        if not is_subscribed(user_id):
            unjoined = get_unjoined_channels(user_id)

            if unjoined:
                markup = types.InlineKeyboardMarkup()
                for (name, channel_id, invite_link) in unjoined:
                    button_url = build_join_url(invite_link, channel_id)
                    if button_url:
                        markup.add(types.InlineKeyboardButton(f"📌 {name}", url=button_url))
                markup.add(types.InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))

                with get_db_connection() as conn:
                    joined_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] - len(unjoined)
                    total_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]

                bot.send_message(chat_id, 
                    f"<b>Access Denied!</b>\n\nYou are in {joined_count}/{total_count} channels.\n\nPlease join the remaining:",
                    reply_markup=markup, parse_mode='HTML')
            return

        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            bot.send_message(chat_id,
                "🔎 <b>Search Number</b>\n\n"
                "Please use: <code>/buy +996</code> or <code>/buy 996</code>",
                parse_mode='HTML')
            return

        prefix = parts[1].strip()
        clean_prefix = prefix.lstrip('+')
        country_flag = ""
        try:
            with get_db_connection() as conn:
                row = conn.execute(
                    """
                    SELECT s.country, COALESCE(s.country_flag, ''), COALESCE(s.country_custom_emoji_id, ''), COALESCE(s.country_display_name, '')
                    FROM numbers n
                    JOIN services s ON s.id = n.service_id
                    WHERE n.number LIKE ? AND n.status='active'
                    LIMIT 1
                    """,
                    (f"{clean_prefix}%",)
                ).fetchone()
            if row:
                country_name, country_flag_text, country_custom_emoji_id, country_display_name = row
                country_label = country_display_name or country_name
                country_flag = format_country_display(
                    country_label,
                    html=True,
                    custom_emoji_id=country_custom_emoji_id,
                    flag_text=country_flag_text,
                )
        except Exception:
            pass
        msg = bot.send_message(
            chat_id,
            f"{country_flag} <b>Getting numbers...</b>",
            parse_mode='HTML',
            reply_markup=types.ReplyKeyboardRemove(),
        )
        search_and_show_results(chat_id, user_id, clean_prefix, edit_msg_id=msg.message_id)

    @bot.message_handler(func=lambda m: m.chat.type == 'private' and user_text_is(m.text, "Search Number"))
    def search_number_start(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        if is_banned(user_id):
            bot.send_message(chat_id, "⛔ You are banned from using this bot.")
            return
        
        if not is_subscribed(user_id):
            unjoined = get_unjoined_channels(user_id)
            
            if unjoined:
                markup = types.InlineKeyboardMarkup()
                for (name, channel_id, invite_link) in unjoined:
                    button_url = build_join_url(invite_link, channel_id)
                    if button_url:
                        markup.add(types.InlineKeyboardButton(f" {name}", url=button_url))
                markup.add(types.InlineKeyboardButton("✅ Verify Join", callback_data="verify_join"))
                
                with get_db_connection() as conn:
                    joined_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] - len(unjoined)
                    total_count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
                
                bot.send_message(chat_id, 
                    f"<b>Access Denied!</b>\n\nYou are in {joined_count}/{total_count} channels.\n\nPlease join the remaining:",
                    reply_markup=markup, parse_mode='HTML')
            return
        
        keyboard = build_main_keyboard()
        
        msg = bot.send_message(
            chat_id,
            "🔎 <b>Search Number</b>\n\n"
            " Please enter the first few digits of the number with country code:\n\n"
            "Example: <code>+88015</code> or <code>+919</code>\n\n"
            "Tap Back or Cancel to return.",
            parse_mode='HTML',
            reply_markup=types.ReplyKeyboardRemove(),
        )
        bot.register_next_step_handler(msg, search_number_in_db)

    def search_number_in_db(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        prefix = message.text.strip()

        if user_text_is(prefix, "Back") or user_text_is(prefix, "Cancel"):
            bot.send_message(chat_id, "🏠 Back to main menu.", reply_markup=build_main_keyboard())
            return
        
        if not prefix:
            bot.send_message(chat_id, "⚠️ Please enter a valid number prefix!")
            return search_number_start(message)
        
        clean_prefix = prefix.lstrip('+')
        country_flag = ""
        try:
            with get_db_connection() as conn:
                row = conn.execute(
                    """
                    SELECT s.country, COALESCE(s.country_flag, ''), COALESCE(s.country_custom_emoji_id, ''), COALESCE(s.country_display_name, '')
                    FROM numbers n
                    JOIN services s ON s.id = n.service_id
                    WHERE n.number LIKE ? AND n.status='active'
                    LIMIT 1
                    """,
                    (f"{clean_prefix}%",)
                ).fetchone()
            if row:
                country_name, country_flag_text, country_custom_emoji_id, country_display_name = row
                country_label = country_display_name or country_name
                country_flag = format_country_display(
                    country_label,
                    html=True,
                    custom_emoji_id=country_custom_emoji_id,
                    flag_text=country_flag_text,
                )
        except Exception:
            pass
        msg = bot.send_message(chat_id, f"{country_flag} <b>Getting numbers...</b>", parse_mode='HTML')
        search_and_show_results(chat_id, user_id, clean_prefix, edit_msg_id=msg.message_id)
