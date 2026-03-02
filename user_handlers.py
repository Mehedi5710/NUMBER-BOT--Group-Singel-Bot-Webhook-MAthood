"""User command and message handlers"""
import time
from telebot import types
from core import get_assignment_mode, mark_numbers_reserved, release_numbers_for_user


def register_handlers(bot, get_db_connection, logger):
    subscribed_cache = {}
    broken_targets_alerted = set()
    BTN_GET_NUMBER = "📱 Get Number"
    BTN_SEARCH_NUMBER = "🔎 Search Number"

    def user_text_is(text, base_label):
        raw = (text or "").strip().lower()
        base = base_label.strip().lower()
        if raw == base:
            return True
        # Accept emoji-prefixed labels without breaking old plain labels.
        cleaned = __import__('re').sub(r'[^a-z ]', '', raw).strip()
        return cleaned == base.lower()

    def format_user_identity(user_id, username=None, first_name=None, last_name=None):
        u = username
        fn = first_name
        ln = last_name
        if not (u or fn or ln):
            try:
                with get_db_connection() as conn:
                    row = conn.execute(
                        "SELECT username, first_name, last_name FROM users WHERE user_id = ?",
                        (user_id,)
                    ).fetchone()
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
                row = conn.execute(
                    "SELECT value FROM bot_config WHERE key='channels_version'"
                ).fetchone()
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
                row = conn.execute(
                    "SELECT value FROM bot_config WHERE key='subscription_recheck_hours'"
                ).fetchone()
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
                channels = conn.execute(
                    "SELECT name, channel_identifier, invite_link FROM channels ORDER BY id"
                ).fetchall()
                
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
                channels = conn.execute("SELECT name, channel_identifier FROM channels").fetchall()
                
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
            banned = conn.execute("SELECT reason FROM banned_users WHERE user_id=?", (user_id,)).fetchone()
            return banned is not None

    def get_country_flag(country_name, sample_numbers=None):
        from flag import get_flag, detect_country_from_numbers
        if sample_numbers:
            try:
                detected = detect_country_from_numbers(sample_numbers, country_hint=country_name)
                if detected and detected.get("flag"):
                    return detected["flag"]
            except Exception:
                pass
        return get_flag(country_name)

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
            services = conn.execute("""
                SELECT s.name, SUM(CASE WHEN n.status='active' AND n.user_id IS NULL THEN 1 ELSE 0 END) as total_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.status = 'active'
                GROUP BY s.name
                HAVING total_count > 0
                ORDER BY s.name
            """).fetchall()

        if not services:
            text = "❌ No services available."
            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
            if edit_msg_id:
                try:
                    bot.delete_message(chat_id, edit_msg_id)
                except:
                    pass
            bot.send_message(chat_id, text, reply_markup=keyboard)
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for service_name, total_count in services:
            import re
            clean_name = re.sub(r'[^\w\s]', '', service_name).strip()
            if not clean_name:
                clean_name = service_name
            label = f"{clean_name} ({total_count})"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"service_select_{service_name}"))

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
            countries = conn.execute("""
                SELECT s.country, s.id, COUNT(n.id) as count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id AND n.status='active' AND n.user_id IS NULL
                WHERE s.name = ? AND s.status = 'active'
                GROUP BY s.country, s.id
                HAVING count > 0
                ORDER BY s.country
            """, (service_name,)).fetchall()

        if not countries:
            text = f" No countries available for {service_name}."
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
        for country, service_id, count in countries:
            flag = get_country_flag(country)
            label = f"{flag} {country} ({count})"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"srv_{service_id}"))

        markup.add(types.InlineKeyboardButton("⬅️ Back to Services", callback_data="get_number"))
        import re
        clean_service_name = re.sub(r'[^\w\s]', '', service_name).strip()
        if not clean_service_name:
            clean_service_name = service_name
        text = f"🌍 <b>Select Country for {clean_service_name}:</b>"
        
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
            conn.execute("""
                INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, last_active)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
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
                keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
                keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
                bot.send_message(chat_id, "👋 Welcome!", reply_markup=keyboard)
            else:
                keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
                keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
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
                keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
                keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
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
            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
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
                    service_info = c.execute("SELECT name, country FROM services WHERE id = ?", (service_id,)).fetchone()
                    conn.commit()
                    
                    if service_info:
                        service_name, country = service_info
                        flag = get_country_flag(country)
                        import re
                        clean_service_display = re.sub(r'[^\w\s]', '', service_name).strip()
                        if not clean_service_display:
                            clean_service_display = service_name
                        success_text = f" <b>Number Assigned!</b>\n\n"
                        success_text += f"{flag} {country}  {clean_service_display}\n"
                        success_text += f"<code>{display_num}</code>\n\n"
                        success_text += " <i>Waiting for OTP...</i>"
                        
                        markup = create_number_markup(service_id, service_name)
                        
                        try:
                            bot.edit_message_text(success_text, chat_id, msg_id, reply_markup=markup, parse_mode='HTML')
                        except:
                            bot.send_message(chat_id, success_text, reply_markup=markup, parse_mode='HTML')
                        
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
                                    "INSERT OR REPLACE INTO banned_users (user_id, reason) VALUES (?, ?)",
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
                                SELECT s.country
                                FROM numbers n
                                JOIN services s ON s.id = n.service_id
                                WHERE n.number LIKE ? AND n.status='active'
                                LIMIT 1
                                """,
                                (f"{prefix}%",)
                            ).fetchone()
                        if row and row[0]:
                            country_flag = get_country_flag(row[0])
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
                                    "INSERT OR REPLACE INTO banned_users (user_id, reason) VALUES (?, ?)",
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

                release_user_numbers(conn, user_id, sms_limit=sms_limit, service_id=service_id)
                conn.commit()
                
                country_flag = ""
                try:
                    svc_row = c.execute("SELECT country FROM services WHERE id=?", (service_id,)).fetchone()
                    if svc_row and svc_row[0]:
                        country_flag = get_country_flag(svc_row[0])
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
                    f"SELECT DISTINCT id, number FROM numbers WHERE service_id=? AND status='active' "
                    f"ORDER BY {order_sql} LIMIT ?",
                    (service_id, max_numbers)
                ).fetchall()
                service_data = c.execute("SELECT name FROM services WHERE id=?", (service_id,)).fetchone()
                service_name = service_data[0] if service_data else "Unknown"

                markup = types.InlineKeyboardMarkup(row_width=1)
                if number_data:
                    service_info = c.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
                    country_name = service_info[1] if service_info else "Unknown"
                    sample_numbers = [raw_num for _, raw_num in number_data]
                    flag = get_country_flag(country_name, sample_numbers=sample_numbers)
                    service_full_name = service_info[0] if service_info else "Unknown"
                    
                    number_buttons = []
                    reserved_ids = []
                    for idx, (num_id, raw_num) in enumerate(number_data, 1):
                        display_num = f"+{raw_num}" if not raw_num.startswith('+') else raw_num
                        copy_text_obj = types.CopyTextButton(text=display_num)
                        number_buttons.append([types.InlineKeyboardButton(f"{flag} {display_num}", copy_text=copy_text_obj)])
                        reserved_ids.append(num_id)
                    mark_numbers_reserved(conn, user_id, reserved_ids)

                    c.execute(
                        "INSERT OR REPLACE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0)",
                        (user_id, now_ts)
                    )
                    
                    conn.commit()
                    import re
                    clean_service_display = re.sub(r'[^\w\s]', '', service_full_name).strip()
                    if not clean_service_display:
                        clean_service_display = service_full_name
                    success_text = f"\n" + \
                                   f"{flag} <b>{country_name} {clean_service_display}</b>\n" + \
                                   f""
                    markup = types.InlineKeyboardMarkup(number_buttons)
                    markup.add(types.InlineKeyboardButton("🔄 Change Numbers", callback_data=f"srv_{service_id}"))
                    markup.add(types.InlineKeyboardButton("🌍 Change Country", callback_data=f"change_country_{service_full_name}"))
                    from core import OTP_GROUP_URL
                    if OTP_GROUP_URL:
                        markup.add(types.InlineKeyboardButton("📢 OTP Group", url=OTP_GROUP_URL))
                    bot.edit_message_text(success_text, chat_id, msg_id, reply_markup=markup)
                else:
                    markup.add(types.InlineKeyboardButton("⬅️ Back to Services", callback_data="get_number"))
                    text = "📭 <b>Out of Stock!</b>\n\nNo numbers available for this service.\nPlease try another service."
                    bot.edit_message_text(text, chat_id, msg_id, reply_markup=markup)

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
            
            unique_numbers = []
            seen_numbers = set()
            
            for num_id, number, service_id in all_numbers:
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
                number_buttons = []
                
                service_info = c.execute("SELECT name, country FROM services WHERE id = ?", (service_id,)).fetchone()
                service_name, country = service_info if service_info else ("Unknown", "Unknown")
                sample_numbers = [number for _, number, _ in unique_numbers]
                flag = get_country_flag(country, sample_numbers=sample_numbers)
                
                reserved_ids = []
                for idx, (num_id, number, srv_id) in enumerate(unique_numbers, 1):
                    display_num = f"+{number}" if not number.startswith('+') else number
                    copy_text_obj = types.CopyTextButton(text=display_num)
                    number_buttons.append([types.InlineKeyboardButton(f"{flag} {display_num}", copy_text=copy_text_obj)])
                    reserved_ids.append(num_id)
                mark_numbers_reserved(conn, user_id, reserved_ids)
                
                now_ts = int(time.time())
                c.execute(
                    "INSERT OR REPLACE INTO user_cooldowns (user_id, last_change_time, warning_count) VALUES (?, ?, 0)",
                    (user_id, now_ts)
                )
                
                conn.commit()
                
                if service_info:
                    import re
                    clean_service_display = re.sub(r'[^\w\s]', '', service_name).strip()
                    if not clean_service_display:
                        clean_service_display = service_name
                    success_text = f"\n" + \
                                   f"{flag} <b>{country} {clean_service_display}</b>\n" + \
                                   f""
                    
                    search_markup = types.InlineKeyboardMarkup(number_buttons)
                    search_markup.add(types.InlineKeyboardButton("🔄 Change Numbers", callback_data=f"search_change_{prefix}"))
                    search_markup.add(types.InlineKeyboardButton("🌍 Change Country", callback_data=f"change_country_{service_name}"))
                    from core import OTP_GROUP_URL
                    if OTP_GROUP_URL:
                        search_markup.add(types.InlineKeyboardButton("📢 OTP Group", url=OTP_GROUP_URL))
                    
                    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
                    keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
                    user_search_cache[user_id] = prefix
                    
                    if edit_msg_id:
                        try:
                            bot.edit_message_text(success_text, chat_id, edit_msg_id, reply_markup=search_markup, parse_mode='HTML')
                        except Exception as e:
                            logger.error(f"Failed to edit message: {e}")
                            bot.send_message(chat_id, success_text, reply_markup=search_markup, parse_mode='HTML')
                            bot.send_message(chat_id, "", reply_markup=keyboard)
                    else:
                        bot.send_message(chat_id, success_text, reply_markup=search_markup, parse_mode='HTML')
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
                    SELECT s.country
                    FROM numbers n
                    JOIN services s ON s.id = n.service_id
                    WHERE n.number LIKE ? AND n.status='active'
                    LIMIT 1
                    """,
                    (f"{clean_prefix}%",)
                ).fetchone()
            if row and row[0]:
                country_flag = get_country_flag(row[0])
        except Exception:
            pass
        msg = bot.send_message(chat_id, f"{country_flag} <b>Getting numbers...</b>", parse_mode='HTML')
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
        
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        keyboard.add(BTN_GET_NUMBER, BTN_SEARCH_NUMBER)
        
        msg = bot.send_message(chat_id, 
            "🔎 <b>Search Number</b>\n\n"
            " Please enter the first few digits of the number with country code:\n\n"
            "Example: <code>+88015</code> or <code>+919</code>",
            parse_mode='HTML',
            reply_markup=keyboard)
        bot.register_next_step_handler(msg, search_number_in_db)

    def search_number_in_db(message):
        user_id = message.from_user.id
        chat_id = message.chat.id
        prefix = message.text.strip()
        
        if not prefix:
            bot.send_message(chat_id, "⚠️ Please enter a valid number prefix!")
            return search_number_start(message)
        
        clean_prefix = prefix.lstrip('+')
        country_flag = ""
        try:
            with get_db_connection() as conn:
                row = conn.execute(
                    """
                    SELECT s.country
                    FROM numbers n
                    JOIN services s ON s.id = n.service_id
                    WHERE n.number LIKE ? AND n.status='active'
                    LIMIT 1
                    """,
                    (f"{clean_prefix}%",)
                ).fetchone()
            if row and row[0]:
                country_flag = get_country_flag(row[0])
        except Exception:
            pass
        msg = bot.send_message(chat_id, f"{country_flag} <b>Getting numbers...</b>", parse_mode='HTML')
        search_and_show_results(chat_id, user_id, clean_prefix, edit_msg_id=msg.message_id)
