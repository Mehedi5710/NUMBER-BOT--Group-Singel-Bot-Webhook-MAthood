# admin.py  Admin panel handlers
from telebot import types
import datetime
import shutil
import re
import threading
import queue
from core import BACKUP_DIR, DB_NAME

# Try to import psutil for system monitoring (optional)
try:
    import psutil
except ImportError:
    psutil = None


def register_handlers(bot, get_db_connection, logger):
    broadcast_queue = queue.Queue()
    broadcast_worker_started = False
    forwarder_panel_users = set()

    def answer_cbq(call, *args, **kwargs):
        """Defensive wrapper: callback queries can expire; never crash polling on answer failures."""
        try:
            bot.answer_callback_query(call.id, *args, **kwargs)
        except Exception as e:
            logger.warning(f"answer_callback_query failed: {e}")

    def start_broadcast_worker():
        nonlocal broadcast_worker_started
        if broadcast_worker_started:
            return
        broadcast_worker_started = True

        def worker():
            while True:
                job = broadcast_queue.get()
                if job is None:
                    break
                admin_id = job.get("admin_id")
                text = job.get("text", "")
                parse_mode = job.get("parse_mode")
                title = job.get("title", "Broadcast")
                media_type = job.get("media_type", "text")
                photo_file_id = job.get("photo_file_id")

                try:
                    with get_db_connection() as conn:
                        users = conn.execute(
                            "SELECT user_id FROM users \n                            WHERE user_id NOT IN (SELECT user_id FROM banned_users)"
                        ).fetchall()
                except Exception as e:
                    logger.error(f"Broadcast load users failed: {e}")
                    users = []

                sent_count = 0
                failed_count = 0
                blocked_count = 0
                not_found_count = 0

                for (user_id,) in users:
                    try:
                        if user_id:
                            if media_type == "photo" and photo_file_id:
                                bot.send_photo(user_id, photo_file_id, caption=text or "", parse_mode=parse_mode)
                            else:
                                bot.send_message(user_id, text, parse_mode=parse_mode)
                            sent_count += 1
                    except Exception as e:
                        error_msg = str(e)
                        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
                            blocked_count += 1
                        elif "chat not found" in error_msg:
                            not_found_count += 1
                        failed_count += 1

                result_text = f" <b>{title} Complete!</b>\n\n"
                result_text += f" Sent: {sent_count}\n"
                result_text += f" Failed: {failed_count}\n"
                if blocked_count > 0:
                    result_text += f" Blocked/Deleted: {blocked_count}\n"
                if not_found_count > 0:
                    result_text += f" Chat Not Found: {not_found_count}\n"
                result_text += f" Total: {sent_count + failed_count}"

                if admin_id:
                    try:
                        bot.send_message(admin_id, result_text, parse_mode='HTML')
                    except Exception as e:
                        logger.error(f"Broadcast status send failed: {e}")

                logger.info(f"{title} finished: {sent_count} sent, {failed_count} failed")
                broadcast_queue.task_done()

        threading.Thread(target=worker, daemon=True).start()

    def enqueue_broadcast(admin_id, text, parse_mode=None, title="Broadcast", media_type="text", photo_file_id=None):
        start_broadcast_worker()
        broadcast_queue.put({
            "admin_id": admin_id,
            "text": text,
            "parse_mode": parse_mode,
            "title": title,
            "media_type": media_type,
            "photo_file_id": photo_file_id,
        })

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

    def send_or_edit(chat_id, text, reply_markup=None, parse_mode=None, edit_msg_id=None):
        """Prefer editing existing admin inline message; fallback to send new message."""
        if edit_msg_id:
            try:
                bot.edit_message_text(
                    text,
                    chat_id,
                    edit_msg_id,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
                return
            except Exception:
                pass
        bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    
    def is_admin(message):
        from core import ADMIN_IDS
        return message.chat.type == 'private' and message.from_user.id in ADMIN_IDS

    def admin_text_is(message, label):
        msg_norm = re.sub(r'[^A-Za-z ]', '', message.text or '').strip().lower()
        label_norm = re.sub(r'[^A-Za-z ]', '', label).strip().lower()
        return label_norm == msg_norm

    def is_cancel_text(text):
        val = (text or "").strip().lower()
        return val in {"cancel", "/cancel", "back", "0"}

    def apply_live_bot_token(new_token):
        """Try to apply new bot token without restarting process."""
        try:
            setattr(bot, "token", new_token)
        except Exception:
            pass
        # Some telebot versions keep mangled private token too.
        try:
            setattr(bot, "_TeleBot__token", new_token)
        except Exception:
            pass

    def get_bot_config_value(key, default=""):
        try:
            with get_db_connection() as conn:
                row = conn.execute("SELECT value FROM bot_config WHERE key=?", (key,)).fetchone()
                if row and row[0] is not None:
                    return str(row[0])
        except Exception as e:
            logger.error(f"Failed reading bot_config {key}: {e}")
        return default

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

    def parse_group_ids(raw_value):
        items = []
        for part in (raw_value or "").split(","):
            v = part.strip()
            if not v:
                continue
            try:
                items.append(int(v))
            except Exception:
                continue
        # Keep order and uniqueness
        unique = []
        seen = set()
        for gid in items:
            if gid in seen:
                continue
            seen.add(gid)
            unique.append(gid)
        return unique

    def load_forwarder_group_ids():
        return parse_group_ids(get_bot_config_value("forwarder_group_ids", ""))

    def save_forwarder_group_ids(group_ids):
        return set_bot_config_value("forwarder_group_ids", ",".join(str(x) for x in group_ids))

    def show_forwarder_panel(message):
        if getattr(message, "from_user", None):
            forwarder_panel_users.add(message.from_user.id)
        groups = load_forwarder_group_ids()
        number_bot_link = get_bot_config_value("forwarder_number_bot_link", "")
        support_group_link = get_bot_config_value("forwarder_support_group_link", "")

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("➕ Add Group ID", "🗑️ Remove Group ID")
        markup.add("🔗 Set Number Bot Link", "🔗 Set Support Group Link")
        markup.add("❌ Remove Number Bot Link", "❌ Remove Support Group Link")
        markup.add("🔐 Forwarder Token", "📌 View Forwarder Config")
        markup.add("⬅️ Back to Settings")

        text = (
            "🤖 <b>Forwarder Panel</b>\n\n"
            f"Group IDs: <b>{len(groups)}</b>\n"
            f"Number Bot Link: {'Set' if number_bot_link else 'Not set'}\n"
            f"Support Group Link: {'Set' if support_group_link else 'Not set'}\n\n"
            "Select an option:"
        )
        bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='HTML')

    def return_to_admin_or_forwarder(message):
        uid = getattr(getattr(message, "from_user", None), "id", None)
        if uid in forwarder_panel_users:
            return show_forwarder_panel(message)
        return show_admin_list_menu(message)

    def bot_name():
        from core import BOT_NAME
        return BOT_NAME

    def bump_channels_version():
        try:
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO bot_config (key, value) VALUES ('channels_version', '1')"
                )
                conn.execute(
                    "UPDATE bot_config SET value = CAST(value AS INTEGER) + 1 WHERE key='channels_version'"
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to bump channels version: {e}")

    def normalize_target_identifier(raw_value):
        """Normalize target identifier to @username or numeric chat id."""
        raw = (raw_value or "").strip()
        if not raw:
            return None, "Target cannot be empty."

        low = raw.lower()
        if low.startswith("https://t.me/") or low.startswith("http://t.me/") or low.startswith("t.me/"):
            raw = raw.split("t.me/", 1)[1].strip()
            raw = raw.split("?", 1)[0].strip("/")

        if raw.startswith("@"):
            username = raw[1:]
            if not re.fullmatch(r"[A-Za-z0-9_]{4,}", username):
                return None, "Invalid @username format."
            return f"@{username}", None

        if re.fullmatch(r"-?\d{6,20}", raw):
            return raw, None

        if re.fullmatch(r"[A-Za-z0-9_]{4,}", raw):
            return f"@{raw}", None

        return None, "Invalid target. Use @username or numeric chat ID."

    def normalize_join_link(raw_value):
        """Normalize join link to https://t.me/... URL."""
        raw = (raw_value or "").strip()
        if not raw:
            return None, "Join link cannot be empty."

        if raw.startswith("@"):
            username = raw[1:]
            if re.fullmatch(r"[A-Za-z0-9_]{4,}", username):
                return f"https://t.me/{username}", None
            return None, "Invalid @username for join link."

        if raw.lower().startswith("https://t.me/") or raw.lower().startswith("http://t.me/"):
            return raw.replace("http://", "https://", 1), None

        if raw.lower().startswith("t.me/"):
            return f"https://{raw}", None

        if re.fullmatch(r"[A-Za-z0-9_]{4,}", raw):
            return f"https://t.me/{raw}", None

        return None, "Invalid join link. Use https://t.me/... or @username."

    def validate_subscription_target(target_identifier):
        """
        Validate that bot can resolve target and has access in that chat/channel/group.
        Returns dict: ok, title, resolved_identifier, chat_type, error.
        """
        try:
            chat = bot.get_chat(target_identifier)
            bot_user = bot.get_me()
            member = bot.get_chat_member(chat.id, bot_user.id)
            status = getattr(member, "status", "")
            allowed = {"creator", "administrator", "member", "restricted"}
            if status not in allowed:
                return {
                    "ok": False,
                    "title": getattr(chat, "title", None) or getattr(chat, "username", None) or str(chat.id),
                    "resolved_identifier": str(chat.id),
                    "chat_type": getattr(chat, "type", "unknown"),
                    "error": f"Bot is not a member/admin in target (status: {status}).",
                }
            return {
                "ok": True,
                "title": getattr(chat, "title", None) or getattr(chat, "username", None) or str(chat.id),
                "resolved_identifier": str(chat.id),
                "chat_type": getattr(chat, "type", "unknown"),
                "error": None,
            }
        except Exception as e:
            return {
                "ok": False,
                "title": None,
                "resolved_identifier": None,
                "chat_type": "unknown",
                "error": str(e),
            }

    @bot.message_handler(commands=['admin'], func=is_admin)
    def admin_panel(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        markup.add('🧩 Create Service', '➕ Add Numbers')
        markup.add('🗑️ Delete Service', '♻️ Reactivate Service')
        markup.add('📊 Dashboard', '⚙️ Bot Settings')
        markup.add('🔐 Access Control', '👥 User Management')
        markup.add('📣 Broadcast', '🛠️ Bot Operations')
        markup.add('🚪 Exit Panel')
        bot.send_message(message.chat.id, f' <b>{bot_name()}</b>\n\nSelect an option:', reply_markup=markup, parse_mode='HTML')

    def is_main_menu_choice(message):
        msg_norm = re.sub(r'[^A-Za-z ]', '', message.text or '').strip().lower()
        return msg_norm in {
            'create service',
            'add numbers',
            'delete service',
            'reactivate service',
            'dashboard',
            'bot settings',
            'database mgmt',
            'database management',
            'access control',
            'user management',
            'broadcast',
            'bot operations',
            'exit panel'
        }

    @bot.message_handler(func=lambda m: is_admin(m) and is_main_menu_choice(m))
    def handle_admin_choice(message):
        normalized = re.sub(r'[^A-Za-z ]', '', message.text or '').strip().lower()
        normalized_map = {
            'create service': ask_service_name_for_new,
            'add numbers': ask_service_name_for_add,
            'delete service': select_service_to_delete,
            'reactivate service': select_service_to_reactivate,
            'dashboard': show_dashboard,
            'bot settings': show_bot_settings,
            'database mgmt': show_database_management,
            'database management': show_database_management,
            'access control': show_access_control,
            'user management': show_user_management,
            'broadcast': broadcast_menu,
            'bot operations': show_bot_operations,
            'exit panel': exit_admin_panel
        }
        for key, handler in normalized_map.items():
            if key in normalized:
                return handler(message)

    def exit_admin_panel(message):
        bot.send_message(message.chat.id, "👋 Exited the admin panel.", reply_markup=types.ReplyKeyboardRemove())

    def broadcast_menu(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add('📢 Send Broadcast', '⬅️ Back to Panel')
        bot.send_message(message.chat.id, ' <b>Broadcast Management</b>\n\nChoose an option:', reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Send Broadcast'))
    def send_broadcast_prompt(message):
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('📝 Plain Text', callback_data='broadcast_format_none'))
        markup.add(types.InlineKeyboardButton('🌐 HTML Format', callback_data='broadcast_format_html'))
        markup.add(types.InlineKeyboardButton('🖼️ Image + Text', callback_data='broadcast_format_image'))
        bot.send_message(message.chat.id, ' <b>Choose Message Format:</b>', reply_markup=markup, parse_mode='HTML')

    @bot.callback_query_handler(func=lambda call: call.data.startswith("broadcast_format_"))
    def handle_broadcast_format(call):
        format_type = call.data.replace("broadcast_format_", "")
        
        format_names = {
            'none': 'Plain Text',
            'html': 'HTML',
            'image': 'Image + Text'
        }
        
        format_tips = {
            'none': 'Send your message as plain text (no formatting)',
            'html': '<b>Bold</b>, <i>Italic</i>, <code>Code</code>, <a href="url">Link</a>',
            'image': 'Send a photo with optional caption text (caption supports HTML tags).'
        }
        
        if format_type == 'image':
            msg = bot.send_message(
                call.message.chat.id,
                " Send a photo now.\n\n"
                "You can add caption text in the same message.\n"
                "Caption supports HTML formatting.",
                reply_markup=types.ReplyKeyboardRemove(),
                parse_mode='HTML'
            )
            bot.register_next_step_handler(msg, process_broadcast, format_type)
            answer_cbq(call)
            return

        msg = bot.send_message(
            call.message.chat.id, 
            f" Enter your broadcast message:\n\n"
            f"Format: <b>{format_names[format_type]}</b>\n"
            f"Tips: {format_tips[format_type]}",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='HTML'
        )
        bot.register_next_step_handler(msg, process_broadcast, format_type)
        answer_cbq(call)

    def process_broadcast(message, format_type='none'):
        if format_type == 'image':
            if not getattr(message, "photo", None):
                bot.send_message(message.chat.id, "⚠️ Please send a photo with optional caption.")
                msg = bot.send_message(message.chat.id, "Send photo now (or use /admin to cancel).")
                bot.register_next_step_handler(msg, process_broadcast, format_type)
                return

            photo_file_id = message.photo[-1].file_id
            caption = (message.caption or "").strip()
            enqueue_broadcast(
                message.from_user.id,
                caption,
                parse_mode='HTML' if caption else None,
                title="Image Broadcast",
                media_type="photo",
                photo_file_id=photo_file_id
            )
            bot.send_message(
                message.chat.id,
                "✅ Image broadcast queued. You will receive a status update when it finishes.",
                reply_markup=types.ReplyKeyboardRemove()
            )
            logger.info(f"Image broadcast queued by admin {message.from_user.id}")
            broadcast_menu(message)
            return

        broadcast_msg = (message.text or "").strip()

        if not broadcast_msg:
            bot.send_message(message.chat.id, "⚠️ Message cannot be empty!")
            return send_broadcast_prompt(message)

        parse_mode_map = {
            'none': None,
            'html': 'HTML',
        }
        parse_mode = parse_mode_map.get(format_type, None)

        enqueue_broadcast(message.from_user.id, broadcast_msg, parse_mode=parse_mode, title="Broadcast")
        bot.send_message(message.chat.id, "✅ Broadcast queued. You will receive a status update when it finishes.", reply_markup=types.ReplyKeyboardRemove())
        logger.info(f"Broadcast queued by admin {message.from_user.id}, format: {format_type}")
        broadcast_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Back to Panel") and m.chat.type == 'private')
    def back_to_panel(message):
        admin_panel(message)

    # Create Service Flow
    def ask_service_name_for_new(message):
        msg = bot.send_message(message.chat.id, " Enter Service Name (you can include emojis, e.g.,  SMS,  Telegram):", reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, create_service_directly)

    def create_service_directly(message):
        service_name = message.text.strip()
        if not service_name:
            bot.send_message(message.chat.id, "⚠️ Service name cannot be empty.")
            return ask_service_name_for_new(message)
        
        try:
            with get_db_connection() as conn:
                # Check if service already exists
                existing = conn.execute("SELECT id FROM services WHERE name = ? AND country = 'Global'", (service_name,)).fetchone()
                if existing:
                    bot.send_message(message.chat.id, f" Service '{service_name}' already exists!", reply_markup=types.ReplyKeyboardRemove())
                    admin_panel(message)
                    return
                    
                # Use a placeholder country for service creation
                conn.execute("INSERT INTO services (name, country) VALUES (?, ?)", (service_name, "Global"))
                conn.commit()
            
            bot.send_message(message.chat.id, f" <b>Service created!</b>\n\n <b>{service_name}</b>", reply_markup=types.ReplyKeyboardRemove())
            logger.info(f"Service created: {service_name}")
            
            # Return to admin panel
            admin_panel(message)
        except Exception as e:
            logger.error(f"Error creating service: {e}")
            bot.send_message(message.chat.id, f"❌ Error: {str(e)}", reply_markup=types.ReplyKeyboardRemove())

    # Add Numbers Flow
    def ask_service_name_for_add(message, edit_msg_id=None):
        with get_db_connection() as conn:
            # Get unique service names (excluding placeholder "Global" country)
            services = conn.execute("""
                SELECT DISTINCT s.name 
                FROM services s 
                ORDER BY s.name
            """).fetchall()
        
        if not services:
            bot.send_message(message.chat.id, " No services available. Create a service first!", reply_markup=types.ReplyKeyboardRemove())
            return admin_panel(message)
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        for (service_name,) in services:
            # Count total numbers across all countries for this service
            with get_db_connection() as conn:
                count = conn.execute("""
                    SELECT COUNT(*) FROM numbers n 
                    JOIN services s ON n.service_id = s.id 
                    WHERE s.name = ? AND n.status = 'active'
                """, (service_name,)).fetchone()[0]
            
            label = f"{service_name} ({count})"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"add_service_{service_name}"))
        
        markup.add(types.InlineKeyboardButton(" Create New Service", callback_data="create_new_service"))
        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_add"))
        
        send_or_edit(
            message.chat.id,
            " <b>Select Service to Add Numbers:</b>",
            reply_markup=markup,
            parse_mode='HTML',
            edit_msg_id=edit_msg_id
        )

    def ask_country_for_service(message, service_name, edit_msg_id=None):
        with get_db_connection() as conn:
            # Get countries that have this service
            countries = conn.execute("""
                SELECT s.country, s.id, COUNT(n.id) as count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id AND n.status='active' AND n.user_id IS NULL
                WHERE s.name = ? AND s.country != 'Global'
                GROUP BY s.country, s.id
                ORDER BY s.country
            """, (service_name,)).fetchall()

        markup = types.InlineKeyboardMarkup(row_width=2)
        
        text = ""
        if countries:
            text += f" <b>Available countries for {service_name}:</b>\n\n"
            for country, service_id, count in countries:
                flag = get_country_flag(country)
                label = f"{flag} {country} ({count})"
                markup.add(types.InlineKeyboardButton(label, callback_data=f"add_to_country_{service_id}"))
        else:
            text += f" <b>No countries yet for {service_name}:</b>\n\n"
        
        # Always offer to add new country
        markup.add(types.InlineKeyboardButton(" Add New Country", callback_data=f"add_new_country_{service_name}"))
        markup.add(types.InlineKeyboardButton(" Back to Services", callback_data="back_to_services"))
        
        if countries:
            text += "Choose country or add new:"
        else:
            text += "Add your first country for this service:"

        send_or_edit(
            message.chat.id,
            text,
            reply_markup=markup,
            parse_mode='HTML',
            edit_msg_id=edit_msg_id
        )

    def ask_for_numbers_file(message, country_name=None, service_name=None, service_id=None):
        flag = get_country_flag(country_name) if country_name else ""
        msg = bot.send_message(message.chat.id, f" Send numbers for {flag} {country_name} - {service_name}:\n\n You can send:\n Text message with numbers (one per line)\n .txt file with numbers\n .xlsx file (numbers in Column A)", reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_and_save_numbers, country_name, service_name, service_id)

    def process_and_save_numbers(message, country_name, service_name, service_id=None):
        numbers = []
        content = ""
        if message.document:
            if message.document.mime_type == 'text/plain':
                file_info = bot.get_file(message.document.file_id)
                content_b = bot.download_file(file_info.file_path)
                try:
                    content = content_b.decode('utf-8', errors='ignore')
                except Exception:
                    content = str(content_b)
            elif message.document.mime_type in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
                # Handle Excel file
                try:
                    import openpyxl
                    from io import BytesIO
                    
                    file_info = bot.get_file(message.document.file_id)
                    file_content = bot.download_file(file_info.file_path)
                    
                    # Load Excel file from bytes
                    workbook = openpyxl.load_workbook(BytesIO(file_content))
                    sheet = workbook.active
                    
                    # Read numbers from Column A (starting from row 1)
                    for row in sheet.iter_rows(min_row=1, min_col=1, max_col=1, values_only=True):
                        cell_value = row[0]
                        if cell_value:
                            # Convert to string and clean
                            clean = __import__('re').sub(r'\D', '', str(cell_value))
                            if clean and len(clean) > 7:
                                numbers.append(clean)
                    
                    bot.send_message(message.chat.id, f" Read {len(numbers)} numbers from Excel file...")
                except ImportError:
                    bot.send_message(message.chat.id, " Excel support not installed. Install openpyxl: pip install openpyxl")
                    return admin_panel(message)
                except Exception as e:
                    bot.send_message(message.chat.id, f" Error reading Excel file: {e}")
                    return admin_panel(message)
            else:
                bot.send_message(message.chat.id, " Invalid file type. Please upload a .txt or .xlsx file.")
                return admin_panel(message)
        elif message.text:
            content = message.text

        if content:  # Process text content
            lines = content.splitlines()
            for line in lines:
                clean = __import__('re').sub(r'\D', '', line)
                if clean and len(clean) > 7:
                    numbers.append(clean)

        if not numbers:
            bot.send_message(message.chat.id, " No valid numbers found.")
            return admin_panel(message)

        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Get or create service (match by both country and service name)
            c.execute("SELECT id FROM services WHERE UPPER(TRIM(country)) = UPPER(TRIM(?)) AND UPPER(TRIM(name)) = UPPER(TRIM(?))", (country_name, service_name))
            service = c.fetchone()

            if service:
                service_id = service[0]
            else:
                c.execute("INSERT INTO services (name, country) VALUES (?, ?)", (service_name, country_name))
                conn.commit()
                service_id = c.lastrowid

            # Check for duplicates
            existing = set()
            for row in c.execute("SELECT number FROM numbers WHERE service_id = ?", (service_id,)):
                existing.add(row[0])
            
            new_numbers = [n for n in numbers if n not in existing]
            duplicates = len(numbers) - len(new_numbers)

            # Get country flag for preview
            flag = get_country_flag(country_name, sample_numbers=numbers)

            # Show preview
            preview_text = f" <b>Preview:</b>\n\n{flag} Country: <b>{country_name}</b>\n Service: <b>{service_name}</b>\n\n"
            preview_text += f"New Numbers: <b>{len(new_numbers)}</b>\n"
            if duplicates > 0:
                preview_text += f"Duplicates (skipped): <b>{duplicates}</b>\n"
            preview_text += f"\nTotal to add: <b>{len(new_numbers)}</b>\n\n"
            
            if len(new_numbers) > 100:
                preview_text += f" <b>Warning:</b> Adding {len(new_numbers)} numbers. This may take a moment.\n\n"
            
            preview_text += "Confirm to add these numbers:"
            
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            markup.add(" Confirm", " Cancel")
            
            msg = bot.send_message(message.chat.id, preview_text, reply_markup=markup)
            bot.register_next_step_handler(msg, confirm_save_numbers, service_id, new_numbers, service_name, country_name, duplicates)

    def confirm_save_numbers(message, service_id, numbers, service_name, country_name, duplicates):
        # ReplyKeyboard button labels in this file often include leading spaces for UI.
        # Some Telegram clients (or user typing) may omit/trim those spaces, so normalize.
        raw_choice = (message.text or "").strip().lower()
        if raw_choice == "cancel":
            bot.send_message(message.chat.id, " Cancelled.", reply_markup=types.ReplyKeyboardRemove())
            return admin_panel(message)

        if raw_choice != "confirm":
            bot.send_message(message.chat.id, "⚠️ Invalid choice.", reply_markup=types.ReplyKeyboardRemove())
            return admin_panel(message)

        with get_db_connection() as conn:
            c = conn.cursor()
            try:
                c.executemany("INSERT INTO numbers (service_id, number) VALUES (?, ?)", [(service_id, num) for num in numbers])
                conn.commit()
                
                result_text = f"✅ <b>Success!</b>\n\n"
                result_text += f"Added: <b>{len(numbers)}</b>\n"
                if duplicates > 0:
                    result_text += f"Duplicates (skipped): <b>{duplicates}</b>\n"
                result_text += f"Service: <b>{service_name}</b>"
                
                bot.send_message(message.chat.id, result_text, reply_markup=types.ReplyKeyboardRemove())
                
                # Get users list before closing connection
                users = c.execute("""
                    SELECT DISTINCT user_id FROM (
                        SELECT DISTINCT user_id FROM users WHERE user_id IS NOT NULL
                        UNION
                        SELECT DISTINCT user_id FROM numbers WHERE user_id IS NOT NULL
                        UNION
                        SELECT DISTINCT user_id FROM otp_log WHERE user_id IS NOT NULL
                    ) AS all_users
                """).fetchall()
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Database Error: {e}")
                admin_panel(message)
                return
        # Automatic broadcast to all users (queued in background)
        if users:
            flag = get_country_flag(country_name)
            broadcast_text = f""" <b>New Numbers Available!</b>

{flag} <b>{country_name}</b>
 Service: <b>{service_name}</b>
 Total Added: <b>{len(numbers)}</b>

 Use /start to get your numbers!"""

            enqueue_broadcast(message.from_user.id, broadcast_text, parse_mode="HTML", title="Auto Broadcast")
            logger.info(f"Numbers added: {len(numbers)} for {country_name}-{service_name}, auto broadcast queued")

        admin_panel(message)

    # Delete Service Flow
    def select_service_to_delete(message):
        with get_db_connection() as conn:
            # Get unique service names with country counts and total numbers
            services = conn.execute("""
                SELECT s.name,
                       COUNT(DISTINCT s.country) as country_count,
                       SUM(CASE WHEN n.status='active' THEN 1 ELSE 0 END) as total_numbers,
                       SUM(n.received_otp) as total_uses
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.status = 'active'
                GROUP BY s.name
                ORDER BY s.name
            """).fetchall()

        if not services:
            bot.send_message(message.chat.id, "ℹ️ No active services to delete.")
            return admin_panel(message)

        markup = types.InlineKeyboardMarkup(row_width=1)

        text = " <b>Select a service to delete:</b>\n\n"

        for service_name, country_count, total_numbers, total_uses in services:
            markup.add(types.InlineKeyboardButton(service_name, callback_data=f"del_service_name_{service_name}"))

        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete"))

        bot.send_message(message.chat.id, text, reply_markup=markup)

    def select_service_to_reactivate(message):
        with get_db_connection() as conn:
            # Get unique inactive service names with country counts
            services = conn.execute("""
                SELECT s.name,
                       COUNT(DISTINCT s.country) as country_count,
                       SUM(CASE WHEN n.status='active' THEN 1 ELSE 0 END) as total_numbers,
                       SUM(n.received_otp) as total_uses
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.status = 'inactive'
                GROUP BY s.name
                ORDER BY s.name
            """).fetchall()

        if not services:
            bot.send_message(message.chat.id, "ℹ️ No inactive services to reactivate.")
            return admin_panel(message)

        markup = types.InlineKeyboardMarkup(row_width=1)

        text = " <b>Select a service to reactivate:</b>\n\n"

        for service_name, country_count, total_numbers, total_uses in services:
            markup.add(types.InlineKeyboardButton(service_name, callback_data=f"reactivate_service_name_{service_name}"))

        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_reactivate"))

        bot.send_message(message.chat.id, text, reply_markup=markup)

    def select_country_for_service_delete(call, service_name):
        with get_db_connection() as conn:
            # Get countries for this service
            countries = conn.execute("""
                SELECT s.country, s.id, COUNT(n.id) as num_count,
                       SUM(n.received_otp) as total_uses,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.name = ? AND s.status = 'active'
                GROUP BY s.country, s.id
                ORDER BY s.country
            """, (service_name,)).fetchall()

        if not countries:
            bot.edit_message_text(" No active countries found for this service.", call.message.chat.id, call.message.message_id)
            return

        markup = types.InlineKeyboardMarkup(row_width=1)

        text = f" <b>Select country for {service_name}:</b>\n\n"

        for country, service_id, num_count, total_uses, assigned_count in countries:
            flag = get_country_flag(country)
            total_uses = int(total_uses) if total_uses else 0
            assigned_count = int(assigned_count) if assigned_count else 0
            label = f"{flag} {country} - {num_count} numbers ({assigned_count} assigned)"
            text += f"  {label}\n    Uses: {total_uses}\n\n"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"del_country_{service_id}"))

        markup.add(types.InlineKeyboardButton(" Back to Services", callback_data="back_to_service_delete"))
        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete"))

        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

    def select_country_for_service_reactivate(call, service_name):
        with get_db_connection() as conn:
            # Get inactive countries for this service
            countries = conn.execute("""
                SELECT s.country, s.id, COUNT(n.id) as num_count,
                       SUM(n.received_otp) as total_uses,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.name = ? AND s.status = 'inactive'
                GROUP BY s.country, s.id
                ORDER BY s.country
            """, (service_name,)).fetchall()

        if not countries:
            bot.edit_message_text(" No inactive countries found for this service.", call.message.chat.id, call.message.message_id)
            return

        markup = types.InlineKeyboardMarkup(row_width=1)

        text = f" <b>Select country to reactivate for {service_name}:</b>\n\n"

        for country, service_id, num_count, total_uses, assigned_count in countries:
            flag = get_country_flag(country)
            total_uses = int(total_uses) if total_uses else 0
            assigned_count = int(assigned_count) if assigned_count else 0
            label = f"{flag} {country} - {num_count} numbers ({assigned_count} assigned)"
            text += f"  {label}\n    Uses: {total_uses}\n\n"
            markup.add(types.InlineKeyboardButton(label, callback_data=f"reactivate_country_{service_id}"))

        markup.add(types.InlineKeyboardButton(" Back to Services", callback_data="back_to_service_reactivate"))
        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_reactivate"))

        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

    def show_delete_methods_for_service_name(call, service_name):
        # Get service-wide statistics
        with get_db_connection() as conn:
            stats = conn.execute("""
                SELECT COUNT(n.id) as total_numbers,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count,
                       SUM(n.received_otp) as total_uses,
                       COUNT(DISTINCT s.country) as country_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.name = ?
            """, (service_name,)).fetchone()

        total_numbers, assigned_count, total_uses, country_count = stats
        total_numbers = int(total_numbers) if total_numbers else 0
        assigned_count = int(assigned_count) if assigned_count else 0
        total_uses = int(total_uses) if total_uses else 0
        country_count = int(country_count) if country_count else 0

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(" HARD DELETE", callback_data=f"hard_delete_service_{service_name}"),
            types.InlineKeyboardButton(" SOFT DELETE", callback_data=f"soft_delete_service_{service_name}")
        )
        markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete"))

        text = f" <b>DELETE SERVICE: {service_name}</b>\n\n"
        text += f"Countries: {country_count}\n"
        text += f"Numbers: {total_numbers} total, {assigned_count} assigned\n"
        text += f"Total Usage: {total_uses} times\n\n"

        text += "<b>Choose deletion method:</b>\n\n"
        text += " <b>HARD DELETE</b> - Remove service entirely\n"
        text += " <b>SOFT DELETE</b> - Deactivate all countries"

        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("del_service_name_"))
    def handle_service_name_selection(call):
        service_name = call.data.replace("del_service_name_", "")

        # Check if service has any active countries
        with get_db_connection() as conn:
            active_countries = conn.execute("SELECT COUNT(*) FROM services WHERE name = ? AND status = 'active'", (service_name,)).fetchone()[0]

        if active_countries > 0:
            # Service has active countries - show country selection
            select_country_for_service_delete(call, service_name)
        else:
            # Service has no active countries - show delete methods directly
            show_delete_methods_for_service_name(call, service_name)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("del_country_"))
    def handle_country_selection_for_delete(call):
        service_id = int(call.data.replace("del_country_", ""))

        with get_db_connection() as conn:
            service = conn.execute("""
                SELECT s.name, s.country, COUNT(n.id) as num_count,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count,
                       SUM(n.received_otp) as total_uses
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.id = ?
            """, (service_id,)).fetchone()

        if service:
            name, country, num_count, assigned_count, total_uses = service
            flag = get_country_flag(country)
            total_uses = int(total_uses) if total_uses else 0
            assigned_count = int(assigned_count) if assigned_count else 0

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton(" HARD DELETE", callback_data=f"hard_delete_{service_id}"),
                types.InlineKeyboardButton(" SOFT DELETE", callback_data=f"soft_delete_{service_id}")
            )
            markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete"))

            text = f" <b>DELETE SERVICE INSTANCE</b>\n\n"
            text += f"Service: <b>{name}</b> ({flag} {country})\n"
            text += f"Numbers: {num_count} total, {assigned_count} assigned\n"
            text += f"Total Usage: {total_uses} times\n\n"

            text += "<b>Choose deletion method:</b>\n\n"
            text += " <b>HARD DELETE</b> - Remove service & all numbers permanently\n"
            text += " <b>SOFT DELETE</b> - Deactivate service (users can't get new numbers)"

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("hard_delete_"))
    def handle_hard_delete(call):
        service_id = int(call.data.replace("hard_delete_", ""))

        with get_db_connection() as conn:
            service = conn.execute("""
                SELECT s.name, s.country, COUNT(n.id) as num_count,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.id = ?
            """, (service_id,)).fetchone()

        if service:
            name, country, num_count, assigned_count = service
            flag = get_country_flag(country)
            assigned_count = int(assigned_count) if assigned_count else 0

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton(" YES, DELETE PERMANENTLY", callback_data=f"confirm_hard_delete_{service_id}"),
                types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete")
            )

            text = f" <b>CONFIRM HARD DELETE</b>\n\n"
            text += f"Target: <b>{name}</b> ({flag} {country})\n"
            text += f"Action: Permanent deletion\n\n"
            text += f"IMPACT:\n"
            text += f" {num_count} numbers will be deleted\n"
            text += f" {assigned_count} users will lose access\n"
            text += f" All service data will be lost forever\n\n"
            text += f" This action CANNOT be undone!"

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("soft_delete_"))
    def handle_soft_delete(call):
        service_id = int(call.data.replace("soft_delete_", ""))

        with get_db_connection() as conn:
            service = conn.execute("""
                SELECT s.name, s.country, COUNT(n.id) as num_count,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.id = ?
            """, (service_id,)).fetchone()

        if service:
            name, country, num_count, assigned_count = service
            flag = get_country_flag(country)
            assigned_count = int(assigned_count) if assigned_count else 0

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton(" YES, DEACTIVATE", callback_data=f"confirm_soft_delete_{service_id}"),
                types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete")
            )

            text = f" <b>CONFIRM SOFT DELETE</b>\n\n"
            text += f"Target: <b>{name}</b> ({flag} {country})\n"
            text += f"Action: Deactivate service\n\n"
            text += f"WHAT HAPPENS:\n"
            text += f" Service hidden from user selection\n"
            text += f" No new number assignments allowed\n"
            text += f" Existing assigned numbers remain active\n"
            text += f"â° Numbers expire naturally (no auto-release)\n"
            text += f" Can reactivate service later\n\n"
            text += f"Active numbers: {assigned_count}"

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("hard_delete_service_"))
    def handle_hard_delete_service(call):
        service_name = call.data.replace("hard_delete_service_", "")

        with get_db_connection() as conn:
            stats = conn.execute("""
                SELECT COUNT(n.id) as total_numbers,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count,
                       COUNT(DISTINCT s.country) as country_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.name = ?
            """, (service_name,)).fetchone()

        total_numbers, assigned_count, country_count = stats
        total_numbers = int(total_numbers) if total_numbers else 0
        assigned_count = int(assigned_count) if assigned_count else 0
        country_count = int(country_count) if country_count else 0

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(" YES, DELETE ENTIRE SERVICE", callback_data=f"confirm_hard_delete_service_{service_name}"),
            types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete")
        )

        text = f" <b>CONFIRM HARD DELETE - ENTIRE SERVICE</b>\n\n"
        text += f"Service: <b>{service_name}</b>\n"
        text += f"Countries: {country_count}\n"
        text += f"Numbers: {total_numbers} total, {assigned_count} assigned\n\n"
        text += f"IMPACT:\n"
        text += f" {total_numbers} numbers will be deleted\n"
        text += f" {assigned_count} users will lose access\n"
        text += f" Service will be completely removed\n\n"
        text += f" This action CANNOT be undone!"

        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("soft_delete_service_"))
    def handle_soft_delete_service(call):
        service_name = call.data.replace("soft_delete_service_", "")

        with get_db_connection() as conn:
            stats = conn.execute("""
                SELECT COUNT(n.id) as total_numbers,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count,
                       COUNT(DISTINCT s.country) as country_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.name = ?
            """, (service_name,)).fetchone()

        total_numbers, assigned_count, country_count = stats
        total_numbers = int(total_numbers) if total_numbers else 0
        assigned_count = int(assigned_count) if assigned_count else 0
        country_count = int(country_count) if country_count else 0

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(" YES, DEACTIVATE ENTIRE SERVICE", callback_data=f"confirm_soft_delete_service_{service_name}"),
            types.InlineKeyboardButton(" Cancel", callback_data="cancel_delete")
        )

        text = f" <b>CONFIRM SOFT DELETE - ENTIRE SERVICE</b>\n\n"
        text += f"Service: <b>{service_name}</b>\n"
        text += f"Countries: {country_count}\n"
        text += f"Numbers: {total_numbers} total, {assigned_count} assigned\n\n"
        text += f"WHAT HAPPENS:\n"
        text += f" Service hidden from all countries\n"
        text += f" No new number assignments allowed\n"
        text += f" Existing assigned numbers remain active\n"
        text += f"â° Numbers expire naturally\n"
        text += f" Can reactivate entire service later\n\n"
        text += f"Active numbers: {assigned_count}"

        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_hard_delete_service_"))
    def confirm_hard_delete_service(call):
        service_name = call.data.replace("confirm_hard_delete_service_", "")

        try:
            with get_db_connection() as conn:
                # Get stats before deletion
                stats = conn.execute("""
                    SELECT COUNT(n.id) as total_numbers, COUNT(DISTINCT s.country) as country_count
                    FROM services s
                    LEFT JOIN numbers n ON s.id = n.service_id
                    WHERE s.name = ?
                """, (service_name,)).fetchone()

                total_numbers, country_count = stats
                total_numbers = int(total_numbers) if total_numbers else 0
                country_count = int(country_count) if country_count else 0

                # Delete all service entries and their numbers
                service_ids = conn.execute("SELECT id FROM services WHERE name = ?", (service_name,)).fetchall()
                deleted_numbers = 0

                for (service_id,) in service_ids:
                    num_count = conn.execute("SELECT COUNT(*) FROM numbers WHERE service_id = ?", (service_id,)).fetchone()[0]
                    deleted_numbers += num_count
                    conn.execute("DELETE FROM numbers WHERE service_id = ?", (service_id,))

                conn.execute("DELETE FROM services WHERE name = ?", (service_name,))
                conn.commit()

            text = f" <b>SERVICE COMPLETELY DELETED</b>\n\n"
            text += f"Service: {service_name}\n"
            text += f"Countries Removed: {country_count}\n"
            text += f"Numbers Deleted: {deleted_numbers}"

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service_name}' completely deleted with {country_count} countries and {deleted_numbers} numbers")

        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Hard delete service error: {e}")

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_soft_delete_service_"))
    def confirm_soft_delete_service(call):
        service_name = call.data.replace("confirm_soft_delete_service_", "")

        try:
            with get_db_connection() as conn:
                # Get stats before deactivation
                stats = conn.execute("""
                    SELECT COUNT(n.id) as total_numbers,
                           COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count,
                           COUNT(DISTINCT s.country) as country_count
                    FROM services s
                    LEFT JOIN numbers n ON s.id = n.service_id
                    WHERE s.name = ?
                """, (service_name,)).fetchone()

                total_numbers, assigned_count, country_count = stats
                total_numbers = int(total_numbers) if total_numbers else 0
                assigned_count = int(assigned_count) if assigned_count else 0
                country_count = int(country_count) if country_count else 0

                # Set all service entries to inactive
                conn.execute("UPDATE services SET status = 'inactive' WHERE name = ?", (service_name,))
                conn.commit()

            text = f" <b>SERVICE DEACTIVATED</b>\n\n"
            text += f"Service: {service_name}\n"
            text += f"Countries Deactivated: {country_count}\n"
            text += f"Numbers Preserved: {total_numbers}\n"
            text += f"Active Assignments: {assigned_count}\n\n"
            text += f"Users can no longer get new numbers,\n"
            text += f"but existing assignments remain active."

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service_name}' soft deleted - {country_count} countries deactivated")

        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Soft delete service error: {e}")

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_hard_delete_"))
    def confirm_hard_delete(call):
        answer_cbq(call)
        service_id = int(call.data.replace("confirm_hard_delete_", ""))

        try:
            with get_db_connection() as conn:
                service = conn.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
                num_count = conn.execute("SELECT COUNT(*) FROM numbers WHERE service_id=?", (service_id,)).fetchone()[0]

                conn.execute("DELETE FROM services WHERE id = ?", (service_id,))
                conn.commit()

            text = f" <b>HARD DELETE COMPLETED</b>\n\n"
            text += f"Service: {service[0]} ({service[1]})\n"
            text += f"Numbers Deleted: {num_count}\n"
            text += f"Status: Permanently removed"

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service[0]}' (ID: {service_id}) hard deleted with {num_count} numbers")

        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Hard delete service error: {e}")

        # Callback query might be stale by the time we finish DB work; don't crash polling.
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_soft_delete_"))
    def confirm_soft_delete(call):
        answer_cbq(call)
        service_id = int(call.data.replace("confirm_soft_delete_", ""))

        try:
            with get_db_connection() as conn:
                service = conn.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
                num_count = conn.execute("SELECT COUNT(*) FROM numbers WHERE service_id=?", (service_id,)).fetchone()[0]

                conn.execute("UPDATE services SET status = 'inactive' WHERE id = ?", (service_id,))
                conn.commit()

            text = f" <b>SOFT DELETE COMPLETED</b>\n\n"
            text += f"Service: {service[0]} ({service[1]})\n"
            text += f"Numbers: {num_count} (preserved)\n"
            text += f"Status: Deactivated\n\n"
            text += f"Users can no longer get new numbers from this service,\n"
            text += f"but existing assignments remain active."

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service[0]}' (ID: {service_id}) soft deleted - status set to inactive")

        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Soft delete service error: {e}")

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_del_"))
    def confirm_delete_service(call):
        service_id = int(call.data.replace("confirm_del_", ""))
        
        try:
            with get_db_connection() as conn:
                service = conn.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
                num_count = conn.execute("SELECT COUNT(*) FROM numbers WHERE service_id=?", (service_id,)).fetchone()[0]
                
                conn.execute("DELETE FROM services WHERE id = ?", (service_id,))
                conn.commit()
            
            text = f" <b>Service Deleted!</b>\n\n"
            text += f"Service: {service[0]} ({service[1]})\n"
            text += f"Numbers Deleted: {num_count}"
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service[0]}' (ID: {service_id}) deleted with {num_count} numbers")
            
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Delete service error: {e}")
        
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("reactivate_service_name_"))
    def handle_reactivate_service_name(call):
        service_name = call.data.replace("reactivate_service_name_", "")
        select_country_for_service_reactivate(call, service_name)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("reactivate_country_"))
    def handle_reactivate_country(call):
        service_id = int(call.data.replace("reactivate_country_", ""))

        with get_db_connection() as conn:
            service = conn.execute("""
                SELECT s.name, s.country, COUNT(n.id) as num_count,
                       COUNT(CASE WHEN n.user_id IS NOT NULL THEN 1 END) as assigned_count
                FROM services s
                LEFT JOIN numbers n ON s.id = n.service_id
                WHERE s.id = ?
            """, (service_id,)).fetchone()

        if service:
            name, country, num_count, assigned_count = service
            flag = get_country_flag(country)
            assigned_count = int(assigned_count) if assigned_count else 0

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(types.InlineKeyboardButton(" YES, REACTIVATE", callback_data=f"confirm_reactivate_{service_id}"))
            markup.add(types.InlineKeyboardButton(" Cancel", callback_data="cancel_reactivate"))

            text = f" <b>REACTIVATE SERVICE</b>\n\n"
            text += f"Service: <b>{name}</b> ({flag} {country})\n"
            text += f"Numbers: {num_count} total, {assigned_count} assigned\n\n"
            text += f"This will make the service available for new number assignments again."

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_reactivate_"))
    def confirm_reactivate_service(call):
        service_id = int(call.data.replace("confirm_reactivate_", ""))

        try:
            with get_db_connection() as conn:
                service = conn.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
                num_count = conn.execute("SELECT COUNT(*) FROM numbers WHERE service_id=?", (service_id,)).fetchone()[0]

                conn.execute("UPDATE services SET status = 'active' WHERE id = ?", (service_id,))
                conn.commit()

            text = f" <b>SERVICE REACTIVATED</b>\n\n"
            text += f"Service: {service[0]} ({service[1]})\n"
            text += f"Numbers: {num_count}\n"
            text += f"Status: Active\n\n"
            text += f"Users can now get new numbers from this service."

            bot.edit_message_text(text, call.message.chat.id, call.message.message_id)
            logger.info(f"Service '{service[0]}' (ID: {service_id}) reactivated")

        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", call.message.chat.id, call.message.message_id)
            logger.error(f"Reactivate service error: {e}")

        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_service_delete")
    def back_to_service_delete(call):
        # Re-run the service selection
        select_service_to_delete(call.message)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_service_reactivate")
    def back_to_service_reactivate(call):
        # Re-run the service reactivation selection
        select_service_to_reactivate(call.message)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "cancel_reactivate")
    def cancel_reactivate_service(call):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        admin_panel(call.message)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "cancel_delete")
    def cancel_delete_service(call):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        admin_panel(call.message)
        answer_cbq(call)

    # Top Users and Ban/Unban
    # Removed show_ban_unban_menu

    def show_bot_settings(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add('📢 OTP Group', '📡 Subscription Targets')
        markup.add('🏷️ Bot Name', '🤖 Forwarder Bot')
        markup.add('🔢 Numbers Limit', '⏱️ Cooldown Seconds')
        markup.add('📩 SMS Limit', '🕒 Sub Check Hours')
        markup.add('🔁 Queue Timer')
        markup.add('⬅️ Back to Panel')
        bot.send_message(message.chat.id, '⚙️ <b>Bot Settings</b>\n\nSelect an option:', reply_markup=markup, parse_mode='HTML')



    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'OTP Group'))
    def open_otp_group_settings(message):
        show_otp_group_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and (admin_text_is(m, 'Subscription Targets') or admin_text_is(m, 'Subscription Channel')))
    def open_subscription_channel_settings(message):
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Bot Name'))
    def open_bot_name_settings(message):
        show_bot_name_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Bot Token'))
    def open_bot_token_settings(message):
        show_bot_token_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and (admin_text_is(m, 'Forwarder Bot') or admin_text_is(m, 'Forwarder Panel')))
    def open_forwarder_panel(message):
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Numbers Limit'))
    def open_numbers_limit_settings(message):
        show_numbers_limit_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Cooldown Seconds'))
    def open_cooldown_settings(message):
        show_cooldown_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'SMS Limit'))
    def open_sms_limit_settings(message):
        show_sms_limit_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Sub Check Hours'))
    def open_sub_check_hours_settings(message):
        show_sub_check_hours_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Queue Timer'))
    def open_queue_timer_settings(message):
        show_queue_timer_menu(message)

    def show_otp_group_settings(message):
        from core import OTP_GROUP_URL
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add('➕ Add OTP Group', '✏️ Change OTP Group')
        markup.add('📌 Current OTP Group', '⬅️ Back to Settings')
        current_group = OTP_GROUP_URL if OTP_GROUP_URL else 'Not set'
        bot.send_message(message.chat.id, f'📢 <b>OTP Group Management</b>\n\nCurrent link:\n<code>{current_group}</code>', reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Add OTP Group'))
    def add_otp_group_prompt(message):
        msg = bot.send_message(message.chat.id, '🔗 Enter OTP Group link:\n\nExample: https://t.me/+xxxxxxxxxxxx\nOr: https://t.me/groupname\n\nType `cancel` to go back.', reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_add_otp_group)

    def process_add_otp_group(message):
        group_url = (message.text or "").strip()
        if is_cancel_text(group_url):
            return show_otp_group_settings(message)
        if not group_url:
            bot.send_message(message.chat.id, '⚠️ OTP Group link cannot be empty.')
            return show_otp_group_settings(message)

        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()

        if 'OTP_GROUP_URL=' in env_content:
            env_content = re.sub(r'OTP_GROUP_URL=.*', f'OTP_GROUP_URL={group_url}', env_content)
        else:
            env_content += f'\nOTP_GROUP_URL={group_url}'

        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)

        logger.info(f'OTP Group updated by {message.from_user.id}: {group_url}')
        from core import reload_config
        reload_config()
        bot.send_message(message.chat.id, f'✅ OTP Group updated:\n\n<code>{group_url}</code>\n\nSettings applied instantly.', reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_otp_group_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Change OTP Group'))
    def change_otp_group_prompt(message):
        msg = bot.send_message(message.chat.id, '✏️ Enter new OTP Group link:\n\nType `cancel` to go back.', reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_add_otp_group)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Current OTP Group'))
    def show_current_otp_group(message):
        from core import OTP_GROUP_URL
        current_group = OTP_GROUP_URL if OTP_GROUP_URL else 'Not set'
        bot.send_message(message.chat.id, f'📌 Current OTP Group:\n\n<code>{current_group}</code>', reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_otp_group_settings(message)

    def show_channel_settings(message):
        with get_db_connection() as conn:
            targets = conn.execute('SELECT id FROM channels').fetchall()

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add('➕ Add Target', '📋 View Targets')
        markup.add('✏️ Edit Target', '🗑️ Remove Target')
        markup.add('🧪 Test Target', '⬅️ Back to Settings')

        bot.send_message(
            message.chat.id,
            f'📡 <b>Subscription Targets</b>\n\nTotal targets: <b>{len(targets)}</b>\n\n'
            f'Add channels/groups where user must join.',
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and (admin_text_is(m, 'Add Target') or admin_text_is(m, 'Add Channel')))
    def add_target_prompt(message):
        msg = bot.send_message(
            message.chat.id,
            '🧩 Step 1/3\nEnter button name users will see.\n\nExample: Join Main Channel\n\nType `cancel` to stop.',
            reply_markup=types.ReplyKeyboardRemove()
        )
        bot.register_next_step_handler(msg, process_target_button_name)

    def process_target_button_name(message):
        button_name = (message.text or '').strip()
        if is_cancel_text(button_name):
            return show_channel_settings(message)
        if len(button_name) < 2:
            bot.send_message(message.chat.id, '⚠️ Button name must be at least 2 characters.')
            return show_channel_settings(message)

        msg = bot.send_message(
            message.chat.id,
            '🧩 Step 2/3\nEnter target identifier.\n\n'
            'Supported:\n'
            '- Numeric chat ID (e.g. -1001234567890)\n'
            '- @username\n'
            '- t.me/username (or https://t.me/username)\n\n'
            'Type `cancel` to stop.'
        )
        bot.register_next_step_handler(msg, process_target_identifier, button_name)

    def process_target_identifier(message, button_name):
        raw_target = (message.text or '').strip()
        if is_cancel_text(raw_target):
            return show_channel_settings(message)
        target_identifier, err = normalize_target_identifier(raw_target)
        if err:
            bot.send_message(message.chat.id, f'⚠️ Invalid target: {err}')
            return show_channel_settings(message)

        validation = validate_subscription_target(target_identifier)
        if not validation["ok"]:
            bot.send_message(
                message.chat.id,
                f"❌ Target check failed:\n{validation['error']}\n\n"
                f"Please add bot as admin/member in that channel/group, then try again."
            )
            return show_channel_settings(message)

        msg = bot.send_message(
            message.chat.id,
            '🧩 Step 3/3\nEnter join link users will click.\n\n'
            'Supported:\n'
            '- https://t.me/...\n'
            '- t.me/...\n'
            '- @username\n\n'
            'Type `cancel` to stop.'
        )
        bot.register_next_step_handler(msg, process_target_join_link, button_name, validation)

    def process_target_join_link(message, button_name, validation):
        raw_link = (message.text or '').strip()
        if is_cancel_text(raw_link):
            return show_channel_settings(message)
        invite_link, err = normalize_join_link(raw_link)
        if err:
            bot.send_message(message.chat.id, f'⚠️ Invalid join link: {err}')
            return show_channel_settings(message)

        target_identifier = validation["resolved_identifier"] or ""
        try:
            with get_db_connection() as conn:
                conn.execute(
                    'INSERT INTO channels (name, channel_identifier, invite_link) VALUES (?, ?, ?)',
                    (button_name, target_identifier, invite_link)
                )
                conn.commit()
            bump_channels_version()
            logger.info(
                f"Target added by {message.from_user.id}: {button_name} "
                f"(ID: {target_identifier}, Link: {invite_link})"
            )
            bot.send_message(
                message.chat.id,
                f"✅ Target added.\n\n"
                f"Button: <b>{button_name}</b>\n"
                f"Target: <code>{target_identifier}</code>\n"
                f"Type: {validation['chat_type']}\n"
                f"Detected: {validation['title']}",
                parse_mode='HTML'
            )
        except Exception as e:
            text = str(e)
            if 'UNIQUE constraint failed: channels.channel_identifier' in text:
                bot.send_message(message.chat.id, 'ℹ️ This target is already added.')
            else:
                bot.send_message(message.chat.id, f'❌ Error adding target: {text}')
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and (admin_text_is(m, 'View Targets') or admin_text_is(m, 'View All Channels')))
    def view_all_channels(message):
        with get_db_connection() as conn:
            targets = conn.execute(
                'SELECT id, name, channel_identifier, invite_link FROM channels ORDER BY id'
            ).fetchall()

        if not targets:
            bot.send_message(message.chat.id, 'ℹ️ No subscription targets configured.', reply_markup=types.ReplyKeyboardRemove())
            return show_channel_settings(message)

        text = '📋 <b>Subscription Targets</b>\n\n'
        for idx, (ch_id, name, target, link) in enumerate(targets, 1):
            check = validate_subscription_target(target)
            status = 'VALID' if check['ok'] else 'NEEDS FIX'
            text += (
                f'{idx}. <b>{name}</b>\n'
                f'   Target: <code>{target}</code>\n'
                f'   Link: {link}\n'
                f'   Status: {status}\n\n'
            )

        bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=types.ReplyKeyboardRemove())
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Edit Target'))
    def edit_target_prompt(message):
        with get_db_connection() as conn:
            targets = conn.execute(
                'SELECT id, name, channel_identifier, invite_link FROM channels ORDER BY id'
            ).fetchall()

        if not targets:
            bot.send_message(message.chat.id, 'ℹ️ No targets to edit.', reply_markup=types.ReplyKeyboardRemove())
            return show_channel_settings(message)

        text = '✏️ Select target number to edit:\n\n'
        for idx, (_, name, target, _) in enumerate(targets, 1):
            text += f'{idx}. {name} ({target})\n'
        text += '\nType `0` or `cancel` to go back.'
        msg = bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_edit_target_choice, targets)

    def process_edit_target_choice(message, targets):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        try:
            choice = int(raw)
        except Exception:
            bot.send_message(message.chat.id, '⚠️ Invalid choice.')
            return show_channel_settings(message)

        if not (1 <= choice <= len(targets)):
            bot.send_message(message.chat.id, '⚠️ Invalid target number.')
            return show_channel_settings(message)

        target_row = targets[choice - 1]
        ch_id, name, target_identifier, invite_link = target_row
        msg = bot.send_message(
            message.chat.id,
            f'Editing: {name}\n\n'
            f'Enter new button name (or "-" to keep current):\n'
            f'Type `cancel` to stop.',
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_edit_target_name, ch_id, name, target_identifier, invite_link)

    def process_edit_target_name(message, ch_id, old_name, old_target, old_link):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        new_name = old_name if raw == '-' else raw
        if len(new_name) < 2:
            bot.send_message(message.chat.id, '⚠️ Invalid button name.')
            return show_channel_settings(message)

        msg = bot.send_message(
            message.chat.id,
            'Enter new target identifier (or "-" to keep current):\nType `cancel` to stop.',
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_edit_target_identifier, ch_id, new_name, old_target, old_link)

    def process_edit_target_identifier(message, ch_id, new_name, old_target, old_link):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        if raw == '-':
            new_target = old_target
            validation = validate_subscription_target(new_target)
            if not validation["ok"]:
                bot.send_message(message.chat.id, f"⚠️ Current target has issue: {validation['error']}")
        else:
            new_target, err = normalize_target_identifier(raw)
            if err:
                bot.send_message(message.chat.id, f'⚠️ Invalid target: {err}')
                return show_channel_settings(message)

            validation = validate_subscription_target(new_target)
            if not validation["ok"]:
                bot.send_message(
                    message.chat.id,
                    f"❌ Target check failed:\n{validation['error']}\n\n"
                    f"Please ensure bot has access, then retry."
                )
                return show_channel_settings(message)
            new_target = validation["resolved_identifier"] or new_target

        msg = bot.send_message(
            message.chat.id,
            'Enter new join link (or "-" to keep current):\nType `cancel` to stop.',
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_edit_target_link, ch_id, new_name, new_target, old_link)

    def process_edit_target_link(message, ch_id, new_name, new_target, old_link):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        if raw == '-':
            new_link = old_link
        else:
            new_link, err = normalize_join_link(raw)
            if err:
                bot.send_message(message.chat.id, f'⚠️ Invalid join link: {err}')
                return show_channel_settings(message)

        try:
            with get_db_connection() as conn:
                conn.execute(
                    'UPDATE channels SET name = ?, channel_identifier = ?, invite_link = ? WHERE id = ?',
                    (new_name, new_target, new_link, ch_id)
                )
                conn.commit()
            bump_channels_version()
            bot.send_message(message.chat.id, '✅ Target updated successfully.', reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            text = str(e)
            if 'UNIQUE constraint failed: channels.channel_identifier' in text:
                bot.send_message(message.chat.id, '⚠️ Another target already uses this identifier.', reply_markup=types.ReplyKeyboardRemove())
            else:
                bot.send_message(message.chat.id, f'❌ Update failed: {text}', reply_markup=types.ReplyKeyboardRemove())
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and (admin_text_is(m, 'Remove Target') or admin_text_is(m, 'Remove Channel')))
    def remove_channel_prompt(message):
        with get_db_connection() as conn:
            targets = conn.execute('SELECT id, name, channel_identifier FROM channels ORDER BY id').fetchall()

        if not targets:
            bot.send_message(message.chat.id, 'ℹ️ No targets to remove.', reply_markup=types.ReplyKeyboardRemove())
            return show_channel_settings(message)

        text = '🗑️ Select target number to remove:\n\n'
        for idx, (_, name, target) in enumerate(targets, 1):
            text += f'{idx}. {name} ({target})\n'
        text += '\nType `0` or `cancel` to go back.'
        msg = bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_remove_channel, targets)

    def process_remove_channel(message, targets):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        try:
            choice = int(raw)
            if 1 <= choice <= len(targets):
                ch_id, name, target = targets[choice - 1]
                with get_db_connection() as conn:
                    conn.execute('DELETE FROM channels WHERE id = ?', (ch_id,))
                    conn.commit()
                bump_channels_version()
                logger.info(f'Target removed by {message.from_user.id}: {name} ({target})')
                bot.send_message(message.chat.id, f'✅ Target removed: {name}', reply_markup=types.ReplyKeyboardRemove())
            else:
                bot.send_message(message.chat.id, '⚠️ Invalid target number.', reply_markup=types.ReplyKeyboardRemove())
        except Exception:
            bot.send_message(message.chat.id, '⚠️ Invalid input.', reply_markup=types.ReplyKeyboardRemove())
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Test Target'))
    def test_target_prompt(message):
        with get_db_connection() as conn:
            targets = conn.execute('SELECT id, name, channel_identifier FROM channels ORDER BY id').fetchall()

        if not targets:
            bot.send_message(message.chat.id, 'ℹ️ No targets configured to test.', reply_markup=types.ReplyKeyboardRemove())
            return show_channel_settings(message)

        text = '🧪 Select target number to test:\n\n'
        for idx, (_, name, target) in enumerate(targets, 1):
            text += f'{idx}. {name} ({target})\n'
        text += '\nType `0` or `cancel` to go back.'
        msg = bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_test_target_choice, targets)

    def process_test_target_choice(message, targets):
        raw = (message.text or '').strip()
        if is_cancel_text(raw):
            return show_channel_settings(message)
        try:
            choice = int(raw)
            if not (1 <= choice <= len(targets)):
                bot.send_message(message.chat.id, '⚠️ Invalid target number.')
                return show_channel_settings(message)

            _, name, target = targets[choice - 1]
            result = validate_subscription_target(target)
            if result["ok"]:
                bot.send_message(
                    message.chat.id,
                    f"✅ Test passed.\n\nName: {name}\nTarget: {target}\nType: {result['chat_type']}\nDetected: {result['title']}"
                )
            else:
                bot.send_message(
                    message.chat.id,
                    f"❌ Test failed.\n\nName: {name}\nTarget: {target}\nReason: {result['error']}"
                )
        except Exception as e:
            bot.send_message(message.chat.id, f'❌ Test error: {e}')
        show_channel_settings(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, 'Back to Settings'))
    def back_to_bot_settings(message):
        show_bot_settings(message)

    def show_bot_token_menu(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("👁️ View Token", "✏️ Change Token")
        markup.add("⬅️ Back to Settings")
        bot.send_message(
            message.chat.id,
            "🔑 <b>Bot Token Management</b>\n\n"
            "Changes are applied instantly for new requests.\n"
            "Select an option:",
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Token"))
    def show_bot_token(message):
        from core import BOT_TOKEN
        masked_token = BOT_TOKEN[:10] + "***" + BOT_TOKEN[-10:] if len(BOT_TOKEN) > 20 else "***"
        bot.send_message(message.chat.id, f"🔐 <b>Current Token:</b>\n\n<code>{masked_token}</code>\n\n(Token is masked for security)", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_bot_token_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Token"))
    def change_token_prompt(message):
        msg = bot.send_message(message.chat.id, "✏️ Enter new bot token:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_change_token)

    def process_change_token(message):
        new_token = (message.text or "").strip()
        if is_cancel_text(new_token):
            return show_bot_token_menu(message)
        
        if not new_token or len(new_token) < 20:
            bot.send_message(message.chat.id, "⚠️ Invalid token! Token too short.")
            return change_token_prompt(message)
        
        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()
        
        if 'BOT_TOKEN=' in env_content:
            env_content = re.sub(r'BOT_TOKEN=.*', f'BOT_TOKEN={new_token}', env_content)
        else:
            env_content += f'\nBOT_TOKEN={new_token}'
        
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        
        from core import reload_config
        reload_config()
        apply_live_bot_token(new_token)
        logger.info(f"Bot token changed by {message.from_user.id} (live apply attempted)")
        bot.send_message(
            message.chat.id,
            "✅ Bot token updated.\n\nApplied instantly for new bot requests.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        show_bot_token_menu(message)

    def show_forwarder_token_menu(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("👁️ View Forwarder Token", "✏️ Change Forwarder Token")
        markup.add("⬅️ Back to Forwarder Panel")
        markup.add("⬅️ Back to Settings")
        bot.send_message(
            message.chat.id,
            "🤖 <b>Forwarder Bot Token</b>\n\n"
            "This token is used only for sending OTP messages.\n"
            "Select an option:",
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Forwarder Token"))
    def open_forwarder_token_menu_from_panel(message):
        show_forwarder_token_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Forwarder Config"))
    def view_forwarder_config(message):
        groups = load_forwarder_group_ids()
        number_bot_link = get_bot_config_value("forwarder_number_bot_link", "")
        support_group_link = get_bot_config_value("forwarder_support_group_link", "")
        groups_text = "\n".join(f"- <code>{gid}</code>" for gid in groups) if groups else "No groups added"
        text = (
            "📌 <b>Forwarder Config</b>\n\n"
            f"<b>Group IDs ({len(groups)}):</b>\n{groups_text}\n\n"
            f"<b>Number Bot Link:</b>\n{number_bot_link or 'Not set'}\n\n"
            f"<b>Support Group Link:</b>\n{support_group_link or 'Not set'}"
        )
        bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=types.ReplyKeyboardRemove())
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Add Group ID"))
    def add_group_id_prompt(message):
        msg = bot.send_message(
            message.chat.id,
            "➕ Enter group chat ID to add.\n\nExample: -1001234567890\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_add_group_id)

    def process_add_group_id(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_forwarder_panel(message)
        try:
            group_id = int(raw)
        except Exception:
            bot.send_message(message.chat.id, "⚠️ Invalid group ID.")
            return add_group_id_prompt(message)

        groups = load_forwarder_group_ids()
        if group_id in groups:
            bot.send_message(message.chat.id, "ℹ️ Group ID already exists.")
            return show_forwarder_panel(message)

        groups.append(group_id)
        if save_forwarder_group_ids(groups):
            bot.send_message(message.chat.id, f"✅ Group ID added: <code>{group_id}</code>", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to save group ID.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Remove Group ID"))
    def remove_group_id_prompt(message):
        groups = load_forwarder_group_ids()
        if not groups:
            bot.send_message(message.chat.id, "ℹ️ No group IDs found.")
            return show_forwarder_panel(message)
        text = "🗑️ Enter group ID to remove:\n\n" + "\n".join(f"- <code>{gid}</code>" for gid in groups)
        text += "\n\nType `cancel` to go back."
        msg = bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_remove_group_id)

    def process_remove_group_id(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_forwarder_panel(message)
        try:
            group_id = int(raw)
        except Exception:
            bot.send_message(message.chat.id, "⚠️ Invalid group ID.")
            return remove_group_id_prompt(message)

        groups = load_forwarder_group_ids()
        if group_id not in groups:
            bot.send_message(message.chat.id, "ℹ️ Group ID not found.")
            return show_forwarder_panel(message)

        groups = [gid for gid in groups if gid != group_id]
        if save_forwarder_group_ids(groups):
            bot.send_message(message.chat.id, f"✅ Group ID removed: <code>{group_id}</code>", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to update group IDs.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Number Bot Link"))
    def set_number_bot_link_prompt(message):
        current = get_bot_config_value("forwarder_number_bot_link", "")
        msg = bot.send_message(
            message.chat.id,
            f"🔗 Enter Number Bot link.\n\nCurrent: {current or 'Not set'}\n\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        bot.register_next_step_handler(msg, process_set_number_bot_link)

    def process_set_number_bot_link(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_forwarder_panel(message)
        if not raw:
            bot.send_message(message.chat.id, "⚠️ Link cannot be empty.")
            return set_number_bot_link_prompt(message)
        if set_bot_config_value("forwarder_number_bot_link", raw):
            bot.send_message(message.chat.id, "✅ Number Bot link updated.")
        else:
            bot.send_message(message.chat.id, "❌ Failed to save Number Bot link.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Remove Number Bot Link"))
    def remove_number_bot_link(message):
        current = get_bot_config_value("forwarder_number_bot_link", "")
        if not current:
            bot.send_message(message.chat.id, "ℹ️ Number Bot link is already empty.")
            return show_forwarder_panel(message)
        if set_bot_config_value("forwarder_number_bot_link", ""):
            bot.send_message(message.chat.id, "✅ Number Bot link removed.")
        else:
            bot.send_message(message.chat.id, "❌ Failed to remove Number Bot link.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Support Group Link"))
    def set_support_group_link_prompt(message):
        current = get_bot_config_value("forwarder_support_group_link", "")
        msg = bot.send_message(
            message.chat.id,
            f"🔗 Enter Support Group link.\n\nCurrent: {current or 'Not set'}\n\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        bot.register_next_step_handler(msg, process_set_support_group_link)

    def process_set_support_group_link(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_forwarder_panel(message)
        if not raw:
            bot.send_message(message.chat.id, "⚠️ Link cannot be empty.")
            return set_support_group_link_prompt(message)
        if set_bot_config_value("forwarder_support_group_link", raw):
            bot.send_message(message.chat.id, "✅ Support Group link updated.")
        else:
            bot.send_message(message.chat.id, "❌ Failed to save Support Group link.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Remove Support Group Link"))
    def remove_support_group_link(message):
        current = get_bot_config_value("forwarder_support_group_link", "")
        if not current:
            bot.send_message(message.chat.id, "ℹ️ Support Group link is already empty.")
            return show_forwarder_panel(message)
        if set_bot_config_value("forwarder_support_group_link", ""):
            bot.send_message(message.chat.id, "✅ Support Group link removed.")
        else:
            bot.send_message(message.chat.id, "❌ Failed to remove Support Group link.")
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Back to Forwarder Panel"))
    def back_to_forwarder_panel(message):
        show_forwarder_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Forwarder Token"))
    def show_forwarder_token(message):
        from core import FORWARDER_BOT_TOKEN
        token_value = FORWARDER_BOT_TOKEN or ""
        masked_token = token_value[:10] + "***" + token_value[-10:] if len(token_value) > 20 else "***"
        bot.send_message(
            message.chat.id,
            f"🔐 <b>Current Forwarder Token:</b>\n\n<code>{masked_token}</code>\n\n(Token is masked for security)",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='HTML'
        )
        show_forwarder_token_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Forwarder Token"))
    def change_forwarder_token_prompt(message):
        msg = bot.send_message(
            message.chat.id,
            "✏️ Enter new forwarder bot token:\n\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_change_forwarder_token)

    def process_change_forwarder_token(message):
        new_token = (message.text or "").strip()
        if is_cancel_text(new_token):
            return show_forwarder_token_menu(message)

        if not new_token or len(new_token) < 20:
            bot.send_message(message.chat.id, "⚠️ Invalid token! Token too short.")
            return change_forwarder_token_prompt(message)

        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()

        if 'FORWARDER_BOT_TOKEN=' in env_content:
            env_content = re.sub(r'FORWARDER_BOT_TOKEN=.*', f'FORWARDER_BOT_TOKEN={new_token}', env_content)
        else:
            env_content += f'\nFORWARDER_BOT_TOKEN={new_token}'

        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)

        from core import reload_config
        reload_config()
        logger.info(f"Forwarder bot token changed by {message.from_user.id}")
        bot.send_message(
            message.chat.id,
            "✅ Forwarder token updated!\n\nApplied instantly for new OTP forwards.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        show_forwarder_token_menu(message)

    def show_bot_name_menu(message):
        from core import BOT_NAME
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("👁️ View Name", "✏️ Change Name")
        markup.add("⬅️ Back to Settings")
        bot.send_message(
            message.chat.id,
            f"🏷️ <b>Bot Name Management</b>\n\nCurrent name: <b>{BOT_NAME}</b>",
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Name"))
    def show_bot_name(message):
        from core import BOT_NAME
        bot.send_message(message.chat.id, f"🏷️ <b>Current Bot Name:</b>\n\n<b>{BOT_NAME}</b>", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_bot_name_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Name"))
    def change_name_prompt(message):
        msg = bot.send_message(message.chat.id, "✏️ Enter new bot name:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_change_bot_name)

    def process_change_bot_name(message):
        new_name = (message.text or "").strip()
        if is_cancel_text(new_name):
            return show_bot_name_menu(message)
        
        if not new_name or len(new_name) < 2:
            bot.send_message(message.chat.id, "⚠️ Bot name too short! (minimum 2 characters)")
            return change_name_prompt(message)
        
        if len(new_name) > 50:
            bot.send_message(message.chat.id, "⚠️ Bot name too long! (maximum 50 characters)")
            return change_name_prompt(message)
        
        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()
        
        import re
        if 'BOT_NAME=' in env_content:
            env_content = re.sub(r'BOT_NAME=.*', f'BOT_NAME={new_name}', env_content)
        else:
            env_content += f'\nBOT_NAME={new_name}'
        
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        
        logger.info(f"Bot name changed by {message.from_user.id} to {new_name}")
        from core import reload_config
        reload_config()
        bot.send_message(message.chat.id, f"✅ Bot name changed to: <b>{new_name}</b>\n\nApplied instantly.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_bot_name_menu(message)

    def show_admin_list_menu(message):
        if getattr(message, "from_user", None):
            forwarder_panel_users.discard(message.from_user.id)
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("👁️ View Admins", "➕ Add Admin")
        markup.add("🗑️ Remove Admin", "⬅️ Back to Panel")
        bot.send_message(message.chat.id, "🔐 <b>Access Control</b>\n\nSelect an option:", reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Admins"))
    def show_admin_list(message):
        from core import ADMIN_IDS
        if not ADMIN_IDS:
            admin_text = " No admins configured!"
        else:
            admin_text = "<b>Current Admins:</b>\n\n"
            for admin_id in ADMIN_IDS:
                username = "no_username"
                full_name = "no_name"
                try:
                    with get_db_connection() as conn:
                        row = conn.execute(
                            "SELECT username, first_name, last_name FROM users WHERE user_id = ?",
                            (admin_id,),
                        ).fetchone()
                    if row:
                        u, fn, ln = row
                        if u:
                            username = f"@{u}"
                        full_name = " ".join([x for x in [fn, ln] if x]).strip() or "no_name"
                    else:
                        chat = bot.get_chat(admin_id)
                        if getattr(chat, "username", None):
                            username = f"@{chat.username}"
                        full_name = (
                            getattr(chat, "full_name", None)
                            or getattr(chat, "first_name", None)
                            or getattr(chat, "title", None)
                            or "no_name"
                        )
                except Exception:
                    pass
                admin_text += f" <code>{admin_id}</code> - {username} - {full_name}\n"
        
        bot.send_message(message.chat.id, f" {admin_text}", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        return_to_admin_or_forwarder(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Add Admin"))
    def add_admin_prompt(message):
        msg = bot.send_message(message.chat.id, "➕ Enter user ID to add as admin:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_add_admin)

    def process_add_admin(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return return_to_admin_or_forwarder(message)
        try:
            new_admin_id = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid user ID!")
            return add_admin_prompt(message)
        
        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()
        
        import re
        current_admins = re.search(r'ADMIN_IDS=([^\n]*)', env_content)
        if current_admins:
            admins_str = current_admins.group(1).strip()
            if str(new_admin_id) not in admins_str:
                admins_str += f",{new_admin_id}"
            env_content = re.sub(r'ADMIN_IDS=.*', f'ADMIN_IDS={admins_str}', env_content)
        else:
            env_content += f'\nADMIN_IDS={new_admin_id}'
        
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        
        logger.info(f"Admin {new_admin_id} added by {message.from_user.id}")
        from core import reload_config
        reload_config()
        bot.send_message(message.chat.id, f"✅ Admin <code>{new_admin_id}</code> added.\n\nApplied instantly.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        return_to_admin_or_forwarder(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Remove Admin"))
    def remove_admin_prompt(message):
        msg = bot.send_message(message.chat.id, "🗑️ Enter user ID to remove from admins:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_remove_admin)

    def process_remove_admin(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return return_to_admin_or_forwarder(message)
        try:
            admin_to_remove = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid user ID!")
            return remove_admin_prompt(message)
        
        with open('.env', 'r', encoding='utf-8') as f:
            env_content = f.read()
        
        import re
        current_admins = re.search(r'ADMIN_IDS=([^\n]*)', env_content)
        if current_admins:
            admins_list = [int(x.strip()) for x in current_admins.group(1).split(',') if x.strip()]
            if admin_to_remove in admins_list:
                admins_list.remove(admin_to_remove)
                new_admins_str = ','.join(map(str, admins_list))
                env_content = re.sub(r'ADMIN_IDS=.*', f'ADMIN_IDS={new_admins_str}', env_content)
                
                with open('.env', 'w', encoding='utf-8') as f:
                    f.write(env_content)
                
                logger.info(f"Admin {admin_to_remove} removed by {message.from_user.id}")
                from core import reload_config
                reload_config()
                bot.send_message(message.chat.id, f"✅ Admin <code>{admin_to_remove}</code> removed.\n\nApplied instantly.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
            else:
                bot.send_message(message.chat.id, f"ℹ️ User ID <code>{admin_to_remove}</code> is not an admin.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        
        return_to_admin_or_forwarder(message)

    def show_numbers_limit_menu(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_limit = c.execute("SELECT value FROM bot_config WHERE key='max_numbers_per_assign'").fetchone()
            limit_value = current_limit[0] if current_limit else "5"
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("✏️ Change Limit", "📌 Current Limit")
        markup.add("⬅️ Back to Settings")
        
        bot.send_message(message.chat.id, f"🔢 <b>Numbers Assignment Limit</b>\n\nCurrent limit: <b>{limit_value}</b> numbers per user\n\nThis controls how many numbers users can assign at once.", reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Limit"))
    def change_numbers_limit_prompt(message):
        msg = bot.send_message(message.chat.id, "✏️ Enter new numbers limit:\n\nExample: 5, 10, 15\n(Minimum: 1, Maximum: 20)\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_change_numbers_limit)

    def process_change_numbers_limit(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_numbers_limit_menu(message)
        try:
            new_limit = int(raw)
            if new_limit < 1 or new_limit > 20:
                bot.send_message(message.chat.id, "⚠️ Invalid limit! Must be between 1 and 20.")
                return change_numbers_limit_prompt(message)
            
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("UPDATE bot_config SET value=?, updated_at=CURRENT_TIMESTAMP WHERE key='max_numbers_per_assign'", (str(new_limit),))
                conn.commit()
            
            logger.info(f"Numbers limit changed to {new_limit} by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f"✅ Numbers limit updated to <b>{new_limit}</b>.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
            show_numbers_limit_menu(message)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number! Please enter a valid integer.")
            return change_numbers_limit_prompt(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Current Limit"))
    def show_current_numbers_limit(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_limit = c.execute("SELECT value FROM bot_config WHERE key='max_numbers_per_assign'").fetchone()
            limit_value = current_limit[0] if current_limit else "5"
        
        bot.send_message(message.chat.id, f"📌 <b>Current Numbers Limit:</b>\n\n{limit_value} numbers per assignment", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_numbers_limit_menu(message)

    def show_cooldown_menu(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_cd = c.execute("SELECT value FROM bot_config WHERE key='change_number_cooldown_seconds'").fetchone()
            cd_value = current_cd[0] if current_cd else "7"
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("✏️ Change Cooldown", "📌 Current Cooldown")
        markup.add("⬅️ Back to Settings")
        
        bot.send_message(message.chat.id, f"⏱️ <b>Change Numbers Cooldown</b>\n\nCurrent cooldown: <b>{cd_value}</b> seconds\n\nThis controls how often users can change numbers.", reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Cooldown"))
    def change_cooldown_prompt(message):
        msg = bot.send_message(message.chat.id, "✏️ Enter new cooldown seconds:\n\nExample: 7, 30, 60\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_change_cooldown)

    def process_change_cooldown(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_cooldown_menu(message)
        try:
            new_cd = int(raw)
            if new_cd < 1:
                bot.send_message(message.chat.id, "⚠️ Invalid cooldown! Must be at least 1 second.")
                return change_cooldown_prompt(message)
            
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('change_number_cooldown_seconds', '7')")
                c.execute("UPDATE bot_config SET value=?, updated_at=CURRENT_TIMESTAMP WHERE key='change_number_cooldown_seconds'", (str(new_cd),))
                conn.commit()
            
            logger.info(f"Cooldown seconds changed to {new_cd} by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f"✅ Cooldown updated to <b>{new_cd}</b> seconds.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
            show_cooldown_menu(message)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number! Please enter a valid integer.")
            return change_cooldown_prompt(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Current Cooldown"))
    def show_current_cooldown(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_cd = c.execute("SELECT value FROM bot_config WHERE key='change_number_cooldown_seconds'").fetchone()
            cd_value = current_cd[0] if current_cd else "7"
        
        bot.send_message(message.chat.id, f"📌 <b>Current Cooldown:</b>\n\n{cd_value} seconds", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_cooldown_menu(message)

    def show_sms_limit_menu(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_limit = c.execute("SELECT value FROM bot_config WHERE key='sms_limit'").fetchone()
            limit_value = current_limit[0] if current_limit else "1"
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("✏️ Change SMS Limit", "📌 Current SMS Limit")
        markup.add("⬅️ Back to Settings")
        
        bot.send_message(message.chat.id, f"📩 <b>SMS Limit</b>\n\nCurrent limit: <b>{limit_value}</b>\n\n0 = delete immediately\n1 = delete after 1 OTP\n2 = delete after 2 OTPs\n3 = delete after 3 OTPs", reply_markup=markup, parse_mode='HTML')

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change SMS Limit"))
    def change_sms_limit_prompt(message):
        msg = bot.send_message(message.chat.id, "✏️ Enter SMS limit (0, 1, 2, 3...):\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_change_sms_limit)

    def process_change_sms_limit(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_sms_limit_menu(message)
        try:
            new_limit = int(raw)
            if new_limit < 0:
                bot.send_message(message.chat.id, "⚠️ Invalid limit! Must be 0 or higher.")
                return change_sms_limit_prompt(message)
            
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('sms_limit', '1')")
                c.execute("UPDATE bot_config SET value=?, updated_at=CURRENT_TIMESTAMP WHERE key='sms_limit'", (str(new_limit),))
                conn.commit()
            
            logger.info(f"SMS limit changed to {new_limit} by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f"✅ SMS limit updated to <b>{new_limit}</b>.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
            show_sms_limit_menu(message)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number! Please enter a valid integer.")
            return change_sms_limit_prompt(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Current SMS Limit"))
    def show_current_sms_limit(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            current_limit = c.execute("SELECT value FROM bot_config WHERE key='sms_limit'").fetchone()
            limit_value = current_limit[0] if current_limit else "1"
        
        bot.send_message(message.chat.id, f"📌 <b>Current SMS Limit:</b>\n\n{limit_value}", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        show_sms_limit_menu(message)

    def _format_hours_value(raw):
        try:
            val = float(raw)
        except Exception:
            val = 0.0
        if val.is_integer():
            return str(int(val))
        return f"{val:.2f}".rstrip("0").rstrip(".")

    def show_sub_check_hours_menu(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute("SELECT value FROM bot_config WHERE key='subscription_recheck_hours'").fetchone()
            hours_value = _format_hours_value(row[0] if row and row[0] is not None else "0")

        mode_text = "Every request" if hours_value == "0" else f"Every {hours_value} hour(s)"
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("✏️ Change Sub Hours", "📌 Current Sub Hours")
        markup.add("⬅️ Back to Settings")
        bot.send_message(
            message.chat.id,
            "🕒 <b>Subscription Recheck Interval</b>\n\n"
            f"Current: <b>{mode_text}</b>\n\n"
            "Set after how many hours an already-verified user should be checked again.\n"
            "Use 0 to check on every request.",
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Change Sub Hours"))
    def change_sub_check_hours_prompt(message):
        msg = bot.send_message(
            message.chat.id,
            "✏️ Enter subscription recheck hours:\n\n"
            "Examples: 0, 1, 6, 12, 24\n"
            "Use 0 = check every request.\n\n"
            "Type `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_change_sub_check_hours)

    def process_change_sub_check_hours(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_sub_check_hours_menu(message)
        try:
            hours = float(raw)
            if hours < 0:
                bot.send_message(message.chat.id, "⚠️ Invalid value! Must be 0 or higher.")
                return change_sub_check_hours_prompt(message)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number! Please enter a valid value.")
            return change_sub_check_hours_prompt(message)

        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO bot_config (key, value) VALUES ('subscription_recheck_hours', '0')")
            c.execute(
                "UPDATE bot_config SET value=?, updated_at=CURRENT_TIMESTAMP WHERE key='subscription_recheck_hours'",
                (_format_hours_value(hours),)
            )
            conn.commit()

        human = "every request" if hours == 0 else f"every {_format_hours_value(hours)} hour(s)"
        bot.send_message(
            message.chat.id,
            f"✅ Subscription recheck updated: <b>{human}</b>.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='HTML'
        )
        show_sub_check_hours_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Current Sub Hours"))
    def show_current_sub_check_hours(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute("SELECT value FROM bot_config WHERE key='subscription_recheck_hours'").fetchone()
            hours_value = _format_hours_value(row[0] if row and row[0] is not None else "0")
        mode_text = "Every request" if hours_value == "0" else f"Every {hours_value} hour(s)"
        bot.send_message(
            message.chat.id,
            f"📌 <b>Current Subscription Recheck:</b>\n\n{mode_text}",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='HTML'
        )
        show_sub_check_hours_menu(message)

    def show_queue_timer_menu(message):
        mode = (get_bot_config_value("assignment_mode", "serial") or "serial").strip().lower()
        mode = "serial" if mode != "random" else "random"
        auto_enabled = get_bot_config_value("auto_release_enabled", "1").strip() == "1"
        reservation_minutes = get_bot_config_value("reservation_minutes", "60")
        interval_seconds = get_bot_config_value("auto_release_interval_sec", "15")

        mode_text = "Serial (queue)" if mode == "serial" else "Random"
        auto_text = "Enabled" if auto_enabled else "Disabled"

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("🔁 Set Serial Mode", "🎲 Set Random Mode")
        markup.add("✏️ Set Timer (Min)", "✏️ Set Check Interval")
        markup.add("✅ Auto Release ON", "❌ Auto Release OFF")
        markup.add("📊 Queue Status", "♻️ Rebuild Queue")
        markup.add("⬅️ Back to Settings")
        bot.send_message(
            message.chat.id,
            "🔁 <b>Queue & Auto-Release</b>\n\n"
            f"Assignment Mode: <b>{mode_text}</b>\n"
            f"Auto Release: <b>{auto_text}</b>\n"
            f"Reservation Timer: <b>{reservation_minutes}</b> min\n"
            f"Check Interval: <b>{interval_seconds}</b> sec\n\n"
            "Released numbers are moved to queue tail in serial mode.",
            reply_markup=markup,
            parse_mode='HTML'
        )

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Serial Mode"))
    def set_assignment_mode_serial(message):
        if set_bot_config_value("assignment_mode", "serial"):
            bot.send_message(message.chat.id, "✅ Assignment mode set to <b>Serial Queue</b>.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to update assignment mode.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Random Mode"))
    def set_assignment_mode_random(message):
        if set_bot_config_value("assignment_mode", "random"):
            bot.send_message(message.chat.id, "✅ Assignment mode set to <b>Random</b>.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to update assignment mode.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Timer Min"))
    def prompt_set_reservation_minutes(message):
        msg = bot.send_message(
            message.chat.id,
            "✏️ Enter reservation timer in minutes (1-1440).\n\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_set_reservation_minutes)

    def process_set_reservation_minutes(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_queue_timer_menu(message)
        try:
            minutes = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number.")
            return prompt_set_reservation_minutes(message)
        if minutes < 1 or minutes > 1440:
            bot.send_message(message.chat.id, "⚠️ Timer must be between 1 and 1440 minutes.")
            return prompt_set_reservation_minutes(message)
        if set_bot_config_value("reservation_minutes", str(minutes)):
            bot.send_message(message.chat.id, f"✅ Reservation timer set to <b>{minutes}</b> minutes.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to save reservation timer.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Set Check Interval"))
    def prompt_set_auto_release_interval(message):
        msg = bot.send_message(
            message.chat.id,
            "✏️ Enter auto-release check interval in seconds (5-3600).\n\nType `cancel` to go back.",
            reply_markup=types.ReplyKeyboardRemove(),
            parse_mode='Markdown'
        )
        bot.register_next_step_handler(msg, process_set_auto_release_interval)

    def process_set_auto_release_interval(message):
        raw = (message.text or "").strip()
        if is_cancel_text(raw):
            return show_queue_timer_menu(message)
        try:
            seconds = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number.")
            return prompt_set_auto_release_interval(message)
        if seconds < 5 or seconds > 3600:
            bot.send_message(message.chat.id, "⚠️ Interval must be between 5 and 3600 seconds.")
            return prompt_set_auto_release_interval(message)
        if set_bot_config_value("auto_release_interval_sec", str(seconds)):
            bot.send_message(message.chat.id, f"✅ Check interval set to <b>{seconds}</b> sec.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to save check interval.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Auto Release ON"))
    def enable_auto_release(message):
        if set_bot_config_value("auto_release_enabled", "1"):
            bot.send_message(message.chat.id, "✅ Auto release <b>enabled</b>.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to enable auto release.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Auto Release OFF"))
    def disable_auto_release(message):
        if set_bot_config_value("auto_release_enabled", "0"):
            bot.send_message(message.chat.id, "✅ Auto release <b>disabled</b>.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, "❌ Failed to disable auto release.")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Queue Status"))
    def show_queue_status(message):
        try:
            from core import get_queue_stats, get_assignment_mode
            stats = get_queue_stats()
            mode = get_assignment_mode()
            mode_text = "Serial Queue" if mode == "serial" else "Random"
            text = (
                "📊 <b>Queue Status</b>\n\n"
                f"Mode: <b>{mode_text}</b>\n"
                f"Total Numbers: <b>{stats['total']}</b>\n"
                f"Active: <b>{stats['active']}</b>\n"
                f"Reserved: <b>{stats['reserved']}</b>\n"
                f"Expired Ready: <b>{stats['expired_ready']}</b>"
            )
            bot.send_message(message.chat.id, text, parse_mode='HTML', reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Failed to read queue status: {e}")
        show_queue_timer_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Rebuild Queue"))
    def rebuild_queue_order(message):
        try:
            from core import rebuild_number_queue
            updated = rebuild_number_queue()
            bot.send_message(
                message.chat.id,
                f"♻️ Queue rebuilt.\n\nUpdated positions: <b>{updated}</b>",
                parse_mode='HTML',
                reply_markup=types.ReplyKeyboardRemove()
            )
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Queue rebuild failed: {e}")
        show_queue_timer_menu(message)



    # ==================== USER MANAGEMENT ====================
    def show_user_management(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("📤 Export User IDs", "📥 Upload User IDs")
        markup.add("⛔ Ban User", "✅ Unban User")
        markup.add("📄 Banned Users List", "⬅️ Back to Panel")
        bot.send_message(message.chat.id, "👥 <b>User Management</b>\n\nSelect an option:", reply_markup=markup, parse_mode='HTML')



    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, "Ban User"))
    def ban_user_prompt(message):
        msg = bot.send_message(message.chat.id, "⛔ Enter user ID to ban:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_ban_user)

    def process_ban_user(message):
        raw = (getattr(message, "text", None) or "").strip()
        if is_cancel_text(raw):
            return show_user_management(message)
        if not raw:
            msg = bot.send_message(message.chat.id, "⚠️ Enter user ID to ban (numbers only):", reply_markup=types.ReplyKeyboardRemove())
            bot.register_next_step_handler(msg, process_ban_user)
            return
        try:
            user_id = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid user ID.", reply_markup=types.ReplyKeyboardRemove())
            return show_user_management(message)

        from core import ADMIN_IDS
        if user_id in ADMIN_IDS:
            bot.send_message(message.chat.id, "⚠️ You cannot ban an admin.", reply_markup=types.ReplyKeyboardRemove())
            return show_user_management(message)

        try:
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO banned_users (user_id, reason) VALUES (?, ?)",
                    (user_id, "Manual ban")
                )
                conn.commit()
            logger.info(f"User {user_id} banned by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f"✅ User <code>{user_id}</code> banned.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Ban failed: {e}", reply_markup=types.ReplyKeyboardRemove())

        show_user_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, "Unban User"))
    def unban_user_prompt(message):
        msg = bot.send_message(message.chat.id, "✅ Enter user ID to unban:\n\nType `cancel` to go back.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='Markdown')
        bot.register_next_step_handler(msg, process_unban_user)

    def process_unban_user(message):
        raw = (getattr(message, "text", None) or "").strip()
        if is_cancel_text(raw):
            return show_user_management(message)
        if not raw:
            msg = bot.send_message(message.chat.id, "⚠️ Enter user ID to unban (numbers only):", reply_markup=types.ReplyKeyboardRemove())
            bot.register_next_step_handler(msg, process_unban_user)
            return
        try:
            user_id = int(raw)
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid user ID.", reply_markup=types.ReplyKeyboardRemove())
            return show_user_management(message)

        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
                conn.commit()
                removed = c.rowcount
            if removed:
                logger.info(f"User {user_id} unbanned by admin {message.from_user.id}")
                bot.send_message(message.chat.id, f"✅ User <code>{user_id}</code> unbanned.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
            else:
                bot.send_message(message.chat.id, f"ℹ️ User <code>{user_id}</code> is not banned.", reply_markup=types.ReplyKeyboardRemove(), parse_mode='HTML')
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Unban failed: {e}", reply_markup=types.ReplyKeyboardRemove())

        show_user_management(message)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("quick_unban_"))
    def quick_unban_user(call):
        try:
            user_id = int(call.data.replace("quick_unban_", ""))
        except Exception:
            answer_cbq(call, "Invalid user id")
            return

        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
                conn.commit()
                removed = c.rowcount
            if removed:
                answer_cbq(call, f"Unbanned {user_id}")
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                bot.send_message(call.message.chat.id, f"✅ User <code>{user_id}</code> unbanned.", parse_mode='HTML')
            else:
                answer_cbq(call, "User is not banned")
        except Exception as e:
            answer_cbq(call, "Unban failed")
            logger.error(f"Quick unban failed for {user_id}: {e}")

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, "Banned Users List"))
    def banned_users_list(message):
        try:
            with get_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT b.user_id, b.banned_at, b.reason, u.username, u.first_name, u.last_name
                    FROM banned_users b
                    LEFT JOIN users u ON u.user_id = b.user_id
                    ORDER BY b.banned_at DESC
                    """
                ).fetchall()
            if not rows:
                bot.send_message(message.chat.id, "ℹ️ No banned users.", reply_markup=types.ReplyKeyboardRemove())
                return show_user_management(message)

            text = "📄 <b>Banned Users</b>\n\n"
            for user_id, banned_at, reason, username, first_name, last_name in rows:
                reason_text = reason if reason else "(no reason)"
                uname = f"@{username}" if username else "no_username"
                full_name = " ".join([x for x in [first_name, last_name] if x]).strip() or "no_name"
                text += f"- <code>{user_id}</code> - {uname} - {full_name} - {reason_text}\n"

            bot.send_message(
                message.chat.id,
                text,
                reply_markup=types.ReplyKeyboardRemove(),
                parse_mode='HTML'
            )
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Failed to load banned users: {e}", reply_markup=types.ReplyKeyboardRemove())

        show_user_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Export User IDs"))
    def export_user_ids(message):
        try:
            with get_db_connection() as conn:
                rows = conn.execute("SELECT user_id FROM users ORDER BY user_id").fetchall()
            user_ids = [str(r[0]) for r in rows if r and r[0] is not None]

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            export_path = BACKUP_DIR / f"user_ids_{timestamp}.txt"
            export_path.write_text("\n".join(user_ids), encoding="utf-8")

            with open(export_path, "rb") as f:
                bot.send_document(message.chat.id, f, caption=f" Exported {len(user_ids)} user IDs")
        except Exception as e:
            logger.error(f"Export user IDs failed: {e}")
            bot.send_message(message.chat.id, f" Export failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        show_user_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Upload User IDs"))
    def upload_user_ids_prompt(message):
        msg = bot.send_message(
            message.chat.id,
            " Send a .txt file with one user ID per line.\n\nExample:\n123456789\n987654321",
            reply_markup=types.ReplyKeyboardRemove()
        )
        bot.register_next_step_handler(msg, process_upload_user_ids)

    def process_upload_user_ids(message):
        if not message.document:
            bot.send_message(message.chat.id, " Please upload a .txt file.")
            return show_user_management(message)

        if message.document.mime_type != "text/plain":
            bot.send_message(message.chat.id, " Invalid file type. Please upload a .txt file.")
            return show_user_management(message)

        try:
            file_info = bot.get_file(message.document.file_id)
            content_b = bot.download_file(file_info.file_path)
            content = content_b.decode("utf-8", errors="ignore")

            raw_lines = [line.strip() for line in content.splitlines() if line.strip()]
            valid_ids = []
            invalid_lines = 0
            for line in raw_lines:
                if line.isdigit():
                    valid_ids.append(int(line))
                else:
                    invalid_lines += 1

            if not valid_ids:
                bot.send_message(message.chat.id, " No valid user IDs found in file.")
                return show_user_management(message)

            with get_db_connection() as conn:
                existing = {row[0] for row in conn.execute("SELECT user_id FROM users").fetchall()}
                new_ids = [uid for uid in valid_ids if uid not in existing]

                if new_ids:
                    conn.executemany("INSERT OR IGNORE INTO users (user_id) VALUES (?)", [(uid,) for uid in new_ids])
                    conn.commit()

            added_count = len(new_ids)
            skipped_count = len(valid_ids) - added_count

            summary = (
                " <b>Upload Complete</b>\n\n"
                f"Added: <b>{added_count}</b>\n"
                f"Skipped duplicates: <b>{skipped_count}</b>\n"
                f"Invalid lines: <b>{invalid_lines}</b>"
            )
            bot.send_message(message.chat.id, summary, parse_mode="HTML", reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            logger.error(f"Upload user IDs failed: {e}")
            bot.send_message(message.chat.id, f" Upload failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        show_user_management(message)

    # Removed all Ban/Unban handlers and menu

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Back to Panel"))
    def back_to_admin_panel(message):
        admin_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Back to Settings"))
    def back_to_bot_settings_handler(message):
        show_bot_settings(message)

    # ==================== DATABASE MANAGEMENT ====================
    def show_database_management(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("📦 DB Size & Info", "🧹 Cleanup Old Data")
        markup.add("🚀 Optimize DB", "📊 Table Stats")
        markup.add("⬅️ Back to Panel")
        
        bot.send_message(message.chat.id, "🗄️ <b>Database Management</b>\n\nChoose an option:", reply_markup=markup)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " DB Size & Info"))
    def show_db_size_info(message):
        import os
        db_path = "number_panel.db"
        
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path) / (1024 * 1024)  # Convert to MB
            
            with get_db_connection() as conn:
                c = conn.cursor()
                
                # Get table info
                tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                
                text = f" <b>Database Information</b>\n\n"
                text += f" <b>File Size:</b> {db_size:.2f} MB\n"
                text += f" <b>Tables:</b> {len(tables)}\n\n"
                
                text += "<b>Table Details:</b>\n"
                for table in tables:
                    table_name = table[0]
                    row_count = c.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    text += f"   {table_name}: {row_count} rows\n"
        else:
            text = " Database file not found!"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_database_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Cleanup Old Data"))
    def cleanup_old_data(message):
        msg = bot.send_message(message.chat.id, " Enter days (remove data older than X days):\n(e.g., 30 for 30+ days old data)", reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_cleanup_data)

    def process_cleanup_data(message):
        try:
            days = int(message.text.strip())
            
            if days < 1:
                bot.send_message(message.chat.id, " Days must be at least 1!", reply_markup=types.ReplyKeyboardRemove())
                show_database_management(message)
                return
            
            with get_db_connection() as conn:
                c = conn.cursor()
                
                # Get count of records to delete
                cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
                old_records = c.execute(
                    "SELECT COUNT(*) FROM numbers WHERE last_used < ?",
                    (cutoff_date,)
                ).fetchone()[0]
                
                # Delete old records
                c.execute("DELETE FROM numbers WHERE last_used < ?", (cutoff_date,))
                conn.commit()
                
                logger.info(f"Cleaned up {old_records} old records older than {days} days by admin {message.from_user.id}")
                bot.send_message(message.chat.id, f" <b>Cleanup Complete</b>\n\n Deleted {old_records} old records\n(Older than {days} days)", reply_markup=types.ReplyKeyboardRemove())
        
        except ValueError:
            bot.send_message(message.chat.id, "⚠️ Invalid number! Please enter a number.", reply_markup=types.ReplyKeyboardRemove())
        
        show_database_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Optimize DB"))
    def optimize_database(message):
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("VACUUM")
                conn.commit()
            
            logger.info(f"Database optimized by admin {message.from_user.id}")
            bot.send_message(message.chat.id, " <b>Database Optimized</b>\n\n VACUUM operation completed successfully!", reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            logger.error(f"Database optimization error: {e}")
            bot.send_message(message.chat.id, f" Optimization failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        
        show_database_management(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Table Stats"))
    def show_table_stats(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            
            text = " <b>Detailed Table Statistics</b>\n\n"
            
            # Services stats
            services = c.execute("SELECT COUNT(*) FROM services").fetchone()[0]
            text += f"<b> Services Table:</b>\n"
            text += f"   Total: {services}\n\n"
            
            # Numbers stats
            total_numbers = c.execute("SELECT COUNT(*) FROM numbers").fetchone()[0]
            active_numbers = c.execute("SELECT COUNT(*) FROM numbers WHERE status='active'").fetchone()[0]
            reserved_numbers = c.execute("SELECT COUNT(*) FROM numbers WHERE status='reserved'").fetchone()[0]
            text += f"<b> Numbers Table:</b>\n"
            text += f"   Total: {total_numbers}\n"
            text += f"   Active: {active_numbers}\n"
            text += f"   Reserved: {reserved_numbers}\n\n"
            
            # Banned users stats
            banned_users = c.execute("SELECT COUNT(*) FROM banned_users").fetchone()[0]
            text += f"<b> Banned Users:</b>\n"
            text += f"   Total: {banned_users}\n"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_database_management(message)

    # ==================== ACCESS CONTROL ====================
    def show_access_control(message):
        show_admin_list_menu(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View All Admins"))
    def view_all_admins(message):
        from core import ADMIN_IDS
        
        text = " <b>All Admins in System</b>\n\n"
        text += f"<b>Total Admins: {len(ADMIN_IDS)}</b>\n\n"
        
        for i, admin_id in enumerate(ADMIN_IDS, 1):
            text += f"{i}. <code>{admin_id}</code>\n"
        
        text += "\n<i>Current admin: You have full access</i>"
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_access_control(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Admin Permissions"))
    def view_admin_permissions(message):
        text = " <b>Admin Permissions</b>\n\n"
        text += "<b>Current Permissions (All Admins):</b>\n"
        text += " View statistics\n"
        text += " Add/Delete services\n"
        text += " Add/Remove numbers\n"
        text += " Ban/Unban users\n"
        text += " View top users\n"
        text += " Configure bot settings\n"
        text += " Backup/Restore database\n"
        text += " Database management\n"
        text += " Full system access\n\n"
        text += "<i> All admins currently have identical full permissions.</i>\n"
        text += "<i>Feature: Role-based permissions coming soon!</i>"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_access_control(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " View Admin Logs"))
    def view_admin_logs(message):
        log_dir = "logs"
        import os
        
        if os.path.exists(log_dir):
            log_files = sorted([f for f in os.listdir(log_dir) if f.startswith("bot_")], reverse=True)
            
            if log_files:
                text = " <b>Admin Activity Logs</b>\n\n"
                text += "<b>Recent Log Files:</b>\n"
                
                for log_file in log_files[:5]:  # Show last 5 logs
                    file_path = os.path.join(log_dir, log_file)
                    file_size = os.path.getsize(file_path) / 1024  # KB
                    text += f" {log_file} ({file_size:.1f} KB)\n"
                
                text += "\n<i> Check logs directory for detailed admin actions</i>"
            else:
                text = " No log files found yet."
        else:
            text = " Logs directory not found."
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_access_control(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " User Roles"))
    def view_user_roles(message):
        text = " <b>User Roles & Access Levels</b>\n\n"
        text += "<b>1. Regular User</b>\n"
        text += "    Get OTP numbers\n"
        text += "    Use numbers\n"
        text += "    View personal stats\n\n"
        text += "<b>2. Admin</b>\n"
        text += "    Full system access\n"
        text += "    Manage bot\n"
        text += "    Configure settings\n\n"
        text += "<i> Role-based access control coming soon!</i>"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_access_control(message)

    # ==================== BOT OPERATIONS ====================
    def show_bot_operations(message):
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        markup.add("❤️ Bot Health Check", "🧼 Clear Cache")
        markup.add("🚨 System Alerts", "⏳ Bot Uptime")
        markup.add("⬅️ Back to Panel")
        
        bot.send_message(message.chat.id, "🛠️ <b>Bot Operations</b>\n\nChoose an option:", reply_markup=markup)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Bot Health Check"))
    def bot_health_check(message):
        import psutil
        import os
        
        try:
            # Get bot process
            process = psutil.Process(os.getpid())
            
            # Memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # CPU usage
            cpu_percent = process.cpu_percent(interval=0.1)
            
            # Uptime (approximate)
            start_time = datetime.datetime.fromtimestamp(process.create_time())
            uptime = datetime.datetime.now() - start_time
            uptime_str = f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m"
            
            text = " <b>Bot Health Status</b>\n\n"
            text += f"<b>Memory Usage:</b> {memory_mb:.2f} MB\n"
            text += f"<b>CPU Usage:</b> {cpu_percent:.1f}%\n"
            text += f"<b>Bot Uptime:</b> {uptime_str}\n"
            text += f"<b>Status:</b>  Running\n\n"
            text += " System operating normally"
        except ImportError:
            text = " <b>Health Check Info</b>\n\n"
            text += " Memory usage: Monitor via system\n"
            text += " CPU usage: Monitor via system\n"
            text += " Install 'psutil' for detailed metrics"
        except Exception as e:
            text = f" Health check error: {e}"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Clear Cache"))
    def clear_cache_operation(message):
        import gc
        
        try:
            # Force garbage collection
            collected = gc.collect()
            
            logger.info(f"Cache cleared by admin {message.from_user.id}. Garbage collected {collected} objects")
            text = f" <b>Cache Cleared</b>\n\n"
            text += f" Freed memory objects: {collected}\n"
            text += f" Cache cleared successfully"
        except Exception as e:
            text = f"❌ Error: {e}"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Export Database"))
    def export_database(message):
        try:
            import shutil
            import time
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"backups/export_db_{timestamp}.db"
            
            shutil.copy("number_panel.db", backup_file)
            logger.info(f"Database exported by admin {message.from_user.id}")
            
            bot.send_message(message.chat.id, f" <b>Export Complete</b>\n\n Database exported to:\n<code>{backup_file}</code>", reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            bot.send_message(message.chat.id, f" Export failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Export Users List"))
    def export_users_list(message):
        try:
            import csv
            
            with get_db_connection() as conn:
                c = conn.cursor()
                users = c.execute(
                    "SELECT DISTINCT user_id FROM numbers WHERE user_id IS NOT NULL ORDER BY user_id"
                ).fetchall()
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            export_file = f"backups/users_export_{timestamp}.csv"
            
            with open(export_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['User ID'])
                for user in users:
                    writer.writerow(user)
            
            logger.info(f"Users list exported by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f" <b>Export Complete</b>\n\n Exported {len(users)} users to:\n<code>{export_file}</code>", reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            bot.send_message(message.chat.id, f" Export failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Export Numbers"))
    def export_numbers_list(message):
        try:
            import csv
            
            with get_db_connection() as conn:
                c = conn.cursor()
                numbers = c.execute(
                    "SELECT s.name, n.number, n.status, n.user_id, n.received_otp FROM numbers n "
                    "JOIN services s ON n.service_id = s.id ORDER BY s.name, n.number"
                ).fetchall()
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            export_file = f"backups/numbers_export_{timestamp}.csv"
            
            with open(export_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Service', 'Number', 'Status', 'User ID', 'Times Used'])
                for num in numbers:
                    writer.writerow(num)
            
            logger.info(f"Numbers list exported by admin {message.from_user.id}")
            bot.send_message(message.chat.id, f" <b>Export Complete</b>\n\n Exported {len(numbers)} numbers to:\n<code>{export_file}</code>", reply_markup=types.ReplyKeyboardRemove())
        except Exception as e:
            bot.send_message(message.chat.id, f" Export failed: {e}", reply_markup=types.ReplyKeyboardRemove())
        
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " System Alerts"))
    def view_system_alerts(message):
        text = " <b>System Alerts</b>\n\n"
        
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                
                # Check for low active numbers
                active_numbers = c.execute("SELECT COUNT(*) FROM numbers WHERE status='active'").fetchone()[0]
                total_numbers = c.execute("SELECT COUNT(*) FROM numbers").fetchone()[0]
                
                text += "<b>Current Status:</b>\n\n"
                
                if active_numbers < 10:
                    text += f" <b>LOW STOCK:</b> Only {active_numbers} active numbers left!\n"
                else:
                    text += f" <b>Stock OK:</b> {active_numbers} active numbers available\n"
                
                # Database size
                import os
                db_size = os.path.getsize("number_panel.db") / (1024 * 1024)
                if db_size > 100:
                    text += f" <b>Large DB:</b> Database is {db_size:.2f} MB\n"
                else:
                    text += f" <b>DB Size:</b> {db_size:.2f} MB\n"
                
                text += f" <b>Total Numbers:</b> {total_numbers}\n"
        except Exception as e:
            text += f" Error reading alerts: {e}"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_bot_operations(message)

    @bot.message_handler(func=lambda m: is_admin(m) and admin_text_is(m, " Bot Uptime"))
    def show_bot_uptime(message):
        import psutil
        import os
        
        try:
            process = psutil.Process(os.getpid())
            start_time = datetime.datetime.fromtimestamp(process.create_time())
            uptime = datetime.datetime.now() - start_time
            
            days = uptime.days
            hours = uptime.seconds // 3600
            minutes = (uptime.seconds % 3600) // 60
            
            text = " <b>Bot Uptime</b>\n\n"
            text += f" <b>Start Time:</b> {start_time.strftime('%d-%m-%Y %H:%M:%S')}\n"
            text += f" <b>Current Time:</b> {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n\n"
            text += f"<b>Uptime:</b>\n"
            text += f"   {days} days\n"
            text += f"   {hours} hours\n"
            text += f"   {minutes} minutes\n"
        except ImportError:
            text = " <b>Bot Uptime</b>\n\n"
            text += "Install 'psutil' package for uptime tracking\n"
            text += "<code>pip install psutil</code>"
        except Exception as e:
            text = f"❌ Error: {e}"
        
        bot.send_message(message.chat.id, text, reply_markup=types.ReplyKeyboardRemove())
        show_bot_operations(message)

    # ==================== BACK TO PANEL HANDLERS ====================
    @bot.message_handler(func=lambda m: is_admin(m) and m.text in [" Database Mgmt Back", " Access Control Back", " Bot Operations Back"])
    def back_from_features(message):
        admin_panel(message)

    @bot.message_handler(func=lambda m: is_admin(m) and m.text in [" Back to Panel", "⬅️ Back to Panel", " Cancel", "❌ Cancel"])
    def handle_back_button(message):
        if message.text in [" Back to Panel", "⬅️ Back to Panel"]:
            admin_panel(message)
        else:
            show_bot_operations(message)
    
    def show_dashboard(message):
        with get_db_connection() as conn:
            c = conn.cursor()
            total_users = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            try:
                total_otps_received = c.execute("SELECT COUNT(*) FROM otp_log").fetchone()[0]
            except Exception:
                total_otps_received = 0

        html = f"""<b>\U0001F4CA DASHBOARD</b>

\U0001F465 Total Users: <b>{total_users}</b>
\U0001F4E9 OTPs Received: <b>{total_otps_received}</b>
"""

        bot.send_message(message.chat.id, html, parse_mode='HTML', reply_markup=types.ReplyKeyboardRemove())
        admin_panel(message)

    # Usage statistics




    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_country_"))
    def handle_add_country(call):
        ask_service_name_for_add(call.message, edit_msg_id=call.message.message_id)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "add_new_service")
    def handle_add_new_service(call):
        msg = bot.send_message(call.message.chat.id, " Enter new Country Name:")
        bot.register_next_step_handler(msg, create_new_service_flow)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("create_service_"))
    def handle_create_service(call):
        country = call.data.replace("create_service_", "")
        msg = bot.send_message(call.message.chat.id, f" Enter Service Name for {country} (you can include emojis):")
        bot.register_next_step_handler(msg, finish_new_service, country)
        answer_cbq(call)

    def create_new_service_flow(message):
        country = message.text.strip()
        if not country:
            bot.send_message(message.chat.id, " Country name cannot be empty.")
            return ask_service_name_for_add(message)
        
        msg = bot.send_message(message.chat.id, f" Enter Service Name for {country} (you can include emojis):")
        bot.register_next_step_handler(msg, finish_new_service, country)

    def finish_new_service(message, country):
        service_name = message.text.strip()
        if not service_name:
            bot.send_message(message.chat.id, "⚠️ Service name cannot be empty.")
            return ask_service_name_for_add(message)
        
        msg = bot.send_message(message.chat.id, f" Send {country} for {service_name}:", reply_markup=types.ReplyKeyboardRemove())
        bot.register_next_step_handler(msg, process_and_save_numbers, country, service_name, None)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_service_"))
    def handle_add_service(call):
        service_name = call.data.replace("add_service_", "")
        ask_country_for_service(call.message, service_name, edit_msg_id=call.message.message_id)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_countries")
    def handle_back_to_countries(call):
        ask_service_name_for_add(call.message, edit_msg_id=call.message.message_id)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "cancel_add")
    def handle_cancel_add(call):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        admin_panel(call.message)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_service_"))
    def handle_add_service(call):
        service_name = call.data.replace("add_service_", "")
        ask_country_for_service(call.message, service_name, edit_msg_id=call.message.message_id)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_to_country_"))
    def handle_add_to_country(call):
        service_id = int(call.data.replace("add_to_country_", ""))
        
        with get_db_connection() as conn:
            service = conn.execute("SELECT name, country FROM services WHERE id=?", (service_id,)).fetchone()
        
        if service:
            service_name, country = service
            ask_for_numbers_file(call.message, country, service_name, service_id)
        
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_new_country_"))
    def handle_add_new_country(call):
        service_name = call.data.replace("add_new_country_", "")
        msg = bot.send_message(call.message.chat.id, f" Enter Country Name for {service_name}:")
        bot.register_next_step_handler(msg, create_country_and_add_numbers, service_name)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_services")
    def handle_back_to_services(call):
        ask_service_name_for_add(call.message, edit_msg_id=call.message.message_id)
        answer_cbq(call)

    @bot.callback_query_handler(func=lambda call: call.data == "create_new_service")
    def handle_create_new_service(call):
        ask_service_name_for_new(call.message)
        answer_cbq(call)

    def create_country_and_add_numbers(message, service_name):
        country_name = message.text.strip()
        if not country_name:
            bot.send_message(message.chat.id, " Country name cannot be empty.")
            msg = bot.send_message(message.chat.id, f" Enter Country Name for {service_name}:")
            bot.register_next_step_handler(msg, create_country_and_add_numbers, service_name)
            return
        
        # Do not create DB row yet; process_and_save_numbers will auto-detect country from numbers
        # and then insert/update the correct service-country record.
        ask_for_numbers_file(message, country_name, service_name, None)
