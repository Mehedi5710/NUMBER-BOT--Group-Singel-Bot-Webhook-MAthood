

import time
import sys

from core import (
    BOT_TOKEN, BOT_NAME, ADMIN_IDS, OTP_GROUP_URL,
    bot, get_db_connection, init_db, logger,
    BACKUP_DIR, DB_NAME, LOG_DIR, cleanup_old_logs,
    WEBHOOK_HOST, WEBHOOK_PORT, WEBHOOK_PATH, WEBHOOK_WORKERS,
    start_auto_release_worker
)

import admin
import user_handlers
import otp


def check_configuration():
    """Validates that all required configuration values are set"""
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not ADMIN_IDS:
        missing.append("ADMIN_IDS")
    if not BOT_NAME or BOT_NAME == "":
        missing.append("BOT_NAME")
    
    if missing:
        print("Missing config:", ", ".join(missing))
        return False
    return True


if __name__ == "__main__":
    if not check_configuration():
        sys.exit(1)
    init_db()
    start_auto_release_worker()
    cleanup_old_logs(days_to_keep=7)
    admin.register_handlers(bot, get_db_connection, logger)
    user_handlers.register_handlers(bot, get_db_connection, logger)
    otp.start_webhook_server(
        host=WEBHOOK_HOST,
        port=WEBHOOK_PORT,
        path=WEBHOOK_PATH,
        workers=WEBHOOK_WORKERS,
    )
    try:
        from telebot.types import BotCommand
        commands = [
            BotCommand("start", "🚀 Start the bot"),
            BotCommand("admin", "🛠️ Admin panel (admin only)"),
            BotCommand("getnumber", "📱 Get phone numbers"),
            BotCommand("buy", "🔎 Search numbers by prefix"),
        ]
        bot.set_my_commands(commands)
    except Exception:
        pass


    print("✅ OTP Bot started!")
    print("🌐 Webhook active - OTPs will be forwarded automatically")
    print("🤖 Bot commands ready - users can use /start, /getnumber, /admin")
    print("⌨️ Press Ctrl+C to stop...")

    try:
        # Start bot command polling (this was crashing before)
        bot.infinity_polling(timeout=30, long_polling_timeout=20)
    except KeyboardInterrupt:
        print("\nBot stopped by user")
    except Exception as e:
        print(f"Bot polling error: {e}")
        print("Continuing with webhook only...")
        # Keep the process alive  webhook
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nBot stopped by user")
