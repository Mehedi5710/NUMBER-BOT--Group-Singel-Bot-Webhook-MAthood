# NUMBER BOT Webhook 2.0

Telegram number assignment + OTP forwarding bot.

- User bot: `/start`, `/getnumber`, `/buy`
- Admin panel: `/admin`
- Webhook endpoint: receives OTP payload and forwards to user + forwarder groups

## Features

- Service + country based number assignment
- Subscription target check (channel/group join required)
- OTP webhook receiver (FastAPI + worker queue)
- Group forward format customizable in `group_message_format.py`
- Country flag detection from phone prefix
- Serial queue mode with tail-on-release behavior
- Auto release timer for reserved numbers

## Project Files

- `bot.Py` entry point
- `core.py` config, DB init, queue + auto-release utilities
- `user_handlers.py` user flow
- `admin.py` admin panel flow
- `otp.py` webhook receive/process/forward
- `group_message_format.py` group OTP message layout
- `country.py`, `flag.py` country + flag helpers

## Requirements

- Python 3.10+
- Telegram bot token from @BotFather

Install:

```bash
python3 -m pip install -r requirements.txt
```

## Environment Setup

1. Copy env template:

```bash
cp .env.example .env
```

2. Fill `.env` values.

Minimum required:

- `BOT_TOKEN`
- `ADMIN_IDS`
- `BOT_NAME`

Webhook config:

- `WEBHOOK_HOST` default `127.0.0.1`
- `WEBHOOK_PORT` default `3232`
- `WEBHOOK_PATH` default `/webhook`
- `WEBHOOK_SECRET` recommended (header auth)

Optional branding for group messages:

- `BRANDING_NAME`
- `BRANDING_URL`

## Run (Linux VPS)

```bash
./run.sh
# or
python3 bot.Py
```

## Run (Windows RDP)

```bat
run.bat
```

## Webhook Format

URL:

`POST http://<host>:<port><path>`

Default local URL:

`POST http://127.0.0.1:3232/webhook`

Headers:

- `Content-Type: application/json`
- `X-Webhook-Secret: <WEBHOOK_SECRET>` (if secret configured)

Payload (single message):

```json
{
  "number": "+8801711223344",
  "service": "Telegram",
  "message": "Your Telegram code is 123456",
  "country": "Bangladesh"
}
```

Payload (batch):

```json
{
  "messages": [
    {
      "number": "+8801711223344",
      "service": "Telegram",
      "message": "Your Telegram code is 123456"
    },
    {
      "number": "+7067788990",
      "service": "WhatsApp",
      "message": "Your code is 778899"
    }
  ]
}
```

Success response:

```json
{"ok": true, "queued": 1}
```

## Test cURL

```bash
curl -X POST "http://127.0.0.1:3232/webhook" \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: YOUR_WEBHOOK_SECRET" \
  -d '{
    "number": "+8801711223344",
    "service": "Telegram",
    "message": "Your Telegram code is 123456"
  }'
```

## Admin: Queue + Timer

`/admin` -> `⚙️ Bot Settings` -> `🔁 Queue Timer`

Available controls:

- `🔁 Set Serial Mode`
- `🎲 Set Random Mode`
- `✏️ Set Timer (Min)`
- `✏️ Set Check Interval`
- `✅ Auto Release ON`
- `❌ Auto Release OFF`
- `📊 Queue Status`
- `♻️ Rebuild Queue`

## GitHub Safety Checklist

Before pushing public:

1. Never commit `.env`
2. Remove runtime data (`logs/`, `backups/`, `*.db`, `__pycache__/`)
3. Use `.env.example` only
4. Rotate tokens if accidentally exposed

This repository now includes `.gitignore` entries for these runtime/sensitive artifacts.
