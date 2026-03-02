# Environment Guide

Use `.env` for all private configuration.

## 1) Create `.env`

Linux/macOS:

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
copy .env.example .env
```

## 2) Required keys

- `BOT_TOKEN`
- `ADMIN_IDS` (comma separated if multiple)
- `BOT_NAME`

## 3) Common optional keys

- `FORWARDER_BOT_TOKEN`
- `OTP_GROUP_URL`
- `WEBHOOK_SECRET`
- `BRANDING_NAME`
- `BRANDING_URL`
- `DB_NAME`, `LOG_DIR`, `BACKUP_DIR`

## 4) Security

- Do not commit `.env`
- Do not share bot tokens/webhook secret
- If leaked, rotate token/secret immediately

## 5) Run

Linux:

```bash
./run.sh
```

Windows:

```bat
run.bat
```
