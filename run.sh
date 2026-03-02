#!/usr/bin/env sh
set -eu

if [ ! -f "bot.Py" ]; then
  echo "ERROR: bot.Py not found in current directory"
  exit 1
fi

if [ -x ".venv/bin/python" ]; then
  PYTHON_EXE=".venv/bin/python"
else
  PYTHON_EXE="python3"
fi

echo "Starting bot with: $PYTHON_EXE"
exec "$PYTHON_EXE" bot.Py
