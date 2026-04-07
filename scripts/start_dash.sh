#!/usr/bin/env bash
set -euo pipefail

cd /app

if pgrep -f "src.bi.dash_app" >/dev/null 2>&1; then
  echo "Dash app already running"
  exit 0
fi

PYTHON_BIN="python3"
if [ -x "/app/.venv/bin/python" ]; then
  PYTHON_BIN="/app/.venv/bin/python"
fi

nohup "$PYTHON_BIN" -m src.bi.dash_app >/tmp/dash_app.log 2>&1 &
echo "Dash app started with $PYTHON_BIN"
