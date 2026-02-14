#!/bin/bash
# ============================================================
# setup_cron.sh — Настройка ежедневного запуска через cron
# ============================================================
# Использование: chmod +x setup_cron.sh && ./setup_cron.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_PATH="$(which python3)"
AGENT_PATH="$SCRIPT_DIR/agent.py"
LOG_PATH="$SCRIPT_DIR/logs/cron.log"

# Ежедневно в 06:00 (настройте время под себя)
CRON_TIME="0 6 * * *"

CRON_CMD="$CRON_TIME cd $SCRIPT_DIR && $PYTHON_PATH $AGENT_PATH >> $LOG_PATH 2>&1"

echo "📋 Добавляю cron задачу:"
echo "   $CRON_CMD"

# Добавляем задачу, не дублируя
(crontab -l 2>/dev/null | grep -v "$AGENT_PATH"; echo "$CRON_CMD") | crontab -

echo "✅ Cron задача установлена. Проверить: crontab -l"
echo "📁 Логи будут в: $LOG_PATH"
