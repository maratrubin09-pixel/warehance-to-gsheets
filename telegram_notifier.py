"""
Telegram Notifier — sends alerts to group chat.

Alerts:
  - Package cost = 0 for an order with shipping
  - Pick&Pack fee = 0 for an order with shipping
  - Sync errors
  - Daily sync summary
"""

import logging
import os
import requests

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str = None, chat_id: str = None):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.bot_token and self.chat_id)
        if not self.enabled:
            logger.warning("Telegram notifications disabled (missing token or chat_id)")

    def send(self, text: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }, timeout=10)
            if resp.status_code == 200:
                logger.info("Telegram message sent")
                return True
            else:
                logger.error(f"Telegram error {resp.status_code}: {resp.text}")
                return False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    def notify_anomalies(self, client_name: str, client_number: str,
                         anomalies: list[dict], spreadsheet_id: str = ""):
        if not anomalies:
            return
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}" if spreadsheet_id else ""
        lines = [f"<b>⚠️ {client_number}.{client_name}</b>", ""]
        for a in anomalies:
            order = a.get("order_number", "?")
            issue = a.get("issue", "?")
            lines.append(f"• Заказ <code>{order}</code>: {issue}")
        if sheet_url:
            lines.append(f"\n🔗 <a href=\"{sheet_url}\">Открыть таблицу</a>")
        self.send("\n".join(lines))

    def notify_sync_summary(self, results: list[dict], elapsed: float):
        ok = [r for r in results if "error" not in r]
        fail = [r for r in results if "error" in r]
        total_orders = sum(r.get("orders", 0) for r in ok)
        total_amount = sum(r.get("total", 0) for r in ok)
        lines = [
            f"<b>📊 Синхронизация завершена</b> ({elapsed:.1f}s)",
            f"✅ Клиентов: {len(ok)} | Заказов: {total_orders} | Сумма: ${total_amount:.2f}",
        ]
        if fail:
            lines.append(f"❌ Ошибки: {len(fail)}")
            for r in fail:
                lines.append(f"  • {r['client']}: {r['error']}")
        self.send("\n".join(lines))

    def notify_error(self, message: str):
        self.send(f"<b>🚨 Ошибка синхронизации</b>\n\n{message}")
