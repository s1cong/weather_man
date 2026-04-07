"""
hko_bot.py
Telegram bot for querying HKO temperature history.

Commands:
    /latest      - return last 5 records (default)
    /latest N    - return last N records (max 20)

Environment variables:
    TG_TOKEN     - Telegram bot token
    TG_CHAT_ID   - Telegram chat ID (only respond to this user)

Usage:
    python hko_bot.py
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
TG_TOKEN    = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID  = str(os.environ.get("TG_CHAT_ID", ""))
DATA_FILE   = Path("hko_maxmin_history.parquet")
DEFAULT_N   = 5
MAX_N       = 20
HKT         = timezone(timedelta(hours=8))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("hko_bot.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

BASE_URL = f"https://api.telegram.org/bot{TG_TOKEN}"


# ── Telegram helpers ──────────────────────────────────────────────────────────

def send(chat_id: str, text: str):
    try:
        requests.post(
            f"{BASE_URL}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Send failed: {e}")


def get_updates(offset: int | None = None) -> list:
    """Long polling — waits up to 30s for new messages."""
    params = {"timeout": 30, "allowed_updates": ["message"]}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(f"{BASE_URL}/getUpdates", params=params, timeout=35)
        r.raise_for_status()
        return r.json().get("result", [])
    except Exception as e:
        log.warning(f"getUpdates failed: {e}")
        time.sleep(5)
        return []


# ── Command handlers ──────────────────────────────────────────────────────────

def handle_latest(chat_id: str, n: int):
    if not DATA_FILE.exists():
        send(chat_id, "❌ No data collected yet.")
        return

    df = pd.read_parquet(DATA_FILE)
    if df.empty:
        send(chat_id, "❌ Data file is empty.")
        return

    n = min(max(1, n), MAX_N)
    recent = df.tail(n)

    lines = [f"🌡 <b>HKO Last {len(recent)} Records</b>"]
    for _, r in recent.iterrows():
        t = r["csv_time"]
        if hasattr(t, "strftime"):
            t_str = t.strftime("%m-%d %H:%M")
        else:
            t_str = str(t)
        lines.append(
            f"{t_str}  HKO <b>{r['hko_high']}°C</b>  "
            f"All-high {r['max_high']}°C ({r['station']})"
        )

    send(chat_id, "\n".join(lines))


def handle_message(message: dict):
    chat_id = str(message.get("chat", {}).get("id", ""))
    text    = message.get("text", "").strip()

    # Only respond to authorised user
    if chat_id != TG_CHAT_ID:
        log.warning(f"Ignored message from unknown chat_id: {chat_id}")
        return

    if text.startswith("/latest"):
        parts = text.split()
        n = DEFAULT_N
        if len(parts) > 1:
            try:
                n = int(parts[1])
            except ValueError:
                send(chat_id, f"⚠️ Usage: /latest or /latest N (max {MAX_N})")
                return
        handle_latest(chat_id, n)

    elif text.startswith("/help") or text.startswith("/start"):
        send(chat_id, (
            "🌡 <b>HKO Weather Bot</b>\n\n"
            f"/latest — last {DEFAULT_N} records\n"
            f"/latest N — last N records (max {MAX_N})"
        ))

    else:
        send(chat_id, "Unknown command. Try /latest or /help")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    if not TG_TOKEN:
        log.error("TG_TOKEN not set")
        return

    log.info("HKO bot started, listening for commands...")
    send(TG_CHAT_ID, "🤖 HKO bot is online. Try /latest or /help")

    offset = None
    while True:
        updates = get_updates(offset)
        for update in updates:
            offset = update["update_id"] + 1
            message = update.get("message")
            if message:
                handle_message(message)


if __name__ == "__main__":
    main()
