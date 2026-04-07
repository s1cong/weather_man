"""
hko_bot.py
Telegram bot for querying HKO temperature data.

Commands:
    /high          - last 5 records from daily high collector
    /high N        - last N records (max 20)
    /realtime      - last 5 records from realtime collector
    /realtime N    - last N records (max 20)
    /help          - show all commands

Environment variables:
    TG_TOKEN     - Telegram bot token
    TG_CHAT_ID   - only respond to this chat ID

Usage:
    python hko_bot.py
"""

import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
TG_TOKEN    = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID  = str(os.environ.get("TG_CHAT_ID", ""))
HIGH_FILE   = Path("hko_maxmin_history.parquet")
RT_FILE     = Path("hko_realtime_history.parquet")
DEFAULT_N   = 5
MAX_N       = 20

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

def query_parquet(filepath: Path, n: int, title: str, row_fmt) -> str:
    if not filepath.exists():
        return "❌ No data collected yet."
    df = pd.read_parquet(filepath)
    if df.empty:
        return "❌ Data file is empty."
    n = min(max(1, n), MAX_N)
    recent = df.tail(n)
    lines = [f"🌡 <b>{title} (last {len(recent)})</b>"]
    for _, r in recent.iterrows():
        lines.append(row_fmt(r))
    return "\n".join(lines)


def fmt_high(r) -> str:
    t = r["csv_time"]
    t_str = t.strftime("%m-%d %H:%M") if hasattr(t, "strftime") else str(t)
    return f"{t_str}  HKO high <b>{r['hko_high']}°C</b>  All-high {r['max_high']}°C ({r['station']})"


def fmt_realtime(r) -> str:
    t = r["csv_time"]
    t_str = t.strftime("%m-%d %H:%M") if hasattr(t, "strftime") else str(t)
    return f"{t_str}  HKO <b>{r['hko_temp']}°C</b>  All-high {r['max_temp']}°C ({r['max_station']})"


def parse_n(parts: list) -> int | None:
    if len(parts) > 1:
        try:
            return int(parts[1])
        except ValueError:
            return None
    return DEFAULT_N


def handle_message(message: dict):
    chat_id = str(message.get("chat", {}).get("id", ""))
    text    = message.get("text", "").strip()

    if chat_id != TG_CHAT_ID:
        log.warning(f"Ignored message from: {chat_id}")
        return

    parts = text.split()
    cmd   = parts[0].lower() if parts else ""

    if cmd in ("/high", "/daily"):
        n = parse_n(parts)
        if n is None:
            send(chat_id, f"⚠️ Usage: /high or /high N (max {MAX_N})")
            return
        msg = query_parquet(HIGH_FILE, n, "Daily High Temp", fmt_high)
        send(chat_id, msg)

    elif cmd in ("/realtime", "/rt"):
        n = parse_n(parts)
        if n is None:
            send(chat_id, f"⚠️ Usage: /realtime or /realtime N (max {MAX_N})")
            return
        msg = query_parquet(RT_FILE, n, "Realtime Temp", fmt_realtime)
        send(chat_id, msg)

    elif cmd in ("/help", "/start"):
        send(chat_id, (
            "🌡 <b>HKO Weather Bot</b>\n\n"
            f"/high — last {DEFAULT_N} daily high records\n"
            f"/high N — last N records (max {MAX_N})\n\n"
            f"/realtime — last {DEFAULT_N} realtime records\n"
            f"/realtime N — last N records (max {MAX_N})"
        ))

    else:
        send(chat_id, "Unknown command. Try /high, /realtime, or /help")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    if not TG_TOKEN:
        log.error("TG_TOKEN not set")
        return

    log.info("HKO bot started, listening for commands...")
    send(TG_CHAT_ID, "🤖 HKO bot online.\n/high — daily high\n/realtime — realtime temp\n/help — all commands")

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
