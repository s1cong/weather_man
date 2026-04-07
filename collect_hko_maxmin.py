"""
HKO 午夜至现在最高/最低气温采集
================================
每5分钟请求一次 CSV，检查时间戳，存入 parquet。
有新数据时通过 Telegram 推送最新5条记录。

输出表结构:
    csv_time | fetch_time | hko_high | hko_low | max_high | station

环境变量:
    TG_TOKEN   - Telegram bot token
    TG_CHAT_ID - Telegram chat ID
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone, timedelta
import time
import os
import signal
import sys
import logging

# ── 配置 ──────────────────────────────────────────────────────────────────────
URL            = "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_since_midnight_maxmin.csv"
OUTPUT         = "hko_maxmin_history.parquet"
LOG_FILE       = "hko_maxmin_collect.log"
POLL_SEC       = 60 
HKT            = timezone(timedelta(hours=8))
TARGET_STATION = "HK Observatory"

TG_TOKEN   = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

last_csv_time = None


# ── Telegram 推送 ─────────────────────────────────────────────────────────────
def tg_send(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram 推送失败: {e}")


def tg_push_latest(history: pd.DataFrame):
    """推送 parquet 里最新5条数据"""
    recent = history.tail(5)
    lines = ["🌡 <b>HKO 截止目前最高温</b>"]
    for _, r in recent.iterrows():
        lines.append(
            f"{r['csv_time'].strftime('%m-%d %H:%M')}  "
            f"HKO <b>{r['hko_high']}°C</b>  "
            f"全港最高 {r['max_high']}°C ({r['station']})"
        )
    tg_send("\n".join(lines))


# ── 拉取 CSV ──────────────────────────────────────────────────────────────────
def fetch_csv() -> pd.DataFrame | None:
    try:
        r = requests.get(URL, timeout=15)
        r.raise_for_status()
        r.encoding = "utf-8-sig"
        df = pd.read_csv(StringIO(r.text))
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        log.warning(f"拉取失败: {e}")
        return None


# ── 解析 CSV ──────────────────────────────────────────────────────────────────
def parse(df: pd.DataFrame, fetch_time: datetime) -> dict | None:
    try:
        time_col = [c for c in df.columns if "Timestamp" in c or "Date" in c][0]
        csv_time_str = str(df[time_col].iloc[0]).strip()
        csv_time = datetime.strptime(csv_time_str, "%Y%m%d%H%M").replace(tzinfo=HKT)

        stn_col = [c for c in df.columns if "Station" in c or "Automatic" in c][0]
        max_col = [c for c in df.columns if "Maximum" in c][0]
        min_col = [c for c in df.columns if "Minimum" in c][0]

        df[stn_col] = df[stn_col].str.strip()
        df[max_col] = pd.to_numeric(df[max_col], errors="coerce")
        df[min_col] = pd.to_numeric(df[min_col], errors="coerce")

        hko = df[df[stn_col] == TARGET_STATION]
        if hko.empty:
            log.warning(f"找不到 {TARGET_STATION}")
            return None

        hko_high = float(hko[max_col].iloc[0])
        hko_low  = float(hko[min_col].iloc[0])

        max_idx  = df[max_col].idxmax()
        max_high = float(df.loc[max_idx, max_col])
        station  = str(df.loc[max_idx, stn_col])

        return {
            "csv_time":   csv_time,
            "fetch_time": fetch_time,
            "hko_high":   hko_high,
            "hko_low":    hko_low,
            "max_high":   max_high,
            "station":    station,
        }

    except Exception as e:
        log.warning(f"解析失败: {e}")
        return None


# ── 加载已有数据 ───────────────────────────────────────────────────────────────
def load_existing() -> pd.DataFrame:
    if os.path.exists(OUTPUT):
        df = pd.read_parquet(OUTPUT)
        log.info(f"加载已有数据: {len(df)} 行，最新 csv_time: {df['csv_time'].max()}")
        return df
    return pd.DataFrame(columns=["csv_time", "fetch_time", "hko_high", "hko_low", "max_high", "station"])


# ── 保存 ──────────────────────────────────────────────────────────────────────
def save(df: pd.DataFrame):
    df = df.drop_duplicates(subset=["csv_time"], keep="last")
    df = df.sort_values("csv_time").reset_index(drop=True)
    df.to_parquet(OUTPUT, index=False)


# ── 主循环 ────────────────────────────────────────────────────────────────────
def main():
    global last_csv_time

    signal.signal(signal.SIGINT,  lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    log.info("=" * 60)
    log.info("HKO 最高/最低气温采集启动")
    log.info(f"输出: {OUTPUT}  间隔: {POLL_SEC//60} 分钟")
    log.info("=" * 60)

    tg_send("🌡 HKO 气温采集已启动")

    history = load_existing()
    if not history.empty:
        last_csv_time = str(history["csv_time"].max())

    while True:
        fetch_time = datetime.now(HKT)
        raw = fetch_csv()

        if raw is None:
            log.warning("拉取失败，等待下次")
            time.sleep(POLL_SEC)
            continue

        row = parse(raw, fetch_time)
        if row is None:
            time.sleep(POLL_SEC)
            continue

        csv_time_str = row["csv_time"].strftime("%Y%m%d%H%M")
        is_new = (csv_time_str != last_csv_time)

        if is_new:
            new_row = pd.DataFrame([row])
            history = pd.concat([history, new_row], ignore_index=True)
            save(history)
            last_csv_time = csv_time_str
            tg_push_latest(history)
            log.info(
                f"★ NEW  csv={csv_time_str}  "
                f"hko_high={row['hko_high']}°C  hko_low={row['hko_low']}°C  "
                f"max_high={row['max_high']}°C ({row['station']})"
            )
            log.info(f"  → 已保存，当前总行数: {len(history)}")
        else:
            log.info(f"  -    csv={csv_time_str}  hko_high={row['hko_high']}°C  (无新数据)")

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
