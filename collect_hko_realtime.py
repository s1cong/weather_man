"""
collect_hko_realtime.py
HKO 实时气温采集（每5分钟）
================================
采集 HK Observatory 实时温度及全港最高温。

输出表结构:
    csv_time | fetch_time | hko_temp | max_temp | max_station

用法:
    python collect_hko_realtime.py

依赖:
    pip install requests pandas pyarrow
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
URL            = "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_1min_temperature.csv"
OUTPUT         = "hko_realtime_history.parquet"
LOG_FILE       = "hko_realtime_collect.log"
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
    recent = history.tail(5)
    lines = ["🌡 <b>HKO 实时温度</b>"]
    for _, r in recent.iterrows():
        t = r["csv_time"]
        t_str = t.strftime("%m-%d %H:%M") if hasattr(t, "strftime") else str(t)
        lines.append(
            f"{t_str}  HKO <b>{r['hko_temp']}°C</b>  "
            f"全港最高 {r['max_temp']}°C ({r['max_station']})"
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
        time_col = df.columns[0]
        stn_col  = [c for c in df.columns if "Station" in c or "Automatic" in c][0]
        tmp_col  = [c for c in df.columns if "Temperature" in c or "Temp" in c][0]

        # 时间戳：格式 202604080630 存为科学计数法，先转 int
        csv_time_raw = str(df[time_col].iloc[0]).strip()
        # 处理科学计数法如 2.02604E+11
        csv_time_int = int(float(csv_time_raw))
        csv_time = datetime.strptime(str(csv_time_int), "%Y%m%d%H%M").replace(tzinfo=HKT)

        df[stn_col] = df[stn_col].str.strip()
        df[tmp_col] = pd.to_numeric(df[tmp_col], errors="coerce")

        hko = df[df[stn_col] == TARGET_STATION]
        if hko.empty:
            log.warning(f"找不到 {TARGET_STATION}")
            return None

        hko_temp = float(hko[tmp_col].iloc[0])

        max_idx     = df[tmp_col].idxmax()
        max_temp    = float(df.loc[max_idx, tmp_col])
        max_station = str(df.loc[max_idx, stn_col])

        return {
            "csv_time":    csv_time,
            "fetch_time":  fetch_time,
            "hko_temp":    hko_temp,
            "max_temp":    max_temp,
            "max_station": max_station,
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
    return pd.DataFrame(columns=["csv_time", "fetch_time", "hko_temp", "max_temp", "max_station"])


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
    log.info("HKO 实时气温采集启动")
    log.info(f"输出: {OUTPUT}  间隔: {POLL_SEC//60} 分钟")
    log.info("=" * 60)

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
                f"hko_temp={row['hko_temp']}°C  "
                f"max_temp={row['max_temp']}°C ({row['max_station']})"
            )
            log.info(f"  → 已保存，当前总行数: {len(history)}")
        else:
            log.info(f"  -    csv={csv_time_str}  hko_temp={row['hko_temp']}°C  (无新数据)")

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
