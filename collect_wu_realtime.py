import requests
import pandas as pd
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/ubuntu/weather_man/collect_wu_realtime.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

API_KEY    = "7bd1d84cc9f4480a91d84cc9f4f80a1d"
STATION_ID = "IHONGKON111"
OUT_FILE   = Path("/home/ubuntu/weather_man/wu_realtime.parquet")
POLL_SEC   = 60  # 每分钟poll一次

def fetch_wu() -> dict | None:
    try:
        r = requests.get(
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": STATION_ID,
                "format":    "json",
                "units":     "m",
                "apiKey":    API_KEY,
            },
            timeout=10,
        )
        r.raise_for_status()
        obs = r.json()["observations"][0]
        temp = obs["metric"]["temp"]
        if temp is None:
            log.warning("WU returned None temp")
            return None
        return {
            "timestamp": pd.Timestamp(obs["obsTimeUtc"]).tz_convert("Asia/Hong_Kong"),
            "temp":      float(temp),
            "station":   STATION_ID,
        }
    except Exception as e:
        log.error(f"WU fetch error: {e}")
        return None

def write_parquet(row: dict):
    df = pd.DataFrame([row])
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("Asia/Hong_Kong")
    df["station"]   = df["station"].astype("category")
    df.to_parquet(OUT_FILE, index=False)
    log.info(f"写入: {row['timestamp']}  temp={row['temp']}°C")

if __name__ == "__main__":
    log.info(f"启动 WU 实时采集 → {OUT_FILE}")
    while True:
        row = fetch_wu()
        if row:
            write_parquet(row)
        time.sleep(POLL_SEC)
