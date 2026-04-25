"""
collect_ob_snapshot.py
订阅 Polymarket HK 最高温 event 下所有 YES token 的 orderbook，
维护内存 ob 状态，每次任意一档 ask 发生变化时存一行 5 档快照。

- 自动发现今天+后两天共 3 个 event
- 每个 event 数据存进各自对应的 parquet（按 event 日期命名）
- 每小时重新检查是否有新 event 上线
- 每个 event 到当天 HKT 24:00 后停止采集

用法:
    python collect_ob_snapshot.py

存储路径:
    /home/ubuntu/weather_man/poly_data/ob_snapshots/YYYY-MM-DD.parquet
"""

import json
import logging
import re
import threading
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import websocket

# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────
GAMMA_API    = "https://gamma-api.polymarket.com"
CLOB_WS      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# DATA_DIR     = Path("/home/ubuntu/weather_man/poly_data/ob_snapshots")
DATA_DIR = Path("/Users/eric/Developer/poly_data/ob_snapshots")
WS_RECONNECT = 10
FLUSH_EVERY  = 30
TRACK_DAYS   = 3
HKT          = ZoneInfo("Asia/Hong_Kong")

MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collect_ob_snapshot.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────

def hkt_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(HKT)


def slug_for_date(dt: datetime) -> str:
    month = dt.strftime("%B").lower()
    return f"highest-temperature-in-hong-kong-on-{month}-{dt.day}-{dt.year}"


def target_slugs() -> list[str]:
    today = hkt_now()
    return [slug_for_date(today + timedelta(days=i)) for i in range(TRACK_DAYS)]


def parse_event_date(slug: str) -> date:
    m = re.search(r'-on-(\w+)-(\d+)-(\d{4})$', slug)
    if m:
        return date(int(m.group(3)), MONTHS[m.group(1)], int(m.group(2)))
    raise ValueError(f"无法从 slug 解析日期: {slug}")


def slug_to_bucket(slug: str) -> str:
    m = re.search(r'-(\d+)c?or.?below', slug)
    if m: return f"{m.group(1)}-"
    m = re.search(r'-(\d+)c?or.?higher', slug)
    if m: return f"{m.group(1)}+"
    m = re.search(r'-(\d+)c$', slug)
    if m: return m.group(1)
    return slug


# ──────────────────────────────────────────────
# Parquet Schema
# ──────────────────────────────────────────────

OB_SCHEMA = pa.schema([
    pa.field("timestamp",  pa.timestamp("us", tz="Asia/Hong_Kong")),
    pa.field("bucket",     pa.string()),
    pa.field("ask1_price", pa.float32()), pa.field("ask1_size", pa.float32()),
    pa.field("ask2_price", pa.float32()), pa.field("ask2_size", pa.float32()),
    pa.field("ask3_price", pa.float32()), pa.field("ask3_size", pa.float32()),
    pa.field("ask4_price", pa.float32()), pa.field("ask4_size", pa.float32()),
    pa.field("ask5_price", pa.float32()), pa.field("ask5_size", pa.float32()),
])


def append_rows(rows: list[dict], event_date: date):
    if not rows:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{event_date}.parquet"
    new_table = pa.Table.from_pylist(rows, schema=OB_SCHEMA)
    if path.exists():
        combined = pa.concat_tables([pq.read_table(path), new_table])
    else:
        combined = new_table
    pq.write_table(combined, path, compression="snappy")
    log.info(f"写入 {len(rows)} 行 ob 快照 → {path.name}")


# ──────────────────────────────────────────────
# Gamma API：拉所有 event 的 YES token
# ──────────────────────────────────────────────

def fetch_yes_tokens(event_slug: str) -> list[dict]:
    r = requests.get(f"{GAMMA_API}/events", params={"slug": event_slug}, timeout=15)
    r.raise_for_status()
    events = r.json()
    if not events:
        return []

    event = events[0] if isinstance(events, list) else events
    tokens = []
    for m in event.get("markets", []):
        clob_ids = m.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = [clob_ids]
        if clob_ids:
            tokens.append({
                "token_id":    clob_ids[0],
                "market_slug": m.get("slug", ""),
                "question":    m.get("question", ""),
                "bucket":      slug_to_bucket(m.get("slug", "")),
                "event_slug":  event_slug,
                "event_date":  parse_event_date(event_slug),
            })
    return tokens


def fetch_all_tokens() -> list[dict]:
    slugs = target_slugs()
    all_tokens = []
    seen = set()
    for slug in slugs:
        tokens = fetch_yes_tokens(slug)
        if tokens:
            log.info(f"✓ {slug}  →  {len(tokens)} 个 YES token")
        else:
            log.info(f"✗ {slug}  →  未找到")
        for t in tokens:
            if t["token_id"] not in seen:
                seen.add(t["token_id"])
                all_tokens.append(t)
    return all_tokens


# ──────────────────────────────────────────────
# OB 状态维护 + 快照记录
# ──────────────────────────────────────────────

class ObSnapshotTracker:
    def __init__(self):
        self.token_meta: dict[str, dict] = {}
        self._asks: dict[str, dict[str, float]] = {}
        self._last_top5: dict[str, list] = {}
        self._pending: dict[date, list[dict]] = {}   # event_date → rows
        self._lock = threading.Lock()

    def update_tokens(self, tokens: list[dict]):
        new_meta = {t["token_id"]: t for t in tokens}
        added = set(new_meta) - set(self.token_meta)
        self.token_meta = new_meta
        for tid in added:
            self._asks[tid] = {}
        if added:
            log.info(f"新增 {len(added)} 个 token，下次重连后生效")

    @staticmethod
    def _top5(asks: dict[str, float]) -> list[tuple[float, float]]:
        result = sorted(
            ((float(p), s) for p, s in asks.items() if s > 0),
            key=lambda x: x[0]
        )[:5]
        while len(result) < 5:
            result.append((float("nan"), 0.0))
        return result

    def _handle_book(self, msg: dict):
        token_id = msg.get("asset_id", "")
        if token_id not in self.token_meta:
            return
        new_asks = {}
        for item in msg.get("asks", []):
            price = str(item.get("price", ""))
            size  = float(item.get("size", 0))
            if price and size > 0:
                new_asks[price] = size
        self._asks[token_id] = new_asks
        self._maybe_snapshot(token_id)

    def _handle_price_change(self, msg: dict):
        for pc in msg.get("price_changes", []):
            token_id = pc.get("asset_id", "")
            if token_id not in self.token_meta:
                continue
            if pc.get("side", "").upper() != "SELL":
                continue
            price = str(pc.get("price", ""))
            size  = float(pc.get("size", 0))
            if size == 0:
                self._asks[token_id].pop(price, None)
            else:
                self._asks[token_id][price] = size
            self._maybe_snapshot(token_id)

    def _maybe_snapshot(self, token_id: str):
        meta = self.token_meta[token_id]
        event_date = meta["event_date"]
        # event 当天 HKT 24:00 后停止采集
        if hkt_now().date() > event_date:
            return
        top5 = self._top5(self._asks.get(token_id, {}))
        if top5 == self._last_top5.get(token_id):
            return
        self._last_top5[token_id] = top5
        row = {"timestamp": hkt_now(), "bucket": meta["bucket"]}
        for i, (price, size) in enumerate(top5, start=1):
            row[f"ask{i}_price"] = price
            row[f"ask{i}_size"]  = size
        with self._lock:
            self._pending.setdefault(event_date, []).append(row)

    def on_open(self, ws):
        token_ids = list(self.token_meta.keys())
        ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))
        log.info(f"WS 已订阅 {len(token_ids)} 个 YES token")

    def on_message(self, ws, raw):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for msg in data:
                    self._handle_book(msg)
            elif isinstance(data, dict):
                et = data.get("event_type", "")
                if et == "book":
                    self._handle_book(data)
                elif et == "price_change":
                    self._handle_price_change(data)
        except Exception as e:
            log.warning(f"消息处理失败: {e}  raw={raw[:200]}")

    def on_error(self, ws, err):
        log.error(f"WS 错误: {err}")

    def on_close(self, ws, code, msg):
        log.warning(f"WS 断开: code={code}")

    def flush_loop(self):
        while True:
            time.sleep(FLUSH_EVERY)
            with self._lock:
                pending = {k: v[:] for k, v in self._pending.items()}
                self._pending.clear()
            for event_date, rows in pending.items():
                append_rows(rows, event_date)

    def run(self):
        threading.Thread(target=self.flush_loop, daemon=True).start()
        while True:
            if not self.token_meta:
                time.sleep(10)
                continue
            try:
                ws = websocket.WebSocketApp(
                    CLOB_WS,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.error(f"WS 异常: {e}")
            log.info(f"{WS_RECONNECT}s 后重连...")
            time.sleep(WS_RECONNECT)


# ──────────────────────────────────────────────
# 每小时刷新 event 列表
# ──────────────────────────────────────────────

def refresh_loop(tracker: ObSnapshotTracker):
    while True:
        time.sleep(3600)
        log.info("刷新 event 列表...")
        tokens = fetch_all_tokens()
        if tokens:
            tracker.update_tokens(tokens)


# ──────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────

def main():
    log.info(f"追踪 {TRACK_DAYS} 天 event: {target_slugs()}")

    tokens = fetch_all_tokens()
    if not tokens:
        log.error("未找到任何 event，退出")
        return

    tracker = ObSnapshotTracker()
    tracker.update_tokens(tokens)

    threading.Thread(target=refresh_loop, args=(tracker,), daemon=True, name="Refresher").start()

    log.info("OB snapshot 采集启动...")
    tracker.run()


if __name__ == "__main__":
    main()

