"""
collect_poly_event.py
追踪 Polymarket HK 最高温 event 下所有 market 的价格变化

- 自动追踪 HKT 当天及后两天的 event
- 每小时重新检查是否有新 event 上线
- 价格变化通过 WebSocket 实时捕捉
- 数据按 HKT 日期分区存 Parquet

用法:
    python collect_poly_event.py

依赖:
    pip install websocket-client requests pandas pyarrow
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import websocket

# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────
GAMMA_API    = "https://gamma-api.polymarket.com"
CLOB_WS      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR     = Path("./poly_data")
WS_RECONNECT = 10
TRACK_DAYS   = 3
HKT          = timedelta(hours=8)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collect_poly_event.log"),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 日期工具
# ──────────────────────────────────────────────

def hkt_now() -> datetime:
    return datetime.now(timezone.utc) + HKT

def slug_for_date(dt: datetime) -> str:
    month = dt.strftime("%B").lower()
    day   = str(dt.day)
    year  = str(dt.year)
    return f"highest-temperature-in-hong-kong-on-{month}-{day}-{year}"

def target_slugs() -> list[str]:
    today = hkt_now()
    return [slug_for_date(today + timedelta(days=i)) for i in range(TRACK_DAYS)]


# ──────────────────────────────────────────────
# Parquet 写入
# ──────────────────────────────────────────────

PRICE_SCHEMA = pa.schema([
    pa.field("event_time",  pa.timestamp("us", tz="UTC")),
    pa.field("token_id",    pa.string()),
    pa.field("market_slug", pa.string()),
    pa.field("question",    pa.string()),
    pa.field("event_slug",  pa.string()),
    pa.field("outcome", pa.string()),
    pa.field("best_bid",    pa.float32()),
    pa.field("best_ask",    pa.float32()),
    pa.field("mid",         pa.float32()),
    pa.field("spread",      pa.float32()),
])


def append_rows(rows: list[dict]):
    if not rows:
        return
    import pyarrow.compute as pc
    new_table = pa.Table.from_pylist(rows, schema=PRICE_SCHEMA)
    for event_slug in new_table.column("event_slug").unique().to_pylist():
        mask = pc.equal(new_table.column("event_slug"), event_slug)
        event_table = new_table.filter(mask)
        path = DATA_DIR / "price_ticks" / f"event={event_slug}"
        path.mkdir(parents=True, exist_ok=True)
        path = path / "data.parquet"
        if path.exists():
            existing = pq.read_table(path)
            existing = existing.select([f.name for f in PRICE_SCHEMA if f.name in existing.schema.names])
            existing = existing.cast(pa.schema([f for f in PRICE_SCHEMA if f.name in existing.schema.names]))
            for f in PRICE_SCHEMA:
                if f.name not in existing.schema.names:
                    existing = existing.append_column(f.name, pa.array([None]*len(existing), type=f.type))
            combined = pa.concat_tables([existing, event_table])
        else:
            combined = event_table
        pq.write_table(combined, path, compression="snappy")
    log.info(f"已写入 {len(rows)} 条价格记录")


# ──────────────────────────────────────────────
# Gamma API：发现 markets
# ──────────────────────────────────────────────

def fetch_event_markets(slug: str) -> list[dict]:
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=15)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        log.warning(f"Gamma API 失败 ({slug}): {e}")
        return []

    if not events:
        return []

    event = events[0] if isinstance(events, list) else events
    markets_raw = event.get("markets", [])
    if not markets_raw:
        return []

    markets = []
    for m in markets_raw:
        clob_ids = m.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = [clob_ids]
        for i, token_id in enumerate(clob_ids):
            markets.append({
                "token_id":      token_id,
                "market_slug":   m.get("slug", ""),
                "question":      m.get("question", ""),
                "outcome":       ["YES", "NO"][i] if i < 2 else str(i),
                "event_slug":    slug,
                "condition_id":  m.get("conditionId", ""),
            })
    return markets


def fetch_all_markets() -> list[dict]:
    slugs = target_slugs()
    all_markets = []
    seen = set()
    for slug in slugs:
        markets = fetch_event_markets(slug)
        if markets:
            log.info(f"✓ {slug}  →  {len(markets)} 个 token")
        else:
            log.info(f"✗ {slug}  →  未找到")
        for m in markets:
            if m["token_id"] not in seen:
                seen.add(m["token_id"])
                all_markets.append(m)

    if all_markets:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        meta_path = DATA_DIR / "market_meta.parquet"
        new_meta = pd.DataFrame(all_markets)
        if meta_path.exists():
            old_meta = pd.read_parquet(meta_path)
            new_meta = pd.concat([old_meta, new_meta], ignore_index=True).drop_duplicates('token_id')
        new_meta.to_parquet(meta_path, index=False)

    return all_markets


# ──────────────────────────────────────────────
# WebSocket：价格追踪
# ──────────────────────────────────────────────

class OrderbookTracker:
    def __init__(self):
        self.token_meta: dict[str, dict] = {}
        self._last: dict[str, tuple]     = {}
        self._pending: list[dict]        = []
        self._lock = threading.Lock()
        self._ws   = None

    def update_markets(self, markets: list[dict]):
        new_meta = {m["token_id"]: m for m in markets}
        added = set(new_meta) - set(self.token_meta)
        self.token_meta = new_meta
        if added:
            log.info(f"新增 {len(added)} 个 token，下次重连后生效")

    @staticmethod
    def _best_bid(orders):
        prices = [float(o["price"]) for o in orders if float(o.get("size", 0)) > 0]
        return max(prices) if prices else None

    @staticmethod
    def _best_ask(orders):
        prices = [float(o["price"]) for o in orders if float(o.get("size", 0)) > 0]
        return min(prices) if prices else None

    def on_open(self, ws):
        token_ids = list(self.token_meta.keys())
        ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))
        log.info(f"WebSocket 已订阅 {len(token_ids)} 个 token")

    def on_message(self, ws, raw):
        try:
            msgs = json.loads(raw)
            if isinstance(msgs, dict):
                msgs = [msgs]
            for msg in msgs:
                self._process(msg)
        except Exception as e:
            log.warning(f"消息解析失败: {e}")

    def _process(self, msg):
        token_id = msg.get("asset_id", msg.get("token_id", ""))
        if token_id not in self.token_meta:
            return
        bb = self._best_bid(msg.get("bids", []))
        ba = self._best_ask(msg.get("asks", []))
        if bb is None or ba is None:
            return
        if self._last.get(token_id) == (bb, ba):
            return
        self._last[token_id] = (bb, ba)
        meta = self.token_meta[token_id]
        with self._lock:
            self._pending.append({
                "event_time":  datetime.now(timezone.utc),
                "token_id":    token_id,
                "market_slug": meta["market_slug"],
                "question":    meta["question"],
                "event_slug":  meta["event_slug"],
                "outcome": meta["outcome"],
                "best_bid":    bb,
                "best_ask":    ba,
                "mid":         (bb + ba) / 2,
                "spread":      ba - bb,
            })

    def on_error(self, ws, err):
        log.error(f"WebSocket 错误: {err}")

    def on_close(self, ws, code, msg):
        log.warning(f"WebSocket 断开: code={code}")

    def flush_loop(self):
        while True:
            time.sleep(30)
            with self._lock:
                rows = self._pending[:]
                self._pending.clear()
            if rows:
                append_rows(rows)

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
                self._ws = ws
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.error(f"WebSocket 异常: {e}")
            log.info(f"{WS_RECONNECT}秒后重连...")
            time.sleep(WS_RECONNECT)


# ──────────────────────────────────────────────
# 每小时刷新 market 列表
# ──────────────────────────────────────────────

def refresh_loop(tracker: OrderbookTracker):
    while True:
        time.sleep(3600)
        log.info("刷新 market 列表...")
        markets = fetch_all_markets()
        if markets:
            tracker.update_markets(markets)
            if tracker._ws:
                tracker._ws.close()


# ──────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"启动 HK 温度 event 追踪器（追踪 {TRACK_DAYS} 天）")
    log.info(f"目标 slugs: {target_slugs()}")

    markets = fetch_all_markets()
    if not markets:
        log.warning("当前没有找到任何 event，将持续重试...")

    tracker = OrderbookTracker()
    tracker.update_markets(markets)

    threading.Thread(target=refresh_loop, args=(tracker,), daemon=True, name="Refresher").start()

    tracker.run()


if __name__ == "__main__":
    main()
