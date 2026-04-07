"""
collect_poly_event.py
追踪 Polymarket HK 最高温 event 下所有 market 的：
  - 价格变化（orderbook best bid/ask）via WebSocket
  - 公开成交记录（谁在买卖）via CLOB REST 轮询

环境变量（在 EC2 上设置）：
  POLY_API_KEY
  POLY_SECRET
  POLY_PASSPHRASE
  POLY_ADDRESS

用法:
    python collect_poly_event.py

依赖:
    pip install websocket-client requests pandas pyarrow
"""

import hashlib
import hmac
import json
import logging
import os
import base64
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
CLOB_API     = "https://clob.polymarket.com"
CLOB_WS      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR     = Path("./poly_data")
WS_RECONNECT = 10
TRACK_DAYS   = 3
TRADE_POLL   = 60      # 秒：轮询 trades 的间隔（每分钟一次）
HKT          = timedelta(hours=8)

API_KEY        = os.environ.get("POLY_API_KEY", "")
API_SECRET     = os.environ.get("POLY_SECRET", "")
API_PASSPHRASE = os.environ.get("POLY_PASSPHRASE", "")
API_ADDRESS    = os.environ.get("POLY_ADDRESS", "")

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
# CLOB 认证头（官方 urlsafe HMAC 格式）
# ──────────────────────────────────────────────

def clob_auth_headers(method: str, path: str, body=None) -> dict:
    timestamp = str(int(time.time()))
    message   = timestamp + method.upper() + path
    if body:
        message += str(body).replace("'", '"')
    secret = base64.urlsafe_b64decode(API_SECRET)
    sig    = base64.urlsafe_b64encode(
        hmac.new(secret, message.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    return {
        "POLY_ADDRESS":    API_ADDRESS,
        "POLY_API_KEY":    API_KEY,
        "POLY_SIGNATURE":  sig,
        "POLY_TIMESTAMP":  timestamp,
        "POLY_PASSPHRASE": API_PASSPHRASE,
    }


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
    pa.field("best_bid",    pa.float32()),
    pa.field("best_ask",    pa.float32()),
    pa.field("mid",         pa.float32()),
    pa.field("spread",      pa.float32()),
])

TRADE_SCHEMA = pa.schema([
    pa.field("trade_time",  pa.timestamp("us", tz="UTC")),
    pa.field("trade_id",    pa.string()),
    pa.field("token_id",    pa.string()),
    pa.field("market_slug", pa.string()),
    pa.field("question",    pa.string()),
    pa.field("event_slug",  pa.string()),
    pa.field("side",        pa.string()),
    pa.field("price",       pa.float32()),
    pa.field("size",        pa.float32()),
    pa.field("maker_addr",  pa.string()),
    pa.field("taker_addr",  pa.string()),
])


def append_rows(table_name: str, rows: list[dict], schema: pa.Schema):
    if not rows:
        return
    date_str = hkt_now().strftime("%Y-%m-%d")
    path = DATA_DIR / table_name / f"date={date_str}"
    path.mkdir(parents=True, exist_ok=True)
    path = path / "data.parquet"
    new_table = pa.Table.from_pylist(rows, schema=schema)
    if path.exists():
        existing = pq.read_table(path)
        combined = pa.concat_tables([existing, new_table])
    else:
        combined = new_table
    pq.write_table(combined, path, compression="snappy")


def load_seen_trade_ids() -> set:
    """启动时从已有 parquet 加载所有 trade_id，防止重启后重复写入"""
    seen = set()
    trades_dir = DATA_DIR / "trades"
    if not trades_dir.exists():
        return seen
    for f in trades_dir.rglob("*.parquet"):
        try:
            df = pq.read_table(f, columns=["trade_id"]).to_pandas()
            seen.update(df["trade_id"].tolist())
        except Exception:
            pass
    log.info(f"已加载 {len(seen)} 个历史 trade_id")
    return seen


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
                "token_id":    token_id,
                "market_slug": m.get("slug", ""),
                "question":    m.get("question", ""),
                "outcome":     ["YES", "NO"][i] if i < 2 else str(i),
                "event_slug":  slug,
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
        pd.DataFrame(all_markets).to_parquet(DATA_DIR / "market_meta.parquet", index=False)

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
                append_rows("price_ticks", rows, PRICE_SCHEMA)
                log.info(f"已写入 {len(rows)} 条价格记录")

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
# REST 轮询：拉公开成交（带正确认证）
# ──────────────────────────────────────────────

class TradePoller:
    def __init__(self, seen_ids: set):
        self.token_meta: dict[str, dict] = {}
        self._seen = seen_ids  # 启动时从 parquet 预加载

    def update_markets(self, markets: list[dict]):
        self.token_meta = {m["token_id"]: m for m in markets}

    def poll_token(self, token_id: str) -> list[dict]:
        path = f"/trades?asset_id={token_id}&limit=300"
        try:
            headers = clob_auth_headers("GET", path)
            r = requests.get(f"{CLOB_API}{path}", headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            trades_raw = data if isinstance(data, list) else data.get("data", [])
        except Exception as e:
            log.warning(f"trades 拉取失败 {token_id[:16]}...: {e}")
            return []

        new_rows = []
        for t in trades_raw:
            tid = str(t.get("id", t.get("trade_id", "")))
            if not tid or tid in self._seen:
                continue
            self._seen.add(tid)

            ts_raw = t.get("match_time", t.get("timestamp", t.get("created_at", "")))
            try:
                ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            meta = self.token_meta[token_id]
            new_rows.append({
                "trade_time":  ts,
                "trade_id":    tid,
                "token_id":    token_id,
                "market_slug": meta["market_slug"],
                "question":    meta["question"],
                "event_slug":  meta["event_slug"],
                "side":        t.get("side", ""),
                "price":       float(t.get("price", 0)),
                "size":        float(t.get("size", t.get("amount", 0))),
                "maker_addr":  t.get("maker", t.get("maker_address", "")),
                "taker_addr":  t.get("taker", t.get("taker_address", "")),
            })

        return new_rows

    def run(self):
        log.info("Trade poller 启动")
        while True:
            if not self.token_meta:
                time.sleep(10)
                continue
            all_rows = []
            for token_id in list(self.token_meta.keys()):
                rows = self.poll_token(token_id)
                all_rows.extend(rows)
                time.sleep(0.2)
            if all_rows:
                append_rows("trades", all_rows, TRADE_SCHEMA)
                log.info(f"新成交 {len(all_rows)} 笔")
            time.sleep(TRADE_POLL)


# ──────────────────────────────────────────────
# 每小时刷新 market 列表
# ──────────────────────────────────────────────

def refresh_loop(tracker: OrderbookTracker, poller: TradePoller):
    while True:
        time.sleep(3600)
        log.info("刷新 market 列表...")
        markets = fetch_all_markets()
        if markets:
            tracker.update_markets(markets)
            poller.update_markets(markets)
            if tracker._ws:
                tracker._ws.close()


# ──────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not API_KEY:
        log.error("未找到 POLY_API_KEY，trades 采集将失败")
    if not API_ADDRESS:
        log.error("未找到 POLY_ADDRESS，trades 采集将失败")

    log.info(f"启动 HK 温度 event 追踪器（追踪 {TRACK_DAYS} 天）")
    log.info(f"目标 slugs: {target_slugs()}")

    markets = fetch_all_markets()
    if not markets:
        log.warning("当前没有找到任何 event，将持续重试...")

    seen_ids = load_seen_trade_ids()

    tracker = OrderbookTracker()
    poller  = TradePoller(seen_ids)
    tracker.update_markets(markets)
    poller.update_markets(markets)

    threading.Thread(target=refresh_loop, args=(tracker, poller), daemon=True, name="Refresher").start()
    threading.Thread(target=poller.run, daemon=True, name="TradePoller").start()

    tracker.run()


if __name__ == "__main__":
    main()

