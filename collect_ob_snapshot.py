"""
collect_ob_snapshot.py
订阅 Polymarket 指定 event 下所有 YES token 的 orderbook，
维护内存 ob 状态，每次任意一档 ask 发生变化时存一行 5 档快照。

用法:
    python collect_ob_snapshot.py --event "highest-temperature-in-hong-kong-on-april-24-2026"

存储路径:
    /home/ubuntu/weather_man/poly_data/ob_snapshots/YYYY-MM-DD.parquet
    每天一个文件，所有 YES token 在同一文件，用 token_id 区分。

Schema:
    timestamp   datetime64[us, Asia/Hong_Kong]
    token_id    str
    ask1_price  float32
    ask1_size   float32
    ask2_price  float32
    ask2_size   float32
    ask3_price  float32
    ask3_size   float32
    ask4_price  float32
    ask4_size   float32
    ask5_price  float32
    ask5_size   float32
"""

import argparse
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
DATA_DIR     = Path("/home/ubuntu/weather_man/poly_data/ob_snapshots")
WS_RECONNECT = 10   # 秒：断线重连等待
FLUSH_EVERY  = 30   # 秒：批量写盘间隔
HKT          = ZoneInfo("Asia/Hong_Kong")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collect_ob_snapshot.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def slug_to_bucket(slug):
    import re
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
    pa.field("bucket", pa.string()),
    pa.field("ask1_price", pa.float32()),
    pa.field("ask1_size",  pa.float32()),
    pa.field("ask2_price", pa.float32()),
    pa.field("ask2_size",  pa.float32()),
    pa.field("ask3_price", pa.float32()),
    pa.field("ask3_size",  pa.float32()),
    pa.field("ask4_price", pa.float32()),
    pa.field("ask4_size",  pa.float32()),
    pa.field("ask5_price", pa.float32()),
    pa.field("ask5_size",  pa.float32()),
])

EMPTY_ASK = {"price": float("nan"), "size": 0.0}


def _today_path() -> Path:
    date_str = datetime.now(HKT).strftime("%Y-%m-%d")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"{date_str}.parquet"


def append_rows(rows: list[dict]):
    if not rows:
        return
    path = _today_path()
    new_table = pa.Table.from_pylist(rows, schema=OB_SCHEMA)
    if path.exists():
        existing = pq.read_table(path)
        combined = pa.concat_tables([existing, new_table])
    else:
        combined = new_table
    pq.write_table(combined, path, compression="snappy")
    log.info(f"写入 {len(rows)} 行 ob 快照 → {path.name}")


# ──────────────────────────────────────────────
# Gamma API：拉 YES token 列表
# ──────────────────────────────────────────────
def fetch_yes_tokens(event_slug: str) -> list[dict]:
    log.info(f"查询 event: {event_slug}")
    r = requests.get(f"{GAMMA_API}/events", params={"slug": event_slug}, timeout=15)
    r.raise_for_status()
    events = r.json()

    if not events:
        log.warning("slug 未匹配，尝试关键词搜索...")
        r = requests.get(f"{GAMMA_API}/events", params={"q": event_slug, "limit": 10}, timeout=15)
        r.raise_for_status()
        events = r.json()

    if not events:
        raise ValueError(f"找不到 event: {event_slug}")

    event = events[0] if isinstance(events, list) else events
    log.info(f"找到 event: {event.get('title', '?')}")

    tokens = []
    for m in event.get("markets", []):
        clob_ids = m.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = [clob_ids]
        if clob_ids:
            # index 0 = YES token
            tokens.append({
                "token_id":    clob_ids[0],
                "market_slug": m.get("slug", ""),
                "question":    m.get("question", ""),
                "bucket":      slug_to_bucket(m.get("slug", "")),  # 新增
            })

    log.info(f"共找到 {len(tokens)} 个 YES token")
    return tokens


# ──────────────────────────────────────────────
# OB 状态维护 + 快照记录
# ──────────────────────────────────────────────
class ObSnapshotTracker:
    def __init__(self, tokens: list[dict]):
        self.tokens     = tokens
        self.token_ids  = {t["token_id"] for t in tokens}

        # 内存 ob：token_id → { price_str → size_float }
        # 只维护 asks（卖方挂单），不存 bids
        self._asks: dict[str, dict[str, float]] = {t["token_id"]: {} for t in tokens}

        # 上一次存快照时的 top5，用于去重
        self._last_top5: dict[str, list] = {}

        self._pending: list[dict] = []
        self._lock    = threading.Lock()
        self.token_meta = {t["token_id"]: t for t in tokens}

    # ── 从 asks dict 取 top5 ──────────────────
    @staticmethod
    def _top5(asks: dict[str, float]) -> list[tuple[float, float]]:
        """返回价格最低的 5 档 (price, size)，不足 5 档用 nan/0 填充"""
        sorted_asks = sorted(
            ((float(p), s) for p, s in asks.items() if s > 0),
            key=lambda x: x[0]
        )
        result = sorted_asks[:5]
        while len(result) < 5:
            result.append((float("nan"), 0.0))
        return result

    # ── 处理 book snapshot（订阅后第一条完整 ob）──
    def _handle_book(self, msg: dict):
        token_id = msg.get("asset_id", "")
        if token_id not in self.token_ids:
            return

        asks_raw = msg.get("asks", [])
        new_asks  = {}
        for item in asks_raw:
            price = str(item.get("price", ""))
            size  = float(item.get("size", 0))
            if price and size > 0:
                new_asks[price] = size

        self._asks[token_id] = new_asks
        self._maybe_snapshot(token_id)
        log.debug(f"[BOOK] {token_id[:12]}...  asks={len(new_asks)} 档")

    # ── 处理 price_change（增量更新）────────────
    def _handle_price_change(self, msg: dict):
        for pc in msg.get("price_changes", []):
            token_id = pc.get("asset_id", "")
            if token_id not in self.token_ids:
                continue
            side  = pc.get("side", "").upper()
            if side != "SELL":
                # 只关心 asks（SELL side）
                continue

            price = str(pc.get("price", ""))
            size  = float(pc.get("size", 0))

            if size == 0:
                self._asks[token_id].pop(price, None)
            else:
                self._asks[token_id][price] = size

            self._maybe_snapshot(token_id)

    # ── 如果 top5 发生变化就记录快照 ──────────
    def _maybe_snapshot(self, token_id: str):
        top5 = self._top5(self._asks[token_id])
        if top5 == self._last_top5.get(token_id):
            return
        self._last_top5[token_id] = top5

        now = datetime.now(HKT)
        row = {"timestamp": now, "bucket": self.token_meta[token_id]["bucket"]}
        for i, (price, size) in enumerate(top5, start=1):
            row[f"ask{i}_price"] = price
            row[f"ask{i}_size"]  = size

        with self._lock:
            self._pending.append(row)

    # ── WebSocket 回调 ────────────────────────
    def on_open(self, ws):
        sub = {"assets_ids": [t["token_id"] for t in self.tokens], "type": "market"}
        ws.send(json.dumps(sub))
        log.info(f"WS 已订阅 {len(self.tokens)} 个 YES token")

    def on_message(self, ws, raw):
        try:
            data = json.loads(raw)
            # book snapshot 是 list
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

    # ── 后台 flush 线程 ───────────────────────
    def flush_loop(self):
        while True:
            time.sleep(FLUSH_EVERY)
            with self._lock:
                rows = self._pending[:]
                self._pending.clear()
            if rows:
                append_rows(rows)

    # ── 主循环（带断线重连）──────────────────
    def run(self):
        threading.Thread(target=self.flush_loop, daemon=True).start()
        while True:
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
# 主入口
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Polymarket OB snapshot collector")
    parser.add_argument(
        "--event",
        required=True,
        help="Event slug，例如 highest-temperature-in-hong-kong-on-april-24-2026",
    )
    args = parser.parse_args()

    tokens = fetch_yes_tokens(args.event)
    log.info("YES token 列表:")
    for t in tokens:
        log.info(f"  {t['question'][:60]}  token={t['token_id'][:16]}...")

    tracker = ObSnapshotTracker(tokens)
    log.info("OB snapshot 采集启动...")
    tracker.run()


if __name__ == "__main__":
    main()
