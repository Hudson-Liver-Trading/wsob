from datetime import datetime
from helper.S3Helper import S3ParquetBuffer
from helper.WShandler import BaseOrderBookWS
from collections import namedtuple
import threading
import time

class ParadexBBO(BaseOrderBookWS):
    def __init__(self, market: str, s3_buffer: S3ParquetBuffer):
        self.market = market
        self.channel = f"bbo.{market}"
        ws_url = "wss://ws.api.prod.paradex.trade/v1"
        super().__init__(name=f"Paradex-{market}", ws_url=ws_url, s3_buffer=s3_buffer)

    def _build_subscribe_message(self):
        return {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {"channel": self.channel},
            "id": 1,
        }

    def _parse_order_book(self, raw: dict) -> dict | None:
        # Paradex subscription update format
        if raw.get("method") != "subscription":
            return None
        params = raw.get("params", {})
        if params.get("channel") != self.channel:
            return None

        inner = params.get("data", {})
        bid_px = inner.get("bid")
        bid_sz = inner.get("bid_size")
        ask_px = inner.get("ask")
        ask_sz = inner.get("ask_size")

        if not (bid_px and bid_sz and ask_px and ask_sz):
            return None

        ts = datetime.utcnow()  # use server ts if they give you one, but they don't here

        return {
            "timestamp": ts,
            "para_best_bid_px": float(bid_px),
            "para_best_bid_sz": float(bid_sz),
            "para_best_ask_px": float(ask_px),
            "para_best_ask_sz": float(ask_sz),
        }

HyperOrderBook = namedtuple(
    "HyperOrderBook",
    ["ts", "best_bid_px", "best_bid_sz", "best_ask_px", "best_ask_sz"],
)


class HyperliquidL2(BaseOrderBookWS):
    def __init__(
        self,
        symbol: str,
        s3_buffer: S3ParquetBuffer,
        snapshot_interval: float = 0.1,
    ):
        ws_url = "wss://api.hyperliquid.xyz/ws"
        super().__init__(name=f"Hyperliquid-{symbol}", ws_url=ws_url, s3_buffer=s3_buffer)
        self.symbol = symbol
        self.snapshot_interval = snapshot_interval

        self._latest: HyperOrderBook | None = None
        self._latest_lock = threading.Lock()
        self._snapshot_thread: threading.Thread | None = None
        self._snapshot_stop = threading.Event()

    def _build_subscribe_message(self):
        return {
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": self.symbol,
            },
        }

    def _parse_order_book(self, raw: dict) -> dict | None:
        # Here we don't push directly to S3; we update latest and let
        # snapshot thread sample at fixed interval.
        if raw.get("channel") != "l2Book":
            return None

        inner = raw.get("data", {})
        if "levels" not in inner:
            return None

        bids = inner["levels"][0]
        asks = inner["levels"][1]
        if not bids or not asks:
            return None

        best_bid_px = float(bids[0]["px"])
        best_bid_sz = float(bids[0]["sz"])
        best_ask_px = float(asks[0]["px"])
        best_ask_sz = float(asks[0]["sz"])

        ts = datetime.fromtimestamp(inner["time"] / 1000)

        ob = HyperOrderBook(ts, best_bid_px, best_bid_sz, best_ask_px, best_ask_sz)
        with self._latest_lock:
            self._latest = ob

        # No direct S3 write here
        return None

    def _snapshot_loop(self):
        while not self._snapshot_stop.is_set():
            with self._latest_lock:
                ob = self._latest
            if ob is not None:
                now = datetime.utcnow()
                row = {
                    "timestamp": now,
                    "hl_best_bid_px": ob.best_bid_px,
                    "hl_best_bid_sz": ob.best_bid_sz,
                    "hl_best_ask_px": ob.best_ask_px,
                    "hl_best_ask_sz": ob.best_ask_sz,
                }
                self.s3_buffer.add_row(row)
                # print(f"[Hyperliquid] snapshot {row}")
            time.sleep(self.snapshot_interval)

    def start(self):
        super().start()
        if not self._snapshot_thread or not self._snapshot_thread.is_alive():
            self._snapshot_stop.clear()
            self._snapshot_thread = threading.Thread(
                target=self._snapshot_loop, daemon=True
            )
            self._snapshot_thread.start()
            print("[Hyperliquid] Snapshot thread started.")

    def stop(self):
        self._snapshot_stop.set()
        if self._snapshot_thread:
            self._snapshot_thread.join(timeout=5)
        super().stop()

BinOrderBook = namedtuple(
    "BinOrderBook",
    ["ts", "best_bid_px", "best_bid_sz", "best_ask_px", "best_ask_sz"],
)


class BinanceDepth10(BaseOrderBookWS):
    def __init__(self, symbol: str, s3_buffer: S3ParquetBuffer):
        """
        symbol like 'ETHUSDT'
        """
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth10@100ms"
        super().__init__(name=f"Binance-{symbol}", ws_url=ws_url, s3_buffer=s3_buffer)

    def _build_subscribe_message(self):
        # Binance stream URL already selects the channel; no subscribe msg needed
        return None

    def _parse_order_book(self, raw: dict) -> dict | None:
        # 'bids' and 'asks' are lists of [price, qty]
        bids = raw.get("bids")
        asks = raw.get("asks")
        if not bids or not asks:
            return None

        best_bid_px, best_bid_sz = bids[0]
        best_ask_px, best_ask_sz = asks[0]

        ob = BinOrderBook(
            datetime.utcnow(),
            float(best_bid_px),
            float(best_bid_sz),
            float(best_ask_px),
            float(best_ask_sz),
        )

        return {
            "timestamp": ob.ts,
            "bin_best_bid_px": ob.best_bid_px,
            "bin_best_bid_sz": ob.best_bid_sz,
            "bin_best_ask_px": ob.best_ask_px,
            "bin_best_ask_sz": ob.best_ask_sz,
        }
