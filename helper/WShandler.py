import json
import websocket
from helper.S3Helper import S3ParquetBuffer
import threading
import time
from abc import ABC, abstractmethod


class BaseOrderBookWS(ABC):
    """
    Base class for a single order-book WebSocket feed that pushes rows into an S3ParquetBuffer.
    """

    def __init__(
        self,
        name: str,
        ws_url: str,
        s3_buffer: S3ParquetBuffer,
        ping_interval: int = 20,
        ping_timeout: int = 10,
        reconnect_delay: float = 5.0,
    ):
        self.name = name
        self.ws_url = ws_url
        self.s3_buffer = s3_buffer
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect_delay = reconnect_delay

        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    @abstractmethod
    def _build_subscribe_message(self) -> dict | None:
        """
        Return message to send on open. Return None if no subscribe message is needed.
        """
        ...

    @abstractmethod
    def _parse_order_book(self, raw: dict) -> dict | None:
        """
        Parse raw JSON into a dict row for S3ParquetBuffer.
        Return None to skip.
        """
        ...

    # WebSocket callbacks

    def _on_open(self, ws):
        print(f"[{self.name}] WebSocket connected.")
        sub_msg = self._build_subscribe_message()
        if sub_msg is not None:
            ws.send(json.dumps(sub_msg))
            print(f"[{self.name}] Subscription sent: {sub_msg}")

    def _on_message(self, ws, message: str):
        try:
            data = json.loads(message)
            row = self._parse_order_book(data)
            if row is not None:
                self.s3_buffer.add_row(row)
        except Exception as e:
            print(f"[{self.name}] on_message error: {e}")

    def _on_error(self, ws, error):
        print(f"[{self.name}] WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print(f"[{self.name}] WebSocket closed: {close_status_code}, {close_msg}")

    # Lifecycle

    def _run_forever(self):
        while not self._stop_event.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                )
            except Exception as e:
                print(f"[{self.name}] run_forever exception: {e}")
            if self._stop_event.is_set():
                break
            print(f"[{self.name}] Reconnecting after {self.reconnect_delay}s...")
            time.sleep(self.reconnect_delay)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, daemon=True)
        self._thread.start()
        print(f"[{self.name}] Started.")

    def stop(self):
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
        # flush remaining rows
        self.s3_buffer.flush()
        print(f"[{self.name}] Stopped.")
