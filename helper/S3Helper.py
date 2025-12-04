import threading
import io
import time
from datetime import datetime

import boto3
from botocore.config import Config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class S3ParquetBuffer:
    """
    Accumulates rows as dicts, periodically flushes to Parquet and uploads to S3.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        buffer_size: int = 1000,
        aws_region: str | None = None,
        flush_interval_sec: float | None = 5.0,
    ):
        """
        bucket: S3 bucket name, e.g. '2092-2968-9871.13012225'
        prefix: S3 key prefix, e.g. 'orderbooks/paradex/'
        buffer_size: flush when this many rows accumulated
        flush_interval_sec: also flush periodically to avoid data sitting forever in memory
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()

        # Single shared client with tuned config for stability
        session = boto3.session.Session()
        cfg = Config(
            region_name=aws_region,
            max_pool_connections=50,
            retries={"max_attempts": 10, "mode": "adaptive"},
            tcp_keepalive=True,
        )
        self.s3 = session.client("s3", config=cfg)

    def add_row(self, row: dict):
        """
        Thread-safe append. 'row' should contain a 'timestamp' and numeric fields.
        """
        with self._lock:
            self._buffer.append(row)
            need_by_size = len(self._buffer) >= self.buffer_size
            need_by_time = (
                self.flush_interval_sec is not None
                and (time.time() - self._last_flush) >= self.flush_interval_sec
            )

        if need_by_size or need_by_time:
            self.flush()

    def flush(self):
        """
        Convert buffer to Parquet and upload to S3 as a single object.
        """
        with self._lock:
            if not self._buffer:
                return
            df = pd.DataFrame(self._buffer)
            self._buffer.clear()
            self._last_flush = time.time()

        # If user screwed up and no rows -> bail
        if df.empty:
            return

        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Simple schema: infer from pandas
        table = pa.Table.from_pandas(df)

        # Create object key with time
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        key = f"{self.prefix}orderbook_{now}.parquet"

        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Upload
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        print(f"[S3] Uploaded {len(df)} rows to s3://{self.bucket}/{key}")
