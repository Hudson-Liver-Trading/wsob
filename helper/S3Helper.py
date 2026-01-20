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
        max_upload_retries: int = 3,
    ):
        """
        bucket: S3 bucket name, e.g. '2092-2968-9871.13012225'
        prefix: S3 key prefix, e.g. 'orderbooks/paradex/'
        buffer_size: flush when this many rows accumulated
        flush_interval_sec: also flush periodically to avoid data sitting forever in memory
        max_upload_retries: number of times to retry S3 upload on failure
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec
        self.max_upload_retries = max_upload_retries

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()  # Separate lock to prevent concurrent flushes
        self._last_flush = time.time()

        # Single shared client with tuned config for stability
        session = boto3.session.Session()
        cfg = Config(
            region_name=aws_region,
            max_pool_connections=50,
            retries={"max_attempts": 10, "mode": "adaptive"},
            tcp_keepalive=True,
            connect_timeout=10,
            read_timeout=30,
        )
        self.s3 = session.client("s3", config=cfg)

    def add_row(self, row: dict):
        """
        Thread-safe append. 'row' should contain a 'timestamp' and numeric fields.
        """
        should_flush = False
        with self._lock:
            self._buffer.append(row)
            need_by_size = len(self._buffer) >= self.buffer_size
            need_by_time = (
                self.flush_interval_sec is not None
                and (time.time() - self._last_flush) >= self.flush_interval_sec
            )
            should_flush = need_by_size or need_by_time

        if should_flush:
            self.flush()

    def flush(self):
        """
        Convert buffer to Parquet and upload to S3 as a single object.
        Thread-safe: only one flush can run at a time, and data is only cleared after successful upload.
        """
        # Prevent concurrent flushes - if another thread is flushing, skip
        if not self._flush_lock.acquire(blocking=False):
            return
        
        try:
            # Swap out the buffer atomically
            with self._lock:
                if not self._buffer:
                    return
                rows_to_flush = self._buffer.copy()
                # Don't clear yet - only clear after successful upload
            
            df = pd.DataFrame(rows_to_flush)
            
            if df.empty:
                with self._lock:
                    # Remove the rows we copied (they might have been added to by now)
                    self._buffer = self._buffer[len(rows_to_flush):]
                    self._last_flush = time.time()
                return

            # Ensure timestamp column is datetime
            if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Simple schema: infer from pandas
            table = pa.Table.from_pandas(df)

            # Create object key with time
            now = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            key = f"{self.prefix}orderbook_{now}.parquet"

            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            body = buf.getvalue()

            # Upload with retry logic
            last_error = None
            for attempt in range(self.max_upload_retries):
                try:
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
                    print(f"[S3] Uploaded {len(df)} rows to s3://{self.bucket}/{key}")
                    
                    # Only clear buffer after successful upload
                    with self._lock:
                        # Remove only the rows we successfully uploaded
                        self._buffer = self._buffer[len(rows_to_flush):]
                        self._last_flush = time.time()
                    return  # Success
                    
                except Exception as e:
                    last_error = e
                    wait_time = (2 ** attempt) + (time.time() % 1)  # Exponential backoff with jitter
                    print(f"[S3] Upload attempt {attempt + 1}/{self.max_upload_retries} failed: {e}")
                    if attempt < self.max_upload_retries - 1:
                        print(f"[S3] Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
            
            # All retries failed - keep data in buffer for next flush attempt
            print(f"[S3] ERROR: All {self.max_upload_retries} upload attempts failed. Data retained in buffer ({len(rows_to_flush)} rows). Last error: {last_error}")
            
        finally:
            self._flush_lock.release()
