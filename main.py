
from helper.S3Helper import S3ParquetBuffer
from Exchange import ParadexBBO, HyperliquidL2, BinanceDepth10
import time


if __name__ == "__main__":
    BUCKET = "2092-2968-9871.13012225"
    AWS_REGION = "ap-southeast-1"  # change to actual region of your bucket

    # One S3 buffer per venue (separate prefixes)
    paradex_buffer = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="orderbooks/paradex",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    hyper_buffer = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="orderbooks/hyperliquid",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    binance_buffer = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="orderbooks/binance",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    paradex_ws = ParadexBBO(market="ETH-USD-PERP", s3_buffer=paradex_buffer)
    hyper_ws = HyperliquidL2(symbol="ETH", s3_buffer=hyper_buffer, snapshot_interval=0.1)
    binance_ws = BinanceDepth10(symbol="ETHUSDT", s3_buffer=binance_buffer)

    # Start all three feeds
    paradex_ws.start()
    hyper_ws.start()
    binance_ws.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        paradex_ws.stop()
        hyper_ws.stop()
        binance_ws.stop()
