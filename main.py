
from helper.S3Helper import S3ParquetBuffer
from Exchange import ParadexBBO, HyperliquidL2, BinanceDepth10, LighterOrderBook
import time


if __name__ == "__main__":
    BUCKET = "2092-2968-9871.13012225"
    AWS_REGION = "ap-southeast-1"  # change to actual region of your bucket

    # One S3 buffer per venue (separate prefixes)
    paradex_buffer = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/paradex/eth",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    # hyper_buffer = S3ParquetBuffer(
    #     bucket=BUCKET,
    #     prefix="orderbooks/hyperliquid/eth",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    binance_buffer = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/binance/eth",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )
    
    paradex_buffer_btc = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/paradex/btc",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    # hyper_buffer_btc = S3ParquetBuffer(
    #     bucket=BUCKET,
    #     prefix="orderbooks/hyperliquid/btc",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    binance_buffer_btc = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/binance/btc",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )
    paradex_buffer_sol = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/paradex/sol",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    # hyper_buffer_sol = S3ParquetBuffer(
    #     bucket=BUCKET,
    #     prefix="orderbooks/hyperliquid/sol",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    binance_buffer_sol = S3ParquetBuffer(
        bucket=BUCKET,
        prefix="ob/binance/sol",
        buffer_size=500,
        aws_region=AWS_REGION,
        flush_interval_sec=5.0,
    )

    # lighter_buffer_eth = S3ParquetBuffer(
    #     bucket=BUCKET,
    #     prefix="orderbooks/lighter/eth",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    # lighter_buffer_btc = S3ParquetBuffer(  
    #     bucket=BUCKET,
    #     prefix="orderbooks/lighter/btc",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    # lighter_buffer_sol = S3ParquetBuffer(
    #     bucket=BUCKET,
    #     prefix="orderbooks/lighter/sol",
    #     buffer_size=500,
    #     aws_region=AWS_REGION,
    #     flush_interval_sec=5.0,
    # )

    
    

    paradex_ws = ParadexBBO(market="ETH-USD-PERP", s3_buffer=paradex_buffer)
    # hyper_ws = HyperliquidL2(symbol="ETH", s3_buffer=hyper_buffer, snapshot_interval=0.1)
    binance_ws = BinanceDepth10(symbol="ETHUSDT", s3_buffer=binance_buffer)
    
    paradex_ws_btc = ParadexBBO(market="BTC-USD-PERP", s3_buffer=paradex_buffer_btc)
    # hyper_ws_btc = HyperliquidL2(symbol="BTC", s3_buffer=hyper_buffer_btc, snapshot_interval=0.1)
    binance_ws_btc = BinanceDepth10(symbol="BTCUSDT", s3_buffer=binance_buffer_btc)
    
    paradex_ws_sol = ParadexBBO(market="SOL-USD-PERP", s3_buffer=paradex_buffer_sol)
    # hyper_ws_sol = HyperliquidL2(symbol="SOL", s3_buffer=hyper_buffer_sol, snapshot_interval=0.1)
    binance_ws_sol = BinanceDepth10(symbol="SOLUSDT", s3_buffer=binance_buffer_sol)

    # lighter_ws_eth = LighterOrderBook(market_index=2048, s3_buffer=lighter_buffer_eth)
    # lighter_ws_btc = LighterOrderBook(market_index=1, s3_buffer=lighter_buffer_btc)
    # lighter_ws_sol = LighterOrderBook(market_index=2, s3_buffer=lighter_buffer_sol)
    
    paradex_ws.start()
    # hyper_ws.start()
    binance_ws.start()
    
    paradex_ws_btc.start()
    # hyper_ws_btc.start()
    binance_ws_btc.start()
    
    paradex_ws_sol.start()
    # hyper_ws_sol.start()
    binance_ws_sol.start()

    # lighter_ws_eth.start()
    # lighter_ws_btc.start()
    # lighter_ws_sol.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        paradex_ws.stop()
        # hyper_ws.stop()
        binance_ws.stop()
        
        paradex_ws_btc.stop()
        # hyper_ws_btc.stop()
        binance_ws_btc.stop()
        
        paradex_ws_sol.stop()
        # hyper_ws_sol.stop()
        binance_ws_sol.stop()

        # lighter_ws_eth.stop()
        # lighter_ws_btc.stop()
        # lighter_ws_sol.stop()
