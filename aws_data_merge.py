import boto3
import pandas as pd
from collections import defaultdict
from io import BytesIO
import re

# Configuration
BUCKET_NAME = "2092-2968-9871.13012225"
EXCHANGES = ["binance", "lighter", "paradex"]
COINS = ["btc", "eth", "sol"]

s3 = boto3.client("s3")


def list_objects(prefix: str) -> list[str]:
    """List all objects under a prefix"""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def object_exists(key: str) -> bool:
    """Check if an object exists in S3"""
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def parse_filename(key: str) -> tuple[str, str] | None:
    """Extract date and hour from filename"""
    filename = key.split("/")[-1]
    match = re.match(r"orderbook_(\d{8})_(\d{2})\d{4}_\d+\.parquet", filename)
    if match:
        return match.group(1), match.group(2)
    return None


def download_parquet(key: str) -> pd.DataFrame:
    """Download a parquet file from S3"""
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))


def upload_parquet(df: pd.DataFrame, key: str):
    """Upload a DataFrame as parquet to S3"""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())


def delete_objects(keys: list[str]):
    """Delete multiple objects from S3"""
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        s3.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )


def process_exchange_coin(exchange: str, coin: str):
    """Process all files for a given exchange and coin"""
    prefix = f"orderbooks/{exchange}/{coin}/"
    keys = list_objects(prefix)

    if not keys:
        print(f"No files found for {exchange}/{coin}")
        return

    # Group files by (date, hour)
    hourly_groups: dict[tuple[str, str], list[str]] = defaultdict(list)

    for key in keys:
        parsed = parse_filename(key)
        if parsed:
            date, hour = parsed
            hourly_groups[(date, hour)].append(key)

    # Process each hourly group
    for (date, hour), group_keys in sorted(hourly_groups.items()):
        print(f"Processing {exchange}/{coin} - {date} {hour}:00 ({len(group_keys)} files)")

        # Download new files
        dfs = []
        for key in group_keys:
            try:
                df = download_parquet(key)
                dfs.append(df)
            except Exception as e:
                print(f"  Error downloading {key}: {e}")
                continue

        if not dfs:
            continue

        # Check if hourly file already exists (for appending)
        output_key = f"hourly_orderbooks/{exchange}/{coin}/orderbook_{date}_{hour}.parquet"

        if object_exists(output_key):
            print(f"  Existing hourly file found, appending...")
            try:
                existing_df = download_parquet(output_key)
                dfs.insert(0, existing_df)
            except Exception as e:
                print(f"  Error downloading existing file: {e}")

        # Merge all DataFrames
        merged_df = pd.concat(dfs, ignore_index=True)

        # Remove duplicates and sort by timestamp
        if "timestamp" in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=["timestamp"])
            merged_df = merged_df.sort_values("timestamp").reset_index(drop=True)

        # Upload merged file
        try:
            upload_parquet(merged_df, output_key)
            print(f"  Uploaded: {output_key} ({len(merged_df)} rows)")

            # Delete original files only after successful upload
            delete_objects(group_keys)
            print(f"  Deleted {len(group_keys)} original files")

        except Exception as e:
            print(f"  Error uploading {output_key}: {e}")


def main():
    for exchange in EXCHANGES:
        for coin in COINS:
            print(f"\n{'='*50}")
            print(f"Processing {exchange}/{coin}")
            print("=" * 50)
            process_exchange_coin(exchange, coin)


if __name__ == "__main__":
    main()
