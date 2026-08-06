"""Merge raw 5-second ob2 orderbook parquet into hourly files.

Consolidates s3://<bucket>/ob2/{exchange}/{coin}/orderbook_YYYYMMDD_HHMMSS_us.parquet
into hourly s3://<bucket>/hourly_orderbooks2/{exchange}/{coin}/orderbook_YYYYMMDD_HH.parquet,
then deletes the merged originals.

Hardened 2026-08-07 (previously a manual script, last run by hand 2026-02-27):
  * NEVER touches the current (in-progress) hour -- the old version could merge
    and DELETE files for the hour the scraper was still writing, losing rows
    written between the listing and the delete. This was a live data-loss race.
  * Deletes originals only after re-reading the uploaded hourly object and
    confirming it contains at least as many rows as were merged.
  * --since / --until bound the work so a scheduled run is cheap and a backlog
    catch-up can be chunked.
  * --no-delete for a dry-ish run that consolidates without removing sources.
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import re
import sys
from collections import defaultdict
from io import BytesIO

import boto3
import pandas as pd

BUCKET_NAME = "2092-2968-9871.13012225"
# Sequential GETs measured 3.5 files/s -> the 160-day backlog would take
# >1 month of runtime. Parallel downloads are what make it tractable.
DOWNLOAD_WORKERS = 32
EXCHANGES = ["binance", "paradex"]
COINS = ["btc", "eth", "sol"]
FNAME_RE = re.compile(r"orderbook_(\d{8})_(\d{2})\d{4}_\d+\.parquet")

s3 = boto3.client("s3")


def list_objects(prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def download_parquet(key: str) -> pd.DataFrame:
    r = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_parquet(BytesIO(r["Body"].read()))


def _safe_download(key: str):
    try:
        return download_parquet(key)
    except Exception:                                   # noqa: BLE001
        return None


def upload_parquet(df: pd.DataFrame, key: str) -> None:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf.getvalue())


def delete_objects(keys: list[str]) -> None:
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        s3.delete_objects(Bucket=BUCKET_NAME,
                          Delete={"Objects": [{"Key": k} for k in batch]})


def process(exchange: str, coin: str, since: str, until: str,
            current_hour: tuple[str, str], do_delete: bool) -> dict:
    # List by NARROW per-day prefix. Listing the whole ob2/{ex}/{coin}/ prefix
    # takes >15 min once a backlog accumulates (~500k tiny objects) and every
    # run paid that cost before filtering by date -- measured 2026-08-07.
    keys: list[str] = []
    d0 = dt.datetime.strptime(since, "%Y%m%d").date()
    d1 = dt.datetime.strptime(until, "%Y%m%d").date()
    day = d0
    while day <= d1:
        keys.extend(list_objects(
            f"ob2/{exchange}/{coin}/orderbook_{day:%Y%m%d}_"))
        day += dt.timedelta(days=1)
    if not keys:
        return {"groups": 0, "rows": 0, "deleted": 0}

    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for k in keys:
        m = FNAME_RE.match(k.split("/")[-1])
        if not m:
            continue
        date, hour = m.group(1), m.group(2)
        if not (since <= date <= until):
            continue
        if (date, hour) >= current_hour:      # never touch the in-progress hour
            continue
        groups[(date, hour)].append(k)

    stats = {"groups": 0, "rows": 0, "deleted": 0}
    for (date, hour), gkeys in sorted(groups.items()):
        out_key = f"hourly_orderbooks2/{exchange}/{coin}/orderbook_{date}_{hour}.parquet"
        dfs = []
        with cf.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as ex:
            for k, res in zip(gkeys, ex.map(_safe_download, gkeys)):
                if res is None:
                    print(f"  ! download {k}", flush=True)
                else:
                    dfs.append(res)
        if not dfs:
            continue
        if object_exists(out_key):
            try:
                dfs.insert(0, download_parquet(out_key))
            except Exception as e:                      # noqa: BLE001
                print(f"  ! existing {out_key}: {e}", flush=True)

        merged = pd.concat(dfs, ignore_index=True)
        if "timestamp" in merged.columns:
            merged = (merged.drop_duplicates(subset=["timestamp"])
                            .sort_values("timestamp").reset_index(drop=True))
        upload_parquet(merged, out_key)

        # verify before destroying sources
        ok = False
        try:
            ok = len(download_parquet(out_key)) >= len(merged)
        except Exception as e:                          # noqa: BLE001
            print(f"  ! verify {out_key}: {e}", flush=True)
        if ok and do_delete:
            delete_objects(gkeys)
            stats["deleted"] += len(gkeys)
        elif not ok:
            print(f"  ! VERIFY FAILED, keeping sources for {out_key}", flush=True)

        stats["groups"] += 1
        stats["rows"] += len(merged)
        print(f"  {exchange}/{coin} {date} {hour}:00 <- {len(gkeys)} files "
              f"= {len(merged)} rows{' (kept)' if not do_delete else ''}", flush=True)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=(dt.datetime.now(dt.timezone.utc).date()
                                    - dt.timedelta(days=2)).strftime("%Y%m%d"),
                    help="YYYYMMDD inclusive (default: 2 days ago)")
    ap.add_argument("--until", default=dt.datetime.now(dt.timezone.utc)
                    .strftime("%Y%m%d"), help="YYYYMMDD inclusive (default: today)")
    ap.add_argument("--no-delete", action="store_true")
    a = ap.parse_args()

    now = dt.datetime.now(dt.timezone.utc)
    current_hour = (now.strftime("%Y%m%d"), now.strftime("%H"))
    total = {"groups": 0, "rows": 0, "deleted": 0}
    for ex in EXCHANGES:
        for coin in COINS:
            s = process(ex, coin, a.since, a.until, current_hour, not a.no_delete)
            for k in total:
                total[k] += s[k]
    print(f"DONE groups={total['groups']} rows={total['rows']} "
          f"deleted_files={total['deleted']} (skipped in-progress hour "
          f"{current_hour[0]} {current_hour[1]}:00 UTC)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
