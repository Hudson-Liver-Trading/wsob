import boto3
from io import BytesIO
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize S3 client
s3_client = boto3.client('s3')

def download_single_file(bucket, key):
    file_obj = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = BytesIO(file_obj['Body'].read())
    df = pl.read_parquet(buffer)
    return df
    
def load_and_del_obj(bucket, prefix):
    #download files using paginator
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            parquet_files.extend([obj['Key'] for obj in page['Contents']])

    dataframes = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(download_single_file, bucket, key): key 
            for key in parquet_files
        }
        
        for i, future in enumerate(as_completed(futures)):
            df = future.result()
            if df is not None:
                dataframes.append(df)
            
            # Progress indicator
            if (i + 1) % 50 == 0:
                print(f"  Downloaded {i + 1}/{len(parquet_files)}")
    
    # delete files after download
    delete_files(bucket, parquet_files)
    
    # concat df and return it
    combined_df = pl.concat(dataframes, how='vertical')
    combined_df = combined_df.sort(by='timestamp')
    return combined_df
    
def upload_df(df, bucket, output_key):
    try:
        # if there is an existing file, merge with it
        existing_obj = s3_client.get_object(Bucket=bucket, Key=output_key)
        existing_buffer = BytesIO(existing_obj['Body'].read())
        existing_df = pl.read_parquet(existing_buffer)
        
        df = pl.concat([existing_df, df], how='vertical')
        df = df.unique().sort(by='timestamp') 
        print(f"Merged with existing file. New shape: {df.shape}")
        
    except s3_client.exceptions.NoSuchKey:
        print(f"No existing file, creating new one")
    except Exception as e:
        print(f"No existing file or error: {e}")
    
    # if no existing file, create a new one
    buffer = BytesIO()
    df.write_parquet(
        buffer,
        compression='snappy',
        use_pyarrow=True
    )
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )

def delete_files(bucket, file_keys):
    for i in range(0, len(file_keys), 1000):
        batch = file_keys[i:i+1000]
        delete_objects = {'Objects': [{'Key': key} for key in batch]}
        
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete=delete_objects
        )
        
        deleted = response.get('Deleted', [])
        errors = response.get('Errors', [])
        
        print(f"  Deleted {len(deleted)} files")
        if errors:
            print(f"  Errors: {errors}")

def upload_and_delete_obj(bucket, prefix, output_key):
    df = load_and_del_obj(bucket, prefix)
    upload_df(df, bucket, output_key)

################################################################################################################################
bucket = '2092-2968-9871.13012225'
exchanges = 'binance paradex lighter hyperliquid'.split()
spots = "btc eth sol".split()
start_date = datetime(2025,12,6)

while start_date < datetime.now():
    for exchange in exchanges:
        for spot in spots:
            prefix = f'orderbooks/{exchange}/{spot}/orderbook_{start_date.strftime(format="%Y%m%d")}_'
            output_key = f'daily/{exchange}/{spot}/orderbook_{start_date.strftime(format="%Y%m%d")}.parquet'

            try:
                upload_and_delete_obj(bucket, prefix, output_key)
            except Exception as e:
                print(f"Error: {e}")
################################################################################################################################