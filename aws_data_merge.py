import boto3
from io import BytesIO
import polars as pl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import sys
import gc
import psutil
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def download_single_file(s3_client, bucket, key, max_retries=3):
    """Download a single parquet file from S3"""
    for attempt in range(max_retries):
        try:
            file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            buffer = BytesIO(file_obj['Body'].read())
            df = pl.read_parquet(buffer)
            buffer.close()
            del buffer, file_obj
            return df
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {key}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to download {key} after {max_retries} attempts")
                return None
            time.sleep(2 ** attempt)


def download_batch(s3_client, bucket, file_keys, max_workers=4):
    """Download a batch of files concurrently"""
    dataframes = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_single_file, s3_client, bucket, key): key
            for key in file_keys
        }
        
        for future in as_completed(futures):
            try:
                df = future.result()
                if df is not None:
                    dataframes.append(df)
            except Exception as e:
                logger.error(f"Error downloading file: {e}")
    
    if not dataframes:
        return None
    
    # Combine batch into single dataframe
    combined = pl.concat(dataframes, how='vertical')
    del dataframes
    gc.collect()
    
    return combined


def upload_dataframe(s3_client, df, bucket, key, max_retries=3):
    """Upload a dataframe to S3"""
    for attempt in range(max_retries):
        try:
            buffer = BytesIO()
            df.write_parquet(buffer, compression='snappy', use_pyarrow=True)
            buffer.seek(0)
            
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            buffer.close()
            del buffer
            return True
            
        except Exception as e:
            logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(2 ** attempt)


def download_dataframe(s3_client, bucket, key):
    """Download a dataframe from S3"""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(file_obj['Body'].read())
        df = pl.read_parquet(buffer)
        buffer.close()
        del buffer, file_obj
        return df
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"Error downloading {key}: {e}")
        return None


def delete_files(s3_client, bucket, file_keys):
    """Delete files from S3 in batches"""
    try:
        for i in range(0, len(file_keys), 1000):
            batch = file_keys[i:i + 1000]
            delete_objects = {'Objects': [{'Key': key} for key in batch]}
            
            response = s3_client.delete_objects(Bucket=bucket, Delete=delete_objects)
            
            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])
            
            logger.info(f"Deleted {len(deleted)} files")
            if errors:
                logger.error(f"Delete errors: {errors}")
                
    except Exception as e:
        logger.error(f"Error deleting files: {e}")


def list_files(s3_client, bucket, prefix):
    """List all files with given prefix"""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents']])
    
    return files


def process_in_batches(
    s3_client,
    bucket,
    prefix,
    output_key,
    batch_size=50,
    max_workers=4,
    memory_limit_mb=4000
):
    """
    Process files in batches with incremental merging.
    
    Strategy:
    1. Download batch of files
    2. Merge with accumulated result (stored in temp S3 key)
    3. Clear memory
    4. Repeat until all files processed
    5. Move temp to final output
    """
    temp_key = f"temp/{output_key}"
    
    try:
        # Get all source files
        source_files = list_files(s3_client, bucket, prefix)
        
        if not source_files:
            logger.warning(f"No files found for prefix: {prefix}")
            return False
        
        logger.info(f"Found {len(source_files)} files to process")
        logger.info(f"Processing in batches of {batch_size}")
        
        total_batches = (len(source_files) + batch_size - 1) // batch_size
        processed_files = []
        
        for batch_num, i in enumerate(range(0, len(source_files), batch_size), 1):
            batch_files = source_files[i:i + batch_size]
            
            logger.info(f"Batch {batch_num}/{total_batches}: Processing {len(batch_files)} files")
            logger.info(f"Memory before batch: {get_memory_usage():.1f} MB")
            
            # Download current batch
            batch_df = download_batch(s3_client, bucket, batch_files, max_workers)
            
            if batch_df is None:
                logger.warning(f"Batch {batch_num} returned no data")
                continue
            
            logger.info(f"Batch {batch_num} downloaded: {batch_df.shape}")
            
            # Load existing accumulated data (if any)
            accumulated_df = download_dataframe(s3_client, bucket, temp_key)
            
            # Merge batch with accumulated data
            if accumulated_df is not None:
                logger.info(f"Merging with accumulated data: {accumulated_df.shape}")
                merged_df = pl.concat([accumulated_df, batch_df], how='vertical')
                del accumulated_df, batch_df
                gc.collect()
            else:
                merged_df = batch_df
                del batch_df
                gc.collect()
            
            # Sort and deduplicate
            merged_df = merged_df.sort(by='timestamp').unique()
            logger.info(f"Merged result: {merged_df.shape}")
            
            # Upload merged result to temp location
            success = upload_dataframe(s3_client, merged_df, bucket, temp_key)
            
            if not success:
                logger.error(f"Failed to upload batch {batch_num}")
                return False
            
            # Track processed files for deletion
            processed_files.extend(batch_files)
            
            # Clear memory
            del merged_df
            gc.collect()
            
            logger.info(f"Memory after batch: {get_memory_usage():.1f} MB")
            
            # Memory safety check
            if get_memory_usage() > memory_limit_mb:
                logger.warning(f"High memory usage: {get_memory_usage():.1f} MB")
                gc.collect()
                time.sleep(1)
        
        # Move temp to final output (merge with existing if present)
        logger.info("Finalizing output...")
        
        temp_df = download_dataframe(s3_client, bucket, temp_key)
        if temp_df is None:
            logger.error("Failed to download temp file for finalization")
            return False
        
        # Check for existing output file
        existing_df = download_dataframe(s3_client, bucket, output_key)
        
        if existing_df is not None:
            logger.info(f"Merging with existing output: {existing_df.shape}")
            final_df = pl.concat([existing_df, temp_df], how='vertical')
            final_df = final_df.sort(by='timestamp').unique()
            del existing_df, temp_df
            gc.collect()
        else:
            final_df = temp_df
            del temp_df
            gc.collect()
        
        logger.info(f"Final dataframe: {final_df.shape}")
        
        # Upload final result
        success = upload_dataframe(s3_client, final_df, bucket, output_key)
        del final_df
        gc.collect()
        
        if not success:
            logger.error("Failed to upload final output")
            return False
        
        # Clean up: delete temp file
        try:
            s3_client.delete_object(Bucket=bucket, Key=temp_key)
            logger.info("Deleted temp file")
        except Exception as e:
            logger.warning(f"Failed to delete temp file: {e}")
        
        # Delete source files
        delete_files(s3_client, bucket, processed_files)
        
        logger.info(f"Successfully processed {len(processed_files)} files")
        return True
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        # Attempt cleanup
        try:
            s3_client.delete_object(Bucket=bucket, Key=temp_key)
        except:
            pass
        return False


def main():
    s3_client = boto3.client(
        's3',
        config=boto3.session.Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
    )
    
    bucket = '2092-2968-9871.13012225'
    exchanges = ['binance', 'paradex', 'lighter', 'hyperliquid']
    spots = ['btc', 'eth', 'sol']
    start_date = datetime(2025, 12, 8)
    
    # Configuration
    batch_size = 30  # Files per batch
    max_workers = 4  # Concurrent downloads
    memory_limit_mb = 4000  # Memory warning threshold
    
    logger.info("Starting batch processing job")
    logger.info(f"Batch size: {batch_size}, Workers: {max_workers}")
    logger.info(f"Initial memory: {get_memory_usage():.1f} MB")
    
    while start_date < datetime.now():
        date_str = start_date.strftime('%Y%m%d')
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing date: {start_date.strftime('%Y-%m-%d')}")
        logger.info(f"{'='*50}")
        
        for exchange in exchanges:
            for spot in spots:
                prefix = f'orderbooks/{exchange}/{spot}/orderbook_{date_str}_'
                output_key = f'daily/{exchange}/{spot}/orderbook_{date_str}.parquet'
                
                logger.info(f"\nProcessing: {exchange}/{spot}")
                
                try:
                    success = process_in_batches(
                        s3_client=s3_client,
                        bucket=bucket,
                        prefix=prefix,
                        output_key=output_key,
                        batch_size=batch_size,
                        max_workers=max_workers,
                        memory_limit_mb=memory_limit_mb
                    )
                    
                    if success:
                        logger.info(f"✓ Completed {exchange}/{spot}")
                    else:
                        logger.error(f"✗ Failed {exchange}/{spot}")
                        
                except Exception as e:
                    logger.error(f"✗ Error {exchange}/{spot}: {e}")
                
                gc.collect()
                time.sleep(1)
        
        start_date += timedelta(days=1)
        
        # Daily memory check
        current_memory = get_memory_usage()
        logger.info(f"\nDaily memory usage: {current_memory:.1f} MB")
        
        if current_memory > memory_limit_mb:
            logger.warning("High memory - forcing cleanup")
            gc.collect()
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
