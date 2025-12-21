import boto3
from io import BytesIO
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import sys
import gc
import psutil
import os

# Set up logging - Remove the problematic mkdir line
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Only log to console
    ]
)
logger = logging.getLogger(__name__)

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def download_single_file(s3_client, bucket, key, max_retries=3):
    for attempt in range(max_retries):
        try:
            file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            buffer = BytesIO(file_obj['Body'].read())
            df = pl.read_parquet(buffer)
            
            # Clear buffer immediately
            buffer.close()
            del buffer, file_obj
            
            return df
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {key}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to download {key} after {max_retries} attempts")
                return None
            time.sleep(2 ** attempt)

def load_and_del_obj_chunked(s3_client, bucket, prefix, chunk_size=50):
    """Process files in chunks to manage memory"""
    try:
        logger.info(f"Starting processing for prefix: {prefix}")
        
        # Get all files
        parquet_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                parquet_files.extend([obj['Key'] for obj in page['Contents']])
        
        logger.info(f"Found {len(parquet_files)} files to process")
        
        if not parquet_files:
            logger.warning(f"No files found for prefix: {prefix}")
            return None

        # Process in chunks to manage memory
        all_chunks = []
        
        for i in range(0, len(parquet_files), chunk_size):
            chunk_files = parquet_files[i:i + chunk_size]
            logger.info(f"Processing chunk {i//chunk_size + 1}, files {i+1}-{min(i+chunk_size, len(parquet_files))}")
            logger.info(f"Memory usage before chunk: {get_memory_usage():.1f} MB")
            
            # Process chunk
            chunk_dataframes = []
            successful_downloads = 0
            
            with ThreadPoolExecutor(max_workers=1) as executor:  # Reduced workers
                futures = {
                    executor.submit(download_single_file, s3_client, bucket, key): key 
                    for key in chunk_files
                }
                
                for future in as_completed(futures):
                    try:
                        df = future.result()
                        if df is not None:
                            chunk_dataframes.append(df)
                            successful_downloads += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing file: {e}")
            
            # Combine chunk and clear individual dataframes
            if chunk_dataframes:
                chunk_combined = pl.concat(chunk_dataframes, how='vertical')
                all_chunks.append(chunk_combined)
                
                # Clear chunk dataframes from memory
                del chunk_dataframes
                gc.collect()
                
                logger.info(f"Chunk processed. Memory usage: {get_memory_usage():.1f} MB")
            
            # Memory check
            if get_memory_usage() > 4000:  # 4GB limit
                logger.warning(f"High memory usage: {get_memory_usage():.1f} MB")
                gc.collect()
        
        # Final combination
        if not all_chunks:
            logger.error("No data was successfully processed")
            return None
        
        logger.info(f"Combining {len(all_chunks)} chunks...")
        combined_df = pl.concat(all_chunks, how='vertical')
        
        # Clear chunks from memory
        del all_chunks
        gc.collect()
        
        combined_df = combined_df.sort(by='timestamp')
        logger.info(f"Final dataframe shape: {combined_df.shape}")
        logger.info(f"Final memory usage: {get_memory_usage():.1f} MB")
        
        # Delete files after successful processing
        delete_files(s3_client, bucket, parquet_files)
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Error in load_and_del_obj_chunked: {e}")
        return None

def upload_df_streaming(s3_client, df, bucket, output_key, max_retries=3):
    """Upload with better memory management"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Memory before upload: {get_memory_usage():.1f} MB")
            
            # Check for existing file
            existing_df = None
            try:
                existing_obj = s3_client.get_object(Bucket=bucket, Key=output_key)
                existing_buffer = BytesIO(existing_obj['Body'].read())
                existing_df = pl.read_parquet(existing_buffer)
                existing_buffer.close()
                del existing_buffer, existing_obj
                
                logger.info(f"Found existing file with shape: {existing_df.shape}")
                
            except s3_client.exceptions.NoSuchKey:
                logger.info("No existing file, creating new one")
            except Exception as e:
                logger.info(f"No existing file or error: {e}")
            
            # Merge if needed
            if existing_df is not None:
                logger.info("Merging with existing data...")
                df = pl.concat([existing_df, df], how='vertical')
                del existing_df  # Free memory immediately
                gc.collect()
                
                df = df.unique().sort(by='timestamp')
                logger.info(f"Merged dataframe shape: {df.shape}")
            
            # Write to buffer
            buffer = BytesIO()
            df.write_parquet(
                buffer,
                compression='snappy',
                use_pyarrow=True
            )
            buffer.seek(0)
            
            # Upload
            s3_client.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            # Clean up
            buffer.close()
            del buffer
            gc.collect()
            
            logger.info(f"Successfully uploaded {output_key}")
            logger.info(f"Memory after upload: {get_memory_usage():.1f} MB")
            return True
            
        except Exception as e:
            logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to upload {output_key} after {max_retries} attempts")
                return False
            time.sleep(2 ** attempt)
            gc.collect()

def upload_and_delete_obj(s3_client, bucket, prefix, output_key):
    try:
        logger.info(f"Starting processing: {prefix}")
        initial_memory = get_memory_usage()
        
        df = load_and_del_obj_chunked(s3_client, bucket, prefix, chunk_size=30)  # Smaller chunks
        
        if df is not None:
            success = upload_df_streaming(s3_client, df, bucket, output_key)
            
            # Clean up
            del df
            gc.collect()
            
            final_memory = get_memory_usage()
            logger.info(f"Memory change: {initial_memory:.1f} -> {final_memory:.1f} MB")
            
            return success
        else:
            logger.warning(f"No data to upload for {output_key}")
            return False
            
    except Exception as e:
        logger.error(f"Error in upload_and_delete_obj: {e}")
        gc.collect()  # Clean up on error
        return False

def delete_files(s3_client, bucket, file_keys):
    try:
        for i in range(0, len(file_keys), 1000):
            batch = file_keys[i:i+1000]
            delete_objects = {'Objects': [{'Key': key} for key in batch]}
            
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete=delete_objects
            )
            
            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])
            
            logger.info(f"Deleted {len(deleted)} files")
            if errors:
                logger.error(f"Delete errors: {errors}")
                
    except Exception as e:
        logger.error(f"Error deleting files: {e}")

def main():
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        config=boto3.session.Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
    )
    
    bucket = '2092-2968-9871.13012225'
    exchanges = 'binance paradex lighter hyperliquid'.split()
    spots = "btc eth sol".split()
    start_date = datetime(2025, 12, 8)
    
    logger.info("Starting data processing job")
    logger.info(f"Initial memory usage: {get_memory_usage():.1f} MB")
    
    while start_date < datetime.now():
        logger.info(f"Processing date: {start_date.strftime('%Y-%m-%d')}")
        
        for exchange in exchanges:
            for spot in spots:
                prefix = f'orderbooks/{exchange}/{spot}/orderbook_{start_date.strftime(format="%Y%m%d")}_'
                output_key = f'daily/{exchange}/{spot}/orderbook_{start_date.strftime(format="%Y%m%d")}.parquet'
                
                logger.info(f"Processing {exchange}/{spot}")
                
                try:
                    success = upload_and_delete_obj(s3_client, bucket, prefix, output_key)
                    if success:
                        logger.info(f"✓ Completed {exchange}/{spot}")
                    else:
                        logger.error(f"✗ Failed {exchange}/{spot}")
                        
                except Exception as e:
                    logger.error(f"✗ Error processing {exchange}/{spot}: {e}")
                
                # Force garbage collection between tasks
                gc.collect()
                time.sleep(2)  # Give system time to clean up
        
        start_date += timedelta(days=1)
        
        # Memory check after each day
        current_memory = get_memory_usage()
        logger.info(f"Memory usage after day: {current_memory:.1f} MB")
        
        if current_memory > 1000:  # 5GB warning
            logger.warning("High memory usage detected, forcing cleanup")
            gc.collect()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
