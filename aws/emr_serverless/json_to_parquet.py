
import sys
import boto3
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("MTG-JSON-to-Parquet-Dual-Output") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.memory", "20g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "6g") \
        .config("spark.driver.cores", "1") \
        .getOrCreate()

    # Optional parameter for SNS topic (can be passed from Step Function)
    topic_arn = os.environ.get('TOPIC_ARN')
    
    # Get date info
    dates_dict = get_dates()
    
    # Define S3 paths
    bucket_name = 'mtgdump'
    json_key = f"mtg_temp_json/all_cards_{dates_dict['short_date']}.json"
    
    # Define output paths for both parquet files
    daily_parquet_key = f"mtg_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}"
    static_parquet_key = f"mtg_static_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}"
    
    input_path = f"s3://{bucket_name}/{json_key}"
    daily_output_path = f"s3://{bucket_name}/{daily_parquet_key}"
    static_output_path = f"s3://{bucket_name}/{static_parquet_key}"
    
    print(f"Processing JSON from {input_path}")
    print(f"Creating daily parquet: {daily_output_path}")
    print(f"Creating static parquet: {static_output_path}")
    
    try:
        # Read JSON from S3 once
        df_raw = spark.read.option("multiline", "true").json(input_path)
        
        # Cache the raw data since we'll use it twice
        df_raw.cache()
        
        # Process daily prices data
        print("Processing daily prices data...")
        df_daily = process_daily_prices(df_raw, dates_dict['formatted_date'])
        daily_count = df_daily.count()
        
        # Write daily prices parquet
        df_daily.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "none") \
            .parquet(daily_output_path)

        print(f"Successfully wrote {daily_count} rows to daily parquet")
        
        # Process static card data  
        print("Processing static card data...")
        df_static = process_static_fields(df_raw, dates_dict['formatted_date'])
        static_count = df_static.count()

        df_static.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(static_output_path)

        print(f"Successfully wrote {static_count} rows to static parquet")
        
        # Unpersist cached data
        df_raw.unpersist()
        
        # Send success notification
        send_sns_notification(daily_count, static_count, daily_parquet_key, static_parquet_key, bucket_name, topic_arn)
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        send_failure_notification(str(e), topic_arn)
        raise e
    finally:
        spark.stop()

def process_daily_prices(df_raw, pull_date):
    # Extract the fields we need
    df_filtered = df_raw.select(
        col("id"),
        col("prices.usd").alias("usd"),
        col("prices.usd_foil").alias("usd_foil")
    ).filter(
        # Only keep rows where at least one price exists
        col("usd").isNotNull() | col("usd_foil").isNotNull()
    ).withColumn("pull_date", lit(pull_date))
    
    # Convert to DOUBLE type to match Athena table (not DecimalType)
    df_final = df_filtered.withColumn("usd", col("usd").cast("double")) \
                         .withColumn("usd_foil", col("usd_foil").cast("double"))
    
    return df_final

def process_static_fields(df_raw, pull_date):
    df_filtered = df_raw.select(
        col("id"),
        col("oracle_id"),
        col("mtgo_id"), 
        col("mtgo_foil_id"),
        col("tcgplayer_id"),
        col("cardmarket_id"),
        col("name"),
        col("lang"),
        col("released_at"),
        col("set_name"),
        col("set"),
        col("set_type"),
        col("rarity")
    ).withColumn("pull_date", lit(pull_date))
    
    # Cast ID columns to double to match Athena schema
    df_final = df_filtered.withColumn("mtgo_id", col("mtgo_id").cast("double")) \
                         .withColumn("mtgo_foil_id", col("mtgo_foil_id").cast("double")) \
                         .withColumn("tcgplayer_id", col("tcgplayer_id").cast("double")) \
                         .withColumn("cardmarket_id", col("cardmarket_id").cast("double"))
    
    return df_final

def send_sns_notification(daily_count, static_count, daily_key, static_key, bucket_name, topic_arn):
    try:
        if topic_arn:
            sns_client = boto3.client('sns', region_name='us-west-2')
            message = {
                'default': 'This is the default message',
                'email': f"""Daily Parquet row count = {daily_count}
Daily Parquet file created at: s3://{bucket_name}/{daily_key}

Static Parquet row count = {static_count}
Static Parquet file created at: s3://{bucket_name}/{static_key}"""
            }
            
            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(message),
                MessageStructure='json'
            )
            print(f"SNS message sent. Response: {response}")
        else:
            print("No topic_arn provided, skipping SNS notification")
            
    except Exception as e:
        print(f"Error sending SNS notification: {str(e)}")
        # Don't fail the job for SNS errors

def send_failure_notification(error_message, topic_arn):
    """Send failure SNS notification"""
    try:
        if topic_arn:
            sns_client = boto3.client('sns', region_name='us-west-2')
            message = {
                'default': 'This is the default message',
                'email': f'Error occurred during processing: {error_message}'
            }
            
            sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(message),
                MessageStructure='json'
            )
            print("Failure SNS message sent")
            
    except Exception as e:
        print(f"Error sending failure SNS notification: {str(e)}")

def get_dates():
    current_date = datetime.now()
    
    # Uncomment to force a specific date for testing
    # temp_date = '2024-12-08'
    # current_date = datetime.strptime(temp_date, '%Y-%m-%d')
    
    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }

if __name__ == "__main__":
    main()