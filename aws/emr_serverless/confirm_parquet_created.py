import boto3
from datetime import datetime
import json
import os

def lambda_handler(event, context):
    
    # Initialize clients
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    
    # Configuration
    bucket_name = 'mtgdump'
    topic_arn = os.environ.get('TOPIC_ARN')
    
    dates_dict = get_dates()
    
    # Define folder paths
    daily_parquet_key = f"mtg_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}"
    static_parquet_key = f"mtg_static_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}"
    
    try:
        # Check daily folder
        daily_files = list_files_in_folder(s3_client, bucket_name, daily_parquet_key)
        
        # Check static folder  
        static_files = list_files_in_folder(s3_client, bucket_name, static_parquet_key)
        
        # Send notification
        send_notification(sns_client, topic_arn, daily_files, static_files, dates_dict, daily_parquet_key, static_parquet_key)
        
        return {
            'statusCode': 200,
            'message': f'Folder check complete for {dates_dict["formatted_date"]}',
            'daily_files': len(daily_files),
            'static_files': len(static_files)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'error': str(e)}

def list_files_in_folder(s3_client, bucket_name, folder_key):
    """List files in S3 folder"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_key + '/'
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'name': obj['Key'].split('/')[-1],
                    'size_mb': round(obj['Size'] / (1024 * 1024), 2)
                })
        
        return files
        
    except Exception as e:
        print(f"Error checking folder {folder_key}: {str(e)}")
        return []

def send_notification(sns_client, topic_arn, daily_files, static_files, dates_dict, daily_key, static_key):
    """Send simple SNS notification"""
    
    email_message = f"""MTG S3 Folder Check - {dates_dict['formatted_date']}

Daily Parquet Folder (s3://mtgdump/{daily_key}/):
  Files found: {len(daily_files)}"""

    for file in daily_files:
        email_message += f"\n  - {file['name']} ({file['size_mb']} MB)"

    email_message += f"""

Static Parquet Folder (s3://mtgdump/{static_key}/):
  Files found: {len(static_files)}"""

    for file in static_files:
        email_message += f"\n  - {file['name']} ({file['size_mb']} MB)"

    message = {
        'default': 'This is the default message',
        'email': email_message
    }

    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageStructure='json'
        )
        print(f"SNS message sent. Response: {response}")
    except Exception as e:
        print(f"Error sending SNS: {str(e)}")

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