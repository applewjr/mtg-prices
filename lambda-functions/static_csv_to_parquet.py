import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os

s3 = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    dates_dict = get_dates()
    
    # Define S3 paths
    bucket_name = 'mtgdump'
    csv_key = f"{event['data_folder']['csv']}/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/{event['data_file']['csv']}_{dates_dict['short_date']}.csv"
    parquet_key = f"{event['data_folder']['parquet']}/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/{event['data_file']['parquet']}_{dates_dict['short_date']}.parquet"
    
    # Define local file paths in /tmp
    csv_file_path = f"/tmp/{event['data_file']['csv']}_{dates_dict['short_date']}.csv"
    parquet_file_path = f"/tmp/{event['data_file']['parquet']}_{dates_dict['short_date']}.parquet"
    
    try:
        # Download CSV from S3 to /tmp
        s3.download_file(bucket_name, csv_key, csv_file_path)
        print(f"Successfully downloaded CSV from S3: {csv_key}")
    except Exception as e:
        print(f"Error downloading CSV from S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error downloading CSV from S3: {str(e)}")
        }
    
    try:
        # Convert CSV to Parquet
        df = pd.read_csv(csv_file_path)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file_path, row_group_size=2000)
        print(f"Successfully converted CSV to Parquet: {parquet_file_path}")
    except Exception as e:
        print(f"Error converting CSV to Parquet: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error converting CSV to Parquet: {str(e)}")
        }
    
    try:
        # Upload Parquet to S3
        s3.upload_file(parquet_file_path, bucket_name, parquet_key)
        print(f"Successfully uploaded Parquet to S3: {parquet_key}")
    except Exception as e:
        print(f"Error uploading Parquet to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error uploading Parquet to S3: {str(e)}")
        }
    
    # Send SNS notification
    try:
        topic_arn = os.environ.get('TOPIC_ARN')
        message = {
            'default': 'This is the default message',
            'email': f'Parquet file created at: s3://{bucket_name}/{parquet_key}'
        }

        sns_response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageStructure='json'
        )
        print(f"SNS message sent. Response: {sns_response}")
    except Exception as e:
        print(f"Error sending SNS notification: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error sending SNS notification: {str(e)}")
        }

    return {
        'statusCode': 200,
        'run_type': event['run_type'],
        'data_folder_csv': event['data_folder']['csv'],
        'data_file_csv': event['data_file']['csv'],
        'data_folder_parquet': event['data_folder']['parquet'],
        'data_file_parquet': event['data_file']['parquet'],
        'body': json.dumps(f"Successfully processed and uploaded Parquet file to S3: {parquet_key}")
    }

def get_dates():
    current_date = datetime.now()

    ### Temp force a specific date
    # temp_date = '2024-12-08'
    # current_date = datetime.strptime(temp_date, '%Y-%m-%d')

    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }