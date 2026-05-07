import boto3
import json
import urllib3
import time
from datetime import datetime

from utils import get_dates, get_multiple_parameters

s3 = boto3.client('s3')
sns_client = boto3.client('sns')
ssm = boto3.client('ssm')
http = urllib3.PoolManager()


def lambda_handler(event, context):
    dates_dict = get_dates()

    # Get configuration
    param_names = [
        '/mtg/s3/buckets/primary_bucket',
        '/mtg/sns/status_topic_arn'
    ]
    params = get_multiple_parameters(param_names, ssm)
    primary_bucket = params['/mtg/s3/buckets/primary_bucket']
    status_topic_arn = params['/mtg/sns/status_topic_arn']

    # Scryfall API headers (required)
    headers = {
        'User-Agent': 'MTGPriceTracker/1.0 (jamesapplewhite.com/mtg)',
        'Accept': 'application/json'
    }

    try:
        # Fetch bulk data metadata
        url = 'https://api.scryfall.com/bulk-data'
        response = make_scryfall_request(url, headers)

        bulk_data = json.loads(response.data.decode('utf-8'))

        # Find the bulk data file for all cards
        all_cards_data = next(item for item in bulk_data['data'] if item['type'] == 'default_cards')
        all_cards_url = all_cards_data['download_uri']

        # Rate limiting delay (50-100ms between requests as recommended)
        time.sleep(0.1)  # 100ms delay

        # Download the bulk data file
        response = make_scryfall_request(all_cards_url, headers)

        # Upload data to S3
        s3_key = f'mtg_temp_json/all_cards_{dates_dict["short_date"]}.json'
        s3.put_object(Bucket=primary_bucket, Key=s3_key, Body=response.data)

        # Send success notification
        send_notification(
            sns_client,
            status_topic_arn,
            success=True,
            dates_dict=dates_dict,
            primary_bucket=primary_bucket,
            s3_key=s3_key,
            size_mb=round(len(response.data) / (1024 * 1024), 2)
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Data downloaded to S3 successfully')
        }

    except Exception as e:
        error_message = str(e)
        print(f"Error: {error_message}")

        # Send failure notification
        send_notification(
            sns_client,
            status_topic_arn,
            success=False,
            dates_dict=dates_dict,
            primary_bucket=primary_bucket,
            error=error_message
        )

        raise

def make_scryfall_request(url, headers, max_retries=3, backoff_factor=2):
    """
    Make HTTP request to Scryfall with retry logic for 429 responses
    """
    for attempt in range(max_retries):
        try:
            response = http.request('GET', url, headers=headers)
            
            if response.status == 200:
                return response
            elif response.status == 429:
                # Rate limited - exponential backoff
                wait_time = backoff_factor ** attempt
                print(f"Rate limited (429). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
                continue
            else:
                # Other HTTP errors
                raise Exception(f"HTTP {response.status} error fetching {url}")
                
        except Exception as e:
            if attempt == max_retries - 1:
                # Last attempt failed
                raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")
            else:
                # Wait before retry
                wait_time = backoff_factor ** attempt
                print(f"Request failed. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
    
    # Should not reach here, but just in case
    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

def send_notification(sns_client, status_topic_arn, success, dates_dict, primary_bucket, s3_key=None, size_mb=None, error=None):
    """Send SNS notification for success or failure"""
    if success:
        subject = f"MTG Data Pull SUCCESS - {dates_dict['formatted_date']}"
        email_message = f"""MTG Scryfall Bulk Data Pull - SUCCESS

Date: {dates_dict['formatted_date']}
Bucket: {primary_bucket}
Key: {s3_key}
Size: {size_mb} MB
"""
    else:
        subject = f"MTG Data Pull FAILED - {dates_dict['formatted_date']}"
        email_message = f"""MTG Scryfall Bulk Data Pull - FAILED

Date: {dates_dict['formatted_date']}
Bucket: {primary_bucket}

Error:
{error}
"""

    message = {
        'default': 'This is the default message',
        'email': email_message
    }

    try:
        response = sns_client.publish(
            TopicArn=status_topic_arn,
            Subject=subject,
            Message=json.dumps(message),
            MessageStructure='json'
        )
        print(f"SNS message sent. Response: {response}")
    except Exception as e:
        print(f"Error sending SNS: {str(e)}")