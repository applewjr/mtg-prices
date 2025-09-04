import boto3
import json
import urllib3
import time
from datetime import datetime

s3 = boto3.client('s3')
ssm = boto3.client('ssm')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    dates_dict = get_dates()

    # Get configuration
    param_names = [
        '/mtg/s3/buckets/primary_bucket'
    ]
    params = get_multiple_parameters(param_names)
    primary_bucket = params['/mtg/s3/buckets/primary_bucket']

    # Scryfall API headers (required)
    headers = {
        'User-Agent': 'MTGPriceTracker/1.0 (jamesapplewhite.com/mtg)',
        'Accept': 'application/json'
    }

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
    s3.put_object(Bucket=primary_bucket, Key=f'mtg_temp_json/all_cards_{dates_dict['short_date']}.json', Body=response.data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data downloaded to S3 successfully')
    }

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

def get_dates():
    current_date = datetime.now()

    ### Temp force a specific date
    # temp_date = '2024-12-03'
    # current_date = datetime.strptime(temp_date, '%Y-%m-%d')

    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }

def get_multiple_parameters(parameter_names):
    try:
        response = ssm.get_parameters(
            Names=parameter_names,
            WithDecryption=True
        )

        # Check for missing parameters
        if response.get('InvalidParameters'):
            raise Exception(f"Missing parameters: {response['InvalidParameters']}")

        params = {}
        for param in response['Parameters']:
            params[param['Name']] = param['Value']
        
        return params
    except Exception as e:
        print(f"Error getting parameters: {e}")
        raise