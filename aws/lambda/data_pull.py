import boto3
import json
import urllib3
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

    # Fetch bulk data metadata
    url = 'https://api.scryfall.com/bulk-data'
    response = http.request('GET', url)

    if response.status != 200:
        raise Exception(f"Error fetching bulk data: {response.status}")

    bulk_data = json.loads(response.data.decode('utf-8'))

    # Find the bulk data file for all cards
    all_cards_data = next(item for item in bulk_data['data'] if item['type'] == 'default_cards')
    all_cards_url = all_cards_data['download_uri']

    # Download the bulk data file
    response = http.request('GET', all_cards_url)
    
    if response.status != 200:
        raise Exception(f"Error downloading all cards data: {response.status}")

    # Upload data to S3
    s3.put_object(Bucket=primary_bucket, Key=f'mtg_temp_json/all_cards_{dates_dict['short_date']}.json', Body=response.data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data downloaded to S3 successfully')
    }

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