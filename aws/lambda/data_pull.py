import boto3
import json
import urllib3
from datetime import datetime

def lambda_handler(event, context):
    dates_dict = get_dates()

    http = urllib3.PoolManager()
    s3 = boto3.client('s3')

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
    s3.put_object(Bucket='mtgdump', Key=f'mtg_temp_json/all_cards_{dates_dict['short_date']}.json', Body=response.data)
    
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