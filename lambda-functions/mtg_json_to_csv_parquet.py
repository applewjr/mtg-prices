import boto3
import json
import csv
from datetime import datetime
from decimal import Decimal
from io import StringIO
import ijson
import os

def lambda_handler(event, context):
    dates_dict = get_dates()

    s3 = boto3.client('s3')
    sns_client = boto3.client('sns')

    # S3 bucket and object details
    bucket_name = 'mtgdump'
    object_key = f"mtg_temp_json/all_cards_{dates_dict['short_date']}.json"
    topic_arn = os.environ.get('TOPIC_ARN')

    print("Starting to process JSON with ijson for memory efficiency")

    try:
        # Get JSON data from S3 and stream it with ijson
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        json_stream = ijson.items(response['Body'], "item")  # Stream JSON objects under the top-level array
    except Exception as e:
        error_message = f"Error reading data from S3: {str(e)}"
        print(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

    filtered_data = []
    csv_content = None

    try:
        if event['run_type'] == 'daily_prices':
            filtered_data = process_daily_prices_stream(json_stream, dates_dict['formatted_date'])
            csv_content = create_csv(filtered_data, ["id", "usd", "usd_foil", "pull_date"])
        elif event['run_type'] == 'static_prices':
            filtered_data = process_static_prices_stream(json_stream, dates_dict['formatted_date'])
            csv_content = create_csv(filtered_data, ["id", "oracle_id", "mtgo_id", "mtgo_foil_id", "tcgplayer_id",
                                                     "cardmarket_id", "name", "lang", "released_at", "set_name", "set",
                                                     "set_type", "rarity", "pull_date"])
        else:
            error_message = f"Unexpected run_type: {event['run_type']}"
            print(error_message)
            return {'statusCode': 400, 'body': json.dumps(error_message)}

    except Exception as e:
        error_message = f"Error processing JSON stream: {str(e)}"
        print(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

    # Upload CSV to S3
    try:
        csv_object_key = f"{event['data_folder']['csv']}/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/{event['data_file']['csv']}_{dates_dict['short_date']}.csv"
        s3.put_object(Bucket=bucket_name, Key=csv_object_key, Body=csv_content)
        print(f"Successfully uploaded CSV to S3: {csv_object_key}")
    except Exception as e:
        error_message = f"Error uploading CSV to S3: {str(e)}"
        print(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

    # Send SNS email notification
    try:
        row_count = len(filtered_data)
        message = {
            'default': 'This is the default message',
            'email': f'CSV row count = {row_count}\nCSV file created at: s3://{bucket_name}/{csv_object_key}'
        }

        sns_response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageStructure='json'
        )
        print(f"SNS message sent. Response: {sns_response}")
    except Exception as e:
        error_message = f"Error sending SNS notification: {str(e)}"
        print(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

    return {
        'statusCode': 200,
        'run_type': event['run_type'],
        'data_folder_csv': event['data_folder']['csv'],
        'data_file_csv': event['data_file']['csv'],
        'data_folder_parquet': event['data_folder']['parquet'],
        'data_file_parquet': event['data_file']['parquet'],
        'body': json.dumps('Data processed, CSV uploaded to S3, and notification sent successfully')
    }


def process_daily_prices_stream(json_stream, pull_date):
    filtered_keys = {"id"}
    keys_to_keep_from_prices = {"usd", "usd_foil"}

    filtered_data = []
    for card in json_stream:
        filtered_card = {key: card[key] for key in filtered_keys if key in card}
        if "prices" in card:
            prices = {k: Decimal(str(v)) for k, v in card["prices"].items() if k in keys_to_keep_from_prices and v is not None}
            if prices:
                filtered_card.update(prices)
                filtered_card["pull_date"] = pull_date
                filtered_data.append(filtered_card)

    return filtered_data


def process_static_prices_stream(json_stream, pull_date):
    static_keys = [
        "id", "oracle_id", "mtgo_id", "mtgo_foil_id", "tcgplayer_id", "cardmarket_id",
        "name", "lang", "released_at", "set_name", "set", "set_type", "rarity"
    ]

    filtered_data = []
    for card in json_stream:
        filtered_card = {key: card[key] for key in static_keys if key in card}
        filtered_card["pull_date"] = pull_date
        filtered_data.append(filtered_card)

    return filtered_data


def create_csv(data, header):
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=header)
    csv_writer.writeheader()
    csv_writer.writerows(data)
    return csv_buffer.getvalue()


def get_dates():
    current_date = datetime.now()

    # Temp force a specific date
    # temp_date = '2024-12-08'
    # current_date = datetime.strptime(temp_date, '%Y-%m-%d')

    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }