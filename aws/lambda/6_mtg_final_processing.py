import json
import boto3
import pandas as pd
from io import StringIO
import math
from datetime import datetime
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the current dates
    dates = get_dates()
    
    # Define S3 bucket and file paths
    input_bucket = 'mtgdump'
    output_bucket = 'mtgserve'
    input_key = f"mtg_temp_daily/{dates['short_date']}_daily_out_raw.csv"
    output_key = os.environ.get('OUTPUT_S3_KEY')
    
    # Read the input CSV from S3
    try:
        csv_file = s3.get_object(Bucket=input_bucket, Key=input_key)
        csv_content = csv_file['Body'].read().decode('utf-8')
        
        # Create a file-like object from the CSV content
        csv_file_like = StringIO(csv_content)

        # Process the CSV data using the new transformation logic
        processed_data = process_csv(csv_file_like)

        # Convert processed DataFrame to CSV
        output_csv = processed_data.to_csv(index=False)

        # Upload the processed CSV back to S3
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=output_csv)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed CSV uploaded to {output_key}")
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

def get_dates():
    current_date = datetime.now()

    ## Temp force a specific date -- this forced date doesn't seem to work on this lambda
    # temp_date = '2024-12-08'
    # current_date = datetime.strptime(temp_date, '%Y-%m-%d')

    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }

def process_csv(file_path):
    # Ingest the CSV into a DataFrame
    df = pd.read_csv(file_path)

    # Convert pull_date and released_at to datetime
    df['pull_date'] = pd.to_datetime(df['pull_date'])
    df['released_at'] = pd.to_datetime(df['released_at'])

    # Get today's date
    today = pd.to_datetime('today').normalize() 
        # - pd.Timedelta(days=1)

    # Define the dates for price comparisons: 1 week ago, 2 weeks ago, and 4 weeks ago
    one_week_ago = today - pd.DateOffset(weeks=1)
    two_weeks_ago = today - pd.DateOffset(weeks=2)
    four_weeks_ago = today - pd.DateOffset(weeks=4)

    # Initialize a list to store the processed results
    results = []

    # Group by 'id' and process each group
    grouped = df.groupby('id')

    for id_, group in grouped:
        row = {
            'id': id_,
            'tcgplayer_id': f'https://www.tcgplayer.com/product/{int(float(group["tcgplayer_id"].iloc[0]))}' 
                            if not math.isnan(group['tcgplayer_id'].iloc[0]) else None,
            'name': group['name'].iloc[0],
            'set_name': group['set_name'].iloc[0],
            'set_type': group['set_type'].iloc[0],
            'released_at': group['released_at'].iloc[0].date()
        }

        # Get the prices on today, one week ago, two weeks ago, and four weeks ago
        today_price_data = group.loc[group['pull_date'] == today, ['usd', 'pull_date']].values
        one_week_ago_price = group.loc[group['pull_date'] == one_week_ago, 'usd'].values
        two_weeks_ago_price = group.loc[group['pull_date'] == two_weeks_ago, 'usd'].values
        four_weeks_ago_price = group.loc[group['pull_date'] == four_weeks_ago, 'usd'].values

        # Assign today's price and handle missing values
        if len(today_price_data) > 0:
            row['today_price'] = today_price_data[0][0]
            row['today_price_date'] = today_price_data[0][1].date()
        else:
            row['today_price'] = None
            row['today_price_date'] = None

        row['1wk_ago_price'] = one_week_ago_price[0] if len(one_week_ago_price) > 0 else None
        row['2wk_ago_price'] = two_weeks_ago_price[0] if len(two_weeks_ago_price) > 0 else None
        row['4wk_ago_price'] = four_weeks_ago_price[0] if len(four_weeks_ago_price) > 0 else None

        # Calculate price differences (today's price / price from previous periods)
        if row['1wk_ago_price'] and row['today_price']:
            row['1wk_diff'] = round(row['today_price'] / row['1wk_ago_price'], 4)
        else:
            row['1wk_diff'] = None

        if row['2wk_ago_price'] and row['today_price']:
            row['2wk_diff'] = round(row['today_price'] / row['2wk_ago_price'], 4)
        else:
            row['2wk_diff'] = None

        if row['4wk_ago_price'] and row['today_price']:
            row['4wk_diff'] = round(row['today_price'] / row['4wk_ago_price'], 4)
        else:
            row['4wk_diff'] = None

        # Only append rows where today's price is at least 1
        if row['today_price'] is not None and row['today_price'] >= 1:
            results.append(row)

    # Convert results to a DataFrame
    result_df = pd.DataFrame(results)

    # Sort by the highest 4-week difference, descending
    result_df = result_df.sort_values(by='4wk_diff', ascending=False).reset_index(drop=True)

    return result_df
