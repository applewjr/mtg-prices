import boto3
import json
import time
from datetime import datetime
from urllib.parse import urlparse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the current date in YYYYMMDD format
    date_info = get_dates()
    short_date = date_info['short_date']

    # Define the output S3 path
    s3_bucket = 'mtgdump'
    s3_key = f"mtg_temp_daily/{short_date}_daily_out_raw.csv"
    output_location = f"s3://{s3_bucket}/athena_output/"

    # SQL query to execute
    query = """
    SELECT
      price.id,
      CAST(static.tcgplayer_id AS INT) AS tcgplayer_id,
      static.name,
      static.set_name,
      static.set_type,
      static.released_at,
      price.usd,
      price.pull_date
    FROM mtg_prices_iceberg AS price
    INNER JOIN mtg_static_parquet AS static ON price.id = static.id
    WHERE price.usd IS NOT NULL
      AND price.usd <> 0
      AND CAST(static.released_at AS DATE) >= DATE_ADD('year', -10, current_date)
      AND CAST(price.pull_date AS DATE) IN (
        current_date,                     -- Today
        DATE_ADD('day', -7, current_date), -- 1 week ago
        DATE_ADD('day', -14, current_date),-- 2 weeks ago
        DATE_ADD('day', -28, current_date) -- 4 weeks ago
      )
    """

    # Execute the Athena query
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'mtg'
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Check the status of the query
    query_status = None
    while query_status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = response['QueryExecution']['Status']['State']
        if query_status == 'SUCCEEDED':
            print("Query succeeded")
        elif query_status in ['FAILED', 'CANCELLED']:
            print(f"Query {query_status}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Query {query_status}: {response['QueryExecution']['Status']['StateChangeReason']}")
            }
        else:
            print("Query is still running, waiting for 2 seconds...")
            time.sleep(2)

    # Get the output file location
    query_output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
    parsed_url = urlparse(query_output_location)
    source_bucket = parsed_url.netloc
    source_key = parsed_url.path.lstrip('/')

    # Copy the result file to the final location with the desired name
    s3.copy_object(
        Bucket=s3_bucket,
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Key=s3_key
    )

    # Optionally, delete the original Athena-generated file
    s3.delete_object(Bucket=source_bucket, Key=source_key)

    # Return success message
    return {
        'statusCode': 200,
        'body': json.dumps(f"CSV successfully uploaded to s3://{s3_bucket}/{s3_key}")
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