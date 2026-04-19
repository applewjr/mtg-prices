import boto3
import json
import time
from urllib.parse import urlparse

from utils import get_multiple_parameters

s3 = boto3.client('s3')
ssm = boto3.client('ssm')
athena = boto3.client('athena')

def lambda_handler(event, context):
    # Get configuration
    params = get_multiple_parameters([
        '/mtg/s3/buckets/primary_bucket',
        '/mtg/s3/buckets/output_bucket',
        '/mtg/s3/paths/final_output_key'
    ], ssm)
    primary_bucket = params['/mtg/s3/buckets/primary_bucket']
    output_bucket = params['/mtg/s3/buckets/output_bucket']
    final_output_key = params['/mtg/s3/paths/final_output_key']

    output_location = f"s3://{primary_bucket}/athena_output/"

    query = """
    SELECT * FROM vw_mtg_daily_query
    """

    # Start query
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # Poll until complete
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = response['QueryExecution']['Status']['State']
        if query_status == 'SUCCEEDED':
            print("Query succeeded")
            break
        elif query_status in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status']['StateChangeReason']
            return {
                'statusCode': 500,
                'body': json.dumps(f"Query {query_status}: {reason}")
            }
        time.sleep(2)

    # Copy Athena output directly to final destination
    athena_output_key = urlparse(
        response['QueryExecution']['ResultConfiguration']['OutputLocation']
    ).path.lstrip('/')

    s3.copy_object(
        Bucket=output_bucket,
        CopySource={'Bucket': primary_bucket, 'Key': athena_output_key},
        Key=final_output_key
    )
    s3.delete_object(Bucket=primary_bucket, Key=athena_output_key)

    return {
        'statusCode': 200,
        'body': json.dumps(f"CSV uploaded to s3://{output_bucket}/{final_output_key}")
    }