import boto3
from datetime import datetime
import json
import time

from utils import get_dates, get_multiple_parameters

athena = boto3.client('athena')
sns_client = boto3.client('sns')
ssm = boto3.client('ssm')

def lambda_handler(event, context):

    # Pull source row counts from step function payload
    source_daily_count = event.get('daily_row_count', 0)
    source_static_count = event.get('static_row_count', 0)

    dates_dict = get_dates()

    # Get configuration
    param_names = [
        '/mtg/s3/buckets/primary_bucket',
        '/mtg/sns/status_topic_arn'
    ]
    params = get_multiple_parameters(param_names, ssm)
    primary_bucket = params['/mtg/s3/buckets/primary_bucket']
    status_topic_arn = params['/mtg/sns/status_topic_arn']

    s3_output = f's3://{primary_bucket}/athena_out/'

    body_return = {}
    body_return['default'] = 'This is the default message'



    ##### Process daily prices partition
    print("Adding partition for daily prices table...")
    query1 = f"""
    ALTER TABLE mtg_prices_parquet ADD IF NOT EXISTS
    PARTITION (year='{dates_dict['year']}', month='{dates_dict['month']}', day='{dates_dict['day']}')
    LOCATION 's3://{primary_bucket}/mtg_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/'
    """

    response1 = athena.start_query_execution(
        QueryString=query1,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
        )
    
    query1_execution_id = response1['QueryExecutionId']
    query1_logs = wait_for_query_to_complete(query1_execution_id, athena)

    body_return['daily_prices_partition'] = response1
    body_return['daily_prices_partition_logs'] = query1_logs



    ##### Process static data partition
    print("Adding partition for static data table...")
    query2 = f"""
    ALTER TABLE mtg_static_parquet
    SET LOCATION 's3://{primary_bucket}/mtg_static_parquet/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/';
    """

    response2 = athena.start_query_execution(
        QueryString=query2,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
        )
    
    query2_execution_id = response2['QueryExecutionId']
    query2_logs = wait_for_query_to_complete(query2_execution_id, athena)

    body_return['static_data_partition'] = response2
    body_return['static_data_partition_logs'] = query2_logs



    ##### Process Iceberg merge for daily prices
    print("Merging daily prices into Iceberg table...")
    query3 = f"""
    MERGE INTO mtg_prices_iceberg AS target
    USING (
        SELECT
            id,
            CAST(usd AS DECIMAL(10, 2)) AS usd,
            CAST(usd_foil AS DECIMAL(10, 2)) AS usd_foil,
            CAST(pull_date AS DATE) AS pull_date
        FROM mtg_prices_parquet
        WHERE year = '{dates_dict['year']}'
        AND month = '{dates_dict['month']}'
        AND day = '{dates_dict['day']}'
    ) AS source
    ON target.id = source.id AND target.pull_date = source.pull_date
    WHEN NOT MATCHED THEN
    INSERT (id, usd, usd_foil, pull_date)
    VALUES (source.id, source.usd, source.usd_foil, source.pull_date);
    """

    response3 = athena.start_query_execution(
        QueryString=query3,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
        )
    
    query3_execution_id = response3['QueryExecutionId']
    query3_logs = wait_for_query_to_complete(query3_execution_id, athena)

    body_return['iceberg_merge'] = response3
    body_return['iceberg_merge_logs'] = query3_logs



    ##### Get count from Iceberg table
    print("Getting count from Iceberg table...")
    query4 = f"""
    SELECT count(*) as total_count 
    FROM mtg_prices_iceberg 
    WHERE date(pull_date) = date('{dates_dict['formatted_date']}')
    """

    response4 = athena.start_query_execution(
        QueryString=query4,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    
    query4_execution_id = response4['QueryExecutionId']
    query4_logs = wait_for_query_to_complete(query4_execution_id, athena)
    results4 = athena.get_query_results(QueryExecutionId=query4_execution_id)

    iceberg_count = None
    if len(results4['ResultSet']['Rows']) > 1:
        iceberg_count = results4['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    else:
        iceberg_count = '0'

    body_return['iceberg_count_query'] = response4
    body_return['iceberg_count_logs'] = query4_logs
    body_return['iceberg_count_results'] = results4
    body_return['iceberg_count'] = iceberg_count



    ##### Get count from daily prices parquet table
    print("Getting count from daily prices parquet table...")
    query5 = f"""
    SELECT count(*) as total_count 
    FROM mtg_prices_parquet
    WHERE year = '{dates_dict['year']}'
        AND month = '{dates_dict['month']}'
        AND day = '{dates_dict['day']}'
    """

    response5 = athena.start_query_execution(
        QueryString=query5,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
    )

    query5_execution_id = response5['QueryExecutionId']
    query5_logs = wait_for_query_to_complete(query5_execution_id, athena)
    results5 = athena.get_query_results(QueryExecutionId=query5_execution_id)

    parquet_prices_count = None
    if len(results5['ResultSet']['Rows']) > 1:
        parquet_prices_count = results5['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    else:
        parquet_prices_count = '0'

    body_return['parquet_prices_count_query'] = response5
    body_return['parquet_prices_count_logs'] = query5_logs
    body_return['parquet_prices_count_results'] = results5
    body_return['parquet_prices_count'] = parquet_prices_count



    ##### Get count from static parquet table
    print("Getting count from static parquet table...")
    query6 = f"""
    SELECT count(*) as total_count 
    FROM mtg_static_parquet
    """

    response6 = athena.start_query_execution(
        QueryString=query6,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
    )

    query6_execution_id = response6['QueryExecutionId']
    query6_logs = wait_for_query_to_complete(query6_execution_id, athena)
    results6 = athena.get_query_results(QueryExecutionId=query6_execution_id)

    parquet_static_count = None
    if len(results6['ResultSet']['Rows']) > 1:
        parquet_static_count = results6['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    else:
        parquet_static_count = '0'

    body_return['parquet_static_count_query'] = response6
    body_return['parquet_static_count_logs'] = query6_logs
    body_return['parquet_static_count_results'] = results6
    body_return['parquet_static_count'] = parquet_static_count



    ##### Compare source counts vs Athena counts
    daily_prices_match = int(parquet_prices_count) == source_daily_count
    static_match = int(parquet_static_count) == source_static_count
    iceberg_match = int(iceberg_count) == source_daily_count  # Iceberg mirrors daily prices

    count_comparison = {
        'daily_prices': {
            'source': source_daily_count,
            'athena_parquet': int(parquet_prices_count),
            'athena_iceberg': int(iceberg_count),
            'parquet_match': daily_prices_match,
            'iceberg_match': iceberg_match
        },
        'static': {
            'source': source_static_count,
            'athena_parquet': int(parquet_static_count),
            'match': static_match
        },
        'all_counts_match': daily_prices_match and static_match and iceberg_match
    }

    body_return['count_comparison'] = count_comparison
    print(f"Count comparison: {json.dumps(count_comparison, indent=2)}")



    ##### Prepare SNS notification
    email_message = f"""
MTG Data Partitioning for {dates_dict['formatted_date']}

Count Verification:
Daily Prices — Source: {source_daily_count:,} | Parquet: {int(parquet_prices_count):,} | Iceberg: {int(iceberg_count):,} | Match: {daily_prices_match and iceberg_match}
Static Cards — Source: {source_static_count:,} | Parquet: {int(parquet_static_count):,} | Match: {static_match}
All Counts Match: {count_comparison['all_counts_match']}

Daily Prices Partition:
{query1_logs}

Static Data Partition:
{query2_logs}

Iceberg Merge:
{query3_logs}

Iceberg Count Verification:
{query4_logs}
Final Iceberg Prices Count: {iceberg_count}

Parquet Daily Prices Count Verification:
{query5_logs}
Final Parquet Prices Count: {parquet_prices_count}

Parquet Static Count Verification:
{query6_logs}
Final Parquet Static Count: {parquet_static_count}
    """
    
    sns_return = {
        'default': 'This is the default message',
        'email': email_message
    }

    overall = "SUCCESS" if count_comparison['all_counts_match'] else "FAIL"

    subject = f"MTG Data Partitioning {overall} - {dates_dict['formatted_date']}: (D:{daily_prices_match and iceberg_match} S:{static_match})"

    # Send SNS email notification
    try:
        sns_response = sns_client.publish(
            TopicArn=status_topic_arn,
            Message=json.dumps(sns_return),
            MessageStructure='json',
            Subject=subject
        )

        print(f"SNS message sent. Response: {sns_response}")
        body_return['sns_response'] = sns_response
    except Exception as e:
        error_message = f"Error sending SNS notification: {str(e)}"
        print(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

    return {
        'statusCode': 200,
        'message': 'Both partitions processed successfully',
        'date_processed': dates_dict['formatted_date'],
        'source_daily_count': source_daily_count,
        'source_static_count': source_static_count,
        'iceberg_count': iceberg_count,
        'parquet_prices_count': parquet_prices_count,
        'parquet_static_count': parquet_static_count,
        'count_comparison': count_comparison,
        'body': body_return
    }

def wait_for_query_to_complete(query_execution_id, athena_client, check_interval=5):
    """
    Poll the query status every `check_interval` seconds
    until it is in a final state: SUCCEEDED, FAILED, or CANCELLED.
    Logs the StateChangeReason if the query fails or is canceled.
    Returns a JSON object with all printed statements.
    """
    logs = []

    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        state_change_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'No further details')

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            log_statement = f"Query {query_execution_id} finished with status: {status}"
            logs.append(log_statement)
            print(log_statement)
            if status in ['FAILED', 'CANCELLED']:
                reason_log = f"Reason: {state_change_reason}"
                logs.append(reason_log)
                print(reason_log)
            break
        else:
            log_statement = f"Query {query_execution_id} is in status '{status}'. Waiting {check_interval}s..."
            logs.append(log_statement)
            print(log_statement)
            time.sleep(check_interval)

    return json.dumps({"logs": logs}, indent=4)