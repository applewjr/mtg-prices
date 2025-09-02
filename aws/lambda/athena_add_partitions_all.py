import boto3
from datetime import datetime
import json
import time

athena = boto3.client('athena')
sns_client = boto3.client('sns')
ssm = boto3.client('ssm')

def lambda_handler(event, context):

    dates_dict = get_dates()

    # Get configuration
    param_names = [
        '/mtg/s3/buckets/primary_bucket',
        '/mtg/sns/status_topic_arn'
    ]
    params = get_multiple_parameters(param_names)
    primary_bucket = params['/mtg/s3/buckets/primary_bucket']
    status_topic_arn = params['/mtg/sns/status_topic_arn']

    s3_output = f's3://{primary_bucket}/athena_out/'

    body_return = {}
    body_return['default'] = 'This is the default message'

    # Process daily prices partition
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

    # Process static data partition
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

    # Process Iceberg merge for daily prices
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

    # Get count from Iceberg table
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

    # Prepare SNS notification
    email_message = f"""
    MTG Data Partitioning Complete for {dates_dict['formatted_date']}

    Daily Prices Partition:
    {response1}
    {query1_logs}

    Static Data Partition:
    {response2}
    {query2_logs}

    Iceberg Merge:
    {response3}
    {query3_logs}

    Iceberg Count Verification:
    {response4}
    {query4_logs}
    Final Iceberg Count: {iceberg_count}
    """
    
    sns_return = {
        'default': 'This is the default message',
        'email': email_message
    }

    # Send SNS email notification
    try:
        sns_response = sns_client.publish(
            TopicArn=status_topic_arn,
            Message=json.dumps(sns_return),
            MessageStructure='json'
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
        'iceberg_count': iceberg_count,
        'body': body_return
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

def wait_for_query_to_complete(query_execution_id, athena_client, check_interval=5):
    """
    Poll the query status every `check_interval` seconds
    until it is in a final state: SUCCEEDED, FAILED, or CANCELLED.
    Logs the StateChangeReason if the query fails or is canceled.
    Returns a JSON object with all printed statements.
    """
    logs = []  # List to store all log statements

    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        state_change_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'No further details')

        log_statement = f"Query {query_execution_id} finished with status: {status}" if status in ['SUCCEEDED', 'FAILED', 'CANCELLED'] else f"Query {query_execution_id} is in status '{status}'. Waiting {check_interval}s..."
        logs.append(log_statement)
        print(log_statement)

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
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