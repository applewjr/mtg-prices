import boto3
from datetime import datetime
import json
import time
import os

def lambda_handler(event, context):

    dates_dict = get_dates()

    athena = boto3.client('athena')
    s3_output = 's3://mtgdump/athena_out/'

    sns_client = boto3.client('sns')
    topic_arn = os.environ.get('TOPIC_ARN')

    body_return = {}
    body_return['default'] = 'This is the default message'

    if event['run_type'] == 'daily_prices':
        query1 = f"""
        ALTER TABLE {event['table']['parquet']} ADD IF NOT EXISTS
        PARTITION (year='{dates_dict['year']}', month='{dates_dict['month']}', day='{dates_dict['day']}')
        LOCATION 's3://mtgdump/{event['data_folder']['parquet']}/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/'
        """
    else:
        query1 = f"""
        ALTER TABLE {event['table']['parquet']}
        SET LOCATION 's3://mtgdump/{event['data_folder']['parquet']}/year={dates_dict['year']}/month={dates_dict['month']}/day={dates_dict['day']}/';
        """

    response1 = athena.start_query_execution(
        QueryString=query1,
        QueryExecutionContext={'Database': 'mtg'},
        ResultConfiguration={'OutputLocation': s3_output}
        )
    
    query1_execution_id = response1['QueryExecutionId']
    query1_logs = wait_for_query_to_complete(query1_execution_id, athena)

    body_return['response1'] = response1
    body_return['response1_logs'] = query1_logs

    email_message = f"""
    {response1}
    {query1_logs}
    """
    sns_return = {
        'default': 'This is the default message',
        'email': email_message
    }

    if event['run_type'] == 'daily_prices':
        query2 = f"""
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

        response2 = athena.start_query_execution(
            QueryString=query2,
            QueryExecutionContext={'Database': 'mtg'},
            ResultConfiguration={'OutputLocation': s3_output}
            )
        
        query2_execution_id = response2['QueryExecutionId']
        query2_logs = wait_for_query_to_complete(query2_execution_id, athena)

        body_return['response2'] = response2
        body_return['response2_logs'] = query2_logs


        query3 = f"""
        SELECT count(*) as total_count 
        FROM mtg_prices_iceberg 
        WHERE date(pull_date) = date('{dates_dict['formatted_date']}')
        """

        response3 = athena.start_query_execution(
            QueryString=query3,
            QueryExecutionContext={'Database': 'mtg'},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        
        query3_execution_id = response3['QueryExecutionId']
        query3_logs = wait_for_query_to_complete(query3_execution_id, athena)
        results3 = athena.get_query_results(QueryExecutionId=query3_execution_id)

        iceberg_count = None
        if len(results3['ResultSet']['Rows']) > 1:
            iceberg_count = results3['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        else:
            iceberg_count = '0'

        body_return['response3'] = response3
        body_return['response3_logs'] = query3_logs
        body_return['results3_raw'] = results3
        body_return['iceberg_count'] = iceberg_count


        email_message = f"""
        {response1}
        {query1_logs}

        {response2}
        {query2_logs}

        {response3}
        {query3_logs}
        {results3}
        {iceberg_count = }
        """
        sns_return = {
            'default': 'This is the default message',
            'email': email_message
        }

    # Send SNS email notification
    try:
        sns_response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(sns_return),
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
        'table_csv': event['table']['csv'],
        'data_folder_csv': event['data_folder']['csv'],
        'table_parquet': event['table']['parquet'],
        'data_file_parquet': event['data_folder']['parquet'],
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