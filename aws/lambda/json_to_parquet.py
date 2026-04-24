import boto3
import json
import ijson
import pyarrow as pa
import pyarrow.parquet as pq

from utils import get_dates, get_multiple_parameters

s3 = boto3.client('s3')
ssm = boto3.client('ssm')


def lambda_handler(event, context):
    dates_dict = get_dates()

    # Get configuration from SSM
    try:
        param_names = ['/mtg/s3/buckets/primary_bucket']
        params = get_multiple_parameters(param_names, ssm)
        primary_bucket = params['/mtg/s3/buckets/primary_bucket']

    except Exception as e:
        error_message = f"Error getting SSM parameters: {str(e)}"
        print(error_message)
        raise

    # Define S3 paths
    json_key = f"mtg_temp_json/all_cards_{dates_dict['short_date']}.json"

    daily_parquet_key = (
        f"mtg_parquet/year={dates_dict['year']}/month={dates_dict['month']}"
        f"/day={dates_dict['day']}/daily_prices_{dates_dict['short_date']}.parquet"
    )
    static_parquet_key = (
        f"mtg_static_parquet/year={dates_dict['year']}/month={dates_dict['month']}"
        f"/day={dates_dict['day']}/static_cards_{dates_dict['short_date']}.parquet"
    )

    print(f"Reading JSON from s3://{primary_bucket}/{json_key}")

    # Stream JSON once, collect both row sets in a single pass
    try:
        response = s3.get_object(Bucket=primary_bucket, Key=json_key)
        json_stream = ijson.items(response['Body'], 'item')

        daily_rows, static_rows = process_stream(json_stream, dates_dict['formatted_date'])
    except Exception as e:
        error_message = f"Error processing JSON stream: {str(e)}"
        print(error_message)
        raise

    print(f"Daily price rows: {len(daily_rows)}")
    print(f"Static card rows: {len(static_rows)}")

    # Write daily prices parquet
    try:
        daily_local_path = f"/tmp/daily_prices_{dates_dict['short_date']}.parquet"
        write_daily_parquet(daily_rows, daily_local_path)
        s3.upload_file(daily_local_path, primary_bucket, daily_parquet_key)
        print(f"Uploaded daily parquet: s3://{primary_bucket}/{daily_parquet_key}")
        # s3.upload_file(daily_local_path, primary_bucket_testing, daily_parquet_key)
        # print(f"Uploaded daily parquet: s3://{primary_bucket_testing}/{daily_parquet_key}")
    except Exception as e:
        error_message = f"Error writing daily parquet: {str(e)}"
        print(error_message)
        raise

    # Write static card parquet
    try:
        static_local_path = f"/tmp/static_cards_{dates_dict['short_date']}.parquet"
        write_static_parquet(static_rows, static_local_path)
        s3.upload_file(static_local_path, primary_bucket, static_parquet_key)
        print(f"Uploaded static parquet: s3://{primary_bucket}/{static_parquet_key}")
        # s3.upload_file(static_local_path, primary_bucket_testing, static_parquet_key)
        # print(f"Uploaded static parquet: s3://{primary_bucket_testing}/{static_parquet_key}")
    except Exception as e:
        error_message = f"Error writing static parquet: {str(e)}"
        print(error_message)
        raise

    return {
        'statusCode': 200,
        'daily_parquet_key': daily_parquet_key,
        'static_parquet_key': static_parquet_key,
        'daily_row_count': len(daily_rows),
        'static_row_count': len(static_rows),
        'body': json.dumps('JSON converted to daily + static parquet successfully')
    }


def process_stream(json_stream, pull_date):
    """
    Single pass over the JSON stream. For each card, extract daily price fields
    (if any price exists) and static card fields. Returns two lists of dicts.
    """
    daily_rows = []
    static_rows = []

    static_keys = [
        'id', 'oracle_id', 'mtgo_id', 'mtgo_foil_id', 'tcgplayer_id',
        'cardmarket_id', 'name', 'lang', 'released_at', 'set_name',
        'set', 'set_type', 'rarity'
    ]

    for card in json_stream:
        # Daily prices: only keep cards with at least one non-null price
        prices = card.get('prices') or {}
        usd = prices.get('usd')
        usd_foil = prices.get('usd_foil')

        if usd is not None or usd_foil is not None:
            daily_rows.append({
                'id': card.get('id'),
                'usd': float(usd) if usd is not None else None,
                'usd_foil': float(usd_foil) if usd_foil is not None else None,
                'pull_date': pull_date
            })

        # Static fields: always emit a row
        static_row = {key: card.get(key) for key in static_keys}
        static_row['pull_date'] = pull_date
        static_rows.append(static_row)

    return daily_rows, static_rows


def write_daily_parquet(rows, output_path):
    """
    Write daily prices parquet with explicit schema to match Athena table.
    usd and usd_foil are DOUBLE, matching the pyspark behavior.
    """
    schema = pa.schema([
        pa.field('id', pa.string()),
        pa.field('usd', pa.float64()),
        pa.field('usd_foil', pa.float64()),
        pa.field('pull_date', pa.string()),
    ])

    arrays = [
        pa.array([r['id'] for r in rows], type=pa.string()),
        pa.array([r['usd'] for r in rows], type=pa.float64()),
        pa.array([r['usd_foil'] for r in rows], type=pa.float64()),
        pa.array([r['pull_date'] for r in rows], type=pa.string()),
    ]

    table = pa.Table.from_arrays(arrays, schema=schema)
    # Match pyspark's uncompressed daily output
    pq.write_table(table, output_path, compression='none')


def write_static_parquet(rows, output_path):
    """
    Write static card parquet with explicit schema.
    ID columns cast to DOUBLE to match Athena schema, same as pyspark.
    """
    def to_double(v):
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    schema = pa.schema([
        pa.field('id', pa.string()),
        pa.field('oracle_id', pa.string()),
        pa.field('mtgo_id', pa.float64()),
        pa.field('mtgo_foil_id', pa.float64()),
        pa.field('tcgplayer_id', pa.float64()),
        pa.field('cardmarket_id', pa.float64()),
        pa.field('name', pa.string()),
        pa.field('lang', pa.string()),
        pa.field('released_at', pa.string()),
        pa.field('set_name', pa.string()),
        pa.field('set', pa.string()),
        pa.field('set_type', pa.string()),
        pa.field('rarity', pa.string()),
        pa.field('pull_date', pa.string()),
    ])

    def col(name, caster=None):
        if caster is None:
            return [r.get(name) for r in rows]
        return [caster(r.get(name)) for r in rows]

    arrays = [
        pa.array(col('id'), type=pa.string()),
        pa.array(col('oracle_id'), type=pa.string()),
        pa.array(col('mtgo_id', to_double), type=pa.float64()),
        pa.array(col('mtgo_foil_id', to_double), type=pa.float64()),
        pa.array(col('tcgplayer_id', to_double), type=pa.float64()),
        pa.array(col('cardmarket_id', to_double), type=pa.float64()),
        pa.array(col('name'), type=pa.string()),
        pa.array(col('lang'), type=pa.string()),
        pa.array(col('released_at'), type=pa.string()),
        pa.array(col('set_name'), type=pa.string()),
        pa.array(col('set'), type=pa.string()),
        pa.array(col('set_type'), type=pa.string()),
        pa.array(col('rarity'), type=pa.string()),
        pa.array(col('pull_date'), type=pa.string()),
    ]

    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, output_path, compression='snappy')