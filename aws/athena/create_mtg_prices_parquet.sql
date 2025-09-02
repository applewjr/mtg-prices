CREATE EXTERNAL TABLE IF NOT EXISTS mtg.mtg_prices_parquet (
     id STRING
    ,usd DOUBLE
    ,usd_foil DOUBLE
    ,pull_date STRING
    )
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://${MTG_PRIMARY_BUCKET}/mtg_parquet';