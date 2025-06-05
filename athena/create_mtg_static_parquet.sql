CREATE EXTERNAL TABLE IF NOT EXISTS mtg.mtg_static_parquet (
   id STRING
  ,oracle_id STRING
  ,mtgo_id DOUBLE
  ,mtgo_foil_id DOUBLE
  ,tcgplayer_id DOUBLE
  ,cardmarket_id DOUBLE
  ,name STRING
  ,lang STRING
  ,released_at STRING
  ,set_name STRING
  ,set STRING
  ,set_type STRING
  ,rarity STRING
  ,pull_date STRING
  )
STORED AS PARQUET
LOCATION 's3://mtgdump/mtg_static_parquet/year=2024/month=08/day=09/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

ALTER TABLE {event['table']['parquet']}
SET LOCATION 's3://mtgdump/{event['data_folder']['parquet']}/year={year}/month={month}/day={day}/';