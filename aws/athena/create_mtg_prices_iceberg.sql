CREATE TABLE IF NOT EXISTS mtg.mtg_prices_iceberg (
	 id STRING
	,usd DECIMAL(10, 2)
	,usd_foil DECIMAL(10, 2)
	,pull_date DATE
	)
PARTITIONED BY (`pull_date`)
LOCATION 's3://mtgdump/mtg_prices_iceberg'
TBLPROPERTIES (
	 'table_type'='iceberg'
	,'format'='parquet'
	);