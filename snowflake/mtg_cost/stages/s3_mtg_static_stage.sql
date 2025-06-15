CREATE OR REPLACE STAGE s3_mtg_static_stage
	URL = 's3://mtgdump/mtg_static_parquet/'
	CREDENTIALS = (
	AWS_KEY_ID = '{{AWS_ACCESS_KEY_ID}}' 
	AWS_SECRET_KEY = '{{AWS_SECRET_ACCESS_KEY}}'
	)
	FILE_FORMAT = (TYPE = 'PARQUET');