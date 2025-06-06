CREATE OR REPLACE PROCEDURE MTG_COST.PUBLIC.LOAD_MTG_PRICES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING;
BEGIN
    -- Execute the COPY INTO command
    COPY INTO MTG_COST.PUBLIC.MTG_PRICES 
    FROM @s3_mtg_prices_stage 
    FILE_FORMAT = (TYPE = 'PARQUET') 
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE 
    ON_ERROR = 'CONTINUE' 
    FORCE = FALSE;
    
    -- Get the result information
    result_msg := 'COPY INTO operation completed successfully. Check COPY_HISTORY for details.';
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred during COPY INTO operation: ' || SQLERRM;
END;
$$;