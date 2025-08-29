CREATE OR REPLACE PROCEDURE MTG_COST.PUBLIC.LOAD_MTG_STATIC(
    load_date DATE DEFAULT CURRENT_DATE()
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_date_str STRING;
    file_path STRING;
    result_msg STRING;
    rows_merged INTEGER;
    rows_inserted INTEGER;
    rows_updated INTEGER;
    rows_skipped INTEGER;
BEGIN
    -- Get date in YYYYMMDD format (using parameter or current date)
    current_date_str := TO_CHAR(load_date, 'YYYYMMDD');
    
    -- Build dynamic file path
    file_path := '@s3_mtg_static_stage/year=' || YEAR(load_date) || 
                '/month=' || LPAD(MONTH(load_date), 2, '0') || 
                '/day=' || LPAD(DAY(load_date), 2, '0') || 
                '/';

    -- Step 1: Create temporary staging table
    CREATE OR REPLACE TEMPORARY TABLE temp_mtg_static_weekly LIKE MTG_COST.PUBLIC.MTG_STATIC;
    
    -- Step 2: Load new data into staging table using dynamic path
    EXECUTE IMMEDIATE 'COPY INTO temp_mtg_static_weekly FROM ' || file_path || 
                    ' PATTERN = ''.*\.parquet$'' ' ||
                    ' FILE_FORMAT = (TYPE = ''PARQUET'') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE';

    -- Step 3: Merge from staging table with date-based logic
    MERGE INTO MTG_COST.PUBLIC.MTG_STATIC AS target
    USING temp_mtg_static_weekly AS source
    ON target.ID = source.ID
    WHEN MATCHED AND source.PULL_DATE > target.PULL_DATE THEN
    UPDATE SET
        ORACLE_ID = source.ORACLE_ID,
        MTGO_ID = source.MTGO_ID,
        MTGO_FOIL_ID = source.MTGO_FOIL_ID,
        TCGPLAYER_ID = source.TCGPLAYER_ID,
        CARDMARKET_ID = source.CARDMARKET_ID,
        NAME = source.NAME,
        LANG = source.LANG,
        RELEASED_AT = source.RELEASED_AT,
        SET_NAME = source.SET_NAME,
        SET_ABBR = source.SET_ABBR,
        SET_TYPE = source.SET_TYPE,
        RARITY = source.RARITY,
        PULL_DATE = source.PULL_DATE
    WHEN NOT MATCHED THEN
        INSERT (ID, ORACLE_ID, MTGO_ID, MTGO_FOIL_ID, TCGPLAYER_ID, CARDMARKET_ID, 
            NAME, LANG, RELEASED_AT, SET_NAME, SET_ABBR, SET_TYPE, RARITY, PULL_DATE)
        VALUES (source.ID, source.ORACLE_ID, source.MTGO_ID, source.MTGO_FOIL_ID, 
            source.TCGPLAYER_ID, source.CARDMARKET_ID, source.NAME, source.LANG, 
            source.RELEASED_AT, source.SET_NAME, source.SET_ABBR, source.SET_TYPE, 
            source.RARITY, source.PULL_DATE);
    
    -- Get detailed merge results
    rows_inserted := (SELECT COUNT(*) FROM temp_mtg_static_weekly t WHERE NOT EXISTS 
                      (SELECT 1 FROM MTG_COST.PUBLIC.MTG_STATIC m WHERE m.ID = t.ID));
    
    rows_updated := (SELECT COUNT(*) FROM temp_mtg_static_weekly t 
                     WHERE EXISTS (SELECT 1 FROM MTG_COST.PUBLIC.MTG_STATIC m 
                                  WHERE m.ID = t.ID AND t.PULL_DATE > m.PULL_DATE));
    
    rows_skipped := (SELECT COUNT(*) FROM temp_mtg_static_weekly t 
                     WHERE EXISTS (SELECT 1 FROM MTG_COST.PUBLIC.MTG_STATIC m 
                                  WHERE m.ID = t.ID AND t.PULL_DATE <= m.PULL_DATE));
    
    rows_merged := rows_inserted + rows_updated;
    
    -- Step 4: Clean up
    DROP TABLE temp_mtg_static_weekly;
    
    -- Return success message with details
    result_msg := 'MTG Static data load completed successfully. File: ' || file_path || 
                  '. Rows inserted: ' || rows_inserted || ', Rows updated: ' || rows_updated || 
                  ', Rows skipped (older data): ' || rows_skipped || 
                  ', Total processed: ' || rows_merged;
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        -- Clean up in case of error
        DROP TABLE IF EXISTS temp_mtg_static_weekly;
        RETURN 'Error occurred during MTG Static data load. File: ' || file_path || 
               '. Error: ' || SQLERRM;
END;
$$;