CREATE OR REPLACE TASK MTG_COST.PUBLIC.WEEKLY_MTG_STATIC_LOAD
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 5 * * 5 America/Los_Angeles' -- 5:00 AM PST every Friday
AS 
CALL MTG_COST.PUBLIC.LOAD_MTG_STATIC();

-- ALTER TASK MTG_COST.PUBLIC.WEEKLY_MTG_STATIC_LOAD RESUME;

-- SHOW TASKS IN SCHEMA MTG_COST.PUBLIC;