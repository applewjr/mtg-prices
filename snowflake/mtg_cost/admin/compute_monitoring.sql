SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
ORDER BY START_TIME DESC;

SELECT
     TO_DATE(START_TIME) AS day
    ,WAREHOUSE_NAME
    ,SUM(CREDITS_USED) AS credits_used
    ,ROUND(SUM(CREDITS_USED)*2,2) AS cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY day, WAREHOUSE_NAME
ORDER BY day DESC, credits_used DESC;

SELECT
     WAREHOUSE_NAME
    ,SUM(CREDITS_USED) AS credits_used
    ,ROUND(SUM(CREDITS_USED)*2,2) AS cost
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY WAREHOUSE_NAME
ORDER BY credits_used DESC;