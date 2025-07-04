CREATE VIEW daily_compute_costs AS

SELECT
  TO_DATE(START_TIME) AS day,
  ROUND(SUM(CREDITS_USED)*2,2) AS daily_usd,
  SUM(CREDITS_USED) AS daily_credits
  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GROUP BY day
ORDER BY day DESC;