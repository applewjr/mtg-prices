CREATE OR REPLACE VIEW vw_mtg_daily_query AS

WITH base AS (
SELECT
    price.id,
    CAST(static.tcgplayer_id AS INT) AS tcgplayer_id,
    static.name,
    static.set_name,
    static.set_type,
    CAST(static.released_at AS DATE) AS released_at,
    CAST(price.usd AS DOUBLE) AS usd, 
    CAST(price.pull_date AS DATE) AS pull_date
FROM mtg_prices_iceberg AS price
INNER JOIN mtg_static_parquet AS static ON price.id = static.id
WHERE price.usd IS NOT NULL
    AND price.usd <> 0
    AND CAST(static.released_at AS DATE) >= DATE_ADD('year', -10, current_date)
    AND CAST(price.pull_date AS DATE) IN (
    current_date,
    DATE_ADD('day', -7,  current_date),
    DATE_ADD('day', -14, current_date),
    DATE_ADD('day', -28, current_date)
    )
),
pivoted AS (
SELECT
    id,
    MAX(tcgplayer_id) AS tcgplayer_id,
    MAX(name) AS name,
    MAX(set_name) AS set_name,
    MAX(set_type) AS set_type,
    MAX(released_at) AS released_at,
    MAX(CASE WHEN pull_date = current_date THEN usd END) AS today_price,
    MAX(CASE WHEN pull_date = current_date THEN pull_date END) AS today_price_date,
    MAX(CASE WHEN pull_date = DATE_ADD('day', -7,  current_date) THEN usd END) AS "1wk_ago_price",
    MAX(CASE WHEN pull_date = DATE_ADD('day', -14, current_date) THEN usd END) AS "2wk_ago_price",
    MAX(CASE WHEN pull_date = DATE_ADD('day', -28, current_date) THEN usd END) AS "4wk_ago_price"
FROM base
GROUP BY id
)

SELECT
id,
'https://www.tcgplayer.com/product/' || CAST(tcgplayer_id AS VARCHAR) AS tcgplayer_id,
name,
set_name,
set_type,
released_at,
today_price,
today_price_date,
"1wk_ago_price",
"2wk_ago_price",
"4wk_ago_price",
ROUND(today_price / NULLIF("1wk_ago_price", 0), 4) AS "1wk_diff",
ROUND(today_price / NULLIF("2wk_ago_price", 0), 4) AS "2wk_diff",
ROUND(today_price / NULLIF("4wk_ago_price", 0), 4) AS "4wk_diff"
FROM pivoted
WHERE today_price IS NOT NULL
AND today_price >= 1
ORDER BY "4wk_diff" DESC NULLS LAST