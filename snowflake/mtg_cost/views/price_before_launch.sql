CREATE OR REPLACE VIEW price_before_launch AS

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    -- AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    AND static.released_at >= '2024-08-02' -- starting with Bloomburrow
    AND date_diff >= -45
    AND date_diff <= 0
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;