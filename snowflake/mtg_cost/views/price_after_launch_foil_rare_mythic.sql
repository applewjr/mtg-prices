CREATE OR REPLACE VIEW price_after_launch_foil_rare_mythic AS

SELECT
 DATEDIFF(day, static.released_at, prices.pull_date) AS date_diff
,AVG(prices.usd_foil) AS avg_usd
,static.set_name
FROM mtg_static AS static
LEFT JOIN mtg_prices AS prices ON static.id = prices.id
WHERE 1=1
    AND static.set_name IN (
         'Tarkir: Dragonstorm'
        ,'Aetherdrift'
        ,'Duskmourn: House of Horror'
        ,'Bloomburrow'
        ,'Final Fantasy'
        )
    -- AND static.set_name = 'Final Fantasy'
    AND static.rarity IN ('mythic', 'rare')
    AND static.set_type IN ('expansion')
    -- AND YEAR(static.released_at) IN ('2025')
    AND date_diff >= 1
    AND date_diff <= 300
GROUP BY date_diff, static.set_name
ORDER BY date_diff DESC;