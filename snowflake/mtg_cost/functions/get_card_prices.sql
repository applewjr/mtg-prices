CREATE OR REPLACE FUNCTION MTG_COST.PUBLIC.GET_CARD_PRICES(card_id STRING)
RETURNS TABLE (
     name STRING
    ,usd NUMBER(8,2)
    ,usd_foil NUMBER(8,2)
    ,pull_date DATE
)
LANGUAGE SQL
AS
$$
SELECT
     static.name
    ,price.usd
    ,price.usd_foil
    ,price.pull_date
FROM "MTG_COST"."PUBLIC"."MTG_PRICES" AS price
LEFT JOIN "MTG_COST"."PUBLIC"."MTG_STATIC" AS static ON price.id = static.id
WHERE price.id = card_id
ORDER BY price.pull_date
$$;

-- SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_PRICES('883c6111-c921-4cd6-930d-4fa335ef2871'));