CREATE OR REPLACE FUNCTION MTG_COST.PUBLIC.GET_CARD_ID(card_name STRING, set_search STRING)
RETURNS TABLE (
     name STRING
    ,set_name STRING
    ,id STRING
    ,tcgplayer_url STRING
    ,price_records_count NUMBER
    ,avg_price NUMBER(10,2)
    ,min_price NUMBER(10,2)
    ,max_price NUMBER(10,2)
    ,avg_foil_price NUMBER(10,2)
    ,min_foil_price NUMBER(10,2)
    ,max_foil_price NUMBER(10,2)
)
LANGUAGE SQL
AS
$$
    SELECT
         s.NAME
        ,s.SET_NAME
        ,s.ID
        ,'https://www.tcgplayer.com/product/' || s.TCGPLAYER_ID AS tcgplayer_url
        ,COUNT(p.ID) AS price_records_count
        ,ROUND(AVG(p.USD), 2) AS avg_price
        ,ROUND(MIN(p.USD), 2) AS avg_price
        ,ROUND(MAX(p.USD), 2) AS avg_price
        ,ROUND(AVG(p.USD_FOIL), 2) AS avg_price
        ,ROUND(MIN(p.USD_FOIL), 2) AS avg_price
        ,ROUND(MAX(p.USD_FOIL), 2) AS avg_price
    FROM MTG_COST.PUBLIC.MTG_STATIC s
    LEFT JOIN MTG_COST.PUBLIC.MTG_PRICES p ON s.ID = p.ID
    WHERE (card_name IS NULL OR card_name = '' OR LOWER(s.NAME) LIKE '%' || LOWER(card_name) || '%')
      AND (set_search IS NULL OR set_search = '' OR LOWER(s.SET_NAME) LIKE '%' || LOWER(set_search) || '%')
    GROUP BY s.NAME, s.SET_NAME, s.ID, s.TCGPLAYER_ID
    ORDER BY s.NAME
    LIMIT 10000
$$;

-- SELECT * FROM TABLE(MTG_COST.PUBLIC.GET_CARD_ID('squall', 'final')) LIMIT 100;