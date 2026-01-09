-- models/01_staging/stg_land_price_trends.sql
WITH base AS (
    SELECT 
        pp.podb_land_price_id,
        pp.address,
        ST_ASWKT(pp.point_land_price) as land_geom_wkt, -- 文字列化
        lpts.research_year,
        lpts.posted_land_price
    FROM {{ source('marketplace_land_price', 'E_LP_PP') }} pp
    INNER JOIN {{ source('marketplace_land_price', 'E_LP_TS') }} lpts
        ON pp.podb_land_price_id = lpts.podb_land_price_id
    WHERE pp.rlc_index_name = '住宅地'
)
SELECT
    podb_land_price_id,
    address,
    TO_GEOGRAPHY(land_geom_wkt) as land_geom, -- ここで型を戻す
    MAX(CASE WHEN research_year = 2025 THEN posted_land_price END) as price_2025,
    MAX(CASE WHEN research_year = 2023 THEN posted_land_price END) as price_2023,
    MAX(CASE WHEN research_year = 2021 THEN posted_land_price END) as price_2021
FROM base
GROUP BY 
    podb_land_price_id, 
    address, 
    land_geom_wkt -- 💡 番号ではなく名前で確実に指定