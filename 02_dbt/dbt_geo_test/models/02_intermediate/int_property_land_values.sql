{{ config(materialized='table') }}

WITH unique_buildings AS (
    SELECT building_id, lon, lat FROM {{ ref('stg_train_data') }}
    UNION DISTINCT
    SELECT building_id, lon, lat FROM {{ ref('stg_test_data') }}
),

building_geom AS (
    SELECT 
        building_id,
        ST_POINT(lon, lat) AS b_geom
    FROM unique_buildings
),

joined_trends AS (
    SELECT
        b.building_id,
        -- 💡 stg_land_price_trends の実際のカラム名を使用
        l.PRICE_2025,
        l.PRICE_2023,
        l.PRICE_2021,
        -- 最新の2025年価格の上昇率を簡易的に計算（特徴量として強力）
        (l.PRICE_2025 - l.PRICE_2023) / NULLIF(l.PRICE_2023, 0) as growth_rate_2025,
        ROW_NUMBER() OVER (PARTITION BY b.building_id ORDER BY ST_DISTANCE(b.b_geom, l.LAND_GEOM) ASC) as dns
    FROM building_geom b
    CROSS JOIN {{ ref('stg_land_price_trends') }} l
    WHERE ST_DWITHIN(b.b_geom, l.LAND_GEOM, 5000) -- 5km以内
)

SELECT
    building_id,
    PRICE_2025 as nearest_land_price,
    PRICE_2023 as land_price_2023,
    growth_rate_2025 as land_growth_rate
FROM joined_trends
WHERE dns = 1