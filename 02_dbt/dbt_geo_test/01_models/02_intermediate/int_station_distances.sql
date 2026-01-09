{{ config(materialized='table') }}

WITH all_properties AS (
    -- 💡 ここで DISTINCT を使い、建物単位（座標単位）に絞り込むことで計算を爆速化
    SELECT DISTINCT building_id, lon, lat, 'train' AS data_type FROM {{ ref('stg_train_data') }}
    UNION ALL
    SELECT DISTINCT building_id, lon, lat, 'test' AS data_type FROM {{ ref('stg_test_data') }}
),

property_geom AS (
    SELECT 
        building_id,
        data_type,
        ST_POINT(lon, lat) AS p_geom
    FROM all_properties
),

distances AS (
    SELECT
        p.building_id,
        p.data_type,
        s.station_name,
        s.city_name,
        ST_DISTANCE(p.p_geom, s.station_geom) AS calc_dist_m
    FROM property_geom p
    CROSS JOIN {{ ref('stg_stations') }} s
    -- 5km以内の駅に絞り込み
    WHERE ST_DWITHIN(p.p_geom, s.station_geom, 5000)
),

ranked_stations AS (
    SELECT 
        *,
        -- 建物×データタイプごとに最も近い駅を1つ抽出
        ROW_NUMBER() OVER (PARTITION BY building_id, data_type ORDER BY calc_dist_m ASC) as dns
    FROM distances
)

SELECT
    building_id,
    data_type,
    station_name,
    city_name,
    calc_dist_m AS recovered_walk_dist
FROM ranked_stations
WHERE dns = 1