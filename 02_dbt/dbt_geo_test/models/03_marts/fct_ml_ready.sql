{{ config(materialized='table') }}

WITH base_records AS (
    -- 1. Stagingからデータを統合
    SELECT 
        id, 'train' AS data_type, target_price, house_area, floor_count, 
        year_built, target_ym, full_address, addr_group,
        eki_name_raw, walk_dist_raw, lon, lat, building_id
    FROM {{ ref('stg_train_data') }}
    
    UNION ALL
    
    SELECT 
        id, 'test' AS data_type, NULL AS target_price, house_area, floor_count, 
        year_built, target_ym, full_address, addr_group,
        eki_name_raw, walk_dist_raw, lon, lat, building_id
    FROM {{ ref('stg_test_data') }}
),

calculated_features AS (
    SELECT
        *,
        -- 💡 【重要】月単位の築年数算出 (YYYYMM形式から正確な経過月数を計算)
        (
            ((FLOOR(target_ym / 100) * 12) + MOD(target_ym, 100)) - 
            ((FLOOR(year_built / 100) * 12) + MOD(year_built, 100))
        ) / 12.0 AS building_age
    FROM base_records
)

SELECT
    f.id,
    f.data_type,
    f.target_price,
    f.house_area,
    f.floor_count,
    -- 築年数がNULLの場合は後でPythonで埋めるため、ここでは計算結果をそのまま出す
    f.building_age,
    f.lon,
    f.lat,
    f.building_id,
    f.full_address,
    f.addr_group,
    
    -- 💡 【重要】駅名の統合 (元データを最優先、なければ空間結合の結果を使う)
    CASE 
        WHEN f.eki_name_raw IS NULL OR f.eki_name_raw = '＊＊＊＊' THEN s.STATION_NAME 
        ELSE f.eki_name_raw 
    END AS STATION_NAME,
    s.city_name,
    
    -- 💡 【重要】徒歩距離の統合 (元データの徒歩分をm換算(1分=80m)し、なければ空間結合を使う)
    COALESCE(f.walk_dist_raw * 80, s.recovered_walk_dist) AS final_walk_dist,
    
    -- 地価公示データの結合
    l.nearest_land_price,
    l.land_growth_rate

FROM calculated_features f
-- 空間結合の結果を結合 (095475などの救済用)
LEFT JOIN {{ ref('int_station_distances') }} s 
    ON f.building_id = s.building_id AND f.data_type = s.data_type
-- 地価公示データの結合
LEFT JOIN {{ ref('int_property_land_values') }} l 
    ON f.building_id = l.building_id