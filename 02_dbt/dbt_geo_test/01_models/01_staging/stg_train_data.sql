SELECT
    -- train には id がないので、行番号で代用
    CAST(ROW_NUMBER() OVER (ORDER BY building_id, full_address) AS STRING) AS id,
    CAST(building_id AS STRING) AS building_id,
    CAST(money_room AS FLOAT) AS target_price,
    
    -- 💡 面積：正確な値がなければMAX/MINの中央値で救済
    COALESCE(
        CAST(unit_area AS FLOAT), 
        (CAST(unit_area_max AS FLOAT) + CAST(unit_area_min AS FLOAT)) / 2.0
    ) AS house_area,
    
    CAST(floor_count AS FLOAT) AS floor_count,
    CAST(year_built AS INT) AS year_built,
    CAST(target_ym AS INT) AS target_ym,
    
    -- 💡 住所：完全住所と、集計用の住所グループを作成
    CAST(full_address AS STRING) AS full_address,
    LEFT(CAST(full_address AS STRING), 12) AS addr_group,
    
    CAST(lon AS FLOAT) AS lon,
    CAST(lat AS FLOAT) AS lat,
    
    -- 💡 駅：元データにある確実な駅名を採用
    CAST(eki_name1 AS STRING) AS eki_name_raw,
    CAST(walk_distance1 AS FLOAT) AS walk_dist_raw
FROM {{ source('raw_data', 'train') }}