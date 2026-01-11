SELECT
    CAST(id AS STRING) AS id,
    CAST(building_id AS STRING) AS building_id,
    
    -- 💡 面積：正確な値がなければMAX/MINの中央値で救済（Testでも重要！）
    COALESCE(
        CAST(unit_area AS FLOAT), 
        (CAST(unit_area_max AS FLOAT) + CAST(unit_area_min AS FLOAT)) / 2.0
    ) AS house_area,
    
    CAST(floor_count AS FLOAT) AS floor_count,
    CAST(year_built AS INT) AS year_built,
    CAST(target_ym AS INT) AS target_ym,
    
    -- 💡 住所：完全住所と丁目レベルのグループ。欠損補完のキーになります
    CAST(full_address AS STRING) AS full_address,
    LEFT(CAST(full_address AS STRING), 12) AS addr_group,
    
    CAST(lon AS FLOAT) AS lon,
    CAST(lat AS FLOAT) AS lat,
    
    -- 💡 駅：元データにある信頼性の高い駅名と徒歩距離
    CAST(eki_name1 AS STRING) AS eki_name_raw,
    CAST(walk_distance1 AS FLOAT) AS walk_dist_raw,
    CAST(addr2_name AS STRING) AS town_name_raw -- 西中島4丁目などの町名レベル
FROM {{ source('raw_data', 'test') }}