-- models/01_staging/stg_stations.sql
SELECT
    station_name,
    city_name, -- 💡 ここで市区町村名を確実に取り込む
    ST_POINT(longitude, latitude) as station_geom 
FROM {{ source('marketplace_data', 'E_SR_PS_2') }}