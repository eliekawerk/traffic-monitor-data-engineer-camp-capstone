{{ config(materialized='view') }}

SELECT 
    toDate(datetime)                        AS date,
    weekday,
    travel_direction,
    toHour(datetime)                        AS hour,
    ROUND(AVG(speed_in_km_per_hour),2)      AS average_speed_in_km_per_hour,
    COUNT(*)                                AS car_count
FROM staging_traffic_vw
GROUP BY toDate(datetime) , weekday,travel_direction, toHour(datetime)  
ORDER BY toDate(datetime) , weekday, travel_direction, toHour(datetime)  