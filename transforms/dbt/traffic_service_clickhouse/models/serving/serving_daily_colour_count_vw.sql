{{ config(materialized='view') }}

SELECT 
    weekday,
    travel_direction,    
    colour,
    COUNT(*)    AS car_count
FROM staging_traffic_vw
GROUP BY weekday,travel_direction, colour
ORDER BY weekday, travel_direction, colour