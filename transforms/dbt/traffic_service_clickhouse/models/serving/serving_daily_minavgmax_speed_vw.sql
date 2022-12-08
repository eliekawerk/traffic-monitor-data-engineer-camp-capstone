{{ config(materialized='view') }}

    SELECT 
        weekday,
        travel_direction,
        MIN(speed_in_km_per_hour)               AS min_speed_in_km_per_hour,
        ROUND(AVG(speed_in_km_per_hour),2)      AS avg_speed_in_km_per_hour,    
        MAX(speed_in_km_per_hour)               AS max_speed_in_km_per_hour
    FROM staging_traffic_vw
    GROUP BY weekday,travel_direction
    ORDER BY weekday, travel_direction