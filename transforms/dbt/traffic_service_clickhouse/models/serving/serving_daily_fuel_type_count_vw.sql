{{ config(materialized='view') }}

    SELECT 
        weekday,
        travel_direction,    
        fuel_type,
        COUNT(*)    AS car_count
    FROM staging_traffic_vw
    GROUP BY weekday,travel_direction, fuel_type
    ORDER BY weekday, travel_direction, fuel_type