{{ config(materialized='view') }}

SELECT 
    travel_direction,
    weekday,
    COUNT(*)                AS car_count,
    SUM(passenger_count)    AS sum_of_passengers
FROM staging_traffic_vw
GROUP BY travel_direction, weekday
ORDER BY travel_direction, weekday