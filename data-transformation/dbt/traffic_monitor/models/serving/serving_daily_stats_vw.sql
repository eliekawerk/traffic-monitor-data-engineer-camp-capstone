{{ config(materialized='view') }}

SELECT
    travel_direction,
    TODATE(datetime) AS date,
    COUNT(*) AS car_count,
    MIN(speed_in_km_per_hour) AS min_speed_in_km_per_hour,
    ROUND(AVG(speed_in_km_per_hour), 2) AS avg_speed_in_km_per_hour,
    MAX(speed_in_km_per_hour) AS max_speed_in_km_per_hour,
    SUM(passenger_count) AS sum_of_passengers
FROM {{ ref('staging_traffic_vw') }}
GROUP BY TODATE(datetime), travel_direction
