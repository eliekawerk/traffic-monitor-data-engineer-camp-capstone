{{ config(materialized='view') }}

SELECT
    travel_direction,
    fuel_type,
    colour,
    make,
    toDate(datetime) AS date,
    COUNT(*) AS car_count
FROM {{ ref('staging_traffic_vw') }}
GROUP BY date, travel_direction, fuel_type, colour, make
