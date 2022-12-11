{{ config(materialized='view') }}

SELECT
    toDate(datetime)        AS date,
    travel_direction,
    fuel_type,
    colour,
    make, 
    count(*)                AS car_count
FROM {{ ref('staging_traffic_vw') }}
GROUP BY date, travel_direction, fuel_type, colour, make
