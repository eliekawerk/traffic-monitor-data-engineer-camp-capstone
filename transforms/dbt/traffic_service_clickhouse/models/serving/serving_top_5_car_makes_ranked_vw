{{ config(materialized='view') }}

WITH cte_car_make_count AS (
    SELECT
        make,
        COUNT(*)                AS car_counts
    FROM {{ ref('staging_traffic_vw') }}
    GROUP BY travel_direction, make
)
SELECT
    make,
    car_counts,
    RANK() OVER (ORDER BY car_counts DESC) AS rank_of_car_makes
FROM cte_car_make_count
LIMIT 5