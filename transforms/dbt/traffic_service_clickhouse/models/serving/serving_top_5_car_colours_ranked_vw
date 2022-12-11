{{ config(materialized='view') }}

WITH cte_car_make_count AS (
    SELECT
        colour,
        COUNT(*)                        AS car_counts
    FROM {{ ref('staging_traffic_vw') }}
    GROUP BY colour
)
SELECT
    colour,
    car_counts,
    RANK() OVER (ORDER BY car_counts DESC) AS rank_of_car_colours
FROM cte_car_make_count

LIMIT 5