{{ config(materialized='view') }}

SELECT
    weekday,
    travel_direction,
    colour
FROM {{ ref('staging_traffic_vw') }}
WHERE colour IN ('white', 'black', 'sliver')
