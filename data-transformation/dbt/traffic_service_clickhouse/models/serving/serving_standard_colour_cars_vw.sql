{{ config(materialized='view') }}

SELECT 
    weekday,
    travel_direction,    
    colour
FROM {{ ref('staging_traffic_vw') }}
where colour in ('white', 'black', 'sliver')