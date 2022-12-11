{{ config(materialized='view') }}

SELECT 
    weekday,
    travel_direction,    
    colour
FROM staging_traffic_vw
where colour in ('white', 'black', 'silver')