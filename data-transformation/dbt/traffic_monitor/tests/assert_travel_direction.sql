-- Test assertion for travel direction
SELECT *
FROM {{ ref('staging_traffic_vw') }}
WHERE travel_direction NOT IN ('Northbound', 'Southbound')
