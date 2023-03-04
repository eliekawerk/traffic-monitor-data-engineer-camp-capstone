-- Test assertion for valid car speed as it passes the sensor
SELECT *
FROM {{ ref('staging_traffic_vw') }}
WHERE speed_in_km_per_hour < 1