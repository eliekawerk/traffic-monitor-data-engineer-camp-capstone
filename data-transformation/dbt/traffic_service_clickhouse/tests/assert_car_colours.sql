-- Test assertion for valid car colour
SELECT *
FROM {{ ref('staging_traffic_vw') }}
WHERE colour NOT IN ('white', 'green', 'blue', 'red', 'black', 'silver', 'yellow')