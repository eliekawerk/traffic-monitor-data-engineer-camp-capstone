-- Test assertion for valid fuel type
SELECT
    *
FROM {{ ref('staging_traffic_vw') }}
WHERE fuel_type NOT IN ('diesel', 'electric', 'petrol')