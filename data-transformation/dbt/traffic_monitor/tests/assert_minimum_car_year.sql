-- Test assertion for car years - we don't expect production cars prior to 1892 to exist
SELECT *
FROM {{ ref('staging_traffic_vw') }}
WHERE year < 1892
