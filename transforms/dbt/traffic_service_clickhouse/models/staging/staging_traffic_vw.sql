{{ config(materialized='view') }}

    SELECT
        parseDateTimeBestEffort(rt.datetimestamp) AS datetime,
        CASE 
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 1 THEN 'Monday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 2 THEN 'Tuesday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 3 THEN 'Wednesday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 4 THEN 'Thursday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 5 THEN 'Friday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 6 THEN 'Saturday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 7 THEN 'Sunday'
            ELSE Null
        END AS weekday,
        rt.Year AS year,
        rt.Make AS make,
        rt.Model AS model,
        rt.Category AS category,
        rt.license_plate,
        rt.colour,
        rt.fuel_type,
        rt.passenger_count,
        rt.travel_direction,
        rt.speed AS speed_in_km_per_hour
    --FROM raw_traffic AS rt
    FROM {{ source('default', 'raw_traffic') }}