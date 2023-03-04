{{ config(materialized='view') }}

    SELECT
        rt.year AS year,
        rt.make AS make,
        rt.model AS model,
        rt.category AS category,
        rt.license_plate,
        rt.colour,
        rt.fuel_type,
        rt.passenger_count,
        rt.lane,
        rt.travel_direction,
        rt.speed AS speed_in_km_per_hour,
        parseDateTimeBestEffort(rt.datetimestamp) AS datetime,
        CASE
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 1 THEN 'Monday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 2 THEN 'Tuesday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 3 THEN 'Wednesday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 4 THEN 'Thursday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 5 THEN 'Friday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 6 THEN 'Saturday'
            WHEN toDayOfWeek(parseDateTimeBestEffort(rt.datetimestamp)) = 7 THEN 'Sunday'
        END AS weekday
    FROM {{ source('default', 'raw_traffic') }} AS rt
