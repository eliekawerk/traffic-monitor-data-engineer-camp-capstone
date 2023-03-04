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
        PARSEDATETIMEBESTEFFORT(rt.datetimestamp) AS datetime,
        CASE
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 1 THEN 'Monday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 2 THEN 'Tuesday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 3 THEN 'Wednesday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 4 THEN 'Thursday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 5 THEN 'Friday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 6 THEN 'Saturday'
            WHEN TODAYOFWEEK(PARSEDATETIMEBESTEFFORT(rt.datetimestamp)) = 7 THEN 'Sunday'
        END AS weekday
    FROM {{ source('default', 'raw_traffic') }} AS rt
