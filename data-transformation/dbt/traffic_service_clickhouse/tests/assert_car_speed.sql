-- Test assertion for valid car speed as it passes the sensor
select *
from staging_traffic_vw
where speed_in_km_per_hour < 1