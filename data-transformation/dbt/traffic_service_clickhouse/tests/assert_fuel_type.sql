-- Test assertion for valid fuel type
select *
from staging_traffic_vw
where fuel_type not in ('diesel', 'electric', 'petrol')