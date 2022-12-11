-- Test assertion for travel direction
select *
from staging_traffic_vw
where travel_direction not in ('Northbound', 'Southbound', 'Eastbound', 'Westbound')