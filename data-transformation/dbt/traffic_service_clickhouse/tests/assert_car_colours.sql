-- Test assertion for valid car colour
select *
from staging_traffic_vw
where colour not in ('white', 'green', 'blue', 'red', 'black', 'silver', 'yellow')