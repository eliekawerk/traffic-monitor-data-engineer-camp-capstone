-- Test assertion for car years - we don't expect production cars prior to 1892 to exist
select *
from staging_traffic_vw
where year < 1892
