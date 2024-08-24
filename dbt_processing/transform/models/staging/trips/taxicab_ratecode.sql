
select *
from {{ source('trips_staging', 'ratecode') }}