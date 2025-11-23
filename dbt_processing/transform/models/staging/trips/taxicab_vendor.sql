
select *
from {{ source('trips_staging', 'vendor') }}