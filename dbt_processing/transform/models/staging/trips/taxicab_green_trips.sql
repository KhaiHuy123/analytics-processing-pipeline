
select *
from {{ source('trips_staging', 'green_trips') }}