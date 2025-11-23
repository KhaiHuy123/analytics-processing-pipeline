
select *
from {{ source('trips_staging', 'yellow_trips') }}