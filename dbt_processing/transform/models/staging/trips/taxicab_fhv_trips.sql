
select *
from {{ source('trips_staging', 'fhv_trips') }}