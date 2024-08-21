select *
from {{ source('trips_staging', 'trip_type') }}