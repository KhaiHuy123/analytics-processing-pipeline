select *
from {{ source('trips_staging', 'store_fwd_flag') }}