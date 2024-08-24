
select *
from {{ source('trips_staging', 'payment_type') }}