
select *
from {{ source('services_staging', 'shl_taxi_initial_inspect') }}