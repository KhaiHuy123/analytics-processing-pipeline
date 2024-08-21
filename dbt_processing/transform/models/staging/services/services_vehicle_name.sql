select *
from {{ source('services_staging', 'vehicle_name') }}