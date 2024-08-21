select *
from {{ source('services_staging', 'fhv_vehicles') }}