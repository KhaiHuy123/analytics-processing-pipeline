select *
from {{ source('services_staging', 'vehicle_year_fhv_vehicle') }}