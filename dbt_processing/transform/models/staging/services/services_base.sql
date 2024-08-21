select *
from {{ source('services_staging', 'base') }}