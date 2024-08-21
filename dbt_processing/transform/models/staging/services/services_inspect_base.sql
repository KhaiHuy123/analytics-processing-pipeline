select *
from {{ source('services_staging', 'inspect_base') }}