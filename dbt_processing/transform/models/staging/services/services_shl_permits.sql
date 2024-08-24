
select *
from {{ source('services_staging', 'shl_permits') }}