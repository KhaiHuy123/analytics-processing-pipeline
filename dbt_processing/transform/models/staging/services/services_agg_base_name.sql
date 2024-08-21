select *
from {{ source('services_staging', 'agg_base_name') }}