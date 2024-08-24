
select *
from {{ source('services_staging', 'fhv_base_aggregate_report') }}