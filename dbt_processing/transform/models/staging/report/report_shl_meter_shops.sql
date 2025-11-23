
select *
from {{ source('report_staging', 'shl_meter_shops') }}