
select *
from {{ source('report_staging', 'data_reports_monthly') }}