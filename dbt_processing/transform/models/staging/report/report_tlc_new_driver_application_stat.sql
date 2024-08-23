select *
from {{ source('report_staging', 'tlc_new_driver_application_stat') }}