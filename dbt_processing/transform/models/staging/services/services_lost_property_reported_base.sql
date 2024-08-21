select *
from {{ source('services_staging', 'lost_property_reported_base') }}