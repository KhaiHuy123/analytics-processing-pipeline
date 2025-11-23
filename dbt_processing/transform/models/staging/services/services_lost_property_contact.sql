
select *
from {{ source('services_staging', 'lost_property_contact') }}