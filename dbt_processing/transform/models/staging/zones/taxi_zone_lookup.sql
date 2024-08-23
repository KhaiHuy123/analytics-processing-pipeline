select *
from {{ source('zones_staging', 'taxi_zone_lookup') }}