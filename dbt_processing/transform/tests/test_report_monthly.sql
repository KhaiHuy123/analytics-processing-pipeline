
-- testing, expectation : nothing returned

select *
from {{ ref("report_monthly") }} drm
where drm.vehicles_per_day > drm.unique_vehicles