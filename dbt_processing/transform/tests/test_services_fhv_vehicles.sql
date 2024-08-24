
-- testing, expectation : nothing returned

-- purpose is to make sure no inactive vehicles included
select *
from {{ ref("services_fhv_vehicles") }} fv
where fv.active <> 'YES'
or extract(year from fv.certification_date) > extract(year from fv.expiration_date)

-- skip this this because operating FHV vehicles can be done without license renewals
-- select * from {{ source('services_staging', 'fhv_vehicles') }} fv
-- where fv.reason = 'G' and fv.active = 'YES' and
-- extract(year from fv.hack_up_date) > extract(year from fv.certification_date);

