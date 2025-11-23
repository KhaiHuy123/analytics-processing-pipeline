
-- testing, expectation : nothing returned

-- purpose is to make sure certification is the latest one
select *
from {{ ref("services_shl_permits") }} sp
where extract(year from sp.hack_up_date)  > extract(year from sp.certification_date)
and sp.active = 'YES'
and sp.suspension_reason <> 'G'

