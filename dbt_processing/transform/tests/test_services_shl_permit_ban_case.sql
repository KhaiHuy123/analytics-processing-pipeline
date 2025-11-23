
-- testing, expectation : nothing returned

-- purpose is to make sure all ban case has the legal reason
select *
from {{ ref("services_shl_permits") }} sp
where sp.suspension_reason <> 'G'
and sp.active ='YES'

