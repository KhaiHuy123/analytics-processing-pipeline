
-- macros vehicle type
{% macro ban_vehicle_summary() %}
with
    ban_case_vehicle as (
        select
            sp.name,
            sp.active,
            sp.suspension_reason,
            vt.vehicle_type as vehicle_type,
            sp.vehicle_year as vehicle_year,
            sp.license_number,
            sp.certification_date
        from {{ ref("services_shl_permits") }} sp
        left join {{ ref("services_vehicle_type") }} vt
        on vt.id = sp.vehicle_type_id
    ),
    ban_vehicles_summary as (
        select
            bh.vehicle_type,
            bh.vehicle_year,
            count(bh.vehicle_type) as ban_cases
        from ban_case_vehicle bh
        where bh.suspension_reason <> 'G'
        group by bh.vehicle_type, bh.vehicle_year
    )
select * from ban_vehicles_summary
{% endmacro %}
