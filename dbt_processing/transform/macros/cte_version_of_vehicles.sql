
-- macros version of vehicles
{% macro version_of_vehicles() %}
with
    fhv_vehicles_report as (
        select
            fv.active ,
            fv.base_type,
            fv.reason,
            vn."name" ,
            vyfv.vehicle_year ,
            wa.wheelchair_accessible,
            fv.expiration_date,
            fv.certification_date,
            fv.hack_up_date
        from {{ ref("services_fhv_vehicles") }} fv
        left join {{ ref("services_vehicle_name") }} vn
        on fv.name_id = vn.id
        left join {{ ref("services_vehicle_year_fhv_vehicle") }} vyfv
        on fv.vehicle_year_id = vyfv.id
        left join {{ ref("services_wheelchair_access") }} wa
        on fv.wheelchair_accessible_id = wa.id
    ),
    vehicle_year as (
        select
            fvr.vehicle_year,
            fvr.base_type,
            fvr.active,
            count(fvr.vehicle_year) as num_of_versions_released,
            fvr.wheelchair_accessible
        from fhv_vehicles_report fvr
        group by fvr.vehicle_year, fvr.base_type, fvr.active,
                 fvr.wheelchair_accessible
        order by fvr.vehicle_year
    )
select * from vehicle_year

{% endmacro %}