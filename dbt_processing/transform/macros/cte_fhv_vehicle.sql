
-- macros fhv vehicle type
{% macro vehicle_types_summary(include_vehicle_type_included=true, include_base_type_contained=true) %}
with
    fhv_vehicles_details_cte as (
    select
        fv.active,
        fv.certification_date,
        wa.wheelchair_accessible,
        fv.base_type as vehicle_base_type,
        fv.license_type,
        vn.name,
        b.base_number,
        b.base_name
    from {{ ref("services_fhv_vehicles") }} fv
    left join {{ ref("services_vehicle_name") }} vn
    on fv.name_id = vn.id
    left join {{ ref("services_base") }} b
    on b.id = fv.base_number_id
    left join {{ ref("services_wheelchair_access") }} wa
    on wa.id = fv.wheelchair_accessible_id
    where extract(year from fv.certification_date) >= 2023
    )
{% if include_vehicle_type_included %}
,
    vehicle_types_included_cte as (
    select
        fvd.base_name, fvd.vehicle_base_type,
        count(fvd.name) as included_vehicle_types
    from fhv_vehicles_details_cte fvd
    group by fvd.base_name, fvd.vehicle_base_type
    )
{% endif %}
{% if include_base_type_contained %}
,
    base_types_contained_cte as (
    select
        fvd.vehicle_base_type,
        count(fvd.name) as total_vehicles_by_type
    from fhv_vehicles_details_cte fvd
    group by fvd.vehicle_base_type
    )
{% endif %}
select *
from
    {% if include_vehicle_type_included %}
        vehicle_types_included_cte
    {% elif include_base_type_contained %}
        base_types_contained_cte
    {% endif %}
{% endmacro %}
