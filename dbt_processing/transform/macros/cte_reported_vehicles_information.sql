
-- macros reported vehicles
{% macro reported_vehicles() %}
with
    reported_vehicles as(
        select
            lpc.license_number ,
            lpc."name" ,
            lpv."type" ,
            lpv.dmv_plate_number
        from {{ ref("services_lost_property_contact") }} lpc
        join {{ ref("services_lost_property_vehicles") }} lpv
        on lpc.vehicles_id = lpv.id
    ),
    reported_vehicles_summary as (
        select
            rv."type",
            rv."name",
            count(rv."type") as vehicle_type
        from reported_vehicles rv
        group by rv."type", rv."name"
    )
select * from reported_vehicles_summary
{% endmacro %}