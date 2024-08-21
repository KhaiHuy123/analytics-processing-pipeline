
-- macros fhv vehicle type
{% macro monthly_report (license_class, include_vehicles_on_work_report=True) %}
with
    report as (
        select *
        from {{ ref("report_monthly") }} drm
        where drm.license_class like '%{{ license_class }}%'
    )
{% if include_vehicles_on_work_report %}
,
    vehicles_on_work_report as (
        select
            r.vehicles_per_day as on_work_vehicles,
            r.unique_vehicles as total_vehicles,
            (r.unique_vehicles - r.vehicles_per_day) as unvailable_vehicles
        from report r
    )
{% endif %}
,
    fare_box_per_trips as (
        select
            r.month_year,
            r.license_class,
            round(r.farebox_per_day::numeric, 2) as farebox_per_day ,
            r.trips_per_day,
            round((r.farebox_per_day / r.trips_per_day)::numeric, 2) as fare_per_trip
        from report r
    )

select *
from
    {% if include_vehicles_on_work_report %}
        vehicles_on_work_report
    {% else %}
        fare_box_per_trips
    {% endif %}
{% endmacro %}
