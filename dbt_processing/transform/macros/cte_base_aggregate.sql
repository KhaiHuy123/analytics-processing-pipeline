
-- macros base aggregate
{% macro total_trips_summary(include_total_trips_per_year=true, include_total_trips_per_month=true) %}
{% if include_total_trips_per_year %}
with
    total_trips_per_year_cte as (
        select
            fbar.year as trip_year,
            sum(fbar.total_dispatched_trips) as dispatched_trips,
            sum(fbar.total_dispatched_shared_trips) as shared_trips
        from {{ ref("services_fhv_base_aggregate_report") }} fbar
        where fbar.year >= 2020
        group by fbar.year
    )
{% endif %}
{% if include_total_trips_per_month %}
with
    total_trips_per_month_cte as (
        select
            fbar.year as trip_year,
            fbar.month as trip_month,
            sum(fbar.total_dispatched_trips) as total_dispatched_trips,
            sum(fbar.unique_dispatched_vehicles) as unique_vehicles,
            sum(fbar.total_dispatched_shared_trips) as shared_trips
        from {{ ref("services_fhv_base_aggregate_report") }} fbar
        where fbar.year >= 2020
        group by fbar.year, fbar.month
        order by fbar.year, fbar.month asc
    )
{% endif %}
select *
from
    {% if include_total_trips_per_month %}
        total_trips_per_month_cte
    {% else %}
        total_trips_per_year_cte
    {% endif %}
{% endmacro %}
