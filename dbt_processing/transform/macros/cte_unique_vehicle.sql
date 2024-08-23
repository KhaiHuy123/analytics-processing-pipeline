
-- macros unique vehicle
{% macro unique_vehicles_summary() %}
with
    unique_vehicles_per_base_cte as (
        select
            fbar.dba,
            fbar."year",
            fbar."month",
            fbar.total_dispatched_trips,
            fbar.total_dispatched_shared_trips,
            fbar.unique_dispatched_vehicles,
            b.base_number,
            b.base_name
        from {{ ref("services_fhv_base_aggregate_report") }} fbar
        full join {{ ref("services_base") }} b
        on b.id = fbar.base_name_id
    ),
    unique_vehicles_summary_cte as (
        select
            uvb.base_name,
            uvb."year",
            uvb."month",
            sum(uvb.unique_dispatched_vehicles) as unique_vehicles
        from unique_vehicles_per_base_cte as uvb
        group by uvb.base_name, uvb."year", uvb."month"
        having uvb."year" >= 2020
        order by uvb."year"
    )
select * from unique_vehicles_summary_cte
{% endmacro %}
