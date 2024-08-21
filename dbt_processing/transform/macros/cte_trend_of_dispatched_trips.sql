
-- macros trend of dispatched trips
{% macro trend_of_dispatched_trips() %}
with
    fhv_agg_report as (
        select
            fbar.dba, abn.base_name  ,
            fbar."year",
            fbar."month",
            fbar.month_name ,
            fbar.total_dispatched_trips ,
            fbar.total_dispatched_shared_trips,
            fbar.unique_dispatched_vehicles
        from {{ ref("services_fhv_base_aggregate_report") }} fbar
        join {{ ref("services_agg_base_name") }} abn
        on fbar.base_name_id = abn.id
    ),
    trend_agg_report as (
        select
            far."year",
            far."month",
            far.month_name,
            sum(far.total_dispatched_trips) as total_dispatched_trips,
            sum(far.total_dispatched_shared_trips) as total_dispatched_shared_trips,
            sum(unique_dispatched_vehicles) as unique_dispatched_vehicles
        from fhv_agg_report far
        group by far."month", far.month_name, far."year"
        order by "year", "month"
    )
select * from trend_agg_report
{% endmacro %}