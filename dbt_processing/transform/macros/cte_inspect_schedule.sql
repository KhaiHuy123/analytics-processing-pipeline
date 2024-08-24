
-- macros inspect schedule
{% macro inspect_schedule_summary() %}
with
    base_schedule as (
        select
            stii.permit_number,
            stii.schedule_date ,
            stii.schedule_time,
            b.base_number
        from {{ ref("services_shl_taxi_initial_inspect") }} stii
        join {{ ref("services_inspect_base") }} b
        on b.id = stii.base_number_id
    ),
    base_schedule_summary as (
        select
            bs.schedule_date, bs.schedule_time,
            count(bs.base_number) as num_of_bases_to_inspect
        from base_schedule bs
        group by bs.schedule_date, bs.schedule_time
        order by bs.schedule_date
    )
select * from base_schedule_summary
{% endmacro %}

