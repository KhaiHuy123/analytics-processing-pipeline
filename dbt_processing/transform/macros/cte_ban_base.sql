
-- macros ban_base
{% macro ban_bases_summary() %}
with
    ban_base_before_2023 as (
        select
            sp.active,
            sp.suspension_reason,
            sp.certification_date,
            b.base_number,
            b.base_name
        from {{ ref("services_shl_permits") }} sp
        left join {{ ref("services_base") }} b
        on sp.base_number_id = b.id
        where sp.suspension_reason <> 'g'
        and extract(year from sp.certification_date) < 2023
        group by b.base_number, b.base_name, sp.suspension_reason, sp.active, sp.certification_date
    ),
    ban_base_after_2023 as (
        select
            sp.active,
            sp.suspension_reason,
            sp.certification_date,
            b.base_number,
            b.base_name
        from {{ ref("services_shl_permits") }} sp
        left join {{ ref("services_base") }} b
        on sp.base_number_id = b.id
        where sp.suspension_reason <> 'g'
        and extract(year from sp.certification_date) >= 2023
        group by b.base_number, b.base_name, sp.suspension_reason, sp.active, sp.certification_date
    ),
    report_ban_base_before_2023 as (
        select
            bf.base_name,
            count(bf.suspension_reason) as ban_cases_received_before_2023
        from ban_base_before_2023 bf
        group by bf.base_name
    ),
    report_ban_base_after_2023 as (
        select
          af.base_name,
          count(af.suspension_reason) as ban_cases_received_after_2023
        from ban_base_after_2023 af
        group by af.base_name
    ),
    summary_ban_base as (
        select
            coalesce(rbf.base_name, 'UNKNOWN') as base_name,
            coalesce(rbf.ban_cases_received_before_2023, 0) as ban_cases_received_before_2023,
            coalesce(raf.ban_cases_received_after_2023, 0) as ban_cases_received_after_2023
        from report_ban_base_before_2023 rbf
        full join report_ban_base_after_2023 raf
        on rbf.base_name = raf.base_name
    )
select * from summary_ban_base
{% endmacro %}
