
-- macros ban_case
{% macro ban_cases_summary() %}
with
    ban_reason_after_2023 as (
        select
            sp.suspension_reason as reason_after_2023,
            count(sp.suspension_reason) as ban_cases_after_2023
        from {{ ref("services_shl_permits") }} sp
        where sp.active <> 'yes'
        and extract(year from sp.certification_date) >= 2023
        group by sp.suspension_reason
    ),
    ban_reason_before_2023 as (
        select
            sp.suspension_reason as reason_before_2023,
            count(sp.suspension_reason) as ban_cases_before_2023
        from {{ ref("services_shl_permits") }} sp
        where sp.active <> 'yes'
        and extract(year from sp.certification_date) < 2023
        group by sp.suspension_reason
    ),
    summary_ban_reason as (
        select
            coalesce(be.reason_before_2023, 'UNKNOWN') as reason_banned_before_2023,
            coalesce(be.ban_cases_before_2023, 0) as cases_applied_before_2023,
            coalesce(ae.reason_after_2023, 'UNKNOWN') as reason_banned_after_2023,
            coalesce(ae.ban_cases_after_2023, 0) as cases_applied_after_2023
        from ban_reason_after_2023 ae
        full join ban_reason_before_2023 be
        on ae.reason_after_2023 = be.reason_before_2023
    )
select * from summary_ban_reason
{% endmacro %}
