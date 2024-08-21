
-- macros reported base
{% macro reported_base() %}
with
    reported_base as (
        select
            lpc.license_number,
            lpc."name" ,
            lprb.base_name,
            lprb.base_license_number ,
            lprb.base_license_number,
            lprb.base_phone_number,
            lprb.base_address,
            lprb.base_website
        from {{ ref("services_lost_property_contact") }} lpc
        left join {{ ref("services_lost_property_reported_base") }} lprb
        on lpc.base_id = lprb.id
    ),
    reported_base_summary as (
        select
            rb.base_name,
            count(rb.base_name) as reported_cases
        from reported_base rb
        group by rb.base_name
    )
select * from reported_base_summary
{% endmacro %}
