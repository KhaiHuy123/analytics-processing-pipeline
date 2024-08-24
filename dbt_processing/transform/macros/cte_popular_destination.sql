
-- macros popular destination
{% macro popular_destination_summary (include_favorite_pick_up_location=True,
                                      include_favorite_drop_off_location=True) %}
with
	favorite_pick_up_location_cte as (
		select
			ft.pulocationid,
			tzl.borough ,
			tzl."zone",
			tzl.service_zone,
			count(distinct(ft.pulocationid, ft.dolocationid)) as number_of_picking_up
		from {{ ref("taxicab_fhv_trips") }} ft
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid  = ft.pulocationid
		group by ft.pulocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
	favorite_drop_off_location_cte as (
		select
			ft.dolocationid,
			tzl.borough ,
			tzl."zone",
			tzl.service_zone,
			count(distinct(ft.pulocationid, ft.dolocationid)) as number_of_dropping_off
		from {{ ref("taxicab_fhv_trips") }} ft
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid = ft.dolocationid
		group by ft.dolocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
	report_pick_up_location_cte as (
		select
			fpl.borough,
			fpl."zone",
			sum(fpl.number_of_picking_up) as num_of_picking_up
		from favorite_pick_up_location_cte fpl
		group by fpl.borough, fpl."zone"
		order by fpl.borough
	),
	report_drop_off_location_cte as (
		select
			fdl.borough,
			fdl."zone",
			sum(fdl.number_of_dropping_off) as num_of_dropping_off
		from favorite_drop_off_location_cte fdl
		group by fdl.borough, fdl."zone"
		order by fdl.borough
	)
select *
from
    {% if include_favorite_pick_up_location %}
        report_pick_up_location_cte
    {% elif include_favorite_drop_off_location %}
        report_drop_off_location_cte
    {% endif %}
{% endmacro %}
