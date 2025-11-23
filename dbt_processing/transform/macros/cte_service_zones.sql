
-- macros service zone pick up/drop off
{% macro service_zone_summary() %}
with
	journey_start_point_cte as (
		select
			distinct (cast(ft.pulocationid as integer),
			cast(ft.dolocationid as integer)) as start_end_location_id,
			tzl.borough as picked_borough ,
			tzl.zone as picked_zone,
			tzl.service_zone as picked_service_zone,
			tzl.locationid as picked_location_id
		from {{ ref("taxicab_fhv_trips") }} ft
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid  = ft.pulocationid
		order by start_end_location_id
	),
	journey_end_point_cte as (
		select
			distinct (cast(ft.pulocationid as integer), cast(ft.dolocationid as integer)) as start_end_location_id,
			tzl.borough as dropped_borough , tzl.zone as dropped_zone,
			tzl.service_zone as dropped_service_zone,
			tzl.locationid as dropped_location_id
		from {{ ref("taxicab_fhv_trips") }} ft
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid  = ft.dolocationid
		order by start_end_location_id desc
	),
	summary_start_end_point_cte as (
		select
			jsp.picked_location_id ,
			jep.dropped_location_id,
			jsp.picked_borough ,
			jep.dropped_borough,
			jsp.picked_zone ,
			jep.dropped_zone,
			jsp.picked_service_zone,
			jep.dropped_service_zone
		from journey_start_point_cte jsp
		full join journey_end_point_cte jep
		on jsp.start_end_location_id = jep.start_end_location_id
	)
select * from summary_start_end_point_cte
{% endmacro %}
