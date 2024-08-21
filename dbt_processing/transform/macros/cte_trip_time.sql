
-- macros trip time
{% macro trip_time_summary (include_time_moving=True, include_time_operating=True) %}
with
	trip_time_location_cte as (
		select
			ft.dispatching_base_num ,
			ft.affiliated_base_number,
			(ft.dropoff_datetime - ft.pickup_datetime) as time_serving,
			ft.dropoff_datetime,
			ft.pickup_datetime,
			ft.pulocationid,
			ft.dolocationid
		from {{ ref("taxicab_fhv_trips") }} ft
	),
	time_moving_cte as (
		select
			ttl.pulocationid,
			ttl.dolocationid,
			min(ttl.time_serving) as fatest_serving ,
			max(ttl.time_serving) as longest_serving,
			extract(minute from avg(ttl.time_serving::time)) as avg_time_serving
		from trip_time_location_cte as ttl
		group by ttl.pulocationid, ttl.dolocationid
		order by ttl.pulocationid, ttl.dolocationid
	),
	time_operating_cte as (
		select
			ttl.dispatching_base_num,
			ttl.affiliated_base_number,
			min(ttl.time_serving) as fatest_serving ,
			max(ttl.time_serving) as longest_serving,
			avg(ttl.time_serving::time) as avg_time_serving
		from trip_time_location_cte as ttl
		group by ttl.dispatching_base_num, ttl.affiliated_base_number
		order by ttl.dispatching_base_num, ttl.affiliated_base_number
	)
select *
from
    {% if include_time_moving %}
        time_moving_cte
    {% elif include_time_operating %}
        time_operating_cte
    {% endif %}
{% endmacro %}
