
-- macros green_trips
{% macro green_trips (include_report_pick_up_location=True,
                      include_report_drop_up_location=True,
                      include_unpaid_money=True,
                      include_distance_moving=True,
                      include_group_additional_money=True) %}
with
	favorite_pick_up_location as (
		select
			gt.pulocationid,
			tzl.borough ,
			tzl."zone", 
			tzl.service_zone,
			count(distinct(gt.pulocationid, gt.dolocationid)) as num_of_picking_up_locations,
			count((gt.pulocationid, gt.dolocationid)) as num_of_picking_up_trips
		from {{ ref("taxicab_green_trips") }} gt
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid  = gt.pulocationid
		group by gt.pulocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
    favorite_drop_off_location as (
		select
			gt.dolocationid,
			tzl.borough ,
			tzl."zone",
			tzl.service_zone,
			count(distinct(gt.pulocationid, gt.dolocationid)) as num_of_dropping_off_locations,
			count((gt.pulocationid, gt.dolocationid)) as num_of_dropping_off_trips
		from {{ ref("taxicab_green_trips") }} gt
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid = gt.dolocationid
		group by gt.dolocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
{% if include_report_pick_up_location %}
	report_pick_up_location as (
		select
			fpl.borough, fpl."zone",
			sum(fpl.num_of_picking_up_locations) as num_of_picking_up_locations,
			sum(fpl.num_of_picking_up_trips) as num_of_picking_up_trips
		from favorite_pick_up_location fpl
		group by fpl.borough, fpl."zone"
		order by fpl.borough
	),
{% endif %}
{% if include_report_drop_up_location %}
	report_drop_up_location as (
		select
			fdl.borough, fdl."zone",
			sum(fdl.num_of_dropping_off_locations) as num_of_dropping_off_locations,
			sum(fdl.num_of_dropping_off_trips) as num_of_dropping_off_trips
		from favorite_drop_off_location fdl
		group by fdl.borough, fdl."zone"
		order by fdl.borough
	),
{% endif %}
	negative_fare as (
		select
			gt.lpep_dropoff_datetime ,
			gt.lpep_pickup_datetime ,
			gt.fare_amount ,
			pt.payment_description
		from {{ ref("taxicab_green_trips") }} gt
		join {{ ref("taxicab_payment_type") }} pt
		on gt.payment_type = pt.id
		where gt.fare_amount < 0
	)
{% if include_unpaid_money %}
	,
	unpaid_money as (
		select
			nf.lpep_dropoff_datetime,
			nf.lpep_pickup_datetime,
			(nf.lpep_dropoff_datetime - nf.lpep_pickup_datetime) as time_clock_run,
			nf.fare_amount,
			nf.payment_description
		from negative_fare nf
	)
{% endif %}
{% if include_group_additional_money %}
	,
	group_additional_money as (
		select
			count(gt.extra) as frequency,
			gt.extra,
			gt.mta_tax,
			gt.tolls_amount ,
			gt.ehail_fee ,
			gt.improvement_surcharge ,
			gt.congestion_surcharge
		from {{ ref("taxicab_green_trips") }} gt
		group by gt.mta_tax , gt.extra, gt.tolls_amount, gt.ehail_fee,
				 gt.improvement_surcharge, gt.congestion_surcharge
		order by gt.mta_tax , gt.extra, gt.tolls_amount, gt.ehail_fee,
				 gt.improvement_surcharge, gt.congestion_surcharge
	)
{% endif %}
{% if include_distance_moving %}
	,
	distance_moving as (
		select
			gt.pulocationid,
			gt.dolocationid,
			round(cast(avg(gt.trip_distance)as numeric), 2) as distance_moving_average
		from {{ ref("taxicab_green_trips") }} gt
		group by pulocationid, dolocationid
		order by pulocationid, dolocationid
	)
{% endif %}
select *
from
    {% if include_report_pick_up_location %}
        report_pick_up_location
    {% elif include_report_drop_up_location %}
        report_drop_up_location
    {% elif include_unpaid_money %}
        unpaid_money
    {% elif include_distance_moving %}
        distance_moving
    {% elif include_group_additional_money %}
        group_additional_money
    {% endif %}
{% endmacro %}
