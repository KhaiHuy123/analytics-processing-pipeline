
-- macros yellow_trips
{% macro yellow_trips (include_report_pick_up_location=True,
                       include_report_drop_up_location=True,
                       include_unpaid_money=True,
                       include_distance_moving=True,
                       include_group_additional_money=True) %}
with
	favorite_pick_up_location as (
		select
			yt.pulocationid, tzl.borough , tzl."zone", tzl.service_zone,
			count(distinct(yt.pulocationid, yt.dolocationid)) as number_of_picking_up_locations,
			count((yt.pulocationid, yt.dolocationid)) as number_of_picking_up_trips
		from {{ ref("taxicab_yellow_trips") }} yt
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid  = yt.pulocationid
		group by yt.pulocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
	favorite_drop_off_location as (
		select
			yt.dolocationid, tzl.borough , tzl."zone", tzl.service_zone,
			count(distinct(yt.pulocationid, yt.dolocationid)) as number_of_dropping_off_locations,
			count((yt.pulocationid, yt.dolocationid)) as number_of_dropping_off_trips
		from {{ ref("taxicab_yellow_trips") }} yt
		join {{ ref("taxi_zone_lookup") }} tzl
		on tzl.locationid = yt.dolocationid
		group by yt.dolocationid, tzl.borough, tzl."zone", tzl.service_zone
	),
{% if include_report_pick_up_location %}
	report_pick_up_location as (
		select
			fpl.borough, fpl."zone",
			sum(fpl.number_of_picking_up_locations) as num_of_picking_up_locations,
			sum(fpl.number_of_picking_up_trips) as num_of_picking_up_trips
		from favorite_pick_up_location fpl
		group by fpl.borough, fpl."zone"
		order by fpl.borough
	),
{% endif %}
{% if include_report_drop_up_location %}
	report_drop_up_location as (
		select
			fdl.borough, fdl."zone",
			sum(fdl.number_of_dropping_off_locations) as num_of_dropping_off_locations,
			sum(fdl.number_of_dropping_off_trips) as num_of_dropping_off_trips
		from favorite_drop_off_location fdl
		group by fdl.borough, fdl."zone"
		order by fdl.borough
	),
{% endif %}
	negative_fare as (
		select
			yt.tpep_dropoff_datetime , yt.tpep_pickup_datetime ,
			yt.fare_amount , pt.payment_description
		from {{ ref("taxicab_yellow_trips") }} yt
		join {{ ref("taxicab_payment_type") }} pt
		on yt.payment_type = pt.id
		where yt.fare_amount < 0
	)
{% if include_unpaid_money %}
	,
	unpaid_money as (
		select
			nf.tpep_dropoff_datetime,
			nf.tpep_pickup_datetime,
			(nf.tpep_pickup_datetime - nf.tpep_dropoff_datetime) as time_clock_run,
			nf.fare_amount, nf.payment_description
		from negative_fare nf
	)
{% endif %}
{% if include_group_additional_money %}
	,
	group_additional_money as (
		select
			count(yt.extra) as frequency,
			yt.extra,
			yt.mta_tax,
			yt.tolls_amount ,
			yt.congestion_surcharge ,
			yt.improvement_surcharge ,
			yt.airport_fee
		from {{ ref("taxicab_yellow_trips") }} yt
		group by yt.mta_tax , yt.extra, yt.tolls_amount, yt.congestion_surcharge ,
				 yt.improvement_surcharge, yt.airport_fee
		order by yt.mta_tax , yt.extra, yt.tolls_amount, yt.congestion_surcharge ,
				 yt.improvement_surcharge, yt.airport_fee
	)
{% endif %}
{% if include_distance_moving %}
	,
	distance_moving as (
		select
			yt.pulocationid, yt.dolocationid,
			round(cast(avg(yt.trip_distance)as numeric), 2) as distance_moving_average
		from {{ ref("taxicab_yellow_trips") }} yt
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
