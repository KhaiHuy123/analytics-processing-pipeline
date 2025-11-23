
from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource, get_asset_key_for_model
from dagster import file_relative_path

DBT_MANIFEST_PATH = file_relative_path(__file__, "../../../dbt_processing/transform/target/manifest.json")


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    name="dbt_asset",
)
def dbt_asset(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_run_invocation = dbt.cli(["build", "--select"], context=context)
    yield from dbt_run_invocation.stream()


# source report
dbt_report_meter_shops_key = get_asset_key_for_model([dbt_asset], "report_shl_meter_shops")
dbt_report_new_driver_application_key = get_asset_key_for_model([dbt_asset],
                                                                "report_tlc_new_driver_application_stat")
# source services
dbt_services_agg_base_license_key = get_asset_key_for_model([dbt_asset], "services_agg_base_license")
dbt_services_agg_base_name_key = get_asset_key_for_model([dbt_asset], "services_agg_base_name")
dbt_services_base_key = get_asset_key_for_model([dbt_asset], "services_base")
dbt_services_fhv_agg_base_report_key = get_asset_key_for_model([dbt_asset], "services_fhv_base_aggregate_report")
dbt_services_fhv_vehicles_key = get_asset_key_for_model([dbt_asset], "services_fhv_vehicles")
dbt_services_inspect_base_key = get_asset_key_for_model([dbt_asset], "services_inspect_base")
dbt_services_lost_property_contact_key = get_asset_key_for_model([dbt_asset], "services_lost_property_contact")
dbt_services_lost_property_reported_base_key = get_asset_key_for_model([dbt_asset],
                                                                       "services_lost_property_reported_base")
dbt_services_lost_property_vehicles_key = get_asset_key_for_model([dbt_asset], "services_lost_property_vehicles")
dbt_services_shl_permits_key = get_asset_key_for_model([dbt_asset], "services_shl_permits")
dbt_services_shl_taxi_initial_inspect_key = get_asset_key_for_model([dbt_asset], "services_shl_taxi_initial_inspect")
dbt_services_veh_key = get_asset_key_for_model([dbt_asset], "services_veh")
dbt_services_vehicle_name_key = get_asset_key_for_model([dbt_asset], "services_vehicle_name")
dbt_services_vehicle_type_key = get_asset_key_for_model([dbt_asset], "services_vehicle_type")
dbt_services_wheelchair_access_key = get_asset_key_for_model([dbt_asset], "services_wheelchair_access")

# source zones
dbt_zones_taxi_zone_lookup_key = get_asset_key_for_model([dbt_asset], "taxi_zone_lookup")

# metrics agg_report
dbt_agg_report_unique_vehicles_key = get_asset_key_for_model([dbt_asset], "agg_report_unique_vehicle")
dbt_agg_report_ban_bases_key = get_asset_key_for_model([dbt_asset], "agg_report_ban_bases")
dbt_agg_report_ban_cases_key = get_asset_key_for_model([dbt_asset], "agg_report_ban_cases")
dbt_agg_report_ban_vehicles_key = get_asset_key_for_model([dbt_asset], "agg_report_ban_vehicles")
dbt_agg_report_shl_inspect_schedule_key = get_asset_key_for_model([dbt_asset], "agg_report_shl_inspect_schedule")

# metrics reported_information
dbt_reported_base_key = get_asset_key_for_model([dbt_asset], "reported_base")
dbt_reported_vehicles_key = get_asset_key_for_model([dbt_asset], "reported_vehicles")

# metrics trend
dbt_trend_of_dispatched_trips_key = get_asset_key_for_model([dbt_asset], "trend_of_dispatched_trips")

# metrics vehicle_version
dbt_version_of_vehicles_key = get_asset_key_for_model([dbt_asset], "version_of_vehicles")

# metrics monthly_report
dbt_available_vehicles_high_volume_key = get_asset_key_for_model([dbt_asset], "available_vehicles_high_volume")
dbt_available_vehicles_black_key = get_asset_key_for_model([dbt_asset], "available_vehicles_black")
dbt_available_vehicles_green_key = get_asset_key_for_model([dbt_asset], "available_vehicles_green")
dbt_available_vehicles_livery_key = get_asset_key_for_model([dbt_asset], "available_vehicles_livery")
dbt_available_vehicles_limo_key = get_asset_key_for_model([dbt_asset], "available_vehicles_limo")
dbt_available_vehicles_yellow_key = get_asset_key_for_model([dbt_asset], "available_vehicles_yellow")
dbt_farebox_per_trips_black_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_black")
dbt_farebox_per_trips_green_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_green")
dbt_farebox_per_trips_high_volume_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_high_volume")
dbt_farebox_per_trips_limo_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_limo")
dbt_farebox_per_trips_livery_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_livery")
dbt_farebox_per_trips_yellow_key = get_asset_key_for_model([dbt_asset], "farebox_per_trips_yellow")

# metrics trips
dbt_fhv_base_type_contained = get_asset_key_for_model([dbt_asset], "fhv_base_type_contained")
dbt_fhv_popular_dropoff_location = get_asset_key_for_model([dbt_asset], "fhv_popular_dropoff_location")
dbt_fhv_popular_pickup_location = get_asset_key_for_model([dbt_asset], "fhv_popular_pickup_location")
dbt_fhv_total_trips_per_month = get_asset_key_for_model([dbt_asset], "fhv_total_trips_per_month")
dbt_fhv_total_trips_per_year = get_asset_key_for_model([dbt_asset], "fhv_total_trips_per_year")
dbt_fhv_trip_time_moving = get_asset_key_for_model([dbt_asset], "fhv_trip_time_moving")
dbt_fhv_trip_time_operating = get_asset_key_for_model([dbt_asset], "fhv_trip_time_operating")
dbt_fhv_vehicle_type = get_asset_key_for_model([dbt_asset], "fhv_vehicle_type")

dbt_green_distance_moving = get_asset_key_for_model([dbt_asset], "green_distance_moving")
dbt_green_group_additional_money = get_asset_key_for_model([dbt_asset], "green_group_additional_money")
dbt_green_popular_dropoff_location = get_asset_key_for_model([dbt_asset], "green_popular_dropoff_location")
dbt_green_popular_pickup_location = get_asset_key_for_model([dbt_asset], "green_popular_pickup_location")
dbt_green_unpaid_money = get_asset_key_for_model([dbt_asset], "green_unpaid_money")

dbt_yellow_distance_moving = get_asset_key_for_model([dbt_asset], "yellow_distance_moving")
dbt_yellow_group_additional_money = get_asset_key_for_model([dbt_asset], "yellow_group_additional_money")
dbt_yellow_popular_dropoff_location = get_asset_key_for_model([dbt_asset], "yellow_popular_dropoff_location")
dbt_yellow_popular_pickup_location = get_asset_key_for_model([dbt_asset], "yellow_popular_pickup_location")
dbt_yellow_unpaid_money = get_asset_key_for_model([dbt_asset], "yellow_unpaid_money")