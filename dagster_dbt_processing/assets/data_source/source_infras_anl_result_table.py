
from dagster import asset, AssetIn
from ..constant import (
    TABLE_ANL_METER_SHOPS,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_TAXI_ZONES,
    TABLE_ANL_NEW_DRIVERS,
    TABLE_PSQL_AGG_REPORT_BAN_BASES,
    TABLE_PSQL_AGG_REPORT_BAN_CASES,
    TABLE_PSQL_AGG_REPORT_BAN_VEHICLES,
    TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE,
    TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE,
    TABLE_PSQL_REPORTED_BASE,
    TABLE_PSQL_REPORTED_VEHICLES,
    TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS,
    TABLE_PSQL_VERSION_OF_VEHICLES,
    TABLE_PSQL_AVAILABLE_VEHICLES_BLACK,
    TABLE_PSQL_AVAILABLE_VEHICLES_GREEN,
    TABLE_PSQL_AVAILABLE_VEHICLES_YELLOW,
    TABLE_PSQL_AVAILABLE_VEHICLES_HIGH_VOLUME,
    TABLE_PSQL_AVAILABLE_VEHICLES_LIMO,
    TABLE_PSQL_AVAILABLE_VEHICLES_LIVERY,
    TABLE_PSQL_FAREBOX_PER_TRIPS_BLACK,
    TABLE_PSQL_FAREBOX_PER_TRIPS_GREEN,
    TABLE_PSQL_FAREBOX_PER_TRIPS_YELLOW,
    TABLE_PSQL_FAREBOX_PER_TRIPS_HIGH_VOLUME,
    TABLE_PSQL_FAREBOX_PER_TRIPS_LIMO,
    TABLE_PSQL_FAREBOX_PER_TRIPS_LIVERY,
)


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_LIVERY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_LIVERY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_LIVERY} ",
)
def minio_farebox_per_trips_livery(farebox_per_trips_livery):
    return farebox_per_trips_livery


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_LIMO.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_LIMO.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_LIMO} ",
)
def minio_farebox_per_trips_limo(farebox_per_trips_limo):
    return farebox_per_trips_limo


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_HIGH_VOLUME.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_HIGH_VOLUME.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_HIGH_VOLUME} ",
)
def minio_farebox_per_trips_high_volume(farebox_per_trips_high_volume):
    return farebox_per_trips_high_volume


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_YELLOW.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_YELLOW.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_YELLOW} ",
)
def minio_farebox_per_trips_yellow(farebox_per_trips_yellow):
    return farebox_per_trips_yellow


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_GREEN.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_GREEN.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_GREEN} ",
)
def minio_farebox_per_trips_green(farebox_per_trips_green):
    return farebox_per_trips_green


@asset(
    name=TABLE_PSQL_FAREBOX_PER_TRIPS_BLACK.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FAREBOX_PER_TRIPS_BLACK.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FAREBOX_PER_TRIPS_BLACK} ",
)
def minio_farebox_per_trips_black(farebox_per_trips_black):
    return farebox_per_trips_black


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_LIVERY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_LIVERY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_LIVERY} ",
)
def minio_available_vehicles_livery(available_vehicles_livery):
    return available_vehicles_livery


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_LIMO.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_LIMO.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_LIMO} "
)
def minio_available_vehicles_limo(available_vehicles_limo):
    return available_vehicles_limo


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_HIGH_VOLUME.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_HIGH_VOLUME.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_HIGH_VOLUME} ",
)
def minio_available_vehicles_high_volume(available_vehicles_high_volume):
    return available_vehicles_high_volume


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_YELLOW.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_YELLOW.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_YELLOW} ",
)
def minio_available_vehicles_yellow(available_vehicles_yellow):
    return available_vehicles_yellow


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_GREEN.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_GREEN.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_GREEN} ",
)
def minio_available_vehicles_green(available_vehicles_green):
    return available_vehicles_green


@asset(
    name=TABLE_PSQL_AVAILABLE_VEHICLES_BLACK.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AVAILABLE_VEHICLES_BLACK.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AVAILABLE_VEHICLES_BLACK} ",
)
def minio_available_vehicles_black(available_vehicles_black):
    return available_vehicles_black


@asset(
    name=TABLE_PSQL_AGG_REPORT_BAN_BASES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AGG_REPORT_BAN_BASES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AGG_REPORT_BAN_BASES} ",
)
def minio_agg_report_ban_bases(agg_report_ban_bases):
    return agg_report_ban_bases


@asset(
    name=TABLE_PSQL_AGG_REPORT_BAN_CASES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AGG_REPORT_BAN_CASES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AGG_REPORT_BAN_CASES} ",
)
def minio_agg_report_ban_cases(agg_report_ban_cases):
    return agg_report_ban_cases


@asset(
    name=TABLE_PSQL_AGG_REPORT_BAN_VEHICLES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AGG_REPORT_BAN_VEHICLES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AGG_REPORT_BAN_VEHICLES} ",
)
def minio_agg_report_ban_vehicles(agg_report_ban_vehicles):
    return agg_report_ban_vehicles


@asset(
    name=TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE} ",
)
def minio_agg_report_unique_vehicles(agg_report_unique_vehicle):
    return agg_report_unique_vehicle


@asset(
    name=TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE} ",
)
def minio_agg_shl_inspect_schedule(agg_report_shl_inspect_schedule):
    return agg_report_shl_inspect_schedule


@asset(
    name=TABLE_ANL_METER_SHOPS.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_ANL_METER_SHOPS.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_ANL_METER_SHOPS} ",
)
def minio_dim_report_monthly(shl_meter_shops):
    return shl_meter_shops


@asset(
    name=TABLE_PSQL_REPORTED_BASE.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_REPORTED_BASE.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_REPORTED_BASE} ",
)
def minio_agg_reported_base(reported_base):
    return reported_base


@asset(
    name=TABLE_PSQL_REPORTED_VEHICLES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_REPORTED_VEHICLES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_REPORTED_VEHICLES} ",
)
def minio_dim_reported_vehicles(reported_vehicles):
    return reported_vehicles


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_LICENSE.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_ANL_DIM_AGG_BASE_LICENSE.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_ANL_DIM_AGG_BASE_LICENSE} ",
)
def minio_dim_services_agg_base_license(agg_base_license):
    return agg_base_license


@asset(
    name=TABLE_ANL_DIM_VEH.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_ANL_DIM_VEH.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_ANL_DIM_VEH} ",
)
def minio_services_veh(veh):
    return veh


@asset(
    name=TABLE_ANL_TAXI_ZONES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_ANL_TAXI_ZONES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_ANL_TAXI_ZONES} ",
)
def minio_taxi_zones_lookup(taxi_zone_lookup):
    return taxi_zone_lookup


@asset(
    name=TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS} ",
)
def minio_trend_of_dispatched_trips(trend_of_dispatched_trips):
    return trend_of_dispatched_trips


@asset(
    name=TABLE_ANL_NEW_DRIVERS.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_ANL_NEW_DRIVERS.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_ANL_NEW_DRIVERS} ",
)
def minio_trend_of_new_driver(tlc_new_driver_application_stat):
    return tlc_new_driver_application_stat


@asset(
    name=TABLE_PSQL_VERSION_OF_VEHICLES.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_VERSION_OF_VEHICLES.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_VERSION_OF_VEHICLES} ",
)
def minio_version_of_vehicles(version_of_vehicles):
    return version_of_vehicles
    