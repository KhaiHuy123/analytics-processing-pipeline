from dagster import asset, AssetIn
from ..constant import (
    TABLE_PSQL_FHV_BASE_TYPE_CONTAINED,
    TABLE_PSQL_FHV_POPULAR_DROPOFF,
    TABLE_PSQL_FHV_POPULAR_PICKUP,
    TABLE_PSQL_FHV_TOTAL_TRIPS_PER_MONTH,
    TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR,
    TABLE_PSQL_TRIPS_TIME_MOVING,
    TABLE_PSQL_TRIPS_TIME_OPERATING,
    TABLE_PSQL_FHV_VEHICLE_TYPE,
    TABLE_PSQL_GREEN_DISTANCE_MOVING,
    TABLE_PSQL_GREEN_ADDITIONAL_MONEY,
    TABLE_PSQL_GREEN_DROPOFF_LOCATION,
    TABLE_PSQL_GREEN_PICKUP_LOCATION,
    TABLE_PSQL_GREEN_UNPAID_MONEY,
    TABLE_PSQL_YELLOW_DISTANCE_MOVING,
    TABLE_PSQL_YELLOW_ADDITIONAL_MONEY,
    TABLE_PSQL_YELLOW_POPULAR_DROPOFF,
    TABLE_PSQL_YELLOW_POPULAR_PICKUP,
    TABLE_PSQL_YELLOW_UNPAID_MONEY
)


@asset(
    name=TABLE_PSQL_FHV_BASE_TYPE_CONTAINED.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_BASE_TYPE_CONTAINED.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_BASE_TYPE_CONTAINED} ",
)
def minio_fhv_base_type_contained(fhv_base_type_contained):
    return fhv_base_type_contained


@asset(
    name=TABLE_PSQL_FHV_POPULAR_DROPOFF.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_POPULAR_DROPOFF.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_POPULAR_DROPOFF} ",
)
def minio_fhv_popular_dropoff_location(fhv_popular_dropoff_location):
    return fhv_popular_dropoff_location


@asset(
    name=TABLE_PSQL_FHV_POPULAR_PICKUP.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_POPULAR_PICKUP.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_POPULAR_PICKUP} ",
)
def minio_fhv_popular_pickup_location(fhv_popular_pickup_location):
    return fhv_popular_pickup_location


@asset(
    name=TABLE_PSQL_FHV_TOTAL_TRIPS_PER_MONTH.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_TOTAL_TRIPS_PER_MONTH.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_TOTAL_TRIPS_PER_MONTH} ",
)
def minio_fhv_total_trips_per_month(fhv_total_trips_per_month):
    return fhv_total_trips_per_month


@asset(
    name=TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR} ",
)
def minio_fhv_total_trips_per_year(fhv_total_trips_per_year):
    return fhv_total_trips_per_year


@asset(
    name=TABLE_PSQL_TRIPS_TIME_MOVING.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_TRIPS_TIME_MOVING.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_TRIPS_TIME_MOVING} ",
)
def minio_fhv_trip_time_moving(fhv_trip_time_moving):
    return fhv_trip_time_moving


@asset(
    name=TABLE_PSQL_TRIPS_TIME_OPERATING.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_TRIPS_TIME_OPERATING.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_TRIPS_TIME_OPERATING} ",
)
def minio_fhv_trip_time_operating(fhv_trip_time_operating):
    return fhv_trip_time_operating


@asset(
    name=TABLE_PSQL_FHV_VEHICLE_TYPE.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_FHV_VEHICLE_TYPE.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_FHV_VEHICLE_TYPE} "
)
def minio_fhv_vehicle_type(fhv_vehicle_type):
    return fhv_vehicle_type


@asset(
    name=TABLE_PSQL_GREEN_DISTANCE_MOVING.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_GREEN_DISTANCE_MOVING.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_GREEN_DISTANCE_MOVING} ",
)
def minio_green_distance_moving(green_distance_moving):
    return green_distance_moving


@asset(
    name=TABLE_PSQL_GREEN_ADDITIONAL_MONEY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_GREEN_ADDITIONAL_MONEY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_GREEN_ADDITIONAL_MONEY} ",
)
def minio_green_group_additional_money(green_group_additional_money):
    return green_group_additional_money


@asset(
    name=TABLE_PSQL_GREEN_DROPOFF_LOCATION.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_GREEN_DROPOFF_LOCATION.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_GREEN_DROPOFF_LOCATION} ",
)
def minio_green_popular_dropoff_location(green_popular_dropoff_location):
    return green_popular_dropoff_location


@asset(
    name=TABLE_PSQL_GREEN_PICKUP_LOCATION.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_GREEN_PICKUP_LOCATION.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_GREEN_PICKUP_LOCATION} ",
)
def minio_green_popular_pickup_location(green_popular_pickup_location):
    return green_popular_pickup_location


@asset(
    name=TABLE_PSQL_GREEN_UNPAID_MONEY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_GREEN_UNPAID_MONEY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_GREEN_UNPAID_MONEY} ",
)
def minio_green_unpaid_money(green_unpaid_money):
    return green_unpaid_money


@asset(
    name=TABLE_PSQL_YELLOW_DISTANCE_MOVING.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_YELLOW_DISTANCE_MOVING.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_YELLOW_DISTANCE_MOVING} ",
)
def minio_yellow_distance_moving(yellow_distance_moving):
    return yellow_distance_moving


@asset(
    name=TABLE_PSQL_YELLOW_ADDITIONAL_MONEY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_YELLOW_ADDITIONAL_MONEY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_YELLOW_ADDITIONAL_MONEY} ",
)
def minio_yellow_group_additional_money(yellow_group_additional_money):
    return yellow_group_additional_money


@asset(
    name=TABLE_PSQL_YELLOW_POPULAR_DROPOFF.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_YELLOW_POPULAR_DROPOFF.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_YELLOW_POPULAR_DROPOFF} ",
)
def minio_yellow_popular_dropoff_location(yellow_popular_dropoff_location):
    return yellow_popular_dropoff_location


@asset(
    name=TABLE_PSQL_YELLOW_POPULAR_PICKUP.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_YELLOW_POPULAR_PICKUP.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_YELLOW_POPULAR_PICKUP} ",
)
def minio_yellow_popular_pickup_location(yellow_popular_pickup_location):
    return yellow_popular_pickup_location


@asset(
    name=TABLE_PSQL_YELLOW_UNPAID_MONEY.lower(),
    key_prefix=["minio", "result"],
    ins={f"{TABLE_PSQL_YELLOW_UNPAID_MONEY.lower()}": AssetIn(key_prefix=["result"])},
    compute_kind='MinIO',
    description=f"Download file from MinIO for table{TABLE_PSQL_YELLOW_UNPAID_MONEY} ",
)
def minio_yellow_unpaid_money(yellow_unpaid_money):
    return yellow_unpaid_money
