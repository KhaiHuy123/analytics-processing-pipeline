
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    TRIPS_SCHEMA,
    TABLE_TS_GREEN_TRIPS,
    execute_query_and_log_performance,
    ZONES_SCHEMA,
    TABLE_ANL_TAXI_ZONES,
    query_table,
    download_data_from_minio,
    MD_BUCKET,
    TABLE_TS_DIM_VENDOR,
    TABLE_TS_DIM_TRIP_TYPE,
    TABLE_TS_DIM_RATECODE,
    TABLE_TS_DIM_PAYMENT_TYPE,
    API_LIST_GREEN,
    split_chunks_api_list
)
from ..ingest_time_series import ts_green_trips
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="ts_green_trips_fact_table", key_prefix=["ts_green_trips", "fact_table"],
    description="collect ts_green_trips_fact_table", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def fact_table_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_GREEN_TRIPS}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_green_trips_dim_table_1", key_prefix=["ts_green_trips", "dim_table_1"],
    description="collect ts_green_trips_dim_table_1", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def dim_table_1_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_green_trips_dim_table_2", key_prefix=["ts_green_trips", "dim_table_2"],
    description="collect ts_green_trips_dim_table_2", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def dim_table_2_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_PAYMENT_TYPE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_green_trips_dim_table_3", key_prefix=["ts_green_trips", "dim_table_3"],
    description="collect ts_green_trips_dim_table_3", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def dim_table_3_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_RATECODE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_green_trips_dim_table_4", key_prefix=["ts_green_trips", "dim_table_4"],
    description="collect ts_green_trips_dim_table_4", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def dim_table_4_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_TRIP_TYPE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_green_trips_dim_table_5", key_prefix=["ts_green_trips", "dim_table_5"],
    description="collect ts_green_trips_dim_table_5", compute_kind="duckdb",
    deps=[ts_green_trips.ts_green_trips]
)
def dim_table_5_ts_green_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_VENDOR}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "ts_green_trips_fact_table": AssetIn(key_prefix=["ts_green_trips", "fact_table"]),
        "ts_green_trips_dim_table_1": AssetIn(key_prefix=["ts_green_trips", "dim_table_1"]),
        "ts_green_trips_dim_table_2": AssetIn(key_prefix=["ts_green_trips", "dim_table_2"]),
        "ts_green_trips_dim_table_3": AssetIn(key_prefix=["ts_green_trips", "dim_table_3"]),
        "ts_green_trips_dim_table_4": AssetIn(key_prefix=["ts_green_trips", "dim_table_4"]),
        "ts_green_trips_dim_table_5": AssetIn(key_prefix=["ts_green_trips", "dim_table_5"]),
    },
    name=f"{TABLE_TS_GREEN_TRIPS}", description="execute querying for ts_green_trips",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{TRIPS_SCHEMA}"], compute_kind="duckdb",
)
def preview_ts_green_trips(context,
                           ts_green_trips_fact_table: SQL,
                           ts_green_trips_dim_table_1: SQL,
                           ts_green_trips_dim_table_2: SQL,
                           ts_green_trips_dim_table_3: SQL,
                           ts_green_trips_dim_table_4: SQL,
                           ts_green_trips_dim_table_5: SQL):

    execute_query_and_log_performance(context, ts_green_trips_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_GREEN_TRIPS}")

    execute_query_and_log_performance(context, ts_green_trips_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}")

    execute_query_and_log_performance(context, ts_green_trips_dim_table_2, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_PAYMENT_TYPE}")

    execute_query_and_log_performance(context, ts_green_trips_dim_table_3, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_RATECODE}")

    execute_query_and_log_performance(context, ts_green_trips_dim_table_4, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_TRIP_TYPE}")

    execute_query_and_log_performance(context, ts_green_trips_dim_table_5, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_VENDOR}")


@asset(
    name=f"{TABLE_TS_GREEN_TRIPS.lower()}",
    key_prefix=["minio", "green_trips"],
    description=f"download file from MinIO for table {TABLE_TS_GREEN_TRIPS}",
    compute_kind="MinIO",
    deps=[preview_ts_green_trips],
)
def download_green_trips(context: AssetExecutionContext):
    sub_lists = split_chunks_api_list(API_LIST_GREEN, chunk_size=1)
    for idx, _ in enumerate(sub_lists):
        download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                                 object_name=TABLE_TS_GREEN_TRIPS.lower(),
                                 context=context, raw_data=True, idx=idx)
