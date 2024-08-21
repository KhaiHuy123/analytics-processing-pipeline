
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    TRIPS_SCHEMA,
    TABLE_TS_FHV_TRIPS,
    ZONES_SCHEMA,
    TABLE_ANL_TAXI_ZONES,
    execute_query_and_log_performance,
    query_table,
    download_data_from_minio,
    MD_BUCKET,
    API_LIST_FHV,
    split_chunks_api_list
)
from ..ingest_time_series import ts_fhv_trips
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="ts_fhv_trips_fact_table", key_prefix=["ts_fhv_trips", "fact_table"],
    description="collect ts_fhv_trips_fact_table", compute_kind="duckdb",
    deps=[ts_fhv_trips.ts_fhv_trips]
)
def fact_table_ts_fhv_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_FHV_TRIPS}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="ts_fhv_trips_dim_table_1", key_prefix=["ts_fhv_trips", "dim_table_1"],
    description="collect ts_fhv_trips_dim_table_1", compute_kind="duckdb",
    deps=[ts_fhv_trips.ts_fhv_trips]
)
def dim_table_1_ts_fhv_trips(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "ts_fhv_trips_fact_table": AssetIn(key_prefix=["ts_fhv_trips", "fact_table"]),
        "ts_fhv_trips_dim_table_1": AssetIn(key_prefix=["ts_fhv_trips", "dim_table_1"])
    },
    name=f"{TABLE_TS_FHV_TRIPS}", description="execute querying for ts_fhv_trips",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{TRIPS_SCHEMA}"], compute_kind="duckdb",
)
def preview_ts_fhv_trips(context,
                         ts_fhv_trips_fact_table: SQL,
                         ts_fhv_trips_dim_table_1: SQL):
    
    execute_query_and_log_performance(context, ts_fhv_trips_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{TRIPS_SCHEMA}.{TABLE_TS_FHV_TRIPS}")
    
    execute_query_and_log_performance(context, ts_fhv_trips_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}")


@asset(
    name=f"{TABLE_TS_FHV_TRIPS.lower()}",
    key_prefix=["minio", "fhv_trips"],
    description=f"download file from MinIO for table {TABLE_TS_FHV_TRIPS}",
    compute_kind="MinIO",
    deps=[preview_ts_fhv_trips],
)
def download_fhv_trips(context: AssetExecutionContext):
    sub_lists = split_chunks_api_list(API_LIST_FHV, chunk_size=1)
    for idx, _ in enumerate(sub_lists):
        download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                                 object_name=TABLE_TS_FHV_TRIPS.lower(),
                                 context=context, raw_data=True, idx=idx)
