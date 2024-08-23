
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    ENV_SCHEMA,
    TABLE_TS_HOURLY_MONITORING,
    execute_query_and_log_performance,
    query_table,
    download_data_from_minio,
    MD_BUCKET
)
from ..ingest_time_series import ts_hourly_monitoring
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="ts_hourly_monitoring_fact_table", key_prefix=["ts_hourly_monitoring", "fact_table"],
    description="collect ts_hourly_monitoring_fact_table", compute_kind="duckdb",
    deps=[ts_hourly_monitoring.ts_hourly_monitoring]
)
def fact_table_ts_hourly_monitoring(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ENV_SCHEMA}.{TABLE_TS_HOURLY_MONITORING}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "ts_hourly_monitoring_fact_table": AssetIn(key_prefix=["ts_hourly_monitoring", "fact_table"]),
    },
    name=f"{TABLE_TS_HOURLY_MONITORING}", description="execute querying for ts_hourly_monitoring",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{ENV_SCHEMA}"], compute_kind="duckdb",
)
def preview_ts_hourly_monitoring(context,
                                 ts_hourly_monitoring_fact_table: SQL):
    
    execute_query_and_log_performance(context, ts_hourly_monitoring_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{ENV_SCHEMA}.{TABLE_TS_HOURLY_MONITORING}")


@asset(
    name=f"{TABLE_TS_HOURLY_MONITORING.lower()}",
    key_prefix=["minio", "hourly_monitoring"],
    description=f"download file from MinIO for table {TABLE_TS_HOURLY_MONITORING}",
    compute_kind="MinIO",
    deps=[preview_ts_hourly_monitoring],
)
def download_hourly_monitoring(context: AssetExecutionContext):
    download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                             object_name=TABLE_TS_HOURLY_MONITORING.lower(), context=context, raw_data=False)
