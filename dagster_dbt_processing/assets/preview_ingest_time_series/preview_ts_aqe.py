
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    ENV_SCHEMA,
    TABLE_TS_AQE,
    execute_query_and_log_performance,
    query_table,
    download_data_from_minio,
    MD_BUCKET
)
from ..ingest_time_series import ts_aqe
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="ts_aqe_fact_table", key_prefix=["ts_aqe", "fact_table"],
    description="collect ts_aqe_fact_table", compute_kind="duckdb",
    deps=[ts_aqe.ts_aqe]
)
def fact_table_ts_aqe(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ENV_SCHEMA}.{TABLE_TS_AQE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "ts_aqe_fact_table": AssetIn(key_prefix=["ts_aqe", "fact_table"]),
    },
    name=f"{TABLE_TS_AQE}", description="execute querying for ts_aqe",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{ENV_SCHEMA}"], compute_kind="duckdb",
)
def preview_ts_aqe(context,
                   ts_aqe_fact_table: SQL):
    
    execute_query_and_log_performance(context, ts_aqe_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{ENV_SCHEMA}.{TABLE_TS_AQE}")


@asset(
    name=f"{TABLE_TS_AQE.lower()}",
    key_prefix=["minio", "aqe"],
    description=f"download file from MinIO for table {TABLE_TS_AQE}",
    compute_kind="MinIO",
    deps=[preview_ts_aqe],
)
def download_ts_aqe(context: AssetExecutionContext):
    download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                             object_name=TABLE_TS_AQE.lower(), context=context, raw_data=False)
