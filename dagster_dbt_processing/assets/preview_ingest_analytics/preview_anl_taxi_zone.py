
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    ZONES_SCHEMA,
    TABLE_ANL_TAXI_ZONES,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_taxi_zone
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_taxi_zones_fact_table", key_prefix=["anl_tazi_zones", "fact_table"],
    description="collect anl_taxi_zones_fact_table", compute_kind="duckdb",
    deps=[anl_taxi_zone.anl_taxi_zone]
)
def fact_table_anl_taxi_zones(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_taxi_zones_fact_table": AssetIn(key_prefix=["anl_tazi_zones", "fact_table"]),
    },
    name=TABLE_ANL_TAXI_ZONES, description="execute querying for anl_taxi_zones",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[ZONES_SCHEMA], compute_kind="duckdb",
)
def preview_anl_taxi_zones(context,
                           anl_taxi_zones_fact_table: SQL):
    
    execute_query_and_log_performance(context, anl_taxi_zones_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}")

