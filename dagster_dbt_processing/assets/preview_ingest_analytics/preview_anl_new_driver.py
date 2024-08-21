
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    REPORT_SCHEMA,
    TABLE_ANL_NEW_DRIVERS,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_new_driver
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_new_driver_fact_table", key_prefix=["anl_new_driver", "fact_table"],
    description="collect anl_new_driver_fact_table", compute_kind="duckdb",
    deps=[anl_new_driver.anl_new_driver]
)
def fact_table_anl_new_driver(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_NEW_DRIVERS}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_new_driver_fact_table": AssetIn(key_prefix=["anl_new_driver", "fact_table"]),
    },
    name=TABLE_ANL_NEW_DRIVERS, description="execute querying for anl_new_driver",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[REPORT_SCHEMA], compute_kind="duckdb",
)
def preview_anl_new_driver(context,
                           anl_new_driver_fact_table: SQL):
    
    execute_query_and_log_performance(context, anl_new_driver_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{REPORT_SCHEMA}.{TABLE_ANL_NEW_DRIVERS}")
