
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    REPORT_SCHEMA,
    TABLE_ANL_METER_SHOPS,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_meter_shop
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_meter_shop_fact_table", key_prefix=["anl_meter_shop", "fact_table"],
    description="collect anl_meter_shop_fact_table", compute_kind="duckdb",
    deps=[anl_meter_shop.anl_meter_shop]
)
def fact_table_anl_meter_shop(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_METER_SHOPS}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_meter_shop_fact_table": AssetIn(key_prefix=["anl_meter_shop", "fact_table"]),
    },
    name=TABLE_ANL_METER_SHOPS, description="execute querying for anl_meter_shop",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[REPORT_SCHEMA], compute_kind="duckdb",
)
def preview_anl_meter_shop(context,
                           anl_meter_shop_fact_table: SQL):
    
    execute_query_and_log_performance(context, anl_meter_shop_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{REPORT_SCHEMA}.{TABLE_ANL_METER_SHOPS}")

