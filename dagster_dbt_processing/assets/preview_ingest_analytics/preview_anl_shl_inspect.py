
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_SHL_INSPECT,
    TABLE_ANL_DIM_INSPECT_BASE,
    execute_query_and_log_performance,
    query_table,
)
from ..ingest_analytics import anl_shl_inspect
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_shl_inspect_fact_table", key_prefix=["anl_shl_inspect", "fact_table"],
    description="collect anl_shl_inspect_fact_table", compute_kind="duckdb",
    deps=[anl_shl_inspect.anl_shl_inspect]
)
def fact_table_anl_shl_inspect(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_INSPECT}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_shl_inspect_dim_table_1", key_prefix=["anl_shl_inspect", "dim_table_1"],
    description="collect anl_shl_inspect_dim_table_1", compute_kind="duckdb",
    deps=[anl_shl_inspect.anl_shl_inspect]
)
def dim_table_1_anl_shl_inspect(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_INSPECT_BASE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_shl_inspect_fact_table": AssetIn(key_prefix=["anl_shl_inspect", "fact_table"]),
        "anl_shl_inspect_dim_table_1": AssetIn(key_prefix=["anl_shl_inspect", "dim_table_1"])
    },
    name=f"{TABLE_ANL_SHL_INSPECT}", description="execute querying for anl_shl_inspect",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{SERVICES_SCHEMA}"], compute_kind="duckdb",
)
def preview_anl_shl_inspect(context,
                            anl_shl_inspect_fact_table: SQL,
                            anl_shl_inspect_dim_table_1: SQL):

    execute_query_and_log_performance(context, anl_shl_inspect_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_INSPECT}")

    execute_query_and_log_performance(context, anl_shl_inspect_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_INSPECT_BASE}")


