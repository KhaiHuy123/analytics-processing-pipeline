
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_SHL_PERMIT,
    TABLE_ANL_DIM_VEHICLE_TYPE,
    TABLE_ANL_DIM_BASE,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_shl_permit
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_shl_permit_fact_table", key_prefix=["anl_shl_permit", "fact_table"],
    description="collect anl_shl_permit_fact_table", compute_kind="duckdb",
    deps=[anl_shl_permit.anl_shl_permit]
)
def fact_table_anl_shl_permit(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_PERMIT}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_shl_permit_dim_table_1", key_prefix=["anl_shl_permit", "dim_table_1"],
    description="collect anl_shl_permit_dim_table_2", compute_kind="duckdb",
    deps=[anl_shl_permit.anl_shl_permit]
)
def dim_table_1_anl_shl_permit(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_TYPE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_shl_permit_dim_table_2", key_prefix=["anl_shl_permit", "dim_table_2"],
    description="collect anl_shl_permit_dim_table_3", compute_kind="duckdb",
    deps=[anl_shl_permit.anl_shl_permit]
)
def dim_table_2_anl_shl_permit(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_shl_permit_fact_table": AssetIn(key_prefix=["anl_shl_permit", "fact_table"]),
        "anl_shl_permit_dim_table_1": AssetIn(key_prefix=["anl_shl_permit", "dim_table_1"]),
        "anl_shl_permit_dim_table_2": AssetIn(key_prefix=["anl_shl_permit", "dim_table_2"]),
    },
    name=TABLE_ANL_SHL_PERMIT, description="execute querying for anl_shl_permit",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[SERVICES_SCHEMA], compute_kind="duckdb",
)
def preview_anl_shl_permit(context,
                           anl_shl_permit_fact_table: SQL,
                           anl_shl_permit_dim_table_1: SQL,
                           anl_shl_permit_dim_table_2: SQL):

    execute_query_and_log_performance(context, anl_shl_permit_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_PERMIT}")

    execute_query_and_log_performance(context, anl_shl_permit_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_TYPE}")

    execute_query_and_log_performance(context, anl_shl_permit_dim_table_2, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}")

