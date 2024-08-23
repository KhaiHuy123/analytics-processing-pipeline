
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_BASE_REPORT,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_AGG_BASE_NAME,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_base_report
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_base_report_fact_table", key_prefix=["anl_base_report", "fact_table"],
    description="collect anl_base_report_fact_table", compute_kind="duckdb",
    deps=[anl_base_report.anl_base_report]
)
def fact_table_anl_base_report(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_BASE_REPORT}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_base_report_dim_table_1", key_prefix=["anl_base_report", "dim_table_1"],
    description="collect anl_base_report_dim_table_1", compute_kind="duckdb",
    deps=[anl_base_report.anl_base_report]
)
def dim_table_1_anl_base_report(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_base_report_dim_table_2", key_prefix=["anl_base_report", "dim_table_2"],
    description="collect anl_base_report_dim_table_2", compute_kind="duckdb",
    deps=[anl_base_report.anl_base_report]
)
def dim_table_2_anl_base_report(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_NAME}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_base_report_fact_table": AssetIn(key_prefix=["anl_base_report", "fact_table"]),
        "anl_base_report_dim_table_1": AssetIn(key_prefix=["anl_base_report", "dim_table_1"]),
        "anl_base_report_dim_table_2": AssetIn(key_prefix=["anl_base_report", "dim_table_2"])
    },
    name=TABLE_ANL_BASE_REPORT, description="execute querying for anl_base_report",
    required_resource_keys={"duckdb_io_manager"},  key_prefix=[SERVICES_SCHEMA], compute_kind="duckdb",
)
def preview_anl_base_report(context,
                            anl_base_report_fact_table: SQL,
                            anl_base_report_dim_table_1: SQL,
                            anl_base_report_dim_table_2: SQL):
    
    execute_query_and_log_performance(context, anl_base_report_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_BASE_REPORT}")
    
    execute_query_and_log_performance(context, anl_base_report_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE}")

    execute_query_and_log_performance(context, anl_base_report_dim_table_2, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_NAME}")
