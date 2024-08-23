
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    REPORT_SCHEMA,
    TABLE_ANL_MONTHLY_REPORT,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_monthly_report
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_monthly_report_fact_table", key_prefix=["anl_monthly_report", "fact_table"],
    description="collect anl_monthly_report_fact_table", compute_kind="duckdb",
    deps=[anl_monthly_report.anl_monthly_report]
)
def fact_table_anl_monthly_report(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_MONTHLY_REPORT}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_monthly_report_fact_table": AssetIn(key_prefix=["anl_monthly_report", "fact_table"]),
    },
    name=TABLE_ANL_MONTHLY_REPORT, description="execute querying for anl_monthly_report",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[REPORT_SCHEMA], compute_kind="duckdb",
)
def preview_anl_monthly_report(context,
                               anl_monthly_report_fact_table: SQL):
    
    execute_query_and_log_performance(context, anl_monthly_report_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{REPORT_SCHEMA}.{TABLE_ANL_MONTHLY_REPORT}")
