
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_LOST_PROPERTY,
    TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_lost_property
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_lost_property_fact_table", key_prefix=["anl_lost_property", "fact_table"],
    description="collect anl_lost_property_fact_table", compute_kind="duckdb",
    deps=[anl_lost_property.anl_lost_property]
)
def fact_table_anl_lost_property(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_LOST_PROPERTY}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_lost_property_dim_table_1", key_prefix=["anl_lost_property", "dim_table_1"],
    description="collect anl_lost_property_dim_table_1", compute_kind="duckdb",
    deps=[anl_lost_property.anl_lost_property]
)
def dim_table_1_anl_lost_property(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_lost_property_dim_table_2", key_prefix=["anl_lost_property", "dim_table_2"],
    description="collect anl_lost_property_dim_table_2", compute_kind="duckdb",
    deps=[anl_lost_property.anl_lost_property]
)
def dim_table_2_anl_lost_property(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_lost_property_fact_table": AssetIn(key_prefix=["anl_lost_property", "fact_table"]),
        "anl_lost_property_dim_table_1": AssetIn(key_prefix=["anl_lost_property", "dim_table_1"]),
        "anl_lost_property_dim_table_2": AssetIn(key_prefix=["anl_lost_property", "dim_table_2"])
    },
    name=TABLE_ANL_LOST_PROPERTY, description="execute querying for anl_lost_property",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[SERVICES_SCHEMA], compute_kind="duckdb",
)
def preview_anl_lost_property(context,
                              anl_lost_property_fact_table: SQL,
                              anl_lost_property_dim_table_1: SQL,
                              anl_lost_property_dim_table_2: SQL):
    
    execute_query_and_log_performance(context, anl_lost_property_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_LOST_PROPERTY}")
    
    execute_query_and_log_performance(context, anl_lost_property_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE}")

    execute_query_and_log_performance(context, anl_lost_property_dim_table_2, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES}")

