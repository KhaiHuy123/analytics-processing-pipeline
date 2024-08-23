
from dagster import asset, AssetIn, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_FHV_VEHICLE,
    TABLE_ANL_DIM_BASE,
    TABLE_ANL_DIM_VEHICLE_NAME,
    TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
    execute_query_and_log_performance,
    query_table
)
from ..ingest_analytics import anl_fhv_vehicle
from ...resources.duckdb_io_manager import SQL


@asset(
    name="anl_fhv_vehicle_fact_table", key_prefix=["anl_fhv_vehicle", "fact_table"],
    description="collect anl_fhv_vehicle_fact_table", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def fact_table_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_FHV_VEHICLE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_fhv_vehicle_dim_table_1", key_prefix=["anl_fhv_vehicle", "dim_table_1"],
    description="collect anl_fhv_vehicle_dim_table_1", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def dim_table_1_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_fhv_vehicle_dim_table_2", key_prefix=["anl_fhv_vehicle", "dim_table_2"],
    description="collect anl_fhv_vehicle_dim_table_2", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def dim_table_2_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_NAME}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_fhv_vehicle_dim_table_3", key_prefix=["anl_fhv_vehicle", "dim_table_3"],
    description="collect anl_fhv_vehicle_dim_table_3", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def dim_table_3_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_WHEELCHAIR_ACCESS}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_fhv_vehicle_dim_table_4", key_prefix=["anl_fhv_vehicle", "dim_table_4"],
    description="collect anl_fhv_vehicle_dim_table_4", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def dim_table_4_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEH}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    name="anl_fhv_vehicle_dim_table_5", key_prefix=["anl_fhv_vehicle", "dim_table_5"],
    description="collect anl_fhv_vehicle_dim_table_5", compute_kind="duckdb",
    deps=[anl_fhv_vehicle.anl_fhv_vehicles]
)
def dim_table_5_anl_fhv_vehicle(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "anl_fhv_vehicle_fact_table": AssetIn(key_prefix=["anl_fhv_vehicle", "fact_table"]),
        "anl_fhv_vehicle_dim_table_1": AssetIn(key_prefix=["anl_fhv_vehicle", "dim_table_1"]),
        "anl_fhv_vehicle_dim_table_2": AssetIn(key_prefix=["anl_fhv_vehicle", "dim_table_2"]),
        "anl_fhv_vehicle_dim_table_3": AssetIn(key_prefix=["anl_fhv_vehicle", "dim_table_3"]),
        "anl_fhv_vehicle_dim_table_4": AssetIn(key_prefix=["anl_fhv_vehicle", "dim_table_4"]),
        "anl_fhv_vehicle_dim_table_5": AssetIn(key_prefix=["anl_fhv_vehicle", "dim_table_5"])
    },
    name=f"{TABLE_ANL_FHV_VEHICLE}", description="execute querying for anl_fhv_vehicle",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{SERVICES_SCHEMA}"], compute_kind="duckdb",
)
def preview_anl_fhv_vehicle(context,
                            anl_fhv_vehicle_fact_table: SQL,
                            anl_fhv_vehicle_dim_table_1: SQL,
                            anl_fhv_vehicle_dim_table_2: SQL,
                            anl_fhv_vehicle_dim_table_3: SQL,
                            anl_fhv_vehicle_dim_table_4: SQL,
                            anl_fhv_vehicle_dim_table_5: SQL):
    
    execute_query_and_log_performance(context, anl_fhv_vehicle_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_FHV_VEHICLE}")
    
    execute_query_and_log_performance(context, anl_fhv_vehicle_dim_table_1, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}")

    execute_query_and_log_performance(context, anl_fhv_vehicle_dim_table_2, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_NAME}")

    execute_query_and_log_performance(context, anl_fhv_vehicle_dim_table_3, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_WHEELCHAIR_ACCESS}")

    execute_query_and_log_performance(context, anl_fhv_vehicle_dim_table_4, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEH}")

    execute_query_and_log_performance(context, anl_fhv_vehicle_dim_table_5, context.resources.duckdb_io_manager,
                                      query_name=f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE}")

