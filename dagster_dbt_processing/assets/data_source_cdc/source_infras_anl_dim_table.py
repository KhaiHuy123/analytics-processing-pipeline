
from dagster import asset, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_DIM_AGG_BASE_NAME,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_VEHICLE_NAME,
    TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
    TABLE_ANL_DIM_BASE,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
    TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
    TABLE_ANL_DIM_INSPECT_BASE,
    TABLE_ANL_DIM_VEHICLE_TYPE,
    collect_data,
    generate_metadata_from_dataframe
)
from ..infras_analytics import (
    infras_anl_dim_table,
)

import pandas as pd


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_NAME.lower(),
    description="create dml for dimension table ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_base_name],
)
def mysql_anl_dim_base_name(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_NAME}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_LICENSE.lower(),
    description="create dml for dimension table", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_base_license],
)
def mysql_anl_dim_base_license(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_NAME.lower(),
    description="create dml for dimension table ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_vehicle_name],
)
def mysql_anl_dim_vehicle_name(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_NAME}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_WHEELCHAIR_ACCESS.lower(),
    description=f"dimension table {TABLE_ANL_DIM_WHEELCHAIR_ACCESS}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_wheelchair_access],
)
def mysql_anl_dim_wheelchair_access(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_WHEELCHAIR_ACCESS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_BASE.lower(),
    description=f"dimension table {TABLE_ANL_DIM_BASE}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_base],
)
def mysql_anl_dim_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEH.lower(),
    description=f"dimension table {TABLE_ANL_DIM_VEH}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_veh],
)
def mysql_anl_dim_veh(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEH}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE.lower(),
    description=f"dimension table {TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE} ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_vehicle_year_fhv_vehicle],
)
def mysql_anl_dim_vehicle_year_fhv_vehicle(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES.lower(),
    description=f"dimension table {TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES} ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_lost_property_vehicles],
)
def mysql_anl_dim_lost_property_vehicles(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE.lower(),
    description=f"dimension table {TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE} ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_lost_property_reported_base],
)
def mysql_anl_dim_lost_property_reported_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"dimension table {TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_INSPECT_BASE.lower(),
    description=f"dimension table {TABLE_ANL_DIM_INSPECT_BASE}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_inspect_base],
)
def mysql_anl_dim_inspect_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"dimension table {TABLE_ANL_DIM_INSPECT_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_TYPE.lower(),
    description=f"dimension table {TABLE_ANL_DIM_VEHICLE_TYPE} ", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_anl_dim_table.collect_anl_dim_vehicle_type],
)
def mysql_anl_dim_vehicle_type(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_TYPE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)
    