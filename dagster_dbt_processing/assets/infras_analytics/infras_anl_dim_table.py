
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
from ..preview_ingest_analytics import (
    preview_anl_base_report,
    preview_anl_fhv_vehicle,
    preview_anl_lost_property,
    preview_anl_shl_inspect,
    preview_anl_shl_permit
)

import pandas as pd


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_NAME.lower(), key_prefix=["anl", "dim", "base_name"],
    description=f"dimension table {TABLE_ANL_DIM_AGG_BASE_NAME}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_base_report.preview_anl_base_report],
)
def collect_anl_dim_base_name(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_NAME}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_LICENSE.lower(), key_prefix=["anl", "dim", "base_license"],
    description=f"dimension table {TABLE_ANL_DIM_AGG_BASE_LICENSE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_base_report.preview_anl_base_report],
)
def collect_anl_dim_base_license(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_NAME.lower(), key_prefix=["anl", "dim", "vehicle_name"],
    description=f"dimension table {TABLE_ANL_DIM_VEHICLE_NAME}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_fhv_vehicle.preview_anl_fhv_vehicle],
)
def collect_anl_dim_vehicle_name(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_NAME}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_WHEELCHAIR_ACCESS.lower(), key_prefix=["anl", "dim", "wheelchair_access"],
    description=f"dimension table {TABLE_ANL_DIM_WHEELCHAIR_ACCESS}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_fhv_vehicle.preview_anl_fhv_vehicle],
)
def collect_anl_dim_wheelchair_access(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_WHEELCHAIR_ACCESS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_BASE.lower(), key_prefix=["anl", "dim", "base"],
    description=f"dimension table {TABLE_ANL_DIM_BASE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_fhv_vehicle.preview_anl_fhv_vehicle, preview_anl_shl_permit.preview_anl_shl_permit],
)
def collect_anl_dim_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEH.lower(), key_prefix=["anl", "dim", "veh"],
    description=f"dimension table {TABLE_ANL_DIM_VEH}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_fhv_vehicle.preview_anl_fhv_vehicle],
)
def collect_anl_dim_veh(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEH}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE.lower(), key_prefix=["anl", "dim", "vehicle_year_fhv"],
    description=f"dimension table {TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_fhv_vehicle.preview_anl_fhv_vehicle],
)
def collect_anl_dim_vehicle_year_fhv_vehicle(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES.lower(), key_prefix=["anl", "dim", "lost_property_vehicles"],
    description=f"dimension table {TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_lost_property.preview_anl_lost_property],
)
def collect_anl_dim_lost_property_vehicles(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE.lower(), key_prefix=["anl", "dim", "lost_property_reported_base"],
    description=f"dimension table {TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_lost_property.preview_anl_lost_property],
)
def collect_anl_dim_lost_property_reported_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_INSPECT_BASE.lower(), key_prefix=["anl", "dim", "inspect_base"],
    description=f"dimension table {TABLE_ANL_DIM_INSPECT_BASE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_shl_inspect.preview_anl_shl_inspect],
)
def collect_anl_dim_inspect_base(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_INSPECT_BASE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_DIM_VEHICLE_TYPE.lower(), key_prefix=["anl", "dim", "vehicle_type"],
    description=f"dimension table {TABLE_ANL_DIM_VEHICLE_TYPE} ", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[preview_anl_shl_permit.preview_anl_shl_permit],
)
def collect_anl_dim_vehicle_type(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_TYPE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)
