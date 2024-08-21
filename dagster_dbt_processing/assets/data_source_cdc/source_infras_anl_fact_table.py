
from dagster import asset, Output
from dagster_duckdb import DuckDBResource

from ..constant import (
    SERVICES_SCHEMA,
    ZONES_SCHEMA,
    REPORT_SCHEMA,
    TABLE_ANL_BASE_REPORT,
    TABLE_ANL_FHV_VEHICLE,
    TABLE_ANL_LOST_PROPERTY,
    TABLE_ANL_METER_SHOPS,
    TABLE_ANL_MONTHLY_REPORT,
    TABLE_ANL_NEW_DRIVERS,
    TABLE_ANL_SHL_INSPECT,
    TABLE_ANL_SHL_PERMIT,
    TABLE_ANL_TAXI_ZONES,
    collect_data,
    generate_metadata_from_dataframe,
    TABLE_ANL_DIM_AGG_BASE_NAME,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_VEHICLE_NAME,
    TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
    TABLE_ANL_DIM_BASE,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
    TABLE_ANL_DIM_VEHICLE_TYPE,
    TABLE_ANL_DIM_INSPECT_BASE,
    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
    TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE
)

from .source_infras_anl_dim_table import (
    mysql_anl_dim_base_name,
    mysql_anl_dim_base_license,
    mysql_anl_dim_vehicle_name,
    mysql_anl_dim_wheelchair_access,
    mysql_anl_dim_base,
    mysql_anl_dim_veh,
    mysql_anl_dim_vehicle_year_fhv_vehicle,
    mysql_anl_dim_vehicle_type,
    mysql_anl_dim_inspect_base,
    mysql_anl_dim_lost_property_vehicles,
    mysql_anl_dim_lost_property_reported_base
)
from ..infras_analytics import (
    infras_anl_fact_table
)

import pandas as pd


@asset(
    name=TABLE_ANL_BASE_REPORT.lower(),
    description="create dml fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[mysql_anl_dim_base_name, mysql_anl_dim_base_license],
)
def mysql_fact_fhv_base_report(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_BASE_REPORT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_DIM_AGG_BASE_NAME.lower(),
                                    TABLE_ANL_DIM_AGG_BASE_LICENSE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_FHV_VEHICLE.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[mysql_anl_dim_vehicle_name, mysql_anl_dim_wheelchair_access,
          mysql_anl_dim_base, mysql_anl_dim_veh, mysql_anl_dim_vehicle_year_fhv_vehicle],
)
def mysql_fact_fhv_vehicles(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_FHV_VEHICLE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_DIM_BASE.lower(), TABLE_ANL_DIM_VEHICLE_NAME.lower(),
                                    TABLE_ANL_DIM_WHEELCHAIR_ACCESS.lower(), TABLE_ANL_DIM_VEH.lower(),
                                    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_SHL_PERMIT.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[mysql_anl_dim_vehicle_type, mysql_anl_dim_base],
)
def mysql_fact_shl_permit(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_PERMIT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_DIM_VEHICLE_TYPE.lower(),
                                    TABLE_ANL_DIM_BASE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_SHL_INSPECT.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[mysql_anl_dim_inspect_base],
)
def mysql_fact_shl_inspect(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_INSPECT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_DIM_INSPECT_BASE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_LOST_PROPERTY.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[mysql_anl_dim_lost_property_vehicles, mysql_anl_dim_lost_property_reported_base],
)
def mysql_fact_lost_property_contact(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_LOST_PROPERTY}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE.lower(),
                                    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_METER_SHOPS.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[infras_anl_fact_table.collect_fact_meter_shop],
)
def mysql_fact_meter_shop(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_METER_SHOPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_TAXI_ZONES.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[infras_anl_fact_table.collect_fact_taxi_zones],
)
def mysql_fact_taxi_zones(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_MONTHLY_REPORT.lower(),
    description="create dml  fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[infras_anl_fact_table.collect_fact_monthly_report],
)
def mysql_fact_monthly_report(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_MONTHLY_REPORT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_NEW_DRIVERS.lower(),
    description="create dml fact metadata",
    compute_kind="MySQL", io_manager_key="mysql_io_manager",
    deps=[infras_anl_fact_table.collect_fact_new_driver],
)
def mysql_fact_new_driver(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_NEW_DRIVERS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)
