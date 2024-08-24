
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

from ..preview_ingest_analytics import (
    preview_anl_meter_shop,
    preview_anl_monthly_report,
    preview_anl_new_driver,
    preview_anl_taxi_zone
)

from ..infras_analytics.infras_anl_dim_table import (
    collect_anl_dim_base_name,
    collect_anl_dim_base_license,
    collect_anl_dim_vehicle_name,
    collect_anl_dim_wheelchair_access,
    collect_anl_dim_base,
    collect_anl_dim_veh,
    collect_anl_dim_vehicle_year_fhv_vehicle,
    collect_anl_dim_vehicle_type,
    collect_anl_dim_inspect_base,
    collect_anl_dim_lost_property_vehicles,
    collect_anl_dim_lost_property_reported_base
)

import pandas as pd


@asset(
    name=TABLE_ANL_BASE_REPORT.lower(), key_prefix=["anl", "fact", "base_report"],
    description=f"fact table {TABLE_ANL_BASE_REPORT}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[collect_anl_dim_base_name, collect_anl_dim_base_license],
)
def collect_fact_fhv_base_report(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_BASE_REPORT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_DIM_AGG_BASE_LICENSE.lower(),
                                    TABLE_ANL_DIM_AGG_BASE_NAME.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_FHV_VEHICLE.lower(), key_prefix=["anl", "fact", "fhv_vehicle"],
    description=f"fact table {TABLE_ANL_FHV_VEHICLE}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[collect_anl_dim_vehicle_name, collect_anl_dim_wheelchair_access,
          collect_anl_dim_base, collect_anl_dim_veh, collect_anl_dim_vehicle_year_fhv_vehicle],
)
def collect_fact_fhv_vehicles(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_FHV_VEHICLE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_DIM_BASE.lower(), TABLE_ANL_DIM_VEHICLE_NAME.lower(),
                                    TABLE_ANL_DIM_WHEELCHAIR_ACCESS.lower(), TABLE_ANL_DIM_VEH.lower(),
                                    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_SHL_PERMIT.lower(), key_prefix=["anl", "fact", "shl_permit"],
    description=f"fact table {TABLE_ANL_SHL_PERMIT}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[collect_anl_dim_vehicle_type, collect_anl_dim_base],
)
def collect_fact_shl_permit(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_PERMIT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_DIM_VEHICLE_TYPE.lower(),
                                    TABLE_ANL_DIM_BASE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_SHL_INSPECT.lower(), key_prefix=["anl", "fact", "shl_inspect"],
    description=f"fact table {TABLE_ANL_SHL_INSPECT}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[collect_anl_dim_inspect_base],
)
def collect_fact_shl_inspect(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_SHL_INSPECT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_DIM_INSPECT_BASE.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_LOST_PROPERTY.lower(), key_prefix=["anl", "fact", "lost_property"],
    description=f"fact table {TABLE_ANL_LOST_PROPERTY}", compute_kind="postgres",
    io_manager_key="psql_services_io_manager",
    deps=[collect_anl_dim_lost_property_vehicles, collect_anl_dim_lost_property_reported_base],
)
def collect_fact_lost_property_contact(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{SERVICES_SCHEMA}.{TABLE_ANL_LOST_PROPERTY}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE.lower(),
                                    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES.lower()]
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_METER_SHOPS.lower(), key_prefix=["anl", "fact", "meter_shops"],
    description=f"fact table {TABLE_ANL_METER_SHOPS}", compute_kind="postgres",
    io_manager_key="psql_report_io_manager",
    deps=[preview_anl_meter_shop.preview_anl_meter_shop],
)
def collect_fact_meter_shop(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_METER_SHOPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_TAXI_ZONES.lower(), key_prefix=["anl", "fact", "taxi_zones"],
    description=f"fact table {TABLE_ANL_TAXI_ZONES}", compute_kind="postgres",
    io_manager_key="psql_zones_io_manager",
    deps=[preview_anl_taxi_zone.preview_anl_taxi_zones],
)
def collect_fact_taxi_zones(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_MONTHLY_REPORT.lower(), key_prefix=["anl", "fact", "monthly_report"],
    description=f"fact table {TABLE_ANL_MONTHLY_REPORT}", compute_kind="postgres",
    io_manager_key="psql_report_io_manager",
    deps=[preview_anl_monthly_report.preview_anl_monthly_report],
)
def collect_fact_monthly_report(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_MONTHLY_REPORT}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_ANL_NEW_DRIVERS.lower(), key_prefix=["anl", "fact", "new_drivers"],
    description=f"fact table {TABLE_ANL_NEW_DRIVERS}", compute_kind="postgres",
    io_manager_key="psql_report_io_manager",
    deps=[preview_anl_new_driver.preview_anl_new_driver],
)
def collect_fact_new_driver(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{REPORT_SCHEMA}.{TABLE_ANL_NEW_DRIVERS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    return Output(value=result, metadata=metadata)
