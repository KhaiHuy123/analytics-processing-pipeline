
from dagster import asset, Output
from dagster_duckdb import DuckDBResource
from ..constant import (
    TRIPS_SCHEMA,
    TABLE_TS_DIM_VENDOR,
    TABLE_TS_DIM_PAYMENT_TYPE,
    TABLE_TS_DIM_TRIP_TYPE,
    TABLE_TS_DIM_RATECODE,
    TABLE_TS_DIM_STORE_FWD_FLAG,
    generate_metadata_from_dataframe,
    collect_data
)

import pandas as pd


@asset(
    name=TABLE_TS_DIM_VENDOR.lower(),
    description=f"dimension table {TABLE_TS_DIM_VENDOR}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
)
def mysql_ts_vendor(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_VENDOR}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_DIM_PAYMENT_TYPE.lower(),
    description=f"dimension table {TABLE_TS_DIM_PAYMENT_TYPE}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
)
def mysql_ts_payment_type(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_PAYMENT_TYPE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_DIM_TRIP_TYPE.lower(),
    description=f"dimension table {TABLE_TS_DIM_TRIP_TYPE}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
)
def mysql_ts_trip_type(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_TRIP_TYPE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_DIM_RATECODE.lower(),
    description=f"dimension table {TABLE_TS_DIM_RATECODE}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
)
def mysql_ts_ratecode(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_RATECODE}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_DIM_STORE_FWD_FLAG.lower(),
    description=f"dimension table {TABLE_TS_DIM_STORE_FWD_FLAG}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
)
def mysql_ts_stored_fwd_flag(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_DIM_STORE_FWD_FLAG}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    return Output(value=result, metadata=metadata)
