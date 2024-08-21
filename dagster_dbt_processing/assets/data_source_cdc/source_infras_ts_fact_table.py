

from dagster import asset, Output
from dagster_duckdb import DuckDBResource

from ..constant import (
    TRIPS_SCHEMA,
    ZONES_SCHEMA,
    collect_data,
    generate_metadata_from_dataframe,
    TABLE_TS_FHV_TRIPS,
    TABLE_TS_GREEN_TRIPS,
    TABLE_TS_YELLOW_TRIPS,
    TABLE_ANL_TAXI_ZONES,
    TABLE_ANL_TXZ_REFERENCES_ID,
    TABLE_TS_DIM_VENDOR,
    TABLE_TS_DIM_PAYMENT_TYPE,
    TABLE_TS_DIM_TRIP_TYPE,
    TABLE_TS_DIM_RATECODE,
    additional_fk,
)
from ..infras_time_series import (
    infras_ts_fact_table
)
from .source_infras_ts_dim_table import (
    mysql_ts_vendor,
    mysql_ts_ratecode,
    mysql_ts_trip_type,
    mysql_ts_payment_type,
    mysql_ts_stored_fwd_flag
)

import pandas as pd


@asset(
    name=TABLE_TS_FHV_TRIPS.lower(),
    description=f"fact table {TABLE_TS_FHV_TRIPS}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_ts_fact_table.collect_fact_fhv_trips],
)
def mysql_fact_fhv_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_FHV_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_TAXI_ZONES.lower()]
    metadata["reference_schema"] = ZONES_SCHEMA.lower()
    metadata["reference_id"] = TABLE_ANL_TXZ_REFERENCES_ID
    metadata["skip_insertion_status"] = 1
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_GREEN_TRIPS.lower(),
    description=f"fact table {TABLE_TS_GREEN_TRIPS}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[mysql_ts_vendor, mysql_ts_ratecode, mysql_ts_trip_type, mysql_ts_payment_type, mysql_ts_stored_fwd_flag],
)
def mysql_fact_green_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_GREEN_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, additional_list=additional_fk, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_TAXI_ZONES.lower()]
    metadata["reference_schema"] = ZONES_SCHEMA.lower()
    metadata["reference_id"] = TABLE_ANL_TXZ_REFERENCES_ID
    metadata["additional_reference_tables"] = [
        TABLE_TS_DIM_VENDOR.lower(),
        TABLE_TS_DIM_RATECODE.lower(),
        TABLE_TS_DIM_PAYMENT_TYPE.lower(),
        TABLE_TS_DIM_TRIP_TYPE.lower(),
    ]
    metadata["skip_insertion_status"] = 1
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_YELLOW_TRIPS.lower(),
    description=f"fact table {TABLE_TS_YELLOW_TRIPS}", compute_kind="MySQL",
    io_manager_key="mysql_io_manager",
    deps=[infras_ts_fact_table.collect_fact_yellow_trips],
)
def mysql_fact_yellow_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_YELLOW_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, additional_list=additional_fk, set_up=False)
    metadata["reference_tables"] = [TABLE_ANL_TAXI_ZONES.lower()]
    metadata["reference_schema"] = ZONES_SCHEMA.lower()
    metadata["reference_id"] = TABLE_ANL_TXZ_REFERENCES_ID
    metadata["additional_reference_tables"] = [
        TABLE_TS_DIM_VENDOR.lower(),
        TABLE_TS_DIM_RATECODE.lower(),
        TABLE_TS_DIM_PAYMENT_TYPE.lower()
    ]
    metadata["skip_insertion_status"] = 1
    return Output(value=result, metadata=metadata)
