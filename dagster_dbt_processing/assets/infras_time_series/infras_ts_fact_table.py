
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
from ..preview_ingest_time_series import (
    preview_ts_fhv_trips,
    preview_ts_green_trips,
    preview_ts_yellow_trips,
)
from ..infras_analytics import (
    infras_anl_fact_table
)
from .infras_ts_dim_table import (
    collect_ts_ratecode,
    collect_ts_vendor,
    collect_ts_trip_type,
    collect_ts_payment_type,
    collect_ts_stored_fwd_flag
)

import pandas as pd


@asset(
    name=TABLE_TS_FHV_TRIPS.lower(), key_prefix=["ts", "fact", "fhv_trips"],
    description=f"fact table {TABLE_TS_FHV_TRIPS}", compute_kind="postgres",
    io_manager_key="psql_trips_io_manager",
    deps=[preview_ts_fhv_trips.download_fhv_trips,
          infras_anl_fact_table.collect_fact_taxi_zones],
)
def collect_fact_fhv_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_FHV_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result)
    metadata["reference_tables"] = [TABLE_ANL_TAXI_ZONES.lower()]
    metadata["reference_schema"] = ZONES_SCHEMA.lower()
    metadata["reference_id"] = TABLE_ANL_TXZ_REFERENCES_ID
    metadata["skip_insertion_status"] = 1
    return Output(value=result, metadata=metadata)


@asset(
    name=TABLE_TS_GREEN_TRIPS.lower(), key_prefix=["ts", "fact", "green_trips"],
    description=f"fact table {TABLE_TS_GREEN_TRIPS}", compute_kind="postgres",
    io_manager_key="psql_trips_io_manager",
    deps=[preview_ts_green_trips.download_green_trips,
          infras_anl_fact_table.collect_fact_taxi_zones,
          collect_ts_vendor, collect_ts_ratecode, collect_ts_payment_type,
          collect_ts_trip_type, collect_ts_stored_fwd_flag],
)
def collect_fact_green_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_GREEN_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, additional_list=additional_fk)
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
    name=TABLE_TS_YELLOW_TRIPS.lower(), key_prefix=["ts", "fact", "yellow_trips"],
    description=f"fact table {TABLE_TS_YELLOW_TRIPS}", compute_kind="postgres",
    io_manager_key="psql_trips_io_manager",
    deps=[preview_ts_yellow_trips.download_yellow_trips,
          infras_anl_fact_table.collect_fact_taxi_zones,
          collect_ts_vendor, collect_ts_stored_fwd_flag,
          collect_ts_ratecode, collect_ts_payment_type],
)
def collect_fact_yellow_trips(duckdb: DuckDBResource) -> Output[pd.DataFrame]:
    table_name = f"{TRIPS_SCHEMA}.{TABLE_TS_YELLOW_TRIPS}"
    result = collect_data(table_name, duckdb)
    metadata = generate_metadata_from_dataframe(result, additional_list=additional_fk)
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
