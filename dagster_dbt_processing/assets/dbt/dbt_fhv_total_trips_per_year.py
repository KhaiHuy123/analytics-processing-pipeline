
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_fhv_total_trips_per_year
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR
)


@asset(
    name=TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR.lower(),
    key_prefix=["result"],
    deps=([dbt_fhv_total_trips_per_year]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_ts_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR}"
)
def fhv_total_trips_per_year(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data

