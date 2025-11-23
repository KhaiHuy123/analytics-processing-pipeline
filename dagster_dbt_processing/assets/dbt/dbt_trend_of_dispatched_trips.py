
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_trend_of_dispatched_trips_key
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS,
)


@asset(
    name=TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS.lower(),
    key_prefix=["result"],
    deps=([dbt_trend_of_dispatched_trips_key]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS}"
)
def trend_of_dispatched_trips(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data
