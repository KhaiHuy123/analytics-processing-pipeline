
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_fhv_vehicle_type
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_FHV_VEHICLE_TYPE
)


@asset(
    name=TABLE_PSQL_FHV_VEHICLE_TYPE.lower(),
    key_prefix=["result"],
    deps=([dbt_fhv_vehicle_type]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_ts_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_FHV_VEHICLE_TYPE}"
)
def fhv_vehicle_type(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_FHV_VEHICLE_TYPE}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data

