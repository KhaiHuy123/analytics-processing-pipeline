
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_agg_report_unique_vehicles_key
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE
)


@asset(
    name=TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE.lower(),
    key_prefix=["result"],
    deps=([dbt_agg_report_unique_vehicles_key]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE}"
)
def agg_report_ban_bases(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data
