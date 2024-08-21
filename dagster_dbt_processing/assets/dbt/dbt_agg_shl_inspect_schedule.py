
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_agg_report_shl_inspect_schedule_key
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE
)


@asset(
    name=TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE.lower(),
    key_prefix=["result"],
    deps=([dbt_agg_report_shl_inspect_schedule_key]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE}"
)
def agg_shl_inspect_schedule(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data
