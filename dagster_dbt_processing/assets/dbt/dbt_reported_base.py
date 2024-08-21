
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_reported_base_key
from ..constant import (
    PROCESS_SCHEMA,
    TABLE_PSQL_REPORTED_BASE
)


@asset(
    name=TABLE_PSQL_REPORTED_BASE.lower(),
    key_prefix=["result"],
    deps=([dbt_reported_base_key]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_PSQL_REPORTED_BASE}"
)
def agg_reported_base(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {PROCESS_SCHEMA}.{TABLE_PSQL_REPORTED_BASE}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data
