
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_report_new_driver_application_key
from ..constant import (
    REPORT_SCHEMA,
    TABLE_ANL_NEW_DRIVERS,
)


@asset(
    name=TABLE_ANL_NEW_DRIVERS.lower(),
    key_prefix=["result"],
    deps=([dbt_report_new_driver_application_key]),
    required_resource_keys={"psql_result_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_ANL_NEW_DRIVERS}"
)
def new_driver_application(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {REPORT_SCHEMA}.{TABLE_ANL_NEW_DRIVERS}"
    pd_data = context.resources.psql_result_extractor.extract_data(sql_stm, context)
    return pd_data
