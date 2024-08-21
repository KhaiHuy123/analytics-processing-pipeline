
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_report_meter_shops_key
from ..constant import (
    REPORT_SCHEMA,
    TABLE_ANL_METER_SHOPS
)


@asset(
    name=TABLE_ANL_METER_SHOPS.lower(),
    key_prefix=["result"],
    deps=([dbt_report_meter_shops_key]),
    required_resource_keys={"psql_report_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_ANL_METER_SHOPS}"
)
def meter_shop(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {REPORT_SCHEMA}.{TABLE_ANL_METER_SHOPS}"
    pd_data = context.resources.psql_report_extractor.extract_data(sql_stm, context)
    return pd_data
