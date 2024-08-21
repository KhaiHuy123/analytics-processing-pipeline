
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_services_agg_base_license_key
from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_DIM_AGG_BASE_LICENSE
)


@asset(
    name=TABLE_ANL_DIM_AGG_BASE_LICENSE.lower(),
    key_prefix=["result"],
    deps=([dbt_services_agg_base_license_key]),
    required_resource_keys={"psql_services_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_ANL_DIM_AGG_BASE_LICENSE}"
)
def services_agg_base_license(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE}"
    pd_data = context.resources.psql_services_extractor.extract_data(sql_stm, context)
    return pd_data
