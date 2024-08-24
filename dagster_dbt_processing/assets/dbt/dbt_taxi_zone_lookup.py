
import pandas as pd
from dagster import asset
from .dbt_assets import dbt_zones_taxi_zone_lookup_key
from ..constant import (
    ZONES_SCHEMA,
    TABLE_ANL_TAXI_ZONES
)


@asset(
    name=TABLE_ANL_TAXI_ZONES.lower(),
    key_prefix=["result"],
    deps=([dbt_zones_taxi_zone_lookup_key]),
    required_resource_keys={"psql_zones_extractor"},
    io_manager_key="minio_anl_io_manager",
    compute_kind='MinIO',
    description=f"upload to minio for table {TABLE_ANL_TAXI_ZONES}"
)
def taxi_zones_lookup(context) -> pd.DataFrame:
    sql_stm = f"SELECT * FROM {ZONES_SCHEMA}.{TABLE_ANL_TAXI_ZONES}"
    pd_data = context.resources.psql_zones_extractor.extract_data(sql_stm, context)
    return pd_data
