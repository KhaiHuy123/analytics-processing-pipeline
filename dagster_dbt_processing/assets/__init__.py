
from . import (
    dbt,
    ingest_analytics, ingest_geometry, ingest_time_series, ingest_setup,
    preview_ingest_analytics, preview_ingest_geometry, preview_ingest_time_series,
    infras_analytics, infras_time_series, data_source
)
from .constant import (
    DBT_GROUP_NAME, AIRBYTE_GROUP_NAME,
    INGEST_ANL_GROUP_NAME, INGEST_GEO_GROUP_NAME, INGEST_TS_GROUP_NAME, INGEST_SET_UP,
    PREVIEW_ANL_GROUP_NAME, PREVIEW_GEO_GROUP_NAME, PREVIEW_TS_GROUP_NAME,
    INFRAS_ANL_GROUP_NAME, INFRAS_TS_GROUP_NAME, SOURCE_GROUP_NAME
)
from dagster import load_assets_from_package_module

dbt_assets = load_assets_from_package_module(
    package_module=dbt, group_name=DBT_GROUP_NAME
)
ingest_analytics_assets = load_assets_from_package_module(
    package_module=ingest_analytics, group_name=INGEST_ANL_GROUP_NAME
)
ingest_geometry_assets = load_assets_from_package_module(
    package_module=ingest_geometry, group_name=INGEST_GEO_GROUP_NAME
)
ingest_time_series_assets = load_assets_from_package_module(
    package_module=ingest_time_series, group_name=INGEST_TS_GROUP_NAME
)
ingest_set_up_assets = load_assets_from_package_module(
    package_module=ingest_setup, group_name=INGEST_SET_UP
)
preview_analytics_assets = load_assets_from_package_module(
    package_module=preview_ingest_analytics, group_name=PREVIEW_ANL_GROUP_NAME
)
preview_geometry_assets = load_assets_from_package_module(
    package_module=preview_ingest_geometry, group_name=PREVIEW_GEO_GROUP_NAME
)
preview_time_series_assets = load_assets_from_package_module(
   package_module=preview_ingest_time_series, group_name=PREVIEW_TS_GROUP_NAME
)
infras_anl_assets = load_assets_from_package_module(
    package_module=infras_analytics, group_name=INFRAS_ANL_GROUP_NAME
)
infras_ts_assets = load_assets_from_package_module(
    package_module=infras_time_series, group_name=INFRAS_TS_GROUP_NAME
)
data_source_assets = load_assets_from_package_module(
    package_module=data_source, group_name=SOURCE_GROUP_NAME
)
