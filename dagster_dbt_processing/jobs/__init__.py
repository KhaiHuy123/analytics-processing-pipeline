
from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition, daily_partition
from ..assets.constant import (
    INGEST_ANL_GROUP_NAME, INGEST_GEO_GROUP_NAME, INGEST_TS_GROUP_NAME,
    PREVIEW_ANL_GROUP_NAME, PREVIEW_GEO_GROUP_NAME, PREVIEW_TS_GROUP_NAME
)

ingest_anl = AssetSelection.groups(INGEST_ANL_GROUP_NAME)
ingest_geo = AssetSelection.groups(INGEST_GEO_GROUP_NAME)
ingest_ts = AssetSelection.groups(INGEST_TS_GROUP_NAME)
preview_anl = AssetSelection.groups(PREVIEW_ANL_GROUP_NAME)
preview_geo = AssetSelection.groups(PREVIEW_GEO_GROUP_NAME)
preview_ts = AssetSelection.groups(PREVIEW_TS_GROUP_NAME)

# reload jobs
reload_anl_data = define_asset_job(
    name="reload_anl_data",
    partitions_def=weekly_partition,
    selection=ingest_anl,
)
reload_geo_data = define_asset_job(
    name="reload_geo_data",
    partitions_def=monthly_partition,
    selection=ingest_geo,
)
reload_ts_data = define_asset_job(
    name="reload_ts_data",
    partitions_def=daily_partition,
    selection=ingest_ts,
)

# preview data
preview_anl_data = define_asset_job(
    name="preview_anl_data",
    partitions_def=weekly_partition,
    selection=preview_anl,
)
preview_geo_data = define_asset_job(
    name="preview_geo_data",
    partitions_def=monthly_partition,
    selection=preview_geo,
)
preview_ts_data = define_asset_job(
    name="preview_ts_data",
    partitions_def=daily_partition,
    selection=preview_ts,
)


