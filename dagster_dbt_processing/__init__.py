
from dagster import Definitions
from .assets import (
    dbt_assets,
    ingest_analytics_assets, ingest_geometry_assets, ingest_time_series_assets, ingest_set_up_assets,
    preview_analytics_assets, preview_geometry_assets, preview_time_series_assets,
    infras_anl_assets, data_source_assets, infras_ts_assets, data_source_cdc_assets
)
from .jobs import (
    reload_anl_data, reload_geo_data, reload_ts_data,
    preview_anl_data, preview_geo_data, preview_ts_data
)
from .schedules import (
    reload_anl_data_schedule, reload_geo_data_schedule, reload_ts_data_schedule,
    preview_anl_data_schedule, preview_geo_data_schedule, preview_ts_data_schedule
)
from .resources import resources

all_assets = [*dbt_assets,  *ingest_set_up_assets, *infras_anl_assets, *data_source_assets,
              *infras_ts_assets,  *data_source_cdc_assets,
              *ingest_analytics_assets, *ingest_geometry_assets, *ingest_time_series_assets,
              *preview_analytics_assets, *preview_geometry_assets, *preview_time_series_assets]
all_jobs = [reload_anl_data, reload_geo_data, reload_ts_data,
            preview_anl_data, preview_geo_data, preview_ts_data]
all_schedules = [reload_anl_data_schedule, reload_geo_data_schedule, reload_ts_data_schedule,
                 preview_anl_data_schedule, preview_geo_data_schedule, preview_ts_data_schedule]

definitions = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=all_jobs,
    schedules=all_schedules,
)

