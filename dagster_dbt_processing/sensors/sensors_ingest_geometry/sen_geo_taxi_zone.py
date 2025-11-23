
from dagster import (
    sensor,
    RunRequest,
    get_dagster_logger,
    EventRecordsFilter,
    DagsterEventType,
    SensorEvaluationContext,
    DefaultSensorStatus,
    SkipReason
)
from ...assets.ingest_geometry import geo_taxi_zone

logger = get_dagster_logger()

@sensor(
    name="geo_taxi_zone_failure_sensor",
    description=f"re-run failure sensor for table {geo_taxi_zone.TABLE_GEO_TAXI_ZONES.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=geo_taxi_zone.geo_taxi_zone
)
def geo_taxi_zone_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=geo_taxi_zone.geo_taxi_zone.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {geo_taxi_zone.geo_taxi_zone.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=geo_taxi_zone.geo_taxi_zone.key.to_user_string(),
            asset_selection=[geo_taxi_zone.geo_taxi_zone.key]
        )
    else:
        yield SkipReason(f"Asset {geo_taxi_zone.geo_taxi_zone.key} passed.")
