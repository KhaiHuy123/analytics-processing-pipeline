
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
from ...assets.ingest_analytics import anl_taxi_zone

logger = get_dagster_logger()

@sensor(
    name="anl_taxi_zone_failure_sensor",
    description=f"re-run failure sensor for table {anl_taxi_zone.TABLE_ANL_TAXI_ZONES.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_taxi_zone.anl_taxi_zone
)
def anl_taxi_zone_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_taxi_zone.anl_taxi_zone.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_taxi_zone.anl_taxi_zone.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_taxi_zone.anl_taxi_zone.to_user_string(),
            asset_selection=[anl_taxi_zone.anl_taxi_zone.key]
        )
    else:
        yield SkipReason(f"Asset {anl_taxi_zone.anl_taxi_zone.key} passed.")
