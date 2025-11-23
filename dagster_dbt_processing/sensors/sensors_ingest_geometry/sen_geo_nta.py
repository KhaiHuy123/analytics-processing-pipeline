
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
from ...assets.ingest_geometry import geo_nta

logger = get_dagster_logger()

@sensor(
    name="geo_nta_failure_sensor",
    description=f"re-run failure sensor for table {geo_nta.TABLE_GEO_NTA.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=geo_nta.geo_nta
)
def geo_nta_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=geo_nta.geo_nta.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {geo_nta.geo_nta.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=geo_nta.geo_nta.key.to_user_string(),
            asset_selection=[geo_nta.geo_nta.key]
        )
    else:
        yield SkipReason(f"Asset {geo_nta.geo_nta.key} passed.")
