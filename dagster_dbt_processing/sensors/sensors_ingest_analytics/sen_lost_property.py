
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
from ...assets.ingest_analytics import anl_lost_property

logger = get_dagster_logger()

@sensor(
    name="anl_lost_property_failure_sensor",
    description=f"re-run failure sensor for table {anl_lost_property.TABLE_ANL_LOST_PROPERTY.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_lost_property.anl_lost_property
)
def anl_lost_property_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_lost_property.anl_lost_property.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_lost_property.anl_lost_property.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_lost_property.anl_lost_property.key.to_user_string(),
            asset_selection=[anl_lost_property.anl_lost_property.key]
        )
    else:
        yield SkipReason(f"Asset {anl_lost_property.anl_lost_property.key} passed.")
