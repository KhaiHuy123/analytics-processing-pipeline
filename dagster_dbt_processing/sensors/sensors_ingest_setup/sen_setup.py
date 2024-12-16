
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
from ...assets.ingest_setup import ingest_setup

logger = get_dagster_logger()

@sensor(
    name="ingest_setup_failure_sensor",
    description=f"re-run failure sensor for setting up for ingestion",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=ingest_setup.schema
)
def ingest_setup_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=ingest_setup.schema.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {ingest_setup.schema.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=ingest_setup.schema.key.to_user_string(),
            asset_selection=[ingest_setup.schema.key]
        )
    else:
        yield SkipReason(f"Asset {ingest_setup.schema.key} passed.")
