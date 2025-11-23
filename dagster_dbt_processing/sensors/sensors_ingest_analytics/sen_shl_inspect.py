
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
from ...assets.ingest_analytics import anl_shl_inspect

logger = get_dagster_logger()

@sensor(
    name="anl_shl_inspect_failure_sensor",
    description=f"re-run failure sensor for table {anl_shl_inspect.TABLE_ANL_SHL_INSPECT.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_shl_inspect.anl_shl_inspect
)
def anl_shl_inspect_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_shl_inspect.anl_shl_inspect.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_shl_inspect.anl_shl_inspect.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_shl_inspect.anl_shl_inspect.key.to_user_string(),
            asset_selection=[anl_shl_inspect.anl_shl_inspect.key]
        )
    else:
        yield SkipReason(f"Asset {anl_shl_inspect.anl_shl_inspect.key} passed.")
