
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
from ...assets.ingest_analytics import anl_dim_table

logger = get_dagster_logger()

@sensor(
    name="anl_dim_table_failure_sensor",
    description=f"re-run failure sensor for analytics dim table(s)",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_dim_table.anl_dim_table
)
def anl_dim_table_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_dim_table.anl_dim_table.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_dim_table.anl_dim_table.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_dim_table.anl_dim_table.key.to_user_string(),
            asset_selection=[anl_dim_table.anl_dim_table.key]
        )
    else:
        yield SkipReason(f"Asset {anl_dim_table.anl_dim_table.key} passed.")
