
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
from ...assets.ingest_time_series import ts_dim_table

logger = get_dagster_logger()

@sensor(
    name="ts_dim_table_failure_sensor",
    description=f"re-run failure sensor for time-series dim table(s)",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=ts_dim_table.ts_dim_table
)
def ts_dim_table_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=ts_dim_table.ts_dim_table.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {ts_dim_table.ts_dim_table.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=ts_dim_table.ts_dim_table.key.to_user_string(),
            asset_selection=[ts_dim_table.ts_dim_table.key]
        )
    else:
        yield SkipReason(f"Asset {ts_dim_table.ts_dim_table.key} passed.")
