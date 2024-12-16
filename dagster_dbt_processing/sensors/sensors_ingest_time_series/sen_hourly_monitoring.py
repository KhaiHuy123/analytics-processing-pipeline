
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
from ...assets.ingest_time_series import ts_hourly_monitoring

logger = get_dagster_logger()

@sensor(
    name="ts_hourly_monitoring_failure_sensor",
    description=f"re-run failure sensor for table {ts_hourly_monitoring.TABLE_TS_HOURLY_MONITORING.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=ts_hourly_monitoring.ts_hourly_monitoring
)
def ts_hourly_monitoring_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=ts_hourly_monitoring.ts_hourly_monitoring.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {ts_hourly_monitoring.ts_hourly_monitoring.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=ts_hourly_monitoring.ts_hourly_monitoring.to_user_string(),
            asset_selection=[ts_hourly_monitoring.ts_hourly_monitoring.key]
        )
    else:
        yield SkipReason(f"Asset {ts_hourly_monitoring.ts_hourly_monitoring.key} passed.")
