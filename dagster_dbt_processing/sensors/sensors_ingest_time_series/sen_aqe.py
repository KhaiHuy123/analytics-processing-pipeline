
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
from ...assets.ingest_time_series import ts_aqe

logger = get_dagster_logger()

@sensor(
    name="ts_aqe_failure_sensor",
    description=f"re-run failure sensor for table {ts_aqe.TABLE_TS_AQE.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=ts_aqe.ts_aqe
)
def ts_aqe_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=ts_aqe.ts_aqe.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {ts_aqe.ts_aqe.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=ts_aqe.ts_aqe.to_user_string(),
            asset_selection=[ts_aqe.ts_aqe.key]
        )
    else:
        yield SkipReason(f"Asset {ts_aqe.ts_aqe.key} passed.")
