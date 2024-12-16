
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
from ...assets.ingest_time_series import ts_fhv_trips

logger = get_dagster_logger()

@sensor(
    name="ts_fhv_trips_failure_sensor",
    description=f"re-run failure sensor for table {ts_fhv_trips.TABLE_TS_FHV_TRIPS.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=ts_fhv_trips.ts_fhv_trips
)
def ts_fhv_trips_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=ts_fhv_trips.ts_fhv_trips.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {ts_fhv_trips.ts_fhv_trips.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=ts_fhv_trips.ts_fhv_trips.to_user_string(),
            asset_selection=[ts_fhv_trips.ts_fhv_trips.key]
        )
    else:
        yield SkipReason(f"Asset {ts_fhv_trips.ts_fhv_trips.key} passed.")
