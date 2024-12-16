
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
from ...assets.ingest_analytics import anl_new_driver

logger = get_dagster_logger()

@sensor(
    name="anl_new_driver_failure_sensor",
    description=f"re-run failure sensor for table {anl_new_driver.TABLE_ANL_NEW_DRIVERS.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_new_driver.anl_new_driver
)
def anl_new_driver_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_new_driver.anl_new_driver.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_new_driver.anl_new_driver.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_new_driver.anl_new_driver.key.to_user_string(),
            asset_selection=[anl_new_driver.anl_new_driver.key]
        )
    else:
        yield SkipReason(f"Asset {anl_new_driver.anl_new_driver.key} passed.")
