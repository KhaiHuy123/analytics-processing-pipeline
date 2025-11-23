
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
from ...assets.ingest_analytics import anl_meter_shop

logger = get_dagster_logger()

@sensor(
    name="anl_meter_shop_failure_sensor",
    description=f"re-run failure sensor for table {anl_meter_shop.TABLE_ANL_METER_SHOPS.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_meter_shop.anl_meter_shop
)
def anl_meter_shop_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_meter_shop.anl_meter_shop.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_meter_shop.anl_meter_shop.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_meter_shop.anl_meter_shop.key.to_user_string(),
            asset_selection=[anl_meter_shop.anl_meter_shop.key]
        )
    else:
        yield SkipReason(f"Asset {anl_meter_shop.anl_meter_shop.key} passed.")
