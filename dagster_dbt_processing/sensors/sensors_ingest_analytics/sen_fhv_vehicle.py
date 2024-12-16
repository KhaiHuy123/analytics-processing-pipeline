
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
from ...assets.ingest_analytics import anl_fhv_vehicle

logger = get_dagster_logger()

@sensor(
    name="anl_fhv_vehicle_failure_sensor",
    description=f"re-run failure sensor for table {anl_fhv_vehicle.TABLE_ANL_FHV_VEHICLE.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_fhv_vehicle.anl_fhv_vehicles
)
def anl_fhv_vehicle_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_fhv_vehicle.anl_fhv_vehicles.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_fhv_vehicle.anl_fhv_vehicles.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_fhv_vehicle.anl_fhv_vehicles.key.to_user_string(),
            asset_selection=[anl_fhv_vehicle.anl_fhv_vehicles.key]
        )
    else:
        yield SkipReason(f"Asset {anl_fhv_vehicle.anl_fhv_vehicles.key} passed.")
