
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
from ...assets.ingest_analytics import anl_monthly_report

logger = get_dagster_logger()

@sensor(
    name="anl_monthly_report_failure_sensor",
    description=f"re-run failure sensor for table {anl_monthly_report.TABLE_ANL_MONTHLY_REPORT.lower()}",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    target=anl_monthly_report.anl_monthly_report
)
def anl_monthly_report_failure_sensor(context: SensorEvaluationContext):
    failure_events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE,
            asset_key=anl_monthly_report.anl_monthly_report.key
        ),
        limit=1
    )
    if failure_events:
        logger.info(f"Asset {anl_monthly_report.anl_monthly_report.key} failed. Triggering re-run.")
        yield RunRequest(
            run_key=anl_monthly_report.anl_monthly_report.key.to_user_string(),
            asset_selection=[anl_monthly_report.anl_monthly_report.key]
        )
    else:
        yield SkipReason(f"Asset {anl_monthly_report.anl_monthly_report.key} passed.")
