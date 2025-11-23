
from dagster import (
    run_failure_sensor,
    RunFailureSensorContext,
    DefaultSensorStatus,
    RunRequest
)
from ...jobs import preview_geo_data

@run_failure_sensor(
    name="preview_geo_failure_sensor",
    request_job=preview_geo_data,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description="re-run preview_geo job"
)
def preview_geometry_failure_sensor(context:RunFailureSensorContext):
    job_name = context.dagster_run.job_name
    run_config = {
        "ops": {"status_report": {"config": {"job_name": job_name}}}
    }
    return RunRequest(
        run_key=f"{job_name}_id",
        run_config=run_config,
        job_name=job_name
    )
