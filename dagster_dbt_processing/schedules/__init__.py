
from ..jobs import (
    reload_anl_data, preview_anl_data,
    reload_geo_data, preview_geo_data,
    reload_ts_data, preview_ts_data
)
from dagster import ScheduleDefinition

'''

Crontab Syntax
+---------------- minute (0 - 59)
|  +------------- hour (0 - 23)
|  |  +---------- day of month (1 - 31)
|  |  |  +------- month of year (1 - 12)
|  |  |  |  +---- day of week (0 - 6) (Sunday is 0 or 7)
|  |  |  |  |
*  *  *  *  *  command to be executed

* means all values are acceptable

'''

# reload schedule
reload_anl_data_schedule = ScheduleDefinition(
    job=reload_anl_data,
    cron_schedule="15 20 * * 1-6",  # every day from Monday to Saturday at 20:15
)
reload_geo_data_schedule = ScheduleDefinition(
    job=reload_geo_data,
    cron_schedule="15 23 * * 6",  # Saturday at 23:15
)
reload_ts_data_schedule = ScheduleDefinition(
    job=reload_ts_data,
    cron_schedule="15 21 * * 1-6",  # every day from Monday to Saturday at 21:15
)

# preview data schedule
preview_anl_data_schedule = ScheduleDefinition(
    job=preview_anl_data,
    cron_schedule="45 20 * * 1-6",  # every day from Monday to Saturday at 20:45
)
preview_geo_data_schedule = ScheduleDefinition(
    job=preview_geo_data,
    cron_schedule="45 23 * * 6",  # Saturday at 23:45
)
preview_ts_data_schedule = ScheduleDefinition(
    job=preview_ts_data,
    cron_schedule="45 21 * * 1-6",  # every day from Monday to Saturday at 21:45
)
