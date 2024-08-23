
from dagster_dbt import DbtCliResource
from dagster import file_relative_path

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_processing/transform")
DBT_PROFILES_PATH = file_relative_path(__file__, "../../dbt_processing/transform")

DBT_RESOURCE = DbtCliResource(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH
)
