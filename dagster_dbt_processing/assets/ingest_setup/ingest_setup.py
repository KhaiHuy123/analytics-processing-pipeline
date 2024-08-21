
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    SERVICES_SCHEMA,
    REPORT_SCHEMA,
    TRIPS_SCHEMA,
    ENV_SCHEMA,
    ZONES_SCHEMA
)


@asset(name="schema", key_prefix=["mother_duck", "schema"],
       description="create schemas in Mother Duck Cloud",
       compute_kind="duckdb")
def schema(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        # Create dim table
        create_schema_query = f"""
            CREATE SCHEMA IF NOT EXISTS {SERVICES_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {REPORT_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {TRIPS_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {ENV_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {ZONES_SCHEMA};
        """
        conn.execute(create_schema_query)
        context.log.info("Create schemas successfully")

