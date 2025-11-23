
import os
from dagster_duckdb import DuckDBResource

database = os.getenv("MOTHER_DUCK_DATABASE")
token = os.getenv("MOTHER_DUCK_TOKEN")

if not database or not token:
    raise ValueError("MOTHER_DUCK_DATABASE and MOTHER_DUCK_TOKEN must be set as environment variables")

connection_string = f"md:{database}?motherduck_token={token}"
DUCKDB_RESOURCE = DuckDBResource(
    database=connection_string,
)
