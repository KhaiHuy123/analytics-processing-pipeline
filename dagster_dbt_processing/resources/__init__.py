
import os
from .dbt_resources import DBT_RESOURCE
from .duckdb_resources import DUCKDB_RESOURCE
from .duckdb_io_manager import DuckDB_IOManager, DuckDB
from .postgres_io_manager import PostgresSQL_IOManager
from .minio_io_manager import MinIO_IOManager
from .postgres_extractor import PostgresExtractor


def create_minio_config(bucket):
    return {
        "endpoint_url": os.getenv("ENDPOINT_URL_MINIO"),
        "bucket": os.getenv(bucket),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID_MINIO"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KRY_MINIO"),
    }


def create_psql_config(database, schema):
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv(database),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "schema": os.getenv(schema)
    }

PSQL_ZONES_SCHEMA_CONFIG = create_psql_config("POSTGRES_DB", "POSTGRES_ZONES")
PSQL_SERVICE_SCHEMA_CONFIG = create_psql_config("POSTGRES_DB", "POSTGRES_SERVICES")
PSQL_REPORT_SCHEMA_CONFIG = create_psql_config("POSTGRES_DB", "POSTGRES_REPORT")
PSQL_RESULT_SCHEMA_CONFIG = create_psql_config("POSTGRES_DB", "POSTGRES_PROCESS")
PSQL_TRIPS_SCHEMA_CONFIG = create_psql_config("POSTGRES_DB", "POSTGRES_TRIPS")
MINIO_ANL_CONFIG = create_minio_config("MINIO_ANL_BUCKET")
MINIO_GEO_CONFIG = create_minio_config("MINIO_GEO_BUCKET")
MINIO_TS_CONFIG = create_minio_config("MINIO_TS_BUCKET")
MINIO_MD_CONFIG = create_minio_config("MINIO_MD_BUCKET")

resources = {
    "dbt": DBT_RESOURCE,
    "duckdb": DUCKDB_RESOURCE,
    "duckdb_io_manager": DuckDB_IOManager(duckdb=DuckDB(url=os.getenv("MOTHER_DUCK_SHARE_URL"))),
    "minio_anl_io_manager": MinIO_IOManager(config=MINIO_ANL_CONFIG),
    "minio_geo_io_manager": MinIO_IOManager(config=MINIO_GEO_CONFIG),
    "minio_ts_io_manager": MinIO_IOManager(config=MINIO_TS_CONFIG),
    "psql_zones_io_manager": PostgresSQL_IOManager(config=PSQL_ZONES_SCHEMA_CONFIG),
    "psql_services_io_manager": PostgresSQL_IOManager(config=PSQL_SERVICE_SCHEMA_CONFIG),
    "psql_report_io_manager": PostgresSQL_IOManager(config=PSQL_REPORT_SCHEMA_CONFIG),
    "psql_trips_io_manager": PostgresSQL_IOManager(config=PSQL_TRIPS_SCHEMA_CONFIG),
    "psql_zones_extractor": PostgresExtractor(config=PSQL_ZONES_SCHEMA_CONFIG),
    "psql_services_extractor": PostgresExtractor(config=PSQL_SERVICE_SCHEMA_CONFIG),
    "psql_report_extractor": PostgresExtractor(config=PSQL_REPORT_SCHEMA_CONFIG),
    "psql_result_extractor": PostgresExtractor(config=PSQL_RESULT_SCHEMA_CONFIG),
    }

