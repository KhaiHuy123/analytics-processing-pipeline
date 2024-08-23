
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource

from ..constant import (
    ZONES_SCHEMA,
    TABLE_GEO_TAXI_ZONES,
    MD_BUCKET,
    execute_query_and_log_performance,
    query_table,
    download_data_from_minio,
)
from ..ingest_geometry import geo_taxi_zone
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="geo_taxi_zone_fact_table", key_prefix=["geo_taxi_zone", "fact_table"],
    description="collect geo_taxi_zone_fact_table", compute_kind="duckdb",
    deps=[geo_taxi_zone.geo_taxi_zone]
)
def fact_table_geo_taxi_zone(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_GEO_TAXI_ZONES}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "geo_taxi_zone_fact_table": AssetIn(key_prefix=["geo_taxi_zone", "fact_table"]),
    },
    name=f"{TABLE_GEO_TAXI_ZONES}", description="execute querying for geo_taxi_zone",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{ZONES_SCHEMA}"], compute_kind="duckdb",
)
def preview_geo_taxi_zone(context,
                          geo_taxi_zone_fact_table: SQL):
    
    execute_query_and_log_performance(context, geo_taxi_zone_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{ZONES_SCHEMA}.{TABLE_GEO_TAXI_ZONES}")


@asset(
    name=f"{TABLE_GEO_TAXI_ZONES.lower()}",
    key_prefix=["minio", "geo_nta"],
    description=f"download file from MinIO for table {TABLE_GEO_TAXI_ZONES}",
    compute_kind="MinIO",
    deps=[preview_geo_taxi_zone],
)
def download_geo_taxi_zone(context: AssetExecutionContext):
    download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                             object_name=TABLE_GEO_TAXI_ZONES.lower(), context=context, raw_data=False)
