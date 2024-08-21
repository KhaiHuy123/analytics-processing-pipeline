
from dagster import asset, AssetIn, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    ZONES_SCHEMA,
    TABLE_GEO_NTA,
    MD_BUCKET,
    execute_query_and_log_performance,
    query_table,
    download_data_from_minio,
)
from ..ingest_geometry import geo_nta
from ...resources.duckdb_io_manager import SQL
from ...resources import MINIO_MD_CONFIG


@asset(
    name="geo_nta_fact_table", key_prefix=["geo_nta", "fact_table"],
    description="collect geo_nta_fact_table", compute_kind="duckdb",
    deps=[geo_nta.geo_nta]
)
def fact_table_geo_nta(duckdb: DuckDBResource) -> Output[SQL]:
    table_name = f"{ZONES_SCHEMA}.{TABLE_GEO_NTA}"
    sql_obj: SQL = query_table(table_name, duckdb)
    return Output(value=sql_obj)


@asset(
    ins={
        "geo_nta_fact_table": AssetIn(key_prefix=["geo_nta", "fact_table"]),
    },
    name=f"{TABLE_GEO_NTA}", description="execute querying for geo_nta",
    required_resource_keys={"duckdb_io_manager"}, key_prefix=[f"{ZONES_SCHEMA}"], compute_kind="duckdb",
)
def preview_geo_nta(context,
                    geo_nta_fact_table: SQL):
    
    execute_query_and_log_performance(context, geo_nta_fact_table, context.resources.duckdb_io_manager,
                                      query_name=f"{ZONES_SCHEMA}.{TABLE_GEO_NTA}")


@asset(
    name=f"{TABLE_GEO_NTA.lower()}",
    key_prefix=["minio", "geo_nta"],
    description=f"download file from MinIO for table {TABLE_GEO_NTA}",
    compute_kind="MinIO",
    deps=[preview_geo_nta],
)
def download_geo_nta(context: AssetExecutionContext):
    download_data_from_minio(config=MINIO_MD_CONFIG, bucket_name=MD_BUCKET,
                             object_name=TABLE_GEO_NTA.lower(), context=context, raw_data=False)
