
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    load_data_from_api,
    create_name_mapping,
    process_columns_name,
    GEO_NY_NTA,
    ZONES_SCHEMA,
    TABLE_GEO_NTA,
    numerical_data_types,
    log_statistics,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    insert_data_into_table,
    create_dtype_mapping,
    convert_data_type,
    ny_nta_dtype_mapping,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    ACCESS_KEY,
    SECRET_KEY,
    SECRET_TYPE,
    SECRET_GEO_NTA,
    MD_BUCKET
)
from ..ingest_setup import ingest_setup


@asset(
    name="geo_nta", key_prefix=["geo", "nta"],
    description="download and process NY_NTA data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def geo_nta(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [GEO_NY_NTA]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    dtype_mapping = create_dtype_mapping(df, ny_nta_dtype_mapping)
    df = convert_data_type(df, dtype_mapping, context)

    review_null_value(df, context)

    log_statistics(df, numerical_data_types, context)

    using_arrow = True
    if using_arrow:
        data = df.to_arrow()
    else:
        data = df.to_pandas()

    with duckdb.get_connection() as conn:
        create_table_from_polars_dataframe(df, ZONES_SCHEMA, TABLE_GEO_NTA, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, ZONES_SCHEMA, TABLE_GEO_NTA, context)

        insert_data_into_table(conn, data, ZONES_SCHEMA, TABLE_GEO_NTA, context)

        create_secret(conn=conn, secret_name=SECRET_GEO_NTA, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=ZONES_SCHEMA, table=TABLE_GEO_NTA,
                        file_name=TABLE_GEO_NTA.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
