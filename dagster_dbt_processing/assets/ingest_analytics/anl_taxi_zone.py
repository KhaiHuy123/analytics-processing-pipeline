
import polars as pl
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    get_data_type_groups,
    numerical_data_types,
    categorical_data_types,
    process_columns_name,
    convert_data_type,
    create_name_mapping,
    create_dtype_mapping,
    ANL_TAXI_ZONE,
    TABLE_ANL_TAXI_ZONES,
    ZONES_SCHEMA,
    fill_null_for_categorical_columns,
    fill_null_for_numerical_columns,
    log_statistics,
    insert_data_into_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    ACCESS_KEY,
    SECRET_KEY,
    MD_BUCKET,
    SECRET_TYPE,
    SECRET_ANL_TAXI_ZONE,
)
from ..ingest_setup import ingest_setup


@asset(
    name="anl_taxi_zone", key_prefix=["anl", "taxi_zone"],
    description="download and process taxi_zone_lookup data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def anl_taxi_zone(context: AssetExecutionContext, duckdb: DuckDBResource):
    df = pl.read_csv(ANL_TAXI_ZONE)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    dtype_mapping = create_dtype_mapping(df)
    df = convert_data_type(df, dtype_mapping, context)

    data_type_groups = get_data_type_groups(df)
    df = fill_null_for_numerical_columns(df, data_type_groups, numerical_data_types, context)
    df = fill_null_for_categorical_columns(df, data_type_groups, categorical_data_types, context)

    review_null_value(df, context)

    log_statistics(df, numerical_data_types, context)

    using_arrow = True
    if using_arrow:
        data = df.to_arrow()
    else:
        data = df.to_pandas()

    with duckdb.get_connection() as conn:
        create_table_from_polars_dataframe(df, ZONES_SCHEMA, TABLE_ANL_TAXI_ZONES, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, ZONES_SCHEMA, TABLE_ANL_TAXI_ZONES, context)

        insert_data_into_table(conn, data, ZONES_SCHEMA, TABLE_ANL_TAXI_ZONES, context)

        create_secret(conn=conn, secret_name=SECRET_ANL_TAXI_ZONE, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=ZONES_SCHEMA, table=TABLE_ANL_TAXI_ZONES,
                        file_name=TABLE_ANL_TAXI_ZONES.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn, table_name=TABLE_ANL_TAXI_ZONES)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
