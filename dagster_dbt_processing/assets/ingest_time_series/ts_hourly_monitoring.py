
import polars as pl
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    process_columns_name,
    create_name_mapping,
    TS_HOURLY_MONITORING,
    ENV_SCHEMA,
    TABLE_TS_HOURLY_MONITORING,
    numerical_data_types,
    log_statistics,
    insert_data_into_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    create_dtype_mapping,
    convert_data_type,
    get_data_type_groups,
    fill_null_for_numerical_columns,
    fill_null_for_categorical_columns,
    categorical_data_types,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    ACCESS_KEY,
    SECRET_TYPE,
    SECRET_KEY,
    MD_BUCKET,
    SECRET_TS_HOURLY_MONITORING
)
from ..ingest_setup import ingest_setup


@asset(
    name="ts_hourly_monitoring", key_prefix=["ts", "hourly_monitoring"],
    description="download and process time-series hourly_monitoring AQE data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def ts_hourly_monitoring(context: AssetExecutionContext, duckdb: DuckDBResource):
    df = pl.read_csv(TS_HOURLY_MONITORING)

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
        create_table_from_polars_dataframe(df, ENV_SCHEMA, TABLE_TS_HOURLY_MONITORING, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, ENV_SCHEMA, TABLE_TS_HOURLY_MONITORING, context)

        insert_data_into_table(conn, data, ENV_SCHEMA, TABLE_TS_HOURLY_MONITORING, context)

        create_secret(conn=conn, secret_name=SECRET_TS_HOURLY_MONITORING, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=ENV_SCHEMA, table=TABLE_TS_HOURLY_MONITORING,
                        file_name=TABLE_TS_HOURLY_MONITORING.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
