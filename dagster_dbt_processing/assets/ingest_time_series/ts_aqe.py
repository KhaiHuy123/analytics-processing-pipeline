
import polars as pl
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    process_columns_name,
    create_name_mapping,
    TS_AQE,
    ENV_SCHEMA,
    TABLE_TS_AQE,
    convert_data_type,
    create_dtype_mapping,
    numerical_data_types,
    log_statistics,
    insert_data_into_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    get_data_type_groups,
    fill_null_for_numerical_columns,
    fill_null_for_categorical_columns,
    categorical_data_types,
    ny_nta_dtype_mapping,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    MD_BUCKET,
    ACCESS_KEY,
    SECRET_KEY,
    SECRET_TYPE,
    SECRET_TS_AQE
)
from ..ingest_setup import ingest_setup


@asset(
    name="ts_aqe", key_prefix=["ts", "aqe"],
    description="download and process time-series AQE data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def ts_aqe(context: AssetExecutionContext, duckdb: DuckDBResource):
    df = pl.read_csv(TS_AQE)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    dtype_mapping = create_dtype_mapping(df, ny_nta_dtype_mapping)
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
        create_table_from_polars_dataframe(df, ENV_SCHEMA, TABLE_TS_AQE, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, ENV_SCHEMA, TABLE_TS_AQE, context)

        insert_data_into_table(conn, data, ENV_SCHEMA, TABLE_TS_AQE, context)

        create_secret(conn=conn, secret_name=SECRET_TS_AQE, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=ENV_SCHEMA, table=TABLE_TS_AQE,
                        file_name=TABLE_TS_AQE.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn, table_name=TABLE_TS_AQE)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
