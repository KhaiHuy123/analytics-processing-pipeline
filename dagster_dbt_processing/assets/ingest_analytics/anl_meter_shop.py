
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    load_data_from_api,
    get_data_type_groups,
    numerical_data_types,
    categorical_data_types,
    process_columns_name,
    convert_data_type,
    create_name_mapping,
    create_dtype_mapping,
    TABLE_ANL_METER_SHOPS,
    SECRET_ANL_METER_SHOP,
    ANL_METER_SHOP,
    REPORT_SCHEMA,
    ACCESS_KEY,
    SECRET_KEY,
    SECRET_TYPE,
    MD_BUCKET,
    fill_null_for_numerical_columns,
    fill_null_for_categorical_columns,
    log_statistics,
    insert_data_into_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    meter_shop_dtype_mapping,
    append_suffix_to_time_column,
    convert_format_time,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3
)
from ..ingest_setup import ingest_setup


@asset(
    name="anl_meter_shop", key_prefix=["anl", "meter_shop"],
    description="download and process meter_shop data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def anl_meter_shop(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [ANL_METER_SHOP]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    df = append_suffix_to_time_column(df, column="time")
    df = convert_format_time(df, column="time")
    dtype_mapping = create_dtype_mapping(df, meter_shop_dtype_mapping)
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
        create_table_from_polars_dataframe(df, REPORT_SCHEMA, TABLE_ANL_METER_SHOPS, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, REPORT_SCHEMA, TABLE_ANL_METER_SHOPS, context)

        insert_data_into_table(conn, data, REPORT_SCHEMA, TABLE_ANL_METER_SHOPS, context)

        create_secret(conn=conn, secret_name=SECRET_ANL_METER_SHOP, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=REPORT_SCHEMA, table=TABLE_ANL_METER_SHOPS,
                        file_name=TABLE_ANL_METER_SHOPS.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn, table_name=TABLE_ANL_METER_SHOPS)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
