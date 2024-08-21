
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
    fill_null_for_categorical_columns,
    fill_null_for_numerical_columns,
    log_statistics,
    insert_data_into_table,
    process_dim_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    fhv_base_report_dtype_mapping,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    SERVICES_SCHEMA,
    TABLE_ANL_BASE_REPORT,
    ANL_FHV_AGG_BASE_REPORT,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_AGG_BASE_NAME,
    SECRET_ANL_BASE_REPORT,
    SECRET_TYPE,
    SECRET_KEY,
    ACCESS_KEY,
    MD_BUCKET,
)
from . anl_dim_table import anl_dim_table


@asset(
    name="anl_base_report", key_prefix=["anl", "base_report"],
    description="download and process fhv_base_aggregate_report data",
    retry_policy=retry_policy,
    compute_kind="duckdb", deps=[anl_dim_table]
)
def anl_base_report(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [ANL_FHV_AGG_BASE_REPORT]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    dtype_mapping = create_dtype_mapping(df, fhv_base_report_dtype_mapping)
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
        create_table_from_polars_dataframe(df, SERVICES_SCHEMA, TABLE_ANL_BASE_REPORT, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, SERVICES_SCHEMA, TABLE_ANL_BASE_REPORT, context)

        insert_data_into_table(conn, data, SERVICES_SCHEMA, TABLE_ANL_BASE_REPORT, context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_BASE_REPORT,
                          dim_table=TABLE_ANL_DIM_AGG_BASE_LICENSE, foreign_key_column="base_license_number_id",
                          dim_column="base_license_number", dim_table_value_column="base_license_number",
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_BASE_REPORT,
                          dim_table=TABLE_ANL_DIM_AGG_BASE_NAME, foreign_key_column="base_name_id",
                          dim_column="base_name", dim_table_value_column="base_name",
                          dim_table_id_column="id", context=context)

        create_secret(conn=conn, secret_name=SECRET_ANL_BASE_REPORT, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_BASE_REPORT,
                        file_name=TABLE_ANL_BASE_REPORT.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_AGG_BASE_LICENSE,
                        file_name=TABLE_ANL_DIM_AGG_BASE_LICENSE.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_AGG_BASE_NAME,
                        file_name=TABLE_ANL_DIM_AGG_BASE_NAME.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
