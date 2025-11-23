
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
    SERVICES_SCHEMA,
    ANL_SHL_PERMIT,
    TABLE_ANL_SHL_PERMIT,
    TABLE_ANL_DIM_VEHICLE_TYPE,
    TABLE_ANL_DIM_BASE,
    fill_null_for_numerical_columns,
    fill_null_for_categorical_columns,
    log_statistics,
    insert_data_into_table,
    process_dim_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    shl_permit_dtype_mapping,
    shl_permit_name_mapping,
    append_suffix_to_time_column,
    convert_format_time,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    ACCESS_KEY,
    SECRET_KEY,
    SECRET_TYPE,
    MD_BUCKET,
    SECRET_ANL_SHL_INSPECT
)
from .anl_dim_table import anl_dim_table


@asset(
    name="anl_shl_permit", key_prefix=["anl", "shl_permit"],
    description="download and process shl_permit data",
    retry_policy=retry_policy,
    compute_kind="duckdb", deps=[anl_dim_table]
)
def anl_shl_permit(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [ANL_SHL_PERMIT]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df, shl_permit_name_mapping)
    df = process_columns_name(df, name_mapping, context)

    df = append_suffix_to_time_column(df)
    df = convert_format_time(df)
    dtype_mapping = create_dtype_mapping(df, shl_permit_dtype_mapping)
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
        create_table_from_polars_dataframe(df, SERVICES_SCHEMA, TABLE_ANL_SHL_PERMIT, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, SERVICES_SCHEMA, TABLE_ANL_SHL_PERMIT, context)

        insert_data_into_table(conn, data, SERVICES_SCHEMA, TABLE_ANL_SHL_PERMIT, context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_SHL_PERMIT,
                          dim_table=TABLE_ANL_DIM_VEHICLE_TYPE,
                          foreign_key_column="vehicle_type_id",
                          dim_column="vehicle_type",
                          dim_table_value_column="vehicle_type",
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_SHL_PERMIT,
                          dim_table=TABLE_ANL_DIM_BASE,
                          foreign_key_column="base_number_id",
                          dim_columns=(
                              "base_number", "base_name", "base_telephone_number", "base_address", "base_website"),
                          dim_table_value_columns=(
                              "base_number", "base_name", "base_telephone_number", "base_address", "base_website"),
                          dim_table_id_column="id", context=context)

        create_secret(conn=conn, secret_name=SECRET_ANL_SHL_INSPECT, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_SHL_PERMIT,
                        file_name=TABLE_ANL_SHL_PERMIT.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_VEHICLE_TYPE,
                        file_name=TABLE_ANL_DIM_VEHICLE_TYPE.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_BASE,
                        file_name=TABLE_ANL_DIM_BASE.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn, table_name=TABLE_ANL_SHL_PERMIT)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
