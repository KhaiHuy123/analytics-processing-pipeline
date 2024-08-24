
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
    ANL_LOST_PROPERTY,
    TABLE_ANL_LOST_PROPERTY,
    SECRET_ANL_LOST_PROPERTY,
    TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
    ACCESS_KEY,
    SECRET_KEY,
    SECRET_TYPE,
    MD_BUCKET,
    fill_null_for_categorical_columns,
    fill_null_for_numerical_columns,
    log_statistics,
    insert_data_into_table,
    process_dim_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    lost_property_contact_dtype_mapping,
    append_suffix_to_time_column,
    convert_format_time,
    lost_property_name_mapping,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
)
from .anl_dim_table import anl_dim_table


@asset(
    name="anl_lost_property", key_prefix=["anl", "lost_property"],
    description="download and process lost_property_contact data",
    retry_policy=retry_policy, deps=[anl_dim_table],
    compute_kind="duckdb",
)
def anl_lost_property(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [ANL_LOST_PROPERTY]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df, lost_property_name_mapping)
    df = process_columns_name(df, name_mapping, context)

    df = append_suffix_to_time_column(df)
    df = convert_format_time(df)
    dtype_mapping = create_dtype_mapping(df, lost_property_contact_dtype_mapping)
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
        create_table_from_polars_dataframe(df, SERVICES_SCHEMA, TABLE_ANL_LOST_PROPERTY, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, SERVICES_SCHEMA, TABLE_ANL_LOST_PROPERTY, context)

        insert_data_into_table(conn, data, SERVICES_SCHEMA, TABLE_ANL_LOST_PROPERTY, context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_LOST_PROPERTY,
                          dim_table=TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
                          foreign_key_column="base_id",
                          dim_columns=(
                                "base_name", "base_license_number", "base_phone_number",
                                "base_address", "base_website"),
                          dim_table_value_columns=(
                              "base_name", "base_license_number", "base_phone_number",
                              "base_address", "base_website"),
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_LOST_PROPERTY,
                          dim_table=TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
                          foreign_key_column="vehicles_id",
                          dim_columns=(
                                "type", "dmv_plate_number"),
                          dim_table_value_columns=(
                              "type", "dmv_plate_number"),
                          dim_table_id_column="id", context=context)

        create_secret(conn=conn, secret_name=SECRET_ANL_LOST_PROPERTY, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_LOST_PROPERTY,
                        file_name=TABLE_ANL_LOST_PROPERTY.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
                        file_name=TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
                        file_name=TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )

