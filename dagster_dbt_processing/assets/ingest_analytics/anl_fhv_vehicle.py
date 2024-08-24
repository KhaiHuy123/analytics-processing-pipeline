
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
    fhv_vehicle_name_mapping,
    fhv_vehicle_dtype_mapping,
    convert_format_time,
    append_suffix_to_time_column,
    review_schema,
    review_null_value,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3,
    SERVICES_SCHEMA,
    ANL_FHV_VEHICLE,
    TABLE_ANL_FHV_VEHICLE,
    TABLE_ANL_DIM_BASE,
    TABLE_ANL_DIM_VEHICLE_NAME,
    TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
    SECRET_ANL_FHV_VEHICLE,
    SECRET_TYPE,
    SECRET_KEY,
    ACCESS_KEY,
    MD_BUCKET,
)
from .anl_dim_table import anl_dim_table


@asset(
    name="anl_fhv_vehicles", key_prefix=["anl", "fhv_vehicles"],
    description="download and process fhv_vehicle data",
    retry_policy=retry_policy, deps=[anl_dim_table],
    compute_kind="duckdb",
)
def anl_fhv_vehicles(context: AssetExecutionContext, duckdb: DuckDBResource):
    api_list = [ANL_FHV_VEHICLE]
    df = load_data_from_api(api_list)

    review_schema(df, context)

    name_mapping = create_name_mapping(df, fhv_vehicle_name_mapping)
    df = process_columns_name(df, name_mapping, context)

    df = append_suffix_to_time_column(df)
    df = convert_format_time(df)
    dtype_mapping = create_dtype_mapping(df, fhv_vehicle_dtype_mapping)
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
        create_table_from_polars_dataframe(df, SERVICES_SCHEMA, TABLE_ANL_FHV_VEHICLE, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, SERVICES_SCHEMA, TABLE_ANL_FHV_VEHICLE, context)

        insert_data_into_table(conn, data, SERVICES_SCHEMA, TABLE_ANL_FHV_VEHICLE, context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_FHV_VEHICLE,
                          dim_table=TABLE_ANL_DIM_BASE,
                          foreign_key_column="base_number_id",
                          dim_columns=(
                              "base_number", "base_name", "base_telephone_number", "base_address", "base_website"),
                          dim_table_value_columns=(
                              "base_number", "base_name", "base_telephone_number", "base_address", "base_website"),
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_FHV_VEHICLE,
                          dim_table=TABLE_ANL_DIM_VEHICLE_NAME,
                          foreign_key_column="name_id",
                          dim_column="name",
                          dim_table_value_column="name",
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_FHV_VEHICLE,
                          dim_table=TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
                          foreign_key_column="wheelchair_accessible_id",
                          dim_column="wheelchair_accessible",
                          dim_table_value_column="wheelchair_accessible",
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_FHV_VEHICLE,
                          dim_table=TABLE_ANL_DIM_VEH,
                          foreign_key_column="veh_id",
                          dim_column="veh",
                          dim_table_value_column="veh",
                          dim_table_id_column="id", context=context)

        process_dim_table(conn=conn, schema_name=SERVICES_SCHEMA, fact_table=TABLE_ANL_FHV_VEHICLE,
                          dim_table=TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
                          foreign_key_column="vehicle_year_id",
                          dim_column="vehicle_year",
                          dim_table_value_column="vehicle_year",
                          dim_table_id_column="id", context=context)

        create_secret(conn=conn, secret_name=SECRET_ANL_FHV_VEHICLE, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_FHV_VEHICLE,
                        file_name=TABLE_ANL_FHV_VEHICLE.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_BASE,
                        file_name=TABLE_ANL_DIM_BASE.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_VEHICLE_NAME,
                        file_name=TABLE_ANL_DIM_VEHICLE_NAME.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
                        file_name=TABLE_ANL_DIM_WHEELCHAIR_ACCESS.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_VEH,
                        file_name=TABLE_ANL_DIM_VEH.lower().replace("_", ""), context=context)

        copy_file_to_s3(conn=conn,  bucket_name=MD_BUCKET,
                        schema=SERVICES_SCHEMA, table=TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
                        file_name=TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn=conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
