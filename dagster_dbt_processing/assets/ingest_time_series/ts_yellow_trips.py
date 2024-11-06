
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    load_data_from_online_file,
    numerical_data_types,
    process_columns_name,
    convert_data_type,
    create_name_mapping,
    create_dtype_mapping,
    API_LIST_YELLOW,
    TRIPS_SCHEMA,
    TABLE_TS_YELLOW_TRIPS,
    log_statistics,
    insert_data_into_table,
    split_chunks_api_list,
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
    SECRET_KEY,
    SECRET_TYPE,
    ACCESS_KEY,
    MD_BUCKET,
    SECRET_TS_YELLOW_TRIPS,
    process_static_table,
    TABLE_TS_DIM_PAYMENT_TYPE,
    TABLE_TS_DIM_RATECODE,
    TABLE_TS_DIM_VENDOR
)
from .ts_dim_table import ts_dim_table


@asset(
    name="ts_yellow_trips", key_prefix=["ts", "yellow_trips"],
    description="download and process yellow_trips data",
    retry_policy=retry_policy,
    compute_kind="duckdb", deps=[ts_dim_table]
)
def ts_yellow_trips(context: AssetExecutionContext, duckdb: DuckDBResource):
    context.log.info("API List")
    context.log.info(API_LIST_YELLOW)
    sub_lists = split_chunks_api_list(API_LIST_YELLOW, chunk_size=1)

    with duckdb.get_connection() as conn:
        for idx, sub_list in enumerate(sub_lists):
            df = load_data_from_online_file(sub_list, context)

            review_schema(df, context)

            name_mapping = create_name_mapping(df)
            df = process_columns_name(df, name_mapping, context)

            dtype_mapping = create_dtype_mapping(df)
            df = convert_data_type(df, dtype_mapping, context)

            review_null_value(df, context)

            log_statistics(df, numerical_data_types, context)

            using_arrow = True
            if using_arrow:
                data = df.to_arrow()
            else:
                data = df.to_pandas()

            create_table_from_polars_dataframe(df, TRIPS_SCHEMA, TABLE_TS_YELLOW_TRIPS, mapping_polars_dtype,
                                               conn, context)
            view_raw_data(data, TRIPS_SCHEMA, TABLE_TS_YELLOW_TRIPS, context)

            insert_data_into_table(conn, data, TRIPS_SCHEMA, TABLE_TS_YELLOW_TRIPS, context)

            process_static_table(conn=conn, fact_schema=TRIPS_SCHEMA, fact_table=TABLE_TS_YELLOW_TRIPS,
                                 dim_schema=TRIPS_SCHEMA, dim_table=TABLE_TS_DIM_PAYMENT_TYPE,
                                 dim_column="payment_type", dim_table_id_column="id", context=context)

            process_static_table(conn=conn, fact_schema=TRIPS_SCHEMA, fact_table=TABLE_TS_YELLOW_TRIPS,
                                 dim_schema=TRIPS_SCHEMA, dim_table=TABLE_TS_DIM_RATECODE,
                                 dim_column="ratecodeid", dim_table_id_column="id", context=context)

            process_static_table(conn=conn, fact_schema=TRIPS_SCHEMA, fact_table=TABLE_TS_YELLOW_TRIPS,
                                 dim_schema=TRIPS_SCHEMA, dim_table=TABLE_TS_DIM_VENDOR,
                                 dim_column="vendorid", dim_table_id_column="id", context=context)

            create_secret(conn=conn, secret_name=SECRET_TS_YELLOW_TRIPS, secret_type=SECRET_TYPE,
                          access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

            copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                            schema=TRIPS_SCHEMA, table=TABLE_TS_YELLOW_TRIPS,
                            file_name=TABLE_TS_YELLOW_TRIPS.lower().replace("_", ""), context=context, idx=idx)

            copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                            schema=TRIPS_SCHEMA, table=TABLE_TS_DIM_PAYMENT_TYPE,
                            file_name=TABLE_TS_DIM_PAYMENT_TYPE.lower().replace("_", ""), context=context, idx=idx)

            copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                            schema=TRIPS_SCHEMA, table=TABLE_TS_DIM_RATECODE,
                            file_name=TABLE_TS_DIM_RATECODE.lower().replace("_", ""), context=context, idx=idx)

            copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                            schema=TRIPS_SCHEMA, table=TABLE_TS_DIM_VENDOR,
                            file_name=TABLE_TS_DIM_VENDOR.lower().replace("_", ""), context=context, idx=idx)
            context.log.info(f"Processing {sub_list} finished _ idx : {idx}")

        metadata = fetch_metadata(conn=conn, table_name=TABLE_TS_YELLOW_TRIPS)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
