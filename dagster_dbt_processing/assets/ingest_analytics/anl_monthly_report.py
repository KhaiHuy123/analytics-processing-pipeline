
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
    ANL_MONTHLY_REPORT,
    REPORT_SCHEMA,
    TABLE_ANL_MONTHLY_REPORT,
    SECRET_ANL_MONTHLY_REPORT,
    ACCESS_KEY,
    SECRET_TYPE,
    SECRET_KEY,
    MD_BUCKET,
    fill_null_for_numerical_columns,
    fill_null_for_categorical_columns,
    log_statistics,
    insert_data_into_table,
    retry_policy,
    create_column_value_dict,
    create_table_from_polars_dataframe,
    mapping_polars_dtype,
    clean_dataframe,
    monthly_report_dtype_mapping,
    edit_columns,
    review_null_value,
    review_schema,
    view_raw_data,
    fetch_metadata,
    create_secret,
    copy_file_to_s3
)
from ..ingest_setup import ingest_setup


@asset(
    name="anl_monthly_report", key_prefix=["anl", "monthly_report"],
    description="download and monthly_report data",
    retry_policy=retry_policy, deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def anl_monthly_report(context: AssetExecutionContext, duckdb: DuckDBResource):
    df = pl.read_csv(ANL_MONTHLY_REPORT)

    review_schema(df, context)

    name_mapping = create_name_mapping(df)
    df = process_columns_name(df, name_mapping, context)

    cleaned_df = clean_dataframe(df[edit_columns])
    df = df.with_columns([cleaned_df[column] for column in edit_columns])

    dtype_mapping = create_dtype_mapping(df, monthly_report_dtype_mapping)
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
        create_table_from_polars_dataframe(df, REPORT_SCHEMA, TABLE_ANL_MONTHLY_REPORT, mapping_polars_dtype,
                                           conn, context)
        view_raw_data(data, REPORT_SCHEMA, TABLE_ANL_MONTHLY_REPORT, context)

        insert_data_into_table(conn, data, REPORT_SCHEMA, TABLE_ANL_MONTHLY_REPORT, context)

        create_secret(conn=conn, secret_name=SECRET_ANL_MONTHLY_REPORT, secret_type=SECRET_TYPE,
                      access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, context=context)

        copy_file_to_s3(conn=conn, bucket_name=MD_BUCKET,
                        schema=REPORT_SCHEMA, table=TABLE_ANL_MONTHLY_REPORT,
                        file_name=TABLE_ANL_MONTHLY_REPORT.lower().replace("_", ""), context=context)

        metadata = fetch_metadata(conn)

        metadata_mapping = create_column_value_dict(metadata)

        context.add_output_metadata(
            metadata=metadata_mapping
        )
