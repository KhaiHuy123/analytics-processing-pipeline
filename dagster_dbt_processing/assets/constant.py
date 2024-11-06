
import datetime
import io
import os
import time
import re
import numpy as np
import requests
import polars as pl
import geopandas as gpd
import pandas as pd
import urllib3
from io import BytesIO
from collections import Counter
from duckdb import duckdb as db
from minio import Minio, S3Error
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig
from sklearn.experimental.enable_iterative_imputer import IterativeImputer
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, VotingRegressor
from sklearn.neighbors import KNeighborsRegressor
from typing import List
from dagster import AssetExecutionContext, OpExecutionContext, RetryPolicy, Backoff, op
from dagster_duckdb import DuckDBResource
from ..resources.duckdb_io_manager import SQL, DuckDB_IOManager
from ..resources.minio_io_manager import MinIOHelper


# functions
def generate_link(data_type, year, month, extensions):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{data_type}_{year}-{str(month).zfill(2)}.{extensions}"


def generate_links_for_data_type(data_type, start_years, start_months, end_years, end_months, extensions):
    links = []
    for year in range(start_years, end_years + 1):
        for month in range(start_months, 13 if year < end_years else end_months + 1):
            link = generate_link(data_type, year, month, extensions)
            links.append(link)
    return links


def generate_all_links(data_type, start_years, start_months, end_years, end_months, extensions):
    all_links = {}
    for dtype in data_type:
        links = generate_links_for_data_type(dtype, start_years, start_months, end_years, end_months, extensions)
        all_links[dtype] = links
    return all_links


def split_chunks_api_list(api_list: List[str], chunk_size: int = 100) -> List[List[str]]:
    if chunk_size > len(api_list):
        raise ValueError("Chunk size need to be less or equal to length of api list")
    sub_lists = []
    for i in range(0, len(api_list), chunk_size):
        sub_list = api_list[i:i + chunk_size]
        sub_lists.append(sub_list)
    return sub_lists


def fetch_api_limit_offset(api_url, offset=0, limit=1000) -> pl.DataFrame:
    try:
        url = f"{api_url}?$limit={limit}&$offset={offset}"
        response = requests.get(url)
        if response.status_code == 200:
            df = pl.DataFrame(response.json())
            return df
        else:
            print(f"Failed to fetch data from API: {api_url} (Status code: {response.status_code})")
            return pl.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from API: {api_url} (Error: {e})")
        return pl.DataFrame()


def load_data_from_api(api_list) -> pl.DataFrame:
    combined_df = pl.DataFrame()
    for api_url in api_list:
        offset = 0
        limit = 1000
        while True:
            df_chunk = fetch_api_limit_offset(api_url, offset=offset, limit=limit)
            if df_chunk.shape[0] == 0:
                break
            combined_df = pl.concat([combined_df, df_chunk], how="diagonal", parallel=True)
            offset += limit
    return combined_df


def load_data_from_online_file(api_list, context: AssetExecutionContext) -> pl.DataFrame:
    dataframes = []
    http = urllib3.PoolManager()

    for api_url in api_list:
        try:
            context.log.info(f"Fetching data from: {api_url}")
            if isinstance(api_url, str):
                response = http.request('GET', api_url)
                if response.status == 200:
                    data = BytesIO(response.data)
                    df = pl.read_parquet(data)
                    dataframes.append(df)
                else:
                    context.log.info(f"Failed to fetch data from API: {api_url} (Status code: {response.status})")
            else:
                context.log.info(f"Invalid URL format: {api_url}")
        except urllib3.exceptions.HTTPError as err:
            context.log.info(f"Failed to fetch data from API: {api_url} (Error: {err})")
        except Exception as e:
            context.log.info(f"An error occurred: {e}")

    if dataframes:
        return pl.concat(dataframes, how="vertical", parallel=True)
    else:
        return pl.DataFrame()


def get_data_type_groups(df: pl.DataFrame) -> dict:
    data_type = df.schema
    numeric_data_types = {pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                          pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32}
    cate_data_types = [pl.Utf8, pl.String, pl.Object]
    group_dict = {"Numeric": [], "Category": [], **{}}

    for col, dtype in data_type.items():
        if dtype in numeric_data_types:
            group_dict["Numeric"].append(col)
        elif dtype in cate_data_types:
            group_dict["Category"].append(col)
        else:
            group_dict[dtype] = []
            group_dict[dtype].append(col)
    return group_dict


def review_schema(df: pl.DataFrame, context: AssetExecutionContext):
    context.log.info("Schema : ")
    context.log.info(df.schema)


def view_raw_data(data, schema, table, context):
    context.log.info(f"View Raw Data : {data}")
    context.log.info(f"Table to insert : {schema}.{table}")


def review_null_value(df: pl.DataFrame, context: AssetExecutionContext):
    context.log.info("Check NULL in Data : ")
    context.log.info(df.null_count().to_arrow())


def fetch_metadata(conn, table_name):
    metadata = conn.execute(
        f"SELECT * FROM duckdb_tables() WHERE table_name = '{table_name}'"
    ).fetchdf()
    return metadata


def process_numerical_dtype(df: pl.DataFrame, num_cols, context: AssetExecutionContext):
    try:
        recommended_result = recommended_value_filled(df, num_cols, context)
        processed_df = recommended_result
        context.log.info(f"Filled NULL for all numerical columns successfully ")
        return processed_df
    except Exception as e:
        context.log.info(f"Error occurred while processing numerical columns as : {e}")


def process_categorical_dtype(df: pl.DataFrame, cate_cols, context: AssetExecutionContext, fill_value="UNKNOWN"):
    try:
        for col in cate_cols:
            df = df.with_columns(pl.col(col).fill_null(fill_value).alias(col))
        context.log.info(f"Fill NULL for all categorical columns successfully")
        return df
    except Exception as e:
        context.log.info(f"Error occurred while processing categorical columns as : {e}")


def clean_name(name: str):
    name = name.lower()
    clean_col_name = re.sub(r'[\\*@.#$%^&()!]', '', name)
    clean_col_name = clean_col_name.replace(" ", "_")
    clean_col_name = clean_col_name.replace("/", "_")
    return clean_col_name


def create_name_mapping(df: pl.DataFrame, custom_mapping: dict[str, str] = None) -> dict[str, str]:
    mapping = {}
    schema = df.schema
    for col_name in schema.keys():
        if custom_mapping and (col_name in custom_mapping):
            cleaned_name = custom_mapping[col_name]
        else:
            cleaned_name = clean_name(col_name)
        mapping[col_name] = cleaned_name
    return mapping


def create_dtype_mapping(df: pl.DataFrame, custom_mapping: dict[str, pl.DataType] = None) -> dict[str, pl.DataType]:
    mapping = {}
    schema = df.schema
    for col_name in schema.keys():
        if custom_mapping and col_name in custom_mapping:
            dtype = custom_mapping[col_name]
        else:
            dtype = schema[col_name]
        mapping[col_name] = dtype
    return mapping


def process_columns_name(df: pl.DataFrame, mapping: dict[str, str], context: AssetExecutionContext):
    df = df.rename(mapping=mapping)
    context.log.info(f"Renamed columns")
    return df


def clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    for column in df.columns:
        df = df.with_columns(
            pl.when((pl.col(column).str.contains(r"-")) & (pl.col(column).str.contains(r"%")))
              .then(pl.lit("0.0"))
              .otherwise(pl.col(column))
              .alias(column)
        )
        df = df.with_columns(
            pl.col(column).str.replace_all("-", "").alias(column)
        )
        df = df.with_columns(
            pl.col(column).str.replace_all(",", "").alias(column)
        )
        df = df.with_columns(
            pl.when(pl.col(column) == "")
              .then(pl.lit(None).cast(pl.String))
              .otherwise(pl.col(column))
              .alias(column)
        )
        df = df.with_columns(
            pl.when(pl.col(column).str.contains(r"%"))
              .then(pl.col(column).str.replace("%", "").cast(pl.Float64) / 100)
              .otherwise(pl.col(column))
              .alias(column)
        )
    return df


def cast_dtype(df: pl.DataFrame, columns, dtype):
    for column in columns:
        df = df.with_columns(
            pl.col(column).cast(dtype, strict=False).alias(column)
        )


def get_distribution_type(skew: float) -> str:
    if skew == 0:
        return "balanced"
    elif skew > 0:
        return "right-skewed"
    else:
        return "left-skewed"


def get_simple_fill_value(df: pl.DataFrame, distribution_type: str) -> pl.DataFrame:
    impute_strategy = {
        "balanced": SimpleImputer(strategy="mean"),
        "right-skewed": SimpleImputer(strategy="median"),
        "left-skewed": SimpleImputer(strategy="median"),
    }
    if distribution_type not in impute_strategy:
        raise ValueError(f"Invalid distribution type: {distribution_type}")
    filled_df = impute_strategy[distribution_type]
    pd_df = pd.DataFrame(filled_df.fit_transform(df), columns=df.columns)
    res = pl.DataFrame(pd_df, schema=df.schema)
    return res


def get_knn_fill_value(df: pl.DataFrame, n_neighbors=10) -> pl.DataFrame:
    df_pd = df.to_pandas()
    knn_imputed = KNNImputer(n_neighbors=n_neighbors, weights="distance")
    imputed_df = knn_imputed.fit_transform(df_pd)
    res = pl.DataFrame(imputed_df, schema=df.schema)
    return res


def get_mice_fill_value(df: pl.DataFrame, max_iter=100, linear=True, non_linear=True) -> pl.DataFrame:
    df_pd = df.to_pandas()
    if linear:
        lr = LinearRegression()
        estimator = IterativeImputer(estimator=lr, max_iter=max_iter)
    elif non_linear:
        est = RandomForestRegressor()
        estimator = IterativeImputer(estimator=est, max_iter=max_iter)
    else:
        rfr = RandomForestRegressor(n_estimators=100)
        kr = KNeighborsRegressor(n_neighbors=10, weights="distance")
        voting_regressor = VotingRegressor(estimators=[('rfr', rfr), ('kr', kr)])
        estimator = IterativeImputer(estimator=voting_regressor, max_iter=max_iter)
    imputed_df = estimator.fit_transform(df_pd)
    res = pl.DataFrame(imputed_df, schema=df.schema)
    return res


def get_missing_data_ratio(df: pl.DataFrame) -> float:
    missing_counts = df.null_count()
    total_counts = df.shape[0] * df.shape[1]
    missing_ratio = (missing_counts / total_counts)
    return missing_ratio.to_pandas().sum().sum()


def find_most_frequent_value(data_list):
    if not data_list:
        return None
    mode_value = Counter(data_list).most_common(1)[0][0]
    return mode_value


def overall_correlation(data: pd.DataFrame, threshold=0.5):
    corr_matrix = data.corr().abs()
    np.fill_diagonal(corr_matrix.values, np.nan)
    high_corr_pairs_ratio = (corr_matrix > threshold).sum().sum() / (data.shape[1] * (data.shape[1] - 1))
    return high_corr_pairs_ratio


def recommended_value_filled(df: pl.DataFrame, col_names, context: AssetExecutionContext,
                             max_iter=100, n_neighbors=10) -> pl.DataFrame:
    distribution_list = []
    for col_name in col_names:
        skew = df[col_name].skew()
        distribution_type = get_distribution_type(skew)
        distribution_list.append(distribution_type)

    trend_of_distribution = find_most_frequent_value(distribution_list)
    missing_ratio = get_missing_data_ratio(df)
    corr_anl = correlation_analysis(df)
    pearson_high_corr_ratio: float = corr_anl.get("pearson_high_corr_ratio")
    pearson_low_corr_ratio: float = corr_anl.get("pearson_low_corr_ratio")
    spearman_high_corr_ratio: float = corr_anl.get("spearman_high_corr_ratio")
    spearman_low_corr_ratio: float = corr_anl.get("spearman_low_corr_ratio")
    pearson_high_corr_pairs = corr_anl.get("pearson_high_corr_pairs")
    pearson_low_corr_pairs = corr_anl.get("pearson_low_corr_pairs")
    spearman_high_corr_pairs = corr_anl.get("spearman_high_corr_pairs")
    spearman_low_corr_pairs = corr_anl.get("spearman_low_corr_pairs")

    context.log.info(f"- Trend of Distribution (over all columns) : {trend_of_distribution}")
    context.log.info(f"- Missing Ratio : {round(missing_ratio * 100, 5)} % ")
    context.log.info(f"- High pearson_corr ratio : {round(pearson_high_corr_ratio * 100, 5)} %")
    context.log.info(f"- High spearman_corr ratio : {round(spearman_high_corr_ratio * 100, 5)} %")
    context.log.info(f"- Low pearson_corr ratio : {round(pearson_low_corr_ratio * 100, 5)} %")
    context.log.info(f"- Low spearman_corr ratio : {round(spearman_low_corr_ratio * 100, 5)} %")
    context.log.info(f"- Pearson_high_corr_pairs : {pearson_high_corr_pairs}")
    context.log.info(f"- Pearson_low_corr_pairs : {pearson_low_corr_pairs}")
    context.log.info(f"- Spearman_high_corr_pairs : {spearman_high_corr_pairs}")
    context.log.info(f"- Spearman_low_corr_pairs : {spearman_low_corr_pairs}")

    if trend_of_distribution == "balanced" and missing_ratio <= 0.15:
        context.log.info("- Using Simple Strategy Imputation for filling missing data")
        df_recommend = get_simple_fill_value(df, trend_of_distribution)
    elif pearson_high_corr_ratio >= 0.40 and spearman_high_corr_ratio <= 0.20:
        context.log.info("- Using MICE Imputation (high pearson_corr) for filling missing data")
        df_recommend = get_mice_fill_value(df, max_iter, linear=True, non_linear=False)
    elif spearman_high_corr_ratio >= 0.40 and pearson_high_corr_ratio <= 0.20:
        context.log.info("- Using MICE Imputation (high spearman_corr) for filling missing data")
        df_recommend = get_mice_fill_value(df, max_iter, linear=False, non_linear=True)
    elif spearman_high_corr_ratio >= 0.40 and pearson_high_corr_ratio >= 0.40:
        context.log.info("- Using MICE Imputation (voting regressor) for filling missing data")
        df_recommend = get_mice_fill_value(df, max_iter, linear=False, non_linear=False)
    else:
        context.log.info("- Using KNN Imputation for filling missing data")
        df_recommend = get_knn_fill_value(df, n_neighbors)
    return df_recommend


def calculate_and_log_statistics(df: pl.DataFrame, context: AssetExecutionContext, column_name, whisker_cf=1.5):
    mean = df[column_name].mean()
    median = df[column_name].median()
    stddev = df[column_name].std()

    try:
        context.log.info(f"Distribution for '{column_name}':")
        context.log.info(f"- Mean: {mean:.2f}")
        context.log.info(f"- Median: {median:.2f}")
        context.log.info(f"- STD: {stddev:.2f}")
        context.log.info(f"- Skew value : {df[column_name].skew():.2f}")

        if df[column_name].skew() > 0:
            shape_description = "Askew Right"
        elif df[column_name].skew() < 0:
            shape_description = "Askew Left"
        else:
            shape_description = "Balanced"
        context.log.info(f"- Shape: {shape_description}")

        q1 = df[column_name].quantile(0.25)
        q3 = df[column_name].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - whisker_cf * iqr
        upper_bound = q3 + whisker_cf * iqr
        context.log.info(f"- Lower_bound: {lower_bound}")
        context.log.info(f"- Upper_bound: {upper_bound}")
        outliers_mask = (df[column_name] < lower_bound) | (df[column_name] > upper_bound)
        outliers = df.filter(outliers_mask)[column_name].to_list()
        context.log.info(f"- Outliers: {outliers}")
    except Exception as e:
        context.log.info(e)
        return


def correlation_analysis(df: pl.DataFrame, threshold=0.5):
    df_pandas = df.to_pandas()
    columns = df_pandas.columns.tolist()
    n = len(columns)

    pearson_corr_matrix = df_pandas.corr(method='pearson')
    spearman_corr_matrix = df_pandas.corr(method='spearman')

    pearson_high_corr_pairs, pearson_low_corr_pairs = [], []
    spearman_high_corr_pairs, spearman_low_corr_pairs = [], []

    for i in range(n):
        for j in range(i + 1, n):
            col1, col2 = columns[i], columns[j]

            pearson_corr = pearson_corr_matrix.loc[col1, col2]
            spearman_corr = spearman_corr_matrix.loc[col1, col2]

            pearson_res_test = {(col1, col2): {"correlation": pearson_corr}}
            spearman_res_test = {(col1, col2): {"correlation": spearman_corr}}

            if abs(pearson_corr) >= threshold:
                pearson_high_corr_pairs.append(pearson_res_test)
            else:
                pearson_low_corr_pairs.append(pearson_res_test)

            if abs(spearman_corr) >= threshold:
                spearman_high_corr_pairs.append(spearman_res_test)
            else:
                spearman_low_corr_pairs.append(spearman_res_test)

    pearson_high_corr_ratio, pearson_low_corr_ratio = calc_corr_ratio(pearson_high_corr_pairs,
                                                                      pearson_low_corr_pairs)
    spearman_high_corr_ratio, spearman_low_corr_ratio = calc_corr_ratio(spearman_high_corr_pairs,
                                                                        spearman_low_corr_pairs)
    result = {
        "pearson_high_corr_ratio": pearson_high_corr_ratio,
        "pearson_low_corr_ratio": pearson_low_corr_ratio,
        "spearman_high_corr_ratio": spearman_high_corr_ratio,
        "spearman_low_corr_ratio": spearman_low_corr_ratio,
        "pearson_high_corr_pairs": pearson_high_corr_pairs,
        "pearson_low_corr_pairs": pearson_low_corr_pairs,
        "spearman_high_corr_pairs": spearman_high_corr_pairs,
        "spearman_low_corr_pairs": spearman_low_corr_pairs
    }
    return result


def calc_corr_ratio(high_corr_pairs, low_corr_pairs):
    total_pairs = len(high_corr_pairs) + len(low_corr_pairs)
    return len(high_corr_pairs) / total_pairs, len(low_corr_pairs) / total_pairs


def fill_null_for_numerical_columns(df: pl.DataFrame, data_type_groups: dict, numerical_data_type,
                                    context: AssetExecutionContext):
    context.log.info("1) Fill NULL for numerical columns ... ")
    try:
        for data_type in numerical_data_type:
            if data_type in data_type_groups.keys():
                numerical_columns = data_type_groups.get(data_type, [])
                if not numerical_columns:
                    continue
                numerical_df = process_numerical_dtype(df[numerical_columns], numerical_columns,
                                                       context)
                df = df.with_columns([numerical_df[column] for column in numerical_columns])
    except Exception as e:
        context.log.info(e)
        context.log.info(f"Skip filling NULL for numerical columns. ")
    return df


def fill_null_for_categorical_columns(df: pl.DataFrame, data_type_groups: dict,
                                      categorical_data_type, context: AssetExecutionContext):
    context.log.info("2) Fill NULL for categorical columns ...")
    try:
        for data_type in categorical_data_type:
            if data_type in data_type_groups.keys():
                categorical_columns = data_type_groups.get(data_type, [])
                if not categorical_columns:
                    continue
                categorical_df = process_categorical_dtype(df, categorical_columns, context)
                df = df.with_columns([categorical_df[column] for column in categorical_columns])
    except Exception as e:
        context.log.info(e)
        context.log.info(f"Skip filling NULL for categorical columns. ")
    return df


def log_statistics(df, numerical_data_type, context: AssetExecutionContext):
    context.log.info("3) Calculate statistics for numerical columns... ")
    any_numerical_columns = False
    for col in df.columns:
        if df[col].dtype in numerical_data_type:
            calculate_and_log_statistics(df, context, col)
            any_numerical_columns = True
    if not any_numerical_columns:
        context.log.info("Skip calculating statistics (No numerical columns found in the DataFrame).")


def convert_data_type(df: pl.DataFrame, data_type_mapping: dict[str, pl.DataType],
                      context: AssetExecutionContext) -> pl.DataFrame:
    expressions = []
    for col_name, new_dtype in data_type_mapping.items():
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in the DataFrame.")
        expressions.append(pl.col(col_name).cast(new_dtype).alias(col_name))
    context.log.info("Converted data type")
    return df.select(expressions)


def convert_time_column(df: pl.DataFrame, context: AssetExecutionContext,
                        time_column="schedule_time", am_pm=True):
    time_parts = df[time_column].str.strip().str.split(":").to_list()
    context.log.info("Time parts : ")
    context.log.info(time_parts)
    if am_pm:
        for idx, _ in enumerate(time_parts):
            if not len(time_parts[idx]) == 2:
                raise ValueError(f"Invalid time format: Expected 'HH:MM[AM/PM]' but got '{df[time_column][idx]}'")
        formatted_times = []
        for row in time_parts:
            hour_str, minute_am_pm = row
            am_pm = minute_am_pm[-2:].upper()
            hour_str = int(hour_str)
            if am_pm == "PM" and hour_str != 12:
                hour_str = (hour_str + 12) % 24
            elif am_pm == "AM" and hour_str == 12:
                hour_str = 0
            minute = int(minute_am_pm[:-2])
            formatted_time = f"{hour_str:02d}:{minute:02d}:00"
            formatted_times.append(formatted_time)

        context.log.info("Check Formatted Data : ")
        context.log.info(formatted_times)
        converted_time_series = pl.Series(formatted_times, dtype=pl.Utf8)
        context.log.info("Check Formatted Columns : ")
        context.log.info(converted_time_series)

        df = df.with_columns([
            pl.Series(name=time_column, values=converted_time_series)
        ])
        df = df.with_columns(pl.col(time_column).str.strptime(pl.Time, "%H:%M:%S").alias(time_column))

        context.log.info("View Time Columns : ")
        context.log.info(df[time_column])
        context.log.info(f"Processed {time_column} successfully")
    else:
        formatted_times = []
        for row in time_parts:
            hour_str, minute_str, second_str = row
            hour_str = int(hour_str)
            minute_str = int(minute_str)
            second_str = second_str.split(".")[0]
            second_str = int(second_str)
            formatted_time = f"{hour_str:02d}:{minute_str:02d}:{second_str:02d}"
            formatted_times.append(formatted_time)

        context.log.info("Check Formatted Data : ")
        context.log.info(formatted_times)
        converted_time_series = pl.Series(formatted_times, dtype=pl.Utf8)
        context.log.info("Check Formatted Columns : ")
        context.log.info(converted_time_series)

        df = df.with_columns([
            pl.Series(name=time_column, values=converted_time_series)
        ])
        df = df.with_columns(pl.col(time_column).str.strptime(pl.Time, "%H:%M:%S").alias(time_column))

        context.log.info("View Time Columns : ")
        context.log.info(df[time_column])
        context.log.info(f"Processed {time_column} successfully")
    return df


def convert_format_time(df: pl.DataFrame, column: str = "last_time_updated") -> pl.DataFrame:
    df = df.with_columns(pl.col(column).str.strptime(pl.Time, "%H:%M:%S").alias(column))
    return df


def append_suffix_to_time_column(df: pl.DataFrame,
                                 column: str = "last_time_updated", suffix: str = ":00") -> pl.DataFrame:
    df = df.with_columns((pl.col(column) + suffix).alias(column))
    return df


def create_table_from_polars_dataframe(df: pl.DataFrame, schema_name: str, table_name: str,
                                       mapping_dtype_references: dict, conn, context: AssetExecutionContext):
    if not schema_name or not table_name:
        raise ValueError("`schema_name` or `table_name` cannot be empty or table has already exist")
    try:
        schema = df.schema
        context.log.info("Check schema before creating table")
        context.log.info(schema)
        columns_sql = []
        for col_name, dtype in schema.items():
            polars_dtype = dtype
            duckdb_dtype = mapping_dtype_references[polars_dtype]

            if polars_dtype.is_nested():
                conn.execute(f"""
                    INSTALL spatial;
                    LOAD spatial;
                """)
            columns_sql.append(f"{col_name} {duckdb_dtype}")
        columns_sql_str = ",\n\t\t".join(columns_sql)
        sql_create_table = f"""
            CREATE OR REPLACE TABLE {schema_name}.{table_name} (
                {columns_sql_str}
            )
        """
        context.log.info(sql_create_table)
        conn.execute(sql_create_table)
        context.log.info(f"Create {schema_name}.{table_name} successfully!")
    except Exception as e:
        context.log.info(f"Error occurred while creating table: {e}")
        raise ValueError(f"{e}")


def create_table_from_geo_dataframe(gdf: gpd.GeoDataFrame, schema_name: str, table_name: str,
                                    mapping_dtype_references: dict, conn, context: AssetExecutionContext):

    if not schema_name or not table_name:
        raise ValueError("`schema_name` or `table_name` can not be empty or table has already exist")

    with conn:
        try:

            geom_col_name = gdf.geometry.name
            geom_type = gdf.geometry.geom_type

            sql_create_table = f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} (
                    {geom_col_name} {mapping_dtype_references[geom_type]},
            """

            for col_name in gdf.columns:
                if col_name != geom_col_name:
                    dtype = gdf.dtypes[col_name]

                    duckdb_dtype = mapping_dtype_references[dtype.name]
                    sql_create_table += f"""
                            {col_name} {duckdb_dtype},
                        """
            sql_create_table = sql_create_table[:-2]
            sql_create_table += ")"
            conn.execute(sql_create_table)
            context.log.info(f"Create {schema_name}.{table_name} successfully!")

            conn.execute(f"SELECT * FROM {schema_name}.{table_name}").fetchdf()
            context.log.info("View Created Table")
        except Exception as e:
            context.log.error(f"Error occurred while creating table as : {e}")
            raise ValueError(f"{e}")


def insert_data_into_table(conn, df, schema_name, table_name, context, use_pyarrow=True):
    try:
        if use_pyarrow:
            conn.execute(f"""
                INSTALL arrow;
                LOAD arrow;
            """)
            conn.from_arrow(df).insert_into(f"{schema_name}.{table_name}")
        else:
            conn.from_df(df).insert_into(f"{schema_name}.{table_name}")
        context.log.info(f"Inserted data into {schema_name}.{table_name}")
    except Exception as e:
        context.log.error(f"Failed to insert data : {e}")


def check_column_count(dim_column, dim_columns):
    if isinstance(dim_column, str) and dim_column != "":
        return 1
    elif isinstance(dim_columns, tuple) and dim_columns != ():
        return len(dim_columns)
    else:
        raise ValueError(f"Invalid 'dim_column'/'dim_columns' type : {type(dim_column)} / {type(dim_columns)} or "
                         f"Invalid 'dim_column'/'dim_columns' values : {dim_column} / {dim_columns} "
                         f"Type for 'dim_column'/'dim_columns' must be : str/tuple"
                         f"Value for 'dim_column/'dim_columns' can not be contained  : '' or () ")


def process_dim_table(conn, schema_name, fact_table, dim_table, foreign_key_column, context: AssetExecutionContext,
                      dim_column: str = "", dim_columns: tuple = (),
                      dim_table_value_column: str = "", dim_table_value_columns: tuple = (),
                      dim_table_id_column="id", set_up=True):

    column_count = check_column_count(dim_column, dim_columns)

    if dim_column != "" and dim_columns != ():
        raise ValueError("Cannot specify both 'dim_column' and 'dim_columns'")
    elif dim_column == "" and dim_columns == ():
        raise ValueError("Either 'dim_column' or 'dim_columns' must be provided")
    temp_table = f"{fact_table}_TEMP"
    if column_count == 1:
        if set_up:
            conn.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{temp_table} AS
                SELECT DISTINCT {fact_table}.{dim_column}
                FROM {schema_name}.{fact_table};
            """)

            current_max_id = conn.execute(f"""
                SELECT COALESCE(MAX({dim_table_id_column}), 0) FROM {schema_name}.{dim_table};
            """).fetchone()[0]

            conn.execute(f"""
                INSERT INTO {schema_name}.{dim_table} ({dim_table_id_column}, {dim_table_value_column})
                SELECT row_number() OVER () + {current_max_id} AS {dim_table_id_column}, {temp_table}.{dim_column}
                FROM {schema_name}.{temp_table};
            """)
        if set_up:

            conn.execute(f"""
                ALTER TABLE {schema_name}.{fact_table}
                ADD COLUMN {foreign_key_column} INT;
            """)

        conn.execute(f"""
            UPDATE {schema_name}.{fact_table} fact
            SET {foreign_key_column} = dim.{dim_table_id_column}
            FROM {schema_name}.{dim_table} dim
            WHERE fact.{dim_column} = dim.{dim_table_value_column};
        """)
        if set_up:

            conn.execute(f"""
                ALTER TABLE {schema_name}.{fact_table}
                DROP COLUMN {dim_column};
            """)
            conn.execute(f"""
                DROP TABLE IF EXISTS {schema_name}.{temp_table};
            """)
        context.log.info(f"Process dim table {schema_name}.{dim_table} finished")
    else:
        if set_up:
            prefixed_columns = [f"{fact_table}.{col}" for col in dim_columns]
            conn.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{temp_table} AS
                SELECT DISTINCT {', '.join(prefixed_columns)}
                FROM {schema_name}.{fact_table};
            """)

            current_max_id = conn.execute(f"""
                SELECT COALESCE(MAX({dim_table_id_column}), 0) FROM {schema_name}.{dim_table};
            """).fetchone()[0]

            prefixed_columns = [f"{temp_table}.{col}" for col in dim_columns]
            conn.execute(f"""
                INSERT INTO {schema_name}.{dim_table} ({dim_table_id_column}, {', '.join(dim_table_value_columns)})
                SELECT row_number() OVER () + {current_max_id} AS {dim_table_id_column}, {', '.join(prefixed_columns)}
                FROM {schema_name}.{temp_table};
            """)
        if set_up:

            conn.execute(f"""
                ALTER TABLE {schema_name}.{fact_table}
                ADD COLUMN {foreign_key_column} INT;
            """)

        where_clause = " OR ".join([f"fact.{col} = dim.{col}" for col in dim_columns])
        conn.execute(f"""
            UPDATE {schema_name}.{fact_table} fact
            SET {foreign_key_column} = dim.{dim_table_id_column}
            FROM {schema_name}.{dim_table} dim
            WHERE {where_clause};
        """)
        if set_up:

            for col in dim_columns:
                conn.execute(f"""
                    ALTER TABLE {schema_name}.{fact_table}
                    DROP COLUMN {col};
                """)
            conn.execute(f"""
                DROP TABLE IF EXISTS {schema_name}.{temp_table};
            """)
        context.log.info(f"Process dim table {schema_name}.{dim_table} finished")


def process_static_table(conn, fact_schema, fact_table, dim_schema, dim_table,
                         dim_column, context: AssetExecutionContext, dim_table_id_column="id"):
    conn.execute(f"""
        INSERT INTO {dim_schema}.{dim_table} ({dim_table_id_column})
        SELECT {dim_column} AS {dim_table_id_column}
        FROM (
            SELECT DISTINCT {fact_table}.{dim_column}
            FROM {fact_schema}.{fact_table}
            WHERE {dim_column} NOT IN  (
                SELECT DISTINCT {dim_table}.{dim_table_id_column} FROM {dim_schema}.{dim_table})
        ) AS subquery;
        """)
    context.log.info(f"Process dim table {dim_schema}.{dim_table} finished")


def create_secret(conn, secret_name, secret_type, access_key_id,
                  secret_access_key, context: AssetExecutionContext):
    sql = f"""
    CREATE SECRET IF NOT EXISTS {secret_name} (
        TYPE {secret_type},
        KEY_ID '{access_key_id}',
        SECRET '{secret_access_key}',
        PROVIDER CONFIG
    );
    """
    conn.execute(sql)
    context.log.info(f'Create secret with name {secret_name} - type {secret_type} successfully ')
    context.log.info(sql)


def copy_file_to_s3(conn, bucket_name, file_name,
                    schema, table, context: AssetExecutionContext, idx=None):
    current_day = datetime.datetime.now()
    day_month_year = current_day.strftime("%d%m%y")

    config = create_minio_config(bucket_name)
    minio_connector = initialize_minio(config)
    create_bucket(minio_connector, bucket_name)
    object_name = f"{file_name}/{file_name}_{day_month_year}_idx_{idx}.parquet" if idx is not None\
        else f"{file_name}/{file_name}_{day_month_year}.parquet"
    sql = f"""
        SELECT * FROM {schema}.{table}
    """
    data = conn.execute(sql).fetch_arrow_table()
    data: pd.DataFrame = data.to_pandas()
    with io.BytesIO() as buffer:
        data.to_parquet(buffer)
        buffer.seek(0)
        minio_connector.put_object(bucket_name, object_name,
                                   buffer, len(buffer.getvalue()), 'application/octet-stream')
    stat_object(minio_connector, bucket_name, object_name, data, context)
    context.log.info(f'Exported data from {schema}.{table} to MINIO/S3 : {bucket_name}/{file_name}.parquet')


def stat_object(minio_connector: Minio, bucket_name, object_name, data, context: AssetExecutionContext):
    try:
        stat = minio_connector.stat_object(bucket_name, object_name)
        context.log.info(f"Object: {object_name} - Stat: {stat}")
        context.log.info(f"Bucket : {stat.bucket_name}")
        context.log.info(f"Object : {stat.object_name}")
        context.log.info(f"Version_id : {stat.version_id}")
        context.log.info(f"Metadata: {stat.metadata}")
        context.log.info(f"Content_Type: {stat.content_type}")
    except S3Error as e:
        context.log.info(f"Object : {object_name} does not exist - "
                         f"Error Status : {e.response.status} - "
                         f"Creating object {object_name} ... ")
        with io.BytesIO() as buffer:
            data.to_parquet(buffer)
            buffer.seek(0)
            minio_connector.put_object(bucket_name, object_name,
                                       buffer, len(buffer.getvalue()), 'application/octet-stream')
        context.log.info(f"Object : {object_name} created  ")


def create_bucket(minio_client: Minio, bucket_name):
    if minio_client.bucket_exists(bucket_name):
        print(f"{bucket_name} has already exists")
    else:
        print(f"{bucket_name} does not exist")
        print(f"Creating bucket {bucket_name}")
        minio_client.make_bucket(bucket_name)
        versioning_config = VersioningConfig(ENABLED)
        minio_client.set_bucket_versioning(bucket_name, versioning_config)


def initialize_minio(config):
    try:
        return Minio(
            endpoint=config["endpoint_url"],
            access_key=config["aws_access_key_id"],
            secret_key=config["aws_secret_access_key"],
            secure=False
        )
    except Exception as e:
        raise ValueError(f"Fail to create MinIo connector because of: {e}")


def create_minio_config(bucket):
    return {
        "endpoint_url": os.getenv("ENDPOINT_URL_MINIO"),
        "bucket": os.getenv(bucket),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID_MINIO"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KRY_MINIO"),
    }


def view_list_object(list_objects, object_name, context):
    context.log.info(f"Version of object {object_name}")
    for obj in list_objects:
        context.log.info(obj.version_id)


def view_latest_version_id(list_objects, context):
    latest_version_id = sorted(list_objects, key=lambda obj: obj.version_id, reverse=True)[0].version_id
    context.log.info("latest_version_id")
    context.log.info(latest_version_id)
    return latest_version_id


def download_data_from_minio(config, bucket_name, object_name, context: AssetExecutionContext,
                             raw_data=True, idx=None):
    current_day = datetime.datetime.now()
    day_month_year = current_day.strftime("%d%m%y")

    minio_helper = MinIOHelper()
    minio_connector = initialize_minio(config)
    object_name = object_name.lower().replace("_", "")
    d_object_name = f'{object_name}/{object_name}_{day_month_year}_idx_{idx}.parquet' if idx is not None \
        else f'{object_name}/{object_name}_{day_month_year}.parquet'
    if idx is not None:
        file_name = f"{object_name}_{idx}.csv" if raw_data else f"{object_name}_{idx}.parquet"
    else:
        file_name = f"{object_name}.csv" if raw_data else f"{object_name}.parquet"
    dir_name = "data" if raw_data else "processed_data"
    file_extension = d_object_name.rsplit('.', 1)[-1]

    list_objects_generator = minio_connector.list_objects(bucket_name=bucket_name,
                                                          include_version=True,
                                                          prefix=d_object_name)
    list_objects = list(list_objects_generator)
    view_list_object(list_objects, d_object_name, context)
    latest_version_id = view_latest_version_id(list_objects, context)

    data = minio_helper.read_dataframe(
        minio_client=minio_connector,
        bucket_name=bucket_name,
        object_name=d_object_name,
        file_extension=file_extension,
        version_id=latest_version_id,
        context=context
    )
    context.log.info("Read data from MinIO successfully ... ")

    output_directory = f"./datasource_init/{dir_name}/{object_name}" if raw_data \
        else f"./{dir_name}/{object_name}"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    path = os.path.join(output_directory, file_name)

    res = data.to_pandas()
    if raw_data:
        res.to_csv(path, index=False)
    else:
        res.to_parquet(path)
    context.log.info(f"Download from MinIO successfully ... ")
    context.log.info(f"{path}")


def load_dataframe_to_duckdb(df, db_file, table_name):
    con = db.connect(db_file)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    if isinstance(df, pd.DataFrame):
        df = pl.DataFrame(df)
    if isinstance(df, pl.DataFrame):
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    else:
        raise TypeError("df must be pandas.DataFrame or polars.DataFrame")
    con.close()


def execute_query_and_log_performance(context: OpExecutionContext, query: SQL, duckdb_io_manager: DuckDB_IOManager,
                                      query_name: str = None):
    start_time = time.time()
    result = duckdb_io_manager.duckdb.query(query)
    end_time = time.time()

    query_time = end_time - start_time
    context.log.info(f"Query on {query_name or 'unspecified table'} executed in {query_time:.2f} seconds")

    context.log.info(f"Data collected from {query_name}")
    context.log.info(result.to_pandas())
    context.log.info(f"Details:")
    try:
        get_arrow_dtypes(result, context)
    except Exception as e:
        context.log.info(e)
        get_pandas_dtypes(result.to_pandas(), context)


def get_pandas_dtypes(df, context):
    context.log.info("Fetch by pandas dataframe")
    context.log.info("Data Type for columns:")
    for col in df.columns:
        context.log.info(f"{col}: {df[col].dtype}")


def get_polars_dtypes(df, context):
    context.log.info("Fetch by polars dataframe")
    context.log.info("Data Type for columns:")
    for col in df.schema.column_names:
        context.log.info(f"{col}: {df.schema[col].dtype}")


def get_arrow_dtypes(table, context):
    context.log.info("Fetch by arrow table ")
    context.log.info("Data Type for columns:")
    for col, field in zip(table.column_names, table.schema):
        context.log.info(f"{col}: {field.type}")


retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,
    backoff=Backoff.EXPONENTIAL,
)


@op(name="query_table",
    description="Executes a query statement using DuckDB",
    retry_policy=retry_policy)
def query_table(table_name: str, duckdb: DuckDBResource) -> SQL:
    statement = f"SELECT * FROM {table_name}"
    with duckdb.get_connection() as conn:
        result = conn.sql(statement).fetch_arrow_table()
    return SQL("SELECT * FROM $df", df=result)


@op(name="join_table",
    description="Executes a pre-defined join statement using DuckDB ",
    retry_policy=retry_policy)
def join_tables(statement, duckdb: DuckDBResource) -> SQL:
    with duckdb.get_connection() as conn:
        joined_query_statement = conn.sql(statement).fetch_arrow_table()
    return SQL("SELECT * FROM $df", df=joined_query_statement)


@op(name="collect_data",
    description="Collect table's information from Mother Duck Table",
    retry_policy=retry_policy)
def collect_data(table_name: str, duckdb: DuckDBResource) -> pd.DataFrame:
    statement = f"SELECT * FROM {table_name}"
    with duckdb.get_connection() as conn:
        relation = conn.sql(statement).fetch_arrow_table()
        result = relation.to_pandas()
        return result


def create_column_value_dict(dataframe: pd.DataFrame) -> dict:
    if dataframe.empty:
        raise ValueError("Empty DataFrame provided for column-value extraction.")
    column_value_dict = {}
    for column_name, column_data in dataframe.items():
        column_value_dict[column_name] = column_data.tolist()
    return column_value_dict


def generate_metadata_from_dataframe(df: pd.DataFrame, set_up=True, additional_list=()):
    df_pl = pl.DataFrame(df)
    columns_metadata = []

    foreign_keys = [
        col for col in df_pl.columns if col.endswith('_id')
        or col.endswith("locationid") and len(col) > len("locationid")
    ]
    primary_keys = [
        col for col in df_pl.columns if col == 'id'
        or (col.endswith('locationid') and col not in foreign_keys)
    ]

    additional_foreign_keys = [
        col for col in df_pl.columns if
        (col in additional_list)
        or (col.endswith("id") and col not in foreign_keys and col not in primary_keys)
    ]

    for col in foreign_keys:
        if df_pl[col].dtype != pl.Int64 and col.endswith('locationid'):
            df_pl = df_pl.with_columns(pl.col(col).cast(pl.Int64))

    for col in additional_foreign_keys:
        if df_pl[col].dtype != pl.Int64:
            df_pl = df_pl.with_columns(pl.col(col).cast(pl.Int64))

    for col in df.columns:
        if col not in primary_keys and col not in foreign_keys and col not in additional_foreign_keys:
            if set_up:
                sub_dict = {col: (map_dtype_to_postgres(df_pl[col].dtype))}
            else:
                sub_dict = {col: (map_dtype_to_mysql(df_pl[col].dtype))}
        else:
            if set_up:
                sub_dict = {col: "SERIAL"}
            else:
                sub_dict = {col: "INT"}
        columns_metadata.append(sub_dict)

    return {
        "columns": columns_metadata,
        "primary_keys": primary_keys if primary_keys else [],
        "foreign_keys": foreign_keys if foreign_keys else [],
        "additional_foreign_keys": additional_foreign_keys if additional_foreign_keys else [],
        "skip_insertion_status": 0,
        "use_bulking_load": 0
    }


def map_dtype_to_postgres(dtype) -> str:
    print(dtype)
    dtype_mapping = {
        pl.Int64: "INTEGER",
        pl.Int32: "INT",
        pl.Int8: "INT4",
        pl.Float64: "FLOAT",
        pl.Float32: "FLOAT4",
        pl.Datetime: "TIMESTAMP",
        pl.Datetime(time_unit='us', time_zone=None): "TIMESTAMP",
        pl.Time: "TIME",
        pl.Date: "TIMESTAMP",
        pl.String: "TEXT",
        pl.Utf8: "TEXT",
    }
    return dtype_mapping.get(dtype, "SERIAL")


def map_dtype_to_mysql(dtype) -> str:
    dtype_mapping = {
        pl.Int64: "BIGINT",
        pl.Int32: "INT",
        pl.Int8: "SMALLINT",
        pl.Float64: "DOUBLE",
        pl.Float32: "FLOAT",
        pl.Datetime: "DATETIME",
        pl.Datetime(time_unit='us', time_zone=None): "DATETIME",
        pl.Date: "DATE",
        pl.Time: "TIME",
        pl.String: "VARCHAR(255)",
        pl.Utf8: "VARCHAR(255)",
    }
    return dtype_mapping.get(dtype, "VARCHAR(255)")


# api
ANL_FHV_VEHICLE = "https://data.cityofnewyork.us/resource/8wbx-tsch.json"
ANL_FHV_AGG_BASE_REPORT = "https://data.cityofnewyork.us/resource/2v9c-2k7f.json"
ANL_SHL_PERMIT = "https://data.cityofnewyork.us/resource/yhuu-4pt3.json"
ANL_SHL_INSPECT = "https://data.cityofnewyork.us/resource/rdxc-q253.json"
ANL_LOST_PROPERTY = "https://data.cityofnewyork.us/resource/dg7a-jiz2.json"
ANL_METER_SHOP = "https://data.cityofnewyork.us/resource/56e3-rp8d.json"
ANL_NEW_DRIVER = "https://data.cityofnewyork.us/resource/dpec-ucu7.json"
ANL_TAXI_ZONE = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ANL_MONTHLY_REPORT = "https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv"

GEO_TAXI_ZONE = "https://data.cityofnewyork.us/resource/755u-8jsi.json"
GEO_NY_NTA = "https://data.cityofnewyork.us/resource/93vf-i5bz.json"

TS_HOURLY_MONITORING = "https://azdohv2staticweb.blob.core.windows.net/$web/hist/csv/2024/4/hourlyMonitoring.csv"
TS_AQE = "https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/neighborhood-air-quality/aqe-nta.csv"

data_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
start_year = 2023
start_month = 1
end_year = 2023
end_month = 12
extension = "parquet"

# partitioning
START_DATE = "2023-01-01"
END_DATE = "2024-01-01"

# data type for processing
numerical_data_types = [
    pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, "Numeric"
]

categorical_data_types = [pl.Utf8, pl.String, pl.Object, "Category"]

additional_fk = ("payment_type", "ratecodeid", "trip_type", "vendorid")

# mapping
mapping_polars_dtype = {
    # Polars                                    # DuckDB/Mother Duck
    pl.Int8:                                     "SMALLINT",
    pl.Int16:                                    "SMALLINT",
    pl.Int32:                                    "INT",
    pl.Int64:                                    "BIGINT",
    pl.UInt8:                                    "SMALLINT",
    pl.UInt16:                                   "SMALLINT",
    pl.UInt32:                                   "INT",
    pl.UInt64:                                   "BIGINT",
    pl.Float32:                                  "REAL",
    pl.Float64:                                  "DOUBLE",
    pl.String:                                   "VARCHAR(255)",
    pl.Object:                                   "VARCHAR(255)",
    pl.Boolean:                                  "BOOLEAN",
    pl.Date:                                     "DATE",
    pl.Datetime:                                 "TIMESTAMP",
    pl.Datetime(time_unit='us', time_zone=None): "TIMESTAMP",
    pl.Datetime(time_unit='ns', time_zone=None): "TIMESTAMP",
    pl.Null:                                     "REAL",
    pl.Time:                                     "TIME",
    pl.Categorical:                              "VARCHAR(255)",
    pl.Utf8:                                     "TEXT",
    pl.Struct(
        [
            pl.Field
            ("type", pl.String),
            pl.Field
            ("coordinates", pl.List(pl.List(pl.List(pl.List(pl.Float64)))))
        ]
    ):
    """
    STRUCT(
        type VARCHAR,
        coordinates DOUBLE[][][][]
    )
    """
}

mapping_geo_pandas_dtype = {
    # Geo DataFrame                  DuckDB/Mother Duck
    "geometry":                      "GEOMETRY",
    "Point":                         "POINT",
    "LineString":                    "LINESTRING",
    "Polygon":                       "POLYGON",
    "MultiPoint":                    "MULTIPOINT",
    "MultiLineString":               "MULTILINESTRING",
    "MultiPolygon":                  "MULTIPOLYGON",
    "GeometryCollection":            "GEOMETRYCOLLECTION",
    "Int8":                          "SMALLINT",
    "Int16":                         "SMALLINT",
    "Int32":                         "INT",
    "Int64":                         "BIGINT",
    "UInt8":                         "SMALLINT",
    "UInt16":                        "SMALLINT",
    "UInt32":                        "INT",
    "UInt64":                        "BIGINT",
    "Float32":                       "REAL",
    "Float64":                       "DOUBLE",
    "String":                        "VARCHAR(255)",
    "Boolean":                       "BOOLEAN",
    "Date":                          "DATE",
    "DateTime":                      "TIMESTAMP",
    "Time":                          "TIME",
    "Category":                      "VARCHAR(255)"
}

# custom name mapping
fhv_vehicle_name_mapping = {
    "website": "base_website"
}

lost_property_name_mapping = {
    "last_updated_time": "last_time_updated",
    "last_updated_date": "last_date_updated"
}

shl_inspect_name_mapping = {
    "last_updated_time": "last_time_updated",
    "last_updated_date": "last_date_updated"
}

shl_permit_name_mapping = {
    "time_updated": "last_time_updated",
    "date_updated": "last_date_updated"
}

new_driver_name_mapping = {
    "lastupdate": "last_date_updated"
}

# custom data type mapping
fhv_base_report_dtype_mapping = {
    "year": pl.Int32,
    "month": pl.Int32,
    "total_dispatched_trips": pl.Int64,
    "total_dispatched_shared_trips": pl.Int64,
    "unique_dispatched_vehicles": pl.Int32,
    "dba": pl.Utf8
}

fhv_vehicle_dtype_mapping = {
    "vehicle_year": pl.Int32,
    "expiration_date": pl.Datetime,
    "last_time_updated": pl.Time,
    "hack_up_date": pl.Datetime,
    "last_date_updated": pl.Datetime,
    "certification_date": pl.Datetime
}

lost_property_contact_dtype_mapping = {
    "last_date_updated": pl.Datetime,
}

shl_inspect_dtype_mapping = {
    "schedule_date": pl.Datetime,
    "last_date_updated": pl.Datetime,
    "last_time_updated": pl.Time
}

shl_permit_dtype_mapping = {
    "last_date_updated": pl.Datetime,
    "last_time_updated": pl.Time,
    "certification_date": pl.Datetime,
    "hack_up_date": pl.Datetime,
    "vehicle_year": pl.Datetime
}

meter_shop_dtype_mapping = {
    "date": pl.Datetime,
    "time": pl.Time
}

monthly_report_dtype_mapping = {
    "trips_per_day": pl.Float64,
    "farebox_per_day": pl.Float64,
    "unique_drivers": pl.Int64,
    "unique_vehicles": pl.Int64,
    "vehicles_per_day": pl.Int64,
    "percent_of_trips_paid_with_credit_card": pl.Float64,
    "trips_per_day_shared": pl.Float64
}

new_driver_dtype_mapping = {
    "app_no": pl.String
}

ny_nta_dtype_mapping = {
    "shape_leng": pl.Float64,
    "shape_area": pl.Float64
}

geo_taxi_zone_dtype_mapping = {
    "shape_leng": pl.Float64,
    "shape_area": pl.Float64
}

fhv_trips_dtype_mapping = {
    "pulocationid": pl.Int32,
    "dolocationid": pl.Int32,
}

# columns to edit
edit_columns = ["farebox_per_day",
                "percent_of_trips_paid_with_credit_card",
                "trips_per_day_shared", "trips_per_day",
                "unique_drivers", "unique_vehicles",
                "vehicles_per_day"]

ALL_LINKS = generate_all_links(data_types, start_year,
                               start_month, end_year, end_month, extension)

TS_YELLOW_TRIPS, TS_GREEN_TRIPS, TS_FHV_TRIPS, TS_FHVHV_TRIPS = ALL_LINKS.values()

API_LIST_YELLOW = TS_YELLOW_TRIPS[0:2]
API_LIST_GREEN = TS_GREEN_TRIPS[0:2]
API_LIST_FHV = TS_FHV_TRIPS[0:2]

REPORT_SCHEMA = "REPORT"
SERVICES_SCHEMA = "SERVICES"
TRIPS_SCHEMA = "TRIPS"
ZONES_SCHEMA = "ZONES"
ENV_SCHEMA = "ENVIRONMENTS"
PROCESS_SCHEMA = "dbt_source_process"

TABLE_ANL_BASE_REPORT = "FHV_BASE_AGGREGATE_REPORT"
TABLE_ANL_FHV_VEHICLE = "FHV_VEHICLES"
TABLE_ANL_LOST_PROPERTY = "LOST_PROPERTY_CONTACT"
TABLE_ANL_METER_SHOPS = "SHL_METER_SHOPS"
TABLE_ANL_NEW_DRIVERS = "TLC_NEW_DRIVER_APPLICATION_STAT"
TABLE_ANL_SHL_INSPECT = "SHL_TAXI_INITIAL_INSPECT"
TABLE_ANL_SHL_PERMIT = "SHL_PERMITS"
TABLE_ANL_TAXI_ZONES = "TAXI_ZONE_LOOKUP"
TABLE_ANL_TXZ_REFERENCES_ID = "locationid"
TABLE_ANL_MONTHLY_REPORT = "DATA_REPORTS_MONTHLY"
TABLE_GEO_NTA = "NY_NTA"
TABLE_GEO_TAXI_ZONES = "TAXI_ZONE_GEO"

TABLE_TS_AQE = "AQE"
TABLE_TS_HOURLY_MONITORING = "HOURLY_MONITORING"
TABLE_TS_FHV_TRIPS = "FHV_TRIPS"
TABLE_TS_FHVHV_TRIPS = "FHVHV_TRIPS"
TABLE_TS_GREEN_TRIPS = "GREEN_TRIPS"
TABLE_TS_YELLOW_TRIPS = "YELLOW_TRIPS"

TABLE_TS_DIM_VENDOR = "VENDOR"
TABLE_TS_DIM_PAYMENT_TYPE = "PAYMENT_TYPE"
TABLE_TS_DIM_TRIP_TYPE = "TRIP_TYPE"
TABLE_TS_DIM_RATECODE = "RATECODE"
TABLE_TS_DIM_HVFHS_LICENSE = "HVFHS_LICENSE"
TABLE_TS_DIM_STORE_FWD_FLAG = "STORE_FWD_FLAG"
TABLE_ANL_DIM_AGG_BASE_NAME = "AGG_BASE_NAME"
TABLE_ANL_DIM_AGG_BASE_LICENSE = "AGG_BASE_LICENSE"
TABLE_ANL_DIM_VEHICLE_NAME = "VEHICLE_NAME"
TABLE_ANL_DIM_WHEELCHAIR_ACCESS = "WHEELCHAIR_ACCESS"
TABLE_ANL_DIM_BASE = "BASE"
TABLE_ANL_DIM_VEH = "VEH"
TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE = "VEHICLE_YEAR_FHV_VEHICLE"
TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES = "LOST_PROPERTY_VEHICLES"
TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE = "LOST_PROPERTY_REPORTED_BASE"
TABLE_ANL_DIM_INSPECT_BASE = "INSPECT_BASE"
TABLE_ANL_DIM_VEHICLE_TYPE = "VEHICLE_TYPE"

# secret
SECRET_ANL_BASE_REPORT = "secret_anl_base_report"
SECRET_ANL_FHV_VEHICLE = "secret_anl_fhv_vehicle"
SECRET_ANL_LOST_PROPERTY = "secret_anl_lost_property"
SECRET_ANL_METER_SHOP = "secret_anl_meter_shop"
SECRET_ANL_MONTHLY_REPORT = "secret_anl_monthly_report"
SECRET_ANL_NEW_DRIVER = "secret_anl_new_driver"
SECRET_ANL_SHL_INSPECT = "secret_anl_shl_permit"
SECRET_ANL_TAXI_ZONE = "secret_anl_taxi_zone"
SECRET_GEO_NTA = "secret_geo_nta"
SECRET_GEO_TAXI_ZONE = "secret_geo_taxi_zone"
SECRET_TS_FHV_TRIPS = "secret_ts_fhv_trips"
SECRET_TS_FHVHV_TRIPS = "secret_ts_fhvhv_trips"
SECRET_TS_YELLOW_TRIPS = "secret_ts_yellow_trips"
SECRET_TS_GREEN_TRIPS = "secret_ts_green_trips"
SECRET_TS_AQE = "secret_ts_aqe"
SECRET_TS_HOURLY_MONITORING = "secret_ts_hourly_monitoring"
SECRET_TYPE = "S3"

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID_MINIO")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KRY_MINIO")
ANL_BUCKET = os.getenv("MINIO_ANL_BUCKET")
GEO_BUCKET = os.getenv("MINIO_GEO_BUCKET")
TS_BUCKET = os.getenv("MINIO_TS_BUCKET")
MD_BUCKET = os.getenv("MINIO_MD_BUCKET")

TABLE_PSQL_AGG_REPORT_BAN_BASES = "agg_report_ban_bases"
TABLE_PSQL_AGG_REPORT_BAN_CASES = "agg_report_ban_cases"
TABLE_PSQL_AGG_REPORT_BAN_VEHICLES = "agg_report_ban_vehicles"
TABLE_PSQL_AGG_REPORT_UNIQUE_VEHICLE = "agg_report_unique_vehicle"
TABLE_PSQL_AGG_SHL_INSPECT_SCHEDULE = "agg_report_shl_inspect_schedule"
TABLE_PSQL_REPORTED_BASE = "reported_base"
TABLE_PSQL_REPORTED_VEHICLES = "reported_vehicles"
TABLE_PSQL_TREND_OF_DISPATCHED_TRIPS = "trend_of_dispatched_trips"
TABLE_PSQL_VERSION_OF_VEHICLES = "version_of_vehicles"
TABLE_PSQL_AVAILABLE_VEHICLES_BLACK = "available_vehicles_black"
TABLE_PSQL_AVAILABLE_VEHICLES_GREEN = "available_vehicles_green"
TABLE_PSQL_AVAILABLE_VEHICLES_YELLOW = "available_vehicles_yellow"
TABLE_PSQL_AVAILABLE_VEHICLES_HIGH_VOLUME = "available_vehicles_high_volume"
TABLE_PSQL_AVAILABLE_VEHICLES_LIMO = "available_vehicles_limo"
TABLE_PSQL_AVAILABLE_VEHICLES_LIVERY = "available_vehicles_livery"
TABLE_PSQL_FAREBOX_PER_TRIPS_BLACK = "farebox_per_trips_black"
TABLE_PSQL_FAREBOX_PER_TRIPS_GREEN = "farebox_per_trips_green"
TABLE_PSQL_FAREBOX_PER_TRIPS_YELLOW = "farebox_per_trips_yellow"
TABLE_PSQL_FAREBOX_PER_TRIPS_HIGH_VOLUME = "farebox_per_trips_high_volume"
TABLE_PSQL_FAREBOX_PER_TRIPS_LIMO = "farebox_per_trips_limo"
TABLE_PSQL_FAREBOX_PER_TRIPS_LIVERY = "farebox_per_trips_livery"
TABLE_PSQL_FHV_BASE_TYPE_CONTAINED = "fhv_base_type_contained"
TABLE_PSQL_FHV_POPULAR_DROPOFF = "fhv_popular_dropoff_location"
TABLE_PSQL_FHV_POPULAR_PICKUP = "fhv_popular_pickup_location"
TABLE_PSQL_FHV_TOTAL_TRIPS_PER_MONTH = "fhv_total_trips_per_month"
TABLE_PSQL_FHV_TOTAL_TRIPS_PER_YEAR = "fhv_total_trips_per_year"
TABLE_PSQL_TRIPS_TIME_MOVING = "fhv_trip_time_moving"
TABLE_PSQL_TRIPS_TIME_OPERATING = "fhv_trip_time_operating"
TABLE_PSQL_FHV_VEHICLE_TYPE = "fhv_vehicle_type"
TABLE_PSQL_GREEN_DISTANCE_MOVING = "green_distance_moving"
TABLE_PSQL_GREEN_ADDITIONAL_MONEY = "green_group_additional_money"
TABLE_PSQL_GREEN_DROPOFF_LOCATION = "green_popular_dropoff_location"
TABLE_PSQL_GREEN_PICKUP_LOCATION = "green_popular_pickup_location"
TABLE_PSQL_GREEN_UNPAID_MONEY = "green_unpaid_money"
TABLE_PSQL_YELLOW_DISTANCE_MOVING = "yellow_distance_moving"
TABLE_PSQL_YELLOW_ADDITIONAL_MONEY = "yellow_group_additional_money"
TABLE_PSQL_YELLOW_POPULAR_DROPOFF = "yellow_popular_dropoff_location"
TABLE_PSQL_YELLOW_POPULAR_PICKUP = "yellow_popular_pickup_location"
TABLE_PSQL_YELLOW_UNPAID_MONEY = "yellow_unpaid_money"

# group name
RAW_LAYER = "raw"
SOURCE_LAYER = "source"
TARGET_LAYER = "target"
PROCESS_LAYER = "process"

# dbt group name
DBT_GROUP_NAME = "dbt"

# airbyte group name
AIRBYTE_GROUP_NAME = "airbyte"

# ingest
INGEST_ANL_GROUP_NAME = "ingest_anl"
INGEST_GEO_GROUP_NAME = "ingest_geo"
INGEST_TS_GROUP_NAME = "ingest_ts"
INGEST_SET_UP = "set_up"

# preview
PREVIEW_ANL_GROUP_NAME = "preview_anl"
PREVIEW_GEO_GROUP_NAME = "preview_geo"
PREVIEW_TS_GROUP_NAME = "preview_ts"

# infrastructure
INFRAS_ANL_GROUP_NAME = "infras_anl"
INFRAS_TS_GROUP_NAME = "infras_ts"

# source
SOURCE_GROUP_NAME = "data_source"
