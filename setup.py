from setuptools import find_packages, setup

setup(
    name="dagster_dbt_processing",
    packages=find_packages(exclude=["dagster_dbt_processing_tests"]),
    install_requires=[
        "dagster", "dagster-postgres", "dagster-airbyte", "dagster-airflow",
        "dagster-duckdb", "dagster-pandas", "dagster-polars",
        "dagster-embedded-elt", "dagster-pandas", "dagster-dbt", "dagstermill",
        "dbt-postgres", "dbt-trino", "duckdb", "papermill", "pyspark", "scrapy", "selenium",
        "minio", "mysql-connector-python", "psycopg2-binary", "basemap", "geos", "pyodbc", "mysql-connector-python",
        "geopandas", "polars", "pyarrow", "sqlescapy", "shapely", " scikit-learn", "fancyimpute",
        "urllib3"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
 