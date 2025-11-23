
import psycopg2
import pandas as pd
from dagster import AssetExecutionContext


class PostgresExtractor:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str, context: AssetExecutionContext) -> pd.DataFrame:
        # Connect to PostgresSQL database
        connection = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            database=self._config["database"],
            user=self._config["user"],
            password=self._config["password"])

        df = pd.read_sql(sql, connection)
        context.log.info("Data")
        context.log.info(df)

        # Close the connection
        connection.close()
        return df
