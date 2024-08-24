
import pandas as pd
import mysql.connector as mc


class MySQL_ExtractorIOManager:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str, path: str) -> None:
        # Connect to MySQL database
        connection = mc.connect(
            host=self._config["host"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"]
        )

        # Extract data to pandas DataFrame
        df = pd.read_sql(sql, connection)

        # Close the connection
        connection.close()

        df.to_parquet(path)

