
import pandas as pd
import mysql.connector as mc


class MySQL_ExtractorIOManager:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str, path: str) -> None:
        connection = mc.connect(
            host=self._config["host"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"]
        )

        df = pd.read_sql(sql, connection)

        connection.close()

        df.to_parquet(path)

