
from dagster import IOManager
from duckdb import connect
from string import Template
from sqlescapy import sqlescape
from typing import Mapping
import polars as pl
import pandas as pd
import pyarrow as pa


class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


class DuckDB:
    def __init__(self, options="", url=":memory:"):
        self.options = options
        self.url = url

    def query(self, select_statement: SQL):
        with connect(":memory:") as db:
            db.query("install httpfs; load httpfs;")
            db.query(self.options)

            dataframes = self._collect_dataframes(select_statement)
            for key, df in dataframes.items():
                db.register(key, df)

            result = db.query(self._sql_to_string(select_statement)).fetch_arrow_table()
            return result if result is not None else None

    def _sql_to_string(self, s: SQL) -> str:
        replacements = {}
        keys = list(s.bindings.keys())
        values = list(s.bindings.values())

        for key, value in zip(keys, values):
            if isinstance(value, pa.Table):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, pl.DataFrame):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, pd.DataFrame):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, SQL):
                replacements[key] = f"({self._sql_to_string(value)})"
            elif isinstance(value, str):
                replacements[key] = f"'{sqlescape(value)}'"
            elif isinstance(value, (int, float, bool)):
                replacements[key] = str(value)
            elif value is None:
                replacements[key] = "null"
            else:
                raise ValueError(f"Invalid type for {key}")
        return Template(s.sql).safe_substitute(replacements)

    def _collect_dataframes(self, s: SQL) -> Mapping[str, pa.Table]:
        dataframes = {}
        keys = list(s.bindings.keys())
        values = list(s.bindings.values())

        for key, value in zip(keys, values):
            if isinstance(value, pa.Table):
                dataframes[f"df_{id(value)}"] = value
            elif isinstance(value, SQL):
                dataframes.update(self._collect_dataframes(value))
        return dataframes


class DuckDB_IOManager(IOManager):
    def __init__(self, duckdb: DuckDB, prefix=""):
        self.duckdb = duckdb
        self.prefix = prefix

    def _get_table_name(self, context):
        if context.has_asset_key:
            asset_key = context.get_asset_identifier()
        else:
            asset_key = context.get_identifier()

        schema = asset_key[0]
        table_name = '_'.join(asset_key[1:])
        return f"{self.prefix}{schema}.{table_name}"

    def handle_output(self, context, select_statement: SQL):
        if select_statement is None:
            return

        if not isinstance(select_statement, SQL):
            raise ValueError(
                f"Expected asset to return a SQL; got {select_statement!r}"
            )

        self.duckdb.query(
            SQL(
                "create or replace table $table_name as $select_statement",
                table_name=self._get_table_name(context),
                select_statement=select_statement,
            )
        )

    def load_input(self, context) -> SQL:
        return SQL(
            "select * from $table_name", table_name=self._get_table_name(context)
        )

