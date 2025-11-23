
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from ..constant import (
    TRIPS_SCHEMA,
    TABLE_TS_DIM_VENDOR,
    TABLE_TS_DIM_PAYMENT_TYPE,
    TABLE_TS_DIM_TRIP_TYPE,
    TABLE_TS_DIM_RATECODE,
    TABLE_TS_DIM_HVFHS_LICENSE,
    TABLE_TS_DIM_STORE_FWD_FLAG
)
from ..ingest_setup import ingest_setup


@asset(name="ts_dim_table", key_prefix=["ts", "dim_table"],
       deps=[ingest_setup.schema],
       description="create time-series dimensional table in Mother Duck Cloud ",
       compute_kind="duckdb")
def ts_dim_table(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        create_schema_query = f"""
            CREATE SCHEMA IF NOT EXISTS {TRIPS_SCHEMA};
        """

        create_table_query = f"""
            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_VENDOR} (
                id INT PRIMARY KEY,
                vendor_name TEXT
            );

            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_PAYMENT_TYPE} (
                id INT PRIMARY KEY,
                payment_description TEXT DEFAULT 'UNKNOWN'
            );

            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_TRIP_TYPE} (
                id INT PRIMARY KEY,
                trip_type_description TEXT DEFAULT 'UNKNOWN'
            );

            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_RATECODE} (
                id INT PRIMARY KEY,
                ratecode_description TEXT DEFAULT 'UNKNOWN'
            );

            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_HVFHS_LICENSE} (
                hvfhs_license_num TEXT PRIMARY KEY,
                agent TEXT DEFAULT 'UNKNOWN'
            );

            CREATE OR REPLACE TABLE {TRIPS_SCHEMA}.{TABLE_TS_DIM_STORE_FWD_FLAG} (
                flag TEXT PRIMARY KEY,
                note TEXT DEFAULT 'UNKNOWN'
            );

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_PAYMENT_TYPE} (id, payment_description)
            VALUES
                (1, 'Credit card'),
                (2, 'Cash'),
                (3, 'No charge'),
                (4, 'Dispute'),
                (5, 'Voided trip'),
                (6, 'Credit card');

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_TRIP_TYPE} (id, trip_type_description)
            VALUES
                (1, 'Street-hail'),
                (2, 'Dispatch');

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_RATECODE} (id, ratecode_description)
            VALUES
                (1, 'Standard rate'),
                (2, 'JFK'),
                (3, 'Newark'),
                (4, 'Nassau or Westchester'),
                (5, 'Negotiated fare'),
                (6, 'Group ride');

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_VENDOR} (id, vendor_name)
            VALUES
                (1, 'Creative Mobile Technologies, LLC'),
                (2, 'VeriFone Inc');

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_HVFHS_LICENSE} (hvfhs_license_num, agent)
            VALUES
                ('HV0002', 'Juno'),
                ('HV0003', 'Uber'),
                ('HV0004', 'Via'),
                ('HV0005', 'Lyft');

            INSERT INTO {TRIPS_SCHEMA}.{TABLE_TS_DIM_STORE_FWD_FLAG} (flag, note)
            VALUES
                ('Y', 'store and forward trip'),
                ('N', 'not a store and forward trip');
        """
        conn.execute(create_schema_query)
        conn.execute(create_table_query)
        context.log.info("Create dim table fod time series data successfully")
