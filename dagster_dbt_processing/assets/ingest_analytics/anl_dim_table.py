
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource

from ..constant import (
    SERVICES_SCHEMA,
    TABLE_ANL_DIM_AGG_BASE_NAME,
    TABLE_ANL_DIM_AGG_BASE_LICENSE,
    TABLE_ANL_DIM_VEHICLE_NAME,
    TABLE_ANL_DIM_WHEELCHAIR_ACCESS,
    TABLE_ANL_DIM_BASE,
    TABLE_ANL_DIM_VEH,
    TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE,
    TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES,
    TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE,
    TABLE_ANL_DIM_INSPECT_BASE,
    TABLE_ANL_DIM_VEHICLE_TYPE
)
from ..ingest_setup import ingest_setup


@asset(
    name="anl_dim_table", key_prefix=["anl", "dim_table"],
    description="create analytics dimensional table in Mother Duck Cloud",
    deps=[ingest_setup.schema],
    compute_kind="duckdb"
)
def anl_dim_table(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        # Create dim table
        create_table_query = f"""
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_NAME} (
                id INT PRIMARY KEY ,
                base_name TEXT
            );
        
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_AGG_BASE_LICENSE} (
                id INT PRIMARY KEY  ,
                base_license_number VARCHAR
            );        

            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_NAME} (
                id INT PRIMARY KEY  ,
                name TEXT
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_WHEELCHAIR_ACCESS} (
                id INT PRIMARY KEY  ,
                wheelchair_accessible VARCHAR
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_BASE} (
                id INT PRIMARY KEY  ,
                base_number VARCHAR,
                base_name TEXT,
                base_telephone_number VARCHAR,
                base_address TEXT,
                base_website TEXT
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEH} (
                id INT PRIMARY KEY  ,
                veh VARCHAR
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_YEAR_FHV_VEHICLE} (
                id INT PRIMARY KEY  ,
                vehicle_year INT4
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_VEHICLES} (
                id INT PRIMARY KEY  ,
                type TEXT,
                dmv_plate_number TEXT
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_LOST_PROPERTY_REPORTED_BASE} (
                id INT PRIMARY KEY  ,
                base_name TEXT,
                base_license_number VARCHAR,
                base_phone_number VARCHAR,
                base_address TEXT,
                base_website TEXT
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_INSPECT_BASE} (
                id INT PRIMARY KEY  ,
                base_number TEXT
            );
            
            CREATE OR REPLACE TABLE {SERVICES_SCHEMA}.{TABLE_ANL_DIM_VEHICLE_TYPE} (
                id INT PRIMARY KEY  ,
                vehicle_type VARCHAR
            );
        """
        conn.execute(create_table_query)
        context.log.info("Create dim table for analytics data successfully")
