# Taxi-NYC-Processing Pipeline 
- [DEMO VIDEO](https://drive.google.com/drive/folders/1RyPruEoPn-zYbq3FFYwOhJEzLBbye8sv?usp=sharing)

## Dashboard

![image](https://github.com/user-attachments/assets/0ac7c933-aafe-40b4-8645-e99d9d78041e)

![image](https://github.com/user-attachments/assets/73b4cf05-b51e-4b38-93bf-29767fc8bab2)

![image](https://github.com/user-attachments/assets/9237f4d8-2688-40ef-bf87-9f1e86a4bd24)

![image](https://github.com/user-attachments/assets/18958f99-a359-4f99-bf99-0dab6078d530)

## Data 

References: [Open Data NYC](https://opendata.cityofnewyork.us/data/) 

- Taxi data Open Data NYC (lastest version)

- Taxi Aggregated data (lastest version)

- Taxi Trip data (lastest version)

## Update Policy

- According to TLC (Taxi Limousine Commisson), taxi-data is provided in different domains and forms.

- TLC is the company that control and operate taxi industry in New York.

- More details ? [Watch this](https://www.nyc.gov/site/tlc/index.page)  

## Future Development 

- Build a machine learning model on Google Colab to perform time series forecasting.

- Build sensors in Dagster to automatically re-run assets if they fail

## What errors can be appeared in this pipeline ?

- Case 1: request timeout (because of Mother Duck Server)

- Case 2: Parameter of provided APIs is changed or name of columns changed, ....

- That why we need sensors to solve these problems

## Data Pipeline

![image](https://github.com/user-attachments/assets/a48e5c9a-8597-4960-a9f9-c035254ecba5)


## The data pipeline execution can be summarized as follows

- Data Ingestion:
  
  Collect data from an API and store it as a DataFrame.


- Data Preprocessing:
  
    Preprocess the data using machine learning and statistical libraries in Python.
  
    Connect to Mother Duck Cloud and design the data model using SQL commands.

  
- Data Replication:
  
    Replicate data from Mother Duck Cloud tables to MinIO via the Relational API and subsequently to a local DuckDB database.
  
    Note that since DuckDB is an embedded database, all data will be deleted after loading and logging.

  
- Data Transfer to Postgres:
  
    Download data from MinIO to the local machine as CSV files in preparation for loading into Postgres.
  
    Automatically generate DML statements, including primary keys, foreign keys, and indexes (if applicable).
  
  
- DBT Execution:
  
    Execute the DBT pipeline after data has been loaded into Postgres.
  
    After this step, all data information will be aggregated and stored as files on the local machine.

  
- Visualization:
 
  Visualize the data using Streamlit.

## Technique

    .env
    
    # PostgresSQL
    POSTGRES_HOST=thes_psql
    POSTGRES_PORT=5432
    POSTGRES_DB=admin_database
    POSTGRES_SCHEMA=dbt_source
    POSTGRES_USER=admin_user
    POSTGRES_PASSWORD=admin123
    POSTGRES_HOST_AUTH_METHOD=trust
    POSTGRES_ZONES=zones
    POSTGRES_SERVICES=services
    POSTGRES_REPORT=report
    POSTGRES_TRIPS=trips
    POSTGRES_PROCESS=dbt_source_process
    
    # MySQL
    MYSQL_HOST=thes_mysql
    MYSQL_USER=mysql_user
    MYSQL_PASSWORD=mysql_password
    MYSQL_ROOT_PASSWORD=mysql_root_password
    MYSQL_DB=mysql_database
    MYSQL_ZONES=zones
    MYSQL_SERVICES=services
    MYSQL_REPORT=report
    
    # Dagster
    DAGSTER_PG_HOST=dagster_postgres
    DAGSTER_PG_USER=admin_user
    DAGSTER_PG_PASSWORD=admin123
    DAGSTER_PG_DB=postgres
    DAGSTER_PG_DB_HOST_AUTH_METHOD=trust
    
    # MinIO
    ENDPOINT_URL_MINIO=minio:9000
    AWS_ACCESS_KEY_ID_MINIO=minio
    AWS_SECRET_ACCESS_KRY_MINIO=minio123
    MINIO_ROOT_USER=minio
    MINIO_ROOT_PASSWORD=minio123
    MINIO_ACCESS_KEY=minio
    MINIO_SECRET_KEY=minio123
    MINIO_ANL_BUCKET=analytics
    MINIO_GEO_BUCKET=geometry
    MINIO_TS_BUCKET=timeseries
    MINIO_MD_BUCKET=motherduck
    
    # DuckDB / MotherDuck
    MOTHER_DUCK_TOKEN=
    MOTHER_DUCK_DATABASE=
    MOTHER_DUCK_SHARE_URL=
