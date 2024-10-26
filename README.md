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

- Build sensors in Dagster to automatically re-run assets if they fail.

- Build data-pipeline test cases to check if it works (at early stage).

- Apply new query optimization strategies in PostgreSQL (especially when data batch is becoming larger).

## What errors can be appeared in this pipeline ?

- Case 1: Request timeout (because of Mother Duck Server)

- Case 2: Parameter of provided APIs is changed or name of columns changed, ....

- That why we need sensors to solve these problems.

## Data Pipeline

![image](https://github.com/user-attachments/assets/79e41ce9-14b9-4cb6-a64c-bb5bb1a6d471)

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

## Why I designed pipeline like this ?

- We need to determined how we can use collected data, make sure you know how and when they are updated.

- You can manually choose what data that you want to analyze because these codes are generated dynamically so we can process any kind of tabular data.

- Everytime you execute pipeline, MinIO will store all versions of data. MinIO is used as GitHub in this case. 

## How to use this pipeline?

- First, you need to create Mother Duck account. [Mother Duck](https://motherduck.com/) is the Cloud Service that provide us availability of using [DuckDB](https://duckdb.org/) on cloud environment.

- Next, make sure you have installed [Docker](https://www.docker.com/) on your local machine. If not, that's ok, because they will help you with that. How to use Docker Desktop ? [Watch this](https://www.youtube.com/watch?v=7y50rZItKCQ)

- You can clone this respository and execute pipeline. Read [run.md](https://github.com/KhaiHuy123/taxi-nyc-processing-pipeline/blob/main/run.md) for more details.

- How to clone this respository ? [Watcth this](https://www.youtube.com/watch?v=8Dd7KRpKeaE)

- After cloning section, you can't use this pipeline immediately because you don't have `manifest.json` in your `dbt project`. Without this file, you can't activate the pipeline.

- Comment all codes related to `dbt_assets` in module `dagster_dbt_processing/__init__.py` and `dagster_dbt_processing/assets/__init__.py`.

- Cut `dbt folder` and `data_source folder` in `dagster_dbt_processing/assets/` (paste them at somewhere you can remember).

- Try to run this pipeline locally. You need to replace `domain name` of services in `docker network` by `localhost` to run it.

- Run `dbt pipeline` to create `manifest.json`. Read [this](https://github.com/KhaiHuy123/analytics-processing-pipeline/tree/main/dbt_processing/transform#readme) for more details. 

- After running `dbt pipeline`, you will see the `manifest.json` in `target folder` of `dbt project`. 

- Replace `localhost` by `domain name` of services.

- Bring `dbt folder` and `data_source folder` back to their original places.
  
- Run `docker-compose build` to build services and run `docker-compose up -d` after all containers are built.
  
- If you are using Docker Destop and you can see these below pictures on your screen, it means your analytics system is ready to work. Good Luck !

![image](https://github.com/user-attachments/assets/dfe418d6-6671-4092-8bde-19b0d5c006b9)

![image](https://github.com/user-attachments/assets/a8d74a00-25b3-48a4-b23e-1111a753df20)

![image](https://github.com/user-attachments/assets/87743cda-a489-4694-bd01-61effa235f44)

![image](https://github.com/user-attachments/assets/73d89836-541b-4b41-a670-ce079a5cc22e)


