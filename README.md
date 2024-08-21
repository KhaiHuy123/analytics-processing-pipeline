# Taxi-NYC-Processing Pipeline 
[DEMO VIDEO](https://drive.google.com/drive/folders/1RyPruEoPn-zYbq3FFYwOhJEzLBbye8sv?usp=sharing)

## Dashboard
![image](https://github.com/user-attachments/assets/bfc1f9a0-7a0d-4202-9bd4-7e74e1c4e266)

![image](https://github.com/user-attachments/assets/6357b964-e19a-4779-a4ed-976a234c7ebf)

## Data 

Can be found on  [Open Data NYC](https://opendata.cityofnewyork.us/data/) 

- Taxi data Open Data NYC (lastest version)

- Taxi Aggregated data (lastest version)

- Taxi Trip data (lastest version)

## Future Development 

- Build a machine learning model on Google Colab to perform time series forecasting.    

## Data Pipeline

![image](https://github.com/user-attachments/assets/68eefa2f-9679-410d-98a2-82b114701621)

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

 
