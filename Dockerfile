
FROM python:3.10-slim

WORKDIR /opt/dagster/app

RUN mkdir -p /opt/dagster/app/dagster_dbt_processing
RUN mkdir -p /opt/dagster/app/dagster_dbt_processing_tests
RUN mkdir -p /opt/dagster/app/dbt_processing/transform

COPY requirements.txt /opt/dagster/app
COPY pyproject.toml /opt/dagster/app
COPY setup.py /opt/dagster/app
COPY setup.cfg /opt/dagster/app
COPY .env /opt/dagster/app
COPY processed_data /opt/dagster/app/processed_data
COPY datasource_init/data /opt/dagster/app/datasource_init/data
COPY dagster_dbt_processing /opt/dagster/app/dagster_dbt_processing
COPY dagster_dbt_processing_tests /opt/dagster/app/dagster_dbt_processing_tests
COPY dbt_processing/transform /opt/dagster/app/dbt_processing/transform

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 4000
CMD [ "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_dbt_processing" ]
