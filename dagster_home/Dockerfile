
FROM python:3.10-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home
WORKDIR $DAGSTER_HOME

RUN mkdir -p $DAGSTER_HOME/ &&\
    mkdir -p $DAGSTER_HOME/storage && \
    mkdir -p $DAGSTER_HOME/compute_logs && \
    mkdir -p $DAGSTER_HOME/local_artifact_storage

COPY dagster.yaml $DAGSTER_HOME
COPY workspace.yaml $DAGSTER_HOME
COPY requirements.txt $DAGSTER_HOME

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 4000 3000 5433 5432
CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "4000"]
