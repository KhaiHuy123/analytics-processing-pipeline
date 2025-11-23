
FROM python:3.10-slim

ENV DBT_DIR=/usr/src/app
WORKDIR $DBT_DIR

COPY . $DBT_DIR

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["dbt", "build", "--profiles-dir", "./"]
