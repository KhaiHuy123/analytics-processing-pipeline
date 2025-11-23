
FROM postgres:16

COPY ./warehouse_setup/*.sql /docker-entrypoint-initdb.d/
COPY ./data /data

EXPOSE 5432
