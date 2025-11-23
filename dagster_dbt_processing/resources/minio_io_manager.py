
import io
import json
import os
import pandas as pd
import polars as pl
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from minio.commonconfig import ENABLED
from minio.error import S3Error
from minio.api import VersioningConfig
from datetime import datetime

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class MinIOHelper:
    @staticmethod
    def read_dataframe(minio_client: Minio, bucket_name, object_name,
                       file_extension, version_id, context):
        try:
            if file_extension == 'parquet':
                parquet_object = minio_client.get_object(bucket_name=bucket_name,
                                                         object_name=object_name,
                                                         version_id=version_id)
                parquet_content = io.BytesIO(parquet_object.read())
                return pl.read_parquet(parquet_content)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
        except S3Error as e:
            context.log.info(f"Error reading {object_name}: {e}")
            return None

    @staticmethod
    def upload_dataframe(minio_client: Minio, bucket_name, object_name,
                         dataframe: pd.DataFrame, file_extension, context):
        try:
            if file_extension == 'parquet':
                with io.BytesIO() as buffer:
                    dataframe.to_parquet(buffer)
                    buffer.seek(0)
                    minio_client.put_object(bucket_name, object_name,
                                            buffer, len(buffer.getvalue()), 'application/octet-stream')
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
            context.log.info(f"Uploaded {object_name} successfully")
        except S3Error as e:
            context.log.info(f"Error uploading {object_name}: {e}")


class MinIO_IOManager(IOManager):
    def __init__(self, config):
        self._config = self.initialize_minio(config)
        self.bucket_name = config["bucket"]
        self.create_bucket(self._config, self.bucket_name)
        self.versioning_config = VersioningConfig(ENABLED)
        self._config.set_bucket_versioning(self.bucket_name, self.versioning_config)
        self._minio_helper = MinIOHelper()

    @staticmethod
    def initialize_minio(config):
        try:
            return Minio(
                endpoint=config["endpoint_url"],
                access_key=config["aws_access_key_id"],
                secret_key=config["aws_secret_access_key"],
                secure=False
            )
        except S3Error as e:
            raise ValueError(f"Fail to create MinIo connector because of: {e}")

    @staticmethod
    def create_bucket(minio_client: Minio, bucket_name):
        if minio_client.bucket_exists(bucket_name):
            print(f"{bucket_name} has already exists")
        else:
            print(f"{bucket_name} does not exist")
            print(f"Creating bucket {bucket_name}")
            minio_client.make_bucket(bucket_name)

    @staticmethod
    def view_list_object(list_objects, object_name, context):
        context.log.info(f"Version of object {object_name}")
        for obj in list_objects:
            context.log.info(obj.version_id)

    @staticmethod
    def view_latest_version_id(list_objects, context):
        latest_version_id = sorted(list_objects, key=lambda obj: obj.version_id, reverse=True)[0].version_id
        context.log.info("latest_version_id")
        context.log.info(latest_version_id)
        return latest_version_id

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        context.log.info(f"Preview data before upload ... ")
        context.log.info(obj)

        object_name = context.asset_key.path[1].replace("_", "")
        object_name = f'{object_name}/{object_name}_{day_month_year}.parquet'
        file_extension = object_name.rsplit('.', 1)[-1]

        self._minio_helper.upload_dataframe(
            minio_client=self._config,
            bucket_name=self.bucket_name,
            object_name=object_name,
            dataframe=obj,
            file_extension=file_extension,
            context=context
        )
        context.log.info(f"Upload to MinIO successfully ... ")
        context.log.info(object_name)

    def load_input(self, context: InputContext):
        object_name = context.asset_key.path[1].replace("_", "")
        d_object_name = f'{object_name}/{object_name}_{day_month_year}.parquet'

        file_name = f"{object_name}.parquet"
        file_extension = d_object_name.rsplit('.', 1)[-1]

        list_objects_generator = self._config.list_objects(self.bucket_name,
                                                           include_version=True,
                                                           prefix=d_object_name)
        list_objects = list(list_objects_generator)
        self.view_list_object(list_objects, d_object_name, context)
        latest_version_id = self.view_latest_version_id(list_objects, context)

        data = self._minio_helper.read_dataframe(
            minio_client=self._config,
            bucket_name=self.bucket_name,
            object_name=d_object_name,
            file_extension=file_extension,
            version_id=latest_version_id,
            context=context
        )
        context.log.info("Read data from MinIO successfully ... ")

        output_directory = f"./processed_data/{object_name}"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        path = os.path.join(output_directory, file_name)

        res = data.to_pandas()
        res.to_parquet(path)

        context.log.info(f"Download from MinIO successfully ... ")
        context.log.info(path)
