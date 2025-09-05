import sys
import boto3
import duckdb
import logging
import psycopg2
from typing import List, Optional
from lake.sls.settings import MinioSettings,SrcMinioSettings, PostgresSettings
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from botocore.exceptions import ClientError, ConnectTimeoutError
from botocore.config import Config
from lake.util.conf_loader import Configs
import time
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DuckLakeManager(Configs):
    pg_catalog:str = None
    s3_source_create_command: str = None
    duckdb_connection: duckdb.DuckDBPyConnection = None
    def __init__(self,config_path):
        super(DuckLakeManager,self).__init__(config_path)
        self._connectivity_assessment()
        self.duckdb_connection = duckdb.connect()
        print(self.pg_catalog)
        print(self.s3_source_create_command)
        installation_status = self.__install_duckdb_extensions()
        if installation_status is not None:
            sys.exit(1)

        self.__initialize_ducklake()

    def _connectivity_assessment(self):
        s3_object = self.SRC.storage

        pg_object = self.SRC.catalog
        try:
            logger.info(f"checking connectivity for source s3")
            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_object.get_address,
                aws_access_key_id=s3_object.access_key.get_secret_value(),
                aws_secret_access_key=s3_object.secret.get_secret_value()
            )
        except ClientError as e:
            logger.error(f'failed to assert connection to s3://{s3_object.scope}| {e}')
            time.sleep(3)
            self._connectivity_assessment()
        try:
            bucket_data = s3_client.head_bucket(Bucket=s3_object.scope)
            assert bucket_data['ResponseMetadata']['HTTPStatusCode'] == 200
            logger.error(f'bucket {s3_object.scope} already exist (no action needed)')
        except ClientError as e:
            logger.error(f'cannot find bucket with name {s3_object.scope} (creating...)')
            try:
                s3_client.create_bucket(Bucket=s3_object.scope)
            except ClientError as e:
                logger.error(f'there is a problem with defined bucket name or \
                                client doesnt seem to have right permissions to create new bucket\
                                -> {s3_object.scope}')
                time.sleep(3)
                self._connectivity_assessment()
        self.s3_source_create_command = (
            "create secret storage (type s3, "
            + f"key_id '{s3_object.access_key.get_secret_value()}', "
            + f"secret '{s3_object.secret.get_secret_value()}', "
            + f"endpoint '{s3_object.host}:{s3_object.port}', "
            + f"scope 's3://{s3_object.scope}',"
            + f"use_ssl {s3_object.secure}, "
            + f"url_style '{s3_object.style}'"
            ");"
        )
        logger.info("s3 connectivity test successfull")
        pg_conn = None
        try:
            logger.info(f"checking connectivity for catalog relational storage")
            pg_conn = psycopg2.connect(
                host=pg_object.host,
                port=pg_object.port,
                user=pg_object.username.get_secret_value(),
                password=pg_object.password.get_secret_value(),
                dbname='postgres',
            )
            pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with pg_conn.cursor() as cursor:
                db_name = pg_object.database
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
                )
                exists = cursor.fetchone()
                if not exists:
                    logger.info(f"database '{db_name}' not found. Creating it now...")
                    cursor.execute(f'CREATE DATABASE "{db_name}"')
                    logger.info(f"database '{db_name}' created successfully.")
                else:
                    logger.info(
                        f"database '{db_name}' already exists. No action needed."
                    )
        except psycopg2.Error as e:
            logger.error(f"postgreSQL error: {e}")
            time.sleep(3)
            self._connectivity_assessment()
        finally:
            if pg_conn:
                pg_conn.close()
        logger.info("catalog connectivity test successfull")
        self.pg_catalog = (
            "postgres:"
            + f"dbname={pg_object.database} "
            + f"host={pg_object.host} "
            + f"port={pg_object.port} "
            + f"user={pg_object.username.get_secret_value()} "
            + f"password={pg_object.password.get_secret_value()} "
        )

    def __install_duckdb_extensions(
        self, extensions: List = ["ducklake", "postgres", "httpfs"]
    ) -> Optional[Exception]:
        for extension_name in extensions:
            try:
                self.duckdb_connection.sql(f"INSTALL {extension_name};")
                self.duckdb_connection.sql(f"LOAD {extension_name};")
                logger.info(f"{extension_name} installed and loaded successfully.")

            except duckdb.HTTPException as e:
                logger.error(
                    f"extension {extension_name} not found or you might have connectivity issues:\n{e}"
                )
                return e
            except Exception as e:
                logger.error(
                    f"during installation of {extension_name} an unexpected error has occoured: {e}"
                )
                return e

    # TODO: add proper error handling to assert database and s3 bucket conditions
    def __initialize_ducklake(self):
        setup_ducklake_command = (
            f"ATTACH 'ducklake:{self.pg_catalog}' AS lake (DATA_PATH 's3://test');"
        )
        if self.DEST.
        data = (
            "create secret datas (type s3, "
            + f"key_id '{self.DEST.target.access_key.get_secret_value()}', "
            + f"secret '{self.DEST.target.secret.get_secret_value()}', "
            + f"endpoint '{self.DEST.target.host}:{self.DEST.target.port}', "
            + f"scope 's3://{self.DEST.target.scope}',"
            + f"use_ssl {self.DEST.target.secure}, "
            + f"url_style '{self.DEST.target.style}'"
            ");"
        )
        logger.debug(setup_ducklake_command)
        self.duckdb_connection.execute(self.s3_source_create_command)
        self.duckdb_connection.execute(data)
        self.duckdb_connection.execute(setup_ducklake_command)
        self.duckdb_connection.execute("use lake;")
        config_params = "SET http_timeout=100;"
        logger.info("ducklake: 'lake' initialized successfully.")
