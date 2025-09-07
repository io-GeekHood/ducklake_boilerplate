import sys
import boto3
import duckdb
import psycopg2
from typing import List, Optional
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from botocore.exceptions import ClientError, ConnectTimeoutError
from botocore.config import Config
from lake.util.conf_loader import Configs
import time
from lake.util.logger import logger
from duckdb import CatalogException
class DuckLakeManager(Configs):
    pg_catalog:str = None
    s3_source_create_command: str = None
    healthy: bool = None
    duckdb_connection: duckdb.DuckDBPyConnection = None
    def __init__(self,config_path):
        super(DuckLakeManager,self).__init__(config_path)
        self.duckdb_connection = duckdb.connect()
        try:
            self._attach()
            result = self.duckdb_connection.execute("SHOW TABLES").fetchall()
            if len(result) == 0:
               raise CatalogException
            logger.info(f"attached existing ducklake {self.DEST.catalog.lake_alias} with {len(result)} tables")
        except CatalogException:
            logger.warning(f"catalog Not found! (Creating {self.DEST.catalog.lake_alias}...)")
            self._connectivity_assessment()
            installation_status = self.__install_duckdb_extensions()
            if installation_status is not None:
                sys.exit(1)
    
    def _get_dest_storage_secret(self):
        return (
            "create secret ds (type s3, "
            + f"key_id '{self.DEST.storage.access_key.get_secret_value()}', "
            + f"secret '{self.DEST.storage.secret.get_secret_value()}', "
            + f"endpoint '{self.DEST.storage.host}:{self.DEST.storage.port}', "
            + f"scope 's3://{self.DEST.storage.scope}',"
            + f"use_ssl {self.DEST.storage.secure}, "
            + f"url_style '{self.DEST.storage.style}'"
            ");"
        )
    def _get_dest_catalog_definition(self):
        return (
            "postgres:"
            + f"dbname={self.DEST.catalog.database} "
            + f"host={self.DEST.catalog.host} "
            + f"port={self.DEST.catalog.port} "
            + f"user={self.DEST.catalog.username.get_secret_value()} "
            + f"password={self.DEST.catalog.password.get_secret_value()} "
        )
    def _get_dest_catalog_secret(self):
        return (
			"create secret catalog (type postgres, "
			+ f"host '{self.DEST.catalog.host}', "
			+ f"port {self.DEST.catalog.port}  , "
			+ f"database '{self.DEST.catalog.database}', "
			+ f"user '{self.DEST.catalog.username.get_secret_value()}',"
			+ f"passweord {self.DEST.catalog.password.get_secret_value()}, "
			+ f"url_style '{self.SRC.storage.style}'"
			");"
		)
        # ATTACH '' AS postgres_db_one (TYPE postgres, SECRET postgres_secret_one);
    def _get_src_s3_secret(self):
        return (
			"create secret target (type s3, "
			+ f"key_id '{self.SRC.storage.access_key.get_secret_value()}', "
			+ f"secret '{self.SRC.storage.secret.get_secret_value()}', "
			+ f"endpoint '{self.SRC.storage.host}:{self.SRC.storage.port}', "
			+ f"scope 's3://{self.SRC.storage.scope}',"
			+ f"use_ssl {self.SRC.storage.secure}, "
			+ f"url_style '{self.SRC.storage.style}'"
			");"
		)
    def _attach(self):
        setup_ducklake_command = (
            f"ATTACH 'ducklake:{self._get_dest_catalog_definition()}' AS {self.DEST.catalog.lake_alias} (DATA_PATH 's3://{self.DEST.storage.scope}');"
        )
        self.duckdb_connection.execute(self._get_dest_storage_secret())
        self.duckdb_connection.execute(self._get_src_s3_secret())
        self.duckdb_connection.execute(setup_ducklake_command)
        self.duckdb_connection.execute(f"use {self.DEST.catalog.lake_alias};")


    def _connectivity_assessment(self):
        s3_object = self.DEST.storage
        pg_object = self.DEST.catalog
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
            logger.debug(f'bucket {s3_object.scope} already exist (no action needed)')
        except ClientError as e:
            logger.error(f'cannot find bucket with name {s3_object.scope} (creating...)')
            try:
                s3_client.create_bucket(Bucket=s3_object.scope)
            except ClientError as e:
                logger.error(f'there is a problem with defined bucket name or \
                             client doesnt seem to have right permissions to create new bucket\
                                -> {s3_object.scope} {e}')
                time.sleep(3)
                self._connectivity_assessment()
        logger.info("s3 connectivity test successfull")
        pg_conn = None
        try:
            logger.info(f"checking connectivity for catalog data-store({pg_object})")
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
