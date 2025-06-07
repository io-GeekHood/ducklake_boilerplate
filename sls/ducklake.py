import sys
import boto3
import duckdb
import logging
import psycopg2
from typing import List, Optional
from sls.settings import MinioSettings, PostgresSettings
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from botocore.exceptions import ClientError, ConnectTimeoutError
from botocore.config import Config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DuckLakeManager:
    def __init__(self):
        self.minio_settings = MinioSettings()
        self.postgres_settings = PostgresSettings()
        self.duckdb_connection = duckdb.connect()

        infrastructure_setup_status = self.__setup_infrastructure()
        if infrastructure_setup_status is not None:
            sys.exit(1)

        installation_status = self.__install_duckdb_extensions()
        if installation_status is not None:
            sys.exit(1)

        self.__initialize_ducklake()

    def __check_postgres_connection(self) -> bool:
        pg_conn = None
        try:
            logger.info("checking PostgreSQL connection and credentials...")
            pg_conn = psycopg2.connect(
                host=self.postgres_settings.host,
                port=self.postgres_settings.port,
                user=self.postgres_settings.user,
                password=self.postgres_settings.password.get_secret_value(),
                dbname="postgres",
                connect_timeout=5,
            )
            logger.info("postgreSQL connection successful.")
            return True
        except psycopg2.OperationalError as e:
            logger.error(f"postgreSQL connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"an unexpected error occurred during PostgreSQL check: {e}")
            return False
        finally:
            if pg_conn:
                pg_conn.close()

    def __setup_postgres_database(self):
        pg_conn = None
        try:
            logger.info(
                "connecting to PostgreSQL server to check database existence..."
            )
            pg_conn = psycopg2.connect(
                host=self.postgres_settings.host,
                port=self.postgres_settings.port,
                user=self.postgres_settings.user,
                password=self.postgres_settings.password.get_secret_value(),
                dbname="postgres",
            )
            pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with pg_conn.cursor() as cursor:
                db_name = self.postgres_settings.dbname
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
            raise
        finally:
            if pg_conn:
                pg_conn.close()

    def __check_minio_connection(self) -> bool:
        try:
            logger.info("checking minIO/S3 connection and credentials...")

            config = Config(
                connect_timeout=1, read_timeout=1, retries={"max_attempts": 1}
            )
            s3_client = boto3.client(
                "s3",
                endpoint_url=self.minio_settings.endpoint_url,
                aws_access_key_id=self.minio_settings.access_key.get_secret_value(),
                aws_secret_access_key=self.minio_settings.secret_key.get_secret_value(),
                config=config,
            )
            s3_client.list_buckets()
            logger.info("minIO/S3 connection successful.")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in (
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
            ):
                logger.error(
                    "minIO/S3 connection failed: Invalid credentials provided."
                )
            else:
                logger.error(f"minIO/S3 connection failed with client error: {e}")
            return False
        except (ConnectTimeoutError, Exception) as e:
            logger.error(
                f"minIO/S3 connection failed: Cannot connect to endpoint '{self.minio_settings.endpoint}'. Error: {e}"
            )
            return False

    def __setup_minio_bucket(self):
        try:
            logger.info("connecting to minIO/S3 to check bucket existence...")

            s3_client = boto3.client(
                "s3",
                endpoint_url=self.minio_settings.endpoint_url,
                aws_access_key_id=self.minio_settings.access_key.get_secret_value(),
                aws_secret_access_key=self.minio_settings.secret_key.get_secret_value(),
            )

            bucket_name = self.minio_settings.bucket_name
            try:
                # head_bucket is a lightweight way to check for existence
                s3_client.head_bucket(Bucket=bucket_name)
                logger.info(
                    f"minIO bucket '{bucket_name}' already exists. No action needed."
                )
            except ClientError as e:
                # If the error code is 404, the bucket does not exist.
                if e.response["Error"]["Code"] == "404":
                    logger.info(
                        f"minIO bucket '{bucket_name}' not found. Creating it now..."
                    )
                    s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"minIO bucket '{bucket_name}' created successfully.")
                else:
                    # Re-raise any other unexpected errors
                    logger.error("an unexpected S3 ClientError occurred.")
                    raise

        except Exception as e:
            logger.error(f"minIO/S3 setup failed: {e}")
            raise

    def __setup_infrastructure(self) -> Optional[Exception]:
        try:
            if not self.__check_postgres_connection():
                logger.error("could not connect to postgreSQL.")
                return psycopg2.errors.ConnectionException()
            self.__setup_postgres_database()

            if not self.__check_minio_connection():
                logger.error("could not connect to minio/S3.")
                return ConnectTimeoutError()

            self.__setup_minio_bucket()
            logger.info("infrastructure setup completed successfully.")

        except Exception as e:
            logger.error(
                f"during infrastructure setup, an unexpected error has occoured: {e}"
            )
            return e

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
        s3_create_secret_command = (
            "create secret (type s3, "
            + f"key_id '{self.minio_settings.access_key.get_secret_value()}', "
            + f"secret '{self.minio_settings.secret_key.get_secret_value()}', "
            + f"endpoint '{self.minio_settings.endpoint}', "
            + f"use_ssl false, "
            + "url_style 'path'"
            ");"
        )
        logger.debug(s3_create_secret_command)

        setup_ducklake_command = (
            "ATTACH 'ducklake:postgres:"
            + f"dbname={self.postgres_settings.dbname} "
            + f"host={self.postgres_settings.host} "
            + f"port={self.postgres_settings.port} "
            + f"user={self.postgres_settings.user} "
            + f"password={self.postgres_settings.password.get_secret_value()}' "
            + f"as lake (DATA_PATH 's3://{self.minio_settings.bucket_name}');"
        )
        logger.debug(setup_ducklake_command)

        self.duckdb_connection.execute(s3_create_secret_command)
        self.duckdb_connection.execute(setup_ducklake_command)

        self.duckdb_connection.execute("use lake;")
        logger.info("ducklake: 'lake' initialized successfully.")
