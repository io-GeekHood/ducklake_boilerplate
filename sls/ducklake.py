import sys
import duckdb
import logging
from typing import List, Optional
from sls.settings import MinioSettings, PostgresSettings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DuckLakeManager:
    def __init__(self):
        self.minio_settings = MinioSettings()
        self.postgres_settings = PostgresSettings()
        self.duckdb_connection = duckdb.connect()

        installation_status = self.__install_duckdb_extensions()
        if installation_status is not None:
            sys.exit(1)

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
    def initialize_ducklake(self):
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
