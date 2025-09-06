from lake.connector.core import DuckLakeManager
from lake.util.logger import logger
from duckdb import DuckDBPyConnection


class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
        target_secret = (
            "create secret target (type s3, "
            + f"key_id '{self.DEST.storage.access_key.get_secret_value()}', "
            + f"secret '{self.DEST.storage.secret.get_secret_value()}', "
            + f"endpoint '{self.DEST.storage.host}:{self.DEST.storage.port}', "
            + f"scope 's3://{self.DEST.storage.scope}',"
            + f"use_ssl {self.DEST.storage.secure}, "
            + f"url_style '{self.DEST.storage.style}'"
            ");"
        )
        res = self.duckdb_connection.execute(target_secret)
        logger.debug(f"TargetSecret={target_secret} RESULT {res.fetchone()}")
        setup_ducklake_command = (
            f"ATTACH 'ducklake:{self.pg_catalog}' AS {self.SRC.catalog.lake_alias} (DATA_PATH 's3://{self.SRC.storage.scope}');"
        )
        res = self.duckdb_connection.execute(setup_ducklake_command)
        self.duckdb_connection.execute(f"use {self.SRC.catalog.lake_alias};")
    def exec(self,cmd:str) -> DuckDBPyConnection:
        return self.duckdb_connection.execute(cmd)