from lake.connector.core import DuckLakeManager
from lake.util.logger import logger
from duckdb import DuckDBPyConnection


class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
    def exec(self,cmd:str) -> DuckDBPyConnection:
        return self.duckdb_connection.execute(cmd)