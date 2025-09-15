
from lake.connector.core import DuckLakeManager
from lake.util.logger import logger



class Connector(DuckLakeManager):
    def __init__(self,config_path):
        super(Connector,self).__init__(config_path)
                

    def deploy(self):
        # connect to your storage src
        self.duckdb_connection.execute(f"use {self.SRC.storage.lake_alias};")
        read_from_src_storage = "select * from read_parquet('s3://big_parquets_bucket/logs_2024-09-*.parquet')"
        result = self.duckdb_connection.execute(read_from_src_storage)
        print(result.df())

        # connect to your postgres src
        self.duckdb_connection.execute(f"use {self.SRC.postgres.lake_alias};")
        read_from_src_pg = "select * from public.my_table_in_src limit 100 ;"
        result = self.duckdb_connection.execute(read_from_src_pg)
        print(result.df())

        # connect to your ducklake
        self.duckdb_connection.execute(f"use {self.DEST.catalog.lake_alias};")
        read_from_ducklake = "select * from stream_table;"
        result = self.duckdb_connection.execute(read_from_src_pg)
        result = self.duckdb_connection.sql(read_from_ducklake)
        print(result.df())
