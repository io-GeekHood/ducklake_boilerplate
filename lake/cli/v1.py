
from lake.sls.ducklake import DuckLakeManager
from lake.sls.settings import MinioSettings,PostgresSettings
from lake.util.conf_loader import Configs
import boto3
from botocore.exceptions import ClientError, ConnectTimeoutError
from botocore.config import Config
import s3fs
def test():
    dlm = DuckLakeManager('resources/config.yml')
    res = dlm.duckdb_connection.execute("CREATE TABLE flowtrack_202 AS SELECT * FROM read_parquet('s3://flowtrack/logs_2024-09-20T00-15.parquet')")
    res = dlm.duckdb_connection.execute("select * from flowtrack_202;").df()
    print(res)
    # print(res.fetchall())
