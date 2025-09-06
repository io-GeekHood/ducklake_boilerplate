
from lake.connector.core import DuckLakeManager
from lake.connector.s3 import Connector
from lake.util.conf_loader import Configs
from botocore.exceptions import ClientError, ConnectTimeoutError
from botocore.config import Config
import s3fs
import duckdb
def test():

    cnn = Connector('resources/config.yml')


    res = cnn.exec("CREATE TABLE flowtrack_202 AS SELECT * FROM read_parquet('s3://flowtrack/logs_2024-09-20T00-15.parquet')")
    res = cnn.exec("select * from flowtrack_202;").df()
    print(res)
    # print(res.fetchall())
