
from lake.connector.s3 import Connector
from lake.connector import connect
def run(src):
    cnn = connect(src)
    cnn.attach()
