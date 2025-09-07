import importlib
import sys
from typing import Literal, cast,Union
from .s3 import Connector as s3_cnn
from .kafka import Connector as kafka_cnn
def connect(servicer:Literal['kafka','s3'],config_path:str) -> Union[s3_cnn,kafka_cnn]:
    try:
        module = importlib.import_module(f".{servicer}", package=__name__)
        return module.Connector(config_path)
    except ImportError:
        print(f"Error: Server '{servicer}' not found")
        sys.exit(1)
