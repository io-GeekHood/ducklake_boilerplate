# import json
# import sys
# import time

# import minio
# import pyarrow.parquet as pq
# import s3fs
# import pandas as pd
# import os
# import logging
# import asyncio
# # import dask.dataframe as pd
# from urllib.parse import urlsplit
# from minio import Minio
# import numpy as np
# from pymongo import MongoClient
# import pyarrow as pa
# logging.basicConfig(level=logging.DEBUG)

# S3Host = os.environ.get('S3_HOST', 'http://localhost:9000/')
# MinioUser = os.environ.get('AWS_ACCESS', 'hitadmin')
# MinioPass = os.environ.get('AWS_SECRET', 'sghllkfij,dhvrndld')
# IMAGE_BUCKET = os.environ.get('DKP_IMG_BUCK_NAME',"test")
# REFRENCE_BUCKET = os.environ.get('DKP_REF_BUCK_NAME',"test")


# def read_parquets(bucket:str) -> pd.DataFrame:
#     fsremote = s3fs.S3FileSystem(
#         anon=False,
#         use_ssl=False,
#         client_kwargs={
#             "region_name": "eu-central-1",
#             "endpoint_url": S3Host,
#             "aws_access_key_id": MinioUser,
#             "aws_secret_access_key": MinioPass,
#             "verify": True,
#         }
#     )
#     logging.info(f"connecting to s3fs ({S3Host}) user ({MinioUser}) secret ({MinioPass})")
#     pandas_dataframe = pq.ParquetDataset(f"/{bucket}/", filesystem=fsremote).read_pandas().to_pandas()

#     file_paths = []
#     # for idx,item in pandas_dataframe.iterrows():
#     #     file_name = "digikala-images-main/" + str(item["file_id"]) + "_" + str(item["file_index"]) + ".jpg"
#     #     file_paths.append(file_name)
#     # pandas_dataframe["file_name"] = file_paths

#     pandas_dataframe = pandas_dataframe.drop_duplicates(subset=['file_id','file_index'],keep="first")
#     logging.info(f"parquet loaded from s3fs {pandas_dataframe.head(4)}")
#     return pandas_dataframe
