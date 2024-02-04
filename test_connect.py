import s3fs
from pyarrow import fs, parquet as pq
import pyarrow as pa
import pandas as pd
import os


MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ACCESS_KEY_ID='YRLq1PcbrjetETyR'
AWS_SECRET_ACCESS_KEY='I1hp9Sg6KuQq4uLE61mSgbFtzA7kwkGZ'
MINIO_ROOT_USER='minioadmin'
MINIO_ROOT_PASSWORD='minioadmin'
MINIO_BUCKET_NAME='kafka-logins'

print(f"MINIO_BUCKET_NAME: {MINIO_BUCKET_NAME}")
print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
print(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")

print('iniciando...')

# create a new connection to MinIO
minio = fs.S3FileSystem(
     endpoint_override="127.0.0.1:9000",
     access_key=AWS_ACCESS_KEY_ID,
     secret_key=AWS_SECRET_ACCESS_KEY,
     scheme="http")
print(minio)
# fs = s3fs.S3FileSystem(
#     anon=False,
#     # key=s3fs_config.aws_access_key_id,
#     # secret=s3fs_config.aws_secret_access_key,
#     use_ssl=False,
#     client_kwargs={
#         "region_name": "us-east-1",
#         "endpoint_url": "http://localhost:9001",
#         "aws_access_key_id": AWS_ACCESS_KEY_ID,
#         "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
#         "verify": False,
#     }
# )
# print(fs)

path_to_s3_object = f"{MINIO_BUCKET_NAME}/logins.parquet"
print(f"path bucket: {path_to_s3_object}")

data ={"hoge": [11,22,33],"foo": ['a','b','c'],"segm":['first','second','third']}

df = pd.DataFrame(data)
print("DataFrame Pandas...")
print(df)
print(df.info())
print("Escrita no Minio...")
pq.write_to_dataset(
    pa.Table.from_pandas(df),
    root_path=path_to_s3_object,
    partition_cols=['segm'],
    filesystem=minio
)
