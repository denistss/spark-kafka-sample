from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType
from minio import Minio
import s3fs
from pyarrow import fs, parquet as pq
import pyarrow as pa
import pandas as pd
import os


spark = SparkSession.builder.appName("AppSpark").getOrCreate()
print(f'spark connection: {spark}')
spark.sparkContext.setLogLevel('WARN')

# MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ACCESS_KEY_ID='YRLq1PcbrjetETyR'
AWS_SECRET_ACCESS_KEY='I1hp9Sg6KuQq4uLE61mSgbFtzA7kwkGZ'
MINIO_ROOT_USER='minioadmin'
MINIO_ROOT_PASSWORD='minioadmin'
MINIO_BUCKET_NAME='kafka-logins'

print(f"MINIO_BUCKET_NAME: {MINIO_BUCKET_NAME}")
print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
print(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")
s3accessKeyAws = "YRLq1PcbrjetETyR"
s3secretKeyAws = "I1hp9Sg6KuQq4uLE61mSgbFtzA7kwkGZ"
connectionTimeOut = "1000"
IP_CONTAINER='172.18.0.5'
s3endPointLoc = f"http://{IP_CONTAINER}:9000"

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3endPointLoc)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", connectionTimeOut)
spark.sparkContext._jsc.hadoopConfiguration().set("spark.sql.debug.maxToStringFields", "100")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

print('iniciando...')

# create a new connection to MinIO
minio = fs.S3FileSystem(
     endpoint_override=f"{IP_CONTAINER}:9000",
     access_key=AWS_ACCESS_KEY_ID,
     secret_key=AWS_SECRET_ACCESS_KEY,
     scheme="http")
print(minio)

## Test Write Minio OK ##
# path_to_s3_object = f"{MINIO_BUCKET_NAME}/logins2.parquet"
# print(f"path bucket: {path_to_s3_object}")
# data ={"hoge": [11,22,33],"foo": ['a','b','c'],"segm":['first','second','third']}
# df = pd.DataFrame(data)
# print("DataFrame Pandas...")
# print(df)
# print(df.info())
# print("Escrita no Minio...")
# pq.write_to_dataset(
#     pa.Table.from_pandas(df),
#     root_path=path_to_s3_object,
#     partition_cols=['segm'],
#     filesystem=minio
# )

print('Pyspark...')
# example {"user_id": 32880, "user_name": "Susan Miller", "user_address": "75052 Kimberly Courts Suite 157 | South Erintown | NR", "platform": "Laptop", "signup_at": "2023-02-16 21:09:19"}
topicReadSchema = StructType (
    [
        StructField("user_id", StringType()),
        StructField("user_name", StringType()),
        StructField("user_address", StringType()),
        StructField("platform", StringType()),
        StructField("signup_at", StringType())
    ]
)

# spark = SparkSession.builder.appName("AppSpark").getOrCreate()

# print(f'spark connection: {spark}')
# spark.sparkContext.setLogLevel('WARN')

kafkaServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe","user-tracker")                  \
    .option("startingOffsets","earliest")\
    .load() 

kafkaServerStreamingDF = kafkaServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

StreammingDF = kafkaServerStreamingDF.withColumn("value",from_json("value",topicReadSchema))\
        .select(col('value.*'))
        # .createOrReplaceTempView("loginDataView")

print('columns:')
print(StreammingDF.columns)

print('Pyspark - Antes de ler a Fila...')
count = spark.read.table("myTable").count()
print(f'count: {count}')
spark.read.table("myTable").show()

# strm = (StreammingDF.writeStream.outputMode("append").format("parquet")
#  .option("path", "/tmp/logins/logins_table")
#  .option("checkpointLocation", "/tmp/logins/checkpoint")
#  .toTable("myTable"))

(StreammingDF.writeStream.outputMode("append").format("parquet")
 .option("path", "/tmp/logins/logins_table")
 .option("checkpointLocation", "/tmp/logins/checkpoint")
 .start())

# StreammingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# def saveData(dataf, batchId):
#         (dataf.write.format("parquet")
#                 .option("path", "/tmp/logins/logins_table")
#                 .option("checkpointLocation", "/tmp/logins/checkpoint")
#                 .mode("append").save())

# stcal = (StreammingDF.writeStream.forechBatch(saveData)
#          .outputMode("append")
#          .trigger(processingTime="5 seconds").start())
# stcal.awaitTermination()
     
print('Pyspark - Depois de ler a Fila...')
count = spark.read.table("myTable").count()
print(f'count: {count}')
spark.read.table("myTable").show()

# spark.stop()
print("fim do teste...")