from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

print('iniciando...')
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

spark = SparkSession.builder.appName("AppSpark").getOrCreate()

print(f'spark connection: {spark}')
spark.sparkContext.setLogLevel('WARN')

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

StreammingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# spark.stop()
print("fim do teste...")