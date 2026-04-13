import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Redpanda(Kafka-compatible) broker / topic
KAFKA_BROKER = os.getenv("BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "energy_ticks")

# Store in mounted / data (host´s stream-lakehouse/data)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/bronze/energy_ticks")
CHECKPOINT = os.getenv("CHECKPOINT", "/data/checkpoints/energy_ticks")

schema = StructType([
    StructField("market_ts", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("price_eur_mwh", DoubleType(), True),
    StructField("demand_mw", IntegerType(), True),
    StructField("source", StringType(), True),
])

spark = SparkSession.builder.appName("stream_kafka_to_parquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Read stream from Kafka
df_raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BROKER)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")
         .load()
)

# 2) Value (JSON string) -> Schema parsing
# Ensure parsing stability by removing characters like \r from Windows
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df_json = df_json.withColumn("json_str", regexp_replace(col("json_str"), "\\r", ""))

df = (
    df_json
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("processing_ts", current_timestamp())
)

# 3) Append and save to Parquet + checkpoint
query = (
    df.writeStream
      .format("parquet")
      .option("path", OUTPUT_DIR)
      .option("checkpointLocation", CHECKPOINT)
      .outputMode("append")
      .trigger(processingTime="5 seconds")
      .start() 
)

query.awaitTermination()