# Basic-Distributed-system-using-python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RealTimeDataProcessing") \
    .getOrCreate()

# Define schema for the incoming JSON data
schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("age", IntegerType())

# Create a Kafka DataFrame representing the stream of input lines from Kafka
input_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input_topic") \
    .load()

# Parse JSON data from Kafka stream and apply schema
parsed_stream_df = input_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Perform real-time data processing
processed_stream_df = parsed_stream_df \
    .withColumn("age_group", expr(
        "CASE WHEN age < 18 THEN 'Child' " +
        "WHEN age >= 18 AND age < 65 THEN 'Adult' " +
        "ELSE 'Senior' END"))

# Define output sink for the processed data
output_query = processed_stream_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Await termination of the streaming query
output_query.awaitTermination()

# Stop SparkSession
spark.stop()
