from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType

# Spark Session
spark = SparkSession.builder \
    .appName("clientStreamIngest-1") \
    .master("spark://localhost:7077") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@127.0.0.1:30000/admin?authSource=admin") \
    .getOrCreate()

# DataFrame that reads from the input Kafka topic
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "batch-topic-1")
    .load()
    .selectExpr("CAST(value AS STRING) as kafka_value")
    .selectExpr("from_json(kafka_value, 'listing_id STRING, id STRING, date STRING, reviewer_id STRING, reviewer_name STRING, comments STRING') as data")
    .select("data.*")
    .withColumn("listing_id_id_reviewer_id", concat_ws("-", col("listing_id"), col("id"), col("reviewer_id")))
    .select("listing_id_id_reviewer_id", "date", "reviewer_name", "comments")
)

# Write the transformed messages to MongoDB
df.write.format("mongo").mode("append").option("database", "batch_airbnb").option("collection", "reviews").save()