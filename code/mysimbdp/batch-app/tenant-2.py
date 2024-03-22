from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark Session
spark = SparkSession.builder \
    .appName("clientBatchIngest-2") \
    .master("spark://localhost:7077") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@127.0.0.1:30000/admin?authSource=admin") \
    .getOrCreate()

# Create a DataFrame that reads from the input Kafka topic
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "batch-topic-2")
    .load()
    .selectExpr("CAST(value AS STRING) as kafka_value")  
    .selectExpr("from_json(kafka_value, 'time STRING, readable_time STRING, acceleration STRING, acceleration_x STRING, acceleration_y STRING, humidity STRING, pressure STRING, temperature STRING, dev_id STRING') as data")
    .select("data.*")
    .drop("time")
    .withColumnRenamed("readable_time", "time")
    .select("time", "acceleration", "acceleration_x", "acceleration_y", "humidity", "pressure", "temperature", "dev_id")
)

# Write the transformed messages to MongoDB
df.write.format("mongo").mode("append").option("database", "batch_zoo").option("collection", "tortoise").save()
