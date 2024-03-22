from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark Session
spark = SparkSession.builder \
    .appName("clientStreamIngest-1") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@127.0.0.1:30000/admin?authSource=admin") \
    .getOrCreate()

# Define global variable for termination condition
terminate_query = False

# Create a DataFrame that reads from the input Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stream-topic-1")
    .load()
    .selectExpr("CAST(value AS STRING) as kafka_value") 
    .selectExpr("from_json(kafka_value, 'listing_id STRING, id STRING, date STRING, reviewer_id STRING, reviewer_name STRING, comments STRING') as data")
    .select("data.*")
    .withColumn("listing_id_id_reviewer_id", concat_ws("-", col("listing_id"), col("id"), col("reviewer_id")))
    .select("listing_id_id_reviewer_id", "date", "reviewer_name", "comments")
)

# Define a function to write to MongoDB
def write_to_mongo(df, epoch_id):
    df.write.format("mongo").mode("append").option("database", "stream_airbnb").option("collection", "reviews").save()

# Write the DataFrame near-real-time to MongoDB
query = (
    df.writeStream
    .outputMode("update")
    .foreachBatch(write_to_mongo)
    .trigger(processingTime="1 second")
    .start()
)

try:
    while not terminate_query:
        # Run indefinitely until termination condition is set
        pass
except KeyboardInterrupt:
    # Set termination condition when KeyboardInterrupt is received
    terminate_query = True
    query.stop()