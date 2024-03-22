from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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
    .option("subscribe", "stream-topic-2")
    .load()
    .selectExpr("CAST(value AS STRING) as kafka_value")
    .selectExpr("from_json(kafka_value, 'time STRING, readable_time STRING, acceleration STRING, acceleration_x STRING, acceleration_y STRING, humidity STRING, pressure STRING, temperature STRING, dev_id STRING') as data")
    .select("data.*")
    .drop("time")
    .withColumnRenamed("readable_time", "time")
    .select("time", "acceleration", "acceleration_x", "acceleration_y", "humidity", "pressure", "temperature", "dev_id")
)

# Define a function to write to MongoDB
def write_to_mongo(df, epoch_id):
    df.write.format("mongo").mode("append").option("database", "stream_zoo").option("collection", "tortoise").save()

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
