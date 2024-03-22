import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
from datetime import datetime
import os
import subprocess
import csv

def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()

def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', help='Input file')
    parser.add_argument('-c', '--chunksize', help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', help='sleep time in second')
    parser.add_argument('-t', '--topic', help='kafka topic')
    args = parser.parse_args()

    # Load input data
    INPUT_DATA_FILE = args.input_file
    if not os.path.isfile(INPUT_DATA_FILE):
        print(f"Error: File not found - {INPUT_DATA_FILE}")
        exit()
    chunksize = int(args.chunksize)
    sleeptime = int(args.sleeptime)
    KAFKA_TOPIC = args.topic

    input_data = pd.read_csv(INPUT_DATA_FILE, iterator=True, chunksize=chunksize)
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER})
    
    row_count = 0 
    start_time = time.time() 
    
    for chunk_data in input_data:
        chunk = chunk_data.dropna()
        for index, row in chunk.iterrows():
            json_data = json.dumps(row.to_dict(), default=datetime_converter)
            print(f'DEBUG: Send {json_data} to Kafka')
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
            kafka_producer.poll(0)
            row_count += 1 
        print(f'{row_count} rows processed so far')
        time.sleep(sleeptime)

    kafka_producer.flush()

    # Conditionally set spark_submit_command based on the Kafka topic
    if KAFKA_TOPIC == "batch-topic-2":
        spark_submit_command = [
            'spark-submit',
            '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0',
            os.path.join(os.path.dirname(__file__), 'tenant-2.py')
        ]
    elif KAFKA_TOPIC == "batch-topic-1":
        spark_submit_command = [
            'spark-submit',
            '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0',
            os.path.join(os.path.dirname(__file__), 'tenant-1.py')
        ]
    else:
        print(f"Unknown Kafka topic: {KAFKA_TOPIC}. No Spark job submitted.")
        spark_submit_command = None

    # Run spark-submit command upon completion
    if spark_submit_command:
        try:
            subprocess.run(spark_submit_command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running spark-submit: {e}")

    end_time = time.time() 
    elapsed_time = end_time - start_time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_log_path = os.path.join(os.path.dirname(__file__), '../../../logs', f'batch_log_{KAFKA_TOPIC}.csv')
    metrics = [timestamp, sleeptime, chunksize, row_count, elapsed_time/60, row_count/elapsed_time]
    file_exists = os.path.isfile(batch_log_path)
    with open(batch_log_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['timestamp', 'sleep', 'chunk', 'total', 'exec Time', 'per Sec'])
        writer.writerow(metrics)
    print(f"*** {KAFKA_TOPIC} job done. ***")
    print("*** Data file moving to processed archive. ***")
    # move the file to the processed folder
    file_name, file_extension = os.path.splitext(os.path.basename(INPUT_DATA_FILE))
    new_file_name = f"{file_name}_{timestamp}{file_extension}"
    destination_path = os.path.join(os.path.dirname(__file__), '../../../data/client-staging-input-directory/processed/', new_file_name)
    subprocess.run(['mv', INPUT_DATA_FILE, destination_path], check=True)