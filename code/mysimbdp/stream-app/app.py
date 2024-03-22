import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
from datetime import datetime
import os
import csv
import subprocess

def datetime_converter(dt):
    if isinstance(dt, datetime):
        return dt.__str__()

def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

def move_file(source_path, destination_path):
    try:
        subprocess.run(['mv', source_path, destination_path], check=True)
        print(f"Moved file from {source_path} to {destination_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error moving file: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', help='Input file')
    parser.add_argument('-c', '--chunksize', help='chunk size for big file')
    parser.add_argument('-s', '--sleeptime', help='sleep time in second')
    parser.add_argument('-t', '--topic', help='kafka topic')
    parser.add_argument('-w', '--wait_time', help='maximum time to wait for input file in seconds', default=3600)  # Default: 1 hour
    args = parser.parse_args()

    chunksize = int(args.chunksize)
    sleeptime = int(args.sleeptime)
    wait_time = int(args.wait_time)
    KAFKA_TOPIC = args.topic

    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER})
    row_count = 0 
    exec_row_count = 0
    row_limit = 50000 
    start_waiting_time = time.time()

    while True:
        if os.path.isfile(args.input_file) and (row_count <= row_limit):
            input_data = pd.read_csv(args.input_file, iterator=True, chunksize=chunksize)
            start_time = time.time() 
            for chunk_data in input_data:
                chunk = chunk_data.dropna()
                for index, row in chunk.iterrows():
                    json_data = json.dumps(row.to_dict(), default=datetime_converter)
                    print(f'DEBUG: Send {json_data} to Kafka')
                    kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=kafka_delivery_error)
                    kafka_producer.poll(0)
                    row_count += 1 
                    exec_row_count += 1
                    if row_count >= row_limit:
                        print("Row limit reached. Waiting for new data input...")
                        break
                else:
                    print(f'{row_count} rows processed so far')
                    time.sleep(sleeptime)
                    continue
                break  # Break out of the outer loop if row count reaches 500
                
            kafka_producer.flush()
            end_time = time.time() 
            elapsed_time = end_time - start_time
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_log_path = os.path.join(os.path.dirname(__file__), '../../../logs', f'stream_log_{KAFKA_TOPIC}.csv')
            metrics = [timestamp, sleeptime, chunksize, exec_row_count, elapsed_time/60, exec_row_count/elapsed_time]
            exec_row_count = 0 
            file_exists = os.path.isfile(batch_log_path)
            with open(batch_log_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['timestamp', 'sleep', 'chunk', 'total', 'exec', 'per Sec'])
                writer.writerow(metrics)
            
            # Move the file to the processed folder
            file_name, file_extension = os.path.splitext(os.path.basename(args.input_file))
            new_file_name = f"{file_name}_{timestamp}{file_extension}"
            destination_path = os.path.join(os.path.dirname(__file__), '../../../data/client-staging-input-directory/processed/', new_file_name)
            move_file(args.input_file, destination_path)

        elif row_count >= row_limit:
            # Check if maximum waiting time exceeded
            if time.time() - start_waiting_time > wait_time:
                print(f"Maximum waiting time ({wait_time} seconds) exceeded. Exiting...")
                break
            print(f"Row limit reached. Please Upgrade limit...")
            time.sleep(5)
        else:
            # Check if maximum waiting time exceeded
            if time.time() - start_waiting_time > wait_time:
                print(f"Maximum waiting time ({wait_time} seconds) exceeded. Exiting...")
                break

            print(f"Input data not found. Waiting for data input...")
            time.sleep(5)
