import subprocess

def perform_action(username, action):
    global user_processes

    if username == "tenant-2":
        if action == "start":
            print("Starting action for tenant-2...")
#python3 app.py -i ../../../data/client-staging-input-directory/ruuvitag.csv -c 1 -s 5 -t stream-topic-2
            subprocess.Popen(["python3", "app.py", 
                            "-i", "../../../data/client-staging-input-directory/ruuvitag.csv", 
                            "-c", "1", "-s", "1", "-t", "stream-topic-2"])
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 tenant-2.py
            subprocess.Popen(["spark-submit", "--packages", 
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0", 
                            "tenant-2.py"],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    elif username == "tenant-1":
        if action == "start":
            print("Starting action for tenant-1...")
#python3 app.py -i ../../../data/client-staging-input-directory/reviews.csv -c 1 -s 5 -t stream-topic-1
            subprocess.Popen(["python3", "app.py", 
                            "-i", "../../../data/client-staging-input-directory/reviews.csv", 
                            "-c", "1", "-s", "1", "-t", "stream-topic-1"])
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 tenant-1.py
            subprocess.Popen(["spark-submit", "--packages", 
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0", 
                            "tenant-1.py"],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) 
    else:
        print("Unknown username. Please try again with a valid username.")

def main():
    username = input("Please enter your username (or type 'exit' to quit): ")

    if username.lower() == "exit":
        print("Exiting...")
        return

    if username != "tenant-1" and username != "tenant-2":
        print("Unknown username. Please try again with a valid username.")
        return

    while True:
        action = input("Do you want to start (or type 'exit' to quit)? ")

        if action.lower() == "exit":
            print("Exiting...")
            break
        if action != "start":
            print("Invalid action. Please enter 'start' or 'exit'.")
            continue
        perform_action(username, action)

if __name__ == "__main__":
    main()
