import os
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        file_path = event.src_path
        file_name = os.path.basename(file_path)

        if file_name.startswith("ruuvitag"):
            if self.check_tenant2_constraints(file_path):
                print(f"New Tenant 2 file created: {file_path}")
                print("File meets the specified constraints for Tenant 2.")
                self.run_tenant2(file_path)
                print(f"Tenant 2 Batch Processing started:{file_path}")
            else:
                print(f"File created: {file_path}")
                print("File does not meet the specified constraints for Tenant 2.")
        elif file_name.startswith("reviews"):
            if self.check_tenant1_constraints(file_path):
                print(f"New reviews file created: {file_path}")
                print("File meets the specified constraints for Tenant 1.")
                self.run_tenant1(file_path)
                print(f"Tenant 1 Batch Processing started:{file_path}")
            else:
                print(f"File created: {file_path}")
                print("File does not meet the specified constraints for Tenant 1.")

    def check_tenant2_constraints(self, file_path):
        try:
            if not file_path.endswith(".csv"):
                return False
            with open(file_path, "r") as file:
                first_line = file.readline()
                if not first_line.startswith("time,readable_time,acceleration,acceleration_x,acceleration_y,acceleration_z,battery,humidity,pressure,temperature,dev-id"):
                    return False
            if os.path.getsize(file_path) > 7000000:
                return False
            return True
        except Exception as e:
            print(f"Error occurred while checking Tenant 1 file: {e}")
            return False
            
    def run_tenant2(self, file_path):
        command = f"python3 app.py -i {file_path} -c 1 -s 0 -t batch-topic-2"
        try:
            subprocess.Popen(command, shell=True)
        except Exception as e:
            print(f"Error running command: {e}")

    def check_tenant1_constraints(self, file_path):
        try:
            if not file_path.endswith(".csv"):
                return False
            with open(file_path, "r") as file:
                first_line = file.readline()
                if not first_line.startswith("listing_id,id,date,reviewer_id,reviewer_name,comments"):
                    return False
            if os.path.getsize(file_path) > 7000000:
                return False
            return True
        except Exception as e:
            print(f"Error occurred while checking Tenant 1 file: {e}")
            return False
    def run_tenant1(self, file_path):
        command = f"python3 app.py -i {file_path} -c 1 -s 0 -t batch-topic-1"
        try:
            subprocess.Popen(command, shell=True)
        except Exception as e:
            print(f"Error running command: {e}")

if __name__ == "__main__":
    path_to_watch = "./../../../data/client-staging-input-directory"
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=False)
    observer.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
