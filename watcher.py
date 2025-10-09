import os
import time
import subprocess

visited = set()

try:
    while True:
        files = os.listdir("./config/")
        for file in files:
            path = f"./config/{file}"
            if not os.path.isfile(path):
                continue
            modification_timestamp = os.path.getmtime(path)
            if modification_timestamp not in visited:
                visited.add(modification_timestamp)
                os.rename(path, "./config/config.json")
                time.sleep(3)
                subprocess.run(["python3", "app.py"])
                print(f"{file} processed successfully.")
        time.sleep(2)
except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
