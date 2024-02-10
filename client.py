import threading
import time

import requests


class QueueClient:
    def __init__(self, server_url):
        self.server_url = server_url
        self.subscribe_func = None

    def push(self, key, value):
        url = f"{self.server_url}/push"

        data = {"queue_name": key, "value": value}
        response = requests.post(url, json=data)

        if response.status_code == 200:
            return "Message pushed successfully."
        else:
            return "Failed to push message."

    def pull(self):
        url = f"{self.server_url}/pop"
        response = requests.post(url, params={})

        if response.status_code == 200:
            return response.json()["key"], response.json()["value"]
        else:
            return "Failed to pop message."

    def pull_periodically(self, f):
        while True:
            key, value = self.pull()
            if key and value:
                f(key, value)
            time.sleep(1)

    def subscribe(self, f):
        self.subscribe_func = f
        thread = threading.Thread(target=self.pull_periodically(f))
        thread.start()


if __name__ == "__main__":
    server_url = "http://localhost:8000"
    client = QueueClient(server_url)
    client.subscribe(None)
