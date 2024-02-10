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
            if self.subscribe_func != None:
                self.subscribe_func(key, value)
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

    def subscribe(self, f):
        self.subscribe_func = f


if __name__ == "__main__":
    server_url = "http://localhost:8000"
    client = QueueClient(server_url)



