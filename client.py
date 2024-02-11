import base64
import json
import threading
import time
import requests


class QueueClient:
    def __init__(self, server_url, headers):
        self.server_url = server_url
        self.headers = headers
        self.subscribe_func = None

    def push(self, key, value):
        url = f"{self.server_url}/push"
        data = json.dumps({"key": key, "value": value})

        response = requests.request("POST", url, data=data, headers=self.headers)

        if response.status_code == 200:
            return "Message pushed successfully."
        else:
            return "Failed to push message."

    def pull(self):
        url = f"{self.server_url}/pop"

        response = requests.post(url, params={}, headers=self.headers)

        response_content = str(response.content)
        response_values = json.loads(response_content[2:len(response_content) - 1])

        if response.status_code == 200:
            if response_values['message'] == 'ok':
                decoded_decimal = [byte for byte in base64.b64decode(response_values['value'])]
                return response_values['key'], decoded_decimal
            else:
                return response_values['message']
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
    server_url = "http://87.247.170.145:8000"
    client = QueueClient(server_url, {'Content-Type': 'application/json'})

    print(client.push("testQueue26", [8, 10, 3, 5]))
    print(client.pull())
