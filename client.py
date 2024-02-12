import base64
import json
import threading
import time

import requests


class QueueClient:
    def __init__(self, url, headers):
        self.server_url = url
        self.headers = headers

    def push(self, key, value):
        url = f"{self.server_url}/push"
        key_str = str(key)
        encoded_value = base64.b64encode(json.dumps(value).encode('utf-8')).decode('utf-8')
        data = json.dumps({"key": key_str, "value": encoded_value})
        response = requests.request("POST", url, data=data, headers=self.headers)

        if response.status_code == 200:
            return f"Message {value} pushed successfully to queue {key_str}."
        else:
            return response.content.decode('utf-8')

    def pull(self):
        url = f"{self.server_url}/pop"
        response = requests.post(url, headers=self.headers)
        if response.status_code == 200:
            response_values = response.json()
            if response_values['message'] == 'ok':
                decoded_value = base64.b64decode(response_values['value'])
                decoded_decimal = [byte for byte in decoded_value]
                return response_values['key'], decoded_decimal
            else:
                return response_values['message']
        else:
            return response.content.decode('utf-8')


    def pull_periodically(self, f):
        consecutive_failed_pulls = 0
        while True:
            pull_result = self.pull()
            if isinstance(pull_result, tuple):
                key, value = pull_result
                if key and value:
                    f(key, value)
                consecutive_failed_pulls = 0
            else:
                consecutive_failed_pulls += 1
                if consecutive_failed_pulls >= 100:
                    break
            time.sleep(1)

    def subscribe(self, f):
        thread = threading.Thread(target=self.pull_periodically, args=(f,))
        thread.start()


def push_messages(client, num_messages=100, queue_name="testQueue111"):
    for i in range(num_messages):
        message = f"Test message {i}"
        result = client.push(queue_name, message)
        if "successfully" in result:
            print(f"Pushed: {message} to {queue_name}")
        else:
            print(f"Failed to push {message} to {queue_name}. Error: {result}")
        time.sleep(0.1)


def subscription_handler(key, value):
    print(f"Subscribed: Received message from {key}. Value: {value}")


def subscribe_and_receive_messages(client):
    client.subscribe(subscription_handler)


def subscribe_test():
    server_url = "http://87.247.170.145:8000"
    headers = {'Content-Type': 'application/json'}
    client_subscriber = QueueClient(server_url, headers)
    client_pusher = QueueClient(server_url, headers)
    subscriber_thread = threading.Thread(target=subscribe_and_receive_messages, args=(client_subscriber,))
    subscriber_thread.start()
    time.sleep(2)
    num_messages = 10
    queue_name = "testQueue111"
    push_messages(client_pusher, num_messages, queue_name)
    time.sleep(5)


############################################################
def push_load_test(num_clients=10, num_messages=100):
    clients = [QueueClient(server_url, {'Content-Type': 'application/json'}) for _ in
               range(num_clients)]  # Create clients
    threads = [threading.Thread(target=push_messages, args=(client, num_messages, idx)) for idx, client in
               enumerate(clients)]

    start_time = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    end_time = time.time()
    print(f"Total time: {end_time - start_time}")


#############################################################
def pull_messages(client, num_pulls=100, client_id=0):
    success_count = 0
    failure_count = 0
    for i in range(num_pulls):
        result = client.pull()
        if isinstance(result, tuple):
            success_count += 1
            print(f"Client {client_id}: Pull {i + 1}/{num_pulls} succeeded. Key: {result[0]}, Value: {result[1]}")
        else:
            failure_count += 1
            print(f"Client {client_id}: Pull {i + 1}/{num_pulls} failed. Error: {result}")
    print(f"Client {client_id}: Finished pulling messages. Success: {success_count}, Failures: {failure_count}")
    return success_count, failure_count


def concurrent_pull_test(client, num_pullers=5, num_messages=100):
    pullers = [threading.Thread(target=pull_messages, args=(client, num_messages, i)) for i in range(num_pullers)]
    for puller in pullers:
        puller.start()
    for puller in pullers:
        puller.join()


if __name__ == "__main__":
    server_url = "http://87.247.170.145:8000"
    client = QueueClient(server_url, {'Content-Type': 'application/json'})
    print(client.pull())
    subscribe_test()