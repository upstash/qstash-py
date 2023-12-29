from client import Client
import json


"""
TODO: 
1. Retry Logic
2. Create publish_json along with publish methods
3. Misc method (topics, dlq, etc) 
4. Receiver
5. Tests
"""


def main():
    client.publish(
        {
            "body": json.dumps({"message": "Hello World!"}),
            "url": "https://seanqstash.requestcatcher.com",
            "delay": 10,
            "headers": {"test-header": "test-value"},
        }
    )


client = Client(token="<MY_TOKEN>")

if __name__ == "__main__":
    main()
