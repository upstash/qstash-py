from client import Client
import json


"""
TODO: 
1. PublishResponse
2. Misc method (topics, dlq, etc) 
3. Receiver
4. Tests
"""


def main():
    res = client.publish_json(
        {
            "body": {"ex_key": "ex_value"},
            "url": "https://seanqstash.requestcatcher.com",
            "delay": 5,
            "headers": {
                "test-header": "test-value",
            },
        }
    )

    print(res)


client = Client(
    token="<MY_KEY>"
)

if __name__ == "__main__":
    main()
