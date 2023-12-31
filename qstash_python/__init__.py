from client import Client

"""
TODO: 
1. Misc method (topics, dlq, etc) 
2. Receiver
3. Tests
"""


def main():
    res = client.publish_json(
        {
            "body": {"ex_key": "ex_value"},
            "url": "https://seanqstash.requestcatcher.com",
            # "topic": "another1",
            # "delay": 5,
            "headers": {
                "test-header": "test-value",
            },
        }
    )

    print(res)


client = Client(token="<MY_KEY")

if __name__ == "__main__":
    main()
