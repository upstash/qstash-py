from client import Client

"""
TODO: 
1. HTTP return type
2. Misc methods (topics, dlq, schedules, events) 
3. Receiver
4. Tests
"""


def main():
    res = client.publish_json(
        {
            "body": {"ex_key": "ex_value"},
            "url": "https://seanqstash.requestcatcher.com",
            # "topic": "another",
            # "delay": 5,
            "headers": {
                "test-header": "test-value",
            },
        }
    )

    message_id = res["messageId"]
    print(message_id)
    messages = client.messages()
    print(messages.get(message_id))


client = Client(token="<MY_KEY")

if __name__ == "__main__":
    main()
