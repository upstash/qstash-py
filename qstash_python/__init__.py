from client import Client

"""
TODO: 
1. HTTP return type
2. Misc methods (dlq, schedules, events) 
3. Convert methods to async
4. Receiver
5. Tests
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

    topics = client.topics()
    print("Topics", topics.list())
    # topics.add_endpoints({
    #   "name": "another3",
    #   "endpoints": [{
    #     "name": "another3_endpoint1",
    #     "url": "https://seanqstash.requestcatcher.com",
    #   }]
    # })


client = Client("MY_KEY")

if __name__ == "__main__":
    main()
