from client import Client

"""
TODO: 
1. Misc methods (schedules, events) 
2. Convert methods to async
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

    # topics = client.topics()
    # print("Topics", topics.list())
    # topics.add_endpoints({
    #   "name": "another3",
    #   "endpoints": [{
    #     "name": "another3_endpoint1",
    #     "url": "https://seanqstash.requestcatcher.com",
    #   }]
    # })

    # dlq = client.dlq()
    # print(dlq.list_messages())


client = Client("MY_KEY")

if __name__ == "__main__":
    main()
