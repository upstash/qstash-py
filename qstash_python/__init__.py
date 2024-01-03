from client import Client

"""
TODO: 
1. Convert methods to async
2. Receiver
3. Tests
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
    # messages = client.messages()
    # print(messages.get(message_id))

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

    # print(len(client.events().get("events", [])))

    # schedules = client.schedules()
    # schedules.create({
    #     "destination": "https://seanqstash.requestcatcher.com",
    #     "cron": "* * * * *",
    #     "body": {
    #         "ex_key": "ex_value"
    #     }
    # })

    # scheds = schedules.list()
    # schedules.delete(scheds[0]["scheduleId"])


client = Client("MY_KEY")

if __name__ == "__main__":
    main()
