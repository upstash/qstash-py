from client import Client
import json


def main():
    client.publish(
        {
            "body": json.dumps({"message": "Hello World!"}),
            "url": "https://seanqstash.requestcatcher.com",
            "delay": 10,
        }
    )


client = Client(token="<YOUR_TOKEN>")

if __name__ == "__main__":
    main()
