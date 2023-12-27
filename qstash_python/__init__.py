from client import Client
import json

def main():
  client.publish({
    "body": json.dumps({
      "message": "Hello World!"
    }),
    "url": "https://seanqstash.requestcatcher.com",
    "delay": 10,
  })
 


client = Client(token="eyJVc2VySUQiOiJjNGVjMmFjYi0yM2Q1LTRkZGYtYmM1ZC1kZjIxZWEzYTMxMjUiLCJQYXNzd29yZCI6IjhmYjQ2MjE2YWY3ZjRlMTg5MGE2ZjdlM2U0MmMwZmVjIn0=")

if __name__ == "__main__":
  main()