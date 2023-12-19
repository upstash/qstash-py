# This may have to be qstash_python.client ? That's how it is in the other repos
# from client import Client

# client = Client(3)

# print(client.get_x())

from qstash_types import PublishRequest
publish_request = PublishRequest(body="Hello", delay=10, url="https://example.com")

print(publish_request.url)
print(publish_request.attr_exists("url"), "should be True")
print(publish_request.attr_exists("body"), "should be True")
print(publish_request.attr_exists("doesnt exist"), "should be False")
