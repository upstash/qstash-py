from upstash_qstash.upstash_http import HttpClient
from typing import TypedDict

GetKeysResponse = TypedDict(
    "GetKeysResponse",
    {
        "current": str,
        "next": str,
    },
)


class Keys:
    def __init__(self, http: HttpClient):
        self.http = http

    def get(self) -> GetKeysResponse:
        """
        Retrieve your current and next signing keys
        """
        return self.http.request(
            {
                "path": ["v2", "keys"],
                "method": "GET",
            }
        )

    def rotate(self) -> GetKeysResponse:
        """
        Rotate your signing keys
        """
        return self.http.request(
            {
                "path": ["v2", "keys", "rotate"],
                "method": "POST",
            }
        )
