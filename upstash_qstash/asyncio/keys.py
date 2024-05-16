from upstash_qstash.keys import GetKeysResponse
from upstash_qstash.upstash_http import HttpClient


class Keys:
    def __init__(self, http: HttpClient):
        self.http = http

    async def get(self) -> GetKeysResponse:
        """
        Asynchronously retrieve your current and next signing keys
        """
        return await self.http.request_async(
            {
                "path": ["v2", "keys"],
                "method": "GET",
            }
        )

    async def rotate(self) -> GetKeysResponse:
        """
        Asynchronously rotate your signing keys
        """
        return await self.http.request_async(
            {
                "path": ["v2", "keys", "rotate"],
                "method": "POST",
            }
        )
