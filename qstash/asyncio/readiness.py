from qstash.asyncio.http import AsyncHttpClient
from qstash.readiness import ReadinessResponse, parse_readiness_response


class AsyncReadinessApi:
    def __init__(self, http: AsyncHttpClient) -> None:
        self._http = http

    async def check(self) -> ReadinessResponse:
        """
        Checks the readiness of the QStash service.
        """
        response = await self._http.request(
            path="/v2/readiness",
            method="GET",
        )

        return parse_readiness_response(response)
