import asyncio

import pytest

from qstash import AsyncQStash
from qstash.flow_control_api import GlobalParallelismInfo
from qstash.message import FlowControl, PublishResponse


FLOW_CONTROL_KEY = "test-flow-control-key-async"


@pytest.mark.asyncio
async def test_flow_control_get_async(async_client: AsyncQStash) -> None:
    # Publish a message with flow control to ensure the key exists
    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://httpstat.us/200?sleep=30000",
        flow_control=FlowControl(
            key=FLOW_CONTROL_KEY,
            parallelism=5,
            rate=10,
            period="1m",
        ),
    )
    assert isinstance(result, PublishResponse)
    assert result.message_id

    # Small delay to let flow control state propagate
    await asyncio.sleep(1)

    # Get a single flow control by key
    single = await async_client.flow_control.get(FLOW_CONTROL_KEY)
    assert single.key == FLOW_CONTROL_KEY
    assert isinstance(single.wait_list_size, int)
    assert isinstance(single.parallelism_max, int)
    assert isinstance(single.parallelism_count, int)

    # Clean up message
    await async_client.message.cancel(result.message_id)


@pytest.mark.asyncio
async def test_flow_control_get_global_parallelism_async(
    async_client: AsyncQStash,
) -> None:
    info = await async_client.flow_control.get_global_parallelism()
    assert isinstance(info, GlobalParallelismInfo)
    assert isinstance(info.parallelism_max, int)
    assert isinstance(info.parallelism_count, int)
