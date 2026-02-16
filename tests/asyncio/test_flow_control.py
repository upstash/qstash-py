import asyncio

import pytest

from qstash import AsyncQStash
from qstash.message import FlowControl


FLOW_CONTROL_KEY = "test-flow-control-key-async"


@pytest.mark.asyncio
async def test_flow_control_lifecycle_async(async_client: AsyncQStash) -> None:
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
    assert result["messageId"]

    # Small delay to let flow control state propagate
    await asyncio.sleep(1)

    # List all flow controls
    flow_controls = await async_client.flow_control.list()
    assert isinstance(flow_controls, list)

    found = [fc for fc in flow_controls if fc.key == FLOW_CONTROL_KEY]
    assert len(found) > 0

    fc = found[0]
    assert fc.key == FLOW_CONTROL_KEY
    assert isinstance(fc.wait_list_size, int)
    assert isinstance(fc.parallelism_max, int)
    assert isinstance(fc.parallelism_count, int)
    assert isinstance(fc.rate_max, int)
    assert isinstance(fc.rate_count, int)
    assert isinstance(fc.rate_period, int)
    assert isinstance(fc.rate_period_start, int)

    # Get a single flow control by key
    single = await async_client.flow_control.get(FLOW_CONTROL_KEY)
    assert single.key == FLOW_CONTROL_KEY
    assert isinstance(single.wait_list_size, int)

    # Clean up message
    await async_client.message.cancel(result["messageId"])


@pytest.mark.asyncio
async def test_flow_control_list_with_search_async(
    async_client: AsyncQStash,
) -> None:
    flow_controls = await async_client.flow_control.list(search=FLOW_CONTROL_KEY)
    assert isinstance(flow_controls, list)

    for fc in flow_controls:
        assert FLOW_CONTROL_KEY in fc.key


@pytest.mark.asyncio
async def test_flow_control_reset_async(async_client: AsyncQStash) -> None:
    # Reset should not raise
    await async_client.flow_control.reset(FLOW_CONTROL_KEY)

    # After reset, get should still work (counters are zeroed)
    fc = await async_client.flow_control.get(FLOW_CONTROL_KEY)
    assert fc.key == FLOW_CONTROL_KEY
    assert fc.parallelism_count == 0
    assert fc.rate_count == 0
