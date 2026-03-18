import asyncio
import time

import pytest

from qstash import AsyncQStash
from qstash.flow_control_api import GlobalParallelismInfo
from qstash.message import FlowControl, PublishResponse
from tests import assert_eventually_async


@pytest.mark.asyncio
async def test_flow_control_get_async(async_client: AsyncQStash) -> None:
    flow_control_key = f"fc-info-{int(time.time() * 1000)}"

    # Publish a message with flow control to ensure the key exists
    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(
            key=flow_control_key,
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
    single = await async_client.flow_control.get(flow_control_key)
    assert single.key == flow_control_key
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


@pytest.mark.asyncio
async def test_flow_control_pause_resume_async(async_client: AsyncQStash) -> None:
    flow_control_key = f"fc-pause-{int(time.time() * 1000)}"

    # Publish a message with flow control to ensure the key exists
    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(
            key=flow_control_key,
            parallelism=5,
            rate=10,
            period="1m",
        ),
    )
    assert isinstance(result, PublishResponse)
    assert result.message_id

    # Pause the flow control key
    await async_client.flow_control.pause(flow_control_key)

    # Verify it's paused
    paused = await async_client.flow_control.get(flow_control_key)
    assert paused.is_paused is True

    # Resume the flow control key
    await async_client.flow_control.resume(flow_control_key)

    # Verify it's resumed
    resumed = await async_client.flow_control.get(flow_control_key)
    assert resumed.is_paused is False

    # Clean up
    await async_client.message.cancel(result.message_id)


@pytest.mark.asyncio
async def test_flow_control_pin_unpin_async(async_client: AsyncQStash) -> None:
    flow_control_key = f"fc-pin-{int(time.time() * 1000)}"

    # Publish a message with flow control to ensure the key exists
    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(
            key=flow_control_key,
            parallelism=5,
            rate=10,
            period="1m",
        ),
    )
    assert isinstance(result, PublishResponse)
    assert result.message_id

    # Pin the configuration
    await async_client.flow_control.pin(
        flow_control_key,
        {"parallelism": 3, "rate": 20, "period": 120},
    )

    # Verify it's pinned
    pinned = await async_client.flow_control.get(flow_control_key)
    assert pinned.is_pinned_parallelism is True
    assert pinned.is_pinned_rate is True
    assert pinned.parallelism_max == 3
    assert pinned.rate_max == 20
    assert pinned.rate_period == 120

    # Unpin the configuration
    await async_client.flow_control.unpin(
        flow_control_key,
        {"parallelism": True, "rate": True},
    )

    # Verify it's unpinned
    unpinned = await async_client.flow_control.get(flow_control_key)
    assert unpinned.is_pinned_parallelism is False
    assert unpinned.is_pinned_rate is False

    # Clean up
    await async_client.message.cancel(result.message_id)


@pytest.mark.asyncio
async def test_flow_control_unpin_single_async(async_client: AsyncQStash) -> None:
    flow_control_key = f"fc-unpin-single-{int(time.time() * 1000)}"

    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(
            key=flow_control_key,
            parallelism=5,
            rate=10,
            period="1m",
        ),
    )
    assert isinstance(result, PublishResponse)

    # Pin both parallelism and rate
    await async_client.flow_control.pin(
        flow_control_key,
        {"parallelism": 3, "rate": 20, "period": 120},
    )

    pinned = await async_client.flow_control.get(flow_control_key)
    assert pinned.is_pinned_parallelism is True
    assert pinned.is_pinned_rate is True

    # Unpin only parallelism
    await async_client.flow_control.unpin(flow_control_key, {"parallelism": True})

    partial = await async_client.flow_control.get(flow_control_key)
    assert partial.is_pinned_parallelism is False
    assert partial.is_pinned_rate is True

    # Clean up
    await async_client.message.cancel(result.message_id)


@pytest.mark.asyncio
async def test_flow_control_reset_rate_async(async_client: AsyncQStash) -> None:
    flow_control_key = f"fc-reset-{int(time.time() * 1000)}"

    # Publish a message with flow control to ensure the key exists
    result = await async_client.message.publish_json(
        body={"test": "value"},
        url="https://mock.httpstatus.io/200?sleep=30000",
        flow_control=FlowControl(
            key=flow_control_key,
            parallelism=5,
            rate=10,
            period="1m",
        ),
    )
    assert isinstance(result, PublishResponse)
    assert result.message_id

    # Reset the rate
    await async_client.flow_control.reset_rate(flow_control_key)

    # We use assert_eventually because reset_rate doesn't take effect immediately.
    # Not ideal but it's what we have.
    async def check_rate_reset() -> None:
        info = await async_client.flow_control.get(flow_control_key)
        assert info.rate_count == 0

    await assert_eventually_async(check_rate_reset)

    # Clean up
    await async_client.message.cancel(result.message_id)
