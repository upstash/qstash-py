from typing import Callable

import pytest

from qstash import AsyncQStash
from qstash.message import FlowControl


@pytest.mark.asyncio
async def test_schedule_lifecycle_async(
    async_client: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://mock.httpstatus.io/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule_async(async_client, schedule_id)

    assert len(schedule_id) > 0

    res = await async_client.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"

    list_res = await async_client.schedule.list()
    assert any(s.schedule_id == schedule_id for s in list_res)

    await async_client.schedule.delete(schedule_id)

    list_res = await async_client.schedule.list()
    assert not any(s.schedule_id == schedule_id for s in list_res)


@pytest.mark.asyncio
async def test_schedule_pause_resume_async(
    async_client: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://mock.httpstatus.io/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule_async(async_client, schedule_id)

    assert len(schedule_id) > 0

    res = await async_client.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"
    assert res.paused is False

    await async_client.schedule.pause(schedule_id)

    res = await async_client.schedule.get(schedule_id)
    assert res.paused is True

    await async_client.schedule.resume(schedule_id)

    res = await async_client.schedule.get(schedule_id)
    assert res.paused is False


@pytest.mark.asyncio
async def test_schedule_with_flow_control_async(
    async_client: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://mock.httpstatus.io/200",
        body={"ex_key": "ex_value"},
        flow_control=FlowControl(key="flow-key", parallelism=2),
    )
    cleanup_schedule_async(async_client, schedule_id)

    schedule = await async_client.schedule.get(schedule_id)

    flow_control = schedule.flow_control
    assert flow_control is not None
    assert flow_control.key == "flow-key"
    assert flow_control.parallelism == 2
    assert flow_control.rate is None
    assert flow_control.period == 1


@pytest.mark.asyncio
async def test_schedule_enqueue_async(
    async_client: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://mock.httpstatus.io/200",
        body={"key": "value"},
        queue="schedule-queue",
    )
    cleanup_schedule_async(async_client, schedule_id)

    schedule = await async_client.schedule.get(schedule_id)

    assert schedule.queue == "schedule-queue"
