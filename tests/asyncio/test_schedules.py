from typing import Callable

import pytest

from upstash_qstash import AsyncQStash


@pytest.mark.asyncio
async def test_schedule_lifecycle_async(
    async_qstash: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_qstash.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://example.com",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule_async(async_qstash, schedule_id)

    assert len(schedule_id) > 0

    res = await async_qstash.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"

    list_res = await async_qstash.schedule.list()
    assert any(s.schedule_id == schedule_id for s in list_res)

    await async_qstash.schedule.delete(schedule_id)

    list_res = await async_qstash.schedule.list()
    assert not any(s.schedule_id == schedule_id for s in list_res)


@pytest.mark.asyncio
async def test_schedule_pause_resume_async(
    async_qstash: AsyncQStash,
    cleanup_schedule_async: Callable[[AsyncQStash, str], None],
) -> None:
    schedule_id = await async_qstash.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://example.com",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule_async(async_qstash, schedule_id)

    assert len(schedule_id) > 0

    res = await async_qstash.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"
    assert res.paused is False

    await async_qstash.schedule.pause(schedule_id)

    res = await async_qstash.schedule.get(schedule_id)
    assert res.paused is True

    await async_qstash.schedule.resume(schedule_id)

    res = await async_qstash.schedule.get(schedule_id)
    assert res.paused is False
