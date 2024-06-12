import pytest
from upstash_qstash import AsyncQStash


@pytest.mark.asyncio
async def test_schedule_lifecycle_async(async_qstash: AsyncQStash) -> None:
    sched_id = await async_qstash.schedule.create_json(
        cron="* * * * *",
        destination="https://example.com",
        body={"ex_key": "ex_value"},
    )

    assert len(sched_id) > 0

    res = await async_qstash.schedule.get(sched_id)
    assert res.schedule_id == sched_id
    assert res.cron == "* * * * *"

    list_res = await async_qstash.schedule.list()
    assert any(s.schedule_id == sched_id for s in list_res)

    await async_qstash.schedule.delete(sched_id)

    list_res = await async_qstash.schedule.list()
    assert not any(s.schedule_id == sched_id for s in list_res)
