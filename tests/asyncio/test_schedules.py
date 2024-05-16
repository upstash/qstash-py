import asyncio

import pytest

from qstash_tokens import QSTASH_TOKEN
from upstash_qstash.asyncio import Client


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


@pytest.mark.asyncio
async def test_schedule_lifecycle(client):
    sched = client.schedules()
    create_res = await sched.create(
        {
            "cron": "* * * * *",
            "destination": "https://example.com",
            "body": {"ex_key": "ex_value"},
            "headers": {
                "content-type": "application/json",
            },
        }
    )

    sched_id = create_res["scheduleId"]
    assert sched_id is not None

    print("Waiting 1 seconds for schedule to be delivered")
    await asyncio.sleep(1)

    print("Checking if schedule is delivered")
    get_res = await sched.get(sched_id)
    assert get_res["scheduleId"] == sched_id
    assert get_res["cron"] == "* * * * *"

    print("Checking if schedule is in list")
    list_res = await sched.list()
    assert any(s["scheduleId"] == sched_id for s in list_res)

    print("Deleting schedule")
    await sched.delete(sched_id)

    print("Waiting 1 second for schedule to be deleted")
    await asyncio.sleep(1)

    print("Checking if schedule is deleted")
    list_res = await sched.list()
    assert not any(s["scheduleId"] == sched_id for s in list_res)
