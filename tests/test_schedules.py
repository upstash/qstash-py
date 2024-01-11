import pytest
import time
from upstash_qstash import Client
from qstash_tokens import QSTASH_TOKEN


@pytest.fixture
def client():
    return Client(QSTASH_TOKEN)


def test_schedule_lifecycle(client):
    sched = client.schedules()
    create_res = sched.create(
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
    time.sleep(1)

    print("Checking if schedule is delivered")
    get_res = sched.get(sched_id)
    assert get_res["scheduleId"] == sched_id
    assert get_res["cron"] == "* * * * *"

    print("Checking if schedule is in list")
    list_res = sched.list()
    assert any(s["scheduleId"] == sched_id for s in list_res)

    print("Deleting schedule")
    sched.delete(sched_id)

    print("Waiting 1 second for schedule to be deleted")
    time.sleep(1)

    print("Checking if schedule is deleted")
    list_res = sched.list()
    assert not any(s["scheduleId"] == sched_id for s in list_res)
