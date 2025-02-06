from typing import Callable

import pytest

from qstash import QStash


@pytest.fixture
def cleanup_schedule(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    schedule_ids = []

    def register(client: QStash, schedule_id: str) -> None:
        schedule_ids.append((client, schedule_id))

    def delete():
        for client, schedule_id in schedule_ids:
            try:
                client.schedule.delete(schedule_id)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


def test_schedule_lifecycle(
    client: QStash,
    cleanup_schedule: Callable[[QStash, str], None],
) -> None:
    schedule_id = client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://httpstat.us/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule(client, schedule_id)

    assert len(schedule_id) > 0

    res = client.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"

    list_res = client.schedule.list()
    assert any(s.schedule_id == schedule_id for s in list_res)

    client.schedule.delete(schedule_id)

    list_res = client.schedule.list()
    assert not any(s.schedule_id == schedule_id for s in list_res)


def test_schedule_pause_resume(
    client: QStash,
    cleanup_schedule: Callable[[QStash, str], None],
) -> None:
    schedule_id = client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://httpstat.us/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule(client, schedule_id)

    assert len(schedule_id) > 0

    res = client.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"
    assert res.paused is False

    client.schedule.pause(schedule_id)

    res = client.schedule.get(schedule_id)
    assert res.paused is True

    client.schedule.resume(schedule_id)

    res = client.schedule.get(schedule_id)
    assert res.paused is False

def test_schedule_with_flow_control(client: QStash, cleanup_schedule: Callable[[QStash, str], None]) -> None:

    schedule_id = client.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://httpstat.us/200",
        body={"ex_key": "ex_value"},
        flow_control={
            "key": "flow-key",
            "parallelism": 2
        }
    )

    schedule = client.schedule.get(schedule_id)

    assert schedule.flow_control_key == "flow-key"
    assert schedule.parallelism == 2
    assert schedule.rate_per_second is None

    cleanup_schedule(client, schedule_id)
