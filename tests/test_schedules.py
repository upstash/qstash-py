from typing import Callable

import pytest

from upstash_qstash import QStash


@pytest.fixture
def cleanup_schedule(request: pytest.FixtureRequest) -> Callable[[QStash, str], None]:
    schedule_ids = []

    def register(qstash: QStash, schedule_id: str) -> None:
        schedule_ids.append((qstash, schedule_id))

    def delete():
        for qstash, schedule_id in schedule_ids:
            try:
                qstash.schedule.delete(schedule_id)
            except Exception:
                pass

    request.addfinalizer(delete)

    return register


def test_schedule_lifecycle(
    qstash: QStash,
    cleanup_schedule: Callable[[QStash, str], None],
) -> None:
    schedule_id = qstash.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://httpstat.us/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule(qstash, schedule_id)

    assert len(schedule_id) > 0

    res = qstash.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"

    list_res = qstash.schedule.list()
    assert any(s.schedule_id == schedule_id for s in list_res)

    qstash.schedule.delete(schedule_id)

    list_res = qstash.schedule.list()
    assert not any(s.schedule_id == schedule_id for s in list_res)


def test_schedule_pause_resume(
    qstash: QStash,
    cleanup_schedule: Callable[[QStash, str], None],
) -> None:
    schedule_id = qstash.schedule.create_json(
        cron="1 1 1 1 1",
        destination="https://httpstat.us/200",
        body={"ex_key": "ex_value"},
    )

    cleanup_schedule(qstash, schedule_id)

    assert len(schedule_id) > 0

    res = qstash.schedule.get(schedule_id)
    assert res.schedule_id == schedule_id
    assert res.cron == "1 1 1 1 1"
    assert res.paused is False

    qstash.schedule.pause(schedule_id)

    res = qstash.schedule.get(schedule_id)
    assert res.paused is True

    qstash.schedule.resume(schedule_id)

    res = qstash.schedule.get(schedule_id)
    assert res.paused is False
