import time

from qstash import QStash
from qstash.message import FlowControl, PublishResponse


FLOW_CONTROL_KEY = "test-flow-control-key"


def test_flow_control_lifecycle(client: QStash) -> None:
    # Publish a message with flow control to ensure the key exists
    result = client.message.publish_json(
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
    time.sleep(1)

    # List all flow controls
    flow_controls = client.flow_control.list()
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
    single = client.flow_control.get(FLOW_CONTROL_KEY)
    assert single.key == FLOW_CONTROL_KEY
    assert isinstance(single.wait_list_size, int)
    assert isinstance(single.parallelism_max, int)

    # Clean up message
    client.message.cancel(result.message_id)


def test_flow_control_list_with_search(client: QStash) -> None:
    flow_controls = client.flow_control.list(search=FLOW_CONTROL_KEY)
    assert isinstance(flow_controls, list)

    for fc in flow_controls:
        assert FLOW_CONTROL_KEY in fc.key


def test_flow_control_reset(client: QStash) -> None:
    # Reset should not raise
    client.flow_control.reset(FLOW_CONTROL_KEY)

    # After reset, get should still work (counters are zeroed)
    fc = client.flow_control.get(FLOW_CONTROL_KEY)
    assert fc.key == FLOW_CONTROL_KEY
    assert fc.parallelism_count == 0
    assert fc.rate_count == 0
