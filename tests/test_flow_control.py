import time

from qstash import QStash
from qstash.flow_control_api import GlobalParallelismInfo
from qstash.message import FlowControl, PublishResponse


FLOW_CONTROL_KEY = "test-flow-control-key"


def test_flow_control_get(client: QStash) -> None:
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

    # Get a single flow control by key
    single = client.flow_control.get(FLOW_CONTROL_KEY)
    assert single.key == FLOW_CONTROL_KEY
    assert isinstance(single.wait_list_size, int)
    assert isinstance(single.parallelism_max, int)
    assert isinstance(single.parallelism_count, int)

    # Clean up message
    client.message.cancel(result.message_id)


def test_flow_control_get_global_parallelism(client: QStash) -> None:
    info = client.flow_control.get_global_parallelism()
    assert isinstance(info, GlobalParallelismInfo)
    assert isinstance(info.parallelism_max, int)
    assert isinstance(info.parallelism_count, int)
