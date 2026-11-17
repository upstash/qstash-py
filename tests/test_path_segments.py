import pytest

from qstash import AsyncQStash, QStash
from qstash.errors import QStashError
from qstash.http import destination_path, path_segment


@pytest.mark.parametrize(
    "value",
    [
        "msg_abc-123.x~y",
        "user:1@example.com+a=b",
        "order:123",
        "a,b;c&d$e",
        "100%",
        "a b",
    ],
)
def test_path_segment_is_kept(value: str) -> None:
    assert path_segment(value) == value


@pytest.mark.parametrize(
    "value",
    ["a/b", "a\\b", "a?b", "a#b", "a\tb", "a\nb", " a", "a ", "../keys"],
)
def test_path_segment_with_separators_is_rejected(value: str) -> None:
    with pytest.raises(QStashError, match="Queue name cannot contain"):
        path_segment(value, "Queue name")


@pytest.mark.parametrize(
    "value",
    [".", "..", "%2e", "%2E%2e", ".%2e", ".\t.", ".. ", " ..", "..\n"],
)
def test_dot_path_segment_is_rejected(value: str) -> None:
    with pytest.raises(QStashError, match='Queue name cannot be "." or ".."'):
        path_segment(value, "Queue name")


def test_path_segment_is_returned_as_plain_string() -> None:
    class Lookalike(str):
        def __format__(self, format_spec: str) -> str:
            return "../keys"

    assert f"{path_segment(Lookalike('ok'))}" == "ok"
    assert f"{destination_path(Lookalike('ok'))}" == "ok"


def test_empty_or_non_string_path_segment_is_rejected() -> None:
    with pytest.raises(QStashError, match="Queue name cannot be empty"):
        path_segment("", "Queue name")

    with pytest.raises(QStashError, match="Queue name must be a string"):
        path_segment(["a"], "Queue name")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "destination",
    [
        "https://example.com",
        "https://example.com/a/b.c?x=../y#z",
        "my-url-group",
        "https://example.com/.well-known/x",
    ],
)
def test_destination_is_kept(destination: str) -> None:
    assert destination_path(destination) == destination


@pytest.mark.parametrize(
    "destination",
    [
        "https://example.com/a/../b",
        "https://example.com/./a",
        "https://example.com/%2e%2e/a",
        "https://example.com\\..\\a",
        "https://example.com/a/.. ",
        "https://example.com/a/.\t./b",
        "..",
    ],
)
def test_destination_with_dot_segments_is_rejected(destination: str) -> None:
    with pytest.raises(QStashError, match="path segments"):
        destination_path(destination)


def test_empty_destination_is_rejected() -> None:
    for destination in ["", "?a=b", "#a"]:
        with pytest.raises(QStashError, match="Destination cannot be empty"):
            destination_path(destination)


def test_client_checks_ids_before_sending() -> None:
    client = QStash("token", base_url="http://localhost:1", retry=False)

    with pytest.raises(QStashError, match="Message id"):
        client.message.get("../keys")
    with pytest.raises(QStashError, match="Message id"):
        client.message.cancel("?")
    with pytest.raises(QStashError, match="DLQ id"):
        client.dlq.delete("a/b")
    with pytest.raises(QStashError, match="Schedule id"):
        client.schedule.delete("..")
    with pytest.raises(QStashError, match="Queue name"):
        client.queue.pause("a/b")
    with pytest.raises(QStashError, match="URL group name"):
        client.url_group.delete("a#b")
    with pytest.raises(QStashError, match="Flow control key"):
        client.flow_control.get("tenant#42")
    with pytest.raises(QStashError, match="Destination"):
        client.message.publish_json(url="https://example.com/a/../b")
    with pytest.raises(QStashError, match="Queue name"):
        client.message.enqueue_json(queue="a/b", url="https://example.com")
    with pytest.raises(QStashError, match="Destination"):
        client.schedule.create(destination="https://example.com/../a", cron="* * * * *")


@pytest.mark.asyncio
async def test_async_client_checks_ids_before_sending() -> None:
    client = AsyncQStash("token", base_url="http://localhost:1", retry=False)

    with pytest.raises(QStashError, match="Message id"):
        await client.message.get("../keys")
    with pytest.raises(QStashError, match="DLQ id"):
        await client.dlq.delete("a/b")
    with pytest.raises(QStashError, match="Schedule id"):
        await client.schedule.delete("..")
    with pytest.raises(QStashError, match="Queue name"):
        await client.queue.pause("a/b")
    with pytest.raises(QStashError, match="URL group name"):
        await client.url_group.delete("a#b")
    with pytest.raises(QStashError, match="Flow control key"):
        await client.flow_control.get("tenant#42")
    with pytest.raises(QStashError, match="Destination"):
        await client.message.publish_json(url="https://example.com/a/../b")
    with pytest.raises(QStashError, match="Message id"):
        await client.message.cancel("?")
    with pytest.raises(QStashError, match="Queue name"):
        await client.message.enqueue_json(queue="a/b", url="https://example.com")
    with pytest.raises(QStashError, match="Destination"):
        await client.schedule.create(
            destination="https://example.com/../a", cron="* * * * *"
        )
