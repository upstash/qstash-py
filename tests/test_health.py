from qstash import QStash


def test_liveness(client: QStash) -> None:
    result = client.liveness()
    assert result == "OK"


def test_readiness(client: QStash) -> None:
    result = client.readiness()
    assert result == "OK"
