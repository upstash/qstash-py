from qstash import QStash


def test_readiness(client: QStash) -> None:
    response = client.readiness()
    assert response == "OK"
