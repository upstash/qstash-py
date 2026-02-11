from qstash import QStash


def test_check(client: QStash) -> None:
    response = client.readiness.check()
    assert response.ready is True
