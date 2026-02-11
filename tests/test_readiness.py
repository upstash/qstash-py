from qstash import QStash


def test_readiness(client: QStash) -> None:
    response = client.readiness()

    assert response is not None
    assert isinstance(response.ready, bool)
    assert response.ready is True
