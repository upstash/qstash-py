from qstash import QStash


def test_readiness(client: QStash) -> None:
    response = client.readiness()
    assert response is not None
    assert isinstance(response.success, bool)
    assert isinstance(response.message, str)
    assert isinstance(response.status_code, int)
    assert response.timestamp is not None
