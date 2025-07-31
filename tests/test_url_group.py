from qstash import QStash


def test_url_group(client: QStash) -> None:
    name = "python_url_group"
    client.url_group.delete(name)

    client.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://mock.httpstatus.io/200"},
            {"url": "https://mock.httpstatus.io/201"},
        ],
    )

    url_group = client.url_group.get(name)
    assert url_group.name == name
    assert any(
        True for e in url_group.endpoints if e.url == "https://mock.httpstatus.io/200"
    )
    assert any(True for e in url_group.endpoints if e.url == "https://mock.httpstatus.io/201")

    url_groups = client.url_group.list()
    assert any(True for ug in url_groups if ug.name == name)

    client.url_group.remove_endpoints(
        url_group=name,
        endpoints=[
            {
                "url": "https://mock.httpstatus.io/201",
            }
        ],
    )

    url_group = client.url_group.get(name)
    assert url_group.name == name
    assert any(
        True for e in url_group.endpoints if e.url == "https://mock.httpstatus.io/200"
    )
    assert not any(
        True for e in url_group.endpoints if e.url == "https://mock.httpstatus.io/201"
    )
