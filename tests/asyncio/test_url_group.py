import pytest

from upstash_qstash import AsyncQStash


@pytest.mark.asyncio
async def test_url_group_async(async_qstash: AsyncQStash) -> None:
    name = "python_url_group"
    await async_qstash.url_group.delete(name)

    await async_qstash.url_group.upsert_endpoints(
        url_group=name,
        endpoints=[
            {"url": "https://httpstat.us/200"},
            {"url": "https://httpstat.us/201"},
        ],
    )

    url_group = await async_qstash.url_group.get(name)
    assert url_group.name == name
    assert any(True for e in url_group.endpoints if e.url == "https://httpstat.us/200")
    assert any(True for e in url_group.endpoints if e.url == "https://httpstat.us/201")

    url_groups = await async_qstash.url_group.list()
    assert any(True for ug in url_groups if ug.name == name)

    await async_qstash.url_group.remove_endpoints(
        url_group=name,
        endpoints=[
            {
                "url": "https://httpstat.us/201",
            }
        ],
    )

    url_group = await async_qstash.url_group.get(name)
    assert url_group.name == name
    assert any(True for e in url_group.endpoints if e.url == "https://httpstat.us/200")
    assert not any(
        True for e in url_group.endpoints if e.url == "https://httpstat.us/201"
    )
