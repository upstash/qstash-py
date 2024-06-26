import pytest

from tests import assert_eventually_async
from upstash_qstash import AsyncQStash
from upstash_qstash.message import PublishResponse


async def assert_failed_eventually_async(
    async_qstash: AsyncQStash, *msg_ids: str
) -> None:
    async def assertion() -> None:
        messages = (await async_qstash.dlq.list()).messages

        matched_messages = [msg for msg in messages if msg.message_id in msg_ids]
        assert len(matched_messages) == len(msg_ids)

        for msg in matched_messages:
            dlq_msg = await async_qstash.dlq.get(msg.dlq_id)
            assert dlq_msg.response_body == "404 Not Found"
            assert msg.response_body == "404 Not Found"

        if len(msg_ids) == 1:
            await async_qstash.dlq.delete(matched_messages[0].dlq_id)
        else:
            deleted = await async_qstash.dlq.delete_many(
                [m.dlq_id for m in matched_messages]
            )
            assert deleted == len(msg_ids)

        messages = (await async_qstash.dlq.list()).messages
        matched = any(True for msg in messages if msg.message_id in msg_ids)
        assert not matched

    await assert_eventually_async(
        assertion,
        initial_delay=2.0,
        retry_delay=1.0,
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_dlq_get_and_delete_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish_json(
        url="http://httpstat.us/404",
        retries=0,
    )

    assert isinstance(res, PublishResponse)

    await assert_failed_eventually_async(async_qstash, res.message_id)


@pytest.mark.asyncio
async def test_dlq_get_and_delete_many_async(async_qstash: AsyncQStash) -> None:
    msg_ids = []
    for _ in range(5):
        res = await async_qstash.message.publish_json(
            url="http://httpstat.us/404",
            retries=0,
        )

        assert isinstance(res, PublishResponse)
        msg_ids.append(res.message_id)

    await assert_failed_eventually_async(async_qstash, *msg_ids)


@pytest.mark.asyncio
async def test_dlq_filter_async(async_qstash: AsyncQStash) -> None:
    res = await async_qstash.message.publish_json(
        url="http://httpstat.us/404",
        retries=0,
    )

    assert isinstance(res, PublishResponse)

    async def assertion():
        messages = (
            await async_qstash.dlq.list(
                filter={"message_id": res.message_id},
                count=1,
            )
        ).messages

        assert len(messages) == 1
        assert messages[0].message_id == res.message_id

        await async_qstash.dlq.delete(messages[0].dlq_id)

    await assert_eventually_async(
        assertion,
        initial_delay=2.0,
        retry_delay=1.0,
        timeout=10.0,
    )
