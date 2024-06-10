from tests import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY
from upstash_qstash import QStash


def test_get(qstash: QStash) -> None:
    key = qstash.signing_key.get()
    assert key.current == QSTASH_CURRENT_SIGNING_KEY
    assert key.next == QSTASH_NEXT_SIGNING_KEY
