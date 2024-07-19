from qstash import QStash
from tests import QSTASH_CURRENT_SIGNING_KEY, QSTASH_NEXT_SIGNING_KEY


def test_get(client: QStash) -> None:
    key = client.signing_key.get()
    assert key.current == QSTASH_CURRENT_SIGNING_KEY
    assert key.next == QSTASH_NEXT_SIGNING_KEY
