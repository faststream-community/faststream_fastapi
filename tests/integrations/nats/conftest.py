import pytest
from faststream.nats import JStream


@pytest.fixture()
def stream(queue: str) -> JStream:
    return JStream(queue)
