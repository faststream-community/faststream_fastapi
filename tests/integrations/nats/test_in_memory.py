import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.nats import JStream, NatsBroker, PullSub

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.nats.abstract import NatsAbstractInMemoryTestCaseConfig


@pytest.mark.nats()
class TestInMemoryNats(NatsAbstractInMemoryTestCaseConfig, BaseInMemoryTestCaseConfig[NatsBroker]):
    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(messages: list[str]) -> None:
            mock(messages)
            event.set()

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish(b"hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(["hello"])

    async def test_path(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(f"{queue}.{{name}}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(name: str) -> str:
            return name

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                "hi",
                f"{queue}.john",
                timeout=0.5,
            )
            assert await r.decode() == "john"
