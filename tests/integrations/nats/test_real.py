import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.nats import JStream, NatsBroker, PullSub

from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.nats.abstract import NatsAbstractTestCaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
class TestRealNats(NatsAbstractTestCaseConfig, BaseRealTestCaseConfig[NatsBroker]):
    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + ".{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(message: str, name: str) -> None:
            mock(message=message, name=name)
            event.set()

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", f"{queue}.john"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(message="hello", name="john")

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

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(["hello"])
