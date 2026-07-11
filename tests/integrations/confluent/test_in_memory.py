import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.confluent import KafkaBroker

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.confluent.abstract import ConfluentAbstractInMemoryTestCaseConfig


@pytest.mark.confluent()
class TestInMemoryConfluent(
    ConfluentAbstractInMemoryTestCaseConfig,
    BaseInMemoryTestCaseConfig[KafkaBroker],
):
    async def test_batch_testclient(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: list[str]) -> None:
            event.set()
            mock(msg)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
