import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.kafka import KafkaBroker

from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.kafka.abstract import KafkaAbstractTestCaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestRealKafka(KafkaAbstractTestCaseConfig, BaseRealTestCaseConfig[KafkaBroker]):
    async def test_batch_real(
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

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hi", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
