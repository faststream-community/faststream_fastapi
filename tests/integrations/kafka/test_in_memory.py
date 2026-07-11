import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.kafka import KafkaBroker

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.kafka.abstract import KafkaAbstractInMemoryTestCaseConfig


@pytest.mark.kafka()
class TestInMemoryKafka(
    KafkaAbstractInMemoryTestCaseConfig,
    BaseInMemoryTestCaseConfig[KafkaBroker],
):
    async def test_group_instance_id(self) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            "test-topic",
            group_id="test-group",
            group_instance_id="instance-4",
        )

        sub = broker.subscriber(*args, **kwargs)

        assert sub._connection_args["group_instance_id"] == "instance-4"

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
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])
