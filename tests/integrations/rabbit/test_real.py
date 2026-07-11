import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.rabbit.abstract import RabbitAbstractTestCaseConfig


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestRealRabbit(RabbitAbstractTestCaseConfig, BaseRealTestCaseConfig[RabbitBroker]):
    @pytest.mark.asyncio()
    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            RabbitQueue(
                queue,
                routing_key="in.{name}",
            ),
            RabbitExchange(
                queue + "1",
                type=ExchangeType.TOPIC,
            ),
        )

        @broker.subscriber(*args, **kwargs)
        def subscriber_handler(msg: str, name: str) -> None:
            mock(msg=msg, name=name)
            event.set()

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", "in.john", queue + "1"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")
