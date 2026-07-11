import pytest
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.rabbit.abstract import RabbitAbstractInMemoryTestCaseConfig


@pytest.mark.rabbit()
@pytest.mark.asyncio()
class TestInMemoryRabbit(
    RabbitAbstractInMemoryTestCaseConfig,
    BaseInMemoryTestCaseConfig[RabbitBroker],
):
    async def test_path(self) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            RabbitQueue(
                "",
                routing_key="in.{name}",
            ),
            RabbitExchange(
                "test",
                type=ExchangeType.TOPIC,
            ),
        )

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(name: str) -> str:
            return name

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                "hi",
                "in.john",
                "test",
                timeout=0.5,
            )
            assert await r.decode() == "john"
