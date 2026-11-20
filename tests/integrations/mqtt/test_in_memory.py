from unittest.mock import MagicMock

import pytest
from faststream.mqtt import MQTTBroker

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.mqtt.abstract import MQTTAbstractInMemoryTestCaseConfig


@pytest.mark.mqtt()
class TestInMemoryMQTT(MQTTAbstractInMemoryTestCaseConfig, BaseInMemoryTestCaseConfig[MQTTBroker]):
    async def test_path(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + "/{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(name: str) -> None:
            mock(name)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hi", f"{queue}/john")

        mock.assert_called_once_with("john")
