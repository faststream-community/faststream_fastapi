import asyncio
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.mqtt import MQTTBroker

from faststream_fastapi import FastStreamAPI
from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.mqtt.abstract import MQTTAbstractTestCaseConfig


@pytest.mark.connected()
@pytest.mark.mqtt()
class TestRealMQTT(MQTTAbstractTestCaseConfig, BaseRealTestCaseConfig[MQTTBroker]):
    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + "/{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(msg: str, name: str) -> None:
            mock(msg=msg, name=name)
            event.set()

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", f"{queue}/john"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")

    async def test_conneciton_safe_false(self, mock: MagicMock) -> None:
        broker = self.get_broker("mqtt://localhost:6767")

        app = FastStreamAPI(broker, application=mock)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait(
                [asyncio.create_task(app._connect_broker(broker))],
                timeout=self.timeout,
            )
