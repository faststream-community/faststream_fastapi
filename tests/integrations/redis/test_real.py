from asyncio import Event, create_task, sleep, wait
from unittest.mock import MagicMock

import pytest
from faststream.redis import ListSub, RedisBroker, StreamSub

from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.redis.abstract import RedisAbstractTestCaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
class TestRealRedis(RedisAbstractTestCaseConfig, BaseRealTestCaseConfig[RedisBroker]):
    async def test_path(self, mock: MagicMock, event: Event) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(channel="in.{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(msg: str, name: str) -> None:
            mock(msg=msg, name=name)
            event.set()

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("hello", "in.john")),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(msg="hello", name="john")

    async def test_batch_real(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            list=ListSub(
                queue,
                batch=True,
                max_records=1,
            ),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: list[str]) -> None:
            event.set()
            mock(msg)

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("hi", list=queue)),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    @pytest.mark.slow()
    async def test_consume_stream(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            stream=StreamSub(
                queue,
                polling_interval=1000,
            ),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with self.get_test_client(broker):
            await sleep(0.5)

            await wait(
                (
                    create_task(broker.publish("hello", stream=queue)),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with("hello")

    @pytest.mark.slow()
    async def test_consume_stream_batch(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            stream=StreamSub(
                queue,
                polling_interval=1000,
                batch=True,
            ),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: list[str]) -> None:
            mock(msg)
            event.set()

        async with self.get_test_client(broker):
            await sleep(0.5)

            await wait(
                (
                    create_task(broker.publish("hello", stream=queue)),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        mock.assert_called_once_with(["hello"])
