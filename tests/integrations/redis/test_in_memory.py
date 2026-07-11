from asyncio import Event, create_task, wait
from unittest.mock import MagicMock

import pytest
from faststream.redis import ListSub, RedisBroker, StreamSub

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.redis.abstract import RedisAbstractInMemoryTestCaseConfig


@pytest.mark.redis()
class TestInMemoryRedis(
    RedisAbstractInMemoryTestCaseConfig,
    BaseInMemoryTestCaseConfig[RedisBroker],
):
    async def test_batch_testclient(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            list=ListSub(queue, batch=True, max_records=1),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: list[str]) -> None:
            event.set()
            mock(msg)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await wait(
                (
                    create_task(broker.publish("hi", list=queue)),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    async def test_stream_batch_testclient(
        self,
        mock: MagicMock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            stream=StreamSub(queue, batch=True),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(msg: list[str]) -> None:
            event.set()
            mock(msg)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await wait(
                (
                    create_task(broker.publish("hi", stream=queue)),
                    create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_with(["hi"])

    async def test_path(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + ".{name}")

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
