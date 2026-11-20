from asyncio import Event, create_task, wait
from unittest.mock import MagicMock

import pytest
from faststream.redis import ListSub, RedisBroker, StreamSub
from typing_extensions import override

from tests.base.in_memory import BaseInMemoryTestCaseConfig
from tests.integrations.redis.abstract import RedisAbstractInMemoryTestCaseConfig


@pytest.mark.redis()
class TestInMemoryRedis(
    RedisAbstractInMemoryTestCaseConfig,
    BaseInMemoryTestCaseConfig[RedisBroker],
):
    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="FastStream Redis BinaryWriter.write_string uses character count for UTF-8 bytes",
    )
    @override
    async def test_unicode_header(self, queue: str, mock: MagicMock) -> None:
        await super().test_unicode_header(queue, mock)

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

    async def test_path(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + ".{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def subscriber_handler(name: str) -> None:
            mock(name)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hi", f"{queue}.john")

        mock.assert_called_once_with("john")
