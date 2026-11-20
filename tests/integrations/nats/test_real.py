import asyncio
import logging
from asyncio import Event
from unittest.mock import MagicMock

import pytest
from faststream.nats import JStream, NatsBroker, PullSub
from nats.errors import NoServersError

from faststream_fastapi import FastStreamAPI
from tests.base.real import BaseRealTestCaseConfig
from tests.integrations.nats.abstract import NatsAbstractTestCaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
class TestRealNats(NatsAbstractTestCaseConfig, BaseRealTestCaseConfig[NatsBroker]):
    async def test_logs_nats_connection_failure(
        self,
        mock: MagicMock,
        caplog: pytest.LogCaptureFixture,
        unused_tcp_port: int,
    ) -> None:
        broker = self.get_broker(
            servers=f"nats://127.0.0.1:{unused_tcp_port}",
            max_reconnect_attempts=1,
            reconnect_time_wait=0,
        )
        app = FastStreamAPI(broker, application=mock, safe_connection=False)
        try:
            with caplog.at_level(logging.WARNING, logger="faststream_fastapi.faststream_api"):
                await asyncio.wait_for(app._connect_broker(broker), timeout=self.timeout)
        finally:
            await broker.stop()

        assert any(
            record.name == "faststream_fastapi.faststream_api"
            and record.levelno >= logging.WARNING
            and record.exc_info
            and isinstance(record.exc_info[1], NoServersError)
            for record in caplog.records
        )

    async def test_path(
        self,
        queue: str,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue + ".{name}")

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(message: str, name: str) -> None:
            mock(message=message, name=name)
            event.set()

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(
                        broker.publish("hello", f"{queue}.john"),
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(message="hello", name="john")

    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
        mock: MagicMock,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        def subscriber_handler(messages: list[str]) -> None:
            mock(messages)
            event.set()

        async with self.get_test_client(broker):
            await asyncio.wait(
                (
                    asyncio.create_task(broker.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(["hello"])
