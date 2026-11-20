import logging
from asyncio import Event, create_task, wait
from typing import Annotated, Any, Generic, TypeVar
from unittest.mock import MagicMock, Mock

import pytest
from fastapi import BackgroundTasks
from faststream import Depends as FSDepends
from faststream.exceptions import SetupError

from faststream_fastapi import Context, FastStreamAPI, StreamMessage
from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi._internal.fs_re_exports.context import ContextRepo
from tests.base.abstract import AbstractTestCaseConfig

_BrokerT = TypeVar("_BrokerT", bound=BrokerUsecase[Any, Any])


@pytest.mark.asyncio
class BaseRealTestCaseConfig(AbstractTestCaseConfig[_BrokerT], Generic[_BrokerT]):
    async def test_base_real(
        self,
        mock: Mock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(message: str) -> None:
            event.set()
            mock(message)

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("hi", queue)),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with("hi")

    async def test_background(
        self,
        mock: Mock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        def background_task(message: str) -> None:
            event.set()
            mock(message)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(message: str, tasks: BackgroundTasks) -> None:
            tasks.add_task(background_task, message)

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("hi", queue)),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_with("hi")

    async def test_context_default(self, mock: Mock, queue: str, event: Event) -> None:
        broker = self.get_broker()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(message: Any = Context(context_key)) -> None:
            try:
                mock(message == broker.context.resolve(context_key) and message["1"] == "1")
            finally:
                event.set()

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(
                        broker.publish("", queue, headers={"1": "1"}),  # type: ignore[call-arg]
                    ),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    async def test_context_annotated(
        self,
        mock: Mock,
        queue: str,
        event: Event,
        context: ContextRepo,
    ) -> None:
        broker = self.get_broker()

        context_key = "message.headers"

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(message: Annotated[Any, Context(context_key)]) -> None:
            try:
                mock(message == broker.context.resolve(context_key) and message["1"] == "1")
            finally:
                event.set()

        async with self.get_test_client(broker, context=context):
            await wait(
                (
                    create_task(
                        broker.publish("", queue, headers={"1": "1"}),  # type: ignore[call-arg]
                    ),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(True)

    # @pytest.mark.flaky(reruns=3, reruns_delay=1)
    async def test_initial_context(
        self,
        queue: str,
        event: Event,
        context: ContextRepo,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(
            message: int,
            data: set[int] = Context(queue, initial=set),
        ) -> None:
            data.add(message)
            if len(data) == 2:
                event.set()

        async with self.get_test_client(broker, context=context):
            await wait(
                (
                    create_task(broker.publish(1, queue)),
                    create_task(broker.publish(2, queue)),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert broker.context.get(queue) == {1, 2}
        broker.context.reset_global(queue)

    async def test_double_real(
        self,
        mock: Mock,
        queue: str,
        event: Event,
        event2: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_subscriber_params(queue + "2")

        @broker.subscriber(*args, **kwargs)
        @broker.subscriber(*args2, **kwargs2)
        async def subscriber_handler(message: str) -> None:
            if event.is_set():
                event2.set()
            else:
                event.set()

            mock()

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("hi", queue)),
                    create_task(broker.publish("hi", queue + "2")),
                    create_task(event.wait()),
                    create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2

    async def test_base_publisher_real(
        self,
        mock: Mock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)
        args2, kwargs2 = self.get_publisher_params(queue + "resp")

        @broker.subscriber(*args, **kwargs)
        @broker.publisher(*args2, **kwargs2)
        async def request_handler() -> str:
            return "hi"

        args3, kwargs3 = self.get_subscriber_params(queue + "resp")

        @broker.subscriber(*args3, **kwargs3)
        async def response_handler(message: str) -> None:
            event.set()
            mock(message)

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish("", queue)),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with("hi")

    async def test_injection_fastapi(
        self,
        mock: Mock,
        queue: str,
        event: Event,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def subscriber_handler(message: StreamMessage) -> None:
            mock("app" in message.scope)
            event.set()

        async with self.get_test_client(broker):
            await wait(
                (
                    create_task(broker.publish(None, queue)),
                    create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        mock.assert_called_once_with(True)

    async def test_conneciton_safe_false(
        self,
        mock: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = self.get_broker()
        error = ConnectionError("Broker connection failed")

        async def connection_hook(broker: _BrokerT) -> None:
            raise error

        app = FastStreamAPI(
            broker,
            application=mock,
            safe_connection=False,
            connection_hook=connection_hook,
        )
        logger_name = "faststream_fastapi.faststream_api"
        with caplog.at_level(logging.WARNING, logger=logger_name):
            await app._connect_broker(broker)

        records = [record for record in caplog.records if record.name == logger_name]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert records[0].exc_info is not None
        assert records[0].exc_info[1] is error

    async def test_conneciton_safe_true(self, mock: MagicMock) -> None:
        broker = self.get_broker()
        error = ConnectionError("Broker connection failed")

        async def connection_hook(broker: _BrokerT) -> None:
            raise error

        app = FastStreamAPI(
            broker,
            application=mock,
            safe_connection=True,
            connection_hook=connection_hook,
        )
        with pytest.raises(ConnectionError) as exc_info:
            await app._connect_broker(broker)
        assert exc_info.value is error

    async def test_partial_startup_closes_broker_connection(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(value: int = FSDepends(lambda: 1)) -> int:
            return value

        try:
            with pytest.raises(SetupError, match=r"faststream\.Depends"):
                async with self.get_test_client(broker):
                    pass
            assert broker._connection is None
        finally:
            await broker.stop()

    @pytest.mark.parametrize("failure_first", [False, True])
    async def test_shutdown_closes_other_brokers(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        failure_first: bool,
    ) -> None:
        healthy = self.get_broker()
        broken = self.get_broker()
        original_stop = broken.stop

        async def failing_stop() -> None:
            await original_stop()
            raise RuntimeError("shutdown failed")

        monkeypatch.setattr(broken, "stop", failing_stop)
        brokers = (broken, healthy) if failure_first else (healthy, broken)
        try:
            with pytest.raises(RuntimeError, match="shutdown failed"):
                async with self.get_test_client(*brokers):
                    pass
            assert healthy._connection is None
        finally:
            await healthy.stop()
            await original_stop()
