from asyncio import Event
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager
from functools import partial
from gc import collect
from json import JSONDecodeError
from pathlib import Path as FilePath
from typing import Annotated, Any, Generic, TypeVar
from unittest.mock import MagicMock, call

import pytest
import yaml  # type: ignore[import-untyped]
from asgi_lifespan import LifespanManager
from fastapi import BackgroundTasks, Body, Depends, FastAPI, Header, Path, Query, Request
from fastapi import Response as HTTPResponse
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from faststream import (
    Context as FSContext,
)
from faststream import (
    Depends as FSDepends,
)
from faststream import (
    Response,
)
from faststream.exceptions import SetupError
from faststream.message import StreamMessage as NativeMessage
from pydantic import ValidationError
from starlette.background import BackgroundTask
from syrupy.assertion import SnapshotAssertion

from faststream_fastapi import Context, FastStreamAPI
from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi._internal.fs_re_exports.context import ContextRepo
from faststream_fastapi.asyncapi_config import AsyncAPIConfig
from tests.base.abstract import AbstractTestCaseConfig

_BrokerT = TypeVar("_BrokerT", bound=BrokerUsecase[Any, Any])


@pytest.mark.asyncio()
class BaseInMemoryTestCaseConfig(AbstractTestCaseConfig[_BrokerT], Generic[_BrokerT]):
    async def test_base(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                "hi",
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi"

    async def test_standalone_broker_injects_request(self, queue: str) -> None:
        broker = self.get_broker()
        application = FastAPI()
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> bool:
            return request.app is application

        FastStreamAPI(broker, application=application)

        async with self.patch_broker(broker):
            reply = await broker.request("", queue, timeout=self.timeout)
            assert await reply.decode() is True

    async def test_request(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello() -> Response:
            return Response("Hi!", headers={"x-header": "test"})

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                "hi",
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "Hi!"
            assert r.headers["x-header"] == "test"

    async def test_invalid(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(msg: int) -> None:
            mock(msg)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            with pytest.raises((RequestValidationError, ValidationError)):
                await broker.publish("hi", queue)

        mock.assert_not_called()

    async def test_headers(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(w: str = Header()) -> None:
            mock(w)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish(  # type: ignore[call-arg]
                "",
                queue,
                headers={"w": "hi"},
            )

        mock.assert_called_once_with("hi")

    async def test_depends(self, mock: MagicMock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> str:
            mock.dependency(body)
            return body

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str, dependency: str = Depends(dependency)) -> None:
            mock.handler(dependency)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.handler.assert_called_once_with("hi")
        mock.dependency.assert_called_once_with("hi")

    async def test_mixed_depends(self, mock: MagicMock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> str:
            mock.dependency(body)
            return body

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(
            body: str,
            dependency: Annotated[
                str,
                Depends(dependency),
                FSDepends(dependency),  # will be ignored
            ],
        ) -> None:
            mock.handler(dependency)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.handler.assert_called_once_with("hi")
        mock.dependency.assert_called_once_with("hi")

    async def test_depends_from_fastdepends_default(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber_handler(dependency: Any = FSDepends(lambda: 1)) -> None:
            pass

        with pytest.raises(SetupError):
            async with (
                self.patch_broker(broker) as broker,
                self.get_test_client(broker),
            ):
                pass

    async def test_depends_from_fastdepends_annotated(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber_handler(dependency: Annotated[Any, FSDepends(lambda: 1)]) -> None:
            pass

        with pytest.raises(SetupError):
            async with (
                self.patch_broker(broker) as broker,
                self.get_test_client(broker),
            ):
                pass

    async def test_depends_combined_annotated(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber(
            dependency: Annotated[
                str,
                FSDepends(lambda: "faststream_depends"),
                Depends(lambda: "fastapi_depends"),
            ],
        ) -> None:
            mock(dependency)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("", queue)

        mock.assert_called_once_with("fastapi_depends")

    async def test_faststream_context(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(message: Any = FSContext()) -> None:
            mock(await message.decode())

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.assert_called_once_with({"body": "hi"})

    async def test_context_initializer_is_shared_across_brokers(
        self,
        queue: str,
        context: ContextRepo,
    ) -> None:
        first = self.get_broker()
        second = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        async def handler(
            message: Annotated[NativeMessage[Any], Context("message")],
            seen: list[int] = Context("seen", initial=list),
        ) -> list[int]:
            value = await message.decode()
            assert isinstance(value, int)
            seen.append(value)
            return seen

        first.subscriber(*args, **kwargs)(handler)
        second.subscriber(*args, **kwargs)(handler)

        async with (
            self.patch_broker(first, second),
            self.get_test_client(first, second, context=context),
        ):
            reply = await first.request(1, queue, timeout=self.timeout)
            assert await reply.decode() == [1]
            assert context.get("seen") == [1]
            reply = await second.request(2, queue, timeout=self.timeout)
            assert await reply.decode() == [1, 2]
            assert context.get("seen") == [1, 2]

    async def test_faststream_context_annotated(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, FSContext("message")]) -> None:
            mock(await msg.decode())

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.assert_called_once_with({"body": "hi"})

    async def test_combined_context_annotated(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(
            message: Annotated[
                Any,
                Context("message"),
                FSContext("message"),
            ],
        ) -> None:
            mock(await message.decode())

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.assert_called_once_with({"body": "hi"})

    async def test_nested_combined_context_annotated(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(
            message: Annotated[
                Annotated[Any, FSContext("message")],
                Context("message"),
            ],
        ) -> None:
            mock(await message.decode())

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.assert_called_once_with({"body": "hi"})

    async def test_yield_depends(self, mock: MagicMock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> Iterator[str]:
            mock.start()
            yield body
            mock.close()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str, dependency: str = Depends(dependency)) -> None:
            mock.start_call_count(mock.start.call_count)
            mock.close_call_count(mock.close.call_count)
            mock.handler(dependency)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish({"body": "hi"}, queue)

        mock.handler.assert_called_once_with("hi")
        mock.start_call_count.assert_called_once_with(1)
        mock.close_call_count.assert_called_once_with(0)
        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_broker_dependencies(self, mock: MagicMock, queue: str) -> None:
        def dependency() -> None:
            mock.dependency()

        broker = self.get_broker(dependencies=(Depends(dependency, use_cache=False),))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str) -> None:
            mock.handler(body)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hi", queue)

        mock.handler.assert_called_once_with("hi")
        mock.dependency.assert_called_once()

    async def test_subscriber_dependencies(self, mock: MagicMock, queue: str) -> None:
        broker = self.get_broker()

        def dependency() -> None:
            mock.dependency()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(dependency, use_cache=False),),
        )

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str) -> None:
            mock.handler(body)

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hi", queue)

        mock.handler.assert_called_once_with("hi")

        mock.dependency.assert_called_once()

    async def test_existed_lifespan_startup(self, mock: MagicMock) -> None:
        @asynccontextmanager
        async def lifespan(application: FastAPI) -> AsyncGenerator[dict[str, bool], None]:
            mock.start()
            yield {"lifespan": True}
            mock.close()

        application = FastAPI(lifespan=lifespan)

        @application.get("/")
        async def handler(request: Request) -> None:
            assert request.state.lifespan

        broker = self.get_broker()

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker, application=application) as test_client,
        ):
            await test_client.get("/")

        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_lifespan_app(self, mock: MagicMock) -> None:
        @asynccontextmanager
        async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
            mock(application)
            yield None

        application = FastAPI(lifespan=lifespan)
        broker = self.get_broker()

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker, application=application),
        ):
            pass

        mock.assert_called_once_with(application)

    async def test_subscriber_mock(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def m() -> None:
            pass

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hello", queue)
            m.mock.assert_called_once_with("hello")

    async def test_publisher_mock(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_publisher_params(queue + "resp")

        publisher = broker.publisher(*args, **kwargs)

        args2, kwargs2 = self.get_subscriber_params(queue)

        @publisher
        @broker.subscriber(*args2, **kwargs2)
        async def request_handler() -> str:
            return "response"

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            await broker.publish("hello", queue)
            publisher.mock.assert_called_with("response")

    async def test_dependency_overrides(self, mock: MagicMock, queue: str) -> None:
        broker = self.get_broker()

        def dependency() -> None:
            raise AssertionError

        def overriding_dependency() -> None:
            mock.overriding_dependency()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(dep: Annotated[None, Depends(dependency)]) -> None:
            mock.handler(dep)

        application = FastAPI()
        application.dependency_overrides[dependency] = overriding_dependency

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker, application=application),
        ):
            await broker.publish("hi", queue)

        mock.handler.assert_called_once_with(None)

        mock.overriding_dependency.assert_called_once()

    async def test_asyncapi_docs_with_str(self) -> None:
        broker = self.get_broker()

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path="/asyncapi",
            ) as client,
        ):
            r_html = await client.get("/asyncapi")
            assert r_html.status_code == 200
            assert r_html.headers["content-type"].startswith("text/html")

    async def test_asyncapi_docs_with_config(self) -> None:
        broker = self.get_broker()

        asyncapi_config = AsyncAPIConfig("/asyncapi")

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=asyncapi_config,
            ) as client,
        ):
            r_html = await client.get("/asyncapi")
            assert r_html.status_code == 200
            assert r_html.headers["content-type"].startswith("text/html")

    async def test_asyncapi_docs_with_none(self) -> None:
        broker = self.get_broker()

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=None,
            ) as client,
        ):
            r_html = await client.get("/asyncapi")
            assert r_html.status_code == 404

    async def test_asyncapi_json_and_yaml(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params("asyncapi.input")

        @broker.subscriber(*args, **kwargs)
        async def handler(payload: Annotated[dict[str, int], Body()]) -> None:
            pass

        config = AsyncAPIConfig(path="/asyncapi")

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=config,
            ) as client,
        ):
            r_json = await client.get("/asyncapi.json")
            r_yaml = await client.get("/asyncapi.yaml")

        assert r_json.status_code == 200
        assert r_json.headers["content-type"] == "application/json"
        assert r_yaml.status_code == 200
        assert r_yaml.headers["content-type"] == "application/octet-stream"
        schema = r_json.json()
        assert schema == snapshot_json
        assert yaml.safe_load(r_yaml.content) == schema

    async def test_asyncapi_json_missing(self) -> None:
        broker = self.get_broker()

        config = AsyncAPIConfig(
            path="/asyncapi",
            asyncapi_json_path=None,
        )

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=config,
            ) as client,
        ):
            r_json = await client.get("/asyncapi.json")

            assert r_json.status_code == 404

    async def test_asyncapi_yaml_missing(self) -> None:
        broker = self.get_broker()

        config = AsyncAPIConfig(
            path="/asyncapi",
            asyncapi_yaml_path=None,
        )

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=config,
            ) as client,
        ):
            r_yaml = await client.get("/asyncapi.yaml")
            assert r_yaml.status_code == 404

    async def test_asyncapi_try(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(message: str) -> None:
            mock(message)

        asyncapi_config = AsyncAPIConfig("/asyncapi")

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=asyncapi_config,
            ) as client,
        ):
            r_try = await client.post(
                "/asyncapi/try",
                json={
                    "channelName": queue,
                    "message": {
                        "message": "hi",
                    },
                },
            )
            assert r_try.status_code == 200

            mock.assert_called_once_with("hi")

    async def test_asyncapi_try_missing(self) -> None:
        broker = self.get_broker()

        asyncapi_config = AsyncAPIConfig(
            "/asyncapi",
            try_it_out_path=None,
        )

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=asyncapi_config,
            ) as client,
        ):
            r_try = await client.post("/asyncapi/try", json={})
            assert r_try.status_code == 404

    async def test_dependency_only_header(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        def dependency(x_token: str = Header()) -> str:
            return x_token

        @broker.subscriber(*args, **kwargs)
        async def handler(token: str = Depends(dependency)) -> None:
            mock(token)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                "",
                queue,
                headers={"x-token": "ready"},
            )

        mock.assert_called_once_with("ready")

    async def test_dependency_only_body(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        def dependency(payload: dict[str, int] = Body()) -> dict[str, int]:
            return payload

        @broker.subscriber(*args, **kwargs)
        async def handler(payload: dict[str, int] = Depends(dependency)) -> None:
            mock(payload)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish({"value": 1}, queue)

        mock.assert_called_once_with({"value": 1})

    async def test_dependency_only_scalar_query(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        def dependency(value: int) -> int:
            return value

        @broker.subscriber(*args, **kwargs)
        async def handler(value: int = Depends(dependency)) -> None:
            mock(value)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(2, queue)

        mock.assert_called_once_with(2)

    async def test_scalar_body_embed_false(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(value: int = Body(embed=False)) -> None:
            mock(value)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(2, queue)

        mock.assert_called_once_with(2)

    async def test_scalar_body_embed_true(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(value: int = Body(embed=True)) -> None:
            mock(value)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish({"value": 2}, queue)

        mock.assert_called_once_with(2)

    async def test_multiple_body_fields(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(left: int = Body(), right: int = Body()) -> None:
            mock(left, right)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish({"left": 2, "right": 3}, queue)

        mock.assert_called_once_with(2, 3)

    async def test_request_json_and_bytes(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        wire_body = b'{ "value" : 1 }'

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            mock(await request.body(), await request.json())

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                wire_body,
                queue,
                headers={"content-type": "application/json"},
            )

        mock.assert_called_once_with(wire_body, {"value": 1})

    async def test_request_stream_reads_wire_bytes(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            mock(b"".join([chunk async for chunk in request.stream()]))

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(b"stream data", queue)

        mock.assert_called_once_with(b"stream data")

    async def test_request_json_rejects_non_json(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            await request.json()

        async with self.patch_broker(broker), self.get_test_client(broker):
            with pytest.raises(JSONDecodeError):
                await broker.publish(b"not JSON", queue)

    @pytest.mark.parametrize("header_name", ["x-token", "X-Token"])
    async def test_header_case(self, queue: str, mock: MagicMock, header_name: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(x_token: str = Header()) -> None:
            mock(x_token)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                "",
                queue,
                headers={header_name: "ready"},
            )

        mock.assert_called_once_with("ready")

    async def test_list_header(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(x_token: list[str] = Header()) -> None:
            mock(x_token)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                "",
                queue,
                headers={"x-token": "ready"},
            )

        mock.assert_called_once_with(["ready"])

    async def test_request_precedes_scalar_query(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request, value: int = Query(alias="number")) -> None:
            mock(value)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(2, queue)

        mock.assert_called_once_with(2)

    async def test_scalar_path_parameter(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(value: Annotated[int, Path()], option: str = "default") -> None:
            mock(value, option)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(2, queue)

        mock.assert_called_once_with(2, "default")

    async def test_class_subscriber(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        class ClassBasedHandler:
            def __init__(self, callback: MagicMock) -> None:
                self.callback = callback

            async def __call__(self, value: str) -> None:
                self.callback(value)

        broker.subscriber(*args, **kwargs)(ClassBasedHandler(mock))

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("ready", queue)

        mock.assert_called_once_with("ready")

    async def test_app_context_exposes_brokers(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(
            first: Annotated[Any, Context("app.broker")],
            all_brokers: Annotated[Any, Context("app.brokers")],
        ) -> None:
            mock(first is broker, all_brokers == [broker])

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("", queue)

        mock.assert_called_once_with(True, True)

    async def test_export_schema_without_lifespan(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.get_broker()
        app = FastStreamAPI(broker, application=FastAPI())
        assert app.schema is app.schema
        args, kwargs = self.get_subscriber_params("asyncapi.input")

        @broker.subscriber(*args, **kwargs)
        async def handler(payload: int, context: Annotated[str, FSContext("token")]) -> None:
            pass

        assert app.schema.to_specification().to_jsonable() == snapshot_json

    async def test_schema_export_releases_nonpersistent_subscriber(self, queue: str) -> None:
        broker = self.get_broker()
        app = FastStreamAPI(broker, application=FastAPI())
        args, kwargs = self.get_subscriber_params(queue, persistent=False)
        subscriber = broker.subscriber(*args, **kwargs)

        @subscriber
        async def handler() -> None:
            pass

        schema = app.schema.to_specification().to_jsonable()
        assert schema["channels"]

        del handler, subscriber
        collect()

        assert not broker.subscribers

    async def test_unicode_header(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(x_user: str = Header()) -> None:
            mock(x_user)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                "",
                queue,
                headers={"X-User": "Иван"},
            )

        mock.assert_called_once_with("Иван")

    async def test_json_response(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> JSONResponse:
            return JSONResponse({"value": 1}, headers={"x-result": "ready"})

        async with self.patch_broker(broker), self.get_test_client(broker):
            response = await broker.request("", queue, timeout=self.timeout)
            assert await response.decode() == {"value": 1}
            assert response.headers["x-result"] == "ready"

    async def test_injected_response_headers(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(response: HTTPResponse) -> str:
            response.headers["x-result"] = "ready"
            return "ok"

        async with self.patch_broker(broker), self.get_test_client(broker):
            response = await broker.request("", queue, timeout=self.timeout)
            assert await response.decode() == "ok"
            assert response.headers["x-result"] == "ready"

    async def test_streaming_response(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> StreamingResponse:
            chunks: list[str | bytes] = ['{"value":', b"1}"]
            return StreamingResponse(
                iter(chunks),
                media_type="application/json",
                background=BackgroundTask(mock, "done"),
            )

        async with self.patch_broker(broker), self.get_test_client(broker):
            response = await broker.request("", queue, timeout=self.timeout)
            assert await response.decode() == {"value": 1}

        mock.assert_called_once_with("done")

    async def test_file_response(self, queue: str, tmp_path: FilePath) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        file = tmp_path / "payload.json"
        file.write_bytes(b'{"value":1}')

        @broker.subscriber(*args, **kwargs)
        async def handler() -> FileResponse:
            return FileResponse(file)

        async with self.patch_broker(broker), self.get_test_client(broker):
            response = await broker.request("", queue, timeout=self.timeout)
            assert await response.decode() == {"value": 1}
            assert response.headers["content-length"] == str(file.stat().st_size)

    async def test_streaming_reply_fits_at_size_limit(self, queue: str) -> None:
        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"abcd"
            yield b"efgh"

        @broker.subscriber(*args, **kwargs)
        async def handler() -> StreamingResponse:
            return StreamingResponse(chunks())

        app = FastStreamAPI(broker, application=FastAPI(), max_streaming_reply_size=8)
        async with self.patch_broker(broker), LifespanManager(app):
            reply = await broker.request("", queue, timeout=self.timeout)
            assert await reply.decode() == b"abcdefgh"

    async def test_oversized_streaming_reply_stops_iteration(self, queue: str) -> None:
        broker = self.get_broker()
        closed = Event()
        args, kwargs = self.get_subscriber_params(queue)

        async def chunks() -> AsyncIterator[bytes]:
            try:
                yield b"abcdefgh"
                yield b"x"
                pytest.fail("The reply exceeded the buffer limit on the previous chunk")
            finally:
                closed.set()

        @broker.subscriber(*args, **kwargs)
        async def handler() -> StreamingResponse:
            return StreamingResponse(chunks())

        app = FastStreamAPI(broker, application=FastAPI(), max_streaming_reply_size=8)
        async with self.patch_broker(broker), LifespanManager(app):
            with pytest.raises(ValueError, match=r"reply exceeds.*8-byte"):
                await broker.request("", queue, timeout=self.timeout)
            assert closed.is_set()

    async def test_file_reply_size_limit(self, queue: str, tmp_path: FilePath) -> None:
        broker = self.get_broker()
        file = tmp_path / "payload.bin"
        file.write_bytes(b"x" * (1024 * 1024 + 1))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> FileResponse:
            return FileResponse(file)

        async with self.patch_broker(broker), self.get_test_client(broker):
            with pytest.raises(ValueError, match=r"reply exceeds.*1048576-byte"):
                await broker.request("", queue, timeout=self.timeout)

    async def test_repeated_response_headers(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> HTTPResponse:
            response = HTTPResponse()
            response.headers.append("vary", "origin")
            response.headers.append("vary", "accept")
            return response

        async with self.patch_broker(broker), self.get_test_client(broker):
            response = await broker.request("", queue, timeout=self.timeout)
            assert response.headers["vary"] == "origin, accept"

    async def test_multiple_response_cookies(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> HTTPResponse:
            response = HTTPResponse()
            response.set_cookie("first", "1")
            response.set_cookie("second", "2")
            return response

        async with self.patch_broker(broker), self.get_test_client(broker):
            with pytest.raises(ValueError, match="Set-Cookie"):
                await broker.request("", queue, timeout=self.timeout)

    async def test_response_background_task(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler() -> JSONResponse:
            return JSONResponse({"value": 1}, background=BackgroundTask(mock, "done"))

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("", queue)

        mock.assert_called_once_with("done")

    async def test_injected_response_background_task(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(tasks: BackgroundTasks) -> JSONResponse:
            tasks.add_task(mock, "done")
            return JSONResponse({"value": 1})

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("", queue)

        mock.assert_called_once_with("done")

    async def test_class_constructor_subscriber(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        class Handler:
            def __init__(self, value: str) -> None:
                mock(value)

        broker.subscriber(*args, **kwargs)(Handler)

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("ready", queue)

        mock.assert_called_once_with("ready")

    async def test_partial(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        async def handler(value: str, prefix: str) -> None:
            mock(value, prefix)

        broker.subscriber(*args, **kwargs)(partial(handler, prefix="!"))

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish("ready", queue)

        mock.assert_called_once_with("ready", "!")

    async def test_context_cannot_be_overridden_by_headers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(message: Annotated[Any, Context("message")]) -> None:
            mock(await message.decode())

        async with self.patch_broker(broker), self.get_test_client(broker):
            await broker.publish(  # type: ignore[call-arg]
                {"value": 1},
                queue,
                headers={"context__": "invalid"},
            )

        mock.assert_called_once_with({"value": 1})

    async def test_lifespan_state_reaches_subscriber(self, queue: str, mock: MagicMock) -> None:
        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncGenerator[dict[str, str], None]:
            yield {"shared": "ready"}

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            mock(request.state.shared)
            request.state.shared = "changed"

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker, application=FastAPI(lifespan=lifespan)),
        ):
            await broker.publish("", queue)
            await broker.publish("", queue)

        assert mock.call_args_list == [call("ready"), call("ready")]

    async def test_state_available_during_broker_startup(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncGenerator[dict[str, str], None]:
            yield {"shared": "ready"}

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            mock(request.state.shared)

        async def connection_hook(broker: _BrokerT) -> None:
            await broker.start()
            await broker.publish("", queue)

        app = FastStreamAPI(
            broker,
            application=FastAPI(lifespan=lifespan),
            connection_hook=connection_hook,
            safe_connection=True,
        )
        async with self.patch_broker(broker), LifespanManager(app):
            mock.assert_called_once_with("ready")

    async def test_repeated_lifespan_background_tasks(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(tasks: BackgroundTasks) -> None:
            tasks.add_task(mock)

        app = FastStreamAPI(broker, application=FastAPI(), safe_connection=True)
        async with self.patch_broker(broker):
            async with LifespanManager(app):
                await broker.publish("", queue)
                assert mock.call_count == 1

            async with LifespanManager(app):
                await broker.publish("", queue)
                assert mock.call_count == 2

    async def test_repeated_lifespan_state_and_asyncapi_routes(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        cycle = 0

        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncGenerator[dict[str, int], None]:
            nonlocal cycle
            cycle += 1
            yield {"cycle": cycle}

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(request: Request) -> None:
            mock(request.state.cycle)

        fastapi = FastAPI(lifespan=lifespan)
        app = FastStreamAPI(
            broker,
            application=fastapi,
            asyncapi_path="/asyncapi",
            safe_connection=True,
        )
        route_counts = []
        async with self.patch_broker(broker):
            async with LifespanManager(app):
                await broker.publish("", queue)
                route_counts.append(len(fastapi.routes))

            async with LifespanManager(app):
                await broker.publish("", queue)
                route_counts.append(len(fastapi.routes))

        assert mock.call_args_list == [call(1), call(2)]
        assert len(set(route_counts)) == 1
