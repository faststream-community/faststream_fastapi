from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Generic, TypeVar
from unittest.mock import Mock

import pytest
from fastapi import Depends, FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
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
from pydantic import ValidationError

from faststream_fastapi import Context
from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi.asyncapi_config import AsyncAPIConfig
from tests.base.abstract import AbstractTestCaseConfig

_BrokerT = TypeVar("_BrokerT", bound=BrokerUsecase[Any, Any])


@pytest.mark.asyncio()
class BaseInMemoryTestCaseConfig(AbstractTestCaseConfig[_BrokerT], Generic[_BrokerT]):
    async def test_base(self, queue: str) -> None:
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
            assert await r.decode() == "hi", r

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

    async def test_invalid(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(msg: int) -> None: ...

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            with pytest.raises((RequestValidationError, ValidationError)):
                await broker.publish("hi", queue)

    async def test_headers(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(w: str = Header()) -> str:
            return w

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(  # type: ignore[call-arg]
                "",
                queue,
                headers={"w": "hi"},
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

    async def test_depends(self, mock: Mock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> str:
            mock(body)
            return body

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str, dependency: str = Depends(dependency)) -> str:
            return dependency

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                {"body": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once_with("hi")

    async def test_mixed_depends(self, mock: Mock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> str:
            mock(body)
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
        ) -> str:
            return dependency

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                {"body": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once_with("hi")

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

    async def test_depends_combined_annotated(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber(
            dependency: Annotated[Any, FSDepends(lambda: 1), Depends(lambda: 1)],
        ) -> None:
            pass

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            pass

    async def test_faststream_context(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(message: Any = FSContext()) -> None:
            pass

        with pytest.raises(SetupError):
            async with (
                self.patch_broker(broker) as broker,
                self.get_test_client(broker),
            ):
                pass

    async def test_faststream_context_annotated(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(msg: Annotated[Any, FSContext()]) -> None:
            pass

        with pytest.raises(SetupError):
            async with (
                self.patch_broker(broker),
                self.get_test_client(broker),
            ):
                pass

    async def test_combined_context_annotated(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(
            message: Annotated[
                Any,
                Context("message.headers"),
                FSContext("message.headers"),
            ],
        ) -> None:
            pass

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker),
        ):
            pass

    async def test_nested_combined_context_annotated(self, queue: str) -> None:
        broker = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(
            message: Annotated[
                Annotated[Any, FSContext("message.headers")],
                Context("message.headers"),
            ],
        ) -> None:
            pass

        async with (
            self.patch_broker(broker),
            self.get_test_client(broker),
        ):
            pass

    async def test_yield_depends(self, mock: Mock, queue: str) -> None:
        broker = self.get_broker()

        def dependency(body: str) -> Iterator[str]:
            mock.start()
            yield body
            mock.close()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str, dependency: str = Depends(dependency)) -> str:
            mock.start.assert_called_once()
            assert mock.close.call_count == 0

            return dependency

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                {"body": "hi"},
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_broker_dependencies(self, mock: Mock, queue: str) -> None:
        def dependency() -> None:
            mock()

        broker = self.get_broker(dependencies=(Depends(dependency, use_cache=False),))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str) -> str:
            return body

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request("hi", queue, timeout=0.5)
            assert await r.decode() == "hi", r

        mock.assert_called_once()

    async def test_subscriber_dependencies(self, mock: Mock, queue: str) -> None:
        broker = self.get_broker()

        def dependency() -> None:
            mock()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(dependency, use_cache=False),),
        )

        @broker.subscriber(*args, **kwargs)
        async def hello(body: str) -> str:
            return body

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker),
        ):
            r = await broker.request(
                "hi",
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once()

    async def test_existed_lifespan_startup(self, mock: Mock) -> None:
        @asynccontextmanager
        async def lifespan(application: FastAPI) -> AsyncIterator[dict[str, bool]]:
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

    async def test_subscriber_mock(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def m() -> str:
            return "hi"

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

    async def test_dependency_overrides(self, mock: Mock, queue: str) -> None:
        broker = self.get_broker()

        def dependency() -> None:
            raise AssertionError

        def overriding_dependency() -> None:
            mock()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(dep: Annotated[None, Depends(dependency)]) -> str:
            return "hi"

        application = FastAPI()
        application.dependency_overrides[dependency] = overriding_dependency

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(broker, application=application),
        ):
            r = await broker.request(
                "hi",
                queue,
                timeout=0.5,
            )
            assert await r.decode() == "hi", r

        mock.assert_called_once()

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

    async def test_asyncapi_json(self) -> None:
        broker = self.get_broker()

        config = AsyncAPIConfig(path="/asyncapi")

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=config,
            ) as client,
        ):
            r_json = await client.get("/asyncapi.json")

            assert r_json.status_code == 200
            assert r_json.headers.get("Content-Type") == "application/json"
            assert "asyncapi" in r_json.json()

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

    async def test_asyncapi_yaml(self) -> None:
        broker = self.get_broker()

        config = AsyncAPIConfig(path="/asyncapi")

        async with (
            self.patch_broker(broker) as broker,
            self.get_test_client(
                broker,
                asyncapi_path=config,
            ) as client,
        ):
            r_yaml = await client.get("/asyncapi.yaml")
            assert r_yaml.status_code == 200
            assert "asyncapi:" in r_yaml.content.decode()

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

    async def test_asyncapi_try(self, queue: str, mock: Mock) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def hello(message: str) -> str:
            mock("hi")
            return "Hello"

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
