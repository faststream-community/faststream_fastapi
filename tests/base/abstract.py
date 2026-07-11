from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, ClassVar, Generic, TypeVar, overload

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.specification.base import SpecificationFactory
from httpx2 import ASGITransport, AsyncClient

from faststream_fastapi import FastStreamAPI
from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi._internal.fs_re_exports.context import ContextRepo
from faststream_fastapi.asyncapi_config import AsyncAPIConfig

_BrokerT = TypeVar("_BrokerT", bound=BrokerUsecase[Any, Any])


class AbstractTestCaseConfig(ABC, Generic[_BrokerT]):
    timeout: ClassVar[float] = 3.0

    @abstractmethod
    def get_broker(
        self,
        *,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> _BrokerT:
        raise NotImplementedError

    @overload
    def patch_broker(
        self,
        brokers: _BrokerT,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[_BrokerT]: ...

    @overload
    def patch_broker(
        self,
        *brokers: _BrokerT,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[_BrokerT, ...]]: ...

    def patch_broker(
        self,
        *brokers: _BrokerT,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        return args, kwargs

    def get_publisher_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        return args, kwargs

    @asynccontextmanager
    async def get_test_client(
        self,
        *brokers: _BrokerT,
        application: FastAPI | None = None,
        context: ContextRepo | None = None,
        specification: SpecificationFactory | None = None,
        asyncapi_path: str | AsyncAPIConfig | None = None,
    ) -> AsyncIterator[AsyncClient]:
        if application is None:
            application = FastAPI()

        faststream_api = FastStreamAPI(
            *brokers,
            application=application,
            context=context,
            specification=specification,
            asyncapi_path=asyncapi_path,
        )
        async with (
            LifespanManager(faststream_api) as lifespan_manager,
            AsyncClient(
                base_url="http://test",
                transport=ASGITransport(lifespan_manager.app),
            ) as client,
        ):
            yield client
