from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import TracebackType
from typing import Any, cast

from fastapi import FastAPI
from fastapi.background import BackgroundTasks
from faststream.message import StreamMessage
from faststream.middlewares import BaseMiddleware
from faststream.specification.base import SpecificationFactory
from starlette.types import Lifespan, Receive, Scope, Send

from faststream_fastapi._internal.asyncapi_router import AsyncAPIRouter
from faststream_fastapi._internal.config import Config
from faststream_fastapi._internal.fs_re_exports.application import StartAbleApplication
from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi._internal.fs_re_exports.context import ContextRepo
from faststream_fastapi._internal.fs_re_exports.di import FastDependsConfig
from faststream_fastapi._internal.get_dependant import get_fastapi_dependant
from faststream_fastapi._internal.wrap_callable_to_fastapi_compatible import (
    wrap_callable_to_fastapi_compatible,
)
from faststream_fastapi.asyncapi_config import AsyncAPIConfig


class _BackgroundMiddleware(BaseMiddleware):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool | None:
        if not exc_type and (
            background := cast(
                "BackgroundTasks | None",
                getattr(self.context.get_local("message"), "background", None),
            )
        ):
            await background()

        return await super().after_processed(exc_type, exc_val, exc_tb)


def _subscriber_compatibility_wrapper(
    config: Config,
    context: ContextRepo,
) -> Callable[[Callable[..., Any]], Callable[["StreamMessage[Any]"], Awaitable[Any]]]:
    def subscriber_compatibility_wrapper(
        endpoint: Callable[..., Any],
    ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
        return wrap_callable_to_fastapi_compatible(
            user_callable=endpoint,
            config=config,
            context=context,
        )

    return subscriber_compatibility_wrapper


class FastStreamApi:
    def __init__(
        self,
        application: FastAPI,
        brokers: Sequence[BrokerUsecase[Any, Any]],
        context: ContextRepo | None = None,
        # AsyncAPI
        specification: SpecificationFactory | None = None,
        asyncapi_path: str | AsyncAPIConfig | None = None,
    ) -> None:
        self._application = application
        self._application.router.lifespan_context = self._wrap_lifespan(
            self._application.router.lifespan_context,
        )

        self._brokers = brokers

        self._startable_application = StartAbleApplication(
            *brokers,
            specification=specification,
            config=FastDependsConfig(
                get_dependent=get_fastapi_dependant,
                context=context or ContextRepo(),
            ),
        )
        self._config = Config(
            application=application,
            dependency_overrides_provider=self._application,
        )

        if asyncapi_path is not None:
            if isinstance(asyncapi_path, str):
                asyncapi_config = AsyncAPIConfig(asyncapi_path)
            else:
                asyncapi_config = asyncapi_path

        else:
            asyncapi_config = None

        self._asyncapi_config = asyncapi_config

        for broker in self._brokers:
            broker.config.add_middleware(_BackgroundMiddleware)

            for subscriber in broker.subscribers:
                subscriber._call_decorators = (
                    _subscriber_compatibility_wrapper(
                        config=self._config,
                        context=self._startable_application.context,
                    ),
                    *subscriber._call_decorators,
                )

    def _wrap_lifespan(
        self,
        lifespan_context: Lifespan[Any],
    ) -> Callable[[Any], AbstractAsyncContextManager[Any, Any]]:
        @asynccontextmanager
        async def wrapped(app: Any) -> AsyncIterator[Any]:
            if self._asyncapi_config is not None:
                asyncapi_router = AsyncAPIRouter(
                    brokers=self._brokers,
                    config=self._asyncapi_config,
                    schema=self._startable_application.schema,
                )
                self._application.include_router(asyncapi_router)

            for broker in self._brokers:
                await broker.start()

            self._startable_application.context.set_global("fastapi_app", app)

            async with lifespan_context(app) as lifespan_context_result:
                yield lifespan_context_result

            for broker in self._brokers:
                await broker.stop()

        return wrapped

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            self._config.set_asgi_state(scope["state"])

        return await self._application(scope, receive, send)
