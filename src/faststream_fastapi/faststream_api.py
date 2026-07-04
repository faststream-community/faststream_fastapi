from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import TracebackType
from typing import Any, cast

from fastapi import FastAPI
from fastapi.background import BackgroundTasks
from faststream._internal.application import StartAbleApplication
from faststream._internal.broker import BrokerUsecase
from faststream._internal.context import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import StreamMessage
from faststream.middlewares import BaseMiddleware
from starlette.types import Lifespan, Receive, Scope, Send

from faststream_fastapi._internal.asyncapi_router import AsyncAPIRouter
from faststream_fastapi._internal.config import Config
from faststream_fastapi._internal.get_dependant import get_fastapi_dependant
from faststream_fastapi._internal.wrap_callable_to_fastapi_compatible import (
    wrap_callable_to_fastapi_compatible,
)


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


class FastStreamApi:
    def __init__(
        self,
        application: FastAPI,
        brokers: Sequence[BrokerUsecase[Any, Any]],
        context: ContextRepo | None = None,
        dependency_overrides_provider: Any | None = None,
    ) -> None:
        self._application = application
        self._brokers = brokers

        self._setup_brokers()

        startable_application = StartAbleApplication(
            *brokers,
            config=FastDependsConfig(
                get_dependent=get_fastapi_dependant,
                context=context or ContextRepo(),
            ),
        )
        self._startable_application = startable_application

        asyncapi_router = AsyncAPIRouter(
            brokers=brokers,
            schema=self._startable_application.schema,
        )
        self._application.include_router(asyncapi_router)
        self._application.router.lifespan_context = self._wrap_lifespan(
            self._application.router.lifespan_context,
        )

        self.config = Config(
            application=application,
            dependency_overrides_provider=dependency_overrides_provider,
        )

    def _wrap_lifespan(
        self,
        lifespan_context: Lifespan[Any],
    ) -> Callable[[Any], AbstractAsyncContextManager[Any, Any]]:
        @asynccontextmanager
        async def wrapped(app: Any) -> AsyncIterator[Any]:
            for broker in self._brokers:
                await broker.start()

            self._startable_application.context.set_global("fastapi_app", app)

            async with lifespan_context(app) as lifespan_context_result:
                yield lifespan_context_result

            for broker in self._brokers:
                await broker.stop()

        return wrapped

    def _setup_brokers(self) -> None:
        for broker in self._brokers:
            broker.config.add_middleware(_BackgroundMiddleware)

            for subscriber in broker.subscribers:
                subscriber._call_decorators = (
                    self._subscriber_compatibility_wrapper,
                    *subscriber._call_decorators,
                )

    def _subscriber_compatibility_wrapper(
        self,
        endpoint: Callable[..., Any],
    ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
        """Patch user function to make it FastAPI-compatible."""
        return wrap_callable_to_fastapi_compatible(
            user_callable=endpoint,
            context=self._startable_application.context,
            config=self.config,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            self.config.set_asgi_state(scope["state"])

        return await self._application(scope, receive, send)
