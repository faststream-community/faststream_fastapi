import traceback
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.params import Depends
from faststream.message import StreamMessage
from faststream.specification.base import SpecificationFactory
from starlette.types import Receive, Scope, Send

from faststream_fastapi._internal.asyncapi_router import AsyncAPIRouter
from faststream_fastapi._internal.background_middleware import _BackgroundMiddleware
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


def _subscriber_compatibility_wrapper(
    config: Config,
    context: ContextRepo,
    dependencies: Iterable[Depends],
) -> Callable[[Callable[..., Any]], Callable[["StreamMessage[Any]"], Awaitable[Any]]]:
    def subscriber_compatibility_wrapper(
        endpoint: Callable[..., Any],
    ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
        return wrap_callable_to_fastapi_compatible(
            user_callable=endpoint,
            config=config,
            context=context,
            dependencies=dependencies,
        )

    return subscriber_compatibility_wrapper


class FastStreamAPI:
    def __init__(
        self,
        *brokers: BrokerUsecase[Any, Any],
        application: FastAPI,
        context: ContextRepo | None = None,
        # AsyncAPI
        specification: SpecificationFactory | None = None,
        asyncapi_path: str | AsyncAPIConfig | None = None,
    ) -> None:
        self._application = application

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
                dependencies = (
                    *broker.config.broker_dependencies,
                    *subscriber._call_options.dependencies,
                )
                subscriber._call_decorators = (
                    _subscriber_compatibility_wrapper(
                        config=self._config,
                        context=self._startable_application.context,
                        dependencies=dependencies,  # type: ignore[arg-type]
                    ),
                    *subscriber._call_decorators,
                )

    # For FastStream docs gen
    @property
    def schema(self) -> SpecificationFactory:
        return self._startable_application.schema

    @asynccontextmanager
    async def _lifespan_context(self, application: Any) -> AsyncIterator[None]:
        if self._asyncapi_config is not None:
            asyncapi_router = AsyncAPIRouter(
                brokers=self._brokers,
                config=self._asyncapi_config,
                schema=self._startable_application.schema,
            )
            self._application.include_router(asyncapi_router)

        started_brokers: list[BrokerUsecase[Any, Any]] = []

        try:
            for broker in self._brokers:
                await broker.start()
                started_brokers.append(broker)

            yield None
        finally:
            for started_broker in started_brokers:
                await started_broker.stop()

    async def lifespan(self, scope: Scope, receive: Receive, send: Send) -> None:
        started = False
        app: Any = scope.get("app")
        await receive()
        try:
            async with (
                self._lifespan_context(app),
                self._application.router.lifespan_context(app) as maybe_state,
            ):
                if maybe_state is not None:
                    if "state" not in scope:
                        msg = 'The server does not support "state" in the lifespan scope.'
                        raise RuntimeError(msg)  # noqa: TRY301

                    scope["state"].update(maybe_state)

                await send({"type": "lifespan.startup.complete"})
                started = True
                await receive()
        except BaseException:
            exc_text = traceback.format_exc()
            if started:
                await send({"type": "lifespan.shutdown.failed", "message": exc_text})
            else:
                await send({"type": "lifespan.startup.failed", "message": exc_text})
            raise
        else:
            await send({"type": "lifespan.shutdown.complete"})

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self.lifespan(scope, receive, send)
            return None

        return await self._application(scope, receive, send)
