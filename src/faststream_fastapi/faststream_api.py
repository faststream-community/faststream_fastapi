import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import AsyncExitStack, asynccontextmanager
from logging import getLogger
from typing import Any, Self
from weakref import WeakSet

from fastapi import FastAPI
from fastapi.params import Depends
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
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

_logger = getLogger(__name__)


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


async def _default_connection_hook(broker: BrokerUsecase[Any, Any]) -> None:
    await broker.start()


class FastStreamAPI:
    def __init__(
        self,
        *brokers: BrokerUsecase[Any, Any],
        application: FastAPI,
        context: ContextRepo | None = None,
        max_streaming_reply_size: int = 1024 * 1024,
        # AsyncAPI
        specification: SpecificationFactory | None = None,
        asyncapi_path: str | AsyncAPIConfig | None = None,
        # Low API
        safe_connection: bool = False,
        connection_hook: Callable[[Any], Awaitable[None]] | None = None,
    ) -> None:
        if max_streaming_reply_size <= 0:
            msg = "max_streaming_reply_size must be positive."
            raise ValueError(msg)

        self._application = application

        self._brokers = brokers
        self._patched_subscribers: WeakSet[SubscriberUsecase[Any]] = WeakSet()
        self._patched_brokers: list[BrokerUsecase[Any, Any]] = []
        self._asyncapi_router_added = False

        self._startable_application = StartAbleApplication(
            *brokers,
            specification=specification,
            config=FastDependsConfig(
                get_dependent=get_fastapi_dependant,
                context=context or ContextRepo(),
            ),
        )
        self._startable_application.config.context.set_global("app", self)

        self._config = Config(
            application=application,
            dependency_overrides_provider=self._application,
            context=self._startable_application.context,
            max_streaming_reply_size=max_streaming_reply_size,
        )

        if asyncapi_path is not None:
            if isinstance(asyncapi_path, str):
                asyncapi_config = AsyncAPIConfig(asyncapi_path)
            else:
                asyncapi_config = asyncapi_path

        else:
            asyncapi_config = None

        self._asyncapi_config = asyncapi_config
        self._safe_connection = safe_connection

        if connection_hook is None:
            connection_hook = _default_connection_hook

        self._connection_hook = connection_hook

        for broker in self._brokers:
            self._patch_broker(broker)

    # For FastStream docs gen
    @property
    def schema(self) -> SpecificationFactory:
        for broker in self._brokers:
            self._patch_broker(broker)
        return self._startable_application.schema

    @property
    def broker(self) -> BrokerUsecase[Any, Any] | None:
        return self._startable_application.broker

    @property
    def brokers(self) -> list[BrokerUsecase[Any, Any]]:
        return self._startable_application.brokers

    @property
    def context(self) -> ContextRepo:
        return self._startable_application.context

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope["app"] = self._application

        if scope["type"] == "lifespan":
            await self.lifespan(scope, receive, send)
            return None

        return await self._application(scope, receive, send)

    async def lifespan(self, scope: Scope, receive: Receive, send: Send) -> None:
        started = False
        app: Any = scope.get("app")
        await receive()
        try:
            async with self._application.router.lifespan_context(app) as maybe_state:
                if maybe_state is not None:
                    if "state" not in scope:
                        msg = 'The server does not support "state" in the lifespan scope.'
                        raise RuntimeError(msg)  # noqa: TRY301

                    scope["state"].update(maybe_state)

                self._config.asgi_state = scope.get("state", {})
                async with self._lifespan_context(app):
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

    @asynccontextmanager
    async def _lifespan_context(self, application: Self) -> AsyncGenerator[None, None]:
        if self._asyncapi_config is not None and not self._asyncapi_router_added:
            asyncapi_router = AsyncAPIRouter(
                brokers=self._brokers,
                config=self._asyncapi_config,
                schema=self.schema,
            )
            self._application.include_router(asyncapi_router)
            self._asyncapi_router_added = True

        async with AsyncExitStack() as stack:
            for broker in self._brokers:
                self._patch_broker(broker)
                stack.push_async_callback(broker.stop)
                await self._connect_broker(broker)

            yield None

    def _patch_broker(self, broker: BrokerUsecase[Any, Any]) -> None:
        if broker not in self._patched_brokers:
            broker.config.add_middleware(_BackgroundMiddleware)
            self._patched_brokers.append(broker)

        for subscriber in broker.subscribers:
            if subscriber in self._patched_subscribers:
                continue

            dependencies = (
                *broker.config.broker_dependencies,
                *subscriber._call_options.dependencies,
            )
            subscriber._call_decorators = (
                _subscriber_compatibility_wrapper(
                    config=self._config,
                    context=broker.context,
                    dependencies=dependencies,  # type: ignore[arg-type]
                ),
                *subscriber._call_decorators,
            )
            self._patched_subscribers.add(subscriber)

    async def _connect_broker(self, broker: BrokerUsecase[Any, Any]) -> None:
        try:
            await self._connection_hook(broker)
        except Exception as error:
            if self._safe_connection:
                raise
            _logger.warning("The start to broker %r was unsuccessful.", broker, exc_info=error)
