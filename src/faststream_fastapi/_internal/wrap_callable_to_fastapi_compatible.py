import inspect
from asyncio import Event
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from copy import copy
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from fast_depends.dependencies import Dependant
from fastapi.dependencies.models import Dependant as FastAPIDependant
from fastapi.params import Depends
from fastapi.routing import run_endpoint_function, serialize_response
from faststream.exceptions import SetupError
from faststream.message import StreamMessage as NativeMessage
from faststream.response import Response, ensure_response
from starlette.datastructures import Headers
from starlette.responses import Response as HTTPResponse
from starlette.responses import StreamingResponse
from starlette.types import Message, Scope

from faststream_fastapi._internal.config import Config
from faststream_fastapi._internal.fastapi_compat import (
    FASTAPI_V106,
    FASTAPI_V121,
    raise_fastapi_validation_error,
    solve_faststream_dependency,
)
from faststream_fastapi._internal.fs_re_exports.context import ContextRepo
from faststream_fastapi._internal.get_dependant import (
    get_fastapi_native_dependant,
    has_forbidden_types,
    is_faststream_decorated,
    iter_dependants,
    mark_faststream_decorated,
)
from faststream_fastapi._internal.replace_context import replace_context
from faststream_fastapi.stream_message import StreamMessage

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar("T_HandlerReturn")


def wrap_callable_to_fastapi_compatible(
    user_callable: Callable[P_HandlerParams, T_HandlerReturn],
    *,
    config: Config,
    context: ContextRepo,
    dependencies: Iterable[Depends],
) -> Callable[[NativeMessage[Any]], Awaitable[Any]]:
    user_callable = replace_context(user_callable)

    if has_forbidden_types(user_callable, (Dependant,)):
        msg = (
            f"Incorrect `faststream.Depends` usage at `{user_callable.__name__}`. "
            "For FastAPI integration use `fastapi.Depends` instead."
        )
        raise SetupError(msg)

    if is_faststream_decorated(user_callable):
        return user_callable  # type: ignore[return-value]

    parsed_callable = build_faststream_to_fastapi_parser(
        dependent=get_fastapi_native_dependant(user_callable, dependencies),
        config=config,
        context=context,
    )

    mark_faststream_decorated(parsed_callable)
    return wraps(user_callable)(parsed_callable)


def build_faststream_to_fastapi_parser(
    *,
    dependent: FastAPIDependant,
    config: Config,
    context: ContextRepo,
) -> Callable[[NativeMessage[Any]], Awaitable[Any]]:
    if dependent.call is None:  # pragma: no cover
        raise ValueError("dependent.call is None")  # noqa: TRY003

    consume = make_fastapi_execution(
        dependent=dependent,
        config=config,
    )

    first_arg = _get_scalar_payload_alias(dependent)

    async def parsed_consumer(message: NativeMessage[Any]) -> Any:
        body = await message.decode()

        if isinstance(body, dict):
            path = body
        elif first_arg is not None and not isinstance(body, list):
            path = {first_arg: body}
        else:
            path = {}

        stream_message = StreamMessage(
            body=body,
            headers=message.headers,
            path={**path, **message.path},
            raw_body=message.body if isinstance(message.body, bytes) else None,
        )
        stream_message.scope["faststream_context__"] = context
        stream_message.scope["faststream_config__"] = config
        return await consume(stream_message, message)

    return parsed_consumer


def _get_scalar_payload_alias(dependant: FastAPIDependant) -> str | None:
    for dependency in iter_dependants(dependant):
        fields = {
            field.name: field.alias
            for field in (*dependency.query_params, *dependency.path_params)
        }
        if fields and dependency.call is not None:
            for name in inspect.signature(dependency.call).parameters:
                if name in fields:
                    return fields[name]
    return None


def make_fastapi_execution(
    *,
    dependent: FastAPIDependant,
    config: Config,
) -> Callable[
    [StreamMessage, NativeMessage[Any]],
    Awaitable[Response],
]:
    is_coroutine = inspect.iscoroutinefunction(dependent.call)

    async def app(
        request: StreamMessage,
        raw_message: NativeMessage[Any],  # to support BackgroundTasks by middleware
    ) -> Response:
        async with AsyncExitStack() as stack:
            kwargs = {}
            if FASTAPI_V121:
                request.scope["fastapi_inner_astack"] = stack
                function_stack = AsyncExitStack()
                await stack.enter_async_context(function_stack)
                request.scope["fastapi_function_astack"] = function_stack

            if FASTAPI_V106:
                kwargs = {"async_exit_stack": stack}

            else:  # pragma: no cover
                request.scope["fastapi_astack"] = stack

            request.scope["app"] = config.application
            request.scope["state"] = config.asgi_state.copy()

            solved_result = await solve_faststream_dependency(
                request=request,
                dependant=dependent,
                dependency_overrides_provider=config.dependency_overrides_provider,
                **kwargs,
            )

            raw_message.background = solved_result.background_tasks  # type: ignore[attr-defined]

            if solved_result.errors:
                raise_fastapi_validation_error(solved_result.errors, request.decoded_body)

            function_result = await run_endpoint_function(
                dependant=dependent,
                values=solved_result.values,
                is_coroutine=is_coroutine,
            )

            if isinstance(function_result, HTTPResponse):
                raw_message.background = (  # type: ignore[attr-defined]
                    function_result.background or solved_result.background_tasks
                )
                return await _to_broker_response(
                    function_result,
                    request.scope,
                    config.max_streaming_reply_size,
                )

            response = ensure_response(function_result)
            if solved_result.response.raw_headers:
                response.headers = {
                    **_broker_headers(solved_result.response.headers),
                    **response.headers,
                }

            response.body = await serialize_response(
                response_content=response.body,
                is_coroutine=is_coroutine,
            )

            return response

        raise AssertionError("unreachable")

    return app


async def _to_broker_response(
    response: HTTPResponse,
    scope: Scope,
    max_streaming_reply_size: int,
) -> Response:
    if hasattr(response, "body"):
        return Response(bytes(response.body), headers=_broker_headers(response.headers))

    # A broker reply carries one payload. Collect the ASGI stream before publishing it.
    payload = bytearray()
    headers: dict[str, str] = {}
    finished = Event()

    async def receive() -> Message:
        await finished.wait()
        return {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        nonlocal headers
        if message["type"] == "http.response.start":
            headers = _broker_headers(Headers(raw=message["headers"]))
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            chunk_size = chunk.nbytes if isinstance(chunk, memoryview) else len(chunk)
            if len(payload) + chunk_size > max_streaming_reply_size:
                msg = f"Broker reply exceeds the {max_streaming_reply_size}-byte streaming limit."
                raise ValueError(msg)
            payload.extend(chunk)
            if not message.get("more_body", False):
                finished.set()

    # The broker middleware runs the original background task after processing.
    stream = copy(response)
    stream.background = None
    try:
        await stream({**scope, "asgi": {"version": "3.0", "spec_version": "2.4"}}, receive, send)
    finally:
        if isinstance(stream, StreamingResponse) and isinstance(
            stream.body_iterator,
            AsyncGenerator,
        ):
            await stream.body_iterator.aclose()
    return Response(bytes(payload), headers=headers)


def _broker_headers(headers: Headers) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in headers.items():
        if name not in result:
            result[name] = value
        elif name == "set-cookie":
            msg = "Broker response headers cannot represent multiple Set-Cookie values."
            raise ValueError(msg)
        else:
            result[name] += ", " + value
    return result
