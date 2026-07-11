import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from functools import wraps
from itertools import dropwhile
from typing import Any, ParamSpec, TypeVar

from fast_depends.dependencies import Dependant
from fastapi.dependencies.models import Dependant as FastAPIDependant
from fastapi.params import Depends
from fastapi.routing import run_endpoint_function, serialize_response
from faststream.exceptions import SetupError
from faststream.message import StreamMessage as NativeMessage
from faststream.response import Response, ensure_response

from faststream_fastapi._internal.config import Config
from faststream_fastapi._internal.fastapi_compat import (
    FASTAPI_V106,
    FASTAPI_V121,
    raise_fastapi_validation_error,
    solve_faststream_dependency,
)
from faststream_fastapi._internal.fs_re_exports.context import Context, ContextRepo
from faststream_fastapi._internal.get_dependant import (
    get_fastapi_native_dependant,
    has_forbidden_types,
    is_faststream_decorated,
    mark_faststream_decorated,
)
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
    if has_forbidden_types(user_callable, (Dependant,)):
        msg = (
            f"Incorrect `faststream.Depends` usage at `{user_callable.__name__}`. "
            "For FastAPI integration use `fastapi.Depends` instead."
        )
        raise SetupError(msg)

    if has_forbidden_types(user_callable, (Context,)):
        msg = (
            f"Incorrect `faststream.Context` usage at `{user_callable.__name__}`. "
            "For FastAPI integration use `faststream.[broker].fastapi.Context` instead."
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

    dependencies_names = tuple(i.name for i in dependent.dependencies)

    first_arg = next(
        dropwhile(
            lambda i: i in dependencies_names,
            inspect.signature(dependent.call).parameters,
        ),
        None,
    )

    async def parsed_consumer(message: NativeMessage[Any]) -> Any:
        body = await message.decode()

        fastapi_body: dict[str, Any] | list[Any]
        stream_message: StreamMessage
        if first_arg is not None:
            if isinstance(body, dict):
                path = fastapi_body = body or {}
            elif isinstance(body, list):
                fastapi_body, path = body, {}
            else:
                path = fastapi_body = {first_arg: body}

            stream_message = StreamMessage(
                body=fastapi_body,
                headers={"context__": context, **message.headers},
                path={**path, **message.path},
            )

        else:
            stream_message = StreamMessage(
                body={},
                headers={"context__": context},
                path={},
            )
        return await consume(stream_message, message)

    return parsed_consumer


def make_fastapi_execution(
    *,
    dependent: FastAPIDependant,
    config: Config,
) -> Callable[
    [StreamMessage, NativeMessage[Any]],
    Awaitable[Response],
]:
    is_coroutine = asyncio.iscoroutinefunction(dependent.call)

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
            request.scope["state"] = config.asgi_state

            solved_result = await solve_faststream_dependency(
                request=request,
                dependant=dependent,
                dependency_overrides_provider=config.dependency_overrides_provider,
                **kwargs,
            )

            raw_message.background = solved_result.background_tasks  # type: ignore[attr-defined]

            if solved_result.errors:
                raise_fastapi_validation_error(solved_result.errors, request._body)  # type: ignore[arg-type]

            function_result = await run_endpoint_function(
                dependant=dependent,
                values=solved_result.values,
                is_coroutine=is_coroutine,
            )

            response = ensure_response(function_result)

            response.body = await serialize_response(
                response_content=response.body,
                is_coroutine=is_coroutine,
            )

            return response

        raise AssertionError("unreachable")

    return app
