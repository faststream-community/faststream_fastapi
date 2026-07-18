import functools
import inspect
from collections.abc import Callable, Mapping
from typing import (
    Annotated,
    Any,
    ParamSpec,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from fastapi.dependencies.utils import get_typed_signature
from faststream._internal.context import Context as ContextCls

from faststream_fastapi import Context as PluginContext

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar("T_HandlerReturn")


def replace_context(
    func: Callable[P_HandlerParams, T_HandlerReturn],
) -> Callable[P_HandlerParams, T_HandlerReturn]:
    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)
    else:

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

    endpoint_signature = get_typed_signature(func)
    new_params = get_new_parameters(endpoint_signature.parameters)
    new_annotations = get_new_annotations(get_type_hints(func, include_extras=True))

    wrapper.__name__ = func.__name__
    wrapper.__qualname__ = func.__qualname__
    wrapper.__doc__ = func.__doc__
    wrapper.__module__ = func.__module__
    wrapper.__annotations__ = new_annotations
    wrapper.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=new_params,
        return_annotation=endpoint_signature.return_annotation,
    )

    return cast(Callable[P_HandlerParams, T_HandlerReturn], wrapper)


def get_new_parameters(
    signature_params: Mapping[str, inspect.Parameter],
) -> list[inspect.Parameter]:
    new_params = []

    for param in signature_params.values():
        if (
            param.annotation is not inspect.Signature.empty
            and get_origin(param.annotation) is Annotated
        ):
            new_params.append(
                inspect.Parameter(
                    name=param.name,
                    default=param.default,
                    kind=param.kind,
                    annotation=replace_annotated(param.annotation, param.name),
                ),
            )
        elif isinstance(param.default, ContextCls):
            new_param = inspect.Parameter(
                name=param.name,
                default=create_plugin_context(param.default, param.name),
                kind=param.kind,
                annotation=param.annotation,
            )
            new_params.append(new_param)
        else:
            new_params.append(param)

    return new_params


def get_new_annotations(type_hints: Mapping[str, Any]) -> dict[str, Any]:
    new_annotations = {}

    for param_name, type_hint in type_hints.items():
        if get_origin(type_hint) is Annotated:
            new_annotations[param_name] = replace_annotated(type_hint, param_name)
        else:
            new_annotations[param_name] = type_hint

    return new_annotations


def replace_annotated(type_hint: Any, parameter_name: str) -> Any:
    annotated_args = get_args(type_hint)

    new_annotated_args = [annotated_args[0]]
    for arg in annotated_args[1:]:
        if isinstance(arg, ContextCls):
            new_annotated_args.append(create_plugin_context(arg, parameter_name))
        else:
            new_annotated_args.append(arg)

    return Annotated[tuple(new_annotated_args)]


def create_plugin_context(fs_context: ContextCls, parameter_name: str) -> Any:
    fs_context.set_param_name(parameter_name)

    return PluginContext(
        name=fs_context.name or fs_context.param_name,
        default=fs_context.default,
        initial=fs_context.initial,
    )
