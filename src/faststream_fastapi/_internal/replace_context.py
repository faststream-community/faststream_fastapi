import functools
import inspect
import sys
from collections.abc import Callable, Mapping
from typing import (
    Annotated,
    Any,
    ParamSpec,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

from fastapi.dependencies.utils import get_typed_annotation
from faststream._internal.context import Context as ContextCls

from faststream_fastapi import Context as PluginContext

P_HandlerParams = ParamSpec("P_HandlerParams")
T_HandlerReturn = TypeVar("T_HandlerReturn")


def replace_context(
    func: Callable[P_HandlerParams, T_HandlerReturn],
) -> Callable[P_HandlerParams, T_HandlerReturn]:
    annotation_source: Any = func
    while isinstance(annotation_source, functools.partial):
        annotation_source = annotation_source.func

    if not inspect.isroutine(annotation_source) and not inspect.isclass(annotation_source):
        annotation_source = annotation_source.__call__

    if inspect.iscoroutinefunction(annotation_source):

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await cast(Callable[..., Any], func)(*args, **kwargs)
    else:

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

    # The callable's effective signature may have been supplied by a decorator.
    try:
        signature = inspect.signature(func, eval_str=True)
    except NameError:
        signature = inspect.signature(func)

    source = inspect.unwrap(annotation_source)
    module = sys.modules.get(getattr(source, "__module__", ""))
    globalns = getattr(source, "__globals__", vars(module) if module is not None else {})
    parameters = {
        name: param.replace(annotation=get_typed_annotation(param.annotation, globalns))
        for name, param in signature.parameters.items()
    }
    new_params = get_new_parameters(parameters)
    new_annotations = {
        param.name: param.annotation
        for param in new_params
        if param.annotation is not inspect.Parameter.empty
    }
    if signature.return_annotation is not inspect.Signature.empty:
        new_annotations["return"] = signature.return_annotation

    wrapper.__name__ = getattr(func, "__name__", type(func).__name__)
    wrapper.__qualname__ = getattr(func, "__qualname__", wrapper.__name__)
    wrapper.__doc__ = func.__doc__
    wrapper.__module__ = getattr(func, "__module__", getattr(source, "__module__", __name__))
    wrapper.__annotations__ = new_annotations
    wrapper.__signature__ = signature.replace(parameters=new_params)  # type: ignore[attr-defined]

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
