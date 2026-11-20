from collections.abc import Callable
from typing import Any

from fastapi import Request, params

from faststream_fastapi._internal.config import Config
from faststream_fastapi._internal.fs_re_exports.constants import EMPTY
from faststream_fastapi._internal.fs_re_exports.context import (
    ContextRepo,
    resolve_context_by_name,
)


def Context(  # noqa: N802
    name: str = "",
    *,
    default: Any = EMPTY,
    initial: Callable[..., Any] | None = None,
) -> Any:
    def solve_context(request: Request) -> Any:
        context: ContextRepo = request.scope["faststream_context__"]
        try:
            return context.resolve(name)
        except (KeyError, AttributeError):
            if EMPTY != default or initial is None:  # noqa: SIM300
                return default

        config: Config = request.scope["faststream_config__"]
        # FastAPI runs sync dependencies in worker threads. Recheck under the lock
        # so concurrent subscribers reuse the first value initialized by the application.
        with config.context_init_lock:
            return resolve_context_by_name(
                name=name,
                default=default,
                initial=initial,
                context=config.context,
            )

    return params.Depends(solve_context, use_cache=True)
