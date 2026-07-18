from collections.abc import Callable
from typing import Annotated, Any

from fastapi import params

from faststream_fastapi._internal.fs_re_exports.constants import EMPTY
from faststream_fastapi._internal.fs_re_exports.context import (
    resolve_context_by_name,
)


def Context(  # noqa: N802
    name: str = "",
    *,
    default: Any = EMPTY,
    initial: Callable[..., Any] | None = None,
) -> Any:
    def solve_context(
        context: Annotated[
            Any,
            params.Header(alias="context__", include_in_schema=False),
        ],
    ) -> Any:
        return resolve_context_by_name(
            name=name,
            default=default,
            initial=initial,
            context=context,
        )

    return params.Depends(solve_context, use_cache=True)
