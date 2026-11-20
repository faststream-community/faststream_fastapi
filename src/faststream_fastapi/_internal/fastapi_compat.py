from dataclasses import dataclass
from typing import Any, Never

from fastapi import __version__ as FASTAPI_VERSION  # noqa: N812
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import solve_dependencies
from starlette.background import BackgroundTasks
from starlette.responses import Response

from faststream_fastapi._internal.get_dependant import iter_dependants
from faststream_fastapi.stream_message import StreamMessage

major, minor, patch, *_ = FASTAPI_VERSION.split(".")

_FASTAPI_MAJOR, _FASTAPI_MINOR = int(major), int(minor)

FASTAPI_V2 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR > 100
FASTAPI_V106 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 106
FASTAPI_V121 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 121
FASTAPI_V128 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 128

try:
    _FASTAPI_PATCH = int(patch)
except ValueError:
    FASTAPI_v102_3 = True
    FASTAPI_v102_4 = True
else:
    FASTAPI_v102_3 = (
        _FASTAPI_MAJOR > 0
        or _FASTAPI_MINOR > 112
        or (_FASTAPI_MINOR == 112 and _FASTAPI_PATCH > 2)
    )
    FASTAPI_v102_4 = (
        _FASTAPI_MAJOR > 0
        or _FASTAPI_MINOR > 112
        or (_FASTAPI_MINOR == 112 and _FASTAPI_PATCH > 3)
    )

__all__ = (
    "RequestValidationError",
    "create_response_field",
    "raise_fastapi_validation_error",
    "solve_faststream_dependency",
)


@dataclass
class SolvedDependency:
    values: dict[str, Any]
    errors: list[Any]
    background_tasks: BackgroundTasks | None
    response: Response


if FASTAPI_V128:
    from fastapi.exceptions import RequestValidationError

    def raise_fastapi_validation_error(
        errors: list[Any],
        body: Any,
    ) -> Never:
        raise RequestValidationError(errors, body=body)

elif FASTAPI_V2:
    from fastapi._compat import _normalize_errors  # type: ignore[attr-defined]
    from fastapi.exceptions import RequestValidationError

    def raise_fastapi_validation_error(
        errors: list[Any],
        body: Any,
    ) -> Never:
        raise RequestValidationError(_normalize_errors(errors), body=body)

else:
    from pydantic import (  # type: ignore[assignment]
        ValidationError as RequestValidationError,
    )
    from pydantic import (
        create_model,
    )

    ROUTER_VALIDATION_ERROR_MODEL = create_model("StreamRoute")

    def raise_fastapi_validation_error(
        errors: list[Any],
        body: Any,
    ) -> Never:
        raise RequestValidationError(errors, ROUTER_VALIDATION_ERROR_MODEL)  # type: ignore[call-arg]


if FASTAPI_v102_3:
    from fastapi.utils import (
        create_model_field as create_response_field,
    )

    async def solve_faststream_dependency(
        request: StreamMessage,
        dependant: Dependant,
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        body_fields = [
            field for dependency in iter_dependants(dependant) for field in dependency.body_params
        ]
        embed_body_fields = len({field.name for field in body_fields}) > 1 or any(
            getattr(field.field_info, "embed", False) for field in body_fields
        )
        extra = {"embed_body_fields": embed_body_fields} if FASTAPI_v102_4 else {}
        solved_result = await solve_dependencies(
            request=request,
            body=request.decoded_body,
            dependant=dependant,
            dependency_overrides_provider=dependency_overrides_provider,
            **extra,  # type: ignore[arg-type]
            **kwargs,
        )
        values, errors, background = (
            solved_result.values,
            solved_result.errors,
            solved_result.background_tasks,
        )

        return SolvedDependency(
            values=values,
            errors=errors,
            background_tasks=background,
            response=solved_result.response,
        )

else:
    from fastapi.utils import (  # type: ignore[attr-defined,no-redef]
        create_response_field,
    )

    async def solve_faststream_dependency(
        request: StreamMessage,
        dependant: Dependant,
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        solved_result = await solve_dependencies(
            request=request,
            body=request.decoded_body,
            dependant=dependant,
            dependency_overrides_provider=dependency_overrides_provider,
            **kwargs,
        )

        (
            values,
            errors,
            background,
            response,
            _dependency_cache,
        ) = solved_result  # type: ignore[misc]

        return SolvedDependency(
            values=values,  # type: ignore[has-type]
            errors=errors,  # type: ignore[has-type]
            background_tasks=background,  # type: ignore[has-type]
            response=response,  # type: ignore[has-type]
        )
