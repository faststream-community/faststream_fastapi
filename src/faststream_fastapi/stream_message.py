from typing import Any, Generic

from fastapi import Request
from faststream._internal.types import MsgType


class StreamMessage(Request, Generic[MsgType]):
    scope: dict[str, Any]
    _cookies: dict[str, Any]
    _headers: dict[str, Any]  # type: ignore[assignment]
    _body: dict[str, Any] | list[Any]  # type: ignore[assignment]
    _query_params: dict[str, Any]  # type: ignore[assignment]

    def __init__(
        self,
        *,
        body: dict[str, Any] | list[Any],
        headers: dict[str, Any],
        path: dict[str, Any],
    ) -> None:
        self._headers = headers
        self._body = body
        self._query_params = path

        self.scope = {"path_params": self._query_params}
        self._cookies = {}
