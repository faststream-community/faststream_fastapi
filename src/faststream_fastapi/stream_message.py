from typing import Any

from fastapi import Request


class StreamMessage(Request):
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
