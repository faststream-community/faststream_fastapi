from typing import Any

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from starlette.datastructures import Headers
from starlette.types import Message, Scope
from typing_extensions import override

from faststream_fastapi._internal.fs_re_exports._compat import json_dumps


class _MessageHeaders(Headers):
    """Expose broker header text (including Unicode) through the Starlette header interface."""

    def __init__(self, headers: dict[str, Any]) -> None:
        raw: list[tuple[bytes, bytes]] = []
        for name, value in headers.items():
            values = value if isinstance(value, (list, tuple)) else (value,)
            raw.extend(
                (name.lower().encode(), item if isinstance(item, bytes) else str(item).encode())
                for item in values
            )
        super().__init__(raw=raw)

    @override
    def __getitem__(self, key: str) -> str:
        key_bytes = key.lower().encode()
        for name, value in self._list:
            if name == key_bytes:
                return value.decode("utf-8", "surrogateescape")
        raise KeyError(key)

    @override
    def __contains__(self, key: Any) -> bool:
        return bool(self.getlist(key))

    @override
    def keys(self) -> list[str]:  # type: ignore[override]
        return [name.decode() for name, _ in self._list]

    @override
    def getlist(self, key: str) -> list[str]:
        key_bytes = key.lower().encode()
        return [v.decode("utf-8", "surrogateescape") for k, v in self._list if k == key_bytes]

    @override
    def items(self) -> list[tuple[str, str]]:  # type: ignore[override]
        return [
            (key.decode(), value.decode("utf-8", "surrogateescape")) for key, value in self._list
        ]

    @override
    def values(self) -> list[str]:  # type: ignore[override]
        return [value.decode("utf-8", "surrogateescape") for _, value in self._list]


class StreamMessage(Request):
    def __init__(
        self,
        *,
        body: Any,
        headers: dict[str, Any],
        path: dict[str, Any],
        raw_body: bytes | None = None,
    ) -> None:
        message_headers = _MessageHeaders(headers)
        scope: Scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "root_path": "",
            "scheme": "http",
            "query_string": b"",
            "headers": message_headers.raw,
            "path_params": path,
        }
        consumed = False

        async def receive() -> Message:
            nonlocal consumed
            if consumed:
                return {"type": "http.disconnect"}
            consumed = True
            data = raw_body if raw_body is not None else json_dumps(jsonable_encoder(body))
            return {"type": "http.request", "body": data}

        super().__init__(scope, receive)
        self._headers = message_headers
        self.decoded_body = body
        self._query_params = path  # type: ignore[assignment]
