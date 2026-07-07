import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from faststream._internal.broker import BrokerUsecase
from faststream.asgi.factories.asyncapi.try_it_out import TryItOutProcessor
from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    ASYNCAPI_TRY_IT_PLUGIN_URL,
    get_asyncapi_html,
)
from faststream.specification.base import SpecificationFactory
from faststream.specification.schema import Tag, TagDict


class _Empty: ...


_EMPTY = _Empty()


class AsyncAPIRouter(APIRouter):
    def __init__(
        self,
        path: str,
        description: str | None = None,
        tags: Sequence[Tag | TagDict | dict[str, Any]] | None = None,
        unique_id: str | None = None,
        include_in_schema: bool = False,
        asyncapi_json_path: str | None | _Empty = _EMPTY,
        *,
        try_it_out_path: str | None | _Empty = _EMPTY,
        sidebar: bool = True,
        info: bool = True,
        servers: bool = True,
        operations: bool = True,
        messages: bool = True,
        schemas: bool = True,
        errors: bool = True,
        expand_message_examples: bool = True,
        asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
        asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
        try_it_out_plugin_url: str = ASYNCAPI_TRY_IT_PLUGIN_URL,
    ) -> None:
        self._path = path

        if isinstance(asyncapi_json_path, _Empty):
            asyncapi_json_path = path.rstrip("/") + ".json"
        self._asyncapi_json_path = asyncapi_json_path

        if isinstance(try_it_out_path, _Empty):
            try_it_out_path = path.rstrip("/") + "/try"
        self._try_it_out_path = try_it_out_path

        self._description = description
        self._tags = tags
        self._unique_id = unique_id
        self._include_in_schema = include_in_schema

        self._sidebar = sidebar
        self._info = info
        self._servers = servers
        self._operations = operations
        self._messages = messages
        self._schemas = schemas
        self._errors = errors
        self._expand_message_examples = expand_message_examples
        self._asyncapi_js_url = asyncapi_js_url
        self._asyncapi_css_url = asyncapi_css_url
        self._try_it_out_plugin_url = try_it_out_plugin_url

        super().__init__(include_in_schema=include_in_schema)

    def _setup(
        self,
        brokers: Sequence[BrokerUsecase],
        schema: SpecificationFactory,
    ) -> None:
        try:
            try_processor = TryItOutProcessor(*brokers)
        except ValueError:
            try_processor = None

        self.get(self._path)(self.make_serve_asyncapi_schema(schema))
        self.get(f"{self._path}.json")(self.make_download_app_json_schema(schema))
        self.get(f"{self._path}.yaml")(self.make_download_app_yaml_schema(schema))

        if try_processor is not None:
            self.post(f"{self._path}/try", include_in_schema=False)(
                self.make_try_asyncapi_schema(try_processor),
            )

    def make_download_app_json_schema(
        self, schema: SpecificationFactory
    ) -> Callable[[], Awaitable[Response]]:
        async def download_app_json_schema() -> Response:
            return Response(
                content=json.dumps(
                    schema.to_specification().to_jsonable(),
                    indent=2,
                ),
                headers={"Content-Type": "application/json"},
            )

        return download_app_json_schema

    def make_download_app_yaml_schema(
        self, schema: SpecificationFactory
    ) -> Callable[[], Awaitable[Response]]:
        async def download_app_yaml_schema() -> Response:
            return Response(
                content=schema.to_specification().to_yaml(),
                headers={
                    "Content-Type": "application/octet-stream",
                },
            )

        return download_app_yaml_schema

    def make_serve_asyncapi_schema(
        self, schema: SpecificationFactory
    ) -> Callable[[], Awaitable[Response]]:
        async def serve_asyncapi_schema() -> Response:
            return HTMLResponse(
                content=get_asyncapi_html(
                    schema.to_specification(),
                    sidebar=self._sidebar,
                    info=self._info,
                    servers=self._servers,
                    operations=self._operations,
                    messages=self._messages,
                    schemas=self._schemas,
                    errors=self._errors,
                    expand_message_examples=self._expand_message_examples,
                    asyncapi_js_url=self._asyncapi_js_url,
                    asyncapi_css_url=self._asyncapi_css_url,
                    try_it_out_plugin_url=self._try_it_out_plugin_url,
                    try_it_out_path=self._try_it_out_path,
                ),
            )

        return serve_asyncapi_schema

    def make_try_asyncapi_schema(
        self, try_processor: TryItOutProcessor | None
    ) -> Callable[[Request], Awaitable[Response]]:
        async def try_asyncapi_schema(request: Request) -> Response:
            if try_processor is None:
                return JSONResponse({"details": "Try process unavailable"}, 400)

            try:
                body = await request.json()
            except Exception as e:
                return JSONResponse({"details": f"Invalid JSON: {e}"}, 400)

            result = await try_processor.process(body)
            return Response(
                content=result.body,
                status_code=result.status_code,
                media_type="application/json",
            )

        return try_asyncapi_schema
