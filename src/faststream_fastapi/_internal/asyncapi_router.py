import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from faststream.asgi.factories.asyncapi.try_it_out import TryItOutProcessor
from faststream.specification.asyncapi.site import get_asyncapi_html
from faststream.specification.base import SpecificationFactory

from faststream_fastapi._internal.fs_re_exports.broker import BrokerUsecase
from faststream_fastapi.asyncapi_config import AsyncAPIConfig


class AsyncAPIRouter(APIRouter):
    def __init__(
        self,
        brokers: Sequence[BrokerUsecase[Any, Any]],
        config: AsyncAPIConfig,
        schema: SpecificationFactory,
    ) -> None:
        super().__init__(include_in_schema=config.include_in_schema)

        self._config = config

        try:
            try_processor = TryItOutProcessor(*brokers)
        except ValueError:
            try_processor = None

        self.get(self._config.path)(self.make_serve_asyncapi_schema(schema))

        if self._config.asyncapi_json_path is not None:
            self.get(self._config.asyncapi_json_path)(self.make_download_app_json_schema(schema))

        if self._config.asyncapi_yaml_path is not None:
            self.get(self._config.asyncapi_yaml_path)(self.make_download_app_yaml_schema(schema))

        if try_processor is not None and self._config.try_it_out_path is not None:
            self.post(self._config.try_it_out_path, include_in_schema=False)(
                self.make_try_asyncapi_schema(try_processor),
            )

    def make_download_app_json_schema(
        self,
        schema: SpecificationFactory,
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
        self,
        schema: SpecificationFactory,
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
        self,
        schema: SpecificationFactory,
    ) -> Callable[[], Awaitable[Response]]:
        async def serve_asyncapi_schema() -> Response:
            return HTMLResponse(
                content=get_asyncapi_html(
                    schema.to_specification(),
                    sidebar=self._config.sidebar,
                    info=self._config.info,
                    servers=self._config.servers,
                    operations=self._config.operations,
                    messages=self._config.messages,
                    schemas=self._config.schemas,
                    errors=self._config.errors,
                    expand_message_examples=self._config.expand_message_examples,
                    asyncapi_js_url=self._config.asyncapi_js_url,
                    asyncapi_css_url=self._config.asyncapi_css_url,
                    try_it_out_plugin_url=self._config.try_it_out_plugin_url,
                    try_it_out_path=self._config.try_it_out_path,
                ),
            )

        return serve_asyncapi_schema

    def make_try_asyncapi_schema(
        self,
        try_processor: TryItOutProcessor,
    ) -> Callable[[Request], Awaitable[Response]]:
        async def try_asyncapi_schema(request: Request) -> Response:
            try:
                body = await request.json()
            except Exception as e:  # noqa: BLE001
                return JSONResponse({"details": f"Invalid JSON: {e}"}, 400)

            result = await try_processor.process(body)
            return Response(
                content=result.body,
                status_code=result.status_code,
                media_type="application/json",
            )

        return try_asyncapi_schema
