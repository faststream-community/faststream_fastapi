import json
from collections.abc import Sequence
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from faststream._internal.broker import BrokerUsecase
from faststream.asgi.factories.asyncapi.try_it_out import TryItOutProcessor
from faststream.specification.asyncapi.site import get_asyncapi_html
from faststream.specification.base import SpecificationFactory


class AsyncAPIRouter(APIRouter):
    def __init__(
        self,
        schema: SpecificationFactory,
        brokers: Sequence[BrokerUsecase[Any, Any]],
        schema_url: str = "/asyncapi",
    ) -> None:
        super().__init__()

        try:
            try_processor = TryItOutProcessor(*brokers)
        except ValueError:
            try_processor = None

        self._try_processor = try_processor
        self._schema_url = schema_url
        self._schema = schema

        self._set_routes()

    def _set_routes(self) -> None:
        self.get(self._schema_url)(self.serve_asyncapi_schema)
        self.get(f"{self._schema_url}.json")(self.download_app_json_schema)
        self.get(f"{self._schema_url}.yaml")(self.download_app_yaml_schema)

        if self._try_processor is not None:
            self.post(f"{self._schema_url}/try", include_in_schema=False)(
                self.try_asyncapi_schema,
            )

    def download_app_json_schema(self) -> Response:
        return Response(
            content=json.dumps(
                self._schema.to_specification().to_jsonable(),
                indent=2,
            ),
            headers={"Content-Type": "application/json"},
        )

    def download_app_yaml_schema(self) -> Response:
        return Response(
            content=self._schema.to_specification().to_yaml(),
            headers={
                "Content-Type": "application/octet-stream",
            },
        )

    def serve_asyncapi_schema(
        self,
        *,
        sidebar: bool = True,
        info: bool = True,
        servers: bool = True,
        operations: bool = True,
        messages: bool = True,
        schemas: bool = True,
        errors: bool = True,
        expandMessageExamples: bool = True,
    ) -> HTMLResponse:
        """Serve the AsyncAPI schema as an HTML response."""
        return HTMLResponse(
            content=get_asyncapi_html(
                self._schema.to_specification(),
                sidebar=sidebar,
                info=info,
                servers=servers,
                operations=operations,
                messages=messages,
                schemas=schemas,
                errors=errors,
                expand_message_examples=expandMessageExamples,
                try_it_out_path=f"{self._schema_url}/try",
            ),
        )

    async def try_asyncapi_schema(self, request: Request) -> Response:
        if self._try_processor is None:
            return JSONResponse({"details": "Try process unavailable"}, 400)

        """Publish a message coming from the AsyncAPI try-it-out plugin."""
        try:
            body = await request.json()
        except Exception as e:
            return JSONResponse({"details": f"Invalid JSON: {e}"}, 400)

        result = await self._try_processor.process(body)
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
