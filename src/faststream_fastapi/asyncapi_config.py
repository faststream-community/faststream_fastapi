from collections.abc import Sequence
from typing import Any

from faststream.specification.asyncapi.site import (
    ASYNCAPI_CSS_DEFAULT_URL,
    ASYNCAPI_JS_DEFAULT_URL,
    ASYNCAPI_TRY_IT_PLUGIN_URL,
)
from faststream.specification.schema import Tag, TagDict


class _Empty: ...


_EMPTY = _Empty()


class AsyncAPIConfig:
    def __init__(
        self,
        path: str,
        description: str | None = None,
        tags: Sequence[Tag | TagDict | dict[str, Any]] | None = None,
        unique_id: str | None = None,
        include_in_schema: bool = False,
        asyncapi_json_path: str | None | _Empty = _EMPTY,
        asyncapi_yaml_path: str | None | _Empty = _EMPTY,
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
        self.path = path

        if isinstance(asyncapi_json_path, _Empty):
            asyncapi_json_path = path.rstrip("/") + ".json"
        self.asyncapi_json_path = asyncapi_json_path

        if isinstance(asyncapi_yaml_path, _Empty):
            asyncapi_yaml_path = path.rstrip("/") + ".yaml"
        self.asyncapi_yaml_path = asyncapi_yaml_path

        if isinstance(try_it_out_path, _Empty):
            try_it_out_path = path.rstrip("/") + "/try"
        self.try_it_out_path = try_it_out_path

        self.description = description
        self.tags = tags
        self.unique_id = unique_id
        self.include_in_schema = include_in_schema

        self.sidebar = sidebar
        self.info = info
        self.servers = servers
        self.operations = operations
        self.messages = messages
        self.schemas = schemas
        self.errors = errors
        self.expand_message_examples = expand_message_examples
        self.asyncapi_js_url = asyncapi_js_url
        self.asyncapi_css_url = asyncapi_css_url
        self.try_it_out_plugin_url = try_it_out_plugin_url
