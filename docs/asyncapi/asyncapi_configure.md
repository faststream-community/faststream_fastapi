# AsyncAPI Configure

You can configure **AsyncAPI** in your application using two things.
`SpecificationFactory` from FastStream itself, through brokers and through the plugin config `AsyncAPIConfig`.
We will not focus on the first two methods. They are described in the [documentation](https://faststream.ag2.ai/latest/getting-started/asyncapi/custom/)

!!! Tip
    Inject `SpecificationFactory` in `FastStreamAPI` as follows:
    ```python
    from faststream.specification import AsyncAPI
    from faststream_fastapi import FastStreamAPI

    application = FastStreamAPI(
        ...,
        specification=AsyncAPI(...),
    )
    ```

## AsyncAPIConfig

This is a plugin config that partially configures **AsyncAPI**, http handles and asyncapi documentation.

### Simple usage

The simplest application is to install the asyncapi documentation.
It doesn't exist by default.

```python
from faststream_fastapi import FastStreamAPI

application = FastStreamAPI(
    ...,
    asyncapi_path="/asyncapi",
) 
```

### Custom configure

To do this, use `AsyncAPIConfig`.

```python
from faststream_fastapi import FastStreamAPI, AsyncAPIConfig

application = FastStreamAPI(
    ...,
    asyncapi_path=AsyncAPIConfig(
        "/asyncapi",
        description="AsyncAPI Description",
        tags=["tag1", "tag2"],
        unique_id="unique_id",
        include_in_schema=True,
        asyncapi_json_path="asyncapi_json", # (1)!
        asyncapi_yaml_path="asyncapi_yaml", # (2)!
        try_it_out_path="try_it_out", # (3)!
        sidebar=True,
        info=True,
        servers=True,
        operations=True,
        messages=True,
        schemas=True,
        errors=True,
        expand_message_examples=True,
        # custom paths to js scripts for asyncapi docs
        asyncapi_js_url="...",
        asyncapi_css_url="...",
        try_it_out_plugin_url="...",
    ),
) 
```

1.  full path `/asyncapi/asyncapi_json.json`
2.  full path `/asyncapi/asyncapi_yaml.yaml`
3.  full path `/asyncapi/try_it_out/try`

