# Get started

**faststream_fastapi** it is **FastAPI** plugin for **FastStream**.

## Installing

=== "pip"
    ```bash
    pip install faststream_fastapi
    ```

=== "uv"
    ```shell
    uv add faststream_fastapi
    ```

=== "poetry"
    ```shell
    poetry add faststream_fastapi
    ```

## Basic Usage

Add the following code to a new file(e.g `serve.py`):

```py title="serve.py"
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Response, Path, Query
from faststream.nats import NatsBroker
from faststream_fastapi import FastStreamAPI

fastapi = FastAPI()
broker = NatsBroker()

@broker.subscriber("hi.{name}")
async def hello(name: Annotated[str, Path()]) -> Response:
    return Response(f"Hello, {name}!")

@fastapi.get("/")
async def hello_web_handler(name: Annotated[str, Query()]) -> Response:
    resp = await broker.request(f"Hi, my name is {name}!", f"hi.{name}")
    return Response(await resp.decode())

application = FastStreamAPI(broker, application=fastapi)
```

And just run this command:
```shell title="console"
uvicorn run serve:application
```
