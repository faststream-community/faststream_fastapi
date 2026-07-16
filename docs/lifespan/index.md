# Lifespan

The lifespan in FastAPI allows you to set your own state

```py
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI

@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[dict[str, Any]]:
    yield {"data": "data"}
```

And you can access this state as follows

```py
from fastapi import Request

@broker.subscriber("subject")
async def handle(request: Request) -> None:
    assert request.state.data == "data"
```

!!! Also
    Also, at the time of the launch of lifespan, all your brokers are running
