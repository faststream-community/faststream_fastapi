# Context Fields Declaration

You can also store your own objects in the **Context** from **faststream**.

!!! note
    You can use a regular `faststream_fastapi.Context`


## Global

To declare an application-level context field, you need to create a ContextRepo and pass it to the FastStreamAPI.

```py
from faststream_fastapi import FastStreamAPI

application = FastStreamAPI(...)

application.context.set_global("secret_str", "my-perfect-secret")
```

or

```py
from faststream.context import ContextRepo
from faststream_fastapi import FastStreamAPI

FastStreamAPI(
    ...,
    context=ContextRepo(
        {
            "secret_str": "my-perfect-secret"
        },
    ),
)
```

## Local

To set a local context (available only within the message processing scope), use the context manager scope.
It could me extremely uselful to fill context with additional options in Middlewares

```py
from typing import Any, Annotated

from fastapi import FastAPI
from faststream import BaseMiddleware, Context
from faststream.nats import NatsBroker, NatsMessage
from faststream.types import AsyncFuncAny
from faststream.message import StreamMessage
from faststream_fastapi import FastStreamAPI

class Middleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        with self.context.scope("correlation_id", msg.correlation_id):
            return await super().consume_scope(call_next, msg)

broker = NatsBroker(middlewares=[Middleware])
app = FastStreamAPI(broker, application=FastAPI())

@broker.subscriber("test-subject")
async def handle(
    message: NatsMessage,  # get from the context too
    correlation_id: Annotated[str, Context()],
) -> None:
    assert correlation_id == message.correlation_id
```

## How to get сontext object

To do this, you need to use

```py
from faststream import Context

@broker.subscriber("subject")
async def handle(context = Context()) -> None: ...
```

or `Context("context")`

```py
from faststream import Context

@broker.subscriber("subject")
async def handle(context_repo = Context("context")) -> None: ...
```

or you can use a ready-made Annotated type from the plugin.

```py
from faststream import ContextRepo

@broker.subscriber("subject")
async def handle(context_repo: ContextRepo) -> None: ...
```


## More information

For more information, refer to the FastStream documentation on [Context](https://faststream.ag2.ai/latest/getting-started/context/)
